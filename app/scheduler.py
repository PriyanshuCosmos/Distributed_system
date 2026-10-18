"""Scheduler and Stale Lease Reaper service for TaskFlow."""

import asyncio
import signal
import sys
import uuid
from typing import Optional

from sqlalchemy import select

from app.config import get_settings
from app.database import get_sessionmaker
from app.logging_config import get_logger, setup_logging
from app.models import Job, JobAttempt, JobStatus, utc_now
from app.queue.redis_queue import get_broker
from app.services.outbox_dispatcher import OutboxDispatcher

logger = get_logger("scheduler")


class Scheduler:
    """Manages scheduled job dispatch, outbox polling, and stale worker lease recovery."""

    def __init__(self, scheduler_id: Optional[str] = None) -> None:
        self.settings = get_settings()
        self.scheduler_id = scheduler_id or f"scheduler-{uuid.uuid4().hex[:6]}"
        self.broker = get_broker()
        self.sessionmaker = get_sessionmaker()
        self.outbox_dispatcher = OutboxDispatcher()
        self.stop_event = asyncio.Event()

    async def start(self) -> None:
        """Start the scheduler loops."""
        logger.info(f"Starting TaskFlow Scheduler '{self.scheduler_id}'")

        while not self.stop_event.is_set():
            # Acquire distributed leader lock to ensure only one scheduler runs dispatches in multi-instance setups
            lock_token = f"{self.scheduler_id}:{uuid.uuid4().hex}"
            lock_acquired = await self.broker.acquire_lock(
                "taskflow:lock:scheduler", lock_token, expire_ms=4000
            )

            if lock_acquired:
                try:
                    # 1. Dispatch due scheduled jobs in Redis
                    await self.broker.dispatch_due_scheduled_jobs(limit=100)

                    # 2. Sync due scheduled/retrying jobs in Database
                    await self._sync_due_jobs_in_db()

                    # 3. Sweep and recover stale worker leases (Crash Recovery)
                    await self._reap_stale_leases()

                    # 4. Sweep transactional outbox
                    await self.outbox_dispatcher.sweep_and_dispatch()

                except Exception as e:
                    logger.error(f"Error during scheduler cycle: {e}", exc_info=True)
                finally:
                    # Release lock safely
                    await self.broker.release_lock("taskflow:lock:scheduler", lock_token)
            else:
                logger.debug("Another scheduler instance holds the leader lock. Standing by...")

            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.settings.SCHEDULER_POLL_INTERVAL
                )
                break
            except asyncio.TimeoutError:
                pass

        logger.info(f"Scheduler {self.scheduler_id} safely stopped.")

    async def _sync_due_jobs_in_db(self) -> int:
        """Find jobs in DB with status SCHEDULED or RETRYING where run_at <= now and advance them."""
        now = utc_now()
        async with self.sessionmaker() as session:
            stmt = (
                select(Job)
                .where(
                    Job.status.in_([JobStatus.SCHEDULED, JobStatus.RETRYING]),
                    Job.run_at <= now,
                )
                .limit(50)
            )
            res = await session.execute(stmt)
            due_jobs = list(res.scalars().all())

            for job in due_jobs:
                job.status = JobStatus.QUEUED
                job.updated_at = now
                # Enqueue to Redis ready queue
                await self.broker.enqueue(job.id, job.task_type, job.priority)

            if due_jobs:
                await session.commit()
                logger.info(f"Synced {len(due_jobs)} due scheduled jobs from DB to QUEUED state")
            return len(due_jobs)

    async def _reap_stale_leases(self) -> int:
        """
        Detect and recover stale jobs where worker crashed or disconnected.
        Criteria: status == RUNNING and lease_expires_at < now()
        """
        now = utc_now()
        recovered_count = 0

        async with self.sessionmaker() as session:
            stmt = (
                select(Job)
                .where(
                    Job.status == JobStatus.RUNNING,
                    Job.lease_expires_at < now,
                )
                .limit(50)
            )
            res = await session.execute(stmt)
            stale_jobs = list(res.scalars().all())

            for job in stale_jobs:
                stale_worker = job.worker_id or "unknown"
                logger.warning(
                    f"Detected stale lease on job {job.id} (worker={stale_worker}, "
                    f"lease_expired_at={job.lease_expires_at.isoformat()}). Reaping job."
                )

                # Record timeout attempt
                attempt = JobAttempt(
                    job_id=job.id,
                    attempt_number=job.attempt_count,
                    worker_id=stale_worker,
                    started_at=job.started_at or now,
                    finished_at=now,
                    duration_ms=0.0,
                    status="TIMED_OUT",
                    error_message=f"Worker {stale_worker} lease expired without renewal (worker crash / partition detected)",
                )
                session.add(attempt)

                # Check if retries remain
                if job.attempt_count < job.max_attempts:
                    job.status = JobStatus.QUEUED
                    job.worker_id = None
                    job.lease_token = None
                    job.lease_expires_at = None
                    job.updated_at = now
                    # Re-enqueue to ready queue for another worker to pick up
                    await self.broker.enqueue(job.id, job.task_type, job.priority)
                    logger.info(
                        f"Job {job.id} recovered from crashed worker {stale_worker} and re-enqueued (attempt {job.attempt_count}/{job.max_attempts})"
                    )
                else:
                    job.status = JobStatus.FAILED
                    job.worker_id = None
                    job.lease_token = None
                    job.lease_expires_at = None
                    job.finished_at = now
                    job.last_error = f"Exhausted max attempts after worker {stale_worker} crash"
                    job.updated_at = now
                    await self.broker.move_to_dead_letter(
                        job.id,
                        reason=f"Exhausted max attempts after worker lease expiration on {stale_worker}",
                    )
                    logger.error(
                        f"Job {job.id} exhausted attempts after worker {stale_worker} crash. Moved to dead-letter."
                    )

                recovered_count += 1

            if recovered_count > 0:
                await session.commit()
                logger.info(f"Stale lease reaper recovered {recovered_count} abandoned jobs")

        return recovered_count

    def stop(self) -> None:
        """Signal scheduler to stop."""
        self.stop_event.set()


def run_scheduler_cli() -> None:
    """CLI entrypoint to run scheduler process."""
    setup_logging()
    scheduler = Scheduler()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, scheduler.stop)
    else:

        def win_sig_handler(sig, frame):
            logger.info("Received interrupt signal on Windows, stopping scheduler...")
            scheduler.stop()

        signal.signal(signal.SIGINT, win_sig_handler)
        signal.signal(signal.SIGTERM, win_sig_handler)

    try:
        loop.run_until_complete(scheduler.start())
    except (KeyboardInterrupt, SystemExit):
        scheduler.stop()
        loop.run_until_complete(asyncio.sleep(0.5))
    finally:
        loop.close()


if __name__ == "__main__":
    run_scheduler_cli()
