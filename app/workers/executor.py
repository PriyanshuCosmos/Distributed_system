"""Worker job executor with distributed lease renewal and fencing token protection."""

import asyncio
import time
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, Optional
from uuid import UUID

from sqlalchemy import select, update

from app.config import get_settings
from app.database import get_sessionmaker
from app.logging_config import get_logger
from app.models import Job, JobAttempt, JobStatus, utc_now
from app.queue.redis_queue import get_broker
from app.services.job_service import JobService
from app.task_registry import TaskExecutionError, registry

logger = get_logger("worker.executor")


class JobExecutor:
    """Handles atomic claiming, heartbeat leasing, execution, and state persistence for a job."""

    def __init__(self, worker_id: str) -> None:
        self.worker_id = worker_id
        self.sessionmaker = get_sessionmaker()
        self.broker = get_broker()
        self.settings = get_settings()

    async def execute_job(self, job_id_str: str) -> bool:
        """
        Claim and execute a single job.
        Returns True if claimed and executed, False if skipped.
        """
        try:
            job_id = UUID(job_id_str)
        except ValueError:
            logger.error(f"Invalid UUID received from queue: {job_id_str}")
            return False

        lease_token = uuid.uuid4().hex
        lease_duration = timedelta(seconds=self.settings.WORKER_LEASE_DURATION)
        lease_expires_at = utc_now() + lease_duration

        # 1. ATOMIC CLAIM WITH FENCING TOKEN
        async with self.sessionmaker() as session:
            # Atomic update: only claim if job is in QUEUED, RETRYING, or PENDING_DISPATCH
            stmt = (
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status.in_(
                        [JobStatus.QUEUED, JobStatus.RETRYING, JobStatus.PENDING_DISPATCH]
                    ),
                )
                .values(
                    status=JobStatus.RUNNING,
                    worker_id=self.worker_id,
                    lease_token=lease_token,
                    lease_expires_at=lease_expires_at,
                    attempt_count=Job.attempt_count + 1,
                    started_at=utc_now(),
                    updated_at=utc_now(),
                )
            )
            result = await session.execute(stmt)
            if result.rowcount == 0:
                # Job was already claimed by another worker, cancelled, or not eligible
                logger.info(
                    f"Skipping job {job_id}: could not acquire claim (already claimed or cancelled)"
                )
                return False

            await session.commit()

            # Retrieve updated job details for execution
            res = await session.execute(select(Job).where(Job.id == job_id))
            job = res.scalar_one()
            task_type = job.task_type
            payload = dict(job.payload)
            attempt_count = job.attempt_count
            max_attempts = job.max_attempts

        logger.info(
            f"Claimed job {job_id} ({task_type}) on worker {self.worker_id} "
            f"(attempt {attempt_count}/{max_attempts}, lease_token={lease_token[:8]}...)",
            extra={
                "job_id": str(job_id),
                "task_type": task_type,
                "worker_id": self.worker_id,
                "attempt": attempt_count,
            },
        )

        # 2. SPAWN HEARTBEAT RENEWAL TASK
        heartbeat_stop = asyncio.Event()
        heartbeat_task = asyncio.create_task(
            self._renew_lease_loop(job_id, lease_token, heartbeat_stop)
        )

        start_time = time.time()
        start_dt = utc_now()
        execution_status = "FAILED"
        result_data: Optional[Dict[str, Any]] = None
        error_message: Optional[str] = None

        # 3. DISPATCH TASK HANDLER
        try:
            handler = registry.get_handler(task_type)
            # Execute handler with timeout safety
            result_data = await asyncio.wait_for(
                handler(payload, attempt_count),
                timeout=self.settings.DEFAULT_TASK_TIMEOUT_SECONDS,
            )
            execution_status = "SUCCEEDED"
        except asyncio.TimeoutError:
            error_message = (
                f"Execution timed out after {self.settings.DEFAULT_TASK_TIMEOUT_SECONDS}s"
            )
            logger.error(f"Job {job_id} timed out on worker {self.worker_id}")
        except TaskExecutionError as e:
            error_message = str(e)
            logger.warning(f"Job {job_id} failed with handler error: {e}")
        except Exception as e:
            error_message = f"Unexpected execution error: {type(e).__name__}: {str(e)}"
            logger.error(f"Job {job_id} failed with unexpected exception", exc_info=True)
        finally:
            # Stop lease heartbeats immediately
            heartbeat_stop.set()
            try:
                await heartbeat_task
            except Exception:
                pass

        finish_dt = utc_now()
        duration_ms = round((time.time() - start_time) * 1000.0, 2)

        # 4. PERSIST RESULT AND ATTEMPT WITH FENCING TOKEN
        await self._finalize_job(
            job_id=job_id,
            lease_token=lease_token,
            task_type=task_type,
            attempt_count=attempt_count,
            max_attempts=max_attempts,
            start_dt=start_dt,
            finish_dt=finish_dt,
            duration_ms=duration_ms,
            execution_status=execution_status,
            result_data=result_data,
            error_message=error_message,
        )

        return True

    async def _renew_lease_loop(
        self, job_id: UUID, lease_token: str, stop_event: asyncio.Event
    ) -> None:
        """Background task renewing worker lease timestamp in DB while job is running."""
        interval = self.settings.WORKER_HEARTBEAT_INTERVAL
        duration = timedelta(seconds=self.settings.WORKER_LEASE_DURATION)

        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval)
                break
            except asyncio.TimeoutError:
                pass

            new_expiry = utc_now() + duration
            try:
                async with self.sessionmaker() as session:
                    # Update lease only if lease_token still matches (fencing invariant)
                    stmt = (
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.lease_token == lease_token,
                            Job.status == JobStatus.RUNNING,
                        )
                        .values(lease_expires_at=new_expiry, updated_at=utc_now())
                    )
                    res = await session.execute(stmt)
                    await session.commit()
                    if res.rowcount == 0:
                        logger.warning(
                            f"Worker {self.worker_id} lost lease for job {job_id}. "
                            "Job was likely reaped as stale."
                        )
                        break
            except Exception as e:
                logger.warning(f"Error renewing lease for job {job_id}: {e}")

    async def _finalize_job(
        self,
        job_id: UUID,
        lease_token: str,
        task_type: str,
        attempt_count: int,
        max_attempts: int,
        start_dt: datetime,
        finish_dt: datetime,
        duration_ms: float,
        execution_status: str,
        result_data: Optional[Dict[str, Any]],
        error_message: Optional[str],
    ) -> None:
        """Persist execution outcome, enforce fencing tokens, and handle retries or dead-lettering."""
        async with self.sessionmaker() as session:
            # Record JobAttempt history
            attempt = JobAttempt(
                job_id=job_id,
                attempt_number=attempt_count,
                worker_id=self.worker_id,
                started_at=start_dt,
                finished_at=finish_dt,
                duration_ms=duration_ms,
                status=execution_status,
                error_message=error_message,
            )
            session.add(attempt)

            if execution_status == "SUCCEEDED":
                # Fencing token check: only finalize if lease_token matches!
                stmt = (
                    update(Job)
                    .where(
                        Job.id == job_id,
                        Job.lease_token == lease_token,
                        Job.status == JobStatus.RUNNING,
                    )
                    .values(
                        status=JobStatus.SUCCEEDED,
                        result=result_data,
                        finished_at=finish_dt,
                        updated_at=finish_dt,
                        last_error=None,
                        lease_token=None,
                        lease_expires_at=None,
                    )
                )
                res = await session.execute(stmt)
                await session.commit()
                if res.rowcount == 0:
                    logger.error(
                        f"Fencing token rejected completion write for job {job_id}. "
                        "A newer attempt or reaper already reclaimed this job."
                    )
                else:
                    logger.info(
                        f"Job {job_id} succeeded in {duration_ms}ms on worker {self.worker_id}",
                        extra={
                            "job_id": str(job_id),
                            "duration_ms": duration_ms,
                            "status": "SUCCEEDED",
                        },
                    )
            else:
                # FAILED ATTEMPT: Determine if eligible for retry
                is_retryable = attempt_count < max_attempts
                if is_retryable:
                    backoff_delay = JobService.calculate_backoff(
                        attempt=attempt_count,
                        base_seconds=self.settings.BACKOFF_BASE_SECONDS,
                        max_seconds=self.settings.BACKOFF_MAX_SECONDS,
                        jitter=self.settings.BACKOFF_JITTER,
                    )
                    next_run_at = utc_now() + timedelta(seconds=backoff_delay)

                    stmt = (
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.lease_token == lease_token,
                            Job.status == JobStatus.RUNNING,
                        )
                        .values(
                            status=JobStatus.RETRYING,
                            run_at=next_run_at,
                            last_error=error_message,
                            updated_at=finish_dt,
                            lease_token=None,
                            lease_expires_at=None,
                        )
                    )
                    await session.execute(stmt)
                    await session.commit()

                    # Push to Redis scheduled queue for backoff delay
                    await self.broker.schedule(
                        job_id=job_id,
                        task_type=task_type,
                        run_at=next_run_at,
                        priority=0,
                    )
                    logger.warning(
                        f"Job {job_id} failed attempt {attempt_count}/{max_attempts}. "
                        f"Scheduled retry in {backoff_delay}s at {next_run_at.isoformat()}"
                    )
                else:
                    # MAX ATTEMPTS EXHAUSTED: Move to FAILED / Dead-Letter
                    stmt = (
                        update(Job)
                        .where(
                            Job.id == job_id,
                            Job.lease_token == lease_token,
                            Job.status == JobStatus.RUNNING,
                        )
                        .values(
                            status=JobStatus.FAILED,
                            finished_at=finish_dt,
                            last_error=error_message,
                            updated_at=finish_dt,
                            lease_token=None,
                            lease_expires_at=None,
                        )
                    )
                    await session.execute(stmt)
                    await session.commit()

                    # Push to Redis Dead Letter Queue
                    await self.broker.move_to_dead_letter(
                        job_id=job_id,
                        reason=f"Max attempts ({max_attempts}) exhausted. Error: {error_message}",
                    )
                    logger.error(
                        f"Job {job_id} permanently failed after {attempt_count} attempts. "
                        "Moved to dead-letter queue."
                    )
