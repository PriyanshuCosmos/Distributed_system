"""Transactional Outbox Dispatcher for at-least-once queue delivery."""

import asyncio
from datetime import datetime

from sqlalchemy import select, update

from app.config import get_settings
from app.database import get_sessionmaker
from app.logging_config import get_logger
from app.models import Job, JobStatus, OutboxMessage, utc_now
from app.queue.redis_queue import get_broker

logger = get_logger("service.outbox")


class OutboxDispatcher:
    """Dispatches pending outbox events from PostgreSQL to Redis."""

    def __init__(self) -> None:
        self.sessionmaker = get_sessionmaker()
        self.broker = get_broker()
        self.settings = get_settings()

    async def sweep_and_dispatch(self, limit: int = 50) -> int:
        """Scan and dispatch pending outbox messages."""
        dispatched_count = 0
        async with self.sessionmaker() as session:
            # Query pending outbox messages ordered by creation time
            stmt = (
                select(OutboxMessage)
                .where(OutboxMessage.status == "PENDING")
                .order_by(OutboxMessage.created_at.asc())
                .limit(limit)
            )
            res = await session.execute(stmt)
            messages = list(res.scalars().all())

            for msg in messages:
                try:
                    payload = msg.payload
                    job_id = payload.get("job_id")
                    task_type = payload.get("task_type")
                    priority = payload.get("priority", 0)
                    run_at_str = payload.get("run_at")

                    if msg.topic == "jobs:scheduled" and run_at_str:
                        run_at = datetime.fromisoformat(run_at_str)
                        await self.broker.schedule(job_id, task_type, run_at, priority)
                    else:
                        await self.broker.enqueue(job_id, task_type, priority)
                        # Also advance job status in DB if it was PENDING_DISPATCH
                        await session.execute(
                            update(Job)
                            .where(Job.id == msg.job_id, Job.status == JobStatus.PENDING_DISPATCH)
                            .values(status=JobStatus.QUEUED, updated_at=utc_now())
                        )

                    msg.status = "DISPATCHED"
                    msg.dispatched_at = utc_now()
                    dispatched_count += 1
                except Exception as e:
                    msg.retry_count += 1
                    logger.error(f"Failed to dispatch outbox message {msg.id} ({e})")

            if dispatched_count > 0:
                await session.commit()
                logger.info(f"Outbox dispatcher processed {dispatched_count} messages")

        return dispatched_count

    async def run_loop(self, stop_event: asyncio.Event) -> None:
        """Continuous polling loop for outbox dispatcher service."""
        logger.info("Outbox dispatcher loop started.")
        while not stop_event.is_set():
            try:
                await self.sweep_and_dispatch()
            except Exception as e:
                logger.error(f"Unexpected error in outbox loop: {e}", exc_info=True)

            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self.settings.OUTBOX_POLL_INTERVAL
                )
            except asyncio.TimeoutError:
                pass
        logger.info("Outbox dispatcher loop stopped.")
