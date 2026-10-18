"""Tests for Transactional Outbox consistency and sweep dispatcher."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus, OutboxMessage, utc_now
from app.queue.redis_queue import RedisQueueBroker
from app.services.outbox_dispatcher import OutboxDispatcher


@pytest.mark.asyncio
async def test_outbox_sweep_and_dispatch(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    now = utc_now()

    # 1. Simulate job committed to DB with PENDING outbox message (as if Redis was momentarily down)
    job = Job(
        task_type="simulate_report",
        payload={"rows": 75},
        priority=3,
        status=JobStatus.PENDING_DISPATCH,
        created_at=now,
        updated_at=now,
    )
    db_session.add(job)
    await db_session.flush()

    outbox_msg = OutboxMessage(
        job_id=job.id,
        topic="jobs:ready",
        payload={"job_id": str(job.id), "task_type": job.task_type, "priority": job.priority},
        status="PENDING",
        created_at=now,
    )
    db_session.add(outbox_msg)
    await db_session.commit()

    # Outbox dispatcher sweeps
    dispatcher = OutboxDispatcher()
    dispatched_count = await dispatcher.sweep_and_dispatch()
    assert dispatched_count == 1

    # Verify outbox message is marked DISPATCHED
    await db_session.refresh(outbox_msg)
    assert outbox_msg.status == "DISPATCHED"
    assert outbox_msg.dispatched_at is not None

    # Verify job status in DB advanced to QUEUED
    await db_session.refresh(job)
    assert job.status == JobStatus.QUEUED

    # Verify job arrived in Redis ready queue
    popped = await test_broker.pop_ready_job(timeout=1.0)
    assert popped is not None
    assert popped["job_id"] == str(job.id)
