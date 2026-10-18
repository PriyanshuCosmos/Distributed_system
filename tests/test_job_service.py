"""Tests for JobService business logic and state invariants."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus, utc_now
from app.queue.redis_queue import RedisQueueBroker
from app.schemas import JobCreate
from app.services.job_service import (
    InvalidJobStateError,
    JobService,
)


@pytest.mark.asyncio
async def test_create_job_persistence(
    db_session: AsyncSession, test_broker: RedisQueueBroker
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(
        task_type="simulate_report",
        payload={"rows": 50},
        priority=2,
    )
    job, is_new = await service.create_job(job_in)
    assert is_new is True
    assert job.id is not None
    assert job.task_type == "simulate_report"
    assert job.status in [JobStatus.QUEUED, JobStatus.PENDING_DISPATCH]

    # Verify retrieval
    retrieved = await service.get_job(job.id)
    assert retrieved is not None
    assert retrieved.id == job.id


@pytest.mark.asyncio
async def test_idempotency_key_deduplication(
    db_session: AsyncSession, test_broker: RedisQueueBroker
) -> None:
    service = JobService(db_session)
    key = "idem_unique_txn_999"

    job_in1 = JobCreate(
        task_type="process_text",
        payload={"text": "First submission"},
        idempotency_key=key,
    )
    job1, is_new1 = await service.create_job(job_in1)
    assert is_new1 is True

    # Second submission with the same idempotency key
    job_in2 = JobCreate(
        task_type="process_text",
        payload={"text": "Second submission with duplicate key"},
        idempotency_key=key,
    )
    job2, is_new2 = await service.create_job(job_in2)
    assert is_new2 is False
    assert job1.id == job2.id  # Must return identical job without duplicating


@pytest.mark.asyncio
async def test_job_cancellation_rules(
    db_session: AsyncSession, test_broker: RedisQueueBroker
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(task_type="sleep_task", payload={"seconds": 5})
    job, _ = await service.create_job(job_in)

    # Cancel while queued
    cancelled = await service.cancel_job(job.id)
    assert cancelled.status == JobStatus.CANCELLED
    assert cancelled.finished_at is not None

    # Attempting to cancel already cancelled job raises InvalidJobStateError
    with pytest.raises(InvalidJobStateError):
        await service.cancel_job(job.id)


@pytest.mark.asyncio
async def test_running_job_cancellation_conflict(db_session: AsyncSession) -> None:
    service = JobService(db_session)
    # Create job directly in RUNNING state
    running_job = Job(
        task_type="sleep_task",
        payload={"seconds": 10},
        status=JobStatus.RUNNING,
        worker_id="test-worker-1",
        created_at=utc_now(),
        updated_at=utc_now(),
    )
    db_session.add(running_job)
    await db_session.commit()
    await db_session.refresh(running_job)

    # Cancelling a RUNNING job must raise conflict error
    with pytest.raises(InvalidJobStateError) as exc:
        await service.cancel_job(running_job.id)
    assert "currently running on worker" in str(exc.value)


def test_exponential_backoff_calculation() -> None:
    # Attempt 1: base * 2^(0) = 2.0 (+/- jitter)
    b1 = JobService.calculate_backoff(1, base_seconds=2.0, jitter=False)
    assert b1 == 2.0

    # Attempt 2: base * 2^(1) = 4.0
    b2 = JobService.calculate_backoff(2, base_seconds=2.0, jitter=False)
    assert b2 == 4.0

    # Attempt 3: base * 2^(2) = 8.0
    b3 = JobService.calculate_backoff(3, base_seconds=2.0, jitter=False)
    assert b3 == 8.0

    # Max ceiling check
    b_capped = JobService.calculate_backoff(10, base_seconds=2.0, max_seconds=30.0, jitter=False)
    assert b_capped == 30.0
