"""Tests for retries, backoff delays, and dead-letter queue routing."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus, utc_now
from app.queue.redis_queue import RedisQueueBroker
from app.schemas import JobCreate
from app.services.job_service import JobService
from app.workers.executor import JobExecutor


@pytest.mark.asyncio
async def test_retry_on_transient_failure(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(
        task_type="simulated_failure",
        payload={"fail_until_attempt": 2, "error_message": "Transient network 503"},
        max_attempts=3,
    )
    job, _ = await service.create_job(job_in)

    executor = JobExecutor(worker_id="worker-test")

    # Attempt 1: Fails
    await executor.execute_job(str(job.id))
    await db_session.refresh(job)

    assert job.status == JobStatus.RETRYING
    assert job.attempt_count == 1
    assert job.run_at is not None  # Future backoff timestamp set
    assert "Transient network 503" in (job.last_error or "")

    # Verify attempt recorded
    attempts = await service.get_job_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].status == "FAILED"

    # Reset status to QUEUED as if scheduler dispatched the retry
    job.status = JobStatus.QUEUED
    await db_session.commit()

    # Attempt 2: Recovers and succeeds
    await executor.execute_job(str(job.id))
    await db_session.refresh(job)

    assert job.status == JobStatus.SUCCEEDED
    assert job.attempt_count == 2
    assert job.result is not None
    assert job.result["status"] == "recovered"

    attempts2 = await service.get_job_attempts(job.id)
    assert len(attempts2) == 2
    assert attempts2[1].status == "SUCCEEDED"


@pytest.mark.asyncio
async def test_exhausted_retries_routes_to_dead_letter(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(
        task_type="simulated_failure",
        payload={"always_fail": True, "error_message": "Permanent defect"},
        max_attempts=2,
    )
    job, _ = await service.create_job(job_in)
    executor = JobExecutor(worker_id="worker-test")

    # Attempt 1: Fails -> RETRYING
    await executor.execute_job(str(job.id))
    await db_session.refresh(job)
    assert job.status == JobStatus.RETRYING

    job.status = JobStatus.QUEUED
    await db_session.commit()

    # Attempt 2 (Max attempt): Fails -> FAILED / Dead-Letter
    await executor.execute_job(str(job.id))
    await db_session.refresh(job)

    assert job.status == JobStatus.FAILED
    assert job.finished_at is not None
    assert job.attempt_count == 2

    # Check dead-letter queue in broker
    depth = await test_broker.get_queue_depth()
    assert depth["dead_letter"] == 1


@pytest.mark.asyncio
async def test_manual_retry_of_failed_job(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    service = JobService(db_session)
    # Create dead-lettered job
    job = Job(
        task_type="simulate_report",
        payload={"rows": 50},
        status=JobStatus.FAILED,
        attempt_count=3,
        max_attempts=3,
        last_error="Fatal timeout",
        created_at=utc_now(),
        updated_at=utc_now(),
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Manually retry with additional attempt
    retried_job = await service.retry_failed_job(job.id, reset_attempts=False)
    assert retried_job.status in [JobStatus.QUEUED, JobStatus.PENDING_DISPATCH]
    assert retried_job.max_attempts == 4  # Granted extra attempt
    assert retried_job.last_error is None
