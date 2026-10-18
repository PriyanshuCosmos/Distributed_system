"""Tests for worker execution, atomic claiming, and lease fencing tokens."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus, utc_now
from app.queue.redis_queue import RedisQueueBroker
from app.schemas import JobCreate
from app.services.job_service import JobService
from app.workers.executor import JobExecutor


@pytest.mark.asyncio
async def test_worker_successful_job_execution(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(
        task_type="simulate_report",
        payload={"rows": 100, "report_type": "sales"},
        priority=1,
    )
    job, _ = await service.create_job(job_in)

    executor = JobExecutor(worker_id="test-worker-1")
    executed = await executor.execute_job(str(job.id))
    assert executed is True

    # Verify DB state
    await db_session.refresh(job)
    assert job.status == JobStatus.SUCCEEDED
    assert job.result is not None
    assert job.result["rows_processed"] == 100
    assert job.finished_at is not None
    assert job.attempt_count == 1

    # Verify JobAttempt history
    attempts = await service.get_job_attempts(job.id)
    assert len(attempts) == 1
    assert attempts[0].status == "SUCCEEDED"
    assert attempts[0].worker_id == "test-worker-1"
    assert attempts[0].duration_ms is not None


@pytest.mark.asyncio
async def test_atomic_claim_prevents_dual_worker_execution(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    service = JobService(db_session)
    job_in = JobCreate(task_type="process_text", payload={"text": "Distributed queue test"})
    job, _ = await service.create_job(job_in)

    executor1 = JobExecutor(worker_id="worker-alpha")
    executor2 = JobExecutor(worker_id="worker-beta")

    # Worker 1 claims and executes
    claimed1 = await executor1.execute_job(str(job.id))
    assert claimed1 is True

    # Worker 2 attempts to claim the same job (which is now SUCCEEDED or already claimed)
    claimed2 = await executor2.execute_job(str(job.id))
    assert claimed2 is False  # Must be rejected by atomic UPDATE condition!


@pytest.mark.asyncio
async def test_fencing_token_rejects_zombie_worker_write(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    executor = JobExecutor(worker_id="zombie-worker")

    # Create job with lease_token="valid-token-A"
    job = Job(
        task_type="simulate_report",
        payload={"rows": 50},
        status=JobStatus.RUNNING,
        worker_id="reaper-assigned-worker",
        lease_token="newer-lease-token-B",  # Newer attempt holds token B
        attempt_count=2,
        created_at=utc_now(),
        updated_at=utc_now(),
    )
    db_session.add(job)
    await db_session.commit()
    await db_session.refresh(job)

    # Zombie worker attempts to finalize job with stale token "stale-token-A"
    await executor._finalize_job(
        job_id=job.id,
        lease_token="stale-token-A",
        task_type="simulate_report",
        attempt_count=1,
        max_attempts=3,
        start_dt=utc_now(),
        finish_dt=utc_now(),
        duration_ms=150.0,
        execution_status="SUCCEEDED",
        result_data={"rows_processed": 50},
        error_message=None,
    )

    # Verify that the job was NOT overwritten to SUCCEEDED and retained its newer token
    await db_session.refresh(job)
    assert job.status == JobStatus.RUNNING
    assert job.lease_token == "newer-lease-token-B"
