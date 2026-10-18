"""Tests for scheduler dispatching and stale lease crash recovery."""

from datetime import timedelta

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Job, JobStatus, utc_now
from app.queue.redis_queue import RedisQueueBroker
from app.scheduler import Scheduler


@pytest.mark.asyncio
async def test_scheduler_syncs_due_jobs_from_db(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    scheduler = Scheduler()
    now = utc_now()

    # Create scheduled job due 5 seconds ago
    due_job = Job(
        task_type="simulate_report",
        payload={"rows": 20},
        status=JobStatus.SCHEDULED,
        run_at=now - timedelta(seconds=5),
        created_at=now,
        updated_at=now,
    )
    # Create scheduled job due in 1 hour (not due)
    future_job = Job(
        task_type="simulate_report",
        payload={"rows": 20},
        status=JobStatus.SCHEDULED,
        run_at=now + timedelta(hours=1),
        created_at=now,
        updated_at=now,
    )
    db_session.add_all([due_job, future_job])
    await db_session.commit()

    # Run scheduler sync
    synced = await scheduler._sync_due_jobs_in_db()
    assert synced == 1

    await db_session.refresh(due_job)
    await db_session.refresh(future_job)

    assert due_job.status == JobStatus.QUEUED
    assert future_job.status == JobStatus.SCHEDULED


@pytest.mark.asyncio
async def test_stale_lease_reaper_recovers_crashed_worker_job(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    scheduler = Scheduler()
    now = utc_now()

    # Create job abandoned by a crashed worker
    crashed_job = Job(
        task_type="sleep_task",
        payload={"seconds": 10},
        status=JobStatus.RUNNING,
        worker_id="crashed-worker-99",
        lease_token="lease-token-dead",
        lease_expires_at=now - timedelta(seconds=20),  # Expired lease
        attempt_count=1,
        max_attempts=3,
        started_at=now - timedelta(seconds=35),
        created_at=now - timedelta(seconds=40),
        updated_at=now - timedelta(seconds=35),
    )
    db_session.add(crashed_job)
    await db_session.commit()

    # Reaper runs
    reaped_count = await scheduler._reap_stale_leases()
    assert reaped_count == 1

    await db_session.refresh(crashed_job)
    # Recovered to QUEUED with lease cleared
    assert crashed_job.status == JobStatus.QUEUED
    assert crashed_job.worker_id is None
    assert crashed_job.lease_token is None
    assert crashed_job.lease_expires_at is None

    # Verify attempt history recorded TIMED_OUT
    from app.services.job_service import JobService

    service = JobService(db_session)
    attempts = await service.get_job_attempts(crashed_job.id)
    assert len(attempts) == 1
    assert attempts[0].status == "TIMED_OUT"
    assert "crashed-worker-99" in (attempts[0].error_message or "")


@pytest.mark.asyncio
async def test_stale_lease_reaper_exhausted_moves_to_failed(
    db_session: AsyncSession,
    test_broker: RedisQueueBroker,
) -> None:
    scheduler = Scheduler()
    now = utc_now()

    # Job on its final attempt when worker crashed
    exhausted_job = Job(
        task_type="sleep_task",
        payload={"seconds": 10},
        status=JobStatus.RUNNING,
        worker_id="fatal-worker-1",
        lease_token="lease-token-fatal",
        lease_expires_at=now - timedelta(seconds=15),
        attempt_count=3,
        max_attempts=3,
        started_at=now - timedelta(seconds=30),
        created_at=now - timedelta(seconds=35),
        updated_at=now - timedelta(seconds=30),
    )
    db_session.add(exhausted_job)
    await db_session.commit()

    await scheduler._reap_stale_leases()
    await db_session.refresh(exhausted_job)

    # Must be marked FAILED and routed to dead letter
    assert exhausted_job.status == JobStatus.FAILED
    assert exhausted_job.finished_at is not None

    depth = await test_broker.get_queue_depth()
    assert depth["dead_letter"] == 1
