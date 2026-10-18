"""Tests for Redis broker priority queue, scheduling, and distributed locks."""

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from app.queue.redis_queue import RedisQueueBroker


@pytest.mark.asyncio
async def test_priority_queue_ordering(test_broker: RedisQueueBroker) -> None:
    # Enqueue Normal priority job (0), Low priority job (-5), High priority job (10)
    await test_broker.enqueue("job-normal", "simulate_report", priority=0)
    await test_broker.enqueue("job-low", "simulate_report", priority=-5)
    await test_broker.enqueue("job-high", "simulate_report", priority=10)

    # High priority (+10) must pop first
    first = await test_broker.pop_ready_job(timeout=1.0)
    assert first is not None
    assert first["job_id"] == "job-high"

    # Normal priority (0) must pop second
    second = await test_broker.pop_ready_job(timeout=1.0)
    assert second is not None
    assert second["job_id"] == "job-normal"

    # Low priority (-5) must pop third
    third = await test_broker.pop_ready_job(timeout=1.0)
    assert third is not None
    assert third["job_id"] == "job-low"


@pytest.mark.asyncio
async def test_fifo_tiebreaking_same_priority(test_broker: RedisQueueBroker) -> None:
    # Jobs with identical priority should pop in FIFO order of submission
    await test_broker.enqueue("job-first", "process_text", priority=5)
    await asyncio.sleep(0.01)  # Ensure distinct arrival timestamp
    await test_broker.enqueue("job-second", "process_text", priority=5)

    item1 = await test_broker.pop_ready_job(timeout=1.0)
    item2 = await test_broker.pop_ready_job(timeout=1.0)

    assert item1["job_id"] == "job-first"
    assert item2["job_id"] == "job-second"


@pytest.mark.asyncio
async def test_scheduled_queue_dispatch(test_broker: RedisQueueBroker) -> None:
    # 1. Schedule a job in the past (already due)
    past_time = datetime.now(timezone.utc) - timedelta(seconds=10)
    await test_broker.schedule("job-due", "simulate_report", run_at=past_time, priority=2)

    # 2. Schedule a job far in the future (not due)
    future_time = datetime.now(timezone.utc) + timedelta(hours=1)
    await test_broker.schedule("job-future", "simulate_report", run_at=future_time, priority=2)

    # Ready queue is initially empty
    ready_pop = await test_broker.pop_ready_job(timeout=0.2)
    assert ready_pop is None

    # Run dispatch cycle
    moved = await test_broker.dispatch_due_scheduled_jobs()
    assert moved == 1

    # Ready queue should now yield the due job
    popped = await test_broker.pop_ready_job(timeout=1.0)
    assert popped is not None
    assert popped["job_id"] == "job-due"

    # Future job remains in scheduled queue
    depth = await test_broker.get_queue_depth()
    assert depth["scheduled"] == 1
    assert depth["ready"] == 0


@pytest.mark.asyncio
async def test_distributed_lock_acquire_and_release(test_broker: RedisQueueBroker) -> None:
    lock_key = "test:lock:leader"
    token_a = "token-instance-A"
    token_b = "token-instance-B"

    # Instance A acquires lock
    acquired = await test_broker.acquire_lock(lock_key, token_a, expire_ms=3000)
    assert acquired is True

    # Instance B cannot acquire the same lock while held
    acquired_b = await test_broker.acquire_lock(lock_key, token_b, expire_ms=3000)
    assert acquired_b is False

    # Instance B cannot release A's lock
    released_b = await test_broker.release_lock(lock_key, token_b)
    assert released_b is False

    # Instance A successfully releases its lock
    released_a = await test_broker.release_lock(lock_key, token_a)
    assert released_a is True

    # Now instance B can acquire it
    acquired_b2 = await test_broker.acquire_lock(lock_key, token_b, expire_ms=3000)
    assert acquired_b2 is True
