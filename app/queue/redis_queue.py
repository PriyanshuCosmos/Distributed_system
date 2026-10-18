"""Redis broker implementation for Priority Queue, Scheduled Jobs, and Coordination."""

import asyncio
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

import redis.asyncio as aioredis

from app.config import get_settings
from app.logging_config import get_logger

logger = get_logger("queue.redis")

# Redis Key Namespaces
KEY_QUEUE_READY = "taskflow:queue:ready"  # ZSET: score = priority + arrival tiebreaker
KEY_QUEUE_SCHEDULED = "taskflow:queue:scheduled"  # ZSET: score = run_at epoch seconds
KEY_QUEUE_DEAD_LETTER = "taskflow:queue:dead_letter"  # LIST: dead-lettered job IDs
KEY_ACTIVE_WORKERS = "taskflow:workers"  # SET: registered worker IDs
PREFIX_WORKER_HEARTBEAT = "taskflow:worker:"  # HASH: taskflow:worker:<id>:heartbeat
KEY_SCHEDULER_LOCK = "taskflow:lock:scheduler"  # STRING: distributed lock for scheduler loop

# Lua Script: Atomic move of due scheduled jobs to ready queue
LUA_MOVE_DUE_JOBS = """
local scheduled_key = KEYS[1]
local ready_key = KEYS[2]
local now_epoch = tonumber(ARGV[1])
local max_count = tonumber(ARGV[2])

local due_items = redis.call('ZRANGEBYSCORE', scheduled_key, '-inf', now_epoch, 'LIMIT', 0, max_count)
local moved_count = 0

for _, item_str in ipairs(due_items) do
    local decoded = cjson.decode(item_str)
    local priority = tonumber(decoded.priority) or 0
    local score = (priority * 1000000000000) + (10000000000 - now_epoch)
    redis.call('ZREM', scheduled_key, item_str)
    redis.call('ZADD', ready_key, score, item_str)
    moved_count = moved_count + 1
end

return moved_count
"""

# Lua Script: Safe Distributed Lock Release (only releases if token matches)
LUA_RELEASE_LOCK = """
if redis.call("get", KEYS[1]) == ARGV[1] then
    return redis.call("del", KEYS[1])
else
    return 0
end
"""


class RedisQueueBroker:
    """Manages enqueueing, dequeuing, priority ordering, scheduling, and distributed locks."""

    def __init__(self, client: Optional[aioredis.Redis] = None) -> None:
        self._client = client
        self._is_fake = False
        self._move_due_script = None
        self._release_lock_script = None

    async def get_client(self) -> aioredis.Redis:
        """Initialize or return the connected Redis client."""
        if self._client is None:
            settings = get_settings()
            if settings.USE_FAKEREDIS:
                import fakeredis.aioredis

                self._client = fakeredis.aioredis.FakeRedis(decode_responses=True)
                self._is_fake = True
                logger.info("Using in-memory FakeRedis broker (development/test mode)")
            else:
                try:
                    self._client = aioredis.from_url(
                        settings.REDIS_URL,
                        decode_responses=True,
                        socket_connect_timeout=2.0,
                    )
                    await self._client.ping()
                    logger.info(f"Connected to Redis at {settings.REDIS_URL}")
                except Exception as e:
                    logger.warning(
                        f"Failed to connect to real Redis at {settings.REDIS_URL} ({e}). "
                        "Falling back to in-memory FakeRedis broker."
                    )
                    import fakeredis.aioredis

                    self._client = fakeredis.aioredis.FakeRedis(decode_responses=True)
                    self._is_fake = True

        return self._client

    @staticmethod
    def calculate_priority_score(priority: int, timestamp: Optional[float] = None) -> float:
        """
        Calculate ZSET score for priority ordering.
        Higher priority value = higher score (pops first).
        Earlier arrival timestamp = higher tiebreak score within same priority (FIFO).
        """
        t = timestamp if timestamp is not None else time.time()
        # Scale priority by 1e12. Within same priority, tiebreaker is (1e10 - t), so earlier t gives higher score.
        return (priority * 1_000_000_000_000.0) + (10_000_000_000.0 - t)

    async def enqueue(self, job_id: UUID | str, task_type: str, priority: int = 0) -> None:
        """Push a job into the ready priority queue."""
        client = await self.get_client()
        job_data = json.dumps({"job_id": str(job_id), "task_type": task_type, "priority": priority})
        score = self.calculate_priority_score(priority)
        await client.zadd(KEY_QUEUE_READY, {job_data: score})
        logger.debug(
            f"Job {job_id} enqueued to {KEY_QUEUE_READY} (priority={priority}, score={score})"
        )

    async def schedule(
        self, job_id: UUID | str, task_type: str, run_at: datetime, priority: int = 0
    ) -> None:
        """Push a job into the scheduled queue ordered by run_at epoch seconds."""
        client = await self.get_client()
        job_data = json.dumps({"job_id": str(job_id), "task_type": task_type, "priority": priority})
        score = run_at.timestamp()
        await client.zadd(KEY_QUEUE_SCHEDULED, {job_data: score})
        logger.debug(f"Job {job_id} scheduled for {run_at.isoformat()} (epoch={score})")

    async def pop_ready_job(self, timeout: float = 2.0) -> Optional[Dict[str, Any]]:
        """
        Pop the highest priority job from the ready queue.
        Uses ZPOPMAX for strict priority ordering.
        If empty, polls with small sleep up to timeout seconds.
        """
        client = await self.get_client()
        start = time.time()
        while True:
            # ZPOPMAX returns list of (member, score) tuples
            popped = await client.zpopmax(KEY_QUEUE_READY, count=1)
            if popped:
                member_str, _ = popped[0]
                return json.loads(member_str)

            if time.time() - start >= timeout:
                return None

            await asyncio.sleep(0.1)

    async def dispatch_due_scheduled_jobs(self, limit: int = 100) -> int:
        """
        Atomically inspect scheduled queue and move all jobs with score <= now to ready queue.
        """
        client = await self.get_client()
        now_epoch = time.time()

        try:
            # Register or execute Lua script for atomic migration
            res = await client.eval(
                LUA_MOVE_DUE_JOBS, 2, KEY_QUEUE_SCHEDULED, KEY_QUEUE_READY, now_epoch, limit
            )
            moved_count = int(res) if res is not None else 0
            if moved_count > 0:
                logger.info(f"Dispatched {moved_count} due scheduled jobs to ready queue")
            return moved_count
        except Exception as e:
            # Fallback in case Lua is disabled or fakeredis mock behavior differs
            logger.debug(f"Lua dispatch fallback: {e}")
            items = await client.zrangebyscore(
                KEY_QUEUE_SCHEDULED, min="-inf", max=now_epoch, start=0, num=limit
            )
            moved = 0
            for item_str in items:
                removed = await client.zrem(KEY_QUEUE_SCHEDULED, item_str)
                if removed:
                    data = json.loads(item_str)
                    score = self.calculate_priority_score(data.get("priority", 0))
                    await client.zadd(KEY_QUEUE_READY, {item_str: score})
                    moved += 1
            return moved

    async def remove_from_queues(self, job_id: UUID | str) -> bool:
        """Remove a job from both ready and scheduled queues (used during cancellation)."""
        client = await self.get_client()
        str_id = str(job_id)
        removed_any = False

        # Scan and remove matching job_id from ready ZSET
        for q_key in (KEY_QUEUE_READY, KEY_QUEUE_SCHEDULED):
            items = await client.zrange(q_key, 0, -1)
            for item_str in items:
                try:
                    data = json.loads(item_str)
                    if data.get("job_id") == str_id:
                        if await client.zrem(q_key, item_str):
                            removed_any = True
                except Exception:
                    continue
        return removed_any

    async def move_to_dead_letter(self, job_id: UUID | str, reason: str) -> None:
        """Push exhausted failed job ID into the dead-letter queue."""
        client = await self.get_client()
        payload = json.dumps({"job_id": str(job_id), "reason": reason, "timestamp": time.time()})
        await client.rpush(KEY_QUEUE_DEAD_LETTER, payload)
        logger.warning(f"Job {job_id} routed to dead-letter queue: {reason}")

    # ==========================================================================
    # Worker Heartbeat and Lease Tracking
    # ==========================================================================

    async def register_worker(self, worker_id: str, concurrency: int = 4) -> None:
        """Register worker in the active workers set."""
        client = await self.get_client()
        await client.sadd(KEY_ACTIVE_WORKERS, worker_id)
        await self.send_heartbeat(worker_id, active_jobs=0, concurrency=concurrency)

    async def send_heartbeat(
        self, worker_id: str, active_jobs: int = 0, concurrency: int = 4, ttl: int = 15
    ) -> None:
        """Refresh worker heartbeat key with TTL."""
        client = await self.get_client()
        key = f"{PREFIX_WORKER_HEARTBEAT}{worker_id}:heartbeat"
        data = {
            "worker_id": worker_id,
            "last_heartbeat": datetime.now(timezone.utc).isoformat(),
            "active_jobs": active_jobs,
            "concurrency": concurrency,
        }
        await client.set(key, json.dumps(data), ex=ttl)

    async def deregister_worker(self, worker_id: str) -> None:
        """Remove worker on graceful shutdown."""
        client = await self.get_client()
        await client.srem(KEY_ACTIVE_WORKERS, worker_id)
        await client.delete(f"{PREFIX_WORKER_HEARTBEAT}{worker_id}:heartbeat")

    async def get_active_workers(self) -> List[Dict[str, Any]]:
        """Return list of workers whose heartbeats are still active."""
        client = await self.get_client()
        worker_ids = await client.smembers(KEY_ACTIVE_WORKERS)
        active = []
        dead_ids = []

        for w_id in worker_ids:
            key = f"{PREFIX_WORKER_HEARTBEAT}{w_id}:heartbeat"
            raw = await client.get(key)
            if raw:
                try:
                    active.append(json.loads(raw))
                except Exception:
                    active.append({"worker_id": w_id, "last_heartbeat": "unknown"})
            else:
                dead_ids.append(w_id)

        # Clean up expired workers from set
        if dead_ids:
            await client.srem(KEY_ACTIVE_WORKERS, *dead_ids)

        return active

    # ==========================================================================
    # Distributed Lock (for Scheduler Leader / Singleton)
    # ==========================================================================

    async def acquire_lock(self, lock_key: str, token: str, expire_ms: int = 5000) -> bool:
        """Acquire a distributed lock using SET NX PX."""
        client = await self.get_client()
        res = await client.set(lock_key, token, px=expire_ms, nx=True)
        return bool(res)

    async def release_lock(self, lock_key: str, token: str) -> bool:
        """Safely release distributed lock only if token matches."""
        client = await self.get_client()
        try:
            res = await client.eval(LUA_RELEASE_LOCK, 1, lock_key, token)
            return bool(res)
        except Exception:
            curr = await client.get(lock_key)
            if curr == token:
                await client.delete(lock_key)
                return True
            return False

    # ==========================================================================
    # Queue Depth and Stats
    # ==========================================================================

    async def get_queue_depth(self) -> Dict[str, int]:
        """Return counts of ready, scheduled, and dead-letter queues."""
        client = await self.get_client()
        ready = await client.zcard(KEY_QUEUE_READY)
        scheduled = await client.zcard(KEY_QUEUE_SCHEDULED)
        dead = await client.llen(KEY_QUEUE_DEAD_LETTER)
        return {"ready": ready, "scheduled": scheduled, "dead_letter": dead}

    async def close(self) -> None:
        """Close connection to Redis."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None


# Global queue broker singleton
_broker: Optional[RedisQueueBroker] = None


def get_broker() -> RedisQueueBroker:
    """Return the global queue broker instance."""
    global _broker
    if _broker is None:
        _broker = RedisQueueBroker()
    return _broker
