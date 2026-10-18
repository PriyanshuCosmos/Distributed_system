# TaskFlow Interview Guide: Distributed Systems Questions & Answers

This guide is designed to help you explain the technical decisions made in **TaskFlow** during engineering interviews. Every answer maps directly to real code implemented in this repository.

---

### Q1: How do you prevent dual-write inconsistencies between PostgreSQL and Redis when enqueuing jobs?
**Answer:**
A naive approach writes to the database and then sends a command to Redis. If the process crashes or network partitions between those two operations, the database says the job is queued, but Redis never received it.

In TaskFlow, we use the **Transactional Outbox Pattern** (`app/services/job_service.py` and `app/services/outbox_dispatcher.py`):
1. Inside a single database transaction, we persist both the `Job` record and an `OutboxMessage` record.
2. If the transaction commits, PostgreSQL guarantees durability.
3. We attempt an immediate best-effort push to Redis for sub-millisecond latency.
4. If that push fails, a background `OutboxDispatcher` periodically polls for `PENDING` outbox entries (`SELECT ... WHERE status = 'PENDING'`), enqueues them to Redis, and marks them `DISPATCHED`.

---

### Q2: What delivery semantics does TaskFlow guarantee, and how do you handle duplicate execution?
**Answer:**
TaskFlow guarantees **At-Least-Once Delivery**, not exactly-once delivery. In a distributed environment with network partitions, crashes, and timeouts, exactly-once delivery across independent systems is a theoretical impossibility (the Two Generals' Problem).

To handle at-least-once delivery safely, we enforce **idempotency**:
1. **API Submission Idempotency**: Clients can submit an `idempotency_key`. The `JobService.create_job()` method checks for an existing record with that key and returns the existing job instead of duplicating it.
2. **Task Handler Idempotency**: Handlers should be designed to be idempotent (e.g., using deterministic transaction IDs or UPSERT operations in downstream systems).

---

### Q3: How do you prevent two healthy workers from claiming and processing the same job simultaneously?
**Answer:**
We use a **two-phase atomic claim with database double-check**:
1. The worker pops a job pointer from the Redis ready queue (`ZPOPMAX`). Because Redis operations are atomic, only one worker receives that queue item.
2. The worker immediately executes a conditional SQL update (`app/workers/executor.py`):
   ```sql
   UPDATE jobs
   SET status = 'RUNNING',
       worker_id = :worker_id,
       lease_token = :lease_token,
       lease_expires_at = :expires_at,
       attempt_count = attempt_count + 1
   WHERE id = :job_id
     AND status IN ('QUEUED', 'RETRYING', 'PENDING_DISPATCH');
   ```
If another worker claimed the job or it was cancelled, `rows_affected == 0`, and the worker skips execution without error.

---

### Q4: How does TaskFlow handle a worker crashing while executing a job?
**Answer:**
When a worker claims a job, it is granted a 15-second distributed lease (`lease_expires_at`). While the task executes, a background coroutine renews this lease every 5 seconds.

If the worker node dies (power loss, OOM killer, segfault):
1. The heartbeat stops.
2. After 15 seconds, the lease expires.
3. The `Scheduler`'s `_reap_stale_leases()` background job finds all jobs where `status == 'RUNNING' AND lease_expires_at < NOW()`.
4. It logs a `JobAttempt` with status `TIMED_OUT`.
5. If `attempt_count < max_attempts`, it resets status to `QUEUED` and re-enqueues the job to Redis so another healthy worker can pick it up. If retries are exhausted, it routes the job to the dead-letter queue.

---

### Q5: What is a fencing token and why did you implement it?
**Answer:**
A major distributed systems problem is the **"zombie worker"**: a worker might experience an extended stop-the-world GC pause or CPU starvation. The reaper assumes the worker crashed, expires its lease, and re-assigns the job to a second worker. If the first worker eventually resumes, it might attempt to write its stale results to the database, overwriting newer work!

To prevent this, TaskFlow issues a cryptographically random **fencing token** (`lease_token`) on every claim (`app/workers/executor.py`). When a worker finalizes a job:
```sql
UPDATE jobs
SET status = 'SUCCEEDED', result = :result, lease_token = NULL
WHERE id = :job_id
  AND lease_token = :my_token
  AND status = 'RUNNING';
```
If the lease was reaped or reassigned to another worker with a newer token, `rows_affected == 0`. The zombie worker's write is rejected, preserving state integrity.

---

### Q6: How does priority scheduling work in Redis?
**Answer:**
We store ready jobs in a Redis Sorted Set (`ZSET`) scored with a combination of priority and arrival timestamp:
$$\text{Score} = (\text{Priority} \times 10^{12}) + (10^{10} - \text{Timestamp}_{\text{epoch}})$$
- Priority acts as the primary sort key (higher integer pops first).
- Within the same priority level, $(10^{10} - t)$ acts as a tie-breaker, guaranteeing strict First-In-First-Out (**FIFO**) ordering.
- Workers poll using `ZPOPMAX`, providing $O(\log N)$ extraction of the highest-priority, earliest-arrived job.

---

### Q7: Why did you add jitter to exponential backoff?
**Answer:**
When a downstream dependency suffers an outage (e.g., an external API returns 503), many jobs fail at roughly the same time. If all workers use pure exponential backoff ($2^n$), they will all retry simultaneously at $t = 2s, 4s, 8s$, causing synchronized retry storms (the **Thundering Herd problem**).

In `JobService.calculate_backoff()`, we multiply the exponential delay by a random uniform jitter factor ($\pm 20\%$):
$$\text{Delay}_{\text{jittered}} = \text{Delay} \times \text{Uniform}(0.8, 1.2)$$
This spreads retry attempts smoothly across time, allowing the downstream system to recover.

---

### Q8: How do you prevent multiple scheduler instances from dispatching the same scheduled jobs?
**Answer:**
In a multi-node cluster with multiple scheduler replicas, we use a **Redis distributed lock** (`taskflow:lock:scheduler`).
1. The scheduler attempts to acquire the lock using `SET key token NX PX 4000`.
2. Only the instance that acquires the lock runs the dispatch and reaper cycle.
3. The lock is released using a Lua script that verifies the token before deletion to ensure an instance never accidentally deletes a lock held by a successor.
4. Scheduled job migration is executed via an atomic Lua script (`LUA_MOVE_DUE_JOBS`) that reads from `taskflow:queue:scheduled` and writes to `taskflow:queue:ready` in a single atomic Redis step.
