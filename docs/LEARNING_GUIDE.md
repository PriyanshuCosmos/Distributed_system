# TaskFlow Learning Guide & Codebase Walkthrough

Welcome to **TaskFlow**! This guide is written for students, junior engineers, and interviewers who want to understand how a modern distributed task queue is structured in Python.

---

## 1. Codebase Structure Overview

The project is structured with strict separation of concerns:

```
Distributed systems/
├── app/
│   ├── main.py              # FastAPI application, CORS, lifespan, and dashboard routes
│   ├── config.py            # Pydantic Settings (env vars, timeouts, URLs)
│   ├── logging_config.py    # Structured console and JSON logging
│   ├── database.py          # Async SQLAlchemy engine and session factory
│   ├── models.py            # ORM models (Job, JobAttempt, OutboxMessage)
│   ├── schemas.py           # Pydantic request/response validation schemas
│   ├── task_registry.py     # Registry of allowed built-in task handlers
│   ├── queue/
│   │   ├── __init__.py
│   │   └── redis_queue.py   # Redis Priority ZSET, Scheduled ZSET, Lua scripts, and distributed locks
│   ├── services/
│   │   ├── __init__.py
│   │   ├── job_service.py   # Job business logic (creation, outbox, cancel, retry)
│   │   └── outbox_dispatcher.py # Reliable background outbox sweeper
│   ├── workers/
│   │   ├── __init__.py
│   │   ├── executor.py      # Single job execution, atomic claim, heartbeat leasing & fencing
│   │   └── worker.py        # Concurrent worker daemon process & graceful shutdown
│   ├── scheduler.py         # Scheduler process, scheduled job dispatch & stale lease reaper
│   └── routes/
│       ├── __init__.py
│       ├── jobs.py          # /api/jobs endpoints
│       ├── stats.py         # /api/stats endpoint
│       └── health.py        # /api/health and /api/ready probes
├── static/
│   ├── index.html           # Frontend dashboard semantic HTML
│   ├── styles.css           # Modern dark SaaS design system
│   └── app.js               # Frontend JavaScript calling real REST APIs
├── alembic/                 # Database migrations
├── scripts/
│   ├── run_dev.py           # All-in-one local runner (API + Worker + Scheduler)
│   ├── seed_jobs.py         # Sample job generator
│   └── benchmark.py         # Performance benchmark client measuring real latency percentiles
├── tests/                   # Full pytest suite (34 unit & integration tests)
├── Dockerfile               # Multi-stage production container
└── docker-compose.yml       # Production-ready Compose stack
```

---

## 2. Module by Module Breakdown

### `app/models.py`
Defines the database schema using **SQLAlchemy 2.0**:
- `Job`: Represents a unit of work. Key fields include `id` (UUID), `task_type`, `payload` (JSON), `status` (Enum), `priority` (Integer), `attempt_count`, `max_attempts`, `result`, and lease metadata (`worker_id`, `lease_token`, `lease_expires_at`).
- `JobAttempt`: Historical audit trail of every execution attempt. Records worker ID, duration in milliseconds, status (`SUCCEEDED`, `FAILED`, `TIMED_OUT`), and error messages.
- `OutboxMessage`: Part of the Transactional Outbox pattern. Guarantees that job creation in the DB and message publication to Redis happen reliably.
- `VALID_TRANSITIONS`: A dictionary enforcing the job state machine (e.g. `QUEUED -> RUNNING -> SUCCEEDED`).

### `app/task_registry.py`
Security boundary that prevents arbitrary code execution:
- Instead of using `eval()` or unpickling untrusted user code, only handlers registered with `registry.register(task_type, handler)` can be executed.
- Built-in handlers:
  - `simulate_report`: Heavy data aggregation simulation.
  - `process_text`: Word count, character count, and sentiment analysis.
  - `simulated_failure`: Configurable test handler that fails until a given attempt number.
  - `sleep_task`: Simulates non-blocking I/O to test leases and timeouts.
  - `send_notification`: Simulates idempotent external webhook or email delivery.

### `app/queue/redis_queue.py`
The Redis coordination layer:
- `calculate_priority_score(priority, timestamp)`: Computes the 64-bit float score for the Redis Sorted Set `taskflow:queue:ready`.
- `enqueue(job_id, task_type, priority)`: Adds a job to the ready ZSET.
- `schedule(job_id, task_type, run_at, priority)`: Adds a future job to `taskflow:queue:scheduled`.
- `pop_ready_job()`: Atomically pops the highest priority job using `ZPOPMAX`.
- `acquire_lock(lock_key, token, expire_ms)`: Implements Redis distributed locking using `SET NX PX`.
- `release_lock()`: Safe lock release using Lua script that checks ownership token before deleting.

### `app/services/job_service.py`
Business logic layer:
- `create_job()`: Handles idempotency key checking, saves the job and outbox message in a single database transaction, and attempts immediate Redis publication.
- `cancel_job()`: Validates that the job is not running or already terminal, sets status to `CANCELLED`, and removes it from Redis queues.
- `calculate_backoff()`: Computes exponential delay with uniform random jitter.

### `app/workers/executor.py` & `app/workers/worker.py`
Worker execution engine:
- `Worker`: Runs an async worker loop bounded by `asyncio.Semaphore(concurrency)`.
- `JobExecutor.execute_job()`:
  1. Executes atomic `UPDATE` with `WHERE status IN ('QUEUED', 'RETRYING')`.
  2. Generates a unique `lease_token`.
  3. Launches a background heartbeat task to keep the lease alive.
  4. Calls the task handler.
  5. On completion, validates the fencing token before updating status to `SUCCEEDED` or scheduling a retry.

### `app/scheduler.py`
Periodic background coordination service:
- `_sync_due_jobs_in_db()`: Advances scheduled jobs whose `run_at` has passed.
- `_reap_stale_leases()`: Finds jobs in `RUNNING` status whose worker lease has expired, recovers them back to `QUEUED`, or routes them to dead-letter queue if retries are exhausted.

---

## 3. How to Run the Tests

The test suite uses `pytest` with `pytest-asyncio` and `fakeredis`:

```bash
# Activate virtual environment
.\.venv\Scripts\Activate.ps1  # Windows PowerShell
source .venv/bin/activate     # macOS / Linux

# Run all 34 tests
pytest -v
```

All tests execute against an isolated in-memory SQLite database and isolated FakeRedis broker, making them 100% self-contained and reproducible.
