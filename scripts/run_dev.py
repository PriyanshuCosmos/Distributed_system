"""Run TaskFlow all-in-one local development server (API + Worker + Scheduler).

Runs with SQLite and in-memory FakeRedis if external PostgreSQL/Redis are unavailable,
enabling instant exploration on local machines without Docker.
"""

import asyncio
import os
import signal
import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Set standalone development environment defaults if not set
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("USE_FAKEREDIS", "true")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///./taskflow.db")
os.environ.setdefault("SYNC_DATABASE_URL", "sqlite:///./taskflow.db")

import uvicorn

from app.config import get_settings
from app.database import init_db
from app.logging_config import get_logger, setup_logging
from app.scheduler import Scheduler
from app.workers.worker import Worker

logger = get_logger("run_dev")


async def main() -> None:
    settings = get_settings()
    setup_logging(log_level=settings.LOG_LEVEL)

    print("=" * 70)
    print("TaskFlow All-In-One Local Development Server")
    print("=" * 70)
    print(f"API Dashboard:  http://localhost:{settings.API_PORT}")
    print(f"API Docs:       http://localhost:{settings.API_PORT}/docs")
    print(f"Database:       {settings.DATABASE_URL}")
    print(
        f"Redis Broker:   {'In-Memory FakeRedis' if settings.USE_FAKEREDIS else settings.REDIS_URL}"
    )
    print(f"Worker Threads: {settings.WORKER_CONCURRENCY}")
    print("-" * 70)
    print("Press Ctrl+C to shut down gracefully.\n")

    # Initialize DB Schema
    await init_db()

    # Create Worker and Scheduler instances
    worker = Worker(worker_id="dev-worker-1", concurrency=settings.WORKER_CONCURRENCY)
    scheduler = Scheduler(scheduler_id="dev-scheduler-1")

    # Uvicorn Server Config
    config = uvicorn.Config(
        app="app.main:app",
        host="0.0.0.0",
        port=settings.API_PORT,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config)

    # Setup termination signal handling
    stop_event = asyncio.Event()

    def handle_signal():
        print("\n[!] Shutdown initiated...")
        worker.stop()
        scheduler.stop()
        server.should_exit = True
        stop_event.set()

    if sys.platform != "win32":
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, handle_signal)
    else:
        signal.signal(signal.SIGINT, lambda s, f: handle_signal())
        signal.signal(signal.SIGTERM, lambda s, f: handle_signal())

    # Run API server, Worker, and Scheduler concurrently
    tasks = [
        asyncio.create_task(server.serve(), name="uvicorn-api"),
        asyncio.create_task(worker.start(), name="taskflow-worker"),
        asyncio.create_task(scheduler.start(), name="taskflow-scheduler"),
    ]

    try:
        await asyncio.gather(*tasks, return_exceptions=True)
    except (KeyboardInterrupt, asyncio.CancelledError):
        handle_signal()
    finally:
        print("[+] All TaskFlow processes terminated safely.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
