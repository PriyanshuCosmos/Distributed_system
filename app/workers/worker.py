"""Worker process loop managing task concurrency and graceful shutdown."""

import asyncio
import os
import signal
import socket
import sys
import uuid
from typing import Optional, Set

from app.config import get_settings
from app.logging_config import get_logger, setup_logging
from app.queue.redis_queue import get_broker
from app.workers.executor import JobExecutor

logger = get_logger("worker.process")


class Worker:
    """Worker daemon consuming jobs from Redis queue and executing them concurrently."""

    def __init__(self, worker_id: Optional[str] = None, concurrency: Optional[int] = None) -> None:
        self.settings = get_settings()
        self.worker_id = worker_id or f"worker-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.concurrency = concurrency or self.settings.WORKER_CONCURRENCY
        self.broker = get_broker()
        self.executor = JobExecutor(self.worker_id)
        self.stop_event = asyncio.Event()
        self.active_tasks: Set[asyncio.Task] = set()
        self.semaphore = asyncio.Semaphore(self.concurrency)

    async def start(self) -> None:
        """Start the worker processing loop."""
        logger.info(
            f"Starting TaskFlow Worker '{self.worker_id}' with concurrency={self.concurrency}"
        )

        # Register worker in Redis
        await self.broker.register_worker(self.worker_id, concurrency=self.concurrency)

        # Start periodic worker heartbeat
        heartbeat_task = asyncio.create_task(self._worker_heartbeat_loop())

        try:
            while not self.stop_event.is_set():
                # Acquire concurrency slot before polling queue to prevent queue starvation
                await self.semaphore.acquire()

                if self.stop_event.is_set():
                    self.semaphore.release()
                    break

                # Pop highest priority job from Redis ready queue
                try:
                    job_data = await self.broker.pop_ready_job(
                        timeout=self.settings.WORKER_POLL_TIMEOUT
                    )
                except Exception as e:
                    logger.error(f"Error polling Redis queue: {e}")
                    self.semaphore.release()
                    await asyncio.sleep(1.0)
                    continue

                if not job_data:
                    # Queue is empty, release concurrency slot and continue polling
                    self.semaphore.release()
                    continue

                job_id_str = job_data.get("job_id")
                if not job_id_str:
                    self.semaphore.release()
                    continue

                # Launch async task to execute job
                task = asyncio.create_task(self._run_job_wrapper(job_id_str))
                self.active_tasks.add(task)
                task.add_done_callback(self.active_tasks.discard)

        finally:
            logger.info(f"Worker {self.worker_id} initiating graceful shutdown...")
            heartbeat_task.cancel()

            # Wait for active tasks to complete up to timeout
            if self.active_tasks:
                logger.info(f"Waiting for {len(self.active_tasks)} in-flight tasks to finish...")
                done, pending = await asyncio.wait(self.active_tasks, timeout=15.0)
                if pending:
                    logger.warning(
                        f"Cancelling {len(pending)} tasks that exceeded shutdown timeout"
                    )
                    for p in pending:
                        p.cancel()

            # Deregister worker from Redis
            try:
                await self.broker.deregister_worker(self.worker_id)
            except Exception:
                pass
            logger.info(f"Worker {self.worker_id} safely terminated.")

    async def _run_job_wrapper(self, job_id_str: str) -> None:
        """Execute job and ensure semaphore is released upon completion."""
        try:
            await self.executor.execute_job(job_id_str)
        except Exception as e:
            logger.error(f"Unhandled error in job wrapper for {job_id_str}: {e}", exc_info=True)
        finally:
            self.semaphore.release()

    async def _worker_heartbeat_loop(self) -> None:
        """Periodically broadcast worker liveness to Redis."""
        while not self.stop_event.is_set():
            try:
                active_count = len(self.active_tasks)
                await self.broker.send_heartbeat(
                    self.worker_id,
                    active_jobs=active_count,
                    concurrency=self.concurrency,
                    ttl=int(self.settings.WORKER_HEARTBEAT_INTERVAL * 3),
                )
            except Exception as e:
                logger.warning(f"Failed to send worker heartbeat: {e}")

            try:
                await asyncio.wait_for(
                    self.stop_event.wait(), timeout=self.settings.WORKER_HEARTBEAT_INTERVAL
                )
                break
            except asyncio.TimeoutError:
                pass

    def stop(self) -> None:
        """Signal worker to stop accepting new jobs."""
        self.stop_event.set()


def run_worker_cli() -> None:
    """CLI entrypoint to run worker process."""
    setup_logging()
    concurrency_env = os.environ.get("WORKER_CONCURRENCY")
    concurrency = int(concurrency_env) if concurrency_env else None
    worker_id = os.environ.get("WORKER_ID")

    worker = Worker(worker_id=worker_id, concurrency=concurrency)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Attach signal handlers for graceful shutdown on POSIX / Windows
    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, worker.stop)
    else:
        # Windows signal handling helper
        def win_sig_handler(sig, frame):
            logger.info("Received interrupt signal on Windows, stopping worker...")
            worker.stop()

        signal.signal(signal.SIGINT, win_sig_handler)
        signal.signal(signal.SIGTERM, win_sig_handler)

    try:
        loop.run_until_complete(worker.start())
    except (KeyboardInterrupt, SystemExit):
        worker.stop()
        loop.run_until_complete(asyncio.sleep(0.5))
    finally:
        loop.close()


if __name__ == "__main__":
    run_worker_cli()
