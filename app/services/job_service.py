"""Job business logic service layer."""

import random
from typing import List, Optional, Tuple
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.logging_config import get_logger
from app.models import Job, JobAttempt, JobStatus, OutboxMessage, utc_now
from app.queue.redis_queue import get_broker
from app.schemas import JobCreate
from app.task_registry import registry

logger = get_logger("service.job")


class InvalidJobStateError(Exception):
    """Raised when an illegal state transition is attempted."""

    pass


class JobNotFoundError(Exception):
    """Raised when requested job does not exist."""

    pass


class JobService:
    """Encapsulates all job lifecycle operations and state management."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.broker = get_broker()
        self.settings = get_settings()

    @staticmethod
    def calculate_backoff(
        attempt: int,
        base_seconds: float = 2.0,
        max_seconds: float = 300.0,
        jitter: bool = True,
    ) -> float:
        """
        Calculate exponential backoff with optional random jitter.
        Formula: min(max_delay, base * 2^(attempt - 1)) +/- jitter
        """
        attempt_clamped = max(1, attempt)
        exponential_delay = base_seconds * (2 ** (attempt_clamped - 1))
        delay = min(max_seconds, exponential_delay)
        if jitter:
            # Add uniform +/- 20% jitter to prevent synchronized retry storms
            delay = delay * random.uniform(0.8, 1.2)
        return round(delay, 2)

    async def create_job(self, job_in: JobCreate) -> Tuple[Job, bool]:
        """
        Idempotently create and persist a new job using Transactional Outbox pattern.
        Returns: (Job, is_new: bool)
        """
        # Validate that task_type is registered
        if not registry.is_registered(job_in.task_type):
            raise ValueError(
                f"Unsupported task type '{job_in.task_type}'. "
                f"Supported task types are: {registry.list_tasks()}"
            )

        # 1. Idempotency Check
        if job_in.idempotency_key:
            stmt = select(Job).where(Job.idempotency_key == job_in.idempotency_key)
            result = await self.session.execute(stmt)
            existing_job = result.scalar_one_or_none()
            if existing_job:
                logger.info(
                    f"Idempotent submission detected for key '{job_in.idempotency_key}'. "
                    f"Returning existing job {existing_job.id}"
                )
                return existing_job, False

        now = utc_now()
        is_scheduled = job_in.run_at is not None and job_in.run_at > now
        initial_status = JobStatus.SCHEDULED if is_scheduled else JobStatus.PENDING_DISPATCH

        # 2. Persist Job Record
        new_job = Job(
            task_type=job_in.task_type,
            payload=job_in.payload,
            priority=job_in.priority,
            run_at=job_in.run_at,
            max_attempts=job_in.max_attempts,
            idempotency_key=job_in.idempotency_key,
            status=initial_status,
            created_at=now,
            updated_at=now,
        )
        self.session.add(new_job)
        await self.session.flush()  # Generate UUID id for outbox

        # 3. Persist Outbox Event within the same DB Transaction
        topic = "jobs:scheduled" if is_scheduled else "jobs:ready"
        outbox_entry = OutboxMessage(
            job_id=new_job.id,
            topic=topic,
            payload={
                "job_id": str(new_job.id),
                "task_type": new_job.task_type,
                "priority": new_job.priority,
                "run_at": new_job.run_at.isoformat() if new_job.run_at else None,
            },
            status="PENDING",
            created_at=now,
        )
        self.session.add(outbox_entry)

        # Commit DB transaction: Job is now 100% durably recorded in PostgreSQL
        await self.session.commit()
        await self.session.refresh(new_job)

        # 4. Immediate Dispatch Attempt (Low-latency path)
        try:
            if is_scheduled:
                await self.broker.schedule(
                    job_id=new_job.id,
                    task_type=new_job.task_type,
                    run_at=new_job.run_at,
                    priority=new_job.priority,
                )
            else:
                await self.broker.enqueue(
                    job_id=new_job.id,
                    task_type=new_job.task_type,
                    priority=new_job.priority,
                )
                # Transition status to QUEUED upon confirmed Redis enqueue
                new_job.status = JobStatus.QUEUED
                new_job.updated_at = utc_now()
                outbox_entry.status = "DISPATCHED"
                outbox_entry.dispatched_at = utc_now()
                await self.session.commit()
                await self.session.refresh(new_job)
        except Exception as e:
            # Redis temporary partition: Outbox dispatcher background loop will deliver it safely!
            logger.warning(
                f"Immediate Redis enqueue failed for job {new_job.id} ({e}). "
                "Transactional Outbox Dispatcher will retry dispatch."
            )

        return new_job, True

    async def get_job(self, job_id: UUID) -> Optional[Job]:
        """Fetch a single job by UUID."""
        stmt = select(Job).where(Job.id == job_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_jobs(
        self,
        status: Optional[JobStatus] = None,
        task_type: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> Tuple[List[Job], int]:
        """Query jobs with optional status/type filters and pagination."""
        query = select(Job)
        count_query = select(func.count(Job.id))

        if status:
            query = query.where(Job.status == status)
            count_query = count_query.where(Job.status == status)
        if task_type:
            query = query.where(Job.task_type == task_type)
            count_query = count_query.where(Job.task_type == task_type)

        query = query.order_by(Job.created_at.desc()).offset(offset).limit(limit)

        total_res = await self.session.execute(count_query)
        total = total_res.scalar_one()

        jobs_res = await self.session.execute(query)
        jobs = list(jobs_res.scalars().all())

        return jobs, total

    async def get_job_attempts(self, job_id: UUID) -> List[JobAttempt]:
        """Retrieve attempt history for a job."""
        stmt = (
            select(JobAttempt)
            .where(JobAttempt.job_id == job_id)
            .order_by(JobAttempt.attempt_number.asc())
        )
        res = await self.session.execute(stmt)
        return list(res.scalars().all())

    async def cancel_job(self, job_id: UUID) -> Job:
        """
        Cancel a job if it has not yet completed.
        Enforces state transition rules and returns explicit conflict error for running jobs.
        """
        job = await self.get_job(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if job.status == JobStatus.RUNNING:
            raise InvalidJobStateError(
                f"Job {job_id} is currently running on worker '{job.worker_id}'. "
                "Cannot interrupt active execution."
            )

        if job.status in {JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED}:
            raise InvalidJobStateError(
                f"Job {job_id} is already in terminal state '{job.status.value}' and cannot be cancelled."
            )

        # Valid transitions for cancellation: PENDING_DISPATCH, QUEUED, SCHEDULED, RETRYING
        job.status = JobStatus.CANCELLED
        job.updated_at = utc_now()
        job.finished_at = utc_now()

        # Remove from Redis queues so worker won't pick it up
        await self.broker.remove_from_queues(job_id)

        await self.session.commit()
        await self.session.refresh(job)
        logger.info(f"Job {job_id} successfully cancelled.")
        return job

    async def retry_failed_job(self, job_id: UUID, reset_attempts: bool = False) -> Job:
        """
        Manually retry an eligible failed (dead-lettered) job.
        """
        job = await self.get_job(job_id)
        if not job:
            raise JobNotFoundError(f"Job {job_id} not found")

        if job.status != JobStatus.FAILED:
            raise InvalidJobStateError(
                f"Only jobs in FAILED state can be manually retried. Current state is '{job.status.value}'."
            )

        if reset_attempts:
            job.attempt_count = 0
        else:
            job.max_attempts += 1  # Grant an additional attempt

        job.status = JobStatus.PENDING_DISPATCH
        job.last_error = None
        job.updated_at = utc_now()

        # Add outbox entry to re-publish
        outbox = OutboxMessage(
            job_id=job.id,
            topic="jobs:ready",
            payload={
                "job_id": str(job.id),
                "task_type": job.task_type,
                "priority": job.priority,
            },
            status="PENDING",
            created_at=utc_now(),
        )
        self.session.add(outbox)

        await self.session.commit()
        await self.session.refresh(job)

        # Enqueue directly
        try:
            await self.broker.enqueue(job.id, job.task_type, job.priority)
            job.status = JobStatus.QUEUED
            outbox.status = "DISPATCHED"
            outbox.dispatched_at = utc_now()
            await self.session.commit()
            await self.session.refresh(job)
        except Exception as e:
            logger.warning(f"Direct retry enqueue failed: {e}. Outbox will handle.")

        logger.info(f"Job {job_id} re-enqueued for execution.")
        return job

    async def get_system_stats(self) -> dict:
        """Aggregate system stats from DB and Redis broker."""
        # 1. Job counts by status from PostgreSQL
        stmt = select(Job.status, func.count(Job.id)).group_by(Job.status)
        res = await self.session.execute(stmt)
        status_counts = {s.value: 0 for s in JobStatus}
        total_jobs = 0
        for status_enum, count in res.all():
            status_counts[status_enum.value] = count
            total_jobs += count

        # Calculate success rate
        succeeded = status_counts.get("SUCCEEDED", 0)
        failed = status_counts.get("FAILED", 0)
        completed = succeeded + failed
        success_rate = round((succeeded / completed * 100.0), 1) if completed > 0 else 100.0

        # 2. Redis queue depths
        queue_depth = await self.broker.get_queue_depth()

        # 3. Active workers
        active_workers = await self.broker.get_active_workers()

        return {
            "queue_depth": queue_depth,
            "jobs_by_status": status_counts,
            "active_workers": active_workers,
            "total_jobs": total_jobs,
            "success_rate": success_rate,
        }
