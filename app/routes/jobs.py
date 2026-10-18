"""REST API routes for Job submission, inspection, cancellation, and retry."""

import math
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.models import JobStatus
from app.schemas import (
    JobAttemptResponse,
    JobCreate,
    JobListResponse,
    JobResponse,
    JobRetryRequest,
)
from app.services.job_service import (
    InvalidJobStateError,
    JobNotFoundError,
    JobService,
)

router = APIRouter(prefix="/api/jobs", tags=["Jobs"])


@router.post(
    "",
    response_model=JobResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Submit a new background job",
    description="Submits a job to TaskFlow. Uses Transactional Outbox for guaranteed enqueueing and supports idempotency keys.",
)
async def submit_job(
    job_in: JobCreate,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    service = JobService(db)
    try:
        job, is_new = await service.create_job(job_in)
        return JobResponse.model_validate(job)
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(e))


@router.get(
    "",
    response_model=JobListResponse,
    summary="List and filter jobs",
    description="Query jobs with optional status and task_type filters and pagination.",
)
async def list_jobs(
    status_filter: Optional[JobStatus] = Query(
        None, alias="status", description="Filter by job status"
    ),
    task_type: Optional[str] = Query(None, description="Filter by registered task type"),
    page: int = Query(1, ge=1, description="Page number (1-indexed)"),
    size: int = Query(20, ge=1, le=100, description="Items per page"),
    db: AsyncSession = Depends(get_db),
) -> JobListResponse:
    service = JobService(db)
    offset = (page - 1) * size
    items, total = await service.list_jobs(
        status=status_filter,
        task_type=task_type,
        limit=size,
        offset=offset,
    )
    pages = math.ceil(total / size) if total > 0 else 1
    return JobListResponse(
        items=[JobResponse.model_validate(j) for j in items],
        total=total,
        page=page,
        size=size,
        pages=pages,
    )


@router.get(
    "/{job_id}",
    response_model=JobResponse,
    summary="Get job details",
    description="Retrieve comprehensive job details, current status, execution result, and error information.",
)
async def get_job(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    service = JobService(db)
    job = await service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found",
        )
    return JobResponse.model_validate(job)


@router.get(
    "/{job_id}/attempts",
    response_model=List[JobAttemptResponse],
    summary="Get job execution attempts",
    description="Retrieve chronological history of all execution attempts, worker IDs, durations, and errors.",
)
async def get_job_attempts(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> List[JobAttemptResponse]:
    service = JobService(db)
    # Verify job exists first
    job = await service.get_job(job_id)
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Job with ID {job_id} not found",
        )
    attempts = await service.get_job_attempts(job_id)
    return [JobAttemptResponse.model_validate(a) for a in attempts]


@router.post(
    "/{job_id}/cancel",
    response_model=JobResponse,
    summary="Cancel a queued or scheduled job",
    description="Cancels an eligible pending job. Returns 409 Conflict if job is currently running or already terminal.",
)
async def cancel_job(
    job_id: UUID,
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    service = JobService(db)
    try:
        cancelled_job = await service.cancel_job(job_id)
        return JobResponse.model_validate(cancelled_job)
    except JobNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except InvalidJobStateError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))


@router.post(
    "/{job_id}/retry",
    response_model=JobResponse,
    summary="Retry a failed job",
    description="Manually retries an eligible job that has failed and entered the dead-letter state.",
)
async def retry_job(
    job_id: UUID,
    req: JobRetryRequest = JobRetryRequest(),
    db: AsyncSession = Depends(get_db),
) -> JobResponse:
    service = JobService(db)
    try:
        retried_job = await service.retry_failed_job(job_id, reset_attempts=req.reset_attempts)
        return JobResponse.model_validate(retried_job)
    except JobNotFoundError as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except InvalidJobStateError as e:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(e))
