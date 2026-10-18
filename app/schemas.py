"""Pydantic schemas for request validation and response serialization."""

import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.models import JobStatus


class JobCreate(BaseModel):
    """Schema for submitting a new job."""

    task_type: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Registered task identifier (e.g., 'simulate_report', 'process_text')",
        examples=["simulate_report"],
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="JSON serializable arguments for the task",
        examples=[{"rows": 500, "report_type": "sales"}],
    )
    priority: int = Field(
        default=0,
        ge=-100,
        le=100,
        description="Job priority. Higher integers execute before lower integers (e.g. 10=High, 0=Normal, -10=Low)",
    )
    run_at: Optional[datetime] = Field(
        default=None,
        description="Optional future UTC timestamp for delayed or scheduled execution",
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum execution attempts before routing to dead-letter queue",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        max_length=128,
        description="Optional unique submission key to prevent duplicate job creation",
    )

    @field_validator("run_at")
    @classmethod
    def ensure_utc(cls, v: Optional[datetime]) -> Optional[datetime]:
        if v is not None:
            if v.tzinfo is None:
                # If naive, assume UTC
                v = v.replace(tzinfo=timezone.utc)
            else:
                v = v.astimezone(timezone.utc)
        return v

    @field_validator("payload")
    @classmethod
    def validate_payload_size(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        try:
            serialized = json.dumps(v)
            if len(serialized.encode("utf-8")) > 65536:
                raise ValueError("Payload size exceeds maximum allowed 64 KB limit")
        except (TypeError, OverflowError) as e:
            raise ValueError(f"Payload must be JSON serializable: {e}")
        return v


class JobResponse(BaseModel):
    """Detailed job representation returned by the API."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_type: str
    payload: Dict[str, Any]
    status: JobStatus
    priority: int
    created_at: datetime
    updated_at: datetime
    run_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    attempt_count: int
    max_attempts: int
    last_error: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None
    worker_id: Optional[str] = None


class JobAttemptResponse(BaseModel):
    """Execution attempt log entry."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    job_id: UUID
    attempt_number: int
    worker_id: str
    started_at: datetime
    finished_at: Optional[datetime] = None
    duration_ms: Optional[float] = None
    status: str
    error_message: Optional[str] = None


class JobListResponse(BaseModel):
    """Paginated list of jobs."""

    items: List[JobResponse]
    total: int
    page: int
    size: int
    pages: int


class JobRetryRequest(BaseModel):
    """Request payload to manually retry a failed job."""

    reset_attempts: bool = Field(
        default=False,
        description="If true, resets attempt_count to 0; otherwise increments and gives 1 additional attempt",
    )


class QueueDepth(BaseModel):
    ready: int = 0
    scheduled: int = 0
    dead_letter: int = 0


class WorkerInfo(BaseModel):
    worker_id: str
    last_heartbeat: str
    active_jobs: int = 0
    concurrency: int = 4


class SystemStatsResponse(BaseModel):
    """Comprehensive system-wide queue and job statistics."""

    queue_depth: QueueDepth
    jobs_by_status: Dict[str, int]
    active_workers: List[WorkerInfo]
    total_jobs: int
    success_rate: float
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class HealthResponse(BaseModel):
    """Service liveness and readiness response."""

    status: str  # "ok" or "degraded"
    database: str  # "ok" or "unhealthy"
    redis: str  # "ok" or "unhealthy"
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    app: str = "TaskFlow"
    version: str = "0.1.0"
