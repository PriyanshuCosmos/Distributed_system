"""SQLAlchemy models and job lifecycle enums."""

import enum
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.types import JSON, TypeDecorator


def utc_now() -> datetime:
    """Return timezone-aware current UTC time."""
    return datetime.now(timezone.utc)


class CompatibleJSON(TypeDecorator):
    """Dialect-agnostic JSON type that uses JSONB on PostgreSQL and JSON on others."""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB(none_as_null=True))
        return dialect.type_descriptor(JSON(none_as_null=True))


class Base(DeclarativeBase):
    pass


class JobStatus(str, enum.Enum):
    """Documented lifecycle states for TaskFlow jobs."""

    PENDING_DISPATCH = "PENDING_DISPATCH"  # In DB outbox, waiting for Redis queue publish
    QUEUED = "QUEUED"  # Available in Redis ready queue
    SCHEDULED = "SCHEDULED"  # Scheduled for future UTC execution
    RUNNING = "RUNNING"  # Claimed by a worker process with active lease
    RETRYING = "RETRYING"  # Failed an attempt; awaiting exponential backoff
    SUCCEEDED = "SUCCEEDED"  # Successfully completed
    FAILED = "FAILED"  # Exhausted all retries; dead-lettered
    CANCELLED = "CANCELLED"  # Explicitly cancelled prior to execution


# Valid transitions mapping to enforce state machine invariants
VALID_TRANSITIONS = {
    JobStatus.PENDING_DISPATCH: {JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.CANCELLED},
    JobStatus.QUEUED: {JobStatus.RUNNING, JobStatus.CANCELLED},
    JobStatus.SCHEDULED: {JobStatus.QUEUED, JobStatus.CANCELLED},
    JobStatus.RUNNING: {
        JobStatus.SUCCEEDED,
        JobStatus.RETRYING,
        JobStatus.FAILED,
        JobStatus.CANCELLED,
    },
    JobStatus.RETRYING: {JobStatus.QUEUED, JobStatus.SCHEDULED, JobStatus.CANCELLED},
    JobStatus.SUCCEEDED: set(),  # Terminal
    JobStatus.FAILED: {JobStatus.QUEUED, JobStatus.PENDING_DISPATCH},  # Allows safe manual retry
    JobStatus.CANCELLED: set(),  # Terminal
}


class Job(Base):
    """Primary durable job record in PostgreSQL."""

    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    payload: Mapped[Dict[str, Any]] = mapped_column(CompatibleJSON, nullable=False, default=dict)

    status: Mapped[JobStatus] = mapped_column(
        Enum(JobStatus, native_enum=False, length=30),
        default=JobStatus.QUEUED,
        nullable=False,
        index=True,
    )

    # Priority convention: higher integer = higher priority (e.g. 10=High, 0=Normal, -10=Low)
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False, index=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False, index=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now, nullable=False
    )

    # Scheduled future time or next retry time
    run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    attempt_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3, nullable=False)

    last_error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    result: Mapped[Optional[Dict[str, Any]]] = mapped_column(CompatibleJSON, nullable=True)

    # Optional idempotency key to prevent duplicate job creation
    idempotency_key: Mapped[Optional[str]] = mapped_column(
        String(128), unique=True, nullable=True, index=True
    )

    # Worker lease metadata for distributed crash detection and fencing
    worker_id: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    lease_token: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    lease_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    # Relationships
    attempts: Mapped[List["JobAttempt"]] = relationship(
        "JobAttempt",
        back_populates="job",
        cascade="all, delete-orphan",
        order_by="JobAttempt.attempt_number",
    )
    outbox_entries: Mapped[List["OutboxMessage"]] = relationship(
        "OutboxMessage", back_populates="job", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_jobs_status_run_at", "status", "run_at"),
        Index("ix_jobs_status_priority", "status", "priority"),
        Index("ix_jobs_lease_expiry", "status", "lease_expires_at"),
    )

    def can_transition_to(self, new_status: JobStatus) -> bool:
        """Validate whether transition to new_status is allowed."""
        return new_status in VALID_TRANSITIONS.get(self.status, set())


class JobAttempt(Base):
    """Historical record of an individual worker execution attempt."""

    __tablename__ = "job_attempts"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(100), nullable=False)

    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False
    )
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    duration_ms: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    status: Mapped[str] = mapped_column(
        String(30), nullable=False
    )  # SUCCEEDED, FAILED, TIMED_OUT, CANCELLED
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    job: Mapped["Job"] = relationship("Job", back_populates="attempts")

    __table_args__ = (Index("ix_job_attempts_job_number", "job_id", "attempt_number"),)


class OutboxMessage(Base):
    """Transactional Outbox record to guarantee atomic DB write and Redis publication."""

    __tablename__ = "outbox_messages"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("jobs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    topic: Mapped[str] = mapped_column(String(50), nullable=False, default="jobs:ready")
    payload: Mapped[Dict[str, Any]] = mapped_column(CompatibleJSON, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, nullable=False, index=True
    )
    dispatched_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    status: Mapped[str] = mapped_column(
        String(20), default="PENDING", nullable=False, index=True
    )  # PENDING, DISPATCHED, FAILED
    retry_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)

    job: Mapped["Job"] = relationship("Job", back_populates="outbox_entries")

    __table_args__ = (Index("ix_outbox_status_created", "status", "created_at"),)
