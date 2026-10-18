"""Tests for schema validation, models, and task registry constraints."""

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from app.models import Job, JobStatus
from app.schemas import JobCreate
from app.task_registry import registry


def test_valid_job_create() -> None:
    data = {
        "task_type": "simulate_report",
        "payload": {"rows": 100, "report_type": "sales"},
        "priority": 5,
        "max_attempts": 3,
    }
    job_in = JobCreate(**data)
    assert job_in.task_type == "simulate_report"
    assert job_in.priority == 5
    assert job_in.max_attempts == 3
    assert job_in.payload["rows"] == 100


def test_empty_task_type_rejected() -> None:
    with pytest.raises(ValidationError):
        JobCreate(task_type="", payload={})


def test_oversized_payload_rejected() -> None:
    oversized_dict = {"data": "x" * 70000}  # ~70 KB
    with pytest.raises(ValidationError) as exc:
        JobCreate(task_type="process_text", payload=oversized_dict)
    assert "Payload size exceeds maximum allowed 64 KB limit" in str(exc.value)


def test_priority_and_attempts_bounds() -> None:
    with pytest.raises(ValidationError):
        JobCreate(task_type="simulate_report", priority=999)  # Max is 100

    with pytest.raises(ValidationError):
        JobCreate(task_type="simulate_report", max_attempts=50)  # Max is 20


def test_naive_datetime_converted_to_utc() -> None:
    naive_dt = datetime(2027, 1, 1, 12, 0, 0)
    job_in = JobCreate(task_type="simulate_report", run_at=naive_dt)
    assert job_in.run_at.tzinfo == timezone.utc


def test_job_state_machine_valid_transitions() -> None:
    job = Job(status=JobStatus.QUEUED)
    assert job.can_transition_to(JobStatus.RUNNING) is True
    assert job.can_transition_to(JobStatus.CANCELLED) is True
    assert (
        job.can_transition_to(JobStatus.SUCCEEDED) is False
    )  # Cannot jump from QUEUED directly to SUCCEEDED

    job.status = JobStatus.RUNNING
    assert job.can_transition_to(JobStatus.SUCCEEDED) is True
    assert job.can_transition_to(JobStatus.RETRYING) is True
    assert job.can_transition_to(JobStatus.FAILED) is True

    job.status = JobStatus.SUCCEEDED
    assert job.can_transition_to(JobStatus.RUNNING) is False  # Terminal state


def test_task_registry_rejection() -> None:
    assert registry.is_registered("simulate_report") is True
    assert registry.is_registered("process_text") is True
    assert registry.is_registered("arbitrary_python_code") is False

    with pytest.raises(KeyError):
        registry.get_handler("arbitrary_python_code")
