"""REST API routes for health checks, readiness probes, and task registry inspection."""

from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.logging_config import get_logger
from app.queue.redis_queue import get_broker
from app.schemas import HealthResponse
from app.task_registry import registry

logger = get_logger("routes.health")
router = APIRouter(prefix="/api", tags=["Health & Registry"])


@router.get(
    "/health",
    summary="Process liveness check",
    description="Indicates whether the FastAPI HTTP server process is running.",
)
async def health_check() -> Dict[str, str]:
    return {"status": "ok", "timestamp": datetime.now(timezone.utc).isoformat()}


@router.get(
    "/ready",
    response_model=HealthResponse,
    summary="Dependency readiness probe",
    description="Distinguishes process health from PostgreSQL and Redis dependency availability.",
)
async def readiness_probe(
    response: Response,
    db: AsyncSession = Depends(get_db),
) -> HealthResponse:
    db_status = "ok"
    redis_status = "ok"

    # 1. Check PostgreSQL connectivity
    try:
        await db.execute(text("SELECT 1"))
    except Exception as e:
        logger.error(f"Readiness check failed for Database: {e}")
        db_status = "unhealthy"

    # 2. Check Redis broker connectivity
    try:
        broker = get_broker()
        client = await broker.get_client()
        await client.ping()
    except Exception as e:
        logger.error(f"Readiness check failed for Redis: {e}")
        redis_status = "unhealthy"

    is_ready = db_status == "ok" and redis_status == "ok"
    if not is_ready:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE

    return HealthResponse(
        status="ok" if is_ready else "degraded",
        database=db_status,
        redis=redis_status,
    )


@router.get(
    "/tasks",
    summary="List registered task types",
    description="Returns all registered built-in task types that can be submitted to TaskFlow.",
)
async def list_available_tasks() -> Dict[str, Any]:
    tasks = registry.list_tasks()
    descriptions = {
        "simulate_report": "Simulates computation of an aggregated data report (parameters: rows, report_type)",
        "process_text": "Performs word counts, character counts, and basic sentiment heuristic (parameters: text)",
        "simulated_failure": "Simulates transient or permanent task failure for testing retries (parameters: fail_until_attempt, error_message)",
        "sleep_task": "Simulates I/O latency to test worker concurrency, leases, and cancellation (parameters: seconds)",
        "send_notification": "Simulates idempotent notification delivery (parameters: recipient, channel)",
    }
    return {
        "tasks": [
            {"name": t, "description": descriptions.get(t, "Registered handler")} for t in tasks
        ]
    }
