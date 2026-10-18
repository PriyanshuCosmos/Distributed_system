"""REST API routes for monitoring and system statistics."""

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.schemas import SystemStatsResponse
from app.services.job_service import JobService

router = APIRouter(prefix="/api/stats", tags=["Monitoring"])


@router.get(
    "",
    response_model=SystemStatsResponse,
    summary="Get system metrics and queue stats",
    description="Returns real-time queue depths, job counts categorized by lifecycle status, and active registered workers.",
)
async def get_stats(
    db: AsyncSession = Depends(get_db),
) -> SystemStatsResponse:
    service = JobService(db)
    stats_dict = await service.get_system_stats()
    return SystemStatsResponse.model_validate(stats_dict)
