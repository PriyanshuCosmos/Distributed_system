"""TaskFlow FastAPI application entrypoint."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from app.config import get_settings
from app.database import close_db, init_db
from app.logging_config import get_logger, setup_logging
from app.queue.redis_queue import get_broker
from app.routes import health_router, jobs_router, stats_router

logger = get_logger("main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan managing database initialization and connection cleanup."""
    settings = get_settings()
    setup_logging(log_level=settings.LOG_LEVEL)
    logger.info("Initializing TaskFlow API service...")

    # Initialize database tables
    try:
        await init_db()
    except Exception as e:
        logger.error(f"Failed to initialize database tables: {e}", exc_info=True)

    # Initialize Redis broker client
    try:
        broker = get_broker()
        await broker.get_client()
    except Exception as e:
        logger.warning(f"Initial Redis connection note: {e}")

    logger.info("TaskFlow API is ready to accept requests.")
    yield

    logger.info("Shutting down TaskFlow API service...")
    try:
        broker = get_broker()
        await broker.close()
    except Exception:
        pass

    try:
        await close_db()
    except Exception:
        pass
    logger.info("TaskFlow API shutdown complete.")


app = FastAPI(
    title="TaskFlow: Distributed Task Queue & Job Scheduler",
    description=(
        "Production-grade distributed task queue and scheduler featuring "
        "priority scheduling, transactional outbox guarantee, distributed leases, "
        "and worker crash recovery."
    ),
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS Middleware configuration
settings = get_settings()
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API Routers
app.include_router(jobs_router)
app.include_router(stats_router)
app.include_router(health_router)

# Mount Dashboard Static Assets
static_dir = Path(__file__).resolve().parent.parent / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    @app.get("/", include_in_schema=False)
    async def serve_dashboard() -> FileResponse:
        """Serve the TaskFlow frontend dashboard."""
        index_file = static_dir / "index.html"
        return FileResponse(index_file)


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Security invariant: Prevent leaking internal stack traces to clients."""
    logger.error(
        f"Unhandled exception on {request.method} {request.url.path}: {exc}", exc_info=True
    )
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"detail": "An internal server error occurred. Please contact the administrator."},
    )
