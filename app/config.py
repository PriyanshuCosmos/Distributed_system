"""Application configuration using Pydantic Settings."""

from functools import lru_cache
from typing import List

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # General Environment
    ENVIRONMENT: str = Field(default="development", description="Runtime environment")
    APP_NAME: str = Field(default="TaskFlow", description="Application name")
    LOG_LEVEL: str = Field(default="INFO", description="Logging level")
    API_HOST: str = Field(default="0.0.0.0", description="API host to bind to")
    API_PORT: int = Field(default=8000, description="API port to bind to")

    # Database Settings
    # Default to PostgreSQL for production/Docker; SQLite is also supported for standalone/tests
    DATABASE_URL: str = Field(
        default="postgresql+psycopg://taskflow:taskflow@localhost:5432/taskflow",
        description="Async database connection string",
    )
    SYNC_DATABASE_URL: str = Field(
        default="postgresql+psycopg://taskflow:taskflow@localhost:5432/taskflow",
        description="Synchronous database connection string for Alembic",
    )
    DB_POOL_SIZE: int = Field(default=10, description="SQLAlchemy connection pool size")
    DB_MAX_OVERFLOW: int = Field(default=20, description="SQLAlchemy max pool overflow")

    # Redis Broker Settings
    REDIS_URL: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL for queue and coordination",
    )
    USE_FAKEREDIS: bool = Field(
        default=False,
        description="Force using in-memory fake redis broker (useful for local dev without Docker)",
    )

    # Worker Settings
    WORKER_CONCURRENCY: int = Field(
        default=4, description="Number of concurrent task execution slots per worker process"
    )
    WORKER_POLL_TIMEOUT: float = Field(
        default=2.0, description="Seconds to block when polling the queue"
    )
    WORKER_HEARTBEAT_INTERVAL: float = Field(
        default=5.0, description="Seconds between worker lease heartbeat renewals"
    )
    WORKER_LEASE_DURATION: float = Field(
        default=15.0, description="Seconds before an unrenewed job lease is considered stale"
    )

    # Retry and Backoff Defaults
    DEFAULT_MAX_RETRIES: int = Field(
        default=3, description="Default max attempts allowed for jobs if not specified"
    )
    DEFAULT_TASK_TIMEOUT_SECONDS: float = Field(
        default=60.0, description="Default timeout in seconds for job execution"
    )
    BACKOFF_BASE_SECONDS: float = Field(
        default=2.0, description="Base multiplier for exponential backoff (base * 2^attempt)"
    )
    BACKOFF_MAX_SECONDS: float = Field(
        default=300.0, description="Maximum backoff delay ceiling in seconds"
    )
    BACKOFF_JITTER: bool = Field(
        default=True,
        description="Add random jitter (+/- 20%) to backoff to prevent thundering herds",
    )

    # Scheduler and Outbox Intervals
    SCHEDULER_POLL_INTERVAL: float = Field(
        default=1.0, description="Seconds between scheduler checks for due jobs"
    )
    STALE_JOB_CHECK_INTERVAL: float = Field(
        default=5.0, description="Seconds between stale worker lease reaper runs"
    )
    OUTBOX_POLL_INTERVAL: float = Field(
        default=1.0, description="Seconds between transactional outbox publisher sweeps"
    )

    # Security & Request Constraints
    MAX_PAYLOAD_BYTES: int = Field(
        default=65536, description="Maximum payload size in bytes (64 KB default)"
    )
    CORS_ORIGINS: List[str] = Field(default=["*"], description="Allowed CORS origins")


@lru_cache()
def get_settings() -> Settings:
    """Return cached application settings singleton."""
    return Settings()
