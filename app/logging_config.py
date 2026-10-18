"""Structured logging configuration for TaskFlow."""

import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any, Dict


class StructuredJsonFormatter(logging.Formatter):
    """Formats log records as structured JSON lines for production observability."""

    def format(self, record: logging.LogRecord) -> str:
        log_obj: Dict[str, Any] = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Include custom structured fields if passed via extra={}
        for key in (
            "job_id",
            "task_type",
            "worker_id",
            "attempt",
            "duration_ms",
            "status",
            "queue",
        ):
            val = getattr(record, key, None)
            if val is not None:
                log_obj[key] = val

        if record.exc_info:
            log_obj["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_obj)


class CleanConsoleFormatter(logging.Formatter):
    """Readable formatted console output for local development."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        color = self.COLORS.get(record.levelname, self.RESET)
        prefix = f"[{timestamp}] {color}{record.levelname:<7}{self.RESET} [{record.name}]"

        extra_parts = []
        for key in ("job_id", "task_type", "worker_id", "attempt", "duration_ms", "status"):
            val = getattr(record, key, None)
            if val is not None:
                extra_parts.append(f"{key}={val}")

        extra_str = f" ({' '.join(extra_parts)})" if extra_parts else ""
        msg = f"{prefix} {record.getMessage()}{extra_str}"
        if record.exc_info:
            msg += f"\n{self.formatException(record.exc_info)}"
        return msg


def setup_logging(log_level: str = "INFO", json_logs: bool = False) -> None:
    """Configure root and application loggers."""
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    # Remove existing handlers to prevent duplicates
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    if json_logs:
        handler.setFormatter(StructuredJsonFormatter())
    else:
        handler.setFormatter(CleanConsoleFormatter())

    root_logger.addHandler(handler)

    # Silence overly verbose external loggers
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
    logging.getLogger("asyncio").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """Return a logger instance with the given name."""
    return logging.getLogger(name)
