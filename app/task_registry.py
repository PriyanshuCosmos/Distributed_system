"""Registered task handlers for TaskFlow workers.

Security invariant: Arbitrary code execution is strictly prohibited.
Only explicitly registered handler functions can be dispatched.
"""

import asyncio
import hashlib
import re
from typing import Any, Callable, Coroutine, Dict


class TaskExecutionError(Exception):
    """Raised when a task handler fails during execution."""

    pass


class TaskRegistry:
    """Central registry mapping task_type string names to callable async handlers."""

    def __init__(self) -> None:
        self._handlers: Dict[
            str, Callable[[Dict[str, Any], int], Coroutine[Any, Any, Dict[str, Any]]]
        ] = {}

    def register(
        self,
        task_type: str,
        handler: Callable[[Dict[str, Any], int], Coroutine[Any, Any, Dict[str, Any]]],
    ) -> None:
        """Register a new task handler."""
        self._handlers[task_type] = handler

    def is_registered(self, task_type: str) -> bool:
        """Check if a task type is supported."""
        return task_type in self._handlers

    def get_handler(
        self, task_type: str
    ) -> Callable[[Dict[str, Any], int], Coroutine[Any, Any, Dict[str, Any]]]:
        """Retrieve handler callable for task type."""
        if task_type not in self._handlers:
            raise KeyError(
                f"Unsupported task type '{task_type}'. Valid tasks: {list(self._handlers.keys())}"
            )
        return self._handlers[task_type]

    def list_tasks(self) -> list[str]:
        """Return list of all registered task type identifiers."""
        return sorted(list(self._handlers.keys()))


# Global task registry instance
registry = TaskRegistry()


# ==============================================================================
# Built-in Task Handlers
# ==============================================================================


async def handle_simulate_report(payload: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    """Simulates a heavy report aggregation workflow."""
    rows = min(int(payload.get("rows", 100)), 100000)
    report_type = str(payload.get("report_type", "sales_summary"))

    # Simulate non-blocking computational latency proportional to work size
    sim_delay = min(rows / 2000.0, 1.0)
    if sim_delay > 0:
        await asyncio.sleep(sim_delay)

    # Compute deterministic mock aggregations based on parameters
    total_val = rows * 42.50
    avg_val = 42.50
    checksum = hashlib.sha256(f"{report_type}:{rows}:{attempt}".encode()).hexdigest()[:12]

    return {
        "report_type": report_type,
        "rows_processed": rows,
        "metrics": {
            "total_revenue": round(total_val, 2),
            "average_ticket": round(avg_val, 2),
            "median_index": rows // 2,
            "variance": 14.28,
        },
        "report_checksum": checksum,
        "completed_attempt": attempt,
    }


async def handle_process_text(payload: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    """Performs tokenization, word count, and basic NLP extraction on input text."""
    text = str(payload.get("text", ""))
    if not text:
        text = "TaskFlow distributed task processing engine text sample"

    words = re.findall(r"\b\w+\b", text)
    word_count = len(words)
    char_count = len(text)
    unique_words = len(set(w.lower() for w in words))

    # Basic sentiment score heuristic
    positive_words = {"good", "great", "excellent", "fast", "reliable", "success", "distributed"}
    negative_words = {"bad", "slow", "fail", "broken", "bug", "error"}
    pos_matches = sum(1 for w in words if w.lower() in positive_words)
    neg_matches = sum(1 for w in words if w.lower() in negative_words)

    sentiment = "neutral"
    if pos_matches > neg_matches:
        sentiment = "positive"
    elif neg_matches > pos_matches:
        sentiment = "negative"

    return {
        "word_count": word_count,
        "character_count": char_count,
        "unique_words": unique_words,
        "sentiment": sentiment,
        "preview": text[:50] + ("..." if len(text) > 50 else ""),
        "completed_attempt": attempt,
    }


async def handle_simulated_failure(payload: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    """Configurable handler designed to test retries, backoff, and dead-letter behavior."""
    fail_until_attempt = int(payload.get("fail_until_attempt", 2))
    always_fail = bool(payload.get("always_fail", False))
    error_message = str(
        payload.get("error_message", "Downstream service unavailable (503 Service Unavailable)")
    )

    if always_fail or attempt < fail_until_attempt:
        raise TaskExecutionError(
            f"Simulated failure on attempt {attempt} (configured fail_until_attempt={fail_until_attempt}): {error_message}"
        )

    return {
        "status": "recovered",
        "recovered_on_attempt": attempt,
        "message": "Task succeeded after preceding retry attempts",
    }


async def handle_sleep_task(payload: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    """Simulates an I/O wait, testing worker heartbeats, leases, and cancellation races."""
    seconds = min(float(payload.get("seconds", 2.0)), 30.0)
    await asyncio.sleep(seconds)
    return {
        "slept_seconds": seconds,
        "completed_attempt": attempt,
    }


async def handle_send_notification(payload: Dict[str, Any], attempt: int) -> Dict[str, Any]:
    """Simulates idempotent notification delivery (e.g. email, SMS, or webhook)."""
    recipient = str(payload.get("recipient", "ops@example.com"))
    channel = str(payload.get("channel", "email"))
    delivery_id = f"del_{hashlib.md5(f'{recipient}:{channel}:{attempt}'.encode()).hexdigest()[:8]}"

    await asyncio.sleep(0.05)  # Simulate network latency
    return {
        "delivery_id": delivery_id,
        "recipient": recipient,
        "channel": channel,
        "delivered": True,
        "completed_attempt": attempt,
    }


# Register all built-in tasks
registry.register("simulate_report", handle_simulate_report)
registry.register("process_text", handle_process_text)
registry.register("simulated_failure", handle_simulated_failure)
registry.register("sleep_task", handle_sleep_task)
registry.register("send_notification", handle_send_notification)
