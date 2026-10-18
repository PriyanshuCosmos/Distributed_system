"""Seed sample jobs of various task types, priorities, and schedules."""

import asyncio
from datetime import datetime, timedelta, timezone

import httpx


async def seed() -> None:
    base_url = "http://localhost:8000"
    print(f"Connecting to TaskFlow API at {base_url}...")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Check health
        try:
            h = await client.get(f"{base_url}/api/health")
            if h.status_code != 200:
                print("API not ready. Start the API server first.")
                return
        except Exception as e:
            print(f"Failed to connect to API: {e}")
            return

        sample_jobs = [
            {
                "task_type": "simulate_report",
                "payload": {"rows": 1500, "report_type": "quarterly_earnings"},
                "priority": 10,
                "idempotency_key": "sample_q3_report",
            },
            {
                "task_type": "process_text",
                "payload": {
                    "text": "Distributed systems require careful failure recovery, idempotent handlers, and leases."
                },
                "priority": 5,
            },
            {
                "task_type": "simulate_report",
                "payload": {"rows": 300, "report_type": "user_retention"},
                "priority": 0,
            },
            {
                "task_type": "simulated_failure",
                "payload": {
                    "fail_until_attempt": 2,
                    "error_message": "Transient network timeout (504)",
                },
                "priority": 0,
                "max_attempts": 3,
            },
            {
                "task_type": "simulated_failure",
                "payload": {
                    "always_fail": True,
                    "error_message": "Permanent schema validation failure",
                },
                "priority": -5,
                "max_attempts": 2,
            },
            {
                "task_type": "sleep_task",
                "payload": {"seconds": 2.5},
                "priority": 1,
            },
            {
                "task_type": "send_notification",
                "payload": {"recipient": "admin@example.com", "channel": "webhook"},
                "priority": 0,
            },
            # Scheduled job in future (20 seconds from now)
            {
                "task_type": "simulate_report",
                "payload": {"rows": 500, "report_type": "future_scheduled_batch"},
                "priority": 0,
                "run_at": (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat(),
            },
        ]

        print(f"Submitting {len(sample_jobs)} sample jobs...")
        for job_def in sample_jobs:
            resp = await client.post(f"{base_url}/api/jobs", json=job_def)
            if resp.status_code == 201:
                data = resp.json()
                print(
                    f"  [+] Created job {data['id']} ({data['task_type']}, priority={data['priority']}, status={data['status']})"
                )
            else:
                print(f"  [-] Failed: {resp.text}")

        print("\nSeeding complete! Check the dashboard at http://localhost:8000")


if __name__ == "__main__":
    asyncio.run(seed())
