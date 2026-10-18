"""Reproducible performance benchmark script for TaskFlow.

Measures actual job submission throughput, execution latency, queue wait time,
and percentiles (p50, p95, p99) against a running TaskFlow instance.
"""

import argparse
import asyncio
import statistics
import time
from typing import Any, Dict, List

import httpx


def calculate_percentiles(values: List[float]) -> Dict[str, float]:
    """Return p50, p95, and p99 percentiles for a list of values."""
    if not values:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "min": 0.0, "max": 0.0, "avg": 0.0}
    sorted_vals = sorted(values)
    n = len(sorted_vals)
    return {
        "min": round(sorted_vals[0], 2),
        "avg": round(statistics.mean(sorted_vals), 2),
        "p50": round(sorted_vals[int(n * 0.50)], 2),
        "p95": round(sorted_vals[min(int(n * 0.95), n - 1)], 2),
        "p99": round(sorted_vals[min(int(n * 0.99), n - 1)], 2),
        "max": round(sorted_vals[-1], 2),
    }


async def submit_worker(
    client: httpx.AsyncClient,
    base_url: str,
    queue: asyncio.Queue,
    results: List[Dict[str, Any]],
) -> None:
    """Submitter worker pulling jobs from internal queue."""
    while not queue.empty():
        idx = await queue.get()
        start = time.time()
        try:
            payload = {
                "task_type": "process_text",
                "payload": {"text": f"Benchmark job sample data index {idx}"},
                "priority": idx % 5,
            }
            resp = await client.post(f"{base_url}/api/jobs", json=payload, timeout=10.0)
            latency = (time.time() - start) * 1000.0
            if resp.status_code == 201:
                data = resp.json()
                results.append(
                    {"success": True, "job_id": data["id"], "submit_latency_ms": latency}
                )
            else:
                results.append(
                    {"success": False, "status": resp.status_code, "submit_latency_ms": latency}
                )
        except Exception as e:
            results.append({"success": False, "error": str(e), "submit_latency_ms": 0.0})
        finally:
            queue.task_done()


async def run_benchmark(
    base_url: str,
    job_count: int,
    concurrency: int,
    wait_for_completion: bool = True,
    poll_timeout: float = 60.0,
) -> None:
    """Run full benchmark execution."""
    print("=" * 70)
    print("TaskFlow Performance Benchmark")
    print("=" * 70)
    print(f"Target URL:            {base_url}")
    print(f"Total Jobs to Submit:  {job_count}")
    print(f"Submitter Concurrency: {concurrency}")
    print(f"Wait for Execution:    {wait_for_completion}")
    print("-" * 70)

    # 1. Check API Liveness
    async with httpx.AsyncClient() as client:
        try:
            h_resp = await client.get(f"{base_url}/api/health", timeout=3.0)
            if h_resp.status_code != 200:
                print(f"ERROR: API health check failed with status {h_resp.status_code}")
                return
        except Exception as e:
            print(f"ERROR: Could not connect to TaskFlow API at {base_url}: {e}")
            return

    # 2. Queue Submissions
    work_queue: asyncio.Queue = asyncio.Queue()
    for i in range(job_count):
        await work_queue.put(i)

    submission_results: List[Dict[str, Any]] = []

    print("[*] Starting concurrent job submission...")
    start_submit_time = time.time()

    async with httpx.AsyncClient(limits=httpx.Limits(max_connections=concurrency * 2)) as client:
        workers = [
            asyncio.create_task(submit_worker(client, base_url, work_queue, submission_results))
            for _ in range(concurrency)
        ]
        await asyncio.gather(*workers)

    total_submit_time = time.time() - start_submit_time
    success_submits = [r for r in submission_results if r.get("success")]
    failed_submits = [r for r in submission_results if not r.get("success")]

    submit_latencies = [r["submit_latency_ms"] for r in success_submits]
    submit_percentiles = calculate_percentiles(submit_latencies)
    throughput = len(success_submits) / total_submit_time if total_submit_time > 0 else 0

    print("\n[+] Submission Phase Completed:")
    print(
        f"    Total Submitted:       {len(success_submits)} / {job_count} (Errors: {len(failed_submits)})"
    )
    print(f"    Elapsed Time:          {total_submit_time:.2f} seconds")
    print(f"    Submission Throughput: {throughput:.2f} jobs/second")
    print(
        f"    API POST Latency:      p50={submit_percentiles['p50']}ms | p95={submit_percentiles['p95']}ms | p99={submit_percentiles['p99']}ms"
    )

    # 3. Wait for Workers to Execute Jobs (if enabled)
    if wait_for_completion and success_submits:
        print("\n[*] Monitoring queue execution until all submitted jobs complete...")
        job_ids = [r["job_id"] for r in success_submits]
        poll_start = time.time()

        execution_durations: List[float] = []
        completed_count = 0
        failed_execution_count = 0

        async with httpx.AsyncClient() as client:
            while time.time() - poll_start < poll_timeout:
                stats_res = await client.get(f"{base_url}/api/stats")
                if stats_res.status_code == 200:
                    stats = stats_res.json()
                    depth = stats.get("queue_depth", {})
                    ready_depth = depth.get("ready", 0)
                    running_count = stats.get("jobs_by_status", {}).get("RUNNING", 0)

                    if ready_depth == 0 and running_count == 0:
                        print("    -> Ready queue and running workers reached 0.")
                        break

                await asyncio.sleep(1.0)

            # Sample execution latencies from attempts
            print("[*] Querying execution durations for sample jobs...")
            sample_ids = job_ids[: min(100, len(job_ids))]
            for j_id in sample_ids:
                try:
                    att_res = await client.get(f"{base_url}/api/jobs/{j_id}/attempts")
                    if att_res.status_code == 200:
                        attempts = att_res.json()
                        for a in attempts:
                            if a.get("duration_ms") is not None:
                                execution_durations.append(a["duration_ms"])
                            if a.get("status") == "SUCCEEDED":
                                completed_count += 1
                            elif a.get("status") == "FAILED":
                                failed_execution_count += 1
                except Exception:
                    pass

        exec_percentiles = calculate_percentiles(execution_durations)
        print("\n[+] Execution Phase Completed:")
        print(
            f"    Total Processed in Sample: {completed_count} succeeded, {failed_execution_count} failed"
        )
        if execution_durations:
            print(
                f"    Task Execution Latency:    p50={exec_percentiles['p50']}ms | p95={exec_percentiles['p95']}ms | p99={exec_percentiles['p99']}ms"
            )

    print("=" * 70)


def main() -> None:
    parser = argparse.ArgumentParser(description="TaskFlow Benchmark Runner")
    parser.add_argument("--url", default="http://localhost:8000", help="TaskFlow API base URL")
    parser.add_argument("-n", "--jobs", type=int, default=100, help="Number of jobs to submit")
    parser.add_argument("-c", "--concurrency", type=int, default=10, help="Concurrent HTTP clients")
    parser.add_argument("--no-wait", action="store_true", help="Do not wait for worker execution")
    parser.add_argument(
        "--timeout", type=float, default=60.0, help="Max wait timeout for completion"
    )
    args = parser.parse_args()

    asyncio.run(
        run_benchmark(
            base_url=args.url.rstrip("/"),
            job_count=args.jobs,
            concurrency=args.concurrency,
            wait_for_completion=not args.no_wait,
            poll_timeout=args.timeout,
        )
    )


if __name__ == "__main__":
    main()
