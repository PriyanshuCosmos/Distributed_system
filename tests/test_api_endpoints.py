"""Integration tests for all FastAPI REST API endpoints."""

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_api_submit_job_success(async_client: AsyncClient) -> None:
    resp = await async_client.post(
        "/api/jobs",
        json={
            "task_type": "simulate_report",
            "payload": {"rows": 150, "report_type": "monthly"},
            "priority": 5,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data
    assert data["task_type"] == "simulate_report"
    assert data["priority"] == 5
    assert data["status"] in ["QUEUED", "PENDING_DISPATCH"]


@pytest.mark.asyncio
async def test_api_submit_invalid_task_type(async_client: AsyncClient) -> None:
    resp = await async_client.post(
        "/api/jobs",
        json={
            "task_type": "malicious_eval_task",
            "payload": {},
        },
    )
    assert resp.status_code == 422
    assert "Unsupported task type" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_api_list_jobs_and_pagination(async_client: AsyncClient) -> None:
    # Submit 3 jobs
    for i in range(3):
        await async_client.post(
            "/api/jobs",
            json={"task_type": "process_text", "payload": {"text": f"Sample {i}"}},
        )

    # Test pagination
    resp = await async_client.get("/api/jobs?page=1&size=2")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["items"]) == 2
    assert data["total"] >= 3
    assert data["pages"] >= 2


@pytest.mark.asyncio
async def test_api_get_job_detail_and_not_found(async_client: AsyncClient) -> None:
    # 1. Create a job
    create_resp = await async_client.post(
        "/api/jobs",
        json={"task_type": "simulate_report", "payload": {"rows": 10}},
    )
    job_id = create_resp.json()["id"]

    # 2. Retrieve job details
    get_resp = await async_client.get(f"/api/jobs/{job_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["id"] == job_id

    # 3. Retrieve non-existent job
    fake_id = "00000000-0000-0000-0000-000000000000"
    missing_resp = await async_client.get(f"/api/jobs/{fake_id}")
    assert missing_resp.status_code == 404


@pytest.mark.asyncio
async def test_api_cancel_job(async_client: AsyncClient) -> None:
    create_resp = await async_client.post(
        "/api/jobs",
        json={"task_type": "sleep_task", "payload": {"seconds": 5}},
    )
    job_id = create_resp.json()["id"]

    cancel_resp = await async_client.post(f"/api/jobs/{job_id}/cancel")
    assert cancel_resp.status_code == 200
    assert cancel_resp.json()["status"] == "CANCELLED"

    # Cancelling again returns 409 Conflict
    conflict_resp = await async_client.post(f"/api/jobs/{job_id}/cancel")
    assert conflict_resp.status_code == 409


@pytest.mark.asyncio
async def test_api_system_stats(async_client: AsyncClient) -> None:
    resp = await async_client.get("/api/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "queue_depth" in data
    assert "jobs_by_status" in data
    assert "active_workers" in data
    assert "total_jobs" in data


@pytest.mark.asyncio
async def test_api_health_and_readiness(async_client: AsyncClient) -> None:
    # Health probe
    h_resp = await async_client.get("/api/health")
    assert h_resp.status_code == 200
    assert h_resp.json()["status"] == "ok"

    # Readiness probe
    r_resp = await async_client.get("/api/ready")
    assert r_resp.status_code == 200
    r_data = r_resp.json()
    assert r_data["database"] == "ok"
    assert r_data["redis"] == "ok"


@pytest.mark.asyncio
async def test_api_task_registry(async_client: AsyncClient) -> None:
    resp = await async_client.get("/api/tasks")
    assert resp.status_code == 200
    tasks = resp.json()["tasks"]
    task_names = [t["name"] for t in tasks]
    assert "simulate_report" in task_names
    assert "process_text" in task_names
    assert "simulated_failure" in task_names
