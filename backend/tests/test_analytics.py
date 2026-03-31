"""
tests/test_analytics.py — Analytics endpoints auth and response shape tests.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_fingerprint_no_auth(client: AsyncClient):
    """GET /api/fingerprint without auth → 401 (_require_workspace raises)."""
    resp = await client.get("/api/fingerprint")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_monthly_no_auth(client: AsyncClient):
    """GET /api/monthly without auth → 401 (_require_workspace raises)."""
    resp = await client.get("/api/monthly")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_fingerprint_with_auth_no_data(client: AsyncClient, other_auth_headers):
    """
    GET /api/fingerprint with auth (no data for this workspace) → 200.
    Returns a list (may be empty or contain KPI entries with empty monthly lists).
    """
    resp = await client.get("/api/fingerprint", headers=other_auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)


@pytest.mark.anyio
async def test_summary_with_auth(client: AsyncClient, auth_headers):
    """GET /api/summary with auth → 200, returns expected shape."""
    resp = await client.get("/api/summary", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "uploads" in data
    assert "kpis_tracked" in data
    assert "kpis_available" in data
    assert "months_of_data" in data
    assert "status_breakdown" in data
    sb = data["status_breakdown"]
    assert "green" in sb
    assert "yellow" in sb
    assert "red" in sb


@pytest.mark.anyio
async def test_benchmarks_no_auth(client: AsyncClient):
    """GET /api/benchmarks (no auth needed) → 200, returns dict with a 'stage' key or KPI data."""
    resp = await client.get("/api/benchmarks")
    assert resp.status_code == 200
    data = resp.json()
    # Response is a dict of KPI keys → benchmark percentiles
    assert isinstance(data, dict)
    # Should be non-empty since BENCHMARKS is populated in kpi_defs
    assert len(data) > 0
