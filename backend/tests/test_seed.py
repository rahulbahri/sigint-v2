"""
tests/test_seed.py — Seed endpoint auth and basic response tests.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_seed_demo_no_auth(client: AsyncClient):
    """GET /api/seed-demo without auth → 401."""
    resp = await client.get("/api/seed-demo")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_seed_demo_projection_no_auth(client: AsyncClient):
    """GET /api/seed-demo-projection without auth → 401."""
    resp = await client.get("/api/seed-demo-projection")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_seed_multiyear_no_auth(client: AsyncClient):
    """GET /api/seed-multiyear without auth → 401."""
    resp = await client.get("/api/seed-multiyear")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_seed_demo_with_auth(client: AsyncClient, auth_headers):
    """GET /api/seed-demo with auth → 200, seeds data scoped to workspace."""
    resp = await client.get("/api/seed-demo", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data.get("seeded") is True
    assert data.get("months") == 12
    assert data.get("transactions") > 0
    assert "upload_id" in data
