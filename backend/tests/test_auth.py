"""
tests/test_auth.py — Auth flow integration tests.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_health_no_auth(client: AsyncClient):
    """GET /api/health → 200 without any auth."""
    resp = await client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


@pytest.mark.anyio
async def test_me_no_token(client: AsyncClient):
    """GET /api/auth/me → 401 without a token."""
    resp = await client.get("/api/auth/me")
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_me_valid_token(client: AsyncClient, auth_headers):
    """GET /api/auth/me → 200 with valid token, returns correct email."""
    resp = await client.get("/api/auth/me", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["email"] == "alice@testcorp.com"
    assert data["valid"] is True


@pytest.mark.anyio
async def test_me_expired_token(client: AsyncClient, expired_headers):
    """GET /api/auth/me → 401 with expired token."""
    resp = await client.get("/api/auth/me", headers=expired_headers)
    assert resp.status_code == 401


@pytest.mark.anyio
async def test_request_link_free_email(client: AsyncClient):
    """POST /api/auth/request-link with a free email (gmail.com) → 403."""
    resp = await client.post(
        "/api/auth/request-link",
        json={"email": "user@gmail.com"},
    )
    assert resp.status_code == 403


@pytest.mark.anyio
async def test_verify_nonexistent_token(client: AsyncClient):
    """POST /api/auth/verify with a non-existent token → 401."""
    resp = await client.post(
        "/api/auth/verify",
        json={"token": "this-token-does-not-exist-at-all-abc123"},
    )
    assert resp.status_code == 401
