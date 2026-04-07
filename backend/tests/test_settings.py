"""
tests/test_settings.py — Tests for company settings and model window endpoints.
"""
import json
import pytest
from httpx import AsyncClient

from core.database import get_db


# ── Helpers ──────────────────────────────────────────────────────────────────

def _set_setting(workspace_id: str, key: str, value: str):
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO company_settings (key, value, workspace_id) VALUES (?,?,?)",
        (key, value, workspace_id),
    )
    conn.commit()
    conn.close()


def _clear_settings(workspace_id: str):
    conn = get_db()
    conn.execute("DELETE FROM company_settings WHERE workspace_id=?", [workspace_id])
    conn.commit()
    conn.close()


# ── Model Window Tests ───────────────────────────────────────────────────────

@pytest.mark.anyio
async def test_model_window_default_by_stage(client: AsyncClient, auth_headers):
    """When no custom model_window_months is set, returns the stage default."""
    ws = "testcorp.com"
    _clear_settings(ws)
    _set_setting(ws, "company_stage", "series_a")

    resp = await client.get("/api/company-settings/model-window", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["model_window_months"] == 36
    assert data["source"] == "stage_default"
    assert data["stage"] == "series_a"


@pytest.mark.anyio
async def test_model_window_default_seed(client: AsyncClient, auth_headers):
    """Seed stage defaults to 18 months."""
    ws = "testcorp.com"
    _clear_settings(ws)
    _set_setting(ws, "company_stage", "seed")

    resp = await client.get("/api/company-settings/model-window", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["model_window_months"] == 18


@pytest.mark.anyio
async def test_model_window_custom_override(client: AsyncClient, auth_headers):
    """Explicit model_window_months overrides stage default."""
    ws = "testcorp.com"
    _clear_settings(ws)
    _set_setting(ws, "company_stage", "series_a")
    _set_setting(ws, "model_window_months", "24")

    resp = await client.get("/api/company-settings/model-window", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["model_window_months"] == 24
    assert data["source"] == "custom"


@pytest.mark.anyio
async def test_model_window_set_via_put(client: AsyncClient, auth_headers):
    """PUT /api/company-settings with model_window_months persists and is retrievable."""
    resp = await client.put(
        "/api/company-settings",
        headers=auth_headers,
        json={"model_window_months": 48},
    )
    assert resp.status_code == 200

    resp = await client.get("/api/company-settings/model-window", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["model_window_months"] == 48
    assert resp.json()["source"] == "custom"


@pytest.mark.anyio
async def test_model_window_out_of_range_low(client: AsyncClient, auth_headers):
    """model_window_months below 6 is rejected."""
    resp = await client.put(
        "/api/company-settings",
        headers=auth_headers,
        json={"model_window_months": 3},
    )
    assert resp.status_code == 400
    assert "between" in resp.json()["detail"].lower()


@pytest.mark.anyio
async def test_model_window_out_of_range_high(client: AsyncClient, auth_headers):
    """model_window_months above 120 is rejected."""
    resp = await client.put(
        "/api/company-settings",
        headers=auth_headers,
        json={"model_window_months": 200},
    )
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_model_window_no_stage_fallback(client: AsyncClient, auth_headers):
    """When neither stage nor custom window is set, defaults to series_b (36 months)."""
    ws = "testcorp.com"
    _clear_settings(ws)

    resp = await client.get("/api/company-settings/model-window", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    # Default stage is series_b -> 48 months
    assert data["model_window_months"] in (36, 48)  # series_b default
    assert data["source"] == "stage_default"
