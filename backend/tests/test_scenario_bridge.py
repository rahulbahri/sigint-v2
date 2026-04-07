"""
tests/test_scenario_bridge.py -- Tests for trained coefficients and scenario-forecast bridge.
"""
import json
import pytest
from datetime import datetime

from httpx import AsyncClient
from core.database import get_db


WS = "testcorp.com"


def _seed_monthly(workspace_id: str, n_months: int = 24):
    conn = get_db()
    y, m = 2023, 1
    for i in range(n_months):
        kpis = {
            "revenue_growth": 5.0 + i * 0.1,
            "gross_margin": 62.0 + i * 0.2,
            "operating_margin": 32.0 + i * 0.15,
            "nrr": 105.0 + (i % 6) * 0.5,
            "churn_rate": 2.5 - i * 0.02,
            "burn_multiple": 1.2 - i * 0.01,
            "arr_growth": 6.0 + i * 0.1,
            "cac_payback": 10.0 - i * 0.1,
            "ltv_cac": 4.0 + i * 0.05,
            "opex_ratio": 30.0 - i * 0.1,
            "headcount_eff": 8000 + i * 200,
            "rev_per_employee": 96000 + i * 2400,
            "dso": 35.0,
        }
        conn.execute(
            "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
            (1, y, m, json.dumps(kpis), workspace_id),
        )
        m += 1
        if m > 12:
            m = 1
            y += 1
    conn.commit()
    conn.close()


def _seed_model(workspace_id: str):
    """Seed a minimal trained model for testing."""
    conn = get_db()
    kpis = ["revenue_growth", "gross_margin", "churn_rate", "nrr", "burn_multiple",
            "operating_margin", "arr_growth", "cac_payback", "ltv_cac", "opex_ratio",
            "headcount_eff", "rev_per_employee", "dso"]
    current = {k: 60.0 for k in kpis}
    current["revenue_growth"] = 7.0
    current["churn_rate"] = 2.0
    current["nrr"] = 106.0
    vr = {}
    for k in kpis:
        c = current[k]
        vr[k] = {
            "min": c - 20, "max": c + 20,
            "p10": c - 10, "p25": c - 5, "p50": c, "p75": c + 5, "p90": c + 10,
            "current": c, "deltas": [0.1, -0.2, 0.3, -0.1, 0.2],
        }
    mean_d = {k: 0.1 for k in kpis}
    std_d = {k: 0.5 for k in kpis}

    conn.execute("DELETE FROM markov_models WHERE workspace_id=?", [workspace_id])
    conn.execute(
        "INSERT INTO markov_models (kpis, thresholds, self_matrices, cross_matrices, "
        "current_states, upstream_kpis, days_back, trained_at, workspace_id) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        (json.dumps(kpis), json.dumps(vr), json.dumps(mean_d), json.dumps(std_d),
         json.dumps(current), json.dumps(kpis[:5]), 365, datetime.utcnow().isoformat(),
         workspace_id),
    )
    conn.commit()
    conn.close()


def _clear(workspace_id: str):
    conn = get_db()
    conn.execute("DELETE FROM monthly_data WHERE workspace_id=?", [workspace_id])
    conn.execute("DELETE FROM markov_models WHERE workspace_id=?", [workspace_id])
    conn.commit()
    conn.close()


# ── Trained Coefficients Tests ───────────────────────────────────────────────

@pytest.mark.anyio
async def test_trained_coefficients_no_model(client: AsyncClient, auth_headers):
    """Without a trained model, returns static industry averages."""
    _clear(WS)
    resp = await client.get("/api/scenarios/trained-coefficients", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert data["calibrated"] is False
    assert data["source"] == "industry_average"
    assert "gross_margin" in data["coefficients"]
    assert "revenue_growth" in data["coefficients"]["gross_margin"]


@pytest.mark.anyio
async def test_trained_coefficients_with_model(client: AsyncClient, auth_headers):
    """With a trained model, returns coefficients (calibrated or merged)."""
    _clear(WS)
    _seed_monthly(WS, 24)
    _seed_model(WS)

    resp = await client.get("/api/scenarios/trained-coefficients", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    # Should have coefficients regardless of calibration status
    assert "coefficients" in data
    assert isinstance(data["coefficients"], dict)
    # Must match CAUSAL_MAP shape: output KPIs as keys
    assert "gross_margin" in data["coefficients"]
    assert "burn_multiple" in data["coefficients"]

    _clear(WS)


@pytest.mark.anyio
async def test_trained_coefficients_shape(client: AsyncClient, auth_headers):
    """Output matches CAUSAL_MAP structure: {kpi: {lever: float}}."""
    _clear(WS)
    resp = await client.get("/api/scenarios/trained-coefficients", headers=auth_headers)
    data = resp.json()
    for kpi_key, lever_map in data["coefficients"].items():
        assert isinstance(lever_map, dict), f"{kpi_key} should map to dict of levers"
        for lever_id, coeff in lever_map.items():
            assert isinstance(coeff, (int, float)), f"{kpi_key}.{lever_id} should be numeric"


@pytest.mark.anyio
async def test_trained_coefficients_requires_auth(client: AsyncClient):
    """GET without auth returns 401."""
    resp = await client.get("/api/scenarios/trained-coefficients")
    assert resp.status_code == 401


# ── Scenario-Forecast Bridge Tests ──────────────────────────────────────────

@pytest.mark.anyio
async def test_run_forecast_no_model(client: AsyncClient, auth_headers):
    """POST without trained model returns 400."""
    _clear(WS)
    resp = await client.post(
        "/api/scenarios/run-forecast",
        headers=auth_headers,
        json={"levers": {"revenue_growth": 5}, "horizon_days": 90, "n_samples": 50},
    )
    assert resp.status_code == 400


@pytest.mark.anyio
async def test_run_forecast_with_model(client: AsyncClient, auth_headers):
    """POST with trained model returns trajectories."""
    _clear(WS)
    _seed_monthly(WS, 24)
    _seed_model(WS)

    resp = await client.post(
        "/api/scenarios/run-forecast",
        headers=auth_headers,
        json={"levers": {"revenue_growth": 5, "churn_adj": -1}, "horizon_days": 90, "n_samples": 50},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "trajectories" in data
    assert "overrides_applied" in data
    assert isinstance(data["trajectories"], dict)
    # Should have applied at least one override
    assert len(data["overrides_applied"]) > 0

    _clear(WS)


@pytest.mark.anyio
async def test_run_forecast_zero_levers_no_overrides(client: AsyncClient, auth_headers):
    """Zero-value levers produce no overrides (baseline projection)."""
    _clear(WS)
    _seed_monthly(WS, 24)
    _seed_model(WS)

    resp = await client.post(
        "/api/scenarios/run-forecast",
        headers=auth_headers,
        json={"levers": {"revenue_growth": 0, "churn_adj": 0}, "horizon_days": 90, "n_samples": 50},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["overrides_applied"]) == 0

    _clear(WS)


@pytest.mark.anyio
async def test_run_forecast_returns_levers_received(client: AsyncClient, auth_headers):
    """Response includes the original levers for transparency."""
    _clear(WS)
    _seed_monthly(WS, 24)
    _seed_model(WS)

    levers = {"revenue_growth": 3, "gross_margin_adj": 2}
    resp = await client.post(
        "/api/scenarios/run-forecast",
        headers=auth_headers,
        json={"levers": levers, "horizon_days": 60, "n_samples": 50},
    )
    assert resp.status_code == 200
    assert resp.json()["levers_received"] == levers
    assert resp.json()["horizon_days"] == 60

    _clear(WS)
