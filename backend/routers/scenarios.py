"""
routers/scenarios.py — Saved scenarios CRUD, trained coefficients, and
scenario-to-forecast bridge (/api/scenarios/*).
"""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel as _BM2

from core.database import get_db
from core.deps import _get_workspace, _require_workspace

router = APIRouter()

# ── Static fallback (industry averages, mirrors ScenarioPlanner.jsx) ─────────

_STATIC_CAUSAL_MAP = {
    "gross_margin":     {"revenue_growth": 0.05, "gross_margin_adj": 1.0, "cost_reduction": 0.8},
    "operating_margin": {"revenue_growth": 0.04, "gross_margin_adj": 0.9, "headcount_delta": -0.3, "cost_reduction": 0.7},
    "ebitda_margin":    {"revenue_growth": 0.04, "gross_margin_adj": 0.85, "headcount_delta": -0.3},
    "nrr":              {"churn_adj": -0.8, "expansion_adj": 0.8},
    "arr_growth":       {"revenue_growth": 0.9, "churn_adj": -0.4},
    "churn_rate":       {"churn_adj": 1.0},
    "burn_multiple":    {"revenue_growth": -0.5, "headcount_delta": 0.4, "cost_reduction": -0.6},
    "cac_payback":      {"cac_adj": 0.7, "revenue_growth": -0.2, "gross_margin_adj": -0.3},
    "ltv_cac":          {"churn_adj": -0.5, "gross_margin_adj": 0.4, "cac_adj": -0.5},
    "dso":              {"revenue_growth": 0.1},
    "opex_ratio":       {"revenue_growth": -0.4, "headcount_delta": 0.5, "cost_reduction": -0.7},
    "headcount_eff":    {"revenue_growth": 0.6, "headcount_delta": -0.5},
    "rev_per_employee": {"revenue_growth": 0.5, "headcount_delta": -0.6},
}

# Maps lever IDs to the KPIs they most directly represent
_LEVER_TO_KPI = {
    "revenue_growth":   "revenue_growth",
    "gross_margin_adj": "gross_margin",
    "churn_adj":        "churn_rate",
    "cac_adj":          "cac_payback",
    "headcount_delta":  "headcount_eff",
    "cost_reduction":   "opex_ratio",
    "expansion_adj":    "nrr",
}


class _ScenarioSaveRequest(_BM2):
    name:        str
    levers_json: str
    notes:       str = ""


@router.get("/api/scenarios", tags=["Scenarios"])
def list_scenarios(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, levers_json, notes, created_at, updated_at "
        "FROM saved_scenarios WHERE workspace_id=? ORDER BY updated_at DESC",
        [workspace_id],
    ).fetchall()
    conn.close()
    return {"scenarios": [dict(r) for r in rows]}


@router.post("/api/scenarios", tags=["Scenarios"])
async def save_scenario(body: _ScenarioSaveRequest, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO saved_scenarios (workspace_id, name, levers_json, notes) VALUES (?,?,?,?)",
        [workspace_id, body.name.strip(), body.levers_json, body.notes],
    )
    conn.commit()
    new_id = cur.lastrowid if cur else None
    conn.close()
    return {"id": new_id, "status": "saved"}


@router.put("/api/scenarios/{scenario_id}", tags=["Scenarios"])
async def update_scenario(scenario_id: int, body: _ScenarioSaveRequest, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute(
        "UPDATE saved_scenarios SET name=?, levers_json=?, notes=?, updated_at=datetime('now') "
        "WHERE id=? AND workspace_id=?",
        [body.name.strip(), body.levers_json, body.notes, scenario_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "updated"}


@router.delete("/api/scenarios/{scenario_id}", tags=["Scenarios"])
async def delete_scenario(scenario_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute(
        "DELETE FROM saved_scenarios WHERE id=? AND workspace_id=?",
        [scenario_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "deleted"}


# ─── Trained Coefficients ────────────────────────────────────────────────────

@router.get("/api/scenarios/trained-coefficients", tags=["Scenarios"])
def trained_coefficients(request: Request):
    """
    Return causal sensitivity coefficients for the Scenario Planner.

    If a trained forecast model exists with sufficient ontology edges,
    returns company-calibrated coefficients. Otherwise returns the static
    industry-average fallback (same as the frontend CAUSAL_MAP).
    """
    workspace_id = _require_workspace(request)

    # Try to build calibrated coefficients from the trained model
    conn = get_db()
    model_row = conn.execute(
        "SELECT id, kpis, current_states, thresholds FROM markov_models "
        "WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id],
    ).fetchone()

    if not model_row:
        conn.close()
        return {
            "coefficients": _STATIC_CAUSAL_MAP,
            "calibrated": False,
            "source": "industry_average",
            "message": "No trained model found. Using industry-average coefficients.",
        }

    # Load ontology edges (causal relationships discovered during training)
    from routers.forecast import _mrk_causal_pairs, _build_deduped_causal_map

    causal_pairs = _mrk_causal_pairs()
    model_kpis = set(json.loads(model_row["kpis"]))
    causal_map = _build_deduped_causal_map(causal_pairs, model_kpis)
    conn.close()

    if not causal_map or len(causal_map) < 3:
        return {
            "coefficients": _STATIC_CAUSAL_MAP,
            "calibrated": False,
            "source": "industry_average",
            "message": "Insufficient causal relationships discovered. Using industry-average coefficients.",
        }

    # Build calibrated coefficients in CAUSAL_MAP shape: {output_kpi: {lever: coeff}}
    calibrated = {}
    output_kpis = list(_STATIC_CAUSAL_MAP.keys())

    for output_kpi in output_kpis:
        upstream = causal_map.get(output_kpi, [])
        if not upstream:
            # No edges for this KPI — use static fallback for just this row
            calibrated[output_kpi] = _STATIC_CAUSAL_MAP.get(output_kpi, {})
            continue

        lever_coeffs = {}
        for src_kpi, strength, direction in upstream:
            # Map the upstream KPI to the closest scenario lever
            lever_id = _kpi_to_lever(src_kpi)
            if lever_id is None:
                continue
            sign = -1.0 if direction == "negative" else 1.0
            coeff = round(float(strength) * sign, 4)
            # If multiple sources map to the same lever, take the strongest
            if lever_id not in lever_coeffs or abs(coeff) > abs(lever_coeffs[lever_id]):
                lever_coeffs[lever_id] = coeff

        # Merge: calibrated edges override static, but keep static for levers without edges
        static_for_kpi = _STATIC_CAUSAL_MAP.get(output_kpi, {})
        merged = {**static_for_kpi, **lever_coeffs}
        calibrated[output_kpi] = merged

    return {
        "coefficients": calibrated,
        "calibrated": True,
        "source": "trained_model",
        "edges_used": sum(len(v) for v in causal_map.values()),
        "message": f"Calibrated from {sum(len(v) for v in causal_map.values())} causal relationships.",
    }


def _kpi_to_lever(kpi_key: str) -> Optional[str]:
    """Map a KPI key to the closest scenario lever."""
    # Direct mappings
    _KPI_LEVER_MAP = {
        "revenue_growth": "revenue_growth",
        "arr_growth":     "revenue_growth",
        "gross_margin":   "gross_margin_adj",
        "churn_rate":     "churn_adj",
        "nrr":            "expansion_adj",
        "cac_payback":    "cac_adj",
        "headcount_eff":  "headcount_delta",
        "rev_per_employee":"headcount_delta",
        "opex_ratio":     "cost_reduction",
        "operating_margin":"cost_reduction",
        "burn_multiple":  "cost_reduction",
        "expansion_rate": "expansion_adj",
        "contraction_rate":"churn_adj",
        "logo_retention": "churn_adj",
        "customer_ltv":   "churn_adj",
        "ltv_cac":        "cac_adj",
        "sales_efficiency":"cac_adj",
        "marketing_roi":  "cac_adj",
    }
    return _KPI_LEVER_MAP.get(kpi_key)


# ─── Scenario → Forecast Bridge ─────────────────────────────────────────────

class _RunForecastRequest(_BM2):
    levers:       dict          # {lever_id: float_value_in_pp}
    horizon_days: int = 90
    n_samples:    int = 400


@router.post("/api/scenarios/run-forecast", tags=["Scenarios"])
async def run_forecast_from_scenario(body: _RunForecastRequest, request: Request):
    """
    Translate scenario lever adjustments into forecast overrides and run
    Monte Carlo projection. Returns probabilistic trajectories (p10/p50/p90).

    This bridges the gap between Scenario Planner (deterministic lever model)
    and Forward Signals (stochastic Monte Carlo).
    """
    workspace_id = _require_workspace(request)

    conn = get_db()
    model_row = conn.execute(
        "SELECT id, kpis, current_states, thresholds FROM markov_models "
        "WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id],
    ).fetchone()
    conn.close()

    if not model_row:
        raise HTTPException(400, "No trained forecast model. Train the model in Forward Signals first.")

    current_values = json.loads(model_row["current_states"])
    value_ranges = json.loads(model_row["thresholds"])
    model_kpis = json.loads(model_row["kpis"])

    # Translate lever values to forecast overrides
    # Override format: {kpi: state_idx} where state_idx 0-4 maps to p10/p25/p50/p75/p90
    overrides = {}
    for lever_id, lever_value in body.levers.items():
        if lever_value == 0:
            continue
        target_kpi = _LEVER_TO_KPI.get(lever_id)
        if not target_kpi or target_kpi not in value_ranges:
            continue

        # Compute the target value: current + lever adjustment
        current = current_values.get(target_kpi, 0)
        target = current + float(lever_value)

        # Map the target value to the closest percentile index (0-4)
        vr = value_ranges[target_kpi]
        percentiles = [vr.get("p10", current), vr.get("p25", current),
                       vr.get("p50", current), vr.get("p75", current),
                       vr.get("p90", current)]

        # Find the closest percentile
        best_idx = 2  # default to p50
        best_dist = abs(target - percentiles[2])
        for idx, pval in enumerate(percentiles):
            dist = abs(target - pval)
            if dist < best_dist:
                best_dist = dist
                best_idx = idx

        overrides[target_kpi] = best_idx

    # Run the existing Monte Carlo projection
    from routers.forecast import _project_scenario

    horizon = min(body.horizon_days, 730)
    n_samples = min(body.n_samples, 2000)

    result = _project_scenario(horizon, overrides, n_samples, workspace_id)

    if isinstance(result, dict) and result.get("status") == "no_model":
        raise HTTPException(400, result.get("message", "No model available"))

    return {
        "trajectories": result.get("trajectories", {}),
        "causal_paths": result.get("causal_paths", {}),
        "overrides_applied": overrides,
        "levers_received": body.levers,
        "horizon_days": horizon,
        "n_samples": n_samples,
    }
