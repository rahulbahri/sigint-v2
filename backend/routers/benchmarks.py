"""
routers/benchmarks.py — Benchmarks, KPI definitions, and health endpoints.
"""

from fastapi import APIRouter, HTTPException, Request

from core.database import get_db
from core.kpi_defs import KPI_DEFS, BENCHMARKS

router = APIRouter()


@router.get("/api/kpi-definitions", tags=["KPIs"])
def kpi_definitions():
    """Return all Priority-1 KPI definitions with formulas, units, and targets."""
    conn = get_db()
    targets = {r["kpi_key"]: r["target_value"] for r in conn.execute("SELECT * FROM kpi_targets").fetchall()}
    conn.close()
    return [{"target": targets.get(k["key"]), **k} for k in KPI_DEFS]


@router.get("/api/kpi-definitions/{kpi_key}", tags=["KPIs"])
def kpi_definition(kpi_key: str):
    """Return a single KPI definition by key."""
    match = next((k for k in KPI_DEFS if k["key"] == kpi_key), None)
    if not match:
        raise HTTPException(404, f"KPI '{kpi_key}' not found")
    return match


@router.get("/api/benchmarks", tags=["Analytics"])
def get_benchmarks(stage: str = "series_b"):
    """Return industry benchmark percentiles (p25/p50/p75) for the given company stage.
    Valid stages: seed, series_a, series_b, series_c.
    Source: OpenView SaaS Benchmarks, Bessemer Venture Partners, SaaS Capital."""
    valid = {"seed", "series_a", "series_b", "series_c"}
    if stage not in valid:
        stage = "series_b"
    result = {}
    for kpi_key, stages in BENCHMARKS.items():
        if stage in stages:
            result[kpi_key] = stages[stage]
    return {"stage": stage, "benchmarks": result}
