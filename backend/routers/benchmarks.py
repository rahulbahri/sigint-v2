"""
routers/benchmarks.py — Benchmarks, KPI definitions, and health endpoints.
"""

from fastapi import APIRouter, HTTPException, Request

from core.database import get_db
from core.deps import _get_workspace
from core.kpi_defs import KPI_DEFS, BENCHMARKS

router = APIRouter()


@router.get("/api/kpi-definitions", tags=["KPIs"])
def kpi_definitions(request: Request):
    """Return all Priority-1 KPI definitions with formulas, units, and targets."""
    workspace_id = _get_workspace(request)
    conn = get_db()
    targets = {r["kpi_key"]: r["target_value"] for r in conn.execute(
        "SELECT kpi_key, target_value FROM kpi_targets WHERE workspace_id=?", [workspace_id]
    ).fetchall()}
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

    Data sourced from Tier-1 curated public survey data. Sources are disclosed per-KPI.
    Primary sources:
      1. SaaS Capital — Annual SaaS Growth Survey (saas-capital.com/research)
      2. OpenView Partners — SaaS Benchmarks Report (openviewpartners.com/benchmarks)
      3. KeyBanc Capital Markets — Private SaaS Company Survey
      4. Bessemer Venture Partners — State of the Cloud Report (bvp.com/atlas)
      5. Insight Partners — SaaS Metrics Report
      6. Andreessen Horowitz — a16z SaaS Benchmarks
      7. ChartMogul — SaaS Growth Report (chartmogul.com/resources)
      8. Paddle / ProfitWell — SaaS Retention Report
    """
    valid = {"seed", "series_a", "series_b", "series_c"}
    if stage not in valid:
        stage = "series_b"
    result = {}
    for kpi_key, stages in BENCHMARKS.items():
        if stage in stages:
            result[kpi_key] = stages[stage]
    return {"stage": stage, "benchmarks": result}


@router.get("/api/benchmarks/sources", tags=["Analytics"])
def get_benchmark_sources():
    """Return the curated list of benchmark data sources with links and descriptions."""
    return {
        "sources": [
            {
                "id": "saas_capital",
                "name": "SaaS Capital",
                "description": "Annual SaaS Growth Survey — growth, retention, and efficiency metrics across 1,500+ private SaaS companies",
                "url": "https://www.saas-capital.com/research/",
                "cadence": "Annual",
                "kpis": ["arr_growth", "nrr", "gross_margin", "burn_multiple"],
            },
            {
                "id": "openview",
                "name": "OpenView Partners",
                "description": "SaaS Benchmarks Report — product-led and sales-led growth metrics",
                "url": "https://openviewpartners.com/saas-benchmarks-report/",
                "cadence": "Annual",
                "kpis": ["cac_payback", "ltv_cac", "churn_rate", "revenue_growth"],
            },
            {
                "id": "keybanc",
                "name": "KeyBanc Capital Markets",
                "description": "Private SaaS Company Survey — deep-dive on unit economics and go-to-market efficiency",
                "url": "https://kbcm.com/saas-survey/",
                "cadence": "Annual",
                "kpis": ["gross_margin", "operating_margin", "arr_growth", "burn_multiple"],
            },
            {
                "id": "bessemer",
                "name": "Bessemer Venture Partners",
                "description": "State of the Cloud — global SaaS leader benchmarks and rule-of-40 analysis",
                "url": "https://www.bvp.com/atlas/state-of-the-cloud",
                "cadence": "Annual",
                "kpis": ["arr_growth", "gross_margin", "ebitda_margin", "ltv_cac"],
            },
            {
                "id": "insight",
                "name": "Insight Partners",
                "description": "SaaS Metrics That Matter — scaling benchmarks from seed through IPO",
                "url": "https://www.insightpartners.com/",
                "cadence": "Annual",
                "kpis": ["nrr", "churn_rate", "cac_payback"],
            },
            {
                "id": "a16z",
                "name": "Andreessen Horowitz (a16z)",
                "description": "SaaS efficiency and growth benchmarks from portfolio analysis",
                "url": "https://a16z.com/",
                "cadence": "Periodic",
                "kpis": ["burn_multiple", "revenue_growth", "gross_margin"],
            },
            {
                "id": "chartmogul",
                "name": "ChartMogul",
                "description": "SaaS Growth Report — subscription metrics from 2,100+ SaaS companies",
                "url": "https://chartmogul.com/resources/saas-growth-report/",
                "cadence": "Annual",
                "kpis": ["arr_growth", "churn_rate", "nrr"],
            },
            {
                "id": "profitwell",
                "name": "Paddle / ProfitWell",
                "description": "SaaS Retention Report — churn and expansion benchmarks",
                "url": "https://www.paddle.com/resources/profitwell-blog/churn-benchmarks",
                "cadence": "Annual",
                "kpis": ["churn_rate", "nrr", "logo_retention"],
            },
        ],
        "disclaimer": (
            "All benchmarks are sourced from publicly available industry surveys and reports. "
            "Each benchmark cites its source for full transparency. Benchmarks represent "
            "median or percentile ranges from survey cohorts and should be used as directional "
            "context, not precise targets. Company-specific factors may cause material deviation."
        ),
    }
