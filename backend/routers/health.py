"""
routers/health.py — Health Score endpoint + Home Screen data aggregation.
GET /api/health-score
GET /api/home
GET /api/kpi-detail/{kpi_key}
"""
import json
from typing import Optional

from fastapi import APIRouter, Query, Request

from core.database import get_db
from core.deps import _get_workspace
from core.health_score import compute_health_score, _is_on_target, _is_critical, _gap_pct
from core.intelligence import (
    benchmark_position,
    streak_detection,
    domain_narratives,
    period_comparison,
    stage_aware_actions,
    compute_kpi_correlations,
    decision_check_ins,
    _normalise_stage,
)
from core.kpi_defs import KPI_DEFS, ALL_CAUSATION_RULES, BENCHMARKS

router = APIRouter()


def _parse_weight_and_period_params(
    w_momentum: Optional[float],
    w_target: Optional[float],
    w_risk: Optional[float],
    from_year: Optional[int],
    from_month: Optional[int],
    to_year: Optional[int],
    to_month: Optional[int],
):
    """Return kwargs dict for compute_health_score from optional query params."""
    kwargs = {}
    if w_momentum is not None:
        kwargs["w_momentum"] = w_momentum
    if w_target is not None:
        kwargs["w_target"] = w_target
    if w_risk is not None:
        kwargs["w_risk"] = w_risk
    if from_year is not None and from_month is not None:
        kwargs["from_period"] = (from_year, from_month)
    if to_year is not None and to_month is not None:
        kwargs["to_period"] = (to_year, to_month)
    return kwargs


@router.get("/api/health-score", tags=["Intelligence"])
def get_health_score(
    request: Request,
    w_momentum: Optional[float] = Query(None),
    w_target: Optional[float] = Query(None),
    w_risk: Optional[float] = Query(None),
    from_year: Optional[int] = Query(None),
    from_month: Optional[int] = Query(None),
    to_year: Optional[int] = Query(None),
    to_month: Optional[int] = Query(None),
):
    """Return the workspace health score with full breakdown."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        return {
            "score": 0, "grade": "—", "label": "No Data", "color": "grey",
            "momentum": 0, "target_achievement": 0, "risk_flags": 0,
            "kpis_green": 0, "kpis_yellow": 0, "kpis_red": 0, "kpis_grey": 0,
            "months_of_data": 0, "needs_attention": [], "doing_well": [],
            "momentum_trend": "stable",
        }
    kwargs = _parse_weight_and_period_params(
        w_momentum, w_target, w_risk, from_year, from_month, to_year, to_month
    )
    conn = get_db()
    result = compute_health_score(conn, workspace_id, **kwargs)
    conn.close()
    return result


@router.get("/api/home", tags=["Intelligence"])
def get_home(
    request: Request,
    w_momentum: Optional[float] = Query(None),
    w_target: Optional[float] = Query(None),
    w_risk: Optional[float] = Query(None),
    from_year: Optional[int] = Query(None),
    from_month: Optional[int] = Query(None),
    to_year: Optional[int] = Query(None),
    to_month: Optional[int] = Query(None),
    cw_gap: Optional[float] = Query(None, description="Criticality weight: Gap Severity (0-1)"),
    cw_trend: Optional[float] = Query(None, description="Criticality weight: Trend Momentum (0-1)"),
    cw_impact: Optional[float] = Query(None, description="Criticality weight: Business Impact (0-1)"),
    cw_domain: Optional[float] = Query(None, description="Criticality weight: Domain Urgency (0-1)"),
):
    """
    Aggregated home-screen payload: health score + recent brief + spotlight KPIs.
    Used by the HomeScreen component to avoid multiple round trips.
    """
    workspace_id = _get_workspace(request)
    conn = get_db()

    kwargs = _parse_weight_and_period_params(
        w_momentum, w_target, w_risk, from_year, from_month, to_year, to_month
    )

    # Build criticality weights if any were provided
    if any(v is not None for v in [cw_gap, cw_trend, cw_impact, cw_domain]):
        crit_w = {}
        if cw_gap is not None:    crit_w["gap"]    = cw_gap
        if cw_trend is not None:  crit_w["trend"]  = cw_trend
        if cw_impact is not None: crit_w["impact"] = cw_impact
        if cw_domain is not None: crit_w["domain"] = cw_domain
        kwargs["criticality_weights"] = crit_w

    health = compute_health_score(conn, workspace_id, **kwargs)

    has_period_filter = from_year is not None or to_year is not None

    # Data period (min/max year-month) for "last updated" display
    period_row = conn.execute(
        "SELECT MIN(year*100+month) as mn, MAX(year*100+month) as mx FROM monthly_data WHERE workspace_id=?",
        [workspace_id]
    ).fetchone()
    latest_upload = conn.execute(
        "SELECT uploaded_at FROM uploads WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id]
    ).fetchone()

    def _ym(val):
        if not val: return None
        yr, mo = divmod(int(val), 100)
        return f"{yr}-{mo:02d}"

    data_period = {
        "from": _ym(period_row["mn"]) if period_row and period_row["mn"] else None,
        "to":   _ym(period_row["mx"]) if period_row and period_row["mx"] else None,
        "uploaded_at": latest_upload["uploaded_at"] if latest_upload else None,
    }

    # Pull monthly data for recent trend sparklines
    if has_period_filter:
        spark_query = "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=?"
        spark_params: list = [workspace_id]
        if from_year is not None and from_month is not None:
            spark_query += " AND (year > ? OR (year = ? AND month >= ?))"
            spark_params.extend([from_year, from_year, from_month])
        if to_year is not None and to_month is not None:
            spark_query += " AND (year < ? OR (year = ? AND month <= ?))"
            spark_params.extend([to_year, to_year, to_month])
        spark_query += " ORDER BY year DESC, month DESC"
        rows = conn.execute(spark_query, spark_params).fetchall()
    else:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 6",
            [workspace_id]
        ).fetchall()
    targets_rows = conn.execute(
        "SELECT kpi_key, target_value, direction, unit FROM kpi_targets WHERE workspace_id=?",
        [workspace_id]
    ).fetchall()
    targets_map = {r["kpi_key"]: {"target": r["target_value"], "direction": r["direction"] or "higher", "unit": r["unit"] or ""} for r in targets_rows}

    # Build spotlight data for needs_attention and doing_well KPIs
    kpi_monthly: dict = {}
    for row in rows:
        d = json.loads(row["data_json"])
        period = f"{row['year']}-{row['month']:02d}"
        for k, v in d.items():
            if v is not None and k not in ("year", "month"):
                kpi_monthly.setdefault(k, []).append({"period": period, "value": v})

    def _kpi_spotlight(key: str) -> dict:
        t  = targets_map.get(key, {})
        mo = sorted(kpi_monthly.get(key, []), key=lambda x: x["period"])
        vals = [m["value"] for m in mo]
        avg  = round(sum(vals) / len(vals), 2) if vals else None
        return {
            "key":       key,
            "target":    t.get("target"),
            "direction": t.get("direction", "higher"),
            "unit":      t.get("unit", ""),
            "avg":       avg,
            "sparkline": [m["value"] for m in mo[-6:]],
        }

    # ── Company stage (for benchmark positioning) ──────────────────────────
    try:
        stage_row = conn.execute(
            "SELECT value FROM company_settings WHERE workspace_id=? AND key='funding_stage'",
            [workspace_id],
        ).fetchone()
        company_stage = _normalise_stage(stage_row["value"] if stage_row else None)
    except Exception:
        company_stage = "series_a"

    # P3.2: Fetch active decisions for check-in detection
    try:
        decision_rows = conn.execute(
            "SELECT id, title, status, kpi_context, decided_at FROM decisions WHERE workspace_id=? AND status='active'",
            [workspace_id],
        ).fetchall()
        raw_decisions = [dict(r) for r in decision_rows]
    except Exception:
        raw_decisions = []

    conn.close()

    # ── Composite criticality ranking ─────────────────────────────────────────
    composite_ranked = health.get("composite_ranked", [])
    composite_map = {r["key"]: r for r in composite_ranked}

    # ── Enrich needs_attention ────────────────────────────────────────────────
    ranked_map = {r["key"]: r for r in health.get("needs_attention_ranked", [])}
    needs_enriched = []
    for k in health["needs_attention"]:
        spot = _kpi_spotlight(k)
        # Legacy gap data
        r = ranked_map.get(k)
        if r:
            spot["gap_pct"] = r["gap_pct"]
        # Composite criticality data
        cr = composite_map.get(k)
        if cr:
            spot["composite"]    = cr["composite"]
            spot["gap_score"]    = cr["gap_score"]
            spot["trend_score"]  = cr["trend_score"]
            spot["impact_score"] = cr["impact_score"]
            spot["domain_score"] = cr["domain_score"]
            spot["domain"]       = cr["domain"]
            spot["domain_label"] = cr["domain_label"]

        # P1.1: Benchmark positioning
        bench = benchmark_position(
            k, spot.get("avg"), spot.get("direction", "higher"), company_stage,
        )
        if bench:
            spot["benchmark"] = bench

        # P1.2: Consecutive-month streak
        monthly_sorted = sorted(
            kpi_monthly.get(k, []), key=lambda x: x["period"],
        )
        t = targets_map.get(k, {})
        streak = streak_detection(
            k, monthly_sorted, t.get("target"), t.get("direction", "higher"),
        )
        spot["miss_streak"]   = streak["miss_streak"]
        spot["streak_label"]  = streak["streak_label"]
        spot["is_structural"] = streak["is_structural"]

        needs_enriched.append(spot)

    # Re-sort by composite score (most critical first)
    needs_enriched.sort(key=lambda x: x.get("composite", 0), reverse=True)
    for idx, item in enumerate(needs_enriched):
        item["rank"] = idx + 1

    # ── Enrich doing_well ─────────────────────────────────────────────────────
    doing_well_enriched = []
    for k in health["doing_well"]:
        spot = _kpi_spotlight(k)
        cr = composite_map.get(k)
        if cr:
            spot["domain"]       = cr["domain"]
            spot["domain_label"] = cr["domain_label"]
        bench = benchmark_position(
            k, spot.get("avg"), spot.get("direction", "higher"), company_stage,
        )
        if bench:
            spot["benchmark"] = bench
        doing_well_enriched.append(spot)

    # ── P1.3: Domain-level narratives ─────────────────────────────────────────
    domain_groups = health.get("domain_groups", [])
    total_red = len(health.get("needs_attention", []))
    domain_narrs = domain_narratives(domain_groups, total_red)

    # ── P1.4: Period-over-period comparison ───────────────────────────────────
    period_delta = period_comparison(kpi_monthly, targets_map)

    # ── P3.2: Decision check-ins (30-day reminders) ──────────────────────────
    from datetime import datetime as _dt
    check_ins = decision_check_ins(
        raw_decisions,
        [k for k in health.get("needs_attention", [])],
        _dt.utcnow().isoformat(),
    )

    return {
        "health":          health,
        "data_period":     data_period,
        "needs_attention": needs_enriched,
        "doing_well":      doing_well_enriched,
        "domain_groups":   domain_groups,
        "domain_narratives": domain_narrs,
        "period_comparison": period_delta,
        "decision_check_ins": check_ins,
        "company_stage":   company_stage,
        "composite_methodology": {
            "signals": [
                {"key": "gap",    "label": "Gap Severity",    "weight": 25, "desc": "How far the KPI is from its target"},
                {"key": "trend",  "label": "Trend Momentum",  "weight": 25, "desc": "Rate of deterioration or improvement over recent months"},
                {"key": "impact", "label": "Business Impact",  "weight": 30, "desc": "Downstream causal effect on other KPIs (from the causal graph)"},
                {"key": "domain", "label": "Domain Urgency",  "weight": 20, "desc": "Business-area survival tier (Cash > Revenue > Retention > Profitability > Efficiency)"},
            ],
            "description": "Composite Criticality Score = (Gap × 25%) + (Trend × 25%) + (Impact × 30%) + (Domain × 20%). Higher score = more critical. This multi-signal approach prevents ranking KPIs solely by distance from target, and instead surfaces metrics with outsized business impact.",
        },
    }


# ─── KPI Detail ──────────────────────────────────────────────────────────────

# Build lookup dicts once at import time
_KPI_DEFS_MAP = {d["key"]: d for d in KPI_DEFS}

# Data requirements: what raw data columns are needed to compute each KPI
DATA_REQUIREMENTS = {
    "revenue_growth":        ["Monthly revenue figures"],
    "gross_margin":          ["Monthly revenue", "Cost of goods sold (COGS)"],
    "operating_margin":      ["Monthly revenue", "COGS", "Operating expenses (OpEx)"],
    "ebitda_margin":         ["EBITDA or (Revenue, COGS, OpEx, Depreciation & Amortization)"],
    "cash_conv_cycle":       ["Days Sales Outstanding (DSO)", "Days Inventory Outstanding (DIO)", "Days Payable Outstanding (DPO)"],
    "dso":                   ["Accounts receivable (AR)", "Monthly revenue"],
    "ar_turnover":           ["Net credit sales", "Average accounts receivable"],
    "avg_collection_period": ["AR Turnover Ratio (or data to compute it)"],
    "cei":                   ["Beginning AR", "Monthly sales", "Ending AR", "Current AR"],
    "ar_aging_current":      ["Current AR (0-30 days)", "Total AR"],
    "ar_aging_overdue":      ["Overdue AR (30+ days)", "Total AR"],
    "billable_utilization":  ["Billable hours", "Total available hours"],
    "arr_growth":            ["Monthly or annual recurring revenue (ARR)"],
    "nrr":                   ["Starting MRR", "Expansion revenue", "Churned revenue", "Contraction revenue"],
    "burn_multiple":         ["Net burn (cash outflow)", "Net new ARR"],
    "opex_ratio":            ["Operating expenses", "Monthly revenue"],
    "contribution_margin":   ["Revenue", "COGS", "Variable costs"],
    "revenue_quality":       ["Recurring revenue", "Total revenue"],
    "cac_payback":           ["Customer acquisition cost (CAC)", "ARPU", "Gross margin percentage"],
    "sales_efficiency":      ["New ARR", "Sales & marketing spend"],
    "customer_concentration":["Revenue by customer (top customer revenue, total revenue)"],
    "recurring_revenue":     ["Recurring revenue", "Total revenue"],
    "churn_rate":            ["Lost customers per month", "Total customers"],
    "operating_leverage":    ["Month-over-month change in operating income", "Month-over-month change in revenue"],
    "growth_efficiency":     ["ARR growth rate", "Burn multiple"],
    "revenue_momentum":      ["Current month revenue growth", "Annual average revenue growth"],
    "revenue_fragility":     ["Customer concentration", "Churn rate", "Net revenue retention (NRR)"],
    "burn_convexity":        ["Month-over-month burn multiple figures"],
    "margin_volatility":     ["6 months of gross margin data"],
    "pipeline_conversion":   ["MQL count", "Closed-won deal count"],
    "customer_decay_slope":  ["Month-over-month churn rate figures"],
    "customer_ltv":          ["ARPU", "Gross margin percentage", "Monthly churn rate"],
    "pricing_power_index":   ["Month-over-month ARPU change", "Month-over-month customer volume change"],
}


def _build_causal_chain(
    kpi_key: str, depth: int = 0, max_depth: int = 3, visited: Optional[set] = None
) -> dict:
    """
    Recursively walk ALL_CAUSATION_RULES to build a multi-hop cause-effect tree.
    Returns a dict with node, hop, root_causes, and children list.
    """
    if visited is None:
        visited = set()

    causation = ALL_CAUSATION_RULES.get(kpi_key, {})
    node = {
        "node": kpi_key,
        "hop": depth,
        "root_causes": causation.get("root_causes", []),
        "children": [],
    }

    if depth >= max_depth or kpi_key in visited:
        return node

    visited.add(kpi_key)

    for downstream_key in causation.get("downstream_impact", []):
        child = _build_causal_chain(downstream_key, depth + 1, max_depth, visited.copy())
        node["children"].append(child)

    return node


@router.get("/api/kpi-detail/{kpi_key}", tags=["Intelligence"])
def get_kpi_detail(kpi_key: str, request: Request):
    """
    Return rich detail for a single KPI: definition, causation rules,
    benchmarks, monthly time series, target, and current status.
    """
    workspace_id = _get_workspace(request)
    conn = get_db()

    # KPI definition
    kpi_def = _KPI_DEFS_MAP.get(kpi_key, {})

    # Causation rules
    causation = ALL_CAUSATION_RULES.get(kpi_key, {})

    # Benchmarks
    benchmark = BENCHMARKS.get(kpi_key, {})

    # Full monthly time series
    rows = conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month",
        [workspace_id],
    ).fetchall()

    time_series = []
    for row in rows:
        d = json.loads(row["data_json"])
        val = d.get(kpi_key)
        if val is not None:
            time_series.append({
                "year": row["year"],
                "month": row["month"],
                "period": f"{row['year']}-{row['month']:02d}",
                "value": val,
            })

    # Target and direction
    target_row = conn.execute(
        "SELECT target_value, direction, unit FROM kpi_targets WHERE workspace_id=? AND kpi_key=?",
        [workspace_id, kpi_key],
    ).fetchone()

    target_value = target_row["target_value"] if target_row else None
    direction = target_row["direction"] if target_row else kpi_def.get("direction", "higher")
    unit = target_row["unit"] if target_row else kpi_def.get("unit", "")

    # Compute current status (green / yellow / red / grey) using direction-aware helpers
    status = "grey"
    pct_of_target = None
    if time_series and target_value is not None:
        recent_vals = [pt["value"] for pt in time_series[-3:]]
        avg = sum(recent_vals) / len(recent_vals)
        gap = _gap_pct(avg, target_value, direction)
        pct_of_target = round(gap * 100, 1)
        if _is_on_target(avg, target_value, direction):
            status = "green"
        elif _is_critical(avg, target_value, direction):
            status = "red"
        else:
            status = "yellow"

    # Direction guidance
    direction_label = "Higher is better" if direction == "higher" else "Lower is better"

    # Causal chain (multi-hop)
    causal_chain = _build_causal_chain(kpi_key)

    # Typical range from benchmarks (company stage, falling back to series_a)
    try:
        stage_row = conn.execute(
            "SELECT value FROM company_settings WHERE workspace_id=? AND key='funding_stage'",
            [workspace_id],
        ).fetchone()
        company_stage = _normalise_stage(stage_row["value"] if stage_row else None)
    except Exception:
        company_stage = "series_a"

    typical_range = None
    stage_bench = benchmark.get(company_stage) or benchmark.get("series_a")
    if stage_bench and "p25" in stage_bench and "p75" in stage_bench:
        typical_range = {"low": stage_bench["p25"], "high": stage_bench["p75"]}

    # Stage-aware corrective actions
    base_actions = causation.get("corrective_actions", [])
    actions = stage_aware_actions(kpi_key, base_actions, company_stage)

    # Benchmark positioning
    bench_pos = None
    if time_series and target_value is not None:
        recent_avg = sum(pt["value"] for pt in time_series[-3:]) / min(len(time_series), 3)
        bench_pos = benchmark_position(kpi_key, recent_avg, direction, company_stage)

    # Data-driven correlations
    kpi_monthly_all: dict = {}
    for row in conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month",
        [workspace_id],
    ).fetchall():
        d = json.loads(row["data_json"])
        period = f"{row['year']}-{row['month']:02d}"
        for k, v in d.items():
            if v is not None and k not in ("year", "month"):
                kpi_monthly_all.setdefault(k, []).append({"period": period, "value": v})

    correlations = compute_kpi_correlations(kpi_monthly_all, kpi_key)

    # Data requirements
    data_requirements = DATA_REQUIREMENTS.get(kpi_key)

    conn.close()

    return {
        "kpi_key":        kpi_key,
        "name":           kpi_def.get("name", kpi_key),
        "formula":        kpi_def.get("formula", ""),
        "unit":           unit,
        "direction":      direction,
        "direction_label": direction_label,
        "domain":         kpi_def.get("domain", ""),
        "target":         target_value,
        "pct_of_target":  pct_of_target,
        "status":         status,
        "time_series":    time_series,
        "root_causes":       causation.get("root_causes", []),
        "downstream_impact": causation.get("downstream_impact", []),
        "corrective_actions": actions,
        "benchmarks":        benchmark,
        "benchmark_position": bench_pos,
        "causal_chain":      causal_chain,
        "correlations":      correlations,
        "typical_range":     typical_range,
        "data_requirements": data_requirements,
    }
