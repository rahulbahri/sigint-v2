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

    conn.close()

    # Enrich needs_attention with ranking data (gap_pct, rank)
    ranked_map = {r["key"]: r for r in health.get("needs_attention_ranked", [])}
    needs_enriched = []
    for k in health["needs_attention"]:
        spot = _kpi_spotlight(k)
        r = ranked_map.get(k)
        if r:
            spot["gap_pct"] = r["gap_pct"]
            spot["rank"]    = r["rank"]
        needs_enriched.append(spot)

    return {
        "health":          health,
        "data_period":     data_period,
        "needs_attention": needs_enriched,
        "doing_well":      [_kpi_spotlight(k) for k in health["doing_well"]],
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

    conn.close()

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

    # Typical range from benchmarks (series_a p25–p75)
    typical_range = None
    series_a_bench = benchmark.get("series_a")
    if series_a_bench and "p25" in series_a_bench and "p75" in series_a_bench:
        typical_range = {"low": series_a_bench["p25"], "high": series_a_bench["p75"]}

    # Data requirements
    data_requirements = DATA_REQUIREMENTS.get(kpi_key)

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
        "corrective_actions":causation.get("corrective_actions", []),
        "benchmarks":        benchmark,
        "causal_chain":      causal_chain,
        "typical_range":     typical_range,
        "data_requirements": data_requirements,
    }
