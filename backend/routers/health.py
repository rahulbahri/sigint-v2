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
from core.deps import _require_workspace
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
from core.kpi_defs import KPI_DEFS, EXTENDED_ONTOLOGY_METRICS, ALL_CAUSATION_RULES, BENCHMARKS

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
    workspace_id = _require_workspace(request)
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
    workspace_id = _require_workspace(request)
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

    # Load ontology edges for Granger-weighted criticality scoring
    _ont_edges_tuples = {}
    try:
        _edge_rows = conn.execute(
            "SELECT source, target, granger_pval, confidence_tier, strength, direction "
            "FROM ontology_edges"
        ).fetchall()
        for _er in _edge_rows:
            _src = _er["source"] if isinstance(_er, dict) else _er[0]
            _tgt = _er["target"] if isinstance(_er, dict) else _er[1]
            _ont_edges_tuples[(_src, _tgt)] = {
                "granger_pval": _er["granger_pval"] if isinstance(_er, dict) else _er[2],
                "confidence_tier": _er["confidence_tier"] if isinstance(_er, dict) else _er[3],
                "strength": _er["strength"] if isinstance(_er, dict) else _er[4],
                "direction": (_er["direction"] if isinstance(_er, dict) else _er[5]) if len(_er) > 5 else "positive",
            }
    except Exception:
        pass
    kwargs["ontology_edges"] = _ont_edges_tuples

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
            "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 12",
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
            if k.startswith("_") or k in ("year", "month") or v is None:
                continue
            kpi_monthly.setdefault(k, []).append({"period": period, "value": v})

    def _kpi_spotlight(key: str) -> dict:
        t  = targets_map.get(key, {})
        kd = _KPI_DEFS_MAP.get(key, {})  # Fallback to kpi_defs for unit/direction
        mo = sorted(kpi_monthly.get(key, []), key=lambda x: x["period"])
        # Use last 6 months for average — matches health_score.py kpi_avgs window
        recent_vals = [m["value"] for m in mo[-6:]]
        avg  = round(sum(recent_vals) / len(recent_vals), 2) if recent_vals else None
        return {
            "key":       key,
            "target":    t.get("target"),
            "direction": t.get("direction") or kd.get("direction", "higher"),
            "unit":      t.get("unit") or kd.get("unit", ""),
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

    # Load ontology edges for Granger-weighted narrative engine
    ontology_edges_map = {}
    ontology_edges_tuples = {}  # {(src, tgt): {...}} for criticality + validation
    try:
        edge_rows = conn.execute(
            "SELECT source, target, granger_pval, confidence_tier, strength, direction "
            "FROM ontology_edges"
        ).fetchall()
        for er in edge_rows:
            src = er["source"] if isinstance(er, dict) else er[0]
            tgt = er["target"] if isinstance(er, dict) else er[1]
            edge_data = {
                "granger_pval": er["granger_pval"] if isinstance(er, dict) else er[2],
                "confidence_tier": er["confidence_tier"] if isinstance(er, dict) else er[3],
                "strength": er["strength"] if isinstance(er, dict) else er[4],
                "direction": (er["direction"] if isinstance(er, dict) else er[5]) if len(er) > 5 else "positive",
            }
            ontology_edges_map[f"{src}->{tgt}"] = edge_data
            ontology_edges_tuples[(src, tgt)] = edge_data
    except Exception:
        pass  # Table may not exist or be empty

    conn.close()

    # ── Data-driven root cause analysis ──────────────────────────────────────
    from core.narrative_engine import enrich_needs_attention as _run_root_cause_analysis

    # Build time_series from kpi_monthly for the narrative engine
    _ts_for_engine = {}
    for k, entries in kpi_monthly.items():
        sorted_entries = sorted(entries, key=lambda x: x["period"])
        _ts_for_engine[k] = [e["value"] for e in sorted_entries if isinstance(e.get("value"), (int, float))]

    _directions_for_engine = {
        t_key: t_val.get("direction", "higher") for t_key, t_val in targets_map.items()
    }
    _kpi_avgs_for_engine = {}
    for k, entries in kpi_monthly.items():
        vals = [e["value"] for e in entries if isinstance(e.get("value"), (int, float))]
        recent = vals[-6:] if len(vals) >= 6 else vals
        _kpi_avgs_for_engine[k] = sum(recent) / len(recent) if recent else None

    root_cause_analyses = _run_root_cause_analysis(
        health.get("needs_attention", []),
        _kpi_avgs_for_engine,
        _ts_for_engine,
        {k: v.get("target") for k, v in targets_map.items()},
        _directions_for_engine,
        ontology_edges_map,
    )

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
            # Use composite gap_pct if legacy one is missing or negative
            if not spot.get("gap_pct") or spot["gap_pct"] < 0:
                spot["gap_pct"] = cr["gap_pct"]

        # Compute direction-aware status from actual data
        _avg = spot.get("avg")
        _tval = targets_map.get(k, {}).get("target")
        _dirn = targets_map.get(k, {}).get("direction", "higher")
        if _avg is not None and _tval is not None:
            from core.health_score import _is_on_target, _is_critical
            if _is_on_target(_avg, _tval, _dirn):
                spot["status"] = "green"
            elif _is_critical(_avg, _tval, _dirn):
                spot["status"] = "red"
            else:
                spot["status"] = "yellow"
        else:
            spot["status"] = "grey"

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

        # Data-driven root cause analysis
        rca = root_cause_analyses.get(k)
        if rca:
            spot["root_cause_analysis"] = rca

        # Causal consistency validation (top-down + bottom-up)
        try:
            from core.narrative_engine import validate_causal_consistency
            _targets_for_val = {kk: vv.get("target") for kk, vv in targets_map.items() if vv.get("target") is not None}
            causal_val = validate_causal_consistency(
                k, spot.get("status", "grey"),
                _kpi_avgs_for_engine, _targets_for_val,
                _directions_for_engine, _ts_for_engine,
                ontology_edges=ontology_edges_tuples,
            )
            spot["causal_validation"] = causal_val
        except Exception:
            pass  # Graceful degradation — don't block rendering

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

    # ── Enrich yellow (watch zone) KPIs with full spotlight data ────────────
    yellow_enriched = []
    for k, pct_ratio in health.get("yellow_kpis_raw", []):
        spot = _kpi_spotlight(k)
        spot["pct"]    = round(pct_ratio * 100, 1)
        spot["status"] = "yellow"
        cr = composite_map.get(k)
        if cr:
            spot["domain"]       = cr["domain"]
            spot["domain_label"] = cr["domain_label"]
        bench = benchmark_position(
            k, spot.get("avg"), spot.get("direction", "higher"), company_stage,
        )
        if bench:
            spot["benchmark"] = bench
        yellow_enriched.append(spot)

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
        "watch_zone":      yellow_enriched,
        "domain_groups":   domain_groups,
        "domain_narratives": domain_narrs,
        "period_comparison": period_delta,
        "decision_check_ins": check_ins,
        "company_stage":   company_stage,
        "root_cause_analyses": root_cause_analyses,
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
# Union of KPI_DEFS + EXTENDED_ONTOLOGY_METRICS — EXTENDED has 30 KPIs not in KPI_DEFS
_KPI_DEFS_MAP = {d["key"]: d for d in KPI_DEFS}
for _ext in EXTENDED_ONTOLOGY_METRICS:
    if _ext.get("key") and _ext["key"] not in _KPI_DEFS_MAP:
        _KPI_DEFS_MAP[_ext["key"]] = _ext

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


def _deduplicate_actions(primary: str, candidates: list[str]) -> list[str]:
    """Remove candidates that substantially overlap with the primary action.

    Uses entity-aware matching: if both the primary and a candidate reference
    the same KPI entity AND the same data point (e.g. '33.5%'), they are
    treated as semantic duplicates even if worded differently.
    """
    import re as _re
    if not primary:
        return candidates
    primary_lower = primary.lower()

    # Extract KPI entity names and numeric data points from the primary action
    _ENTITY_RE = _re.compile(
        r'[a-z][a-z\s]{3,35}(?:rate|multiple|efficiency|growth|ratio|margin|churn|'
        r'revenue|cost|payback|retention|conversion|velocity|leverage|runway|burn)'
    )
    _NUM_RE = _re.compile(r'\d+\.?\d*%')
    primary_entities = set(_ENTITY_RE.findall(primary_lower))
    primary_nums = set(_NUM_RE.findall(primary_lower))

    result = []
    for c in candidates:
        c_lower = c.lower()
        # Exact match
        if c_lower.strip() == primary_lower.strip():
            continue
        # Entity + data-point overlap: same KPI mentioned with same percentage
        c_entities = set(_ENTITY_RE.findall(c_lower))
        shared_entities = primary_entities & c_entities
        if shared_entities and primary_nums:
            c_nums = set(_NUM_RE.findall(c_lower))
            if primary_nums & c_nums:
                continue  # Same entity + same number = semantic duplicate
        # Legacy: prefix match
        if len(c_lower) > 10 and c_lower[:40] in primary_lower:
            continue
        # Legacy: first-sentence overlap
        first_sentence = c_lower.split('.')[0].strip()
        if len(first_sentence) > 20 and first_sentence in primary_lower:
            continue
        result.append(c)
    return result


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
    workspace_id = _require_workspace(request)
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
        recent_vals = [pt["value"] for pt in time_series[-6:]]
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
            if k.startswith("_") or k in ("year", "month") or v is None:
                continue
            kpi_monthly_all.setdefault(k, []).append({"period": period, "value": v})

    correlations = compute_kpi_correlations(kpi_monthly_all, kpi_key)

    # Data requirements
    data_requirements = DATA_REQUIREMENTS.get(kpi_key)

    # Data-driven root cause analysis (replaces static templates)
    from core.narrative_engine import analyze_root_causes as _analyze_rca

    # Build time_series dict for narrative engine
    _ts_for_rca = {}
    _dirs_for_rca = {}
    for k, entries in kpi_monthly_all.items():
        sorted_entries = sorted(entries, key=lambda x: x["period"])
        _ts_for_rca[k] = [e["value"] for e in sorted_entries if isinstance(e.get("value"), (int, float))]
    # Load directions from targets
    try:
        for trow in conn.execute("SELECT kpi_key, direction FROM kpi_targets WHERE workspace_id=?", [workspace_id]).fetchall():
            _dirs_for_rca[trow["kpi_key"] if isinstance(trow, dict) else trow[0]] = (trow["direction"] if isinstance(trow, dict) else trow[1]) or "higher"
    except Exception:
        pass

    # Load ontology edges
    _edges_for_rca = {}
    try:
        for er in conn.execute("SELECT source, target, granger_pval, confidence_tier FROM ontology_edges").fetchall():
            ek = f"{er['source']}->{er['target']}" if isinstance(er, dict) else f"{er[0]}->{er[1]}"
            _edges_for_rca[ek] = {
                "granger_pval": er["granger_pval"] if isinstance(er, dict) else er[2],
                "confidence_tier": er["confidence_tier"] if isinstance(er, dict) else er[3],
            }
    except Exception:
        pass

    conn.close()

    rca = _analyze_rca(kpi_key, {}, _ts_for_rca, {}, _dirs_for_rca, _edges_for_rca)

    # Use data-driven root causes if available, fall back to static templates
    if rca.get("data_grounded") and rca.get("confirmed_causes"):
        driven_causes = [
            f"{c['name']} deteriorated {abs(c['delta_pct']):.1f}% "
            f"({'statistically confirmed' if c['confidence'] == 'granger_confirmed' else 'directionally supported'})"
            for c in rca["confirmed_causes"]
        ]
        # Prepend data-driven causes, append static as fallback context
        final_root_causes = driven_causes + [f"[Template] {c}" for c in causation.get("root_causes", [])[:1]]
    else:
        # Use validated root causes (template causes with data-contradicted ones removed)
        final_root_causes = rca.get("validated_root_causes", causation.get("root_causes", []))

    # ── AI-powered company-specific actions ──────────────────────────────────
    # Generate actions grounded in actual data using Claude API.
    # Falls back to template actions if API is unavailable.
    from core.narrative_engine import generate_ai_actions as _gen_ai_actions

    _company_stage = "series_a"  # default
    _company_name = ""
    try:
        _stage_conn = get_db()
        for _cs in _stage_conn.execute(
            "SELECT key, value FROM company_settings WHERE workspace_id=? AND key IN ('funding_stage','company_name')",
            [workspace_id],
        ).fetchall():
            _k = _cs[0] if not isinstance(_cs, dict) else _cs["key"]
            _v = _cs[1] if not isinstance(_cs, dict) else _cs["value"]
            if _k == "funding_stage" and _v:
                _company_stage = _v
            elif _k == "company_name" and _v:
                _company_name = _v
        _stage_conn.close()
    except Exception:
        pass

    ai_actions = _gen_ai_actions(
        kpi_key=kpi_key,
        kpi_name=kpi_def.get("name", kpi_key),
        unit=unit,
        direction=direction,
        current_value=avg,
        target_value=target_value,
        time_series=time_series,
        confirmed_causes=rca.get("confirmed_causes", []),
        downstream_impact=causation.get("downstream_impact", []),
        stage=_company_stage,
        company_name=_company_name,
        benchmark=benchmark,
        template_actions=actions,  # fallback if AI unavailable
    )

    # If AI generated actions, use them. Prepend contextual_action if data-grounded.
    if ai_actions and ai_actions != actions:
        if rca.get("contextual_action") and rca.get("data_grounded"):
            deduped = _deduplicate_actions(rca["contextual_action"], ai_actions)
            final_actions = [rca["contextual_action"]] + deduped[:2]
        else:
            final_actions = ai_actions[:3]
        _actions_source = "data_grounded"
    elif rca.get("contextual_action"):
        deduped = _deduplicate_actions(rca["contextual_action"], actions[:3])
        final_actions = [rca["contextual_action"]] + deduped[:2]
        _actions_source = "data_driven_context"
    else:
        final_actions = actions
        _actions_source = "template"

    # Remove exact duplicates
    _seen = set()
    _unique = []
    for a in final_actions:
        _norm = a.strip().lower()
        if _norm not in _seen:
            _seen.add(_norm)
            _unique.append(a)
    final_actions = _unique

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
        "root_causes":       final_root_causes,
        "downstream_impact": causation.get("downstream_impact", []),
        "corrective_actions": final_actions,
        "actions_source": _actions_source,
        "root_cause_analysis": rca,
        "data_grounded": rca.get("data_grounded", False),
        "benchmarks":        benchmark,
        "benchmark_position": bench_pos,
        "causal_chain":      causal_chain,
        "correlations":      correlations,
        "typical_range":     typical_range,
        "data_requirements": data_requirements,
    }
