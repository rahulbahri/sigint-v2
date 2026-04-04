"""
core/health_score.py — Company Health Score algorithm.

Score = Momentum (30%) + Target Achievement (40%) + Risk Flags (30%)

All three components are normalised to 0-100 before weighting.
"""
import json
import math
from typing import Optional

from core.criticality import (
    compute_composite_criticality,
    group_by_domain,
    get_kpi_domain,
    DOMAIN_LABELS,
)
from core.kpi_defs import KPI_DEFS, EXTENDED_ONTOLOGY_METRICS

# ── Human-readable KPI name lookup ──────────────────────────────────────────
_KEY_TO_NAME = {d["key"]: d["name"] for d in KPI_DEFS + EXTENDED_ONTOLOGY_METRICS}


def _friendly_name(key: str) -> str:
    """Return the human-readable name for a KPI key, e.g. 'pricing_power_index' → 'Pricing Power Index'."""
    return _KEY_TO_NAME.get(key, key.replace("_", " ").title())


def _compute_momentum(time_series_by_kpi: dict, directions: dict) -> float:
    """
    Momentum: compare the average of the last 3 months vs the 3 months before that.
    Returns 0-100 (50 = flat, 100 = all KPIs improving, 0 = all degrading).
    """
    improving = 0
    declining = 0
    for key, vals in time_series_by_kpi.items():
        if len(vals) < 6:
            continue
        recent = vals[-3:]
        prior  = vals[-6:-3]
        recent_avg = sum(recent) / len(recent)
        prior_avg  = sum(prior)  / len(prior)
        direction  = directions.get(key, "higher")
        if direction == "higher":
            if recent_avg > prior_avg * 1.005:
                improving += 1
            elif recent_avg < prior_avg * 0.995:
                declining += 1
        else:  # lower is better (e.g. churn, burn)
            if recent_avg < prior_avg * 0.995:
                improving += 1
            elif recent_avg > prior_avg * 1.005:
                declining += 1
    total = improving + declining
    if total == 0:
        return 50.0  # no signal
    # normalise: improving/(improving+declining) → 0-100
    return round((improving / total) * 100, 1)


def _compute_target_achievement(
    kpi_avgs: dict,
    targets: dict,
) -> float:
    """
    Target Achievement: % of KPIs that are green (≥98% of target).
    Returns 0-100.
    """
    if not targets:
        return 50.0  # no targets set
    green = 0
    scored = 0
    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue
        scored += 1
        direction = "higher"  # default; overridden below via directions dict
        pct = avg / tval if tval else 0
        if pct >= 0.98:
            green += 1
    if scored == 0:
        return 50.0
    return round((green / scored) * 100, 1)


def _is_on_target(avg: float, tval: float, direction: str) -> bool:
    """Check if a KPI is on target (within 2% tolerance)."""
    if direction == "higher":
        return avg >= tval * 0.98 if tval >= 0 else avg >= tval * 1.02
    else:  # lower is better
        return avg <= tval * 1.02 if tval >= 0 else avg <= tval * 0.98


def _is_critical(avg: float, tval: float, direction: str) -> bool:
    """Check if a KPI is critically off target (>10% miss)."""
    if direction == "higher":
        return avg < tval * 0.90 if tval >= 0 else avg < tval * 1.10
    else:  # lower is better
        return avg > tval * 1.10 if tval >= 0 else avg > tval * 0.90


def _gap_pct(avg: float, tval: float, direction: str) -> float:
    """
    Compute how well a KPI is performing vs target as a 0-2+ ratio.
    >= 1.0 means on or above target.  < 1.0 means below target.
    Works correctly with negative values and both directions.
    """
    if direction == "higher":
        if tval == 0:
            return 1.0 if avg >= 0 else 0.0
        # For higher-is-better: how close is avg to target?
        if tval > 0:
            return avg / tval
        else:
            # Negative target (unusual): closer to 0 is better
            return tval / avg if avg != 0 else 0.0
    else:  # lower is better
        # For lower-is-better: being BELOW target is GOOD
        if avg == tval:
            return 1.0
        if tval == 0:
            return 1.0 if avg <= 0 else 0.0
        # How much better/worse than target?
        # If avg < target (good): gap > 1.0
        # If avg > target (bad): gap < 1.0
        if tval > 0:
            return tval / avg if avg > 0 else 2.0  # avg <= 0 when target > 0 means excellent
        else:
            # Both negative: e.g. avg=-5, target=-3 (lower better means -5 < -3, good)
            return avg / tval if tval != 0 else 1.0


def _compute_target_achievement_with_directions(
    kpi_avgs: dict,
    targets: dict,
    directions: dict,
) -> float:
    """Target achievement with direction awareness."""
    if not targets:
        return 50.0
    green = 0
    scored = 0
    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue
        scored += 1
        direction = directions.get(key, "higher")
        if _is_on_target(avg, tval, direction):
            green += 1
    if scored == 0:
        return 50.0
    return round((green / scored) * 100, 1)


def _compute_risk_flags(
    kpi_avgs: dict,
    targets: dict,
    directions: dict,
) -> float:
    """
    Risk score: 100 - penalty for each red KPI.
    Each red KPI deducts (100 / max(total_scored, 1)) points.
    Returns 0-100.
    """
    red = 0
    scored = 0
    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue
        scored += 1
        direction = directions.get(key, "higher")
        if _is_critical(avg, tval, direction):
            red += 1
    if scored == 0:
        return 70.0  # no targets = moderate score
    return round(max(0.0, (1 - red / scored) * 100), 1)


def compute_health_score(
    conn,
    workspace_id: str,
    w_momentum: float = 0.30,
    w_target: float = 0.40,
    w_risk: float = 0.30,
    from_period: Optional[tuple] = None,
    to_period: Optional[tuple] = None,
    criticality_weights: Optional[dict] = None,
) -> dict:
    """
    Main entry point. Pulls data from DB and returns full health score breakdown.

    Parameters:
        w_momentum:  weight for momentum component (default 0.30)
        w_target:    weight for target achievement component (default 0.40)
        w_risk:      weight for risk flags component (default 0.30)
        from_period: optional (year, month) tuple for start of date range
        to_period:   optional (year, month) tuple for end of date range

    Returns dict with score, grade, component breakdowns, KPI detail lists,
    narrative_detail, and weights.
    """
    # ── Pull monthly data ──────────────────────────────────────────────────────
    query = "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=?"
    params: list = [workspace_id]
    if from_period is not None:
        query += " AND (year > ? OR (year = ? AND month >= ?))"
        params.extend([from_period[0], from_period[0], from_period[1]])
    if to_period is not None:
        query += " AND (year < ? OR (year = ? AND month <= ?))"
        params.extend([to_period[0], to_period[0], to_period[1]])
    query += " ORDER BY year, month"
    rows = conn.execute(query, params).fetchall()

    targets_rows = conn.execute(
        "SELECT kpi_key, target_value, direction FROM kpi_targets WHERE workspace_id=?",
        [workspace_id]
    ).fetchall()

    targets = {}
    directions = {}
    for r in targets_rows:
        targets[r["kpi_key"]]    = r["target_value"]
        directions[r["kpi_key"]] = r["direction"] or "higher"

    # Build time series per KPI (ordered by year, month)
    # Filter NaN/Inf to prevent downstream computation failures
    import math as _math
    time_series: dict = {}
    for row in rows:
        d = json.loads(row["data_json"])
        for k, v in d.items():
            if v is not None and k not in ("year", "month"):
                try:
                    fv = float(v)
                    if _math.isfinite(fv):
                        time_series.setdefault(k, []).append(fv)
                except (ValueError, TypeError):
                    pass

    # Latest-period averages (last 3 months or all if <3)
    kpi_avgs: dict = {}
    for k, vals in time_series.items():
        recent = vals[-3:] if len(vals) >= 3 else vals
        kpi_avgs[k] = sum(recent) / len(recent) if recent else None

    # ── Component scores ───────────────────────────────────────────────────────
    momentum          = _compute_momentum(time_series, directions)
    target_achieve    = _compute_target_achievement_with_directions(kpi_avgs, targets, directions)
    risk              = _compute_risk_flags(kpi_avgs, targets, directions)

    # ── Per-KPI detail for each component ─────────────────────────────────────
    momentum_kpis = []
    for key, vals in time_series.items():
        if len(vals) < 6:
            continue
        recent_slice = vals[-3:]
        prior_slice  = vals[-6:-3]
        if not recent_slice or not prior_slice:
            continue
        recent_avg = sum(recent_slice) / len(recent_slice)
        prior_avg  = sum(prior_slice) / len(prior_slice)
        direction  = directions.get(key, "higher")
        if direction == "higher":
            status = "improving" if recent_avg > prior_avg * 1.005 else ("declining" if recent_avg < prior_avg * 0.995 else "stable")
        else:
            status = "improving" if recent_avg < prior_avg * 0.995 else ("declining" if recent_avg > prior_avg * 1.005 else "stable")
        delta_pct = round(((recent_avg - prior_avg) / abs(prior_avg)) * 100, 1) if prior_avg != 0 else 0.0
        momentum_kpis.append({"key": key, "name": _friendly_name(key), "status": status, "delta_pct": delta_pct})
    momentum_kpis.sort(key=lambda x: x["delta_pct"])

    target_kpis = []
    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue
        dirn = directions.get(key, "higher")
        on_target = _is_on_target(avg, tval, dirn)
        target_kpis.append({
            "key": key, "name": _friendly_name(key),
            "avg": round(avg, 2), "target": tval,
            "on_target": on_target, "direction": dirn,
        })
    target_kpis.sort(key=lambda x: (0 if x["on_target"] else 1, x["key"]))

    risk_kpis = []
    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue
        dirn = directions.get(key, "higher")
        is_red = _is_critical(avg, tval, dirn)
        if is_red:
            risk_kpis.append({
                "key": key, "name": _friendly_name(key),
                "avg": round(avg, 2), "target": tval, "direction": dirn,
            })
    risk_kpis.sort(key=lambda x: x["key"])

    # Weighted composite
    raw_score = (momentum * w_momentum) + (target_achieve * w_target) + (risk * w_risk)
    score = round(raw_score)

    # ── Grade and label ────────────────────────────────────────────────────────
    if score >= 85:
        grade, label, color = "A", "Excellent",  "green"
    elif score >= 70:
        grade, label, color = "B", "Good",       "green"
    elif score >= 55:
        grade, label, color = "C", "Moderate",   "amber"
    elif score >= 40:
        grade, label, color = "D", "Needs Work", "amber"
    else:
        grade, label, color = "F", "Critical",   "red"

    # ── KPI status buckets ─────────────────────────────────────────────────────
    green_kpis = []
    yellow_kpis = []
    red_kpis = []
    grey_kpis = []

    all_keys = set(list(targets.keys()) + list(kpi_avgs.keys()))
    for key in all_keys:
        avg  = kpi_avgs.get(key)
        tval = targets.get(key)
        dirn = directions.get(key, "higher")
        if avg is None:
            grey_kpis.append(key)
            continue
        if tval is None:
            grey_kpis.append(key)
            continue
        pct = _gap_pct(avg, tval, dirn)
        if _is_on_target(avg, tval, dirn):
            green_kpis.append((key, pct))
        elif _is_critical(avg, tval, dirn):
            red_kpis.append((key, pct))
        else:
            yellow_kpis.append((key, pct))

    # Sort for display (worst first for red, best first for green)
    red_kpis.sort(key=lambda x: x[1])
    green_kpis.sort(key=lambda x: x[1], reverse=True)
    yellow_kpis.sort(key=lambda x: x[1])

    # Momentum trend label
    if momentum >= 60:
        momentum_trend = "improving"
    elif momentum >= 45:
        momentum_trend = "stable"
    else:
        momentum_trend = "declining"

    # ── Narrative detail ─────────────────────────────────────────────────────
    # Score meaning
    if score >= 85:
        score_meaning = (
            f"A score of {score}/100 means the vast majority of your tracked KPIs are on or above target. "
            "Your business health is excellent — maintain current strategies and look for expansion opportunities."
        )
    elif score >= 70:
        score_meaning = (
            f"A score of {score}/100 means most of your tracked KPIs are performing well with a few areas to monitor. "
            "Your business health is good — focus on the yellow/red KPIs to push into excellent territory."
        )
    elif score >= 55:
        score_meaning = (
            f"A score of {score}/100 means a meaningful portion of your KPIs are below target. "
            "Your business health is moderate — prioritise the underperforming metrics to prevent further decline."
        )
    elif score >= 40:
        score_meaning = (
            f"A score of {score}/100 means many of your tracked KPIs are significantly off-target. "
            "Your business health needs work — focused corrective action is required on revenue, retention, and efficiency metrics."
        )
    else:
        score_meaning = (
            f"A score of {score}/100 means the majority of your tracked KPIs are significantly off-target. "
            "Your business health is in the critical zone — immediate corrective action is needed across revenue, retention, and efficiency metrics."
        )

    top_drags = [{"key": k, "name": _friendly_name(k), "gap_pct": round((1 - p) * 100, 1)} for k, p in red_kpis[:3]]
    top_wins  = [{"key": k, "name": _friendly_name(k), "gap_pct": round((p - 1) * 100, 1)} for k, p in green_kpis[:3]]

    primary_action = ""
    if red_kpis:
        worst_key = red_kpis[0][0]
        worst_name = _friendly_name(worst_key)
        worst_gap = round((1 - red_kpis[0][1]) * 100, 1)
        primary_action = (
            f"Your most critical metric is {worst_name} which is {worst_gap}% below target. "
            f"Investigate root causes and develop a 30-day recovery plan."
        )

    # ── Data sufficiency assessment ──────────────────────────────────────────
    total_kpis_expected = len(targets)
    total_kpis_with_data = len([k for k in targets if kpi_avgs.get(k) is not None])
    data_coverage_pct = round(total_kpis_with_data / max(total_kpis_expected, 1) * 100)
    months_available = len(rows)

    if data_coverage_pct >= 90 and months_available >= 12:
        data_sufficiency = "Complete"
        data_sufficiency_note = (
            f"All core metrics are reporting ({total_kpis_with_data} of {total_kpis_expected} KPIs) "
            f"with {months_available} months of history. Health score is fully reliable."
        )
    elif data_coverage_pct >= 70 and months_available >= 6:
        data_sufficiency = "Adequate"
        data_sufficiency_note = (
            f"{total_kpis_with_data} of {total_kpis_expected} KPIs have data ({data_coverage_pct}% coverage) "
            f"across {months_available} months. Health score is directionally reliable but "
            f"missing KPIs ({', '.join(grey_kpis[:5])}) may affect completeness."
        )
    elif data_coverage_pct >= 40:
        data_sufficiency = "Partial"
        data_sufficiency_note = (
            f"Only {total_kpis_with_data} of {total_kpis_expected} KPIs have data ({data_coverage_pct}% coverage). "
            f"Health score should be treated as preliminary. Connect additional data sources "
            f"to improve accuracy."
        )
    else:
        data_sufficiency = "Insufficient"
        data_sufficiency_note = (
            f"Only {total_kpis_with_data} of {total_kpis_expected} KPIs have data ({data_coverage_pct}% coverage). "
            f"Health score is not reliable at this coverage level. Please connect your primary "
            f"accounting and CRM systems to enable meaningful analysis."
        )

    narrative_detail = {
        "score_meaning":        score_meaning,
        "top_drags":            top_drags,
        "top_wins":             top_wins,
        "primary_action":       primary_action,
        "data_sufficiency":     data_sufficiency,
        "data_sufficiency_note": data_sufficiency_note,
        "data_coverage_pct":    data_coverage_pct,
        "kpis_missing_data":    grey_kpis,
    }

    # ── Composite criticality scoring ─────────────────────────────────────────
    composite_ranked = compute_composite_criticality(
        kpi_avgs, targets, directions, time_series,
        weights=criticality_weights,
    )
    domain_groups = group_by_domain(composite_ranked)

    return {
        "score":              score,
        "grade":              grade,
        "label":              label,
        "color":              color,
        "momentum":           momentum,
        "target_achievement": target_achieve,
        "risk_flags":         risk,
        "kpis_green":         len(green_kpis),
        "kpis_yellow":        len(yellow_kpis),
        "kpis_red":           len(red_kpis),
        "kpis_grey":          len(grey_kpis),
        "months_of_data":     len(rows),
        "needs_attention":    [k for k, _ in (red_kpis + yellow_kpis)],
        "needs_attention_ranked": [
            {"key": k, "gap_pct": round((1 - p) * 100, 1), "rank": i + 1}
            for i, (k, p) in enumerate(red_kpis)
        ],
        "doing_well":         [k for k, _ in green_kpis],
        "momentum_trend":     momentum_trend,
        "green_kpis_detail":  [{"key": k, "pct": round(p * 100, 1)} for k, p in green_kpis],
        "yellow_kpis_detail": [{"key": k, "pct": round(p * 100, 1)} for k, p in yellow_kpis],
        "red_kpis_detail":    [{"key": k, "pct": round(p * 100, 1)} for k, p in red_kpis],
        "grey_kpis_list":     grey_kpis,
        "weights":            {"momentum": w_momentum, "target": w_target, "risk": w_risk},
        "component_detail": {
            "momentum": {
                "score": momentum,
                "kpis": momentum_kpis,
                "total_improving": sum(1 for k in momentum_kpis if k["status"] == "improving"),
                "total_declining": sum(1 for k in momentum_kpis if k["status"] == "declining"),
                "total_stable":    sum(1 for k in momentum_kpis if k["status"] == "stable"),
            },
            "target_achievement": {
                "score": target_achieve,
                "kpis": target_kpis,
                "total_on_target": sum(1 for k in target_kpis if k["on_target"]),
                "total_off_target": sum(1 for k in target_kpis if not k["on_target"]),
            },
            "risk": {
                "score": risk,
                "kpis": risk_kpis,
                "total_red": len(risk_kpis),
                "total_scored": len([k for k, t in targets.items() if t is not None and kpi_avgs.get(k) is not None]),
            },
        },
        "narrative_detail":   narrative_detail,
        "composite_ranked":   composite_ranked,
        "domain_groups":      domain_groups,
    }
