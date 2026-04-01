"""
core/health_score.py — Company Health Score algorithm.

Score = Momentum (30%) + Target Achievement (40%) + Risk Flags (30%)

All three components are normalised to 0-100 before weighting.
"""
import json
import math
from typing import Optional


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
        if direction == "higher":
            pct = avg / tval if tval else 0
            if pct >= 0.98:
                green += 1
        else:
            pct = tval / avg if avg else 0
            if pct >= 0.98:
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
        if direction == "higher":
            pct = avg / tval if tval else 0
            if pct < 0.90:
                red += 1
        else:
            pct = tval / avg if avg else 0
            if pct < 0.90:
                red += 1
    if scored == 0:
        return 70.0  # no targets = moderate score
    return round(max(0.0, (1 - red / scored) * 100), 1)


def compute_health_score(conn, workspace_id: str) -> dict:
    """
    Main entry point. Pulls data from DB and returns full health score breakdown.

    Returns:
        {
            "score": 74,
            "grade": "B",
            "label": "Moderate",
            "color": "amber",
            "momentum": 65.0,
            "target_achievement": 78.0,
            "risk_flags": 80.0,
            "kpis_green": 12,
            "kpis_yellow": 4,
            "kpis_red": 3,
            "kpis_grey": 5,
            "months_of_data": 24,
            "needs_attention": [...],  # top 5 red/yellow KPIs
            "doing_well": [...],       # top 5 green KPIs
            "momentum_trend": "improving" | "stable" | "declining",
        }
    """
    # ── Pull monthly data ──────────────────────────────────────────────────────
    rows = conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month",
        [workspace_id]
    ).fetchall()

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
    time_series: dict = {}
    for row in rows:
        d = json.loads(row["data_json"])
        for k, v in d.items():
            if v is not None and k not in ("year", "month"):
                time_series.setdefault(k, []).append(float(v))

    # Latest-period averages (last 3 months or all if <3)
    kpi_avgs: dict = {}
    for k, vals in time_series.items():
        recent = vals[-3:] if len(vals) >= 3 else vals
        kpi_avgs[k] = sum(recent) / len(recent) if recent else None

    # ── Component scores ───────────────────────────────────────────────────────
    momentum          = _compute_momentum(time_series, directions)
    target_achieve    = _compute_target_achievement_with_directions(kpi_avgs, targets, directions)
    risk              = _compute_risk_flags(kpi_avgs, targets, directions)

    # Weighted composite
    raw_score = (momentum * 0.30) + (target_achieve * 0.40) + (risk * 0.30)
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
        if dirn == "higher":
            pct = avg / tval if tval else 0
            if pct >= 0.98:
                green_kpis.append((key, pct))
            elif pct >= 0.90:
                yellow_kpis.append((key, pct))
            else:
                red_kpis.append((key, pct))
        else:
            pct = tval / avg if avg else 0
            if pct >= 0.98:
                green_kpis.append((key, pct))
            elif pct >= 0.90:
                yellow_kpis.append((key, pct))
            else:
                red_kpis.append((key, pct))

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
        "needs_attention":    [k for k, _ in (red_kpis + yellow_kpis)[:6]],
        "doing_well":         [k for k, _ in green_kpis[:6]],
        "momentum_trend":     momentum_trend,
    }
