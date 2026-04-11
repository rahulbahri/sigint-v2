"""
core/board_narrative.py — Board-pack-level narrative generation.

Builds on narrative_engine.py (per-KPI root cause analysis) to produce
pack-level narratives: executive summaries, signal detection, domain
stories, period comparisons, and outlook bullets.

All functions are pure-functional (no DB access). Data is passed as arguments.
"""
from __future__ import annotations

import math
from typing import Optional

from core.kpi_defs import ALL_CAUSATION_RULES, KPI_DEFS, EXTENDED_ONTOLOGY_METRICS
from core.narrative_engine import analyze_root_causes, generate_ai_actions, _friendly

# ── Domain classification (mirrors frontend DOMAIN_MAP) ─────────────────────

DOMAIN_KEYWORDS = {
    "growth":     ["revenue", "arr", "mrr", "growth", "cac", "ltv", "pipeline",
                   "deal", "win_rate", "new_", "magic_number"],
    "retention":  ["nrr", "churn", "retention", "activation", "nps",
                   "satisfaction", "health", "adoption", "time_to_value", "ttv"],
    "efficiency": ["margin", "burn", "sales_cycle", "payback", "opex",
                   "cogs", "utilization", "rule_of_40", "headcount", "rev_per"],
    "cashflow":   ["cash", "runway", "fcf", "free_cash", "operating_cash",
                   "dso", "ar_", "working_capital", "current_ratio"],
    "risk":       ["concentration", "fragility", "convexity", "contraction"],
    "profitability": ["ebitda", "contribution", "gross_profit", "pricing_power",
                      "operating_leverage"],
}

DOMAIN_LABELS = {
    "growth":        "Growth & Acquisition",
    "retention":     "Retention & Expansion",
    "efficiency":    "Operational Efficiency",
    "cashflow":      "Cash & Liquidity",
    "risk":          "Risk & Concentration",
    "profitability": "Profitability & Margins",
    "other":         "Other Metrics",
}


def _get_domain(kpi_key: str, kpi_name: str = "") -> str:
    """Classify a KPI into a business domain using keyword matching."""
    k = (kpi_key + " " + kpi_name).lower()
    for domain, keywords in DOMAIN_KEYWORDS.items():
        if any(w in k for w in keywords):
            return domain
    return "other"


def _fmt_val(val, unit: str = "") -> str:
    """Format a value with its unit for narrative text."""
    if val is None:
        return "N/A"
    u = (unit or "").lower()
    if u in ("pct", "%"):
        return f"{val:.1f}%"
    if u in ("usd", "$"):
        if abs(val) >= 1_000_000:
            return f"${val / 1_000_000:.1f}M"
        if abs(val) >= 1_000:
            return f"${val / 1_000:.0f}K"
        return f"${val:,.0f}"
    if u == "days":
        return f"{val:.0f} days"
    if u == "months":
        return f"{val:.1f} months"
    if u in ("ratio", "x"):
        return f"{val:.2f}x"
    return f"{val:.1f}"


def _cell_status(val, target, direction: str) -> str:
    """Determine status: green/yellow/red/grey."""
    if val is None or target is None or target == 0:
        return "grey"
    if direction == "higher":
        r = val / target
    else:
        r = target / val if val != 0 else 0
    if r >= 0.98:
        return "green"
    if r >= 0.90:
        return "yellow"
    return "red"


def _gap_pct(avg, target, direction: str) -> Optional[float]:
    """Direction-aware gap percentage."""
    if avg is None or target is None or target == 0:
        return None
    raw = (avg / target - 1) * 100
    return raw if direction == "higher" else -raw


# ── Signal Detection (ported from frontend BoardReady.jsx) ──────────────────

def detect_signals(fingerprint: list[dict]) -> list[dict]:
    """Detect hidden structural signals in the KPI data.

    fingerprint: list of KPI dicts with keys:
        key, name, avg, target, direction, unit, fy_status,
        monthly: [{period: "2025-01", value: float}, ...]

    Returns up to 5 signal dicts with: severity, title, body, affected_kpis
    """
    signals = []

    # 1. Red streaks (>= 3 consecutive months)
    streakers = []
    for kpi in fingerprint:
        streak = _red_streak(kpi)
        if streak >= 3:
            streakers.append((kpi, streak))
    streakers.sort(key=lambda x: -x[1])
    if streakers:
        k, s = streakers[0]
        signals.append({
            "severity": "critical",
            "title": f"{k['name']} has missed target {s} consecutive months",
            "body": (
                f"A streak of {s} months indicates a structural failure, not a one-off miss. "
                f"Sustained red streaks compound — each additional month makes recovery "
                f"significantly harder. This requires escalation."
            ),
            "affected_kpis": [x[0]["key"] for x in streakers],
        })

    # 2. Green traps (green average but deteriorating trend)
    traps = []
    for kpi in fingerprint:
        if kpi.get("fy_status") != "green":
            continue
        vals = [m["value"] for m in (kpi.get("monthly") or []) if m.get("value") is not None]
        if len(vals) < 3:
            continue
        last3 = vals[-3:]
        if kpi.get("direction") == "higher":
            if last3[2] < last3[0]:
                traps.append(kpi)
        else:
            if last3[2] > last3[0]:
                traps.append(kpi)
    if traps:
        k = traps[0]
        signals.append({
            "severity": "warning",
            "title": f"{k['name']} is green on paper but the trend is deteriorating",
            "body": (
                f"The average meets target, but the last 3 months show a consistent "
                f"adverse trajectory. If the trend continues, this KPI will breach the "
                f"warning threshold within 1-2 quarters."
            ),
            "affected_kpis": [t["key"] for t in traps],
        })

    # 3. Recovery momentum (off target but improving)
    recovering = []
    for kpi in fingerprint:
        if kpi.get("fy_status") == "green":
            continue
        vals = [m["value"] for m in (kpi.get("monthly") or []) if m.get("value") is not None]
        if len(vals) < 3:
            continue
        last3 = vals[-3:]
        if kpi.get("direction") == "higher":
            if last3[2] > last3[0] * 1.02:
                recovering.append(kpi)
        else:
            if last3[2] < last3[0] * 0.98:
                recovering.append(kpi)
    if recovering:
        k = recovering[0]
        signals.append({
            "severity": "positive",
            "title": f"{k['name']} is below target but showing genuine momentum",
            "body": (
                f"Despite missing its target, {k['name']} has improved consistently "
                f"over the last 3 months. If sustained, this could represent a turning point."
            ),
            "affected_kpis": [r["key"] for r in recovering],
        })

    # 4. Growth masking retention
    retention_kpis = [k for k in fingerprint if any(
        w in (k.get("key", "") + " " + k.get("name", "")).lower()
        for w in ["nrr", "churn", "retention", "logo"]
    )]
    growth_kpis = [k for k in fingerprint if any(
        w in (k.get("key", "") + " " + k.get("name", "")).lower()
        for w in ["revenue", "arr", "mrr"]
    )]
    if (retention_kpis and growth_kpis
            and any(k.get("fy_status") != "green" for k in retention_kpis)
            and any(k.get("fy_status") == "green" for k in growth_kpis)):
        signals.append({
            "severity": "warning",
            "title": "Growth is masking a retention problem",
            "body": (
                "Top-line revenue looks healthy, but retention metrics are under stress. "
                "Retention problems typically surface in the revenue line 2-3 quarters later "
                "after churn compounds. Reviewing only the income statement will miss this signal."
            ),
            "affected_kpis": [k["key"] for k in retention_kpis if k.get("fy_status") != "green"],
        })

    # 5. Domain clustering (2+ warnings in same domain)
    by_domain: dict[str, list] = {}
    for kpi in fingerprint:
        d = _get_domain(kpi.get("key", ""), kpi.get("name", ""))
        by_domain.setdefault(d, []).append(kpi)

    for domain, kpis in by_domain.items():
        if domain == "other":
            continue
        amber_red = [k for k in kpis if k.get("fy_status") in ("yellow", "red")]
        if len(amber_red) >= 2:
            label = DOMAIN_LABELS.get(domain, domain)
            signals.append({
                "severity": "warning",
                "title": f"{len(amber_red)} {label} metrics simultaneously under pressure",
                "body": (
                    f"Clustered warnings within {label} suggest a systemic constraint rather "
                    f"than isolated underperformance. The root cause is usually structural."
                ),
                "affected_kpis": [k["key"] for k in amber_red],
            })
            break  # Only flag the worst domain

    return signals[:5]


def _red_streak(kpi: dict) -> int:
    """Count consecutive red months from the most recent month backwards."""
    monthly = kpi.get("monthly") or []
    by_month = {}
    for m in monthly:
        mo = int(m["period"].split("-")[1])
        by_month[mo] = m.get("value")
    streak = 0
    for mo in range(12, 0, -1):
        val = by_month.get(mo)
        if _cell_status(val, kpi.get("target"), kpi.get("direction", "higher")) == "red":
            streak += 1
        else:
            break
    return streak


# ── Executive Summary ───────────────────────────────────────────────────────

def generate_executive_summary(
    health: dict,
    fingerprint: list[dict],
    signals: list[dict],
    period_label: str,
    critical_analyses: dict = None,
) -> list[str]:
    """Generate 2-4 paragraph executive summary with causal intelligence.

    Returns a list of paragraph strings.
    """
    critical_analyses = critical_analyses or {}
    paragraphs = []

    score = health.get("score", 0)
    label = health.get("label", "")
    n_green = health.get("kpis_green", 0)
    n_yellow = health.get("kpis_yellow", 0)
    n_red = health.get("kpis_red", 0)
    total = n_green + n_yellow + n_red

    # Paragraph 1: Health score + status overview
    red_kpis = [k for k in fingerprint if k.get("fy_status") == "red"]
    if n_red == 0:
        paragraphs.append(
            f"For {period_label}, the business health score is {score}/100 ({label}). "
            f"All {total} tracked KPIs are within target thresholds, with "
            f"{n_green} on target and {n_yellow} in the watch zone."
        )
    else:
        worst = max(red_kpis, key=lambda k: abs(_gap_pct(k.get("avg"), k.get("target"), k.get("direction", "higher")) or 0))
        worst_gap = _gap_pct(worst.get("avg"), worst.get("target"), worst.get("direction", "higher"))
        paragraphs.append(
            f"For {period_label}, the business health score is {score}/100 ({label}). "
            f"Of {total} tracked KPIs, {n_red} are in critical status, "
            f"{n_yellow} require monitoring, and {n_green} are on target. "
            f"The most severe miss is {worst['name']} at "
            f"{_fmt_val(worst.get('avg'), worst.get('unit', ''))} vs target "
            f"{_fmt_val(worst.get('target'), worst.get('unit', ''))}"
            f"{f' ({abs(worst_gap):.0f}% gap)' if worst_gap else ''}."
        )

    # Paragraph 2: Causal chain for worst KPI (if analysis available)
    if red_kpis and critical_analyses:
        worst_key = red_kpis[0].get("key", "")
        analysis = critical_analyses.get(worst_key, {})
        if analysis.get("narrative"):
            # Also check downstream cascade
            downstream = ALL_CAUSATION_RULES.get(worst_key, {}).get("downstream_impact", [])
            downstream_red = [k for k in fingerprint
                              if k.get("key") in downstream and k.get("fy_status") == "red"]
            cascade_text = ""
            if downstream_red:
                names = ", ".join(k["name"] for k in downstream_red[:3])
                cascade_text = (
                    f" This is cascading: {red_kpis[0]['name']} directly impacts "
                    f"{names}, which are also in critical status."
                )
            paragraphs.append(analysis["narrative"] + cascade_text)

    # Paragraph 3: Key signals
    critical_signals = [s for s in signals if s["severity"] == "critical"]
    warning_signals = [s for s in signals if s["severity"] == "warning"]
    if critical_signals or warning_signals:
        signal_parts = []
        for s in (critical_signals + warning_signals)[:2]:
            signal_parts.append(s["title"])
        paragraphs.append(
            "Key signals this period: " + "; ".join(signal_parts) + "."
        )

    # Paragraph 4: Bright spots
    green_kpis = [k for k in fingerprint if k.get("fy_status") == "green"]
    if green_kpis:
        top_green = green_kpis[:3]
        names = ", ".join(k["name"] for k in top_green)
        paragraphs.append(f"Bright spots: {names} are on or above target.")

    return paragraphs


# ── Causal Chain Narrative ──────────────────────────────────────────────────

def generate_causal_narratives(
    critical_kpis: list[dict],
    kpi_avgs: dict,
    time_series: dict,
    targets: dict,
    directions: dict,
    ontology_edges: dict = None,
) -> list[dict]:
    """Generate knowledge-graph-based causal narratives for critical KPIs.

    Returns list of {kpi_key, kpi_name, headline, narrative, actions, severity}.
    """
    ontology_edges = ontology_edges or {}
    results = []

    for kpi in critical_kpis[:5]:
        key = kpi.get("key", "")
        name = kpi.get("name", _friendly(key))
        avg = kpi_avgs.get(key)
        target = targets.get(key)
        direction = directions.get(key, "higher")
        unit = kpi.get("unit", "")
        streak = _red_streak(kpi)

        # Run root cause analysis from narrative_engine
        analysis = analyze_root_causes(
            key, kpi_avgs, time_series, targets, directions, ontology_edges
        )

        # Build headline
        gap = _gap_pct(avg, target, direction)
        headline = f"{name}: {_fmt_val(avg, unit)} vs target {_fmt_val(target, unit)}"
        if gap is not None:
            headline += f" ({abs(gap):.0f}% gap)"
        if streak >= 3:
            headline += f" — {streak}-month red streak"

        # Generate data-grounded actions
        downstream = ALL_CAUSATION_RULES.get(key, {}).get("downstream_impact", [])
        template_actions = ALL_CAUSATION_RULES.get(key, {}).get("corrective_actions", [])
        actions = generate_ai_actions(
            kpi_key=key, kpi_name=name, unit=unit, direction=direction,
            current_value=avg, target_value=target,
            time_series=[{"value": v} for v in (time_series.get(key) or [])],
            confirmed_causes=analysis.get("confirmed_causes", []),
            downstream_impact=downstream,
            template_actions=template_actions,
        )

        results.append({
            "kpi_key": key,
            "kpi_name": name,
            "headline": headline,
            "narrative": analysis.get("narrative", ""),
            "actions": actions,
            "severity": "critical",
            "downstream_count": len(downstream),
            "confirmed_causes": analysis.get("confirmed_causes", []),
        })

    return results


# ── Domain Narratives ───────────────────────────────────────────────────────

def generate_domain_narratives(
    fingerprint: list[dict],
) -> dict[str, dict]:
    """Generate per-domain narratives from fingerprint data.

    Returns dict keyed by domain with:
        label, story, red_count, yellow_count, green_count, kpis, worst_kpi
    """
    by_domain: dict[str, list] = {}
    for kpi in fingerprint:
        d = _get_domain(kpi.get("key", ""), kpi.get("name", ""))
        by_domain.setdefault(d, []).append(kpi)

    results = {}
    for domain, kpis in by_domain.items():
        red = [k for k in kpis if k.get("fy_status") == "red"]
        yellow = [k for k in kpis if k.get("fy_status") == "yellow"]
        green = [k for k in kpis if k.get("fy_status") == "green"]

        # Find worst by gap magnitude
        def gap_mag(k):
            g = _gap_pct(k.get("avg"), k.get("target"), k.get("direction", "higher"))
            return abs(g) if g is not None else 0
        worst = max(red, key=gap_mag) if red else None

        # Build story
        if len(green) == len(kpis):
            names = ", ".join(k["name"] for k in green)
            story = (
                f"All {len(kpis)} KPIs are on target: {names}. "
                f"This domain is performing well across the board."
            )
        elif red and worst:
            red_names = ", ".join(k["name"] for k in red)
            worst_gap = gap_mag(worst)
            story = (
                f"{len(red)} of {len(kpis)} KPIs are critical: {red_names}. "
                f"{worst['name']} is the most concerning at "
                f"{_fmt_val(worst.get('avg'), worst.get('unit', ''))}, "
                f"{worst_gap:.1f}% from target of "
                f"{_fmt_val(worst.get('target'), worst.get('unit', ''))}."
            )
            if yellow:
                story += (
                    f" Additionally, {len(yellow)} KPI{'s are' if len(yellow) > 1 else ' is'} "
                    f"in the watch zone."
                )
        elif yellow:
            yellow_names = ", ".join(k["name"] for k in yellow)
            story = (
                f"No critical failures, but {len(yellow)} metric"
                f"{'s are' if len(yellow) > 1 else ' is'} trending adversely: {yellow_names}. "
                f"{len(green)} KPI{'s remain' if len(green) != 1 else ' remains'} on target."
            )
        else:
            story = (
                f"{len(kpis)} KPIs tracked: {len(green)} on target, "
                f"{len(yellow)} watch, {len(red)} critical."
            )

        results[domain] = {
            "label": DOMAIN_LABELS.get(domain, domain),
            "story": story,
            "red_count": len(red),
            "yellow_count": len(yellow),
            "green_count": len(green),
            "total": len(kpis),
            "kpis": kpis,
            "worst_kpi": worst,
            "has_issues": len(red) > 0 or len(yellow) > 0,
        }

    return results


# ── Period Comparison ───────────────────────────────────────────────────────

def generate_period_comparison(
    current_fingerprint: list[dict],
    prior_fingerprint: list[dict],
    period_label_current: str,
    period_label_prior: str,
) -> dict:
    """Compare two periods and generate delta narrative.

    Returns:
        title, summary, improved, deteriorated, new_reds, resolved_reds, deltas
    """
    current_map = {k["key"]: k for k in current_fingerprint}
    prior_map = {k["key"]: k for k in prior_fingerprint}
    common_keys = set(current_map.keys()) & set(prior_map.keys())

    improved = []
    deteriorated = []
    new_reds = []
    resolved_reds = []
    deltas = []

    for key in common_keys:
        cur = current_map[key]
        pri = prior_map[key]
        cur_avg = cur.get("avg")
        pri_avg = pri.get("avg")
        if cur_avg is None or pri_avg is None or pri_avg == 0:
            continue

        delta_pct = (cur_avg - pri_avg) / abs(pri_avg) * 100
        direction = cur.get("direction", "higher")

        # Direction-aware: improvement depends on direction
        is_improved = (direction == "higher" and delta_pct > 2) or \
                      (direction != "higher" and delta_pct < -2)
        is_deteriorated = (direction == "higher" and delta_pct < -2) or \
                          (direction != "higher" and delta_pct > 2)

        entry = {
            "key": key,
            "name": cur.get("name", key),
            "delta_pct": round(delta_pct, 1),
            "current_avg": cur_avg,
            "prior_avg": pri_avg,
            "unit": cur.get("unit", ""),
        }
        deltas.append(entry)

        if is_improved:
            improved.append(entry)
        elif is_deteriorated:
            deteriorated.append(entry)

        # Track status transitions
        if cur.get("fy_status") == "red" and pri.get("fy_status") != "red":
            new_reds.append(entry)
        elif cur.get("fy_status") != "red" and pri.get("fy_status") == "red":
            resolved_reds.append(entry)

    # Sort by magnitude
    improved.sort(key=lambda x: -abs(x["delta_pct"]))
    deteriorated.sort(key=lambda x: -abs(x["delta_pct"]))

    # Summary narrative
    parts = []
    if improved:
        parts.append(f"{len(improved)} KPIs improved")
    if deteriorated:
        parts.append(f"{len(deteriorated)} deteriorated")
    if new_reds:
        names = ", ".join(e["name"] for e in new_reds[:3])
        parts.append(f"{len(new_reds)} new critical ({names})")
    if resolved_reds:
        names = ", ".join(e["name"] for e in resolved_reds[:3])
        parts.append(f"{len(resolved_reds)} resolved ({names})")

    summary = (
        f"Comparing {period_label_current} vs {period_label_prior}: "
        + "; ".join(parts) + "." if parts else
        f"No significant changes between {period_label_prior} and {period_label_current}."
    )

    return {
        "title": f"{period_label_current} vs {period_label_prior}",
        "summary": summary,
        "improved": improved[:5],
        "deteriorated": deteriorated[:5],
        "new_reds": new_reds,
        "resolved_reds": resolved_reds,
        "deltas": deltas,
    }


# ── Outlook ─────────────────────────────────────────────────────────────────

def generate_outlook(
    fingerprint: list[dict],
    signals: list[dict],
    domain_narratives: dict,
) -> list[str]:
    """Generate 3-5 forward-looking outlook bullets."""
    bullets = []

    # Streak risk
    streakers = [(k, _red_streak(k)) for k in fingerprint]
    streakers = [(k, s) for k, s in streakers if s >= 3]
    streakers.sort(key=lambda x: -x[1])
    if streakers:
        k, s = streakers[0]
        bullets.append(
            f"Monitor {k['name']} closely — a {s}-month red streak is the "
            f"highest-priority operational risk."
        )

    # Green traps
    traps = [k for k in fingerprint if k.get("fy_status") == "green" and _is_declining(k)]
    if traps:
        bullets.append(
            f"{traps[0]['name']} will likely move from green to amber within "
            f"60-90 days if the current declining trajectory is not reversed."
        )

    # Volume of reds
    red_kpis = [k for k in fingerprint if k.get("fy_status") == "red"]
    if len(red_kpis) >= 3:
        bullets.append(
            f"With {len(red_kpis)} critical KPIs, the board should request a "
            f"corrective action plan with accountable owners and measurable "
            f"30-day milestones."
        )

    # Protect greens
    green_kpis = [k for k in fingerprint if k.get("fy_status") == "green"]
    if green_kpis and red_kpis:
        bullets.append(
            f"Protect the {len(green_kpis)} on-target KPIs from resource diversion "
            f"toward problem areas — over-correction is a common intervention failure mode."
        )

    # Systemic domain issues
    for domain, info in domain_narratives.items():
        if info.get("red_count", 0) >= 2:
            bullets.append(
                f"{info['label']} has {info['red_count']} critical KPIs — "
                f"consider a focused operational review of this business area."
            )
            break

    if not bullets:
        bullets.append(
            "Continue monitoring the current KPI set — no acute risks detected."
        )

    return bullets[:5]


def _is_declining(kpi: dict) -> bool:
    """Check if a KPI's last 3 months show a declining trend."""
    vals = [m["value"] for m in (kpi.get("monthly") or []) if m.get("value") is not None]
    if len(vals) < 3:
        return False
    last3 = vals[-3:]
    if kpi.get("direction") == "higher":
        return last3[2] < last3[0]
    return last3[2] > last3[0]


# ── "So What" Business Context ──────────────────────────────────────────────

def build_so_what(kpi_key: str, avg, target, direction: str, unit: str = "") -> Optional[str]:
    """Generate business-context explanation for a KPI's current state."""
    key = kpi_key.lower()
    gap = _gap_pct(avg, target, direction)
    gap_str = f"{abs(gap):.0f}" if gap is not None else None

    if "nrr" in key or "net_revenue_retention" in key:
        if avg is not None and avg < 100:
            return "Below 100% means the customer base is contracting without new sales."
        if avg is not None and avg >= 110:
            return "Above 110% indicates strong expansion — existing customers are funding growth."
        return "NRR at 100-110%: stable but growth requires continuous new sales effort."

    if "churn" in key:
        annual = f"{avg * 12:.0f}" if avg else None
        if annual:
            return f"At this rate, ~{annual}% of the customer base churns annually."
        return "Churn rate impacts long-term revenue compounding."

    if "burn_multiple" in key or "burn multiple" in key:
        if avg:
            return f"Every ${avg:.1f} spent generates $1 of new ARR."
        return "Burn multiple measures capital efficiency of growth."

    if "gross_margin" in key or "gross margin" in key:
        if avg:
            return f"Each revenue dollar generates {avg:.0f} cents of gross profit."
        return "Gross margin determines the ceiling on long-term profitability."

    if "cac" in key and "payback" not in key:
        if gap_str:
            word = "more" if gap and gap < 0 else "less"
            return f"Acquiring each customer costs {gap_str}% {word} than target."
        return "CAC drives the efficiency of the growth engine."

    if "runway" in key:
        if avg:
            return f"At current burn, {avg:.0f} months of runway remaining."
        return "Runway determines strategic optionality."

    if gap is not None:
        if gap < 0:
            severity = "structurally significant" if abs(gap) > 15 else "manageable with targeted intervention"
            return f"{abs(gap):.1f}% below target — gap is {severity}."
        return f"{gap:.1f}% above target — a signal worth protecting."

    return None


# ── Talk Track Generation ───────────────────────────────────────────────────

def generate_talk_track(slide_type: str, data_context: dict) -> str:
    """Generate structured speaker notes for a slide."""

    if slide_type == "title":
        company = data_context.get("company_name", "the company")
        period = data_context.get("period_label", "")
        return (
            f"Welcome to the {company} performance review for {period}. "
            f"This deck is generated from live operational data using Axiom Intelligence. "
            f"Every number and narrative is grounded in actual KPI trends, not templates."
        )

    if slide_type == "health_summary":
        score = data_context.get("score", 0)
        label = data_context.get("label", "")
        n_red = data_context.get("n_red", 0)
        n_green = data_context.get("n_green", 0)
        return (
            f"Health Score: {score}/100 — {label}. "
            f"{n_green} KPIs are on target, {n_red} need attention. "
            f"The score weights momentum at 30%, target achievement at 40%, and risk at 30%. "
            f"Let's walk through the specific areas."
        )

    if slide_type == "causal_analysis":
        narratives = data_context.get("narratives", [])
        if narratives:
            top = narratives[0]
            return (
                f"The most critical item is {top['kpi_name']}. "
                f"{top['narrative']} "
                f"Action: {top['actions'][0] if top.get('actions') else 'Under investigation.'}"
            )
        return "No critical causal chains identified this period."

    if slide_type == "signals":
        signals = data_context.get("signals", [])
        parts = []
        for s in signals[:3]:
            parts.append(f"- {s['title']}: {s['body'][:120]}")
        return "Hidden signals to discuss:\n" + "\n".join(parts) if parts else "No hidden signals detected."

    if slide_type == "domain":
        domain_info = data_context.get("domain_info", {})
        label = domain_info.get("label", "")
        story = domain_info.get("story", "")
        return f"{label}: {story}"

    if slide_type == "actions":
        return (
            "These corrective actions are ranked by composite criticality score. "
            "Each action is data-grounded — derived from actual trend analysis and "
            "causal chain validation, not generic templates."
        )

    if slide_type == "outlook":
        return (
            "These outlook items are derived from signal detection algorithms "
            "that identify structural patterns in the data: red streaks, green traps, "
            "growth-retention divergence, and domain clustering."
        )

    if slide_type == "takeaways":
        return (
            "Close with the key board asks. Focus on decisions that need endorsement, "
            "resources that need approval, and risks that need acknowledgement."
        )

    return ""
