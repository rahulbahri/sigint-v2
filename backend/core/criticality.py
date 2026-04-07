"""
core/criticality.py — Composite Criticality Score Engine.

Replaces the single-signal "gap from target" ranking with a multi-signal
composite that considers:

  1. Gap Severity (25%)  — normalised distance from target
  2. Trend Momentum (25%)  — rate & direction of deterioration / improvement
  3. Business Impact (30%)  — downstream causal weight from the KPI DAG
  4. Domain Urgency (20%)  — business-area survival tier

Each signal is normalised to 0–100 before weighting.  Higher = more critical.

The composite is auditable: every KPI's score breakdown is returned so the
UI can show *why* a KPI ranks where it does.

Design principles:
  • Deterministic — same data in → same scores out.
  • No ML / training required — pure rules + graph traversal.
  • Flexible — weights are configurable per workspace (company_settings).
  • Auditable — full breakdown returned for every KPI.
  • Defensive — missing data degrades gracefully (falls back to 50/100 neutral).
"""

from __future__ import annotations

import math
from typing import Optional

from core.kpi_defs import ALL_CAUSATION_RULES, ONTOLOGY_DOMAIN, EXTENDED_ONTOLOGY_METRICS, KPI_DEFS


# ── Domain Urgency Tiers ─────────────────────────────────────────────────────
# Tier 1 (Existential): cash / risk — if these fail the business dies
# Tier 2 (Revenue Engine): growth / revenue — top-line health
# Tier 3 (Retention): retention — base durability
# Tier 4 (Profitability): profitability — margin sustainability
# Tier 5 (Efficiency): efficiency — operational optimisation

DOMAIN_URGENCY = {
    "cashflow":      100,
    "risk":          95,
    "growth":        80,
    "revenue":       75,
    "retention":     70,
    "profitability": 55,
    "efficiency":    45,
}

# Default composite weights (sum to 1.0)
DEFAULT_WEIGHTS = {
    "gap":    0.25,
    "trend":  0.25,
    "impact": 0.30,
    "domain": 0.20,
}


# ── Build unified domain lookup (core + extended KPIs) ───────────────────────
def _build_domain_map() -> dict:
    """Merge ONTOLOGY_DOMAIN + KPI_DEFS domains + EXTENDED_ONTOLOGY_METRICS."""
    dm: dict[str, str] = dict(ONTOLOGY_DOMAIN)
    for kdef in KPI_DEFS:
        if kdef.get("domain") and kdef["key"] not in dm:
            dm[kdef["key"]] = kdef["domain"]
    for ext in EXTENDED_ONTOLOGY_METRICS:
        if ext.get("domain") and ext["key"] not in dm:
            dm[ext["key"]] = ext["domain"]
    return dm


_DOMAIN_MAP = _build_domain_map()


# ── Build causal impact cache (count of downstream nodes reachable) ──────────
def _count_downstream(key: str, rules: dict, visited: set | None = None) -> int:
    """BFS count of unique downstream nodes reachable from `key`."""
    if visited is None:
        visited = set()
    if key in visited:
        return 0
    visited.add(key)
    children = rules.get(key, {}).get("downstream_impact", [])
    count = len(children)
    for child in children:
        count += _count_downstream(child, rules, visited)
    return count


def _build_impact_scores() -> dict:
    """
    Pre-compute impact score (0–100) for every KPI in the causation graph.
    Impact = normalised downstream reach.  More downstream nodes ⇒ higher impact.
    """
    raw: dict[str, int] = {}
    for key in ALL_CAUSATION_RULES:
        raw[key] = _count_downstream(key, ALL_CAUSATION_RULES)

    if not raw:
        return {}

    max_reach = max(raw.values()) or 1
    return {k: round((v / max_reach) * 100, 1) for k, v in raw.items()}


_IMPACT_SCORES = _build_impact_scores()


# ── Signal calculators ───────────────────────────────────────────────────────

def _gap_score(avg: float, target: float, direction: str) -> float:
    """
    Gap severity: how far is the KPI from target, normalised to 0–100.
    0 = on or above target (no criticality), 100 = maximally off-target.
    """
    if target == 0:
        if direction == "higher":
            return 0.0 if avg >= 0 else 50.0
        else:
            return 0.0 if avg <= 0 else 50.0

    if direction == "higher":
        if avg >= target:
            return 0.0  # on or above target
        gap_pct = (target - avg) / abs(target) * 100
    else:  # lower is better
        if avg <= target:
            return 0.0
        gap_pct = (avg - target) / abs(target) * 100

    # Clamp to 0–100 (>100% miss caps at 100)
    return min(max(gap_pct, 0.0), 100.0)


def _trend_score(
    time_series: list[float],
    direction: str,
) -> float:
    """
    Trend momentum score: 0 = rapidly improving, 100 = rapidly deteriorating.
    Uses linear regression slope over last 6 data points (or fewer if not available).
    Then normalises based on coefficient of variation.
    """
    if len(time_series) < 3:
        return 50.0  # insufficient data → neutral

    pts = time_series[-6:]  # last 6 months
    n = len(pts)
    mean_y = sum(pts) / n
    if mean_y == 0:
        return 50.0

    # Simple OLS slope: y = a + b*x
    mean_x = (n - 1) / 2.0
    num = sum((i - mean_x) * (pts[i] - mean_y) for i in range(n))
    den = sum((i - mean_x) ** 2 for i in range(n))
    if den == 0:
        return 50.0

    slope = num / den

    # Normalise slope as % of mean per month
    slope_pct = (slope / abs(mean_y)) * 100

    # For higher-is-better: negative slope = deteriorating (high criticality)
    # For lower-is-better: positive slope = deteriorating (high criticality)
    if direction == "higher":
        deterioration = -slope_pct
    else:
        deterioration = slope_pct

    # Map to 0–100: -10%/month → 0, 0 → 50, +10%/month → 100
    # Clamp to 0–100
    score = 50.0 + (deterioration * 5.0)  # ±10%/mo maps to 0/100
    return min(max(score, 0.0), 100.0)


def _impact_score(kpi_key: str, ontology_edges: dict = None) -> float:
    """
    Return causal impact score (0–100), weighted by Granger confidence.

    Base score = downstream node count (pre-computed).
    If ontology_edges are provided, weight by statistical confidence:
    - Granger-confirmed edges (p<0.05) count as full weight (1.0)
    - Expert-prior edges count as half weight (0.5)
    """
    base = _IMPACT_SCORES.get(kpi_key, 30.0)
    if not ontology_edges:
        return base

    # Count confirmed vs expert-prior edges for this KPI
    confirmed = 0
    expert = 0
    for (src, tgt), edge in ontology_edges.items():
        if src == kpi_key:
            tier = edge.get("confidence_tier", "expert_prior")
            if tier == "granger_confirmed":
                confirmed += 1
            else:
                expert += 1

    total_edges = confirmed + expert
    if total_edges == 0:
        return base

    # Weight: confirmed edges at 1.0, expert-prior at 0.5
    confidence_ratio = (confirmed * 1.0 + expert * 0.5) / total_edges
    # Scale the base score by confidence (0.5–1.0 range, never below half the base)
    return round(base * max(0.5, confidence_ratio), 1)


def _domain_score(kpi_key: str) -> float:
    """Return domain urgency score (0–100) based on the KPI's business domain."""
    domain = _DOMAIN_MAP.get(kpi_key)
    if domain:
        return DOMAIN_URGENCY.get(domain, 50.0)
    return 50.0  # unknown domain → neutral


def get_kpi_domain(kpi_key: str) -> str:
    """Return the business domain for a KPI, or 'other'."""
    return _DOMAIN_MAP.get(kpi_key, "other")


# ── Display labels ───────────────────────────────────────────────────────────

DOMAIN_LABELS = {
    "cashflow":      "Cash & Liquidity",
    "risk":          "Risk & Concentration",
    "growth":        "Growth & Acquisition",
    "revenue":       "Revenue Quality",
    "retention":     "Retention & Expansion",
    "profitability": "Profitability & Margins",
    "efficiency":    "Operational Efficiency",
    "other":         "Other Metrics",
}

DOMAIN_ORDER = ["cashflow", "risk", "growth", "revenue", "retention", "profitability", "efficiency", "other"]


# ── Main composite scoring ───────────────────────────────────────────────────

def compute_composite_criticality(
    kpi_avgs: dict[str, float],
    targets: dict[str, float],
    directions: dict[str, str],
    time_series_by_kpi: dict[str, list[float]],
    weights: dict[str, float] | None = None,
    ontology_edges: dict = None,
) -> list[dict]:
    """
    Compute composite criticality score for all KPIs that have both data and targets.

    Parameters
    ----------
    kpi_avgs : dict  — recent period average per KPI
    targets : dict   — target value per KPI
    directions : dict — "higher" or "lower" per KPI
    time_series_by_kpi : dict — ordered list of values per KPI
    weights : dict   — optional override {gap, trend, impact, domain} summing to 1.0
    ontology_edges : dict — {(src, tgt): {confidence_tier, granger_pval, ...}} for Granger weighting

    Returns
    -------
    List of dicts sorted by composite score descending (most critical first):
        {
            key, composite, rank,
            gap_score, trend_score, impact_score, domain_score,
            domain, domain_label,
            gap_pct, direction, avg, target,
            weights_used
        }
    """
    w = weights or DEFAULT_WEIGHTS
    # Normalise weights to sum to 1.0
    w_sum = sum(w.values()) or 1.0
    w_gap    = w.get("gap", 0.25) / w_sum
    w_trend  = w.get("trend", 0.25) / w_sum
    w_impact = w.get("impact", 0.30) / w_sum
    w_domain = w.get("domain", 0.20) / w_sum

    results: list[dict] = []

    for key, tval in targets.items():
        if tval is None:
            continue
        avg = kpi_avgs.get(key)
        if avg is None:
            continue

        dirn = directions.get(key, "higher")
        ts = time_series_by_kpi.get(key, [])

        g = _gap_score(avg, tval, dirn)
        t = _trend_score(ts, dirn)
        i = _impact_score(key, ontology_edges)
        d = _domain_score(key)

        composite = round(
            g * w_gap + t * w_trend + i * w_impact + d * w_domain,
            1,
        )

        # Raw gap_pct for display (always non-negative: how far below target)
        # A KPI above target (for higher-is-better) has gap_pct = 0, not negative
        if dirn == "higher":
            raw_gap = round(max(0.0, (1 - avg / tval) * 100), 1) if tval != 0 else 0.0
        else:
            raw_gap = round(max(0.0, (avg / tval - 1) * 100), 1) if tval != 0 else 0.0

        domain = get_kpi_domain(key)
        results.append({
            "key":           key,
            "composite":     composite,
            "gap_score":     round(g, 1),
            "trend_score":   round(t, 1),
            "impact_score":  round(i, 1),
            "domain_score":  round(d, 1),
            "domain":        domain,
            "domain_label":  DOMAIN_LABELS.get(domain, domain.title()),
            "gap_pct":       raw_gap,
            "direction":     dirn,
            "avg":           avg,
            "target":        tval,
        })

    # Exclude KPIs that are on or above target — they are NOT critical
    # gap_score == 0 means the KPI meets or exceeds its target (direction-aware)
    results = [r for r in results if r["gap_score"] > 0.0]

    # Sort by composite descending (most critical first)
    results.sort(key=lambda x: x["composite"], reverse=True)

    # Assign ranks
    for idx, r in enumerate(results):
        r["rank"] = idx + 1

    # Attach weights used for auditability
    weights_used = {"gap": round(w_gap, 3), "trend": round(w_trend, 3),
                    "impact": round(w_impact, 3), "domain": round(w_domain, 3)}
    for r in results:
        r["weights_used"] = weights_used

    return results


def group_by_domain(scored_kpis: list[dict]) -> list[dict]:
    """
    Group scored KPIs by business domain, ordered by domain urgency.

    Returns list of:
        {
            domain, domain_label, urgency,
            kpis: [...sorted by composite desc],
            worst_composite, avg_composite, count
        }
    """
    buckets: dict[str, list[dict]] = {}
    for kpi in scored_kpis:
        d = kpi.get("domain", "other")
        buckets.setdefault(d, []).append(kpi)

    groups = []
    for domain in DOMAIN_ORDER:
        kpis = buckets.get(domain)
        if not kpis:
            continue
        kpis_sorted = sorted(kpis, key=lambda x: x["composite"], reverse=True)
        composites = [k["composite"] for k in kpis_sorted]
        groups.append({
            "domain":          domain,
            "domain_label":    DOMAIN_LABELS.get(domain, domain.title()),
            "urgency":         DOMAIN_URGENCY.get(domain, 50),
            "kpis":            kpis_sorted,
            "worst_composite": max(composites),
            "avg_composite":   round(sum(composites) / len(composites), 1),
            "count":           len(kpis_sorted),
        })

    # Sort groups by worst_composite descending (most critical domain first)
    groups.sort(key=lambda g: g["worst_composite"], reverse=True)
    return groups
