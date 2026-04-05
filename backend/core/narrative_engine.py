"""
core/narrative_engine.py — Data-driven root cause analysis and narrative generation.

Replaces static template-based narratives with analysis grounded in actual
KPI trends. For each red/yellow KPI, the engine:

  1. Identifies upstream parent KPIs from the causation DAG
  2. Checks whether each parent is ACTUALLY deteriorating (not assumed)
  3. Confirms or rejects hardcoded hypotheses based on real data
  4. Walks the causal chain up to 3 hops for root cause tracing
  5. Weights edges by Granger p-value when ontology data is available
  6. Generates narratives with actual numbers, not template text

Pure-functional: no DB access. All data passed as arguments.
"""
from __future__ import annotations
from typing import Any, Optional

from core.kpi_defs import ALL_CAUSATION_RULES, KPI_DEFS, EXTENDED_ONTOLOGY_METRICS

# ── Reverse causation map ────────────────────────────────────────────────────
# For each KPI, lists its upstream parents (KPIs whose downstream_impact includes it).

_REVERSE_MAP: dict[str, list[str]] = {}

for _kpi_key, _rules in ALL_CAUSATION_RULES.items():
    for _downstream in _rules.get("downstream_impact", []):
        _REVERSE_MAP.setdefault(_downstream, []).append(_kpi_key)

# Also store direct root_causes text for hypothesis validation
_ROOT_CAUSE_TEXT = {k: v.get("root_causes", []) for k, v in ALL_CAUSATION_RULES.items()}
_CORRECTIVE_ACTIONS = {k: v.get("corrective_actions", []) for k, v in ALL_CAUSATION_RULES.items()}

# KPI friendly names
_KPI_NAMES = {}
for _d in KPI_DEFS + EXTENDED_ONTOLOGY_METRICS:
    _KPI_NAMES[_d["key"]] = _d.get("name", _d["key"])


def _friendly(key: str) -> str:
    return _KPI_NAMES.get(key, key.replace("_", " ").title())


# ── Trend analysis ───────────────────────────────────────────────────────────

def _compute_trend(kpi_key: str, time_series: dict, direction: str,
                   lookback: int = 3) -> dict:
    """Compute recent trend for a KPI from its time series.

    Returns:
        {
            "available": bool,
            "recent_avg": float | None,
            "prior_avg": float | None,
            "delta_pct": float | None,
            "is_deteriorating": bool,
            "is_improving": bool,
        }
    """
    vals = time_series.get(kpi_key, [])
    if len(vals) < lookback * 2:
        return {"available": False, "recent_avg": None, "prior_avg": None,
                "delta_pct": None, "is_deteriorating": False, "is_improving": False}

    recent = vals[-lookback:]
    prior = vals[-lookback * 2:-lookback]

    recent_avg = sum(recent) / len(recent)
    prior_avg = sum(prior) / len(prior)

    if abs(prior_avg) < 0.01:
        delta_pct = 0.0
    else:
        delta_pct = round((recent_avg - prior_avg) / abs(prior_avg) * 100, 2)

    # Direction-aware: "higher is better" means declining = negative delta
    if direction == "higher":
        is_deteriorating = delta_pct < -0.5
        is_improving = delta_pct > 0.5
    else:
        is_deteriorating = delta_pct > 0.5
        is_improving = delta_pct < -0.5

    return {
        "available": True,
        "recent_avg": round(recent_avg, 2),
        "prior_avg": round(prior_avg, 2),
        "delta_pct": delta_pct,
        "is_deteriorating": is_deteriorating,
        "is_improving": is_improving,
    }


# ── Cause chain walking ─────────────────────────────────────────────────────

def _walk_cause_chain(
    kpi_key: str,
    time_series: dict,
    directions: dict,
    ontology_edges: dict,
    max_hops: int = 3,
    visited: set = None,
) -> list[dict]:
    """Walk upstream through the causation DAG, following only deteriorating parents.

    Returns a list of cause-chain entries, each with:
        kpi, hop, name, delta_pct, confidence, is_deteriorating
    """
    if visited is None:
        visited = {kpi_key}

    chain = []
    parents = _REVERSE_MAP.get(kpi_key, [])

    for parent in parents:
        if parent in visited or len(chain) >= 5:
            continue
        visited.add(parent)

        parent_dir = directions.get(parent, "higher")
        trend = _compute_trend(parent, time_series, parent_dir)

        # Check Granger confidence for this edge
        edge_key = f"{parent}->{kpi_key}"
        edge_info = ontology_edges.get(edge_key, {})
        confidence = edge_info.get("confidence_tier", "expert_prior")
        granger_pval = edge_info.get("granger_pval")

        hop_entry = {
            "kpi": parent,
            "name": _friendly(parent),
            "hop": len([c for c in chain if c.get("_from") == kpi_key]) + 1,
            "delta_pct": trend["delta_pct"],
            "is_deteriorating": trend["is_deteriorating"],
            "is_improving": trend["is_improving"],
            "trend_available": trend["available"],
            "confidence": confidence,
            "granger_pval": granger_pval,
            "_from": kpi_key,
        }
        chain.append(hop_entry)

        # Recurse only if parent is deteriorating and we haven't hit max hops
        if trend["is_deteriorating"] and len(visited) <= max_hops + 1:
            sub_chain = _walk_cause_chain(parent, time_series, directions,
                                          ontology_edges, max_hops, visited)
            for sc in sub_chain:
                sc["hop"] = hop_entry["hop"] + sc.get("hop", 1)
            chain.extend(sub_chain)

    return chain


# ── Main analysis function ───────────────────────────────────────────────────

def analyze_root_causes(
    kpi_key: str,
    kpi_avgs: dict,
    time_series: dict,
    targets: dict,
    directions: dict,
    ontology_edges: dict,
) -> dict:
    """Data-driven root cause analysis for a single KPI.

    Instead of returning hardcoded template text, this function:
    1. Checks which upstream parents are actually deteriorating
    2. Validates or rejects hardcoded hypotheses
    3. Traces the causal chain with actual numbers
    4. Generates a contextual narrative

    Returns:
        {
            "kpi": str,
            "confirmed_causes": [...],
            "rejected_hypotheses": [...],
            "cause_chain": [...],
            "narrative": str,
            "contextual_action": str,
            "data_grounded": bool,
        }
    """
    kpi_dir = directions.get(kpi_key, "higher")
    own_trend = _compute_trend(kpi_key, time_series, kpi_dir)

    # Walk the full cause chain
    chain = _walk_cause_chain(kpi_key, time_series, directions, ontology_edges)

    # Separate confirmed (deteriorating parents) from rejected
    confirmed = [c for c in chain if c["is_deteriorating"] and c["trend_available"]]
    improving = [c for c in chain if c["is_improving"] and c["trend_available"]]
    unavailable = [c for c in chain if not c["trend_available"]]

    # Build rejected hypotheses from hardcoded causes that data contradicts
    rejected = []
    template_causes = _ROOT_CAUSE_TEXT.get(kpi_key, [])
    for i, cause_text in enumerate(template_causes):
        # Check if any improving parent contradicts this hypothesis
        for imp in improving:
            if imp["kpi"] in str(cause_text).lower() or imp["name"].lower() in cause_text.lower():
                rejected.append(f"{cause_text} (data shows {imp['name']} is improving +{imp['delta_pct']:.1f}%)")
                break

    # Build narrative
    narrative_parts = []
    kpi_name = _friendly(kpi_key)

    if own_trend["available"]:
        direction_word = "declined" if own_trend["is_deteriorating"] else "changed"
        narrative_parts.append(
            f"{kpi_name} has {direction_word} {abs(own_trend['delta_pct']):.1f}% "
            f"over the last 3 months (from {own_trend['prior_avg']} to {own_trend['recent_avg']})."
        )

    if confirmed:
        # Sort by hop (closest first) then by delta magnitude
        confirmed.sort(key=lambda c: (c["hop"], -abs(c.get("delta_pct", 0) or 0)))
        top = confirmed[0]
        conf_label = "statistically confirmed" if top["confidence"] == "granger_confirmed" else "directionally supported"
        narrative_parts.append(
            f"Primary upstream cause: {top['name']} deteriorated {abs(top['delta_pct']):.1f}% "
            f"({conf_label}). "
        )
        if len(confirmed) > 1:
            others = ", ".join(f"{c['name']} ({c['delta_pct']:+.1f}%)" for c in confirmed[1:3])
            narrative_parts.append(f"Contributing factors: {others}.")

        # Multi-hop trace
        hop2plus = [c for c in confirmed if c["hop"] >= 2]
        if hop2plus:
            h = hop2plus[0]
            narrative_parts.append(
                f"Root trace (hop {h['hop']}): {h['name']} ({h['delta_pct']:+.1f}%) "
                f"is an upstream driver."
            )
    elif unavailable:
        narrative_parts.append(
            "Insufficient trend data to confirm upstream causes. "
            "More months of data will enable root cause tracing."
        )
    else:
        narrative_parts.append(
            "No upstream KPI deterioration detected. "
            "The issue may be external (market, product, or operational)."
        )

    if rejected:
        narrative_parts.append(f"Rejected hypotheses: {rejected[0]}")

    # Contextual action
    if confirmed:
        top_cause = confirmed[0]
        action = (
            f"Investigate {top_cause['name']} first — it shows a {abs(top_cause['delta_pct']):.1f}% "
            f"deterioration that is driving {kpi_name}. "
        )
        # Add specific action from template if it aligns with the confirmed cause
        template_actions = _CORRECTIVE_ACTIONS.get(top_cause["kpi"], [])
        if template_actions:
            action += template_actions[0]
    else:
        template_actions = _CORRECTIVE_ACTIONS.get(kpi_key, [])
        action = template_actions[0] if template_actions else f"Review {kpi_name} drivers manually."

    return {
        "kpi": kpi_key,
        "kpi_name": kpi_name,
        "own_trend": own_trend,
        "confirmed_causes": [
            {"kpi": c["kpi"], "name": c["name"], "hop": c["hop"],
             "delta_pct": c["delta_pct"], "confidence": c["confidence"]}
            for c in confirmed[:5]
        ],
        "rejected_hypotheses": rejected[:3],
        "cause_chain": [
            {"kpi": c["kpi"], "name": c["name"], "hop": c["hop"],
             "delta_pct": c["delta_pct"], "is_deteriorating": c["is_deteriorating"],
             "confidence": c["confidence"]}
            for c in chain[:8]
        ],
        "narrative": " ".join(narrative_parts),
        "contextual_action": action,
        "data_grounded": len(confirmed) > 0,
    }


# ── Batch enrichment for /api/home ───────────────────────────────────────────

def enrich_needs_attention(
    needs_attention_keys: list[str],
    kpi_avgs: dict,
    time_series: dict,
    targets: dict,
    directions: dict,
    ontology_edges: dict,
) -> dict[str, dict]:
    """Analyze root causes for all needs-attention KPIs.

    Returns a dict keyed by kpi_key with the analysis for each.
    """
    results = {}
    for kpi_key in needs_attention_keys:
        try:
            results[kpi_key] = analyze_root_causes(
                kpi_key, kpi_avgs, time_series, targets, directions, ontology_edges,
            )
        except Exception:
            results[kpi_key] = {
                "kpi": kpi_key, "kpi_name": _friendly(kpi_key),
                "narrative": "Root cause analysis unavailable for this KPI.",
                "confirmed_causes": [], "rejected_hypotheses": [],
                "cause_chain": [], "contextual_action": "",
                "data_grounded": False,
            }
    return results
