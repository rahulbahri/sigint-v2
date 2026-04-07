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
  7. Uses Claude API to generate company-specific corrective actions
     grounded in the actual data context (not generic templates)

Pure-functional: no DB access. All data passed as arguments.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from typing import Any, Optional

from core.kpi_defs import ALL_CAUSATION_RULES, KPI_DEFS, EXTENDED_ONTOLOGY_METRICS

log = logging.getLogger("narrative_engine")

# ── AI action cache (thread-safe, TTL-based) ────────────────────────────────
# Caches Claude-generated actions per (kpi_key, data_hash) for 1 hour to
# avoid repeated API calls for the same KPI state.
_AI_ACTION_CACHE: dict[str, dict] = {}
_AI_CACHE_LOCK = threading.Lock()
_AI_CACHE_TTL = 3600  # 1 hour

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
        # DATA-DRIVEN SELF-ANALYSIS FALLBACK:
        # No upstream parent is deteriorating. Analyze the KPI's own behavior
        # and its downstream impact to provide actionable context.
        downstream = ALL_CAUSATION_RULES.get(kpi_key, {}).get("downstream_impact", [])
        at_risk_downstream = []
        for ds in downstream:
            ds_trend = _compute_trend(ds, time_series, directions.get(ds, "higher"))
            if ds_trend["is_deteriorating"]:
                at_risk_downstream.append((ds, ds_trend["delta_pct"]))

        if at_risk_downstream:
            ds_names = ", ".join(f"{_friendly(d)} ({dp:+.1f}%)" for d, dp in at_risk_downstream[:3])
            narrative_parts.append(
                f"No upstream deterioration detected — this appears to be a primary driver. "
                f"Downstream KPIs already impacted: {ds_names}."
            )
        elif own_trend["available"] and abs(own_trend["delta_pct"] or 0) < 2:
            narrative_parts.append(
                f"This KPI is stable (only {abs(own_trend['delta_pct']):.1f}% change) but below target. "
                f"This is a structural gap rather than an active deterioration — review whether the target is realistic or if operational changes are needed."
            )
        else:
            narrative_parts.append(
                "No upstream KPI deterioration detected — the issue may be driven by external factors "
                "(market conditions, competitive pressure, or operational capacity constraints) "
                "not captured in the current KPI framework."
            )

    if rejected:
        narrative_parts.append(f"Rejected hypothesis: {rejected[0]}")

    # Contextual action
    if confirmed:
        top_cause = confirmed[0]
        action = (
            f"Investigate {top_cause['name']} first — it shows a {abs(top_cause['delta_pct']):.1f}% "
            f"deterioration that is driving {kpi_name}. "
        )
        template_actions = _CORRECTIVE_ACTIONS.get(top_cause["kpi"], [])
        if template_actions:
            action += template_actions[0]
    elif at_risk_downstream if 'at_risk_downstream' in dir() else False:
        action = (
            f"Address {kpi_name} urgently — it is a primary driver affecting "
            f"{len(at_risk_downstream)} downstream KPIs. "
        )
        template_actions = _CORRECTIVE_ACTIONS.get(kpi_key, [])
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


# ── AI-powered action generation ─────────────────────────────────────────────

def generate_ai_actions(
    kpi_key: str,
    kpi_name: str,
    unit: str,
    direction: str,
    current_value: float | None,
    target_value: float | None,
    time_series: list[dict],
    confirmed_causes: list[dict],
    downstream_impact: list[str],
    stage: str = "series_a",
    company_name: str = "",
    benchmark: dict | None = None,
    template_actions: list[str] | None = None,
) -> list[str]:
    """Generate company-specific corrective actions using Claude API.

    Uses the full data context (actual values, confirmed causal chain,
    trend direction, benchmark position, company stage) to produce
    actionable, grounded recommendations -- not generic templates.

    Returns a list of 3 action strings, or falls back to template_actions
    if the Claude API is unavailable.
    """
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not api_key:
        log.warning("[AI Actions] ANTHROPIC_API_KEY not set, falling back to templates")
        return template_actions or []

    # Check cache first
    cache_key = f"{kpi_key}:{current_value}:{target_value}:{stage}"
    with _AI_CACHE_LOCK:
        cached = _AI_ACTION_CACHE.get(cache_key)
        if cached and time.time() < cached["expires_at"]:
            log.info("[AI Actions] Cache hit for %s", kpi_key)
            return cached["actions"]

    # Build the data context for Claude
    trend_desc = "stable"
    if len(time_series) >= 3:
        recent = [r["value"] for r in time_series[-3:] if r.get("value") is not None]
        prior = [r["value"] for r in time_series[-6:-3] if r.get("value") is not None]
        if recent and prior:
            r_avg = sum(recent) / len(recent)
            p_avg = sum(prior) / len(prior)
            if p_avg:
                delta = ((r_avg - p_avg) / abs(p_avg)) * 100
                if direction == "higher":
                    trend_desc = f"improving ({delta:+.1f}%)" if delta > 2 else f"declining ({delta:+.1f}%)" if delta < -2 else "stable"
                else:
                    trend_desc = f"improving ({delta:+.1f}%)" if delta < -2 else f"worsening ({delta:+.1f}%)" if delta > 2 else "stable"

    cause_context = ""
    if confirmed_causes:
        cause_lines = []
        for c in confirmed_causes[:3]:
            conf = "statistically confirmed by Granger causality" if c.get("confidence") == "granger_confirmed" else "directionally supported by trend data"
            cause_lines.append(f"  - {c['name']}: {abs(c['delta_pct']):.1f}% deterioration ({conf})")
        cause_context = "Confirmed upstream causes (verified against actual data):\n" + "\n".join(cause_lines)
    else:
        cause_context = "No upstream KPI deterioration confirmed -- issue may be direct/external."

    downstream_context = ""
    if downstream_impact:
        ds_names = ", ".join(_friendly(d) for d in downstream_impact[:5])
        downstream_context = f"Downstream KPIs at risk if not addressed: {ds_names}"

    benchmark_context = ""
    if benchmark:
        p50 = benchmark.get("p50")
        quartile = benchmark.get("quartile", "")
        if p50 is not None:
            benchmark_context = f"Peer benchmark (median for {stage.replace('_', ' ')}): {p50}. Company is in the {quartile} quartile."

    pct_off = ""
    if current_value is not None and target_value is not None and target_value != 0:
        gap = ((current_value - target_value) / abs(target_value)) * 100
        pct_off = f" ({gap:+.1f}% vs target)"

    prompt = f"""You are a senior CFO analyst. Generate exactly 3 specific, actionable corrective actions for a {stage.replace('_', ' ')} stage company{f' ({company_name})' if company_name else ''}.

KPI: {kpi_name}
Current value: {current_value}{pct_off}
Target: {target_value}
Unit: {unit}
Direction: {direction} is better
Trend (3-month): {trend_desc}

{cause_context}

{downstream_context}

{benchmark_context}

Rules:
- Each action must be specific and operational (not generic advice like "improve efficiency")
- Reference the ACTUAL numbers and confirmed causes above
- If upstream causes are confirmed, the first action must address the root cause, not the symptom
- If no upstream causes, focus on direct operational levers for this specific KPI
- Each action should be 1-2 sentences max
- Actions must be different from each other (no repetition)
- Format: just the 3 actions, one per line, numbered 1-3. No other text."""

    try:
        import httpx
        resp = httpx.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-3-5-haiku-20241022",
                "max_tokens": 300,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        if resp.status_code != 200:
            log.error("[AI Actions] Claude API error %d: %s", resp.status_code, resp.text[:200])
            return template_actions or []

        text = resp.json().get("content", [{}])[0].get("text", "")
        # Parse numbered lines
        actions = []
        for line in text.strip().split("\n"):
            line = line.strip()
            if not line:
                continue
            # Remove leading number/bullet
            for prefix in ["1.", "2.", "3.", "1)", "2)", "3)", "-", "*"]:
                if line.startswith(prefix):
                    line = line[len(prefix):].strip()
                    break
            if line:
                actions.append(line)

        if not actions:
            log.warning("[AI Actions] Claude returned empty actions, falling back to templates")
            return template_actions or []

        # Cache the result
        with _AI_CACHE_LOCK:
            _AI_ACTION_CACHE[cache_key] = {
                "actions": actions[:3],
                "expires_at": time.time() + _AI_CACHE_TTL,
            }

        log.info("[AI Actions] Generated %d actions for %s", len(actions), kpi_key)
        return actions[:3]

    except Exception as e:
        log.error("[AI Actions] Failed: %s, falling back to templates", e)
        return template_actions or []


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
