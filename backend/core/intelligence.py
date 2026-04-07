"""
core/intelligence.py — Narrative intelligence layer for the Home Screen.

Provides four enrichment functions consumed by /api/home:
  1. benchmark_position()  — peer-relative context for each KPI
  2. streak_detection()    — consecutive-month miss counts
  3. domain_narratives()   — one-sentence diagnosis per business domain
  4. period_comparison()   — what improved / deteriorated vs prior period

All functions are pure (no DB access) and deterministic (same data → same output).
"""
from __future__ import annotations

from typing import Optional

from core.kpi_defs import BENCHMARKS, EXTENDED_ONTOLOGY_METRICS, KPI_DEFS
from core.health_score import _friendly_name


# ── Stage key normalisation ──────────────────────────────────────────────────
_STAGE_ALIASES = {
    "seed":     "seed",
    "pre-seed": "seed",
    "pre_seed": "seed",
    "series a": "series_a",
    "series_a": "series_a",
    "series b": "series_b",
    "series_b": "series_b",
    "series c": "series_c",
    "series_c": "series_c",
    "growth":   "series_c",
}

_STAGE_DISPLAY = {
    "seed":     "Seed",
    "series_a": "Series A",
    "series_b": "Series B",
    "series_c": "Series C",
}


def _normalise_stage(raw: Optional[str]) -> str:
    """Map user-entered funding stage to a BENCHMARKS key."""
    if not raw:
        return "series_a"
    return _STAGE_ALIASES.get(raw.strip().lower().replace("-", " "), "series_a")


# ═════════════════════════════════════════════════════════════════════════════
# 1.  BENCHMARK POSITIONING
# ═════════════════════════════════════════════════════════════════════════════

def benchmark_position(
    kpi_key: str,
    avg: Optional[float],
    direction: str,
    stage: str,
) -> Optional[dict]:
    """
    Return benchmark context for a single KPI.

    Returns dict with:
        quartile   – "bottom", "below_median", "above_median", "top"
        peer_p25   – 25th-percentile value for the stage
        peer_p50   – median value for the stage
        peer_p75   – 75th-percentile value for the stage
        label      – human-readable sentence
    """
    bench = BENCHMARKS.get(kpi_key, {}).get(stage)
    if not bench or avg is None:
        return None

    p25 = bench.get("p25")
    p50 = bench.get("p50")
    p75 = bench.get("p75")
    if p25 is None or p50 is None or p75 is None:
        return None

    stage_label = _STAGE_DISPLAY.get(stage, stage)
    name = _friendly_name(kpi_key)

    # Determine quartile (direction-aware)
    if direction == "lower":
        # Lower is better: below p25 = top quartile
        if avg <= p25:
            quartile = "top"
        elif avg <= p50:
            quartile = "above_median"
        elif avg <= p75:
            quartile = "below_median"
        else:
            quartile = "bottom"
    else:
        if avg >= p75:
            quartile = "top"
        elif avg >= p50:
            quartile = "above_median"
        elif avg >= p25:
            quartile = "below_median"
        else:
            quartile = "bottom"

    _Q_TEXT = {
        "bottom":       f"bottom quartile for {stage_label} peers (below p25 of {p25})",
        "below_median": f"below median for {stage_label} peers (median: {p50})",
        "above_median": f"above median for {stage_label} peers (median: {p50})",
        "top":          f"top quartile for {stage_label} peers (above p75 of {p75})",
    }

    return {
        "quartile":    quartile,
        "peer_p25":    p25,
        "peer_p50":    p50,
        "peer_p75":    p75,
        "stage":       stage,
        "stage_label": stage_label,
        "label":       _Q_TEXT[quartile],
    }


# ═════════════════════════════════════════════════════════════════════════════
# 2.  CONSECUTIVE-MONTH STREAK DETECTION
# ═════════════════════════════════════════════════════════════════════════════

def streak_detection(
    kpi_key: str,
    monthly_values: list[dict],
    target: Optional[float],
    direction: str,
) -> dict:
    """
    Count consecutive months the KPI missed target (walking backwards from
    the most recent month).

    monthly_values: list of {"period": "YYYY-MM", "value": float} sorted by
                    period ascending.

    Returns dict with:
        miss_streak   – int, 0 if currently on target
        streak_label  – human-readable label or None
        is_structural – True if streak >= 6 months
    """
    if not monthly_values or target is None:
        return {"miss_streak": 0, "streak_label": None, "is_structural": False}

    streak = 0
    for entry in reversed(monthly_values):
        v = entry["value"]
        if v is None:
            break
        # Check if this month missed target
        if direction == "lower":
            missed = v > target * 1.02 if target >= 0 else v > target * 0.98
        else:
            missed = v < target * 0.98 if target >= 0 else v < target * 1.02
        if missed:
            streak += 1
        else:
            break

    label = None
    if streak >= 2:
        name = _friendly_name(kpi_key)
        if streak >= 12:
            label = (
                f"{name} has missed target for {streak} consecutive months "
                "— this is structural, not cyclical. Recovery requires "
                "process-level intervention, not incremental fixes."
            )
        elif streak >= 6:
            label = (
                f"{name} has missed target for {streak} consecutive months "
                "— a sustained pattern indicating a systemic issue. "
                "Escalate before the gap compounds further."
            )
        elif streak >= 3:
            label = (
                f"{name} has missed target for {streak} consecutive months. "
                "Three or more months establishes a trend — investigate "
                "root causes before this becomes structural."
            )
        else:
            label = (
                f"{name} has missed target for {streak} consecutive months. "
                "Monitor closely."
            )

    return {
        "miss_streak":   streak,
        "streak_label":  label,
        "is_structural": streak >= 6,
    }


# ═════════════════════════════════════════════════════════════════════════════
# 3.  DOMAIN-LEVEL NARRATIVES
# ═════════════════════════════════════════════════════════════════════════════

_DOMAIN_CONTEXT = {
    "cashflow": {
        "name":   "Cash & Liquidity",
        "risk":   "Cash problems are existential — they constrain every other initiative.",
        "action": "Tighten collections, extend payables, and stress-test runway under downside scenarios.",
    },
    "risk": {
        "name":   "Risk & Concentration",
        "risk":   "Concentration risk makes revenue fragile — a single churn event can move the P&L.",
        "action": "Diversify the customer base and implement key-account retention programmes.",
    },
    "growth": {
        "name":   "Growth & Acquisition",
        "risk":   "Growth shortfalls compound — each missed month widens the gap to plan.",
        "action": "Focus on pipeline quality over volume and reduce sales cycle length.",
    },
    "revenue": {
        "name":   "Revenue Quality",
        "risk":   "Revenue quality issues mean growth is being bought, not earned.",
        "action": "Protect pricing power and shift mix toward recurring, high-margin revenue.",
    },
    "retention": {
        "name":   "Retention & Expansion",
        "risk":   "Retention failures materialise in revenue 2-3 quarters later — by the time they appear in the income statement, the damage is done.",
        "action": "Activate expansion playbooks for top accounts and instrument early-warning churn signals.",
    },
    "profitability": {
        "name":   "Profitability & Margins",
        "risk":   "Margin erosion reduces runway and investor confidence simultaneously.",
        "action": "Audit cost structure for fixed-to-variable conversion opportunities.",
    },
    "efficiency": {
        "name":   "Operational Efficiency",
        "risk":   "Efficiency gaps drain cash without producing proportional growth.",
        "action": "Benchmark unit economics against stage peers and set guardrails.",
    },
}


def domain_narratives(
    domain_groups: list[dict],
    total_red: int,
) -> list[dict]:
    """
    Generate a data-specific diagnosis for each pressured business domain.

    domain_groups: list from group_by_domain() — each has domain, count,
                   worst_composite, avg_composite, kpis (list with key, gap_pct, etc.).
    total_red:     total number of red KPIs across all domains.

    Returns list of dicts with:
        domain, domain_label, narrative, urgency_note
    """
    results = []
    for dg in domain_groups:
        domain = dg["domain"]
        count  = dg["count"]
        ctx    = _DOMAIN_CONTEXT.get(domain, {})
        name   = ctx.get("name", dg.get("domain_label", domain.title()))
        kpi_list = dg.get("kpis", [])

        # Build data-specific narrative from actual KPI details
        concentration = count / total_red if total_red > 0 else 0

        # Sort KPIs by gap severity (worst first)
        sorted_kpis = sorted(kpi_list, key=lambda k: abs(k.get("gap_pct", 0) or 0), reverse=True)

        if count >= 2 and sorted_kpis:
            # Name the specific KPIs and their gap percentages
            worst = sorted_kpis[0]
            worst_name = _friendly_name(worst.get("key", ""))
            worst_gap = abs(worst.get("gap_pct", 0) or 0)
            kpi_names = ", ".join(_friendly_name(k.get("key", "")) for k in sorted_kpis[:4])

            if concentration >= 0.5:
                narrative = (
                    f"{name} is the most pressured domain with {count} of {total_red} "
                    f"critical metrics: {kpi_names}. "
                    f"{worst_name} is the most concerning with a {worst_gap:.0f}% gap to target. "
                    f"{ctx.get('risk', '')}"
                )
            elif concentration >= 0.3:
                narrative = (
                    f"{name} carries {count} critical metrics: {kpi_names}. "
                    f"{worst_name} has the largest gap at {worst_gap:.0f}% from target. "
                    f"{ctx.get('action', '')}"
                )
            else:
                narrative = (
                    f"{name} has {count} metrics in the critical zone: {kpi_names}. "
                    f"{worst_name} leads at {worst_gap:.0f}% gap. {ctx.get('action', '')}"
                )
        elif count == 1 and sorted_kpis:
            single = sorted_kpis[0]
            single_name = _friendly_name(single.get("key", ""))
            single_gap = abs(single.get("gap_pct", 0) or 0)
            narrative = (
                f"{name} has 1 critical metric: {single_name} "
                f"({single_gap:.0f}% gap to target). "
                "Monitor for spread to adjacent KPIs in this domain."
            )
        else:
            narrative = (
                f"{name} has {count} critical metric(s). "
                f"{ctx.get('action', 'Review the underlying drivers.')}"
            )

        results.append({
            "domain":       domain,
            "domain_label": name,
            "narrative":    narrative,
            "urgency_note": ctx.get("action", ""),
        })

    return results


# ═════════════════════════════════════════════════════════════════════════════
# 4.  PERIOD-OVER-PERIOD COMPARISON
# ═════════════════════════════════════════════════════════════════════════════

def period_comparison(
    kpi_monthly: dict,
    targets_map: dict,
    prev_period: Optional[str] = None,
    curr_period: Optional[str] = None,
) -> dict:
    """
    Compare KPI status between the two most recent periods.

    kpi_monthly: {kpi_key: [{"period": "YYYY-MM", "value": float}, ...]}
    targets_map: {kpi_key: {"target": float, "direction": str}}

    Returns dict with:
        improved       – list of {key, name, prev, curr, delta, unit}
        deteriorated   – list of {key, name, prev, curr, delta, unit}
        newly_critical – list of KPI keys that moved into red
        recovered      – list of KPI keys that moved out of red
        narrative      – one-paragraph summary
    """
    improved = []
    deteriorated = []
    newly_critical = []
    recovered = []

    for kpi_key, entries in kpi_monthly.items():
        if len(entries) < 2:
            continue
        sorted_entries = sorted(entries, key=lambda x: x["period"])
        curr_val = sorted_entries[-1]["value"]
        prev_val = sorted_entries[-2]["value"]
        curr_pd  = sorted_entries[-1]["period"]
        prev_pd  = sorted_entries[-2]["period"]

        if curr_val is None or prev_val is None:
            continue

        t = targets_map.get(kpi_key, {})
        target = t.get("target")
        direction = t.get("direction", "higher")
        unit = t.get("unit", "")

        delta = round(curr_val - prev_val, 2)
        name = _friendly_name(kpi_key)

        # Determine if this moved in the right direction
        if direction == "lower":
            is_better = curr_val < prev_val
        else:
            is_better = curr_val > prev_val

        # Only include meaningful movements (>0.5% of value to filter noise)
        abs_prev = abs(prev_val) if prev_val != 0 else 1
        pct_change = abs(delta) / abs_prev * 100
        if pct_change < 0.5:
            continue

        entry = {
            "key":   kpi_key,
            "name":  name,
            "prev":  round(prev_val, 2),
            "curr":  round(curr_val, 2),
            "delta": delta,
            "unit":  unit,
            "prev_period": prev_pd,
            "curr_period": curr_pd,
        }

        if is_better:
            improved.append(entry)
        else:
            deteriorated.append(entry)

        # Check for status transitions (red ↔ non-red)
        if target is not None:
            from core.health_score import _is_critical, _is_on_target
            was_critical = _is_critical(prev_val, target, direction)
            now_critical = _is_critical(curr_val, target, direction)
            if now_critical and not was_critical:
                newly_critical.append(kpi_key)
            elif was_critical and not now_critical:
                recovered.append(kpi_key)

    # Sort by absolute delta magnitude (biggest movers first)
    improved.sort(key=lambda x: abs(x["delta"]), reverse=True)
    deteriorated.sort(key=lambda x: abs(x["delta"]), reverse=True)

    # Build narrative
    parts = []
    n_imp = len(improved)
    n_det = len(deteriorated)

    if n_imp > 0 and n_det > 0:
        parts.append(
            f"Net position: {n_imp} KPI{'s' if n_imp != 1 else ''} improved, "
            f"{n_det} deteriorated."
        )
    elif n_imp > 0:
        parts.append(f"All {n_imp} moving KPI{'s' if n_imp != 1 else ''} improved.")
    elif n_det > 0:
        parts.append(f"All {n_det} moving KPI{'s' if n_det != 1 else ''} deteriorated — no positive offsets.")

    if improved:
        best = improved[0]
        parts.append(f"Biggest win: {best['name']} moved from {best['prev']} to {best['curr']}.")
    if deteriorated:
        worst = deteriorated[0]
        parts.append(f"Biggest concern: {worst['name']} moved from {worst['prev']} to {worst['curr']}.")

    if newly_critical:
        names = ", ".join(_friendly_name(k) for k in newly_critical[:3])
        parts.append(f"Newly critical: {names}.")
    if recovered:
        names = ", ".join(_friendly_name(k) for k in recovered[:3])
        parts.append(f"Recovered from critical: {names}.")

    return {
        "improved":       improved[:10],
        "deteriorated":   deteriorated[:10],
        "newly_critical": newly_critical,
        "recovered":      recovered,
        "narrative":      " ".join(parts) if parts else "No significant movements detected.",
    }


# ═════════════════════════════════════════════════════════════════════════════
# 5.  STAGE-AWARE CORRECTIVE ACTIONS
# ═════════════════════════════════════════════════════════════════════════════

_STAGE_CONTEXT = {
    "seed": {
        "prefix": "At Seed stage with a small customer base",
        "themes": {
            "churn_rate":            "losing even 1-2 accounts has outsized impact — prioritise 1:1 founder-led retention over scalable programmes.",
            "cac_payback":           "capital is scarce — focus on organic and referral channels before scaling paid acquisition.",
            "burn_multiple":         "runway preservation is existential — cut discretionary spend before optimising unit economics.",
            "revenue_growth":        "growth at this stage is about product-market fit signals, not scale — focus on conversion rate and repeat usage over volume.",
            "customer_concentration":"with few customers, concentration is natural — focus on diversifying the pipeline rather than restructuring the base.",
            "nrr":                   "expansion in a small base is about depth of engagement — ensure the product solves a must-have problem before upselling.",
        },
    },
    "series_a": {
        "prefix": "At Series A with early product-market fit",
        "themes": {
            "churn_rate":            "retention is the proof point investors watch — instrument early-warning signals and assign a dedicated CS resource.",
            "cac_payback":           "payback period determines how fast you can reinvest — test 2-3 channels and double down on the one with best LTV:CAC.",
            "burn_multiple":         "investors expect efficient growth — target <2x burn multiple and track monthly.",
            "revenue_growth":        "growth rate is the primary valuation driver — prioritise pipeline velocity and expansion revenue.",
            "sales_efficiency":      "each S&M dollar must produce measurable ARR — track revenue per rep and cost per SQL weekly.",
        },
    },
    "series_b": {
        "prefix": "At Series B scaling operations",
        "themes": {
            "churn_rate":            "at scale, a 1% churn improvement compounds to significant ARR — invest in automated health scoring and proactive outreach.",
            "cac_payback":           "efficiency matters as much as growth now — benchmark against peers (target <18 months) and optimise by segment.",
            "burn_multiple":         "path to profitability should be visible — model the break-even timeline and communicate it to the board.",
            "gross_margin":          "margin expansion is expected at this stage — renegotiate infrastructure contracts and review pricing tiers.",
            "operating_margin":      "operating leverage should be emerging — fixed costs should grow slower than revenue.",
        },
    },
    "series_c": {
        "prefix": "At Series C / Growth stage",
        "themes": {
            "churn_rate":            "at this scale, retention is a profit centre — implement predictive churn models and dedicated renewal teams.",
            "burn_multiple":         "investors expect near-profitability — every dollar of spend should have a clear ROI model.",
            "gross_margin":          "margins should be best-in-class — automate cost centres and pursue volume discounts.",
            "operating_margin":      "positive operating margin is the expectation — identify and eliminate any remaining negative-ROI activities.",
            "nrr":                   "NRR >120% is the gold standard at this stage — invest in product-led expansion and usage-based pricing.",
        },
    },
}


def stage_aware_actions(
    kpi_key: str,
    base_actions: list[str],
    stage: str,
) -> list[str]:
    """
    Prepend a stage-specific context sentence to the standard corrective
    actions if one exists for this KPI + stage combination.

    Returns a new list (does not mutate base_actions).
    """
    stage_cfg = _STAGE_CONTEXT.get(stage, {})
    theme = stage_cfg.get("themes", {}).get(kpi_key)
    if not theme:
        return list(base_actions)
    prefix = stage_cfg.get("prefix", "At your current stage")
    return [f"{prefix}, {theme}"] + list(base_actions)


# ═════════════════════════════════════════════════════════════════════════════
# 6.  DATA-DRIVEN ROOT CAUSE INFERENCE (CORRELATION ENGINE)
# ═════════════════════════════════════════════════════════════════════════════

def compute_kpi_correlations(
    kpi_monthly: dict,
    target_kpi: str,
    min_overlap: int = 4,
) -> list[dict]:
    """
    For a given target KPI, compute Pearson correlation with every other
    KPI's month-over-month deltas.  This surfaces which KPIs actually move
    together in THIS company's data (as opposed to the static causal rules).

    Uses delta correlation (MoM changes) rather than level correlation to
    avoid spurious trends-in-common effects.

    kpi_monthly: {kpi_key: [{"period": "YYYY-MM", "value": float}, ...]}
    target_kpi:  the KPI to find correlates for.
    min_overlap: minimum shared periods to compute correlation.

    Returns sorted list of:
        {key, name, correlation, abs_correlation, lag, direction_label}
    """
    target_entries = sorted(
        kpi_monthly.get(target_kpi, []), key=lambda x: x["period"],
    )
    if len(target_entries) < min_overlap + 1:
        return []

    # Build target delta series keyed by period
    target_deltas = {}
    for i in range(1, len(target_entries)):
        period = target_entries[i]["period"]
        delta = target_entries[i]["value"] - target_entries[i - 1]["value"]
        target_deltas[period] = delta

    results = []
    for other_kpi, other_entries in kpi_monthly.items():
        if other_kpi == target_kpi:
            continue
        other_sorted = sorted(other_entries, key=lambda x: x["period"])
        if len(other_sorted) < min_overlap + 1:
            continue

        # Build other delta series
        other_deltas = {}
        for i in range(1, len(other_sorted)):
            period = other_sorted[i]["period"]
            other_deltas[period] = other_sorted[i]["value"] - other_sorted[i - 1]["value"]

        # Align on shared periods (lag=0: same month)
        shared = sorted(set(target_deltas) & set(other_deltas))
        if len(shared) < min_overlap:
            continue

        t_vals = [target_deltas[p] for p in shared]
        o_vals = [other_deltas[p] for p in shared]

        # Pearson correlation
        corr = _pearson(t_vals, o_vals)
        if corr is None:
            continue

        abs_corr = abs(corr)
        if abs_corr < 0.3:
            continue  # weak correlation — don't surface

        dir_label = "moves together" if corr > 0 else "moves inversely"

        results.append({
            "key":            other_kpi,
            "name":           _friendly_name(other_kpi),
            "correlation":    round(corr, 3),
            "abs_correlation": round(abs_corr, 3),
            "strength":       "strong" if abs_corr >= 0.7 else "moderate",
            "direction_label": dir_label,
        })

    results.sort(key=lambda x: x["abs_correlation"], reverse=True)
    return results[:10]


def _pearson(x: list[float], y: list[float]) -> Optional[float]:
    """Compute Pearson correlation coefficient. Returns None if degenerate."""
    n = len(x)
    if n < 3:
        return None
    mx = sum(x) / n
    my = sum(y) / n
    sx = (sum((xi - mx) ** 2 for xi in x)) ** 0.5
    sy = (sum((yi - my) ** 2 for yi in y)) ** 0.5
    if sx == 0 or sy == 0:
        return None
    cov = sum((x[i] - mx) * (y[i] - my) for i in range(n))
    return cov / (sx * sy)


# ═════════════════════════════════════════════════════════════════════════════
# 7.  DECISION CHECK-IN DETECTION
# ═════════════════════════════════════════════════════════════════════════════

def decision_check_ins(
    decisions: list[dict],
    needs_attention_keys: list[str],
    now_iso: str,
) -> list[dict]:
    """
    Identify active decisions that are due for a 30-day check-in.

    decisions:            list of decision dicts from DB (id, title, status,
                          kpi_context, decided_at).
    needs_attention_keys: list of currently-red KPI keys.
    now_iso:              current ISO timestamp string.

    Returns list of check-in prompts:
        {decision_id, title, days_since, linked_kpis,
         still_critical (list of KPIs still red), prompt}
    """
    from datetime import datetime, timedelta

    try:
        now = datetime.fromisoformat(now_iso.replace("Z", "+00:00"))
    except Exception:
        now = datetime.utcnow()

    check_ins = []
    red_set = set(needs_attention_keys)

    for dec in decisions:
        if dec.get("status") != "active":
            continue

        decided_at_raw = dec.get("decided_at", "")
        try:
            decided_at = datetime.fromisoformat(
                decided_at_raw.replace("Z", "+00:00")
            )
        except Exception:
            continue

        days_since = (now - decided_at).days
        if days_since < 30:
            continue  # not yet due

        linked = dec.get("kpi_context", [])
        if isinstance(linked, str):
            try:
                import json as _json
                linked = _json.loads(linked)
            except Exception:
                linked = []

        still_critical = [k for k in linked if k in red_set]

        prompt_parts = [
            f"You committed to \"{dec['title']}\" {days_since} days ago.",
        ]
        if still_critical:
            names = ", ".join(_friendly_name(k) for k in still_critical[:3])
            prompt_parts.append(
                f"The linked KPIs ({names}) are still critical. "
                "Review whether the action is working or needs adjustment."
            )
        else:
            prompt_parts.append(
                "The linked KPIs have improved. Consider recording "
                "the outcome and marking this decision as resolved."
            )

        check_ins.append({
            "decision_id":   dec.get("id"),
            "title":         dec.get("title", ""),
            "days_since":    days_since,
            "linked_kpis":   linked,
            "still_critical": still_critical,
            "prompt":        " ".join(prompt_parts),
        })

    return check_ins
