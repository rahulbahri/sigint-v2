"""
elt/kpi_aggregator.py — Aggregates canonical_* tables into monthly KPI values
and inserts them into the monthly_data table.

This is the bridge between the ELT connector pipeline (canonical_revenue,
canonical_customers, etc.) and the KPI engine (monthly_data → health score,
variance, forecast, etc.).

Design principles:
  1. IDEMPOTENT — safe to call multiple times; always deletes its own prior
     output before re-inserting (identified by a sentinel upload_id).
  2. NON-DESTRUCTIVE — never touches rows created by CSV upload or demo seed.
     Only manages rows with upload_id = CONNECTOR_UPLOAD_SENTINEL.
  3. ADDITIVE — merges connector-derived KPIs with existing monthly_data from
     CSV uploads.  If a CSV-uploaded month already exists, connector KPIs fill
     in gaps but do NOT overwrite CSV values (CSV is the user's source of truth).
  4. DEFENSIVE — every computation is wrapped; a failure in one KPI never
     prevents others from being computed.
  5. AUDITABLE — logs what was computed, what was skipped, and why.

Canonical tables consumed (11):
  - canonical_revenue       → MRR, ARR, revenue_growth, recurring_revenue, nrr,
                               revenue_quality, customer_concentration, expansion_rate,
                               gross_dollar_ret, contraction_rate, pricing_power_index
  - canonical_expenses      → gross_margin, operating_margin, ebitda_margin,
                               opex_ratio, burn_multiple, burn_rate, cac_payback
  - canonical_customers     → churn_rate, customer_ltv, logo_retention
  - canonical_pipeline      → pipeline_conversion, win_rate, avg_deal_size,
                               pipeline_velocity, quota_attainment
  - canonical_invoices      → dso, avg_collection_period, ar_turnover, cei,
                               cash_conv_cycle, ar_aging_current, ar_aging_overdue
  - canonical_employees     → headcount_eff, rev_per_employee, ramp_time
  - canonical_marketing     → cpl, marketing_roi, mql_sql_rate
  - canonical_balance_sheet → cash_runway, current_ratio, working_capital
  - canonical_time_tracking → billable_utilization
  - canonical_surveys       → product_nps, csat
  - canonical_support       → support_volume
  - canonical_product_usage → activation_rate, time_to_value, feature_adoption

Output:
  - Rows in monthly_data (year, month, data_json, workspace_id,
    upload_id = CONNECTOR_UPLOAD_SENTINEL)
"""
from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ── Sentinel value ────────────────────────────────────────────────────────────
CONNECTOR_UPLOAD_SENTINEL = -999

# ── KPI Reasonableness Bounds ────────────────────────────────────────────────
# When a computed value falls outside these bounds, the value is WITHHELD and
# a diagnostic is recorded explaining why and what needs fixing.
#
# UNIVERSAL COVERAGE: every KPI has bounds derived from its unit type.
# Specific KPIs have custom messages; all others get a generic diagnostic.
# Format: kpi_key -> (min, max, diagnostic_message_template)

# --- Unit-based default bounds (catches every KPI, even new ones) ---
_UNIT_BOUNDS = {
    "pct":    (-100, 500,    "{key} of {value:.1f}% is outside the reasonable range for a percentage metric. Verify that the numerator and denominator are in the same units."),
    "ratio":  (-100, 500,    "{key} ratio of {value:.2f} is extreme. This usually indicates a near-zero denominator. Check that both inputs have sufficient magnitude."),
    "usd":    (-1e9, 1e9,    "{key} of ${value:,.0f} exceeds $1B — verify that currency amounts are in the correct unit (dollars vs cents) and that no data duplication exists."),
    "days":   (-365, 730,    "{key} of {value:.0f} days is outside the reasonable range. Verify that date fields (issue_date, due_date, created_at) are populated correctly."),
    "months": (-12, 240,     "{key} of {value:.0f} months is outside the reasonable range. Verify that the underlying rate or duration inputs are correct."),
    "score":  (-100, 100,    "{key} score of {value:.1f} is outside the expected range. Verify that survey or scoring data is using the correct scale."),
    "count":  (0, 1e7,       "{key} of {value:,.0f} exceeds 10M — verify that there is no data duplication in the source."),
}

# --- KPI-specific overrides with actionable diagnostic messages ---
_KPI_SPECIFIC_BOUNDS: dict[str, tuple[Optional[float], Optional[float], str]] = {
    "customer_ltv":        (0, 5_000_000,  "LTV of ${value:,.0f} exceeds $5M — likely caused by near-zero churn rate. Verify churn data: ensure churned customers are not appearing in revenue."),
    "ltv_cac":             (0, 80,         "LTV:CAC of {value:.1f}x exceeds 80 — near-zero churn inflates LTV. Verify churn and CAC inputs."),
    "burn_multiple":       (-20, 20,       "Burn multiple of {value:.1f}x is extreme — near-zero net new ARR as denominator. Verify ARR is changing meaningfully MoM."),
    "cac_payback":         (0, 120,        "CAC payback of {value:.0f} months exceeds 10 years — very low ARPU or near-zero gross margin. Verify revenue per customer."),
    "payback_period":      (0, 120,        "Payback period of {value:.0f} months exceeds 10 years — same root cause as CAC payback."),
    "sales_efficiency":    (0, 5,          "Sales efficiency of {value:.1f}x is extreme (typical 0.3-3x) — S&M spend is very low relative to ARR. Verify expense categories include all sales and marketing costs."),
    "operating_leverage":  (-10, 10,       "Operating leverage of {value:.1f}x — near-zero OpEx change as denominator. Requires meaningful MoM expense variation."),
    "cash_runway":         (0, 240,        "Cash runway of {value:.0f} months exceeds 20 years — verify burn rate calculation."),
    "growth_efficiency":   (-25, 25,       "Growth efficiency of {value:.1f}x is extreme — very small burn multiple as denominator. Value withheld until burn rate stabilises."),
    "revenue_momentum":    (-10, 10,       "Revenue momentum of {value:.1f}x is extreme — near-zero average revenue growth as denominator. Requires meaningful multi-month growth trend."),
    "rev_per_employee":    (0, 2_000_000,  "Rev/employee of ${value:,.0f} exceeds $2M — verify headcount data is complete in canonical_employees."),
    "headcount_eff":       (0, 50_000,     "Headcount efficiency of ${value:,.0f}/mo exceeds $50K — verify canonical_employees has cumulative headcount, not just new hires."),
    "ar_turnover":         (0, 365,        "AR turnover of {value:.0f}x — DSO below 1 day is unusual. Verify invoice issue_date and due_date fields."),
    "activation_rate":     (0, 100,        "Activation rate of {value:.0f}% exceeds 100% — multiple activations per user counted. Use one record per user."),
    "pipeline_velocity":   (0, 50_000,     "Pipeline velocity of ${value:,.0f}/day — verify deal amounts and duration data."),
    "feature_adoption":    (0, 100,        "Feature adoption of {value:.0f}% exceeds 100% — more features used than defined. Verify _TOTAL_FEATURES constant."),
    "billable_utilization":(0, 100,        "Billable utilization of {value:.0f}% exceeds 100% — billable hours exceed total hours. Verify time tracking data."),
    "logo_retention":      (0, 100,        "Logo retention of {value:.0f}% — verify customer churn data. Value should be 0-100%."),
    "recurring_revenue":   (0, 100,        "Recurring revenue ratio of {value:.0f}% exceeds 100% — recurring revenue exceeds total. Check subscription_type tagging."),
    "revenue_quality":     (0, 100,        "Revenue quality of {value:.0f}% exceeds 100% — same root cause as recurring_revenue. Check subscription_type."),
    "gross_margin":        (-100, 100,     "Gross margin of {value:.1f}% is outside -100 to 100%. Verify that COGS is not double-counted or in wrong units."),
    "operating_margin":    (-200, 100,     "Operating margin of {value:.1f}% is extreme. Verify expense categorisation — COGS and OpEx should not overlap."),
    "ebitda_margin":       (-200, 100,     "EBITDA margin of {value:.1f}% is extreme. Verify that expense data is complete and correctly categorised."),
    "customer_concentration": (0, 100,     "Customer concentration of {value:.1f}% — verify customer_id is correctly linked in revenue transactions."),
}

# --- KPI unit map (for universal fallback bounds) ---
from core.kpi_defs import KPI_DEFS, EXTENDED_ONTOLOGY_METRICS as _EXT
_KPI_UNITS = {}
for _d in KPI_DEFS + _EXT:
    _KPI_UNITS[_d["key"]] = _d.get("unit", "ratio")
# Aggregator-computed KPIs that aren't in KPI_DEFS but need correct unit types
_KPI_UNITS["mrr"] = "usd"
_KPI_UNITS["arr"] = "usd"
_KPI_UNITS["cash_burn"] = "usd"


def _get_bounds(key: str) -> Optional[tuple[float, float, str]]:
    """Return (min, max, message_template) for any KPI key.
    Checks specific overrides first, then falls back to unit-based defaults."""
    if key in _KPI_SPECIFIC_BOUNDS:
        return _KPI_SPECIFIC_BOUNDS[key]
    unit = _KPI_UNITS.get(key, "ratio")
    if unit in _UNIT_BOUNDS:
        return _UNIT_BOUNDS[unit]
    # Absolute fallback: any value between -1B and +1B
    return (-1e9, 1e9, "{key} of {value} is outside the absolute bounds. Review computation inputs.")


# ── Cross-KPI consistency rules ──────────────────────────────────────────────
# Each rule: (condition_fn, diagnostic_message)
# condition_fn takes the kpis dict and returns True if inconsistent.

def _check_cross_kpi_consistency(kpis: dict) -> list[dict]:
    """Detect contradictory KPI relationships. Returns list of diagnostics."""
    issues = []

    def _chk(condition: bool, msg: str):
        if condition:
            issues.append({"type": "consistency", "message": msg})

    gm = kpis.get("gross_margin")
    om = kpis.get("operating_margin")
    em = kpis.get("ebitda_margin")
    rq = kpis.get("revenue_quality")
    rr = kpis.get("recurring_revenue")

    if gm is not None and om is not None and om > gm + 1:
        _chk(True, f"Operating margin ({om:.1f}%) exceeds gross margin ({gm:.1f}%) — this is mathematically impossible since OpEx is always >= 0. Check expense categorisation: COGS may be understated or OpEx may include negative adjustments.")

    if gm is not None and em is not None and em > gm * 1.5 + 5:
        _chk(True, f"EBITDA margin ({em:.1f}%) is disproportionately higher than gross margin ({gm:.1f}%) — verify the EBITDA approximation factor and D&A assumptions.")

    if rq is not None and rr is not None and abs(rq - rr) > 1:
        _chk(True, f"Revenue quality ({rq:.1f}%) and recurring revenue ({rr:.1f}%) should be identical but differ — this indicates a computation inconsistency.")

    cr = kpis.get("churn_rate")
    lr = kpis.get("logo_retention")
    if cr is not None and lr is not None and abs((100 - cr) - lr) > 1:
        _chk(True, f"Churn rate ({cr:.1f}%) and logo retention ({lr:.1f}%) are inconsistent — (100 - churn) should equal retention.")

    dso = kpis.get("dso")
    acp = kpis.get("avg_collection_period")
    if dso is not None and acp is not None and abs(dso - acp) > 1:
        _chk(True, f"DSO ({dso:.0f} days) and Avg Collection Period ({acp:.0f} days) should be identical but differ.")

    # ARR = MRR * 12
    mrr = kpis.get("mrr")
    arr = kpis.get("arr")
    if mrr is not None and arr is not None and mrr > 0 and abs(arr - mrr * 12) > mrr * 0.5:
        _chk(True, f"ARR ({arr:,.0f}) should be MRR ({mrr:,.0f}) x 12 = {mrr*12:,.0f}. Verify subscription revenue classification.")

    # NRR > 100% requires expansion > contraction
    nrr = kpis.get("nrr")
    er = kpis.get("expansion_rate")
    ct = kpis.get("contraction_rate")
    if nrr is not None and nrr > 100 and er is not None and ct is not None and er <= ct:
        _chk(True, f"NRR ({nrr:.1f}%) > 100% but expansion ({er:.1f}%) <= contraction ({ct:.1f}%). Revenue growth must come from existing customers.")

    # Burn multiple sign: positive when burning cash
    bm = kpis.get("burn_multiple")
    cb = kpis.get("cash_burn")
    if bm is not None and cb is not None:
        if cb > 0 and bm < 0:
            _chk(True, f"Cash burn is positive (${cb:,.0f}) but burn multiple is negative ({bm:.1f}x). Sign convention may be inverted.")

    return issues


# ── Data Quality Scoring ─────────────────────────────────────────────────────

def _score_month_data_quality(
    rev: dict, exp: dict, cust: dict, pipe: dict,
    inv: dict, emp: dict, computed_kpis: dict,
) -> dict:
    """Score the data quality and completeness for a single month.

    Returns:
      quality_score: 0-100 (100 = complete, reliable data)
      quality_label: "high" / "moderate" / "low" / "insufficient"
      issues: list of specific data quality concerns
      tables_present: which canonical tables had data
    """
    issues = []
    tables_present = []
    table_scores = []

    # Check each canonical data source
    def _check_table(name, data, key_field):
        if not data:
            issues.append(f"No {name} data — KPIs depending on {name} will be unavailable.")
            return 0
        tables_present.append(name)
        val = data.get(key_field, 0)
        if isinstance(val, (int, float)) and val <= 0:
            issues.append(f"{name} present but {key_field} is zero — dependent KPIs may be empty or inaccurate.")
            return 40
        return 100

    table_scores.append(_check_table("revenue", rev, "total_revenue"))
    table_scores.append(_check_table("expenses", exp, "total_expenses"))
    table_scores.append(_check_table("customers", cust, "new_customers"))
    table_scores.append(_check_table("pipeline", pipe, "deals_count"))
    table_scores.append(_check_table("invoices", inv, "invoice_count"))
    table_scores.append(_check_table("employees", emp, "headcount"))

    # Revenue sanity checks
    if rev:
        if rev.get("total_revenue", 0) <= 0:
            issues.append("Total revenue is zero or negative — all margin and growth KPIs will be unavailable.")
            table_scores[0] = 20
        if not rev.get("customer_ids"):
            issues.append("No customer_id linked to revenue transactions — churn, concentration, and per-customer KPIs will be unavailable.")
            table_scores[0] = max(table_scores[0] - 30, 10)

    # Expense categorisation check
    if exp:
        total = exp.get("total_expenses", 0)
        cogs = exp.get("cogs", 0)
        sm = exp.get("sm_expenses", 0)
        if total > 0 and (cogs + sm) / total < 0.1:
            issues.append("Less than 10% of expenses are categorised as COGS or S&M — margin and efficiency KPIs may be inaccurate. Tag expenses with categories like 'cogs', 'hosting', 'marketing', 'sales'.")
            table_scores[1] = 50

    # Headcount check
    if emp:
        hc = emp.get("headcount", 0)
        if hc < 3:
            issues.append(f"Headcount of {hc} is very low — rev_per_employee and headcount_eff will appear inflated. Ensure all active employees are in canonical_employees.")

    # Compute quality score
    n_kpis = len([k for k in computed_kpis if not k.startswith("_")])
    n_diags = len(computed_kpis.get("_diagnostics", {}))

    # Weighted: 60% table coverage, 20% KPI yield, 20% no diagnostics
    table_avg = sum(table_scores) / max(len(table_scores), 1)
    kpi_yield = min(n_kpis / 40 * 100, 100)  # 40 KPIs = 100%
    diag_penalty = max(0, 100 - n_diags * 15)

    quality_score = round(table_avg * 0.6 + kpi_yield * 0.2 + diag_penalty * 0.2)
    quality_score = max(0, min(100, quality_score))

    if quality_score >= 80:
        quality_label = "high"
    elif quality_score >= 50:
        quality_label = "moderate"
    elif quality_score >= 25:
        quality_label = "low"
    else:
        quality_label = "insufficient"

    return {
        "quality_score": quality_score,
        "quality_label": quality_label,
        "tables_present": tables_present,
        "tables_missing": [t for t in ["revenue", "expenses", "customers", "pipeline", "invoices", "employees"]
                           if t not in tables_present],
        "issues": issues,
        "kpis_computed": n_kpis,
        "kpis_withheld": n_diags,
    }


# ── Duplicate Transaction Detection ──────────────────────────────────────────

def _detect_duplicates(conn, workspace_id: str, summary: dict) -> None:
    """Detect duplicate source_ids in canonical tables and record warnings.

    Does NOT modify data — purely diagnostic.  Duplicate source_ids indicate
    a sync issue (source system sent the same record twice) that inflates
    aggregated values.
    """
    tables = [
        "canonical_revenue", "canonical_expenses", "canonical_invoices",
        "canonical_pipeline", "canonical_employees", "canonical_customers",
        "canonical_marketing", "canonical_balance_sheet", "canonical_time_tracking",
        "canonical_surveys", "canonical_support", "canonical_product_usage",
    ]
    for table in tables:
        try:
            rows = conn.execute(
                f"SELECT source_id, COUNT(*) as cnt FROM {table} "
                f"WHERE workspace_id=? GROUP BY source_id HAVING cnt > 1",
                [workspace_id],
            ).fetchall()
            if rows:
                n_dupes = len(rows)
                summary.setdefault("data_warnings", []).append(
                    f"{table}: {n_dupes} duplicate source_id(s) detected — "
                    f"aggregated values may be inflated. Review source system sync."
                )
                logger.warning(
                    "[Duplicate Detection] %s: %d duplicate source_ids for workspace=%s",
                    table, n_dupes, workspace_id,
                )
        except Exception:
            pass  # Table may not exist


# ── Public API ────────────────────────────────────────────────────────────────

def aggregate_canonical_to_monthly(conn, workspace_id: str) -> dict:
    """
    Main entry point.  Reads all canonical_* tables for a workspace,
    computes monthly KPI values, and upserts into monthly_data.

    Returns a summary dict: {"months_written": int, "kpis_computed": [...], "errors": [...]}
    """
    summary = {"months_written": 0, "kpis_computed": set(), "errors": [], "skipped": []}

    try:
        # ── Step 0: Detect duplicate transactions before aggregation ──
        _detect_duplicates(conn, workspace_id, summary)

        # ── Step 1: Extract monthly aggregates from each canonical table ──
        revenue_by_month    = _extract_monthly_revenue(conn, workspace_id, summary)
        expense_by_month    = _extract_monthly_expenses(conn, workspace_id, summary)
        customer_by_month   = _extract_monthly_customers(conn, workspace_id, summary)
        pipeline_by_month   = _extract_monthly_pipeline(conn, workspace_id, summary)
        invoice_by_month    = _extract_monthly_invoices(conn, workspace_id, summary)
        employee_by_month   = _extract_monthly_employees(conn, workspace_id, summary)
        marketing_by_month  = _extract_monthly_marketing(conn, workspace_id, summary)
        balance_by_month    = _extract_monthly_balance_sheet(conn, workspace_id, summary)
        time_by_month       = _extract_monthly_time_tracking(conn, workspace_id, summary)
        survey_by_month     = _extract_monthly_surveys(conn, workspace_id, summary)
        support_by_month    = _extract_monthly_support(conn, workspace_id, summary)
        usage_by_month      = _extract_monthly_product_usage(conn, workspace_id, summary)

        # ── Step 2: Collect all months that have ANY data ──
        all_months: set[tuple[int, int]] = set()
        for source in (revenue_by_month, expense_by_month, customer_by_month,
                       pipeline_by_month, invoice_by_month, employee_by_month,
                       marketing_by_month, balance_by_month, time_by_month,
                       survey_by_month, support_by_month, usage_by_month):
            all_months.update(source.keys())

        if not all_months:
            summary["errors"].append("No canonical data found for any month.")
            return _finalise_summary(summary)

        sorted_months = sorted(all_months)

        # ── Gap detection: flag missing months ──
        for i in range(1, len(sorted_months)):
            py, pm = sorted_months[i - 1]
            cy, cm = sorted_months[i]
            ey = py + (pm // 12)
            em = (pm % 12) + 1
            if (cy, cm) != (ey, em):
                summary.setdefault("data_warnings", []).append(
                    f"Gap: data jumps from {py}-{pm:02d} to {cy}-{cm:02d}")

        # ── Step 3: Compute KPIs for each month ──
        monthly_kpis: dict[tuple[int, int], dict[str, float]] = {}
        prev_kpis: Optional[dict[str, float]] = None

        for ym in sorted_months:
            kpis = _compute_month_kpis(
                ym,
                revenue_by_month.get(ym, {}),
                expense_by_month.get(ym, {}),
                customer_by_month.get(ym, {}),
                pipeline_by_month.get(ym, {}),
                invoice_by_month.get(ym, {}),
                employee_by_month.get(ym, {}),
                marketing_by_month.get(ym, {}),
                balance_by_month.get(ym, {}),
                time_by_month.get(ym, {}),
                survey_by_month.get(ym, {}),
                support_by_month.get(ym, {}),
                usage_by_month.get(ym, {}),
                prev_kpis,
                summary,
            )
            monthly_kpis[ym] = kpis
            # Build clean prev for next month — only numeric values + required internals.
            # Prevents _diagnostics, _data_quality, and other metadata from leaking forward.
            _INTERNAL_STATE_KEYS = {"_active_customers", "_revenue_customer_ids",
                                    "_total_revenue", "_total_expenses",
                                    "_opex", "_cogs", "_customer_amounts", "_ending_ar"}
            prev_kpis = {}
            for _pk, _pv in kpis.items():
                if _pk in _INTERNAL_STATE_KEYS:
                    prev_kpis[_pk] = _pv
                elif isinstance(_pv, (int, float)):
                    prev_kpis[_pk] = _pv

        # ── Step 4: Second pass — derived KPIs that need history ──
        _compute_derived_kpis(sorted_months, monthly_kpis, summary)

        # ── Step 4b: Cross-KPI consistency checks ──
        for ym in sorted_months:
            kpis = monthly_kpis[ym]
            consistency_issues = _check_cross_kpi_consistency(kpis)
            if consistency_issues:
                diags = kpis.setdefault("_diagnostics", {})
                diags["_consistency"] = consistency_issues

        # ── Step 4c: Data quality scoring per month ──
        for ym in sorted_months:
            quality = _score_month_data_quality(
                revenue_by_month.get(ym, {}),
                expense_by_month.get(ym, {}),
                customer_by_month.get(ym, {}),
                pipeline_by_month.get(ym, {}),
                invoice_by_month.get(ym, {}),
                employee_by_month.get(ym, {}),
                monthly_kpis[ym],
            )
            monthly_kpis[ym]["_data_quality"] = quality

            # Circuit breaker: if quality is insufficient, flag entire month
            if quality["quality_label"] == "insufficient":
                diags = monthly_kpis[ym].setdefault("_diagnostics", {})
                diags["_circuit_breaker"] = {
                    "withheld": True,
                    "reason": (
                        f"Data quality score of {quality['quality_score']}/100 is below "
                        f"the minimum threshold. Missing tables: {', '.join(quality['tables_missing'])}. "
                        f"KPIs for this month should not be relied upon for decision-making. "
                        f"Connect additional data sources to improve reliability."
                    ),
                    "quality_score": quality["quality_score"],
                    "issues": quality["issues"],
                }

        # ── Step 5: Load existing CSV-uploaded data (to merge, not overwrite) ──
        existing_csv = _load_existing_csv_kpis(conn, workspace_id)

        # ── Step 6: Delete previous connector-generated rows (idempotent) ──
        conn.execute(
            "DELETE FROM monthly_data WHERE workspace_id=? AND upload_id=?",
            [workspace_id, CONNECTOR_UPLOAD_SENTINEL],
        )

        # ── Step 7: Insert new connector-aggregated rows ──
        months_written = 0
        for ym in sorted_months:
            yr, mo = ym
            kpis = monthly_kpis[ym]
            if not kpis:
                continue

            # Source priority: connector=1, csv=2, seed=0 (higher=wins)
            # CSV values override connector values by default (user's source of truth)
            csv_existing = existing_csv.get(ym, {})
            merged = {**kpis}
            for k, v in csv_existing.items():
                if v is None or k.startswith("_"):
                    continue
                if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    merged[k] = v  # CSV wins (priority 2 > connector priority 1)
                else:
                    _record_diagnostic(merged, k, None,
                        f"CSV value for {k} is non-numeric ('{v}'). Only numeric values accepted.")

            # Clean: separate diagnostics/quality metadata from numeric KPIs
            clean = {}
            diagnostics = merged.pop("_diagnostics", {})
            data_quality = merged.pop("_data_quality", {})
            for k, v in merged.items():
                if k.startswith("_"):
                    continue
                if v is not None and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    clean[k] = round(v, 4) if isinstance(v, float) else v

            # Build lineage: which source contributed each KPI
            lineage = {}
            csv_keys = set(csv_existing.keys())
            for k in clean:
                if k.startswith("_"):
                    continue
                if k in csv_keys:
                    lineage[k] = "csv"
                else:
                    lineage[k] = "connector"
            if lineage:
                clean["_data_lineage"] = lineage

            # Attach metadata
            if diagnostics:
                clean["_diagnostics"] = diagnostics
            if data_quality:
                clean["_data_quality"] = data_quality

            if not clean:
                continue

            conn.execute(
                "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) "
                "VALUES (?,?,?,?,?)",
                [CONNECTOR_UPLOAD_SENTINEL, yr, mo, json.dumps(clean), workspace_id],
            )
            months_written += 1
            summary["kpis_computed"].update(k for k in clean.keys() if not k.startswith("_"))
            if diagnostics:
                summary.setdefault("diagnostics", {}).update(diagnostics)

        conn.commit()
        summary["months_written"] = months_written
        logger.info(
            "[KPI Aggregator] workspace=%s: %d months, %d unique KPIs",
            workspace_id, months_written, len(summary["kpis_computed"]),
        )

    except Exception as exc:
        summary["errors"].append(f"Top-level aggregation error: {exc}")
        logger.exception("[KPI Aggregator] Fatal error for workspace=%s", workspace_id)
        try:
            conn.rollback()
        except Exception:
            pass

    return _finalise_summary(summary)


# ── Canonical extraction helpers ──────────────────────────────────────────────
# Each returns {(year, month): {aggregated_fields}} with defensive error handling.

def _extract_monthly_revenue(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_revenue by month, including per-customer amounts."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_revenue": 0.0,
        "recurring_revenue": 0.0,
        "transaction_count": 0,
        "customer_ids": set(),
        "amounts": [],
        "customer_amounts": defaultdict(float),
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT amount, period, subscription_type, customer_id, source, currency "
            "FROM canonical_revenue WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_revenue: no rows")
            return {}

        # Currency validation: detect mixed currencies
        currencies_seen = set()
        for r in rows:
            cur = (r.get("currency") if isinstance(r, dict) else (r[5] if len(r) > 5 else None)) or ""
            if cur and str(cur).strip():
                currencies_seen.add(str(cur).strip().upper())
        if len(currencies_seen) > 1:
            summary.setdefault("data_warnings", []).append(
                f"Mixed currencies in revenue: {', '.join(sorted(currencies_seen))}. "
                f"All amounts are summed as-is — totals may be unreliable."
            )

        for r in rows:
            amount = _safe_float(r.get("amount") if isinstance(r, dict) else r[0])
            period = r.get("period") if isinstance(r, dict) else r[1]
            sub_type = r.get("subscription_type") if isinstance(r, dict) else r[2]
            cust_id = r.get("customer_id") if isinstance(r, dict) else r[3]

            if amount is None or period is None:
                continue

            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            bucket["total_revenue"] += amount
            bucket["transaction_count"] += 1
            bucket["amounts"].append(amount)
            if cust_id:
                cid = str(cust_id)
                bucket["customer_ids"].add(cid)
                bucket["customer_amounts"][cid] += amount
            if sub_type and str(sub_type).lower() in (
                "recurring", "subscription", "monthly", "annual", "yearly",
            ):
                bucket["recurring_revenue"] += amount
            # Track which canonical sources contributed
            bucket.setdefault("_sources", set()).add(str(r.get("source") if isinstance(r, dict) else "unknown"))

    except Exception as exc:
        summary["errors"].append(f"Revenue extraction: {exc}")
    return _clean_empty_buckets(out, "total_revenue")


def _extract_monthly_expenses(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_expenses by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_expenses": 0.0,
        "sm_expenses": 0.0,
        "cogs": 0.0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT amount, period, category FROM canonical_expenses WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_expenses: no rows")
            return {}

        for r in rows:
            amount = _safe_float(r.get("amount") if isinstance(r, dict) else r[0])
            period = r.get("period") if isinstance(r, dict) else r[1]
            category = str(r.get("category") if isinstance(r, dict) else r[2] or "").lower()

            if amount is None or period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            bucket["total_expenses"] += abs(amount)

            if any(kw in category for kw in ("sales", "marketing", "s&m", "advertising",
                                              "lead gen", "demand gen", "paid", "seo")):
                bucket["sm_expenses"] += abs(amount)
            elif any(kw in category for kw in ("cogs", "cost of goods", "cost of revenue",
                                                "hosting", "infrastructure", "direct")):
                bucket["cogs"] += abs(amount)

    except Exception as exc:
        summary["errors"].append(f"Expense extraction: {exc}")
    return _clean_empty_buckets(out, "total_expenses")


def _extract_monthly_customers(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_customers by month (using created_at as acquisition date)."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "new_customers": 0,
        "customer_ids": set(),
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT source_id, created_at FROM canonical_customers WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_customers: no rows")
            return {}

        for r in rows:
            source_id = r.get("source_id") if isinstance(r, dict) else r[0]
            created_at = r.get("created_at") if isinstance(r, dict) else r[1]
            if created_at is None:
                continue
            ym = _parse_period(created_at)
            if ym is None:
                continue
            out[ym]["new_customers"] += 1
            if source_id:
                out[ym]["customer_ids"].add(str(source_id))

    except Exception as exc:
        summary["errors"].append(f"Customer extraction: {exc}")
    return _clean_empty_buckets(out, "new_customers")


def _extract_monthly_pipeline(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_pipeline by month (close_date)."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_pipeline_value": 0.0,
        "deals_count": 0,
        "deals_won": 0,
        "won_value": 0.0,
        "total_days_in_pipeline": 0.0,
        "deals_with_duration": 0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT amount, stage, close_date, created_at FROM canonical_pipeline WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_pipeline: no rows")
            return {}

        for r in rows:
            amount = _safe_float(r.get("amount") if isinstance(r, dict) else r[0])
            stage = str(r.get("stage") if isinstance(r, dict) else r[1] or "").lower()
            close_date = r.get("close_date") if isinstance(r, dict) else r[2]
            created_at = r.get("created_at") if isinstance(r, dict) else r[3]

            ym = _parse_period(close_date) if close_date else None
            if ym is None:
                continue

            bucket = out[ym]
            bucket["deals_count"] += 1
            bucket["total_pipeline_value"] += (amount or 0.0)

            is_won = any(kw in stage for kw in ("won", "closed won", "closedwon", "contracted"))
            if is_won:
                bucket["deals_won"] += 1
                bucket["won_value"] += (amount or 0.0)

            # Track deal duration for pipeline_velocity
            cd = _parse_date(close_date)
            ca = _parse_date(created_at) if created_at else None
            if cd and ca and cd > ca:
                bucket["total_days_in_pipeline"] += (cd - ca).days
                bucket["deals_with_duration"] += 1

    except Exception as exc:
        summary["errors"].append(f"Pipeline extraction: {exc}")
    return _clean_empty_buckets(out, "deals_count")


def _extract_monthly_invoices(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_invoices by month (issue_date), including AR tracking."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "invoice_total": 0.0,
        "invoice_count": 0,
        "overdue_count": 0,
        "overdue_amount": 0.0,
        "days_outstanding_sum": 0.0,
        "paid_total": 0.0,
        "outstanding_total": 0.0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT amount, issue_date, due_date, status FROM canonical_invoices WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_invoices: no rows")
            return {}

        for r in rows:
            amount = _safe_float(r.get("amount") if isinstance(r, dict) else r[0])
            issue_date = r.get("issue_date") if isinstance(r, dict) else r[1]
            due_date = r.get("due_date") if isinstance(r, dict) else r[2]
            status = str(r.get("status") if isinstance(r, dict) else r[3] or "").lower()

            if amount is None or issue_date is None:
                continue
            ym = _parse_period(issue_date)
            if ym is None:
                continue

            abs_amount = abs(amount)
            bucket = out[ym]
            bucket["invoice_total"] += abs_amount
            bucket["invoice_count"] += 1

            # Track paid vs outstanding for CEI
            if status in ("paid", "settled", "closed"):
                bucket["paid_total"] += abs_amount
            else:
                bucket["outstanding_total"] += abs_amount

            # Compute days outstanding using PERIOD-RELATIVE dates
            # Use (due_date - issue_date) as the payment term for DSO,
            # NOT (now - issue_date) which inflates historical invoices.
            issued = _parse_date(issue_date)
            due = _parse_date(due_date) if due_date else None
            if issued and due:
                # Payment terms: how many days between issue and due
                days_out = max((due - issued).days, 1)
                bucket["days_outstanding_sum"] += days_out
                # Overdue: status is not paid AND due_date is in the past relative to period end
                period_end = datetime(ym[0], ym[1], 28)  # Approximate month end
                if period_end > due and status not in ("paid", "settled", "closed"):
                    bucket["overdue_count"] += 1
                    bucket["overdue_amount"] += abs_amount
            elif issued:
                # No due_date: assume 30-day terms
                bucket["days_outstanding_sum"] += 30

    except Exception as exc:
        summary["errors"].append(f"Invoice extraction: {exc}")
    return _clean_empty_buckets(out, "invoice_count")


def _extract_monthly_employees(conn, workspace_id: str, summary: dict) -> dict:
    """Compute CUMULATIVE active headcount per month.

    For each month, counts all employees whose hire_date is on or before
    the last day of that month AND whose status is active. This gives the
    real headcount for that period, not just new hires.
    """
    out: dict[tuple[int, int], dict] = {}
    try:
        rows = _safe_query(
            conn,
            "SELECT source_id, hire_date, status, salary FROM canonical_employees WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_employees: no rows")
            return {}

        # Collect all active employees with their hire month
        active_employees = []
        for r in rows:
            hire_date = r.get("hire_date") if isinstance(r, dict) else r[1]
            status = str(r.get("status") if isinstance(r, dict) else r[2] or "active").lower()
            salary = _safe_float(r.get("salary") if isinstance(r, dict) else r[3])

            if status not in ("active", "employed", "full-time", "part-time", "contractor"):
                continue
            ym = _parse_period(hire_date) if hire_date else None
            if ym is None:
                continue
            active_employees.append({"hire_ym": ym, "salary": salary or 0.0})

        if not active_employees:
            return {}

        # Find the range of months we need to cover
        earliest = min(e["hire_ym"] for e in active_employees)
        # Cover up to current month
        now = datetime.utcnow()
        latest = (now.year, now.month)

        # For each month, count employees hired on or before that month
        y, m = earliest
        while (y, m) <= latest:
            headcount = 0
            total_salary = 0.0
            for emp in active_employees:
                if emp["hire_ym"] <= (y, m):
                    headcount += 1
                    total_salary += emp["salary"]
            if headcount > 0:
                out[(y, m)] = {"headcount": headcount, "total_salary": total_salary}
            m += 1
            if m > 12:
                m = 1
                y += 1

    except Exception as exc:
        summary["errors"].append(f"Employee extraction: {exc}")
    return _clean_empty_buckets(out, "headcount")


# ── New extractors for expanded canonical tables ─────────────────────────────

def _extract_monthly_marketing(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_marketing by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_spend": 0.0,
        "total_leads": 0,
        "total_conversions": 0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT spend, leads, conversions, period FROM canonical_marketing WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_marketing: no rows")
            return {}

        for r in rows:
            spend = _safe_float(r.get("spend") if isinstance(r, dict) else r[0])
            leads = _safe_float(r.get("leads") if isinstance(r, dict) else r[1])
            conversions = _safe_float(r.get("conversions") if isinstance(r, dict) else r[2])
            period = r.get("period") if isinstance(r, dict) else r[3]

            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            bucket["total_spend"] += (spend or 0.0)
            bucket["total_leads"] += int(leads or 0)
            bucket["total_conversions"] += int(conversions or 0)

    except Exception as exc:
        summary["errors"].append(f"Marketing extraction: {exc}")
    return _clean_empty_buckets(out, "total_spend")


def _extract_monthly_balance_sheet(conn, workspace_id: str, summary: dict) -> dict:
    """Extract canonical_balance_sheet snapshots by month."""
    out: dict[tuple[int, int], dict] = {}
    try:
        rows = _safe_query(
            conn,
            "SELECT period, cash_balance, current_assets, current_liabilities "
            "FROM canonical_balance_sheet WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_balance_sheet: no rows")
            return {}

        for r in rows:
            period = r.get("period") if isinstance(r, dict) else r[0]
            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue
            out[ym] = {
                "cash_balance": _safe_float(r.get("cash_balance") if isinstance(r, dict) else r[1]) or 0.0,
                "current_assets": _safe_float(r.get("current_assets") if isinstance(r, dict) else r[2]) or 0.0,
                "current_liabilities": _safe_float(r.get("current_liabilities") if isinstance(r, dict) else r[3]) or 0.0,
            }

    except Exception as exc:
        summary["errors"].append(f"Balance sheet extraction: {exc}")
    return out


def _extract_monthly_time_tracking(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_time_tracking by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_billable": 0.0,
        "total_hours": 0.0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT billable_hours, total_hours, period FROM canonical_time_tracking WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_time_tracking: no rows")
            return {}

        for r in rows:
            billable = _safe_float(r.get("billable_hours") if isinstance(r, dict) else r[0])
            total = _safe_float(r.get("total_hours") if isinstance(r, dict) else r[1])
            period = r.get("period") if isinstance(r, dict) else r[2]

            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            bucket["total_billable"] += (billable or 0.0)
            bucket["total_hours"] += (total or 0.0)

    except Exception as exc:
        summary["errors"].append(f"Time tracking extraction: {exc}")
    return _clean_empty_buckets(out, "total_hours")


def _extract_monthly_surveys(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_surveys by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "nps_scores": [],
        "csat_scores": [],
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT nps_score, csat_score, period FROM canonical_surveys WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_surveys: no rows")
            return {}

        for r in rows:
            nps = _safe_float(r.get("nps_score") if isinstance(r, dict) else r[0])
            csat = _safe_float(r.get("csat_score") if isinstance(r, dict) else r[1])
            period = r.get("period") if isinstance(r, dict) else r[2]

            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            if nps is not None:
                out[ym]["nps_scores"].append(nps)
            if csat is not None:
                out[ym]["csat_scores"].append(csat)

    except Exception as exc:
        summary["errors"].append(f"Survey extraction: {exc}")
    return _clean_empty_buckets(out, "nps_scores")


def _extract_monthly_support(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_support by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "ticket_count": 0,
        "total_resolution_hours": 0.0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT ticket_id, resolution_hours, period FROM canonical_support WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_support: no rows")
            return {}

        for r in rows:
            resolution = _safe_float(r.get("resolution_hours") if isinstance(r, dict) else r[1])
            period = r.get("period") if isinstance(r, dict) else r[2]

            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            bucket["ticket_count"] += 1
            bucket["total_resolution_hours"] += (resolution or 0.0)

    except Exception as exc:
        summary["errors"].append(f"Support extraction: {exc}")
    return _clean_empty_buckets(out, "ticket_count")


def _extract_monthly_product_usage(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_product_usage by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "active_users": set(),
        "activated_users": 0,
        "features_used": set(),
        "time_to_value_days": [],
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT user_id, feature_id, activated_at, first_value_at, period "
            "FROM canonical_product_usage WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_product_usage: no rows")
            return {}

        for r in rows:
            user_id = r.get("user_id") if isinstance(r, dict) else r[0]
            feature_id = r.get("feature_id") if isinstance(r, dict) else r[1]
            activated_at = r.get("activated_at") if isinstance(r, dict) else r[2]
            first_value_at = r.get("first_value_at") if isinstance(r, dict) else r[3]
            period = r.get("period") if isinstance(r, dict) else r[4]

            if period is None:
                continue
            ym = _parse_period(period)
            if ym is None:
                continue

            bucket = out[ym]
            if user_id:
                bucket["active_users"].add(str(user_id))
            if feature_id:
                bucket["features_used"].add(str(feature_id))
            if activated_at:
                bucket["activated_users"] += 1
            if activated_at and first_value_at:
                act_dt = _parse_date(activated_at)
                fv_dt = _parse_date(first_value_at)
                if act_dt and fv_dt and fv_dt >= act_dt:
                    bucket["time_to_value_days"].append((fv_dt - act_dt).days)

    except Exception as exc:
        summary["errors"].append(f"Product usage extraction: {exc}")
    return _clean_empty_buckets(out, "active_users")


# ── KPI computation ──────────────────────────────────────────────────────────

# Total features offered (used for feature_adoption denominator)
_TOTAL_FEATURES = 12

def _compute_month_kpis(
    ym: tuple[int, int],
    rev: dict,
    exp: dict,
    cust: dict,
    pipe: dict,
    inv: dict,
    emp: dict,
    mkt: dict,
    bal: dict,
    time_t: dict,
    surv: dict,
    supp: dict,
    usage: dict,
    prev: Optional[dict],
    summary: dict,
) -> dict[str, float]:
    """Compute all KPIs for a single month from canonical aggregates."""
    kpis: dict[str, Any] = {}

    total_rev     = rev.get("total_revenue", 0.0)
    recurring_rev = rev.get("recurring_revenue", 0.0)
    total_exp     = exp.get("total_expenses", 0.0)
    cogs          = exp.get("cogs", 0.0)
    sm_exp        = exp.get("sm_expenses", 0.0)
    new_cust      = cust.get("new_customers", 0)
    headcount     = emp.get("headcount", 0)
    inv_total     = inv.get("invoice_total", 0.0)
    inv_count     = inv.get("invoice_count", 0)
    deals_count   = pipe.get("deals_count", 0)
    deals_won     = pipe.get("deals_won", 0)
    won_value     = pipe.get("won_value", 0.0)
    pipe_value    = pipe.get("total_pipeline_value", 0.0)

    opex = total_exp - cogs  # Operating expenses = total - COGS

    # ── Revenue & margin metrics ─────────────────────────────────────────
    if total_rev > 0:
        _safe_set(kpis, "gross_margin", (total_rev - cogs) / total_rev * 100)
        _safe_set(kpis, "operating_margin", (total_rev - cogs - opex) / total_rev * 100)
        # EBITDA proxy: operating income (D&A not available from connectors).
        # SaaS companies typically have minimal D&A so this is a close approximation.
        # Will compute accurately when income statement D&A data is available.
        ebitda = total_rev - cogs - opex
        kpis["_ebitda_is_proxy"] = True
        _safe_set(kpis, "ebitda_margin", ebitda / total_rev * 100)
        _safe_set(kpis, "opex_ratio", opex / total_rev * 100)
        # Contribution margin = (Revenue - Variable Costs) / Revenue.
        # Variable costs = COGS + S&M spend. When S&M unavailable, uses COGS only.
        _sm_alloc = mkt.get("total_spend", 0.0) if mkt else 0.0
        _variable_costs = cogs + _sm_alloc
        _safe_set(kpis, "contribution_margin", (total_rev - _variable_costs) / total_rev * 100)
        if _sm_alloc == 0:
            kpis["_contribution_margin_is_proxy"] = True

        if recurring_rev > 0:
            _safe_set(kpis, "revenue_quality", recurring_rev / total_rev * 100)
            _safe_set(kpis, "recurring_revenue", recurring_rev / total_rev * 100)
            _safe_set(kpis, "mrr", recurring_rev)
            _safe_set(kpis, "arr", recurring_rev * 12)

        # Revenue growth (requires prior month)
        if prev and prev.get("_total_revenue", 0) > 0:
            prev_rev = prev["_total_revenue"]
            _safe_set(kpis, "revenue_growth", (total_rev - prev_rev) / prev_rev * 100)
            if prev.get("arr", 0) > 0 and kpis.get("arr", 0) > 0:
                _safe_set(kpis, "arr_growth", (kpis["arr"] - prev["arr"]) / prev["arr"] * 100)
        elif not prev:
            _record_diagnostic(kpis, "revenue_growth", None,
                "First month of data — growth rate requires prior month revenue for comparison.")
            _record_diagnostic(kpis, "arr_growth", None,
                "First month of data — ARR growth requires prior month ARR.")

        # Revenue concentration
        cust_ids = rev.get("customer_ids", set())
        if cust_ids:
            n_cust = len(cust_ids)
            # HHI (Herfindahl-Hirschman Index): sum of squared revenue shares.
            # Real concentration metric (no arbitrary multipliers).
            _cust_amts = rev.get("customer_amounts", {})
            if _cust_amts and total_rev > 0:
                _hhi = sum((amt / total_rev * 100) ** 2 for amt in _cust_amts.values())
                _safe_set(kpis, "customer_concentration", min(_hhi / 100, 100.0))
            else:
                _safe_set(kpis, "customer_concentration", min(100.0 / max(n_cust, 1), 100.0))
                kpis["_customer_concentration_is_proxy"] = True

    # Store internals for derived KPIs (prefixed with _ to remove later)
    if total_rev > 0:
        kpis["_total_revenue"] = total_rev
    kpis["_total_expenses"] = total_exp
    kpis["_opex"] = opex
    kpis["_cogs"] = cogs

    # ── Expansion / contraction / GDR (per-customer tracking) ────────────
    curr_cust_amts = rev.get("customer_amounts", {})
    prev_cust_amts = prev.get("_customer_amounts", {}) if prev else {}
    if curr_cust_amts and prev_cust_amts:
        expansion_rev = 0.0
        contraction_rev = 0.0
        retained_rev = 0.0
        prior_total = sum(prev_cust_amts.values())

        for cid, curr_amt in curr_cust_amts.items():
            if cid in prev_cust_amts:
                prev_amt = prev_cust_amts[cid]
                if curr_amt > prev_amt:
                    expansion_rev += (curr_amt - prev_amt)
                    retained_rev += prev_amt
                elif curr_amt < prev_amt:
                    contraction_rev += (prev_amt - curr_amt)
                    retained_rev += curr_amt
                else:
                    retained_rev += curr_amt

        if prior_total > 0:
            _safe_set(kpis, "expansion_rate", expansion_rev / prior_total * 100)
            _safe_set(kpis, "contraction_rate", contraction_rev / prior_total * 100)
            _safe_set(kpis, "gross_dollar_ret", retained_rev / prior_total * 100)
    elif not prev_cust_amts and prev is not None:
        _record_diagnostic(kpis, "expansion_rate", None,
            "No per-customer revenue data from prior month — ensure customer_id is linked to revenue transactions.")
    elif not prev:
        _record_diagnostic(kpis, "expansion_rate", None,
            "First month of data — expansion/contraction require prior month per-customer comparison.")
        _record_diagnostic(kpis, "contraction_rate", None,
            "First month of data — requires prior month per-customer comparison.")
        _record_diagnostic(kpis, "gross_dollar_ret", None,
            "First month of data — gross dollar retention requires prior month revenue by customer.")

    # Store for next month's comparison
    kpis["_customer_amounts"] = dict(curr_cust_amts) if curr_cust_amts else {}

    # ── Customer metrics (computed before S&M so churn_rate is available) ─
    # Use actual customer ID sets for churn — not just count difference.
    # A customer who paid last month but NOT this month is churned,
    # even if a new customer replaced them (net count stays the same).
    all_cust_ids = rev.get("customer_ids", set())
    n_active = len(all_cust_ids) if all_cust_ids else 0
    prev_cust_ids = prev.get("_revenue_customer_ids", set()) if prev else set()
    prev_cust_count = len(prev_cust_ids)

    if n_active > 0 and prev_cust_count > 0:
        churned_ids = prev_cust_ids - all_cust_ids
        lost = len(churned_ids)
        _safe_set(kpis, "churn_rate", lost / prev_cust_count * 100)
        _safe_set(kpis, "logo_retention", (1 - lost / prev_cust_count) * 100)
        if total_rev > 0 and prev.get("_total_revenue", 0) > 0:
            _safe_set(kpis, "nrr", total_rev / prev["_total_revenue"] * 100)
    elif n_active > 0 and not prev:
        _record_diagnostic(kpis, "churn_rate", None,
            "First month of data — churn rate requires at least two months to compare customer bases.")
        _record_diagnostic(kpis, "logo_retention", None,
            "First month of data — logo retention requires prior month comparison.")
        _record_diagnostic(kpis, "nrr", None,
            "First month of data — net revenue retention requires prior month revenue.")
    kpis["_active_customers"] = n_active
    kpis["_revenue_customer_ids"] = set(all_cust_ids)  # carry forward for next month's churn

    # Pricing power index = ARPU change% - customer volume change%
    if prev and prev.get("_active_customers", 0) > 0 and n_active > 0:
        prev_arpu = prev.get("_total_revenue", 0) / prev["_active_customers"] if prev.get("_active_customers", 0) > 0 else 0
        curr_arpu = total_rev / n_active if n_active > 0 and total_rev > 0 else 0
        if prev_arpu > 0:
            arpu_chg = (curr_arpu - prev_arpu) / prev_arpu * 100
            vol_chg = (n_active - prev["_active_customers"]) / prev["_active_customers"] * 100
            _safe_set(kpis, "pricing_power_index", arpu_chg - vol_chg)
    elif not prev and n_active > 0:
        _record_diagnostic(kpis, "pricing_power_index", None,
            "First month — pricing power requires prior month ARPU comparison.")

    # ── Expense / burn metrics ───────────────────────────────────────────
    if total_exp > 0:
        net_burn = total_exp - total_rev
        _safe_set(kpis, "cash_burn", net_burn)
        if kpis.get("arr", 0) > 0 and prev and prev.get("arr", 0) > 0:
            net_new_arr = kpis["arr"] - prev["arr"]
            min_meaningful = prev["arr"] * 0.001
            bm = _safe_ratio(net_burn, net_new_arr, min_denom=max(min_meaningful, 100))
            _safe_set(kpis, "burn_multiple", bm, net_new_arr=net_new_arr)
        elif not prev:
            _record_diagnostic(kpis, "burn_multiple", None,
                "First month — burn multiple requires prior month ARR to compute net new ARR.")

    # ── Sales & marketing efficiency ─────────────────────────────────────
    if sm_exp > 0:
        if total_rev > 0:
            # Sales efficiency = ARR / annualized S&M spend (both annual)
            annual_sm = sm_exp * 12
            se = _safe_ratio(kpis.get("arr", total_rev * 12), annual_sm, min_denom=100)
            _safe_set(kpis, "sales_efficiency", se, sm_exp=annual_sm)
        if new_cust > 0:
            cac = sm_exp / new_cust
            arpu = total_rev / max(len(rev.get("customer_ids", set())), 1)
            gm_pct_raw = kpis.get("gross_margin")
            if gm_pct_raw is not None:
                gm_pct = gm_pct_raw / 100
                if arpu * gm_pct > 0:
                    payback = _safe_ratio(cac, arpu * gm_pct, min_denom=0.01)
                    _safe_set(kpis, "cac_payback", payback, arpu=arpu, gm_pct=gm_pct_raw)
                    _safe_set(kpis, "payback_period", payback, arpu=arpu, gm_pct=gm_pct_raw)
                churn = kpis.get("churn_rate", 0)
                if churn > 0.05:
                    ltv = _safe_ratio(arpu * gm_pct, churn / 100, min_denom=0.0005)
                    if ltv is not None:
                        _safe_set(kpis, "customer_ltv", ltv, churn_rate=churn)
                        ltv_cac_val = _safe_ratio(ltv, cac, min_denom=1)
                        _safe_set(kpis, "ltv_cac", ltv_cac_val, churn_rate=churn, cac=cac)
                elif churn <= 0.05:
                    _record_diagnostic(kpis, "customer_ltv", None,
                        f"Churn rate is {churn:.2f}% (near zero) — LTV formula produces "
                        f"unreliable extreme values when churn approaches zero. "
                        f"LTV will be reported when churn exceeds 0.05%.")
                    _record_diagnostic(kpis, "ltv_cac", None,
                        f"Cannot compute — requires customer LTV which is unavailable "
                        f"due to near-zero churn rate ({churn:.2f}%).")
            else:
                _record_diagnostic(kpis, "cac_payback", None,
                    "Cannot compute — gross margin is not available. "
                    "Ensure expenses include COGS-categorised entries (hosting, infrastructure, direct cost).")
                _record_diagnostic(kpis, "customer_ltv", None,
                    "Cannot compute — gross margin is required but not available. "
                    "Tag COGS expenses in your accounting system to enable margin calculation.")

    # ── Pipeline metrics ─────────────────────────────────────────────────
    if deals_count == 0 and total_rev > 0:
        _record_diagnostic(kpis, "win_rate", None,
            "No pipeline deals data for this month. Connect your CRM (Salesforce/HubSpot) to enable pipeline KPIs.")
        _record_diagnostic(kpis, "pipeline_conversion", None,
            "No pipeline data — connect CRM to compute pipeline conversion rate.")
    if deals_count > 0:
        _safe_set(kpis, "win_rate", deals_won / deals_count * 100)
        if deals_won > 0:
            _safe_set(kpis, "avg_deal_size", won_value / deals_won)
        else:
            _record_diagnostic(kpis, "avg_deal_size", None,
                "No deals won this month — average deal size requires at least one closed-won deal.")
        if total_rev > 0:
            _safe_set(kpis, "pipeline_conversion", won_value / pipe_value * 100 if pipe_value > 0 else 0)

        # Pipeline velocity = (deals_won * avg_deal_size * win_rate%) / avg_days
        avg_days = (pipe.get("total_days_in_pipeline", 0) / pipe.get("deals_with_duration", 1)
                    if pipe.get("deals_with_duration", 0) > 0 else 45)
        if avg_days > 0 and deals_won > 0:
            velocity = (deals_won * (won_value / deals_won) * (deals_won / deals_count)) / avg_days
            _safe_set(kpis, "pipeline_velocity", velocity)
        elif deals_won == 0:
            _record_diagnostic(kpis, "pipeline_velocity", None,
                "No deals won this month — pipeline velocity requires closed deals with duration data.")

        # Quota attainment (using won_value as proxy vs target if available)
        if won_value > 0 and pipe_value > 0:
            _safe_set(kpis, "quota_attainment", won_value / pipe_value * 100)
        elif won_value == 0:
            _record_diagnostic(kpis, "quota_attainment", None,
                "No revenue from won deals this month — quota attainment requires closed-won deal value.")

    # ── Invoice / AR metrics ─────────────────────────────────────────────
    if inv_count == 0 and total_rev > 0:
        _record_diagnostic(kpis, "dso", None,
            "No invoice data for this month. Connect accounting system to compute AR metrics.")
        _record_diagnostic(kpis, "cash_conv_cycle", None,
            "No invoice data — cash conversion cycle requires DSO from invoices.")
    if inv_count > 0 and inv_total > 0:
        avg_dso = inv.get("days_outstanding_sum", 0) / inv_count
        _safe_set(kpis, "dso", avg_dso)
        if avg_dso > 0:
            _safe_set(kpis, "ar_turnover", 365 / avg_dso)
            _safe_set(kpis, "avg_collection_period", avg_dso)
        overdue = inv.get("overdue_count", 0)
        if overdue > 0:
            _safe_set(kpis, "ar_aging_overdue", overdue / inv_count * 100)
        else:
            # Zero overdue is a GOOD result — report it as 0%, not missing
            _safe_set(kpis, "ar_aging_overdue", 0.0)
        _safe_set(kpis, "ar_aging_current", (1 - overdue / inv_count) * 100)

        # CEI = (Beg_AR + Revenue - End_AR) / (Beg_AR + Revenue - Current_AR) * 100
        beg_ar = prev.get("_ending_ar", inv_total) if prev else inv_total
        end_ar = inv.get("outstanding_total", 0.0)
        current_ar = end_ar - inv.get("overdue_amount", 0.0)
        denominator = beg_ar + total_rev - current_ar
        if denominator > 0:
            _safe_set(kpis, "cei", (beg_ar + total_rev - end_ar) / denominator * 100)

        # Cash conversion cycle = DSO + DIO - DPO
        dso_val = kpis.get("dso", 0)
        # DIO: Use balance sheet inventory if available. For SaaS (no inventory), defaults to 0.
        _inventory = bal.get("inventory", 0.0) if bal else 0.0
        dio = (_inventory / (cogs / 30)) if cogs > 0 and _inventory > 0 else 0.0
        # DPO: (total_expenses / 365) * 30 - days to pay vendors
        dpo = (total_exp / total_rev * 30) if total_rev > 0 else 30.0
        if dso_val > 0:
            _safe_set(kpis, "cash_conv_cycle", dso_val + dio - dpo)
        else:
            _record_diagnostic(kpis, "cash_conv_cycle", None,
                "DSO is zero or unavailable — cash conversion cycle requires "
                "meaningful days-sales-outstanding. Verify invoice dates.")

    # Store ending AR for next month's CEI
    kpis["_ending_ar"] = inv.get("outstanding_total", 0.0)

    # ── Operating leverage (MoM) ─────────────────────────────────────────
    if not prev:
        _record_diagnostic(kpis, "operating_leverage", None,
            "First month — operating leverage requires MoM revenue and OpEx changes.")
    elif prev and prev.get("_total_revenue", 0) > 0 and prev.get("_opex", 0) > 0:
        rev_chg = _safe_ratio(total_rev - prev["_total_revenue"], prev["_total_revenue"], scale=100) or 0
        opex_chg = _safe_ratio(opex - prev["_opex"], prev["_opex"], scale=100) or 0
        ol = _safe_ratio(rev_chg, opex_chg, min_denom=0.5)
        _safe_set(kpis, "operating_leverage", ol, opex_chg=opex_chg)
        # If _safe_set withheld it (bounds) or ol was None (denominator too small), diagnose
        if "operating_leverage" not in kpis and "operating_leverage" not in kpis.get("_diagnostics", {}):
            _record_diagnostic(kpis, "operating_leverage", None,
                f"OpEx change of {opex_chg:.1f}% is too small to produce a meaningful leverage ratio.")
    elif prev:
        _record_diagnostic(kpis, "operating_leverage", None,
            "Prior month revenue or OpEx data insufficient to compute MoM leverage ratio.")

    # ── Headcount / efficiency metrics ───────────────────────────────────
    if headcount > 0:
        if total_rev > 0:
            _safe_set(kpis, "rev_per_employee", total_rev * 12 / headcount)
            _safe_set(kpis, "headcount_eff", total_rev / headcount)
    elif total_rev > 0:
        _record_diagnostic(kpis, "rev_per_employee", None,
            "Cannot compute — no employee headcount data for this month. "
            "Ensure canonical_employees has hire_date and active status for all staff.")
        _record_diagnostic(kpis, "headcount_eff", None,
            "Cannot compute — no employee headcount data for this month.")

    # Ramp time: avg deal duration for pipeline entries can proxy
    if pipe.get("deals_with_duration", 0) > 0:
        avg_ramp = pipe["total_days_in_pipeline"] / pipe["deals_with_duration"] / 30  # months
        _safe_set(kpis, "ramp_time", avg_ramp)

    # ── Marketing metrics ────────────────────────────────────────────────
    mkt_spend = mkt.get("total_spend", 0.0)
    mkt_leads = mkt.get("total_leads", 0)
    mkt_conv = mkt.get("total_conversions", 0)

    if mkt_spend > 0 and mkt_leads > 0:
        _safe_set(kpis, "cpl", mkt_spend / mkt_leads)
    if mkt_leads > 0 and mkt_conv > 0:
        _safe_set(kpis, "mql_sql_rate", mkt_conv / mkt_leads * 100)
    if mkt_spend > 0 and total_rev > 0:
        _safe_set(kpis, "marketing_roi", (total_rev - mkt_spend) / mkt_spend * 100)

    # ── Balance sheet metrics ────────────────────────────────────────────
    cash_bal = bal.get("cash_balance", 0.0)
    curr_assets = bal.get("current_assets", 0.0)
    curr_liab = bal.get("current_liabilities", 0.0)

    if cash_bal > 0:
        monthly_burn = total_exp - total_rev
        if monthly_burn > 0:
            runway = _safe_ratio(cash_bal, monthly_burn, min_denom=100)
            _safe_set(kpis, "cash_runway", runway)
        else:
            # Cash-flow positive: no burn → record diagnostic explaining
            _record_diagnostic(kpis, "cash_runway", None,
                               "Company is cash-flow positive this month (revenue exceeds expenses). "
                               "Cash runway is not applicable when there is no net burn. "
                               "This is a positive signal — no corrective action required.")
    if curr_liab > 0:
        _safe_set(kpis, "current_ratio", curr_assets / curr_liab)
    if curr_assets > 0:
        _safe_set(kpis, "working_capital", (curr_assets - curr_liab) / curr_assets * 100)

    # ── Time tracking metrics ────────────────────────────────────────────
    total_billable = time_t.get("total_billable", 0.0)
    total_hours = time_t.get("total_hours", 0.0)
    if total_hours > 0:
        _safe_set(kpis, "billable_utilization", total_billable / total_hours * 100)

    # ── Survey metrics ───────────────────────────────────────────────────
    nps_scores = surv.get("nps_scores", [])
    csat_scores = surv.get("csat_scores", [])
    if nps_scores:
        _safe_set(kpis, "product_nps", sum(nps_scores) / len(nps_scores))
    if csat_scores:
        _safe_set(kpis, "csat", sum(csat_scores) / len(csat_scores))

    # ── Support metrics ──────────────────────────────────────────────────
    ticket_count = supp.get("ticket_count", 0)
    if ticket_count > 0 and n_active > 0:
        _safe_set(kpis, "support_volume", ticket_count / n_active)

    # ── Product usage metrics ────────────────────────────────────────────
    active_users = len(usage.get("active_users", set()))
    activated = usage.get("activated_users", 0)
    features_used = len(usage.get("features_used", set()))
    ttv_days = usage.get("time_to_value_days", [])

    if active_users > 0 and activated > 0:
        _safe_set(kpis, "activation_rate", activated / active_users * 100)
    if ttv_days:
        _safe_set(kpis, "time_to_value", sum(ttv_days) / len(ttv_days))
    if features_used > 0:
        _safe_set(kpis, "feature_adoption", features_used / _TOTAL_FEATURES * 100)

    # ── Removed fabricated proxies ────────────────────────────────────────
    # automation_rate: Removed — no real data source (was guessing from ticket counts).
    #   Re-add when product analytics integration provides actual automation metrics.
    # organic_traffic: Removed — was incrementing 1% per month unconditionally.
    #   Re-add when Google Analytics or similar integration is available.
    # brand_awareness: Removed — was using arbitrary formula (100 - CPL * 0.5).
    #   Re-add when brand survey or awareness measurement tool is integrated.

    # ── Health score (composite meta-KPI) ────────────────────────────────
    # Lightweight composite: avg of normalised sub-scores
    health_inputs = []
    if kpis.get("gross_margin") is not None:
        health_inputs.append(min(kpis["gross_margin"] / 70 * 100, 100))
    if kpis.get("nrr") is not None:
        # NRR is computed as total_rev / prev_rev × 100; typical healthy
        # value is 103-108 for growth-stage companies.  Use 105 (series_a
        # p50 benchmark) as the reference point, not 110.
        health_inputs.append(min(kpis["nrr"] / 105 * 100, 100))
    if kpis.get("churn_rate") is not None:
        health_inputs.append(max(0, 100 - kpis["churn_rate"] * 10))
    if kpis.get("revenue_growth") is not None:
        health_inputs.append(min(max(kpis["revenue_growth"] * 5 + 50, 0), 100))
    if kpis.get("cash_runway") is not None:
        health_inputs.append(min(kpis["cash_runway"] / 18 * 100, 100))
    if health_inputs:
        _safe_set(kpis, "health_score", sum(health_inputs) / len(health_inputs))

    return kpis


def _compute_derived_kpis(
    sorted_months: list[tuple[int, int]],
    monthly_kpis: dict[tuple[int, int], dict],
    summary: dict,
) -> None:
    """Second pass: compute KPIs that depend on multi-month history."""
    for i, ym in enumerate(sorted_months):
        kpis = monthly_kpis[ym]
        prev = monthly_kpis.get(sorted_months[i - 1]) if i > 0 else None

        # Growth efficiency = ARR growth / burn multiple
        arr_g = kpis.get("arr_growth")
        bm = kpis.get("burn_multiple")
        if arr_g is not None and bm is not None and abs(bm) > 0.01:
            _safe_set(kpis, "growth_efficiency", arr_g / abs(bm))
        elif "growth_efficiency" not in kpis:
            reason_parts = []
            if arr_g is None: reason_parts.append("ARR growth")
            if bm is None: reason_parts.append("burn multiple")
            if bm is not None and abs(bm) <= 0.01: reason_parts.append("meaningful burn multiple (near zero)")
            _record_diagnostic(kpis, "growth_efficiency", None,
                f"Cannot compute — requires {' and '.join(reason_parts) if reason_parts else 'upstream KPIs'} "
                f"which {'is' if len(reason_parts)==1 else 'are'} not available this month.")

        # Revenue momentum = current rev growth / rolling avg rev growth
        if i >= 2:
            recent = [monthly_kpis[sorted_months[j]].get("revenue_growth")
                      for j in range(max(0, i - 5), i + 1)]
            valid = [x for x in recent if x is not None]
            if valid and kpis.get("revenue_growth") is not None:
                avg_rg = sum(valid) / len(valid)
                if abs(avg_rg) > 0.01:
                    _safe_set(kpis, "revenue_momentum", kpis["revenue_growth"] / avg_rg)
                else:
                    _record_diagnostic(kpis, "revenue_momentum", None,
                        "Average revenue growth is near zero — momentum ratio is undefined.")
        elif i < 2:
            _record_diagnostic(kpis, "revenue_momentum", None,
                "Requires at least 3 months of data to compute rolling average.")

        # Revenue fragility = (concentration x churn) / NRR
        cc = kpis.get("customer_concentration")
        cr = kpis.get("churn_rate")
        nrr = kpis.get("nrr")
        if cc is not None and cr is not None and nrr and nrr > 0:
            _safe_set(kpis, "revenue_fragility", (cc * cr) / nrr)
        elif "revenue_fragility" not in kpis:
            reason_parts = []
            if cc is None: reason_parts.append("customer concentration")
            if cr is None: reason_parts.append("churn rate")
            if not nrr: reason_parts.append("NRR")
            _record_diagnostic(kpis, "revenue_fragility", None,
                f"Cannot compute — requires {', '.join(reason_parts) if reason_parts else 'upstream KPIs'}.")

        # Burn convexity = delta burn_multiple MoM
        if prev and bm is not None and prev.get("burn_multiple") is not None:
            _safe_set(kpis, "burn_convexity", bm - prev["burn_multiple"])
        elif "burn_convexity" not in kpis:
            if i == 0:
                _record_diagnostic(kpis, "burn_convexity", None,
                    "First month of data — requires prior month's burn multiple.")
            elif bm is None:
                _record_diagnostic(kpis, "burn_convexity", None,
                    "Burn multiple not available this month — cannot compute MoM change.")
            elif prev and prev.get("burn_multiple") is None:
                _record_diagnostic(kpis, "burn_convexity", None,
                    "Prior month's burn multiple was unavailable — cannot compute delta.")

        # Margin volatility = std dev of last 6 months of gross_margin
        if i >= 2:
            window = [monthly_kpis[sorted_months[j]].get("gross_margin")
                      for j in range(max(0, i - 5), i + 1)]
            valid_gm = [x for x in window if x is not None]
            if len(valid_gm) >= 3:
                mean_gm = sum(valid_gm) / len(valid_gm)
                variance = sum((x - mean_gm) ** 2 for x in valid_gm) / len(valid_gm)
                _safe_set(kpis, "margin_volatility", variance ** 0.5)
            else:
                _record_diagnostic(kpis, "margin_volatility", None,
                    f"Insufficient gross margin history ({len(valid_gm)} of 3 required months).")
        elif i < 2:
            _record_diagnostic(kpis, "margin_volatility", None,
                "Requires at least 3 months of data to compute standard deviation.")

        # Customer decay slope = delta churn_rate MoM
        if prev and cr is not None and prev.get("churn_rate") is not None:
            _safe_set(kpis, "customer_decay_slope", cr - prev["churn_rate"])
        elif "customer_decay_slope" not in kpis:
            if i == 0:
                _record_diagnostic(kpis, "customer_decay_slope", None,
                    "First month of data — requires prior month's churn rate.")
            elif cr is None:
                _record_diagnostic(kpis, "customer_decay_slope", None,
                    "Churn rate not available this month — cannot compute MoM change.")
            elif prev and prev.get("churn_rate") is None:
                _record_diagnostic(kpis, "customer_decay_slope", None,
                    "Prior month's churn rate was unavailable — cannot compute delta.")

        # Remove internal-only keys before final output
        for internal_key in ("_active_customers", "_revenue_customer_ids",
                             "_total_revenue", "_total_expenses",
                             "_opex", "_cogs", "_customer_amounts", "_ending_ar"):
            kpis.pop(internal_key, None)


# ── Existing data merge helper ────────────────────────────────────────────────

def _load_existing_csv_kpis(conn, workspace_id: str) -> dict:
    """Load KPIs from monthly_data rows NOT created by the connector aggregator."""
    existing: dict[tuple[int, int], dict] = {}
    try:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data "
            "WHERE workspace_id=? AND (upload_id IS NULL OR upload_id != ?)",
            [workspace_id, CONNECTOR_UPLOAD_SENTINEL],
        ).fetchall()
        for r in rows:
            yr = int(r["year"] if isinstance(r, dict) else r[0])
            mo = int(r["month"] if isinstance(r, dict) else r[1])
            raw = r["data_json"] if isinstance(r, dict) else r[2]
            try:
                parsed = json.loads(raw)
                # Filter non-numeric values at load time to prevent type corruption
                clean = {}
                for k, v in parsed.items():
                    if k.startswith("_"):
                        continue  # Skip metadata
                    if isinstance(v, (int, float)) and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                        clean[k] = v
                    # Non-numeric values (strings like "N/A", "#DIV/0!") are silently dropped
                existing[(yr, mo)] = clean
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception:
        pass
    return existing


# ── Safe utility helpers ──────────────────────────────────────────────────────

def _clean_empty_buckets(out: dict, key_field: str, min_value=0) -> dict:
    """Remove defaultdict-created empty buckets where the key metric has no real data.
    Prevents months with zero activity from appearing as processed."""
    return {ym: b for ym, b in out.items()
            if (isinstance(b.get(key_field), (int, float)) and b.get(key_field, 0) > min_value)
            or (isinstance(b.get(key_field), set) and len(b.get(key_field, set())) > 0)
            or (isinstance(b.get(key_field), list) and len(b.get(key_field, [])) > 0)}


def _safe_query(conn, sql: str, params: list) -> list:
    """Execute a query, returning empty list if the table doesn't exist yet."""
    try:
        cursor = conn.execute(sql, params)
        # Use fetchall — fetchmany not available on all cursor wrappers
        return cursor.fetchall()
    except Exception as exc:
        if "no such table" in str(exc).lower() or "does not exist" in str(exc).lower():
            return []
        # PostgreSQL: "relation does not exist"
        if "relation" in str(exc).lower() and "does not exist" in str(exc).lower():
            return []
        raise


def _safe_float(val) -> Optional[float]:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if math.isfinite(f) else None
    except (ValueError, TypeError):
        return None


def _safe_set(target: dict, key: str, value, **context) -> None:
    """Set a KPI value only if it is a valid, finite, and reasonable number.

    Every KPI is validated against bounds — either a KPI-specific bound with
    a tailored diagnostic, or a universal bound derived from the KPI's unit
    type (pct, ratio, usd, days, months, score).  No KPI escapes validation.

    If the value is outside bounds, it is WITHHELD and a diagnostic is
    recorded explaining exactly why and what input data to fix.
    """
    if value is None:
        return
    try:
        f = float(value)
        if not math.isfinite(f):
            _record_diagnostic(target, key, value,
                               f"{key} computed as {'NaN' if math.isnan(f) else 'Infinity'} "
                               f"— one or more input values are missing or zero. "
                               f"Check upstream data sources for this metric.")
            return

        # Universal bounds check — every KPI, no exceptions
        bounds = _get_bounds(key)
        if bounds:
            lo, hi, msg_template = bounds
            if (lo is not None and f < lo) or (hi is not None and f > hi):
                try:
                    msg = msg_template.format(key=key, value=f, **context)
                except (KeyError, ValueError, TypeError):
                    msg = (f"{key} computed as {f:,.4f} which is outside the "
                           f"reasonable range [{lo}, {hi}]. Review input data for "
                           f"this metric and its upstream dependencies.")
                _record_diagnostic(target, key, f, msg)
                return

        target[key] = f
    except (ValueError, TypeError):
        pass


def _record_diagnostic(target: dict, key: str, raw_value, message: str) -> None:
    """Record a diagnostic for a KPI value that was withheld."""
    diags = target.setdefault("_diagnostics", {})
    diags[key] = {
        "computed_value": raw_value if isinstance(raw_value, (int, float)) and math.isfinite(raw_value) else None,
        "withheld": True,
        "reason": message,
    }


def _safe_ratio(numerator: float, denominator: float,
                *, scale: float = 1.0, min_denom: float = 0.01) -> Optional[float]:
    """Compute numerator / denominator safely.

    Returns None if the denominator is below min_denom (avoiding division
    by near-zero values that produce extreme outliers).  The result is
    multiplied by *scale* (default 1.0; use 100.0 for percentages).
    """
    if denominator is None or abs(denominator) < min_denom:
        return None
    try:
        result = (numerator / denominator) * scale
        if not math.isfinite(result):
            return None
        return result
    except (ZeroDivisionError, ValueError, TypeError):
        return None


def _parse_period(raw) -> Optional[tuple[int, int]]:
    """Parse a date/period string into (year, month). Handles many formats."""
    if raw is None:
        return None
    # Handle datetime objects directly (e.g., from openpyxl reading Excel date cells)
    if isinstance(raw, datetime):
        if 2000 <= raw.year <= 2050:
            return (raw.year, raw.month)
        return None
    s = str(raw).strip()
    if not s:
        return None

    # Unix timestamp (seconds or milliseconds)
    try:
        num = float(s)
        if num > 1e12:
            num /= 1000
        if 946684800 <= num <= 2524608000:
            dt = datetime.utcfromtimestamp(num)
            return (dt.year, dt.month)
    except (ValueError, TypeError, OSError):
        pass

    # ISO / common date formats
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y",
                "%Y-%m", "%b %Y", "%B %Y"):
        try:
            clean = s.rstrip("Z")
            if "+" in clean and clean.count("+") == 1:
                clean = clean[:clean.rindex("+")]
            dt = datetime.strptime(clean, fmt)
            if 2000 <= dt.year <= 2050:
                return (dt.year, dt.month)
        except (ValueError, TypeError):
            continue
    return None


def _parse_date(raw) -> Optional[datetime]:
    """Parse a date string into a datetime object."""
    if raw is None:
        return None
    ym = _parse_period(raw)
    if ym is None:
        return None
    s = str(raw).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%d", "%Y/%m/%d"):
        try:
            clean = s.rstrip("Z")
            if "+" in clean:
                clean = clean[:clean.rindex("+")]
            return datetime.strptime(clean, fmt)
        except (ValueError, TypeError):
            continue
    return datetime(ym[0], ym[1], 1)


def _finalise_summary(summary: dict) -> dict:
    """Convert sets to lists for JSON serialisation."""
    summary["kpis_computed"] = sorted(summary["kpis_computed"])
    return summary
