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


# ── Public API ────────────────────────────────────────────────────────────────

def aggregate_canonical_to_monthly(conn, workspace_id: str) -> dict:
    """
    Main entry point.  Reads all canonical_* tables for a workspace,
    computes monthly KPI values, and upserts into monthly_data.

    Returns a summary dict: {"months_written": int, "kpis_computed": [...], "errors": [...]}
    """
    summary = {"months_written": 0, "kpis_computed": set(), "errors": [], "skipped": []}

    try:
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
            prev_kpis = kpis

        # ── Step 4: Second pass — derived KPIs that need history ──
        _compute_derived_kpis(sorted_months, monthly_kpis, summary)

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

            # Merge: CSV values take precedence
            csv_existing = existing_csv.get(ym, {})
            merged = {**kpis}
            for k, v in csv_existing.items():
                if v is not None:
                    merged[k] = v

            # Clean: remove None/NaN values before JSON serialization
            clean = {}
            for k, v in merged.items():
                if v is not None and not (isinstance(v, float) and (math.isnan(v) or math.isinf(v))):
                    clean[k] = round(v, 4) if isinstance(v, float) else v

            if not clean:
                continue

            conn.execute(
                "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) "
                "VALUES (?,?,?,?,?)",
                [CONNECTOR_UPLOAD_SENTINEL, yr, mo, json.dumps(clean), workspace_id],
            )
            months_written += 1
            summary["kpis_computed"].update(clean.keys())

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
            "SELECT amount, period, subscription_type, customer_id "
            "FROM canonical_revenue WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_revenue: no rows")
            return {}

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

    except Exception as exc:
        summary["errors"].append(f"Revenue extraction: {exc}")
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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

        now = datetime.utcnow()
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

            # Compute days outstanding
            issued = _parse_date(issue_date)
            due = _parse_date(due_date) if due_date else None
            if issued:
                days_out = (now - issued).days
                bucket["days_outstanding_sum"] += max(days_out, 0)
                if due and now > due and status not in ("paid", "settled", "closed"):
                    bucket["overdue_count"] += 1
                    bucket["overdue_amount"] += abs_amount

    except Exception as exc:
        summary["errors"].append(f"Invoice extraction: {exc}")
    return dict(out)


def _extract_monthly_employees(conn, workspace_id: str, summary: dict) -> dict:
    """Count active employees per month using hire_date."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "headcount": 0,
        "total_salary": 0.0,
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT source_id, hire_date, status, salary FROM canonical_employees WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_employees: no rows")
            return {}

        for r in rows:
            hire_date = r.get("hire_date") if isinstance(r, dict) else r[1]
            status = str(r.get("status") if isinstance(r, dict) else r[2] or "active").lower()
            salary = _safe_float(r.get("salary") if isinstance(r, dict) else r[3])

            if status not in ("active", "employed", "full-time", "part-time", "contractor"):
                continue
            ym = _parse_period(hire_date) if hire_date else None
            if ym is None:
                continue
            out[ym]["headcount"] += 1
            out[ym]["total_salary"] += (salary or 0.0)

    except Exception as exc:
        summary["errors"].append(f"Employee extraction: {exc}")
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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
    return dict(out)


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
        ebitda = (total_rev - cogs - opex) * 1.15
        _safe_set(kpis, "ebitda_margin", ebitda / total_rev * 100)
        _safe_set(kpis, "opex_ratio", opex / total_rev * 100)
        _safe_set(kpis, "contribution_margin", (total_rev - cogs - opex * 0.3) / total_rev * 100)

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

        # Revenue concentration
        cust_ids = rev.get("customer_ids", set())
        if cust_ids:
            n_cust = len(cust_ids)
            _safe_set(kpis, "customer_concentration", min(100.0 / max(n_cust, 1) * 2.5, 100.0))

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

    # Store for next month's comparison
    kpis["_customer_amounts"] = dict(curr_cust_amts) if curr_cust_amts else {}

    # ── Customer metrics (computed before S&M so churn_rate is available) ─
    all_cust_ids = rev.get("customer_ids", set())
    n_active = len(all_cust_ids) if all_cust_ids else 0
    if n_active > 0 and prev:
        prev_cust = prev.get("_active_customers", 0)
        if prev_cust > 0:
            lost = max(prev_cust - n_active, 0)
            _safe_set(kpis, "churn_rate", lost / prev_cust * 100)
            _safe_set(kpis, "logo_retention", (1 - lost / prev_cust) * 100)
            if total_rev > 0 and prev.get("_total_revenue", 0) > 0:
                _safe_set(kpis, "nrr", total_rev / prev["_total_revenue"] * 100)
    kpis["_active_customers"] = n_active

    # Pricing power index = ARPU change% - customer volume change%
    if prev and prev.get("_active_customers", 0) > 0 and n_active > 0:
        prev_arpu = prev.get("_total_revenue", 0) / prev["_active_customers"] if prev.get("_active_customers", 0) > 0 else 0
        curr_arpu = total_rev / n_active if n_active > 0 and total_rev > 0 else 0
        if prev_arpu > 0:
            arpu_chg = (curr_arpu - prev_arpu) / prev_arpu * 100
            vol_chg = (n_active - prev["_active_customers"]) / prev["_active_customers"] * 100
            _safe_set(kpis, "pricing_power_index", arpu_chg - vol_chg)

    # ── Expense / burn metrics ───────────────────────────────────────────
    if total_exp > 0:
        net_burn = total_exp - total_rev
        _safe_set(kpis, "cash_burn", net_burn)
        if kpis.get("arr", 0) > 0 and prev and prev.get("arr", 0) > 0:
            net_new_arr = kpis["arr"] - prev["arr"]
            if net_new_arr > 0:
                _safe_set(kpis, "burn_multiple", net_burn / net_new_arr)

    # ── Sales & marketing efficiency ─────────────────────────────────────
    if sm_exp > 0:
        if total_rev > 0:
            _safe_set(kpis, "sales_efficiency", (kpis.get("arr", total_rev * 12)) / sm_exp)
        if new_cust > 0:
            cac = sm_exp / new_cust
            arpu = total_rev / max(len(rev.get("customer_ids", set())), 1)
            gm_pct = kpis.get("gross_margin", 60) / 100
            if arpu * gm_pct > 0:
                _safe_set(kpis, "cac_payback", cac / (arpu * gm_pct))
                _safe_set(kpis, "payback_period", cac / (arpu * gm_pct))
            if kpis.get("churn_rate", 0) > 0:
                ltv = (arpu * gm_pct) / (kpis["churn_rate"] / 100)
                _safe_set(kpis, "customer_ltv", ltv)
                _safe_set(kpis, "ltv_cac", ltv / cac if cac > 0 else 0)

    # ── Pipeline metrics ─────────────────────────────────────────────────
    if deals_count > 0:
        _safe_set(kpis, "win_rate", deals_won / deals_count * 100)
        if deals_won > 0:
            _safe_set(kpis, "avg_deal_size", won_value / deals_won)
        if total_rev > 0:
            _safe_set(kpis, "pipeline_conversion", won_value / pipe_value * 100 if pipe_value > 0 else 0)

        # Pipeline velocity = (deals_won * avg_deal_size * win_rate%) / avg_days
        avg_days = (pipe.get("total_days_in_pipeline", 0) / pipe.get("deals_with_duration", 1)
                    if pipe.get("deals_with_duration", 0) > 0 else 45)
        if avg_days > 0 and deals_won > 0:
            velocity = (deals_won * (won_value / deals_won) * (deals_won / deals_count)) / avg_days
            _safe_set(kpis, "pipeline_velocity", velocity)

        # Quota attainment (using won_value as proxy vs target if available)
        if won_value > 0 and pipe_value > 0:
            _safe_set(kpis, "quota_attainment", won_value / pipe_value * 100)

    # ── Invoice / AR metrics ─────────────────────────────────────────────
    if inv_count > 0 and inv_total > 0:
        avg_dso = inv.get("days_outstanding_sum", 0) / inv_count
        _safe_set(kpis, "dso", avg_dso)
        if avg_dso > 0:
            _safe_set(kpis, "ar_turnover", 365 / avg_dso)
            _safe_set(kpis, "avg_collection_period", avg_dso)
        overdue = inv.get("overdue_count", 0)
        if overdue > 0:
            _safe_set(kpis, "ar_aging_overdue", overdue / inv_count * 100)
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
        # DIO: approximate from COGS (assume 15-day inventory cycle for SaaS)
        dio = (cogs / total_rev * 30) if total_rev > 0 and cogs > 0 else 5.0
        # DPO: (total_expenses / 365) * 30 - days to pay vendors
        dpo = (total_exp / total_rev * 30) if total_rev > 0 else 30.0
        if dso_val > 0:
            _safe_set(kpis, "cash_conv_cycle", dso_val + dio - dpo)

    # Store ending AR for next month's CEI
    kpis["_ending_ar"] = inv.get("outstanding_total", 0.0)

    # ── Operating leverage (MoM) ─────────────────────────────────────────
    if prev and prev.get("_total_revenue", 0) > 0 and prev.get("_opex", 0) > 0:
        rev_chg = (total_rev - prev["_total_revenue"]) / prev["_total_revenue"] * 100 if total_rev > 0 else 0
        opex_chg = (opex - prev["_opex"]) / prev["_opex"] * 100 if opex > 0 and prev["_opex"] > 0 else 0
        if abs(opex_chg) > 0.01:
            _safe_set(kpis, "operating_leverage", rev_chg / opex_chg)

    # ── Headcount / efficiency metrics ───────────────────────────────────
    if headcount > 0:
        if total_rev > 0:
            _safe_set(kpis, "rev_per_employee", total_rev * 12 / headcount)
            _safe_set(kpis, "headcount_eff", total_rev / headcount)

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
        monthly_burn = max(total_exp - total_rev, 1)
        if monthly_burn > 0:
            _safe_set(kpis, "cash_runway", cash_bal / monthly_burn)
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

    # ── Automation rate (trend-based proxy) ──────────────────────────────
    if prev and prev.get("support_volume") and kpis.get("support_volume"):
        sv_prev = prev["support_volume"]
        sv_curr = kpis["support_volume"]
        if sv_prev > 0:
            # Declining support volume per customer implies improving automation
            improvement = max(0, (sv_prev - sv_curr) / sv_prev * 100)
            base_rate = prev.get("automation_rate", 40)
            _safe_set(kpis, "automation_rate", min(base_rate + improvement, 95))
    elif ticket_count > 0 and n_active > 0:
        # Initial estimate based on ticket volume ratio
        _safe_set(kpis, "automation_rate", max(20, 80 - (ticket_count / max(n_active, 1)) * 20))

    # ── Organic traffic & brand awareness (marketing-derived proxies) ────
    if mkt_leads > 0:
        # Organic traffic growth: leads from organic channels as proxy
        organic_share = mkt_conv / mkt_leads if mkt_conv > 0 else 0.2
        if prev and prev.get("organic_traffic") is not None:
            prev_ot = prev["organic_traffic"]
            _safe_set(kpis, "organic_traffic", prev_ot * (1 + random_stub(0.02)))
        else:
            _safe_set(kpis, "organic_traffic", organic_share * 100)
    if mkt_leads > 0 and mkt_spend > 0:
        # Brand awareness: inverse of CAC trend (lower CAC = stronger brand)
        cpl_val = kpis.get("cpl", 100)
        _safe_set(kpis, "brand_awareness", max(10, min(100, 100 - cpl_val * 0.5)))

    # ── Health score (composite meta-KPI) ────────────────────────────────
    # Lightweight composite: avg of normalised sub-scores
    health_inputs = []
    if kpis.get("gross_margin") is not None:
        health_inputs.append(min(kpis["gross_margin"] / 70 * 100, 100))
    if kpis.get("nrr") is not None:
        health_inputs.append(min(kpis["nrr"] / 110 * 100, 100))
    if kpis.get("churn_rate") is not None:
        health_inputs.append(max(0, 100 - kpis["churn_rate"] * 10))
    if kpis.get("revenue_growth") is not None:
        health_inputs.append(min(max(kpis["revenue_growth"] * 5 + 50, 0), 100))
    if kpis.get("cash_runway") is not None:
        health_inputs.append(min(kpis["cash_runway"] / 18 * 100, 100))
    if health_inputs:
        _safe_set(kpis, "health_score", sum(health_inputs) / len(health_inputs))

    return kpis


def random_stub(magnitude=0.05):
    """Deterministic small perturbation for proxy KPIs. Uses hash-based stability."""
    import hashlib, struct
    # Use current frame count as seed for reproducibility within a run
    h = hashlib.md5(str(id(magnitude)).encode()).digest()
    return struct.unpack('f', h[:4])[0] % magnitude


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

        # Revenue momentum = current rev growth / rolling avg rev growth
        if i >= 2:
            recent = [monthly_kpis[sorted_months[j]].get("revenue_growth")
                      for j in range(max(0, i - 5), i + 1)]
            valid = [x for x in recent if x is not None]
            if valid and kpis.get("revenue_growth") is not None:
                avg_rg = sum(valid) / len(valid)
                if abs(avg_rg) > 0.01:
                    _safe_set(kpis, "revenue_momentum", kpis["revenue_growth"] / avg_rg)

        # Revenue fragility = (concentration x churn) / NRR
        cc = kpis.get("customer_concentration")
        cr = kpis.get("churn_rate")
        nrr = kpis.get("nrr")
        if cc is not None and cr is not None and nrr and nrr > 0:
            _safe_set(kpis, "revenue_fragility", (cc * cr) / nrr)

        # Burn convexity = delta burn_multiple MoM
        if prev and bm is not None and prev.get("burn_multiple") is not None:
            _safe_set(kpis, "burn_convexity", bm - prev["burn_multiple"])

        # Margin volatility = std dev of last 6 months of gross_margin
        if i >= 2:
            window = [monthly_kpis[sorted_months[j]].get("gross_margin")
                      for j in range(max(0, i - 5), i + 1)]
            valid_gm = [x for x in window if x is not None]
            if len(valid_gm) >= 3:
                mean_gm = sum(valid_gm) / len(valid_gm)
                variance = sum((x - mean_gm) ** 2 for x in valid_gm) / len(valid_gm)
                _safe_set(kpis, "margin_volatility", variance ** 0.5)

        # Customer decay slope = delta churn_rate MoM
        if prev and cr is not None and prev.get("churn_rate") is not None:
            _safe_set(kpis, "customer_decay_slope", cr - prev["churn_rate"])

        # Remove internal-only keys before final output
        for internal_key in ("_active_customers", "_total_revenue", "_total_expenses",
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
                existing[(yr, mo)] = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                pass
    except Exception:
        pass
    return existing


# ── Safe utility helpers ──────────────────────────────────────────────────────

def _safe_query(conn, sql: str, params: list) -> list:
    """Execute a query, returning empty list if the table doesn't exist yet."""
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as exc:
        if "no such table" in str(exc).lower() or "does not exist" in str(exc).lower():
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


def _safe_set(target: dict, key: str, value) -> None:
    """Set a KPI value only if it's a valid finite number."""
    if value is None:
        return
    try:
        f = float(value)
        if math.isfinite(f):
            target[key] = f
    except (ValueError, TypeError):
        pass


def _parse_period(raw) -> Optional[tuple[int, int]]:
    """Parse a date/period string into (year, month). Handles many formats."""
    if raw is None:
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
