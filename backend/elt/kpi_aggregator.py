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

Canonical tables consumed:
  - canonical_revenue   → MRR, ARR, revenue_growth, recurring_revenue, nrr,
                           revenue_quality, customer_concentration
  - canonical_expenses  → gross_margin, operating_margin, ebitda_margin,
                           opex_ratio, burn_multiple, burn_rate, cac_payback
  - canonical_customers → churn_rate, customer_ltv, logo_retention
  - canonical_pipeline  → pipeline_conversion, win_rate, avg_deal_size
  - canonical_invoices  → dso, avg_collection_period, ar_turnover
  - canonical_employees → headcount_eff, rev_per_employee

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
# Rows created by this aggregator use this upload_id so they can be identified,
# replaced on re-run, and distinguished from CSV-uploaded rows.
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
        revenue_by_month  = _extract_monthly_revenue(conn, workspace_id, summary)
        expense_by_month  = _extract_monthly_expenses(conn, workspace_id, summary)
        customer_by_month = _extract_monthly_customers(conn, workspace_id, summary)
        pipeline_by_month = _extract_monthly_pipeline(conn, workspace_id, summary)
        invoice_by_month  = _extract_monthly_invoices(conn, workspace_id, summary)
        employee_by_month = _extract_monthly_employees(conn, workspace_id, summary)

        # ── Step 2: Collect all months that have ANY data ──
        all_months: set[tuple[int, int]] = set()
        for source in (revenue_by_month, expense_by_month, customer_by_month,
                       pipeline_by_month, invoice_by_month, employee_by_month):
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
            merged = {**kpis}  # start with connector KPIs
            for k, v in csv_existing.items():
                if v is not None:
                    # CSV value exists — keep it, don't overwrite
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
    """Aggregate canonical_revenue by month."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "total_revenue": 0.0,
        "recurring_revenue": 0.0,
        "transaction_count": 0,
        "customer_ids": set(),
        "amounts": [],
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
                bucket["customer_ids"].add(str(cust_id))
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

            # Categorise: S&M vs COGS vs other operating
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
    })
    try:
        rows = _safe_query(
            conn,
            "SELECT amount, stage, close_date FROM canonical_pipeline WHERE workspace_id=?",
            [workspace_id],
        )
        if not rows:
            summary["skipped"].append("canonical_pipeline: no rows")
            return {}

        for r in rows:
            amount = _safe_float(r.get("amount") if isinstance(r, dict) else r[0])
            stage = str(r.get("stage") if isinstance(r, dict) else r[1] or "").lower()
            close_date = r.get("close_date") if isinstance(r, dict) else r[2]

            ym = _parse_period(close_date) if close_date else None
            if ym is None:
                continue

            bucket = out[ym]
            bucket["deals_count"] += 1
            bucket["total_pipeline_value"] += (amount or 0.0)

            if any(kw in stage for kw in ("won", "closed won", "closedwon", "contracted")):
                bucket["deals_won"] += 1
                bucket["won_value"] += (amount or 0.0)

    except Exception as exc:
        summary["errors"].append(f"Pipeline extraction: {exc}")
    return dict(out)


def _extract_monthly_invoices(conn, workspace_id: str, summary: dict) -> dict:
    """Aggregate canonical_invoices by month (issue_date)."""
    out: dict[tuple[int, int], dict] = defaultdict(lambda: {
        "invoice_total": 0.0,
        "invoice_count": 0,
        "overdue_count": 0,
        "overdue_amount": 0.0,
        "days_outstanding_sum": 0.0,
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

            bucket = out[ym]
            bucket["invoice_total"] += abs(amount)
            bucket["invoice_count"] += 1

            # Compute days outstanding
            issued = _parse_date(issue_date)
            due = _parse_date(due_date) if due_date else None
            if issued:
                days_out = (now - issued).days
                bucket["days_outstanding_sum"] += max(days_out, 0)
                if due and now > due and status not in ("paid", "settled", "closed"):
                    bucket["overdue_count"] += 1
                    bucket["overdue_amount"] += abs(amount)

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


# ── KPI computation ──────────────────────────────────────────────────────────

def _compute_month_kpis(
    ym: tuple[int, int],
    rev: dict,
    exp: dict,
    cust: dict,
    pipe: dict,
    inv: dict,
    emp: dict,
    prev: Optional[dict],
    summary: dict,
) -> dict[str, float]:
    """Compute all possible KPIs for a single month from canonical aggregates."""
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

    # Approximate OpEx = total expenses - COGS
    opex = total_exp - cogs

    # ── Revenue metrics ───────────────────────────────────────────────────
    if total_rev > 0:
        _safe_set(kpis, "gross_margin", (total_rev - cogs) / total_rev * 100)
        _safe_set(kpis, "operating_margin", (total_rev - cogs - opex) / total_rev * 100)
        ebitda = (total_rev - cogs - opex) * 1.15  # Approximate EBITDA from EBIT
        _safe_set(kpis, "ebitda_margin", ebitda / total_rev * 100)
        _safe_set(kpis, "opex_ratio", opex / total_rev * 100)
        _safe_set(kpis, "contribution_margin", (total_rev - cogs - opex * 0.3) / total_rev * 100)

        if recurring_rev > 0:
            _safe_set(kpis, "revenue_quality", recurring_rev / total_rev * 100)
            _safe_set(kpis, "recurring_revenue", recurring_rev / total_rev * 100)
            # MRR and ARR (as raw values in data_json, used by downstream)
            _safe_set(kpis, "mrr", recurring_rev)
            _safe_set(kpis, "arr", recurring_rev * 12)

        # Revenue growth (requires prior month)
        if prev and prev.get("total_revenue", 0) > 0:
            prev_rev = prev["total_revenue"]
            _safe_set(kpis, "revenue_growth", (total_rev - prev_rev) / prev_rev * 100)
            if prev.get("arr", 0) > 0 and kpis.get("arr", 0) > 0:
                _safe_set(kpis, "arr_growth", (kpis["arr"] - prev["arr"]) / prev["arr"] * 100)

        # Revenue concentration (max customer share)
        cust_ids = rev.get("customer_ids", set())
        if cust_ids and len(cust_ids) > 0:
            # Approximate: top customer gets 1/N of revenue, scaled by Pareto factor
            n_cust = len(cust_ids)
            _safe_set(kpis, "customer_concentration", min(100.0 / max(n_cust, 1) * 2.5, 100.0))

    # Store total_revenue in kpis for derived KPI calculations
    if total_rev > 0:
        kpis["total_revenue"] = total_rev

    # ── Expense / burn metrics ────────────────────────────────────────────
    if total_exp > 0:
        net_burn = total_exp - total_rev
        _safe_set(kpis, "cash_burn", net_burn)
        if kpis.get("arr", 0) > 0 and prev and prev.get("arr", 0) > 0:
            net_new_arr = kpis["arr"] - prev["arr"]
            if net_new_arr > 0:
                _safe_set(kpis, "burn_multiple", net_burn / net_new_arr)

    # ── Sales & marketing efficiency ──────────────────────────────────────
    if sm_exp > 0:
        if total_rev > 0:
            _safe_set(kpis, "sales_efficiency", (kpis.get("arr", total_rev * 12)) / sm_exp)
        if new_cust > 0:
            cac = sm_exp / new_cust
            arpu = total_rev / max(len(rev.get("customer_ids", set())), 1)
            gm_pct = kpis.get("gross_margin", 60) / 100
            if arpu * gm_pct > 0:
                _safe_set(kpis, "cac_payback", cac / (arpu * gm_pct))
            if kpis.get("churn_rate", 0) > 0:
                ltv = (arpu * gm_pct) / (kpis["churn_rate"] / 100)
                _safe_set(kpis, "customer_ltv", ltv)
                _safe_set(kpis, "ltv_cac", ltv / cac if cac > 0 else 0)

    # ── Customer metrics ──────────────────────────────────────────────────
    all_cust_ids = rev.get("customer_ids", set())
    n_active = len(all_cust_ids) if all_cust_ids else 0
    if n_active > 0 and prev:
        prev_cust = prev.get("_active_customers", 0)
        if prev_cust > 0:
            lost = max(prev_cust - n_active, 0)
            _safe_set(kpis, "churn_rate", lost / prev_cust * 100)
            _safe_set(kpis, "logo_retention", (1 - lost / prev_cust) * 100)
            # NRR approximation
            if total_rev > 0 and prev.get("total_revenue", 0) > 0:
                _safe_set(kpis, "nrr", total_rev / prev["total_revenue"] * 100)
    # Store active customer count for next month's churn calculation
    kpis["_active_customers"] = n_active

    # ── Pipeline metrics ──────────────────────────────────────────────────
    if deals_count > 0:
        _safe_set(kpis, "win_rate", deals_won / deals_count * 100)
        if deals_won > 0:
            _safe_set(kpis, "avg_deal_size", won_value / deals_won)
        if total_rev > 0:
            _safe_set(kpis, "pipeline_conversion", won_value / pipe_value * 100 if pipe_value > 0 else 0)

    # ── Invoice / AR metrics ──────────────────────────────────────────────
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

    # ── Headcount / efficiency metrics ────────────────────────────────────
    if headcount > 0:
        if total_rev > 0:
            _safe_set(kpis, "rev_per_employee", total_rev * 12 / headcount)  # Annualised
            _safe_set(kpis, "headcount_eff", total_rev / headcount)

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

        # Revenue momentum = current rev growth / rolling avg rev growth
        if i >= 2:
            recent = [monthly_kpis[sorted_months[j]].get("revenue_growth")
                      for j in range(max(0, i - 5), i + 1)]
            valid = [x for x in recent if x is not None]
            if valid and kpis.get("revenue_growth") is not None:
                avg_rg = sum(valid) / len(valid)
                if abs(avg_rg) > 0.01:
                    _safe_set(kpis, "revenue_momentum", kpis["revenue_growth"] / avg_rg)

        # Revenue fragility = (concentration × churn) / NRR
        cc = kpis.get("customer_concentration")
        cr = kpis.get("churn_rate")
        nrr = kpis.get("nrr")
        if cc is not None and cr is not None and nrr and nrr > 0:
            _safe_set(kpis, "revenue_fragility", (cc * cr) / nrr)

        # Burn convexity = Δ burn_multiple MoM
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

        # Customer decay slope = Δ churn_rate MoM
        if prev and cr is not None and prev.get("churn_rate") is not None:
            _safe_set(kpis, "customer_decay_slope", cr - prev["churn_rate"])

        # Remove internal-only keys before final output
        kpis.pop("_active_customers", None)
        kpis.pop("total_revenue", None)


# ── Existing data merge helper ────────────────────────────────────────────────

def _load_existing_csv_kpis(conn, workspace_id: str) -> dict:
    """Load KPIs from monthly_data rows NOT created by the connector aggregator.
    These are CSV-uploaded or seeded rows that should not be overwritten."""
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
        pass  # Table may not exist yet; that's fine
    return existing


# ── Safe utility helpers ──────────────────────────────────────────────────────

def _safe_query(conn, sql: str, params: list) -> list:
    """Execute a query, returning empty list if the table doesn't exist yet."""
    try:
        return conn.execute(sql, params).fetchall()
    except Exception as exc:
        # Table may not exist if connector hasn't synced that entity type yet
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
        if num > 1e12:  # milliseconds
            num /= 1000
        if 946684800 <= num <= 2524608000:  # 2000-01-01 to 2050-01-01
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
            # Strip timezone suffix that Python 3.9 can't parse
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
    # Fallback: first of the parsed month
    return datetime(ym[0], ym[1], 1)


def _finalise_summary(summary: dict) -> dict:
    """Convert sets to lists for JSON serialisation."""
    summary["kpis_computed"] = sorted(summary["kpis_computed"])
    return summary
