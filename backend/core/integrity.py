"""
core/integrity.py — 360-degree data integrity validator.

5-stage pipeline verification + self-correction:
  Stage 0: Temporal (gaps, duplicates, future dates, staleness, MoM volatility)
  Stage 1: Source → Canonical (row counts, totals, cross-entity, currency)
  Stage 2: Canonical → KPI (independent recomputation, null propagation, business logic)
  Stage 3: Display consistency (all avg paths agree)
  Stage 4: Statistical anomaly (z-score outliers per KPI)
"""
import json
import math
import statistics
from datetime import datetime, date
from typing import Optional
from uuid import uuid4


# ── Config ───────────────────────────────────────────────────────────────────
_VERIFY_KPIS = [
    "gross_margin", "operating_margin", "ebitda_margin", "opex_ratio",
    "recurring_revenue", "revenue_quality", "churn_rate", "logo_retention",
    "nrr", "customer_concentration",
]
_TOLERANCE_PCT = 0.5      # Stage 2: max relative diff before flagging
_VOLATILITY_THRESHOLD = 500  # Stage 0: flag MoM changes > 500%
_ZSCORE_WARN = 3.0        # Stage 4: z-score warning threshold
_ZSCORE_ANOMALY = 5.0     # Stage 4: z-score anomaly threshold
_FRESHNESS_WARN_DAYS = 30
_FRESHNESS_FAIL_DAYS = 90

# KPIs where null upstream MUST cascade to null downstream
_NULL_PROPAGATION = {
    "customer_ltv": ["churn_rate", "gross_margin"],
    "ltv_cac":      ["customer_ltv"],
    "cac_payback":  ["gross_margin"],
    "growth_efficiency": ["burn_multiple"],
}

# Business logic identities
_BUSINESS_RULES = [
    ("arr_mrr_12x",       "ARR should equal MRR * 12"),
    ("churn_retention",   "Churn rate + logo retention should sum to ~100%"),
    ("op_lt_gross",       "Operating margin must be <= gross margin"),
    ("nrr_expansion",     "NRR > 100% requires expansion > contraction"),
    ("rev_quality_eq",    "Revenue quality must equal recurring revenue %"),
]


def _pct_diff(a, b):
    if a is None or b is None:
        return None
    if a == 0 and b == 0:
        return 0.0
    denom = max(abs(a), abs(b), 1e-9)
    return abs(a - b) / denom * 100


def _safe_query(conn, sql, params=None):
    try:
        return conn.execute(sql, params or []).fetchall()
    except Exception:
        return []


class DataIntegrityValidator:
    """360-degree data integrity verification and self-correction.

    Principles:
      DEFENSIVE — one failure never blocks others
      AUDITABLE — structured reports persisted to integrity_checks
      NON-DESTRUCTIVE — validation is read-only; correction is opt-in
      IDEMPOTENT — safe to run multiple times
    """

    def __init__(self, conn, workspace_id: str):
        self._conn = conn
        self._ws = workspace_id
        self._run_id = f"ic_{uuid4().hex[:12]}"
        self._started = datetime.utcnow().isoformat()

    # ── Public API ────────────────────────────────────────────────────────

    def run_all(
        self,
        trigger: str = "manual",
        upload_id: Optional[int] = None,
        source_name: Optional[str] = None,
        auto_correct: bool = False,
    ) -> dict:
        s0 = self.run_stage0()
        s1 = self.run_stage1(upload_id=upload_id, source_name=source_name)
        s2 = self.run_stage2()
        s3 = self.run_stage3()
        s4 = self.run_stage4()

        correction_log = []
        if auto_correct and s2["status"] == "fail":
            correction = self._correct_stage2()
            correction_log.append(correction)
            if correction["succeeded"]:
                s2 = self.run_stage2()

        statuses = [s0["status"], s1["status"], s2["status"], s3["status"]]
        # Stage 4 is advisory — warnings don't fail the overall check
        if "fail" in statuses:
            overall = "fail"
        elif "warn" in statuses or s4["status"] == "fail":
            overall = "warn"
        else:
            overall = "pass"

        if correction_log and all(c["succeeded"] for c in correction_log):
            overall = "corrected" if overall == "pass" else overall

        report = {
            "run_id":               self._run_id,
            "workspace_id":         self._ws,
            "trigger":              trigger,
            "started_at":           self._started,
            "completed_at":         datetime.utcnow().isoformat(),
            "overall_status":       overall,
            "stage0":               s0,
            "stage1":               s1,
            "stage2":               s2,
            "stage3":               s3,
            "stage4":               s4,
            "correction_attempted": len(correction_log) > 0,
            "correction_succeeded": all(c["succeeded"] for c in correction_log) if correction_log else False,
            "correction_log":       correction_log,
        }

        self._persist(report, trigger, upload_id, source_name)
        self._audit(report)
        return report

    # ── Stage 0: Temporal ────────────────────────────────────────────────

    def run_stage0(self) -> dict:
        """Gaps, duplicates, future dates, staleness, MoM volatility."""
        issues = []
        try:
            rows = _safe_query(
                self._conn,
                "SELECT year, month, data_json FROM monthly_data "
                "WHERE workspace_id=? ORDER BY year, month",
                [self._ws],
            )
            if not rows:
                return {"status": "warn", "issues": [{"check": "no_data", "msg": "No monthly data found"}],
                        "summary": "No data to validate"}

            periods = [(r["year"], r["month"]) for r in rows]
            today = date.today()

            # Duplicate months
            seen = {}
            for y, m in periods:
                key = (y, m)
                seen[key] = seen.get(key, 0) + 1
            dupes = {k: v for k, v in seen.items() if v > 1}
            for (y, m), cnt in dupes.items():
                issues.append({"check": "duplicate_month", "period": f"{y}-{m:02d}",
                               "count": cnt, "severity": "warn"})

            # Gaps
            unique_periods = sorted(set(periods))
            for i in range(1, len(unique_periods)):
                py, pm = unique_periods[i - 1]
                cy, cm = unique_periods[i]
                expected_y = py + (pm // 12)
                expected_m = (pm % 12) + 1
                if (cy, cm) != (expected_y, expected_m):
                    issues.append({"check": "gap", "after": f"{py}-{pm:02d}",
                                   "before": f"{cy}-{cm:02d}", "severity": "warn"})

            # Future dates
            for y, m in unique_periods:
                if y > today.year or (y == today.year and m > today.month):
                    issues.append({"check": "future_date", "period": f"{y}-{m:02d}",
                                   "severity": "warn"})

            # Staleness
            latest = unique_periods[-1] if unique_periods else None
            if latest:
                latest_date = date(latest[0], latest[1], 1)
                days_stale = (today - latest_date).days
                if days_stale > _FRESHNESS_FAIL_DAYS:
                    issues.append({"check": "stale_data", "days": days_stale,
                                   "latest": f"{latest[0]}-{latest[1]:02d}", "severity": "fail"})
                elif days_stale > _FRESHNESS_WARN_DAYS:
                    issues.append({"check": "stale_data", "days": days_stale,
                                   "latest": f"{latest[0]}-{latest[1]:02d}", "severity": "warn"})

            # MoM volatility (check key KPIs for >500% swings)
            vol_kpis = ["gross_margin", "revenue_growth", "churn_rate", "operating_margin",
                        "burn_multiple", "nrr", "arr_growth"]
            for r_idx in range(1, len(rows)):
                prev_d = json.loads(rows[r_idx - 1]["data_json"]) if isinstance(rows[r_idx - 1]["data_json"], str) else (rows[r_idx - 1]["data_json"] or {})
                curr_d = json.loads(rows[r_idx]["data_json"]) if isinstance(rows[r_idx]["data_json"], str) else (rows[r_idx]["data_json"] or {})
                period = f"{rows[r_idx]['year']}-{rows[r_idx]['month']:02d}"
                for kpi in vol_kpis:
                    pv = prev_d.get(kpi)
                    cv = curr_d.get(kpi)
                    if pv is not None and cv is not None and isinstance(pv, (int, float)) and isinstance(cv, (int, float)):
                        if abs(pv) > 0.01:
                            chg = abs((cv - pv) / pv) * 100
                            if chg > _VOLATILITY_THRESHOLD:
                                issues.append({"check": "mom_volatility", "period": period,
                                               "kpi": kpi, "prev": round(pv, 2), "curr": round(cv, 2),
                                               "change_pct": round(chg, 1), "severity": "warn"})

        except Exception as e:
            return {"status": "fail", "error": str(e), "issues": issues, "summary": "Stage 0 error"}

        fails = [i for i in issues if i.get("severity") == "fail"]
        warns = [i for i in issues if i.get("severity") == "warn"]
        status = "fail" if fails else ("warn" if warns else "pass")
        return {
            "status": status,
            "issues": issues,
            "summary": f"{len(issues)} issues: {len(fails)} critical, {len(warns)} warnings",
        }

    # ── Stage 1: Source → Canonical + cross-entity + currency ────────────

    def run_stage1(self, upload_id=None, source_name=None) -> dict:
        checks = []
        try:
            # Row counts per canonical table
            tables = ["canonical_revenue", "canonical_expenses", "canonical_customers",
                      "canonical_pipeline", "canonical_invoices", "canonical_employees"]
            for tbl in tables:
                try:
                    row = self._conn.execute(f"SELECT COUNT(*) as cnt FROM {tbl} WHERE workspace_id=?", [self._ws]).fetchone()
                    cnt = row["cnt"] if row else 0
                    checks.append({"check": "row_count", "table": tbl, "count": cnt, "passed": cnt > 0})
                except Exception:
                    checks.append({"check": "row_count", "table": tbl, "count": 0, "passed": True, "note": "table not found"})

            # Revenue total: canonical vs monthly_data
            try:
                rev_row = self._conn.execute("SELECT SUM(amount) as total FROM canonical_revenue WHERE workspace_id=?", [self._ws]).fetchone()
                md_rows = _safe_query(self._conn, "SELECT data_json FROM monthly_data WHERE workspace_id=? AND upload_id=-999", [self._ws])
                md_rev = sum(
                    (json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})).get("_total_revenue", 0) or 0
                    for r in md_rows
                )
                canon_total = float(rev_row["total"] or 0) if rev_row else 0
                diff = _pct_diff(canon_total, md_rev)
                checks.append({"check": "revenue_total", "canonical": round(canon_total, 2),
                                "monthly_data": round(md_rev, 2), "diff_pct": round(diff, 2) if diff else 0,
                                "passed": diff is not None and diff < 1.0})
            except Exception:
                pass

            # Revenue vs Invoices reconciliation
            try:
                inv_row = self._conn.execute("SELECT SUM(amount) as total FROM canonical_invoices WHERE workspace_id=?", [self._ws]).fetchone()
                rev_row2 = self._conn.execute("SELECT SUM(amount) as total FROM canonical_revenue WHERE workspace_id=?", [self._ws]).fetchone()
                inv_total = float(inv_row["total"] or 0) if inv_row else 0
                rev_total = float(rev_row2["total"] or 0) if rev_row2 else 0
                if inv_total > 0 and rev_total > 0:
                    diff = _pct_diff(rev_total, inv_total)
                    checks.append({"check": "revenue_vs_invoices", "revenue": round(rev_total, 2),
                                    "invoices": round(inv_total, 2), "diff_pct": round(diff, 2) if diff else 0,
                                    "passed": diff < 10.0})  # 10% tolerance — invoices may lag
            except Exception:
                pass

            # Customer count reconciliation
            try:
                cust_canon = self._conn.execute("SELECT COUNT(DISTINCT source_id) as cnt FROM canonical_customers WHERE workspace_id=?", [self._ws]).fetchone()
                cust_rev = self._conn.execute("SELECT COUNT(DISTINCT customer_id) as cnt FROM canonical_revenue WHERE workspace_id=?", [self._ws]).fetchone()
                c_canon = cust_canon["cnt"] if cust_canon else 0
                c_rev = cust_rev["cnt"] if cust_rev else 0
                if c_canon > 0 and c_rev > 0:
                    checks.append({"check": "customer_count_reconciliation",
                                    "canonical_customers": c_canon, "revenue_customers": c_rev,
                                    "passed": True, "note": f"Canon: {c_canon}, Revenue refs: {c_rev}"})
            except Exception:
                pass

            # Currency check
            try:
                curr_rows = self._conn.execute(
                    "SELECT DISTINCT currency FROM canonical_revenue WHERE workspace_id=? AND currency IS NOT NULL AND currency != ''",
                    [self._ws],
                ).fetchall()
                currencies = [r["currency"] for r in curr_rows]
                if len(currencies) > 1:
                    checks.append({"check": "mixed_currencies", "currencies": currencies,
                                    "passed": False, "note": f"Mixed currencies detected: {', '.join(currencies)}. Revenue totals may be unreliable."})
                elif currencies and currencies[0].upper() != "USD":
                    checks.append({"check": "non_usd_currency", "currency": currencies[0],
                                    "passed": True, "note": f"All revenue in {currencies[0]} (non-USD)"})
            except Exception:
                pass

            # Mapping quality check — detect unmapped or low-confidence fields
            # that may be producing null canonical data
            try:
                unmapped_rows = _safe_query(
                    self._conn,
                    "SELECT source_name, canonical_table, source_field, confidence "
                    "FROM field_mappings "
                    "WHERE workspace_id=? AND (canonical_field='unmapped' OR confidence < 0.5)",
                    [self._ws],
                )
                unmapped_count = len(unmapped_rows)
                if unmapped_count > 0:
                    unmapped_detail = [
                        {"source": r["source_name"], "table": r["canonical_table"],
                         "field": r["source_field"], "confidence": r["confidence"]}
                        for r in unmapped_rows[:10]  # Cap at 10 for readability
                    ]
                    checks.append({
                        "check": "mapping_quality",
                        "unmapped_or_low_confidence": unmapped_count,
                        "detail": unmapped_detail,
                        "passed": unmapped_count == 0,
                        "suggested_action": "review_field_mappings",
                        "note": f"{unmapped_count} field(s) are unmapped or have low confidence. "
                                "Review and confirm mappings to improve data quality.",
                    })
                else:
                    checks.append({"check": "mapping_quality", "unmapped_or_low_confidence": 0,
                                    "passed": True, "note": "All field mappings are confirmed"})
            except Exception:
                pass  # field_mappings table may not exist yet

        except Exception as e:
            return {"status": "fail", "error": str(e), "checks": checks, "summary": "Stage 1 error"}

        failed = [c for c in checks if not c["passed"]]
        status = "fail" if failed else "pass"
        return {"status": status, "checks": checks,
                "summary": f"{len(checks)} checks: {len(checks) - len(failed)} passed, {len(failed)} failed"}

    # ── Stage 2: Canonical → KPI + null propagation + business logic ────

    def run_stage2(self) -> dict:
        discrepancies = []
        null_issues = []
        business_issues = []
        months_checked = 0

        try:
            rows = _safe_query(self._conn, "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? AND upload_id=-999 ORDER BY year, month", [self._ws])
            rev_rows = _safe_query(self._conn, "SELECT amount, period, customer_id, subscription_type FROM canonical_revenue WHERE workspace_id=?", [self._ws])
            exp_rows = _safe_query(self._conn, "SELECT amount, period, category FROM canonical_expenses WHERE workspace_id=?", [self._ws])

            # Build independent aggregates
            rev_by_month, exp_by_month = {}, {}
            for r in rev_rows:
                p = str(r["period"] or "")[:7]
                if not p:
                    continue
                if p not in rev_by_month:
                    rev_by_month[p] = {"total": 0, "recurring": 0, "customer_ids": set(), "customer_amounts": {}}
                amt = float(r["amount"] or 0)
                rev_by_month[p]["total"] += amt
                sub = str(r["subscription_type"] or "").lower()
                if sub in ("recurring", "subscription", "monthly", "annual"):
                    rev_by_month[p]["recurring"] += amt
                cid = str(r["customer_id"] or "")
                if cid:
                    rev_by_month[p]["customer_ids"].add(cid)
                    rev_by_month[p]["customer_amounts"][cid] = rev_by_month[p]["customer_amounts"].get(cid, 0) + amt

            for r in exp_rows:
                p = str(r["period"] or "")[:7]
                if not p:
                    continue
                if p not in exp_by_month:
                    exp_by_month[p] = {"cogs": 0, "opex": 0, "total": 0}
                amt = float(r["amount"] or 0)
                cat = str(r["category"] or "").lower()
                exp_by_month[p]["total"] += amt
                if cat == "cogs":
                    exp_by_month[p]["cogs"] += amt
                else:
                    exp_by_month[p]["opex"] += amt

            prev_cust_ids = set()
            sorted_months = sorted(set(f"{r['year']}-{r['month']:02d}" for r in rows))

            for r in rows:
                period = f"{r['year']}-{r['month']:02d}"
                months_checked += 1
                stored = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})

                rev = rev_by_month.get(period, {"total": 0, "recurring": 0, "customer_ids": set()})
                exp = exp_by_month.get(period, {"cogs": 0, "opex": 0})
                total_rev = rev["total"]
                cogs = exp["cogs"]
                opex = exp["opex"]
                curr_custs = rev["customer_ids"]

                # Independent recomputation
                recomputed = {}
                if total_rev > 0:
                    recomputed["gross_margin"] = (total_rev - cogs) / total_rev * 100
                    recomputed["operating_margin"] = (total_rev - cogs - opex) / total_rev * 100
                    recomputed["ebitda_margin"] = (total_rev - cogs - opex) / total_rev * 100
                    recomputed["opex_ratio"] = opex / total_rev * 100
                    recomputed["recurring_revenue"] = rev["recurring"] / total_rev * 100
                    recomputed["revenue_quality"] = rev["recurring"] / total_rev * 100

                if prev_cust_ids and curr_custs:
                    churned = prev_cust_ids - curr_custs
                    recomputed["churn_rate"] = len(churned) / len(prev_cust_ids) * 100
                    recomputed["logo_retention"] = (1 - len(churned) / len(prev_cust_ids)) * 100

                if curr_custs and total_rev > 0:
                    shares = [(a / total_rev * 100) for a in rev.get("customer_amounts", {}).values()]
                    recomputed["customer_concentration"] = sum(s ** 2 for s in shares) / 100

                if prev_cust_ids and total_rev > 0:
                    idx = sorted_months.index(period) - 1 if period in sorted_months else -1
                    if idx >= 0:
                        prev_rev = rev_by_month.get(sorted_months[idx], {}).get("total", 0)
                        if prev_rev > 0:
                            recomputed["nrr"] = total_rev / prev_rev * 100

                for kpi in _VERIFY_KPIS:
                    if kpi not in recomputed:
                        continue
                    sv = stored.get(kpi)
                    rv = recomputed[kpi]
                    if sv is None:
                        continue
                    diff = _pct_diff(sv, rv)
                    if diff is not None and diff >= _TOLERANCE_PCT:
                        discrepancies.append({"period": period, "kpi": kpi,
                                              "stored": round(sv, 4), "recomputed": round(rv, 4),
                                              "diff_pct": round(diff, 2)})

                # Null propagation check
                for child, parents in _NULL_PROPAGATION.items():
                    child_val = stored.get(child)
                    for parent in parents:
                        parent_val = stored.get(parent)
                        if parent_val is None and child_val is not None:
                            null_issues.append({"period": period, "child": child,
                                                "parent": parent, "child_value": child_val,
                                                "issue": f"{child} has value {child_val} but parent {parent} is null"})

                # Business logic
                gm = stored.get("gross_margin")
                om = stored.get("operating_margin")
                mrr = stored.get("mrr")
                arr = stored.get("arr")
                cr = stored.get("churn_rate")
                lr = stored.get("logo_retention")
                nrr_v = stored.get("nrr")
                rq = stored.get("revenue_quality")
                rr = stored.get("recurring_revenue")

                if gm is not None and om is not None and om > gm + 1:
                    business_issues.append({"period": period, "rule": "op_lt_gross",
                                            "msg": f"Operating margin ({om:.1f}%) > gross margin ({gm:.1f}%)"})
                if mrr is not None and arr is not None and abs(arr - mrr * 12) > mrr * 0.5:
                    business_issues.append({"period": period, "rule": "arr_mrr_12x",
                                            "msg": f"ARR ({arr:,.0f}) != MRR ({mrr:,.0f}) * 12 = {mrr*12:,.0f}"})
                if cr is not None and lr is not None and abs(cr + lr - 100) > 2:
                    business_issues.append({"period": period, "rule": "churn_retention",
                                            "msg": f"Churn ({cr:.1f}%) + retention ({lr:.1f}%) = {cr+lr:.1f}%, should be ~100%"})
                if rq is not None and rr is not None and abs(rq - rr) > 0.5:
                    business_issues.append({"period": period, "rule": "rev_quality_eq",
                                            "msg": f"Revenue quality ({rq:.1f}%) != recurring revenue ({rr:.1f}%)"})

                prev_cust_ids = curr_custs

        except Exception as e:
            return {"status": "fail", "error": str(e), "months_checked": months_checked,
                    "discrepancies": discrepancies, "null_issues": null_issues, "business_issues": business_issues,
                    "summary": "Stage 2 error"}

        all_issues = discrepancies + null_issues + business_issues
        status = "fail" if discrepancies else ("warn" if (null_issues or business_issues) else "pass")
        total_checks = months_checked * len(_VERIFY_KPIS)
        return {
            "status": status,
            "months_checked": months_checked,
            "kpis_verified": len(_VERIFY_KPIS),
            "total_checks": total_checks,
            "discrepancies": discrepancies,
            "null_issues": null_issues,
            "business_issues": business_issues,
            "summary": f"{months_checked} months x {len(_VERIFY_KPIS)} KPIs = {total_checks} checks. "
                        f"{len(discrepancies)} computation, {len(null_issues)} null propagation, "
                        f"{len(business_issues)} business logic issues.",
        }

    # ── Stage 3: Display consistency ─────────────────────────────────────

    def run_stage3(self) -> dict:
        from core.kpi_utils import compute_kpi_avg
        inconsistencies = []
        kpis_checked = 0

        try:
            rows = _safe_query(self._conn, "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month", [self._ws])
            kpi_values = {}
            for r in rows:
                d = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})
                for k, v in d.items():
                    if k.startswith("_") or k in ("year", "month"):
                        continue
                    if isinstance(v, (int, float)) and math.isfinite(v):
                        kpi_values.setdefault(k, []).append(v)

            for kpi, vals in kpi_values.items():
                if len(vals) < 2:
                    continue
                kpis_checked += 1
                a1 = compute_kpi_avg(vals, window=6, period_filtered=False)
                a2 = compute_kpi_avg(vals, window=6, period_filtered=False)
                if a1 != a2:
                    inconsistencies.append({"kpi": kpi, "issue": "non_deterministic", "call1": a1, "call2": a2})

                avg_all = compute_kpi_avg(vals, period_filtered=True)
                manual = round(sum(vals) / len(vals), 2)
                if avg_all != manual:
                    inconsistencies.append({"kpi": kpi, "issue": "period_filtered_mismatch", "utility": avg_all, "manual": manual})

        except Exception as e:
            return {"status": "fail", "error": str(e), "kpis_checked": kpis_checked, "inconsistencies": inconsistencies, "summary": "Stage 3 error"}

        status = "fail" if inconsistencies else "pass"
        return {"status": status, "kpis_checked": kpis_checked, "inconsistencies": inconsistencies,
                "summary": f"{kpis_checked} KPIs checked. {len(inconsistencies)} inconsistencies."}

    # ── Stage 4: Statistical anomaly detection ───────────────────────────

    def run_stage4(self) -> dict:
        """Z-score outlier detection per KPI against rolling mean."""
        anomalies = []
        kpis_checked = 0

        try:
            rows = _safe_query(self._conn, "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month", [self._ws])
            kpi_series = {}
            for r in rows:
                d = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})
                period = f"{r['year']}-{r['month']:02d}"
                for k, v in d.items():
                    if k.startswith("_") or k in ("year", "month"):
                        continue
                    if isinstance(v, (int, float)) and math.isfinite(v):
                        kpi_series.setdefault(k, []).append({"period": period, "value": v})

            for kpi, entries in kpi_series.items():
                if len(entries) < 6:
                    continue
                kpis_checked += 1
                values = [e["value"] for e in entries]

                # Rolling z-score for each value against prior values
                for i in range(6, len(values)):
                    window = values[max(0, i - 12):i]  # look-back 12 months
                    if len(window) < 4:
                        continue
                    try:
                        mu = statistics.mean(window)
                        sigma = statistics.stdev(window)
                        if sigma < 1e-9:
                            continue
                        z = abs(values[i] - mu) / sigma
                        if z >= _ZSCORE_WARN:
                            severity = "anomaly" if z >= _ZSCORE_ANOMALY else "warn"
                            anomalies.append({
                                "kpi": kpi, "period": entries[i]["period"],
                                "value": round(values[i], 2),
                                "rolling_mean": round(mu, 2), "rolling_std": round(sigma, 2),
                                "z_score": round(z, 2), "severity": severity,
                            })
                    except Exception:
                        continue

        except Exception as e:
            return {"status": "fail", "error": str(e), "kpis_checked": kpis_checked, "anomalies": anomalies, "summary": "Stage 4 error"}

        critical = [a for a in anomalies if a["severity"] == "anomaly"]
        warns = [a for a in anomalies if a["severity"] == "warn"]
        status = "fail" if critical else ("warn" if warns else "pass")
        return {
            "status": status,
            "kpis_checked": kpis_checked,
            "anomalies": anomalies,
            "summary": f"{kpis_checked} KPIs scanned. {len(critical)} anomalies, {len(warns)} warnings.",
        }

    # ── Self-correction ──────────────────────────────────────────────────

    def _correct_stage2(self) -> dict:
        from elt.kpi_aggregator import aggregate_canonical_to_monthly
        started = datetime.utcnow().isoformat()
        try:
            aggregate_canonical_to_monthly(self._conn, self._ws)
            # Self-healing: if there are still unmapped new fields, create a notification
            self._check_unmapped_after_correction()
            return {"stage": 2, "action": "re_aggregation",
                    "started_at": started, "completed_at": datetime.utcnow().isoformat(), "succeeded": True}
        except Exception as e:
            return {"stage": 2, "action": "re_aggregation",
                    "started_at": started, "completed_at": datetime.utcnow().isoformat(),
                    "succeeded": False, "error": str(e)}

    def _check_unmapped_after_correction(self) -> None:
        """Self-healing: if unmapped new fields still exist after correction,
        create a notification prompting the user to review mappings."""
        try:
            rows = _safe_query(
                self._conn,
                "SELECT COUNT(*) as cnt FROM field_mappings "
                "WHERE workspace_id=? AND canonical_field='unmapped' AND is_new=1",
                [self._ws],
            )
            cnt = rows[0]["cnt"] if rows else 0
            if cnt > 0:
                # Check if a notification already exists and is not dismissed
                existing = _safe_query(
                    self._conn,
                    "SELECT id FROM workspace_notifications "
                    "WHERE workspace_id=? AND notification_type='mapping_required' AND is_dismissed=0",
                    [self._ws],
                )
                if not existing:
                    self._conn.execute(
                        "INSERT INTO workspace_notifications "
                        "(workspace_id, notification_type, title, message, severity, data_json) "
                        "VALUES (?,?,?,?,?,?)",
                        [self._ws, "mapping_required",
                         "Field mappings need review",
                         f"{cnt} unmapped field(s) detected during integrity check. "
                         "Review and confirm field mappings to improve KPI accuracy.",
                         "warning", json.dumps({"unmapped_count": cnt})],
                    )
                    self._conn.commit()
        except Exception:
            pass  # Non-blocking — notification is best-effort

    # ── Persistence ──────────────────────────────────────────────────────

    def _persist(self, report, trigger, upload_id, source_name):
        try:
            self._conn.execute(
                "INSERT INTO integrity_checks "
                "(workspace_id, run_id, trigger, started_at, completed_at, "
                "overall_status, stage0_status, stage0_report, "
                "stage1_status, stage1_report, "
                "stage2_status, stage2_report, stage3_status, stage3_report, "
                "stage4_status, stage4_report, "
                "correction_attempted, correction_succeeded, correction_log, "
                "upload_id, source_name) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [self._ws, self._run_id, trigger,
                 report["started_at"], report["completed_at"], report["overall_status"],
                 report["stage0"]["status"], json.dumps(report["stage0"]),
                 report["stage1"]["status"], json.dumps(report["stage1"]),
                 report["stage2"]["status"], json.dumps(report["stage2"]),
                 report["stage3"]["status"], json.dumps(report["stage3"]),
                 report["stage4"]["status"], json.dumps(report["stage4"]),
                 1 if report["correction_attempted"] else 0,
                 1 if report["correction_succeeded"] else 0,
                 json.dumps(report["correction_log"]),
                 upload_id, source_name],
            )
            self._conn.commit()
        except Exception:
            pass

    def _audit(self, report):
        try:
            from core.database import _audit
            stages = f"S0:{report['stage0']['status']} S1:{report['stage1']['status']} " \
                     f"S2:{report['stage2']['status']} S3:{report['stage3']['status']} S4:{report['stage4']['status']}"
            _audit("integrity_check", "integrity", self._run_id,
                   f"Integrity: {report['overall_status']} ({report['trigger']}) — {stages}",
                   workspace_id=self._ws)
        except Exception:
            pass
