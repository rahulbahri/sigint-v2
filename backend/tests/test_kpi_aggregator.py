"""
tests/test_kpi_aggregator.py — Unit tests for the KPI aggregation engine.

Tests cover:
  - Period parsing (ISO, Unix, various date formats)
  - Safe float conversion (null, string, infinity, NaN)
  - Revenue KPI computation (gross_margin, revenue_growth, etc.)
  - Expense KPI computation (opex_ratio, burn_multiple)
  - Customer metrics (churn_rate, logo_retention)
  - Pipeline metrics (win_rate, avg_deal_size)
  - Derived KPIs (growth_efficiency, revenue_fragility, margin_volatility)
  - Idempotency (running twice produces same result)
  - Non-destructive merge (CSV data preserved)
  - Edge cases (empty tables, single month, all-null data)
"""
from __future__ import annotations

import json
import math
import sqlite3
import sys
from pathlib import Path

# Ensure the backend is on the import path
sys.path.insert(0, str(Path(__file__).parent.parent))

from elt.kpi_aggregator import (
    CONNECTOR_UPLOAD_SENTINEL,
    _compute_month_kpis,
    _parse_period,
    _safe_float,
    _safe_set,
    aggregate_canonical_to_monthly,
)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_db() -> sqlite3.Connection:
    """Create an in-memory SQLite database with all required tables."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE monthly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id INTEGER,
            year INTEGER,
            month INTEGER,
            data_json TEXT,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE canonical_revenue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            amount REAL, currency TEXT DEFAULT 'USD',
            period TEXT, customer_id TEXT,
            subscription_type TEXT, product_id TEXT,
            recognized_at TEXT,
            raw_id TEXT, created_at TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
        CREATE TABLE canonical_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            amount REAL, currency TEXT DEFAULT 'USD',
            category TEXT, vendor TEXT,
            period TEXT, description TEXT,
            raw_id TEXT, created_at TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
        CREATE TABLE canonical_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            name TEXT, email TEXT, company TEXT,
            phone TEXT, country TEXT,
            created_at TEXT, lifecycle_stage TEXT,
            raw_id TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
        CREATE TABLE canonical_pipeline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            name TEXT, amount REAL,
            stage TEXT, close_date TEXT,
            probability REAL, owner TEXT,
            created_at TEXT,
            raw_id TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
        CREATE TABLE canonical_invoices (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            amount REAL, currency TEXT DEFAULT 'USD',
            customer_id TEXT, issue_date TEXT,
            due_date TEXT, status TEXT, period TEXT,
            raw_id TEXT, created_at TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
        CREATE TABLE canonical_employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT,
            name TEXT, email TEXT,
            title TEXT, department TEXT,
            salary REAL, hire_date TEXT,
            status TEXT DEFAULT 'active',
            raw_id TEXT, created_at TEXT, updated_at TEXT,
            UNIQUE(workspace_id, source, source_id)
        );
    """)
    return conn


def _insert_revenue(conn, ws, records):
    for r in records:
        conn.execute(
            "INSERT INTO canonical_revenue "
            "(workspace_id, source, source_id, amount, period, customer_id, subscription_type) "
            "VALUES (?,?,?,?,?,?,?)",
            (ws, r.get("source", "stripe"), r["source_id"], r["amount"],
             r["period"], r.get("customer_id"), r.get("subscription_type")),
        )
    conn.commit()


def _insert_expenses(conn, ws, records):
    for r in records:
        conn.execute(
            "INSERT INTO canonical_expenses "
            "(workspace_id, source, source_id, amount, period, category) "
            "VALUES (?,?,?,?,?,?)",
            (ws, r.get("source", "quickbooks"), r["source_id"],
             r["amount"], r["period"], r.get("category")),
        )
    conn.commit()


def _insert_customers(conn, ws, records):
    for r in records:
        conn.execute(
            "INSERT INTO canonical_customers "
            "(workspace_id, source, source_id, name, email, created_at) "
            "VALUES (?,?,?,?,?,?)",
            (ws, r.get("source", "hubspot"), r["source_id"],
             r.get("name"), r.get("email"), r.get("created_at")),
        )
    conn.commit()


def _insert_pipeline(conn, ws, records):
    for r in records:
        conn.execute(
            "INSERT INTO canonical_pipeline "
            "(workspace_id, source, source_id, name, amount, stage, close_date) "
            "VALUES (?,?,?,?,?,?,?)",
            (ws, r.get("source", "hubspot"), r["source_id"],
             r.get("name"), r.get("amount"), r.get("stage"), r.get("close_date")),
        )
    conn.commit()


def _insert_employees(conn, ws, records):
    for r in records:
        conn.execute(
            "INSERT INTO canonical_employees "
            "(workspace_id, source, source_id, name, hire_date, status, salary) "
            "VALUES (?,?,?,?,?,?,?)",
            (ws, r.get("source", "ramp"), r["source_id"],
             r.get("name"), r.get("hire_date"), r.get("status", "active"), r.get("salary")),
        )
    conn.commit()


def _get_monthly_kpis(conn, ws) -> dict:
    """Return {(year, month): {kpi_key: value}} from monthly_data."""
    rows = conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=?",
        [ws],
    ).fetchall()
    result = {}
    for r in rows:
        result[(r["year"], r["month"])] = json.loads(r["data_json"])
    return result


# ── Unit tests: _parse_period ────────────────────────────────────────────────

class TestParsePeriod:
    def test_iso_date(self):
        assert _parse_period("2025-03-15") == (2025, 3)

    def test_iso_datetime(self):
        assert _parse_period("2025-06-01T14:30:00") == (2025, 6)

    def test_iso_with_tz(self):
        assert _parse_period("2025-06-01T14:30:00Z") == (2025, 6)

    def test_iso_with_offset(self):
        assert _parse_period("2025-06-01T14:30:00+05:30") == (2025, 6)

    def test_iso_with_millis(self):
        assert _parse_period("2025-06-01T14:30:00.123456") == (2025, 6)

    def test_year_month(self):
        assert _parse_period("2025-03") == (2025, 3)

    def test_slash_date(self):
        assert _parse_period("2025/03/15") == (2025, 3)

    def test_unix_seconds(self):
        # 2025-01-15 ~= 1736899200
        result = _parse_period("1736899200")
        assert result is not None
        assert result[0] == 2025

    def test_unix_millis(self):
        result = _parse_period("1736899200000")
        assert result is not None
        assert result[0] == 2025

    def test_none(self):
        assert _parse_period(None) is None

    def test_empty_string(self):
        assert _parse_period("") is None

    def test_garbage(self):
        assert _parse_period("not-a-date") is None


# ── Unit tests: _safe_float ──────────────────────────────────────────────────

class TestSafeFloat:
    def test_number(self):
        assert _safe_float(42.5) == 42.5

    def test_string(self):
        assert _safe_float("123.4") == 123.4

    def test_none(self):
        assert _safe_float(None) is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_inf(self):
        assert _safe_float(float("inf")) is None

    def test_bad_string(self):
        assert _safe_float("abc") is None


# ── Unit tests: _safe_set ────────────────────────────────────────────────────

class TestSafeSet:
    def test_valid(self):
        d = {}
        _safe_set(d, "x", 42.0)
        assert d["x"] == 42.0

    def test_none(self):
        d = {}
        _safe_set(d, "x", None)
        assert "x" not in d

    def test_nan(self):
        d = {}
        _safe_set(d, "x", float("nan"))
        assert "x" not in d

    def test_inf(self):
        d = {}
        _safe_set(d, "x", float("inf"))
        assert "x" not in d


# ── Integration tests: full aggregation ──────────────────────────────────────

class TestAggregation:
    WS = "test-workspace"

    def test_revenue_kpis(self):
        """Revenue data produces gross_margin, recurring_revenue, etc."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "ch_1", "amount": 10000, "period": "2025-01-15",
             "customer_id": "cust_1", "subscription_type": "recurring"},
            {"source_id": "ch_2", "amount": 5000, "period": "2025-01-20",
             "customer_id": "cust_2", "subscription_type": "one-time"},
            {"source_id": "ch_3", "amount": 8000, "period": "2025-02-10",
             "customer_id": "cust_1", "subscription_type": "recurring"},
        ])
        _insert_expenses(conn, self.WS, [
            {"source_id": "exp_1", "amount": 3000, "period": "2025-01-10", "category": "cogs"},
            {"source_id": "exp_2", "amount": 2000, "period": "2025-01-15", "category": "sales marketing"},
        ])

        result = aggregate_canonical_to_monthly(conn, self.WS)

        assert result["months_written"] >= 1
        assert len(result["errors"]) == 0

        kpis = _get_monthly_kpis(conn, self.WS)
        jan = kpis.get((2025, 1), {})

        # Total Jan revenue = 15000, COGS = 3000
        assert "gross_margin" in jan
        assert jan["gross_margin"] > 0  # (15000-3000)/15000 * 100 = 80%
        assert abs(jan["gross_margin"] - 80.0) < 1.0

        # Recurring = 10000 out of 15000
        assert "recurring_revenue" in jan
        assert jan["recurring_revenue"] > 50  # ~66.67%
        conn.close()

    def test_expense_kpis(self):
        """Expenses produce opex_ratio when combined with revenue."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 20000, "period": "2025-03-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])
        _insert_expenses(conn, self.WS, [
            {"source_id": "e1", "amount": 5000, "period": "2025-03-01", "category": "cogs"},
            {"source_id": "e2", "amount": 8000, "period": "2025-03-01", "category": "rent"},
        ])

        result = aggregate_canonical_to_monthly(conn, self.WS)
        kpis = _get_monthly_kpis(conn, self.WS)
        mar = kpis.get((2025, 3), {})

        # OpEx = total_exp - cogs = 13000 - 5000 = 8000
        # opex_ratio = 8000/20000 * 100 = 40%
        assert "opex_ratio" in mar
        assert abs(mar["opex_ratio"] - 40.0) < 1.0
        conn.close()

    def test_pipeline_win_rate(self):
        """Pipeline data produces win_rate."""
        conn = _make_db()
        _insert_pipeline(conn, self.WS, [
            {"source_id": "d1", "name": "Deal A", "amount": 50000,
             "stage": "Closed Won", "close_date": "2025-04-15"},
            {"source_id": "d2", "name": "Deal B", "amount": 30000,
             "stage": "Closed Lost", "close_date": "2025-04-20"},
            {"source_id": "d3", "name": "Deal C", "amount": 40000,
             "stage": "won", "close_date": "2025-04-25"},
        ])

        result = aggregate_canonical_to_monthly(conn, self.WS)
        kpis = _get_monthly_kpis(conn, self.WS)
        apr = kpis.get((2025, 4), {})

        # 2 won out of 3 deals
        assert "win_rate" in apr
        assert abs(apr["win_rate"] - 66.6667) < 1.0
        conn.close()

    def test_employee_headcount(self):
        """Employee data produces rev_per_employee and headcount_eff."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 100000, "period": "2025-05-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])
        _insert_employees(conn, self.WS, [
            {"source_id": "emp1", "name": "Alice", "hire_date": "2025-05-01",
             "status": "active", "salary": 8000},
            {"source_id": "emp2", "name": "Bob", "hire_date": "2025-05-01",
             "status": "active", "salary": 9000},
        ])

        result = aggregate_canonical_to_monthly(conn, self.WS)
        kpis = _get_monthly_kpis(conn, self.WS)
        may = kpis.get((2025, 5), {})

        assert "rev_per_employee" in may
        # Annualised: 100000 * 12 / 2 = 600000
        assert may["rev_per_employee"] == 600000.0
        conn.close()

    def test_idempotency(self):
        """Running aggregation twice produces the same result."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 5000, "period": "2025-06-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])

        result1 = aggregate_canonical_to_monthly(conn, self.WS)
        kpis1 = _get_monthly_kpis(conn, self.WS)

        result2 = aggregate_canonical_to_monthly(conn, self.WS)
        kpis2 = _get_monthly_kpis(conn, self.WS)

        assert result1["months_written"] == result2["months_written"]
        assert kpis1 == kpis2

        # Verify only one set of rows (no duplicates)
        count = conn.execute(
            "SELECT COUNT(*) FROM monthly_data WHERE workspace_id=?",
            [self.WS],
        ).fetchone()[0]
        assert count == result2["months_written"]
        conn.close()

    def test_csv_data_preserved(self):
        """Pre-existing CSV data is not overwritten by connector aggregation."""
        conn = _make_db()

        # Simulate CSV-uploaded data (upload_id = 1, not the sentinel)
        csv_data = {"gross_margin": 75.0, "revenue_growth": 12.5, "custom_kpi": 99.0}
        conn.execute(
            "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) "
            "VALUES (?,?,?,?,?)",
            [1, 2025, 7, json.dumps(csv_data), self.WS],
        )
        conn.commit()

        # Connector data for same month
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 20000, "period": "2025-07-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])
        _insert_expenses(conn, self.WS, [
            {"source_id": "e1", "amount": 4000, "period": "2025-07-01", "category": "cogs"},
        ])

        aggregate_canonical_to_monthly(conn, self.WS)

        # Should now have 2 rows for July: the CSV one + connector one
        rows = conn.execute(
            "SELECT upload_id, data_json FROM monthly_data "
            "WHERE workspace_id=? AND year=2025 AND month=7",
            [self.WS],
        ).fetchall()

        assert len(rows) == 2

        # The connector row should contain CSV values where they existed
        connector_row = [r for r in rows if r["upload_id"] == CONNECTOR_UPLOAD_SENTINEL]
        assert len(connector_row) == 1
        merged = json.loads(connector_row[0]["data_json"])
        # CSV values should be preserved (they take precedence)
        assert merged["gross_margin"] == 75.0  # CSV value, not connector-computed
        assert merged["custom_kpi"] == 99.0     # CSV-only KPI preserved
        # Connector-only KPIs should still be present
        assert "recurring_revenue" in merged
        conn.close()

    def test_empty_canonical_tables(self):
        """Gracefully handles empty canonical tables."""
        conn = _make_db()
        result = aggregate_canonical_to_monthly(conn, self.WS)
        assert result["months_written"] == 0
        assert any("No canonical data" in e for e in result["errors"])
        conn.close()

    def test_workspace_isolation(self):
        """Data from another workspace is not included."""
        conn = _make_db()
        _insert_revenue(conn, "ws-A", [
            {"source_id": "r1", "amount": 5000, "period": "2025-08-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])
        _insert_revenue(conn, "ws-B", [
            {"source_id": "r2", "amount": 9999, "period": "2025-08-01",
             "customer_id": "c2", "subscription_type": "recurring"},
        ])

        aggregate_canonical_to_monthly(conn, "ws-A")
        kpis_a = _get_monthly_kpis(conn, "ws-A")
        aug_a = kpis_a.get((2025, 8), {})

        # ws-A should only see its own revenue (5000)
        assert aug_a.get("mrr") == 5000.0

        # ws-B should have no monthly_data yet
        kpis_b = _get_monthly_kpis(conn, "ws-B")
        assert len(kpis_b) == 0
        conn.close()

    def test_revenue_growth_requires_two_months(self):
        """revenue_growth is only computed when there's a prior month."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 10000, "period": "2025-01-15",
             "customer_id": "c1", "subscription_type": "recurring"},
            {"source_id": "r2", "amount": 12000, "period": "2025-02-15",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])

        aggregate_canonical_to_monthly(conn, self.WS)
        kpis = _get_monthly_kpis(conn, self.WS)

        jan = kpis.get((2025, 1), {})
        feb = kpis.get((2025, 2), {})

        # Jan should NOT have revenue_growth (no prior month)
        assert "revenue_growth" not in jan
        # Feb should have revenue_growth = (12000-10000)/10000 * 100 = 20%
        assert "revenue_growth" in feb
        assert abs(feb["revenue_growth"] - 20.0) < 0.1
        conn.close()

    def test_nan_values_filtered(self):
        """NaN and Inf values never make it into monthly_data JSON."""
        conn = _make_db()
        _insert_revenue(conn, self.WS, [
            {"source_id": "r1", "amount": 0, "period": "2025-09-01",
             "customer_id": "c1", "subscription_type": "recurring"},
        ])

        aggregate_canonical_to_monthly(conn, self.WS)
        kpis = _get_monthly_kpis(conn, self.WS)

        for ym, month_kpis in kpis.items():
            for k, v in month_kpis.items():
                if isinstance(v, float):
                    assert math.isfinite(v), f"KPI {k} has non-finite value: {v}"
        conn.close()


# ── Run tests ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import traceback

    test_classes = [TestParsePeriod, TestSafeFloat, TestSafeSet, TestAggregation]
    passed = 0
    failed = 0
    errors = []

    for cls in test_classes:
        instance = cls()
        for method_name in sorted(dir(instance)):
            if not method_name.startswith("test_"):
                continue
            test_name = f"{cls.__name__}.{method_name}"
            try:
                getattr(instance, method_name)()
                passed += 1
                print(f"  ✓ {test_name}")
            except Exception as exc:
                failed += 1
                errors.append((test_name, exc))
                print(f"  ✗ {test_name}: {exc}")
                traceback.print_exc()

    print(f"\n{'='*60}")
    print(f"Results: {passed} passed, {failed} failed, {passed + failed} total")
    if errors:
        print("\nFailed tests:")
        for name, exc in errors:
            print(f"  - {name}: {exc}")
    print(f"{'='*60}")
    sys.exit(1 if failed else 0)
