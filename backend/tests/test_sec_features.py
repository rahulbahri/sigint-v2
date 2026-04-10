"""
tests/test_sec_features.py — Tests for SEC-grade analytics features.

Covers all 11 new backend endpoints:
  - ARR Bridge, Cohort Retention, Customer Concentration
  - Margin Decomposition, Cash Waterfall, Unit Economics
  - Accountability Rollup, Restatement History, Seasonality
  - Control Attestation, Scenario Comparison
"""
import json
import pytest


def _get_db():
    from core.database import get_db
    return get_db()


def _seed_revenue(conn, workspace_id, records):
    """Insert test revenue records into canonical_revenue."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canonical_revenue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT, amount REAL, currency TEXT DEFAULT 'USD',
            period TEXT, customer_id TEXT, subscription_type TEXT,
            product_id TEXT, recognized_at TEXT,
            raw_id TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, source, source_id)
        )
    """)
    for r in records:
        conn.execute(
            "INSERT OR IGNORE INTO canonical_revenue "
            "(workspace_id, source, source_id, amount, period, customer_id, subscription_type) "
            "VALUES (?,?,?,?,?,?,?)",
            [workspace_id, r.get("source", "test"), r["source_id"],
             r["amount"], r["period"], r.get("customer_id"), r.get("subscription_type", "recurring")],
        )
    conn.commit()


def _seed_expenses(conn, workspace_id, records):
    """Insert test expense records into canonical_expenses."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canonical_expenses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT, amount REAL, currency TEXT DEFAULT 'USD',
            category TEXT, vendor TEXT, period TEXT, description TEXT,
            raw_id TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, source, source_id)
        )
    """)
    for r in records:
        conn.execute(
            "INSERT OR IGNORE INTO canonical_expenses "
            "(workspace_id, source, source_id, amount, period, category) "
            "VALUES (?,?,?,?,?,?)",
            [workspace_id, "test", r["source_id"], r["amount"], r["period"], r["category"]],
        )
    conn.commit()


def _seed_customers(conn, workspace_id, records):
    """Insert test customer records."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS canonical_customers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            source TEXT, source_id TEXT, name TEXT, email TEXT,
            company TEXT, phone TEXT, country TEXT, created_at TEXT,
            lifecycle_stage TEXT,
            raw_id TEXT,
            UNIQUE(workspace_id, source, source_id)
        )
    """)
    for r in records:
        conn.execute(
            "INSERT OR IGNORE INTO canonical_customers "
            "(workspace_id, source, source_id, name, created_at) "
            "VALUES (?,?,?,?,?)",
            [workspace_id, "test", r["source_id"], r.get("name", r["source_id"]), r.get("created_at")],
        )
    conn.commit()


# ── ARR Bridge ───────────────────────────────────────────────────────────────

@pytest.mark.anyio
class TestArrBridge:
    async def test_arr_bridge_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/arr-bridge", headers=auth_headers)
        assert r.status_code == 200
        assert "periods" in r.json()

    async def test_arr_bridge_with_data(self, client, auth_headers):
        conn = _get_db()
        _seed_revenue(conn, "testcorp.com", [
            {"source_id": "r1", "amount": 1000, "period": "2024-01", "customer_id": "c1"},
            {"source_id": "r2", "amount": 1200, "period": "2024-02", "customer_id": "c1"},
            {"source_id": "r3", "amount": 500, "period": "2024-02", "customer_id": "c2"},
        ])
        conn.close()
        r = await client.get("/api/analytics/arr-bridge", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert len(data["periods"]) >= 1
        assert "summary" in data
        # Verify the bridge balances
        for p in data["periods"]:
            computed_ending = (p["beginning_arr"] + p["new_arr"] +
                             p["expansion_arr"] - p["contraction_arr"] - p["churned_arr"])
            assert abs(computed_ending - p["ending_arr"]) < 1.0, "ARR bridge must balance"


# ── Cohort Retention ─────────────────────────────────────────────────────────

@pytest.mark.anyio
class TestCohortRetention:
    async def test_cohort_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/cohort-retention", headers=auth_headers)
        assert r.status_code == 200
        assert "cohorts" in r.json()

    async def test_cohort_with_data(self, client, auth_headers):
        conn = _get_db()
        _seed_revenue(conn, "testcorp.com", [
            {"source_id": "cr1", "amount": 100, "period": "2024-01", "customer_id": "cc1"},
            {"source_id": "cr2", "amount": 100, "period": "2024-02", "customer_id": "cc1"},
            {"source_id": "cr3", "amount": 200, "period": "2024-01", "customer_id": "cc2"},
        ])
        conn.close()
        r = await client.get("/api/analytics/cohort-retention?metric=revenue", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert len(data["cohorts"]) >= 1
        # First cohort's M0 retention should be 100%
        if data["cohorts"][0]["months"]:
            assert data["cohorts"][0]["months"][0]["retention_pct"] == 100.0


# ── Customer Concentration ───────────────────────────────────────────────────

@pytest.mark.anyio
class TestCustomerConcentration:
    async def test_concentration_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/customer-concentration", headers=auth_headers)
        assert r.status_code == 200

    async def test_concentration_with_data(self, client, auth_headers):
        conn = _get_db()
        _seed_revenue(conn, "testcorp.com", [
            {"source_id": "cn1", "amount": 8000, "period": "2024-03", "customer_id": "big_customer"},
            {"source_id": "cn2", "amount": 1000, "period": "2024-03", "customer_id": "small_1"},
            {"source_id": "cn3", "amount": 1000, "period": "2024-03", "customer_id": "small_2"},
        ])
        conn.close()
        r = await client.get("/api/analytics/customer-concentration?top_n=10", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert len(data["customers"]) >= 1
        assert data["customers"][0]["rank"] == 1
        # big_customer should be >10% → SEC threshold breached
        assert data["sec_threshold_breached"] is True
        # Top 1 should be 80%
        assert data["top_1_pct"] >= 70


# ── Margin Decomposition ────────────────────────────────────────────────────

@pytest.mark.anyio
class TestMarginDecomposition:
    async def test_margin_decomp_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/margin-decomposition", headers=auth_headers)
        assert r.status_code == 200
        assert "periods" in r.json()

    async def test_margin_decomp_with_data(self, client, auth_headers):
        conn = _get_db()
        _seed_revenue(conn, "testcorp.com", [
            {"source_id": "mr1", "amount": 10000, "period": "2024-01", "customer_id": "mc1"},
        ])
        _seed_expenses(conn, "testcorp.com", [
            {"source_id": "me1", "amount": 2000, "period": "2024-01", "category": "hosting"},
            {"source_id": "me2", "amount": 1000, "period": "2024-01", "category": "cogs"},
        ])
        conn.close()
        r = await client.get("/api/analytics/margin-decomposition", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert len(data["periods"]) >= 1


# ── Cash Waterfall ──────────────────────────────────────────────────────────

@pytest.mark.anyio
class TestCashWaterfall:
    async def test_cash_waterfall_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/cash-waterfall", headers=auth_headers)
        assert r.status_code == 200
        assert "periods" in r.json()


# ── Unit Economics ──────────────────────────────────────────────────────────

@pytest.mark.anyio
class TestUnitEconomics:
    async def test_unit_economics(self, client, auth_headers):
        r = await client.get("/api/analytics/unit-economics", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "metrics" in data
        assert "ltv" in data
        assert "cac" in data
        assert "ltv_cac_ratio" in data


# ── Accountability Rollup ───────────────────────────────────────────────────

@pytest.mark.anyio
class TestAccountabilityRollup:
    async def test_accountability_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/accountability-rollup", headers=auth_headers)
        assert r.status_code == 200
        assert "owners" in r.json()


# ── Restatement History ─────────────────────────────────────────────────────

@pytest.mark.anyio
class TestRestatementHistory:
    async def test_restatement_empty(self, client, auth_headers):
        r = await client.get("/api/analytics/restatement-history", headers=auth_headers)
        assert r.status_code == 200
        assert "restatements" in r.json()


# ── Seasonality ─────────────────────────────────────────────────────────────

@pytest.mark.anyio
class TestSeasonality:
    async def test_seasonality_insufficient_data(self, client, auth_headers):
        r = await client.get("/api/analytics/seasonality?kpi_key=revenue_growth", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "kpi_key" in data


# ── Control Attestation ─────────────────────────────────────────────────────

@pytest.mark.anyio
class TestControlAttestation:
    async def test_attestation_no_checks(self, client, auth_headers):
        r = await client.get("/api/integrity-check/attestation", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "controls_tested" in data
        assert "overall_assessment" in data


# ── Scenario Comparison ─────────────────────────────────────────────────────

@pytest.mark.anyio
class TestScenarioComparison:
    async def test_compare_no_ids(self, client, auth_headers):
        r = await client.get("/api/scenarios/compare", headers=auth_headers)
        assert r.status_code == 400

    async def test_compare_empty_ids(self, client, auth_headers):
        r = await client.get("/api/scenarios/compare?ids=999", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        assert "scenarios" in data


# ── GAAP Status in Fingerprint ──────────────────────────────────────────────

@pytest.mark.anyio
class TestGaapStatus:
    async def test_fingerprint_includes_gaap(self, client, auth_headers):
        """Fingerprint response should include gaap_status for each KPI."""
        r = await client.get("/api/fingerprint", headers=auth_headers)
        assert r.status_code == 200
        data = r.json()
        if data:
            assert "gaap_status" in data[0], "Each KPI in fingerprint should have gaap_status"
