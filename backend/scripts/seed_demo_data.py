#!/usr/bin/env python3
"""
scripts/seed_demo_data.py — Comprehensive data seeder for Axiom Intelligence.

Seeds all 11 canonical tables with realistic transaction-level data from
Jan 2022 through Apr 2026, then runs the full KPI aggregator to compute
all 62 KPIs. Also seeds projection data through Dec 2026, KPI targets,
and company settings.

Company narrative:
  2022: Early-stage SaaS, $50K->$120K MRR, 55% gross margin, high burn
  2023: Post Series-A, $120K->$300K MRR, margins to 62%, team to 45
  2024: Scale-up, $300K->$550K MRR, retention challenge mid-year
  2025: Maturing ops, $550K->$750K MRR, AR aging issues Q2-Q3
  2026: Current year, $750K->$900K MRR, mixed signals

Usage:
  cd backend && python3 scripts/seed_demo_data.py
"""
import json
import math
import os
import random
import sys

# Add backend to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from core.database import get_db, init_db
from elt.kpi_aggregator import aggregate_canonical_to_monthly

WORKSPACE = "axiomsync.ai"  # Must match _org_id_for_email("rahul@axiomsync.ai") → domain
random.seed(42)  # Reproducible


def _ensure_canonical_tables(conn):
    """Create all canonical tables using the transformer schema definitions.
    Works on both SQLite and PostgreSQL — the conn wrapper handles DDL translation."""
    from elt.transformer import _CANONICAL_SCHEMAS

    real_cols = {"amount", "salary", "price", "spend", "probability", "leads",
                 "conversions", "cash_balance", "current_assets", "current_liabilities",
                 "total_assets", "total_liabilities", "billable_hours", "total_hours",
                 "usage_count", "nps_score", "csat_score", "resolution_hours", "effort_score"}
    template_cols = {"id", "workspace_id", "raw_id", "created_at", "updated_at"}

    for entity_type, schema in _CANONICAL_SCHEMAS.items():
        schema_cols = [col for col in schema.keys() if col not in template_cols]
        cols_sql = ", ".join(
            f"{col} REAL" if col in real_cols else f"{col} TEXT"
            for col in schema_cols
        )
        # Write as SQLite DDL — the _PGConn.execute() wrapper auto-translates
        # AUTOINCREMENT → SERIAL, CURRENT_TIMESTAMP → NOW() for PostgreSQL
        try:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS canonical_{entity_type} (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    workspace_id TEXT NOT NULL,
                    {cols_sql},
                    raw_id       TEXT,
                    created_at   TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at   TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
        except Exception as e:
            print(f"  [WARN] Table canonical_{entity_type}: {e}")
    conn.commit()

# ── Monthly growth model ─────────────────────────────────────────────────────

def _months(start_y, start_m, end_y, end_m):
    """Generate (year, month) tuples from start to end inclusive."""
    y, m = start_y, start_m
    while (y, m) <= (end_y, end_m):
        yield (y, m)
        m += 1
        if m > 12:
            m = 1
            y += 1


def _period(y, m):
    return f"{y}-{m:02d}"


def _date(y, m, d):
    return f"{y}-{m:02d}-{d:02d}"


def _jitter(base, pct=0.08):
    """Add realistic noise to a base value."""
    return base * (1 + random.uniform(-pct, pct))


# ── MRR trajectory (the spine of all other data) ────────────────────────────

def _mrr_trajectory():
    """Return dict of (y,m) -> target MRR, encoding the company story."""
    mrr = {}
    base = 50000  # Jan 2022 starting MRR
    for y, m in _months(2022, 1, 2026, 4):
        if y == 2022:
            growth = 0.045 + random.uniform(-0.005, 0.01)  # ~4.5% MoM
        elif y == 2023:
            growth = 0.055 + random.uniform(-0.005, 0.01)  # ~5.5% MoM
        elif y == 2024:
            if 5 <= m <= 8:
                growth = 0.025 + random.uniform(-0.005, 0.005)  # retention challenge
            else:
                growth = 0.04 + random.uniform(-0.005, 0.01)
        elif y == 2025:
            growth = 0.03 + random.uniform(-0.005, 0.008)  # maturing
        else:  # 2026
            growth = 0.025 + random.uniform(-0.005, 0.008)

        base *= (1 + growth)
        mrr[(y, m)] = round(base, 2)
    return mrr


# ── Batch insert helper (critical for PostgreSQL performance) ────────────────

def _batch_insert(conn, table, columns, rows, batch_size=200):
    """Insert rows in batches using multi-row VALUES for PostgreSQL performance.
    Individual INSERTs over network = ~20ms each = timeout on 1000+ rows.
    Multi-row INSERT = single round-trip per batch."""
    if not rows:
        return
    from core.database import _USE_PG
    col_list = ",".join(columns)
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        if _USE_PG:
            # PostgreSQL: use %s placeholders
            ph = ",".join(["(" + ",".join(["%s"] * len(columns)) + ")"] * len(batch))
            flat = [v for row in batch for v in row]
            conn._r.cursor().execute(f"INSERT INTO {table} ({col_list}) VALUES {ph}", flat)
            conn._r.commit()
        else:
            # SQLite: use individual inserts with executemany
            ph = ",".join(["?"] * len(columns))
            sql = f"INSERT INTO {table} ({col_list}) VALUES ({ph})"
            for row in batch:
                conn.execute(sql, row)


# ── Seeding functions ────────────────────────────────────────────────────────

def seed_revenue(conn, mrr_curve):
    """Seed canonical_revenue with per-customer transactions."""
    conn.execute("DELETE FROM canonical_revenue WHERE workspace_id=?", [WORKSPACE])

    customer_pool = [f"cust_{i:04d}" for i in range(1, 201)]
    active_customers = set(customer_pool[:15])
    all_customers_ever = set(active_customers)
    row_id = 0
    rows = []

    for (y, m), mrr in sorted(mrr_curve.items()):
        # Churn FIRST: remove customers before adding new ones
        if len(active_customers) > 12 and (y, m) > (2022, 3):
            if y == 2024 and 5 <= m <= 8:
                churn_pct = random.uniform(0.06, 0.12)
            elif y == 2025 and 7 <= m <= 9:
                churn_pct = random.uniform(0.04, 0.08)
            else:
                churn_pct = random.uniform(0.02, 0.05)
            n_churn = max(1, int(len(active_customers) * churn_pct))
            churnable = list(active_customers)
            churned = random.sample(churnable, min(n_churn, max(1, len(churnable) - 8)))
            for c in churned:
                active_customers.discard(c)

        # THEN grow customer base (add new customers)
        n_target = max(12, int(mrr / 8000))
        n_to_add = max(0, min(3, n_target - len(active_customers)))
        for _ in range(n_to_add):
            avail = [c for c in customer_pool if c not in all_customers_ever]
            if not avail:
                break
            new = random.choice(avail)
            active_customers.add(new)
            all_customers_ever.add(new)

        # Distribute MRR across active customers (Pareto-ish)
        weights = [random.paretovariate(1.5) for _ in active_customers]
        total_w = sum(weights)
        total_monthly = mrr + mrr * random.uniform(0.05, 0.15)  # one-time on top

        for cust, w in zip(sorted(active_customers), weights):
            amount = round(total_monthly * w / total_w, 2)
            is_recurring = random.random() < 0.82
            row_id += 1
            day = random.randint(1, 28)
            rows.append(["seed", f"rev_{row_id}", amount, "USD", _date(y, m, day),
                         cust, "recurring" if is_recurring else "one-time", WORKSPACE])

    _batch_insert(conn, "canonical_revenue",
                  ["source", "source_id", "amount", "currency", "period",
                   "customer_id", "subscription_type", "workspace_id"], rows)


def seed_customers(conn, mrr_curve):
    """Seed canonical_customers with acquisition dates."""
    conn.execute("DELETE FROM canonical_customers WHERE workspace_id=?", [WORKSPACE])

    rows = []
    n_total = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        target = max(2, int(mrr / 60000))  # new customers per month
        if y == 2024 and 5 <= m <= 8:
            target = max(1, target - 2)  # slower during challenge

        for i in range(target):
            n_total += 1
            cid = f"cust_{n_total:04d}"
            day = random.randint(1, 28)
            rows.append(
                ["seed", cid, f"Customer {n_total}", f"contact{n_total}@example.com",
                 f"Company {n_total}", random.choice(["US", "CA", "GB", "DE", "AU"]),
                 _date(y, m, day), "customer", WORKSPACE],
            )

    _batch_insert(conn, "canonical_customers",
                  ["source", "source_id", "name", "email", "company",
                   "country", "created_at", "lifecycle_stage", "workspace_id"], rows)


def seed_expenses(conn, mrr_curve):
    """Seed canonical_expenses with categorised transactions."""
    conn.execute("DELETE FROM canonical_expenses WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        total_rev = mrr * 1.12  # approximate total (MRR + one-time)

        # COGS: 30-42% of revenue (improving over time)
        cogs_pct = 0.42 - (y - 2022) * 0.025
        cogs_total = total_rev * max(cogs_pct, 0.28)

        # S&M: 25-35% of revenue
        sm_pct = 0.35 - (y - 2022) * 0.02
        sm_total = total_rev * max(sm_pct, 0.18)

        # G&A: 15-20% of revenue
        ga_total = total_rev * random.uniform(0.12, 0.18)

        # R&D: 10-15% of revenue
        rd_total = total_rev * random.uniform(0.08, 0.14)

        categories = [
            ("cogs",     cogs_total, ["hosting", "infrastructure", "direct cost"]),
            ("s&m",      sm_total,   ["marketing", "sales", "advertising", "lead gen"]),
            ("g&a",      ga_total,   ["g&a", "office", "legal", "admin"]),
            ("r&d",      rd_total,   ["r&d", "engineering", "product"]),
        ]

        for cat_name, cat_total, cat_labels in categories:
            n_txns = random.randint(4, 12)
            for i in range(n_txns):
                row_id += 1
                amount = round(cat_total / n_txns * _jitter(1.0, 0.2), 2)
                day = random.randint(1, 28)
                rows.append(
                    ["seed", f"exp_{row_id}", amount, "USD",
                     random.choice(cat_labels), f"Vendor-{random.randint(1,50)}",
                     _date(y, m, day), f"{cat_name} expense", WORKSPACE],
                )

    _batch_insert(conn, "canonical_expenses",
                  ["source", "source_id", "amount", "currency",
                   "category", "vendor", "period", "description", "workspace_id"], rows)


def seed_pipeline(conn, mrr_curve):
    """Seed canonical_pipeline with deals."""
    conn.execute("DELETE FROM canonical_pipeline WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_deals = max(5, int(mrr / 15000))
        for i in range(n_deals):
            row_id += 1
            amount = round(random.uniform(5000, 80000) * (1 + (y - 2022) * 0.15), 2)
            is_won = random.random() < (0.28 + (y - 2022) * 0.02)
            stage = "closed won" if is_won else random.choice(["prospecting", "qualified", "negotiation", "closed lost"])
            created_day = random.randint(1, 15)
            close_day = random.randint(15, 28)
            # Created 1-3 months before close
            created_m = m - random.randint(1, 3)
            created_y = y
            if created_m < 1:
                created_m += 12
                created_y -= 1
            rows.append(
                ["seed", f"deal_{row_id}", f"Deal {row_id}", amount, stage,
                 _date(y, m, close_day), 0.9 if is_won else random.uniform(0.1, 0.6),
                 f"rep_{random.randint(1,8)}", _date(created_y, created_m, created_day), WORKSPACE],
            )

    _batch_insert(conn, "canonical_pipeline",
                  ["source", "source_id", "name", "amount", "stage",
                   "close_date", "probability", "owner", "created_at", "workspace_id"], rows)


def seed_invoices(conn, mrr_curve):
    """Seed canonical_invoices with AR data."""
    conn.execute("DELETE FROM canonical_invoices WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_invoices = max(10, int(mrr / 5000))
        for i in range(n_invoices):
            row_id += 1
            amount = round(mrr / n_invoices * _jitter(1.0, 0.3), 2)
            issue_day = random.randint(1, 20)
            # Due date 30-60 days after issue (next month or month after)
            terms_days = random.choice([30, 30, 45, 60])
            due_m = m + (terms_days // 30)
            due_y = y
            if due_m > 12:
                due_m -= 12
                due_y += 1
            due_day = min(28, issue_day + (terms_days % 30))
            # AR aging: mostly paid, some overdue (worse in 2025 Q2-Q3)
            if y == 2025 and 4 <= m <= 9:
                paid_prob = 0.65
            else:
                paid_prob = 0.82
            status = "paid" if random.random() < paid_prob else random.choice(["outstanding", "overdue"])
            rows.append(
                ["seed", f"inv_{row_id}", amount, "USD", f"cust_{random.randint(1,150):04d}",
                 _date(y, m, issue_day), _date(due_y, due_m, due_day), status,
                 _period(y, m), WORKSPACE],
            )

    _batch_insert(conn, "canonical_invoices",
                  ["source", "source_id", "amount", "currency", "customer_id",
                   "issue_date", "due_date", "status", "period", "workspace_id"], rows)


def seed_employees(conn, mrr_curve):
    """Seed canonical_employees with headcount growth."""
    conn.execute("DELETE FROM canonical_employees WHERE workspace_id=?", [WORKSPACE])

    rows = []
    departments = ["Engineering", "Sales", "Marketing", "CS", "G&A", "Product"]
    emp_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        target_hc = max(8, int(mrr / 8000))  # 1 employee per $8K MRR
        n_new = max(0, min(6, target_hc - emp_id))
        for i in range(n_new):
            emp_id += 1
            dept = random.choice(departments)
            salary = round(random.uniform(5000, 18000) * (1 + (y - 2022) * 0.05), 2)
            day = random.randint(1, 28)
            rows.append(
                ["seed", f"emp_{emp_id:04d}", f"Employee {emp_id}",
                 f"emp{emp_id}@axiom.co", f"{'Senior ' if random.random() > 0.6 else ''}{dept} Role",
                 dept, salary, _date(y, m, day), "active", WORKSPACE],
            )

    _batch_insert(conn, "canonical_employees",
                  ["source", "source_id", "name", "email", "title",
                   "department", "salary", "hire_date", "status", "workspace_id"], rows)


def seed_marketing(conn, mrr_curve):
    """Seed canonical_marketing with channel spend/leads."""
    conn.execute("DELETE FROM canonical_marketing WHERE workspace_id=?", [WORKSPACE])

    rows = []
    channels = ["paid_search", "content", "events", "social", "organic", "referral"]
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        total_spend = mrr * random.uniform(0.15, 0.25)
        for ch in channels:
            row_id += 1
            ch_share = random.uniform(0.08, 0.35)
            spend = round(total_spend * ch_share, 2)
            leads = max(1, int(spend / random.uniform(40, 120)))
            conversions = max(0, int(leads * random.uniform(0.15, 0.35)))
            rows.append(
                ["seed", f"mkt_{row_id}", ch, spend, "USD",
                 _period(y, m), leads, conversions, WORKSPACE],
            )

    _batch_insert(conn, "canonical_marketing",
                  ["source", "source_id", "channel", "spend", "currency",
                   "period", "leads", "conversions", "workspace_id"], rows)


def seed_balance_sheet(conn, mrr_curve):
    """Seed canonical_balance_sheet with monthly snapshots."""
    conn.execute("DELETE FROM canonical_balance_sheet WHERE workspace_id=?", [WORKSPACE])

    rows = []
    cash = 2_500_000  # Starting cash (post-seed)
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        row_id += 1
        total_rev = mrr * 1.12
        total_exp = mrr * random.uniform(0.85, 1.15)
        net = total_rev - total_exp

        # Series A injection
        if y == 2023 and m == 3:
            cash += 8_000_000

        cash += net
        cash = max(cash, 100_000)

        curr_assets = cash + mrr * random.uniform(0.3, 0.5)
        curr_liab = total_exp * random.uniform(0.2, 0.4)

        rows.append(
            ["seed", f"bs_{row_id}", _period(y, m), round(cash, 2),
             round(curr_assets, 2), round(curr_liab, 2), "USD", WORKSPACE],
        )

    _batch_insert(conn, "canonical_balance_sheet",
                  ["source", "source_id", "period", "cash_balance",
                   "current_assets", "current_liabilities", "currency", "workspace_id"], rows)


def seed_time_tracking(conn, mrr_curve):
    """Seed canonical_time_tracking with billable hours."""
    conn.execute("DELETE FROM canonical_time_tracking WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_workers = max(5, int(mrr / 25000))
        for w in range(n_workers):
            row_id += 1
            total_hrs = round(random.uniform(140, 176), 1)
            # Utilization improves over time: 65% -> 78%
            util_rate = 0.65 + (y - 2022) * 0.03 + random.uniform(-0.05, 0.05)
            billable = round(total_hrs * min(util_rate, 0.85), 1)
            rows.append(
                ["seed", f"tt_{row_id}", f"emp_{w+1:04d}", _period(y, m),
                 billable, total_hrs, WORKSPACE],
            )

    _batch_insert(conn, "canonical_time_tracking",
                  ["source", "source_id", "worker_id", "period",
                   "billable_hours", "total_hours", "workspace_id"], rows)


def seed_surveys(conn, mrr_curve):
    """Seed canonical_surveys with NPS and CSAT scores."""
    conn.execute("DELETE FROM canonical_surveys WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_responses = max(10, int(mrr / 10000))
        for i in range(n_responses):
            row_id += 1
            # NPS range: -100 to 100; trending from 25 to 55
            base_nps = 25 + (y - 2022) * 7 + random.uniform(-15, 15)
            if y == 2024 and 5 <= m <= 8:
                base_nps -= 10  # dip during retention challenge
            nps = round(max(-100, min(100, base_nps)), 1)

            # CSAT: 1-5 scale; trending from 3.5 to 4.3
            base_csat = 3.5 + (y - 2022) * 0.18 + random.uniform(-0.4, 0.4)
            csat = round(max(1, min(5, base_csat)), 2)

            rows.append(
                ["seed", f"surv_{row_id}", f"cust_{random.randint(1,150):04d}",
                 _period(y, m), nps, csat, "quarterly", WORKSPACE],
            )

    _batch_insert(conn, "canonical_surveys",
                  ["source", "source_id", "respondent_id", "period",
                   "nps_score", "csat_score", "survey_type", "workspace_id"], rows)


def seed_support(conn, mrr_curve):
    """Seed canonical_support with ticket data."""
    conn.execute("DELETE FROM canonical_support WHERE workspace_id=?", [WORKSPACE])

    rows = []
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_customers = max(15, int(mrr / 8000))
        # Tickets per customer: declining from 0.8 to 0.4 (automation improving)
        tickets_per = 0.8 - (y - 2022) * 0.08 + random.uniform(-0.1, 0.1)
        n_tickets = max(3, int(n_customers * max(tickets_per, 0.25)))

        for i in range(n_tickets):
            row_id += 1
            resolution_hrs = round(random.uniform(1, 48) * (1 - (y - 2022) * 0.05), 1)
            day = random.randint(1, 28)
            rows.append(
                ["seed", f"tkt_{row_id}", f"TKT-{row_id:05d}", _period(y, m),
                 max(0.5, resolution_hrs), round(random.uniform(1, 5), 1),
                 random.choice(["resolved", "closed", "open"]),
                 f"cust_{random.randint(1,150):04d}", WORKSPACE],
            )

    _batch_insert(conn, "canonical_support",
                  ["source", "source_id", "ticket_id", "period",
                   "resolution_hours", "effort_score", "status", "customer_id", "workspace_id"], rows)


def seed_product_usage(conn, mrr_curve):
    """Seed canonical_product_usage with feature adoption data."""
    conn.execute("DELETE FROM canonical_product_usage WHERE workspace_id=?", [WORKSPACE])

    rows = []
    features = [f"feature_{i}" for i in range(1, 13)]
    row_id = 0
    for (y, m), mrr in sorted(mrr_curve.items()):
        n_users = max(20, int(mrr / 3000))
        # Features adopted increases over time
        n_features_avail = min(12, 5 + (y - 2022) * 2)
        active_features = features[:n_features_avail]

        for u in range(n_users):
            user_id = f"user_{u+1:04d}"
            for feat in random.sample(active_features, min(len(active_features), random.randint(2, n_features_avail))):
                row_id += 1
                activated_day = random.randint(1, 15)
                ttv_days = random.randint(1, 21)
                fv_day = min(28, activated_day + ttv_days)
                rows.append(
                    ["seed", f"usage_{row_id}", user_id, _period(y, m),
                     feat, random.randint(1, 50),
                     _date(y, m, activated_day), _date(y, m, fv_day), WORKSPACE],
                )

    _batch_insert(conn, "canonical_product_usage",
                  ["source", "source_id", "user_id", "period",
                   "feature_id", "usage_count", "activated_at", "first_value_at", "workspace_id"], rows)


def seed_targets(conn):
    """Seed kpi_targets for all 62 KPIs."""
    conn.execute("DELETE FROM kpi_targets WHERE workspace_id=?", [WORKSPACE])

    targets = {
        # Revenue
        "revenue_growth": (8, "higher", "pct"), "arr_growth": (10, "higher", "pct"),
        "recurring_revenue": (80, "higher", "pct"), "revenue_quality": (80, "higher", "pct"),
        "customer_concentration": (15, "lower", "pct"), "avg_deal_size": (25000, "higher", "usd"),
        "expansion_rate": (5, "higher", "pct"), "gross_dollar_ret": (90, "higher", "pct"),
        "customer_ltv": (50000, "higher", "usd"), "pricing_power_index": (2, "higher", "pct"),
        # Profitability
        "gross_margin": (65, "higher", "pct"), "operating_margin": (5, "higher", "pct"),
        "ebitda_margin": (8, "higher", "pct"), "contribution_margin": (55, "higher", "pct"),
        "opex_ratio": (80, "lower", "pct"), "operating_leverage": (1.5, "higher", "ratio"),
        "margin_volatility": (3, "lower", "pct"), "burn_multiple": (2, "lower", "ratio"),
        "payback_period": (18, "lower", "months"),
        # Retention
        "churn_rate": (3, "lower", "pct"), "nrr": (105, "higher", "pct"),
        "logo_retention": (97, "higher", "pct"), "customer_decay_slope": (0, "lower", "pct"),
        "ltv_cac": (3, "higher", "ratio"), "contraction_rate": (2, "lower", "pct"),
        # Efficiency
        "cac_payback": (12, "lower", "months"), "sales_efficiency": (1.2, "higher", "ratio"),
        "headcount_eff": (15000, "higher", "ratio"), "rev_per_employee": (180000, "higher", "usd"),
        "billable_utilization": (75, "higher", "pct"), "ramp_time": (3, "lower", "months"),
        # Cash Flow
        "dso": (35, "lower", "days"), "ar_turnover": (10, "higher", "ratio"),
        "avg_collection_period": (35, "lower", "days"), "cash_conv_cycle": (30, "lower", "days"),
        "cei": (90, "higher", "pct"), "ar_aging_current": (85, "higher", "pct"),
        "ar_aging_overdue": (10, "lower", "pct"), "cash_runway": (18, "higher", "months"),
        "current_ratio": (2, "higher", "ratio"), "working_capital": (50, "higher", "pct"),
        # Growth
        "pipeline_conversion": (25, "higher", "pct"), "win_rate": (30, "higher", "pct"),
        "pipeline_velocity": (500, "higher", "ratio"), "quota_attainment": (80, "higher", "pct"),
        "cpl": (80, "lower", "usd"), "mql_sql_rate": (25, "higher", "pct"),
        "marketing_roi": (200, "higher", "pct"),
        # Derived
        "growth_efficiency": (3, "higher", "ratio"), "revenue_momentum": (1.1, "higher", "ratio"),
        "revenue_fragility": (0.5, "lower", "ratio"), "burn_convexity": (0, "lower", "ratio"),
        # Product & Customer
        "product_nps": (40, "higher", "score"), "csat": (4, "higher", "score"),
        "feature_adoption": (60, "higher", "pct"), "activation_rate": (70, "higher", "pct"),
        "time_to_value": (7, "lower", "days"), "support_volume": (0.5, "lower", "ratio"),
        "automation_rate": (60, "higher", "pct"),
        "health_score": (70, "higher", "score"),
        "organic_traffic": (10, "higher", "pct"), "brand_awareness": (50, "higher", "score"),
    }

    rows = []
    for key, (val, direction, unit) in targets.items():
        rows.append([key, val, direction, unit, WORKSPACE])

    _batch_insert(conn, "kpi_targets",
                  ["kpi_key", "target_value", "direction", "unit", "workspace_id"], rows)


def seed_company_settings(conn):
    """Seed company settings."""
    conn.execute("DELETE FROM company_settings WHERE workspace_id=?", [WORKSPACE])

    settings = {
        "company_name": "Axiom Demo Co",
        "industry": "SaaS / Enterprise Software",
        "funding_stage": "series_a",
        "logo_url": "",
        "criticality_weights": json.dumps({"cw_gap": 25, "cw_trend": 25, "cw_impact": 30, "cw_domain": 20}),
    }
    rows = []
    for key, value in settings.items():
        rows.append([key, value, WORKSPACE])

    _batch_insert(conn, "company_settings",
                  ["key", "value", "workspace_id"], rows)


def seed_projections(conn, mrr_curve):
    """Seed projection_monthly_data for May-Dec 2026."""
    conn.execute("DELETE FROM projection_monthly_data WHERE workspace_id=?", [WORKSPACE])

    # Project from April 2026 MRR forward
    rows = []
    apr_mrr = mrr_curve.get((2026, 4), 850000)
    base = apr_mrr
    for y, m in _months(2026, 5, 2026, 12):
        base *= 1.03  # 3% projected monthly growth
        proj_data = {
            "revenue_growth": round(3 + random.uniform(-0.5, 1), 2),
            "gross_margin": round(68 + random.uniform(-1, 2), 2),
            "operating_margin": round(8 + random.uniform(-1, 2), 2),
            "ebitda_margin": round(12 + random.uniform(-1, 2), 2),
            "arr_growth": round(3.5 + random.uniform(-0.5, 1), 2),
            "nrr": round(112 + random.uniform(-2, 3), 2),
            "burn_multiple": round(1.2 + random.uniform(-0.2, 0.3), 2),
            "churn_rate": round(2.5 + random.uniform(-0.5, 0.5), 2),
            "cac_payback": round(10 + random.uniform(-1, 2), 2),
            "dso": round(32 + random.uniform(-3, 5), 2),
            "customer_concentration": round(8 + random.uniform(-1, 2), 2),
            "recurring_revenue": round(83 + random.uniform(-1, 2), 2),
            "opex_ratio": round(60 + random.uniform(-2, 3), 2),
            "sales_efficiency": round(1.5 + random.uniform(-0.1, 0.2), 2),
            "logo_retention": round(96 + random.uniform(-1, 1), 2),
            "headcount_eff": round(base / 55 * 1.05, 2),
            "rev_per_employee": round(base * 12 / 55, 2),
            "pipeline_conversion": round(28 + random.uniform(-2, 3), 2),
            "win_rate": round(32 + random.uniform(-2, 3), 2),
            "cash_runway": round(22 + random.uniform(-2, 3), 2),
            "product_nps": round(48 + random.uniform(-3, 5), 2),
            "csat": round(4.2 + random.uniform(-0.2, 0.2), 2),
            "billable_utilization": round(78 + random.uniform(-2, 3), 2),
            "mrr": round(base, 2),
            "arr": round(base * 12, 2),
        }
        rows.append([y, m, json.dumps(proj_data), WORKSPACE])

    _batch_insert(conn, "projection_monthly_data",
                  ["year", "month", "data_json", "workspace_id"], rows)


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Axiom Intelligence - Comprehensive Data Seeder")
    print("=" * 60)

    init_db()
    conn = get_db()

    # Ensure all canonical tables exist
    print("\nEnsuring all canonical tables exist...")
    _ensure_canonical_tables(conn)

    # Generate MRR trajectory
    mrr_curve = _mrr_trajectory()
    print(f"\nMRR trajectory: {len(mrr_curve)} months")
    print(f"  Jan 2022: ${mrr_curve[(2022,1)]:,.0f}")
    print(f"  Dec 2023: ${mrr_curve[(2023,12)]:,.0f}")
    print(f"  Dec 2024: ${mrr_curve[(2024,12)]:,.0f}")
    print(f"  Dec 2025: ${mrr_curve[(2025,12)]:,.0f}")
    print(f"  Apr 2026: ${mrr_curve[(2026,4)]:,.0f}")

    # Wipe existing aggregator data
    print("\n[1/14] Wiping existing monthly_data...")
    conn.execute("DELETE FROM monthly_data WHERE workspace_id=?", [WORKSPACE])
    conn.commit()

    # Seed all canonical tables
    print("[2/14] Seeding canonical_revenue...")
    seed_revenue(conn, mrr_curve)
    conn.commit()

    print("[3/14] Seeding canonical_customers...")
    seed_customers(conn, mrr_curve)
    conn.commit()

    print("[4/14] Seeding canonical_expenses...")
    seed_expenses(conn, mrr_curve)
    conn.commit()

    print("[5/14] Seeding canonical_pipeline...")
    seed_pipeline(conn, mrr_curve)
    conn.commit()

    print("[6/14] Seeding canonical_invoices...")
    seed_invoices(conn, mrr_curve)
    conn.commit()

    print("[7/14] Seeding canonical_employees...")
    seed_employees(conn, mrr_curve)
    conn.commit()

    print("[8/14] Seeding canonical_marketing...")
    seed_marketing(conn, mrr_curve)
    conn.commit()

    print("[9/14] Seeding canonical_balance_sheet...")
    seed_balance_sheet(conn, mrr_curve)
    conn.commit()

    print("[10/14] Seeding canonical_time_tracking...")
    seed_time_tracking(conn, mrr_curve)
    conn.commit()

    print("[11/14] Seeding canonical_surveys...")
    seed_surveys(conn, mrr_curve)
    conn.commit()

    print("[12/14] Seeding canonical_support...")
    seed_support(conn, mrr_curve)
    conn.commit()

    print("[13/14] Seeding canonical_product_usage...")
    seed_product_usage(conn, mrr_curve)
    conn.commit()

    # Run KPI aggregator
    print("\n[14/14] Running KPI aggregator...")
    result = aggregate_canonical_to_monthly(conn, WORKSPACE)
    print(f"  Months written: {result['months_written']}")
    print(f"  KPIs computed: {len(result['kpis_computed'])}")
    if result["errors"]:
        print(f"  Errors: {result['errors']}")
    if result["skipped"]:
        print(f"  Skipped: {result['skipped']}")

    # Verify: check latest month has comprehensive KPIs
    latest = conn.execute(
        "SELECT data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 1",
        [WORKSPACE],
    ).fetchone()
    if latest:
        data = json.loads(latest[0] if not isinstance(latest, dict) else latest["data_json"])
        print(f"\n  Latest month KPI count: {len(data)}")
        print(f"  Sample KPIs: {list(data.keys())[:15]}...")

    # Seed targets, settings, projections
    print("\nSeeding targets, company settings, and projections...")
    seed_targets(conn)
    seed_company_settings(conn)
    seed_projections(conn, mrr_curve)
    conn.commit()

    # Final counts
    for table in ["canonical_revenue", "canonical_customers", "canonical_expenses",
                  "canonical_pipeline", "canonical_invoices", "canonical_employees",
                  "canonical_marketing", "canonical_balance_sheet", "canonical_time_tracking",
                  "canonical_surveys", "canonical_support", "canonical_product_usage",
                  "monthly_data", "kpi_targets"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE workspace_id=?", [WORKSPACE]).fetchone()
            c = count[0] if not isinstance(count, dict) else list(count.values())[0]
            print(f"  {table}: {c:,} rows")
        except Exception:
            print(f"  {table}: (table not created yet)")

    print("\n" + "=" * 60)
    print("Seeding complete!")
    print("=" * 60)


if __name__ == "__main__":
    main()
