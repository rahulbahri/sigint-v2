"""
core/database.py — Database abstraction layer (SQLite local / PostgreSQL production),
plus init_db(), _migrate_workspace_data(), and the _audit() helper.
"""
import re
import sqlite3
from pathlib import Path
from typing import Optional

from core.config import (
    DATABASE_URL, _USE_PG, DB_PATH, _PG_UPSERT,
    JWT_SECRET, _ALLOWED_ORIGINS,
)

# ── Conditional import of psycopg2 ────────────────────────────────────────────
if _USE_PG:
    import psycopg2
    import psycopg2.extras


# ── SQL translation helpers ────────────────────────────────────────────────────

def _sql_translate(sql: str) -> str:
    """Translate SQLite SQL to PostgreSQL SQL."""
    sql = sql.replace("?", "%s")

    # INSERT OR IGNORE → INSERT ... ON CONFLICT DO NOTHING
    if re.search(r"INSERT\s+OR\s+IGNORE", sql, re.IGNORECASE):
        sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
        sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"

    # INSERT OR REPLACE → proper PostgreSQL upsert
    elif re.search(r"INSERT\s+OR\s+REPLACE", sql, re.IGNORECASE):
        # If caller already appended ON CONFLICT manually, just strip "OR REPLACE"
        if re.search(r"ON\s+CONFLICT", sql, re.IGNORECASE):
            sql = re.sub(r"\s+OR\s+REPLACE", "", sql, flags=re.IGNORECASE)
        else:
            m_tbl = re.search(r"INSERT\s+OR\s+REPLACE\s+INTO\s+(\w+)\s*\(([^)]+)\)", sql, re.IGNORECASE)
            if m_tbl:
                tbl   = m_tbl.group(1).lower()
                cols  = [c.strip() for c in m_tbl.group(2).split(",")]
                upsert = _PG_UPSERT.get(tbl)
                if upsert:
                    conflict_target, pk_cols = upsert
                    upd_cols = [c for c in cols if c.lower() not in pk_cols]
                    if upd_cols:
                        updates = ", ".join(f"{c}=EXCLUDED.{c}" for c in upd_cols)
                        suffix  = f" ON CONFLICT {conflict_target} DO UPDATE SET {updates}"
                    else:
                        suffix  = f" ON CONFLICT {conflict_target} DO NOTHING"
                else:
                    # Unknown table — safe fallback: strip OR REPLACE, no ON CONFLICT
                    suffix = ""
                sql = re.sub(r"INSERT\s+OR\s+REPLACE\s+INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
                sql = sql.rstrip().rstrip(";") + suffix
            else:
                sql = re.sub(r"\s+OR\s+REPLACE", "", sql, flags=re.IGNORECASE)

    # sqlite_master → information_schema.tables (hardcoded table name variant)
    sql = re.sub(
        r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'\s+AND\s+name\s*=\s*'(\w+)'",
        r"SELECT table_name AS name FROM information_schema.tables WHERE table_schema='public' AND table_name='\1'",
        sql, flags=re.IGNORECASE,
    )
    # sqlite_master → information_schema.tables (parameterised %s variant)
    sql = re.sub(
        r"SELECT\s+name\s+FROM\s+sqlite_master\s+WHERE\s+type\s*=\s*'table'\s+AND\s+name\s*=\s*%s",
        "SELECT table_name AS name FROM information_schema.tables WHERE table_schema='public' AND table_name=%s",
        sql, flags=re.IGNORECASE,
    )

    sql = re.sub(r"datetime\('now'\)", "NOW()", sql, flags=re.IGNORECASE)
    sql = re.sub(r"date\('now'\)", "CURRENT_DATE", sql, flags=re.IGNORECASE)
    return sql


def _schema_translate(sql: str) -> str:
    """Translate SQLite CREATE TABLE DDL to PostgreSQL DDL."""
    sql = _sql_translate(sql)
    sql = sql.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "SERIAL PRIMARY KEY")
    sql = re.sub(r"\bAUTOINCREMENT\b", "", sql)
    sql = re.sub(r"DEFAULT CURRENT_TIMESTAMP", "DEFAULT NOW()", sql, flags=re.IGNORECASE)
    return sql


# ── PostgreSQL compatibility wrappers ─────────────────────────────────────────

class _PGFakeRow(dict):
    """Dict that also supports integer-index access (like sqlite3.Row)."""
    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


class _PGCur:
    """Wraps psycopg2 cursor to behave like sqlite3 cursor."""
    def __init__(self, cur):
        self._c = cur

    def fetchall(self):
        try:
            rows = self._c.fetchall() or []
            return [_PGFakeRow(r) if isinstance(r, dict) else r for r in rows]
        except Exception:
            return []

    def fetchone(self):
        try:
            r = self._c.fetchone()
            return _PGFakeRow(r) if r else None
        except Exception:
            return None

    @property
    def lastrowid(self):
        try:
            self._c.execute("SELECT lastval()")
            row = self._c.fetchone()
            return row[0] if row else None
        except Exception:
            return None


class _PGConn:
    """Wraps psycopg2 connection to behave like sqlite3 connection."""
    def __init__(self, raw):
        self._r = raw
        self._last_cur: Optional[_PGCur] = None

    def execute(self, sql: str, params=None):
        sql = sql.strip()
        if re.match(r"PRAGMA\s+journal_mode", sql, re.IGNORECASE):
            return _PGCur(self._r.cursor())
        m = re.match(r"PRAGMA\s+table_info\((\w+)\)", sql, re.IGNORECASE)
        if m:
            tbl = m.group(1)
            cur = self._r.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("""
                SELECT (ordinal_position-1) AS cid, column_name AS name,
                       data_type AS type,
                       CASE WHEN is_nullable='NO' THEN 1 ELSE 0 END AS notnull,
                       column_default AS dflt_value, 0 AS pk
                FROM information_schema.columns
                WHERE table_name=%s ORDER BY ordinal_position
            """, [tbl])
            self._last_cur = _PGCur(cur)
            return self._last_cur
        # DDL statements need full schema translation (AUTOINCREMENT → SERIAL etc.)
        if re.match(r"\s*(CREATE|ALTER)\s+TABLE", sql, re.IGNORECASE):
            pg_sql = _schema_translate(sql)
        else:
            pg_sql = _sql_translate(sql)
        cur = self._r.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(pg_sql, params if params is not None else None)
            self._last_cur = _PGCur(cur)
            return self._last_cur
        except Exception:
            self._r.rollback()
            raise

    def executescript(self, script: str):
        script = _schema_translate(script)
        cur = self._r.cursor()
        for stmt in [s.strip() for s in script.split(";") if s.strip()]:
            if re.match(r"PRAGMA", stmt, re.IGNORECASE):
                continue
            try:
                cur.execute(stmt)
                self._r.commit()
            except Exception as e:
                self._r.rollback()
                err = str(e).lower()
                if "already exists" not in err and "duplicate" not in err:
                    print(f"[DB][WARN] DDL failed: {e} | stmt={stmt[:120]}")

    def commit(self):
        self._r.commit()

    def close(self):
        self._r.close()

    @property
    def lastrowid(self):
        if self._last_cur:
            return self._last_cur.lastrowid
        return None


# ── Public DB factory ─────────────────────────────────────────────────────────

def get_db():
    if _USE_PG:
        try:
            conn = psycopg2.connect(DATABASE_URL, connect_timeout=10)
            return _PGConn(conn)
        except Exception as _pg_err:
            print(f"[WARN] PostgreSQL unavailable ({_pg_err}), falling back to SQLite")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ── Workspace migration helper ────────────────────────────────────────────────

def _migrate_workspace_data(conn, old_workspace_id: str, new_workspace_id: str):
    """
    One-time migration: re-tag all rows from old_workspace_id (email) to
    new_workspace_id (org domain) so the user's data is accessible under the
    shared org workspace.
    """
    if old_workspace_id == new_workspace_id:
        return
    tables = [
        "uploads", "monthly_data", "kpi_targets", "projection_uploads",
        "projection_monthly_data", "kpi_accountability", "annotations",
        "recommendation_outcomes", "audit_log", "company_settings",
        "decisions", "connector_configs", "markov_models", "saved_scenarios",
    ]
    for tbl in tables:
        try:
            conn.execute(
                f"UPDATE {tbl} SET workspace_id=? WHERE workspace_id=?",
                [new_workspace_id, old_workspace_id],
            )
        except Exception as _e:
            print(f"[DB][WARN] Migration failed for table {tbl}: {_e}")


# ── Audit log helper ──────────────────────────────────────────────────────────

def _audit(conn_or_event_type, event_type_or_entity_type=None, description_or_entity_id=None,
           entity_type_or_description=None, entity_id=None, user: str = "system"):
    """
    Write a row to audit_log. Always opens its own isolated DB connection so
    that audit failures can NEVER roll back the caller's data transaction.

    Calling conventions (both still accepted for backward compatibility):

    Old-style (conn is ignored — a fresh connection is always opened):
        _audit(conn, event_type, description, entity_type=None, entity_id=None)

    New-style:
        _audit(event_type, entity_type, entity_id, description)

    Note: 'user' is a reserved keyword in PostgreSQL — the column is always
    referenced as "user" (double-quoted) so it works on both SQLite and PG.
    """
    try:
        # Detect which calling convention is being used
        if hasattr(conn_or_event_type, 'execute'):
            # Old-style: first arg looks like a connection — ignore it, use own conn
            event_type  = event_type_or_entity_type
            description = description_or_entity_id
            entity_type = entity_type_or_description
            _entity_id  = entity_id
        else:
            # New-style: first arg is event_type string
            event_type  = conn_or_event_type
            entity_type = event_type_or_entity_type
            _entity_id  = description_or_entity_id
            description = entity_type_or_description

        # Always use an isolated connection — never pollute the caller's transaction
        _conn = get_db()
        try:
            # "user" is double-quoted to avoid PostgreSQL reserved-word error
            _conn.execute(
                'INSERT INTO audit_log (event_type, entity_type, entity_id, description, "user") VALUES (?,?,?,?,?)',
                (event_type, entity_type, _entity_id, description, user)
            )
            _conn.commit()
        finally:
            _conn.close()
    except Exception as _e:
        print(f"[AUDIT][WARN] Failed to write audit event '{conn_or_event_type}': {_e}")


# ── Database initialisation ───────────────────────────────────────────────────

# init_db() is kept for backward compatibility and dev convenience.
# In production, run: alembic upgrade head
# This function is safe to call even after migrations — all statements use CREATE TABLE IF NOT EXISTS.
def init_db():
    conn = get_db()
    if not _USE_PG:
        # WAL mode for SQLite only
        conn.execute("PRAGMA journal_mode=WAL")
        conn.commit()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            uploaded_at TEXT,
            row_count INTEGER,
            detected_columns TEXT,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS monthly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            upload_id INTEGER,
            year INTEGER,
            month INTEGER,
            data_json TEXT,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS kpi_targets (
            kpi_key TEXT,
            target_value REAL,
            unit TEXT,
            direction TEXT,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (kpi_key, workspace_id)
        );
        CREATE TABLE IF NOT EXISTS projection_uploads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT,
            uploaded_at TEXT,
            row_count INTEGER,
            detected_columns TEXT,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS projection_monthly_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            projection_upload_id INTEGER,
            year INTEGER,
            month INTEGER,
            data_json TEXT,
            workspace_id TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS kpi_annotations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            kpi_key TEXT NOT NULL,
            period TEXT NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(kpi_key, period)
        );
        CREATE TABLE IF NOT EXISTS kpi_accountability (
            kpi_key TEXT,
            owner TEXT DEFAULT '',
            due_date TEXT DEFAULT '',
            status TEXT DEFAULT 'open',
            last_updated TEXT DEFAULT CURRENT_TIMESTAMP,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (kpi_key, workspace_id)
        );
        CREATE TABLE IF NOT EXISTS company_settings (
            key   TEXT,
            value TEXT,
            workspace_id TEXT DEFAULT '',
            PRIMARY KEY (key, workspace_id)
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type  TEXT NOT NULL,
            entity_type TEXT,
            entity_id   TEXT,
            description TEXT NOT NULL,
            "user"      TEXT DEFAULT 'system',
            ip_address  TEXT,
            created_at  TEXT DEFAULT (datetime('now')),
            workspace_id TEXT DEFAULT ''
        );
    """)
    conn.commit()
    # Annotations table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS annotations (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            kpi_key     TEXT NOT NULL,
            period      TEXT NOT NULL,
            note        TEXT NOT NULL,
            author      TEXT DEFAULT 'CFO',
            created_at  TEXT DEFAULT (datetime('now')),
            updated_at  TEXT DEFAULT (datetime('now')),
            workspace_id TEXT DEFAULT ''
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_annotations_kpi ON annotations(kpi_key, period)")
    # Recommendation outcomes table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS recommendation_outcomes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            kpi_key         TEXT NOT NULL,
            action_text     TEXT NOT NULL,
            started_at      TEXT DEFAULT (datetime('now')),
            resolved_at     TEXT,
            before_value    REAL,
            after_value     REAL,
            before_status   TEXT,
            after_status    TEXT,
            outcome_notes   TEXT,
            was_effective   INTEGER DEFAULT NULL,
            workspace_id    TEXT DEFAULT ''
        )
    """)
    conn.commit()
    # Decision log table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS decisions (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id    TEXT DEFAULT '',
            title           TEXT NOT NULL,
            the_decision    TEXT NOT NULL,
            rationale       TEXT DEFAULT '',
            kpi_context     TEXT DEFAULT '[]',
            outcome         TEXT DEFAULT '',
            decided_by      TEXT DEFAULT 'CFO',
            status          TEXT DEFAULT 'active',
            decided_at      TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_decisions_workspace ON decisions(workspace_id)")
    conn.commit()
    # ALTER TABLE migrations for existing tables (add workspace_id if missing)
    for tbl in ["uploads","monthly_data","kpi_targets","projection_uploads",
                "projection_monthly_data","kpi_accountability","annotations",
                "recommendation_outcomes","audit_log","company_settings"]:
        try:
            conn.execute(f"ALTER TABLE {tbl} ADD COLUMN workspace_id TEXT DEFAULT ''")
            conn.commit()
        except Exception:
            pass  # Column already exists
    # Workspace indexes
    for tbl in ["uploads","monthly_data","kpi_targets","projection_uploads",
                "projection_monthly_data","kpi_accountability","annotations",
                "recommendation_outcomes","audit_log","company_settings"]:
        try:
            conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{tbl}_workspace ON {tbl}(workspace_id)")
            conn.commit()
        except Exception:
            pass
    # Migration: add version_label to projection tables if missing
    if _USE_PG:
        conn.execute("ALTER TABLE projection_uploads ADD COLUMN IF NOT EXISTS version_label TEXT DEFAULT 'v1'")
        conn.execute("ALTER TABLE projection_monthly_data ADD COLUMN IF NOT EXISTS version_label TEXT DEFAULT 'v1'")
        conn.commit()
    else:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(projection_uploads)").fetchall()]
        if "version_label" not in cols:
            conn.execute("ALTER TABLE projection_uploads ADD COLUMN version_label TEXT DEFAULT 'v1'")
            conn.commit()
        cols2 = [r[1] for r in conn.execute("PRAGMA table_info(projection_monthly_data)").fetchall()]
        if "version_label" not in cols2:
            conn.execute("ALTER TABLE projection_monthly_data ADD COLUMN version_label TEXT DEFAULT 'v1'")
            conn.commit()
    # Seed default targets
    default_targets = [
        ("revenue_growth",      6.0,   "pct",    "higher"),
        ("gross_margin",       62.0,   "pct",    "higher"),
        ("operating_margin",   18.0,   "pct",    "higher"),
        ("ebitda_margin",      22.0,   "pct",    "higher"),
        ("cash_conv_cycle",    42.0,   "days",   "lower"),
        ("dso",                35.0,   "days",   "lower"),
        ("ar_turnover",         8.5,   "ratio",  "higher"),
        ("avg_collection_period", 43.0, "days",  "lower"),
        ("cei",                90.0,   "pct",    "higher"),
        ("ar_aging_current",   75.0,   "pct",    "higher"),
        ("ar_aging_overdue",   25.0,   "pct",    "lower"),
        ("billable_utilization", 72.0, "pct",    "higher"),
        ("arr_growth",          7.0,   "pct",    "higher"),
        ("nrr",               105.0,   "pct",    "higher"),
        ("burn_multiple",       1.2,   "ratio",  "lower"),
        ("opex_ratio",         42.0,   "pct",    "lower"),
        ("contribution_margin",46.0,   "pct",    "higher"),
        ("revenue_quality",    80.0,   "pct",    "higher"),
        ("cac_payback",        10.0,   "months", "lower"),
        ("sales_efficiency",    3.0,   "ratio",  "higher"),
        ("customer_concentration",28.0,"pct",    "lower"),
        ("recurring_revenue",  80.0,   "pct",    "higher"),
        ("churn_rate",          2.5,   "pct",    "lower"),
        ("operating_leverage",  1.2,   "ratio",  "higher"),
        ("growth_efficiency",    3.0,  "ratio", "higher"),
        ("revenue_momentum",     1.0,  "ratio", "higher"),
        ("revenue_fragility",    1.0,  "ratio", "lower"),
        ("burn_convexity",       0.0,  "ratio", "lower"),
        ("margin_volatility",    2.0,  "pct",   "lower"),
        ("pipeline_conversion",  5.0,  "pct",   "higher"),
        ("customer_decay_slope", 0.0,  "pct",   "lower"),
        ("customer_ltv",        80.0,  "usd",   "higher"),
        ("pricing_power_index",  3.0,  "pct",   "higher"),
        ("cpl",                150.0,  "usd",   "lower"),
        ("mql_sql_rate",        28.0,  "pct",   "higher"),
        ("win_rate",            30.0,  "pct",   "higher"),
        ("quota_attainment",    85.0,  "pct",   "higher"),
        ("marketing_roi",        3.5,  "ratio", "higher"),
        ("headcount_eff",        1.8,  "ratio", "higher"),
        ("rev_per_employee",   180.0,  "usd",   "higher"),
        ("ltv_cac",              4.0,  "ratio", "higher"),
        ("expansion_rate",      22.0,  "pct",   "higher"),
        ("health_score",        72.0,  "score", "higher"),
        ("logo_retention",      90.0,  "pct",   "higher"),
        ("payback_period",      14.0,  "months","lower"),
    ]
    for row in default_targets:
        conn.execute(
            "INSERT OR IGNORE INTO kpi_targets (kpi_key, target_value, unit, direction, workspace_id) VALUES (?,?,?,?,?)",
            row + ("",)
        )
    conn.commit()
    # Auth tables
    conn.execute("""
        CREATE TABLE IF NOT EXISTS magic_tokens (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            email       TEXT NOT NULL,
            token       TEXT NOT NULL UNIQUE,
            created_at  TEXT DEFAULT (datetime('now')),
            expires_at  TEXT NOT NULL,
            used        INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            email        TEXT NOT NULL UNIQUE,
            org_id       TEXT DEFAULT '',
            role         TEXT DEFAULT 'admin',
            display_name TEXT DEFAULT '',
            status       TEXT DEFAULT 'active',
            invited_by   TEXT DEFAULT '',
            org_migrated INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now')),
            last_login   TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS organisations (
            id           TEXT PRIMARY KEY,
            name         TEXT DEFAULT '',
            plan         TEXT DEFAULT 'free',
            invite_only  INTEGER DEFAULT 0,
            created_at   TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS org_invites (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            org_id      TEXT NOT NULL,
            email       TEXT NOT NULL,
            invited_by  TEXT NOT NULL,
            token       TEXT UNIQUE NOT NULL,
            expires_at  TEXT NOT NULL,
            accepted    INTEGER DEFAULT 0,
            created_at  TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS saved_scenarios (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id TEXT NOT NULL,
            name         TEXT NOT NULL,
            levers_json  TEXT NOT NULL,
            notes        TEXT DEFAULT '',
            created_at   TEXT DEFAULT (datetime('now')),
            updated_at   TEXT DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    # ALTER TABLE migrations for users columns
    for _mig in [
        "ALTER TABLE users ADD COLUMN org_id TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN display_name TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN status TEXT DEFAULT 'active'",
        "ALTER TABLE users ADD COLUMN invited_by TEXT DEFAULT ''",
        "ALTER TABLE users ADD COLUMN org_migrated INTEGER DEFAULT 0",
    ]:
        try:
            conn.execute(_mig)
            conn.commit()
        except Exception:
            pass
    conn.close()
