"""
main.py — Thin app factory: wires together routers, CORS, middleware, static files.
All business logic lives in routers/ and core/.
"""
import os
from pathlib import Path

# Load .env file for local development (no-op if file doesn't exist)
try:
    import dotenv
    dotenv.load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.config import _ALLOWED_ORIGINS
from core.database import get_db, _migrate_workspace_data  # noqa: F401 — imported for side-effect init
from core.security import rate_limit_middleware

# ── Sentry error tracking (no-op if SENTRY_DSN is not set) ───────────────────
_SENTRY_DSN = os.environ.get("SENTRY_DSN", "")
if _SENTRY_DSN:
    import sentry_sdk
    from sentry_sdk.integrations.fastapi import FastApiIntegration
    from sentry_sdk.integrations.starlette import StarletteIntegration
    sentry_sdk.init(
        dsn=_SENTRY_DSN,
        integrations=[StarletteIntegration(), FastApiIntegration()],
        traces_sample_rate=0.2,   # capture 20% of transactions for performance
        send_default_pii=False,   # never send emails / tokens to Sentry
    )

# ── App factory ───────────────────────────────────────────────────────────────

app = FastAPI(
    title="Axiom KPI Dashboard API",
    description="Upload CSVs to compute and track Priority-1 KPIs with 12-month fingerprinting.",
    version="1.0.0",
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json",
)

# ── CORS ──────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Request logging middleware ─────────────────────────────────────────────────

import time as _time
import logging as _logging
_req_logger = _logging.getLogger("axiom.requests")

@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    start = _time.time()
    response = await call_next(request)
    elapsed = round((_time.time() - start) * 1000, 1)
    if not request.url.path.startswith("/assets"):
        _req_logger.info("%s %s %s %sms", request.method, request.url.path, response.status_code, elapsed)
    return response

# ── Rate limiting middleware ───────────────────────────────────────────────────

app.middleware("http")(rate_limit_middleware)

# ── Security headers middleware ────────────────────────────────────────────────

@app.middleware("http")
async def security_headers_middleware(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if "text/html" in response.headers.get("content-type", ""):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
            "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
            "font-src 'self' https://fonts.gstatic.com; "
            "img-src 'self' data: blob:; "
            "connect-src 'self' https://api.anthropic.com; "
            "frame-ancestors 'none';"
        )
    return response

# ── Routers ───────────────────────────────────────────────────────────────────

from routers import (  # noqa: E402 — after middleware setup
    admin,
    alerts,
    analytics,
    annotations,
    auth,
    benchmarks,
    billing,
    board_pack,
    connectors,
    decisions,
    departments,
    forecast,
    segments,
    health,
    jobs,
    ontology,
    org,
    scenarios,
    settings,
    upload,
)

for _router_mod in [
    admin, alerts, analytics, annotations, auth, benchmarks, billing,
    board_pack, connectors, decisions, departments, forecast, health, jobs,
    segments,
    ontology, org, scenarios, settings, upload,
]:
    app.include_router(_router_mod.router)

# ── Auto-seed on startup if database is empty ────────────────────────────────
# Runs at boot time (not in an HTTP request), so no timeout constraint.
# Self-heals after deploys, DB wipes, or fresh PostgreSQL instances.

import threading as _threading  # noqa: E402

def _auto_seed_if_empty():
    """Check if the default workspace has data. If not, seed it.

    SAFETY: Only seeds the DEMO workspace (axiomsync.ai). Never touches
    other workspaces. The _DEMO_ONLY_WS guard prevents demo data from being
    injected into real customer workspaces.
    """
    import logging, random, traceback
    log = logging.getLogger("auto_seed")

    _DEFAULT_WS = os.environ.get("DEFAULT_WORKSPACE", "axiomsync.ai")
    # Safety guard: only auto-seed the demo workspace, never a customer workspace
    _DEMO_ONLY_WS = {"axiomsync.ai", "rahul@axiomsync.ai", "demo.axiomsync.ai"}
    if _DEFAULT_WS not in _DEMO_ONLY_WS:
        log.info("[AUTO_SEED] DEFAULT_WORKSPACE=%s is not a demo workspace — skipping auto-seed.", _DEFAULT_WS)
        return
    # Also check the email-format workspace (pre-migration format)
    _LEGACY_WS = "rahul@axiomsync.ai"
    conn = None
    try:
        conn = get_db()

        # Check if either workspace has data
        for ws_check in [_DEFAULT_WS, _LEGACY_WS]:
            try:
                row = conn.execute(
                    "SELECT COUNT(*) FROM monthly_data WHERE workspace_id=?", [ws_check]
                ).fetchone()
                count = row[0] if not isinstance(row, dict) else list(row.values())[0]
                if count > 10:
                    log.info("[AUTO_SEED] Database has %d months for %s — skipping seed.", count, ws_check)
                    # If data exists under legacy workspace, migrate it to new workspace
                    if ws_check == _LEGACY_WS and ws_check != _DEFAULT_WS:
                        log.info("[AUTO_SEED] Migrating data from %s to %s...", _LEGACY_WS, _DEFAULT_WS)
                        for table in ["monthly_data", "projection_monthly_data", "kpi_targets",
                                      "company_settings", "uploads"]:
                            try:
                                conn.execute(f"UPDATE {table} SET workspace_id=? WHERE workspace_id=?",
                                             [_DEFAULT_WS, _LEGACY_WS])
                            except Exception as e:
                                log.warning("[AUTO_SEED] Skip migrate %s: %s", table, e)
                        conn.commit()
                        log.info("[AUTO_SEED] Migration complete.")

                    # Always ensure targets and settings exist (even if data was migrated)
                    try:
                        tgt_count = conn.execute(
                            "SELECT COUNT(*) FROM kpi_targets WHERE workspace_id=?", [_DEFAULT_WS]
                        ).fetchone()
                        tc = tgt_count[0] if not isinstance(tgt_count, dict) else list(tgt_count.values())[0]
                        if tc < 60:
                            log.info("[AUTO_SEED] Only %d targets — seeding full 62 targets + settings...", tc)
                            import sys as _sys2, random as _r2
                            _sys2.path.insert(0, os.path.join(os.path.dirname(__file__)))
                            from scripts.seed_demo_data import seed_targets, seed_company_settings
                            from scripts import seed_demo_data
                            seed_demo_data.WORKSPACE = _DEFAULT_WS
                            # Wipe old partial targets
                            conn.execute("DELETE FROM kpi_targets WHERE workspace_id=?", [_DEFAULT_WS])
                            conn.execute("DELETE FROM company_settings WHERE workspace_id=?", [_DEFAULT_WS])
                            conn.commit()
                            _r2.seed(42)
                            seed_targets(conn); conn.commit()
                            seed_company_settings(conn); conn.commit()
                            log.info("[AUTO_SEED] Targets + settings seeded for %s", _DEFAULT_WS)
                    except Exception as e:
                        log.warning("[AUTO_SEED] Target seeding failed: %s", e)

                    return
            except Exception:
                pass

        log.info("[AUTO_SEED] Database empty — starting seed for %s...", _DEFAULT_WS)

        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
        from scripts.seed_demo_data import (
            _ensure_canonical_tables, _mrr_trajectory,
            seed_revenue, seed_customers, seed_expenses, seed_pipeline,
            seed_invoices, seed_employees, seed_marketing, seed_balance_sheet,
            seed_time_tracking, seed_surveys, seed_support, seed_product_usage,
            seed_targets, seed_company_settings, seed_projections,
        )
        from scripts import seed_demo_data
        from elt.kpi_aggregator import aggregate_canonical_to_monthly

        seed_demo_data.WORKSPACE = _DEFAULT_WS
        random.seed(42)

        _ensure_canonical_tables(conn)

        # Wipe any partial data
        for table in ["monthly_data", "projection_monthly_data", "kpi_targets",
                      "company_settings",
                      "canonical_revenue", "canonical_expenses", "canonical_customers",
                      "canonical_pipeline", "canonical_invoices", "canonical_employees",
                      "canonical_marketing", "canonical_balance_sheet",
                      "canonical_time_tracking", "canonical_surveys",
                      "canonical_support", "canonical_product_usage"]:
            try:
                conn.execute(f"DELETE FROM {table} WHERE workspace_id=?", [_DEFAULT_WS])
            except Exception:
                pass
        conn.commit()

        mrr = _mrr_trajectory()
        seed_fns = [
            ("revenue", seed_revenue), ("customers", seed_customers),
            ("expenses", seed_expenses), ("pipeline", seed_pipeline),
            ("invoices", seed_invoices), ("employees", seed_employees),
            ("marketing", seed_marketing), ("balance_sheet", seed_balance_sheet),
            ("time_tracking", seed_time_tracking), ("surveys", seed_surveys),
            ("support", seed_support), ("product_usage", seed_product_usage),
        ]
        for name, fn in seed_fns:
            log.info("[AUTO_SEED] Seeding %s...", name)
            try:
                fn(conn, mrr)
                conn.commit()
            except Exception as e:
                log.error("[AUTO_SEED] FAILED on %s: %s", name, e)
                traceback.print_exc()
                try:
                    conn.rollback()
                except Exception:
                    pass
                return  # Stop on first failure

        log.info("[AUTO_SEED] Running KPI aggregator...")
        agg = aggregate_canonical_to_monthly(conn, _DEFAULT_WS)
        log.info("[AUTO_SEED] Aggregator: %d months, %d KPIs", agg["months_written"], len(agg["kpis_computed"]))

        seed_targets(conn); conn.commit()
        seed_company_settings(conn); conn.commit()
        seed_projections(conn, mrr); conn.commit()

        log.info("[AUTO_SEED] COMPLETE — %d months, %d KPIs for %s",
                 agg["months_written"], len(agg["kpis_computed"]), _DEFAULT_WS)

    except Exception as e:
        print(f"[AUTO_SEED] FAILED: {e}")
        traceback.print_exc()
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


# Run auto-seed in a background thread at startup (non-blocking)
_seed_thread = _threading.Thread(target=_auto_seed_if_empty, daemon=True)
_seed_thread.start()


# ── Health endpoint ───────────────────────────────────────────────────────────

from datetime import datetime  # noqa: E402

@app.get("/api/health", tags=["System"])
def health():
    return {
        "status": "ok",
        "timestamp": datetime.utcnow().isoformat(),
        "sentry": "enabled" if _SENTRY_DSN else "disabled",
    }


# ── Database diagnostic endpoint ─────────────────────────────────────────────

@app.get("/api/seed-step", tags=["System"])
def seed_step(request: Request, step: str = "test", workspace: str = "axiomsync.ai"):
    """Run a single seed step synchronously and return the result or error.
    Steps: test, wipe, revenue, customers, expenses, pipeline, invoices,
           employees, marketing, balance, time, surveys, support, usage,
           aggregate, targets, settings, projections
    """
    import traceback, random, sys
    sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
    from scripts.seed_demo_data import (
        _ensure_canonical_tables, _mrr_trajectory,
        seed_revenue, seed_customers, seed_expenses, seed_pipeline,
        seed_invoices, seed_employees, seed_marketing, seed_balance_sheet,
        seed_time_tracking, seed_surveys, seed_support, seed_product_usage,
        seed_targets, seed_company_settings, seed_projections,
    )
    from scripts import seed_demo_data
    from elt.kpi_aggregator import aggregate_canonical_to_monthly

    seed_demo_data.WORKSPACE = workspace
    random.seed(42)

    conn = get_db()
    try:
        if step == "test":
            conn.execute("SELECT 1")
            return {"step": "test", "status": "ok", "db": "connected"}

        if step == "tables":
            _ensure_canonical_tables(conn)
            return {"step": "tables", "status": "ok"}

        if step == "wipe":
            for t in ["monthly_data", "projection_monthly_data", "kpi_targets",
                      "company_settings",
                      "canonical_revenue", "canonical_expenses", "canonical_customers",
                      "canonical_pipeline", "canonical_invoices", "canonical_employees",
                      "canonical_marketing", "canonical_balance_sheet",
                      "canonical_time_tracking", "canonical_surveys",
                      "canonical_support", "canonical_product_usage"]:
                try:
                    conn.execute(f"DELETE FROM {t} WHERE workspace_id=?", [workspace])
                except Exception as e:
                    return {"step": "wipe", "status": "error", "table": t, "error": str(e)}
            conn.commit()
            return {"step": "wipe", "status": "ok"}

        mrr = _mrr_trajectory()
        step_map = {
            "revenue": seed_revenue, "customers": seed_customers,
            "expenses": seed_expenses, "pipeline": seed_pipeline,
            "invoices": seed_invoices, "employees": seed_employees,
            "marketing": seed_marketing, "balance": seed_balance_sheet,
            "time": seed_time_tracking, "surveys": seed_surveys,
            "support": seed_support, "usage": seed_product_usage,
        }

        if step in step_map:
            step_map[step](conn, mrr)
            conn.commit()
            return {"step": step, "status": "ok"}

        if step == "aggregate":
            r = aggregate_canonical_to_monthly(conn, workspace)
            return {"step": "aggregate", "status": "ok",
                    "months": r["months_written"], "kpis": len(r["kpis_computed"]),
                    "errors": r.get("errors", [])}

        if step == "targets":
            seed_targets(conn); conn.commit()
            return {"step": "targets", "status": "ok"}

        if step == "settings":
            seed_company_settings(conn); conn.commit()
            return {"step": "settings", "status": "ok"}

        if step == "projections":
            seed_projections(conn, _mrr_trajectory()); conn.commit()
            return {"step": "projections", "status": "ok"}

        return {"step": step, "status": "error", "error": f"Unknown step: {step}"}

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        return {"step": step, "status": "error", "error": str(e),
                "traceback": traceback.format_exc()}
    finally:
        conn.close()


@app.get("/api/db-status", tags=["System"])
def db_status(request: Request):
    """Shows what data exists in the production database. No auth required."""
    # Show what workspace the current user resolves to
    from core.deps import _get_workspace
    user_workspace = _get_workspace(request)
    conn = get_db()
    try:
        counts = {}
        for table in ["monthly_data", "kpi_targets", "company_settings",
                      "canonical_revenue", "canonical_expenses", "canonical_customers",
                      "canonical_pipeline", "canonical_invoices", "canonical_employees"]:
            try:
                r = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                counts[table] = r[0] if not isinstance(r, dict) else list(r.values())[0]
            except Exception as e:
                counts[table] = f"error: {e}"

        # Latest month
        try:
            r = conn.execute("SELECT year, month FROM monthly_data ORDER BY year DESC, month DESC LIMIT 1").fetchone()
            latest = f"{r[0]}-{r[1]:02d}" if r else "none"
        except Exception:
            latest = "error"

        # Workspace breakdown
        try:
            rows = conn.execute("SELECT workspace_id, COUNT(*) as c FROM monthly_data GROUP BY workspace_id").fetchall()
            workspaces = {str(r[0]): r[1] for r in rows}
        except Exception:
            workspaces = {}

        # Check what workspace the user would get from their JWT
        user_count = 0
        if user_workspace:
            try:
                r = conn.execute("SELECT COUNT(*) FROM monthly_data WHERE workspace_id=?", [user_workspace]).fetchone()
                user_count = r[0] if not isinstance(r, dict) else list(r.values())[0]
            except Exception:
                pass

        # Sample a recent month's data_json to see what KPIs exist
        sample_kpis = []
        sample_raw = ""
        try:
            r = conn.execute(
                "SELECT data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 1",
                [user_workspace or list(workspaces.keys())[0] if workspaces else ""]
            ).fetchone()
            if r:
                raw = r[0] if not isinstance(r, dict) else r["data_json"]
                sample_raw = str(raw)[:500]
                import json as _j
                try:
                    d = _j.loads(raw) if isinstance(raw, str) else {}
                    sample_kpis = list(d.keys())[:20]
                except Exception:
                    sample_kpis = [f"PARSE_ERROR: type={type(raw).__name__}"]
        except Exception as e:
            sample_kpis = [f"QUERY_ERROR: {e}"]

        from core.database import _USE_PG, DATABASE_URL
        return {
            "db_type": "postgresql" if _USE_PG else "sqlite",
            "db_url": DATABASE_URL[:30] + "..." if _USE_PG and DATABASE_URL else "local",
            "user_workspace": user_workspace or "(not authenticated)",
            "user_workspace_months": user_count,
            "latest_month": latest,
            "workspaces": workspaces,
            "table_counts": counts,
            "sample_kpis": sample_kpis,
            "sample_raw": sample_raw,
        }
    finally:
        conn.close()


# ── Global error handler ──────────────────────────────────────────────────────

import traceback as _tb  # noqa: E402
from fastapi import HTTPException as _HTTPException  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402


@app.exception_handler(Exception)
async def generic_error_handler(request: Request, exc: Exception):
    """Catch-all — never expose internal details to clients."""
    print(f"[ERROR] Unhandled on {request.url.path}: {exc}")
    _tb.print_exc()
    if isinstance(exc, _HTTPException):
        return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail})
    return JSONResponse(
        status_code=500,
        content={"detail": "An internal error occurred. Please try again or contact support."}
    )

# ── Ontology discovery shim ───────────────────────────────────────────────────
# The ontology router delegates back here via main._run_ontology_discovery().
# Keep this function in main.py so the shim in routers/ontology.py can find it.

import numpy as np  # noqa: E402
from datetime import datetime as _dt  # noqa: E402

from core.database import get_db as _get_db  # noqa: E402
from core.kpi_defs import KPI_DEFS, ALL_CAUSATION_RULES, EXTENDED_ONTOLOGY_METRICS  # noqa: E402


def _init_ontology_tables(conn):
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ontology_nodes (
            key TEXT PRIMARY KEY,
            name TEXT,
            domain TEXT,
            unit TEXT,
            direction TEXT,
            centrality REAL DEFAULT 0,
            pagerank REAL DEFAULT 0,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS ontology_edges (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT,
            target TEXT,
            relation TEXT,
            strength REAL,
            evidence TEXT,
            direction TEXT DEFAULT 'positive',
            granger_pval REAL,
            granger_lag INTEGER,
            confidence_tier TEXT DEFAULT 'expert_prior',
            UNIQUE(source, target, relation)
        );
        CREATE TABLE IF NOT EXISTS ontology_recommendations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT,
            description TEXT,
            rec_type TEXT,
            path TEXT,
            confidence REAL,
            novelty REAL,
            impact REAL,
            hypothesis TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT
        );
    """)
    conn.commit()
    try:
        conn.execute("ALTER TABLE ontology_edges ADD COLUMN direction TEXT DEFAULT 'positive'")
        conn.commit()
    except Exception:
        pass  # Column already exists


ONTOLOGY_DOMAIN: dict = {
    "revenue_growth": "Revenue", "gross_margin": "Profitability",
    "operating_margin": "Profitability", "ebitda_margin": "Profitability",
    "cash_conv_cycle": "Cash Flow & AR", "dso": "Cash Flow & AR",
    "arr_growth": "Revenue", "nrr": "Retention", "burn_multiple": "Efficiency",
    "opex_ratio": "Efficiency", "contribution_margin": "Profitability",
    "revenue_quality": "Revenue", "cac_payback": "Unit Economics",
    "sales_efficiency": "Growth", "customer_concentration": "Risk",
    "recurring_revenue": "Revenue", "churn_rate": "Retention",
    "operating_leverage": "Efficiency", "pipeline_conversion": "Growth",
    "customer_ltv": "Unit Economics", "pricing_power_index": "Revenue",
    "growth_efficiency": "Efficiency", "revenue_momentum": "Revenue",
    "revenue_fragility": "Risk", "burn_convexity": "Efficiency",
    "margin_volatility": "Risk", "customer_decay_slope": "Retention",
}


def _granger_test(y: list, x: list, max_lag: int = 4) -> tuple:
    from scipy.stats import f as f_dist
    ya = np.array(y, dtype=float)
    xa = np.array(x, dtype=float)
    n  = min(len(ya), len(xa))
    ya, xa = ya[:n], xa[:n]
    best_pval, best_lag = 1.0, 1
    for lag in range(1, max_lag + 1):
        T = n - lag
        if T < lag * 3 + 5:
            continue
        Y        = ya[lag:]
        own_lags = [ya[lag - j : n - j] for j in range(1, lag + 1)]
        x_lags   = [xa[lag - j : n - j] for j in range(1, lag + 1)]
        X_r = np.column_stack([np.ones(T)] + own_lags)
        X_u = np.column_stack([np.ones(T)] + own_lags + x_lags)
        try:
            b_r = np.linalg.lstsq(X_r, Y, rcond=None)[0]
            b_u = np.linalg.lstsq(X_u, Y, rcond=None)[0]
            rss_r = float(np.sum((Y - X_r @ b_r) ** 2))
            rss_u = float(np.sum((Y - X_u @ b_u) ** 2))
            df1, df2 = lag, T - X_u.shape[1]
            if df2 <= 0 or rss_u <= 1e-12:
                continue
            F    = ((rss_r - rss_u) / df1) / (rss_u / df2)
            pval = float(1.0 - f_dist.cdf(max(F, 0.0), df1, df2))
            if pval < best_pval:
                best_pval, best_lag = pval, lag
        except Exception:
            continue
    return round(best_pval, 4), best_lag


def _run_ontology_discovery(workspace_id: str = ""):
    import json as _json
    conn = _get_db()
    _init_ontology_tables(conn)

    now = _dt.utcnow().isoformat()

    # ── 1. Upsert nodes from KPI_DEFS ─────────────────────────────────────
    for kdef in KPI_DEFS:
        key = kdef["key"]
        conn.execute("""
            INSERT INTO ontology_nodes(key, name, domain, unit, direction, updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              name=excluded.name, domain=excluded.domain,
              unit=excluded.unit, direction=excluded.direction,
              updated_at=excluded.updated_at
        """, (key, kdef["name"], ONTOLOGY_DOMAIN.get(key, "other"),
              kdef["unit"], kdef["direction"], now))

    # Upsert extended ontology-only nodes
    for emeta in EXTENDED_ONTOLOGY_METRICS:  # EXTENDED_ONTOLOGY_METRICS is a list, not dict
        ekey = emeta.get("key")
        if not ekey:
            continue
        conn.execute("""
            INSERT INTO ontology_nodes(key, name, domain, unit, direction, updated_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(key) DO UPDATE SET
              name=excluded.name, domain=excluded.domain,
              unit=excluded.unit, direction=excluded.direction,
              updated_at=excluded.updated_at
        """, (ekey, emeta.get("name", ekey), emeta.get("domain", "other"),
              emeta.get("unit", ""), emeta.get("direction", "higher"), now))

    conn.commit()

    # ── 2. Pull monthly data for Granger tests ─────────────────────────────
    if workspace_id:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year, month",
            [workspace_id]
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT year, month, data_json FROM monthly_data ORDER BY year, month"
        ).fetchall()

    time_series: dict = {}
    for r in rows:
        d = _json.loads(r["data_json"])
        for k, v in d.items():
            if v is not None:
                time_series.setdefault(k, []).append(float(v))

    # ── 3. Upsert edges from ALL_CAUSATION_RULES ──────────────────────────
    for source_key, rules in ALL_CAUSATION_RULES.items():
        for target_key in rules.get("downstream_impact", []):
            # Try Granger test if both series have enough data
            granger_pval, granger_lag = 1.0, 1
            y_series = time_series.get(target_key, [])
            x_series = time_series.get(source_key, [])
            if len(y_series) >= 12 and len(x_series) >= 12:
                try:
                    granger_pval, granger_lag = _granger_test(y_series, x_series)
                except Exception:
                    pass

            confidence_tier = "granger_confirmed" if granger_pval < 0.05 else "expert_prior"
            strength = round(1.0 - granger_pval, 3) if granger_pval < 0.05 else 0.5

            conn.execute("""
                INSERT INTO ontology_edges(source, target, relation, strength, evidence,
                                          granger_pval, granger_lag, confidence_tier)
                VALUES (?,?,?,?,?,?,?,?)
                ON CONFLICT(source, target, relation) DO UPDATE SET
                  strength=excluded.strength, evidence=excluded.evidence,
                  granger_pval=excluded.granger_pval, granger_lag=excluded.granger_lag,
                  confidence_tier=excluded.confidence_tier
            """, (source_key, target_key, "CAUSES", strength,
                  f"causation_rules+granger(p={granger_pval})",
                  granger_pval, granger_lag, confidence_tier))

    conn.commit()

    # ── 4. Compute simple PageRank ─────────────────────────────────────────
    node_keys = [r[0] for r in conn.execute("SELECT key FROM ontology_nodes").fetchall()]
    edges     = conn.execute("SELECT source, target, strength FROM ontology_edges").fetchall()

    pr = {k: 1.0 / len(node_keys) for k in node_keys} if node_keys else {}
    adjacency: dict = {}
    out_degree: dict = {}
    for src, tgt, strength in edges:
        adjacency.setdefault(tgt, []).append((src, float(strength or 0.5)))
        out_degree[src] = out_degree.get(src, 0) + 1

    damping = 0.85
    for _ in range(20):
        new_pr = {}
        for k in node_keys:
            rank = (1 - damping) / max(len(node_keys), 1)
            for src, w in adjacency.get(k, []):
                if out_degree.get(src, 0) > 0:
                    rank += damping * pr.get(src, 0) * w / out_degree[src]
            new_pr[k] = rank
        pr = new_pr

    for k, v in pr.items():
        conn.execute("UPDATE ontology_nodes SET pagerank=? WHERE key=?", (round(v, 6), k))
    conn.commit()
    conn.close()
    print(f"[Ontology] Discovery complete: {len(node_keys)} nodes, {len(edges)} edges")


# ── Serve React Frontend ──────────────────────────────────────────────────────

STATIC_DIR = Path(__file__).parent.parent / "frontend" / "dist"
if STATIC_DIR.exists():
    app.mount("/assets", StaticFiles(directory=STATIC_DIR / "assets"), name="assets")

    @app.api_route("/{full_path:path}", methods=["GET", "HEAD"], include_in_schema=False)
    def serve_spa(full_path: str):
        index = STATIC_DIR / "index.html"
        return FileResponse(
            index,
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "Pragma": "no-cache",
                "Expires": "0",
            }
        )
