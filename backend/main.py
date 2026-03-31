"""
main.py — Thin app factory: wires together routers, CORS, middleware, static files.
All business logic lives in routers/ and core/.
"""
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from core.config import _ALLOWED_ORIGINS
from core.database import get_db, _migrate_workspace_data  # noqa: F401 — imported for side-effect init
from core.security import rate_limit_middleware

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
            "style-src 'self' 'unsafe-inline'; "
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
    connectors,
    decisions,
    forecast,
    jobs,
    ontology,
    org,
    scenarios,
    settings,
    upload,
)

for _router_mod in [
    admin, alerts, analytics, annotations, auth, benchmarks, billing,
    connectors, decisions, forecast, jobs, ontology, org, scenarios, settings, upload,
]:
    app.include_router(_router_mod.router)

# ── Health endpoint ───────────────────────────────────────────────────────────

from datetime import datetime  # noqa: E402

@app.get("/api/health", tags=["System"])
def health():
    return {"status": "ok", "timestamp": datetime.utcnow().isoformat()}

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
    for ekey, emeta in EXTENDED_ONTOLOGY_METRICS.items():
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
