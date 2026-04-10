"""
routers/connectors.py — ELT connector pipeline, QuickBooks OAuth, data gaps,
canonical data preview, data quality, and KPI coverage (/api/connectors/*, /api/quickbooks/*, etc.).
"""
import json
import re
import secrets
import sys
import threading as _threading
from pathlib import Path
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse

from core.config import APP_URL, QB_CLIENT_ID, QB_CLIENT_SECRET, QB_REDIRECT_URI
from core.database import get_db
from core.deps import _get_workspace
from core.queue import enqueue as _enqueue
from core.state import _ELT_AVAILABLE, _ELT_IMPORT_ERROR

# ── Per-(workspace, source) sync guard — prevents duplicate concurrent syncs ──
_SYNC_LOCK  = _threading.Lock()
_SYNCING: dict = {}  # (workspace_id, source) -> bool

router = APIRouter()

# ─── ELT Connector Imports ────────────────────────────────────────────────────

sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from connectors.base import encrypt_credentials, decrypt_credentials, ConnectorError
    from connectors.stripe_connector        import StripeConnector
    from connectors.hubspot_connector       import HubSpotConnector
    from connectors.quickbooks_connector    import QuickBooksConnector
    from connectors.xero_connector          import XeroConnector
    from connectors.shopify_connector       import ShopifyConnector
    from connectors.salesforce_connector    import SalesforceConnector
    from connectors.google_sheets_connector import GoogleSheetsConnector
    from connectors.brex_connector          import BrexConnector
    from connectors.ramp_connector          import RampConnector
    from connectors.netsuite_connector      import NetSuiteConnector
    from connectors.sage_intacct_connector  import SageIntacctConnector
    from connectors.snowflake_connector     import SnowflakeConnector
    from elt.transformer     import Transformer
    from elt.gap_detector    import GapDetector
    from elt.kpi_aggregator  import aggregate_canonical_to_monthly
    _ELT_OK = True
    print("[ELT] Connector modules loaded OK")
except Exception as _elt_import_err:
    import traceback as _elt_tb
    print(f"[ELT] WARNING: connector import failed — ELT endpoints disabled: {_elt_import_err}")
    _elt_tb.print_exc()
    _ELT_OK = False
    # Stub classes so the rest of the file doesn't crash
    class ConnectorError(Exception): pass
    class Transformer: pass
    class GapDetector:
        def __init__(self, *args, **kwargs): pass
        def run(self): return {"gaps": [], "summary": {"total_gaps": 0, "critical": 0}, "error": "ELT modules unavailable"}
    def encrypt_credentials(c): return ""
    def decrypt_credentials(t): return {}

_CONNECTORS = {} if not _ELT_OK else {
    "stripe":        StripeConnector(),
    "hubspot":       HubSpotConnector(),
    "quickbooks":    QuickBooksConnector(),
    "xero":          XeroConnector(),
    "shopify":       ShopifyConnector(),
    "salesforce":    SalesforceConnector(),
    "google_sheets": GoogleSheetsConnector(),
    "brex":          BrexConnector(),
    "ramp":          RampConnector(),
    "netsuite":      NetSuiteConnector(),
    "sage_intacct":  SageIntacctConnector(),
    "snowflake":     SnowflakeConnector(),
}

_SOURCE_LABELS = {
    "stripe":        "Stripe",
    "hubspot":       "HubSpot",
    "quickbooks":    "QuickBooks",
    "xero":          "Xero",
    "shopify":       "Shopify",
    "salesforce":    "Salesforce",
    "google_sheets": "Google Sheets",
    "brex":          "Brex",
    "ramp":          "Ramp",
    "netsuite":      "NetSuite",
    "sage_intacct":  "Sage Intacct",
    "snowflake":     "Snowflake",
}


# ─── Audit helper (local, delegates to annotations router if available) ────────

def _audit(event_type: str, entity_type: str, entity_id: str, description: str, user: str = "system"):
    try:
        conn = get_db()
        conn.execute(
            'INSERT INTO audit_log (event_type, entity_type, entity_id, description, "user") VALUES (?,?,?,?,?)',
            (event_type, entity_type, entity_id, description, user)
        )
        conn.commit()
        conn.close()
    except Exception:
        pass


# ─── ELT Table Setup ──────────────────────────────────────────────────────────

def _elt_ensure_tables(conn) -> None:
    conn.execute("""
        CREATE TABLE IF NOT EXISTS connector_configs (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id        TEXT NOT NULL,
            source_name         TEXT NOT NULL,
            credentials_enc     TEXT,
            sync_status         TEXT DEFAULT 'pending',
            last_sync_at        TEXT,
            last_error          TEXT,
            created_at          TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, source_name)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS raw_extracts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id    TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            entity_type     TEXT NOT NULL,
            raw_json        TEXT NOT NULL,
            extracted_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            processed       INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS field_mappings (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id    TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            source_field    TEXT NOT NULL,
            canonical_table TEXT NOT NULL,
            canonical_field TEXT NOT NULL,
            confidence      REAL DEFAULT 0,
            confirmed_by_user INTEGER DEFAULT 0,
            is_new          INTEGER DEFAULT 0,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, source_name, source_field, canonical_table)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sync_field_snapshots (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id    TEXT NOT NULL,
            source_name     TEXT NOT NULL,
            entity_type     TEXT NOT NULL,
            source_field    TEXT NOT NULL,
            first_seen_at   TEXT DEFAULT CURRENT_TIMESTAMP,
            last_seen_at    TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(workspace_id, source_name, entity_type, source_field)
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS workspace_notifications (
            id                 INTEGER PRIMARY KEY AUTOINCREMENT,
            workspace_id       TEXT NOT NULL,
            notification_type  TEXT NOT NULL,
            title              TEXT NOT NULL,
            message            TEXT NOT NULL,
            severity           TEXT DEFAULT 'info',
            data_json          TEXT DEFAULT '{}',
            is_read            INTEGER DEFAULT 0,
            is_dismissed       INTEGER DEFAULT 0,
            created_at         TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Schema migrations for new columns on existing tables
    try:
        conn.execute("ALTER TABLE field_mappings ADD COLUMN is_new INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists
    try:
        conn.execute("ALTER TABLE connector_configs ADD COLUMN has_unmapped_critical INTEGER DEFAULT 0")
    except Exception:
        pass  # Column already exists
    conn.commit()


# ─── Unmapped-field notification helper ───────────────────────────────────────

_CORE_KPIS = {"revenue_growth", "arr_growth", "gross_margin", "churn_rate", "nrr", "burn_multiple"}


def _create_unmapped_notification(
    conn,
    workspace_id: str,
    source_name: str,
    entity_groups: list[dict],
) -> None:
    """Create a workspace_notification when new unmapped fields are detected.

    *entity_groups* is a list of ``{"entity_type": str, "fields": [str, ...]}``.
    Uses ``get_kpi_impact_for_field`` to determine severity.
    """
    try:
        from elt.gap_detector import get_kpi_impact_for_field
    except Exception:
        return  # ELT modules unavailable — skip silently

    # Defensive: validate entity_groups format
    if not isinstance(entity_groups, list):
        return
    for grp in entity_groups:
        if not isinstance(grp, dict) or "entity_type" not in grp or "fields" not in grp:
            return

    blocked_kpis: set[str] = set()
    total_fields = 0
    for grp in entity_groups:
        for field in grp["fields"]:
            total_fields += 1
            kpis = get_kpi_impact_for_field(grp["entity_type"], field)
            blocked_kpis.update(kpis)

    severity = "critical" if blocked_kpis & _CORE_KPIS else "warning"
    source_label = _SOURCE_LABELS.get(source_name, source_name)
    data = {
        "source": source_name,
        "entity_groups": entity_groups,
        "blocked_kpis": sorted(blocked_kpis),
    }
    conn.execute(
        "INSERT INTO workspace_notifications "
        "(workspace_id, notification_type, title, message, severity, data_json) "
        "VALUES (?,?,?,?,?,?)",
        [workspace_id, "unmapped_fields",
         f"New unmapped fields from {source_label}",
         f"{total_fields} new field(s) need mapping. "
         f"{len(blocked_kpis)} KPI(s) may be affected.",
         severity, json.dumps(data)],
    )
    # Update connector badge flag
    if severity == "critical":
        conn.execute(
            "UPDATE connector_configs SET has_unmapped_critical=1 "
            "WHERE workspace_id=? AND source_name=?",
            [workspace_id, source_name],
        )
    conn.commit()


# ─── ELT Sync Engine ──────────────────────────────────────────────────────────

def _run_elt_sync(workspace_id: str, source_name: str, credentials: dict) -> dict:
    """Run full ELT for one source. Called in background thread."""
    connector = _CONNECTORS.get(source_name)
    if not connector:
        return {"error": f"Unknown connector: {source_name}"}
    conn = get_db()
    try:
        _elt_ensure_tables(conn)
        # Refresh token if needed
        try:
            credentials = connector.refresh_token(credentials)
        except Exception:
            pass

        bundles = connector.extract(workspace_id, credentials)
        transformer = Transformer(conn, workspace_id, source_name)
        total_upserted = 0
        all_new_unmapped: list[dict] = []
        for bundle in bundles:
            entity_type = bundle["entity_type"]
            records     = bundle["records"]
            # Store raw
            conn.execute(
                "INSERT INTO raw_extracts (workspace_id, source_name, entity_type, raw_json) "
                "VALUES (?,?,?,?)",
                [workspace_id, source_name, entity_type, json.dumps(records)],
            )
            # Transform + upsert canonical
            canonical = transformer.transform(entity_type, records)
            n = transformer.upsert_canonical(entity_type, canonical)
            total_upserted += n
            # Detect new fields + save/update field mappings
            if records:
                new_fields = transformer.detect_new_fields(entity_type, records[0])
                mappings = transformer.save_mappings(entity_type, records[0], new_fields=new_fields)
                # Check if any new fields are unmapped
                if new_fields:
                    unmapped_new = [
                        f for f in new_fields
                        if any(m["source_field"] == f and m["canonical_field"] == "unmapped"
                               for m in mappings)
                    ]
                    if unmapped_new:
                        all_new_unmapped.append({
                            "entity_type": entity_type,
                            "fields": unmapped_new,
                        })

        # Create notifications for unmapped fields (if any)
        if all_new_unmapped:
            _create_unmapped_notification(conn, workspace_id, source_name, all_new_unmapped)

        # Update connector status
        conn.execute(
            "UPDATE connector_configs SET sync_status='ok', last_sync_at=datetime('now'), "
            "last_error=NULL WHERE workspace_id=? AND source_name=?",
            [workspace_id, source_name],
        )
        conn.commit()
        _audit("data_synced", source_name, workspace_id, f"ELT sync: {total_upserted} records")

        # ── Mapping readiness check (soft gate) ─────────────────────────
        mapping_readiness = {"ready": True}
        try:
            from elt.kpi_aggregator import check_mapping_readiness
            mapping_readiness = check_mapping_readiness(conn, workspace_id)
            if not mapping_readiness["ready"]:
                print(f"[ELT] Mapping readiness: NOT READY — {len(mapping_readiness['blocked_kpis'])} "
                      f"KPIs may be affected for workspace={workspace_id!r}")
        except Exception:
            pass  # Non-fatal

        # ── KPI Aggregation: bridge canonical → monthly_data ──────────────
        # Always aggregate (soft gate) — attach warnings to result
        agg_result = {"skipped": "aggregator not available"}
        try:
            agg_conn = get_db()
            agg_result = aggregate_canonical_to_monthly(agg_conn, workspace_id)
            agg_conn.close()
            print(f"[ELT] KPI aggregation: {agg_result.get('months_written', 0)} months "
                  f"for workspace={workspace_id!r}")
        except Exception as agg_exc:
            print(f"[ELT] KPI aggregation failed (non-fatal): {agg_exc}")
            agg_result = {"error": str(agg_exc)}

        result = {
            "synced": True,
            "records_upserted": total_upserted,
            "kpi_aggregation": agg_result,
            "mapping_readiness": mapping_readiness,
        }
        if all_new_unmapped:
            result["new_unmapped_fields"] = all_new_unmapped
        return result
    except ConnectorError as ce:
        conn.execute(
            "UPDATE connector_configs SET sync_status='error', last_error=? "
            "WHERE workspace_id=? AND source_name=?",
            [str(ce), workspace_id, source_name],
        )
        conn.commit()
        return {"error": str(ce)}
    except Exception as ex:
        conn.execute(
            "UPDATE connector_configs SET sync_status='error', last_error=? "
            "WHERE workspace_id=? AND source_name=?",
            [str(ex), workspace_id, source_name],
        )
        conn.commit()
        return {"error": "Sync failed — check logs"}
    finally:
        conn.close()


# ─── QuickBooks OAuth 2.0 ─────────────────────────────────────────────────────

_QB_AUTH_BASE  = "https://appcenter.intuit.com/connect/oauth2"
_QB_TOKEN_URL  = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
_QB_SCOPE      = "com.intuit.quickbooks.accounting"
_QB_API_BASE   = "https://quickbooks.api.intuit.com/v3"


def _qb_redirect() -> str:
    return QB_REDIRECT_URI or f"{APP_URL}/api/quickbooks/callback"


@router.get("/api/quickbooks/auth-url")
async def qb_auth_url(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not QB_CLIENT_ID:
        raise HTTPException(status_code=503, detail="QuickBooks not configured")
    state = secrets.token_urlsafe(16)
    # Store state so callback can validate it (prevents CSRF)
    conn = get_db()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO oauth_states (state, workspace_id, source_name) VALUES (?,?,?)",
            [state, workspace_id, "quickbooks"],
        )
        conn.commit()
    finally:
        conn.close()
    params = urlencode({
        "client_id": QB_CLIENT_ID,
        "scope": _QB_SCOPE,
        "redirect_uri": _qb_redirect(),
        "response_type": "code",
        "state": state,
    })
    return {"auth_url": f"{_QB_AUTH_BASE}?{params}", "state": state}


@router.get("/api/quickbooks/callback")
async def qb_callback(code: str = "", state: str = "", realmId: str = "", error: str = ""):
    import base64
    if error:
        return RedirectResponse(url=f"{APP_URL}?qb_error={error}")
    if not QB_CLIENT_ID or not code:
        raise HTTPException(status_code=400, detail="Missing code or QuickBooks not configured")
    # Validate state to prevent CSRF
    if not state:
        raise HTTPException(status_code=400, detail="Missing OAuth state parameter")
    conn_state = get_db()
    try:
        state_row = conn_state.execute(
            "SELECT workspace_id FROM oauth_states WHERE state=? AND source_name='quickbooks'",
            [state],
        ).fetchone()
        if not state_row:
            raise HTTPException(status_code=400, detail="Invalid or expired OAuth state")
        _qb_workspace_id = state_row["workspace_id"]
        conn_state.execute("DELETE FROM oauth_states WHERE state=?", [state])
        conn_state.commit()
    finally:
        conn_state.close()
    creds = base64.b64encode(f"{QB_CLIENT_ID}:{QB_CLIENT_SECRET}".encode()).decode()
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            _QB_TOKEN_URL,
            headers={
                "Authorization": f"Basic {creds}",
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": _qb_redirect(),
            }
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=400, detail="Failed to exchange QuickBooks token")
    tokens = resp.json()
    conn = get_db()
    try:
        for key, val in [
            ("qb_access_token",  tokens.get("access_token", "")),
            ("qb_refresh_token", tokens.get("refresh_token", "")),
            ("qb_realm_id",      realmId),
            ("qb_token_type",    tokens.get("token_type", "bearer")),
        ]:
            conn.execute(
                "INSERT OR IGNORE INTO company_settings (key, value, workspace_id) VALUES (?,?,?)",
                [key, "", _qb_workspace_id],
            )
            conn.execute(
                "UPDATE company_settings SET value=? WHERE key=? AND workspace_id=?",
                [val, key, _qb_workspace_id],
            )
        conn.commit()
    finally:
        conn.close()
    _audit("integration_connected", "quickbooks", realmId, "QuickBooks account connected")
    return RedirectResponse(url=f"{APP_URL}?qb_connected=true")


@router.get("/api/quickbooks/status")
async def qb_status(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    settings = {r["key"]: r["value"] for r in conn.execute(
        "SELECT key, value FROM company_settings WHERE workspace_id=?", [workspace_id]
    ).fetchall()}
    conn.close()
    return {
        "connected": bool(settings.get("qb_access_token")),
        "realm_id": settings.get("qb_realm_id", ""),
        "configured": bool(QB_CLIENT_ID),
    }


@router.post("/api/quickbooks/sync")
async def qb_sync(request: Request):
    """Pull P&L data from QuickBooks and return summary."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    settings = {r["key"]: r["value"] for r in conn.execute(
        "SELECT key, value FROM company_settings WHERE workspace_id=?", [workspace_id]
    ).fetchall()}
    conn.close()
    access_token = settings.get("qb_access_token", "")
    realm_id = settings.get("qb_realm_id", "")
    if not access_token or not realm_id:
        raise HTTPException(status_code=400, detail="QuickBooks not connected. Please authorize first.")
    async with httpx.AsyncClient(timeout=20) as client:
        resp = await client.get(
            f"{_QB_API_BASE}/company/{realm_id}/reports/ProfitAndLoss",
            headers={
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json",
            },
            params={"date_macro": "Last Fiscal Year", "summarize_column_by": "Month"}
        )
    if resp.status_code == 401:
        raise HTTPException(status_code=401, detail="QuickBooks token expired. Please reconnect.")
    if resp.status_code != 200:
        raise HTTPException(status_code=400, detail=f"QuickBooks API error: {resp.status_code}")
    _audit("data_synced", "quickbooks", realm_id, "QuickBooks P&L data synced")
    return {"synced": True, "data": resp.json()}


# ─── List all connectors + status ─────────────────────────────────────────────

@router.get("/api/connectors", tags=["ELT"])
async def list_connectors(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    _elt_ensure_tables(conn)
    rows = conn.execute(
        "SELECT source_name, sync_status, last_sync_at, last_error, has_unmapped_critical "
        "FROM connector_configs WHERE workspace_id=?",
        [workspace_id],
    ).fetchall()
    conn.close()
    connected = {}
    for r in rows:
        connected[r[0]] = {
            "source_name":           r[0],
            "label":                 _SOURCE_LABELS.get(r[0], r[0].title()),
            "status":                r[1],
            "last_sync_at":          r[2],
            "last_error":            r[3],
            "has_unmapped_critical": bool(r[4]) if len(r) > 4 else False,
            "connected":             True,
        }

    all_sources = []
    for key, label in _SOURCE_LABELS.items():
        if key in connected:
            all_sources.append(connected[key])
        else:
            all_sources.append({
                "source_name": key,
                "label":       label,
                "status":      "not_connected",
                "last_sync_at": None,
                "last_error":   None,
                "connected":   False,
            })
    return {"connectors": all_sources}


# ─── Stripe: connect with API key ─────────────────────────────────────────────

@router.post("/api/connectors/stripe/connect", tags=["ELT"])
async def stripe_connect(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    body = await request.json()
    api_key = body.get("api_key", "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="api_key is required")
    connector = _CONNECTORS["stripe"]
    if not connector.validate_credentials({"api_key": api_key}):
        raise HTTPException(status_code=400, detail="Invalid Stripe API key — validation failed")
    enc = encrypt_credentials({"api_key": api_key})
    conn = get_db()
    _elt_ensure_tables(conn)
    conn.execute(
        "INSERT INTO connector_configs (workspace_id, source_name, credentials_enc, sync_status) "
        "VALUES (?,?,?,'connected') "
        "ON CONFLICT(workspace_id, source_name) DO UPDATE SET "
        "credentials_enc=excluded.credentials_enc, sync_status='connected', last_error=NULL",
        [workspace_id, "stripe", enc],
    )
    conn.commit()
    conn.close()
    _audit("integration_connected", "stripe", workspace_id, "Stripe API key connected")
    return {"connected": True}


@router.post("/api/connectors/hubspot/connect", tags=["ELT"])
async def hubspot_connect(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    body = await request.json()
    api_key = body.get("api_key", "").strip()
    if not api_key:
        raise HTTPException(status_code=400, detail="api_key is required")
    connector = _CONNECTORS["hubspot"]
    if not connector.validate_credentials({"api_key": api_key}):
        raise HTTPException(status_code=400, detail="Invalid HubSpot token — validation failed. Check scopes include contacts, deals, and companies.")
    enc = encrypt_credentials({"api_key": api_key})
    conn = get_db()
    _elt_ensure_tables(conn)
    conn.execute(
        "INSERT INTO connector_configs (workspace_id, source_name, credentials_enc, sync_status) "
        "VALUES (?,?,?,'connected') "
        "ON CONFLICT(workspace_id, source_name) DO UPDATE SET "
        "credentials_enc=excluded.credentials_enc, sync_status='connected', last_error=NULL",
        [workspace_id, "hubspot", enc],
    )
    conn.commit()
    conn.close()
    _audit("integration_connected", "hubspot", workspace_id, "HubSpot private app token connected")
    return {"connected": True}


# ─── OAuth: get auth URL for OAuth2 sources ───────────────────────────────────

@router.get("/api/connectors/{source}/auth-url", tags=["ELT"])
async def connector_auth_url(source: str, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    connector = _CONNECTORS.get(source)
    if not connector or connector.AUTH_TYPE != "oauth2":
        raise HTTPException(status_code=404, detail=f"OAuth connector '{source}' not found")
    state        = secrets.token_urlsafe(16)
    redirect_uri = f"{APP_URL}/api/connectors/{source}/callback"
    try:
        auth_url = connector.get_auth_url(redirect_uri, state)
    except (NotImplementedError, KeyError) as exc:
        raise HTTPException(status_code=503, detail=f"{source} not configured: {exc}")
    # Store state→workspace mapping briefly in DB
    conn = get_db()
    _elt_ensure_tables(conn)
    conn.execute(
        "INSERT OR REPLACE INTO connector_configs "
        "(workspace_id, source_name, sync_status) VALUES (?,?,'pending') "
        "ON CONFLICT(workspace_id, source_name) DO UPDATE SET sync_status='pending'",
        [workspace_id, source],
    )
    conn.commit()
    # Stash state in a temp table so callback can look up workspace
    conn.execute("""
        CREATE TABLE IF NOT EXISTS oauth_states (
            state TEXT PRIMARY KEY,
            workspace_id TEXT,
            source_name TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.execute(
        "INSERT OR REPLACE INTO oauth_states (state, workspace_id, source_name) VALUES (?,?,?)",
        [state, workspace_id, source],
    )
    conn.commit()
    conn.close()
    return {"auth_url": auth_url, "state": state}


# ─── OAuth callback (shared for all OAuth2 connectors) ────────────────────────

@router.get("/api/connectors/{source}/callback", tags=["ELT"])
async def connector_oauth_callback(
    source: str,
    request: Request,
    code: str = "",
    state: str = "",
    error: str = "",
    shop: str = "",        # Shopify
    realmId: str = "",     # QuickBooks
):
    if error:
        return RedirectResponse(url=f"{APP_URL}?connector_error={source}:{error}")
    connector = _CONNECTORS.get(source)
    if not connector:
        raise HTTPException(status_code=404, detail=f"Unknown connector: {source}")

    # Look up workspace from state
    conn = get_db()
    state_row = conn.execute(
        "SELECT workspace_id FROM oauth_states WHERE state=?", [state]
    ).fetchone()
    if not state_row:
        conn.close()
        return RedirectResponse(url=f"{APP_URL}?connector_error={source}:invalid_state")
    workspace_id = state_row[0]
    conn.execute("DELETE FROM oauth_states WHERE state=?", [state])
    conn.commit()

    # Build redirect_uri from the actual request URL (not APP_URL) so it matches
    # what was used in the authorization request. Xero/Salesforce reject mismatches.
    _scheme = request.headers.get("x-forwarded-proto", request.url.scheme)
    _host   = request.headers.get("host", request.url.netloc)
    redirect_uri = f"{_scheme}://{_host}/api/connectors/{source}/callback"
    try:
        if source == "shopify":
            shop_domain = shop or state.split("|")[-1]
            credentials = connector.exchange_code_for_shop(code, shop_domain)
        elif source == "quickbooks":
            credentials = connector.exchange_code(code, redirect_uri)
            credentials["realmId"] = realmId
        elif source == "salesforce":
            # PKCE: retrieve code_verifier stored during auth-url generation
            _cv_row = conn.execute(
                "SELECT value FROM company_settings WHERE key='sf_code_verifier' AND workspace_id=?",
                [workspace_id],
            ).fetchone()
            _code_verifier = (_cv_row[0] if _cv_row and not isinstance(_cv_row, dict)
                              else _cv_row["value"] if _cv_row else "")
            credentials = connector.exchange_code(code, redirect_uri, code_verifier=_code_verifier)
            # Clean up the temporary verifier
            conn.execute("DELETE FROM company_settings WHERE key='sf_code_verifier' AND workspace_id=?",
                         [workspace_id])
            conn.commit()
        else:
            credentials = connector.exchange_code(code, redirect_uri)
    except ConnectorError as ce:
        conn.execute(
            "UPDATE connector_configs SET sync_status='error', last_error=? "
            "WHERE workspace_id=? AND source_name=?",
            [str(ce), workspace_id, source],
        )
        conn.commit()
        conn.close()
        return RedirectResponse(url=f"{APP_URL}?connector_error={source}:token_exchange_failed")

    enc = encrypt_credentials(credentials)
    conn.execute(
        "INSERT INTO connector_configs (workspace_id, source_name, credentials_enc, sync_status) "
        "VALUES (?,?,?,'connected') "
        "ON CONFLICT(workspace_id, source_name) DO UPDATE SET "
        "credentials_enc=excluded.credentials_enc, sync_status='connected', last_error=NULL",
        [workspace_id, source, enc],
    )
    conn.commit()
    conn.close()
    _audit("integration_connected", source, workspace_id, f"{source.title()} OAuth connected")

    # Kick off initial sync in background (skip if already running)
    row = get_db().execute(
        "SELECT credentials_enc FROM connector_configs WHERE workspace_id=? AND source_name=?",
        [workspace_id, source],
    ).fetchone()
    if row:
        creds = decrypt_credentials(row[0])
        _key = (workspace_id, source)
        with _SYNC_LOCK:
            if not _SYNCING.get(_key):
                _SYNCING[_key] = True
                def _bg_sync(_k=_key, _w=workspace_id, _s=source, _c=creds):
                    try:
                        _run_elt_sync(_w, _s, _c)
                    finally:
                        with _SYNC_LOCK:
                            _SYNCING.pop(_k, None)
                _enqueue(_bg_sync)

    return RedirectResponse(url=f"{APP_URL}?connector_connected={source}")


# ─── Disconnect a connector ───────────────────────────────────────────────────

@router.delete("/api/connectors/{source}", tags=["ELT"])
async def disconnect_connector(source: str, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    conn.execute(
        "DELETE FROM connector_configs WHERE workspace_id=? AND source_name=?",
        [workspace_id, source],
    )
    conn.commit()
    conn.close()
    _audit("integration_disconnected", source, workspace_id, f"{source.title()} disconnected")
    return {"disconnected": True}


# ─── Trigger manual sync ──────────────────────────────────────────────────────

@router.post("/api/connectors/{source}/sync", tags=["ELT"])
async def sync_connector(source: str, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    row = conn.execute(
        "SELECT credentials_enc FROM connector_configs WHERE workspace_id=? AND source_name=?",
        [workspace_id, source],
    ).fetchone()
    conn.close()
    if not row:
        raise HTTPException(status_code=404, detail=f"{source} not connected")
    credentials = decrypt_credentials(row[0])
    _key = (workspace_id, source)
    with _SYNC_LOCK:
        if _SYNCING.get(_key):
            return {"message": f"Sync already in progress for {source}"}
        _SYNCING[_key] = True
    def _bg_sync(_k=_key, _w=workspace_id, _s=source, _c=credentials):
        try:
            _run_elt_sync(_w, _s, _c)
        finally:
            with _SYNC_LOCK:
                _SYNCING.pop(_k, None)
    _enqueue(_bg_sync)
    return {"message": f"Sync started for {source}"}


# ─── Recompute KPIs from canonical data ──────────────────────────────────────

@router.post("/api/connectors/compute-kpis", tags=["ELT"])
async def compute_kpis_from_canonical(request: Request):
    """
    Manually trigger KPI aggregation from canonical tables into monthly_data.
    Useful after field mapping changes or manual corrections.
    """
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    if not _ELT_OK:
        raise HTTPException(status_code=503, detail="ELT modules unavailable")
    conn = get_db()
    try:
        result = aggregate_canonical_to_monthly(conn, workspace_id)
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {exc}")
    finally:
        conn.close()


# ─── Field mappings: list + confirm ───────────────────────────────────────────

@router.get("/api/connectors/mappings", tags=["ELT"])
async def list_field_mappings(request: Request, source: str = "", needs_review: bool = False):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    sql  = "SELECT * FROM field_mappings WHERE workspace_id=?"
    args = [workspace_id]
    if source:
        sql  += " AND source_name=?"
        args.append(source)
    if needs_review:
        sql  += " AND confirmed_by_user=0 AND confidence < 0.85"
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    cols = ["id","workspace_id","source_name","source_field","canonical_table",
            "canonical_field","confidence","confirmed_by_user","is_new","created_at"]
    result = []
    for r in rows:
        d = {}
        for i, c in enumerate(cols):
            d[c] = r[i] if i < len(r) else (0 if c == "is_new" else None)
        result.append(d)
    return {"mappings": result}


@router.put("/api/connectors/mappings/mark-reviewed", tags=["ELT"])
async def mark_fields_reviewed(request: Request, source: str = ""):
    """Clear is_new flags after user has reviewed the staging view."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    sql  = "UPDATE field_mappings SET is_new=0 WHERE workspace_id=? AND is_new=1"
    args: list = [workspace_id]
    if source:
        sql  += " AND source_name=?"
        args.append(source)
    conn.execute(sql, args)
    conn.commit()
    conn.close()
    return {"cleared": True}


@router.put("/api/connectors/mappings/{mapping_id}", tags=["ELT"])
async def confirm_field_mapping(mapping_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    body = await request.json()
    canonical_field = body.get("canonical_field", "")
    if not canonical_field:
        raise HTTPException(status_code=400, detail="canonical_field required")
    conn = get_db()
    conn.execute(
        "UPDATE field_mappings SET canonical_field=?, confirmed_by_user=1, confidence=1.0, is_new=0 "
        "WHERE id=? AND workspace_id=?",
        [canonical_field, mapping_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"updated": True}


# ─── Staging view (grouped by source → entity → fields with KPI impact) ─────

@router.get("/api/connectors/mappings/staging", tags=["ELT"])
async def get_staging_view(request: Request, source: str = ""):
    """Return field mappings grouped by source and entity_type with KPI impact."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")

    try:
        from elt.gap_detector import get_kpi_impact_for_field
    except Exception:
        get_kpi_impact_for_field = lambda t, f: []  # noqa: E731

    conn = get_db()
    sql = "SELECT * FROM field_mappings WHERE workspace_id=?"
    args: list = [workspace_id]
    if source:
        sql += " AND source_name=?"
        args.append(source)
    rows = conn.execute(sql, args).fetchall()
    conn.close()

    cols = ["id", "workspace_id", "source_name", "source_field", "canonical_table",
            "canonical_field", "confidence", "confirmed_by_user", "is_new", "created_at"]

    sources: dict = {}
    total_unmapped = 0
    total_new = 0
    critical_unmapped = 0

    for r in rows:
        d = {}
        for i, c in enumerate(cols):
            d[c] = r[i] if i < len(r) else (0 if c == "is_new" else None)
        src = d["source_name"]
        entity = d["canonical_table"]
        if src not in sources:
            sources[src] = {}
        if entity not in sources[src]:
            sources[src][entity] = {"fields": [], "new_field_count": 0, "unmapped_count": 0}

        kpi_impact = get_kpi_impact_for_field(entity, d["canonical_field"]) if d["canonical_field"] != "unmapped" else []
        is_new = bool(d.get("is_new", 0))
        is_unmapped = d["canonical_field"] == "unmapped"

        sources[src][entity]["fields"].append({
            "id":               d["id"],
            "source_field":     d["source_field"],
            "canonical_field":  d["canonical_field"],
            "canonical_table":  entity,
            "confidence":       d["confidence"],
            "confirmed_by_user": bool(d["confirmed_by_user"]),
            "is_new":           is_new,
            "kpi_impact":       kpi_impact,
        })
        if is_new:
            total_new += 1
            sources[src][entity]["new_field_count"] += 1
        if is_unmapped:
            total_unmapped += 1
            sources[src][entity]["unmapped_count"] += 1
            # Check if unmapped field blocks a core KPI
            possible_impact = get_kpi_impact_for_field(entity, d["source_field"])
            if any(k in _CORE_KPIS for k in possible_impact):
                critical_unmapped += 1

    # Mapping quality score
    all_mappings = [f for s in sources.values() for e in s.values() for f in e["fields"]]
    total = len(all_mappings)
    low_conf = sum(1 for f in all_mappings if not f["confirmed_by_user"] and f["confidence"] < 0.80)
    unmapped = sum(1 for f in all_mappings if f["canonical_field"] == "unmapped")
    quality_score = max(0, round(100 - (low_conf * 5) - (unmapped * 10))) if total else 100
    quality_label = "high" if quality_score >= 80 else "moderate" if quality_score >= 50 else "low"
    issues = []
    if low_conf:
        issues.append(f"{low_conf} field(s) below 80% confidence")
    if unmapped:
        issues.append(f"{unmapped} field(s) unmapped")

    return {
        "sources": sources,
        "total_unmapped": total_unmapped,
        "total_new": total_new,
        "critical_unmapped": critical_unmapped,
        "mapping_quality": {"score": quality_score, "label": quality_label, "issues": issues},
    }


# ─── Bulk confirm + mark reviewed ───────────────────────────────────────────

@router.post("/api/connectors/mappings/bulk-confirm", tags=["ELT"])
async def bulk_confirm_mappings(request: Request):
    """Confirm multiple mappings at once, then recompute KPIs + run integrity."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    body = await request.json()
    items = body.get("mappings", [])
    if not items:
        raise HTTPException(status_code=400, detail="mappings list required")

    conn = get_db()
    updated = 0
    for item in items:
        mid = item.get("id")
        cf  = item.get("canonical_field", "")
        if not mid or not cf:
            continue
        conn.execute(
            "UPDATE field_mappings SET canonical_field=?, confirmed_by_user=1, "
            "confidence=1.0, is_new=0 WHERE id=? AND workspace_id=?",
            [cf, mid, workspace_id],
        )
        updated += 1
    conn.commit()

    # Clear the connector badge if no more critical unmapped
    _refresh_unmapped_badge(conn, workspace_id)

    # Dismiss related unmapped-field notifications
    conn.execute(
        "UPDATE workspace_notifications SET is_dismissed=1 "
        "WHERE workspace_id=? AND notification_type='unmapped_fields' AND is_dismissed=0",
        [workspace_id],
    )
    conn.commit()

    # Recompute KPIs
    agg_result = {}
    try:
        agg_result = aggregate_canonical_to_monthly(conn, workspace_id)
    except Exception as e:
        agg_result = {"error": str(e)}

    # Run integrity check with auto-correction
    integrity_result = {}
    try:
        from core.integrity import DataIntegrityValidator
        validator = DataIntegrityValidator(conn, workspace_id)
        integrity_result = validator.run_all(trigger="mapping_change", auto_correct=True)
    except Exception as e:
        integrity_result = {"error": str(e)}

    conn.close()
    return {
        "updated": updated,
        "kpi_aggregation": agg_result,
        "integrity": integrity_result,
    }


def _refresh_unmapped_badge(conn, workspace_id: str) -> None:
    """Recheck whether any connector still has critical unmapped fields."""
    try:
        from elt.gap_detector import get_kpi_impact_for_field
    except Exception:
        return
    # Reset all badges for this workspace
    conn.execute(
        "UPDATE connector_configs SET has_unmapped_critical=0 WHERE workspace_id=?",
        [workspace_id],
    )
    # Find connectors that still have unmapped fields blocking core KPIs
    rows = conn.execute(
        "SELECT source_name, canonical_table, source_field FROM field_mappings "
        "WHERE workspace_id=? AND canonical_field='unmapped'",
        [workspace_id],
    ).fetchall()
    flagged_sources: set[str] = set()
    for r in rows:
        src, entity, src_field = r[0], r[1], r[2]
        kpis = get_kpi_impact_for_field(entity, src_field)
        if any(k in _CORE_KPIS for k in kpis):
            flagged_sources.add(src)
    for src in flagged_sources:
        conn.execute(
            "UPDATE connector_configs SET has_unmapped_critical=1 "
            "WHERE workspace_id=? AND source_name=?",
            [workspace_id, src],
        )
    conn.commit()


# ─── Notifications ───────────────────────────────────────────────────────────

@router.get("/api/notifications", tags=["Notifications"])
async def list_notifications(request: Request, unread_only: bool = True):
    """List workspace notifications, optionally only unread/undismissed ones."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    _elt_ensure_tables(conn)
    sql  = "SELECT * FROM workspace_notifications WHERE workspace_id=?"
    args: list = [workspace_id]
    if unread_only:
        sql += " AND is_dismissed=0"
    sql += " ORDER BY created_at DESC LIMIT 50"
    rows = conn.execute(sql, args).fetchall()
    conn.close()
    cols = ["id", "workspace_id", "notification_type", "title", "message",
            "severity", "data_json", "is_read", "is_dismissed", "created_at"]
    result = []
    for r in rows:
        d = {}
        for i, c in enumerate(cols):
            d[c] = r[i] if i < len(r) else None
        # Parse data_json
        try:
            d["data"] = json.loads(d.get("data_json") or "{}")
        except Exception:
            d["data"] = {}
        result.append(d)
    return {"notifications": result}


@router.put("/api/notifications/{notification_id}/dismiss", tags=["Notifications"])
async def dismiss_notification(notification_id: int, request: Request):
    """Dismiss (soft-delete) a notification."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    conn.execute(
        "UPDATE workspace_notifications SET is_dismissed=1 "
        "WHERE id=? AND workspace_id=?",
        [notification_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"dismissed": True}


# ─── Data gaps ────────────────────────────────────────────────────────────────

@router.get("/api/data-gaps", tags=["ELT"])
async def get_data_gaps(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    detector = GapDetector(conn, workspace_id)
    report   = detector.run()
    conn.close()
    return report.to_dict()


# ─── Canonical data preview ───────────────────────────────────────────────────

_CANONICAL_ENTITY_ALLOWLIST = frozenset({
    "revenue", "customers", "expenses", "payroll", "invoices",
    "subscriptions", "churn", "pipeline", "contacts", "deals",
})

@router.get("/api/canonical/{entity_type}", tags=["ELT"])
async def get_canonical_data(entity_type: str, request: Request,
                             limit: int = 100, source: str = ""):
    """Get canonical records, optionally filtered by source (e.g. ?source=quickbooks)."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    if entity_type not in _CANONICAL_ENTITY_ALLOWLIST:
        raise HTTPException(status_code=400, detail=f"Unknown entity type '{entity_type}'")
    table = f"canonical_{entity_type}"
    conn        = get_db()
    try:
        if source:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE workspace_id=? AND source=? LIMIT ?",
                [workspace_id, source, limit],
            ).fetchall()
            cnt_row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table} WHERE workspace_id=? AND source=?",
                [workspace_id, source],
            ).fetchone()
            total = cnt_row["cnt"] if cnt_row else 0
        else:
            rows = conn.execute(
                f"SELECT * FROM {table} WHERE workspace_id=? LIMIT ?",
                [workspace_id, limit],
            ).fetchall()
            cnt_row = conn.execute(
                f"SELECT COUNT(*) as cnt FROM {table} WHERE workspace_id=?", [workspace_id]
            ).fetchone()
            total = cnt_row["cnt"] if cnt_row else 0
        if not rows:
            return {"records": [], "total": 0, "source_filter": source or "all"}
        cols    = [d[0] for d in conn.execute(f"PRAGMA table_info({table})").fetchall()]
        records = [dict(zip(cols, r)) for r in rows]
        return {"records": records, "total": total, "source_filter": source or "all"}
    except Exception:
        return {"records": [], "total": 0, "source_filter": source or "all"}
    finally:
        conn.close()


@router.get("/api/connectors/data-summary", tags=["ELT"])
async def connector_data_summary(request: Request):
    """Summary of all canonical data broken down by source connector."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Unauthorised")
    conn = get_db()
    try:
        summary = {}
        for entity in _CANONICAL_ENTITY_ALLOWLIST:
            table = f"canonical_{entity}"
            try:
                rows = conn.execute(
                    f"SELECT source, COUNT(*) as cnt FROM {table} "
                    f"WHERE workspace_id=? GROUP BY source",
                    [workspace_id],
                ).fetchall()
                for r in rows:
                    src = r[0] if not isinstance(r, dict) else r["source"]
                    cnt = r[1] if not isinstance(r, dict) else r["cnt"]
                    if src not in summary:
                        summary[src] = {"source": src, "entities": {}, "total_records": 0}
                    summary[src]["entities"][entity] = cnt
                    summary[src]["total_records"] += cnt
            except Exception:
                pass
        return {"sources": list(summary.values())}
    finally:
        conn.close()


# ─── Data Quality ─────────────────────────────────────────────────────────────

@router.get("/api/data-quality", tags=["ELT"])
async def get_data_quality(request: Request):
    """Scan canonical tables and return data quality issues for this workspace."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    conn = get_db()
    issues = []
    summary = {"total_issues": 0, "critical": 0, "warning": 0, "tables_scanned": 0}

    canonical_tables = [
        "canonical_revenue", "canonical_customers", "canonical_pipeline",
        "canonical_employees", "canonical_expenses", "canonical_products",
        "canonical_invoices", "canonical_marketing"
    ]

    for table in canonical_tables:
        # Check if table exists
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", [table]
        ).fetchone()
        if not exists:
            continue

        summary["tables_scanned"] += 1
        total = conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE workspace_id=?", [workspace_id]
        ).fetchone()[0]
        if total == 0:
            continue

        entity = table.replace("canonical_", "")

        # Check for records with null amount
        if entity in ("revenue", "expenses", "invoices"):
            null_amount = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE workspace_id=? AND (amount IS NULL OR amount='')",
                [workspace_id]
            ).fetchone()[0]
            if null_amount > 0:
                sev = "critical" if null_amount / total > 0.3 else "warning"
                issues.append({
                    "table": table, "issue": "missing_amount",
                    "severity": sev,
                    "count": null_amount, "total": total,
                    "description": f"{null_amount} of {total} {entity} records have no amount",
                    "fix": f"Ensure all {entity} records have an amount value in your source system"
                })
                summary[sev] += 1
                summary["total_issues"] += 1

        # Check for records with no period/date
        period_col = "period" if entity in ("revenue", "expenses") else "created_at"
        has_col = conn.execute(f"PRAGMA table_info({table})").fetchall()
        col_names = [r[1] for r in has_col]
        if period_col in col_names:
            null_period = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE workspace_id=? AND ({period_col} IS NULL OR {period_col}='')",
                [workspace_id]
            ).fetchone()[0]
            if null_period > 0:
                sev = "critical" if null_period / total > 0.3 else "warning"
                issues.append({
                    "table": table, "issue": "missing_period",
                    "severity": sev,
                    "count": null_period, "total": total,
                    "description": f"{null_period} of {total} {entity} records have no date/period",
                    "fix": "Use the transaction date as the period or re-sync with your source"
                })
                summary[sev] += 1
                summary["total_issues"] += 1

        # Check for negative amounts (potential untagged refunds)
        if entity in ("revenue", "invoices") and "amount" in col_names:
            negative = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE workspace_id=? AND amount < 0",
                [workspace_id]
            ).fetchone()[0]
            if negative > 0:
                issues.append({
                    "table": table, "issue": "negative_amount",
                    "severity": "warning",
                    "count": negative, "total": total,
                    "description": f"{negative} {entity} records have negative amounts (possible refunds)",
                    "fix": "Tag these as refunds in your source system or they will reduce revenue totals"
                })
                summary["warning"] += 1
                summary["total_issues"] += 1

        # Check for records with no customer link
        if "customer_id" in col_names and entity == "revenue":
            no_customer = conn.execute(
                f"SELECT COUNT(*) FROM {table} WHERE workspace_id=? AND (customer_id IS NULL OR customer_id='')",
                [workspace_id]
            ).fetchone()[0]
            if no_customer > 0:
                sev = "critical" if no_customer / total > 0.5 else "warning"
                issues.append({
                    "table": table, "issue": "no_customer_link",
                    "severity": sev,
                    "count": no_customer, "total": total,
                    "description": f"{no_customer} revenue records have no linked customer",
                    "fix": "Ensure customers are linked to invoices in your accounting system"
                })
                summary[sev] += 1
                summary["total_issues"] += 1

        # Check for duplicates by source_id
        if "source_id" in col_names:
            dup_query = conn.execute(
                f"SELECT COUNT(*) FROM (SELECT source_id, COUNT(*) as c FROM {table} "
                f"WHERE workspace_id=? AND source_id IS NOT NULL AND source_id!='' "
                f"GROUP BY source_id HAVING c > 1)",
                [workspace_id]
            ).fetchone()[0]
            if dup_query > 0:
                issues.append({
                    "table": table, "issue": "duplicates",
                    "severity": "warning",
                    "count": dup_query, "total": total,
                    "description": f"{dup_query} duplicate {entity} records detected",
                    "fix": "Re-sync from source — duplicates may have been created by multiple syncs"
                })
                summary["warning"] += 1
                summary["total_issues"] += 1

    conn.close()

    # Sort: critical first
    issues.sort(key=lambda x: 0 if x["severity"] == "critical" else 1)

    return {"summary": summary, "issues": issues}


# ─── KPI Coverage Score ───────────────────────────────────────────────────────

_SOURCE_KPI_MAP: dict = {
    "stripe": {
        "kpis":    ["arr_growth","nrr","churn_rate","expansion_rate","ltv_cac",
                    "cac_payback","recurring_revenue","revenue_quality","logo_retention"],
        "domains": ["Revenue", "Retention", "Unit Economics"],
    },
    "quickbooks": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","contribution_margin"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "xero": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","contribution_margin"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "shopify": {
        "kpis":    ["revenue_growth","customer_concentration","revenue_momentum",
                    "revenue_fragility"],
        "domains": ["Revenue", "Risk"],
    },
    "hubspot": {
        "kpis":    ["health_score","logo_retention","expansion_rate","cpl","mql_sql_rate"],
        "domains": ["Retention", "Growth"],
    },
    "salesforce": {
        "kpis":    ["pipeline_conversion","win_rate","quota_attainment",
                    "sales_efficiency","headcount_eff"],
        "domains": ["Growth", "Efficiency"],
    },
    "google_sheets": {
        "kpis":    ["revenue_growth","gross_margin","operating_margin","dso",
                    "churn_rate","nrr","arr_growth","burn_multiple"],
        "domains": ["Revenue", "Profitability"],
    },
    "brex": {
        "kpis":    ["opex_ratio","burn_multiple","contribution_margin",
                    "headcount_eff","rev_per_employee"],
        "domains": ["Efficiency", "Profitability"],
    },
    "ramp": {
        "kpis":    ["opex_ratio","burn_multiple","contribution_margin",
                    "headcount_eff","rev_per_employee"],
        "domains": ["Efficiency", "Profitability"],
    },
    "netsuite": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","billable_utilization"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "sage_intacct": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period"],
        "domains": ["Profitability", "Cash Flow & AR"],
    },
    "snowflake": {
        "kpis":    ["revenue_growth","gross_margin","operating_margin","dso",
                    "churn_rate","nrr","arr_growth","burn_multiple","ltv_cac"],
        "domains": ["Revenue", "Profitability", "Retention"],
    },
}

_TOTAL_KPIS = 57

