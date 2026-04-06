"""
Ramp connector — extracts transactions, spend, and card data.
Auth: OAuth 2.0 client credentials (app-level, no user login required).

Credentials required (Render env vars):
  RAMP_CLIENT_ID      — from Ramp developer portal
  RAMP_CLIENT_SECRET  — from Ramp developer portal
"""
from __future__ import annotations

import logging
import os
import threading
import time
import httpx

from .base import BaseConnector, ConnectorError, _request_with_retry

log = logging.getLogger("connectors.ramp")

_BASE          = "https://api.ramp.com/developer/v1"
_TOKEN_URL     = "https://api.ramp.com/v1/public/customer/token"
_LIMIT         = 100
_CLIENT_ID     = os.environ.get("RAMP_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("RAMP_CLIENT_SECRET", "")

# In-memory token cache (refreshed automatically), guarded by lock
_token_cache: dict = {}
_token_lock = threading.Lock()


def _get_access_token(client_id: str, client_secret: str) -> str:
    """Return a valid access token, refreshing if expired. Thread-safe."""
    cache_key = f"{client_id}:ramp"
    now = time.time()

    with _token_lock:
        cached = _token_cache.get(cache_key)
        if cached and cached["expires_at"] > now + 60:
            return cached["token"]

    # Token missing or expired — fetch a new one outside the lock
    log.info("[Ramp] Fetching new access token for client %s...", client_id[:8])
    with httpx.Client(timeout=15) as client:
        r = _request_with_retry(
            client, "POST", _TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "transactions:read users:read",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    tok = r.json()
    new_entry = {
        "token":      tok["access_token"],
        "expires_at": time.time() + tok.get("expires_in", 3600),
    }

    with _token_lock:
        _token_cache[cache_key] = new_entry

    log.info("[Ramp] Token refreshed successfully")
    return tok["access_token"]


class RampConnector(BaseConnector):
    SOURCE_NAME = "ramp"
    AUTH_TYPE   = "api_key"   # client_credentials flow, treated as API key from UX POV

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            cid = credentials.get("client_id") or _CLIENT_ID
            sec = credentials.get("client_secret") or _CLIENT_SECRET
            token = _get_access_token(cid, sec)
            return bool(token)
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        cid = credentials.get("client_id") or _CLIENT_ID
        sec = credentials.get("client_secret") or _CLIENT_SECRET
        if not cid or not sec:
            raise ConnectorError("Ramp client credentials not configured.")

        log.info("[Ramp] Starting extract for workspace %s", workspace_id)
        token   = _get_access_token(cid, sec)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        transactions = self._fetch_transactions(headers)
        users = self._fetch_users(headers)
        log.info("[Ramp] Extract complete: %d transactions, %d users",
                 len(transactions), len(users))

        return [
            {"entity_type": "expenses",  "records": transactions},
            {"entity_type": "employees", "records": users},
        ]

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_transactions(self, headers: dict) -> list[dict]:
        records: list[dict] = []
        page_token = None
        with httpx.Client(timeout=30) as client:
            for page_num in range(200):
                params: dict = {"page_size": _LIMIT}
                if page_token:
                    params["page_token"] = page_token
                r = _request_with_retry(
                    client, "GET", f"{_BASE}/transactions",
                    headers=headers, params=params,
                )
                data = r.json()
                page_records = data.get("data", [])
                records.extend(page_records)
                page_token = data.get("page", {}).get("next")
                log.info("[Ramp] Transactions page %d: fetched %d (total %d)",
                         page_num + 1, len(page_records), len(records))
                if not page_token:
                    break
        return records

    def _fetch_users(self, headers: dict) -> list[dict]:
        """Fetch all users with cursor-based pagination."""
        records: list[dict] = []
        page_token = None
        with httpx.Client(timeout=20) as client:
            for page_num in range(200):
                params: dict = {"page_size": _LIMIT}
                if page_token:
                    params["page_token"] = page_token
                r = _request_with_retry(
                    client, "GET", f"{_BASE}/users",
                    headers=headers, params=params,
                )
                data = r.json()
                page_records = data.get("data", [])
                records.extend(page_records)
                page_token = data.get("page", {}).get("next")
                log.info("[Ramp] Users page %d: fetched %d (total %d)",
                         page_num + 1, len(page_records), len(records))
                if not page_token:
                    break
        return records
