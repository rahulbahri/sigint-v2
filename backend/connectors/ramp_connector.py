"""
Ramp connector — extracts transactions, spend, and card data.
Auth: OAuth 2.0 client credentials (app-level, no user login required).

Credentials required (Render env vars):
  RAMP_CLIENT_ID      — from Ramp developer portal
  RAMP_CLIENT_SECRET  — from Ramp developer portal
"""
from __future__ import annotations

import os
import time
import httpx

from .base import BaseConnector, ConnectorError

_BASE          = "https://api.ramp.com/developer/v1"
_TOKEN_URL     = "https://api.ramp.com/v1/public/customer/token"
_LIMIT         = 100
_CLIENT_ID     = os.environ.get("RAMP_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("RAMP_CLIENT_SECRET", "")

# In-memory token cache (refreshed automatically)
_token_cache: dict = {}


def _get_access_token(client_id: str, client_secret: str) -> str:
    """Return a valid access token, refreshing if expired."""
    now = time.time()
    cached = _token_cache.get(f"{client_id}:ramp")
    if cached and cached["expires_at"] > now + 60:
        return cached["token"]

    with httpx.Client(timeout=15) as client:
        r = client.post(
            _TOKEN_URL,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "transactions:read users:read",
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
    if r.status_code != 200:
        raise ConnectorError(f"Ramp token fetch failed: {r.text}")
    tok = r.json()
    _token_cache[f"{client_id}:ramp"] = {
        "token":      tok["access_token"],
        "expires_at": now + tok.get("expires_in", 3600),
    }
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

        token   = _get_access_token(cid, sec)
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        return [
            {"entity_type": "expenses",  "records": self._fetch_transactions(headers)},
            {"entity_type": "employees", "records": self._fetch_users(headers)},
        ]

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_transactions(self, headers: dict) -> list[dict]:
        records: list[dict] = []
        page_token = None
        with httpx.Client(timeout=30) as client:
            for _ in range(200):
                params: dict = {"page_size": _LIMIT}
                if page_token:
                    params["page_token"] = page_token
                r = client.get(f"{_BASE}/transactions", headers=headers, params=params)
                if r.status_code != 200:
                    raise ConnectorError(f"Ramp transactions error {r.status_code}: {r.text}")
                data = r.json()
                records.extend(data.get("data", []))
                page_token = data.get("page", {}).get("next")
                if not page_token:
                    break
        return records

    def _fetch_users(self, headers: dict) -> list[dict]:
        with httpx.Client(timeout=20) as client:
            r = client.get(f"{_BASE}/users", headers=headers, params={"page_size": _LIMIT})
        if r.status_code != 200:
            return []
        return r.json().get("data", [])
