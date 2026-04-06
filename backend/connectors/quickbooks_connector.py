"""
QuickBooks Online connector — extracts invoices, payments, customers, accounts, employees.
Auth: OAuth2 (Intuit Identity).
"""
from __future__ import annotations

import logging
import os

import httpx

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.quickbooks")

_AUTH_URL  = "https://appcenter.intuit.com/connect/oauth2"
_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
# Use sandbox URL for development keys, production URL for live keys.
# Intuit sandbox realm IDs start with "9341" (sandbox prefix).
_BASE_PROD    = "https://quickbooks.api.intuit.com/v3/company"
_BASE_SANDBOX = "https://sandbox-quickbooks.api.intuit.com/v3/company"
_LIMIT     = 1000   # QB supports up to 1000 per query


def _qb_base(realm_id: str) -> str:
    """Return the correct QB API base URL based on realm ID.
    Sandbox realm IDs are typically prefixed with '9341' or start with numbers
    that indicate sandbox environment."""
    if os.environ.get("QB_SANDBOX", "").lower() in ("1", "true", "yes"):
        return _BASE_SANDBOX
    # Auto-detect: sandbox realms from Intuit developer portal start with specific prefixes
    if realm_id.startswith("9341") or realm_id.startswith("1234"):
        log.info("Detected QuickBooks sandbox realm %s, using sandbox API", realm_id)
        return _BASE_SANDBOX
    return _BASE_PROD


def _qb_client_id() -> str:
    """Return QuickBooks/Intuit client ID with env var fallback."""
    return os.environ.get("INTUIT_CLIENT_ID") or os.environ["QUICKBOOKS_CLIENT_ID"]


def _qb_client_secret() -> str:
    """Return QuickBooks/Intuit client secret with env var fallback."""
    return os.environ.get("INTUIT_CLIENT_SECRET") or os.environ["QUICKBOOKS_CLIENT_SECRET"]


class QuickBooksConnector(BaseConnector):
    SOURCE_NAME  = "quickbooks"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = ["com.intuit.quickbooks.accounting"]

    # ── OAuth ─────────────────────────────────────────────────────────────────

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        client_id = _qb_client_id()
        scopes    = "%20".join(self.OAUTH_SCOPES)
        return (
            f"{_AUTH_URL}?client_id={client_id}"
            f"&response_type=code"
            f"&scope={scopes}"
            f"&redirect_uri={redirect_uri}"
            f"&state={state}"
        )

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        r = httpx.post(
            _TOKEN_URL,
            data={
                "grant_type":   "authorization_code",
                "code":         code,
                "redirect_uri": redirect_uri,
            },
            auth=(_qb_client_id(), _qb_client_secret()),
            timeout=30,
        )
        if r.status_code != 200:
            raise ConnectorError(f"QuickBooks token exchange failed: {r.text}")
        return r.json()

    def refresh_token(self, credentials: dict) -> dict:
        log.info("Refreshing QuickBooks OAuth token")
        r = httpx.post(
            _TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "refresh_token": credentials["refresh_token"],
            },
            auth=(_qb_client_id(), _qb_client_secret()),
            timeout=30,
        )
        if r.status_code != 200:
            raise ConnectorError(f"QuickBooks token refresh failed: {r.text}")
        return {**credentials, **r.json()}

    # ── Extract ───────────────────────────────────────────────────────────────

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        token      = credentials.get("access_token", "")
        realm_id   = credentials.get("realmId", "")
        if not token or not realm_id:
            raise ConnectorError("QuickBooks credentials missing access_token or realmId.")

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept":        "application/json",
        }
        base = f"{_qb_base(realm_id)}/{realm_id}"

        entities = [
            ("invoices",  "Invoice"),
            ("revenue",   "Payment"),
            ("customers", "Customer"),
            ("expenses",  "Purchase"),
            ("employees", "Employee"),
            ("accounts",  "Account"),
        ]

        results: list[dict] = []
        for entity_type, qb_entity in entities:
            try:
                records = self._query(base, headers, qb_entity)
                results.append({"entity_type": entity_type, "records": records})
            except ConnectorAuthError:
                raise  # auth errors must propagate immediately
            except Exception as exc:
                log.error("QuickBooks: failed to fetch %s: %s", qb_entity, exc)
                results.append({
                    "entity_type": entity_type,
                    "records": [],
                    "error": str(exc),
                })
        return results

    # ── Private ───────────────────────────────────────────────────────────────

    def _query(self, base: str, headers: dict, entity: str) -> list[dict]:
        """Run QB SQL query with pagination and retry."""
        records = []
        start   = 1
        with httpx.Client(timeout=30) as client:
            while True:
                sql = f"SELECT * FROM {entity} STARTPOSITION {start} MAXRESULTS {_LIMIT}"
                r = _request_with_retry(
                    client, "GET", f"{base}/query",
                    headers=headers,
                    params={"query": sql, "minorversion": "65"},
                )
                data  = r.json().get("QueryResponse", {})
                batch = data.get(entity, [])
                records.extend(batch)
                if len(batch) < _LIMIT:
                    break
                start += _LIMIT
        return records
