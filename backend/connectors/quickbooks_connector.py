"""
QuickBooks Online connector — extracts invoices, payments, customers, accounts, employees.
Auth: OAuth2 (Intuit Identity).
"""
from __future__ import annotations

import os
import httpx

from .base import BaseConnector, ConnectorError

_AUTH_URL  = "https://appcenter.intuit.com/connect/oauth2"
_TOKEN_URL = "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer"
_BASE      = "https://quickbooks.api.intuit.com/v3/company"
_LIMIT     = 1000   # QB supports up to 1000 per query


class QuickBooksConnector(BaseConnector):
    SOURCE_NAME  = "quickbooks"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = ["com.intuit.quickbooks.accounting"]

    # ── OAuth ─────────────────────────────────────────────────────────────────

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        client_id = os.environ["QUICKBOOKS_CLIENT_ID"]
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
            auth=(os.environ["QUICKBOOKS_CLIENT_ID"], os.environ["QUICKBOOKS_CLIENT_SECRET"]),
            timeout=30,
        )
        if r.status_code != 200:
            raise ConnectorError(f"QuickBooks token exchange failed: {r.text}")
        return r.json()

    def refresh_token(self, credentials: dict) -> dict:
        r = httpx.post(
            _TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "refresh_token": credentials["refresh_token"],
            },
            auth=(os.environ["QUICKBOOKS_CLIENT_ID"], os.environ["QUICKBOOKS_CLIENT_SECRET"]),
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
        base = f"{_BASE}/{realm_id}"

        return [
            {"entity_type": "invoices",  "records": self._query(base, headers, "Invoice")},
            {"entity_type": "revenue",   "records": self._query(base, headers, "Payment")},
            {"entity_type": "customers", "records": self._query(base, headers, "Customer")},
            {"entity_type": "expenses",  "records": self._query(base, headers, "Purchase")},
            {"entity_type": "employees", "records": self._query(base, headers, "Employee")},
            {"entity_type": "accounts",  "records": self._query(base, headers, "Account")},
        ]

    # ── Private ───────────────────────────────────────────────────────────────

    def _query(self, base: str, headers: dict, entity: str) -> list[dict]:
        """Run QB SQL query with pagination."""
        records = []
        start   = 1
        try:
            with httpx.Client(timeout=30) as client:
                while True:
                    sql = f"SELECT * FROM {entity} STARTPOSITION {start} MAXRESULTS {_LIMIT}"
                    r   = client.get(
                        f"{base}/query",
                        headers=headers,
                        params={"query": sql, "minorversion": "65"},
                    )
                    if r.status_code != 200:
                        raise ConnectorError(
                            f"QuickBooks error {r.status_code} querying {entity}: {r.text}"
                        )
                    data  = r.json().get("QueryResponse", {})
                    batch = data.get(entity, [])
                    records.extend(batch)
                    if len(batch) < _LIMIT:
                        break
                    start += _LIMIT
        except httpx.HTTPError as exc:
            raise ConnectorError(f"QuickBooks network error: {exc}") from exc
        return records
