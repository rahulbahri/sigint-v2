"""
Xero connector — extracts invoices, contacts, accounts, payments.
Auth: OAuth2.
"""
from __future__ import annotations

import logging
import os

import httpx

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.xero")

_AUTH_URL  = "https://login.xero.com/identity/connect/authorize"
_TOKEN_URL = "https://identity.xero.com/connect/token"
_BASE      = "https://api.xero.com/api.xro/2.0"
_LIMIT     = 100


class XeroConnector(BaseConnector):
    SOURCE_NAME  = "xero"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = [
        "openid", "profile", "email", "offline_access",
        "accounting.transactions.read",
        "accounting.contacts.read",
        "accounting.reports.read",
    ]

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        scopes = "%20".join(self.OAUTH_SCOPES)
        return (
            f"{_AUTH_URL}?response_type=code"
            f"&client_id={os.environ['XERO_CLIENT_ID']}"
            f"&redirect_uri={redirect_uri}"
            f"&scope={scopes}"
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
            auth=(os.environ["XERO_CLIENT_ID"], os.environ["XERO_CLIENT_SECRET"]),
            timeout=30,
        )
        if r.status_code != 200:
            raise ConnectorError(f"Xero token exchange failed: {r.text}")
        # Also fetch tenant_id (Xero requires it for API calls)
        tokens = r.json()
        tenant = self._fetch_tenant(tokens["access_token"])
        return {**tokens, "tenant_id": tenant}

    def refresh_token(self, credentials: dict) -> dict:
        log.info("Refreshing Xero OAuth token")
        r = httpx.post(
            _TOKEN_URL,
            data={
                "grant_type":    "refresh_token",
                "refresh_token": credentials["refresh_token"],
            },
            auth=(os.environ["XERO_CLIENT_ID"], os.environ["XERO_CLIENT_SECRET"]),
            timeout=30,
        )
        if r.status_code != 200:
            log.error("Xero token refresh failed: %s", r.text[:200])
            raise ConnectorError(f"Xero token refresh failed: {r.text}")
        log.info("Xero OAuth token refreshed successfully")
        return {**credentials, **r.json()}

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        token     = credentials.get("access_token", "")
        tenant_id = credentials.get("tenant_id", "")
        if not token or not tenant_id:
            raise ConnectorError("Xero credentials missing access_token or tenant_id.")
        headers = {
            "Authorization": f"Bearer {token}",
            "Xero-tenant-id": tenant_id,
            "Accept":         "application/json",
        }

        entities = [
            ("invoices",  "Invoices"),
            ("customers", "Contacts"),
            ("revenue",   "Payments"),
            ("accounts",  "Accounts"),
        ]

        results: list[dict] = []
        for entity_type, xero_endpoint in entities:
            try:
                records = self._get(headers, xero_endpoint)
                results.append({"entity_type": entity_type, "records": records})
            except ConnectorAuthError:
                raise  # auth errors must propagate immediately
            except Exception as exc:
                log.error("Xero: failed to fetch %s: %s", xero_endpoint, exc)
                results.append({
                    "entity_type": entity_type,
                    "records": [],
                    "error": str(exc),
                })
        return results

    def _fetch_tenant(self, access_token: str) -> str:
        r = httpx.get(
            "https://api.xero.com/connections",
            headers={"Authorization": f"Bearer {access_token}"},
            timeout=10,
        )
        if r.status_code != 200 or not r.json():
            raise ConnectorError("Could not fetch Xero tenant ID.")
        return r.json()[0]["tenantId"]

    def _get(self, headers: dict, endpoint: str) -> list[dict]:
        """Paginated GET with retry."""
        records = []
        page    = 1
        with httpx.Client(timeout=30) as client:
            while True:
                r = _request_with_retry(
                    client, "GET", f"{_BASE}/{endpoint}",
                    headers=headers,
                    params={"page": page},
                )
                batch = r.json().get(endpoint, [])
                records.extend(batch)
                if len(batch) < _LIMIT:
                    break
                page += 1
        return records
