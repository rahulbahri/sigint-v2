"""
NetSuite connector — extracts P&L, balance sheet, and transaction data
via SuiteAnalytics REST API (OAuth 1.0a TBA — Token Based Authentication).

Credentials required (Render env vars):
  NETSUITE_ACCOUNT_ID       — your NetSuite account ID (e.g. 1234567)
  NETSUITE_CONSUMER_KEY     — from NetSuite → Setup → Integrations
  NETSUITE_CONSUMER_SECRET  — same integration record
  NETSUITE_TOKEN_ID         — from Setup → Users/Roles → Access Tokens
  NETSUITE_TOKEN_SECRET     — same token record

Note: NetSuite uses OAuth 1.0a (not 2.0). No browser redirect needed —
credentials are set directly in Render env vars.
"""
from __future__ import annotations

import hashlib
import hmac
import os
import time
import base64
import secrets
import urllib.parse
import httpx

from .base import BaseConnector, ConnectorError

_ACCOUNT_ID      = os.environ.get("NETSUITE_ACCOUNT_ID", "")
_CONSUMER_KEY    = os.environ.get("NETSUITE_CONSUMER_KEY", "")
_CONSUMER_SECRET = os.environ.get("NETSUITE_CONSUMER_SECRET", "")
_TOKEN_ID        = os.environ.get("NETSUITE_TOKEN_ID", "")
_TOKEN_SECRET    = os.environ.get("NETSUITE_TOKEN_SECRET", "")


def _build_oauth1_header(
    method: str,
    url: str,
    account_id: str,
    consumer_key: str,
    consumer_secret: str,
    token_id: str,
    token_secret: str,
) -> str:
    """Build OAuth 1.0a Authorization header for NetSuite TBA."""
    nonce     = secrets.token_hex(16)
    timestamp = str(int(time.time()))
    realm     = account_id.upper().replace("-", "_")

    params = {
        "oauth_consumer_key":     consumer_key,
        "oauth_nonce":            nonce,
        "oauth_signature_method": "HMAC-SHA256",
        "oauth_timestamp":        timestamp,
        "oauth_token":            token_id,
        "oauth_version":          "1.0",
    }

    # Build base string
    sorted_params = "&".join(
        f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}"
        for k, v in sorted(params.items())
    )
    base_string = "&".join([
        method.upper(),
        urllib.parse.quote(url, safe=""),
        urllib.parse.quote(sorted_params, safe=""),
    ])

    # Build signing key and compute signature
    signing_key = f"{urllib.parse.quote(consumer_secret, safe='')}&{urllib.parse.quote(token_secret, safe='')}"
    signature   = base64.b64encode(
        hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha256).digest()
    ).decode()
    params["oauth_signature"] = signature

    header_parts = [f'realm="{realm}"'] + [
        f'{k}="{v}"' for k, v in sorted(params.items())
    ]
    return "OAuth " + ", ".join(header_parts)


class NetSuiteConnector(BaseConnector):
    SOURCE_NAME = "netsuite"
    AUTH_TYPE   = "api_key"   # TBA — credentials set in env vars, no OAuth redirect

    def _base_url(self, credentials: dict) -> str:
        acct = (credentials.get("account_id") or _ACCOUNT_ID).upper().replace("-", "_")
        return f"https://{acct}.suitetalk.api.netsuite.com/services/rest/record/v1"

    def _rest_base(self, credentials: dict) -> str:
        acct = (credentials.get("account_id") or _ACCOUNT_ID).upper().replace("-", "_")
        return f"https://{acct}.suitetalk.api.netsuite.com"

    def _auth_header(self, method: str, url: str, credentials: dict) -> str:
        return _build_oauth1_header(
            method=method,
            url=url,
            account_id=credentials.get("account_id") or _ACCOUNT_ID,
            consumer_key=credentials.get("consumer_key") or _CONSUMER_KEY,
            consumer_secret=credentials.get("consumer_secret") or _CONSUMER_SECRET,
            token_id=credentials.get("token_id") or _TOKEN_ID,
            token_secret=credentials.get("token_secret") or _TOKEN_SECRET,
        )

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            url = self._base_url(credentials) + "/salesorder?limit=1"
            headers = {
                "Authorization": self._auth_header("GET", url, credentials),
                "Content-Type": "application/json",
            }
            r = httpx.get(url, headers=headers, timeout=15)
            return r.status_code in (200, 204, 404)
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        acct = credentials.get("account_id") or _ACCOUNT_ID
        if not acct:
            raise ConnectorError("NETSUITE_ACCOUNT_ID not configured")

        return [
            {"entity_type": "revenue",   "records": self._fetch_invoices(credentials)},
            {"entity_type": "expenses",  "records": self._fetch_vendor_bills(credentials)},
            {"entity_type": "customers", "records": self._fetch_customers(credentials)},
        ]

    def _fetch_list(self, credentials: dict, record_type: str, fields: str = "") -> list[dict]:
        """Generic NetSuite REST record list fetch with cursor pagination."""
        base = self._base_url(credentials)
        url  = f"{base}/{record_type}?limit=1000{('&fields=' + fields) if fields else ''}"
        records: list[dict] = []
        with httpx.Client(timeout=45) as client:
            for _ in range(50):
                headers = {
                    "Authorization": self._auth_header("GET", url.split("?")[0], credentials),
                    "Content-Type":  "application/json",
                    "Prefer":        "transient",
                }
                r = client.get(url, headers=headers)
                if r.status_code == 401:
                    raise ConnectorError("NetSuite credentials invalid or expired.")
                if r.status_code not in (200, 204):
                    raise ConnectorError(f"NetSuite {record_type} error {r.status_code}: {r.text[:200]}")
                data  = r.json()
                items = data.get("items", [])
                records.extend(items)
                # Check for next page
                links = {lnk["rel"]: lnk["href"] for lnk in data.get("links", [])}
                if "next" not in links:
                    break
                url = links["next"]
        return records

    def _fetch_invoices(self, credentials: dict) -> list[dict]:
        return self._fetch_list(credentials, "invoice",
                                "id,tranDate,entity,amount,status,currency")

    def _fetch_vendor_bills(self, credentials: dict) -> list[dict]:
        return self._fetch_list(credentials, "vendorbill",
                                "id,tranDate,entity,amount,status")

    def _fetch_customers(self, credentials: dict) -> list[dict]:
        return self._fetch_list(credentials, "customer",
                                "id,entityId,dateCreated,email,balance,salesRep")
