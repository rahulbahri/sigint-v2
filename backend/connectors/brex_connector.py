"""
Brex connector — extracts expenses, transactions, and vendor spend.
Auth: API token (no OAuth — Brex uses long-lived tokens).

Credentials required (Render env vars):
  BREX_API_TOKEN  — from Brex dashboard → Developer → API Keys
"""
from __future__ import annotations

import logging
import os
import httpx

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.brex")

_BASE   = "https://platform.brexapis.com"
_LIMIT  = 100
_TOKEN  = os.environ.get("BREX_API_TOKEN", "")


class BrexConnector(BaseConnector):
    SOURCE_NAME = "brex"
    AUTH_TYPE   = "api_key"

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            r = httpx.get(
                f"{_BASE}/v2/accounts/cash",
                headers=self._headers(credentials),
                timeout=10,
            )
            return r.status_code == 200
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        api_token = credentials.get("api_key") or _TOKEN
        if not api_token:
            raise ConnectorError("Brex API token not configured.")

        creds = {"api_key": api_token}
        entities = [
            ("expenses",  self._fetch_expenses),
            ("employees", self._fetch_users),
        ]
        results = []
        for entity_type, fetcher in entities:
            try:
                records = fetcher(creds)
                results.append({"entity_type": entity_type, "records": records})
            except ConnectorAuthError:
                raise
            except Exception as exc:
                log.error("[brex] Failed to fetch %s: %s", entity_type, exc)
                results.append({"entity_type": entity_type, "records": [], "error": str(exc)})
        return results

    # ── Private helpers ───────────────────────────────────────────────────────

    def _headers(self, credentials: dict) -> dict:
        token = credentials.get("api_key") or _TOKEN
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type":  "application/json",
        }

    def _fetch_expenses(self, credentials: dict) -> list[dict]:
        """Pull card transactions (expenses) from Brex."""
        records: list[dict] = []
        cursor = None
        hdrs   = self._headers(credentials)
        with httpx.Client(timeout=30) as client:
            for _ in range(200):
                params: dict = {"limit": _LIMIT, "status": "APPROVED"}
                if cursor:
                    params["cursor"] = cursor
                r = _request_with_retry(
                    client, "GET",
                    f"{_BASE}/v2/transactions/card/primary",
                    headers=hdrs, params=params,
                )
                data = r.json()
                items = data.get("items", [])
                records.extend(items)
                cursor = data.get("next_cursor")
                if not cursor or not items:
                    break
        return records

    def _fetch_users(self, credentials: dict) -> list[dict]:
        """Pull team/user list for headcount metrics (paginated)."""
        records: list[dict] = []
        cursor = None
        hdrs = self._headers(credentials)
        with httpx.Client(timeout=20) as client:
            for _ in range(200):
                params: dict = {"limit": _LIMIT}
                if cursor:
                    params["cursor"] = cursor
                r = _request_with_retry(
                    client, "GET",
                    f"{_BASE}/v2/users",
                    headers=hdrs, params=params,
                )
                data = r.json()
                items = data.get("items", [])
                records.extend(items)
                cursor = data.get("next_cursor")
                if not cursor or not items:
                    break
        return records
