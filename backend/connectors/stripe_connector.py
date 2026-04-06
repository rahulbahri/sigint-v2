"""
Stripe connector — extracts charges, subscriptions, customers, invoices.
Auth: API key (no OAuth needed).
Retry: exponential backoff on 429/5xx with Retry-After header support.
"""
from __future__ import annotations

import logging

import httpx

from .base import BaseConnector, ConnectorError, _request_with_retry

log = logging.getLogger("connectors.stripe")

_BASE = "https://api.stripe.com/v1"
_LIMIT = 100  # max per page


class StripeConnector(BaseConnector):
    SOURCE_NAME = "stripe"
    AUTH_TYPE = "api_key"

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        api_key = credentials.get("api_key", "")
        if not api_key:
            raise ConnectorError("Stripe API key is missing.")

        headers = {"Authorization": f"Bearer {api_key}"}
        results = []
        entities = [
            ("revenue",       self._fetch_charges),
            ("customers",     self._fetch_customers),
            ("invoices",      self._fetch_invoices),
            ("subscriptions", self._fetch_subscriptions),
        ]
        for entity_type, fetcher in entities:
            try:
                records = fetcher(headers)
                results.append({"entity_type": entity_type, "records": records})
                log.info("[stripe] Fetched %d %s records", len(records), entity_type)
            except Exception as e:
                log.error("[stripe] Failed to fetch %s: %s", entity_type, e)
                results.append({"entity_type": entity_type, "records": [], "error": str(e)})

        return results

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            with httpx.Client(timeout=10) as client:
                r = _request_with_retry(
                    client, "GET", f"{_BASE}/account",
                    headers={"Authorization": f"Bearer {credentials.get('api_key', '')}"},
                    max_retries=1,
                )
                return r.status_code == 200
        except Exception:
            return False

    # ── Private helpers ───────────────────────────────────────────────────────

    def _fetch_charges(self, headers: dict) -> list[dict]:
        return self._stripe_paginate(f"{_BASE}/charges", headers)

    def _fetch_customers(self, headers: dict) -> list[dict]:
        return self._stripe_paginate(f"{_BASE}/customers", headers)

    def _fetch_invoices(self, headers: dict) -> list[dict]:
        return self._stripe_paginate(f"{_BASE}/invoices", headers)

    def _fetch_subscriptions(self, headers: dict) -> list[dict]:
        return self._stripe_paginate(f"{_BASE}/subscriptions", headers, extra={"status": "all"})

    def _stripe_paginate(
        self, url: str, headers: dict, extra: dict | None = None
    ) -> list[dict]:
        records = []
        params = {"limit": _LIMIT, **(extra or {})}
        with httpx.Client(timeout=30) as client:
            while True:
                r = _request_with_retry(client, "GET", url, headers=headers, params=params)
                data = r.json()
                batch = data.get("data", [])
                records.extend(batch)
                if not data.get("has_more") or not batch:
                    break
                params["starting_after"] = batch[-1]["id"]
        return records
