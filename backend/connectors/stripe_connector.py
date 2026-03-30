"""
Stripe connector — extracts charges, subscriptions, customers, invoices.
Auth: API key (no OAuth needed).
"""
from __future__ import annotations

import httpx

from .base import BaseConnector, ConnectorError

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

        return [
            {"entity_type": "revenue",   "records": self._fetch_charges(headers)},
            {"entity_type": "customers", "records": self._fetch_customers(headers)},
            {"entity_type": "invoices",  "records": self._fetch_invoices(headers)},
            {"entity_type": "subscriptions", "records": self._fetch_subscriptions(headers)},
        ]

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            r = httpx.get(
                f"{_BASE}/account",
                headers={"Authorization": f"Bearer {credentials.get('api_key','')}"},
                timeout=10,
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
        try:
            with httpx.Client(timeout=30) as client:
                while True:
                    r = client.get(url, headers=headers, params=params)
                    if r.status_code != 200:
                        raise ConnectorError(
                            f"Stripe API error {r.status_code} on {url}"
                        )
                    data = r.json()
                    records.extend(data.get("data", []))
                    if not data.get("has_more"):
                        break
                    params["starting_after"] = data["data"][-1]["id"]
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Stripe network error: {exc}") from exc
        return records
