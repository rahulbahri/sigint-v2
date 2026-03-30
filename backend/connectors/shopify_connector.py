"""
Shopify connector — extracts orders, customers, products.
Auth: OAuth2 (per-shop token).
"""
from __future__ import annotations

import os
import httpx

from .base import BaseConnector, ConnectorError

_AUTH_PATH  = "/admin/oauth/authorize"
_TOKEN_PATH = "/admin/oauth/access_token"
_LIMIT      = 250   # Shopify max per page
_SCOPES     = "read_orders,read_customers,read_products,read_inventory"


class ShopifyConnector(BaseConnector):
    SOURCE_NAME  = "shopify"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = _SCOPES.split(",")

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        # shop domain stored in state as "nonce|shop_domain"
        shop = state.split("|")[-1] if "|" in state else ""
        client_id = os.environ["SHOPIFY_CLIENT_ID"]
        return (
            f"https://{shop}{_AUTH_PATH}"
            f"?client_id={client_id}"
            f"&scope={_SCOPES}"
            f"&redirect_uri={redirect_uri}"
            f"&state={state}"
        )

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        # redirect_uri carries shop domain as query param in Shopify flow
        # credentials dict will be built by the router which has shop from state
        raise NotImplementedError("Use exchange_code_for_shop(code, shop)")

    def exchange_code_for_shop(self, code: str, shop: str) -> dict:
        r = httpx.post(
            f"https://{shop}{_TOKEN_PATH}",
            json={
                "client_id":     os.environ["SHOPIFY_CLIENT_ID"],
                "client_secret": os.environ["SHOPIFY_CLIENT_SECRET"],
                "code":          code,
            },
            timeout=30,
        )
        if r.status_code != 200:
            raise ConnectorError(f"Shopify token exchange failed: {r.text}")
        data = r.json()
        return {"access_token": data["access_token"], "shop": shop}

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        token = credentials.get("access_token", "")
        shop  = credentials.get("shop", "")
        if not token or not shop:
            raise ConnectorError("Shopify credentials missing access_token or shop.")
        headers = {
            "X-Shopify-Access-Token": token,
            "Content-Type": "application/json",
        }
        base = f"https://{shop}/admin/api/2024-01"
        return [
            {"entity_type": "revenue",   "records": self._fetch_orders(base, headers)},
            {"entity_type": "customers", "records": self._fetch_customers(base, headers)},
            {"entity_type": "products",  "records": self._fetch_products(base, headers)},
        ]

    def _fetch_orders(self, base: str, headers: dict) -> list[dict]:
        return self._paginate_link(f"{base}/orders.json", headers, "orders",
                                   params={"limit": _LIMIT, "status": "any"})

    def _fetch_customers(self, base: str, headers: dict) -> list[dict]:
        return self._paginate_link(f"{base}/customers.json", headers, "customers",
                                   params={"limit": _LIMIT})

    def _fetch_products(self, base: str, headers: dict) -> list[dict]:
        return self._paginate_link(f"{base}/products.json", headers, "products",
                                   params={"limit": _LIMIT})

    def _paginate_link(
        self, url: str, headers: dict, key: str, params: dict
    ) -> list[dict]:
        """Shopify uses Link header pagination."""
        records = []
        next_url: str | None = url
        try:
            with httpx.Client(timeout=30) as client:
                while next_url:
                    r = client.get(next_url, headers=headers,
                                   params=params if next_url == url else None)
                    if r.status_code != 200:
                        raise ConnectorError(
                            f"Shopify error {r.status_code} on {url}: {r.text}"
                        )
                    records.extend(r.json().get(key, []))
                    # Parse Link header for next page
                    link_header = r.headers.get("Link", "")
                    next_url = None
                    for part in link_header.split(","):
                        if 'rel="next"' in part:
                            next_url = part.split(";")[0].strip().strip("<>")
                            break
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Shopify network error: {exc}") from exc
        return records
