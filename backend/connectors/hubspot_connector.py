"""
HubSpot connector — extracts contacts, companies, deals.
Auth: OAuth2.
"""
from __future__ import annotations

import os
import httpx

from .base import BaseConnector, ConnectorError

_BASE     = "https://api.hubapi.com"
_AUTH_URL = "https://app.hubspot.com/oauth/authorize"
_TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
_LIMIT    = 100


class HubSpotConnector(BaseConnector):
    SOURCE_NAME  = "hubspot"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = [
        "crm.objects.contacts.read",
        "crm.objects.deals.read",
        "crm.objects.companies.read",
    ]

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        client_id = os.environ["HUBSPOT_CLIENT_ID"]
        scopes    = "%20".join(self.OAUTH_SCOPES)
        return (
            f"{_AUTH_URL}?client_id={client_id}"
            f"&redirect_uri={redirect_uri}"
            f"&scope={scopes}"
            f"&state={state}"
        )

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        r = httpx.post(_TOKEN_URL, data={
            "grant_type":    "authorization_code",
            "client_id":     os.environ["HUBSPOT_CLIENT_ID"],
            "client_secret": os.environ["HUBSPOT_CLIENT_SECRET"],
            "redirect_uri":  redirect_uri,
            "code":          code,
        }, timeout=30)
        if r.status_code != 200:
            raise ConnectorError(f"HubSpot token exchange failed: {r.text}")
        return r.json()

    def refresh_token(self, credentials: dict) -> dict:
        r = httpx.post(_TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "client_id":     os.environ["HUBSPOT_CLIENT_ID"],
            "client_secret": os.environ["HUBSPOT_CLIENT_SECRET"],
            "refresh_token": credentials["refresh_token"],
        }, timeout=30)
        if r.status_code != 200:
            raise ConnectorError(f"HubSpot token refresh failed: {r.text}")
        return {**credentials, **r.json()}

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        token = credentials.get("access_token", "")
        if not token:
            raise ConnectorError("HubSpot access token missing.")
        headers = {"Authorization": f"Bearer {token}"}
        return [
            {"entity_type": "customers", "records": self._fetch_contacts(headers)},
            {"entity_type": "pipeline",  "records": self._fetch_deals(headers)},
            {"entity_type": "companies", "records": self._fetch_companies(headers)},
        ]

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            r = httpx.get(
                f"{_BASE}/crm/v3/objects/contacts?limit=1",
                headers={"Authorization": f"Bearer {credentials.get('access_token','')}"},
                timeout=10,
            )
            return r.status_code == 200
        except Exception:
            return False

    def _fetch_contacts(self, headers: dict) -> list[dict]:
        props = "firstname,lastname,email,company,createdate,hs_lead_status,lifecyclestage"
        return self._hs_paginate(f"{_BASE}/crm/v3/objects/contacts", headers, props)

    def _fetch_deals(self, headers: dict) -> list[dict]:
        props = "dealname,amount,dealstage,closedate,createdate,pipeline,hubspot_owner_id"
        return self._hs_paginate(f"{_BASE}/crm/v3/objects/deals", headers, props)

    def _fetch_companies(self, headers: dict) -> list[dict]:
        props = "name,domain,industry,numberofemployees,annualrevenue,createdate"
        return self._hs_paginate(f"{_BASE}/crm/v3/objects/companies", headers, props)

    def _hs_paginate(self, url: str, headers: dict, properties: str) -> list[dict]:
        records = []
        params  = {"limit": _LIMIT, "properties": properties}
        try:
            with httpx.Client(timeout=30) as client:
                while True:
                    r = client.get(url, headers=headers, params=params)
                    if r.status_code != 200:
                        raise ConnectorError(
                            f"HubSpot API error {r.status_code} on {url}: {r.text}"
                        )
                    data = r.json()
                    records.extend(data.get("results", []))
                    after = data.get("paging", {}).get("next", {}).get("after")
                    if not after:
                        break
                    params["after"] = after
        except httpx.HTTPError as exc:
            raise ConnectorError(f"HubSpot network error: {exc}") from exc
        return records
