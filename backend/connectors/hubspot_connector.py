"""
HubSpot connector — extracts contacts, companies, deals.
Auth: Private App token (recommended) OR OAuth2.
Private App tokens start with pat-na1-... or pat-na2-...
"""
from __future__ import annotations

import logging
import os
import httpx

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.hubspot")

_BASE      = "https://api.hubapi.com"
_AUTH_URL  = "https://app.hubspot.com/oauth/authorize"
_TOKEN_URL = "https://api.hubapi.com/oauth/v1/token"
_LIMIT     = 100


class HubSpotConnector(BaseConnector):
    SOURCE_NAME  = "hubspot"
    AUTH_TYPE    = "api_key"   # private app token — no OAuth needed
    OAUTH_SCOPES = [
        "crm.objects.contacts.read",
        "crm.objects.deals.read",
        "crm.objects.companies.read",
    ]

    def validate_credentials(self, credentials: dict) -> bool:
        token = credentials.get("api_key", "") or credentials.get("access_token", "")
        if not token:
            return False
        try:
            r = httpx.get(
                f"{_BASE}/crm/v3/objects/contacts?limit=1",
                headers={"Authorization": f"Bearer {token}"},
                timeout=10,
            )
            return r.status_code == 200
        except Exception:
            return False

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        # Support both private app token (api_key) and OAuth access_token
        token = credentials.get("api_key", "") or credentials.get("access_token", "")
        if not token:
            raise ConnectorError("HubSpot token missing.")
        headers = {"Authorization": f"Bearer {token}"}

        entities = [
            ("customers", self._fetch_contacts),
            ("pipeline",  self._fetch_deals),
            ("companies", self._fetch_companies),
        ]
        results = []
        for entity_type, fetcher in entities:
            try:
                records = fetcher(headers)
                results.append({"entity_type": entity_type, "records": records})
            except ConnectorAuthError:
                raise  # auth errors must propagate
            except Exception as exc:
                log.error("[hubspot] Failed to fetch %s: %s", entity_type, exc)
                results.append({"entity_type": entity_type, "records": [], "error": str(exc)})
        return results

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
        with httpx.Client(timeout=30) as client:
            while True:
                r = _request_with_retry(client, "GET", url, headers=headers, params=params)
                data = r.json()
                records.extend(data.get("results", []))
                after = data.get("paging", {}).get("next", {}).get("after")
                if not after:
                    break
                params["after"] = after
        return records
