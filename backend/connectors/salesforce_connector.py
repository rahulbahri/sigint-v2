"""
Salesforce connector — extracts opportunities, accounts, contacts, leads.
Auth: OAuth2.
"""
from __future__ import annotations

import os
import httpx

from .base import BaseConnector, ConnectorError

_AUTH_URL  = "https://login.salesforce.com/services/oauth2/authorize"
_TOKEN_URL = "https://login.salesforce.com/services/oauth2/token"
_API_VER   = "v59.0"
_LIMIT     = 2000   # SOQL max


class SalesforceConnector(BaseConnector):
    SOURCE_NAME  = "salesforce"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = ["api", "refresh_token", "offline_access"]

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        scopes = "%20".join(self.OAUTH_SCOPES)
        return (
            f"{_AUTH_URL}?response_type=code"
            f"&client_id={os.environ['SALESFORCE_CLIENT_ID']}"
            f"&redirect_uri={redirect_uri}"
            f"&scope={scopes}"
            f"&state={state}"
        )

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        r = httpx.post(_TOKEN_URL, data={
            "grant_type":    "authorization_code",
            "code":          code,
            "client_id":     os.environ["SALESFORCE_CLIENT_ID"],
            "client_secret": os.environ["SALESFORCE_CLIENT_SECRET"],
            "redirect_uri":  redirect_uri,
        }, timeout=30)
        if r.status_code != 200:
            raise ConnectorError(f"Salesforce token exchange failed: {r.text}")
        return r.json()   # includes instance_url

    def refresh_token(self, credentials: dict) -> dict:
        r = httpx.post(_TOKEN_URL, data={
            "grant_type":    "refresh_token",
            "refresh_token": credentials["refresh_token"],
            "client_id":     os.environ["SALESFORCE_CLIENT_ID"],
            "client_secret": os.environ["SALESFORCE_CLIENT_SECRET"],
        }, timeout=30)
        if r.status_code != 200:
            raise ConnectorError(f"Salesforce token refresh failed: {r.text}")
        return {**credentials, **r.json()}

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        token        = credentials.get("access_token", "")
        instance_url = credentials.get("instance_url", "")
        if not token or not instance_url:
            raise ConnectorError("Salesforce credentials missing access_token or instance_url.")
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        base    = f"{instance_url}/services/data/{_API_VER}"
        return [
            {"entity_type": "pipeline",  "records": self._soql(base, headers,
                "SELECT Id,Name,Amount,StageName,CloseDate,CreatedDate,OwnerId,AccountId,"
                "Probability,Type,LeadSource FROM Opportunity")},
            {"entity_type": "customers", "records": self._soql(base, headers,
                "SELECT Id,Name,Email,Phone,Title,AccountId,CreatedDate,LeadSource,"
                "Status FROM Contact")},
            {"entity_type": "companies", "records": self._soql(base, headers,
                "SELECT Id,Name,Industry,NumberOfEmployees,AnnualRevenue,"
                "BillingCountry,CreatedDate FROM Account")},
        ]

    def _soql(self, base: str, headers: dict, query: str) -> list[dict]:
        records  = []
        next_url = f"{base}/query?q={query}"
        try:
            with httpx.Client(timeout=30) as client:
                while next_url:
                    r = client.get(next_url, headers=headers)
                    if r.status_code != 200:
                        raise ConnectorError(
                            f"Salesforce query error {r.status_code}: {r.text}"
                        )
                    data = r.json()
                    records.extend(data.get("records", []))
                    next_path = data.get("nextRecordsUrl")
                    # nextRecordsUrl is a path, prepend instance base
                    next_url  = (base.split("/services")[0] + next_path) if next_path else None
        except httpx.HTTPError as exc:
            raise ConnectorError(f"Salesforce network error: {exc}") from exc
        return records
