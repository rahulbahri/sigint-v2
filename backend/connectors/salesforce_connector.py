"""
Salesforce connector — extracts opportunities, accounts, contacts, leads.
Auth: OAuth2.
"""
from __future__ import annotations

import logging
import os
from urllib.parse import urlparse

import httpx

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.salesforce")

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

    def exchange_code(self, code: str, redirect_uri: str, **kwargs) -> dict:
        data = {
            "grant_type":    "authorization_code",
            "code":          code,
            "client_id":     os.environ["SALESFORCE_CLIENT_ID"],
            "client_secret": os.environ["SALESFORCE_CLIENT_SECRET"],
            "redirect_uri":  redirect_uri,
        }
        # PKCE support: newer Salesforce orgs require code_verifier
        if kwargs.get("code_verifier"):
            data["code_verifier"] = kwargs["code_verifier"]
        r = httpx.post(_TOKEN_URL, data=data, timeout=30)
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

        entities = [
            ("pipeline", "SELECT Id,Name,Amount,StageName,CloseDate,CreatedDate,OwnerId,AccountId,"
                         "Probability,Type,LeadSource FROM Opportunity"),
            ("customers", "SELECT Id,Name,Email,Phone,Title,AccountId,CreatedDate,LeadSource"
                          " FROM Contact"),
            ("companies", "SELECT Id,Name,Industry,NumberOfEmployees,AnnualRevenue,"
                          "BillingCountry,CreatedDate FROM Account"),
        ]
        results = []
        for entity_type, query in entities:
            try:
                records = self._soql(base, headers, query)
                results.append({"entity_type": entity_type, "records": records})
            except ConnectorAuthError:
                raise
            except Exception as exc:
                log.error("[salesforce] Failed to fetch %s: %s", entity_type, exc)
                results.append({"entity_type": entity_type, "records": [], "error": str(exc)})
        return results

    def _soql(self, base: str, headers: dict, query: str) -> list[dict]:
        records  = []
        next_url = f"{base}/query?q={query}"
        # Derive instance origin (scheme + host) robustly from the base URL
        parsed = urlparse(base)
        instance_origin = f"{parsed.scheme}://{parsed.netloc}"
        with httpx.Client(timeout=30) as client:
            while next_url:
                r = _request_with_retry(client, "GET", next_url, headers=headers)
                data = r.json()
                records.extend(data.get("records", []))
                next_path = data.get("nextRecordsUrl")
                # nextRecordsUrl is an absolute path like /services/data/v59.0/query/...
                next_url = (instance_origin + next_path) if next_path else None
        return records
