"""
Google Sheets connector — reads a named spreadsheet as the canonical KPI data source.
Auth: OAuth 2.0 (Google identity).

Credentials required (Render env vars):
  GOOGLE_CLIENT_ID       — from Google Cloud Console → OAuth 2.0 client
  GOOGLE_CLIENT_SECRET   — same client

The user's spreadsheet ID is stored in connector credentials after OAuth.
Expected sheet format: columns = month (YYYY-MM), plus any KPI keys as column headers.
"""
from __future__ import annotations

import logging
import os
import httpx
from urllib.parse import urlencode

from .base import BaseConnector, ConnectorError, ConnectorAuthError, _request_with_retry

log = logging.getLogger("connectors.google_sheets")

_GOOGLE_AUTH_URL  = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
_SHEETS_API_BASE  = "https://sheets.googleapis.com/v4/spreadsheets"

_SCOPE = "https://www.googleapis.com/auth/spreadsheets.readonly https://www.googleapis.com/auth/drive.metadata.readonly"

_CLIENT_ID     = os.environ.get("GOOGLE_CLIENT_ID", "")
_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")


class GoogleSheetsConnector(BaseConnector):
    SOURCE_NAME  = "google_sheets"
    AUTH_TYPE    = "oauth2"
    OAUTH_SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.metadata.readonly",
    ]

    # ── OAuth helpers ─────────────────────────────────────────────────────────

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        if not _CLIENT_ID:
            raise ConnectorError("GOOGLE_CLIENT_ID not configured")
        params = urlencode({
            "client_id":     _CLIENT_ID,
            "redirect_uri":  redirect_uri,
            "response_type": "code",
            "scope":         " ".join(self.OAUTH_SCOPES),
            "access_type":   "offline",
            "prompt":        "consent",
            "state":         state,
        })
        return f"{_GOOGLE_AUTH_URL}?{params}"

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        if not _CLIENT_ID or not _CLIENT_SECRET:
            raise ConnectorError("Google credentials not configured")
        with httpx.Client(timeout=15) as client:
            r = client.post(
                _GOOGLE_TOKEN_URL,
                data={
                    "client_id":     _CLIENT_ID,
                    "client_secret": _CLIENT_SECRET,
                    "code":          code,
                    "grant_type":    "authorization_code",
                    "redirect_uri":  redirect_uri,
                },
            )
        if r.status_code != 200:
            raise ConnectorError(f"Google token exchange failed: {r.text}")
        return r.json()  # access_token, refresh_token, expires_in

    def refresh_token(self, credentials: dict) -> dict:
        if not credentials.get("refresh_token"):
            return credentials
        with httpx.Client(timeout=15) as client:
            r = client.post(
                _GOOGLE_TOKEN_URL,
                data={
                    "client_id":     _CLIENT_ID,
                    "client_secret": _CLIENT_SECRET,
                    "refresh_token": credentials["refresh_token"],
                    "grant_type":    "refresh_token",
                },
            )
        if r.status_code != 200:
            raise ConnectorError(f"Google token refresh failed: {r.text}")
        updated = {**credentials, **r.json()}
        return updated

    def validate_credentials(self, credentials: dict) -> bool:
        try:
            r = httpx.get(
                "https://www.googleapis.com/oauth2/v3/userinfo",
                headers={"Authorization": f"Bearer {credentials.get('access_token', '')}"},
                timeout=10,
            )
            return r.status_code == 200
        except Exception:
            return False

    # ── Extract ───────────────────────────────────────────────────────────────

    def extract(self, workspace_id: str, credentials: dict) -> list[dict]:
        access_token  = credentials.get("access_token", "")
        spreadsheet_id = credentials.get("spreadsheet_id", "")

        if not access_token:
            raise ConnectorError("Google Sheets: no access token")
        if not spreadsheet_id:
            raise ConnectorError("Google Sheets: no spreadsheet_id in credentials. "
                                 "Please enter your Google Sheet ID after connecting.")

        hdrs = {"Authorization": f"Bearer {access_token}"}

        # Get spreadsheet metadata to find sheet names
        with httpx.Client(timeout=20) as client:
            meta = _request_with_retry(
                client, "GET",
                f"{_SHEETS_API_BASE}/{spreadsheet_id}",
                headers=hdrs,
            )

        sheets = meta.json().get("sheets", [])
        if not sheets:
            log.warning("[google_sheets] Spreadsheet %s has no sheets", spreadsheet_id)
            return [{"entity_type": "revenue", "records": [], "error": "No sheets found in spreadsheet"}]

        # Read first sheet
        sheet_name = sheets[0].get("properties", {}).get("title")
        if not sheet_name:
            log.warning("[google_sheets] First sheet has no title in spreadsheet %s", spreadsheet_id)
            return [{"entity_type": "revenue", "records": [], "error": "First sheet has no title"}]

        try:
            with httpx.Client(timeout=30) as client:
                data_resp = _request_with_retry(
                    client, "GET",
                    f"{_SHEETS_API_BASE}/{spreadsheet_id}/values/{sheet_name}",
                    headers=hdrs,
                )
        except ConnectorAuthError:
            raise
        except Exception as exc:
            log.error("[google_sheets] Failed to read sheet '%s': %s", sheet_name, exc)
            return [{"entity_type": "revenue", "records": [], "error": str(exc)}]

        values = data_resp.json().get("values", [])
        if len(values) < 2:
            log.info("[google_sheets] Sheet '%s' has fewer than 2 rows, returning empty", sheet_name)
            return [{"entity_type": "revenue", "records": []}]

        headers_row = [str(h).strip().lower() for h in values[0]]
        if not any(headers_row):
            log.warning("[google_sheets] Header row is empty in sheet '%s'", sheet_name)
            return [{"entity_type": "revenue", "records": [], "error": "Header row is empty"}]

        records = []
        for row_idx, row in enumerate(values[1:], start=2):
            try:
                record = {}
                for i, header in enumerate(headers_row):
                    record[header] = row[i] if i < len(row) else None
                records.append(record)
            except Exception as exc:
                log.warning("[google_sheets] Skipping malformed row %d: %s", row_idx, exc)

        # Attempt to detect month column (e.g. "month", "period", "date")
        # Return as revenue-like entity; the transformer will normalise column names
        return [
            {"entity_type": "revenue", "records": records},
        ]
