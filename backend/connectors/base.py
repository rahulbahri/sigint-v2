"""
Base connector class — all source connectors inherit from this.
Provides: credential encryption/decryption, raw extract storage,
sync status tracking, and the extract() interface contract.
"""
from __future__ import annotations

import json
import os
import time
from abc import ABC, abstractmethod
from typing import Any

from cryptography.fernet import Fernet

# ── Encryption helpers ────────────────────────────────────────────────────────
_FERNET: Fernet | None = None


def _get_fernet() -> Fernet:
    global _FERNET
    if _FERNET is None:
        key = os.environ.get("ENCRYPTION_KEY", "")
        if not key:
            # Dev fallback — generate ephemeral key (tokens lost on restart)
            key = Fernet.generate_key().decode()
        _FERNET = Fernet(key.encode() if isinstance(key, str) else key)
    return _FERNET


def encrypt_credentials(creds: dict) -> str:
    return _get_fernet().encrypt(json.dumps(creds).encode()).decode()


def decrypt_credentials(token: str) -> dict:
    return json.loads(_get_fernet().decrypt(token.encode()).decode())


# ── Exceptions ────────────────────────────────────────────────────────────────
class ConnectorError(Exception):
    """Raised when a connector cannot extract data."""


# ── Base class ────────────────────────────────────────────────────────────────
class BaseConnector(ABC):
    """
    Subclass this for every source system.

    Subclasses must implement:
        SOURCE_NAME: str          — e.g. "stripe", "quickbooks"
        AUTH_TYPE: str            — "oauth2" | "api_key"
        extract(conn, workspace_id, credentials) -> list[dict]
            Each dict must have: entity_type, records (list)

    Optional OAuth helpers (override for OAuth2 sources):
        get_auth_url(redirect_uri, state) -> str
        exchange_code(code, redirect_uri) -> dict   # returns credentials dict
        refresh_token(credentials) -> dict           # returns updated credentials
    """

    SOURCE_NAME: str = "unknown"
    AUTH_TYPE: str = "api_key"   # "oauth2" | "api_key"

    # Scopes to request during OAuth (override per connector)
    OAUTH_SCOPES: list[str] = []

    # ── Interface ─────────────────────────────────────────────────────────────

    @abstractmethod
    def extract(
        self,
        workspace_id: str,
        credentials: dict,
    ) -> list[dict]:
        """
        Pull data from the source and return a list of extract bundles:
        [
          {"entity_type": "revenue",   "records": [...]},
          {"entity_type": "customers", "records": [...]},
        ]
        Each record is a raw dict exactly as returned by the source API.
        Do NOT transform here — just return raw data.
        """

    def get_auth_url(self, redirect_uri: str, state: str) -> str:
        raise NotImplementedError(f"{self.SOURCE_NAME} does not support OAuth2")

    def exchange_code(self, code: str, redirect_uri: str) -> dict:
        raise NotImplementedError(f"{self.SOURCE_NAME} does not support OAuth2")

    def refresh_token(self, credentials: dict) -> dict:
        return credentials  # default: no refresh needed (API keys)

    def validate_credentials(self, credentials: dict) -> bool:
        """Quick ping to check credentials are still valid. Override per connector."""
        return True

    # ── Helpers available to all connectors ───────────────────────────────────

    def _paginate(
        self,
        fetch_page,          # callable(cursor) -> (records, next_cursor)
        initial_cursor=None,
        max_pages: int = 200,
    ) -> list[Any]:
        """Generic cursor-based paginator."""
        all_records = []
        cursor = initial_cursor
        for _ in range(max_pages):
            records, cursor = fetch_page(cursor)
            all_records.extend(records)
            if not cursor:
                break
        return all_records

    def _now_iso(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat()
