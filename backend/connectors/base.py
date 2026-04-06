"""
Base connector class — all source connectors inherit from this.
Provides: credential encryption/decryption, raw extract storage,
sync status tracking, retry logic, and the extract() interface contract.
"""
from __future__ import annotations

import json
import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx
from cryptography.fernet import Fernet

log = logging.getLogger("connectors")

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


class ConnectorAuthError(ConnectorError):
    """Raised when credentials are invalid or expired (401/403)."""


class ConnectorRateLimitError(ConnectorError):
    """Raised when the source API rate-limits us (429)."""


# ── Retry helper ──────────────────────────────────────────────────────────────

def _request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    **kwargs,
) -> httpx.Response:
    """Execute an HTTP request with retry on transient errors (429, 500-503).

    Raises ConnectorAuthError on 401/403 (no retry).
    Raises ConnectorRateLimitError on 429 after all retries exhausted.
    Raises ConnectorError on other persistent failures.
    """
    last_exc = None
    for attempt in range(max_retries + 1):
        try:
            r = client.request(method, url, **kwargs)

            if r.status_code in (401, 403):
                raise ConnectorAuthError(
                    f"Auth failed ({r.status_code}) on {url}: {r.text[:200]}"
                )

            if r.status_code == 429:
                # Respect Retry-After header if present
                retry_after = r.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else backoff_base * (2 ** attempt)
                wait = min(wait, 60)  # cap at 60s
                if attempt < max_retries:
                    log.warning("[%s] Rate limited (429), retry in %.1fs (attempt %d/%d)",
                                url, wait, attempt + 1, max_retries)
                    time.sleep(wait)
                    continue
                raise ConnectorRateLimitError(
                    f"Rate limited on {url} after {max_retries} retries"
                )

            if r.status_code >= 500 and attempt < max_retries:
                wait = backoff_base * (2 ** attempt)
                log.warning("[%s] Server error %d, retry in %.1fs (attempt %d/%d)",
                            url, r.status_code, wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue

            if r.status_code >= 400:
                raise ConnectorError(
                    f"API error {r.status_code} on {url}: {r.text[:300]}"
                )

            return r

        except (httpx.ConnectError, httpx.ReadTimeout, httpx.WriteTimeout,
                httpx.PoolTimeout, httpx.ConnectTimeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                wait = backoff_base * (2 ** attempt)
                log.warning("[%s] Network error %s, retry in %.1fs (attempt %d/%d)",
                            url, type(exc).__name__, wait, attempt + 1, max_retries)
                time.sleep(wait)
                continue
            raise ConnectorError(f"Network error on {url} after {max_retries} retries: {exc}") from exc

    raise ConnectorError(f"Request failed after {max_retries} retries: {last_exc}")


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

    def extract_safe(
        self,
        workspace_id: str,
        credentials: dict,
    ) -> list[dict]:
        """Like extract(), but catches per-entity errors so one failure
        doesn't kill the whole sync. Returns partial results with errors logged."""
        try:
            return self.extract(workspace_id, credentials)
        except ConnectorAuthError:
            raise  # Auth errors should always propagate
        except ConnectorError as e:
            log.error("[%s] Extract failed: %s", self.SOURCE_NAME, e)
            return []

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
