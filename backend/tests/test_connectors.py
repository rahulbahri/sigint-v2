"""
tests/test_connectors.py — Unit tests for connector base module and key connectors.

Tests cover:
  - Credential encryption / decryption roundtrip
  - _request_with_retry: success, 429 rate limit, 401 auth error, 500 server error, timeout
  - _paginate generic helper
  - StripeConnector: validate_credentials, extract, partial failure, pagination
  - QuickBooksConnector: extract 6 entity types, partial failure
  - XeroConnector: extract 4 entity types, page-based pagination
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import httpx
import pytest

# Ensure backend is on import path
sys.path.insert(0, str(Path(__file__).parent.parent))

from connectors.base import (
    BaseConnector,
    ConnectorAuthError,
    ConnectorError,
    ConnectorRateLimitError,
    _get_fernet,
    _request_with_retry,
    decrypt_credentials,
    encrypt_credentials,
)
from connectors.stripe_connector import StripeConnector
from connectors.quickbooks_connector import QuickBooksConnector
from connectors.xero_connector import XeroConnector


# ── Helpers ──────────────────────────────────────────────────────────────────

def _mock_response(status_code: int = 200, json_data=None, text="", headers=None):
    """Build a minimal mock httpx.Response."""
    r = MagicMock(spec=httpx.Response)
    r.status_code = status_code
    r.json.return_value = json_data or {}
    r.text = text or json.dumps(json_data or {})
    r.headers = headers or {}
    return r


class _ConcreteConnector(BaseConnector):
    """Concrete subclass for testing base _paginate."""
    SOURCE_NAME = "test"

    def extract(self, workspace_id, credentials):
        return []


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _reset_fernet():
    """Reset the cached Fernet instance between tests so env changes take effect."""
    import connectors.base as _base_mod
    _base_mod._FERNET = None
    yield
    _base_mod._FERNET = None


@pytest.fixture
def stripe():
    return StripeConnector()


@pytest.fixture
def quickbooks():
    return QuickBooksConnector()


@pytest.fixture
def xero():
    return XeroConnector()


# ═══════════════════════════════════════════════════════════════════════════════
# 1. BASE MODULE — Credential encryption
# ═══════════════════════════════════════════════════════════════════════════════

class TestCredentialEncryption:
    """encrypt_credentials / decrypt_credentials roundtrip."""

    def test_roundtrip_simple(self):
        creds = {"api_key": "sk_test_123", "mode": "live"}
        token = encrypt_credentials(creds)
        assert isinstance(token, str)
        assert token != json.dumps(creds)  # encrypted, not plaintext
        assert decrypt_credentials(token) == creds

    def test_roundtrip_nested(self):
        creds = {"access_token": "abc", "meta": {"scopes": ["read", "write"]}}
        assert decrypt_credentials(encrypt_credentials(creds)) == creds

    def test_roundtrip_empty(self):
        assert decrypt_credentials(encrypt_credentials({})) == {}

    def test_decrypt_invalid_token_raises(self):
        with pytest.raises(Exception):
            decrypt_credentials("not-a-valid-fernet-token")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. BASE MODULE — _request_with_retry
# ═══════════════════════════════════════════════════════════════════════════════

class TestRequestWithRetry:
    """_request_with_retry with mocked httpx.Client."""

    def test_success_200(self):
        client = MagicMock()
        client.request.return_value = _mock_response(200, {"ok": True})
        r = _request_with_retry(client, "GET", "https://api.test/v1", max_retries=2)
        assert r.status_code == 200
        assert client.request.call_count == 1

    def test_401_raises_auth_error_immediately(self):
        """401 should raise ConnectorAuthError on the first attempt — no retry."""
        client = MagicMock()
        client.request.return_value = _mock_response(401, text="Unauthorized")
        with pytest.raises(ConnectorAuthError, match="Auth failed.*401"):
            _request_with_retry(client, "GET", "https://api.test/v1", max_retries=3)
        assert client.request.call_count == 1  # no retry

    def test_403_raises_auth_error_immediately(self):
        client = MagicMock()
        client.request.return_value = _mock_response(403, text="Forbidden")
        with pytest.raises(ConnectorAuthError, match="Auth failed.*403"):
            _request_with_retry(client, "GET", "https://api.test/v1", max_retries=3)
        assert client.request.call_count == 1

    @patch("connectors.base.time.sleep")
    def test_429_retries_then_succeeds(self, mock_sleep):
        """429 should trigger retries; success on second attempt."""
        client = MagicMock()
        client.request.side_effect = [
            _mock_response(429, text="Rate limited", headers={}),
            _mock_response(200, {"ok": True}),
        ]
        r = _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=3, backoff_base=0.01)
        assert r.status_code == 200
        assert client.request.call_count == 2
        mock_sleep.assert_called_once()

    @patch("connectors.base.time.sleep")
    def test_429_respects_retry_after_header(self, mock_sleep):
        client = MagicMock()
        client.request.side_effect = [
            _mock_response(429, text="Rate limited", headers={"Retry-After": "5"}),
            _mock_response(200, {"ok": True}),
        ]
        _request_with_retry(client, "GET", "https://api.test/v1",
                            max_retries=3, backoff_base=0.01)
        mock_sleep.assert_called_once_with(5.0)

    @patch("connectors.base.time.sleep")
    def test_429_all_retries_exhausted_raises(self, mock_sleep):
        """429 on every attempt should eventually raise ConnectorRateLimitError."""
        client = MagicMock()
        client.request.return_value = _mock_response(429, text="Rate limited")
        with pytest.raises(ConnectorRateLimitError, match="Rate limited"):
            _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=2, backoff_base=0.01)
        # initial attempt + 2 retries = 3 total
        assert client.request.call_count == 3

    @patch("connectors.base.time.sleep")
    def test_500_retries_then_succeeds(self, mock_sleep):
        """500 server error should trigger retries; success on second attempt."""
        client = MagicMock()
        client.request.side_effect = [
            _mock_response(500, text="Internal Server Error"),
            _mock_response(200, {"ok": True}),
        ]
        r = _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=3, backoff_base=0.01)
        assert r.status_code == 200
        assert client.request.call_count == 2

    @patch("connectors.base.time.sleep")
    def test_500_all_retries_exhausted_raises(self, mock_sleep):
        """Persistent 500 should eventually raise ConnectorError."""
        client = MagicMock()
        client.request.return_value = _mock_response(500, text="Server Error")
        # When all retries are exhausted and status is still >= 400, the
        # final iteration falls through to the >= 400 branch
        with pytest.raises(ConnectorError):
            _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=2, backoff_base=0.01)

    @patch("connectors.base.time.sleep")
    def test_network_timeout_retries_then_succeeds(self, mock_sleep):
        """Network timeout (ReadTimeout) should trigger retries."""
        client = MagicMock()
        client.request.side_effect = [
            httpx.ReadTimeout("read timed out"),
            _mock_response(200, {"data": 1}),
        ]
        r = _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=3, backoff_base=0.01)
        assert r.status_code == 200
        assert client.request.call_count == 2

    @patch("connectors.base.time.sleep")
    def test_connect_error_retries_then_raises(self, mock_sleep):
        """Persistent ConnectError after all retries should raise ConnectorError."""
        client = MagicMock()
        client.request.side_effect = httpx.ConnectError("connection refused")
        with pytest.raises(ConnectorError, match="Network error"):
            _request_with_retry(client, "GET", "https://api.test/v1",
                                max_retries=2, backoff_base=0.01)
        assert client.request.call_count == 3  # 1 + 2 retries

    def test_404_raises_connector_error_no_retry(self):
        """4xx errors other than 401/403/429 should raise immediately."""
        client = MagicMock()
        client.request.return_value = _mock_response(404, text="Not Found")
        with pytest.raises(ConnectorError, match="API error 404"):
            _request_with_retry(client, "GET", "https://api.test/v1", max_retries=3)
        assert client.request.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 3. BASE MODULE — _paginate helper
# ═══════════════════════════════════════════════════════════════════════════════

class TestPaginate:
    """Generic cursor-based paginator from BaseConnector."""

    def test_single_page(self):
        conn = _ConcreteConnector()
        records = conn._paginate(lambda cursor: ([1, 2, 3], None))
        assert records == [1, 2, 3]

    def test_multiple_pages(self):
        pages = [
            ([1, 2], "cursor_a"),
            ([3, 4], "cursor_b"),
            ([5], None),
        ]
        page_idx = [0]

        def fetch_page(cursor):
            result = pages[page_idx[0]]
            page_idx[0] += 1
            return result

        conn = _ConcreteConnector()
        records = conn._paginate(fetch_page)
        assert records == [1, 2, 3, 4, 5]

    def test_max_pages_limits_iteration(self):
        """Should stop after max_pages even if cursor keeps returning."""
        conn = _ConcreteConnector()
        call_count = [0]

        def infinite_pages(cursor):
            call_count[0] += 1
            return ([call_count[0]], f"cursor_{call_count[0]}")

        records = conn._paginate(infinite_pages, max_pages=5)
        assert len(records) == 5
        assert call_count[0] == 5

    def test_empty_first_page(self):
        conn = _ConcreteConnector()
        records = conn._paginate(lambda cursor: ([], None))
        assert records == []

    def test_initial_cursor_passed(self):
        """initial_cursor should be forwarded to the first fetch_page call."""
        received_cursors = []

        def fetch_page(cursor):
            received_cursors.append(cursor)
            return ([1], None)

        conn = _ConcreteConnector()
        conn._paginate(fetch_page, initial_cursor="start_here")
        assert received_cursors[0] == "start_here"


# ═══════════════════════════════════════════════════════════════════════════════
# 4. STRIPE CONNECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class TestStripeValidateCredentials:

    @patch("connectors.stripe_connector.httpx.Client")
    @patch("connectors.stripe_connector._request_with_retry")
    def test_validate_success(self, mock_retry, mock_client_cls, stripe):
        mock_retry.return_value = _mock_response(200, {"id": "acct_123"})
        mock_client_cls.return_value.__enter__ = lambda s: MagicMock()
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        assert stripe.validate_credentials({"api_key": "sk_test_good"}) is True

    @patch("connectors.stripe_connector.httpx.Client")
    @patch("connectors.stripe_connector._request_with_retry")
    def test_validate_failure_auth_error(self, mock_retry, mock_client_cls, stripe):
        mock_retry.side_effect = ConnectorAuthError("bad key")
        mock_client_cls.return_value.__enter__ = lambda s: MagicMock()
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        assert stripe.validate_credentials({"api_key": "sk_test_bad"}) is False

    def test_validate_failure_missing_key(self, stripe):
        # No httpx call needed — empty key will raise inside _request_with_retry
        # but validate_credentials catches all exceptions
        assert stripe.validate_credentials({}) is False


class TestStripeExtract:

    def _mock_stripe_paginate(self, entity_data: dict):
        """Return a side_effect function for _stripe_paginate that returns
        data based on the URL."""
        def _side_effect(url, headers, extra=None):
            for key, records in entity_data.items():
                if key in url:
                    return records
            return []
        return _side_effect

    @patch.object(StripeConnector, "_stripe_paginate")
    def test_extract_returns_all_4_entity_types(self, mock_paginate, stripe):
        mock_paginate.side_effect = self._mock_stripe_paginate({
            "charges":       [{"id": "ch_1"}, {"id": "ch_2"}],
            "customers":     [{"id": "cus_1"}],
            "invoices":      [{"id": "in_1"}],
            "subscriptions": [{"id": "sub_1"}, {"id": "sub_2"}, {"id": "sub_3"}],
        })
        results = stripe.extract("ws_1", {"api_key": "sk_test_123"})
        assert len(results) == 4
        entity_types = {r["entity_type"] for r in results}
        assert entity_types == {"revenue", "customers", "invoices", "subscriptions"}
        # Check record counts
        by_type = {r["entity_type"]: r for r in results}
        assert len(by_type["revenue"]["records"]) == 2
        assert len(by_type["customers"]["records"]) == 1
        assert len(by_type["invoices"]["records"]) == 1
        assert len(by_type["subscriptions"]["records"]) == 3

    @patch.object(StripeConnector, "_stripe_paginate")
    def test_extract_partial_failure(self, mock_paginate, stripe):
        """If one entity fetch fails, others should still succeed."""
        call_count = [0]
        def _side_effect(url, headers, extra=None):
            call_count[0] += 1
            if "invoices" in url:
                raise ConnectorError("Invoices endpoint broken")
            return [{"id": f"rec_{call_count[0]}"}]

        mock_paginate.side_effect = _side_effect
        results = stripe.extract("ws_1", {"api_key": "sk_test_123"})
        assert len(results) == 4
        by_type = {r["entity_type"]: r for r in results}
        # Invoices should have an error and empty records
        assert by_type["invoices"]["records"] == []
        assert "error" in by_type["invoices"]
        # Others should succeed
        assert len(by_type["revenue"]["records"]) == 1
        assert len(by_type["customers"]["records"]) == 1
        assert len(by_type["subscriptions"]["records"]) == 1

    def test_extract_missing_api_key_raises(self, stripe):
        with pytest.raises(ConnectorError, match="API key is missing"):
            stripe.extract("ws_1", {})

    @patch("connectors.stripe_connector._request_with_retry")
    @patch("connectors.stripe_connector.httpx.Client")
    def test_stripe_pagination_multi_page(self, mock_client_cls, mock_retry, stripe):
        """_stripe_paginate should follow has_more until False."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = lambda s: mock_client
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # Page 1: has_more=True
        page1 = _mock_response(200, {
            "data": [{"id": "ch_1"}, {"id": "ch_2"}],
            "has_more": True,
        })
        # Page 2: has_more=False
        page2 = _mock_response(200, {
            "data": [{"id": "ch_3"}],
            "has_more": False,
        })
        mock_retry.side_effect = [page1, page2]

        records = stripe._stripe_paginate(
            "https://api.stripe.com/v1/charges",
            {"Authorization": "Bearer sk_test"},
        )
        assert len(records) == 3
        assert [r["id"] for r in records] == ["ch_1", "ch_2", "ch_3"]
        assert mock_retry.call_count == 2
        # Second call should include starting_after
        _, kwargs2 = mock_retry.call_args_list[1]
        assert kwargs2.get("params", {}).get("starting_after") == "ch_2"

    @patch("connectors.stripe_connector._request_with_retry")
    @patch("connectors.stripe_connector.httpx.Client")
    def test_stripe_pagination_single_page(self, mock_client_cls, mock_retry, stripe):
        """Single page (has_more=False) should make exactly one request."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = lambda s: mock_client
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_retry.return_value = _mock_response(200, {
            "data": [{"id": "ch_1"}],
            "has_more": False,
        })

        records = stripe._stripe_paginate(
            "https://api.stripe.com/v1/charges",
            {"Authorization": "Bearer sk_test"},
        )
        assert len(records) == 1
        assert mock_retry.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 5. QUICKBOOKS CONNECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class TestQuickBooksExtract:

    @patch.object(QuickBooksConnector, "_query")
    def test_extract_returns_all_6_entity_types(self, mock_query, quickbooks):
        """QuickBooks should return invoices, revenue, customers, expenses, employees, accounts."""
        mock_query.side_effect = lambda base, headers, entity: [
            {"Id": f"{entity}_1"}, {"Id": f"{entity}_2"}
        ]
        creds = {"access_token": "tok_abc", "realmId": "12345"}
        results = quickbooks.extract("ws_1", creds)
        assert len(results) == 6
        entity_types = {r["entity_type"] for r in results}
        assert entity_types == {
            "invoices", "revenue", "customers", "expenses", "employees", "accounts"
        }
        for r in results:
            assert len(r["records"]) == 2

    @patch.object(QuickBooksConnector, "_query")
    def test_extract_partial_failure(self, mock_query, quickbooks):
        """One entity failure should not prevent others from being fetched."""
        def _side_effect(base, headers, entity):
            if entity == "Purchase":
                raise ConnectorError("Purchase query timed out")
            return [{"Id": f"{entity}_1"}]

        mock_query.side_effect = _side_effect
        creds = {"access_token": "tok_abc", "realmId": "12345"}
        results = quickbooks.extract("ws_1", creds)
        assert len(results) == 6
        by_type = {r["entity_type"]: r for r in results}
        # expenses (Purchase) should have error
        assert by_type["expenses"]["records"] == []
        assert "error" in by_type["expenses"]
        # Others should succeed
        for etype in ["invoices", "revenue", "customers", "employees", "accounts"]:
            assert len(by_type[etype]["records"]) == 1

    @patch.object(QuickBooksConnector, "_query")
    def test_extract_auth_error_propagates(self, mock_query, quickbooks):
        """ConnectorAuthError from _query should propagate immediately
        and not be swallowed by partial-failure handling."""
        mock_query.side_effect = ConnectorAuthError("Token expired")
        creds = {"access_token": "tok_expired", "realmId": "12345"}
        with pytest.raises(ConnectorAuthError, match="Token expired"):
            quickbooks.extract("ws_1", creds)

    def test_extract_missing_credentials_raises(self, quickbooks):
        with pytest.raises(ConnectorError, match="missing access_token or realmId"):
            quickbooks.extract("ws_1", {"access_token": ""})

    def test_extract_missing_realm_id_raises(self, quickbooks):
        with pytest.raises(ConnectorError, match="missing access_token or realmId"):
            quickbooks.extract("ws_1", {"access_token": "tok_abc"})

    @patch("connectors.quickbooks_connector._request_with_retry")
    @patch("connectors.quickbooks_connector.httpx.Client")
    def test_query_pagination(self, mock_client_cls, mock_retry, quickbooks):
        """_query should paginate using STARTPOSITION / MAXRESULTS until
        a page returns fewer than _LIMIT records."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = lambda s: mock_client
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # Page 1: full page (1000 records) -> keep paginating
        page1_records = [{"Id": str(i)} for i in range(1000)]
        page1 = _mock_response(200, {
            "QueryResponse": {"Invoice": page1_records}
        })
        # Page 2: partial page (50 records) -> stop
        page2_records = [{"Id": str(i)} for i in range(1000, 1050)]
        page2 = _mock_response(200, {
            "QueryResponse": {"Invoice": page2_records}
        })
        mock_retry.side_effect = [page1, page2]

        records = quickbooks._query(
            "https://quickbooks.api.intuit.com/v3/company/12345",
            {"Authorization": "Bearer tok"},
            "Invoice",
        )
        assert len(records) == 1050
        assert mock_retry.call_count == 2


# ═══════════════════════════════════════════════════════════════════════════════
# 6. XERO CONNECTOR
# ═══════════════════════════════════════════════════════════════════════════════

class TestXeroExtract:

    @patch.object(XeroConnector, "_get")
    def test_extract_returns_all_4_entity_types(self, mock_get, xero):
        """Xero should return invoices, customers, revenue, accounts."""
        mock_get.side_effect = lambda headers, endpoint: [
            {"InvoiceID": f"{endpoint}_1"}
        ]
        creds = {"access_token": "tok_abc", "tenant_id": "tid_123"}
        results = xero.extract("ws_1", creds)
        assert len(results) == 4
        entity_types = {r["entity_type"] for r in results}
        assert entity_types == {"invoices", "customers", "revenue", "accounts"}
        for r in results:
            assert len(r["records"]) == 1

    @patch.object(XeroConnector, "_get")
    def test_extract_partial_failure(self, mock_get, xero):
        """One entity failure should not prevent others from being fetched."""
        def _side_effect(headers, endpoint):
            if endpoint == "Contacts":
                raise ConnectorError("Contacts fetch failed")
            return [{"ID": f"{endpoint}_1"}]

        mock_get.side_effect = _side_effect
        creds = {"access_token": "tok_abc", "tenant_id": "tid_123"}
        results = xero.extract("ws_1", creds)
        assert len(results) == 4
        by_type = {r["entity_type"]: r for r in results}
        assert by_type["customers"]["records"] == []
        assert "error" in by_type["customers"]
        for etype in ["invoices", "revenue", "accounts"]:
            assert len(by_type[etype]["records"]) == 1

    @patch.object(XeroConnector, "_get")
    def test_extract_auth_error_propagates(self, mock_get, xero):
        """ConnectorAuthError should propagate, not be caught as partial failure."""
        mock_get.side_effect = ConnectorAuthError("Invalid token")
        creds = {"access_token": "tok_bad", "tenant_id": "tid_123"}
        with pytest.raises(ConnectorAuthError, match="Invalid token"):
            xero.extract("ws_1", creds)

    def test_extract_missing_credentials_raises(self, xero):
        with pytest.raises(ConnectorError, match="missing access_token or tenant_id"):
            xero.extract("ws_1", {"access_token": ""})

    def test_extract_missing_tenant_id_raises(self, xero):
        with pytest.raises(ConnectorError, match="missing access_token or tenant_id"):
            xero.extract("ws_1", {"access_token": "tok_abc"})

    @patch("connectors.xero_connector._request_with_retry")
    @patch("connectors.xero_connector.httpx.Client")
    def test_page_based_pagination(self, mock_client_cls, mock_retry, xero):
        """_get should increment page number until a page returns < _LIMIT records."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = lambda s: mock_client
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        # Page 1: 100 records (full page) -> continue
        page1_records = [{"InvoiceID": str(i)} for i in range(100)]
        page1 = _mock_response(200, {"Invoices": page1_records})
        # Page 2: 30 records (partial page) -> stop
        page2_records = [{"InvoiceID": str(i)} for i in range(100, 130)]
        page2 = _mock_response(200, {"Invoices": page2_records})
        mock_retry.side_effect = [page1, page2]

        headers = {
            "Authorization": "Bearer tok",
            "Xero-tenant-id": "tid_123",
            "Accept": "application/json",
        }
        records = xero._get(headers, "Invoices")
        assert len(records) == 130
        assert mock_retry.call_count == 2
        # Verify page params
        _, kwargs1 = mock_retry.call_args_list[0]
        assert kwargs1["params"]["page"] == 1
        _, kwargs2 = mock_retry.call_args_list[1]
        assert kwargs2["params"]["page"] == 2

    @patch("connectors.xero_connector._request_with_retry")
    @patch("connectors.xero_connector.httpx.Client")
    def test_single_page_no_extra_requests(self, mock_client_cls, mock_retry, xero):
        """If first page returns < _LIMIT records, no second request should be made."""
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = lambda s: mock_client
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        page1_records = [{"InvoiceID": "1"}, {"InvoiceID": "2"}]
        mock_retry.return_value = _mock_response(200, {"Invoices": page1_records})

        records = xero._get(
            {"Authorization": "Bearer tok", "Xero-tenant-id": "tid"},
            "Invoices",
        )
        assert len(records) == 2
        assert mock_retry.call_count == 1


# ═══════════════════════════════════════════════════════════════════════════════
# 7. BASE MODULE — extract_safe wrapper
# ═══════════════════════════════════════════════════════════════════════════════

class TestExtractSafe:
    """extract_safe should catch ConnectorError but re-raise ConnectorAuthError."""

    def test_returns_results_on_success(self):
        conn = _ConcreteConnector()
        conn.extract = MagicMock(return_value=[{"entity_type": "test", "records": [1]}])
        result = conn.extract_safe("ws_1", {})
        assert result == [{"entity_type": "test", "records": [1]}]

    def test_catches_connector_error(self):
        conn = _ConcreteConnector()
        conn.extract = MagicMock(side_effect=ConnectorError("boom"))
        result = conn.extract_safe("ws_1", {})
        assert result == []

    def test_propagates_auth_error(self):
        conn = _ConcreteConnector()
        conn.extract = MagicMock(side_effect=ConnectorAuthError("bad creds"))
        with pytest.raises(ConnectorAuthError):
            conn.extract_safe("ws_1", {})
