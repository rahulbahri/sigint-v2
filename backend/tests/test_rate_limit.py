"""
tests/test_rate_limit.py — Rate limiting fires at the configured threshold.

The middleware uses `request.client.host` for the IP key; httpx ASGITransport
sets `request.client` to None, so the middleware falls back to "unknown".
All 121 requests share the same key ("unknown"), which is what we want to
trigger the limit.

Rate limit is 120 requests per 60-second window for normal endpoints.
The 121st request should return 429.
"""
import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_rate_limit_fires_at_limit(client: AsyncClient):
    """121st request to /api/health from the same apparent IP → 429.

    The autouse reset_rate_limit fixture in conftest.py clears the store
    before this test, so the count starts at 0.
    """

    limit = 120
    responses = []
    for _ in range(limit + 1):
        resp = await client.get("/api/health")
        responses.append(resp.status_code)

    ok_responses   = [s for s in responses if s == 200]
    rate_limited   = [s for s in responses if s == 429]

    assert len(ok_responses) == limit, (
        f"Expected exactly {limit} successful responses before rate limiting, "
        f"got {len(ok_responses)}"
    )
    assert len(rate_limited) >= 1, (
        "Expected at least one 429 after the limit was exceeded"
    )
    # The very last response must be 429
    assert responses[-1] == 429
