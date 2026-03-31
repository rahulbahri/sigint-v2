"""
core/security.py — Rate limiting middleware and email validation helpers.
"""
import time as _time
from collections import defaultdict
from fastapi import Request
from fastapi.responses import JSONResponse

from core.state import _rate_limit_store, _MAGIC_LINK_RATE


def _check_magic_link_rate(email: str) -> bool:
    """Max 5 magic link requests per email per hour."""
    now = _time.time()
    _MAGIC_LINK_RATE[email] = [t for t in _MAGIC_LINK_RATE[email] if now - t < 3600]
    if len(_MAGIC_LINK_RATE[email]) >= 5:
        return False
    _MAGIC_LINK_RATE[email].append(now)
    return True


def _rate_limit(client_ip: str, limit: int = 60, window: int = 60) -> bool:
    """Returns True if request is allowed, False if rate limited."""
    now = _time.time()
    calls = _rate_limit_store[client_ip]
    # Remove calls outside the window
    _rate_limit_store[client_ip] = [t for t in calls if now - t < window]
    if len(_rate_limit_store[client_ip]) >= limit:
        return False
    _rate_limit_store[client_ip].append(now)
    return True


async def rate_limit_middleware(request: Request, call_next):
    # Only rate-limit API routes, not static assets
    if request.url.path.startswith("/api/"):
        client_ip = request.client.host if request.client else "unknown"
        # More strict limit for heavy endpoints
        heavy = any(x in request.url.path for x in ["/forecast/build", "/ontology/discover", "/export/board-deck"])
        limit = 10 if heavy else 120
        if not _rate_limit(client_ip, limit=limit, window=60):
            return JSONResponse(status_code=429, content={"detail": "Too many requests. Please slow down."})
    return await call_next(request)


