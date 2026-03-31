"""
core/deps.py — FastAPI dependency helpers: _get_workspace(), _get_user_email().
"""
from fastapi import Request
from core.config import JWT_SECRET


def _get_workspace(request: Request) -> str:
    """
    Extract workspace_id from JWT.
    Returns org_id (domain for work emails, email for personal) so that
    all members of the same organisation share a single workspace.
    """
    token_str = ""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token_str = auth_header[7:].strip()
    if not token_str:
        token_str = request.cookies.get("axiom_session", "")
    if not token_str:
        return ""
    try:
        import jwt as _j
        payload = _j.decode(token_str, JWT_SECRET, algorithms=["HS256"])
        # Prefer org_id (new tokens); fall back to email (legacy tokens)
        return payload.get("org_id") or payload.get("email", "")
    except Exception:
        return ""


def _get_user_email(request: Request) -> str:
    """Extract the individual user's email from JWT (not the org/workspace id)."""
    token_str = ""
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token_str = auth_header[7:].strip()
    if not token_str:
        token_str = request.cookies.get("axiom_session", "")
    if not token_str:
        return ""
    try:
        import jwt as _j
        payload = _j.decode(token_str, JWT_SECRET, algorithms=["HS256"])
        return payload.get("email", "")
    except Exception:
        return ""


def _require_workspace(request: Request) -> str:
    """
    Like _get_workspace() but raises HTTP 401 instead of returning ''.
    Use this in any endpoint that must not serve data to unauthenticated callers.
    """
    from fastapi import HTTPException
    ws = _get_workspace(request)
    if not ws:
        raise HTTPException(status_code=401, detail="Not authenticated")
    return ws
