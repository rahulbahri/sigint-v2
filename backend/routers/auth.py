"""
routers/auth.py — Magic link authentication endpoints.
"""
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse
from pydantic import BaseModel as _BM2

from core.config import APP_URL, JWT_SECRET, _JWT_AVAILABLE, _org_id_for_email, _is_work_email
from core.database import get_db, _migrate_workspace_data
from core.email import _send_magic_link_email
from core.security import _check_magic_link_rate
from core.state import _MAGIC_TOKEN_CACHE

try:
    import jwt as _jwt
except ImportError:
    _jwt = None

router = APIRouter()


class MagicLinkRequest(_BM2):
    email: str


class VerifyTokenRequest(_BM2):
    token: str


@router.post("/api/auth/request-link")
async def request_magic_link(body: MagicLinkRequest, request: Request):
    email = body.email.strip().lower()
    if not _is_work_email(email):
        raise HTTPException(status_code=403, detail="Please use your work email address. Free email providers (Gmail, Yahoo, etc.) are not supported.")
    if not _check_magic_link_rate(email):
        raise HTTPException(status_code=429, detail="Too many sign-in requests. Please wait an hour before trying again.")
    token = secrets.token_urlsafe(32)
    expires = (datetime.utcnow() + timedelta(minutes=15)).isoformat()
    try:
        conn = get_db()
        conn.execute(
            "INSERT INTO magic_tokens (email, token, expires_at) VALUES (?,?,?)",
            [email, token, expires]
        )
        conn.commit()
        conn.close()
    except Exception as _db_err:
        print(f"[WARN] Could not persist magic token ({_db_err}), using in-memory fallback")
        _MAGIC_TOKEN_CACHE[token] = {"email": email, "expires_at": expires}
    magic_url = f"{APP_URL}/api/auth/login/{token}"
    sent = await _send_magic_link_email(email, magic_url)
    return {"message": "Magic link sent" if sent else "Magic link generated (email not configured)", "sent": sent}


@router.post("/api/auth/verify")
async def verify_magic_token(body: VerifyTokenRequest):
    email = None
    # Check in-memory cache first (DB-unavailable fallback)
    if body.token in _MAGIC_TOKEN_CACHE:
        cached = _MAGIC_TOKEN_CACHE.pop(body.token)
        expires = datetime.fromisoformat(cached["expires_at"])
        if datetime.utcnow() > expires:
            raise HTTPException(status_code=401, detail="Token expired")
        email = cached["email"]
        try:
            _c = get_db()
            _c.execute("UPDATE magic_tokens SET used=1 WHERE token=?", [body.token])
            _c.commit()
            _c.close()
        except Exception:
            pass
    else:
        try:
            conn = get_db()
            row = conn.execute(
                "SELECT * FROM magic_tokens WHERE token=? AND used=0", [body.token]
            ).fetchone()
            if not row:
                raise HTTPException(status_code=401, detail="Invalid or already-used token")
            expires = datetime.fromisoformat(str(row["expires_at"]))
            if datetime.utcnow() > expires:
                raise HTTPException(status_code=401, detail="Token expired")
            email = str(row["email"])
            conn.execute("UPDATE magic_tokens SET used=1 WHERE token=?", [body.token])
            try:
                conn.execute("INSERT OR IGNORE INTO users (email) VALUES (?)", [email])
                conn.execute("UPDATE users SET last_login=? WHERE email=?",
                             [datetime.utcnow().isoformat(), email])
            except Exception:
                pass
            conn.commit()
            conn.close()
        except HTTPException:
            raise
        except Exception as _ve:
            raise HTTPException(status_code=500, detail="Verification failed. Please request a new sign-in link.")
    org_id = _org_id_for_email(email)
    # Ensure org exists
    try:
        conn2 = get_db()
        conn2.execute(
            "INSERT OR IGNORE INTO organisations (id, name) VALUES (?,?)",
            [org_id, org_id],
        )
        # Determine role: first user in org = admin, rest = member
        existing = conn2.execute(
            "SELECT COUNT(*) as c FROM users WHERE org_id=? AND status='active'",
            [org_id],
        ).fetchone()
        role = "admin" if (not existing or existing["c"] == 0) else "member"
        conn2.execute(
            "INSERT OR IGNORE INTO users (email, org_id, role) VALUES (?,?,?)",
            [email, org_id, role],
        )
        conn2.execute(
            "UPDATE users SET last_login=?, org_id=?, role=COALESCE(NULLIF(role,''), ?) WHERE email=?",
            [datetime.utcnow().isoformat(), org_id, role, email],
        )
        # One-time migration: re-tag old per-email workspace data to org workspace
        migrated = conn2.execute(
            "SELECT org_migrated FROM users WHERE email=?", [email]
        ).fetchone()
        if migrated and not migrated["org_migrated"]:
            _migrate_workspace_data(conn2, email, org_id)
            conn2.execute(
                "UPDATE users SET org_migrated=1 WHERE email=?", [email]
            )
        conn2.commit()
        # Fetch confirmed role
        user_row = conn2.execute("SELECT role FROM users WHERE email=?", [email]).fetchone()
        role = user_row["role"] if user_row else "member"
        conn2.close()
    except Exception as _oe:
        print(f"[Auth] Org setup warning: {_oe}")
        role = "member"

    if not _JWT_AVAILABLE:
        raise HTTPException(status_code=500, detail="JWT library not available")
    payload = {
        "email":  email,
        "org_id": org_id,
        "role":   role,
        "exp":    datetime.utcnow() + timedelta(days=30),
        "iat":    datetime.utcnow(),
    }
    token_str = _jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    return {"token": token_str, "email": email, "org_id": org_id, "role": role}


@router.get("/api/auth/login/{token}")
async def magic_link_redirect(token: str):
    """Server-side magic link handler — verifies token, sets cookie, redirects to app."""
    email = None
    if token in _MAGIC_TOKEN_CACHE:
        cached = _MAGIC_TOKEN_CACHE.pop(token)
        expires = datetime.fromisoformat(cached["expires_at"])
        if datetime.utcnow() > expires:
            return RedirectResponse(url=f"{APP_URL}/?auth_error=expired")
        email = cached["email"]
        # Belt-and-suspenders: mark used in DB too in case the INSERT succeeded
        try:
            _c = get_db()
            _c.execute("UPDATE magic_tokens SET used=1 WHERE token=?", [token])
            _c.commit()
            _c.close()
        except Exception:
            pass
    else:
        try:
            conn = get_db()
            row = conn.execute(
                "SELECT * FROM magic_tokens WHERE token=? AND used=0", [token]
            ).fetchone()
            if not row:
                return RedirectResponse(url=f"{APP_URL}/?auth_error=invalid")
            expires = datetime.fromisoformat(str(row["expires_at"]))
            if datetime.utcnow() > expires:
                return RedirectResponse(url=f"{APP_URL}/?auth_error=expired")
            email = str(row["email"])
            conn.execute("UPDATE magic_tokens SET used=1 WHERE token=?", [token])
            try:
                conn.execute("INSERT OR IGNORE INTO users (email) VALUES (?)", [email])
                conn.execute("UPDATE users SET last_login=? WHERE email=?",
                             [datetime.utcnow().isoformat(), email])
            except Exception:
                pass
            conn.commit()
            conn.close()
        except Exception as _e:
            return RedirectResponse(url=f"{APP_URL}/?auth_error=error")
    if not _JWT_AVAILABLE:
        return RedirectResponse(url=f"{APP_URL}/?auth_error=config")
    org_id = _org_id_for_email(email)
    # Ensure org + user exist, run migration
    try:
        _conn = get_db()
        _conn.execute("INSERT OR IGNORE INTO organisations (id, name) VALUES (?,?)", [org_id, org_id])
        existing = _conn.execute("SELECT COUNT(*) as c FROM users WHERE org_id=? AND status='active'", [org_id]).fetchone()
        role = "admin" if (not existing or existing["c"] == 0) else "member"
        _conn.execute("INSERT OR IGNORE INTO users (email, org_id, role) VALUES (?,?,?)", [email, org_id, role])
        _conn.execute("UPDATE users SET last_login=?, org_id=?, role=COALESCE(NULLIF(role,''), ?) WHERE email=?",
                      [datetime.utcnow().isoformat(), org_id, role, email])
        migrated = _conn.execute("SELECT org_migrated FROM users WHERE email=?", [email]).fetchone()
        if migrated and not migrated["org_migrated"]:
            _migrate_workspace_data(_conn, email, org_id)
            _conn.execute("UPDATE users SET org_migrated=1 WHERE email=?", [email])
        _conn.commit()
        user_row = _conn.execute("SELECT role FROM users WHERE email=?", [email]).fetchone()
        role = user_row["role"] if user_row else "member"
        _conn.close()
    except Exception:
        role = "admin"
    payload = {
        "email":  email,
        "org_id": org_id,
        "role":   role,
        "exp":    datetime.utcnow() + timedelta(days=30),
        "iat":    datetime.utcnow(),
    }
    jwt_token = _jwt.encode(payload, JWT_SECRET, algorithm="HS256")
    # Redirect to frontend with JWT in hash fragment (never sent to server, Safari-safe)
    response = RedirectResponse(url=f"{APP_URL}/#jwt={jwt_token}", status_code=302)
    response.set_cookie(
        key="axiom_session",
        value=jwt_token,
        max_age=30*24*3600,  # 30 days
        path="/",
        samesite="lax",
        secure=True,
        httponly=True   # JS reads JWT from the #jwt= hash fragment on first load; cookie is server-only
    )
    return response


@router.get("/api/auth/me")
async def auth_me(request: Request):
    auth_header = request.headers.get("Authorization", "")
    token_str = auth_header.replace("Bearer ", "").strip()
    if not token_str:
        token_str = request.cookies.get("axiom_session", "")
    if not token_str:
        raise HTTPException(status_code=401, detail="No token provided")
    if not _JWT_AVAILABLE:
        raise HTTPException(status_code=500, detail="JWT library not available")
    try:
        payload = _jwt.decode(token_str, JWT_SECRET, algorithms=["HS256"])
        return {"email": payload["email"], "role": payload.get("role", "admin"), "valid": True}
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


@router.post("/api/auth/logout")
async def logout():
    from fastapi.responses import JSONResponse
    response = JSONResponse({"message": "Logged out"})
    response.delete_cookie("axiom_session", path="/")
    return response


@router.get("/api/auth/accept-invite/{token}", tags=["Auth"])
async def accept_invite(token: str):
    """Accept an org invite — sets workspace, redirects to app."""
    conn = get_db()
    inv = conn.execute(
        "SELECT * FROM org_invites WHERE token=? AND accepted=0 AND expires_at > datetime('now')",
        [token],
    ).fetchone()
    if not inv:
        conn.close()
        return RedirectResponse(url=f"{APP_URL}/?auth_error=invite_expired")
    email  = inv["email"]
    org_id = inv["org_id"]
    conn.execute("UPDATE org_invites SET accepted=1 WHERE token=?", [token])
    conn.execute(
        "UPDATE users SET org_id=?, status='active', org_migrated=1 WHERE email=?",
        [org_id, email],
    )
    conn.execute("INSERT OR IGNORE INTO users (email, org_id, role, status) VALUES (?,?,?,?)",
                 [email, org_id, inv["role"] if "role" in inv.keys() else "member", "active"])
    conn.commit()
    conn.close()
    # Issue a magic link token so they get logged straight in
    new_token  = secrets.token_urlsafe(32)
    expires    = (datetime.utcnow() + timedelta(minutes=15)).isoformat()
    conn2 = get_db()
    conn2.execute("INSERT INTO magic_tokens (email, token, expires_at) VALUES (?,?,?)",
                  [email, new_token, expires])
    conn2.commit()
    conn2.close()
    return RedirectResponse(url=f"{APP_URL}/api/auth/login/{new_token}")
