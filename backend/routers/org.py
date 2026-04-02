"""
routers/org.py — Org/team management endpoints (/api/org/*).
"""
import secrets
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel as _BM2

from core.config import APP_URL
from core.database import get_db
from core.deps import _get_workspace, _get_user_email
from core.config import _is_free_email, _email_domain
from core.email import _send_magic_link_email

router = APIRouter()


@router.get("/api/org", tags=["Org"])
def get_org(request: Request):
    """Return current org info + member list."""
    workspace_id = _get_workspace(request)
    user_email   = _get_user_email(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    try:
        conn  = get_db()
        org   = conn.execute("SELECT * FROM organisations WHERE id=?", [workspace_id]).fetchone()
        members = conn.execute(
            "SELECT email, role, display_name, status, last_login, created_at FROM users "
            "WHERE org_id=? AND status != 'removed' ORDER BY created_at ASC",
            [workspace_id],
        ).fetchall()
        try:
            invites = conn.execute(
                "SELECT email, invited_by, created_at, expires_at FROM org_invites "
                "WHERE org_id=? AND accepted=0 AND expires_at > datetime('now') ORDER BY created_at DESC",
                [workspace_id],
            ).fetchall()
        except Exception:
            # org_invites table may not exist yet
            invites = []
        conn.close()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load team data: {e}")
    return {
        "org": {
            "id":          workspace_id,
            "name":        org["name"] if org else workspace_id,
            "plan":        org["plan"] if org else "free",
            "invite_only": bool(org["invite_only"]) if org and "invite_only" in org.keys() else False,
        },
        "members": [
            {
                "email":        r["email"],
                "role":         r["role"] or "member",
                "display_name": (r["display_name"] or "") if "display_name" in r.keys() else "",
                "status":       r["status"] or "active",
                "last_login":   r["last_login"] or "",
                "is_you":       r["email"] == user_email,
            }
            for r in members
        ],
        "pending_invites": [
            {"email": r["email"], "invited_by": r["invited_by"], "sent_at": r["created_at"]}
            for r in invites
        ],
    }


class _InviteRequest(_BM2):
    email: str
    role:  str = "member"


@router.post("/api/org/invite", tags=["Org"])
async def invite_member(body: _InviteRequest, request: Request):
    """Send an invite email to add a new member. Admin only."""
    workspace_id = _get_workspace(request)
    user_email   = _get_user_email(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    # Check caller is admin
    conn = get_db()
    caller = conn.execute("SELECT role FROM users WHERE email=?", [user_email]).fetchone()
    if not caller or caller["role"] != "admin":
        conn.close()
        raise HTTPException(status_code=403, detail="Only admins can invite members")
    invite_email = body.email.strip().lower()
    # Block free email providers (gmail, yahoo, etc.) but allow any work domain
    if not _email_domain(invite_email) or _is_free_email(invite_email):
        conn.close()
        raise HTTPException(status_code=400, detail="Please use a work email address")
    # Create invite token
    inv_token  = secrets.token_urlsafe(32)
    expires_at = (datetime.utcnow() + timedelta(days=7)).isoformat()
    conn.execute(
        "INSERT OR REPLACE INTO org_invites (org_id, email, invited_by, token, expires_at) "
        "VALUES (?,?,?,?,?)",
        [workspace_id, invite_email, user_email, inv_token, expires_at],
    )
    # Pre-create user with pending status so they appear in member list
    conn.execute(
        "INSERT OR IGNORE INTO users (email, org_id, role, status, invited_by) "
        "VALUES (?,?,?,?,?)",
        [invite_email, workspace_id, body.role, "invited", user_email],
    )
    conn.commit()
    conn.close()
    # Send invite email
    invite_url = f"{APP_URL}/api/auth/accept-invite/{inv_token}"
    await _send_magic_link_email(
        invite_email,
        invite_url,
        subject=f"You've been invited to join {workspace_id} on AxiomSync",
        body_override=(
            f"{user_email} has invited you to join their AxiomSync workspace.\n\n"
            f"Click the link below to accept and sign in:\n{invite_url}\n\n"
            "This link expires in 7 days."
        ),
    )
    return {"status": "invited", "email": invite_email}


class _MemberUpdateRequest(_BM2):
    role: str


@router.put("/api/org/members/{member_email}", tags=["Org"])
async def update_member(member_email: str, body: _MemberUpdateRequest, request: Request):
    """Change a member's role. Admin only."""
    workspace_id = _get_workspace(request)
    user_email   = _get_user_email(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    caller = conn.execute("SELECT role FROM users WHERE email=?", [user_email]).fetchone()
    if not caller or caller["role"] != "admin":
        conn.close()
        raise HTTPException(status_code=403, detail="Only admins can change roles")
    if body.role not in ("admin", "member"):
        raise HTTPException(status_code=400, detail="role must be 'admin' or 'member'")
    conn.execute(
        "UPDATE users SET role=? WHERE email=? AND org_id=?",
        [body.role, member_email, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "updated"}


@router.delete("/api/org/members/{member_email}", tags=["Org"])
async def remove_member(member_email: str, request: Request):
    """Remove a member from the org. Admin only. Cannot remove yourself."""
    workspace_id = _get_workspace(request)
    user_email   = _get_user_email(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if member_email == user_email:
        raise HTTPException(status_code=400, detail="You cannot remove yourself")
    conn = get_db()
    caller = conn.execute("SELECT role FROM users WHERE email=?", [user_email]).fetchone()
    if not caller or caller["role"] != "admin":
        conn.close()
        raise HTTPException(status_code=403, detail="Only admins can remove members")
    conn.execute(
        "UPDATE users SET status='removed' WHERE email=? AND org_id=?",
        [member_email, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "removed"}
