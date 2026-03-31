"""
routers/admin.py — Admin-only endpoints (/api/admin/*).
"""
from fastapi import APIRouter, HTTPException, Request

from core.config import ADMIN_EMAIL
from core.database import get_db
from core.deps import _get_user_email, _get_workspace

router = APIRouter()


def _require_admin(request: Request) -> str:
    """Allow access only if the authenticated user's email matches ADMIN_EMAIL."""
    email = _get_user_email(request)
    if not email:
        raise HTTPException(status_code=401, detail="Not authenticated")
    if email != ADMIN_EMAIL:
        raise HTTPException(status_code=403, detail="Admin access required")
    return email


@router.get("/api/admin/stats", tags=["Admin"])
async def admin_stats(request: Request):
    _require_admin(request)
    conn = get_db()
    try:
        total_workspaces = conn.execute("SELECT COUNT(DISTINCT workspace_id) FROM monthly_data WHERE workspace_id != ''").fetchone()[0]
        total_uploads    = conn.execute("SELECT COUNT(*) FROM uploads").fetchone()[0]
        total_datapoints = conn.execute("SELECT COUNT(*) FROM monthly_data").fetchone()[0]
        total_users      = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        recent_raw       = conn.execute("SELECT email, last_login FROM users WHERE last_login IS NOT NULL ORDER BY last_login DESC LIMIT 5").fetchall()
        conn.close()
        return {
            "total_workspaces": total_workspaces,
            "total_uploads": total_uploads,
            "total_data_points": total_datapoints,
            "total_users": total_users,
            "recent_logins": [{"email": str(r["email"]), "last_login": str(r["last_login"])} for r in recent_raw],
        }
    except Exception:
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to fetch stats")


@router.get("/api/admin/workspaces", tags=["Admin"])
async def admin_list_workspaces(request: Request):
    _require_admin(request)
    conn = get_db()
    try:
        ws_rows = conn.execute("SELECT DISTINCT workspace_id FROM monthly_data WHERE workspace_id != ''").fetchall()
        users_raw = conn.execute("SELECT email, role, created_at, last_login FROM users").fetchall()
        users_map = {str(u["email"]): u for u in users_raw}
        result = []
        seen = set()
        for row in ws_rows:
            ws = str(row["workspace_id"])
            seen.add(ws)
            dp  = conn.execute("SELECT COUNT(*) FROM monthly_data WHERE workspace_id=?", [ws]).fetchone()[0]
            upl = conn.execute("SELECT COUNT(*) FROM uploads WHERE workspace_id=?", [ws]).fetchone()[0]
            last_upl = conn.execute("SELECT uploaded_at FROM uploads WHERE workspace_id=? ORDER BY id DESC LIMIT 1", [ws]).fetchone()
            audit_ct = conn.execute("SELECT COUNT(*) FROM audit_log WHERE workspace_id=?", [ws]).fetchone()[0]
            u = users_map.get(ws, {})
            result.append({"email": ws, "data_points": dp, "uploads": upl,
                           "last_upload": str(last_upl["uploaded_at"]) if last_upl else None,
                           "last_login": str(u.get("last_login","")) if u else "",
                           "created_at": str(u.get("created_at","")) if u else "",
                           "role": str(u.get("role","user")) if u else "user",
                           "audit_events": audit_ct})
        for email, u in users_map.items():
            if email not in seen:
                result.append({"email": email, "data_points": 0, "uploads": 0,
                               "last_upload": None, "last_login": str(u.get("last_login","")),
                               "created_at": str(u.get("created_at","")),
                               "role": str(u.get("role","user")), "audit_events": 0})
        conn.close()
        return {"workspaces": result, "total": len(result)}
    except Exception:
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to fetch workspaces")


@router.get("/api/admin/connector-health", tags=["Admin"])
async def admin_connector_health(request: Request):
    """Per-workspace connector status for admin view."""
    _require_admin(request)
    conn = get_db()
    try:
        # Check if connector_configs table exists
        exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='connector_configs'"
        ).fetchone()
        if not exists:
            return {"workspaces": []}

        rows = conn.execute("""
            SELECT workspace_id, source_name, sync_status, last_sync_at, last_error
            FROM connector_configs
            ORDER BY workspace_id, source_name
        """).fetchall()

        # Group by workspace
        ws_map = {}
        for ws_id, source, status, last_sync, last_err in rows:
            if ws_id not in ws_map:
                ws_map[ws_id] = {"workspace_id": ws_id, "connectors": [], "healthy": 0, "errored": 0, "total": 0}
            ws_map[ws_id]["connectors"].append({
                "source": source, "status": status,
                "last_sync_at": last_sync, "last_error": last_err
            })
            ws_map[ws_id]["total"] += 1
            if status == "ok":
                ws_map[ws_id]["healthy"] += 1
            elif status == "error":
                ws_map[ws_id]["errored"] += 1

        return {"workspaces": list(ws_map.values())}
    finally:
        conn.close()


@router.delete("/api/admin/workspace/{email}", tags=["Admin"])
async def admin_delete_workspace(email: str, request: Request):
    _require_admin(request)
    if email == ADMIN_EMAIL:
        raise HTTPException(status_code=400, detail="Cannot delete the admin workspace")
    conn = get_db()
    try:
        for tbl in ["monthly_data","uploads","kpi_targets","projection_monthly_data",
                    "projection_uploads","kpi_accountability","annotations",
                    "recommendation_outcomes","audit_log","company_settings"]:
            conn.execute(f"DELETE FROM {tbl} WHERE workspace_id=?", [email])
        conn.execute("DELETE FROM users WHERE email=?", [email])
        conn.commit()
        conn.close()
        return {"message": f"Workspace {email} deleted"}
    except Exception:
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to delete workspace")
