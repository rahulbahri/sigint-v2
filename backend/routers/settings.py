"""
routers/settings.py — Company settings, logo upload, workspace data reset (/api/company-settings, /api/workspace/data).
"""
import base64

from fastapi import APIRouter, HTTPException, Request, UploadFile, File

from core.database import get_db
from core.deps import _get_workspace

router = APIRouter()

# ─── Company Settings ────────────────────────────────────────────────────────

@router.get("/api/company-settings", tags=["Settings"])
def get_company_settings(request: Request):
    workspace_id = _get_workspace(request)
    conn = get_db()
    rows = conn.execute("SELECT key, value FROM company_settings WHERE workspace_id=?", [workspace_id]).fetchall()
    conn.close()
    return {r["key"]: r["value"] for r in rows}


@router.put("/api/company-settings", tags=["Settings"])
async def update_company_settings(request: Request):
    workspace_id = _get_workspace(request)
    body = await request.json()
    conn = get_db()
    for k, v in body.items():
        conn.execute(
            "INSERT OR REPLACE INTO company_settings (key, value, workspace_id) VALUES (?,?,?)",
            (k, str(v), workspace_id)
        )
    conn.commit()
    conn.close()
    return {"status": "ok"}


_ALLOWED_LOGO_MIME = {"image/png", "image/jpeg", "image/jpg", "image/svg+xml",
                      "image/webp", "image/gif"}
_MAX_LOGO_BYTES = 5 * 1024 * 1024  # 5 MB hard limit


@router.post("/api/company-settings/logo", tags=["Settings"])
async def upload_logo(request: Request, file: UploadFile = File(...)):
    workspace_id = _get_workspace(request)

    # Read file fully so we can check size
    contents = await file.read()

    # Validate file size
    if len(contents) > _MAX_LOGO_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"Logo too large ({len(contents)//1024}KB). Maximum allowed size is 5MB."
        )

    # Validate MIME type — trust content_type but fall back to sniffing magic bytes
    mime = (file.content_type or "").lower().split(";")[0].strip()
    if not mime or mime == "application/octet-stream":
        # Sniff common image magic bytes
        if contents[:8] == b"\x89PNG\r\n\x1a\n":
            mime = "image/png"
        elif contents[:3] == b"\xff\xd8\xff":
            mime = "image/jpeg"
        elif contents[:4] == b"<svg" or b"<svg" in contents[:64]:
            mime = "image/svg+xml"
        elif contents[:4] in (b"RIFF", b"WEBP"):
            mime = "image/webp"
        else:
            mime = "image/png"  # default fallback

    if mime not in _ALLOWED_LOGO_MIME:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported file type '{mime}'. Please upload a PNG, JPG, SVG, or WebP image."
        )

    data_url = f"data:{mime};base64," + base64.b64encode(contents).decode()
    conn = get_db()
    conn.execute(
        "INSERT OR REPLACE INTO company_settings (key, value, workspace_id) VALUES (?,?,?)",
        ("logo", data_url, workspace_id)
    )
    conn.commit()
    conn.close()
    return {"status": "ok", "logo": data_url}


# ─── Workspace Data Reset ─────────────────────────────────────────────────────

@router.delete("/api/workspace/data", tags=["Settings"])
async def delete_workspace_data(request: Request):
    """Delete ALL data for the current workspace. Irreversible."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Authentication required")
    conn = get_db()
    try:
        for tbl in ["monthly_data", "uploads", "kpi_targets", "projection_monthly_data",
                    "projection_uploads", "kpi_accountability", "annotations",
                    "recommendation_outcomes", "audit_log", "company_settings"]:
            conn.execute(f"DELETE FROM {tbl} WHERE workspace_id=?", [workspace_id])
        conn.commit()
        conn.close()
        return {"message": "All workspace data deleted successfully"}
    except Exception:
        conn.close()
        raise HTTPException(status_code=500, detail="Failed to delete workspace data")
