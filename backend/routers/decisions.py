"""
routers/decisions.py — Decision log CRUD (/api/decisions/*).
"""
import json

from fastapi import APIRouter, HTTPException, Request

from core.database import get_db, _audit
from core.deps import _get_workspace

router = APIRouter()


# ── Helpers ──────────────────────────────────────────────────────────────────

def _latest_kpi_values(conn, workspace_id: str, kpi_keys: list[str]) -> dict:
    """
    Fetch the most recent monthly_data row for the workspace and extract
    the current values of the given KPI keys.  Returns {kpi_key: value}.
    """
    if not kpi_keys:
        return {}
    row = conn.execute(
        "SELECT data_json FROM monthly_data "
        "WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 1",
        [workspace_id],
    ).fetchone()
    if not row:
        return {}
    try:
        data = json.loads(row["data_json"]) if isinstance(row["data_json"], str) else row["data_json"]
    except (json.JSONDecodeError, TypeError):
        return {}
    return {k: data[k] for k in kpi_keys if k in data}


# ── GET /api/decisions ───────────────────────────────────────────────────────

@router.get("/api/decisions")
async def get_decisions(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    rows = conn.execute(
        "SELECT id, title, the_decision, rationale, kpi_context, outcome, "
        "decided_by, status, decided_at, kpi_snapshot, resolved_kpi_snapshot "
        "FROM decisions WHERE workspace_id=? "
        "ORDER BY decided_at DESC",
        [workspace_id]
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id":                    r["id"],
            "title":                 r["title"],
            "the_decision":          r["the_decision"],
            "rationale":             r["rationale"] or "",
            "kpi_context":           json.loads(r["kpi_context"] or "[]"),
            "outcome":               r["outcome"] or "",
            "decided_by":            r["decided_by"] or "CFO",
            "status":                r["status"] or "active",
            "decided_at":            r["decided_at"] or "",
            "kpi_snapshot":          json.loads(r["kpi_snapshot"] or "{}"),
            "resolved_kpi_snapshot": json.loads(r["resolved_kpi_snapshot"] or "{}"),
        })
    return {"decisions": result}


# ── POST /api/decisions ──────────────────────────────────────────────────────

@router.post("/api/decisions")
async def create_decision(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()
    title        = (body.get("title") or "").strip()
    the_decision = (body.get("the_decision") or "").strip()
    rationale    = (body.get("rationale") or "").strip()
    kpi_list     = body.get("kpi_context") or []
    kpi_context  = json.dumps(kpi_list)
    decided_by   = (body.get("decided_by") or "CFO").strip()
    if not title or not the_decision:
        raise HTTPException(status_code=400, detail="title and the_decision are required")

    conn = get_db()

    # Snapshot current KPI values at decision time
    snapshot = json.dumps(_latest_kpi_values(conn, workspace_id, kpi_list))

    cur = conn.execute(
        "INSERT INTO decisions "
        "(workspace_id, title, the_decision, rationale, kpi_context, decided_by, kpi_snapshot) "
        "VALUES (?,?,?,?,?,?,?)",
        [workspace_id, title, the_decision, rationale, kpi_context, decided_by, snapshot]
    )
    conn.commit()
    new_id = cur.lastrowid
    conn.close()

    _audit("decision_created", "decision", str(new_id),
           f"Decision logged: {title}",
           workspace_id=workspace_id)

    return {"id": new_id, "status": "created"}


# ── PUT /api/decisions/:id ───────────────────────────────────────────────────

@router.put("/api/decisions/{decision_id}")
async def update_decision(decision_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()

    # Build dynamic SET clause for fields that were actually sent
    updates: dict[str, str | None] = {}
    for field in ("status", "outcome", "title", "rationale", "the_decision", "decided_by"):
        if field in body:
            updates[field] = (body[field] or "").strip() if isinstance(body[field], str) else body[field]

    if "kpi_context" in body:
        updates["kpi_context"] = json.dumps(body["kpi_context"] or [])

    if not updates:
        raise HTTPException(status_code=400, detail="No fields to update")

    conn = get_db()

    # If KPI links changed, re-snapshot current values
    if "kpi_context" in body:
        new_kpis = body["kpi_context"] or []
        updates["kpi_snapshot"] = json.dumps(
            _latest_kpi_values(conn, workspace_id, new_kpis)
        )

    # If resolving/reversing, capture resolved snapshot for before/after comparison
    if body.get("status") in ("resolved", "reversed"):
        existing = conn.execute(
            "SELECT kpi_context FROM decisions WHERE id=? AND workspace_id=?",
            [decision_id, workspace_id],
        ).fetchone()
        if existing:
            kpi_keys = json.loads(existing["kpi_context"] or "[]")
            updates["resolved_kpi_snapshot"] = json.dumps(
                _latest_kpi_values(conn, workspace_id, kpi_keys)
            )

    set_clause = ", ".join(f"{k}=?" for k in updates)
    values = list(updates.values()) + [decision_id, workspace_id]

    conn.execute(
        f"UPDATE decisions SET {set_clause} WHERE id=? AND workspace_id=?",
        values,
    )
    conn.commit()
    conn.close()

    _audit("decision_updated", "decision", str(decision_id),
           f"Decision #{decision_id} updated: {', '.join(updates.keys())}",
           workspace_id=workspace_id)

    return {"status": "updated"}


# ── DELETE /api/decisions/:id ────────────────────────────────────────────────

@router.delete("/api/decisions/{decision_id}")
async def delete_decision(decision_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute(
        "DELETE FROM decisions WHERE id=? AND workspace_id=?",
        [decision_id, workspace_id]
    )
    conn.commit()
    conn.close()
    return {"status": "deleted"}


# ── GET /api/decision-markers ────────────────────────────────────────────────

@router.get("/api/decision-markers", tags=["Decisions"])
async def decision_markers(request: Request):
    """
    Return decisions grouped by YYYY-MM period for chart annotation overlays.
    """
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    rows = conn.execute(
        "SELECT id, title, status, decided_at, kpi_context "
        "FROM decisions WHERE workspace_id=? ORDER BY decided_at DESC",
        [workspace_id],
    ).fetchall()
    conn.close()

    markers: dict[str, list] = {}
    for r in rows:
        decided_at = r["decided_at"] or ""
        if len(decided_at) < 7:
            continue
        period = decided_at[:7]  # "YYYY-MM"
        markers.setdefault(period, []).append({
            "id":          r["id"],
            "title":       r["title"],
            "status":      r["status"],
            "kpi_context": json.loads(r["kpi_context"] or "[]"),
        })
    return {"markers": markers}
