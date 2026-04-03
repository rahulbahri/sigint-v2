"""
routers/decisions.py — Decision log CRUD (/api/decisions/*).
"""
import json

from fastapi import APIRouter, HTTPException, Request

from core.database import get_db, _audit
from core.deps import _get_workspace

router = APIRouter()


@router.get("/api/decisions")
async def get_decisions(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    rows = conn.execute(
        "SELECT id, title, the_decision, rationale, kpi_context, outcome, "
        "decided_by, status, decided_at FROM decisions WHERE workspace_id=? "
        "ORDER BY decided_at DESC",
        [workspace_id]
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        result.append({
            "id":           r["id"],
            "title":        r["title"],
            "the_decision": r["the_decision"],
            "rationale":    r["rationale"] or "",
            "kpi_context":  json.loads(r["kpi_context"] or "[]"),
            "outcome":      r["outcome"] or "",
            "decided_by":   r["decided_by"] or "CFO",
            "status":       r["status"] or "active",
            "decided_at":   r["decided_at"] or "",
        })
    return {"decisions": result}


@router.post("/api/decisions")
async def create_decision(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()
    title        = (body.get("title") or "").strip()
    the_decision = (body.get("the_decision") or "").strip()
    rationale    = (body.get("rationale") or "").strip()
    kpi_context  = json.dumps(body.get("kpi_context") or [])
    decided_by   = (body.get("decided_by") or "CFO").strip()
    if not title or not the_decision:
        raise HTTPException(status_code=400, detail="title and the_decision are required")
    conn = get_db()
    conn.execute(
        "INSERT INTO decisions (workspace_id, title, the_decision, rationale, kpi_context, decided_by) "
        "VALUES (?,?,?,?,?,?)",
        [workspace_id, title, the_decision, rationale, kpi_context, decided_by]
    )
    conn.commit()
    new_id = conn.lastrowid
    conn.close()

    _audit("decision_created", "decision", str(new_id),
           f"Decision logged: {title}",
           workspace_id=workspace_id)

    return {"id": new_id, "status": "created"}


@router.put("/api/decisions/{decision_id}")
async def update_decision(decision_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body   = await request.json()
    status = (body.get("status") or "active")
    outcome = (body.get("outcome") or "").strip()
    conn = get_db()
    conn.execute(
        "UPDATE decisions SET outcome=?, status=? WHERE id=? AND workspace_id=?",
        [outcome, status, decision_id, workspace_id]
    )
    conn.commit()
    conn.close()

    _audit("decision_updated", "decision", str(decision_id),
           f"Decision #{decision_id} status changed to {status}",
           workspace_id=workspace_id)

    return {"status": "updated"}


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
