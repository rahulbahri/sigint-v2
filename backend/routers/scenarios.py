"""
routers/scenarios.py — Saved scenarios CRUD (/api/scenarios/*).
"""
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel as _BM2

from core.database import get_db
from core.deps import _get_workspace

router = APIRouter()


class _ScenarioSaveRequest(_BM2):
    name:        str
    levers_json: str
    notes:       str = ""


@router.get("/api/scenarios", tags=["Scenarios"])
def list_scenarios(request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    rows = conn.execute(
        "SELECT id, name, levers_json, notes, created_at, updated_at "
        "FROM saved_scenarios WHERE workspace_id=? ORDER BY updated_at DESC",
        [workspace_id],
    ).fetchall()
    conn.close()
    return {"scenarios": [dict(r) for r in rows]}


@router.post("/api/scenarios", tags=["Scenarios"])
async def save_scenario(body: _ScenarioSaveRequest, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    cur = conn.execute(
        "INSERT INTO saved_scenarios (workspace_id, name, levers_json, notes) VALUES (?,?,?,?)",
        [workspace_id, body.name.strip(), body.levers_json, body.notes],
    )
    conn.commit()
    new_id = cur.lastrowid if cur else None
    conn.close()
    return {"id": new_id, "status": "saved"}


@router.put("/api/scenarios/{scenario_id}", tags=["Scenarios"])
async def update_scenario(scenario_id: int, body: _ScenarioSaveRequest, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute(
        "UPDATE saved_scenarios SET name=?, levers_json=?, notes=?, updated_at=datetime('now') "
        "WHERE id=? AND workspace_id=?",
        [body.name.strip(), body.levers_json, body.notes, scenario_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "updated"}


@router.delete("/api/scenarios/{scenario_id}", tags=["Scenarios"])
async def delete_scenario(scenario_id: int, request: Request):
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    conn.execute(
        "DELETE FROM saved_scenarios WHERE id=? AND workspace_id=?",
        [scenario_id, workspace_id],
    )
    conn.commit()
    conn.close()
    return {"status": "deleted"}
