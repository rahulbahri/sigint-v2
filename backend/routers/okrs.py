"""
routers/okrs.py — OKR framework (Objectives & Key Results).

Objectives are strategic goals. Key Results are measurable outcomes
linked to KPIs. Progress auto-updates from KPI data.
"""
import json
from fastapi import APIRouter, HTTPException, Request
from core.database import get_db, _audit
from core.deps import _require_workspace

router = APIRouter()


@router.get("/api/okrs", tags=["OKRs"])
def list_okrs(request: Request):
    """List all objectives with their key results."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        obj_rows = conn.execute(
            "SELECT * FROM objectives WHERE workspace_id=? ORDER BY created_at DESC",
            [workspace_id],
        ).fetchall()

        kr_rows = conn.execute(
            "SELECT * FROM key_results WHERE workspace_id=? ORDER BY id",
            [workspace_id],
        ).fetchall()

        # Auto-update key results from live KPI data
        latest_kpis = {}
        try:
            md_row = conn.execute(
                "SELECT data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 1",
                [workspace_id],
            ).fetchone()
            if md_row:
                d = json.loads(md_row["data_json"]) if isinstance(md_row["data_json"], str) else (md_row["data_json"] or {})
                latest_kpis = {k: v for k, v in d.items() if not k.startswith("_") and isinstance(v, (int, float))}
        except Exception:
            pass

        # Group KRs by objective
        kr_by_obj = {}
        for kr in kr_rows:
            oid = kr["objective_id"]
            kr_dict = dict(kr)
            # Auto-update current_value from live KPI
            kpi = kr_dict.get("kpi_key")
            if kpi and kpi in latest_kpis:
                kr_dict["current_value"] = round(latest_kpis[kpi], 2)
                # Auto-compute progress
                target = kr_dict.get("target_value")
                current = kr_dict["current_value"]
                if target and target != 0:
                    kr_dict["progress_pct"] = round(min(max(current / target * 100, 0), 100), 1)
            kr_by_obj.setdefault(oid, []).append(kr_dict)

        objectives = []
        for obj in obj_rows:
            obj_dict = dict(obj)
            krs = kr_by_obj.get(obj["id"], [])
            obj_dict["key_results"] = krs
            # Compute objective progress = avg of KR progress
            if krs:
                obj_dict["progress_pct"] = round(sum(kr.get("progress_pct", 0) for kr in krs) / len(krs), 1)
            else:
                obj_dict["progress_pct"] = 0
            objectives.append(obj_dict)

        return {"objectives": objectives}
    finally:
        conn.close()


@router.post("/api/okrs/objectives", tags=["OKRs"])
async def create_objective(request: Request):
    """Create a new objective."""
    workspace_id = _require_workspace(request)
    body = await request.json()
    title = (body.get("title") or "").strip()
    if not title:
        raise HTTPException(400, "Objective title is required")
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO objectives (workspace_id, title, description, owner, quarter, confidence) "
            "VALUES (?,?,?,?,?,?)",
            [workspace_id, title, body.get("description", ""), body.get("owner", ""),
             body.get("quarter", ""), body.get("confidence", 50)],
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        _audit("okr_created", "objective", str(new_id), f"Objective created: {title}", workspace_id=workspace_id)
        return {"id": new_id, "status": "created"}
    finally:
        conn.close()


@router.put("/api/okrs/objectives/{obj_id}", tags=["OKRs"])
async def update_objective(obj_id: int, request: Request):
    workspace_id = _require_workspace(request)
    body = await request.json()
    conn = get_db()
    try:
        updates = {}
        for f in ("title", "description", "owner", "quarter", "status", "confidence"):
            if f in body:
                updates[f] = body[f]
        if not updates:
            raise HTTPException(400, "No fields to update")
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [obj_id, workspace_id]
        conn.execute(f"UPDATE objectives SET {set_clause} WHERE id=? AND workspace_id=?", values)
        conn.commit()
        return {"status": "updated"}
    finally:
        conn.close()


@router.delete("/api/okrs/objectives/{obj_id}", tags=["OKRs"])
def delete_objective(obj_id: int, request: Request):
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        conn.execute("DELETE FROM key_results WHERE objective_id=? AND workspace_id=?", [obj_id, workspace_id])
        conn.execute("DELETE FROM objectives WHERE id=? AND workspace_id=?", [obj_id, workspace_id])
        conn.commit()
        return {"status": "deleted"}
    finally:
        conn.close()


@router.post("/api/okrs/key-results", tags=["OKRs"])
async def create_key_result(request: Request):
    """Create a key result linked to an objective."""
    workspace_id = _require_workspace(request)
    body = await request.json()
    obj_id = body.get("objective_id")
    title = (body.get("title") or "").strip()
    if not obj_id or not title:
        raise HTTPException(400, "objective_id and title are required")
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO key_results (objective_id, workspace_id, title, kpi_key, target_value, unit) "
            "VALUES (?,?,?,?,?,?)",
            [obj_id, workspace_id, title, body.get("kpi_key", ""),
             body.get("target_value"), body.get("unit", "")],
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        return {"id": new_id, "status": "created"}
    finally:
        conn.close()


@router.put("/api/okrs/key-results/{kr_id}", tags=["OKRs"])
async def update_key_result(kr_id: int, request: Request):
    workspace_id = _require_workspace(request)
    body = await request.json()
    conn = get_db()
    try:
        updates = {}
        for f in ("title", "kpi_key", "target_value", "current_value", "unit", "progress_pct", "status"):
            if f in body:
                updates[f] = body[f]
        if not updates:
            raise HTTPException(400, "No fields to update")
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [kr_id, workspace_id]
        conn.execute(f"UPDATE key_results SET {set_clause} WHERE id=? AND workspace_id=?", values)
        conn.commit()
        return {"status": "updated"}
    finally:
        conn.close()


@router.delete("/api/okrs/key-results/{kr_id}", tags=["OKRs"])
def delete_key_result(kr_id: int, request: Request):
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        conn.execute("DELETE FROM key_results WHERE id=? AND workspace_id=?", [kr_id, workspace_id])
        conn.commit()
        return {"status": "deleted"}
    finally:
        conn.close()
