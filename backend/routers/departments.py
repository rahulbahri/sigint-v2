"""
routers/departments.py — Department CRUD + department dashboard API.

Departments map to KPI domain groups. A "Sales" department owns the
"growth" domain KPIs. The dashboard aggregates health by department.

Default departments are auto-created on first access.
"""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request

from core.database import get_db, _audit
from core.deps import _require_workspace

router = APIRouter()

# Default department templates (auto-created on first access)
_DEFAULT_DEPARTMENTS = [
    {"name": "Sales & Growth",       "domains": ["growth"],                     "color": "#059669", "owner": "VP Sales"},
    {"name": "Finance",              "domains": ["cashflow", "profitability"],  "color": "#0055A4", "owner": "CFO"},
    {"name": "Customer Success",     "domains": ["retention"],                  "color": "#D97706", "owner": "VP CS"},
    {"name": "Revenue Operations",   "domains": ["revenue"],                   "color": "#7C3AED", "owner": "RevOps Lead"},
    {"name": "Operations",           "domains": ["efficiency"],                "color": "#64748B", "owner": "COO"},
    {"name": "Risk & Compliance",    "domains": ["risk"],                      "color": "#DC2626", "owner": "CFO"},
]


def _ensure_defaults(conn, workspace_id: str):
    """Auto-create default departments if none exist."""
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM departments WHERE workspace_id=?",
        [workspace_id],
    ).fetchone()
    if row and row["cnt"] > 0:
        return
    for dept in _DEFAULT_DEPARTMENTS:
        conn.execute(
            "INSERT INTO departments (workspace_id, name, owner, domains, color) VALUES (?,?,?,?,?)",
            [workspace_id, dept["name"], dept["owner"], json.dumps(dept["domains"]), dept["color"]],
        )
    conn.commit()


@router.get("/api/departments", tags=["Departments"])
def list_departments(request: Request):
    """List all departments for the workspace (auto-creates defaults)."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        _ensure_defaults(conn, workspace_id)
        rows = conn.execute(
            "SELECT id, name, owner, domains, color, created_at "
            "FROM departments WHERE workspace_id=? ORDER BY id",
            [workspace_id],
        ).fetchall()
        return {"departments": [
            {**dict(r), "domains": json.loads(r["domains"] or "[]")}
            for r in rows
        ]}
    finally:
        conn.close()


@router.post("/api/departments", tags=["Departments"])
async def create_department(request: Request):
    """Create a new department."""
    workspace_id = _require_workspace(request)
    body = await request.json()
    name = (body.get("name") or "").strip()
    if not name:
        raise HTTPException(400, "Department name is required")
    conn = get_db()
    try:
        conn.execute(
            "INSERT INTO departments (workspace_id, name, owner, domains, color) VALUES (?,?,?,?,?)",
            [workspace_id, name, body.get("owner", ""), json.dumps(body.get("domains", [])),
             body.get("color", "#0055A4")],
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        _audit("department_created", "department", str(new_id), f"Department created: {name}", workspace_id=workspace_id)
        return {"id": new_id, "status": "created"}
    finally:
        conn.close()


@router.put("/api/departments/{dept_id}", tags=["Departments"])
async def update_department(dept_id: int, request: Request):
    """Update a department."""
    workspace_id = _require_workspace(request)
    body = await request.json()
    conn = get_db()
    try:
        updates = {}
        if "name" in body:
            updates["name"] = body["name"]
        if "owner" in body:
            updates["owner"] = body["owner"]
        if "domains" in body:
            updates["domains"] = json.dumps(body["domains"])
        if "color" in body:
            updates["color"] = body["color"]
        if not updates:
            raise HTTPException(400, "No fields to update")
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values = list(updates.values()) + [dept_id, workspace_id]
        conn.execute(f"UPDATE departments SET {set_clause} WHERE id=? AND workspace_id=?", values)
        conn.commit()
        return {"status": "updated"}
    finally:
        conn.close()


@router.delete("/api/departments/{dept_id}", tags=["Departments"])
def delete_department(dept_id: int, request: Request):
    """Delete a department."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        conn.execute("DELETE FROM departments WHERE id=? AND workspace_id=?", [dept_id, workspace_id])
        conn.commit()
        return {"status": "deleted"}
    finally:
        conn.close()


@router.get("/api/departments/dashboard", tags=["Departments"])
def department_dashboard(request: Request):
    """
    Aggregate KPI health by department. Each department maps to domain groups,
    and we compute per-department health summary.
    """
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        _ensure_defaults(conn, workspace_id)

        # Load departments
        dept_rows = conn.execute(
            "SELECT id, name, owner, domains, color FROM departments WHERE workspace_id=?",
            [workspace_id],
        ).fetchall()
        departments = [
            {**dict(r), "domains": json.loads(r["domains"] or "[]")}
            for r in dept_rows
        ]

        # Load health score for domain grouping
        from core.health_score import compute_health_score
        health = compute_health_score(conn, workspace_id)

        domain_groups = health.get("domain_groups", [])
        domain_map = {dg["domain"]: dg for dg in domain_groups}

        # Build per-department dashboard
        dashboard = []
        for dept in departments:
            dept_kpis = []
            dept_red = 0
            dept_yellow = 0
            dept_green = 0
            dept_grey = 0

            for domain in dept["domains"]:
                dg = domain_map.get(domain, {})
                kpis = dg.get("kpis", [])
                dept_kpis.extend(kpis)
                for k in kpis:
                    s = k.get("status", "grey")
                    if s == "red":
                        dept_red += 1
                    elif s == "yellow":
                        dept_yellow += 1
                    elif s == "green":
                        dept_green += 1
                    else:
                        dept_grey += 1

            total = dept_red + dept_yellow + dept_green
            if total > 0:
                dept_score = round((dept_green / total) * 100, 1)
            else:
                dept_score = None

            dashboard.append({
                "id": dept["id"],
                "name": dept["name"],
                "owner": dept["owner"],
                "color": dept["color"],
                "domains": dept["domains"],
                "kpi_count": len(dept_kpis),
                "red": dept_red,
                "yellow": dept_yellow,
                "green": dept_green,
                "grey": dept_grey,
                "score": dept_score,
                "top_issues": [
                    {"key": k["key"], "name": k.get("name", k["key"]),
                     "gap_pct": k.get("gap_pct"), "composite": k.get("composite")}
                    for k in sorted(dept_kpis, key=lambda x: x.get("composite", 0) or 0, reverse=True)
                    if k.get("status") == "red"
                ][:3],
            })

        return {"departments": dashboard, "total_kpis": health.get("kpis_green", 0) + health.get("kpis_yellow", 0) + health.get("kpis_red", 0)}
    finally:
        conn.close()
