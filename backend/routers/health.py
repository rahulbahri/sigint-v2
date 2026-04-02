"""
routers/health.py — Health Score endpoint + Home Screen data aggregation.
GET /api/health-score
GET /api/home
"""
from fastapi import APIRouter, Request

from core.database import get_db
from core.deps import _get_workspace
from core.health_score import compute_health_score

router = APIRouter()


@router.get("/api/health-score", tags=["Intelligence"])
def get_health_score(request: Request):
    """Return the workspace health score with full breakdown."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        return {
            "score": 0, "grade": "—", "label": "No Data", "color": "grey",
            "momentum": 0, "target_achievement": 0, "risk_flags": 0,
            "kpis_green": 0, "kpis_yellow": 0, "kpis_red": 0, "kpis_grey": 0,
            "months_of_data": 0, "needs_attention": [], "doing_well": [],
            "momentum_trend": "stable",
        }
    conn = get_db()
    result = compute_health_score(conn, workspace_id)
    conn.close()
    return result


@router.get("/api/home", tags=["Intelligence"])
def get_home(request: Request):
    """
    Aggregated home-screen payload: health score + recent brief + spotlight KPIs.
    Used by the HomeScreen component to avoid multiple round trips.
    """
    import json
    workspace_id = _get_workspace(request)
    conn = get_db()

    health = compute_health_score(conn, workspace_id)

    # Data period (min/max year-month) for "last updated" display
    period_row = conn.execute(
        "SELECT MIN(year*100+month) as mn, MAX(year*100+month) as mx FROM monthly_data WHERE workspace_id=?",
        [workspace_id]
    ).fetchone()
    latest_upload = conn.execute(
        "SELECT uploaded_at FROM uploads WHERE workspace_id=? ORDER BY id DESC LIMIT 1",
        [workspace_id]
    ).fetchone()

    def _ym(val):
        if not val: return None
        yr, mo = divmod(int(val), 100)
        return f"{yr}-{mo:02d}"

    data_period = {
        "from": _ym(period_row["mn"]) if period_row and period_row["mn"] else None,
        "to":   _ym(period_row["mx"]) if period_row and period_row["mx"] else None,
        "uploaded_at": latest_upload["uploaded_at"] if latest_upload else None,
    }

    # Pull last 6 months of data for recent trend sparklines
    rows = conn.execute(
        "SELECT year, month, data_json FROM monthly_data WHERE workspace_id=? ORDER BY year DESC, month DESC LIMIT 6",
        [workspace_id]
    ).fetchall()
    targets_rows = conn.execute(
        "SELECT kpi_key, target_value, direction, unit FROM kpi_targets WHERE workspace_id=?",
        [workspace_id]
    ).fetchall()
    targets_map = {r["kpi_key"]: {"target": r["target_value"], "direction": r["direction"] or "higher", "unit": r["unit"] or ""} for r in targets_rows}

    # Build spotlight data for needs_attention and doing_well KPIs
    kpi_monthly: dict = {}
    for row in rows:
        d = json.loads(row["data_json"])
        period = f"{row['year']}-{row['month']:02d}"
        for k, v in d.items():
            if v is not None and k not in ("year", "month"):
                kpi_monthly.setdefault(k, []).append({"period": period, "value": v})

    def _kpi_spotlight(key: str) -> dict:
        t  = targets_map.get(key, {})
        mo = sorted(kpi_monthly.get(key, []), key=lambda x: x["period"])
        vals = [m["value"] for m in mo]
        avg  = round(sum(vals) / len(vals), 2) if vals else None
        return {
            "key":       key,
            "target":    t.get("target"),
            "direction": t.get("direction", "higher"),
            "unit":      t.get("unit", ""),
            "avg":       avg,
            "sparkline": [m["value"] for m in mo[-6:]],
        }

    conn.close()

    return {
        "health":          health,
        "data_period":     data_period,
        "needs_attention": [_kpi_spotlight(k) for k in health["needs_attention"]],
        "doing_well":      [_kpi_spotlight(k) for k in health["doing_well"]],
    }
