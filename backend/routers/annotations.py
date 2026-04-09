"""
routers/annotations.py — KPI annotations (v2), accountability, outcomes,
                          smart actions, audit log, and KPI coverage endpoints.
"""
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel as _BM

from core.database import get_db, _audit
from core.deps import _require_workspace
from core.kpi_defs import KPI_DEFS, ALL_CAUSATION_RULES, BENCHMARKS, EXTENDED_ONTOLOGY_METRICS

import numpy as np

router = APIRouter()


# ── Shared helper: _compute_fingerprint_data ──────────────────────────────────
# Imported from routers.analytics to avoid duplication.
def _compute_fingerprint_data(targets_override=None, workspace_id: str = ""):
    from routers.analytics import _compute_fingerprint_data as _cfp
    return _cfp(targets_override=targets_override, workspace_id=workspace_id)



# ─── Annotations (v2 table: kpi_annotations) ─────────────────────────────────

class _AnnotationBody(_BM):
    kpi_key: str
    period: str
    note: str


@router.get("/api/annotations", tags=["Annotations"])
def list_annotations(request: Request, kpi_key: Optional[str] = None, period: Optional[str] = None):
    """List KPI annotations, optionally filtered by kpi_key and/or period."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    clauses: list = ["workspace_id = ?"]
    params: list = [workspace_id]
    if kpi_key:
        clauses.append("kpi_key = ?")
        params.append(kpi_key)
    if period:
        clauses.append("period = ?")
        params.append(period)
    where = " WHERE " + " AND ".join(clauses)
    rows = conn.execute(f"SELECT * FROM kpi_annotations{where} ORDER BY created_at DESC", params).fetchall()
    conn.close()
    return {"annotations": [dict(r) for r in rows]}


@router.put("/api/annotations", tags=["Annotations"])
def upsert_annotation(request: Request, body: _AnnotationBody):
    """Create or update (upsert) a KPI annotation for a given kpi_key + period."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.execute(
        """INSERT INTO kpi_annotations (kpi_key, period, note, created_at, updated_at, workspace_id)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(kpi_key, period) DO UPDATE SET note=excluded.note, updated_at=excluded.updated_at""",
        (body.kpi_key, body.period, body.note, now, now, workspace_id),
    )
    conn.commit()
    row = conn.execute(
        "SELECT * FROM kpi_annotations WHERE kpi_key=? AND period=? AND workspace_id=?",
        (body.kpi_key, body.period, workspace_id),
    ).fetchone()
    conn.close()
    return {"status": "ok", "annotation": dict(row)}


@router.delete("/api/annotations/{annotation_id}", tags=["Annotations"])
def delete_annotation(annotation_id: int, request: Request):
    """Delete a KPI annotation by its ID (workspace-scoped)."""
    workspace_id = _require_workspace(request)
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM kpi_annotations WHERE id=? AND workspace_id=?",
        (annotation_id, workspace_id),
    ).fetchone()
    if not existing:
        conn.close()
        raise HTTPException(status_code=404, detail="Annotation not found")
    conn.execute("DELETE FROM kpi_annotations WHERE id=? AND workspace_id=?", (annotation_id, workspace_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ─── Annotations (legacy table: annotations) ─────────────────────────────────

@router.get("/api/annotations/{kpi_key}", tags=["Annotations"])
def get_annotations(request: Request, kpi_key: str, period: str = None):
    workspace_id = _require_workspace(request)
    conn = get_db()
    if period:
        rows = conn.execute(
            "SELECT * FROM annotations WHERE kpi_key=? AND period=? AND workspace_id=? ORDER BY created_at DESC",
            (kpi_key, period, workspace_id)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM annotations WHERE kpi_key=? AND workspace_id=? ORDER BY period DESC, created_at DESC",
            (kpi_key, workspace_id)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/api/annotations/{kpi_key}", tags=["Annotations"])
async def add_annotation(kpi_key: str, request: Request):
    workspace_id = _require_workspace(request)
    body = await request.json()
    note = body.get("note", "").strip()
    if not note:
        raise HTTPException(status_code=400, detail="Note cannot be empty")
    period = body.get("period", "general")
    author = body.get("author", "CFO")
    conn = get_db()
    cursor = conn.execute(
        "INSERT INTO annotations (kpi_key, period, note, author, workspace_id) VALUES (?,?,?,?,?)",
        (kpi_key, period, note, author, workspace_id)
    )
    ann_id = cursor.lastrowid
    conn.commit()
    conn.close()
    return {"id": ann_id, "kpi_key": kpi_key, "period": period, "note": note, "author": author}


# ─── KPI Accountability ───────────────────────────────────────────────────────

@router.get("/api/accountability", tags=["Accountability"])
def get_accountability(request: Request, kpi_key: Optional[str] = None):
    workspace_id = _require_workspace(request)
    conn = get_db()
    if kpi_key:
        rows = conn.execute("SELECT * FROM kpi_accountability WHERE kpi_key=? AND workspace_id=?", (kpi_key, workspace_id)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM kpi_accountability WHERE workspace_id=?", (workspace_id,)).fetchall()
    conn.close()
    result = {}
    for r in rows:
        result[r["kpi_key"]] = {
            "kpi_key": r["kpi_key"],
            "owner": r["owner"],
            "due_date": r["due_date"],
            "status": r["status"],
            "last_updated": r["last_updated"],
        }
    return {"accountability": result}


@router.put("/api/accountability/{kpi_key}", tags=["Accountability"])
def put_accountability(request: Request, kpi_key: str, body: dict):
    workspace_id = _require_workspace(request)
    owner = body.get("owner", "")
    due_date = body.get("due_date", "")
    status = body.get("status", "open")
    now = datetime.now().isoformat()
    conn = get_db()
    conn.execute("""
        INSERT INTO kpi_accountability (kpi_key, owner, due_date, status, last_updated, workspace_id)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(kpi_key, workspace_id) DO UPDATE SET
            owner = excluded.owner,
            due_date = excluded.due_date,
            status = excluded.status,
            last_updated = excluded.last_updated
    """, (kpi_key, owner, due_date, status, now, workspace_id))
    conn.commit()
    conn.close()
    _audit("accountability_update", "accountability", kpi_key, f"Accountability updated for {kpi_key}")
    return {"status": "ok", "accountability": {"kpi_key": kpi_key, "owner": owner, "due_date": due_date, "status": status, "last_updated": now}}


# ─── Recommendation Outcomes ──────────────────────────────────────────────────

@router.get("/api/outcomes/{kpi_key}", tags=["Outcomes"])
def get_outcomes(request: Request, kpi_key: str):
    workspace_id = _require_workspace(request)
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM recommendation_outcomes WHERE kpi_key=? AND workspace_id=? ORDER BY started_at DESC",
        (kpi_key, workspace_id)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


@router.post("/api/outcomes/{kpi_key}", tags=["Outcomes"])
async def record_outcome(kpi_key: str, request: Request):
    workspace_id = _require_workspace(request)
    body = await request.json()
    conn = get_db()
    cursor = conn.execute("""
        INSERT INTO recommendation_outcomes
        (kpi_key, action_text, before_value, before_status, outcome_notes, workspace_id)
        VALUES (?,?,?,?,?,?)
    """, (
        kpi_key,
        body.get("action_text", ""),
        body.get("before_value"),
        body.get("before_status"),
        body.get("outcome_notes", ""),
        workspace_id
    ))
    conn.commit()
    outcome_id = cursor.lastrowid
    conn.close()
    return {"id": outcome_id, "status": "recorded"}


@router.put("/api/outcomes/{outcome_id}/resolve", tags=["Outcomes"])
async def resolve_outcome(outcome_id: int, request: Request):
    body = await request.json()
    conn = get_db()
    conn.execute("""
        UPDATE recommendation_outcomes
        SET resolved_at=datetime('now'), after_value=?, after_status=?,
            was_effective=?, outcome_notes=?
        WHERE id=?
    """, (
        body.get("after_value"),
        body.get("after_status"),
        1 if body.get("was_effective") else 0,
        body.get("outcome_notes", ""),
        outcome_id
    ))
    conn.commit()
    conn.close()
    return {"status": "resolved"}


# ─── Audit Log ────────────────────────────────────────────────────────────────

@router.get("/api/audit-log", tags=["Audit"])
def get_audit_log(request: Request, limit: int = 100, event_type: str = None):
    workspace_id = _require_workspace(request)
    conn = get_db()
    if event_type:
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE workspace_id=? AND event_type=? ORDER BY created_at DESC LIMIT ?",
            (workspace_id, event_type, limit)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM audit_log WHERE workspace_id=? ORDER BY created_at DESC LIMIT ?",
            (workspace_id, limit)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ─── Smart Actions ────────────────────────────────────────────────────────────

def _build_causal_chain(kpi_key: str, fp_lookup: dict, max_depth: int = 3) -> list:
    status_order = {"red": 0, "yellow": 1, "green": 2, "grey": 3}

    def _gap_pct(kpi_data):
        val = kpi_data.get("avg")
        target = kpi_data.get("target")
        direction = kpi_data.get("direction", "higher")
        if val is None or target is None or target == 0:
            return None
        if direction == "higher":
            return round((val - target) / abs(target) * 100, 1)
        else:
            return round((target - val) / abs(target) * 100, 1)

    def _node(hop, key, kpi_data, label):
        val = kpi_data.get("avg")
        target = kpi_data.get("target")
        status = kpi_data.get("fy_status", "grey")
        return {
            "hop": hop,
            "kpi_key": key,
            "kpi_name": kpi_data.get("name", key.replace("_", " ").title()),
            "value": val,
            "target": target,
            "status": status,
            "gap_pct": _gap_pct(kpi_data),
            "label": label,
        }

    hop_labels = {0: "Surface symptom", 1: "Primary driver", 2: "Contributing cause", 3: "Root cause"}
    chain = []
    visited = set()

    root_data = fp_lookup.get(kpi_key)
    if root_data is None:
        return chain
    chain.append(_node(0, kpi_key, root_data, hop_labels[0]))
    visited.add(kpi_key)

    current_level_keys = [kpi_key]
    for hop in range(1, max_depth + 1):
        next_level_keys = []
        hop_nodes = []
        for parent_key in current_level_keys:
            for source_key, rules in ALL_CAUSATION_RULES.items():
                if parent_key in rules.get("downstream_impact", []):
                    if source_key in visited:
                        continue
                    source_data = fp_lookup.get(source_key)
                    if source_data is None:
                        continue
                    status = source_data.get("fy_status", "grey")
                    if status in ("red", "yellow"):
                        label = hop_labels.get(hop, f"Hop {hop} cause")
                        hop_nodes.append(_node(hop, source_key, source_data, label))
                        visited.add(source_key)
                        next_level_keys.append(source_key)

        hop_nodes.sort(key=lambda x: (
            status_order.get(x["status"], 4),
            -(abs(x["gap_pct"]) if x["gap_pct"] is not None else 0)
        ))
        chain.extend(hop_nodes)
        current_level_keys = next_level_keys
        if not current_level_keys:
            break

    return chain


def _generate_smart_actions(kpi_key: str, fp_data: list, benchmarks_for_stage: dict, stage: str):
    kpi_data = None
    fp_lookup = {}
    for k in fp_data:
        fp_lookup[k["key"]] = k
        if k["key"] == kpi_key:
            kpi_data = k
    if kpi_data is None:
        return None

    current_value = kpi_data["avg"]
    target = kpi_data["target"]
    direction = kpi_data["direction"]
    unit = kpi_data["unit"]
    status = kpi_data["fy_status"]
    kpi_name = kpi_data["name"]
    monthly = kpi_data["monthly"]

    gap_pct = None
    if current_value is not None and target is not None and target != 0:
        if direction == "higher":
            gap_pct = round((current_value - target) / abs(target) * 100, 1)
        else:
            gap_pct = round((target - current_value) / abs(target) * 100, 1)

    bench_info = None
    bench_data = benchmarks_for_stage.get(kpi_key)
    if bench_data and current_value is not None:
        p25 = bench_data.get("p25")
        p50 = bench_data.get("p50")
        p75 = bench_data.get("p75")
        pct_from_median = round((current_value - p50) / abs(p50) * 100, 1) if p50 and p50 != 0 else None
        if direction == "higher":
            if current_value < p25:
                position = "below_p25"
            elif current_value < p50:
                position = "p25_to_p50"
            elif current_value < p75:
                position = "p50_to_p75"
            else:
                position = "above_p75"
        else:
            if current_value > p75:
                position = "below_p25"
            elif current_value > p50:
                position = "p25_to_p50"
            elif current_value > p25:
                position = "p50_to_p75"
            else:
                position = "above_p75"
        bench_info = {
            "p25": p25, "p50": p50, "p75": p75,
            "position": position,
            "pct_from_median": pct_from_median,
        }

    trend_info = None
    if monthly and len(monthly) >= 2:
        sorted_monthly = sorted(monthly, key=lambda m: m["period"])
        last_3 = sorted_monthly[-3:] if len(sorted_monthly) >= 3 else sorted_monthly
        vals_3 = [m["value"] for m in last_3 if m["value"] is not None]

        trend_direction = "stable"
        pct_change_3m = None
        consecutive_declining = 0

        if len(vals_3) >= 2:
            if vals_3[-1] > vals_3[0]:
                trend_direction = "improving" if direction == "higher" else "declining"
            elif vals_3[-1] < vals_3[0]:
                trend_direction = "declining" if direction == "higher" else "improving"

            if vals_3[0] != 0:
                pct_change_3m = round((vals_3[-1] - vals_3[0]) / abs(vals_3[0]) * 100, 1)

            for i in range(len(vals_3) - 1, 0, -1):
                if direction == "higher":
                    if vals_3[i] < vals_3[i - 1]:
                        consecutive_declining += 1
                    else:
                        break
                else:
                    if vals_3[i] > vals_3[i - 1]:
                        consecutive_declining += 1
                    else:
                        break

        consecutive_red = 0
        for m in reversed(sorted_monthly):
            v = m["value"]
            if v is not None and target is not None:
                if direction == "higher":
                    ratio = v / target if target != 0 else 0
                    if ratio < 0.90:
                        consecutive_red += 1
                    else:
                        break
                else:
                    ratio = v / target if target != 0 else 0
                    if ratio > 1.10:
                        consecutive_red += 1
                    else:
                        break
            else:
                break

        trend_info = {
            "direction": trend_direction,
            "last_3_months": last_3,
            "pct_change_3m": pct_change_3m,
            "consecutive_red_months": consecutive_red,
        }

    upstream_causes = []
    for source_key, rules in ALL_CAUSATION_RULES.items():
        if kpi_key in rules.get("downstream_impact", []):
            upstream_kpi = fp_lookup.get(source_key)
            if upstream_kpi:
                u_val = upstream_kpi["avg"]
                u_target = upstream_kpi["target"]
                u_status = upstream_kpi["fy_status"]
                u_direction = upstream_kpi["direction"]
                u_gap = None
                if u_val is not None and u_target is not None and u_target != 0:
                    if u_direction == "higher":
                        u_gap = round((u_val - u_target) / abs(u_target) * 100, 1)
                    else:
                        u_gap = round((u_target - u_val) / abs(u_target) * 100, 1)

                if u_val is not None:
                    fmt_val = f"{u_val}"
                    fmt_target = f"{u_target}" if u_target is not None else "N/A"
                    if u_status in ("red", "yellow"):
                        gap_str = f"{abs(u_gap)}% {'below' if u_gap < 0 else 'above'} target" if u_gap is not None else ""
                        explanation = (
                            f"{upstream_kpi['name']} is at {fmt_val} vs target {fmt_target} ({gap_str}). "
                            f"This directly impacts {kpi_name} as a causal upstream driver."
                        )
                    else:
                        explanation = (
                            f"{upstream_kpi['name']} is healthy at {fmt_val} "
                            f"({'above' if u_direction == 'higher' else 'below'} target of {fmt_target}) "
                            f"— this is not contributing to the problem."
                        )
                    upstream_causes.append({
                        "kpi_key": source_key,
                        "kpi_name": upstream_kpi["name"],
                        "status": u_status,
                        "value": u_val,
                        "target": u_target,
                        "gap_pct": u_gap,
                        "explanation": explanation,
                        "is_data_available": True,
                    })
                else:
                    upstream_causes.append({
                        "kpi_key": source_key,
                        "kpi_name": upstream_kpi["name"],
                        "status": "grey",
                        "value": None,
                        "target": u_target,
                        "gap_pct": None,
                        "explanation": f"No data available for {upstream_kpi['name']}. Collecting it would improve diagnostic accuracy.",
                        "is_data_available": False,
                    })
            else:
                upstream_causes.append({
                    "kpi_key": source_key,
                    "kpi_name": source_key.replace("_", " ").title(),
                    "status": "grey",
                    "value": None,
                    "target": None,
                    "gap_pct": None,
                    "explanation": f"No data available for {source_key.replace('_', ' ').title()}. Collecting it would improve diagnostic accuracy.",
                    "is_data_available": False,
                })

    status_order = {"red": 0, "yellow": 1, "green": 2, "grey": 3}
    upstream_causes.sort(key=lambda x: status_order.get(x["status"], 4))

    rules_for_kpi = ALL_CAUSATION_RULES.get(kpi_key, {})
    downstream_impact = []
    for ds_key in rules_for_kpi.get("downstream_impact", []):
        ds_kpi = fp_lookup.get(ds_key)
        if ds_kpi:
            ds_val = ds_kpi["avg"]
            ds_target = ds_kpi["target"]
            ds_status = ds_kpi["fy_status"]
            ds_direction = ds_kpi["direction"]
            ds_gap = None
            if ds_val is not None and ds_target is not None and ds_target != 0:
                if ds_direction == "higher":
                    ds_gap = round((ds_val - ds_target) / abs(ds_target) * 100, 1)
                else:
                    ds_gap = round((ds_target - ds_val) / abs(ds_target) * 100, 1)
            fmt_ds_val = f"{ds_val}" if ds_val is not None else "N/A"
            fmt_ds_target = f"{ds_target}" if ds_target is not None else "N/A"
            explanation = (
                f"{ds_kpi['name']} is at {fmt_ds_val} (target {fmt_ds_target}, status: {ds_status}). "
                f"If {kpi_name} improves, {ds_kpi['name']} should improve proportionally as a downstream metric."
            )
            downstream_impact.append({
                "kpi_key": ds_key,
                "kpi_name": ds_kpi["name"],
                "status": ds_status,
                "value": ds_val,
                "target": ds_target,
                "explanation": explanation,
            })

    actions = []
    priority = 1

    red_upstreams = [u for u in upstream_causes if u["status"] == "red" and u["is_data_available"]]
    if red_upstreams:
        worst = red_upstreams[0]
        upstream_actions = ALL_CAUSATION_RULES.get(worst["kpi_key"], {}).get("corrective_actions", [])
        specific_action = upstream_actions[0] if upstream_actions else "Investigate root cause"

        impact_str = ""
        if worst["value"] is not None and worst["target"] is not None and current_value is not None and target is not None:
            upstream_recovery_ratio = worst["target"] / worst["value"] if worst["value"] != 0 else 1
            estimated_new_value = round(current_value * upstream_recovery_ratio, 2)
            impact_str = (
                f"If {worst['kpi_name']} recovers from {worst['value']} to the target of {worst['target']}, "
                f"{kpi_name} would improve from {current_value} to approximately {estimated_new_value}, "
                f"assuming other factors remain constant."
            )

        actions.append({
            "priority": priority,
            "action": (
                f"Address {worst['kpi_name']} first — it is the primary upstream driver and is "
                f"{abs(worst['gap_pct'])}% {'below' if worst['gap_pct'] < 0 else 'above'} target. "
                f"{specific_action}."
            ),
            "expected_impact": impact_str,
            "owner_suggestion": "Revenue Operations / VP Sales" if "sales" in worst["kpi_key"] or "pipeline" in worst["kpi_key"] else "Finance / Operations",
            "timeframe": "30-60 days for diagnosis, 60-90 days for improvement",
        })
        priority += 1

    if bench_info and current_value is not None:
        stage_label = stage.replace("_", " ").title()
        position = bench_info["position"]
        if position == "below_p25":
            p25_val = bench_info["p25"]
            p50_val = bench_info["p50"]
            corrective_actions = rules_for_kpi.get("corrective_actions", [])
            specific_fix = corrective_actions[0] if corrective_actions else "Review operational processes"
            actions.append({
                "priority": priority,
                "action": (
                    f"{kpi_name} at {current_value} is below the {stage_label} 25th percentile ({p25_val}). "
                    f"Benchmark against peer operational structure. {specific_fix}."
                ),
                "expected_impact": (
                    f"Reaching the peer median of {p50_val} would represent a "
                    f"{abs(bench_info['pct_from_median'])}% improvement from current levels."
                ),
                "owner_suggestion": "CRO / CFO",
                "timeframe": "Quarterly review cycle",
            })
            priority += 1
        elif position == "p25_to_p50":
            p50_val = bench_info["p50"]
            actions.append({
                "priority": priority,
                "action": (
                    f"{kpi_name} at {current_value} is between the 25th and 50th percentile for {stage_label}. "
                    f"Closing the gap to the median ({p50_val}) should be an operational priority."
                ),
                "expected_impact": (
                    f"Reaching the peer median of {p50_val} would represent a "
                    f"{abs(bench_info['pct_from_median'])}% improvement."
                ),
                "owner_suggestion": "Operations Lead",
                "timeframe": "60-90 days",
            })
            priority += 1

    if trend_info and trend_info["direction"] == "declining" and trend_info["pct_change_3m"] is not None:
        last3 = trend_info["last_3_months"]
        values_str = " -> ".join([f"{m['value']}" for m in last3 if m["value"] is not None])
        pct_chg = abs(trend_info["pct_change_3m"])
        first_val = next((m["value"] for m in last3 if m["value"] is not None), None)

        coinciding_upstream = ""
        for u in red_upstreams:
            u_fp = fp_lookup.get(u["kpi_key"])
            if u_fp and u_fp["monthly"] and len(u_fp["monthly"]) >= 2:
                u_sorted = sorted(u_fp["monthly"], key=lambda m: m["period"])
                u_last3 = u_sorted[-3:] if len(u_sorted) >= 3 else u_sorted
                u_vals = [m["value"] for m in u_last3 if m["value"] is not None]
                if len(u_vals) >= 2 and u_vals[-1] < u_vals[0]:
                    coinciding_upstream = (
                        f" This coincides with {u['kpi_name']} deteriorating from {u_vals[0]} to {u_vals[-1]} "
                        f"— addressing {u['kpi_name']} is likely the highest-leverage fix."
                    )
                    break

        actions.append({
            "priority": priority,
            "action": (
                f"The {len(last3)}-month declining trend ({values_str}, -{pct_chg}%) suggests a structural change, "
                f"not seasonal variance. Investigate what changed in the operational process starting "
                f"{len(last3)} months ago.{coinciding_upstream}"
            ),
            "expected_impact": (
                f"Identifying and reversing the structural cause could restore the metric to its "
                f"{len(last3)}-month-ago level of {first_val}."
            ) if first_val is not None else "Reversing the trend would stabilize the metric.",
            "owner_suggestion": "Operations / Strategy",
            "timeframe": "2-week investigation, 30-day corrective action",
        })
        priority += 1

    if not actions:
        corrective_actions = rules_for_kpi.get("corrective_actions", [])
        for i, ca in enumerate(corrective_actions[:3]):
            actions.append({
                "priority": priority,
                "action": ca,
                "expected_impact": f"Addressing this would help move {kpi_name} toward the target of {target}." if target else "Impact depends on severity of root cause.",
                "owner_suggestion": "Operations Lead",
                "timeframe": "30-60 days",
            })
            priority += 1

    data_gaps = []
    for u in upstream_causes:
        if not u["is_data_available"]:
            data_gaps.append(
                f"{u['kpi_name']} has no data available — this is a critical upstream metric. "
                f"Collecting it would enable more precise diagnosis. Add it to your data collection via the KPI export template."
            )
    if not monthly or len(monthly) < 3:
        data_gaps.append(
            f"Only {len(monthly) if monthly else 0} months of data available for {kpi_name}. "
            f"At least 3 months are needed for reliable trend analysis."
        )

    causal_chain = _build_causal_chain(kpi_key, fp_lookup, max_depth=3)

    chain_kpi_keys = list({node["kpi_key"] for node in causal_chain})
    max_hop_depth = max((node["hop"] for node in causal_chain), default=0)

    total_data_points = 0
    for ck in chain_kpi_keys:
        ck_fp = fp_lookup.get(ck)
        if ck_fp and ck_fp.get("monthly"):
            total_data_points += len([m for m in ck_fp["monthly"] if m.get("value") is not None])

    chain_by_hop: dict = {}
    for node in causal_chain:
        chain_by_hop.setdefault(node["hop"], []).append(node["kpi_name"])
    chain_summary_parts = []
    for h in sorted(chain_by_hop.keys()):
        chain_summary_parts.append(" / ".join(chain_by_hop[h]))
    chain_summary = " → ".join(chain_summary_parts) if chain_summary_parts else kpi_name

    analysis_depth = {
        "total_data_points": total_data_points,
        "kpis_in_chain": len(chain_kpi_keys),
        "max_hop_depth": max_hop_depth,
        "chain_summary": f"{max_hop_depth}-hop causal trace: {chain_summary}",
    }

    wrong_action_map = {
        "burn_multiple": "Increase S&M headcount or pressure sales team",
        "arr_growth": "Increase S&M headcount or pressure sales team",
        "revenue_growth": "Increase S&M headcount or pressure sales team",
        "sales_efficiency": "Hire more sales reps or replace underperformers",
        "win_rate": "Hire more sales reps or replace underperformers",
        "churn_rate": "Assign more CSMs or offer discounts to retain accounts",
        "nrr": "Assign more CSMs or offer discounts to retain accounts",
        "gross_margin": "Reduce headcount or cut vendor contracts",
        "contribution_margin": "Reduce headcount or cut vendor contracts",
        "pipeline_conversion": "Increase demo volume or revise sales process",
        "cpl": "Increase total marketing budget",
        "marketing_roi": "Increase total marketing budget",
    }
    likely_wrong_action = wrong_action_map.get(kpi_key, "Address the surface symptom directly without tracing root cause")

    root_cause_distance = max_hop_depth
    is_deep_cause = root_cause_distance >= 2

    direction_protected = {
        "likely_wrong_action": likely_wrong_action,
        "root_cause_distance": root_cause_distance,
        "is_deep_cause": is_deep_cause,
    }

    return {
        "kpi_key": kpi_key,
        "kpi_name": kpi_name,
        "current_value": current_value,
        "target": target,
        "gap_pct": gap_pct,
        "unit": unit,
        "status": status,
        "benchmark": bench_info,
        "trend": trend_info,
        "upstream_causes": upstream_causes,
        "downstream_impact": downstream_impact,
        "actions": actions,
        "data_gaps": data_gaps,
        "causal_chain": causal_chain,
        "analysis_depth": analysis_depth,
        "direction_protected": direction_protected,
    }


@router.get("/api/smart-actions/{kpi_key}", tags=["Smart Actions"])
def get_smart_actions(request: Request, kpi_key: str, stage: str = "series_b"):
    workspace_id = _require_workspace(request)
    valid_stages = {"seed", "series_a", "series_b", "series_c"}
    if stage not in valid_stages:
        stage = "series_b"

    fp_data = _compute_fingerprint_data(workspace_id=workspace_id)

    bench = {}
    for bk, stages_data in BENCHMARKS.items():
        if stage in stages_data:
            bench[bk] = stages_data[stage]

    valid_keys = {k["key"] for k in fp_data}
    if kpi_key not in valid_keys:
        return JSONResponse(
            status_code=404,
            content={"detail": f"KPI '{kpi_key}' not found. Valid keys: {sorted(valid_keys)}"}
        )

    result = _generate_smart_actions(kpi_key, fp_data, bench, stage)
    if result is None:
        return JSONResponse(
            status_code=404,
            content={"detail": f"Could not generate actions for KPI '{kpi_key}' — no data found."}
        )

    return result


# ─── KPI Coverage Score ───────────────────────────────────────────────────────

_SOURCE_KPI_MAP: dict = {
    "stripe": {
        "kpis":    ["arr_growth","nrr","churn_rate","expansion_rate","ltv_cac",
                    "cac_payback","recurring_revenue","revenue_quality","logo_retention"],
        "domains": ["Revenue", "Retention", "Unit Economics"],
    },
    "quickbooks": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","contribution_margin"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "xero": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","contribution_margin"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "shopify": {
        "kpis":    ["revenue_growth","customer_concentration","revenue_momentum",
                    "revenue_fragility"],
        "domains": ["Revenue", "Risk"],
    },
    "hubspot": {
        "kpis":    ["health_score","logo_retention","expansion_rate","cpl","mql_sql_rate"],
        "domains": ["Retention", "Growth"],
    },
    "salesforce": {
        "kpis":    ["pipeline_conversion","win_rate","quota_attainment",
                    "sales_efficiency","headcount_eff"],
        "domains": ["Growth", "Efficiency"],
    },
    "google_sheets": {
        "kpis":    ["revenue_growth","gross_margin","operating_margin","dso",
                    "churn_rate","nrr","arr_growth","burn_multiple"],
        "domains": ["Revenue", "Profitability"],
    },
    "brex": {
        "kpis":    ["opex_ratio","burn_multiple","contribution_margin",
                    "headcount_eff","rev_per_employee"],
        "domains": ["Efficiency", "Profitability"],
    },
    "ramp": {
        "kpis":    ["opex_ratio","burn_multiple","contribution_margin",
                    "headcount_eff","rev_per_employee"],
        "domains": ["Efficiency", "Profitability"],
    },
    "netsuite": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period",
                    "cei","ar_aging_current","ar_aging_overdue","billable_utilization"],
        "domains": ["Profitability", "Cash Flow & AR", "Efficiency"],
    },
    "sage_intacct": {
        "kpis":    ["gross_margin","operating_margin","ebitda_margin","opex_ratio",
                    "burn_multiple","dso","ar_turnover","avg_collection_period"],
        "domains": ["Profitability", "Cash Flow & AR"],
    },
    "snowflake": {
        "kpis":    ["revenue_growth","gross_margin","operating_margin","dso",
                    "churn_rate","nrr","arr_growth","burn_multiple","ltv_cac"],
        "domains": ["Revenue", "Profitability", "Retention"],
    },
}

_SOURCE_LABELS = {
    "stripe":        "Stripe",
    "hubspot":       "HubSpot",
    "quickbooks":    "QuickBooks",
    "xero":          "Xero",
    "shopify":       "Shopify",
    "salesforce":    "Salesforce",
    "google_sheets": "Google Sheets",
    "brex":          "Brex",
    "ramp":          "Ramp",
    "netsuite":      "NetSuite",
    "sage_intacct":  "Sage Intacct",
    "snowflake":     "Snowflake",
}

_TOTAL_KPIS = 57


@router.get("/api/kpi-coverage")
async def get_kpi_coverage(request: Request):
    workspace_id = _require_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")
    conn = get_db()
    synced_sources = []
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS connector_configs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                workspace_id TEXT NOT NULL,
                source_name TEXT NOT NULL,
                credentials_enc TEXT,
                sync_status TEXT DEFAULT 'pending',
                last_sync_at TEXT,
                last_error TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(workspace_id, source_name)
            )
        """)
        rows = conn.execute(
            "SELECT source_name, last_sync_at FROM connector_configs "
            "WHERE workspace_id=? AND sync_status='ok'",
            [workspace_id]
        ).fetchall()
        synced_sources = rows
    except Exception:
        pass
    has_csv = False
    try:
        cnt = conn.execute(
            "SELECT COUNT(*) FROM monthly_data WHERE workspace_id=?", [workspace_id]
        ).fetchone()
        has_csv = (cnt[0] if cnt else 0) > 0
    except Exception:
        pass
    conn.close()

    covered_kpis: set = set()
    covered_domains: set = set()
    source_detail: list = []
    for row in synced_sources:
        src      = row["source_name"] if isinstance(row, dict) else row[0]
        last_syn = row["last_sync_at"] if isinstance(row, dict) else row[1]
        info = _SOURCE_KPI_MAP.get(src, {})
        new_kpis = set(info.get("kpis", [])) - covered_kpis
        covered_kpis.update(info.get("kpis", []))
        covered_domains.update(info.get("domains", []))
        source_detail.append({
            "source":       src,
            "label":        _SOURCE_LABELS.get(src, src.title()),
            "last_sync_at": last_syn,
            "kpi_count":    len(info.get("kpis", [])),
            "new_kpis":     len(new_kpis),
        })

    csv_kpis = set()
    if has_csv:
        csv_kpis = {"revenue_growth","gross_margin","operating_margin","ebitda_margin",
                    "burn_multiple","dso","churn_rate","nrr","arr_growth",
                    "ltv_cac","cac_payback","opex_ratio","contribution_margin",
                    "customer_concentration","headcount_eff","rev_per_employee"}
        covered_kpis.update(csv_kpis)

    coverage_pct = round(len(covered_kpis) / _TOTAL_KPIS * 100)

    return {
        "coverage_pct":     coverage_pct,
        "covered_kpis":     len(covered_kpis),
        "total_kpis":       _TOTAL_KPIS,
        "covered_domains":  sorted(covered_domains),
        "sources":          source_detail,
        "has_csv_data":     has_csv,
        "source_count":     len(synced_sources),
    }
