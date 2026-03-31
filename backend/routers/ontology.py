"""
routers/ontology.py — Knowledge-graph ontology endpoints (/api/ontology/*).
"""
import json
from typing import Optional

from fastapi import APIRouter, HTTPException, Request

from core.database import get_db
from core.deps import _get_workspace
from core.kpi_defs import ALL_CAUSATION_RULES
from core.queue import enqueue as _enqueue

router = APIRouter()


def _run_ontology_discovery(workspace_id: str = ""):
    """
    Thin shim: import the actual discovery function from main where it lives.
    Because the heavy discovery logic (hundreds of lines) lives in main.py, we
    delegate to it at call time to avoid duplicating logic.
    """
    try:
        import main as _main
        try:
            _main._run_ontology_discovery(workspace_id=workspace_id)
        except TypeError:
            _main._run_ontology_discovery()
    except Exception as exc:
        print(f"[Ontology] discovery error: {exc}")


@router.post("/api/ontology/discover")
def ontology_discover(request: Request):
    """Trigger background knowledge-graph discovery for the current workspace."""
    workspace_id = _get_workspace(request)
    if not workspace_id:
        raise HTTPException(status_code=401, detail="Not authenticated")

    def _bg():
        try:
            _run_ontology_discovery(workspace_id=workspace_id)
        except Exception as exc:
            print(f"Ontology discovery error: {exc}")

    _enqueue(_bg)
    return {"status": "running", "message": "Ontology discovery started — refresh in ~5 seconds"}


@router.get("/api/ontology/graph")
def ontology_graph(domain: Optional[str] = None):
    conn = get_db()
    q = "SELECT * FROM ontology_nodes"
    params = ()
    if domain and domain != "all":
        q += " WHERE domain=?"
        params = (domain,)
    nodes = []
    for r in conn.execute(q, params).fetchall():
        n = dict(r)
        rules = ALL_CAUSATION_RULES.get(n["key"], {})
        n["root_causes"]        = rules.get("root_causes", [])
        n["corrective_actions"] = rules.get("corrective_actions", [])
        n["downstream_impact"]  = rules.get("downstream_impact", [])
        nodes.append(n)
    node_keys = {n["key"] for n in nodes}
    edges = [dict(e) for e in conn.execute("SELECT * FROM ontology_edges").fetchall()
             if e["source"] in node_keys and e["target"] in node_keys]
    conn.close()
    return {"nodes": nodes, "edges": edges}


@router.get("/api/ontology/stats")
def ontology_stats():
    conn = get_db()
    total_nodes = conn.execute("SELECT COUNT(*) FROM ontology_nodes").fetchone()[0]
    total_edges = conn.execute("SELECT COUNT(*) FROM ontology_edges").fetchone()[0]
    active_recs  = conn.execute("SELECT COUNT(*) FROM ontology_recommendations WHERE status='active'").fetchone()[0]
    domain_rows  = conn.execute("SELECT domain, COUNT(*) as cnt FROM ontology_nodes GROUP BY domain").fetchall()
    edge_rows    = conn.execute("SELECT relation, COUNT(*) as cnt FROM ontology_edges GROUP BY relation").fetchall()
    top_nodes    = conn.execute(
        "SELECT key, name, pagerank, domain FROM ontology_nodes ORDER BY pagerank DESC LIMIT 5"
    ).fetchall()
    conn.close()
    return {
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "active_recommendations": active_recs,
        "domain_distribution": {r["domain"]: r["cnt"] for r in domain_rows},
        "edge_type_distribution": {r["relation"]: r["cnt"] for r in edge_rows},
        "top_nodes_by_pagerank": [dict(r) for r in top_nodes],
    }


@router.get("/api/ontology/recommendations")
def ontology_recommendations(rec_type: Optional[str] = None, status: Optional[str] = "active"):
    conn = get_db()
    q = "SELECT * FROM ontology_recommendations WHERE status=?"
    params: list = [status or "active"]
    if rec_type:
        q += " AND rec_type=?"
        params.append(rec_type)
    q += " ORDER BY impact DESC, confidence DESC"
    rows = [dict(r) for r in conn.execute(q, params).fetchall()]
    for r in rows:
        r["path"] = json.loads(r["path"]) if r.get("path") else []
    conn.close()
    return rows


@router.post("/api/ontology/recommendations/{rec_id}/dismiss")
def dismiss_recommendation(rec_id: int):
    conn = get_db()
    conn.execute("UPDATE ontology_recommendations SET status='dismissed' WHERE id=?", (rec_id,))
    conn.commit()
    conn.close()
    return {"status": "dismissed"}
