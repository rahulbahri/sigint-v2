"""
routers/segments.py — Customer segmentation analysis.

Segments customers by revenue tier (Enterprise/Mid/SMB) and lifecycle
stage. Computes per-segment metrics: revenue share, churn rate, ARPU,
concentration risk, and growth trajectory.
"""
import json
from collections import defaultdict
from typing import Optional

from fastapi import APIRouter, Query, Request

from core.database import get_db
from core.deps import _require_workspace

router = APIRouter()

# Revenue tier thresholds (monthly $ per customer)
_TIERS = [
    ("Enterprise", 5000),   # >= $5K/mo
    ("Mid-Market", 1000),   # >= $1K/mo
    ("SMB", 0),             # < $1K/mo
]


def _tier(monthly_rev: float) -> str:
    for name, threshold in _TIERS:
        if monthly_rev >= threshold:
            return name
    return "SMB"


@router.get("/api/segments", tags=["Segments"])
def customer_segments(request: Request):
    """
    Analyze customer segments by revenue tier and lifecycle stage.
    Returns per-segment: revenue, customer count, ARPU, churn, concentration.
    """
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        # Load revenue by customer by month
        rev_rows = conn.execute(
            "SELECT amount, period, customer_id FROM canonical_revenue WHERE workspace_id=?",
            [workspace_id],
        ).fetchall()

        # Load customer lifecycle stages
        cust_rows = conn.execute(
            "SELECT source_id, lifecycle_stage FROM canonical_customers WHERE workspace_id=?",
            [workspace_id],
        ).fetchall()
        cust_stage = {str(r["source_id"]): r["lifecycle_stage"] or "Unknown" for r in cust_rows}

        if not rev_rows:
            return {"segments": {"by_tier": [], "by_stage": []}, "summary": {}}

        # Aggregate revenue per customer per month
        cust_monthly = defaultdict(lambda: defaultdict(float))
        for r in rev_rows:
            cid = str(r["customer_id"] or "")
            period = str(r["period"] or "")[:7]
            if cid and period:
                cust_monthly[period][cid] += float(r["amount"] or 0)

        sorted_periods = sorted(cust_monthly.keys())
        if not sorted_periods:
            return {"segments": {"by_tier": [], "by_stage": []}, "summary": {}}

        # Use latest period for segmentation
        latest = sorted_periods[-1]
        prev = sorted_periods[-2] if len(sorted_periods) >= 2 else None
        latest_custs = cust_monthly[latest]
        prev_custs = cust_monthly[prev] if prev else {}

        # Build tier segments
        tier_data = defaultdict(lambda: {"customers": [], "revenue": 0, "count": 0})
        for cid, rev in latest_custs.items():
            tier = _tier(rev)
            tier_data[tier]["customers"].append(cid)
            tier_data[tier]["revenue"] += rev
            tier_data[tier]["count"] += 1

        total_rev = sum(latest_custs.values())
        total_custs = len(latest_custs)

        by_tier = []
        for tier_name, _ in _TIERS:
            td = tier_data.get(tier_name, {"customers": [], "revenue": 0, "count": 0})
            custs = set(td["customers"])
            prev_in_tier = {cid for cid in (prev_custs or {}) if _tier(prev_custs[cid]) == tier_name}

            # Churn: customers in this tier last month, not present this month
            churned = prev_in_tier - custs if prev_in_tier else set()
            churn_rate = len(churned) / len(prev_in_tier) * 100 if prev_in_tier else 0

            arpu = td["revenue"] / td["count"] if td["count"] > 0 else 0
            rev_share = td["revenue"] / total_rev * 100 if total_rev > 0 else 0

            by_tier.append({
                "tier": tier_name,
                "customers": td["count"],
                "revenue": round(td["revenue"], 2),
                "revenue_share_pct": round(rev_share, 1),
                "arpu": round(arpu, 2),
                "churn_rate": round(churn_rate, 1),
                "churned": len(churned),
                "period": latest,
            })

        # Build lifecycle stage segments
        stage_data = defaultdict(lambda: {"revenue": 0, "count": 0})
        for cid, rev in latest_custs.items():
            stage = cust_stage.get(cid, "Unknown")
            stage_data[stage]["revenue"] += rev
            stage_data[stage]["count"] += 1

        by_stage = []
        for stage, sd in sorted(stage_data.items(), key=lambda x: -x[1]["revenue"]):
            arpu = sd["revenue"] / sd["count"] if sd["count"] > 0 else 0
            rev_share = sd["revenue"] / total_rev * 100 if total_rev > 0 else 0
            by_stage.append({
                "stage": stage,
                "customers": sd["count"],
                "revenue": round(sd["revenue"], 2),
                "revenue_share_pct": round(rev_share, 1),
                "arpu": round(arpu, 2),
            })

        # Concentration: top customer share
        top_custs = sorted(latest_custs.items(), key=lambda x: -x[1])
        top1_pct = (top_custs[0][1] / total_rev * 100) if top_custs and total_rev > 0 else 0
        top5_pct = (sum(v for _, v in top_custs[:5]) / total_rev * 100) if len(top_custs) >= 5 and total_rev > 0 else 0

        return {
            "segments": {"by_tier": by_tier, "by_stage": by_stage},
            "summary": {
                "total_customers": total_custs,
                "total_revenue": round(total_rev, 2),
                "period": latest,
                "top1_concentration_pct": round(top1_pct, 1),
                "top5_concentration_pct": round(top5_pct, 1),
            },
        }
    finally:
        conn.close()
