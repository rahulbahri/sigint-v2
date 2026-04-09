"""
routers/deferred_revenue.py — ASC 606 Revenue Recognition tracking.

Computes deferred revenue by analyzing subscription patterns:
- Monthly subscriptions: recognized immediately (0 deferred)
- Annual/multi-month: recognized ratably over contract term
- One-time: recognized immediately

Returns per-period: recognized revenue, deferred revenue balance,
new bookings, and recognition schedule.
"""
import json
from collections import defaultdict
from datetime import datetime

from fastapi import APIRouter, Request

from core.database import get_db
from core.deps import _require_workspace

router = APIRouter()

# Subscription type → assumed contract months
_CONTRACT_MONTHS = {
    "annual":       12,
    "yearly":       12,
    "bi-annual":    24,
    "biannual":     24,
    "quarterly":     3,
    "monthly":       1,
    "recurring":     1,  # default monthly recognition
    "subscription":  1,  # default monthly
    "one-time":      1,  # immediate recognition
    "one_time":      1,
    "perpetual":     1,
}


def _contract_term(sub_type: str) -> int:
    """Return contract term in months for a subscription type."""
    return _CONTRACT_MONTHS.get((sub_type or "").lower().strip(), 1)


@router.get("/api/deferred-revenue", tags=["Finance"])
def deferred_revenue_schedule(request: Request):
    """
    Compute ASC 606 revenue recognition schedule.

    For each revenue transaction:
    - Determine contract term from subscription_type
    - Spread recognition ratably over the term
    - Track deferred revenue balance per period
    """
    workspace_id = _require_workspace(request)
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT amount, period, customer_id, subscription_type "
            "FROM canonical_revenue WHERE workspace_id=?",
            [workspace_id],
        ).fetchall()

        if not rows:
            return {"schedule": [], "summary": {}, "by_type": []}

        # Build recognition schedule
        # For each transaction, spread the amount across contract_months starting from the transaction period
        recognized_by_period = defaultdict(float)
        deferred_by_period = defaultdict(float)
        bookings_by_period = defaultdict(float)
        type_summary = defaultdict(lambda: {"amount": 0, "count": 0})

        all_periods = set()

        for r in rows:
            amount = float(r["amount"] or 0)
            period = str(r["period"] or "")[:7]
            sub_type = str(r["subscription_type"] or "monthly").lower().strip()
            term = _contract_term(sub_type)

            if not period or amount <= 0:
                continue

            bookings_by_period[period] += amount
            type_summary[sub_type]["amount"] += amount
            type_summary[sub_type]["count"] += 1

            # Parse period to year, month
            try:
                parts = period.split("-")
                yr, mo = int(parts[0]), int(parts[1])
            except (ValueError, IndexError):
                continue

            # Recognize ratably over term months
            monthly_recognition = amount / term
            for offset in range(term):
                rec_mo = mo + offset
                rec_yr = yr + (rec_mo - 1) // 12
                rec_mo = ((rec_mo - 1) % 12) + 1
                rec_period = f"{rec_yr}-{rec_mo:02d}"

                recognized_by_period[rec_period] += monthly_recognition
                all_periods.add(rec_period)

            # Deferred = total booked - recognized so far
            # For multi-month contracts, deferred starts at (amount - monthly) and decreases
            if term > 1:
                for offset in range(term):
                    def_mo = mo + offset
                    def_yr = yr + (def_mo - 1) // 12
                    def_mo = ((def_mo - 1) % 12) + 1
                    def_period = f"{def_yr}-{def_mo:02d}"
                    remaining = amount - monthly_recognition * (offset + 1)
                    if remaining > 0:
                        deferred_by_period[def_period] += remaining

            all_periods.add(period)

        # Build the schedule sorted by period
        sorted_periods = sorted(all_periods)
        schedule = []
        running_deferred = 0

        for period in sorted_periods:
            booked = bookings_by_period.get(period, 0)
            recognized = recognized_by_period.get(period, 0)
            deferred = deferred_by_period.get(period, 0)

            schedule.append({
                "period": period,
                "bookings": round(booked, 2),
                "recognized": round(recognized, 2),
                "deferred_balance": round(deferred, 2),
                "recognition_rate": round(recognized / booked * 100, 1) if booked > 0 else 100.0,
            })

        # Summary
        total_booked = sum(bookings_by_period.values())
        total_recognized = sum(recognized_by_period.values())
        latest_deferred = schedule[-1]["deferred_balance"] if schedule else 0

        by_type = [
            {"type": t, "amount": round(d["amount"], 2), "count": d["count"],
             "term_months": _contract_term(t), "pct_of_total": round(d["amount"] / total_booked * 100, 1) if total_booked > 0 else 0}
            for t, d in sorted(type_summary.items(), key=lambda x: -x[1]["amount"])
        ]

        return {
            "schedule": schedule,
            "summary": {
                "total_booked": round(total_booked, 2),
                "total_recognized": round(total_recognized, 2),
                "current_deferred": round(latest_deferred, 2),
                "deferred_pct": round(latest_deferred / total_booked * 100, 1) if total_booked > 0 else 0,
                "periods": len(sorted_periods),
            },
            "by_type": by_type,
        }
    finally:
        conn.close()
