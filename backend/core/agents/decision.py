"""
DecisionEffectivenessLearner — Mines patterns from decision outcomes.

Correlates logged decisions (with kpi_snapshot → resolved_kpi_snapshot)
to learn what types of decisions work and which don't.
"""
import json
import logging
from collections import defaultdict
from core.agents.base import BaseAgent

logger = logging.getLogger(__name__)


class DecisionEffectivenessLearner(BaseAgent):
    agent_name = "decision_learner"

    def run(self) -> dict:
        # Load resolved decisions
        try:
            rows = self._conn.execute(
                "SELECT id, title, the_decision, rationale, kpi_context, kpi_snapshot, "
                "resolved_kpi_snapshot, decided_by, status, decided_at "
                "FROM decisions WHERE workspace_id=? AND status IN ('resolved', 'reversed')",
                [self._ws],
            ).fetchall()
        except Exception:
            return {"agent": self.agent_name, "status": "skipped", "reason": "query_failed"}

        if len(rows) < 3:
            return {"agent": self.agent_name, "status": "skipped", "reason": "insufficient_decisions"}

        effective = 0
        neutral = 0
        counterproductive = 0
        by_domain = defaultdict(lambda: {"total": 0, "effective": 0})
        by_maker = defaultdict(lambda: {"total": 0, "effective": 0})

        for r in rows:
            snap_before = json.loads(r["kpi_snapshot"] or "{}") if isinstance(r["kpi_snapshot"], str) else (r["kpi_snapshot"] or {})
            snap_after = json.loads(r["resolved_kpi_snapshot"] or "{}") if isinstance(r["resolved_kpi_snapshot"], str) else (r["resolved_kpi_snapshot"] or {})
            kpi_ctx = json.loads(r["kpi_context"] or "[]") if isinstance(r["kpi_context"], str) else (r["kpi_context"] or [])
            maker = r["decided_by"] or "Unknown"

            if not snap_before or not snap_after:
                continue

            # Classify: did linked KPIs improve?
            improved = 0
            worsened = 0
            for kpi in kpi_ctx:
                before = snap_before.get(kpi)
                after = snap_after.get(kpi)
                if before is not None and after is not None:
                    # Simplified: positive delta = improvement (direction-agnostic for now)
                    if after > before * 1.02:
                        improved += 1
                    elif after < before * 0.98:
                        worsened += 1

            if improved > worsened:
                effective += 1
                effectiveness = "effective"
            elif worsened > improved:
                counterproductive += 1
                effectiveness = "counterproductive"
            else:
                neutral += 1
                effectiveness = "neutral"

            # Track by domain (first linked KPI's domain)
            if kpi_ctx:
                domain = kpi_ctx[0].split("_")[0] if "_" in kpi_ctx[0] else "general"
                by_domain[domain]["total"] += 1
                if effectiveness == "effective":
                    by_domain[domain]["effective"] += 1

            by_maker[maker]["total"] += 1
            if effectiveness == "effective":
                by_maker[maker]["effective"] += 1

        total = effective + neutral + counterproductive
        if total < 3:
            return {"agent": self.agent_name, "status": "skipped", "reason": "insufficient_resolved"}

        eff_rate = effective / total * 100

        # Overall effectiveness insight
        self._add_insight(
            insight_type="decision_pattern",
            title=f"Decision effectiveness: {eff_rate:.0f}% ({effective}/{total})",
            description=(
                f"Of {total} resolved decisions: {effective} effective, "
                f"{neutral} neutral, {counterproductive} counterproductive."
            ),
            severity="positive" if eff_rate > 60 else ("warning" if eff_rate < 40 else "info"),
            confidence=min(total / 20, 1.0),
            data={"effective": effective, "neutral": neutral,
                  "counterproductive": counterproductive, "rate": round(eff_rate, 1)},
        )

        # Per-domain patterns
        for domain, stats in by_domain.items():
            if stats["total"] >= 2:
                rate = stats["effective"] / stats["total"] * 100
                self._store_pattern(
                    category="decision_effectiveness",
                    pattern_key=f"domain_{domain}",
                    data={"domain": domain, "total": stats["total"],
                          "effective": stats["effective"], "rate": round(rate, 1)},
                    sample_size=stats["total"],
                    confidence=min(stats["total"] / 10, 1.0),
                )

        # Per-maker patterns
        for maker, stats in by_maker.items():
            if stats["total"] >= 2:
                rate = stats["effective"] / stats["total"] * 100
                self._add_insight(
                    insight_type="decision_maker_pattern",
                    title=f"{maker} decisions: {rate:.0f}% effective ({stats['effective']}/{stats['total']})",
                    description=f"Decisions made by {maker} have a {rate:.0f}% effectiveness rate.",
                    severity="info",
                    confidence=min(stats["total"] / 10, 1.0),
                    data={"maker": maker, **stats, "rate": round(rate, 1)},
                )

        return {
            "agent": self.agent_name,
            "status": "completed",
            "total_decisions": total,
            "effective": effective,
            "effectiveness_rate": round(eff_rate, 1),
            "insights_generated": len(self._insights),
        }
