"""
ThresholdLearner — Adaptive bounds and target suggestions.

Analyzes KPI distributions to suggest optimal bounds and targets.
Compares actuals vs targets over time to detect persistent over/under-performance.
"""
import json
import math
import statistics
import logging
from core.agents.base import BaseAgent

logger = logging.getLogger(__name__)


class ThresholdLearner(BaseAgent):
    agent_name = "threshold_learner"

    def run(self) -> dict:
        series = self._get_kpi_series()
        if not series:
            return {"agent": self.agent_name, "status": "skipped", "reason": "no_data"}

        # Load targets
        try:
            target_rows = self._conn.execute(
                "SELECT kpi_key, target_value, direction FROM kpi_targets WHERE workspace_id=?",
                [self._ws],
            ).fetchall()
            targets = {r["kpi_key"]: {"target": r["target_value"], "direction": r["direction"] or "higher"}
                       for r in target_rows}
        except Exception:
            targets = {}

        suggestions = 0

        for kpi, vals in series.items():
            if len(vals) < 6:
                continue

            target_info = targets.get(kpi)
            if not target_info or target_info["target"] is None:
                continue

            target = target_info["target"]
            direction = target_info["direction"]

            # Check if target is consistently exceeded
            recent_12 = vals[-12:] if len(vals) >= 12 else vals
            if direction == "higher":
                above_count = sum(1 for v in recent_12 if v > target * 1.05)
                below_count = sum(1 for v in recent_12 if v < target * 0.90)
            else:
                above_count = sum(1 for v in recent_12 if v < target * 0.95)  # "below target" is good for lower-is-better
                below_count = sum(1 for v in recent_12 if v > target * 1.10)

            # Suggest raising target if consistently exceeded
            if above_count >= len(recent_12) * 0.75:
                avg = sum(recent_12) / len(recent_12)
                suggested = round(avg * 0.95, 2)  # Set slightly below current performance
                suggestions += 1
                self._add_insight(
                    insight_type="target_suggestion",
                    title=f"Consider raising {kpi.replace('_', ' ').title()} target",
                    description=(
                        f"Target of {target} has been exceeded {above_count}/{len(recent_12)} recent months. "
                        f"Average performance is {avg:.2f}. Consider raising to {suggested}."
                    ),
                    severity="info",
                    confidence=above_count / len(recent_12),
                    data={"kpi": kpi, "current_target": target, "suggested_target": suggested,
                          "avg_performance": round(avg, 2), "months_exceeded": above_count,
                          "months_checked": len(recent_12)},
                )

            # Warn if consistently below target (structural miss)
            elif below_count >= len(recent_12) * 0.75:
                avg = sum(recent_12) / len(recent_12)
                gap_pct = abs((avg - target) / target * 100) if target else 0
                suggestions += 1
                self._add_insight(
                    insight_type="structural_miss",
                    title=f"{kpi.replace('_', ' ').title()} has structurally missed target",
                    description=(
                        f"Below target {below_count}/{len(recent_12)} months ({gap_pct:.0f}% average gap). "
                        f"This is not a variance — it's structural. Either address the root cause or adjust the target."
                    ),
                    severity="warning",
                    confidence=below_count / len(recent_12),
                    data={"kpi": kpi, "target": target, "avg": round(avg, 2),
                          "gap_pct": round(gap_pct, 1), "months_below": below_count},
                )

            # Adaptive bounds: IQR-based outlier detection
            try:
                q1 = statistics.quantiles(vals, n=4)[0]
                q3 = statistics.quantiles(vals, n=4)[2]
                iqr = q3 - q1
                lower = round(q1 - 1.5 * iqr, 2)
                upper = round(q3 + 1.5 * iqr, 2)

                self._store_pattern(
                    category="adaptive_bounds",
                    pattern_key=kpi,
                    data={"q1": round(q1, 2), "q3": round(q3, 2), "iqr": round(iqr, 2),
                          "lower": lower, "upper": upper,
                          "n": len(vals)},
                    sample_size=len(vals),
                    confidence=min(len(vals) / 24, 1.0),  # Full confidence at 24+ months
                )
            except Exception:
                pass

        return {
            "agent": self.agent_name,
            "status": "completed",
            "suggestions": suggestions,
            "insights_generated": len(self._insights),
        }
