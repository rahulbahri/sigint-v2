"""
RegimeLearner — Detects regime transitions and triggers model retraining.

Classifies the latest month's data against existing regime clusters.
If a transition is detected, generates an insight explaining the shift.
"""
import json
import math
import logging
from core.agents.base import BaseAgent

logger = logging.getLogger(__name__)


class RegimeLearner(BaseAgent):
    agent_name = "regime_learner"

    def run(self) -> dict:
        # Load current Markov model
        try:
            model_row = self._conn.execute(
                "SELECT regime_data, seasonality_data, trained_at FROM markov_models "
                "WHERE workspace_id=? ORDER BY trained_at DESC LIMIT 1",
                [self._ws],
            ).fetchone()
        except Exception:
            model_row = None

        if not model_row or not model_row["regime_data"]:
            return {"agent": self.agent_name, "status": "skipped", "reason": "no_trained_model"}

        regime_data = json.loads(model_row["regime_data"]) if isinstance(model_row["regime_data"], str) else model_row["regime_data"]

        # Check months since last training
        trained_at = model_row["trained_at"] or ""
        rows = self._get_monthly_data()
        if len(rows) < 6:
            return {"agent": self.agent_name, "status": "skipped", "reason": "insufficient_data"}

        # Count months since training
        months_since_train = 0
        if trained_at:
            for r in reversed(rows):
                period = f"{r['year']}-{r['month']:02d}"
                if period <= trained_at[:7]:
                    break
                months_since_train += 1

        # Suggest retraining if 3+ new months
        if months_since_train >= 3:
            self._add_insight(
                insight_type="retrain_suggestion",
                title=f"Forecast model is {months_since_train} months stale",
                description=(
                    f"The Markov model was last trained on {trained_at[:10]}. "
                    f"{months_since_train} new months of data are available. "
                    f"Rebuilding would improve forecast accuracy."
                ),
                severity="warning",
                confidence=0.8,
                data={"months_since_train": months_since_train, "trained_at": trained_at},
            )

        # Detect regime transition using latest month vs regime centroids
        current_regime = regime_data.get("current_regime")
        regime_names = regime_data.get("regime_names", {})

        if current_regime is not None and regime_names:
            current_name = regime_names.get(str(current_regime), f"Regime {current_regime}")

            # Check if KPI trends suggest a shift
            series = self._get_kpi_series()
            shift_signals = 0
            improving = []
            deteriorating = []

            for kpi, vals in series.items():
                if len(vals) < 6:
                    continue
                recent_3 = vals[-3:]
                prior_3 = vals[-6:-3]
                r_avg = sum(recent_3) / len(recent_3)
                p_avg = sum(prior_3) / len(prior_3)
                if abs(p_avg) > 0.01:
                    chg = (r_avg - p_avg) / abs(p_avg) * 100
                    if chg > 10:
                        improving.append((kpi, round(chg, 1)))
                        shift_signals += 1
                    elif chg < -10:
                        deteriorating.append((kpi, round(chg, 1)))
                        shift_signals += 1

            if shift_signals >= 3:
                direction = "improving" if len(improving) > len(deteriorating) else "deteriorating"
                top_movers = (improving if direction == "improving" else deteriorating)[:3]
                kpi_str = ", ".join(f"{k.replace('_',' ').title()} ({v:+.1f}%)" for k, v in top_movers)

                self._add_insight(
                    insight_type="regime_transition",
                    title=f"Potential regime shift detected ({direction})",
                    description=(
                        f"Current regime: {current_name}. {shift_signals} KPIs show "
                        f"significant {'improvement' if direction == 'improving' else 'deterioration'}. "
                        f"Key movers: {kpi_str}."
                    ),
                    severity="warning" if direction == "deteriorating" else "positive",
                    confidence=min(shift_signals / 10, 0.95),
                    data={
                        "current_regime": current_name,
                        "direction": direction,
                        "shift_signals": shift_signals,
                        "improving": improving[:5],
                        "deteriorating": deteriorating[:5],
                    },
                )

        return {
            "agent": self.agent_name,
            "status": "completed",
            "months_since_train": months_since_train,
            "insights_generated": len(self._insights),
        }
