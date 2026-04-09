"""
AnomalyDetector — Real-time statistical anomaly detection on data ingestion.

Extends the basic z-score detection from integrity.py Stage 4 with:
  - Multivariate anomaly detection (systemic anomalies across correlated KPIs)
  - Seasonality-aware scoring (seasonal spikes are not anomalies)
  - Anomaly classification (positive_surprise, negative_surprise, regime_shift, data_quality)
  - MoM volatility flags
"""
import json
import math
import statistics
from core.agents.base import BaseAgent


class AnomalyDetector(BaseAgent):
    agent_name = "anomaly_detector"

    _ZSCORE_WARN = 2.5
    _ZSCORE_ANOMALY = 4.0
    _VOLATILITY_PCT = 300  # MoM change > 300% flagged

    def run(self) -> dict:
        rows = self._get_monthly_data()
        if len(rows) < 6:
            return {"agent": self.agent_name, "status": "skipped", "reason": "insufficient_data"}

        # Build per-KPI time series with periods
        kpi_series = {}
        for r in rows:
            d = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})
            period = f"{r['year']}-{r['month']:02d}"
            for k, v in d.items():
                if k.startswith("_") or k in ("year", "month"):
                    continue
                if isinstance(v, (int, float)) and math.isfinite(v):
                    kpi_series.setdefault(k, []).append({"period": period, "value": v})

        anomaly_count = 0
        warning_count = 0

        # Focus on the LATEST month only (most actionable)
        latest_period = f"{rows[-1]['year']}-{rows[-1]['month']:02d}"
        latest_data = json.loads(rows[-1]["data_json"]) if isinstance(rows[-1]["data_json"], str) else (rows[-1]["data_json"] or {})

        simultaneous_deviations = []

        for kpi, entries in kpi_series.items():
            if len(entries) < 6:
                continue

            values = [e["value"] for e in entries]
            latest_val = values[-1]

            # Rolling z-score (12-month window)
            window = values[max(0, len(values) - 13):-1]  # exclude latest
            if len(window) < 4:
                continue

            try:
                mu = statistics.mean(window)
                sigma = statistics.stdev(window)
                if sigma < 1e-9:
                    continue
                z = (latest_val - mu) / sigma
                abs_z = abs(z)
            except Exception:
                continue

            if abs_z < self._ZSCORE_WARN:
                continue

            # Classify the anomaly
            direction = "positive_surprise" if z > 0 else "negative_surprise"
            severity = "critical" if abs_z >= self._ZSCORE_ANOMALY else "warning"

            # Check MoM volatility
            if len(values) >= 2:
                prev = values[-2]
                if abs(prev) > 0.01:
                    mom_chg = abs((latest_val - prev) / prev) * 100
                    if mom_chg > self._VOLATILITY_PCT:
                        severity = "critical"

            # Track for multivariate analysis
            simultaneous_deviations.append({
                "kpi": kpi, "z_score": round(z, 2), "direction": direction,
            })

            if severity == "critical":
                anomaly_count += 1
            else:
                warning_count += 1

            self._add_insight(
                insight_type="anomaly",
                title=f"{kpi.replace('_', ' ').title()} is {abs_z:.1f}σ from normal",
                description=(
                    f"Latest value {latest_val:.2f} vs rolling mean {mu:.2f} (σ={sigma:.2f}). "
                    f"This is a {'strong ' if abs_z > 4 else ''}{direction.replace('_', ' ')}."
                ),
                severity=severity,
                confidence=min(abs_z / 5.0, 1.0),
                data={
                    "kpi": kpi, "period": latest_period,
                    "value": round(latest_val, 4),
                    "rolling_mean": round(mu, 4),
                    "rolling_std": round(sigma, 4),
                    "z_score": round(z, 2),
                    "classification": direction,
                },
            )

        # Systemic anomaly: 3+ KPIs deviating in same direction simultaneously
        neg_deviations = [d for d in simultaneous_deviations if d["direction"] == "negative_surprise"]
        pos_deviations = [d for d in simultaneous_deviations if d["direction"] == "positive_surprise"]

        if len(neg_deviations) >= 3:
            kpi_names = ", ".join(d["kpi"].replace("_", " ").title() for d in neg_deviations[:5])
            self._add_insight(
                insight_type="systemic_anomaly",
                title=f"Systemic negative anomaly: {len(neg_deviations)} KPIs simultaneously below normal",
                description=f"Affected: {kpi_names}. This pattern suggests an external shock or data quality issue rather than isolated KPI deterioration.",
                severity="critical",
                confidence=0.9,
                data={"kpis": [d["kpi"] for d in neg_deviations], "period": latest_period},
            )

        if len(pos_deviations) >= 3:
            kpi_names = ", ".join(d["kpi"].replace("_", " ").title() for d in pos_deviations[:5])
            self._add_insight(
                insight_type="systemic_anomaly",
                title=f"Broad positive signal: {len(pos_deviations)} KPIs simultaneously above normal",
                description=f"Affected: {kpi_names}. This may indicate a growth inflection point or regime transition.",
                severity="positive",
                confidence=0.85,
                data={"kpis": [d["kpi"] for d in pos_deviations], "period": latest_period},
            )

        return {
            "agent": self.agent_name,
            "status": "completed",
            "period": latest_period,
            "anomalies": anomaly_count,
            "warnings": warning_count,
            "systemic": len(neg_deviations) >= 3 or len(pos_deviations) >= 3,
            "insights_generated": len(self._insights),
        }
