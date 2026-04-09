"""
IndustryPatternLearner — Cross-workspace anonymized intelligence.

Aggregates KPI distributions across all workspaces (grouped by company_stage)
to build dynamic benchmarks and discover industry archetypes.

Privacy: NEVER stores or returns absolute values, company names, or workspace IDs.
Only stores percentile positions and anonymized aggregates.
"""
import json
import math
import statistics
import logging
from collections import defaultdict
from core.agents.base import BaseAgent

logger = logging.getLogger(__name__)


class IndustryPatternLearner(BaseAgent):
    agent_name = "industry_learner"

    def run(self) -> dict:
        # Aggregate across ALL workspaces (anonymized)
        try:
            workspaces = self._conn.execute(
                "SELECT DISTINCT workspace_id FROM monthly_data WHERE workspace_id != ''"
            ).fetchall()
        except Exception:
            return {"agent": self.agent_name, "status": "skipped", "reason": "query_failed"}

        if len(workspaces) < 2:
            return {"agent": self.agent_name, "status": "skipped",
                    "reason": f"need 2+ workspaces, have {len(workspaces)}"}

        # Build per-stage KPI distributions
        stage_kpi_values = defaultdict(lambda: defaultdict(list))

        for ws_row in workspaces:
            ws_id = ws_row["workspace_id"]

            # Get company stage
            try:
                stage_row = self._conn.execute(
                    "SELECT value FROM company_settings WHERE workspace_id=? AND key='funding_stage'",
                    [ws_id],
                ).fetchone()
                stage = (stage_row["value"] if stage_row else "series_a") or "series_a"
                stage = stage.lower().replace(" ", "_").replace("-", "_")
                if stage not in ("seed", "series_a", "series_b", "series_c"):
                    stage = "series_a"
            except Exception:
                stage = "series_a"

            # Get latest 6 months of KPI data for this workspace
            try:
                rows = self._conn.execute(
                    "SELECT data_json FROM monthly_data WHERE workspace_id=? "
                    "ORDER BY year DESC, month DESC LIMIT 6",
                    [ws_id],
                ).fetchall()
            except Exception:
                continue

            # Compute averages per KPI
            kpi_sums = defaultdict(lambda: {"sum": 0, "count": 0})
            for r in rows:
                d = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})
                for k, v in d.items():
                    if k.startswith("_") or k in ("year", "month"):
                        continue
                    if isinstance(v, (int, float)) and math.isfinite(v):
                        kpi_sums[k]["sum"] += v
                        kpi_sums[k]["count"] += 1

            for k, s in kpi_sums.items():
                if s["count"] > 0:
                    avg = s["sum"] / s["count"]
                    stage_kpi_values[stage][k].append(avg)

        # Compute dynamic benchmarks
        benchmarks_updated = 0
        for stage, kpi_map in stage_kpi_values.items():
            for kpi, values in kpi_map.items():
                if len(values) < 2:
                    continue

                sorted_vals = sorted(values)
                n = len(sorted_vals)

                def percentile(p):
                    idx = (p / 100) * (n - 1)
                    lo = int(idx)
                    hi = min(lo + 1, n - 1)
                    frac = idx - lo
                    return round(sorted_vals[lo] * (1 - frac) + sorted_vals[hi] * frac, 4)

                try:
                    self._conn.execute(
                        "INSERT INTO industry_intelligence "
                        "(kpi_key, stage, p10, p25, p50, p75, p90, sample_size) "
                        "VALUES (?,?,?,?,?,?,?,?)",
                        [kpi, stage, percentile(10), percentile(25), percentile(50),
                         percentile(75), percentile(90), n],
                    )
                    benchmarks_updated += 1
                except Exception:
                    pass

        try:
            self._conn.commit()
        except Exception:
            pass

        if benchmarks_updated > 0:
            self._add_insight(
                insight_type="industry_update",
                title=f"Dynamic benchmarks updated across {len(workspaces)} workspaces",
                description=(
                    f"Computed percentile benchmarks for {benchmarks_updated} KPI×stage combinations. "
                    f"These supplement static industry benchmarks with real platform data."
                ),
                severity="info",
                confidence=min(len(workspaces) / 10, 1.0),
                data={"workspaces": len(workspaces), "benchmarks_updated": benchmarks_updated,
                      "stages": list(stage_kpi_values.keys())},
            )

        return {
            "agent": self.agent_name,
            "status": "completed",
            "workspaces_analyzed": len(workspaces),
            "benchmarks_updated": benchmarks_updated,
            "insights_generated": len(self._insights),
        }
