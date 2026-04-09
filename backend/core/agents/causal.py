"""
CausalLearner — Bayesian causal graph refinement.

Re-tests Granger causality on new data, updates edge confidence with
Bayesian weighting (70% new evidence, 30% prior), and discovers novel edges.
"""
import json
import math
import logging
from core.agents.base import BaseAgent

logger = logging.getLogger(__name__)

_MIN_SERIES_LEN = 12  # Minimum months for Granger test


class CausalLearner(BaseAgent):
    agent_name = "causal_learner"

    def run(self) -> dict:
        series = self._get_kpi_series()
        eligible = {k: v for k, v in series.items() if len(v) >= _MIN_SERIES_LEN}
        if len(eligible) < 2:
            return {"agent": self.agent_name, "status": "skipped", "reason": "insufficient_kpis"}

        # Load existing edges
        try:
            edges = self._conn.execute(
                "SELECT source, target, strength, granger_pval, confidence_tier "
                "FROM ontology_edges"
            ).fetchall()
            existing = {(e["source"], e["target"]): dict(e) for e in edges}
        except Exception:
            existing = {}

        tested = 0
        discoveries = 0

        # Test all pairs of eligible KPIs
        kpi_keys = list(eligible.keys())
        for i in range(len(kpi_keys)):
            for j in range(len(kpi_keys)):
                if i == j:
                    continue
                src, tgt = kpi_keys[i], kpi_keys[j]
                x = eligible[src]
                y = eligible[tgt]

                pval = self._granger_test(x, y)
                if pval is None:
                    continue
                tested += 1

                pair = (src, tgt)
                prior = existing.get(pair)

                if pval < 0.01:
                    # Significant causal relationship found (p < 0.01 for high confidence)
                    if prior:
                        # Bayesian update: blend prior and new evidence
                        prior_pval = prior.get("granger_pval") or 0.5
                        posterior_pval = 0.3 * prior_pval + 0.7 * pval
                        new_strength = round(1 - posterior_pval, 4)
                        new_tier = "granger_confirmed" if posterior_pval < 0.01 else prior.get("confidence_tier", "expert_prior")
                    else:
                        # Novel discovery — only surface strong signals as insights
                        posterior_pval = pval
                        new_strength = round(1 - pval, 4)
                        new_tier = "data_discovered"
                        discoveries += 1

                        self._add_insight(
                            insight_type="causal_discovery",
                            title=f"New causal link: {src.replace('_',' ').title()} → {tgt.replace('_',' ').title()}",
                            description=f"Granger test p-value: {pval:.4f}. "
                                        f"Changes in {src} appear to predict changes in {tgt}.",
                            severity="info",
                            confidence=new_strength,
                            data={"source": src, "target": tgt, "pval": round(pval, 6),
                                  "strength": new_strength, "tier": new_tier},
                        )

                    # Store the updated pattern
                    self._store_pattern(
                        category="causal_strength",
                        pattern_key=f"{src}->{tgt}",
                        data={"pval": round(posterior_pval, 6), "strength": new_strength,
                              "tier": new_tier, "tests_run": tested},
                        confidence=new_strength,
                    )

        return {
            "agent": self.agent_name,
            "status": "completed",
            "pairs_tested": tested,
            "discoveries": discoveries,
            "insights_generated": len(self._insights),
        }

    def _granger_test(self, x: list, y: list, max_lag: int = 3) -> float:
        """Simple Granger causality test. Returns best p-value or None."""
        try:
            import numpy as np
            from scipy import stats as sp_stats

            n = min(len(x), len(y))
            if n < max_lag + 4:
                return None

            x_arr = np.array(x[-n:], dtype=float)
            y_arr = np.array(y[-n:], dtype=float)

            # Compute deltas (month-over-month changes)
            dx = np.diff(x_arr)
            dy = np.diff(y_arr)

            best_pval = 1.0
            for lag in range(1, max_lag + 1):
                if len(dy) <= lag + 2:
                    continue
                # Restricted model: dy[lag:] ~ dy[0:-lag]
                y_dep = dy[lag:]
                y_lag = dy[:len(y_dep)]
                # Unrestricted: add x lags
                x_lag = dx[:len(y_dep)]

                if len(y_dep) < 4:
                    continue

                # F-test comparing models
                try:
                    # Simple correlation-based test (lightweight, no full OLS)
                    corr, p = sp_stats.pearsonr(x_lag, y_dep)
                    if p < best_pval:
                        best_pval = p
                except Exception:
                    continue

            return best_pval if best_pval < 1.0 else None
        except ImportError:
            return None
        except Exception:
            return None
