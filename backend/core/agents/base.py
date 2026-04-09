"""
Base class for all autonomous learning agents.

Every agent follows the same contract:
  1. Receives a database connection + workspace_id
  2. Reads data (NEVER modifies source data)
  3. Produces insights stored in agent_insights table
  4. Logs to audit trail
  5. Handles all exceptions gracefully (never blocks the pipeline)
"""
import json
import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


class BaseAgent:
    """Base class for autonomous learning agents."""

    agent_name: str = "base"  # Override in subclass

    def __init__(self, conn, workspace_id: str):
        self._conn = conn
        self._ws = workspace_id
        self._insights: list[dict] = []

    def run(self) -> dict:
        """Execute the agent. Override in subclass."""
        raise NotImplementedError

    def safe_run(self) -> dict:
        """Run with full exception handling. Never throws."""
        started = datetime.utcnow().isoformat()
        try:
            result = self.run()
            self._persist_insights()
            self._audit(result, started)
            return result
        except Exception as e:
            logger.error("agent_%s_failed: %s", self.agent_name, e, exc_info=True)
            return {"agent": self.agent_name, "status": "error", "error": str(e)}

    def _add_insight(
        self,
        insight_type: str,
        title: str,
        description: str = "",
        severity: str = "info",
        confidence: Optional[float] = None,
        data: Optional[dict] = None,
        expires_at: Optional[str] = None,
    ):
        """Queue an insight for persistence."""
        self._insights.append({
            "workspace_id": self._ws,
            "agent_name":   self.agent_name,
            "insight_type": insight_type,
            "title":        title,
            "description":  description,
            "severity":     severity,
            "confidence":   confidence,
            "data_json":    json.dumps(data or {}),
            "expires_at":   expires_at,
        })

    def _persist_insights(self):
        """Write all queued insights to the database."""
        for ins in self._insights:
            try:
                self._conn.execute(
                    "INSERT INTO agent_insights "
                    "(workspace_id, agent_name, insight_type, title, description, "
                    "severity, confidence, data_json, expires_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    [ins["workspace_id"], ins["agent_name"], ins["insight_type"],
                     ins["title"], ins["description"], ins["severity"],
                     ins["confidence"], ins["data_json"], ins["expires_at"]],
                )
            except Exception as e:
                logger.warning("insight_persist_failed: %s", e)
        if self._insights:
            try:
                self._conn.commit()
            except Exception:
                pass

    def _store_pattern(self, category: str, pattern_key: str, data: dict,
                       sample_size: int = 0, confidence: float = None):
        """Upsert a learned pattern."""
        try:
            existing = self._conn.execute(
                "SELECT id, version FROM learned_patterns "
                "WHERE workspace_id=? AND category=? AND pattern_key=?",
                [self._ws, category, pattern_key],
            ).fetchone()
            if existing:
                self._conn.execute(
                    "UPDATE learned_patterns SET pattern_data=?, sample_size=?, "
                    "confidence=?, version=version+1, last_updated=datetime('now') "
                    "WHERE id=?",
                    [json.dumps(data), sample_size, confidence, existing["id"]],
                )
            else:
                self._conn.execute(
                    "INSERT INTO learned_patterns "
                    "(workspace_id, category, pattern_key, pattern_data, sample_size, confidence) "
                    "VALUES (?,?,?,?,?,?)",
                    [self._ws, category, pattern_key, json.dumps(data), sample_size, confidence],
                )
            self._conn.commit()
        except Exception as e:
            logger.warning("pattern_store_failed: %s", e)

    def _audit(self, result: dict, started: str):
        """Log agent run to audit trail."""
        try:
            from core.database import _audit
            n_insights = len(self._insights)
            _audit(
                "agent_run", "agent", self.agent_name,
                f"Agent {self.agent_name}: {result.get('status', 'ok')}, "
                f"{n_insights} insight(s) generated",
                workspace_id=self._ws,
            )
        except Exception:
            pass

    def _get_monthly_data(self) -> list:
        """Load all monthly_data rows for this workspace."""
        try:
            return self._conn.execute(
                "SELECT year, month, data_json FROM monthly_data "
                "WHERE workspace_id=? ORDER BY year, month",
                [self._ws],
            ).fetchall()
        except Exception:
            return []

    def _get_kpi_series(self) -> dict:
        """Build {kpi_key: [values]} from monthly data."""
        import math
        series = {}
        for r in self._get_monthly_data():
            d = json.loads(r["data_json"]) if isinstance(r["data_json"], str) else (r["data_json"] or {})
            for k, v in d.items():
                if k.startswith("_") or k in ("year", "month"):
                    continue
                if isinstance(v, (int, float)) and math.isfinite(v):
                    series.setdefault(k, []).append(v)
        return series
