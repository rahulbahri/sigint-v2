"""
Orchestrator — triggers autonomous agents after data ingestion.

Called from upload.py and connectors.py after canonical→monthly aggregation.
Runs agents in a background thread so the upload response isn't blocked.
"""
import json
import logging
import threading
from core.database import get_db

logger = logging.getLogger(__name__)


def run_learning_pipeline(workspace_id: str, trigger: str = "upload"):
    """Run all eligible autonomous agents in a background thread.

    This is the main entry point called after data ingestion. It dispatches
    agents based on data availability (months of history, resolved decisions, etc.)
    """
    def _background():
        conn = get_db()
        try:
            _run_agents(conn, workspace_id, trigger)
        except Exception as e:
            logger.error("learning_pipeline_failed: %s", e, exc_info=True)
        finally:
            conn.close()

    t = threading.Thread(target=_background, daemon=True)
    t.start()
    return t


def _run_agents(conn, workspace_id: str, trigger: str):
    """Execute agents sequentially based on data eligibility."""

    # Count months of data
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM monthly_data WHERE workspace_id=?",
        [workspace_id],
    ).fetchone()
    months = row["cnt"] if row else 0

    results = []

    # Agent 1: AnomalyDetector (always runs, even with minimal data)
    if months >= 3:
        from core.agents.anomaly import AnomalyDetector
        result = AnomalyDetector(conn, workspace_id).safe_run()
        results.append(result)
        logger.info("agent_anomaly: %s insights for %s", result.get("insights_generated", 0), workspace_id)

    # Agent 2: CausalLearner (needs 12+ months for Granger tests)
    if months >= 12:
        try:
            from core.agents.causal import CausalLearner
            result = CausalLearner(conn, workspace_id).safe_run()
            results.append(result)
            logger.info("agent_causal: %s insights for %s", result.get("insights_generated", 0), workspace_id)
        except ImportError:
            pass  # Agent not yet implemented

    # Agent 3: RegimeLearner (needs 6+ months)
    if months >= 6:
        try:
            from core.agents.regime import RegimeLearner
            result = RegimeLearner(conn, workspace_id).safe_run()
            results.append(result)
            logger.info("agent_regime: %s insights for %s", result.get("insights_generated", 0), workspace_id)
        except ImportError:
            pass

    # Agent 4: ThresholdLearner (needs 6+ months)
    if months >= 6:
        try:
            from core.agents.threshold import ThresholdLearner
            result = ThresholdLearner(conn, workspace_id).safe_run()
            results.append(result)
            logger.info("agent_threshold: %s insights for %s", result.get("insights_generated", 0), workspace_id)
        except ImportError:
            pass

    # Agent 5: DecisionEffectivenessLearner (needs 3+ resolved decisions)
    try:
        dec_row = conn.execute(
            "SELECT COUNT(*) as cnt FROM decisions WHERE workspace_id=? AND status IN ('resolved', 'reversed')",
            [workspace_id],
        ).fetchone()
        if dec_row and dec_row["cnt"] >= 3:
            from core.agents.decision import DecisionEffectivenessLearner
            result = DecisionEffectivenessLearner(conn, workspace_id).safe_run()
            results.append(result)
            logger.info("agent_decision: %s insights for %s", result.get("insights_generated", 0), workspace_id)
    except (ImportError, Exception):
        pass

    # Agent 6: IndustryPatternLearner (cross-workspace, runs less frequently)
    try:
        from core.agents.industry import IndustryPatternLearner
        result = IndustryPatternLearner(conn, workspace_id).safe_run()
        results.append(result)
    except ImportError:
        pass

    return results
