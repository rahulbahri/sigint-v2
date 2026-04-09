"""
core/agents/ — Autonomous learning agents for the platform intelligence engine.

Each agent observes data patterns and generates insights stored in agent_insights.
Agents NEVER modify source data — they only recommend. All runs are audit-logged.

Available agents:
  - AnomalyDetector: Real-time anomaly detection on data ingestion
  - CausalLearner: Bayesian causal graph refinement
  - RegimeLearner: Markov regime transition detection + auto-retrain
  - ThresholdLearner: Adaptive bounds and target suggestions
  - DecisionEffectivenessLearner: Decision outcome pattern mining
  - IndustryPatternLearner: Cross-workspace anonymized intelligence
"""
from core.agents.base import BaseAgent
from core.agents.anomaly import AnomalyDetector
from core.agents.orchestrator import run_learning_pipeline
