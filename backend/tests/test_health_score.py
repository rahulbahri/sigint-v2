"""
tests/test_health_score.py — Unit tests for the Health Score algorithm.
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pytest
from core.health_score import (
    _compute_momentum,
    _compute_target_achievement_with_directions as _compute_target_achievement,
    _compute_risk_flags,
)


def test_momentum_all_improving():
    ts = {"rev": [1,2,3,4,5,6,7,8,9]}
    dirs = {"rev": "higher"}
    score = _compute_momentum(ts, dirs)
    assert score == 100.0

def test_momentum_all_declining():
    ts = {"rev": [9,8,7,6,5,4,3,2,1]}
    dirs = {"rev": "higher"}
    score = _compute_momentum(ts, dirs)
    assert score == 0.0

def test_momentum_no_signal_short_series():
    ts = {"rev": [1,2,3]}   # less than 6 months
    dirs = {"rev": "higher"}
    score = _compute_momentum(ts, dirs)
    assert score == 50.0

def test_target_achievement_all_green():
    avgs = {"rev": 100, "margin": 50}
    targets = {"rev": 100, "margin": 50}
    dirs = {"rev": "higher", "margin": "higher"}
    score = _compute_target_achievement(avgs, targets, dirs)
    assert score == 100.0

def test_target_achievement_none_green():
    avgs = {"rev": 50, "margin": 20}
    targets = {"rev": 100, "margin": 50}
    dirs = {"rev": "higher", "margin": "higher"}
    score = _compute_target_achievement(avgs, targets, dirs)
    assert score == 0.0

def test_target_achievement_no_targets():
    score = _compute_target_achievement({}, {}, {})
    assert score == 50.0

def test_risk_flags_no_red():
    avgs = {"rev": 100}
    targets = {"rev": 100}
    dirs = {"rev": "higher"}
    score = _compute_risk_flags(avgs, targets, dirs)
    assert score == 100.0

def test_risk_flags_all_red():
    avgs = {"rev": 10}
    targets = {"rev": 100}
    dirs = {"rev": "higher"}
    score = _compute_risk_flags(avgs, targets, dirs)
    assert score == 0.0

def test_risk_flags_no_targets():
    score = _compute_risk_flags({}, {}, {})
    assert score == 70.0
