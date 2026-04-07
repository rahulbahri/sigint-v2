"""
tests/test_forecast_window.py — Tests for configurable model window and seasonality detection.
"""
import json
import math
import pytest
from datetime import datetime

import numpy as np
from core.database import get_db


# ── Helpers ──────────────────────────────────────────────────────────────────

WS = "forecast-test-ws"


def _insert_monthly(workspace_id: str, year: int, month: int, kpis: dict):
    conn = get_db()
    conn.execute(
        "INSERT INTO monthly_data (upload_id, year, month, data_json, workspace_id) VALUES (?,?,?,?,?)",
        (1, year, month, json.dumps(kpis), workspace_id),
    )
    conn.commit()
    conn.close()


def _clear_monthly(workspace_id: str):
    conn = get_db()
    conn.execute("DELETE FROM monthly_data WHERE workspace_id=?", [workspace_id])
    conn.commit()
    conn.close()


def _seed_months(workspace_id: str, count: int, start_year: int = 2020, start_month: int = 1):
    """Insert `count` months of data starting from (start_year, start_month)."""
    y, m = start_year, start_month
    for i in range(count):
        _insert_monthly(workspace_id, y, m, {
            "revenue_growth": 5.0 + i * 0.1,
            "gross_margin": 60.0 + i * 0.2,
        })
        m += 1
        if m > 12:
            m = 1
            y += 1


# ── Tests ────────────────────────────────────────────────────────────────────

def test_history_no_filter():
    """Without months_back, all data is returned."""
    from routers.forecast import _mrk_monthly_history
    _clear_monthly(WS)
    _seed_months(WS, 60, start_year=2020, start_month=1)

    result = _mrk_monthly_history(WS)
    # Should have 60 values for revenue_growth
    assert len(result.get("revenue_growth", [])) == 60
    _clear_monthly(WS)


def test_history_with_filter():
    """With months_back=24, only the most recent 24 months are returned."""
    from routers.forecast import _mrk_monthly_history
    _clear_monthly(WS)

    now = datetime.utcnow()
    # Seed 60 months ending at current month
    total = 60
    end_year, end_month = now.year, now.month
    # Calculate start from end
    start_total = end_year * 12 + end_month - total + 1
    start_year = start_total // 12
    start_month = start_total % 12
    if start_month <= 0:
        start_month += 12
        start_year -= 1
    _seed_months(WS, total, start_year, start_month)

    result = _mrk_monthly_history(WS, months_back=24)
    count = len(result.get("revenue_growth", []))
    # Should be approximately 24 (may vary by 1 due to cutoff alignment)
    assert 23 <= count <= 25, f"Expected ~24 months, got {count}"
    _clear_monthly(WS)


def test_history_dated_with_filter():
    """_mrk_monthly_history_dated also respects months_back."""
    from routers.forecast import _mrk_monthly_history_dated
    _clear_monthly(WS)

    now = datetime.utcnow()
    total = 48
    end_year, end_month = now.year, now.month
    start_total = end_year * 12 + end_month - total + 1
    start_year = start_total // 12
    start_month = start_total % 12
    if start_month <= 0:
        start_month += 12
        start_year -= 1
    _seed_months(WS, total, start_year, start_month)

    result = _mrk_monthly_history_dated(WS, months_back=12)
    count = len(result)
    assert 11 <= count <= 13, f"Expected ~12 months, got {count}"
    _clear_monthly(WS)


def test_history_filter_larger_than_data():
    """months_back larger than available data returns all data."""
    from routers.forecast import _mrk_monthly_history
    _clear_monthly(WS)
    _seed_months(WS, 10, start_year=2024, start_month=1)

    result = _mrk_monthly_history(WS, months_back=60)
    assert len(result.get("revenue_growth", [])) == 10
    _clear_monthly(WS)


def test_history_filter_none_returns_all():
    """months_back=None returns all data (backward compatible)."""
    from routers.forecast import _mrk_monthly_history
    _clear_monthly(WS)
    _seed_months(WS, 36, start_year=2022, start_month=1)

    result_all = _mrk_monthly_history(WS, months_back=None)
    result_default = _mrk_monthly_history(WS)
    assert len(result_all.get("revenue_growth", [])) == len(result_default.get("revenue_growth", []))
    _clear_monthly(WS)


# ── Seasonality Detection Tests ──────────────────────────────────────────────

def test_seasonality_sine_pattern():
    """36 months of sinusoidal data is detected as seasonal."""
    from routers.forecast import _detect_seasonality

    # Build dated_history with a clear 12-month seasonal pattern
    dated = {}
    for i in range(36):
        y = 2022 + (i // 12)
        m = (i % 12) + 1
        # Sine wave with period 12 months + linear trend
        val = 100 + 20 * math.sin(2 * math.pi * i / 12) + i * 0.5
        dated[(y, m)] = {"revenue_growth": val, "gross_margin": 60.0}

    result = _detect_seasonality(dated, ["revenue_growth", "gross_margin"])
    assert "revenue_growth" in result
    assert result["revenue_growth"]["seasonal"] is True
    assert result["revenue_growth"]["strength"] > 0.3


def test_seasonality_random_data():
    """36 months of random noise is not detected as seasonal."""
    from routers.forecast import _detect_seasonality

    np.random.seed(42)
    dated = {}
    for i in range(36):
        y = 2022 + (i // 12)
        m = (i % 12) + 1
        dated[(y, m)] = {"noisy_metric": np.random.randn() * 10 + 50}

    result = _detect_seasonality(dated, ["noisy_metric"])
    assert "noisy_metric" in result
    assert result["noisy_metric"]["seasonal"] is False


def test_seasonality_insufficient_data():
    """Less than 24 months of data returns empty result for that KPI."""
    from routers.forecast import _detect_seasonality

    dated = {}
    for i in range(12):
        dated[(2024, i + 1)] = {"short_series": 50.0 + i}

    result = _detect_seasonality(dated, ["short_series"])
    assert "short_series" not in result


def test_seasonality_constant_series():
    """Constant series (zero variance) is not seasonal."""
    from routers.forecast import _detect_seasonality

    dated = {}
    for i in range(36):
        y = 2022 + (i // 12)
        m = (i % 12) + 1
        dated[(y, m)] = {"flat_metric": 42.0}

    result = _detect_seasonality(dated, ["flat_metric"])
    assert "flat_metric" in result
    assert result["flat_metric"]["seasonal"] is False
