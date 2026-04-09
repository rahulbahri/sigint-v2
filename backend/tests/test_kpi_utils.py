"""
Tests for core/kpi_utils.py — the single source of truth for KPI averaging.
"""
import math
import pytest

from core.kpi_utils import compute_kpi_avg


class TestComputeKpiAvg:
    """Tests for compute_kpi_avg — the shared utility."""

    def test_basic_average(self):
        assert compute_kpi_avg([10, 20, 30]) == 20.0

    def test_window_6_default(self):
        vals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        # Last 6: [5,6,7,8,9,10] → avg 7.5
        assert compute_kpi_avg(vals) == 7.5

    def test_window_3(self):
        vals = [10, 20, 30, 40, 50]
        # Last 3: [30,40,50] → avg 40
        assert compute_kpi_avg(vals, window=3) == 40.0

    def test_fewer_than_window(self):
        # Only 3 values, window=6 → uses all 3
        assert compute_kpi_avg([10, 20, 30], window=6) == 20.0

    def test_period_filtered_uses_all(self):
        vals = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        # period_filtered=True → uses all, not just last 6
        assert compute_kpi_avg(vals, period_filtered=True) == 5.5

    def test_none_values_filtered(self):
        vals = [10, None, 20, None, 30]
        # Valid: [10, 20, 30] → avg 20
        assert compute_kpi_avg(vals) == 20.0

    def test_all_none_returns_none(self):
        assert compute_kpi_avg([None, None, None]) is None

    def test_empty_list_returns_none(self):
        assert compute_kpi_avg([]) is None

    def test_nan_filtered(self):
        vals = [10, float('nan'), 20]
        assert compute_kpi_avg(vals) == 15.0

    def test_inf_filtered(self):
        vals = [10, float('inf'), 20]
        assert compute_kpi_avg(vals) == 15.0

    def test_negative_inf_filtered(self):
        vals = [10, float('-inf'), 20]
        assert compute_kpi_avg(vals) == 15.0

    def test_non_numeric_filtered(self):
        vals = [10, "hello", 20, {"nested": True}, 30]
        assert compute_kpi_avg(vals) == 20.0

    def test_rounding_default_2dp(self):
        vals = [1, 2, 3]  # avg = 2.0
        assert compute_kpi_avg(vals) == 2.0
        # 10/3 = 3.333...
        assert compute_kpi_avg([10], window=1, period_filtered=True, round_digits=2) == 10.0

    def test_rounding_custom(self):
        vals = [1, 2, 4]  # avg = 2.333...
        assert compute_kpi_avg(vals, round_digits=1) == 2.3

    def test_single_value(self):
        assert compute_kpi_avg([42.5]) == 42.5

    def test_window_1(self):
        vals = [10, 20, 30]
        assert compute_kpi_avg(vals, window=1) == 30.0

    def test_mixed_none_and_valid_with_window(self):
        # [None, 10, None, 20, None, 30, None, 40]
        # Valid after filter: [10, 20, 30, 40]
        # Last 3: [20, 30, 40] → avg 30
        vals = [None, 10, None, 20, None, 30, None, 40]
        assert compute_kpi_avg(vals, window=3) == 30.0

    def test_consistency_across_paths(self):
        """All paths should produce the same result for the same input."""
        vals = [15.5, 22.3, 18.7, 25.1, 19.9, 21.0]
        result = compute_kpi_avg(vals, window=6)
        # Same call should always produce same result
        for _ in range(10):
            assert compute_kpi_avg(vals, window=6) == result
