"""
tests/test_criticality.py — Tests for the Composite Criticality Score engine.
"""
import pytest
from core.criticality import (
    _gap_score,
    _trend_score,
    _impact_score,
    _domain_score,
    get_kpi_domain,
    compute_composite_criticality,
    group_by_domain,
    DEFAULT_WEIGHTS,
    DOMAIN_URGENCY,
    DOMAIN_LABELS,
    DOMAIN_ORDER,
)


# ── Gap Score Tests ──────────────────────────────────────────────────────────

class TestGapScore:
    def test_on_target_higher(self):
        """Higher-is-better KPI at or above target → 0 criticality."""
        assert _gap_score(100, 100, "higher") == 0.0
        assert _gap_score(120, 100, "higher") == 0.0

    def test_below_target_higher(self):
        """Higher-is-better KPI below target → proportional score."""
        score = _gap_score(80, 100, "higher")
        assert score == pytest.approx(20.0, abs=0.1)

    def test_on_target_lower(self):
        """Lower-is-better KPI at or below target → 0 criticality."""
        assert _gap_score(5, 5, "lower") == 0.0
        assert _gap_score(3, 5, "lower") == 0.0

    def test_above_target_lower(self):
        """Lower-is-better KPI above target → proportional score."""
        score = _gap_score(11, 10, "lower")
        assert score == pytest.approx(10.0, abs=0.1)

    def test_capped_at_100(self):
        """Gap > 100% still caps at 100."""
        score = _gap_score(0, 100, "higher")
        assert score == 100.0

    def test_zero_target_higher(self):
        assert _gap_score(5, 0, "higher") == 0.0
        assert _gap_score(-5, 0, "higher") == 50.0

    def test_zero_target_lower(self):
        assert _gap_score(-1, 0, "lower") == 0.0
        assert _gap_score(5, 0, "lower") == 50.0


# ── Trend Score Tests ────────────────────────────────────────────────────────

class TestTrendScore:
    def test_insufficient_data_neutral(self):
        """< 3 data points → neutral 50."""
        assert _trend_score([10, 20], "higher") == 50.0

    def test_improving_higher(self):
        """Rising values for higher-is-better → low criticality (< 50)."""
        score = _trend_score([10, 20, 30, 40, 50, 60], "higher")
        assert score < 50.0

    def test_deteriorating_higher(self):
        """Falling values for higher-is-better → high criticality (> 50)."""
        score = _trend_score([60, 50, 40, 30, 20, 10], "higher")
        assert score > 50.0

    def test_improving_lower(self):
        """Falling values for lower-is-better → low criticality (< 50)."""
        score = _trend_score([60, 50, 40, 30, 20, 10], "lower")
        assert score < 50.0

    def test_deteriorating_lower(self):
        """Rising values for lower-is-better → high criticality (> 50)."""
        score = _trend_score([10, 20, 30, 40, 50, 60], "lower")
        assert score > 50.0

    def test_flat_neutral(self):
        """Flat data → close to 50."""
        score = _trend_score([50, 50, 50, 50, 50, 50], "higher")
        assert score == pytest.approx(50.0, abs=1.0)

    def test_clamped_0_100(self):
        """Extreme slopes clamp to [0, 100]."""
        score = _trend_score([100, 80, 60, 40, 20, 1], "higher")
        assert 0 <= score <= 100

    def test_uses_last_6_points(self):
        """Only last 6 data points are used."""
        # Prepend irrelevant data
        data = [1, 2, 3, 50, 50, 50, 50, 50, 50]
        score = _trend_score(data, "higher")
        assert score == pytest.approx(50.0, abs=1.0)


# ── Impact Score Tests ───────────────────────────────────────────────────────

class TestImpactScore:
    def test_known_high_impact(self):
        """dso has many downstream nodes (cash_conv_cycle, ar_turnover, etc.) → high impact."""
        score = _impact_score("dso")
        assert score >= 25.0  # Should be meaningfully above default 30
        # churn_rate also has downstream nodes
        churn_score = _impact_score("churn_rate")
        assert churn_score > _impact_score("ebitda_margin")  # churn > terminal

    def test_terminal_node_low(self):
        """ebitda_margin has no downstream → low impact."""
        score = _impact_score("ebitda_margin")
        assert score <= 20.0

    def test_unknown_kpi(self):
        """Unknown KPI → default 30."""
        assert _impact_score("nonexistent_kpi_xyz") == 30.0


# ── Domain Score Tests ───────────────────────────────────────────────────────

class TestDomainScore:
    def test_cashflow_highest(self):
        """Cashflow KPIs have highest urgency."""
        score = _domain_score("dso")
        assert score == 100.0

    def test_efficiency_lowest(self):
        """Efficiency KPIs have lowest urgency."""
        score = _domain_score("opex_ratio")
        assert score == 45.0

    def test_unknown_neutral(self):
        """Unknown domain → neutral 50."""
        score = _domain_score("some_unknown_kpi")
        assert score == 50.0

    def test_get_kpi_domain(self):
        assert get_kpi_domain("revenue_growth") == "growth"
        assert get_kpi_domain("dso") == "cashflow"
        assert get_kpi_domain("unknown_xyz") == "other"


# ── Composite Scoring Tests ──────────────────────────────────────────────────

class TestComposite:
    def _make_data(self):
        """Create test data with 3 KPIs in different states."""
        kpi_avgs = {
            "revenue_growth": 5.0,    # target 20, higher → big gap
            "dso": 60.0,              # target 30, lower → big gap (worse)
            "gross_margin": 72.0,     # target 70, higher → on target
        }
        targets = {
            "revenue_growth": 20.0,
            "dso": 30.0,
            "gross_margin": 70.0,
        }
        directions = {
            "revenue_growth": "higher",
            "dso": "lower",
            "gross_margin": "higher",
        }
        time_series = {
            "revenue_growth": [15, 12, 10, 8, 6, 5],    # deteriorating
            "dso": [40, 45, 50, 55, 58, 60],             # deteriorating (rising for lower-is-better)
            "gross_margin": [68, 69, 70, 71, 72, 72],    # improving
        }
        return kpi_avgs, targets, directions, time_series

    def test_returns_all_scored_kpis(self):
        results = compute_composite_criticality(*self._make_data())
        assert len(results) == 3

    def test_sorted_by_composite_desc(self):
        results = compute_composite_criticality(*self._make_data())
        composites = [r["composite"] for r in results]
        assert composites == sorted(composites, reverse=True)

    def test_ranks_assigned(self):
        results = compute_composite_criticality(*self._make_data())
        ranks = [r["rank"] for r in results]
        assert ranks == [1, 2, 3]

    def test_critical_kpi_ranks_higher(self):
        """revenue_growth (big gap + deteriorating + growth domain) should rank above gross_margin (on target)."""
        results = compute_composite_criticality(*self._make_data())
        rg_rank = next(r["rank"] for r in results if r["key"] == "revenue_growth")
        gm_rank = next(r["rank"] for r in results if r["key"] == "gross_margin")
        assert rg_rank < gm_rank

    def test_dso_high_criticality(self):
        """DSO: big gap + deteriorating + cashflow domain (highest urgency) → top ranked."""
        results = compute_composite_criticality(*self._make_data())
        dso = next(r for r in results if r["key"] == "dso")
        assert dso["composite"] > 60  # Should be high

    def test_gross_margin_low_criticality(self):
        """gross_margin: on target + improving → low composite."""
        results = compute_composite_criticality(*self._make_data())
        gm = next(r for r in results if r["key"] == "gross_margin")
        assert gm["composite"] < 50

    def test_breakdown_included(self):
        results = compute_composite_criticality(*self._make_data())
        for r in results:
            assert "gap_score" in r
            assert "trend_score" in r
            assert "impact_score" in r
            assert "domain_score" in r
            assert "domain" in r
            assert "domain_label" in r
            assert "weights_used" in r

    def test_custom_weights(self):
        """Custom weights should change the ranking."""
        results_default = compute_composite_criticality(*self._make_data())
        # Heavily weight domain (cashflow DSO should dominate)
        results_domain = compute_composite_criticality(
            *self._make_data(),
            weights={"gap": 0.05, "trend": 0.05, "impact": 0.05, "domain": 0.85},
        )
        # DSO should be #1 with domain-heavy weights (cashflow = 100 urgency)
        assert results_domain[0]["key"] == "dso"

    def test_skips_no_target(self):
        """KPIs without targets are excluded."""
        avgs = {"revenue_growth": 10.0, "gross_margin": 70.0}
        targets = {"revenue_growth": 20.0}  # No target for gross_margin
        dirs = {"revenue_growth": "higher", "gross_margin": "higher"}
        ts = {"revenue_growth": [10, 10, 10], "gross_margin": [70, 70, 70]}
        results = compute_composite_criticality(avgs, targets, dirs, ts)
        assert len(results) == 1
        assert results[0]["key"] == "revenue_growth"

    def test_skips_no_data(self):
        """KPIs without data are excluded."""
        avgs = {}
        targets = {"revenue_growth": 20.0}
        dirs = {"revenue_growth": "higher"}
        ts = {}
        results = compute_composite_criticality(avgs, targets, dirs, ts)
        assert len(results) == 0


# ── Domain Grouping Tests ────────────────────────────────────────────────────

class TestDomainGrouping:
    def test_groups_kpis_by_domain(self):
        scored = [
            {"key": "dso",            "composite": 80, "domain": "cashflow", "domain_label": "Cash & Liquidity"},
            {"key": "revenue_growth", "composite": 70, "domain": "growth",   "domain_label": "Growth & Acquisition"},
            {"key": "cash_conv_cycle","composite": 60, "domain": "cashflow", "domain_label": "Cash & Liquidity"},
        ]
        groups = group_by_domain(scored)
        assert len(groups) == 2
        cashflow_group = next(g for g in groups if g["domain"] == "cashflow")
        assert cashflow_group["count"] == 2

    def test_groups_sorted_by_worst(self):
        scored = [
            {"key": "dso",            "composite": 80, "domain": "cashflow", "domain_label": "Cash & Liquidity"},
            {"key": "revenue_growth", "composite": 90, "domain": "growth",   "domain_label": "Growth & Acquisition"},
        ]
        groups = group_by_domain(scored)
        # Growth has composite 90, cashflow has 80 → growth first
        assert groups[0]["domain"] == "growth"

    def test_empty_input(self):
        assert group_by_domain([]) == []


# ── Constants Tests ──────────────────────────────────────────────────────────

class TestConstants:
    def test_domain_order_complete(self):
        """All urgency domains appear in DOMAIN_ORDER."""
        for domain in DOMAIN_URGENCY:
            assert domain in DOMAIN_ORDER

    def test_domain_labels_complete(self):
        """All domains in order have labels."""
        for domain in DOMAIN_ORDER:
            assert domain in DOMAIN_LABELS

    def test_default_weights_sum_to_one(self):
        assert sum(DEFAULT_WEIGHTS.values()) == pytest.approx(1.0)
