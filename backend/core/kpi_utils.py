"""
core/kpi_utils.py — Single source of truth for KPI average computation.

Every code path that computes a KPI average MUST use compute_kpi_avg()
to ensure consistent windowing, None handling, and rounding across:
  - health_score.py (health score computation)
  - health.py (home spotlight, kpi-detail, narrative engine avgs)
  - narrative_engine.py (trend comparison)
  - analytics.py (fingerprint, weekly briefing)
  - board_pack.py (board deck export)
"""
import math
from typing import Optional


def compute_kpi_avg(
    values: list,
    window: int = 6,
    period_filtered: bool = False,
    round_digits: int = 2,
) -> Optional[float]:
    """Compute a KPI average from a list of monthly values.

    Parameters
    ----------
    values : list
        Raw monthly values (may contain None, NaN, non-numeric entries).
    window : int
        Number of recent months to use when period_filtered is False.
        Default 6 matches the platform-wide "last 6 months" convention.
    period_filtered : bool
        True = the caller already scoped the data to a user-selected
        period, so use ALL valid values.  False = take the last `window`
        values (standard unfiltered view).
    round_digits : int
        Decimal places for rounding.  Default 2 ensures consistency
        across all display paths.

    Returns
    -------
    float | None
        The rounded average, or None if no valid values exist.
    """
    valid = [
        v for v in values
        if v is not None
        and isinstance(v, (int, float))
        and math.isfinite(v)
    ]
    if not valid:
        return None
    if not period_filtered and len(valid) > window:
        valid = valid[-window:]
    return round(sum(valid) / len(valid), round_digits)
