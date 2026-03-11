from __future__ import annotations

from typing import Dict

TRADING_DAYS_PER_YEAR = 252

WINDOW_DAYS: Dict[str, int] = {
    "1D": 1,
    "5D": 5,
    "20D": 20,
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "1Y": 252,
    "3Y": 252 * 3,
    "5Y": 252 * 5,
}


def window_days(label: str) -> int:
    k = label.strip().upper()
    if k not in WINDOW_DAYS:
        raise ValueError(f"Unknown window label {label!r}. Use one of: {sorted(WINDOW_DAYS)}")
    return WINDOW_DAYS[k]


def expected_cum_return(target_annual_cagr: float, days: int) -> float:
    """
    Translate an annual growth target into the *expected cumulative return* over `days`.
    This is how we avoid nonsense like “20% CAGR per day”.
    """
    if days <= 0:
        return 0.0
    years = days / TRADING_DAYS_PER_YEAR
    return (1.0 + target_annual_cagr) ** years - 1.0


def annualize_from_cum(cum_return: float, days: int) -> float:
    if days <= 0:
        return 0.0
    years = days / TRADING_DAYS_PER_YEAR
    if years <= 0:
        return 0.0
    if cum_return <= -1.0:
        return -1.0
    return (1.0 + cum_return) ** (1.0 / years) - 1.0
