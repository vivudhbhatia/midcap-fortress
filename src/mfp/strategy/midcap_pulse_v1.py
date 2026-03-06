from __future__ import annotations
from dataclasses import dataclass

@dataclass(frozen=True)
class StrategyParams:
    trend_sma: int
    fast_sma: int
    rsi_len: int
    rsi_buy_below: float
    rsi_exit_above: float
    atr_len: int
    stop_atr_mult: float
    max_hold_bars: int

def params_for_timeframe(timeframe: str) -> StrategyParams:
    # Rough trading-day equivalents:
    if timeframe == "1d":
        return StrategyParams(
            trend_sma=200, fast_sma=5, rsi_len=2,
            rsi_buy_below=10, rsi_exit_above=70,
            atr_len=14, stop_atr_mult=1.5,
            max_hold_bars=7,
        )
    if timeframe == "1wk":
        return StrategyParams(
            trend_sma=40, fast_sma=2, rsi_len=2,
            rsi_buy_below=10, rsi_exit_above=70,
            atr_len=14, stop_atr_mult=1.5,
            max_hold_bars=4,
        )
    if timeframe == "1mo":
        return StrategyParams(
            trend_sma=10, fast_sma=2, rsi_len=2,
            rsi_buy_below=10, rsi_exit_above=70,
            atr_len=12, stop_atr_mult=1.5,
            max_hold_bars=2,
        )
    raise ValueError("timeframe must be 1d, 1wk, or 1mo")
