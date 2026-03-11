from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

Config = Dict[str, Any]


DEFAULTS: Config = {
    "universe": {
        "name": "sp400",
        "max_symbols": 60,
    },
    "objectives": {
        # Long-run goal (annualized)
        "target_annual_cagr": 0.20,
        "goal_horizon_years": 4,
        # Softening factor for evaluation thresholds
        "acceptance_factor": 0.75,
        # Which windows to show/evaluate in the UI
        "return_windows": ["1M", "3M", "6M", "1Y", "3Y", "5Y"],
        # If True, portfolio Safety Check fails when returns are below threshold.
        # Default False keeps Safety Check focused on drawdown and integrity.
        "enforce_return_in_safety_check": False,
        # Rolling drawdown limits (interpreted from equity ledger)
        # "1D" means daily (not intraday); with gaps, you still can exceed this.
        "drawdown_limits": {"1D": 0.01, "5D": 0.02, "20D": 0.03},
        # Emergency option (default False):
        # if breached, the critic will recommend “go to cash”.
        "panic_close_on_breach": False,
    },
    "portfolio": {
        "paper_equity_usd": 100000.0,
        # If True, sizing uses the virtual portfolio (scaled down if Alpaca equity < virtual).
        "use_virtual_equity": True,
        # Require portfolio Safety Check + acknowledgement before placing paper orders
        "require_portfolio_safety_check": True,
        "strategies": {
            "MFP_STOCKS_MID": {
                "label": "Stocks: dip buys (Mid-cap)",
                "enabled": True,
                "allocation_pct": 0.35,
                "engine": "pulse_mr",
                "universe": "sp400",
                "symbols": [],
                "proxy": "IJH",
                "max_symbols": 60,
                "overrides": {
                    "risk_per_trade_pct": 0.25,
                    "max_open_risk_pct": 1.00,
                    "max_positions": 6,
                    "stop_atr_mult": 1.5,
                    "rsi_buy_below": 10.0,
                    "rsi_exit_above": 70.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "MFP_STOCKS_LARGE": {
                "label": "Stocks: dip buys (Large-cap)",
                "enabled": True,
                "allocation_pct": 0.35,
                "engine": "pulse_mr",
                "universe": "sp500",
                "symbols": [],
                "proxy": "SPY",
                "max_symbols": 60,
                "overrides": {
                    "risk_per_trade_pct": 0.25,
                    "max_open_risk_pct": 1.00,
                    "max_positions": 6,
                    "stop_atr_mult": 1.5,
                    "rsi_buy_below": 10.0,
                    "rsi_exit_above": 70.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "ETF_MR": {
                "label": "ETF: dip buys (lower gap risk)",
                "enabled": True,
                "allocation_pct": 0.20,
                "engine": "pulse_mr",
                "universe": "",
                "symbols": ["IJH"],
                "proxy": "IJH",
                "max_symbols": 10,
                "overrides": {
                    "risk_per_trade_pct": 0.35,
                    "max_open_risk_pct": 1.50,
                    "max_positions": 1,
                    "stop_atr_mult": 1.25,
                    "rsi_buy_below": 12.0,
                    "rsi_exit_above": 75.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "ETF_TREND": {
                "label": "ETF: trend defense (simple)",
                "enabled": True,
                "allocation_pct": 0.10,
                "engine": "trend_hold",
                "universe": "",
                "symbols": ["SPY"],
                "proxy": "SPY",
                "max_symbols": 10,
                "overrides": {
                    "trend_sma": 200,
                },
            },
        },
        "switching": {
            "enabled": True,
            "sma_len": 200,
            "shock_2d_pct": 0.04,
            "vol_20d_high": 0.03,
            # Simple regime -> weights
            "weights": {
                "RISK_ON": {
                    "MFP_STOCKS_MID": 0.35,
                    "MFP_STOCKS_LARGE": 0.35,
                    "ETF_MR": 0.20,
                    "ETF_TREND": 0.10,
                },
                "RISK_OFF": {"ETF_TREND": 0.30, "CASH": 0.70},
            },
        },
    },
}


def _deep_merge(dst: Config, src: Config) -> Config:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)  # type: ignore[index]
        else:
            dst[k] = v
    return dst


def normalize_config(cfg: Config | None) -> Config:
    base = deepcopy(DEFAULTS)
    if isinstance(cfg, dict):
        _deep_merge(base, cfg)
    return base
