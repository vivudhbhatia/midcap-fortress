from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

DEFAULT_CONFIG: Dict[str, Any] = {
    "version": 1,
    "universe": {
        "name": "sp400",
        "max_symbols": 60,
    },
    "proxy": {
        # S&P 400 proxy: IJH or MDY. Default IJH (more liquid typically).
        "ticker": "IJH",
        "monthly_sma_months": 10,
        "daily_sma": 200,
        "shock_2d_drop_pct": 0.04,  # 4% drop over 2 days triggers pause
        "shock_pause_days": 3,
    },
    "signal": {
        "trend_sma": 200,
        "fast_sma": 5,
        "rsi_len": 2,
        "rsi_buy_below": 10.0,
        "rsi_exit_above": 70.0,
        "atr_len": 14,
        "time_stop_bars": 7,
    },
    "filters": {
        "min_price": 10.0,
        "min_adv20": 20_000_000.0,
        "max_atr_pct": 0.08,
        # Earnings filter: only applies to paper/live (upcoming earnings).
        "earnings_filter_enabled": False,
        "earnings_days_ahead_block": 10,
    },
    "risk": {
        "max_positions": 6,
        "risk_per_trade_pct": 0.25,
        "max_open_risk_pct": 1.00,
        "stop_atr_mult": 1.5,
        "profit_protector_enabled": True,
        "profit_trigger_atr": 0.75,
        "trail_stop_atr": 1.00,
        # Diversification / correlation cap
        "corr_lookback": 60,
        "corr_threshold": 0.80,
        # Rolling DD governor
        "dd_governor": {
            "enabled": True,
            "window_days": 20,
            "warn_dd": 0.02,
            "max_dd": 0.03,
        },
        # Scaling mode: "none" or "confirm_add"
        "scale_in_mode": "none",
    },
    "execution": {
        "require_pretrade_check": True,
        "pretrade_check_max_age_days": 7,
    },
    "pretrade_check": {
        "start": "2011-01-01",
        "end": "",
        "min_cagr": 0.15,
        "require_pass_3pct_window": True,
        "max_symbols": 60,
    },
    "automation": {
        "enabled": True,
        "timezone": "America/New_York",
        "market_open_time": "09:40",  # reconcile/stops
        "midday_time": "12:00",
        "eod_time": "16:10",
        "eod_run_paper_cycle": True,
        "eod_place_orders": False,  # keep False until you trust it
        "place_stops_on_reconcile": True,
    },
}


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _config_path(workspace: Path) -> Path:
    return _state_dir(workspace) / "config.json"


def config_hash(cfg: Dict[str, Any]) -> str:
    payload = json.dumps(cfg, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def load_config(workspace: Path) -> Dict[str, Any]:
    p = _config_path(workspace)
    if not p.exists():
        save_config(workspace, DEFAULT_CONFIG)
        return dict(DEFAULT_CONFIG)
    return json.loads(p.read_text(encoding="utf-8"))


def save_config(workspace: Path, cfg: Dict[str, Any]) -> None:
    p = _config_path(workspace)
    p.write_text(json.dumps(cfg, indent=2, default=str), encoding="utf-8", newline="\n")


def snapshot_config(workspace: Path, cfg: Dict[str, Any]) -> Path:
    hist = _state_dir(workspace) / "config_history"
    hist.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    p = hist / f"config_{ts}.json"
    p.write_text(json.dumps(cfg, indent=2, default=str), encoding="utf-8", newline="\n")
    return p
