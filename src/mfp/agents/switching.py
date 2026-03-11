from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from mfp.config.normalize import normalize_config
from mfp.portfolio.registry import load_strategy_specs


@dataclass(frozen=True)
class Regime:
    key: str
    details: Dict[str, Any]


def _wjson(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _safe_close_series(df: pd.DataFrame) -> pd.Series:
    if "Close" in df.columns:
        return df["Close"].astype("float64").dropna()
    # fallback
    for c in ("close", "c", "adjclose", "Adj Close"):
        if c in df.columns:
            return df[c].astype("float64").dropna()
    return pd.Series([], dtype="float64")


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n, min_periods=n).mean()


def _realized_vol_20d(s: pd.Series) -> float:
    if len(s) < 25:
        return 0.0
    rets = s.pct_change().dropna()
    v = float(rets.tail(20).std())
    return v


def _fetch_proxy_bars(proxy: str, lookback_days: int = 260) -> Optional[pd.DataFrame]:
    """
    Uses Alpaca data client (paper-friendly). If unavailable, returns None.
    """
    try:
        from mfp.paper.alpaca_io import alpaca_data_client, fetch_daily_bars

        dc = alpaca_data_client()
        bars = fetch_daily_bars(dc, [proxy], lookback_days=lookback_days, feed="iex")
        df = bars.get(proxy)
        if df is None or df.empty:
            return None
        return df
    except Exception:
        return None


def compute_regime(cfg_raw: Dict[str, Any]) -> Regime:
    cfg = normalize_config(cfg_raw)
    sw = cfg.get("portfolio", {}).get("switching", {})
    sma_len = int(sw.get("sma_len", 200))
    shock_2d = float(sw.get("shock_2d_pct", 0.04))
    vol_high = float(sw.get("vol_20d_high", 0.03))

    specs = [s for s in load_strategy_specs(cfg) if s.enabled]
    proxies = sorted({s.proxy for s in specs if s.proxy} | {"SPY"})

    details: Dict[str, Any] = {"proxies": {}}
    risk_on_all = True
    high_vol_any = False
    shock_any = False

    for px in proxies:
        df = _fetch_proxy_bars(px)
        if df is None:
            details["proxies"][px] = {"ok": False, "reason": "no_data"}
            continue

        close = _safe_close_series(df)
        if len(close) < sma_len + 5:
            details["proxies"][px] = {"ok": False, "reason": "not_enough_bars"}
            continue

        sma = _sma(close, sma_len)
        last_c = float(close.iloc[-1])
        last_s = float(sma.iloc[-1])

        # 2-day shock
        shock = False
        if len(close) >= 3:
            r2 = float((close.iloc[-1] / close.iloc[-3]) - 1.0)
            shock = r2 <= -shock_2d

        v20 = _realized_vol_20d(close)
        high_vol = v20 >= vol_high

        risk_on = last_c > last_s and (not shock)
        risk_on_all = risk_on_all and risk_on
        high_vol_any = high_vol_any or high_vol
        shock_any = shock_any or shock

        details["proxies"][px] = {
            "ok": True,
            "close": last_c,
            "sma": last_s,
            "risk_on": risk_on,
            "shock_2d": shock,
            "vol_20d": v20,
            "high_vol": high_vol,
        }

    if not risk_on_all:
        key = "RISK_OFF"
    else:
        key = "RISK_ON"

    details["summary"] = {"risk_on_all": risk_on_all, "shock_any": shock_any, "high_vol_any": high_vol_any}
    return Regime(key=key, details=details)


def suggest_allocations(cfg_raw: Dict[str, Any]) -> Dict[str, Any]:
    cfg = normalize_config(cfg_raw)
    sw = cfg.get("portfolio", {}).get("switching", {})
    weights = sw.get("weights", {})
    regime = compute_regime(cfg)

    row = weights.get(regime.key, {})
    # normalize only strategy keys; CASH allowed
    total = sum(float(v) for v in row.values()) if isinstance(row, dict) else 0.0
    if total <= 0:
        return {
            "ok": False,
            "reason": "no_weights_for_regime",
            "regime": regime.key,
            "details": regime.details,
        }

    # Current allocations
    specs = load_strategy_specs(cfg)
    cur = {s.id: float(s.allocation_pct) for s in specs}

    sugg = {k: float(v) / total for k, v in row.items()}  # includes CASH possibly
    changes = []
    for sid, cur_w in cur.items():
        new_w = float(sugg.get(sid, 0.0))
        if abs(new_w - cur_w) > 1e-6:
            changes.append({"strategy_id": sid, "from": cur_w, "to": new_w})

    return {
        "ok": True,
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "regime": regime.key,
        "regime_details": regime.details,
        "suggested_weights": sugg,
        "changes": changes,
        "note": "This is a suggestion. Use Governance to create/approve/apply.",
    }


def apply_suggested_allocations(cfg_raw: Dict[str, Any], suggested_weights: Dict[str, Any]) -> Dict[str, Any]:
    cfg = normalize_config(cfg_raw)
    p = cfg.get("portfolio", {})
    sdict = p.get("strategies", {})
    for sid, spec in sdict.items():
        if isinstance(spec, dict):
            spec["allocation_pct"] = float(suggested_weights.get(sid, 0.0))
    return cfg


def write_suggestion(out_dir: Path, suggestion: Dict[str, Any]) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / "switch_suggestion.json"
    _wjson(p, suggestion)
    return p
