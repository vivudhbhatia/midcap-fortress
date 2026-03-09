from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional


def _load_json(p: Path) -> Optional[Any]:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def load_run_context(run_dir: Path, workspace: Path) -> Dict[str, Any]:
    """
    Load the most relevant artifacts needed to explain a trade.
    Falls back safely if some files do not exist.
    """
    cfg = _load_json(run_dir / "config_used.json")
    if cfg is None:
        cfg = _load_json(workspace / "state" / "config.json")

    qa = _load_json(run_dir / "qa.json") or {}
    verify = _load_json(run_dir / "verify.json") or {}
    intents = _load_json(run_dir / "orderIntents.json")
    if intents is None:
        intents = _load_json(run_dir / "paper_orders_to_submit.json")
    signals = _load_json(run_dir / "signals.json") or {}
    gate = _load_json(run_dir / "paper_gate.json") or {}

    return {
        "config": cfg or {},
        "qa": qa,
        "verify": verify,
        "order_intents": intents or [],
        "signals": signals,
        "paper_gate": gate,
    }


def _get(d: Dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def explain_order(order: Dict[str, Any], ctx: Dict[str, Any]) -> Dict[str, Any]:
    """
    Returns a plain-English explanation + structured sections
    linking order → signal → filters → sizing → safety checks.
    """
    cfg = ctx.get("config", {}) or {}
    qa = ctx.get("qa", {}) or {}
    gate = ctx.get("paper_gate", {}) or {}

    sig_cfg = cfg.get("signal", {}) if isinstance(cfg, dict) else {}
    filt_cfg = cfg.get("filters", {}) if isinstance(cfg, dict) else {}
    risk_cfg = cfg.get("risk", {}) if isinstance(cfg, dict) else {}

    sym = order.get("symbol")
    side = order.get("side")
    qty = order.get("qty")
    reason = order.get("reason", "")

    close = order.get("close")
    sma200 = order.get("sma200")
    sma5 = order.get("sma5")
    rsi2 = order.get("rsi2")
    adv20 = order.get("adv20")
    atr_pct = order.get("atr_pct")
    stop_est = order.get("stop_estimate")
    risk_usd = order.get("risk_usd")
    per_share_risk = order.get("per_share_risk")

    buy_rsi_thr = float(sig_cfg.get("rsi_buy_below", 10.0))
    exit_rsi_thr = float(sig_cfg.get("rsi_exit_above", 70.0))
    min_price = float(filt_cfg.get("min_price", 10.0))
    min_adv = float(filt_cfg.get("min_adv20", 20_000_000.0))
    max_atr_pct = float(filt_cfg.get("max_atr_pct", 0.08))

    signal_checks = {}
    filter_checks = {}

    if close is not None and sma200 is not None:
        signal_checks["Uptrend (price above long-term average)"] = bool(close > sma200)
    if close is not None and sma5 is not None:
        signal_checks["Short-term dip (price below 5‑day average)"] = bool(close < sma5)
    if rsi2 is not None:
        if side == "buy":
            signal_checks[f"Oversold (RSI below {buy_rsi_thr:g})"] = bool(rsi2 < buy_rsi_thr)
        else:
            signal_checks[f"Overbought bounce (RSI above {exit_rsi_thr:g})"] = bool(rsi2 >= exit_rsi_thr)

    if close is not None:
        filter_checks[f"Minimum price (≥ {min_price:g})"] = bool(close >= min_price)
    if adv20 is not None:
        filter_checks[f"Liquidity (ADV20 ≥ {min_adv:,.0f})"] = bool(adv20 >= min_adv)
    if atr_pct is not None:
        filter_checks[f"Volatility (ATR% ≤ {max_atr_pct:g})"] = bool(atr_pct <= max_atr_pct)

    safety = {
        "Safety Check required": bool(_get(cfg, "execution.require_pretrade_check", True)),
        "Safety Check status": _get(qa, "pretrade_validation.ok", None),
        "Safety Check reason": _get(qa, "pretrade_validation.reason", None),
        "Drawdown gate ok": gate.get("ok", None),
        "Drawdown gate reason": gate.get("reason", None),
    }

    sizing = {
        "Quantity": qty,
        "Estimated stop (if provided)": stop_est,
        "Estimated risk in USD (if provided)": risk_usd,
        "Estimated risk per share (if provided)": per_share_risk,
        "Risk per trade setting (%)": risk_cfg.get("risk_per_trade_pct"),
        "Max open risk setting (%)": risk_cfg.get("max_open_risk_pct"),
        "Stop distance setting (ATR multiple)": risk_cfg.get("stop_atr_mult"),
    }

    values = {
        "symbol": sym,
        "side": side,
        "reason": reason,
        "close": close,
        "sma200": sma200,
        "sma5": sma5,
        "rsi2": rsi2,
        "adv20": adv20,
        "atr_pct": atr_pct,
    }

    if side == "buy":
        summary = (
            f"Suggested BUY for **{sym}** (qty {qty}). "
            f"It looks like an uptrend stock that dipped short‑term and became oversold."
        )
    else:
        summary = (
            f"Suggested SELL for **{sym}** (qty {qty}). "
            f"The system thinks the rebound happened (or trend broke) and wants to exit."
        )

    return {
        "summary": summary,
        "order": {"symbol": sym, "side": side, "qty": qty, "reason": reason},
        "signal_checks": signal_checks,
        "filter_checks": filter_checks,
        "values": values,
        "sizing": sizing,
        "safety": safety,
        "raw_order": order,
    }
