from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from mfp.config.normalize import normalize_config
from mfp.config.runtime import load_config
from mfp.paper.equity_ledger import append_equity, compute_drawdown_gate, now_utc_iso


def _wjson(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _wtext(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8", newline="\n")


def _workspace_from_out_dir(out_dir: Path) -> Path:
    # expected: <workspace>/reports/<run_id>
    try:
        return out_dir.parent.parent
    except Exception:
        return out_dir.parent


def paper_status(out_dir: Path) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    from mfp.paper.alpaca_io import (
        alpaca_trading_client,
        get_account_snapshot,
        get_open_orders_snapshot,
        get_positions_snapshot,
    )

    tc = alpaca_trading_client()
    acct = get_account_snapshot(tc)
    pos = get_positions_snapshot(tc)
    orders = get_open_orders_snapshot(tc)

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", pos)
    _wjson(out_dir / "paper_open_orders.json", orders)

    return {"account": acct, "positions": pos, "open_orders": orders}


def _compute_features(
    df: pd.DataFrame, trend_sma: int, fast_sma: int, rsi_len: int, atr_len: int
) -> pd.DataFrame:
    from mfp.indicators import atr, rsi, sma

    d = df.copy()
    d["sma_trend"] = sma(d["Close"], trend_sma)
    d["sma_fast"] = sma(d["Close"], fast_sma)
    d["rsi"] = rsi(d["Close"], rsi_len)
    d["atr"] = atr(d["High"], d["Low"], d["Close"], atr_len)
    d["atr_pct"] = d["atr"] / d["Close"]
    d["dollar_vol"] = d["Close"] * d["Volume"]
    d["adv20"] = d["dollar_vol"].rolling(20).mean()
    return d


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _safe_int(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def paper_cycle(
    out_dir: Path,
    symbols: List[str],
    dry_run: bool = True,
    # NEW
    strategy_id: str = "DEFAULT",
    engine: str = "pulse_mr",
    proxy_ticker: str = "",
    equity_override: Optional[float] = None,
    allow_new_entries: bool = True,
    # Optional per-strategy overrides
    risk_per_trade_pct: Optional[float] = None,
    max_open_risk_pct: Optional[float] = None,
    max_positions: Optional[int] = None,
    stop_atr_mult: Optional[float] = None,
    rsi_buy_below: Optional[float] = None,
    rsi_exit_above: Optional[float] = None,
    trend_sma: Optional[int] = None,
    fast_sma: Optional[int] = None,
    min_price: Optional[float] = None,
    min_adv20: Optional[float] = None,
    max_atr_pct: Optional[float] = None,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    workspace = _workspace_from_out_dir(out_dir)

    cfg = normalize_config(load_config(workspace))
    _wjson(out_dir / "config_used.json", cfg)
    _wjson(out_dir / "strategy.json", {"strategy_id": strategy_id, "engine": engine, "proxy": proxy_ticker})

    # Pull Alpaca state
    from mfp.paper.alpaca_io import (
        alpaca_data_client,
        alpaca_trading_client,
        fetch_daily_bars,
        get_account_snapshot,
        get_open_orders_snapshot,
        get_positions_snapshot,
        is_trading_enabled,
    )

    tc = alpaca_trading_client()
    dc = alpaca_data_client()

    acct = get_account_snapshot(tc)
    positions = get_positions_snapshot(tc)
    open_orders = get_open_orders_snapshot(tc)

    equity_actual = _safe_float(acct.get("equity"), 0.0)
    buying_power = _safe_float(acct.get("buying_power"), 0.0)

    # Append equity to ledger for drawdown gate visibility
    append_equity(workspace, ts_utc=now_utc_iso(), equity=float(equity_actual or 0.0))

    # Portfolio-level drawdown gate (for info inside per-strategy runs)
    dd_limits = cfg.get("objectives", {}).get("drawdown_limits", {"1D": 0.01, "5D": 0.02, "20D": 0.03})
    dd_gate = compute_drawdown_gate(workspace, dd_limits=dd_limits)

    # Strategy sizing equity:
    equity_for_sizing = float(equity_override) if equity_override is not None else equity_actual
    equity_for_sizing = max(1.0, equity_for_sizing)

    # Defaults
    base_risk = cfg.get("risk", {})
    base_sig = cfg.get("signal", {})
    base_filt = cfg.get("filters", {})

    trend_sma = int(trend_sma if trend_sma is not None else base_sig.get("trend_sma", 200))
    fast_sma = int(fast_sma if fast_sma is not None else base_sig.get("fast_sma", 5))
    rsi_len = int(base_sig.get("rsi_len", 2))
    atr_len = int(base_sig.get("atr_len", 14))

    rsi_buy_below = float(rsi_buy_below if rsi_buy_below is not None else base_sig.get("rsi_buy_below", 10.0))
    rsi_exit_above = float(
        rsi_exit_above if rsi_exit_above is not None else base_sig.get("rsi_exit_above", 70.0)
    )

    risk_per_trade_pct = float(
        risk_per_trade_pct if risk_per_trade_pct is not None else base_risk.get("risk_per_trade_pct", 0.25)
    )
    max_open_risk_pct = float(
        max_open_risk_pct if max_open_risk_pct is not None else base_risk.get("max_open_risk_pct", 1.00)
    )
    max_positions = int(max_positions if max_positions is not None else base_risk.get("max_positions", 6))
    stop_atr_mult = float(stop_atr_mult if stop_atr_mult is not None else base_risk.get("stop_atr_mult", 1.5))

    min_price = float(min_price if min_price is not None else base_filt.get("min_price", 10.0))
    min_adv20 = float(min_adv20 if min_adv20 is not None else base_filt.get("min_adv20", 20_000_000.0))
    max_atr_pct = float(max_atr_pct if max_atr_pct is not None else base_filt.get("max_atr_pct", 0.08))

    # Regime gate (fast): proxy < SMA(trend) => no new entries for MR engines.
    regime = {"ok": True, "reason": "ok", "proxy": proxy_ticker, "trend_sma": trend_sma}
    if proxy_ticker:
        bars_px = fetch_daily_bars(dc, [proxy_ticker], lookback_days=trend_sma + 30, feed="iex").get(
            proxy_ticker
        )
        if bars_px is not None and not bars_px.empty:
            fx = _compute_features(bars_px, trend_sma, fast_sma, rsi_len, atr_len)
            last = fx.iloc[-1]
            c = _safe_float(last.get("Close"))
            st = _safe_float(last.get("sma_trend"))
            if st > 0 and c > 0 and c <= st:
                regime = {
                    "ok": False,
                    "reason": "proxy_below_trend",
                    "proxy": proxy_ticker,
                    "close": c,
                    "sma": st,
                }
        else:
            regime = {"ok": True, "reason": "proxy_data_missing", "proxy": proxy_ticker}

    _wjson(out_dir / "regime.json", regime)
    _wjson(
        out_dir / "paper_gate.json",
        {"drawdown_gate": dd_gate, "allow_new_entries": allow_new_entries, "regime": regime},
    )

    # Open positions map
    pos_qty: Dict[str, int] = {}
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        qty = _safe_int(p.get("qty"), 0)
        if qty != 0:
            pos_qty[str(sym).upper()] = qty

    open_order_syms = {str(o.get("symbol")).upper() for o in open_orders if o.get("symbol")}

    # Fetch bars for symbols (+ proxy if not already)
    fetch_syms = [s.upper() for s in symbols]
    bars = fetch_daily_bars(dc, fetch_syms, lookback_days=max(trend_sma + 50, 250), feed="iex")

    feats: Dict[str, pd.DataFrame] = {}
    for sym, df in bars.items():
        if df is None or df.empty:
            continue
        feats[sym] = _compute_features(df, trend_sma, fast_sma, rsi_len, atr_len)

    # Strategy outputs
    exits: List[Dict[str, Any]] = []
    entries: List[Dict[str, Any]] = []

    # Engine: trend_hold (simple ETF trend defense)
    if engine.strip().lower() == "trend_hold":
        for sym in fetch_syms:
            df = feats.get(sym)
            if df is None or len(df) < trend_sma + 5:
                continue
            last = df.iloc[-1]
            close = _safe_float(last.get("Close"))
            st = _safe_float(last.get("sma_trend"))
            if close <= 0 or st <= 0:
                continue

            have = sym in pos_qty
            want_long = close > st

            if have and (not want_long):
                exits.append(
                    {
                        "symbol": sym,
                        "qty": abs(pos_qty[sym]),
                        "reason": "trend_off",
                        "close": close,
                        "sma200": st,
                    }
                )
            if (not have) and want_long and allow_new_entries:
                entries.append({"symbol": sym, "qty": 0, "reason": "trend_on", "close": close, "sma200": st})

        # Size trend_hold entries as “use allocation equity”, single position
        if entries:
            sym = entries[0]["symbol"]
            close = float(entries[0]["close"])
            # Use up to 95% of buying power (if real account smaller, it will naturally limit)
            budget = min(buying_power * 0.95, equity_for_sizing * 0.95)
            qty = int(budget // close) if close > 0 else 0
            if qty > 0:
                entries[0]["qty"] = qty
            else:
                entries = []

    # Engine: pulse_mr (dip-buying in uptrend)
    else:
        # If regime is OFF for proxy, don't open new MR entries.
        mr_entries_allowed = allow_new_entries and bool(regime.get("ok", True))

        # Exits first
        for sym, qty in pos_qty.items():
            df = feats.get(sym)
            if df is None or len(df) < trend_sma + 5:
                continue
            last = df.iloc[-1]
            close = _safe_float(last.get("Close"))
            sma_t = _safe_float(last.get("sma_trend"))
            sma_f = _safe_float(last.get("sma_fast"))
            rsi_v = _safe_float(last.get("rsi"))

            if sma_t <= 0 or sma_f <= 0 or close <= 0:
                continue

            trend_break = close < sma_t
            bounce_exit = (close > sma_f) or (rsi_v >= float(rsi_exit_above))

            if trend_break or bounce_exit:
                exits.append(
                    {
                        "symbol": sym,
                        "qty": abs(int(qty)),
                        "reason": "trend_break" if trend_break else "bounce_exit",
                        "close": close,
                        "sma200": sma_t,
                        "sma5": sma_f,
                        "rsi2": rsi_v,
                    }
                )

        # Entries ranked by RSI (lower = more oversold)
        candidates: List[Dict[str, Any]] = []
        if mr_entries_allowed:
            for sym, df in feats.items():
                if sym in pos_qty:
                    continue
                if sym in open_order_syms:
                    continue
                if len(df) < trend_sma + 5:
                    continue

                last = df.iloc[-1]
                close = _safe_float(last.get("Close"))
                sma_t = _safe_float(last.get("sma_trend"))
                sma_f = _safe_float(last.get("sma_fast"))
                rsi_v = _safe_float(last.get("rsi"))
                atr_v = _safe_float(last.get("atr"))
                adv20 = _safe_float(last.get("adv20"))
                atr_pct = _safe_float(last.get("atr_pct"))

                if close <= 0 or sma_t <= 0 or sma_f <= 0 or atr_v <= 0:
                    continue

                # filters
                if close < min_price:
                    continue
                if adv20 < min_adv20:
                    continue
                if atr_pct > max_atr_pct:
                    continue

                trend_ok = close > sma_t
                pullback = close < sma_f
                oversold = rsi_v < float(rsi_buy_below)
                if trend_ok and pullback and oversold:
                    candidates.append(
                        {
                            "symbol": sym,
                            "score": float(rsi_v),
                            "close": close,
                            "sma200": sma_t,
                            "sma5": sma_f,
                            "rsi2": rsi_v,
                            "atr14": atr_v,
                            "adv20": adv20,
                            "atr_pct": atr_pct,
                        }
                    )
        candidates.sort(key=lambda x: x["score"])

        # sizing budgets
        risk_per_trade_usd = equity_for_sizing * (risk_per_trade_pct / 100.0)
        max_open_risk_usd = equity_for_sizing * (max_open_risk_pct / 100.0)

        # estimate open risk (rough)
        est_open_risk = 0.0
        for sym, qty in pos_qty.items():
            df = feats.get(sym)
            if df is None or df.empty:
                continue
            last = df.iloc[-1]
            atr_v = _safe_float(last.get("atr"))
            if atr_v > 0:
                est_open_risk += abs(int(qty)) * (stop_atr_mult * atr_v)

        slots = max(0, max_positions - len(pos_qty))
        for c in candidates[: max(0, slots * 3)]:
            if slots <= 0:
                break
            close = float(c["close"])
            atr_v = float(c["atr14"])
            stop_est = close - stop_atr_mult * atr_v
            per_share_risk = max(0.01, close - stop_est)
            qty = int(risk_per_trade_usd // per_share_risk)

            if qty <= 0:
                continue

            # buying power
            est_cost = qty * close
            if est_cost > buying_power * 0.95:
                qty = int((buying_power * 0.95) // close)
            if qty <= 0:
                continue

            add_risk = qty * per_share_risk
            if est_open_risk + add_risk > max_open_risk_usd + 1e-9:
                continue

            entries.append(
                {
                    **c,
                    "qty": qty,
                    "stop_estimate": stop_est,
                    "per_share_risk": per_share_risk,
                    "risk_usd": add_risk,
                    "reason": "uptrend_dip_oversold",
                }
            )
            est_open_risk += add_risk
            slots -= 1

    # Build order intents
    orders_to_submit: List[Dict[str, Any]] = []

    for e in exits:
        orders_to_submit.append(
            {
                "strategy_id": strategy_id,
                "side": "sell",
                "symbol": e["symbol"],
                "qty": int(e["qty"]),
                "tif": "opg",
                "type": "market",
                "reason": e.get("reason", "exit"),
                "close": e.get("close"),
                "sma200": e.get("sma200"),
                "sma5": e.get("sma5"),
                "rsi2": e.get("rsi2"),
            }
        )

    for b in entries:
        orders_to_submit.append(
            {
                "strategy_id": strategy_id,
                "side": "buy",
                "symbol": b["symbol"],
                "qty": int(b["qty"]),
                "tif": "opg",
                "type": "market",
                "reason": b.get("reason", "entry"),
                "close": b.get("close"),
                "sma200": b.get("sma200"),
                "sma5": b.get("sma5"),
                "rsi2": b.get("rsi2"),
                "atr14": b.get("atr14"),
                "adv20": b.get("adv20"),
                "atr_pct": b.get("atr_pct"),
                "stop_estimate": b.get("stop_estimate"),
                "risk_usd": b.get("risk_usd"),
            }
        )

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", positions)
    _wjson(out_dir / "paper_open_orders.json", open_orders)
    _wjson(out_dir / "signals.json", {"entries": entries, "exits": exits})
    _wjson(out_dir / "orderIntents.json", orders_to_submit)

    # Place orders (paper) if enabled and not dry_run
    placed: List[Any] = []
    skipped: List[Any] = []

    can_place = bool(is_trading_enabled()) and (not dry_run)

    if can_place:
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest

        for o in orders_to_submit:
            sym = str(o["symbol"]).upper()
            if sym in open_order_syms:
                skipped.append({**o, "skip_reason": "existing_open_order"})
                continue
            req = MarketOrderRequest(
                symbol=sym,
                qty=int(o["qty"]),
                side=OrderSide.SELL if o["side"] == "sell" else OrderSide.BUY,
                time_in_force=TimeInForce.OPG,
                client_order_id=f"mfp-{strategy_id}-{sym}-{o['side']}",
            )
            try:
                resp = tc.submit_order(order_data=req)
                try:
                    placed.append(resp.model_dump())
                except Exception:
                    placed.append(resp.dict())
            except Exception as e:
                skipped.append({**o, "skip_reason": f"submit_failed:{type(e).__name__}"})
    else:
        skipped.append({"note": "TRADING NOT ENABLED or DRY RUN - no orders submitted"})

    ack = {"placed": placed, "skipped": skipped}
    _wjson(out_dir / "orders.json", ack)

    md = []
    md.append("# Strategy Run\n")
    md.append(f"- strategy_id: `{strategy_id}`")
    md.append(f"- engine: `{engine}`")
    md.append(f"- dry_run: `{dry_run}`")
    md.append(f"- will_place_orders: `{can_place}`\n")
    md.append("## Portfolio gate\n")
    md.append(f"- allow_new_entries: `{allow_new_entries}`")
    md.append(f"- drawdown_gate_ok: `{dd_gate.get('ok')}`")
    md.append(f"- regime_ok: `{regime.get('ok')}`\n")
    md.append("## Orders\n")
    md.append(f"- count: `{len(orders_to_submit)}`")
    _wtext(out_dir / "paper_report.md", "\n".join(md) + "\n")

    return {
        "dry_run": dry_run,
        "will_place": can_place,
        "orders_to_submit_count": len(orders_to_submit),
        "placed_count": len(placed),
        "strategy_id": strategy_id,
        "engine": engine,
        "allow_new_entries": allow_new_entries,
    }


def paper_reconcile(
    out_dir: Path, symbols: List[str], place_stops: bool = False, stop_atr_mult: float = 1.5
) -> Dict[str, Any]:
    """
    Morning reconcile (minimal): snapshot account/positions/orders.
    Stop placement is handled elsewhere in your codebase if enabled.
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    from mfp.paper.alpaca_io import alpaca_trading_client, get_account_snapshot, get_positions_snapshot

    tc = alpaca_trading_client()
    acct = get_account_snapshot(tc)
    pos = get_positions_snapshot(tc)

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", pos)
    _wjson(
        out_dir / "paper_stop_actions.json",
        {"note": "stop placement not implemented in v3 reconcile", "place_stops": place_stops},
    )

    return {"stop_actions": {"placed": [], "skipped": []}}
