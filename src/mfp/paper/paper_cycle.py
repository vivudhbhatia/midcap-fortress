from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from mfp.audit.evidence import create_evidence_zip
from mfp.config.runtime import load_config


def _wjson(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _wtext(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8", newline="\n")


def _env_float(name: str, default: float) -> float:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name, "").strip()
    if not v:
        return default
    try:
        return int(v)
    except Exception:
        return default


def _workspace_from_out_dir(out_dir: Path) -> Path:
    # expected: <workspace>/reports/<run_id>
    try:
        return out_dir.parent.parent
    except Exception:
        return out_dir.parent


def paper_status(out_dir: Path) -> Dict[str, Any]:
    """
    Snapshot Alpaca paper account/positions/open-orders and write JSON files.
    """
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
    df: pd.DataFrame, sma_trend: int, sma_fast: int, rsi_len: int, atr_len: int
) -> pd.DataFrame:
    from mfp.indicators import atr, rsi, sma

    d = df.copy()
    d["sma_trend"] = sma(d["Close"], sma_trend)
    d["sma_fast"] = sma(d["Close"], sma_fast)
    d["rsi"] = rsi(d["Close"], rsi_len)
    d["atr"] = atr(d["High"], d["Low"], d["Close"], atr_len)
    d["atr_pct"] = d["atr"] / d["Close"]
    d["dollar_vol"] = d["Close"] * d["Volume"]
    d["adv20"] = d["dollar_vol"].rolling(20).mean()
    return d


def _rolling_drawdown_gate() -> Dict[str, Any]:
    """
    Safety: if account drawdown in last month exceeds 3%, block new entries.
    If portfolio history endpoint isn't available, don't block.
    """
    from mfp.paper.alpaca_io import fetch_portfolio_history_raw

    try:
        h = fetch_portfolio_history_raw(period="1M", timeframe="1D")
        eq = h.get("equity", [])
        if not eq or len(eq) < 5:
            return {"ok": True, "reason": "no_history"}

        s = pd.Series(eq, dtype="float64")
        peak = s.cummax()
        dd = (s / peak) - 1.0
        worst = float(dd.min())  # negative

        return {
            "ok": (abs(worst) < 0.03),
            "worst_dd_1m": float(abs(worst)),
            "reason": "ok" if abs(worst) < 0.03 else "blocked_dd",
        }
    except Exception as e:
        return {"ok": True, "reason": f"history_unavailable:{type(e).__name__}"}


def paper_cycle(
    out_dir: Path,
    symbols: List[str],
    dry_run: bool = True,
    min_price: Optional[float] = None,
    min_adv20: Optional[float] = None,
    max_atr_pct: Optional[float] = None,
    stop_atr_mult: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Nightly cycle:
      - compute signals using latest daily bar
      - create OPG market orders for next open (buys + sells)
      - if trading enabled and dry_run==False, submit orders
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    workspace = _workspace_from_out_dir(out_dir)
    cfg = load_config(workspace)
    _wjson(out_dir / "config_used.json", cfg)

    sig = cfg.get("signal", {})
    filt = cfg.get("filters", {})
    risk = cfg.get("risk", {})

    sma_trend = int(sig.get("trend_sma", 200))
    sma_fast = int(sig.get("fast_sma", 5))
    rsi_len = int(sig.get("rsi_len", 2))
    rsi_buy_below = float(sig.get("rsi_buy_below", 10.0))
    rsi_exit_above = float(sig.get("rsi_exit_above", 70.0))
    atr_len = int(sig.get("atr_len", 14))

    # filters: allow explicit overrides, otherwise read config
    min_price = float(min_price if min_price is not None else filt.get("min_price", 10.0))
    min_adv20 = float(min_adv20 if min_adv20 is not None else filt.get("min_adv20", 20_000_000.0))
    max_atr_pct = float(max_atr_pct if max_atr_pct is not None else filt.get("max_atr_pct", 0.08))

    # risk: allow env overrides for safety/ops
    max_positions = _env_int("MFP_MAX_POSITIONS", int(risk.get("max_positions", 6)))
    risk_per_trade_pct = _env_float("MFP_RISK_PER_TRADE_PCT", float(risk.get("risk_per_trade_pct", 0.25)))
    max_open_risk_pct = _env_float("MFP_MAX_OPEN_RISK_PCT", float(risk.get("max_open_risk_pct", 1.00)))
    stop_atr_mult = float(stop_atr_mult if stop_atr_mult is not None else risk.get("stop_atr_mult", 1.5))

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

    gate = _rolling_drawdown_gate()

    equity = float(acct.get("equity", 0.0) or 0.0)
    buying_power = float(acct.get("buying_power", 0.0) or 0.0)

    risk_per_trade_usd = equity * (risk_per_trade_pct / 100.0)
    max_open_risk_usd = equity * (max_open_risk_pct / 100.0)

    # Fetch bars + compute features
    bars = fetch_daily_bars(dc, symbols, lookback_days=450, feed="iex")
    feats: Dict[str, pd.DataFrame] = {
        s: _compute_features(df, sma_trend, sma_fast, rsi_len, atr_len) for s, df in bars.items()
    }

    # Current positions map
    pos_qty: Dict[str, int] = {}
    pos_entry: Dict[str, float] = {}
    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        try:
            qty = int(float(p.get("qty") or 0))
        except Exception:
            qty = 0
        if qty == 0:
            continue
        pos_qty[sym] = qty
        try:
            pos_entry[sym] = float(p.get("avg_entry_price") or 0.0)
        except Exception:
            pos_entry[sym] = 0.0

    open_order_syms = {o.get("symbol") for o in open_orders if o.get("symbol")}

    # Exits
    exits: List[Dict[str, Any]] = []
    for sym, qty in pos_qty.items():
        df = feats.get(sym)
        if df is None or len(df) < sma_trend + 5:
            continue
        last = df.iloc[-1]

        close = float(last["Close"])
        sma_t = last.get("sma_trend")
        sma_f = last.get("sma_fast")
        rsi_v = last.get("rsi")
        atr_v = last.get("atr")

        if pd.isna(sma_t) or pd.isna(sma_f) or pd.isna(rsi_v):
            continue

        sma_t = float(sma_t)
        sma_f = float(sma_f)
        rsi_v = float(rsi_v)
        atr_v = float(atr_v) if pd.notna(atr_v) else None

        trend_break = close < sma_t
        mean_revert_exit = (close > sma_f) or (rsi_v >= rsi_exit_above)

        if trend_break or mean_revert_exit:
            exits.append(
                {
                    "symbol": sym,
                    "qty": abs(int(qty)),
                    "reason": "trend_break" if trend_break else "bounce_exit",
                    "close": close,
                    "sma200": sma_t,  # keep name for UI compatibility
                    "sma5": sma_f,
                    "rsi2": rsi_v,
                    "atr14": atr_v,
                }
            )

    # Entries (only if gate ok)
    candidates: List[Dict[str, Any]] = []
    if gate.get("ok", True):
        for sym, df in feats.items():
            if sym in pos_qty:
                continue
            if sym in open_order_syms:
                continue
            if len(df) < sma_trend + 5:
                continue

            last = df.iloc[-1]

            close = float(last["Close"])
            sma_t = last.get("sma_trend")
            sma_f = last.get("sma_fast")
            rsi_v = last.get("rsi")
            atr_v = last.get("atr")
            adv20 = last.get("adv20")
            atr_pct = last.get("atr_pct")

            if any(pd.isna(x) for x in [sma_t, sma_f, rsi_v, atr_v, adv20, atr_pct]):
                continue

            sma_t = float(sma_t)
            sma_f = float(sma_f)
            rsi_v = float(rsi_v)
            atr_v = float(atr_v)
            adv20 = float(adv20)
            atr_pct = float(atr_pct)

            # filters
            price_ok = close >= min_price
            liq_ok = adv20 >= min_adv20
            vol_ok = atr_pct <= max_atr_pct

            if not (price_ok and liq_ok and vol_ok):
                continue

            trend_ok = close > sma_t
            pullback = close < sma_f
            oversold = rsi_v < rsi_buy_below
            buy_signal = trend_ok and pullback and oversold

            if buy_signal:
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
                        "checks": {"trend_ok": trend_ok, "pullback": pullback, "oversold": oversold},
                        "filters": {"price_ok": price_ok, "liq_ok": liq_ok, "vol_ok": vol_ok},
                    }
                )

    candidates.sort(key=lambda x: x["score"])

    # Estimate current open risk from existing positions (rough)
    est_open_risk = 0.0
    for sym, qty in pos_qty.items():
        df = feats.get(sym)
        if df is None or len(df) < 20:
            continue
        atr_v = df.iloc[-1].get("atr")
        if pd.isna(atr_v):
            continue
        est_open_risk += abs(int(qty)) * (stop_atr_mult * float(atr_v))

    # Allocate new slots
    slots = max(0, max_positions - len(pos_qty))
    planned_entries: List[Dict[str, Any]] = []
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

        # basic buying power cap
        est_cost = qty * close
        if est_cost > buying_power * 0.95:
            qty = int((buying_power * 0.95) // close)
        if qty <= 0:
            continue

        add_risk = qty * per_share_risk
        if est_open_risk + add_risk > max_open_risk_usd + 1e-9:
            continue

        planned_entries.append(
            {
                **c,
                "qty": qty,
                "stop_estimate": stop_est,
                "per_share_risk": per_share_risk,
                "risk_usd": add_risk,
            }
        )
        est_open_risk += add_risk
        slots -= 1

    # Build order intents WITH explainable fields (for UI)
    orders_to_submit: List[Dict[str, Any]] = []

    for e in exits:
        orders_to_submit.append(
            {
                "side": "sell",
                "symbol": e["symbol"],
                "qty": e["qty"],
                "tif": "opg",
                "type": "market",
                "reason": e["reason"],
                "close": e.get("close"),
                "sma200": e.get("sma200"),
                "sma5": e.get("sma5"),
                "rsi2": e.get("rsi2"),
                "atr14": e.get("atr14"),
            }
        )

    for b in planned_entries:
        orders_to_submit.append(
            {
                "side": "buy",
                "symbol": b["symbol"],
                "qty": b["qty"],
                "tif": "opg",
                "type": "market",
                "reason": "uptrend_dip_oversold",
                "close": b.get("close"),
                "sma200": b.get("sma200"),
                "sma5": b.get("sma5"),
                "rsi2": b.get("rsi2"),
                "atr14": b.get("atr14"),
                "adv20": b.get("adv20"),
                "atr_pct": b.get("atr_pct"),
                "checks": b.get("checks"),
                "filters": b.get("filters"),
                "stop_estimate": b.get("stop_estimate"),
                "per_share_risk": b.get("per_share_risk"),
                "risk_usd": b.get("risk_usd"),
            }
        )

    # Evidence files
    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", positions)
    _wjson(out_dir / "paper_open_orders.json", open_orders)
    _wjson(out_dir / "paper_gate.json", gate)

    _wjson(
        out_dir / "signals.json",
        {"entries": planned_entries, "exits": exits, "candidates_considered": len(candidates)},
    )
    _wjson(out_dir / "paper_orders_to_submit.json", orders_to_submit)

    # Agent-friendly aliases
    _wjson(out_dir / "orderIntents.json", orders_to_submit)

    # Trade explanations (plain-ish structure; UI renders it nicely)
    trade_explanations = []
    for o in orders_to_submit:
        trade_explanations.append(
            {
                "symbol": o.get("symbol"),
                "side": o.get("side"),
                "qty": o.get("qty"),
                "reason": o.get("reason"),
                "signal_values": {
                    "close": o.get("close"),
                    "sma200": o.get("sma200"),
                    "sma5": o.get("sma5"),
                    "rsi2": o.get("rsi2"),
                    "adv20": o.get("adv20"),
                    "atr_pct": o.get("atr_pct"),
                },
                "checks": o.get("checks"),
                "filters": o.get("filters"),
                "sizing": {
                    "stop_estimate": o.get("stop_estimate"),
                    "per_share_risk": o.get("per_share_risk"),
                    "risk_usd": o.get("risk_usd"),
                    "risk_per_trade_pct": risk_per_trade_pct,
                    "max_open_risk_pct": max_open_risk_pct,
                    "stop_atr_mult": stop_atr_mult,
                },
                "safety": {
                    "drawdown_gate": gate,
                    "note": "Safety Check (backtest + human ack) is recorded in qa.json by the command runner.",
                },
            }
        )
    _wjson(out_dir / "trade_explanations.json", trade_explanations)

    # Place orders (if enabled)
    placed: List[Any] = []
    skipped: List[Any] = []

    can_place = is_trading_enabled() and (not dry_run)
    if can_place:
        # import Alpaca request types inside runtime path
        from alpaca.trading.enums import OrderSide, TimeInForce
        from alpaca.trading.requests import MarketOrderRequest

        for o in orders_to_submit:
            sym = o["symbol"]
            if sym in open_order_syms:
                skipped.append({**o, "skip_reason": "existing_open_order"})
                continue

            req = MarketOrderRequest(
                symbol=sym,
                qty=int(o["qty"]),
                side=OrderSide.SELL if o["side"] == "sell" else OrderSide.BUY,
                time_in_force=TimeInForce.OPG,
                client_order_id=f"mfp-{sym}-{o['side']}",
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
    _wjson(out_dir / "paper_broker_ack.json", ack)

    # Agent-friendly alias
    _wjson(out_dir / "orders.json", ack)

    # Human-readable summary
    md = []
    md.append("# Trade Preview / Execution Report\n")
    md.append(f"- trading_enabled_env: `{is_trading_enabled()}`")
    md.append(f"- dry_run: `{dry_run}`")
    md.append(f"- will_place_orders: `{can_place}`\n")
    md.append("## Account\n")
    md.append(f"- equity: **{equity:.2f}**")
    md.append(f"- buying_power: **{buying_power:.2f}**\n")
    md.append("## Drawdown gate\n")
    md.append(f"- ok: **{gate.get('ok')}**")
    md.append(f"- reason: `{gate.get('reason')}`")
    if "worst_dd_1m" in gate:
        md.append(f"- worst_dd_1m: **{gate['worst_dd_1m'] * 100:.2f}%**")

    md.append("\n## Planned sells\n")
    md.extend([f"- SELL {e['symbol']} qty={e['qty']} reason={e['reason']}" for e in exits] or ["- none"])

    md.append("\n## Planned buys\n")
    md.extend(
        [
            f"- BUY {b['symbol']} qty={b['qty']} rsi={b['rsi2']:.2f} stop≈{b['stop_estimate']:.2f} risk≈${b['risk_usd']:.2f}"
            for b in planned_entries
        ]
        or ["- none"]
    )

    _wtext(out_dir / "paper_report.md", "\n".join(md) + "\n")

    zip_path = create_evidence_zip(out_dir)
    return {
        "dry_run": dry_run,
        "will_place": can_place,
        "orders_to_submit_count": len(orders_to_submit),
        "placed_count": len(placed),
        "zip": str(zip_path),
        "gate": gate,
    }


def paper_reconcile(
    out_dir: Path, symbols: List[str], place_stops: bool = False, stop_atr_mult: float = 1.5
) -> Dict[str, Any]:
    """
    Morning reconcile:
      - snapshot account/positions/orders
      - ensure each position has a protective STOP (GTC)
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    from mfp.paper.alpaca_io import (
        alpaca_data_client,
        alpaca_trading_client,
        fetch_daily_bars,
        get_account_snapshot,
        get_positions_snapshot,
        is_trading_enabled,
    )

    tc = alpaca_trading_client()
    dc = alpaca_data_client()

    acct = get_account_snapshot(tc)
    positions = get_positions_snapshot(tc)

    from alpaca.trading.enums import QueryOrderStatus
    from alpaca.trading.requests import GetOrdersRequest

    req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
    open_orders = tc.get_orders(filter=req)
    open_orders_dump = []
    for o in open_orders:
        try:
            open_orders_dump.append(o.model_dump())
        except Exception:
            open_orders_dump.append(o.dict())

    existing_stop_syms = set()
    for o in open_orders_dump:
        ot = o.get("order_type")
        if ot in ("stop", "stop_limit"):
            s = o.get("symbol")
            if s:
                existing_stop_syms.add(s)

    held_syms = sorted({p.get("symbol") for p in positions if p.get("symbol")})
    bars = fetch_daily_bars(dc, held_syms, lookback_days=250, feed="iex")

    # Simple ATR(14) for stop sizing
    from mfp.indicators import atr

    actions: Dict[str, Any] = {"placed": [], "skipped": []}

    for p in positions:
        sym = p.get("symbol")
        if not sym:
            continue
        try:
            qty = abs(int(float(p.get("qty") or 0)))
        except Exception:
            qty = 0
        if qty <= 0:
            continue

        if sym in existing_stop_syms:
            actions["skipped"].append({"symbol": sym, "reason": "stop_exists"})
            continue

        df = bars.get(sym)
        if df is None or len(df) < 30:
            actions["skipped"].append({"symbol": sym, "reason": "no_bars"})
            continue

        a = atr(df["High"], df["Low"], df["Close"], 14).iloc[-1]
        if pd.isna(a):
            actions["skipped"].append({"symbol": sym, "reason": "no_atr"})
            continue

        try:
            entry = float(p.get("avg_entry_price") or 0.0)
        except Exception:
            entry = 0.0
        if entry <= 0:
            actions["skipped"].append({"symbol": sym, "reason": "no_entry_price"})
            continue

        stop_price = max(0.01, entry - stop_atr_mult * float(a))

        if is_trading_enabled() and place_stops:
            from alpaca.trading.enums import OrderSide, TimeInForce
            from alpaca.trading.requests import StopOrderRequest

            try:
                req = StopOrderRequest(
                    symbol=sym,
                    qty=qty,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTC,
                    stop_price=stop_price,
                    client_order_id=f"mfp-stop-{sym}",
                )
                resp = tc.submit_order(order_data=req)
                try:
                    actions["placed"].append(resp.model_dump())
                except Exception:
                    actions["placed"].append(resp.dict())
            except Exception as e:
                actions["skipped"].append({"symbol": sym, "reason": f"submit_failed:{type(e).__name__}"})
        else:
            actions["skipped"].append(
                {"symbol": sym, "reason": "trading_disabled_or_place_stops_false", "stop_price": stop_price}
            )

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", positions)
    _wjson(out_dir / "paper_open_orders.json", open_orders_dump)
    _wjson(out_dir / "paper_stop_actions.json", actions)

    zip_path = create_evidence_zip(out_dir)
    return {"zip": str(zip_path), "stop_actions": actions}
