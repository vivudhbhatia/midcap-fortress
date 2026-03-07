from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
from alpaca.trading.enums import OrderSide, QueryOrderStatus, TimeInForce
from alpaca.trading.requests import GetOrdersRequest, MarketOrderRequest, StopOrderRequest

from mfp.audit.evidence import create_evidence_zip
from mfp.indicators import atr, rsi, sma
from mfp.paper.alpaca_io import (
    alpaca_data_client,
    alpaca_trading_client,
    fetch_daily_bars,
    fetch_portfolio_history_raw,
    get_account_snapshot,
    get_open_orders_snapshot,
    get_positions_snapshot,
    is_trading_enabled,
)


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


def paper_status(out_dir: Path) -> Dict[str, Any]:
    """
    Snapshot Alpaca paper account/positions/open-orders and write JSON files.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    tc = alpaca_trading_client()

    acct = get_account_snapshot(tc)
    pos = get_positions_snapshot(tc)
    orders = get_open_orders_snapshot(tc)

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", pos)
    _wjson(out_dir / "paper_open_orders.json", orders)

    return {"account": acct, "positions": pos, "open_orders": orders}


def _compute_features(df: pd.DataFrame) -> pd.DataFrame:
    d = df.copy()
    d["sma200"] = sma(d["Close"], 200)
    d["sma5"] = sma(d["Close"], 5)
    d["rsi2"] = rsi(d["Close"], 2)
    d["atr14"] = atr(d["High"], d["Low"], d["Close"], 14)
    d["atr_pct"] = d["atr14"] / d["Close"]
    d["dollar_vol"] = d["Close"] * d["Volume"]
    d["adv20"] = d["dollar_vol"].rolling(20).mean()
    return d


def _rolling_drawdown_gate() -> Dict[str, Any]:
    """
    Safety: if account drawdown in last month exceeds 3%, block new entries.
    If portfolio history endpoint isn't available, don't block.
    """
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
    min_price: float = 10.0,
    min_adv20: float = 20_000_000.0,
    max_atr_pct: float = 0.08,
    stop_atr_mult: float = 1.5,
) -> Dict[str, Any]:
    """
    Nightly cycle:
      - compute signals using latest daily bar
      - create OPG market orders for next open (buys + sells)
      - if trading enabled and dry_run==False, submit orders
    """
    out_dir.mkdir(parents=True, exist_ok=True)

    max_positions = _env_int("MFP_MAX_POSITIONS", 6)
    risk_per_trade_pct = _env_float("MFP_RISK_PER_TRADE_PCT", 0.25)
    max_open_risk_pct = _env_float("MFP_MAX_OPEN_RISK_PCT", 1.00)

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
    feats: Dict[str, pd.DataFrame] = {s: _compute_features(df) for s, df in bars.items()}

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
        if df is None or len(df) < 210:
            continue
        last = df.iloc[-1]
        close = float(last["Close"])
        sma200 = float(last["sma200"]) if pd.notna(last["sma200"]) else None
        sma5 = float(last["sma5"]) if pd.notna(last["sma5"]) else None
        rsi2v = float(last["rsi2"]) if pd.notna(last["rsi2"]) else None
        if sma200 is None or sma5 is None or rsi2v is None:
            continue

        trend_break = close < sma200
        mean_revert_exit = (close > sma5) or (rsi2v >= 70)

        if trend_break or mean_revert_exit:
            exits.append(
                {
                    "symbol": sym,
                    "qty": abs(int(qty)),
                    "reason": "trend_break" if trend_break else "mean_revert_exit",
                    "close": close,
                    "sma200": sma200,
                    "sma5": sma5,
                    "rsi2": rsi2v,
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
            if len(df) < 210:
                continue

            last = df.iloc[-1]
            close = float(last["Close"])
            sma200 = float(last["sma200"]) if pd.notna(last["sma200"]) else None
            sma5 = float(last["sma5"]) if pd.notna(last["sma5"]) else None
            rsi2v = float(last["rsi2"]) if pd.notna(last["rsi2"]) else None
            atr14 = float(last["atr14"]) if pd.notna(last["atr14"]) else None
            adv20 = float(last["adv20"]) if pd.notna(last["adv20"]) else None
            atr_pct = float(last["atr_pct"]) if pd.notna(last["atr_pct"]) else None

            if None in (sma200, sma5, rsi2v, atr14, adv20, atr_pct):
                continue
            if close < min_price:
                continue
            if adv20 < min_adv20:
                continue
            if atr_pct > max_atr_pct:
                continue

            trend_ok = close > sma200
            pullback = close < sma5
            buy_signal = trend_ok and pullback and (rsi2v < 10)

            if buy_signal:
                candidates.append(
                    {
                        "symbol": sym,
                        "score": float(rsi2v),  # lower RSI2 = more oversold
                        "close": close,
                        "sma200": sma200,
                        "sma5": sma5,
                        "rsi2": float(rsi2v),
                        "atr14": float(atr14),
                        "adv20": float(adv20),
                        "atr_pct": float(atr_pct),
                    }
                )

    candidates.sort(key=lambda x: x["score"])

    # Estimate current open risk from existing positions
    est_open_risk = 0.0
    for sym, qty in pos_qty.items():
        df = feats.get(sym)
        if df is None or len(df) < 20:
            continue
        atr14 = df.iloc[-1].get("atr14")
        if pd.isna(atr14):
            continue
        est_open_risk += abs(int(qty)) * (stop_atr_mult * float(atr14))

    # Allocate new slots
    slots = max(0, max_positions - len(pos_qty))
    planned_entries: List[Dict[str, Any]] = []
    for c in candidates[: max(0, slots * 3)]:
        if slots <= 0:
            break

        close = float(c["close"])
        atr14 = float(c["atr14"])
        stop_est = close - stop_atr_mult * atr14
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
                "reason": "rsi2_pullback_uptrend",
                "stop_estimate": b["stop_estimate"],
                "risk_usd": b["risk_usd"],
            }
        )

    # Evidence
    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", positions)
    _wjson(out_dir / "paper_open_orders.json", open_orders)
    _wjson(out_dir / "paper_gate.json", gate)
    _wjson(out_dir / "paper_orders_to_submit.json", orders_to_submit)

    placed: List[Any] = []
    skipped: List[Any] = []

    can_place = is_trading_enabled() and (not dry_run)
    if can_place:
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

    _wjson(out_dir / "paper_broker_ack.json", {"placed": placed, "skipped": skipped})

    md = []
    md.append("# Paper Cycle Report\n")
    md.append(f"- trading_enabled_env: `{is_trading_enabled()}`")
    md.append(f"- dry_run: `{dry_run}`")
    md.append(f"- will_place_orders: `{can_place}`\n")
    md.append("## Account\n")
    md.append(f"- equity: **{equity:.2f}**")
    md.append(f"- buying_power: **{buying_power:.2f}**\n")
    md.append("## Drawdown Gate\n")
    md.append(f"- ok: **{gate.get('ok')}**")
    md.append(f"- reason: `{gate.get('reason')}`")
    if "worst_dd_1m" in gate:
        md.append(f"- worst_dd_1m: **{gate['worst_dd_1m'] * 100:.2f}%**")
    md.append("\n## Planned Sells\n")
    md.extend([f"- SELL {e['symbol']} qty={e['qty']} reason={e['reason']}" for e in exits] or ["- none"])
    md.append("\n## Planned Buys\n")
    md.extend(
        [
            f"- BUY {b['symbol']} qty={b['qty']} rsi2={b['rsi2']:.2f} stop_est={b['stop_estimate']:.2f} risk=${b['risk_usd']:.2f}"
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
    tc = alpaca_trading_client()
    dc = alpaca_data_client()

    acct = get_account_snapshot(tc)
    positions = get_positions_snapshot(tc)

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
    bars = fetch_daily_bars(dc, held_syms, lookback_days=450, feed="iex")
    feats: Dict[str, pd.DataFrame] = {s: _compute_features(df) for s, df in bars.items()}

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

        df = feats.get(sym)
        if df is None or len(df) < 50:
            actions["skipped"].append({"symbol": sym, "reason": "no_bars"})
            continue

        atr14 = df.iloc[-1].get("atr14")
        if pd.isna(atr14):
            actions["skipped"].append({"symbol": sym, "reason": "no_atr"})
            continue

        try:
            entry = float(p.get("avg_entry_price") or 0.0)
        except Exception:
            entry = 0.0
        if entry <= 0:
            actions["skipped"].append({"symbol": sym, "reason": "no_entry_price"})
            continue

        stop_price = max(0.01, entry - stop_atr_mult * float(atr14))

        if is_trading_enabled() and place_stops:
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
