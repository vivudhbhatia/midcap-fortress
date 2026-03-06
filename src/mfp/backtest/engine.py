from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
import pandas as pd
import numpy as np

from mfp.data.yfinance_provider import load_prices_yf
from mfp.data.universe_sp400 import get_universe_sp400
from mfp.indicators import sma, rsi, atr
from mfp.strategy.midcap_pulse_v1 import params_for_timeframe
from mfp.backtest.metrics import compute_metrics

def _resample_ohlcv(df: pd.DataFrame, rule: str) -> pd.DataFrame:
    o = df["Open"].resample(rule).first()
    h = df["High"].resample(rule).max()
    l = df["Low"].resample(rule).min()
    c = df["Close"].resample(rule).last()
    v = df["Volume"].resample(rule).sum()
    out = pd.concat([o, h, l, c, v], axis=1).dropna()
    out.columns = ["Open", "High", "Low", "Close", "Volume"]
    return out

@dataclass
class Position:
    symbol: str
    shares: int
    entry_price: float
    stop_price: float
    entry_dt: pd.Timestamp
    bars_held: int = 0

def run_backtest(
    out_dir: Path,
    timeframe: str,
    start: str,
    end: str,
    universe_name: str,
    strategy_name: str,
    max_symbols: int | None = None,
) -> dict:
    if universe_name != "sp400":
        universe_name = "sp400"
    tickers = get_universe_sp400()
    if max_symbols:
        tickers = tickers[:max_symbols]

    cache_dir = out_dir.parent.parent / "data" / "cache"

    daily = load_prices_yf(
        tickers=tickers,
        start=start,
        end=end,
        cache_dir=cache_dir,
        chunk_size=50,
    )

    # Build timeframe bars
    if timeframe == "1d":
        bars = daily
    elif timeframe == "1wk":
        bars = {t: _resample_ohlcv(df, "W-FRI") for t, df in daily.items()}
    elif timeframe == "1mo":
        bars = {t: _resample_ohlcv(df, "M") for t, df in daily.items()}
    else:
        raise ValueError("timeframe must be 1d, 1wk, or 1mo")

    params = params_for_timeframe(timeframe)

    # Precompute indicators
    data = {}
    for t, df in bars.items():
        if len(df) < max(params.trend_sma, params.atr_len) + 5:
            continue
        d = df.copy()
        d["sma_trend"] = sma(d["Close"], params.trend_sma)
        d["sma_fast"] = sma(d["Close"], params.fast_sma)
        d["rsi2"] = rsi(d["Close"], params.rsi_len)
        d["atr"] = atr(d["High"], d["Low"], d["Close"], params.atr_len)
        d["atr_pct"] = d["atr"] / d["Close"]
        data[t] = d

    # Union of dates
    all_dates = sorted(set().union(*[df.index for df in data.values()]))
    if len(all_dates) < 5:
        raise RuntimeError("Not enough data to run backtest (check tickers/time range).")

    initial_cash = 5000.0
    cash = initial_cash
    equity_series = []
    dd_series = []

    # Risk controls (similar to your fortress constraints)
    risk_per_trade_pct = 0.25 / 100.0
    max_open_risk_pct = 1.0 / 100.0
    max_positions = 6

    positions: dict[str, Position] = {}
    trades = []

    def portfolio_equity(dt: pd.Timestamp) -> float:
        eq = cash
        for sym, pos in positions.items():
            df = data.get(sym)
            if df is None or dt not in df.index:
                continue
            eq += pos.shares * float(df.loc[dt, "Close"])
        return float(eq)

    def open_risk_usd() -> float:
        r = 0.0
        for sym, pos in positions.items():
            r += pos.shares * max(0.0, (pos.entry_price - pos.stop_price))
        return float(r)

    for i, dt in enumerate(all_dates[:-1]):
        next_dt = all_dates[i + 1]

        # mark-to-market
        eq = portfolio_equity(dt)
        equity_series.append((dt, eq))

        # compute drawdown
        eq_vals = [e for _, e in equity_series]
        peak = max(eq_vals)
        dd = (eq / peak) - 1.0
        dd_series.append((dt, dd))

        # 1) STOP CHECK (intrabar on dt)
        to_stop = []
        for sym, pos in positions.items():
            df = data[sym]
            if dt not in df.index:
                continue
            low = float(df.loc[dt, "Low"])
            if low <= pos.stop_price:
                # stop filled at stop price
                fill = pos.stop_price
                cash_delta = pos.shares * fill
                cash_delta = float(cash_delta)
                cash_nonlocal = cash_delta  # for clarity
                trades.append({
                    "symbol": sym,
                    "entry_dt": pos.entry_dt,
                    "exit_dt": dt,
                    "entry_price": pos.entry_price,
                    "exit_price": fill,
                    "shares": pos.shares,
                    "exit_reason": "stop",
                    "pnl": (fill - pos.entry_price) * pos.shares
                })
                to_stop.append(sym)
                # update cash after loop to avoid dict mutation complexity
                cash += cash_nonlocal
        for sym in to_stop:
            del positions[sym]

        # 2) EXIT SIGNALS (evaluated at close(dt), executed at open(next_dt))
        exit_syms = []
        for sym, pos in positions.items():
            df = data[sym]
            if dt not in df.index or next_dt not in df.index:
                continue
            close = float(df.loc[dt, "Close"])
            sma_fast = float(df.loc[dt, "sma_fast"])
            sma_trend = float(df.loc[dt, "sma_trend"])
            rsi2v = float(df.loc[dt, "rsi2"])

            pos.bars_held += 1
            time_exit = pos.bars_held >= params.max_hold_bars
            profit_exit = (close > sma_fast) or (rsi2v >= params.rsi_exit_above)
            trend_exit = close < sma_trend

            if time_exit or profit_exit or trend_exit:
                fill = float(df.loc[next_dt, "Open"])
                cash += pos.shares * fill
                trades.append({
                    "symbol": sym,
                    "entry_dt": pos.entry_dt,
                    "exit_dt": next_dt,
                    "entry_price": pos.entry_price,
                    "exit_price": fill,
                    "shares": pos.shares,
                    "exit_reason": "time" if time_exit else ("profit" if profit_exit else "trend"),
                    "pnl": (fill - pos.entry_price) * pos.shares
                })
                exit_syms.append(sym)

        for sym in exit_syms:
            if sym in positions:
                del positions[sym]

        # 3) ENTRY SIGNALS (evaluated at close(dt), executed at open(next_dt))
        if len(positions) >= max_positions:
            continue

        # build candidates
        candidates = []
        for sym, df in data.items():
            if sym in positions:
                continue
            if dt not in df.index or next_dt not in df.index:
                continue
            row = df.loc[dt]
            if pd.isna(row["sma_trend"]) or pd.isna(row["sma_fast"]) or pd.isna(row["rsi2"]) or pd.isna(row["atr"]):
                continue

            close = float(row["Close"])
            sma_trend = float(row["sma_trend"])
            sma_fast = float(row["sma_fast"])
            rsi2v = float(row["rsi2"])

            trend_ok = close > sma_trend
            pullback = close < sma_fast
            buy_signal = trend_ok and pullback and (rsi2v < params.rsi_buy_below)

            if buy_signal:
                # lower RSI is "more oversold" -> higher priority
                candidates.append((sym, rsi2v))

        candidates.sort(key=lambda x: x[1])  # most oversold first

        # risk budgets
        eq_now = portfolio_equity(dt)
        risk_per_trade_usd = eq_now * risk_per_trade_pct
        max_open_risk_usd = eq_now * max_open_risk_pct

        for sym, rsi2v in candidates:
            if len(positions) >= max_positions:
                break
            if open_risk_usd() + risk_per_trade_usd > max_open_risk_usd + 1e-9:
                break

            df = data[sym]
            entry = float(df.loc[next_dt, "Open"])
            atrv = float(df.loc[dt, "atr"])
            stop = entry - params.stop_atr_mult * atrv
            if stop <= 0 or entry <= 0:
                continue
            per_share_risk = entry - stop
            if per_share_risk <= 0:
                continue
            shares = int(risk_per_trade_usd // per_share_risk)
            if shares <= 0:
                continue
            cost = shares * entry
            if cost > cash:
                continue

            cash -= cost
            positions[sym] = Position(
                symbol=sym,
                shares=shares,
                entry_price=entry,
                stop_price=stop,
                entry_dt=next_dt,
                bars_held=0
            )

    # final equity point
    last_dt = all_dates[-1]
    equity_series.append((last_dt, portfolio_equity(last_dt)))
    eq_df = pd.DataFrame(equity_series, columns=["date", "equity"]).set_index("date")
    dd_df = pd.DataFrame(dd_series, columns=["date", "drawdown"]).set_index("date")

    eq_df.to_csv(out_dir / "equity.csv")
    dd_df.to_csv(out_dir / "drawdown.csv")

    trades_df = pd.DataFrame(trades)
    metrics = compute_metrics(eq_df["equity"], timeframe=timeframe, trades=trades_df)

    return {
        "equity": eq_df["equity"],
        "drawdown": dd_df["drawdown"],
        "trades": trades_df,
        "metrics": metrics,
        "meta": {
            "timeframe": timeframe,
            "start": start,
            "end": end,
            "universe_name": universe_name,
            "strategy_name": strategy_name,
            "max_symbols": max_symbols,
        },
    }
