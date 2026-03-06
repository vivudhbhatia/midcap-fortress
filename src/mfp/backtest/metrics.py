from __future__ import annotations
import pandas as pd
import numpy as np

def _periods_per_year(timeframe: str) -> int:
    return {"1d": 252, "1wk": 52, "1mo": 12}[timeframe]

def compute_metrics(equity: pd.Series, timeframe: str, trades: pd.DataFrame) -> dict:
    eq = equity.dropna()
    if len(eq) < 2:
        return {
            "final_equity": float(eq.iloc[-1]) if len(eq) else float("nan"),
            "cagr": 0.0,
            "max_drawdown": 0.0,
            "num_trades": 0,
            "win_rate": 0.0,
        }

    rets = eq.pct_change().dropna()
    ppy = _periods_per_year(timeframe)

    years = len(rets) / ppy
    cagr = (eq.iloc[-1] / eq.iloc[0]) ** (1 / years) - 1 if years > 0 else 0.0

    roll_max = eq.cummax()
    dd = (eq / roll_max) - 1.0
    max_dd = float(dd.min())

    vol = float(rets.std() * np.sqrt(ppy)) if len(rets) > 1 else 0.0
    mean_ann = float(rets.mean() * ppy) if len(rets) else 0.0
    sharpe = (mean_ann / vol) if vol > 1e-12 else 0.0

    num_trades = int(len(trades)) if trades is not None else 0
    if num_trades > 0:
        wins = (trades["pnl"] > 0).sum()
        win_rate = float(wins / num_trades)
        profit_factor = float(trades.loc[trades["pnl"] > 0, "pnl"].sum() / abs(trades.loc[trades["pnl"] < 0, "pnl"].sum() or 1e-12))
        avg_trade = float(trades["pnl"].mean())
    else:
        win_rate = 0.0
        profit_factor = 0.0
        avg_trade = 0.0

    return {
        "final_equity": float(eq.iloc[-1]),
        "cagr": float(cagr),
        "max_drawdown": float(abs(max_dd)),
        "ann_vol": float(vol),
        "sharpe_0rf": float(sharpe),
        "num_trades": num_trades,
        "win_rate": float(win_rate),
        "profit_factor": float(profit_factor),
        "avg_trade_pnl": float(avg_trade),
    }
