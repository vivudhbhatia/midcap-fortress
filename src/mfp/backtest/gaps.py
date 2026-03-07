from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd


def default_windows(timeframe: str) -> List[int]:
    if timeframe == "1d":
        return [5, 20, 60]
    if timeframe == "1wk":
        return [4, 13, 26]
    if timeframe == "1mo":
        return [3, 6, 12]
    return [5, 20, 60]


def rolling_window_max_drawdown(eq: pd.Series, window: int) -> Dict[str, Any]:
    eq = eq.dropna()
    if len(eq) < window or window < 2:
        return {
            "window": int(window),
            "max_dd": 0.0,
            "peak_date": None,
            "trough_date": None,
            "peak_equity": None,
            "trough_equity": None,
        }

    values = eq.values.astype(float)
    dates = eq.index

    worst_dd = 0.0
    worst_peak_date = None
    worst_trough_date = None
    worst_peak_equity = None
    worst_trough_equity = None

    for i in range(0, len(values) - window + 1):
        seg = values[i : i + window]
        cummax = np.maximum.accumulate(seg)
        dd = (seg / cummax) - 1.0
        seg_min = float(dd.min())

        if seg_min < worst_dd:
            worst_dd = seg_min
            trough_idx = int(dd.argmin())
            peak_idx = int(np.argmax(seg[: trough_idx + 1]))

            worst_peak_date = dates[i + peak_idx]
            worst_trough_date = dates[i + trough_idx]
            worst_peak_equity = float(seg[peak_idx])
            worst_trough_equity = float(seg[trough_idx])

    return {
        "window": int(window),
        "max_dd": float(abs(worst_dd)),
        "peak_date": str(worst_peak_date) if worst_peak_date is not None else None,
        "trough_date": str(worst_trough_date) if worst_trough_date is not None else None,
        "peak_equity": worst_peak_equity,
        "trough_equity": worst_trough_equity,
    }


def compute_gap_report(
    equity: pd.Series,
    timeframe: str,
    dd_threshold: float = 0.03,
    windows: Optional[List[int]] = None,
    meta: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    eq = equity.dropna()
    windows = windows or default_windows(timeframe)

    rolling = [rolling_window_max_drawdown(eq, w) for w in windows]
    violations = [r for r in rolling if r["max_dd"] > dd_threshold + 1e-12]

    overall_dd = float(((eq / eq.cummax()) - 1.0).min()) if len(eq) else 0.0

    return {
        "timeframe": timeframe,
        "dd_threshold": float(dd_threshold),
        "overall_max_drawdown": float(abs(overall_dd)),
        "rolling": rolling,
        "violations": violations,
        "pass_drawdown_rule": (len(violations) == 0),
        "meta": meta or {},
        "notes": [
            "Rolling drawdown is computed within-bar windows (bars depend on timeframe).",
            "This is a diagnostic; real trading can be worse due to gaps/slippage.",
        ],
    }
