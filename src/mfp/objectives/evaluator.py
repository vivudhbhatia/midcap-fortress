from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

import pandas as pd

from mfp.objectives.targets import annualize_from_cum, expected_cum_return, window_days


@dataclass(frozen=True)
class WindowEval:
    label: str
    days: int
    cum_return: float
    annualized_return: float
    expected_cum: float
    threshold_cum: float
    pass_: bool
    soft: bool


def _safe_series(equity: Any) -> pd.Series:
    if isinstance(equity, pd.Series):
        s = equity.copy()
        s = s.dropna()
        return s.astype("float64")
    if isinstance(equity, list):
        return pd.Series(equity, dtype="float64").dropna()
    raise TypeError("equity must be a pandas Series or list[float]")


def _rolling_dd(equity: pd.Series, window: int) -> float:
    if len(equity) == 0:
        return 0.0
    w = max(1, int(window))
    roll_max = equity.rolling(w, min_periods=1).max()
    dd = (equity / roll_max) - 1.0
    return float(dd.min())  # negative


def evaluate_objectives(
    equity: Any,
    objectives_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Returns:
      - windows: per-window return checks (short windows marked soft)
      - drawdown: rolling DD checks for 1D/5D/20D
      - pass: overall pass (drawdown hard + optionally return hard)
    """
    s = _safe_series(equity)
    s = s[s > 0.0]
    if len(s) < 5:
        return {"pass": False, "reasons": ["not_enough_equity_points"], "windows": [], "drawdown": {}}

    target = float(objectives_cfg.get("target_annual_cagr", 0.20))
    acceptance = float(objectives_cfg.get("acceptance_factor", 0.75))
    enforce_return = bool(objectives_cfg.get("enforce_return_in_safety_check", False))
    return_windows = objectives_cfg.get("return_windows", ["1Y", "3Y", "5Y"])
    dd_limits = objectives_cfg.get("drawdown_limits", {"1D": 0.01, "5D": 0.02, "20D": 0.03})

    win_results: List[WindowEval] = []
    reasons: List[str] = []

    for wlab in return_windows:
        days = window_days(str(wlab))
        soft = days < 252  # < 1Y is “health info”, not a hard gate by default

        if len(s) < days + 1:
            # Not enough data for this window
            continue

        seg = s.iloc[-(days + 1) :]
        start = float(seg.iloc[0])
        end = float(seg.iloc[-1])
        cum = (end / start) - 1.0 if start > 0 else 0.0
        ann = annualize_from_cum(cum, days)

        exp_cum = expected_cum_return(target, days)
        thr_cum = exp_cum * acceptance
        pass_ = cum >= thr_cum

        win_results.append(
            WindowEval(
                label=str(wlab).upper(),
                days=days,
                cum_return=float(cum),
                annualized_return=float(ann),
                expected_cum=float(exp_cum),
                threshold_cum=float(thr_cum),
                pass_=bool(pass_),
                soft=soft,
            )
        )

        if enforce_return and (not soft) and (not pass_):
            reasons.append(f"return_below_threshold:{wlab}")

    # Drawdown checks (hard safety by default)
    dd_out: Dict[str, Dict[str, Any]] = {}
    dd_pass = True
    for k, lim in dd_limits.items():
        days = window_days(k)
        worst = _rolling_dd(s, days)
        ok = abs(worst) <= float(lim) + 1e-12
        dd_out[str(k).upper()] = {"limit": float(lim), "worst": float(abs(worst)), "pass": bool(ok)}
        if not ok:
            dd_pass = False
            reasons.append(f"drawdown_breach:{k}")

    overall_pass = dd_pass and (len(reasons) == 0 or not enforce_return)

    return {
        "pass": bool(overall_pass),
        "reasons": reasons,
        "windows": [w.__dict__ for w in win_results],
        "drawdown": dd_out,
    }
