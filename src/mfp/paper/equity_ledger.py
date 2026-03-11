from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

import pandas as pd

from mfp.objectives.targets import window_days


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def ledger_path(workspace: Path) -> Path:
    return _state_dir(workspace) / "equity_ledger.csv"


def append_equity(workspace: Path, ts_utc: str, equity: float) -> None:
    p = ledger_path(workspace)
    new_file = not p.exists()
    with p.open("a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if new_file:
            w.writerow(["ts_utc", "equity"])
        w.writerow([ts_utc, f"{equity:.6f}"])


def load_equity_series_daily(workspace: Path) -> pd.Series:
    p = ledger_path(workspace)
    if not p.exists():
        return pd.Series([], dtype="float64")

    df = pd.read_csv(p)
    if df.empty or "ts_utc" not in df.columns or "equity" not in df.columns:
        return pd.Series([], dtype="float64")

    ts = pd.to_datetime(df["ts_utc"], errors="coerce", utc=True)
    eq = pd.to_numeric(df["equity"], errors="coerce")
    d = pd.Series(eq.values, index=ts).dropna()
    if len(d) == 0:
        return pd.Series([], dtype="float64")

    # Keep last equity per day (UTC)
    daily = d.resample("1D").last().dropna()
    return daily.astype("float64")


def _rolling_dd(equity: pd.Series, w: int) -> float:
    if len(equity) == 0:
        return 0.0
    roll_max = equity.rolling(w, min_periods=1).max()
    dd = (equity / roll_max) - 1.0
    return float(dd.min())


def compute_drawdown_gate(
    workspace: Path,
    dd_limits: Dict[str, Any],
) -> Dict[str, Any]:
    s = load_equity_series_daily(workspace)
    if len(s) < 3:
        return {"ok": True, "reason": "no_ledger_yet", "limits": dd_limits}

    out: Dict[str, Any] = {"ok": True, "reason": "ok", "limits": dd_limits, "windows": {}}
    breached = False

    for k, lim in dd_limits.items():
        days = window_days(str(k))
        worst = abs(_rolling_dd(s, days))
        ok = worst <= float(lim) + 1e-12
        out["windows"][str(k).upper()] = {"limit": float(lim), "worst": float(worst), "pass": bool(ok)}
        if not ok:
            breached = True

    if breached:
        out["ok"] = False
        out["reason"] = "drawdown_breach"

    return out


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
