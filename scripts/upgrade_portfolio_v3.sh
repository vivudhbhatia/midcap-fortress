#!/usr/bin/env bash
set -euo pipefail

echo "==> Upgrade Portfolio v3: writing files..."

mkdir -p src/mfp/agents src/mfp/config src/mfp/objectives src/mfp/portfolio src/mfp/paper tests

# -------------------------
# 1) Package init files
# -------------------------
cat > src/mfp/agents/__init__.py <<'PY'
PY

cat > src/mfp/objectives/__init__.py <<'PY'
PY

cat > src/mfp/portfolio/__init__.py <<'PY'
PY

# -------------------------
# 2) Config normalization (defaults for new features)
# -------------------------
cat > src/mfp/config/normalize.py <<'PY'
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict

Config = Dict[str, Any]


DEFAULTS: Config = {
    "universe": {
        "name": "sp400",
        "max_symbols": 60,
    },
    "objectives": {
        # Long-run goal (annualized)
        "target_annual_cagr": 0.20,
        "goal_horizon_years": 4,
        # Softening factor for evaluation thresholds
        "acceptance_factor": 0.75,
        # Which windows to show/evaluate in the UI
        "return_windows": ["1M", "3M", "6M", "1Y", "3Y", "5Y"],
        # If True, portfolio Safety Check fails when returns are below threshold.
        # Default False keeps Safety Check focused on drawdown and integrity.
        "enforce_return_in_safety_check": False,
        # Rolling drawdown limits (interpreted from equity ledger)
        # "1D" means daily (not intraday); with gaps, you still can exceed this.
        "drawdown_limits": {"1D": 0.01, "5D": 0.02, "20D": 0.03},
        # Emergency option (default False):
        # if breached, the critic will recommend “go to cash”.
        "panic_close_on_breach": False,
    },
    "portfolio": {
        "paper_equity_usd": 100000.0,
        # If True, sizing uses the virtual portfolio (scaled down if Alpaca equity < virtual).
        "use_virtual_equity": True,
        # Require portfolio Safety Check + acknowledgement before placing paper orders
        "require_portfolio_safety_check": True,
        "strategies": {
            "MFP_STOCKS_MID": {
                "label": "Stocks: dip buys (Mid-cap)",
                "enabled": True,
                "allocation_pct": 0.35,
                "engine": "pulse_mr",
                "universe": "sp400",
                "symbols": [],
                "proxy": "IJH",
                "max_symbols": 60,
                "overrides": {
                    "risk_per_trade_pct": 0.25,
                    "max_open_risk_pct": 1.00,
                    "max_positions": 6,
                    "stop_atr_mult": 1.5,
                    "rsi_buy_below": 10.0,
                    "rsi_exit_above": 70.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "MFP_STOCKS_LARGE": {
                "label": "Stocks: dip buys (Large-cap)",
                "enabled": True,
                "allocation_pct": 0.35,
                "engine": "pulse_mr",
                "universe": "sp500",
                "symbols": [],
                "proxy": "SPY",
                "max_symbols": 60,
                "overrides": {
                    "risk_per_trade_pct": 0.25,
                    "max_open_risk_pct": 1.00,
                    "max_positions": 6,
                    "stop_atr_mult": 1.5,
                    "rsi_buy_below": 10.0,
                    "rsi_exit_above": 70.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "ETF_MR": {
                "label": "ETF: dip buys (lower gap risk)",
                "enabled": True,
                "allocation_pct": 0.20,
                "engine": "pulse_mr",
                "universe": "",
                "symbols": ["IJH"],
                "proxy": "IJH",
                "max_symbols": 10,
                "overrides": {
                    "risk_per_trade_pct": 0.35,
                    "max_open_risk_pct": 1.50,
                    "max_positions": 1,
                    "stop_atr_mult": 1.25,
                    "rsi_buy_below": 12.0,
                    "rsi_exit_above": 75.0,
                    "trend_sma": 200,
                    "fast_sma": 5,
                },
            },
            "ETF_TREND": {
                "label": "ETF: trend defense (simple)",
                "enabled": True,
                "allocation_pct": 0.10,
                "engine": "trend_hold",
                "universe": "",
                "symbols": ["SPY"],
                "proxy": "SPY",
                "max_symbols": 10,
                "overrides": {
                    "trend_sma": 200,
                },
            },
        },
        "switching": {
            "enabled": True,
            "sma_len": 200,
            "shock_2d_pct": 0.04,
            "vol_20d_high": 0.03,
            # Simple regime -> weights
            "weights": {
                "RISK_ON": {"MFP_STOCKS_MID": 0.35, "MFP_STOCKS_LARGE": 0.35, "ETF_MR": 0.20, "ETF_TREND": 0.10},
                "RISK_OFF": {"ETF_TREND": 0.30, "CASH": 0.70},
            },
        },
    },
}


def _deep_merge(dst: Config, src: Config) -> Config:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_merge(dst[k], v)  # type: ignore[index]
        else:
            dst[k] = v
    return dst


def normalize_config(cfg: Config | None) -> Config:
    base = deepcopy(DEFAULTS)
    if isinstance(cfg, dict):
        _deep_merge(base, cfg)
    return base
PY

# -------------------------
# 3) Objectives: window translation + evaluation
# -------------------------
cat > src/mfp/objectives/targets.py <<'PY'
from __future__ import annotations

from typing import Dict

TRADING_DAYS_PER_YEAR = 252

WINDOW_DAYS: Dict[str, int] = {
    "1D": 1,
    "5D": 5,
    "20D": 20,
    "1M": 21,
    "3M": 63,
    "6M": 126,
    "1Y": 252,
    "3Y": 252 * 3,
    "5Y": 252 * 5,
}


def window_days(label: str) -> int:
    k = label.strip().upper()
    if k not in WINDOW_DAYS:
        raise ValueError(f"Unknown window label {label!r}. Use one of: {sorted(WINDOW_DAYS)}")
    return WINDOW_DAYS[k]


def expected_cum_return(target_annual_cagr: float, days: int) -> float:
    """
    Translate an annual growth target into the *expected cumulative return* over `days`.
    This is how we avoid nonsense like “20% CAGR per day”.
    """
    if days <= 0:
        return 0.0
    years = days / TRADING_DAYS_PER_YEAR
    return (1.0 + target_annual_cagr) ** years - 1.0


def annualize_from_cum(cum_return: float, days: int) -> float:
    if days <= 0:
        return 0.0
    years = days / TRADING_DAYS_PER_YEAR
    if years <= 0:
        return 0.0
    if cum_return <= -1.0:
        return -1.0
    return (1.0 + cum_return) ** (1.0 / years) - 1.0
PY

cat > src/mfp/objectives/evaluator.py <<'PY'
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

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
PY

# -------------------------
# 4) Portfolio Safety certificate (separate from pretrade single-strategy)
# -------------------------
cat > src/mfp/governance/portfolio_safety.py <<'PY'
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from mfp.config.runtime import config_hash


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def certificate_path(workspace: Path) -> Path:
    return _state_dir(workspace) / "portfolio_safety_certificate.json"


def load_certificate(workspace: Path) -> Optional[Dict[str, Any]]:
    p = certificate_path(workspace)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def write_certificate(workspace: Path, cert: Dict[str, Any]) -> Path:
    p = certificate_path(workspace)
    p.write_text(json.dumps(cert, indent=2, default=str), encoding="utf-8", newline="\n")
    return p


def validate_certificate(workspace: Path, cfg: Dict[str, Any], require_reviewed: bool = False) -> Dict[str, Any]:
    cert = load_certificate(workspace)
    if not cert:
        return {"ok": False, "reason": "no_certificate", "cert": None}

    ch = config_hash(cfg)
    if cert.get("config_hash") != ch:
        return {"ok": False, "reason": "settings_changed", "cert": cert}

    created = cert.get("created_ts_utc")
    max_age_days = int(cfg.get("execution", {}).get("pretrade_check_max_age_days", 7) or 7)

    age_days = None
    try:
        dt = datetime.fromisoformat(str(created))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        age_days = (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0
        if age_days > max_age_days:
            return {"ok": False, "reason": "certificate_expired", "age_days": age_days, "cert": cert}
    except Exception:
        pass

    if not bool(cert.get("pass", False)):
        return {"ok": False, "reason": "certificate_failed", "age_days": age_days, "cert": cert}

    if require_reviewed and (not bool(cert.get("reviewed", False))):
        return {"ok": False, "reason": "not_acknowledged", "age_days": age_days, "cert": cert}

    return {"ok": True, "reason": "ok", "age_days": age_days, "cert": cert}


def acknowledge(workspace: Path, reviewer: str = "human") -> Path:
    cert = load_certificate(workspace)
    if not cert:
        raise RuntimeError("No portfolio_safety_certificate.json to acknowledge.")
    cert["reviewed"] = True
    cert["reviewed_by"] = reviewer
    cert["reviewed_ts_utc"] = datetime.now(timezone.utc).isoformat()
    return write_certificate(workspace, cert)
PY

# -------------------------
# 5) Equity ledger for drawdown gating without relying on broker history APIs
# -------------------------
cat > src/mfp/paper/equity_ledger.py <<'PY'
from __future__ import annotations

import csv
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

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
PY

# -------------------------
# 6) Strategy registry + portfolio runner
# -------------------------
cat > src/mfp/portfolio/registry.py <<'PY'
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from mfp.config.normalize import normalize_config
from mfp.data.universe import get_universe


@dataclass(frozen=True)
class StrategySpec:
    id: str
    label: str
    enabled: bool
    allocation_pct: float
    engine: str
    universe: str
    symbols: List[str]
    proxy: str
    max_symbols: int
    overrides: Dict[str, Any]


def load_strategy_specs(cfg_raw: Dict[str, Any]) -> List[StrategySpec]:
    cfg = normalize_config(cfg_raw)
    sdict = cfg.get("portfolio", {}).get("strategies", {})
    out: List[StrategySpec] = []

    for sid, spec in sdict.items():
        if not isinstance(spec, dict):
            continue
        out.append(
            StrategySpec(
                id=str(sid),
                label=str(spec.get("label", sid)),
                enabled=bool(spec.get("enabled", True)),
                allocation_pct=float(spec.get("allocation_pct", 0.0)),
                engine=str(spec.get("engine", "pulse_mr")),
                universe=str(spec.get("universe", "") or ""),
                symbols=[str(x).upper() for x in (spec.get("symbols") or [])],
                proxy=str(spec.get("proxy", "") or ""),
                max_symbols=int(spec.get("max_symbols", 60)),
                overrides=dict(spec.get("overrides") or {}),
            )
        )

    return out


def resolve_symbols(spec: StrategySpec) -> List[str]:
    if spec.symbols:
        return list(spec.symbols)
    if spec.universe:
        syms = get_universe(spec.universe)
        return syms[: max(1, spec.max_symbols)]
    return []
PY

cat > src/mfp/portfolio/portfolio_cycle.py <<'PY'
from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from mfp.config.normalize import normalize_config
from mfp.config.runtime import load_config
from mfp.paper.equity_ledger import append_equity, compute_drawdown_gate, now_utc_iso
from mfp.portfolio.registry import StrategySpec, load_strategy_specs, resolve_symbols


def _wjson(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _safe_float(x: Any) -> Optional[float]:
    try:
        v = float(x)
        return v
    except Exception:
        return None


def _get_account_equity() -> Optional[float]:
    """
    Try to pull paper account equity from Alpaca.
    If not configured, return None.
    """
    try:
        from mfp.paper.alpaca_io import alpaca_trading_client, get_account_snapshot

        tc = alpaca_trading_client()
        acct = get_account_snapshot(tc)
        eq = _safe_float(acct.get("equity"))
        return eq
    except Exception:
        return None


def run_portfolio_cycle(
    workspace: Path,
    out_dir: Path,
    dry_run: bool,
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)

    cfg = normalize_config(load_config(workspace))
    specs = [s for s in load_strategy_specs(cfg) if s.enabled and s.allocation_pct > 0]

    virtual_eq = float(cfg.get("portfolio", {}).get("paper_equity_usd", 100000.0))
    use_virtual = bool(cfg.get("portfolio", {}).get("use_virtual_equity", True))

    actual_eq = _get_account_equity()
    scale = 1.0
    base_eq = virtual_eq

    if actual_eq and actual_eq > 0 and use_virtual:
        scale = min(1.0, actual_eq / max(1e-9, virtual_eq))
        base_eq = virtual_eq * scale
    elif actual_eq and actual_eq > 0 and (not use_virtual):
        base_eq = actual_eq

    # ledger update for drawdown gating
    ts = now_utc_iso()
    append_equity(workspace, ts_utc=ts, equity=float(actual_eq or base_eq))

    dd_limits = cfg.get("objectives", {}).get("drawdown_limits", {"1D": 0.01, "5D": 0.02, "20D": 0.03})
    gate = compute_drawdown_gate(workspace, dd_limits=dd_limits)
    allow_new_entries = bool(gate.get("ok", True))

    summary: Dict[str, Any] = {
        "ts_utc": ts,
        "dry_run": dry_run,
        "virtual_equity": virtual_eq,
        "actual_equity": actual_eq,
        "scale": scale,
        "sizing_equity": base_eq,
        "drawdown_gate": gate,
        "strategies": [],
    }

    from mfp.paper.paper_cycle import paper_cycle  # local import to keep CLI responsive

    sig = inspect.signature(paper_cycle)

    for s in specs:
        syms = resolve_symbols(s)
        sd = out_dir / s.id
        sd.mkdir(parents=True, exist_ok=True)

        strat_eq = base_eq * float(s.allocation_pct)

        # Build kwargs defensively (works even if paper_cycle signature evolves)
        kwargs: Dict[str, Any] = {"out_dir": sd, "symbols": syms, "dry_run": dry_run}

        if "strategy_id" in sig.parameters:
            kwargs["strategy_id"] = s.id
        if "engine" in sig.parameters:
            kwargs["engine"] = s.engine
        if "equity_override" in sig.parameters:
            kwargs["equity_override"] = strat_eq
        if "proxy_ticker" in sig.parameters and s.proxy:
            kwargs["proxy_ticker"] = s.proxy
        if "allow_new_entries" in sig.parameters:
            kwargs["allow_new_entries"] = allow_new_entries

        # Pass numeric overrides if paper_cycle supports them
        for k in (
            "risk_per_trade_pct",
            "max_open_risk_pct",
            "max_positions",
            "stop_atr_mult",
            "rsi_buy_below",
            "rsi_exit_above",
            "trend_sma",
            "fast_sma",
            "min_price",
            "min_adv20",
            "max_atr_pct",
        ):
            if k in sig.parameters and k in s.overrides:
                kwargs[k] = s.overrides[k]

        r = paper_cycle(**kwargs)
        summary["strategies"].append(
            {
                "id": s.id,
                "label": s.label,
                "allocation_pct": s.allocation_pct,
                "engine": s.engine,
                "proxy": s.proxy,
                "symbols_count": len(syms),
                "result": r,
                "strategy_equity": strat_eq,
            }
        )

    _wjson(out_dir / "portfolio_summary.json", summary)

    # Aggregate all order intents into one file for UI convenience
    orders: List[Dict[str, Any]] = []
    for it in summary["strategies"]:
        sid = it["id"]
        sd = out_dir / sid
        p = sd / "orderIntents.json"
        if p.exists():
            try:
                arr = json.loads(p.read_text(encoding="utf-8"))
                if isinstance(arr, list):
                    for o in arr:
                        if isinstance(o, dict):
                            o2 = dict(o)
                            o2.setdefault("strategy_id", sid)
                            orders.append(o2)
            except Exception:
                pass
    _wjson(out_dir / "portfolio_orders.json", orders)

    return summary
PY

# -------------------------
# 7) Switching agent (suggest allocations based on regime)
# -------------------------
cat > src/mfp/agents/switching.py <<'PY'
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
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
PY

# -------------------------
# 8) Critic agent (continuous critique; uses ledger + latest portfolio run)
# -------------------------
cat > src/mfp/agents/critic.py <<'PY'
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from mfp.config.normalize import normalize_config
from mfp.paper.equity_ledger import compute_drawdown_gate, load_equity_series_daily


def _wjson(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _wtext(p: Path, txt: str) -> None:
    p.write_text(txt, encoding="utf-8", newline="\n")


def _latest_portfolio_run(workspace: Path) -> Optional[Path]:
    reports = workspace / "reports"
    if not reports.exists():
        return None
    runs = sorted([p for p in reports.glob("*_portfolio") if p.is_dir()], key=lambda x: x.name, reverse=True)
    return runs[0] if runs else None


def run_critic(workspace: Path, cfg_raw: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    cfg = normalize_config(cfg_raw)
    out_dir.mkdir(parents=True, exist_ok=True)

    objectives = cfg.get("objectives", {})
    dd_limits = objectives.get("drawdown_limits", {"1D": 0.01, "5D": 0.02, "20D": 0.03})
    gate = compute_drawdown_gate(workspace, dd_limits=dd_limits)

    issues: List[Dict[str, Any]] = []
    if not bool(gate.get("ok", True)):
        issues.append({"severity": "HIGH", "topic": "Drawdown", "message": "Drawdown gate breached. Pause new entries."})

    # Check allocation sanity
    strat = cfg.get("portfolio", {}).get("strategies", {})
    alloc_sum = 0.0
    for sid, spec in strat.items():
        if isinstance(spec, dict) and bool(spec.get("enabled", True)):
            alloc_sum += float(spec.get("allocation_pct", 0.0))
    if abs(alloc_sum - 1.0) > 1e-6:
        issues.append(
            {
                "severity": "HIGH",
                "topic": "Portfolio settings",
                "message": f"Allocations do not sum to 100% (sum={alloc_sum:.4f}). Guardrails should block Apply.",
            }
        )

    # Latest run checks
    last = _latest_portfolio_run(workspace)
    if last:
        ps = last / "portfolio_summary.json"
        if ps.exists():
            try:
                summ = json.loads(ps.read_text(encoding="utf-8"))
                if summ.get("drawdown_gate", {}).get("ok") is False:
                    issues.append(
                        {
                            "severity": "HIGH",
                            "topic": "Portfolio gate",
                            "message": "Latest portfolio run was gated due to drawdown. Consider switching to CASH mode.",
                        }
                    )
            except Exception:
                pass

    s = load_equity_series_daily(workspace)
    eq_note = "Equity ledger is empty." if len(s) == 0 else f"Equity points (daily): {len(s)}"

    report = {
        "ts_utc": datetime.now(timezone.utc).isoformat(),
        "ok": len([i for i in issues if i["severity"] == "HIGH"]) == 0,
        "issues": issues,
        "drawdown_gate": gate,
        "notes": [eq_note],
        "latest_portfolio_run": str(last) if last else None,
    }

    _wjson(out_dir / "critic.json", report)

    lines = []
    lines.append("# Critic Report\n")
    lines.append(f"- time (UTC): `{report['ts_utc']}`")
    lines.append(f"- high_severity_issues: `{len([i for i in issues if i['severity']=='HIGH'])}`")
    lines.append("")
    lines.append("## Drawdown gate")
    lines.append(f"- ok: **{gate.get('ok')}**")
    lines.append(f"- reason: `{gate.get('reason')}`")
    if isinstance(gate.get("windows"), dict):
        for k, v in gate["windows"].items():
            lines.append(f"- {k}: worst={v.get('worst'):.4f} limit={v.get('limit'):.4f} pass={v.get('pass')}")
    lines.append("")
    lines.append("## Issues")
    if not issues:
        lines.append("- none ✅")
    else:
        for i in issues:
            lines.append(f"- {i['severity']}: {i['topic']} — {i['message']}")

    _wtext(out_dir / "critic.md", "\n".join(lines) + "\n")
    return report
PY

# -------------------------
# 9) Guardrails: block unsafe portfolio changes
# -------------------------
cat > src/mfp/governance/guardrails.py <<'PY'
from __future__ import annotations

from typing import Any, Dict, List

from mfp.config.normalize import normalize_config


def check_guardrails(cfg_raw: Dict[str, Any]) -> Dict[str, Any]:
    cfg = normalize_config(cfg_raw)

    violations: List[Dict[str, Any]] = []
    warnings: List[Dict[str, Any]] = []

    # Allocation checks
    strat = cfg.get("portfolio", {}).get("strategies", {})
    alloc_sum = 0.0
    for sid, spec in strat.items():
        if not isinstance(spec, dict):
            continue
        if not bool(spec.get("enabled", True)):
            continue
        w = float(spec.get("allocation_pct", 0.0))
        if w < -1e-12 or w > 1.0 + 1e-12:
            violations.append({"message": f"{sid}: allocation_pct must be between 0 and 1 (got {w})."})
        alloc_sum += w

    if abs(alloc_sum - 1.0) > 1e-6:
        violations.append({"message": f"Strategy allocations must sum to 1.0 (100%). Current sum={alloc_sum:.6f}."})

    # Objectives sanity
    obj = cfg.get("objectives", {})
    target = float(obj.get("target_annual_cagr", 0.20))
    if target <= -0.5 or target >= 2.0:
        warnings.append({"message": f"Target CAGR looks unusual: {target:.3f}. Verify objective settings."})

    dd_limits = obj.get("drawdown_limits", {"1D": 0.01, "5D": 0.02, "20D": 0.03})
    for k, v in dd_limits.items():
        lim = float(v)
        if lim <= 0 or lim > 0.5:
            violations.append({"message": f"Drawdown limit {k} must be between 0 and 0.50. Got {lim}."})

    return {"ok": len(violations) == 0, "violations": violations, "warnings": warnings}
PY

# -------------------------
# 10) Paper cycle upgrade: strategy_id, engine, equity_override, allow_new_entries
# -------------------------
cat > src/mfp/paper/paper_cycle.py <<'PY'
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
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
    from mfp.paper.alpaca_io import alpaca_trading_client, get_account_snapshot, get_open_orders_snapshot, get_positions_snapshot

    tc = alpaca_trading_client()
    acct = get_account_snapshot(tc)
    pos = get_positions_snapshot(tc)
    orders = get_open_orders_snapshot(tc)

    _wjson(out_dir / "paper_account.json", acct)
    _wjson(out_dir / "paper_positions.json", pos)
    _wjson(out_dir / "paper_open_orders.json", orders)

    return {"account": acct, "positions": pos, "open_orders": orders}


def _compute_features(df: pd.DataFrame, trend_sma: int, fast_sma: int, rsi_len: int, atr_len: int) -> pd.DataFrame:
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
    rsi_exit_above = float(rsi_exit_above if rsi_exit_above is not None else base_sig.get("rsi_exit_above", 70.0))

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
        bars_px = fetch_daily_bars(dc, [proxy_ticker], lookback_days=trend_sma + 30, feed="iex").get(proxy_ticker)
        if bars_px is not None and not bars_px.empty:
            fx = _compute_features(bars_px, trend_sma, fast_sma, rsi_len, atr_len)
            last = fx.iloc[-1]
            c = _safe_float(last.get("Close"))
            st = _safe_float(last.get("sma_trend"))
            if st > 0 and c > 0 and c <= st:
                regime = {"ok": False, "reason": "proxy_below_trend", "proxy": proxy_ticker, "close": c, "sma": st}
        else:
            regime = {"ok": True, "reason": "proxy_data_missing", "proxy": proxy_ticker}

    _wjson(out_dir / "regime.json", regime)
    _wjson(out_dir / "paper_gate.json", {"drawdown_gate": dd_gate, "allow_new_entries": allow_new_entries, "regime": regime})

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
                exits.append({"symbol": sym, "qty": abs(pos_qty[sym]), "reason": "trend_off", "close": close, "sma200": st})
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


def paper_reconcile(out_dir: Path, symbols: List[str], place_stops: bool = False, stop_atr_mult: float = 1.5) -> Dict[str, Any]:
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
    _wjson(out_dir / "paper_stop_actions.json", {"note": "stop placement not implemented in v3 reconcile", "place_stops": place_stops})

    return {"stop_actions": {"placed": [], "skipped": []}}
PY

# -------------------------
# 11) Patch universe selection in backtest engine (sp400 -> get_universe)
# -------------------------
python - <<'PY'
from __future__ import annotations

from pathlib import Path

p = Path("src/mfp/backtest/engine.py")
if not p.exists():
    print("[skip] src/mfp/backtest/engine.py not found")
    raise SystemExit(0)

txt = p.read_text(encoding="utf-8")

txt2 = txt

if "from mfp.data.universe_sp400 import get_universe_sp400" in txt2:
    txt2 = txt2.replace(
        "from mfp.data.universe_sp400 import get_universe_sp400",
        "from mfp.data.universe import get_universe",
    )

# Replace simple calls if present
txt2 = txt2.replace("get_universe_sp400()", "get_universe(universe_name)")

if txt2 != txt:
    p.write_text(txt2, encoding="utf-8", newline="\n")
    print("[ok] Patched backtest engine to use get_universe(universe_name)")
else:
    print("[info] No changes made to backtest engine (patterns not found).")
PY

# -------------------------
# 12) Update universe resolver to allow "custom:" universe names
# -------------------------
cat > src/mfp/data/universe.py <<'PY'
from __future__ import annotations

from typing import List

from mfp.data.universe_sp400 import get_universe_sp400
from mfp.data.universe_sp500 import get_universe_sp500


def _parse_custom(name: str) -> List[str]:
    # formats:
    #   custom:IJH,SPY
    #   symbols:IJH,SPY
    #   etf:SPY,MDY
    _, rest = name.split(":", 1)
    parts = [p.strip().upper().replace(".", "-") for p in rest.split(",") if p.strip()]
    # de-dupe, keep order
    out: List[str] = []
    seen = set()
    for s in parts:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def get_universe(name: str) -> List[str]:
    """
    name:
      - sp400 / mid / midcap
      - sp500 / large / largecap
      - both / mid+large
      - custom:IJH,SPY  (explicit list)
    """
    n = (name or "sp400").strip()
    nlow = n.lower()

    if ":" in n and nlow.split(":", 1)[0] in {"custom", "symbols", "etf"}:
        return _parse_custom(n)

    if nlow in {"sp400", "mid", "midcap"}:
        return get_universe_sp400()

    if nlow in {"sp500", "large", "largecap"}:
        return get_universe_sp500()

    if nlow in {"both", "mid+large", "large+mid"}:
        # stable order: SP500 first, then add SP400 extras
        sp500 = get_universe_sp500()
        s = set(sp500)
        merged = list(sp500)
        merged.extend([x for x in get_universe_sp400() if x not in s])
        return merged

    raise ValueError(f"Unknown universe name: {name!r}. Use sp400, sp500, both, or custom:AAA,BBB.")
PY

# -------------------------
# 13) OpsBot: add portfolio commands
# -------------------------
cat > src/mfp/ui/github_opsbot.py <<'PY'
from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from mfp.config.normalize import normalize_config


@dataclass
class CmdResult:
    ok: bool
    summary_md: str
    artifacts_dir: Path | None


def _parse_kv(parts: list[str]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for p in parts:
        if "=" in p:
            k, v = p.split("=", 1)
            out[k.strip()] = v.strip()
    return out


def _wjson(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def run_command(command_line: str, workspace: Path) -> CmdResult:
    parts = shlex.split(command_line.strip())
    if not parts or parts[0] not in ("/mfp", "mfp"):
        return CmdResult(False, "❌ Command must start with `/mfp`.", None)

    if parts[0] == "mfp":
        parts[0] = "/mfp"

    cmd = parts[1] if len(parts) > 1 else ""
    kv = _parse_kv(parts[2:])

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = workspace / "reports" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    from mfp.audit.evidence import create_evidence_zip
    from mfp.audit.manifest import write_manifest
    from mfp.audit.runlog import append_runlog
    from mfp.config.runtime import load_config
    from mfp.governance.portfolio_safety import acknowledge, validate_certificate, write_certificate
    from mfp.objectives.evaluator import evaluate_objectives
    from mfp.portfolio.portfolio_cycle import run_portfolio_cycle

    cfg = normalize_config(load_config(workspace))

    # ---------------- status ----------------
    if cmd == "status":
        return CmdResult(True, f"✅ workspace: `{workspace}`\n", out_dir)

    # ---------------- portfolio safety check ----------------
    if cmd == "portfolio-safety-check":
        # Runs backtest sweep for each enabled strategy (pulse_mr strategies only, ETFs included via custom universe).
        # For v3 we keep this as a transparent “evidence generator” rather than a guarantee.
        start = kv.get("start", cfg.get("pretrade_check", {}).get("start", "2011-01-01"))
        end = kv.get("end", cfg.get("pretrade_check", {}).get("end") or datetime.now(timezone.utc).strftime("%Y-%m-%d"))

        strategies = cfg.get("portfolio", {}).get("strategies", {})
        enabled = [k for k, v in strategies.items() if isinstance(v, dict) and bool(v.get("enabled", True))]
        if not enabled:
            return CmdResult(False, "❌ No enabled strategies in config.", out_dir)

        # Use existing backtest engine for pulse_mr; trend_hold is excluded from safety check by default.
        from mfp.backtest.engine import run_backtest

        results: Dict[str, Any] = {"start": start, "end": end, "strategies": {}}
        overall_pass = True
        reasons: List[str] = []

        for sid in enabled:
            spec = strategies[sid]
            engine = str(spec.get("engine", "pulse_mr"))
            universe = str(spec.get("universe", "") or "")
            symbols = spec.get("symbols") or []
            if engine == "trend_hold":
                # v3: trend engine safety check is informational only (still paper-testable)
                results["strategies"][sid] = {"skipped": True, "reason": "trend_hold_not_in_safety_check_v3"}
                continue

            if symbols:
                universe_name = "custom:" + ",".join([str(x).upper() for x in symbols])
            else:
                universe_name = universe or "sp400"

            tf_rows = []
            for tf in ["1d", "1wk", "1mo"]:
                tf_dir = out_dir / sid / tf
                tf_dir.mkdir(parents=True, exist_ok=True)

                bt = run_backtest(
                    out_dir=tf_dir,
                    timeframe=tf,
                    start=start,
                    end=end,
                    universe_name=universe_name,
                    strategy_name="midcap_pulse_v1",
                    max_symbols=int(spec.get("max_symbols", 60)),
                )

                ev = evaluate_objectives(bt["equity"], cfg.get("objectives", {}))
                tf_rows.append({"timeframe": tf, "metrics": bt["metrics"], "objectives_eval": ev})
                if not bool(ev.get("pass", False)):
                    overall_pass = False
                    reasons.extend([f"{sid}:{r}" for r in ev.get("reasons", [])])

            results["strategies"][sid] = {"universe": universe_name, "rows": tf_rows}

        cert = {
            "kind": "portfolio_safety",
            "created_ts_utc": datetime.now(timezone.utc).isoformat(),
            "config_hash": __import__("mfp.config.runtime").config.runtime.config_hash(cfg),
            "pass": bool(overall_pass),
            "reviewed": False,
            "reasons": sorted(set(reasons)),
            "results_dir": str(out_dir),
            "results": results,
        }
        write_certificate(workspace, cert)
        _wjson(out_dir / "portfolio_safety_results.json", cert)

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv})
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True, "pass": cert["pass"]})

        lines = []
        lines.append("✅ Portfolio Safety Check complete\n")
        lines.append(f"- pass: **{cert['pass']}**")
        lines.append(f"- start: `{start}`")
        lines.append(f"- end: `{end}`")
        if cert["reasons"]:
            lines.append("\n## Reasons")
            for r in cert["reasons"]:
                lines.append(f"- {r}")
        lines.append(f"\nArtifacts: `{out_dir}`")
        lines.append(f"Manifest: `{manifest_path.name}`")
        return CmdResult(True, "\n".join(lines) + "\n", out_dir)

    if cmd == "portfolio-safety-ack":
        try:
            p = acknowledge(workspace, reviewer="human")
            _wjson(out_dir / "portfolio_safety_ack.json", {"ok": True, "path": str(p)})
            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
            append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True})
            return CmdResult(True, f"✅ Acknowledged portfolio safety check.\nManifest: `{manifest_path.name}`\n", out_dir)
        except Exception as e:
            return CmdResult(False, f"❌ Ack failed: {type(e).__name__}: {e}\n", out_dir)

    # ---------------- portfolio preview/place ----------------
    if cmd in {"portfolio-preview", "portfolio-place"}:
        dry_run = cmd == "portfolio-preview"
        require = bool(cfg.get("portfolio", {}).get("require_portfolio_safety_check", True))

        # block placing unless acknowledged
        if (not dry_run) and require:
            v = validate_certificate(workspace, cfg, require_reviewed=True)
            if not bool(v.get("ok", False)):
                return CmdResult(
                    False,
                    f"❌ BLOCKED by Portfolio Safety Check: `{v.get('reason')}`\n"
                    "Run safety check + acknowledge before placing.\n",
                    out_dir,
                )

        port_dir = out_dir / "portfolio"
        r = run_portfolio_cycle(workspace=workspace, out_dir=port_dir, dry_run=dry_run)

        _wjson(out_dir / "portfolio_run.json", r)

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv})

        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True, "dry_run": dry_run})

        return CmdResult(
            True,
            "✅ Portfolio run complete\n\n"
            f"- preview_only: `{dry_run}`\n"
            f"- drawdown_gate_ok: `{r.get('drawdown_gate', {}).get('ok')}`\n"
            f"- artifacts: `{out_dir}`\n"
            f"- manifest: `{manifest_path.name}`\n",
            out_dir,
        )

    # ---------------- switching + critic ----------------
    if cmd == "switch-suggest":
        from mfp.agents.switching import suggest_allocations, write_suggestion

        sugg = suggest_allocations(cfg)
        write_suggestion(out_dir, sugg)

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True})

        return CmdResult(True, f"✅ Switch suggestion saved.\nManifest: `{manifest_path.name}`\n", out_dir)

    if cmd == "critic-run":
        from mfp.agents.critic import run_critic

        r = run_critic(workspace, cfg, out_dir=out_dir)

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": bool(r.get('ok', False))})

        return CmdResult(True, f"✅ Critic report written.\nManifest: `{manifest_path.name}`\n", out_dir)

    return CmdResult(False, f"❌ Unknown command: `{cmd}`\n", out_dir)
PY

# -------------------------
# 14) UI: Portfolio-first daily flow + objectives/drawdown dials + why panel
# -------------------------
cat > src/mfp/ui/dashboard_app.py <<'PY'
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st

from mfp.audit.integrity import verify_manifest
from mfp.config.normalize import normalize_config
from mfp.config.runtime import config_hash, load_config
from mfp.governance.guardrails import check_guardrails
from mfp.governance.portfolio_safety import load_certificate, validate_certificate
from mfp.governance.proposals import (
    apply_proposal,
    approve_proposal,
    create_proposal_from_dials,
    list_proposals,
    load_proposal,
    read_changelog,
    reject_proposal,
)
from mfp.objectives.targets import expected_cum_return, window_days
from mfp.ui.github_opsbot import run_command


def workspace_root() -> Path:
    return Path(__file__).resolve().parents[3]


def open_folder(p: Path) -> None:
    try:
        os.startfile(str(p))  # Windows
    except Exception:
        pass


def _short(h: str) -> str:
    return h[:10]


def _load_json(p: Path) -> Optional[Any]:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


st.set_page_config(page_title="Midcap Fortress — Portfolio Control Center", layout="wide")

workspace = workspace_root()
cfg = normalize_config(load_config(workspace))
settings_id = config_hash(cfg)

with st.sidebar:
    st.header("Mode")
    advanced = st.toggle("Advanced view", value=False, key="mode_adv")
    st.markdown("---")
    st.write("**Safety Check** = backtest evidence + your acknowledgement.")
    st.write("**Goal** = long-run annual growth target; short windows are informational.")
    st.write("**Max drop** = rolling drawdown guardrails (daily-based).")

st.title("Midcap Fortress — Portfolio Control Center")
st.caption("Portfolio-first. Every action produces evidence files you can inspect and verify.")

tabs = st.tabs(["Daily Flow", "Portfolio Settings", "Backtests", "Activity & Files", "Governance", "Prototype"])

# ---------------- Daily Flow ----------------
with tabs[0]:
    st.subheader("Daily Flow (one screen)")
    st.write(f"Settings ID: `{_short(settings_id)}`")

    cert = load_certificate(workspace)
    val = validate_certificate(workspace, cfg, require_reviewed=False)

    st.markdown("## Step 1 — Portfolio Safety Check")
    c1, c2 = st.columns([1, 1])
    with c1:
        if st.button("Run Safety Check now", key="daily_run_safety"):
            start = cfg.get("pretrade_check", {}).get("start", "2011-01-01")
            end = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            res = run_command(f"/mfp portfolio-safety-check start={start} end={end}", workspace=workspace)
            st.markdown(res.summary_md)
            st.rerun()
    with c2:
        if val["ok"]:
            st.success("Safety Check exists ✅")
        else:
            st.warning("Safety Check missing/invalid ⚠️")
        st.caption(f"Reason: {val['reason']}")

    st.markdown("## Step 2 — Acknowledge you reviewed results")
    if st.button("I reviewed the Safety Check (acknowledge)", key="daily_ack"):
        res = run_command("/mfp portfolio-safety-ack", workspace=workspace)
        st.markdown(res.summary_md)
        st.rerun()

    st.markdown("---")
    st.markdown("## Step 3 — Preview portfolio trades (no orders)")
    if st.button("Preview trades", key="daily_preview"):
        res = run_command("/mfp portfolio-preview", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_portfolio_run"] = str(res.artifacts_dir)
        st.rerun()

    last_run = Path(st.session_state["last_portfolio_run"]) if "last_portfolio_run" in st.session_state else None
    if last_run and last_run.exists():
        orders_path = last_run / "portfolio" / "portfolio_orders.json"
        orders = _load_json(orders_path) or []
        if isinstance(orders, list) and orders:
            st.dataframe(pd.DataFrame(orders), use_container_width=True)
        else:
            st.info("No orders suggested in latest preview.")
        if st.button("Open preview folder", key="daily_open_preview"):
            open_folder(last_run)

    st.markdown("---")
    st.markdown("## Step 4 — Place paper orders")
    val_place = validate_certificate(workspace, cfg, require_reviewed=True)
    if not val_place["ok"]:
        st.warning(f"Blocked until Safety Check is acknowledged. Reason: {val_place['reason']}")

    if st.button("Place paper orders", disabled=not val_place["ok"], key="daily_place"):
        res = run_command("/mfp portfolio-place", workspace=workspace)
        st.markdown(res.summary_md)
        if res.artifacts_dir:
            st.session_state["last_portfolio_exec"] = str(res.artifacts_dir)
        st.rerun()

    st.markdown("---")
    st.markdown("## Step 5 — Run Critic + Switch Suggestion")
    c3, c4 = st.columns(2)
    with c3:
        if st.button("Run critic now", key="daily_critic"):
            res = run_command("/mfp critic-run", workspace=workspace)
            st.markdown(res.summary_md)
    with c4:
        if st.button("Suggest switching", key="daily_switch"):
            res = run_command("/mfp switch-suggest", workspace=workspace)
            st.markdown(res.summary_md)

# ---------------- Portfolio Settings ----------------
with tabs[1]:
    st.subheader("Portfolio Settings (dials)")
    draft = json.loads(json.dumps(cfg))  # deep copy

    st.markdown("### Goal (plain English)")
    c1, c2, c3 = st.columns(3)
    with c1:
        draft["objectives"]["target_annual_cagr"] = st.number_input(
            "Target growth per year (e.g., 0.20 = 20%)",
            0.0,
            1.0,
            float(draft["objectives"]["target_annual_cagr"]),
            0.01,
            key="obj_target",
        )
    with c2:
        draft["objectives"]["goal_horizon_years"] = st.number_input(
            "Goal horizon (years)",
            1,
            10,
            int(draft["objectives"]["goal_horizon_years"]),
            1,
            key="obj_horizon",
        )
    with c3:
        draft["objectives"]["acceptance_factor"] = st.number_input(
            "Acceptance factor (soften thresholds)",
            0.1,
            1.0,
            float(draft["objectives"]["acceptance_factor"]),
            0.05,
            key="obj_accept",
        )

    st.markdown("### Max drop (rolling guardrails)")
    d1, d2, d3 = st.columns(3)
    with d1:
        draft["objectives"]["drawdown_limits"]["1D"] = st.number_input("Max 1‑day drop", 0.0, 0.2, float(draft["objectives"]["drawdown_limits"]["1D"]), 0.001, key="dd1")
    with d2:
        draft["objectives"]["drawdown_limits"]["5D"] = st.number_input("Max 1‑week drop (5D)", 0.0, 0.3, float(draft["objectives"]["drawdown_limits"]["5D"]), 0.001, key="dd5")
    with d3:
        draft["objectives"]["drawdown_limits"]["20D"] = st.number_input("Max 1‑month drop (20D)", 0.0, 0.5, float(draft["objectives"]["drawdown_limits"]["20D"]), 0.001, key="dd20")

    st.markdown("### Portfolio size + allocations")
    draft["portfolio"]["paper_equity_usd"] = st.number_input(
        "Virtual paper portfolio size (USD)",
        1000.0,
        10000000.0,
        float(draft["portfolio"]["paper_equity_usd"]),
        1000.0,
        key="port_eq",
    )

    # allocations
    strat = draft["portfolio"]["strategies"]
    st.caption("Adjust allocations. They must sum to 100% to pass guardrails.")
    alloc_total = 0.0
    for sid, spec in strat.items():
        if not spec.get("enabled", True):
            continue
        spec["allocation_pct"] = st.slider(
            f"{sid} — {spec.get('label','')}",
            0.0,
            1.0,
            float(spec.get("allocation_pct", 0.0)),
            0.01,
            key=f"alloc_{sid}",
        )
        alloc_total += float(spec["allocation_pct"])
    st.info(f"Allocation sum: {alloc_total:.4f} (must be 1.0000)")

    st.markdown("### What your goal implies in each window")
    target = float(draft["objectives"]["target_annual_cagr"])
    acceptance = float(draft["objectives"]["acceptance_factor"])
    rows = []
    for w in draft["objectives"]["return_windows"]:
        days = window_days(w)
        exp = expected_cum_return(target, days)
        thr = exp * acceptance
        rows.append({"window": w, "days": days, "expected_cum": exp, "threshold_cum": thr})
    st.dataframe(pd.DataFrame(rows), use_container_width=True)

    st.markdown("---")
    guard = check_guardrails(draft)
    if guard["ok"]:
        st.success("Guardrails: PASS ✅")
    else:
        st.error("Guardrails: FAIL ❌")
        for v in guard["violations"]:
            st.write(f"- ❌ {v['message']}")

    if guard["warnings"]:
        st.warning("Warnings:")
        for w in guard["warnings"]:
            st.write(f"- ⚠️ {w['message']}")

    if st.button("Create Change Request", key="create_proposal"):
        pr = create_proposal_from_dials(workspace, proposed_cfg=draft, created_by="human")
        st.success(f"Created proposal: {pr['proposal_id']}")
        st.rerun()

# ---------------- Backtests ----------------
with tabs[2]:
    st.subheader("Backtests (per strategy)")
    st.caption("Run backtests before you place trades.")

    # choose strategy
    strat = cfg["portfolio"]["strategies"]
    sid = st.selectbox("Strategy", list(strat.keys()), key="bt_sid")
    spec = strat[sid]
    universe = "custom:" + ",".join(spec.get("symbols", [])) if spec.get("symbols") else spec.get("universe", "sp400")

    start = st.text_input("Start (YYYY-MM-DD)", cfg.get("pretrade_check", {}).get("start", "2011-01-01"), key="bt_start")
    end = st.text_input("End (YYYY-MM-DD)", datetime.now(timezone.utc).strftime("%Y-%m-%d"), key="bt_end")

    if st.button("Run strategy safety sweep (1d/1wk/1mo)", key="bt_run"):
        res = run_command(f"/mfp portfolio-safety-check start={start} end={end}", workspace=workspace)
        st.markdown(res.summary_md)

    st.info(f"Universe for {sid}: `{universe}` (based on settings)")

# ---------------- Activity & Files ----------------
with tabs[3]:
    st.subheader("Activity & Files")
    reports = workspace / "reports"
    reports.mkdir(parents=True, exist_ok=True)
    runs = sorted([p for p in reports.glob("*") if p.is_dir()], key=lambda x: x.name, reverse=True)

    if not runs:
        st.info("No runs yet.")
    else:
        run_name = st.selectbox("Run folder", [r.name for r in runs], key="act_run")
        run_dir = reports / run_name

        if st.button("Open folder", key="act_open"):
            open_folder(run_dir)

        integ = verify_manifest(run_dir)
        if integ.get("ok"):
            st.success("Integrity: PASS ✅")
        else:
            st.error("Integrity: FAIL ❌")
            if advanced:
                st.json(integ)

        files = sorted([p for p in run_dir.rglob("*") if p.is_file()], key=lambda x: str(x))
        if files:
            rels = [str(p.relative_to(run_dir)) for p in files]
            fsel = st.selectbox("Open file", rels, key="act_file")
            if fsel:
                fp = run_dir / fsel
                if fp.suffix.lower() == ".json":
                    st.json(_load_json(fp))
                else:
                    st.code(fp.read_text(encoding="utf-8", errors="ignore")[:8000])
        else:
            st.info("No files in this run.")

# ---------------- Governance ----------------
with tabs[4]:
    st.subheader("Governance (Approve → Apply)")
    proposals = list_proposals(workspace)
    if not proposals:
        st.info("No proposals yet. Create one from Portfolio Settings.")
    else:
        st.dataframe(pd.DataFrame([{
            "proposal_id": p["proposal_id"],
            "status": p["status"],
            "from": _short(p["base_config_hash"]),
            "to": _short(p["proposed_config_hash"]),
            "safety_ok": bool(p.get("guardrails", {}).get("ok", False)),
        } for p in proposals]), use_container_width=True)

        sel = st.selectbox("Select proposal", [p["proposal_id"] for p in proposals], key="gov_sel")
        pr = load_proposal(workspace, sel)

        st.markdown("### Changes")
        st.dataframe(pd.DataFrame(pr.get("changes", [])), use_container_width=True)

        c1, c2, c3 = st.columns(3)
        with c1:
            if st.button("Approve", key="gov_approve"):
                approve_proposal(workspace, sel, approved_by="human")
                st.success("Approved.")
                st.rerun()
        with c2:
            can_apply = pr["status"] == "APPROVED" and bool(pr.get("guardrails", {}).get("ok", False))
            if st.button("Apply", disabled=not can_apply, key="gov_apply"):
                apply_proposal(workspace, sel, applied_by="human")
                st.success("Applied. Re-run Safety Check before placing orders.")
                st.rerun()
        with c3:
            if st.button("Reject", key="gov_reject"):
                reject_proposal(workspace, sel, rejected_by="human", reason="Rejected in UI")
                st.success("Rejected.")
                st.rerun()

    st.markdown("---")
    st.markdown("### Change history")
    hist = read_changelog(workspace, limit=200)
    if hist:
        st.dataframe(pd.DataFrame(hist), use_container_width=True)
    else:
        st.caption("No applied changes yet.")

# ---------------- Prototype ----------------
with tabs[5]:
    st.subheader("Prototype")
    proto = workspace / "prototype" / "fortress_trading_agent_prototype.html"
    if proto.exists():
        import streamlit.components.v1 as components

        components.html(proto.read_text(encoding="utf-8", errors="ignore"), height=900, scrolling=True)
    else:
        st.info("Prototype file not found. Put it at: prototype/fortress_trading_agent_prototype.html")
PY

# -------------------------
# 15) Tests (fast, offline)
# -------------------------
cat > tests/test_normalize_portfolio.py <<'PY'
from __future__ import annotations

from mfp.config.normalize import normalize_config


def test_normalize_has_portfolio_defaults() -> None:
    cfg = normalize_config({})
    assert "portfolio" in cfg
    assert "objectives" in cfg
    assert "strategies" in cfg["portfolio"]
    assert "drawdown_limits" in cfg["objectives"]
PY

cat > tests/test_objectives_eval.py <<'PY'
from __future__ import annotations

import pandas as pd

from mfp.objectives.evaluator import evaluate_objectives


def test_objectives_eval_smoke() -> None:
    # synthetic equity curve rising 1% per day for 300 days
    s = pd.Series([100.0 * (1.01**i) for i in range(300)])
    res = evaluate_objectives(s, {"target_annual_cagr": 0.20, "acceptance_factor": 0.5, "return_windows": ["1Y"]})
    assert "pass" in res
    assert "drawdown" in res
PY

echo "==> Upgrade Portfolio v3 done."
