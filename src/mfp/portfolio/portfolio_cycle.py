from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from mfp.config.normalize import normalize_config
from mfp.config.runtime import load_config
from mfp.paper.equity_ledger import append_equity, compute_drawdown_gate, now_utc_iso
from mfp.portfolio.registry import load_strategy_specs, resolve_symbols


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
