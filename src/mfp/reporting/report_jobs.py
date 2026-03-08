from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from mfp.audit.evidence import create_evidence_zip
from mfp.audit.manifest import write_manifest
from mfp.audit.runlog import append_runlog
from mfp.config.runtime import config_hash, load_config, snapshot_config
from mfp.paper.paper_cycle import paper_status


def _wjson(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _wtext(p: Path, s: str) -> None:
    p.write_text(s, encoding="utf-8", newline="\n")


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def append_equity_snapshot(workspace: Path, tag: str, equity: float, buying_power: float) -> None:
    p = _state_dir(workspace) / "equity_history.csv"
    now = datetime.now(timezone.utc).isoformat()
    exists = p.exists()
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if not exists:
            w.writerow(["ts_utc", "tag", "equity", "buying_power"])
        w.writerow([now, tag, f"{equity:.2f}", f"{buying_power:.2f}"])


def load_equity_history(workspace: Path) -> pd.DataFrame:
    p = _state_dir(workspace) / "equity_history.csv"
    if not p.exists():
        return pd.DataFrame(columns=["ts_utc", "tag", "equity", "buying_power"])
    df = pd.read_csv(p)
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df["equity"] = pd.to_numeric(df["equity"], errors="coerce")
    df = df.dropna(subset=["ts_utc", "equity"]).sort_values("ts_utc")
    return df


def _period_metrics(eq: pd.Series) -> Dict[str, Any]:
    eq = eq.dropna()
    if len(eq) < 2:
        return {"ok": False, "reason": "not_enough_points"}
    ret = float(eq.iloc[-1] / eq.iloc[0] - 1.0)
    peak = eq.cummax()
    dd = float(((eq / peak) - 1.0).min())
    return {
        "ok": True,
        "return": ret,
        "max_drawdown": float(abs(dd)),
        "start": str(eq.index[0]),
        "end": str(eq.index[-1]),
    }


def run_report(
    workspace: Path, out_dir: Path, kind: str, max_symbols: Optional[int] = None
) -> Dict[str, Any]:
    """
    kind: midday | eod | eow | eom
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    cfg = load_config(workspace)
    cfg_h = config_hash(cfg)

    snap_path = snapshot_config(workspace, cfg)
    _wjson(out_dir / "config_used.json", cfg)
    _wjson(out_dir / "config_meta.json", {"config_hash": cfg_h, "snapshot_path": str(snap_path)})

    decision_trace: Dict[str, Any] = {"kind": kind, "agents": []}

    # Agent 1: status snapshot
    st = paper_status(out_dir=out_dir)
    acct = st["account"]
    equity = float(acct.get("equity", 0.0) or 0.0)
    bp = float(acct.get("buying_power", 0.0) or 0.0)
    append_equity_snapshot(workspace, tag=kind, equity=equity, buying_power=bp)

    decision_trace["agents"].append(
        {"name": "StatusAgent", "outputs": {"equity": equity, "buying_power": bp}}
    )

    # Optional morning reconcile/stops
    if kind == "midday" and cfg.get("automation", {}).get("place_stops_on_reconcile", True):
        # NO placing here; reconcile is its own scheduled job. Midday just reports.
        pass

    paper_cycle_result = None
    if kind == "eod" and cfg.get("automation", {}).get("eod_run_paper_cycle", True):
        # Agent 2: plan (or execute) next-day orders
        # paper_cycle uses universe via OpsBot in normal flow; here we just limit via max_symbols passed in
        # The underlying paper_cycle currently expects an explicit symbol list; keep this report job simple:
        # We rely on OpsBot for trade cycles; this report focuses on snapshots.
        place_orders = bool(cfg.get("automation", {}).get("eod_place_orders", False))
        dry_run = not place_orders

        # IMPORTANT: we don't know your universe list here without importing it; we keep EOD report as "status + reminders".
        paper_cycle_result = {
            "note": "EOD trade planning is run via /mfp paper-cycle (dashboard button).",
            "dry_run": dry_run,
        }

        decision_trace["agents"].append({"name": "EODPlannerAgent", "outputs": paper_cycle_result})

    # Agent: weekly/monthly metrics from equity_history
    if kind in ("eow", "eom"):
        hist = load_equity_history(workspace)
        if len(hist) >= 2:
            hist = hist.set_index("ts_utc")["equity"]
            # use last 7/35 days window as a practical approximation (you can tighten later with NYSE calendar)
            lookback = timedelta(days=7) if kind == "eow" else timedelta(days=35)
            cut = datetime.now(timezone.utc) - lookback
            eq = hist[hist.index >= cut]
            pm = _period_metrics(eq)
        else:
            pm = {"ok": False, "reason": "no_equity_history"}
        _wjson(out_dir / f"{kind}_metrics.json", pm)
        decision_trace["agents"].append({"name": "PeriodMetricsAgent", "outputs": pm})

    # GapFinderAgent: always flag known data gaps
    gaps: List[str] = []
    if not cfg.get("filters", {}).get("earnings_filter_enabled", False):
        gaps.append("Earnings filter is disabled (or no provider). This increases gap risk around earnings.")
    gaps.append(
        "Backtests cannot reliably include historical earnings gaps without a paid earnings calendar. Flagged as a data gap."
    )
    _wjson(out_dir / "gaps.json", {"gaps": gaps})
    decision_trace["agents"].append({"name": "GapFinderAgent", "outputs": {"gaps": gaps}})

    _wjson(out_dir / "decision_trace.json", decision_trace)

    # Summary markdown
    lines = []
    lines.append(f"# {kind.upper()} Report\n")
    lines.append(f"- timestamp_utc: `{datetime.now(timezone.utc).isoformat()}`")
    lines.append(f"- config_hash: `{cfg_h}`\n")
    lines.append("## Account snapshot")
    lines.append(f"- equity: **${equity:,.2f}**")
    lines.append(f"- buying_power: **${bp:,.2f}**\n")
    lines.append("## Notes / Gaps")
    for g in gaps:
        lines.append(f"- ⚠️ {g}")
    _wtext(out_dir / f"{kind}_report.md", "\n".join(lines) + "\n")

    zip_path = create_evidence_zip(out_dir)
    manifest_path = write_manifest(
        out_dir=out_dir, bundle={"paths": [zip_path]}, config={"kind": kind, "config_hash": cfg_h}
    )

    append_runlog(
        workspace,
        {
            "kind": "report",
            "report_kind": kind,
            "out_dir": str(out_dir),
            "config_hash": cfg_h,
            "manifest": str(manifest_path),
            "zip": str(zip_path),
            "equity": equity,
        },
    )

    return {"ok": True, "out_dir": str(out_dir), "zip": str(zip_path), "manifest": str(manifest_path)}
