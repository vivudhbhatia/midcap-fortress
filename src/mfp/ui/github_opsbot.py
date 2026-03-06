from __future__ import annotations

import shlex
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict

from mfp.audit.manifest import write_manifest
from mfp.backtest.engine import run_backtest
from mfp.backtest.report import build_report_bundle


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


def run_command(command_line: str, workspace: Path) -> CmdResult:
    parts = shlex.split(command_line.strip())

    # GitHub uses "/mfp ...". Local Git Bash should use "mfp ..." (no leading slash).
    if not parts or parts[0] not in ("/mfp", "mfp"):
        return CmdResult(
            False,
            "❌ Command must start with `/mfp` (GitHub) or `mfp` (local Git Bash).",
            None,
        )

    # Normalize local -> canonical
    if parts[0] == "mfp":
        parts[0] = "/mfp"

    cmd = parts[1] if len(parts) > 1 else ""
    kv = _parse_kv(parts[2:])

    run_id = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    out_dir = workspace / "reports" / run_id
    out_dir.mkdir(parents=True, exist_ok=True)

    if cmd == "status":
        reports_dir = workspace / "reports"
        latest = sorted([p.name for p in reports_dir.glob("*") if p.is_dir()])
        latest_run = latest[-1] if latest else "none"
        summary = (
            f"✅ Status\n\n"
            f"- latest_report_run: `{latest_run}`\n"
            f"- reports_dir: `{reports_dir}`\n"
        )
        return CmdResult(True, summary, out_dir)

    if cmd == "backtest":
        timeframe = kv.get("timeframe", "1d")
        start = kv.get("start", "2011-01-01")
        end = kv.get("end", datetime.utcnow().strftime("%Y-%m-%d"))
        universe = kv.get("universe", "sp400")
        strategy = kv.get("strategy", "midcap_pulse_v1")
        max_symbols = int(kv.get("max_symbols", "0") or "0")

        bt = run_backtest(
            out_dir=out_dir,
            timeframe=timeframe,
            start=start,
            end=end,
            universe_name=universe,
            strategy_name=strategy,
            max_symbols=max_symbols if max_symbols > 0 else None,
        )

        bundle = build_report_bundle(out_dir=out_dir, backtest_result=bt)
        manifest_path = write_manifest(out_dir=out_dir, bundle=bundle, config=kv)

        m = bt["metrics"]
        summary = (
            f"✅ Backtest complete\n\n"
            f"- run_id: `{run_id}`\n"
            f"- timeframe: `{timeframe}`\n"
            f"- start: `{start}`\n"
            f"- end: `{end}`\n"
            f"- universe: `{universe}`\n"
            f"- strategy: `{strategy}`\n"
            f"- max_symbols: `{max_symbols if max_symbols>0 else 'ALL'}`\n\n"
            f"## Key metrics\n"
            f"- Final equity: **${m['final_equity']:.2f}**\n"
            f"- CAGR: **{m['cagr']*100:.2f}%**\n"
            f"- Max drawdown: **{m['max_drawdown']*100:.2f}%**\n"
            f"- Trades: **{m['num_trades']}**\n"
            f"- Win rate: **{m['win_rate']*100:.2f}%**\n\n"
            f"Artifacts: `{out_dir}`\n"
            f"Manifest: `{manifest_path.name}`\n"
        )
        return CmdResult(True, summary, out_dir)

    return CmdResult(False, f"❌ Unknown command: `{cmd}`", out_dir)
