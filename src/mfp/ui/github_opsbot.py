from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict


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


def _wjson(p: Path, obj) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def run_command(command_line: str, workspace: Path) -> CmdResult:
    parts = shlex.split(command_line.strip())
    if not parts or parts[0] not in ("/mfp", "mfp"):
        return CmdResult(False, "❌ Command must start with `/mfp` (GitHub) or `mfp` (local).", None)

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
    from mfp.backtest.engine import run_backtest
    from mfp.backtest.gaps import compute_gap_report
    from mfp.backtest.report import build_report_bundle, write_gap_report, write_sweep_report
    from mfp.config.runtime import load_config
    from mfp.governance.certificates import (
        evaluate_sweep_for_certificate,
        load_pretrade_certificate,
        mark_pretrade_reviewed,
        validate_pretrade_certificate,
        write_pretrade_certificate,
    )

    cfg = load_config(workspace)

    # ---------------- status ----------------
    if cmd == "status":
        reports_dir = workspace / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        latest = sorted([p.name for p in reports_dir.glob("*") if p.is_dir()])
        latest_run = latest[-1] if latest else "none"
        summary = f"✅ Status\n\n- latest_report_run: `{latest_run}`\n- reports_dir: `{reports_dir}`\n"
        return CmdResult(True, summary, out_dir)

    # ---------------- pretrade certificate ops ----------------
    if cmd == "pretrade-status":
        v = validate_pretrade_certificate(workspace, cfg)
        cert = load_pretrade_certificate(workspace)
        _wjson(out_dir / "pretrade_status.json", {"validation": v, "cert": cert})
        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
        append_runlog(workspace, {"kind": "pretrade-status", "out_dir": str(out_dir), "ok": v["ok"]})
        return CmdResult(
            True,
            f"✅ Pretrade status\n\n- ok: **{v['ok']}**\n- reason: `{v['reason']}`\n- artifacts: `{out_dir}`\n- manifest: `{manifest_path.name}`\n",
            out_dir,
        )

    if cmd == "pretrade-ack":
        try:
            p = mark_pretrade_reviewed(workspace, reviewer="human")
            _wjson(out_dir / "pretrade_ack.json", {"ok": True, "path": str(p)})
            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
            append_runlog(workspace, {"kind": "pretrade-ack", "out_dir": str(out_dir), "ok": True})
            return CmdResult(
                True,
                f"✅ Pretrade certificate acknowledged.\n\n- updated: `{p}`\n- artifacts: `{out_dir}`\n- manifest: `{manifest_path.name}`\n",
                out_dir,
            )
        except Exception as e:
            return CmdResult(False, f"❌ pretrade-ack failed: {type(e).__name__}: {e}\n", out_dir)

    # ---------------- backtest ----------------
    if cmd == "backtest":
        timeframe = kv.get("timeframe", "1d")
        start = kv.get("start", "2011-01-01")
        end = kv.get("end", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
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
        gap = compute_gap_report(bt["equity"], timeframe=timeframe, meta=bt["meta"])
        gap_paths = write_gap_report(out_dir=out_dir, gap_report=gap)
        bundle["paths"].extend(gap_paths)

        zip_path = create_evidence_zip(out_dir)
        bundle["paths"].append(zip_path)
        manifest_path = write_manifest(out_dir=out_dir, bundle=bundle, config=kv)

        m = bt["metrics"]
        worst_window = max([x["max_dd"] for x in gap.get("rolling", [])] or [0.0])

        summary = (
            f"✅ Backtest complete\n\n"
            f"- run_id: `{run_id}`\n"
            f"- timeframe: `{timeframe}`\n"
            f"- start: `{start}`\n"
            f"- end: `{end}`\n"
            f"- universe: `{universe}`\n"
            f"- strategy: `{strategy}`\n"
            f"- max_symbols: `{max_symbols if max_symbols > 0 else 'ALL'}`\n\n"
            f"## Key metrics\n"
            f"- Final equity: **${m['final_equity']:.2f}**\n"
            f"- CAGR: **{m['cagr'] * 100:.2f}%**\n"
            f"- Max drawdown: **{m['max_drawdown'] * 100:.2f}%**\n"
            f"- Trades: **{m['num_trades']}**\n"
            f"- Win rate: **{m['win_rate'] * 100:.2f}%**\n\n"
            f"## Drawdown window rule (3%)\n"
            f"- pass: **{'YES' if gap.get('pass_drawdown_rule') else 'NO'}**\n"
            f"- worst_window_dd: **{worst_window * 100:.2f}%**\n\n"
            f"Artifacts: `{out_dir}`\n"
            f"Evidence zip: `evidence.zip`\n"
            f"Manifest: `{manifest_path.name}`\n"
        )
        append_runlog(workspace, {"kind": "backtest", "out_dir": str(out_dir), "ok": True})
        return CmdResult(True, summary, out_dir)

    # ---------------- backtest-sweep (also issues certificate) ----------------
    if cmd == "backtest-sweep":
        start = kv.get("start", cfg.get("pretrade_check", {}).get("start", "2011-01-01"))
        end = kv.get(
            "end", cfg.get("pretrade_check", {}).get("end") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        )
        universe = kv.get("universe", "sp400")
        strategy = kv.get("strategy", "midcap_pulse_v1")
        max_symbols = int(
            kv.get("max_symbols", str(cfg.get("pretrade_check", {}).get("max_symbols", 60))) or "60"
        )

        rows = []
        all_paths = []

        for tf in ["1d", "1wk", "1mo"]:
            tf_dir = out_dir / tf
            tf_dir.mkdir(parents=True, exist_ok=True)

            bt = run_backtest(
                out_dir=tf_dir,
                timeframe=tf,
                start=start,
                end=end,
                universe_name=universe,
                strategy_name=strategy,
                max_symbols=max_symbols if max_symbols > 0 else None,
            )

            bundle = build_report_bundle(out_dir=tf_dir, backtest_result=bt)
            gap = compute_gap_report(bt["equity"], timeframe=tf, meta=bt["meta"])
            gap_paths = write_gap_report(out_dir=tf_dir, gap_report=gap)
            bundle["paths"].extend(gap_paths)
            all_paths.extend(bundle["paths"])

            m = bt["metrics"]
            worst_window = max([x["max_dd"] for x in gap.get("rolling", [])] or [0.0])
            rows.append(
                {
                    "timeframe": tf,
                    "cagr": m["cagr"],
                    "max_drawdown": m["max_drawdown"],
                    "num_trades": m["num_trades"],
                    "win_rate": m["win_rate"],
                    "pass_drawdown_rule": gap["pass_drawdown_rule"],
                    "worst_window_dd": worst_window,
                }
            )

        sweep = {"rows": rows, "params": kv, "start": start, "end": end, "max_symbols": max_symbols}
        _wjson(out_dir / "sweep.json", sweep)
        sweep_paths = write_sweep_report(out_dir=out_dir, sweep=sweep)
        all_paths.extend(sweep_paths)

        # Issue/update certificate (NOT reviewed yet)
        cert = evaluate_sweep_for_certificate(rows=rows, cfg=cfg, out_dir=out_dir)
        write_pretrade_certificate(workspace, cert)
        _wjson(out_dir / "pretrade_certificate_issued.json", cert)

        zip_path = create_evidence_zip(out_dir)
        all_paths.append(zip_path)

        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": all_paths}, config=kv)

        lines = []
        lines.append("✅ Backtest sweep complete (and pretrade certificate issued)\n")
        lines.append(f"- run_id: `{run_id}`")
        lines.append(f"- start: `{start}`")
        lines.append(f"- end: `{end}`")
        lines.append(f"- max_symbols: `{max_symbols}`\n")
        lines.append("## Summary")
        for r in rows:
            lines.append(
                f"- {r['timeframe']}: CAGR {r['cagr'] * 100:.2f}% | MaxDD {r['max_drawdown'] * 100:.2f}% | "
                f"worst_window_dd {r['worst_window_dd'] * 100:.2f}% | pass_3pct {'YES' if r['pass_drawdown_rule'] else 'NO'}"
            )
        lines.append("\n## Pretrade certificate")
        lines.append(f"- pass: **{cert['pass']}**")
        if cert["reasons"]:
            for rr in cert["reasons"]:
                lines.append(f"- reason: {rr}")
        lines.append("- reviewed: **False** (run `/mfp pretrade-ack` after reviewing the sweep)")
        lines.append(f"\nArtifacts: `{out_dir}`")
        lines.append("Evidence zip: `evidence.zip`")
        lines.append(f"Manifest: `{manifest_path.name}`")

        append_runlog(
            workspace,
            {"kind": "backtest-sweep", "out_dir": str(out_dir), "ok": True, "cert_pass": cert["pass"]},
        )
        return CmdResult(True, "\n".join(lines) + "\n", out_dir)

    # ---------------- gaps ----------------
    if cmd == "gaps":
        timeframe = kv.get("timeframe", "1d")
        start = kv.get("start", "2011-01-01")
        end = kv.get("end", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
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

        gap = compute_gap_report(bt["equity"], timeframe=timeframe, meta=bt["meta"])
        gap_paths = write_gap_report(out_dir=out_dir, gap_report=gap)

        zip_path = create_evidence_zip(out_dir)
        bundle = {"paths": gap_paths + [zip_path]}
        manifest_path = write_manifest(out_dir=out_dir, bundle=bundle, config=kv)

        worst_window = max([x["max_dd"] for x in gap.get("rolling", [])] or [0.0])
        summary = (
            f"✅ Gaps analysis complete\n\n"
            f"- run_id: `{run_id}`\n"
            f"- timeframe: `{timeframe}`\n"
            f"- start: `{start}`\n"
            f"- end: `{end}`\n\n"
            f"## Drawdown window rule (3%)\n"
            f"- pass: **{'YES' if gap.get('pass_drawdown_rule') else 'NO'}**\n"
            f"- worst_window_dd: **{worst_window * 100:.2f}%**\n\n"
            f"Artifacts: `{out_dir}`\n"
            f"Evidence zip: `evidence.zip`\n"
            f"Manifest: `{manifest_path.name}`\n"
        )
        append_runlog(workspace, {"kind": "gaps", "out_dir": str(out_dir), "ok": True})
        return CmdResult(True, summary, out_dir)

    # ---------------- paper commands ----------------
    if cmd == "paper-status":
        try:
            from mfp.paper.paper_cycle import paper_status

            paper_status(out_dir=out_dir)

            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})

            append_runlog(workspace, {"kind": "paper-status", "out_dir": str(out_dir), "ok": True})
            return CmdResult(
                True, f"✅ Paper status written to `{out_dir}`\nManifest: `{manifest_path.name}`\n", out_dir
            )
        except Exception as e:
            return CmdResult(False, f"❌ paper-status failed: {type(e).__name__}: {e}\n", out_dir)

    if cmd == "paper-cycle":
        try:
            from mfp.data.universe_sp400 import get_universe_sp400
            from mfp.paper.paper_cycle import paper_cycle

            max_symbols = int(kv.get("max_symbols", "60") or "60")
            dry_run = kv.get("dry_run", "true").strip().lower() != "false"

            # HARD GOVERNANCE GATE (A)
            cert_status = validate_pretrade_certificate(workspace, cfg)
            qa = {"dry_run": dry_run, "pretrade_validation": cert_status}
            _wjson(out_dir / "qa.json", qa)

            if (not dry_run) and (not cert_status["ok"]):
                _wjson(
                    out_dir / "verify.json", {"ok": False, "blocked": True, "reason": cert_status["reason"]}
                )
                zip_path = create_evidence_zip(out_dir)
                manifest_path = write_manifest(
                    out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv}
                )
                append_runlog(
                    workspace, {"kind": "paper-cycle", "out_dir": str(out_dir), "ok": False, "blocked": True}
                )
                return CmdResult(
                    False,
                    "❌ BLOCKED by governance gate.\n\n"
                    f"- reason: `{cert_status['reason']}`\n"
                    "- fix: run `backtest-sweep`, review artifacts, then run `/mfp pretrade-ack`.\n",
                    out_dir,
                )

            tickers = get_universe_sp400()[:max_symbols]
            _wjson(out_dir / "watchlist.json", {"symbols": tickers, "max_symbols": max_symbols})

            r = paper_cycle(out_dir=out_dir, symbols=tickers, dry_run=dry_run)

            # Agentified bundle mapping (B)
            # orderIntents = paper_orders_to_submit if present
            oi = out_dir / "paper_orders_to_submit.json"
            if oi.exists():
                (out_dir / "orderIntents.json").write_text(
                    oi.read_text(encoding="utf-8"), encoding="utf-8", newline="\n"
                )

            ack = out_dir / "paper_broker_ack.json"
            if ack.exists():
                (out_dir / "orders.json").write_text(
                    ack.read_text(encoding="utf-8"), encoding="utf-8", newline="\n"
                )

            # signals.json minimal (derive from intents)
            if (out_dir / "signals.json").exists() is False and (out_dir / "orderIntents.json").exists():
                intents = json.loads((out_dir / "orderIntents.json").read_text(encoding="utf-8"))
                entries = [x for x in intents if x.get("side") == "buy"]
                exits = [x for x in intents if x.get("side") == "sell"]
                _wjson(out_dir / "signals.json", {"entries": entries, "exits": exits})

            _wjson(
                out_dir / "verify.json",
                {"ok": True, "blocked": False, "summary": r, "pretrade_validation": cert_status},
            )

            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(
                out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv}
            )

            append_runlog(
                workspace, {"kind": "paper-cycle", "out_dir": str(out_dir), "ok": True, "dry_run": dry_run}
            )
            return CmdResult(
                True,
                "✅ Paper cycle complete\n\n"
                f"- dry_run: `{dry_run}`\n"
                f"- orders_to_submit: `{r.get('orders_to_submit_count')}`\n"
                f"- placed: `{r.get('placed_count')}`\n"
                f"- gate: `{r.get('gate', {}).get('reason')}`\n"
                f"- manifest: `{manifest_path.name}`\n\n"
                f"Artifacts: `{out_dir}`\n",
                out_dir,
            )
        except Exception as e:
            return CmdResult(False, f"❌ paper-cycle failed: {type(e).__name__}: {e}\n", out_dir)

    if cmd == "paper-reconcile":
        try:
            from mfp.data.universe_sp400 import get_universe_sp400
            from mfp.paper.paper_cycle import paper_reconcile

            max_symbols = int(kv.get("max_symbols", "60") or "60")
            place_stops = kv.get("place_stops", "false").strip().lower() == "true"

            tickers = get_universe_sp400()[:max_symbols]
            r = paper_reconcile(out_dir=out_dir, symbols=tickers, place_stops=place_stops)

            # recon bundle alias
            sa = out_dir / "paper_stop_actions.json"
            if sa.exists():
                (out_dir / "recon.json").write_text(
                    sa.read_text(encoding="utf-8"), encoding="utf-8", newline="\n"
                )

            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(
                out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv}
            )

            placed = len(r.get("stop_actions", {}).get("placed", []))
            skipped = len(r.get("stop_actions", {}).get("skipped", []))

            append_runlog(workspace, {"kind": "paper-reconcile", "out_dir": str(out_dir), "ok": True})
            return CmdResult(
                True,
                "✅ Paper reconcile complete\n\n"
                f"- place_stops: `{place_stops}`\n"
                f"- stops_placed: `{placed}`\n"
                f"- stops_skipped: `{skipped}`\n"
                f"- manifest: `{manifest_path.name}`\n\n"
                f"Artifacts: `{out_dir}`\n",
                out_dir,
            )
        except Exception as e:
            return CmdResult(False, f"❌ paper-reconcile failed: {type(e).__name__}: {e}\n", out_dir)

    return CmdResult(False, f"❌ Unknown command: `{cmd}`\n", out_dir)
