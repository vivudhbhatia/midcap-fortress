#!/usr/bin/env bash
set -euo pipefail

echo "==> Fixing: (1) Ruff E402 in backtest engine (2) restore /mfp backtest-sweep in opsbot"

# -----------------------------------------------------------------------------
# (1) Fix E402 in src/mfp/backtest/engine.py by moving helper block below imports
# -----------------------------------------------------------------------------
python - <<'PY'
from __future__ import annotations

from pathlib import Path

p = Path("src/mfp/backtest/engine.py")
if not p.exists():
    raise SystemExit("src/mfp/backtest/engine.py not found")

lines = p.read_text(encoding="utf-8").splitlines(True)

start = None
for i, line in enumerate(lines):
    if "# --- Universe helpers (backwards compatible) ---" in line:
        start = i
        break

if start is None:
    print("[info] No helper block found; nothing to move.")
    raise SystemExit(0)

# Helper block ends right before the next top-level import line that appears after it
end = None
for j in range(start + 1, len(lines)):
    s = lines[j]
    if (s.startswith("import ") or s.startswith("from ")) and (len(s) > 0 and not s.startswith(" ")):
        end = j
        break

if end is None:
    # If no import follows, move block before first top-level def
    for j in range(start + 1, len(lines)):
        if lines[j].startswith("def ") or lines[j].startswith("class "):
            end = j
            break

if end is None:
    end = len(lines)

helper = lines[start:end]
del lines[start:end]

# Insert helper block just before the first top-level def/class (after all imports)
insert_at = None
for i, line in enumerate(lines):
    if line.startswith("def ") or line.startswith("class "):
        insert_at = i
        break

if insert_at is None:
    insert_at = len(lines)

# Ensure there is a blank line before inserting (style)
if insert_at > 0 and lines[insert_at - 1].strip() != "":
    helper = ["\n"] + helper

lines[insert_at:insert_at] = helper

p.write_text("".join(lines), encoding="utf-8", newline="\n")
print("[ok] Moved universe helper block below imports in src/mfp/backtest/engine.py")
PY

# -----------------------------------------------------------------------------
# (2) Restore /mfp backtest-sweep in src/mfp/ui/github_opsbot.py (keep v3 commands)
# -----------------------------------------------------------------------------
cat > src/mfp/ui/github_opsbot.py <<'PY'
from __future__ import annotations

import json
import shlex
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class CmdResult:
    ok: bool
    summary_md: str
    artifacts_dir: Optional[Path]


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
    from mfp.config.normalize import normalize_config
    from mfp.config.runtime import config_hash, load_config

    cfg = normalize_config(load_config(workspace))

    # ---------------- status ----------------
    if cmd == "status":
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True})
        return CmdResult(True, f"✅ workspace: `{workspace}`\n", out_dir)

    # ---------------- backtest-sweep (RESTORED for tests + UI) ----------------
    if cmd == "backtest-sweep":
        start = kv.get("start", "2011-01-01")
        end = kv.get("end", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
        max_symbols = int(kv.get("max_symbols", "60") or "60")

        universe_name = kv.get("universe", cfg.get("universe", {}).get("name", "sp400"))
        strategy_name = kv.get("strategy", "midcap_pulse_v1")

        from mfp.backtest.engine import run_backtest

        rows: List[Dict[str, Any]] = []
        for tf in ["1d", "1wk", "1mo"]:
            tf_dir = out_dir / f"bt_{tf}"
            tf_dir.mkdir(parents=True, exist_ok=True)

            bt = run_backtest(
                out_dir=tf_dir,
                timeframe=tf,
                start=start,
                end=end,
                universe_name=universe_name,
                strategy_name=strategy_name,
                max_symbols=max_symbols,
            )
            rows.append({"timeframe": tf, "metrics": bt.get("metrics", {})})

        _wjson(out_dir / "sweep.json", {"start": start, "end": end, "universe": universe_name, "rows": rows})

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv})
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True})

        md = ["✅ Backtest sweep complete", "", f"- universe: `{universe_name}`", f"- start: `{start}`", f"- end: `{end}`", ""]
        for r in rows:
            m = r["metrics"]
            md.append(
                f"- {r['timeframe']}: "
                f"CAGR {m.get('cagr', 0)*100:.2f}% | "
                f"MaxDD {m.get('max_drawdown', 0)*100:.2f}% | "
                f"Trades {m.get('num_trades', 0)}"
            )
        md.append(f"\nArtifacts: `{out_dir}`")
        md.append(f"Manifest: `{manifest_path.name}`")

        return CmdResult(True, "\n".join(md) + "\n", out_dir)

    # ---------------- portfolio safety check (v3) ----------------
    if cmd == "portfolio-safety-check":
        from mfp.governance.portfolio_safety import write_certificate
        from mfp.objectives.evaluator import evaluate_objectives
        from mfp.backtest.engine import run_backtest

        start = kv.get("start", cfg.get("pretrade_check", {}).get("start", "2011-01-01"))
        end = kv.get("end", datetime.now(timezone.utc).strftime("%Y-%m-%d"))

        strategies = cfg.get("portfolio", {}).get("strategies", {})
        enabled = [k for k, v in strategies.items() if isinstance(v, dict) and bool(v.get("enabled", True))]
        if not enabled:
            return CmdResult(False, "❌ No enabled strategies in config.", out_dir)

        results: Dict[str, Any] = {"start": start, "end": end, "strategies": {}}
        overall_pass = True
        reasons: List[str] = []

        for sid in enabled:
            spec = strategies[sid]
            engine = str(spec.get("engine", "pulse_mr"))
            universe = str(spec.get("universe", "") or "")
            symbols = spec.get("symbols") or []

            if engine == "trend_hold":
                results["strategies"][sid] = {"skipped": True, "reason": "trend_hold_not_in_safety_check_v3"}
                continue

            universe_name = ("custom:" + ",".join([str(x).upper() for x in symbols])) if symbols else (universe or "sp400")

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
            "config_hash": config_hash(cfg),
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

        return CmdResult(True, f"✅ Portfolio Safety Check complete (pass={cert['pass']}).\nManifest: `{manifest_path.name}`\n", out_dir)

    if cmd == "portfolio-safety-ack":
        from mfp.governance.portfolio_safety import acknowledge

        try:
            p = acknowledge(workspace, reviewer="human")
            _wjson(out_dir / "portfolio_safety_ack.json", {"ok": True, "path": str(p)})

            zip_path = create_evidence_zip(out_dir)
            manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd})
            append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True})
            return CmdResult(True, f"✅ Acknowledged portfolio safety check.\nManifest: `{manifest_path.name}`\n", out_dir)
        except Exception as e:
            return CmdResult(False, f"❌ Ack failed: {type(e).__name__}: {e}\n", out_dir)

    if cmd in {"portfolio-preview", "portfolio-place"}:
        from mfp.governance.portfolio_safety import validate_certificate
        from mfp.portfolio.portfolio_cycle import run_portfolio_cycle

        dry_run = cmd == "portfolio-preview"
        require = bool(cfg.get("portfolio", {}).get("require_portfolio_safety_check", True))

        if (not dry_run) and require:
            v = validate_certificate(workspace, cfg, require_reviewed=True)
            if not bool(v.get("ok", False)):
                return CmdResult(False, f"❌ BLOCKED by Portfolio Safety Check: `{v.get('reason')}`\n", out_dir)

        port_dir = out_dir / "portfolio"
        r = run_portfolio_cycle(workspace=workspace, out_dir=port_dir, dry_run=dry_run)
        _wjson(out_dir / "portfolio_run.json", r)

        zip_path = create_evidence_zip(out_dir)
        manifest_path = write_manifest(out_dir=out_dir, bundle={"paths": [zip_path]}, config={"cmd": cmd, **kv})
        append_runlog(workspace, {"kind": cmd, "out_dir": str(out_dir), "ok": True, "dry_run": dry_run})

        return CmdResult(True, f"✅ Portfolio run complete (preview={dry_run}).\nManifest: `{manifest_path.name}`\n", out_dir)

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

echo "==> Patch complete."
