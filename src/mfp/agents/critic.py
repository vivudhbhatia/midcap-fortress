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
        issues.append(
            {"severity": "HIGH", "topic": "Drawdown", "message": "Drawdown gate breached. Pause new entries."}
        )

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
    lines.append(f"- high_severity_issues: `{len([i for i in issues if i['severity'] == 'HIGH'])}`")
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
