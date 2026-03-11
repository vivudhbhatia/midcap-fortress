#!/usr/bin/env bash
set -euo pipefail

echo "==> Patch v3 pytest failures (engine universe compat + restore fortress guardrails)"

# 1) Patch backtest engine so:
#   - get_universe_sp400 exists again (tests monkeypatch it)
#   - sp400-forcing is removed (so sp500/both works)
#   - run_backtest uses get_universe(universe_name)
python - <<'PY'
from __future__ import annotations

import re
from pathlib import Path

p = Path("src/mfp/backtest/engine.py")
if not p.exists():
    raise SystemExit("src/mfp/backtest/engine.py not found")

txt = p.read_text(encoding="utf-8")

# Remove the old "force sp400 always" block if present
txt2 = re.sub(
    r"\n\s*if\s+universe_name\s*!=\s*['\"]sp400['\"]\s*:\s*\n\s*universe_name\s*=\s*['\"]sp400['\"]\s*\n",
    "\n",
    txt,
)

# Ensure we import the generic universe resolver as _get_universe
txt2 = txt2.replace(
    "from mfp.data.universe_sp400 import get_universe_sp400",
    "from mfp.data.universe import get_universe as _get_universe",
)
txt2 = txt2.replace(
    "from mfp.data.universe import get_universe",
    "from mfp.data.universe import get_universe as _get_universe",
)

# Ensure tickers selection uses get_universe(universe_name) (wrapper below)
txt2 = re.sub(
    r"tickers\s*=\s*get_universe_sp400\(\)",
    "tickers = get_universe(universe_name)",
    txt2,
)

# Insert backwards-compatible helpers + wrapper get_universe (so tests can monkeypatch get_universe_sp400)
marker = "from mfp.data.universe import get_universe as _get_universe"
if "def get_universe_sp400" not in txt2:
    if marker not in txt2:
        # Insert marker after last import line
        lines = txt2.splitlines()
        last_import = 0
        for i, line in enumerate(lines):
            if line.startswith("import ") or line.startswith("from "):
                last_import = i
        lines.insert(last_import + 1, marker)
        txt2 = "\n".join(lines) + "\n"

    lines = txt2.splitlines()
    idx = None
    for i, line in enumerate(lines):
        if line.strip() == marker:
            idx = i
            break
    if idx is None:
        raise SystemExit("Could not locate universe import marker to insert helpers.")

    helper_block = [
        "",
        "# --- Universe helpers (backwards compatible) ---",
        "# Tests monkeypatch get_universe_sp400; keep it stable.",
        "def get_universe_sp400() -> list[str]:",
        '    return _get_universe("sp400")',
        "",
        "def get_universe_sp500() -> list[str]:",
        '    return _get_universe("sp500")',
        "",
        "def get_universe(universe_name: str) -> list[str]:",
        '    n = (universe_name or "sp400").strip().lower()',
        '    if n in {"sp400", "mid", "midcap"}:',
        "        return get_universe_sp400()",
        '    if n in {"sp500", "large", "largecap"}:',
        "        return get_universe_sp500()",
        "    # supports both / custom:... etc via mfp.data.universe",
        "    return _get_universe(universe_name)",
        "",
    ]
    lines[idx + 1:idx + 1] = helper_block
    txt2 = "\n".join(lines) + "\n"

if txt2 != txt:
    p.write_text(txt2, encoding="utf-8", newline="\n")
    print("[ok] Patched src/mfp/backtest/engine.py")
else:
    print("[info] No changes needed in src/mfp/backtest/engine.py")
PY

# 2) Restore fortress guardrails (v2 rules) AND keep v3 portfolio allocation checks
cat > src/mfp/governance/guardrails.py <<'PY'
from __future__ import annotations

from typing import Any, Dict, List


def _get(d: Dict[str, Any], path: str, default: Any) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def check_guardrails(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Hard rules block Apply (must be conservative).
    Soft warnings are allowed but highlighted.
    """
    violations: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []

    # ---- Fortress safety rules (v2 behavior, required by tests + governance) ----
    require_safety_check = bool(_get(cfg, "execution.require_pretrade_check", True))
    max_age_days = int(_get(cfg, "execution.pretrade_check_max_age_days", 7))

    r_per_trade = float(_get(cfg, "risk.risk_per_trade_pct", 0.25))
    max_open_risk = float(_get(cfg, "risk.max_open_risk_pct", 1.0))
    max_positions = int(_get(cfg, "risk.max_positions", 6))

    dd_enabled = bool(_get(cfg, "risk.dd_governor.enabled", True))
    dd_max = float(_get(cfg, "risk.dd_governor.max_dd", 0.03))

    scale_in = str(_get(cfg, "risk.scale_in_mode", "none"))

    # HARD: cannot disable safety check
    if not require_safety_check:
        violations.append(
            {
                "rule": "SAFETY_CHECK_REQUIRED",
                "message": "Safety Check cannot be turned off. Keep 'Require Safety Check' enabled.",
            }
        )

    # HARD: safety cert age bounds
    if max_age_days < 1 or max_age_days > 30:
        violations.append(
            {
                "rule": "SAFETY_CHECK_MAX_AGE",
                "message": "Safety Check must expire between 1 and 30 days (recommended 7).",
            }
        )

    # HARD: cannot disable drawdown governor
    if not dd_enabled:
        violations.append(
            {
                "rule": "DRAWDOWN_GOVERNOR_REQUIRED",
                "message": "Drawdown protection cannot be turned off.",
            }
        )

    # HARD: must be <= 3% (this is exactly what your failing test expects)
    if dd_max > 0.03 + 1e-12:
        violations.append(
            {
                "rule": "DRAWDOWN_LIMIT",
                "message": "Rolling drawdown limit must be <= 3% (0.03).",
            }
        )

    # HARD: risk bounds
    if r_per_trade <= 0.0 or r_per_trade > 1.0:
        violations.append(
            {
                "rule": "RISK_PER_TRADE_RANGE",
                "message": "Risk per trade must be between 0% and 1%.",
            }
        )

    if max_open_risk <= 0.0 or max_open_risk > 3.0:
        violations.append(
            {
                "rule": "MAX_OPEN_RISK_RANGE",
                "message": "Total open risk must be between 0% and 3%.",
            }
        )

    if max_positions < 1 or max_positions > 20:
        violations.append(
            {
                "rule": "MAX_POSITIONS_RANGE",
                "message": "Max positions must be between 1 and 20.",
            }
        )

    # SOFT warnings
    if r_per_trade > 0.50:
        warnings.append(
            {
                "rule": "HIGH_RISK_PER_TRADE",
                "message": "Risk per trade is above 0.50%. Consider lowering for a 'fortress' profile.",
            }
        )

    if max_open_risk > 1.50:
        warnings.append(
            {
                "rule": "HIGH_OPEN_RISK",
                "message": "Total open risk is above 1.50%. This may conflict with low-drawdown goals.",
            }
        )

    if (r_per_trade * max_positions) > (max_open_risk + 1e-12):
        warnings.append(
            {
                "rule": "POSITION_CAP_MISMATCH",
                "message": "Max positions × risk per trade exceeds max open risk. The system may take fewer positions.",
            }
        )

    if scale_in != "none":
        warnings.append(
            {
                "rule": "SCALE_IN_ENABLED",
                "message": "Scale-in is enabled. Adding to losing trades can increase tail risk.",
            }
        )

    # ---- v3 Portfolio governance rules (new) ----
    strategies = _get(cfg, "portfolio.strategies", {})
    alloc_sum = None
    if isinstance(strategies, dict) and strategies:
        alloc_sum = 0.0
        for sid, spec in strategies.items():
            if not isinstance(spec, dict):
                continue
            if not bool(spec.get("enabled", True)):
                continue
            w = float(spec.get("allocation_pct", 0.0))
            if w < -1e-12 or w > 1.0 + 1e-12:
                violations.append(
                    {
                        "rule": "PORTFOLIO_ALLOCATION_RANGE",
                        "message": f"{sid}: allocation_pct must be between 0 and 1 (got {w}).",
                    }
                )
            alloc_sum += w

        if abs(alloc_sum - 1.0) > 1e-6:
            violations.append(
                {
                    "rule": "PORTFOLIO_ALLOCATION_SUM",
                    "message": f"Strategy allocations must sum to 1.0 (100%). Current sum={alloc_sum:.6f}.",
                }
            )

    # Optional: validate objectives drawdown dial inputs (type/range sanity)
    dd_limits = _get(cfg, "objectives.drawdown_limits", {})
    if isinstance(dd_limits, dict) and dd_limits:
        for k, v in dd_limits.items():
            try:
                lim = float(v)
            except Exception:
                violations.append(
                    {
                        "rule": "OBJECTIVE_DD_LIMIT_TYPE",
                        "message": f"Drawdown limit {k} must be a number.",
                    }
                )
                continue
            if lim <= 0.0 or lim > 0.50:
                violations.append(
                    {
                        "rule": "OBJECTIVE_DD_LIMIT_RANGE",
                        "message": f"Drawdown limit {k} must be between 0 and 0.50. Got {lim}.",
                    }
                )

    evaluated = {
        "require_safety_check": require_safety_check,
        "pretrade_check_max_age_days": max_age_days,
        "risk_per_trade_pct": r_per_trade,
        "max_open_risk_pct": max_open_risk,
        "max_positions": max_positions,
        "dd_governor_enabled": dd_enabled,
        "dd_governor_max_dd": dd_max,
        "scale_in_mode": scale_in,
        "portfolio_allocation_sum": alloc_sum,
    }

    return {"ok": len(violations) == 0, "violations": violations, "warnings": warnings, "evaluated": evaluated}
PY

echo "==> Patch done."
echo "==> Next: ./scripts/fix.sh && ./scripts/check.sh && pytest -q"
