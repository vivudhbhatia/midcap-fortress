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
    Returns:
      {
        "ok": bool,
        "violations": [ { "rule": str, "message": str }... ],
        "warnings": [ { "rule": str, "message": str }... ],
        "evaluated": { ... key values ... }
      }
    """
    violations: List[Dict[str, str]] = []
    warnings: List[Dict[str, str]] = []

    # Pull key values (with safe defaults)
    require_safety_check = bool(_get(cfg, "execution.require_pretrade_check", True))
    max_age_days = int(_get(cfg, "execution.pretrade_check_max_age_days", 7))

    r_per_trade = float(_get(cfg, "risk.risk_per_trade_pct", 0.25))
    max_open_risk = float(_get(cfg, "risk.max_open_risk_pct", 1.0))
    max_positions = int(_get(cfg, "risk.max_positions", 6))

    dd_enabled = bool(_get(cfg, "risk.dd_governor.enabled", True))
    dd_max = float(_get(cfg, "risk.dd_governor.max_dd", 0.03))

    scale_in = str(_get(cfg, "risk.scale_in_mode", "none"))

    # ---- HARD safety rules (block Apply) ----
    if not require_safety_check:
        violations.append(
            {
                "rule": "SAFETY_CHECK_REQUIRED",
                "message": "Safety Check cannot be turned off. Keep 'Require Safety Check' enabled.",
            }
        )

    if max_age_days < 1 or max_age_days > 30:
        violations.append(
            {
                "rule": "SAFETY_CHECK_MAX_AGE",
                "message": "Safety Check must expire between 1 and 30 days (recommended 7).",
            }
        )

    if not dd_enabled:
        violations.append(
            {
                "rule": "DRAWDOWN_GOVERNOR_REQUIRED",
                "message": "Drawdown protection cannot be turned off.",
            }
        )

    if dd_max > 0.03 + 1e-12:
        violations.append(
            {
                "rule": "DRAWDOWN_LIMIT",
                "message": "Rolling drawdown limit must be <= 3% (0.03).",
            }
        )

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

    # ---- SOFT warnings (allowed, but visible) ----
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

    # This is NOT a hard violation because the engine can still cap positions by open-risk.
    if (r_per_trade * max_positions) > (max_open_risk + 1e-12):
        warnings.append(
            {
                "rule": "POSITION_CAP_MISMATCH",
                "message": "Max positions × risk per trade exceeds max open risk. The system may take fewer positions than 'Max positions'.",
            }
        )

    if scale_in != "none":
        warnings.append(
            {
                "rule": "SCALE_IN_ENABLED",
                "message": "Scale-in is enabled. Adding to losing trades can increase tail risk. Consider 'none' for fortress behavior.",
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
    }

    return {
        "ok": len(violations) == 0,
        "violations": violations,
        "warnings": warnings,
        "evaluated": evaluated,
    }
