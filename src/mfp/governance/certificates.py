from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from mfp.config.runtime import config_hash


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _cert_dir(workspace: Path) -> Path:
    d = _state_dir(workspace) / "pretrade"
    d.mkdir(parents=True, exist_ok=True)
    return d


def current_certificate_path(workspace: Path) -> Path:
    return _cert_dir(workspace) / "pretrade_certificate.json"


def _history_dir(workspace: Path) -> Path:
    d = _cert_dir(workspace) / "history"
    d.mkdir(parents=True, exist_ok=True)
    return d


def load_pretrade_certificate(workspace: Path) -> Optional[Dict[str, Any]]:
    p = current_certificate_path(workspace)
    if not p.exists():
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def write_pretrade_certificate(workspace: Path, cert: Dict[str, Any]) -> Path:
    cert = dict(cert)
    cert.setdefault("created_ts_utc", datetime.now(timezone.utc).isoformat())
    p = current_certificate_path(workspace)
    p.write_text(json.dumps(cert, indent=2, default=str), encoding="utf-8", newline="\n")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    hist = _history_dir(workspace) / f"cert_{ts}.json"
    hist.write_text(json.dumps(cert, indent=2, default=str), encoding="utf-8", newline="\n")
    return p


def mark_pretrade_reviewed(workspace: Path, reviewer: str = "human") -> Path:
    cert = load_pretrade_certificate(workspace)
    if not cert:
        raise FileNotFoundError("No pretrade_certificate.json found.")
    cert["reviewed"] = True
    cert["reviewed_by"] = reviewer
    cert["reviewed_ts_utc"] = datetime.now(timezone.utc).isoformat()
    return write_pretrade_certificate(workspace, cert)


def evaluate_sweep_for_certificate(
    rows: List[Dict[str, Any]],
    cfg: Dict[str, Any],
    out_dir: Path,
) -> Dict[str, Any]:
    pcfg = cfg.get("pretrade_check", {})
    min_cagr = float(pcfg.get("min_cagr", 0.15))
    require_pass_window = bool(pcfg.get("require_pass_3pct_window", True))

    # Expect sweep rows include 1d/1wk/1mo
    by_tf = {r.get("timeframe"): r for r in rows if r.get("timeframe")}
    row_1d = by_tf.get("1d")

    reasons: List[str] = []
    pass_ = True

    if not row_1d:
        pass_ = False
        reasons.append("Missing 1d row in sweep results.")
    else:
        cagr_1d = float(row_1d.get("cagr", 0.0) or 0.0)
        if cagr_1d + 1e-12 < min_cagr:
            pass_ = False
            reasons.append(f"1d CAGR {cagr_1d * 100:.2f}% < min {min_cagr * 100:.2f}%.")

    if require_pass_window:
        bad = [r for r in rows if not bool(r.get("pass_drawdown_rule", False))]
        if bad:
            pass_ = False
            reasons.append("3% rolling DD rule failed in one or more timeframes.")

    cert = {
        "kind": "pretrade_certificate",
        "config_hash": config_hash(cfg),
        "sweep_rows": rows,
        "criteria": {
            "min_cagr": min_cagr,
            "require_pass_3pct_window": require_pass_window,
        },
        "pass": pass_,
        "reasons": reasons,
        "reviewed": False,
        "sweep_out_dir": str(out_dir),
    }
    return cert


def validate_pretrade_certificate(workspace: Path, cfg: Dict[str, Any]) -> Dict[str, Any]:
    exec_cfg = cfg.get("execution", {})
    require = bool(exec_cfg.get("require_pretrade_check", True))
    max_age_days = int(exec_cfg.get("pretrade_check_max_age_days", 7))

    cert = load_pretrade_certificate(workspace)

    if not require:
        return {"ok": True, "reason": "pretrade_check_disabled", "cert": cert}

    if not cert:
        return {"ok": False, "reason": "missing_certificate", "cert": None}

    reasons: List[str] = []
    ok = True

    # config hash match = governance guarantee
    if cert.get("config_hash") != config_hash(cfg):
        ok = False
        reasons.append(
            "Config changed since certificate was issued. Re-run pretrade sweep and re-acknowledge."
        )

    if not bool(cert.get("pass", False)):
        ok = False
        reasons.append("Certificate did not pass criteria.")

    if not bool(cert.get("reviewed", False)):
        ok = False
        reasons.append("Certificate not acknowledged (human-in-loop required).")

    # age check
    try:
        created = datetime.fromisoformat(cert["created_ts_utc"].replace("Z", "+00:00"))
    except Exception:
        created = None

    if created is not None:
        age = datetime.now(timezone.utc) - created
        if age > timedelta(days=max_age_days):
            ok = False
            reasons.append(f"Certificate expired (>{max_age_days} days old).")

    return {"ok": ok, "reason": "; ".join(reasons) if reasons else "ok", "cert": cert}
