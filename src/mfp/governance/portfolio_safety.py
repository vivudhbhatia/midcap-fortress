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


def validate_certificate(
    workspace: Path, cfg: Dict[str, Any], require_reviewed: bool = False
) -> Dict[str, Any]:
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
