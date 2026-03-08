from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from mfp.config.runtime import config_hash, load_config, save_config, snapshot_config
from mfp.governance.guardrails import check_guardrails


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state" / "governance"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _proposal_dir(workspace: Path) -> Path:
    d = _state_dir(workspace) / "proposals"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _changelog_path(workspace: Path) -> Path:
    return _state_dir(workspace) / "changelog.jsonl"


def _now_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _short(h: str) -> str:
    return h[:10]


def _diff(a: Any, b: Any, path: str = "") -> List[Dict[str, Any]]:
    changes: List[Dict[str, Any]] = []

    if isinstance(a, dict) and isinstance(b, dict):
        keys = sorted(set(a.keys()) | set(b.keys()))
        for k in keys:
            p = f"{path}.{k}" if path else str(k)
            if k not in a:
                changes.append({"path": p, "from": None, "to": b[k]})
            elif k not in b:
                changes.append({"path": p, "from": a[k], "to": None})
            else:
                changes.extend(_diff(a[k], b[k], p))
        return changes

    if a != b:
        changes.append({"path": path or "<root>", "from": a, "to": b})
    return changes


def _write_json(p: Path, obj: Any) -> None:
    p.write_text(json.dumps(obj, indent=2, default=str), encoding="utf-8", newline="\n")


def _read_json(p: Path) -> Dict[str, Any]:
    return json.loads(p.read_text(encoding="utf-8"))


def list_proposals(workspace: Path) -> List[Dict[str, Any]]:
    d = _proposal_dir(workspace)
    out: List[Dict[str, Any]] = []
    for p in sorted(d.glob("*.json"), key=lambda x: x.name, reverse=True):
        try:
            out.append(_read_json(p))
        except Exception:
            continue
    return out


def load_proposal(workspace: Path, proposal_id: str) -> Dict[str, Any]:
    p = _proposal_dir(workspace) / f"{proposal_id}.json"
    if not p.exists():
        raise FileNotFoundError(f"Proposal not found: {proposal_id}")
    return _read_json(p)


def _save_proposal(workspace: Path, proposal: Dict[str, Any]) -> Path:
    proposal_id = proposal["proposal_id"]
    p = _proposal_dir(workspace) / f"{proposal_id}.json"
    _write_json(p, proposal)
    return p


def create_proposal_from_dials(
    workspace: Path, proposed_cfg: Dict[str, Any], created_by: str = "human"
) -> Dict[str, Any]:
    base_cfg = load_config(workspace)

    base_hash = config_hash(base_cfg)
    proposed_hash = config_hash(proposed_cfg)

    changes = _diff(base_cfg, proposed_cfg)
    if not changes:
        raise ValueError("No changes detected. Adjust at least one dial before creating a change request.")

    guard = check_guardrails(proposed_cfg)

    proposal_id = f"{_now_id()}_{proposed_hash[:8]}"

    proposal = {
        "proposal_id": proposal_id,
        "status": "DRAFT",
        "created_ts_utc": datetime.now(timezone.utc).isoformat(),
        "created_by": created_by,
        "base_config_hash": base_hash,
        "proposed_config_hash": proposed_hash,
        "changes": changes,
        "guardrails": guard,
        "proposed_config": proposed_cfg,
        "approved_by": None,
        "approved_ts_utc": None,
        "applied_by": None,
        "applied_ts_utc": None,
        "applied_from_hash": None,
        "applied_to_hash": None,
        "config_snapshot_path": None,
    }

    _save_proposal(workspace, proposal)
    return proposal


def approve_proposal(workspace: Path, proposal_id: str, approved_by: str = "human") -> Dict[str, Any]:
    pr = load_proposal(workspace, proposal_id)
    if pr["status"] not in ("DRAFT",):
        raise ValueError(f"Cannot approve proposal in status: {pr['status']}")

    pr["status"] = "APPROVED"
    pr["approved_by"] = approved_by
    pr["approved_ts_utc"] = datetime.now(timezone.utc).isoformat()
    _save_proposal(workspace, pr)
    return pr


def reject_proposal(
    workspace: Path, proposal_id: str, rejected_by: str = "human", reason: str = ""
) -> Dict[str, Any]:
    pr = load_proposal(workspace, proposal_id)
    if pr["status"] in ("APPLIED",):
        raise ValueError("Cannot reject an already applied proposal.")
    pr["status"] = "REJECTED"
    pr["rejected_by"] = rejected_by
    pr["rejected_ts_utc"] = datetime.now(timezone.utc).isoformat()
    pr["rejected_reason"] = reason
    _save_proposal(workspace, pr)
    return pr


def _append_changelog(workspace: Path, entry: Dict[str, Any]) -> None:
    p = _changelog_path(workspace)
    line = json.dumps(entry, default=str)
    if p.exists():
        p.write_text(p.read_text(encoding="utf-8") + line + "\n", encoding="utf-8")
    else:
        p.write_text(line + "\n", encoding="utf-8")


def read_changelog(workspace: Path, limit: int = 200) -> List[Dict[str, Any]]:
    p = _changelog_path(workspace)
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8").splitlines()
    out: List[Dict[str, Any]] = []
    for ln in reversed(lines[-limit:]):
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out


def apply_proposal(workspace: Path, proposal_id: str, applied_by: str = "human") -> Dict[str, Any]:
    pr = load_proposal(workspace, proposal_id)

    if pr["status"] != "APPROVED":
        raise ValueError("Apply is only allowed after approval.")

    # Re-check guardrails right before apply (important!)
    guard = check_guardrails(pr["proposed_config"])
    pr["guardrails"] = guard
    if not guard["ok"]:
        _save_proposal(workspace, pr)
        raise ValueError("Blocked by safety rules. Fix violations and create a new change request.")

    # Lineage enforcement: base hash must match current
    cur_cfg = load_config(workspace)
    cur_hash = config_hash(cur_cfg)
    if cur_hash != pr["base_config_hash"]:
        raise ValueError(
            "Settings changed since this change request was created. "
            "Please create a new change request from the latest settings."
        )

    # Apply
    save_config(workspace, pr["proposed_config"])
    snap = snapshot_config(workspace, pr["proposed_config"])

    pr["status"] = "APPLIED"
    pr["applied_by"] = applied_by
    pr["applied_ts_utc"] = datetime.now(timezone.utc).isoformat()
    pr["applied_from_hash"] = cur_hash
    pr["applied_to_hash"] = pr["proposed_config_hash"]
    pr["config_snapshot_path"] = str(snap)

    _save_proposal(workspace, pr)

    _append_changelog(
        workspace,
        {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "proposal_id": pr["proposal_id"],
            "from_settings_id": _short(cur_hash),
            "to_settings_id": _short(pr["proposed_config_hash"]),
            "status": "APPLIED",
            "snapshot": str(snap),
            "applied_by": applied_by,
            "num_changes": len(pr.get("changes", [])),
        },
    )

    return pr
