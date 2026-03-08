from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _state_dir(workspace: Path) -> Path:
    d = workspace / "state"
    d.mkdir(parents=True, exist_ok=True)
    return d


def append_runlog(workspace: Path, record: Dict[str, Any]) -> None:
    p = _state_dir(workspace) / "runlog.jsonl"
    record = dict(record)
    record.setdefault("ts_utc", datetime.now(timezone.utc).isoformat())
    p.write_text(
        p.read_text(encoding="utf-8") + json.dumps(record, default=str) + "\n", encoding="utf-8"
    ) if p.exists() else p.write_text(json.dumps(record, default=str) + "\n", encoding="utf-8")


def read_runlog(workspace: Path, limit: int = 200) -> List[Dict[str, Any]]:
    p = _state_dir(workspace) / "runlog.jsonl"
    if not p.exists():
        return []
    lines = p.read_text(encoding="utf-8").splitlines()
    out = []
    for ln in reversed(lines[-limit:]):
        try:
            out.append(json.loads(ln))
        except Exception:
            continue
    return out
