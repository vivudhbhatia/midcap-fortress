from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _extract_files(manifest: Dict[str, Any]) -> List[Dict[str, Any]]:
    # support multiple shapes
    if isinstance(manifest.get("files"), list):
        return [x for x in manifest["files"] if isinstance(x, dict)]
    if isinstance(manifest.get("artifacts"), list):
        return [x for x in manifest["artifacts"] if isinstance(x, dict)]
    return []


def verify_manifest(out_dir: Path) -> Dict[str, Any]:
    man = out_dir / "manifest.json"
    if not man.exists():
        # fall back: find any manifest*.json
        cands = sorted(out_dir.glob("*manifest*.json"))
        if cands:
            man = cands[0]
        else:
            return {"ok": False, "reason": "manifest_not_found", "mismatches": []}

    manifest = json.loads(man.read_text(encoding="utf-8"))
    files = _extract_files(manifest)

    mismatches = []
    checked = 0

    for f in files:
        rel = f.get("path") or f.get("relpath") or f.get("name")
        expected = f.get("sha256") or f.get("hash")
        if not rel or not expected:
            continue
        p = out_dir / rel
        if not p.exists():
            mismatches.append({"path": rel, "expected": expected, "actual": None, "reason": "missing"})
            continue
        actual = _sha256_file(p)
        checked += 1
        if actual != expected:
            mismatches.append(
                {"path": rel, "expected": expected, "actual": actual, "reason": "hash_mismatch"}
            )

    ok = len(mismatches) == 0
    return {"ok": ok, "checked": checked, "mismatches": mismatches, "manifest": str(man)}
