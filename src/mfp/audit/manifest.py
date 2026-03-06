from __future__ import annotations
from pathlib import Path
from datetime import datetime
import hashlib
import json
import platform
import subprocess

def _sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _git_commit() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    except Exception:
        return "unknown"

def write_manifest(out_dir: Path, bundle: dict, config: dict) -> Path:
    paths = [p for p in bundle.get("paths", []) if p.exists()]
    hashes = {p.name: _sha256_file(p) for p in paths}

    manifest = {
        "timestamp_utc": datetime.utcnow().isoformat() + "Z",
        "git_commit": _git_commit(),
        "platform": {
            "python": platform.python_version(),
            "os": platform.platform(),
        },
        "config": config,
        "artifacts": hashes,
    }

    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8", newline="\n")
    return manifest_path
