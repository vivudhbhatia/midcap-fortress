from __future__ import annotations

from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile


def create_evidence_zip(out_dir: Path, zip_name: str = "evidence.zip") -> Path:
    """
    Create a zip containing everything in out_dir (excluding the zip itself).
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / zip_name

    if zip_path.exists():
        zip_path.unlink()

    with ZipFile(zip_path, "w", compression=ZIP_DEFLATED) as z:
        for p in sorted(out_dir.rglob("*")):
            if p.is_file() and p.name != zip_name:
                z.write(p, arcname=p.relative_to(out_dir))

    return zip_path
