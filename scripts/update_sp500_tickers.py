from __future__ import annotations

import csv
from pathlib import Path

import requests


CSV_URL = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _out_path() -> Path:
    return _repo_root() / "src" / "mfp" / "data" / "sp500_tickers.txt"


def main() -> None:
    out_path = _out_path()
    out_path.parent.mkdir(parents=True, exist_ok=True)

    headers = {
        # Helps avoid “bot” blocking on many hosts
        "User-Agent": "midcap-fortress/1.0 (+paper; research)",
    }
    r = requests.get(CSV_URL, headers=headers, timeout=30)
    r.raise_for_status()

    reader = csv.DictReader(r.text.splitlines())
    tickers: list[str] = []
    for row in reader:
        sym = (row.get("Symbol") or "").strip().upper()
        if not sym:
            continue
        tickers.append(sym)

    tickers = sorted(set(tickers))

    out_path.write_text("\n".join(tickers) + "\n", encoding="utf-8", newline="\n")
    print(f"Wrote {len(tickers)} tickers to {out_path}")


if __name__ == "__main__":
    main()