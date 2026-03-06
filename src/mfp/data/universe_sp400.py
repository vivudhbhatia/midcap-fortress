from __future__ import annotations
from importlib.resources import files

def get_universe_sp400() -> list[str]:
    p = files("mfp.data").joinpath("sp400_tickers.txt")
    txt = p.read_text().strip().splitlines()
    tickers = []
    for line in txt:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        tickers.append(line.upper())
    return tickers
