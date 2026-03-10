from __future__ import annotations

from importlib.resources import files


def get_universe_sp500() -> list[str]:
    p = files("mfp.data").joinpath("sp500_tickers.txt")
    txt = p.read_text(encoding="utf-8").splitlines()
    tickers: list[str] = []
    for line in txt:
        s = line.strip().upper()
        if not s or s.startswith("#"):
            continue
        tickers.append(s)
    return tickers