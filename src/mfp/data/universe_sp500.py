from __future__ import annotations

from importlib.resources import files
from typing import List


# Small fallback set so the project works even if the SP500 list is not populated yet.
# You can (and should) regenerate a full SP500 list using scripts/update_sp500_tickers.py
_FALLBACK_LARGECAP: List[str] = [
    "AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","TSLA","BRK-B","JPM","V","MA","UNH","XOM","LLY",
    "AVGO","HD","COST","PG","JNJ","ABBV","MRK","PEP","KO","WMT","CSCO","ORCL","CRM","NFLX","BAC",
    "ADBE","TMO","ACN","LIN","MCD","ABT","INTU","AMD","DIS","DHR","NKE","WFC","PM","TXN","QCOM",
    "IBM","GE","AMGN","NOW","CAT","GS","MS","BLK","SPGI","RTX","ISRG","INTC","LOW","UNP","COP",
]


def _read_packaged_list() -> List[str]:
    p = files("mfp.data").joinpath("sp500_tickers.txt")
    txt = p.read_text(encoding="utf-8")
    syms: List[str] = []
    for line in txt.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        # yfinance uses BRK-B style for dot tickers
        syms.append(s.replace(".", "-"))
    return syms


def get_universe_sp500() -> List[str]:
    """
    Returns a list of large-cap tickers.
    Primary source: packaged sp500_tickers.txt (recommended).
    Fallback: built-in shortlist (so things still run).
    """
    try:
        syms = _read_packaged_list()
        if len(syms) >= 200:
            return syms
    except Exception:
        pass
    return list(_FALLBACK_LARGECAP)
