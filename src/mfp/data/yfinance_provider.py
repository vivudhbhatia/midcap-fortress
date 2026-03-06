from __future__ import annotations
from pathlib import Path
import pandas as pd
import yfinance as yf
import hashlib
from typing import Dict, List

def _cache_key(tickers: list[str], start: str, end: str) -> str:
    s = ",".join(sorted(tickers)) + f"|{start}|{end}"
    return hashlib.sha256(s.encode()).hexdigest()[:16]

def _chunks(lst: List[str], n: int):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

def load_prices_yf(
    tickers: list[str],
    start: str,
    end: str,
    cache_dir: Path,
    chunk_size: int = 50,
) -> Dict[str, pd.DataFrame]:
    cache_dir.mkdir(parents=True, exist_ok=True)
    key = _cache_key(tickers, start, end)
    cache_file = cache_dir / f"yf_{key}.parquet"

    if cache_file.exists():
        wide = pd.read_parquet(cache_file)
    else:
        frames = []
        for chunk in _chunks(tickers, chunk_size):
            df = yf.download(
                tickers=chunk,
                start=start,
                end=end,
                auto_adjust=True,
                group_by="ticker",
                threads=True,
                progress=False,
            )
            frames.append(df)
        wide = pd.concat(frames, axis=1)
        wide.to_parquet(cache_file)

    out: Dict[str, pd.DataFrame] = {}
    if isinstance(wide.columns, pd.MultiIndex):
        for t in tickers:
            if t in wide.columns.get_level_values(0):
                df = wide[t].dropna()
                df = df.rename(columns=str.title)
                needed = ["Open", "High", "Low", "Close", "Volume"]
                if all(c in df.columns for c in needed):
                    out[t] = df[needed].copy()
    else:
        df = wide.dropna().rename(columns=str.title)
        needed = ["Open", "High", "Low", "Close", "Volume"]
        if all(c in df.columns for c in needed):
            out[tickers[0]] = df[needed].copy()

    return out
