from __future__ import annotations

import io
from pathlib import Path

import pandas as pd
import requests

# Fallback so the repo still works even if the network is blocked
FALLBACK = [
    "AAPL","MSFT","AMZN","NVDA","GOOGL","GOOG","META","TSLA","BRK-B","JPM","V","MA","UNH","XOM","LLY",
    "AVGO","HD","COST","PG","JNJ","ABBV","MRK","PEP","KO","WMT","CSCO","ORCL","CRM","NFLX","BAC",
    "ADBE","TMO","ACN","LIN","MCD","ABT","INTU","AMD","DIS","DHR","NKE","WFC","PM","TXN","QCOM",
    "IBM","GE","AMGN","NOW","CAT","GS","MS","BLK","SPGI","RTX","ISRG","INTC","LOW","UNP","COP",
]


SOURCES = [
    # Usually reliable on corporate networks that allow GitHub
    ("github-datasets", "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/master/data/constituents.csv"),
    # DataHub mirror
    ("datahub", "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"),
]


def fetch_sp500_symbols() -> list[str] | None:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; MidcapFortress/1.0; +https://github.com/)"
    }

    for name, url in SOURCES:
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code != 200:
                print(f"[warn] {name}: HTTP {r.status_code} from {url}")
                continue

            df = pd.read_csv(io.StringIO(r.text))
            # columns typically: Symbol, Name, Sector
            if "Symbol" not in df.columns:
                print(f"[warn] {name}: no Symbol column found")
                continue

            syms = (
                df["Symbol"]
                .astype(str)
                .str.strip()
                .str.replace(".", "-", regex=False)
                .tolist()
            )
            syms = [s for s in syms if s and s != "nan"]

            if len(syms) >= 200:
                print(f"[ok] {name}: fetched {len(syms)} symbols")
                return syms

            print(f"[warn] {name}: fetched only {len(syms)} symbols (too small)")
        except Exception as e:
            print(f"[warn] {name}: failed ({type(e).__name__}: {e})")

    return None


def main() -> None:
    syms = fetch_sp500_symbols()
    if syms is None:
        print("[warn] Could not fetch SP500 list from the network. Writing fallback shortlist.")
        syms = FALLBACK

    out = Path("src/mfp/data/sp500_tickers.txt")
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text("# SP500 tickers (generated)\n" + "\n".join(syms) + "\n", encoding="utf-8", newline="\n")
    print(f"Wrote {len(syms)} tickers -> {out}")


if __name__ == "__main__":
    main()
