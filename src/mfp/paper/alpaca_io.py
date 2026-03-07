from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import pandas as pd
import requests
from alpaca.data.enums import DataFeed
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockBarsRequest
from alpaca.data.timeframe import TimeFrame
from alpaca.trading.client import TradingClient
from alpaca.trading.enums import QueryOrderStatus
from alpaca.trading.requests import GetOrdersRequest
from dotenv import load_dotenv


def _env(name: str, default: Optional[str] = None) -> str:
    v = os.getenv(name, default)
    if v is None or v == "":
        raise RuntimeError(f"Missing required env var: {name}")
    return v


def load_env_once() -> None:
    # loads .env for local runs; harmless in GitHub Actions
    load_dotenv()


def is_trading_enabled() -> bool:
    return os.getenv("MFP_TRADING_ENABLED", "false").strip().lower() == "true"


def is_paper() -> bool:
    return os.getenv("MFP_ALPACA_PAPER", "true").strip().lower() == "true"


def alpaca_trading_client() -> TradingClient:
    load_env_once()
    key = _env("ALPACA_API_KEY")
    sec = _env("ALPACA_API_SECRET")
    return TradingClient(key, sec, paper=is_paper())


def alpaca_data_client() -> StockHistoricalDataClient:
    load_env_once()
    key = _env("ALPACA_API_KEY")
    sec = _env("ALPACA_API_SECRET")
    return StockHistoricalDataClient(key, sec)


def get_account_snapshot(tc: TradingClient) -> Dict[str, Any]:
    acct = tc.get_account()
    # pydantic model -> dict
    try:
        d = acct.model_dump()
    except Exception:
        d = acct.dict()  # older pydantic compatibility
    return d


def get_positions_snapshot(tc: TradingClient) -> List[Dict[str, Any]]:
    pos = tc.get_all_positions()
    out = []
    for p in pos:
        try:
            out.append(p.model_dump())
        except Exception:
            out.append(p.dict())
    return out


def get_open_orders_snapshot(tc: TradingClient) -> List[Dict[str, Any]]:
    req = GetOrdersRequest(status=QueryOrderStatus.OPEN)
    orders = tc.get_orders(filter=req)
    out = []
    for o in orders:
        try:
            out.append(o.model_dump())
        except Exception:
            out.append(o.dict())
    return out


def fetch_daily_bars(
    dc: StockHistoricalDataClient,
    symbols: List[str],
    lookback_days: int = 420,
    feed: str = "iex",
) -> Dict[str, pd.DataFrame]:
    """
    Returns dict[symbol] -> DataFrame indexed by timestamp with columns:
      Open, High, Low, Close, Volume
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=lookback_days)

    feed_enum = DataFeed.IEX if feed.lower() == "iex" else DataFeed.SIP

    req = StockBarsRequest(
        symbol_or_symbols=symbols,
        timeframe=TimeFrame.Day,
        start=start,
        end=end,
        feed=feed_enum,
    )

    df = dc.get_stock_bars(req).df
    # MultiIndex: (symbol, timestamp)
    out: Dict[str, pd.DataFrame] = {}
    if df is None or len(df) == 0:
        return out

    for sym in symbols:
        try:
            sub = df.xs(sym, level=0).copy()
        except Exception:
            continue

        # Alpaca columns are lowercase
        rename = {
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
        sub = sub.rename(columns=rename)
        need = ["Open", "High", "Low", "Close", "Volume"]
        if not all(c in sub.columns for c in need):
            continue
        sub = sub[need].dropna()
        if len(sub) < 50:
            continue
        out[sym] = sub
    return out


def fetch_portfolio_history_raw(period: str = "1M", timeframe: str = "1D") -> Dict[str, Any]:
    """
    Alpaca trading endpoint (not exposed in TradingClient in some versions).
    Uses REST directly to fetch account portfolio history for drawdown gating.
    """
    load_env_once()
    key = _env("ALPACA_API_KEY")
    sec = _env("ALPACA_API_SECRET")

    base = "https://paper-api.alpaca.markets" if is_paper() else "https://api.alpaca.markets"
    url = f"{base}/v2/account/portfolio/history"
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": sec,
    }
    r = requests.get(url, headers=headers, params={"period": period, "timeframe": timeframe}, timeout=30)
    r.raise_for_status()
    return r.json()
