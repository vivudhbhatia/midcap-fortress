from __future__ import annotations

from mfp.data.universe_sp400 import get_universe_sp400
from mfp.data.universe_sp500 import get_universe_sp500


def get_universe(name: str) -> list[str]:
    n = (name or "").strip().lower()
    if n in {"sp400", "mid", "midcap"}:
        return get_universe_sp400()
    if n in {"sp500", "large", "largecap"}:
        return get_universe_sp500()
    if n in {"mid+large", "sp400+sp500", "both", "all"}:
        return sorted(set(get_universe_sp400() + get_universe_sp500()))
    raise ValueError(f"Unknown universe: {name!r}. Use sp400, sp500, or mid+large.")