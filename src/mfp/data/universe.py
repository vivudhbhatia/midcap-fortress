from __future__ import annotations

from typing import List

from mfp.data.universe_sp400 import get_universe_sp400
from mfp.data.universe_sp500 import get_universe_sp500


def get_universe(name: str) -> List[str]:
    """
    name:
      - "sp400"  -> S&P MidCap 400 (mid-caps)
      - "sp500"  -> S&P 500 (large-caps)
      - "both"   -> union(sp400, sp500)
    """
    n = (name or "sp400").strip().lower()

    if n in {"sp400", "mid", "midcap"}:
        return get_universe_sp400()

    if n in {"sp500", "large", "largecap"}:
        return get_universe_sp500()

    if n in {"both", "sp900", "mid+large", "large+mid"}:
        # Stable order: keep SP500 first, then add SP400 names not already present.
        sp500 = get_universe_sp500()
        sp400 = get_universe_sp400()
        s = set(sp500)
        merged = list(sp500)
        merged.extend([x for x in sp400 if x not in s])
        return merged

    raise ValueError(f"Unknown universe name: {name!r}. Use one of: sp400, sp500, both.")
