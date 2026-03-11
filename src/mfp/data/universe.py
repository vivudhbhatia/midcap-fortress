from __future__ import annotations

from typing import List

from mfp.data.universe_sp400 import get_universe_sp400
from mfp.data.universe_sp500 import get_universe_sp500


def _parse_custom(name: str) -> List[str]:
    # formats:
    #   custom:IJH,SPY
    #   symbols:IJH,SPY
    #   etf:SPY,MDY
    _, rest = name.split(":", 1)
    parts = [p.strip().upper().replace(".", "-") for p in rest.split(",") if p.strip()]
    # de-dupe, keep order
    out: List[str] = []
    seen = set()
    for s in parts:
        if s not in seen:
            out.append(s)
            seen.add(s)
    return out


def get_universe(name: str) -> List[str]:
    """
    name:
      - sp400 / mid / midcap
      - sp500 / large / largecap
      - both / mid+large
      - custom:IJH,SPY  (explicit list)
    """
    n = (name or "sp400").strip()
    nlow = n.lower()

    if ":" in n and nlow.split(":", 1)[0] in {"custom", "symbols", "etf"}:
        return _parse_custom(n)

    if nlow in {"sp400", "mid", "midcap"}:
        return get_universe_sp400()

    if nlow in {"sp500", "large", "largecap"}:
        return get_universe_sp500()

    if nlow in {"both", "mid+large", "large+mid"}:
        # stable order: SP500 first, then add SP400 extras
        sp500 = get_universe_sp500()
        s = set(sp500)
        merged = list(sp500)
        merged.extend([x for x in get_universe_sp400() if x not in s])
        return merged

    raise ValueError(f"Unknown universe name: {name!r}. Use sp400, sp500, both, or custom:AAA,BBB.")
