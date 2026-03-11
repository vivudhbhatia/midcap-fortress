from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from mfp.config.normalize import normalize_config
from mfp.data.universe import get_universe


@dataclass(frozen=True)
class StrategySpec:
    id: str
    label: str
    enabled: bool
    allocation_pct: float
    engine: str
    universe: str
    symbols: List[str]
    proxy: str
    max_symbols: int
    overrides: Dict[str, Any]


def load_strategy_specs(cfg_raw: Dict[str, Any]) -> List[StrategySpec]:
    cfg = normalize_config(cfg_raw)
    sdict = cfg.get("portfolio", {}).get("strategies", {})
    out: List[StrategySpec] = []

    for sid, spec in sdict.items():
        if not isinstance(spec, dict):
            continue
        out.append(
            StrategySpec(
                id=str(sid),
                label=str(spec.get("label", sid)),
                enabled=bool(spec.get("enabled", True)),
                allocation_pct=float(spec.get("allocation_pct", 0.0)),
                engine=str(spec.get("engine", "pulse_mr")),
                universe=str(spec.get("universe", "") or ""),
                symbols=[str(x).upper() for x in (spec.get("symbols") or [])],
                proxy=str(spec.get("proxy", "") or ""),
                max_symbols=int(spec.get("max_symbols", 60)),
                overrides=dict(spec.get("overrides") or {}),
            )
        )

    return out


def resolve_symbols(spec: StrategySpec) -> List[str]:
    if spec.symbols:
        return list(spec.symbols)
    if spec.universe:
        syms = get_universe(spec.universe)
        return syms[: max(1, spec.max_symbols)]
    return []
