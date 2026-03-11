from __future__ import annotations

from mfp.config.normalize import normalize_config


def test_normalize_has_portfolio_defaults() -> None:
    cfg = normalize_config({})
    assert "portfolio" in cfg
    assert "objectives" in cfg
    assert "strategies" in cfg["portfolio"]
    assert "drawdown_limits" in cfg["objectives"]
