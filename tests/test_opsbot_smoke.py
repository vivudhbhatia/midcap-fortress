import numpy as np
import pandas as pd

import mfp.backtest.engine as eng
from mfp.ui.github_opsbot import run_command


def _make_ohlcv(start="2016-01-01", end="2020-12-31", seed=1):
    idx = pd.date_range(start, end, freq="B")
    rng = np.random.default_rng(seed)

    rets = rng.normal(0.0002, 0.01, len(idx))
    close = 100.0 * np.exp(np.cumsum(rets))
    close = pd.Series(close, index=idx)

    open_ = close.shift(1).fillna(close.iloc[0])
    span_max = pd.concat([open_, close], axis=1).max(axis=1)
    span_min = pd.concat([open_, close], axis=1).min(axis=1)

    high = span_max * (1 + np.abs(rng.normal(0.001, 0.002, len(idx))))
    low = span_min * (1 - np.abs(rng.normal(0.001, 0.002, len(idx))))
    vol = rng.integers(2_000_000, 8_000_000, len(idx))

    return pd.DataFrame(
        {"Open": open_.values, "High": high.values, "Low": low.values, "Close": close.values, "Volume": vol},
        index=idx,
    )


def test_opsbot_backtest_sweep_offline(tmp_path, monkeypatch):
    def fake_load_prices_yf(tickers, start, end, cache_dir, chunk_size=50):
        base = _make_ohlcv()
        return {t: base.copy() for t in tickers}

    monkeypatch.setattr(eng, "load_prices_yf", fake_load_prices_yf)
    monkeypatch.setattr(eng, "get_universe_sp400", lambda: ["AAA", "BBB"])

    res = run_command("/mfp backtest-sweep start=2018-01-01 end=2019-01-01 max_symbols=2", workspace=tmp_path)
    assert res.ok
    assert res.artifacts_dir is not None
    assert (res.artifacts_dir / "evidence.zip").exists()
