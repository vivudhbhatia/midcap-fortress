from __future__ import annotations

import json
from pathlib import Path

from mfp.ui.explain_trade import explain_order, load_run_context


def test_explain_order_smoke(tmp_path: Path) -> None:
    ws = tmp_path
    run_dir = ws / "reports" / "RUN1"
    run_dir.mkdir(parents=True, exist_ok=True)
    (ws / "state").mkdir(parents=True, exist_ok=True)

    # minimal config + qa for explanation
    (ws / "state" / "config.json").write_text(json.dumps({"signal": {"rsi_buy_below": 10}}), encoding="utf-8")
    (run_dir / "qa.json").write_text(
        json.dumps({"pretrade_validation": {"ok": True, "reason": "ok"}}), encoding="utf-8"
    )

    order = {
        "symbol": "AAA",
        "side": "buy",
        "qty": 10,
        "close": 50,
        "sma200": 45,
        "sma5": 52,
        "rsi2": 5,
        "adv20": 30_000_000,
        "atr_pct": 0.05,
    }
    (run_dir / "orderIntents.json").write_text(json.dumps([order]), encoding="utf-8")

    ctx = load_run_context(run_dir, ws)
    exp = explain_order(order, ctx)
    assert "summary" in exp
    assert "signal_checks" in exp
    assert "filter_checks" in exp
