from __future__ import annotations

import pandas as pd

from mfp.objectives.evaluator import evaluate_objectives


def test_objectives_eval_smoke() -> None:
    # synthetic equity curve rising 1% per day for 300 days
    s = pd.Series([100.0 * (1.01**i) for i in range(300)])
    res = evaluate_objectives(
        s, {"target_annual_cagr": 0.20, "acceptance_factor": 0.5, "return_windows": ["1Y"]}
    )
    assert "pass" in res
    assert "drawdown" in res
