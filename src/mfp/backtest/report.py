from __future__ import annotations
from pathlib import Path
import json
import matplotlib.pyplot as plt
from jinja2 import Template

def build_report_bundle(out_dir: Path, backtest_result: dict) -> dict:
    eq = backtest_result["equity"]
    trades = backtest_result["trades"]
    metrics = backtest_result["metrics"]
    meta = backtest_result["meta"]

    # Save equity chart
    chart_path = out_dir / "equity_curve.png"
    plt.figure()
    eq.plot(legend=False)
    plt.title("Equity Curve")
    plt.tight_layout()
    plt.savefig(chart_path)
    plt.close()

    # Write trades
    trades_path = out_dir / "trades.csv"
    trades.to_csv(trades_path, index=False)

    # Write metrics (UTF-8 for consistency)
    metrics_path = out_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8", newline="\n")

    # Markdown report (UTF-8)
    md_path = out_dir / "report.md"
    md = f"""# Backtest Report

## Meta
- timeframe: `{meta['timeframe']}`
- start: `{meta['start']}`
- end: `{meta['end']}`
- universe: `{meta['universe_name']}`
- strategy: `{meta['strategy_name']}`
- max_symbols: `{meta.get('max_symbols')}`

## Key metrics
- Final equity: **${metrics['final_equity']:.2f}**
- CAGR: **{metrics['cagr']*100:.2f}%**
- Max drawdown: **{metrics['max_drawdown']*100:.2f}%**
- Trades: **{metrics['num_trades']}**
- Win rate: **{metrics['win_rate']*100:.2f}%**
- Profit factor: **{metrics['profit_factor']:.2f}**
- Sharpe (0% RF): **{metrics['sharpe_0rf']:.2f}**

## Notes / Caveats
- This backtest uses a **static universe file**, which can cause survivorship bias.
- Yahoo Finance data quality/availability varies by ticker.
- Stops are simulated using OHLC bar lows (path-dependent effects not modeled).

## Files produced
- `equity.csv`, `drawdown.csv`, `trades.csv`, `metrics.json`, `equity_curve.png`
"""
    md_path.write_text(md, encoding="utf-8", newline="\n")

    # HTML report (UTF-8)
    html_path = out_dir / "report.html"
    tmpl = Template("""
    <html><head><meta charset="utf-8"><title>Backtest Report</title></head>
    <body>
      <h1>Backtest Report</h1>
      <h2>Meta</h2>
      <ul>
        <li>timeframe: {{ meta.timeframe }}</li>
        <li>start: {{ meta.start }}</li>
        <li>end: {{ meta.end }}</li>
        <li>universe: {{ meta.universe_name }}</li>
        <li>strategy: {{ meta.strategy_name }}</li>
        <li>max_symbols: {{ meta.max_symbols }}</li>
      </ul>

      <h2>Key metrics</h2>
      <ul>
        <li>Final equity: ${{ "%.2f"|format(metrics.final_equity) }}</li>
        <li>CAGR: {{ "%.2f"|format(metrics.cagr*100) }}%</li>
        <li>Max drawdown: {{ "%.2f"|format(metrics.max_drawdown*100) }}%</li>
        <li>Trades: {{ metrics.num_trades }}</li>
        <li>Win rate: {{ "%.2f"|format(metrics.win_rate*100) }}%</li>
        <li>Profit factor: {{ "%.2f"|format(metrics.profit_factor) }}</li>
        <li>Sharpe (0% RF): {{ "%.2f"|format(metrics.sharpe_0rf) }}</li>
      </ul>

      <h2>Equity curve</h2>
      <img src="equity_curve.png" style="max-width: 900px;">

      <h2>Notes</h2>
      <ul>
        <li>Static universe -> survivorship bias risk</li>
        <li>Yahoo Finance coverage varies</li>
        <li>OHLC stop simulation is approximate</li>
      </ul>
    </body></html>
    """)
    rendered = tmpl.render(meta=meta, metrics=type("M", (), metrics))
    html_path.write_text(rendered, encoding="utf-8", newline="\n")

    return {
        "paths": [
            chart_path,
            trades_path,
            metrics_path,
            md_path,
            html_path,
            out_dir / "equity.csv",
            out_dir / "drawdown.csv",
        ]
    }
