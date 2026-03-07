from __future__ import annotations

import json
from pathlib import Path

import matplotlib.pyplot as plt
from jinja2 import Template


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=2), encoding="utf-8", newline="\n")


def _write_text(path: Path, txt: str) -> None:
    path.write_text(txt, encoding="utf-8", newline="\n")


def build_report_bundle(out_dir: Path, backtest_result: dict) -> dict:
    eq = backtest_result["equity"]
    dd = backtest_result["drawdown"]
    trades = backtest_result["trades"]
    metrics = backtest_result["metrics"]
    meta = backtest_result["meta"]

    out_dir.mkdir(parents=True, exist_ok=True)

    # Equity chart
    equity_png = out_dir / "equity_curve.png"
    plt.figure()
    eq.plot(legend=False)
    plt.title("Equity Curve")
    plt.tight_layout()
    plt.savefig(equity_png)
    plt.close()

    # Drawdown chart
    dd_png = out_dir / "drawdown_curve.png"
    plt.figure()
    dd.plot(legend=False)
    plt.title("Drawdown")
    plt.tight_layout()
    plt.savefig(dd_png)
    plt.close()

    # Trades
    trades_path = out_dir / "trades.csv"
    trades.to_csv(trades_path, index=False)

    # Metrics
    metrics_path = out_dir / "metrics.json"
    _write_json(metrics_path, metrics)

    # Markdown report
    md_path = out_dir / "report.md"
    md = f"""# Backtest Report

## Meta
- timeframe: `{meta["timeframe"]}`
- start: `{meta["start"]}`
- end: `{meta["end"]}`
- universe: `{meta["universe_name"]}`
- strategy: `{meta["strategy_name"]}`
- max_symbols: `{meta.get("max_symbols")}`
- data_provider: `{meta.get("data_provider")}`
- symbols_requested_count: `{meta.get("symbols_requested_count")}`
- symbols_used_count: `{meta.get("symbols_used_count")}`

## Key metrics
- Final equity: **${metrics["final_equity"]:.2f}**
- CAGR: **{metrics["cagr"] * 100:.2f}%**
- Max drawdown: **{metrics["max_drawdown"] * 100:.2f}%**
- Trades: **{metrics["num_trades"]}**
- Win rate: **{metrics["win_rate"] * 100:.2f}%**
- Profit factor: **{metrics["profit_factor"]:.2f}**
- Sharpe (0% RF): **{metrics["sharpe_0rf"]:.2f}**

## Notes / Caveats
- Static universe file can cause survivorship bias.
- Yahoo Finance coverage varies by ticker.
- Stop simulation uses OHLC lows (path-dependent effects not fully modeled).
"""
    _write_text(md_path, md)

    # HTML report
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
        <li>data_provider: {{ meta.data_provider }}</li>
        <li>symbols_requested_count: {{ meta.symbols_requested_count }}</li>
        <li>symbols_used_count: {{ meta.symbols_used_count }}</li>
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

      <h2>Equity</h2>
      <img src="equity_curve.png" style="max-width: 900px;">

      <h2>Drawdown</h2>
      <img src="drawdown_curve.png" style="max-width: 900px;">
    </body></html>
    """)
    rendered = tmpl.render(meta=meta, metrics=type("M", (), metrics))
    _write_text(html_path, rendered)

    return {
        "paths": [
            equity_png,
            dd_png,
            trades_path,
            metrics_path,
            md_path,
            html_path,
            out_dir / "equity.csv",
            out_dir / "drawdown.csv",
        ]
    }


def write_gap_report(out_dir: Path, gap_report: dict) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / "gap_report.json"
    mp = out_dir / "gap_report.md"

    _write_json(jp, gap_report)

    lines = []
    lines.append("# Gap Report\n")
    lines.append(f"- timeframe: `{gap_report.get('timeframe')}`")
    lines.append(f"- dd_threshold: `{gap_report.get('dd_threshold')}`")
    lines.append(f"- overall_max_drawdown: `{gap_report.get('overall_max_drawdown')}`")
    lines.append(f"- pass_drawdown_rule: `{gap_report.get('pass_drawdown_rule')}`\n")

    lines.append("## Rolling window drawdowns\n")
    lines.append("| window | max_dd | peak_date | trough_date |")
    lines.append("|---:|---:|---|---|")
    for r in gap_report.get("rolling", []):
        lines.append(f"| {r['window']} | {r['max_dd']:.4f} | {r['peak_date']} | {r['trough_date']} |")

    v = gap_report.get("violations", [])
    lines.append("\n## Violations\n")
    if not v:
        lines.append("- None ✅")
    else:
        for x in v:
            lines.append(
                f"- window={x['window']} max_dd={x['max_dd']:.4f} (peak {x['peak_date']} -> trough {x['trough_date']})"
            )

    _write_text(mp, "\n".join(lines) + "\n")
    return [jp, mp]


def write_sweep_report(out_dir: Path, sweep: dict) -> list[Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    jp = out_dir / "sweep_metrics.json"
    mp = out_dir / "sweep_report.md"
    hp = out_dir / "sweep_report.html"

    _write_json(jp, sweep)

    rows = sweep.get("rows", [])

    # Markdown
    lines = []
    lines.append("# Backtest Sweep Report\n")
    lines.append("## Comparison\n")
    lines.append("| timeframe | CAGR | MaxDD | Trades | Win% | pass_3pct_window_dd | worst_window_dd |")
    lines.append("|---|---:|---:|---:|---:|---|---:|")
    for r in rows:
        lines.append(
            f"| {r['timeframe']} | {r['cagr'] * 100:.2f}% | {r['max_drawdown'] * 100:.2f}% | {r['num_trades']} | {r['win_rate'] * 100:.2f}% | "
            f"{'YES' if r['pass_drawdown_rule'] else 'NO'} | {r['worst_window_dd'] * 100:.2f}% |"
        )
    _write_text(mp, "\n".join(lines) + "\n")

    # HTML
    tmpl = Template("""
    <html><head><meta charset="utf-8"><title>Backtest Sweep</title></head>
    <body>
      <h1>Backtest Sweep Report</h1>
      <table border="1" cellpadding="6" cellspacing="0">
        <tr>
          <th>timeframe</th><th>CAGR</th><th>MaxDD</th><th>Trades</th><th>Win%</th><th>Pass 3% window DD</th><th>Worst window DD</th>
        </tr>
        {% for r in rows %}
        <tr>
          <td>{{ r.timeframe }}</td>
          <td>{{ "%.2f"|format(r.cagr*100) }}%</td>
          <td>{{ "%.2f"|format(r.max_drawdown*100) }}%</td>
          <td>{{ r.num_trades }}</td>
          <td>{{ "%.2f"|format(r.win_rate*100) }}%</td>
          <td>{{ "YES" if r.pass_drawdown_rule else "NO" }}</td>
          <td>{{ "%.2f"|format(r.worst_window_dd*100) }}%</td>
        </tr>
        {% endfor %}
      </table>
      <p>Each timeframe folder contains its own full evidence pack (report, trades, gap report, etc.).</p>
    </body></html>
    """)
    rendered = tmpl.render(rows=[type("R", (), r) for r in rows])
    _write_text(hp, rendered)

    return [jp, mp, hp]
