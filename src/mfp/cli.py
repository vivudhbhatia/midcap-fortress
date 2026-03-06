from __future__ import annotations

from datetime import datetime
from pathlib import Path

import typer

from mfp.ui.github_opsbot import run_command

app = typer.Typer(no_args_is_help=True)


@app.command()
def status():
    """
    Show status / latest run info.
    """
    res = run_command("/mfp status", workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def backtest(
    timeframe: str = typer.Option("1d", help="1d, 1wk, or 1mo"),
    start: str = typer.Option("2011-01-01", help="YYYY-MM-DD"),
    end: str = typer.Option(None, help="YYYY-MM-DD (default: today UTC)"),
    max_symbols: int = typer.Option(0, help="Limit tickers for speed (0 = all in universe file)"),
    universe: str = typer.Option("sp400"),
    strategy: str = typer.Option("midcap_pulse_v1"),
):
    """
    Run a backtest and write an auditable evidence pack to reports/<run_id>/.
    """
    if end is None:
        end = datetime.utcnow().strftime("%Y-%m-%d")

    cmd = f"/mfp backtest timeframe={timeframe} start={start} end={end} universe={universe} strategy={strategy}"
    if max_symbols:
        cmd += f" max_symbols={max_symbols}"

    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def ops(command: str):
    """
    Run a raw OpsBot command line.

    IMPORTANT (Git Bash): use prefix `mfp` (NOT `/mfp`) to avoid MSYS path conversion:
      mfp ops "mfp status"
      mfp ops "mfp backtest timeframe=1d start=2018-01-01 end=2019-01-01 max_symbols=15"
    """
    res = run_command(command, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


if __name__ == "__main__":
    app()
