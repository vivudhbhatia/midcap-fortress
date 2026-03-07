from __future__ import annotations

from datetime import datetime
from pathlib import Path

import typer
from dotenv import load_dotenv

from mfp.ui.github_opsbot import run_command

app = typer.Typer(no_args_is_help=True)

# Load .env for local use (safe in CI too)
load_dotenv()


@app.command()
def status():
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
    if end is None:
        end = datetime.utcnow().strftime("%Y-%m-%d")

    cmd = (
        f"/mfp backtest timeframe={timeframe} start={start} end={end} universe={universe} strategy={strategy}"
    )
    if max_symbols:
        cmd += f" max_symbols={max_symbols}"

    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def backtest_sweep(
    start: str = typer.Option("2011-01-01", help="YYYY-MM-DD"),
    end: str = typer.Option(None, help="YYYY-MM-DD (default: today UTC)"),
    max_symbols: int = typer.Option(0, help="Limit tickers for speed (0 = all in universe file)"),
    universe: str = typer.Option("sp400"),
    strategy: str = typer.Option("midcap_pulse_v1"),
):
    if end is None:
        end = datetime.utcnow().strftime("%Y-%m-%d")

    cmd = f"/mfp backtest-sweep start={start} end={end} universe={universe} strategy={strategy}"
    if max_symbols:
        cmd += f" max_symbols={max_symbols}"

    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def gaps(
    timeframe: str = typer.Option("1d", help="1d, 1wk, or 1mo"),
    start: str = typer.Option("2011-01-01", help="YYYY-MM-DD"),
    end: str = typer.Option(None, help="YYYY-MM-DD (default: today UTC)"),
    max_symbols: int = typer.Option(0, help="Limit tickers for speed (0 = all in universe file)"),
    universe: str = typer.Option("sp400"),
    strategy: str = typer.Option("midcap_pulse_v1"),
):
    if end is None:
        end = datetime.utcnow().strftime("%Y-%m-%d")

    cmd = f"/mfp gaps timeframe={timeframe} start={start} end={end} universe={universe} strategy={strategy}"
    if max_symbols:
        cmd += f" max_symbols={max_symbols}"

    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


# ---------------- Paper trading ----------------


@app.command()
def paper_status():
    res = run_command("/mfp paper-status", workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def paper_cycle(
    max_symbols: int = typer.Option(60, help="How many universe symbols to use"),
    dry_run: bool = typer.Option(True, help="If true, do not place orders (plan only)"),
):
    cmd = f"/mfp paper-cycle max_symbols={max_symbols} dry_run={'true' if dry_run else 'false'}"
    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def paper_reconcile(
    max_symbols: int = typer.Option(60, help="How many universe symbols to use"),
    place_stops: bool = typer.Option(
        False, help="If true, place protective stops (requires MFP_TRADING_ENABLED=true)"
    ),
):
    cmd = f"/mfp paper-reconcile max_symbols={max_symbols} place_stops={'true' if place_stops else 'false'}"
    res = run_command(cmd, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


@app.command()
def ops(command: str):
    """
    Raw OpsBot command line (local):
      mfp ops "mfp status"
    """
    res = run_command(command, workspace=Path("."))
    typer.echo(res.summary_md)
    raise typer.Exit(code=0 if res.ok else 1)


if __name__ == "__main__":
    app()
