# Midcap Fortress (GitHub OpsBot + Backtester)

This repo provides:
- GitHub Issue command interface (OpsBot): type /mfp commands in Issues or comments
- 15-year backtesting (daily/weekly/monthly) with auditable evidence packs
- Local CLI for the same OpsBot commands

## Local quickstart (Git Bash)
python -m venv .venv
source .venv/Scripts/activate
pip install -e .
mfp ops "/mfp backtest timeframe=1d start=2011-01-01 end=2012-01-01 max_symbols=30"

## GitHub OpsBot
Create an Issue with a line like:
  /mfp backtest timeframe=1d start=2011-01-01 end=2026-03-01 max_symbols=60

GitHub Actions will run and comment results + upload artifacts.
