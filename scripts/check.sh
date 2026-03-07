#!/usr/bin/env bash
set -euo pipefail

echo "==> Python version"
python --version

echo "==> Install (dev)"
python -m pip install -e ".[dev]" >/dev/null

echo "==> Compile all (catches syntax/import errors)"
python -m compileall -q src

echo "==> Ruff (lint)"
ruff check src tests

echo "==> Pytest"
pytest -q

echo "✅ All checks passed"
