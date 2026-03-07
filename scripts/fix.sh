#!/usr/bin/env bash
set -euo pipefail

python -m pip install -e ".[dev]" >/dev/null

# Auto-fix what can be fixed (imports, unused imports, etc.)
ruff check --fix src tests

# Format code (safe; won't “reflow” strings, but standardizes layout)
ruff format src tests

echo "✅ Auto-fix complete"
