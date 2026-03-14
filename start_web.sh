#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

RESULTS_DIR="${RESULTS_DIR:-$ROOT/results/one_click}"
LIBRARY="${LIBRARY:-$ROOT/templates/library.json}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8002}"

python3 "$ROOT/web_query.py" \
  --results-dir "$RESULTS_DIR" \
  --library "$LIBRARY" \
  --host "$HOST" \
  --port "$PORT"
