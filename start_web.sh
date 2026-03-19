#!/usr/bin/env bash
if [ -z "${BASH_VERSION:-}" ]; then
  exec bash "$0" "$@"
fi
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"

CONFIG="${CONFIG:-$ROOT/run_config.json}"
RESULTS_DIR="${RESULTS_DIR:-}"
LIBRARY="${LIBRARY:-}"
HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8002}"
PYTHON_BIN="${PYTHON_BIN:-}"

if [[ -z "$PYTHON_BIN" ]]; then
  if command -v python3.12 >/dev/null 2>&1; then
    PYTHON_BIN="python3.12"
  else
    PYTHON_BIN="python3"
  fi
fi

args=()
if [[ -n "$RESULTS_DIR" ]]; then
  args+=(--results-dir "$RESULTS_DIR")
fi
if [[ -n "$LIBRARY" ]]; then
  args+=(--library "$LIBRARY")
fi

cmd=("$PYTHON_BIN" "$ROOT/web_query.py" --config "$CONFIG")
if ((${#args[@]})); then
  cmd+=("${args[@]}")
fi
cmd+=(--host "$HOST" --port "$PORT")

"${cmd[@]}"
