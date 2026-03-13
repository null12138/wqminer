#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "Usage: monitor_round.sh <log-file> [lines]" >&2
  exit 1
fi

LOG_FILE="$1"
LINES="${2:-120}"

if [[ ! -f "$LOG_FILE" ]]; then
  echo "log file not found: $LOG_FILE" >&2
  exit 1
fi

echo "[monitor] tail -n $LINES -f $LOG_FILE"
tail -n "$LINES" -f "$LOG_FILE"
