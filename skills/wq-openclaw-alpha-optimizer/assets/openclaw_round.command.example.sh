#!/usr/bin/env bash
set -euo pipefail

python3 skills/wq-openclaw-alpha-optimizer/scripts/run_round.py \
  --workspace . \
  --llm-config ./llm.json \
  --username "$WQ_USERNAME" \
  --password "$WQ_PASSWORD" \
  --target-sharpe 1.25 \
  --target-fitness 1.0 \
  --target-turnover-max 70 \
  --candidate-count 48
