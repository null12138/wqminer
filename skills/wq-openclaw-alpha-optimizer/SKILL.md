---
name: wq-openclaw-alpha-optimizer
description: Run iterative WorldQuant alpha optimization workflows packaged for OpenClaw. Use when you need to repeatedly generate-submit-analyze alpha candidates against hard metric bars (for example sharpe/fitness/turnover), delegate reflection/improvement to a user-provided OpenAI-compatible LLM endpoint, continue multi-round optimization loops, monitor long-running rounds, or package a portable OpenClaw runtime bundle.
---

# WQ OpenClaw Alpha Optimizer

## Overview
Run one-round or multi-round alpha optimization with deterministic orchestration and LLM-in-the-loop candidate generation.
Use the user's LLM config as the only reflection engine.

## Quick Workflow
1. Prepare `llm` and WorldQuant credentials.
2. Run `scripts/run_round.py` for one optimization round.
3. Inspect `openclaw_round_*_report.json` and continue with another round or `scripts/run_loop.py`.
4. Build a portable OpenClaw bundle with `scripts/package_for_openclaw.sh`.

## One Round
Run one round and submit candidates:

```bash
python3 skills/wq-openclaw-alpha-optimizer/scripts/run_round.py \
  --workspace /path/to/wqminer \
  --llm-config /path/to/llm.json \
  --username "your_email" \
  --password "your_password" \
  --target-sharpe 1.25 \
  --target-fitness 1.0 \
  --target-turnover-max 70 \
  --candidate-count 48
```

Use dry-run mode to only generate candidates and report:

```bash
python3 skills/wq-openclaw-alpha-optimizer/scripts/run_round.py \
  --workspace /path/to/wqminer \
  --llm-config /path/to/llm.json \
  --disable-llm \
  --dry-run-submit
```

## Continuous Loop
Run repeated rounds until max rounds, or stop on first bar hit:

```bash
python3 skills/wq-openclaw-alpha-optimizer/scripts/run_loop.py \
  --workspace /path/to/wqminer \
  --max-rounds 12 \
  --sleep-seconds 20 \
  --keep-running-after-hit \
  -- \
  --llm-config /path/to/llm.json \
  --username "your_email" \
  --password "your_password" \
  --target-sharpe 1.25 \
  --target-fitness 1.0 \
  --target-turnover-max 70
```

## OpenClaw Packaging
Create a portable tarball with skill + runtime files:

```bash
bash skills/wq-openclaw-alpha-optimizer/scripts/package_for_openclaw.sh \
  --project-root /path/to/wqminer
```

Monitor a running round log:

```bash
bash skills/wq-openclaw-alpha-optimizer/scripts/monitor_round.sh \
  /path/to/wqminer/results/submit_single/openclaw_round_<tag>.log
```

## Operating Rules
- Pass the user's own LLM endpoint config via `--llm-config`.
- Keep `--concurrency` conservative (default `2`) to reduce rate-limit failures.
- Keep target bars explicit with `--target-*` arguments each run.
- Add `--keep-running-after-hit` when you want uninterrupted continuous optimization.
- Use `openclaw_round_*_report.json` as the source of truth for hit counts and nearest-to-bar candidates.

## References
Read `references/openclaw_runbook.md` for deployment details and stable OpenClaw operating defaults.

## Assets
Use `assets/llm.openclaw.example.json` and `assets/openclaw.env.example` as configuration templates.
