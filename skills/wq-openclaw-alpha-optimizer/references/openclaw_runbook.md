# OpenClaw Runbook

## Goal
Use the LLM interface you provide to continuously reflect on historical alpha results, generate new FASTEXPR candidates, and push the search toward a strict target bar.

## Required Files
- `wq_submitter_single.py`
- `wqminer/` package
- `skills/wq-openclaw-alpha-optimizer/scripts/run_round.py`
- `llm` config JSON with OpenAI-compatible endpoint and API key

## Stable Defaults
- `concurrency=2`
- `max_wait=300`
- `poll_interval=5`
- `candidate_count=48`
- Use explicit target bar on every run.

## One-Round Command
```bash
python3 skills/wq-openclaw-alpha-optimizer/scripts/run_round.py \
  --workspace . \
  --llm-config /path/to/llm.json \
  --username "$WQ_USERNAME" \
  --password "$WQ_PASSWORD" \
  --target-sharpe 1.25 \
  --target-fitness 1.0 \
  --target-turnover-max 70
```

## Continuous Command
```bash
python3 skills/wq-openclaw-alpha-optimizer/scripts/run_loop.py \
  --workspace . \
  --max-rounds 16 \
  --sleep-seconds 20 \
  --keep-running-after-hit \
  -- \
  --llm-config /path/to/llm.json \
  --username "$WQ_USERNAME" \
  --password "$WQ_PASSWORD" \
  --target-sharpe 1.25 \
  --target-fitness 1.0 \
  --target-turnover-max 70
```

## Outputs Per Round
- Candidate templates: `templates/openclaw_candidates_<tag>.json`
- Submit log: `results/submit_single/openclaw_round_<tag>.log`
- Round report: `results/submit_single/openclaw_round_<tag>_report.json`
- Submit result: `results/submit_single/submit_<timestamp>.csv|json|jsonl`

## Round Report Fields
- `generation.llm_candidates`: valid candidates coming from your LLM endpoint
- `generation.reject_stats`: reasons rejected before submit
- `bar.bar_hit_count`: strict hit count for the configured target bar
- `bar.top_near_bar`: closest successful rows for next-round prompt context

## Deployment Notes
- Install dependencies before running: `pip install -r requirements.txt`.
- Keep credentials outside source control.
- If LLM generation quality degrades, increase `--llm-attempts` or tighten `--llm-instruction`.
- If API rate limits occur, reduce `--concurrency` and increase `--max-wait`.
- Use `--keep-running-after-hit` to keep exploring after the first bar hit.
