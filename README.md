# WQMiner (Minimal One-Click)

Core flow only:
- Inspiration generation (LLM)
- FASTEXPR template generation
- Simulation and scoring
- Reflection after every round (LLM)
- Optional evolution rounds
- Auto-append strong results to template library
- Reverse factor detection + negated retry
- Infinite loop by default (stop with Ctrl+C)

## Files you use
- `run.py` (main entry, no `-m`)
- `run_config.example.json` (copy to `run_config.json` and edit)
- `fetch_fields.py` (optional field fetcher with pagination + backoff)
- `templates/library.json` (seed + auto-append library)

## Run
Copy and edit config:
```bash
cp run_config.example.json run_config.json
```
Then run:
```bash
python3 run.py --config run_config.json
```

## Key config knobs
- `concurrency_profile`: parallel profile (`advisor`/`balanced`/`safe`/`custom`); default `advisor` auto-lifts legacy low concurrency
- `concurrency`: requested parallel simulations
- `concurrency_cap`: hard cap for parallel simulations (0 = no cap)
- `batch_size`: fixed expressions per round (`8` recommended for strict optimization batches)
- `poll_interval`: seconds between status polls
- `timeout_sec`: HTTP timeout per request (default 90)
- `max_retries`: request retries for transient errors (default 8)
- `max_rounds`: 0 = infinite
- `sleep_between_rounds`: pause between rounds (seconds)
- `reverse_sharpe_max`: trigger reverse when sharpe is below this (default -1.2)
- `reverse_fitness_max`: trigger reverse when fitness is below this (default -1.0)
- `negate_max_per_round`: cap negated retries per round (0 = unlimited)

Strict preflight knobs (optional):
- `strict_validation`: local expression preflight gate before simulation
- `operator_file`: operator spec JSON (supports custom signatures)
- `max_operator_count`: local estimated operator-count ceiling
- `enforce_exact_batch`: require exact `batch_size` per round
- `required_theme_coverage`: minimum A-F theme coverage for a batch
- `common_operator_limit`: max batch usage for common operators
- `template_guide_path`: markdown template guide path (e.g., `temp.md`)
- `template_style_items`: number of template lines injected into each generation prompt
- `template_seed_count`: number of placeholder-rendered template seeds added before LLM generation
- `dataset_ids`: optional selected dataset id list; when set, field cache/fetch only uses these datasets
- `dataset_field_max_pages`: max field pages per selected dataset
- `dataset_field_page_limit`: page size when pulling selected-dataset fields
- `results_append_file`: append each round's core result rows to a text file

## Auto-append to library
Results with `sharpe >= 1.2` and `fitness >= 1.0` are appended to `templates/library.json` (deduped).
Adjust thresholds in `run_config.json`:
```json
"library_output": "templates/library.json",
"library_sharpe_min": 1.2,
"library_fitness_min": 1.0
```

## Fetch fields (optional)
```bash
python3 fetch_fields.py --credentials credentials.json --region USA --delay 1
```
This script paginates datasets and fields, and backs off on 429.

## Validate expressions locally
Single expression:
```bash
python3 -m wqminer.validate --expression "winsorize(rank(close), std=4)" --max-operator-count 8
```
Batch from file:
```bash
python3 -m wqminer.validate --file templates/library.json --operator-file wqminer/constants/operatorRAW.json
```

## Output
Each round writes JSON into `results/one_click/`.
Every file is a list of rows with only these fields:
```json
[
  {"expression": "rank(close)", "sharpe": 1.12, "fitness": 0.34, "turnover": 12.5}
]
```
Reverse detections are logged to `results/one_click/reverse_flags.jsonl` when enabled.

## Web console (control + query)
One process handles everything: start/stop the flow + query results.
```bash
bash start_web.sh
```
Then open `http://localhost:8002` in your browser.
WebUI now supports selecting `region/universe/delay`, parallel profile + concurrency knobs (advisor/balanced/safe/custom), applying preset combos (USA/ASI/JPN etc.), loading dataset list from cache or live API, multi-selecting `dataset_ids`, fallback manual `dataset_ids` input when API fails, and persisting them into `run_config.json` before start.

Optional overrides:
```bash
CONFIG=run_config.json RESULTS_DIR=results/one_click LIBRARY=templates/library.json PORT=8002 bash start_web.sh
```
