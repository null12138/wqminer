# WQMiner (Minimal One-Click)

Core flow only:
- Inspiration generation (LLM)
- FASTEXPR template generation
- Simulation and scoring
- Optional evolution rounds
- Auto-append strong results to template library

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

## Output
Each run writes JSON files into `results/one_click/`.
Every file is a list of rows with only these fields:
```json
[
  {"expression": "rank(close)", "sharpe": 1.12, "fitness": 0.34, "turnover": 12.5}
]
```
