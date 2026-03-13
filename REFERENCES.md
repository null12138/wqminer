# References Used

This implementation was aligned with the following open-source projects and files:

## 1) zhutoutoutousan/worldquant-miner
- `generation_two/data_fetcher/data_field_fetcher.py`
  - Category-based dataset discovery (`fundamental/analyst/model/news/alternative`)
  - Dataset -> paginated data-field retrieval flow
  - Region/universe/delay exact-match filtering and local cache pattern
- `generation_two/core/region_config.py`
  - Region default universes and neutralization defaults
- `generation_two/constants/operatorRAW.json`
  - Operator catalog (copied into `wqminer/constants/operatorRAW.json`)
- `generation_one/event-based/mapc2025/template_generator.py`
  - LLM prompt structure that constrains generated templates to known operators/data fields

## 2) RussellDash332/WQ-Brain
- `main.py`
  - WorldQuant authentication and simulation submission/polling structure
  - Simulation settings payload format
- `database.py` / `commands.py`
  - Practical baseline field usage and expression style examples

## Notes
- This project rewrites these ideas into a smaller standalone CLI package (`wqminer`) and does not copy execution pipelines verbatim.
