#!/usr/bin/env python3
"""Run the one-click flow using run_config.json (no -m required)."""

import argparse
import logging

from wqminer.config import load_run_config
from wqminer import services


def parse_args():
    parser = argparse.ArgumentParser(description="WQMiner one-click flow")
    parser.add_argument("--config", default="run_config.json", help="Run config JSON path")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def _get(cfg: dict, key: str, default):
    value = cfg.get(key, default)
    return default if value is None else value


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    cfg = load_run_config(args.config)

    summary = services.run_one_click(
        region=_get(cfg, "region", "USA"),
        universe=_get(cfg, "universe", ""),
        delay=int(_get(cfg, "delay", 1)),
        llm_config_path=_get(cfg, "llm_config", "llm.json"),
        credentials_path=_get(cfg, "credentials", ""),
        username=_get(cfg, "username", ""),
        password=_get(cfg, "password", ""),
        template_count=int(_get(cfg, "template_count", 20)),
        style_prompt=_get(cfg, "style", ""),
        inspiration=_get(cfg, "inspiration", ""),
        output_dir=_get(cfg, "output_dir", "results/one_click"),
        concurrency=int(_get(cfg, "concurrency", 3)),
        async_mode=bool(_get(cfg, "async_mode", False)),
        timeout_sec=int(_get(cfg, "timeout_sec", 60)),
        max_retries=int(_get(cfg, "max_retries", 5)),
        poll_interval_sec=int(_get(cfg, "poll_interval", 30)),
        max_wait_sec=int(_get(cfg, "max_wait", 600)),
        max_rounds=int(_get(cfg, "max_rounds", 0)),
        sleep_between_rounds=int(_get(cfg, "sleep_between_rounds", 5)),
        evolve_rounds=int(_get(cfg, "evolve_rounds", 0)),
        evolve_count=int(_get(cfg, "evolve_count", 0)),
        evolve_top_k=int(_get(cfg, "evolve_top_k", 6)),
        concurrency_cap=int(_get(cfg, "concurrency_cap", 0)),
        seed_templates=_get(cfg, "seed_templates", ""),
        library_output=_get(cfg, "library_output", ""),
        library_sharpe_min=float(_get(cfg, "library_sharpe_min", 1.2)),
        library_fitness_min=float(_get(cfg, "library_fitness_min", 1.0)),
        reverse_sharpe_max=float(_get(cfg, "reverse_sharpe_max", -1.2)),
        reverse_fitness_max=float(_get(cfg, "reverse_fitness_max", -1.0)),
        reverse_log=_get(cfg, "reverse_log", ""),
        negate_max_per_round=int(_get(cfg, "negate_max_per_round", 0)),
        retry_failed_rounds=int(_get(cfg, "retry_failed_rounds", 2)),
        retry_failed_sleep=int(_get(cfg, "retry_failed_sleep", 2)),
        disable_proxy=bool(_get(cfg, "disable_proxy", False)),
        notify_url=_get(cfg, "notify_url", ""),
        operator_file=_get(cfg, "operator_file", ""),
        strict_validation=bool(_get(cfg, "strict_validation", False)),
        max_operator_count=int(_get(cfg, "max_operator_count", 0)),
        require_keyword_optional=bool(_get(cfg, "require_keyword_optional", True)),
        batch_size=int(_get(cfg, "batch_size", 0)),
        enforce_exact_batch=bool(_get(cfg, "enforce_exact_batch", False)),
        required_theme_coverage=int(_get(cfg, "required_theme_coverage", 0)),
        common_operator_limit=int(_get(cfg, "common_operator_limit", 0)),
        enforce_explore_theme_pairs=bool(_get(cfg, "enforce_explore_theme_pairs", False)),
        template_guide_path=_get(cfg, "template_guide_path", ""),
        template_style_items=int(_get(cfg, "template_style_items", 0)),
        template_seed_count=int(_get(cfg, "template_seed_count", 0)),
        dataset_ids=_get(cfg, "dataset_ids", []),
        dataset_field_max_pages=int(_get(cfg, "dataset_field_max_pages", 5)),
        dataset_field_page_limit=int(_get(cfg, "dataset_field_page_limit", 50)),
        results_append_file=_get(cfg, "results_append_file", ""),
        baseline_alpha_id=_get(cfg, "baseline_alpha_id", ""),
    )

    for path in summary.get("files", []):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
