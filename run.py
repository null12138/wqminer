#!/usr/bin/env python3
"""Run WQMiner flows using run_config.json (no -m required)."""

import argparse
import logging
import os

from wqminer.config import load_run_config
from wqminer import services


def parse_args():
    parser = argparse.ArgumentParser(description="WQMiner unified runner")
    parser.add_argument(
        "--mode",
        default="oneclick",
        choices=["oneclick", "produce"],
        help="oneclick=local generate+simulate, produce=generate(+optional enqueue only)",
    )
    parser.add_argument("--config", default="run_config.json", help="Run config JSON path")
    parser.add_argument("--output-dir", default="", help="Override output dir (oneclick/produce)")
    parser.add_argument("--count", type=int, default=0, help="Override template_count/batch_size")
    parser.add_argument("--enqueue", action="store_true", help="Only for --mode produce: enqueue jobs to Supabase")
    parser.add_argument("--supabase-url", default="", help="Supabase URL (or SUPABASE_URL env)")
    parser.add_argument(
        "--supabase-service-key",
        default="",
        help="Supabase service role key (or SUPABASE_SERVICE_ROLE_KEY env)",
    )
    parser.add_argument("--queue-priority", type=int, default=0, help="Only for --mode produce")
    parser.add_argument("--queue-max-attempts", type=int, default=6, help="Only for --mode produce")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def _get(cfg: dict, key: str, default):
    value = cfg.get(key, default)
    return default if value is None else value


def _guide_path_value(cfg: dict):
    if cfg.get("template_guide_paths"):
        return _get(cfg, "template_guide_paths", [])
    return _get(cfg, "template_guide_path", "")


def _run_produce(args, cfg: dict) -> dict:
    output_dir = args.output_dir or _get(cfg, "producer_output_dir", "results/producer")
    if args.count > 0:
        batch_size = int(args.count)
        template_count = int(args.count)
    else:
        batch_size = int(_get(cfg, "batch_size", 0))
        template_count = int(_get(cfg, "template_count", 64))

    supabase_url = args.supabase_url or str(_get(cfg, "supabase_url", "")) or os.getenv("SUPABASE_URL", "")
    supabase_service_key = (
        args.supabase_service_key
        or str(_get(cfg, "supabase_service_role_key", ""))
        or os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")
    )

    summary = services.produce_templates_only(
        region=_get(cfg, "region", "USA"),
        universe=_get(cfg, "universe", ""),
        delay=int(_get(cfg, "delay", 1)),
        llm_config_path=_get(cfg, "llm_config", "llm.json"),
        credentials_path=_get(cfg, "credentials", ""),
        username=_get(cfg, "username", ""),
        password=_get(cfg, "password", ""),
        template_count=template_count,
        style_prompt=_get(cfg, "style", ""),
        inspiration=_get(cfg, "inspiration", ""),
        timeout_sec=int(_get(cfg, "timeout_sec", 60)),
        max_retries=int(_get(cfg, "max_retries", 5)),
        disable_proxy=bool(_get(cfg, "disable_proxy", False)),
        operator_file=_get(cfg, "operator_file", ""),
        strict_validation=bool(_get(cfg, "strict_validation", False)),
        max_operator_count=int(_get(cfg, "max_operator_count", 0)),
        require_keyword_optional=bool(_get(cfg, "require_keyword_optional", True)),
        batch_size=batch_size,
        enforce_exact_batch=bool(_get(cfg, "enforce_exact_batch", False)),
        required_theme_coverage=int(_get(cfg, "required_theme_coverage", 0)),
        common_operator_limit=int(_get(cfg, "common_operator_limit", 0)),
        enforce_explore_theme_pairs=bool(_get(cfg, "enforce_explore_theme_pairs", False)),
        template_guide_path=_guide_path_value(cfg),
        template_style_items=int(_get(cfg, "template_style_items", 0)),
        template_seed_count=int(_get(cfg, "template_seed_count", 0)),
        seed_templates=_get(cfg, "seed_templates", ""),
        dataset_ids=_get(cfg, "dataset_ids", []),
        dataset_field_max_pages=int(_get(cfg, "dataset_field_max_pages", 5)),
        dataset_field_page_limit=int(_get(cfg, "dataset_field_page_limit", 50)),
        output_dir=output_dir,
        enqueue=bool(args.enqueue),
        queue_priority=int(args.queue_priority),
        queue_max_attempts=int(args.queue_max_attempts),
        queue_batch_table=str(_get(cfg, "queue_batch_table", "alpha_batches")),
        queue_job_table=str(_get(cfg, "queue_job_table", "alpha_jobs")),
        supabase_url=supabase_url,
        supabase_service_key=supabase_service_key,
    )
    print(f"output_file={summary.get('output_file', '')}")
    print(f"count={summary.get('count', 0)}")
    print(f"enqueued_count={summary.get('enqueued_count', 0)}")
    print(f"queue_batch_id={summary.get('queue_batch_id', '')}")
    return summary


def _run_oneclick(args, cfg: dict) -> dict:
    output_dir = args.output_dir or _get(cfg, "output_dir", "results/one_click")
    template_count = int(args.count) if args.count > 0 else int(_get(cfg, "template_count", 64))
    summary = services.run_one_click(
        region=_get(cfg, "region", "USA"),
        universe=_get(cfg, "universe", ""),
        delay=int(_get(cfg, "delay", 1)),
        llm_config_path=_get(cfg, "llm_config", "llm.json"),
        credentials_path=_get(cfg, "credentials", ""),
        username=_get(cfg, "username", ""),
        password=_get(cfg, "password", ""),
        template_count=template_count,
        style_prompt=_get(cfg, "style", ""),
        inspiration=_get(cfg, "inspiration", ""),
        output_dir=output_dir,
        concurrency=int(_get(cfg, "concurrency", 56)),
        concurrency_profile=str(_get(cfg, "concurrency_profile", "advisor")),
        async_mode=bool(_get(cfg, "async_mode", False)),
        timeout_sec=int(_get(cfg, "timeout_sec", 60)),
        max_retries=int(_get(cfg, "max_retries", 5)),
        poll_interval_sec=int(_get(cfg, "poll_interval", 10)),
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
        template_guide_path=_guide_path_value(cfg),
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
    return summary


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    cfg = load_run_config(args.config)
    if args.mode == "produce":
        _run_produce(args, cfg)
    else:
        _run_oneclick(args, cfg)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
