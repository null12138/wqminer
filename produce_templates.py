#!/usr/bin/env python3
"""Generate template batch locally and optionally enqueue jobs to Supabase."""

import argparse
import logging
import os

from wqminer.config import load_run_config
from wqminer.producer import produce_templates_only


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WQMiner producer-only mode")
    parser.add_argument("--config", default="run_config.json", help="Run config path")
    parser.add_argument("--output-dir", default="", help="Override producer output dir")
    parser.add_argument("--count", type=int, default=0, help="Override template count/batch size")
    parser.add_argument("--enqueue", action="store_true", help="Enqueue generated jobs to Supabase")
    parser.add_argument("--supabase-url", default="", help="Supabase project URL (or env SUPABASE_URL)")
    parser.add_argument(
        "--supabase-service-key",
        default="",
        help="Supabase service role key (or env SUPABASE_SERVICE_ROLE_KEY)",
    )
    parser.add_argument("--queue-priority", type=int, default=0, help="Queue priority")
    parser.add_argument("--queue-max-attempts", type=int, default=6, help="Max retry attempts per job")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    return parser.parse_args()


def _get(cfg: dict, key: str, default):
    value = cfg.get(key, default)
    return default if value is None else value


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg = load_run_config(args.config)

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

    summary = produce_templates_only(
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
        template_guide_path=_get(cfg, "template_guide_path", ""),
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
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
