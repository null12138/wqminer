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
        poll_interval_sec=int(_get(cfg, "poll_interval", 30)),
        max_wait_sec=int(_get(cfg, "max_wait", 600)),
        evolve_rounds=int(_get(cfg, "evolve_rounds", 0)),
        evolve_count=int(_get(cfg, "evolve_count", 0)),
        evolve_top_k=int(_get(cfg, "evolve_top_k", 6)),
        seed_templates=_get(cfg, "seed_templates", ""),
        library_output=_get(cfg, "library_output", ""),
        library_sharpe_min=float(_get(cfg, "library_sharpe_min", 1.2)),
        library_fitness_min=float(_get(cfg, "library_fitness_min", 1.0)),
    )

    for path in summary.get("files", []):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
