"""Command line entry for wqminer."""

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import List, Tuple

from .config import load_credentials, load_llm_config
from .community_scraper import CommunityTemplateScraper
from .iterative_generator import IterativeTemplateGenerator
from .llm_client import OpenAICompatibleLLM
from .miner import FactorMiner, MiningConfig
from .models import SimulationSettings, TemplateCandidate
from .mutator import ExpressionMutator
from .operator_store import load_operators
from .playwright_auth import interactive_login_and_save_state
from .region_config import get_default_neutralization, get_default_universe
from . import services
from .submitter import submit_expressions_concurrent
from .storage import (
    load_data_fields_cache,
    load_templates,
    save_data_fields_cache,
    save_templates,
)
from .swappable_template_generator import SwappableTemplateGenerator
from .syntax_manual import build_syntax_manual, write_syntax_manual_json, write_syntax_manual_markdown
from .syntax_learning import learn_syntax_from_templates, write_syntax_json, write_syntax_markdown
from .template_generator import TemplateGenerator
from .worldquant_client import WorldQuantBrainClient


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="WorldQuant factor miner with LLM template generation")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")

    sub = parser.add_subparsers(dest="command", required=True)

    fetch = sub.add_parser("fetch-fields", help="Fetch data fields from WorldQuant API")
    _add_region_args(fetch)
    _add_credentials_args(fetch, required=False)
    fetch.add_argument("--output", default="", help="Output cache file path")
    fetch.add_argument("--dataset-output", default="", help="Output data-set list JSON path")
    fetch.add_argument("--full", action="store_true", help="Fetch all datasets and all field pages (exhaustive)")
    fetch.add_argument("--max-datasets", type=int, default=10, help="Max datasets to use (ignored when --full)")
    fetch.add_argument("--field-max-pages", type=int, default=5, help="Max field pages per dataset (ignored when --full)")
    fetch.add_argument("--dataset-max-pages", type=int, default=1, help="Max data-set pages per category (ignored when --full)")
    fetch.add_argument("--dataset-page-limit", type=int, default=50, help="Page size for data-sets query")

    gen = sub.add_parser("gen-templates", help="Generate templates from LLM")
    _add_region_args(gen)
    gen.add_argument("--llm-config", required=True, help="LLM config JSON path")
    gen.add_argument("--fields-file", default="", help="Data fields cache JSON path")
    _add_credentials_args(gen, required=False)
    gen.add_argument("--count", type=int, default=24, help="Number of templates to generate")
    gen.add_argument("--style", default="", help="Additional generation constraints")
    gen.add_argument("--inspiration", default="", help="Extra inspiration text appended to style prompt")
    gen.add_argument("--output", default="templates/generated_templates.json", help="Output template file")
    gen.add_argument("--operators-file", default="", help="Operator JSON file path (default bundled)")

    gen_iter = sub.add_parser("gen-templates-iter", help="Iterative generate templates with syntax checker agent")
    _add_region_args(gen_iter)
    gen_iter.add_argument("--llm-config", required=True, help="LLM config JSON path")
    gen_iter.add_argument("--fields-file", default="", help="Data fields cache JSON path")
    _add_credentials_args(gen_iter, required=False)
    gen_iter.add_argument("--count", type=int, default=48, help="Number of templates to generate")
    gen_iter.add_argument("--rounds", type=int, default=3, help="Iterative rounds")
    gen_iter.add_argument("--max-fix-attempts", type=int, default=1, help="LLM syntax-fix attempts per bad expression")
    gen_iter.add_argument("--style", default="", help="Additional generation constraints")
    gen_iter.add_argument("--inspiration", default="", help="Extra inspiration text appended to style prompt")
    gen_iter.add_argument("--syntax-manual", default="", help="Syntax manual markdown/json used as prompt context")
    gen_iter.add_argument("--operators-file", default="", help="Operator JSON file path (default bundled)")
    gen_iter.add_argument("--output", default="templates/generated_templates_iter.json", help="Output template file")
    gen_iter.add_argument("--report-output", default="results/gen_templates_iter_report.json", help="Iteration report JSON path")

    gen_swap = sub.add_parser("gen-swappable", help="Generate swappable templates and fill them with fields")
    _add_region_args(gen_swap)
    gen_swap.add_argument("--llm-config", required=True, help="LLM config JSON path")
    gen_swap.add_argument("--fields-file", default="", help="Data fields cache JSON path")
    _add_credentials_args(gen_swap, required=False)
    gen_swap.add_argument("--template-count", type=int, default=120, help="Swappable template count")
    gen_swap.add_argument("--fills-per-template", type=int, default=10, help="Field fill trials per template")
    gen_swap.add_argument("--max-expressions", type=int, default=600, help="Max expanded expressions")
    gen_swap.add_argument("--batch-size", type=int, default=24, help="LLM templates per request batch")
    gen_swap.add_argument("--max-rounds", type=int, default=10, help="Max LLM generation rounds")
    gen_swap.add_argument("--style", default="", help="Additional generation constraints")
    gen_swap.add_argument("--syntax-manual", default="docs/fast_expr_syntax_manual.json", help="Syntax manual path")
    gen_swap.add_argument("--operators-file", default="", help="Operator JSON file path (default bundled)")
    gen_swap.add_argument("--output-swappable", default="templates/swappable_templates.json", help="Output swappable template file")
    gen_swap.add_argument("--output-filled", default="templates/swappable_filled_templates.json", help="Output expanded template file")
    gen_swap.add_argument("--report-output", default="results/gen_swappable_report.json", help="Output report JSON path")

    mine = sub.add_parser("mine", help="Mutate and simulate templates")
    _add_region_args(mine)
    mine.add_argument("--templates-file", required=True, help="Template file JSON path")
    mine.add_argument("--fields-file", default="", help="Data fields cache JSON path")
    _add_credentials_args(mine, required=False)
    mine.add_argument("--variants-per-template", type=int, default=8)
    mine.add_argument("--rounds", type=int, default=3)
    mine.add_argument("--max-simulations", type=int, default=200)
    mine.add_argument("--sharpe-threshold", type=float, default=1.25)
    mine.add_argument("--fitness-threshold", type=float, default=1.0)
    mine.add_argument("--output-dir", default="results")
    mine.add_argument("--dry-run", action="store_true", help="Skip API simulation and use synthetic scores")

    optimize = sub.add_parser("optimize", help="Submit-feedback-optimize loop (alias of mine)")
    _add_region_args(optimize)
    optimize.add_argument("--templates-file", required=True, help="Template file JSON path")
    optimize.add_argument("--fields-file", default="", help="Data fields cache JSON path")
    _add_credentials_args(optimize, required=False)
    optimize.add_argument("--variants-per-template", type=int, default=8)
    optimize.add_argument("--rounds", type=int, default=3)
    optimize.add_argument("--max-simulations", type=int, default=200)
    optimize.add_argument("--sharpe-threshold", type=float, default=1.25)
    optimize.add_argument("--fitness-threshold", type=float, default=1.0)
    optimize.add_argument("--output-dir", default="results")
    optimize.add_argument("--dry-run", action="store_true", help="Skip API simulation and use synthetic scores")

    run = sub.add_parser("run", help="End-to-end fetch + generate + mine")
    _add_region_args(run)
    _add_credentials_args(run, required=False)
    run.add_argument("--llm-config", required=True, help="LLM config JSON path")
    run.add_argument("--template-count", type=int, default=20)
    run.add_argument("--variants-per-template", type=int, default=8)
    run.add_argument("--rounds", type=int, default=3)
    run.add_argument("--max-simulations", type=int, default=150)
    run.add_argument("--style", default="")
    run.add_argument("--output-dir", default="results")
    run.add_argument("--dry-run", action="store_true")

    validate = sub.add_parser("validate", help="Validate credential/login, data-field access, and optional simulation")
    _add_region_args(validate)
    _add_credentials_args(validate, required=False)
    validate.add_argument("--no-simulation", action="store_true", help="Skip simulation check")
    validate.add_argument("--expression", default="", help="Simulation expression for validation")
    validate.add_argument("--max-wait", type=int, default=240, help="Max wait seconds for simulation polling")
    validate.add_argument("--poll-interval", type=int, default=5, help="Poll interval seconds for simulation")
    validate.add_argument("--output", default="", help="Validation report output JSON path")

    scrape = sub.add_parser("scrape-templates", help="Scrape pages/files and extract FASTEXPR templates")
    scrape.add_argument(
        "--community-url",
        default="https://support.worldquantbrain.com/hc/zh-cn/community/topics",
        help="Seed community URL",
    )
    scrape.add_argument("--seed-url", action="append", default=[], help="Additional seed URL (repeatable)")
    scrape.add_argument("--input-file", action="append", default=[], help="Local file to extract templates from (repeatable)")
    scrape.add_argument("--max-pages", type=int, default=20, help="Max pages to crawl")
    scrape.add_argument("--timeout", type=int, default=30, help="HTTP timeout seconds")
    scrape.add_argument("--no-mirror", action="store_true", help="Disable r.jina mirror fallback")
    scrape.add_argument("--playwright", action="store_true", help="Use Playwright render fetch before HTTP fetch")
    scrape.add_argument("--playwright-wait", type=int, default=8, help="Seconds to wait after Playwright load")
    scrape.add_argument("--playwright-state", default="", help="Playwright storage_state JSON file")
    scrape.add_argument("--playwright-headful", action="store_true", help="Use headed Playwright for scraping")
    scrape.add_argument("--allow-external", action="store_true", help="Allow crawling cross-domain links")
    scrape.add_argument("--max-templates", type=int, default=5000, help="Max extracted templates to save")
    scrape.add_argument("--output-report", default="results/community_scrape_report.json", help="Scrape report JSON path")
    scrape.add_argument("--output-templates", default="templates/scraped_templates.json", help="Extracted template JSON path")

    login = sub.add_parser("community-login", help="Open Playwright for manual challenge/login and save session state")
    login.add_argument(
        "--start-url",
        default="https://support.worldquantbrain.com/hc/zh-cn/community/topics",
        help="Initial URL to open for manual login/challenge pass",
    )
    login.add_argument("--state-file", default="data/cache/community_storage_state.json", help="Output Playwright storage_state JSON")
    login.add_argument("--user-data-dir", default="data/cache/playwright_profile", help="Persistent browser profile directory")
    login.add_argument("--wait-seconds", type=int, default=240, help="How long to wait for manual login/challenge pass")
    login.add_argument("--output-report", default="results/community_login_report.json", help="Login report JSON path")

    harvest = sub.add_parser("harvest-once", help="One-time harvest: API operators+fields, community templates, syntax learning")
    _add_credentials_args(harvest, required=False)
    harvest.add_argument("--regions", default="USA,GLB,EUR,CHN,ASI,IND", help="Comma-separated regions for field crawling")
    harvest.add_argument("--community-url", default="https://support.worldquantbrain.com/hc/zh-cn/community/topics")
    harvest.add_argument("--seed-url", action="append", default=[], help="Extra community seed URL")
    harvest.add_argument("--input-file", action="append", default=[], help="Extra local input file for template extraction")
    harvest.add_argument("--playwright", action="store_true", help="Use Playwright for community fetch")
    harvest.add_argument("--playwright-headful", action="store_true", help="Run Playwright in headed mode")
    harvest.add_argument("--playwright-state", default="", help="Playwright storage_state file")
    harvest.add_argument("--playwright-wait", type=int, default=8, help="Playwright post-load wait seconds")
    harvest.add_argument("--max-pages", type=int, default=20, help="Max community pages to crawl")
    harvest.add_argument("--max-templates", type=int, default=8000, help="Max templates to save")
    harvest.add_argument("--no-full-fields", action="store_true", help="Disable full dataset/field pagination crawl")
    harvest.add_argument("--max-datasets", type=int, default=10, help="Bounded mode: max datasets per region")
    harvest.add_argument("--field-max-pages", type=int, default=5, help="Bounded mode: max field pages per dataset")
    harvest.add_argument("--dataset-max-pages", type=int, default=1, help="Bounded mode: max data-set pages per category")
    harvest.add_argument("--output-dir", default="results/harvest", help="Output directory for one-time harvest artifacts")

    syntax = sub.add_parser("build-syntax-manual", help="Build FASTEXPR syntax manual for LLM prompting")
    syntax.add_argument("--operators-file", default="", help="Operator JSON file path")
    syntax.add_argument("--templates-file", action="append", default=[], help="Template JSON file(s)")
    syntax.add_argument("--input-file", action="append", default=[], help="Extra raw file(s) for template extraction")
    syntax.add_argument("--output-md", default="docs/fast_expr_syntax_manual.md", help="Output markdown path")
    syntax.add_argument("--output-json", default="docs/fast_expr_syntax_manual.json", help="Output json path")
    syntax.add_argument("--max-pages", type=int, default=0, help="Reserved for compatibility (unused)")

    submit = sub.add_parser("submit-concurrent", help="Submit expressions concurrently to simulation API")
    _add_region_args(submit)
    _add_credentials_args(submit, required=False)
    submit.add_argument("--templates-file", required=True, help="Template JSON file with expressions")
    submit.add_argument("--max-submissions", type=int, default=60, help="Max expressions to submit")
    submit.add_argument("--concurrency", type=int, default=3, help="Concurrent workers")
    submit.add_argument("--max-wait", type=int, default=240, help="Max wait seconds per simulation")
    submit.add_argument("--poll-interval", type=int, default=5, help="Poll interval seconds")
    submit.add_argument("--output-dir", default="results/submissions", help="Output directory")

    return parser


def _add_region_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--region", default="USA")
    parser.add_argument("--universe", default="")
    parser.add_argument("--delay", type=int, default=1)
    parser.add_argument("--neutralization", default="")


def _add_credentials_args(parser: argparse.ArgumentParser, required: bool) -> None:
    parser.add_argument("--credentials", required=required, default="", help="Credential JSON path")
    parser.add_argument("--username", default="", help="WorldQuant username/email")
    parser.add_argument("--password", default="", help="WorldQuant password")


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def default_fields_cache_path(region: str, universe: str, delay: int) -> str:
    return f"data/cache/data_fields_{region}_{delay}_{universe}.json"


def resolve_credentials(args, required: bool = True) -> Tuple[str, str]:
    if args.username and args.password:
        return args.username, args.password

    if args.credentials:
        return load_credentials(args.credentials)

    if required:
        raise ValueError("Need --credentials or both --username and --password")

    return "", ""


def build_client_from_args(args, required: bool = True) -> WorldQuantBrainClient:
    username, password = resolve_credentials(args, required=required)
    client = WorldQuantBrainClient(username=username, password=password)
    client.authenticate()
    return client


def fetch_fields_with_source(
    client: WorldQuantBrainClient,
    region: str,
    universe: str,
    delay: int,
    max_datasets: int = 10,
    field_max_pages: int = 5,
    dataset_max_pages: int = 1,
    dataset_page_limit: int = 50,
) -> Tuple[List, str]:
    fields = client.fetch_data_fields(
        region=region,
        universe=universe,
        delay=delay,
        max_datasets=max_datasets,
        max_pages=field_max_pages,
        dataset_max_pages=dataset_max_pages,
        dataset_page_limit=dataset_page_limit,
    )
    if fields:
        return fields, "api"
    return client.load_fallback_default_fields(), "fallback"


def fetch_fields(client: WorldQuantBrainClient, region: str, universe: str, delay: int) -> List:
    fields, _ = fetch_fields_with_source(client, region, universe, delay)
    return fields


def command_fetch_fields(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    result = services.fetch_fields_and_cache(
        region=region,
        universe=universe,
        delay=args.delay,
        credentials_path=args.credentials,
        username=args.username,
        password=args.password,
        full=bool(args.full),
        max_datasets=args.max_datasets,
        field_max_pages=args.field_max_pages,
        dataset_max_pages=args.dataset_max_pages,
        dataset_page_limit=args.dataset_page_limit,
        output=args.output,
        dataset_output=args.dataset_output,
    )

    logging.info(
        "Fetched %d fields and %d datasets for %s/%s/delay=%s (mode=%s)",
        result["field_count"],
        result["dataset_count"],
        region,
        universe,
        args.delay,
        "full" if args.full else "bounded",
    )
    print(result["fields_path"])
    return 0


def _load_or_fetch_fields(args, client: WorldQuantBrainClient) -> List:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    cache_file = args.fields_file or default_fields_cache_path(region, universe, args.delay)

    if Path(cache_file).exists():
        return load_data_fields_cache(cache_file)

    fields = fetch_fields(client, region=region, universe=universe, delay=args.delay)
    save_data_fields_cache(cache_file, fields)
    return fields


def command_gen_templates(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    result = services.generate_templates(
        region=region,
        universe=universe,
        delay=args.delay,
        llm_config_path=args.llm_config,
        fields_file=args.fields_file,
        credentials_path=args.credentials,
        username=args.username,
        password=args.password,
        count=args.count,
        style_prompt=args.style,
        inspiration=args.inspiration,
        output=args.output,
        operators_file=args.operators_file,
    )

    logging.info("Generated %d templates -> %s", result["template_count"], result["output"])
    print(result["output"])
    return 0


def command_gen_templates_iter(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    syntax_guide = _load_syntax_manual_excerpt(args.syntax_manual)
    result = services.generate_templates_iter(
        region=region,
        universe=universe,
        delay=args.delay,
        llm_config_path=args.llm_config,
        fields_file=args.fields_file,
        credentials_path=args.credentials,
        username=args.username,
        password=args.password,
        count=args.count,
        rounds=args.rounds,
        max_fix_attempts=args.max_fix_attempts,
        style_prompt=args.style,
        inspiration=args.inspiration,
        syntax_guide=syntax_guide,
        output=args.output,
        report_output=args.report_output,
        operators_file=args.operators_file,
    )

    logging.info(
        "Iterative generation done: templates=%d requested=%d output=%s",
        result["template_count"],
        max(1, args.count),
        result["output"],
    )
    print(result["output"])
    return 0 if result["template_count"] else 1


def command_gen_swappable(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)

    if args.fields_file and Path(args.fields_file).exists():
        fields = load_data_fields_cache(args.fields_file)
    else:
        client = build_client_from_args(args, required=True)
        fields = fetch_fields(client, region=region, universe=universe, delay=args.delay)
        save_data_fields_cache(default_fields_cache_path(region, universe, args.delay), fields)

    operators = load_operators(args.operators_file) if args.operators_file else _load_operator_candidates("")
    llm = OpenAICompatibleLLM(load_llm_config(args.llm_config))
    syntax_guide = _load_syntax_manual_excerpt(args.syntax_manual)

    generator = SwappableTemplateGenerator(llm=llm, operators=operators)
    swappable = generator.generate_swappable_templates(
        region=region,
        count=max(1, args.template_count),
        style_prompt=args.style,
        syntax_manual_excerpt=syntax_guide,
        batch_size=max(1, args.batch_size),
        max_rounds=max(1, args.max_rounds),
    )

    expanded, expand_report = generator.expand_templates(
        templates=swappable,
        data_fields=fields,
        max_expressions=max(1, args.max_expressions),
        fills_per_template=max(1, args.fills_per_template),
    )

    swap_meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": args.delay,
        "template_count": len(swappable),
        "field_count": len(fields),
        "operator_count": len(operators),
    }
    SwappableTemplateGenerator.save_swappable_templates(args.output_swappable, swappable, metadata=swap_meta)

    filled_meta = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": args.delay,
        "template_source": args.output_swappable,
        "expanded_count": len(expanded),
        "field_count": len(fields),
    }
    save_templates(args.output_filled, expanded, metadata=filled_meta)

    report = {
        "metadata": {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "region": region,
            "universe": universe,
            "delay": args.delay,
            "template_count_requested": max(1, args.template_count),
            "fills_per_template": max(1, args.fills_per_template),
            "max_expressions": max(1, args.max_expressions),
        },
        "swappable_count": len(swappable),
        "expanded_count": len(expanded),
        "expand_report": expand_report,
        "files": {
            "swappable": args.output_swappable,
            "filled": args.output_filled,
        },
    }
    report_path = Path(args.report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    logging.info(
        "Generated swappable templates=%d expanded=%d -> %s",
        len(swappable),
        len(expanded),
        args.output_filled,
    )
    print(args.output_filled)
    return 0 if expanded else 1


def command_mine(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    report = services.optimize_loop(
        region=region,
        universe=universe,
        delay=args.delay,
        templates_file=args.templates_file,
        fields_file=args.fields_file,
        credentials_path=args.credentials,
        username=args.username,
        password=args.password,
        rounds=args.rounds,
        variants_per_template=args.variants_per_template,
        max_simulations=args.max_simulations,
        sharpe_threshold=args.sharpe_threshold,
        fitness_threshold=args.fitness_threshold,
        output_dir=args.output_dir,
        neutralization=args.neutralization,
        dry_run=args.dry_run,
    )

    logging.info("Mining done: %s", report)
    print(report["result_csv"])
    return 0


def command_run(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    neutralization = args.neutralization or get_default_neutralization(region)

    client = build_client_from_args(args, required=True)

    fields = fetch_fields(client, region=region, universe=universe, delay=args.delay)
    fields_cache = default_fields_cache_path(region, universe, args.delay)
    save_data_fields_cache(fields_cache, fields)

    operators = load_operators()
    llm = OpenAICompatibleLLM(load_llm_config(args.llm_config))
    generator = TemplateGenerator(llm=llm, operators=operators)

    templates = generator.generate_templates(
        region=region,
        data_fields=fields,
        count=args.template_count,
        style_prompt=args.style,
    )

    ts = time.strftime("%Y%m%d_%H%M%S")
    template_file = f"templates/generated_templates_{ts}.json"
    save_templates(
        template_file,
        templates,
        {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "region": region,
            "universe": universe,
            "delay": args.delay,
            "field_count": len(fields),
            "operator_count": len(operators),
        },
    )

    mutator = ExpressionMutator(operators=operators)
    miner_client = None if args.dry_run else client
    miner = FactorMiner(client=miner_client, mutator=mutator, output_dir=args.output_dir)

    report = miner.mine(
        seed_expressions=[tpl.expression for tpl in templates],
        available_field_ids=[f.field_id for f in fields if f.field_id],
        settings=SimulationSettings(
            region=region,
            universe=universe,
            delay=args.delay,
            neutralization=neutralization,
        ),
        config=MiningConfig(
            rounds=args.rounds,
            variants_per_template=args.variants_per_template,
            max_simulations=args.max_simulations,
            dry_run=args.dry_run,
        ),
    )

    logging.info("Run completed. templates=%s report=%s", template_file, report)
    print(report["result_csv"])
    return 0


def command_validate(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    neutralization = args.neutralization or get_default_neutralization(region)

    report = {
        "validated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": args.delay,
        "auth_ok": False,
        "fields": {
            "count": 0,
            "source": "none",
            "sample": [],
        },
        "simulation": {
            "enabled": not args.no_simulation,
            "success": False,
            "expression": "",
            "alpha_id": "",
            "sharpe": 0.0,
            "fitness": 0.0,
            "turnover": 0.0,
            "error": "",
            "link": "",
        },
    }

    try:
        client = build_client_from_args(args, required=True)
        report["auth_ok"] = True
    except Exception as exc:
        report["simulation"]["error"] = f"auth_failed: {exc}"
        _print_validation_report(report, args.output)
        return 1

    fields, source = fetch_fields_with_source(client, region=region, universe=universe, delay=args.delay)
    report["fields"]["count"] = len(fields)
    report["fields"]["source"] = source
    report["fields"]["sample"] = [f.field_id for f in fields[:12]]

    if args.no_simulation:
        ok = report["auth_ok"] and report["fields"]["count"] > 0
        _print_validation_report(report, args.output)
        return 0 if ok else 1

    expression = args.expression.strip() if args.expression else _default_validation_expression(fields)
    report["simulation"]["expression"] = expression

    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=args.delay,
        neutralization=neutralization,
    )

    result = client.simulate_expression(
        expression=expression,
        settings=settings,
        poll_interval_sec=max(1, args.poll_interval),
        max_wait_sec=max(30, args.max_wait),
    )
    report["simulation"]["success"] = bool(result.success)
    report["simulation"]["alpha_id"] = result.alpha_id
    report["simulation"]["sharpe"] = result.sharpe
    report["simulation"]["fitness"] = result.fitness
    report["simulation"]["turnover"] = result.turnover
    report["simulation"]["error"] = result.error_message
    report["simulation"]["link"] = result.link

    ok = report["auth_ok"] and report["fields"]["count"] > 0 and report["simulation"]["success"]
    _print_validation_report(report, args.output)
    return 0 if ok else 1


def command_scrape_templates(args) -> int:
    operators = load_operators()
    scraper = CommunityTemplateScraper(op.get("name", "") for op in operators)

    seed_urls = [args.community_url] + list(args.seed_url or [])
    input_files = list(args.input_file or [])

    report = scraper.scrape(
        seed_urls=seed_urls,
        input_files=input_files,
        max_pages=max(1, args.max_pages),
        timeout_sec=max(5, args.timeout),
        use_mirror=not args.no_mirror,
        use_playwright=bool(args.playwright),
        playwright_wait_sec=max(0, args.playwright_wait),
        playwright_storage_state=args.playwright_state,
        playwright_headless=not bool(args.playwright_headful),
        same_domain_only=not args.allow_external,
    )

    templates_raw = report.get("templates", [])
    if args.max_templates > 0:
        templates_raw = templates_raw[: args.max_templates]

    candidates: List[TemplateCandidate] = []
    for item in templates_raw:
        expr = item.get("expression", "").strip()
        if not expr:
            continue
        source_urls = item.get("source_urls", [])
        source_prompt = "scraped"
        if source_urls:
            source_prompt = "scraped:" + ",".join(source_urls[:3])
        candidates.append(
            TemplateCandidate(
                expression=expr,
                source_prompt=source_prompt,
                fields_used=[],
                operators_used=item.get("operators_used", []),
            )
        )

    metadata = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "source": "community_scraper",
        "seed_urls": seed_urls,
        "input_files": input_files,
        "pages_visited": report.get("summary", {}).get("pages_visited", 0),
        "pages_ok": report.get("summary", {}).get("pages_ok", 0),
        "pages_blocked_or_failed": report.get("summary", {}).get("pages_blocked_or_failed", 0),
        "template_count": len(candidates),
        "playwright": bool(args.playwright),
        "mirror": not args.no_mirror,
    }
    save_templates(args.output_templates, candidates, metadata)

    report_path = Path(args.output_report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    logging.info(
        "Scrape completed. templates=%d pages=%d blocked=%d",
        len(candidates),
        metadata["pages_visited"],
        metadata["pages_blocked_or_failed"],
    )
    print(args.output_templates)
    return 0


def command_community_login(args) -> int:
    report = interactive_login_and_save_state(
        start_url=args.start_url,
        state_file=args.state_file,
        user_data_dir=args.user_data_dir,
        wait_seconds=args.wait_seconds,
    )
    report_path = Path(args.output_report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("ok") else 1


def command_harvest_once(args) -> int:
    ts = time.strftime("%Y%m%d_%H%M%S")
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    client = build_client_from_args(args, required=True)

    # 1) Crawl operators from API
    operators = client.fetch_operators(max_pages=None, limit=50)
    if not operators:
        operators = load_operators()
    operator_file = output_dir / f"operators_api_{ts}.json"
    operator_file.write_text(json.dumps(operators, ensure_ascii=False, indent=2), encoding="utf-8")

    # 2) Crawl region fields/datasets
    regions = [r.strip().upper() for r in args.regions.split(",") if r.strip()]
    region_summary = []
    total_field_count = 0
    total_dataset_count = 0
    field_name_set = set()

    for region in regions:
        universe = get_default_universe(region)
        try:
            if args.no_full_fields:
                fields, datasets = client.fetch_data_fields_and_datasets(
                    region=region,
                    universe=universe,
                    delay=1,
                    max_datasets=max(1, args.max_datasets),
                    max_pages=max(1, args.field_max_pages),
                    dataset_max_pages=max(1, args.dataset_max_pages),
                    dataset_page_limit=50,
                )
                mode = "bounded"
            else:
                fields, datasets = client.fetch_data_fields_and_datasets(
                    region=region,
                    universe=universe,
                    delay=1,
                    max_datasets=None,
                    max_pages=None,
                    dataset_max_pages=None,
                    dataset_page_limit=50,
                )
                mode = "full"
        except Exception as exc:
            region_summary.append(
                {
                    "region": region,
                    "universe": universe,
                    "mode": "error",
                    "dataset_count": 0,
                    "field_count": 0,
                    "error": str(exc),
                }
            )
            continue

        field_file = output_dir / f"data_fields_{region}_1_{universe}_{mode}_{ts}.json"
        dataset_file = output_dir / f"data_sets_{region}_1_{universe}_{mode}_{ts}.json"
        save_data_fields_cache(str(field_file), fields)
        dataset_file.write_text(
            json.dumps(
                {
                    "region": region,
                    "universe": universe,
                    "delay": 1,
                    "mode": mode,
                    "dataset_count": len(datasets),
                    "datasets": datasets,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

        for field in fields:
            if field.field_id:
                field_name_set.add(field.field_id)

        total_field_count += len(fields)
        total_dataset_count += len(datasets)
        region_summary.append(
            {
                "region": region,
                "universe": universe,
                "mode": mode,
                "dataset_count": len(datasets),
                "field_count": len(fields),
                "field_file": str(field_file),
                "dataset_file": str(dataset_file),
                "error": "",
            }
        )

    # 3) Scrape community templates
    op_names = [str(op.get("name", "")).strip() for op in operators if op.get("name")]
    scraper = CommunityTemplateScraper(op_names)

    seed_urls = [args.community_url] + list(args.seed_url or [])
    if "community/posts" not in " ".join(seed_urls):
        seed_urls.append("https://support.worldquantbrain.com/hc/zh-cn/community/posts")

    input_files = list(args.input_file or [])
    for candidate in [
        "data/cache/wqb_topics_mirror.md",
        "/tmp/ref-wq-brain/commands.py",
        "/tmp/ref-worldquant-miner/generation_one/event-based/mapc2025/templateRAW.txt",
    ]:
        if Path(candidate).exists() and candidate not in input_files:
            input_files.append(candidate)

    scrape_report = scraper.scrape(
        seed_urls=seed_urls,
        input_files=input_files,
        max_pages=max(1, args.max_pages),
        timeout_sec=30,
        use_mirror=True,
        use_playwright=bool(args.playwright),
        playwright_wait_sec=max(0, args.playwright_wait),
        playwright_storage_state=args.playwright_state,
        playwright_headless=not bool(args.playwright_headful),
        same_domain_only=True,
    )
    report_file = output_dir / f"community_scrape_report_{ts}.json"
    report_file.write_text(json.dumps(scrape_report, ensure_ascii=False, indent=2), encoding="utf-8")

    raw_templates = scrape_report.get("templates", [])
    raw_templates = raw_templates[: max(1, args.max_templates)]
    candidates: List[TemplateCandidate] = []
    for item in raw_templates:
        expr = item.get("expression", "").strip()
        if not expr:
            continue
        source_urls = item.get("source_urls", [])
        source_prompt = "scraped" if not source_urls else "scraped:" + ",".join(source_urls[:3])
        candidates.append(
            TemplateCandidate(
                expression=expr,
                source_prompt=source_prompt,
                fields_used=[],
                operators_used=item.get("operators_used", []),
            )
        )
    template_file = output_dir / f"scraped_templates_{ts}.json"
    save_templates(
        str(template_file),
        candidates,
        {
            "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "source": "harvest_once",
            "pages_visited": scrape_report.get("summary", {}).get("pages_visited", 0),
            "templates_total": len(candidates),
        },
    )

    # 4) Learn syntax
    syntax_report = learn_syntax_from_templates(
        expressions=[c.expression for c in candidates],
        operator_names=op_names,
        top_k=30,
    )
    syntax_md = output_dir / f"fast_expr_syntax_learned_{ts}.md"
    syntax_json = output_dir / f"fast_expr_syntax_learned_{ts}.json"
    write_syntax_markdown(str(syntax_md), syntax_report)
    write_syntax_json(str(syntax_json), syntax_report)

    manual = build_syntax_manual(
        operators=operators,
        expressions=[c.expression for c in candidates],
    )
    manual_md = output_dir / f"fast_expr_syntax_manual_{ts}.md"
    manual_json = output_dir / f"fast_expr_syntax_manual_{ts}.json"
    write_syntax_manual_markdown(str(manual_md), manual)
    write_syntax_manual_json(str(manual_json), manual)

    # 5) Final summary
    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "regions": regions,
        "operators_count": len(operators),
        "total_dataset_count": total_dataset_count,
        "total_field_count": total_field_count,
        "unique_field_count": len(field_name_set),
        "templates_count": len(candidates),
        "region_summary": region_summary,
        "files": {
            "operators_file": str(operator_file),
            "community_scrape_report": str(report_file),
            "templates_file": str(template_file),
            "syntax_markdown": str(syntax_md),
            "syntax_json": str(syntax_json),
            "syntax_manual_markdown": str(manual_md),
            "syntax_manual_json": str(manual_json),
        },
    }
    summary_file = output_dir / f"harvest_summary_{ts}.json"
    summary_file.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(str(summary_file))
    return 0


def command_build_syntax_manual(args) -> int:
    operators = _load_operator_candidates(args.operators_file)
    op_names = [str(op.get("name", "")).strip() for op in operators if op.get("name")]

    template_files = list(args.templates_file or [])
    if not template_files:
        for candidate in [
            "templates/scraped_templates.json",
            "templates/generated_templates.json",
        ]:
            if Path(candidate).exists():
                template_files.append(candidate)

    expressions: List[str] = []
    for file_path in template_files:
        try:
            expressions.extend(load_templates(file_path))
        except Exception as exc:
            logging.warning("Skip template file %s: %s", file_path, exc)

    input_files = list(args.input_file or [])
    if input_files:
        scraper = CommunityTemplateScraper(op_names)
        for file_path in input_files:
            expressions.extend(scraper.extract_templates_from_file(file_path))

    dedup_expr = sorted({x.strip() for x in expressions if x and x.strip()})
    if not dedup_expr:
        raise ValueError("No templates found for manual build. Use --templates-file or --input-file.")

    manual = build_syntax_manual(operators=operators, expressions=dedup_expr)
    write_syntax_manual_markdown(args.output_md, manual)
    write_syntax_manual_json(args.output_json, manual)

    logging.info(
        "Built syntax manual from operators=%d templates=%d -> %s / %s",
        len(operators),
        len(dedup_expr),
        args.output_md,
        args.output_json,
    )
    print(args.output_md)
    return 0


def command_submit_concurrent(args) -> int:
    region = args.region.upper()
    universe = args.universe or get_default_universe(region)
    summary = services.submit_concurrent(
        region=region,
        universe=universe,
        delay=args.delay,
        templates_file=args.templates_file,
        credentials_path=args.credentials,
        username=args.username,
        password=args.password,
        max_submissions=args.max_submissions,
        concurrency=args.concurrency,
        max_wait_sec=args.max_wait,
        poll_interval_sec=args.poll_interval,
        output_dir=args.output_dir,
        neutralization=args.neutralization,
    )
    logging.info(
        "Concurrent submit finished requested=%d success=%d failure=%d",
        summary["requested_count"],
        summary["success_count"],
        summary["failure_count"],
    )
    print(summary["files"]["csv"])
    return 0


def command_optimize(args) -> int:
    return command_mine(args)


def _default_validation_expression(fields) -> str:
    field_ids = {f.field_id for f in fields}
    base = "close" if "close" in field_ids else (next(iter(field_ids)) if field_ids else "close")
    return f"rank(ts_delta({base}, 1))"


def _print_validation_report(report: dict, output_path: str) -> None:
    as_json = json.dumps(report, ensure_ascii=False, indent=2)
    if output_path:
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(as_json, encoding="utf-8")
        logging.info("Validation report written to %s", output_path)
    print(as_json)


def _load_syntax_manual_excerpt(path: str, max_len: int = 3500) -> str:
    if not path:
        return ""
    src = Path(path)
    if not src.exists():
        return ""
    text = src.read_text(encoding="utf-8", errors="ignore").strip()
    if src.suffix.lower() == ".json":
        try:
            payload = json.loads(text)
            rules = payload.get("generation_rules", [])
            checker_rules = payload.get("checker_rules", [])
            top_ops = [x.get("name", "") for x in payload.get("top_operators", [])[:24] if isinstance(x, dict)]
            sample_expr = payload.get("example_templates", [])[:8]
            assembled = []
            if rules:
                assembled.append("Generation rules: " + "; ".join([str(x) for x in rules]))
            if checker_rules:
                assembled.append("Checker rules: " + "; ".join([str(x) for x in checker_rules]))
            if top_ops:
                assembled.append("Top operators: " + ", ".join([str(x) for x in top_ops]))
            if sample_expr:
                assembled.append("Examples: " + " | ".join([str(x) for x in sample_expr]))
            text = "\n".join(assembled) if assembled else text
        except Exception:
            pass
    if len(text) > max_len:
        return text[:max_len]
    return text


def _load_operator_candidates(path: str) -> List[dict]:
    if path:
        return load_operators(path)

    harvest_candidates = sorted(Path("results/harvest").glob("operators_api_*.json"), reverse=True)
    for file_path in harvest_candidates:
        try:
            return load_operators(str(file_path))
        except Exception:
            continue
    return load_operators()


def main(argv: List[str] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(args.log_level)

    if args.command == "fetch-fields":
        return command_fetch_fields(args)
    if args.command == "gen-templates":
        return command_gen_templates(args)
    if args.command == "gen-templates-iter":
        return command_gen_templates_iter(args)
    if args.command == "gen-swappable":
        return command_gen_swappable(args)
    if args.command == "mine":
        return command_mine(args)
    if args.command == "run":
        return command_run(args)
    if args.command == "validate":
        return command_validate(args)
    if args.command == "scrape-templates":
        return command_scrape_templates(args)
    if args.command == "community-login":
        return command_community_login(args)
    if args.command == "harvest-once":
        return command_harvest_once(args)
    if args.command == "build-syntax-manual":
        return command_build_syntax_manual(args)
    if args.command == "submit-concurrent":
        return command_submit_concurrent(args)
    if args.command == "optimize":
        return command_optimize(args)

    parser.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main())
