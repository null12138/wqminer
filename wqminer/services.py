"""Service layer decoupling CLI/WebUI from core workflows."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .config import load_credentials, load_llm_config
from .inspiration import merge_style_prompt
from .iterative_generator import IterativeTemplateGenerator
from .llm_client import OpenAICompatibleLLM
from .miner import FactorMiner, MiningConfig
from .models import DataField, SimulationSettings
from .mutator import ExpressionMutator
from .operator_store import load_operators
from .region_config import get_default_neutralization, get_default_universe
from .storage import load_data_fields_cache, load_templates, save_data_fields_cache, save_templates
from .template_generator import TemplateGenerator
from .worldquant_client import WorldQuantBrainClient
from .submitter import submit_expressions_concurrent


def default_fields_cache_path(region: str, universe: str, delay: int) -> str:
    return f"data/cache/data_fields_{region}_{delay}_{universe}.json"


def resolve_credentials(
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    required: bool = True,
) -> Tuple[str, str]:
    if username and password:
        return username, password
    if credentials_path:
        return load_credentials(credentials_path)
    if required:
        raise ValueError("Need credentials (credentials file or username/password)")
    return "", ""


def build_client(username: str, password: str) -> WorldQuantBrainClient:
    client = WorldQuantBrainClient(username=username, password=password)
    client.authenticate()
    return client


def load_or_fetch_fields(
    region: str,
    universe: str,
    delay: int,
    fields_file: str = "",
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    required_if_missing: bool = True,
) -> Tuple[List[DataField], str]:
    region = region.upper()
    universe = universe or get_default_universe(region)
    cache_path = fields_file or default_fields_cache_path(region, universe, delay)

    if fields_file and Path(fields_file).exists():
        return load_data_fields_cache(fields_file), fields_file
    if Path(cache_path).exists():
        return load_data_fields_cache(cache_path), cache_path

    if not required_if_missing:
        return [], cache_path

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)
    client = build_client(user, pwd)
    fields = client.fetch_data_fields(region=region, universe=universe, delay=delay)
    if not fields:
        fields = client.load_fallback_default_fields()
    save_data_fields_cache(cache_path, fields)
    return fields, cache_path


def fetch_fields_and_cache(
    region: str,
    universe: str,
    delay: int,
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    full: bool = False,
    max_datasets: int = 10,
    field_max_pages: int = 5,
    dataset_max_pages: int = 1,
    dataset_page_limit: int = 50,
    output: str = "",
    dataset_output: str = "",
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)
    client = build_client(user, pwd)

    if full:
        fields, datasets = client.fetch_data_fields_and_datasets(
            region=region,
            universe=universe,
            delay=delay,
            max_datasets=None,
            max_pages=None,
            dataset_max_pages=None,
            dataset_page_limit=dataset_page_limit,
        )
        source = "api_full"
    else:
        fields, datasets = client.fetch_data_fields_and_datasets(
            region=region,
            universe=universe,
            delay=delay,
            max_datasets=max_datasets,
            max_pages=field_max_pages,
            dataset_max_pages=dataset_max_pages,
            dataset_page_limit=dataset_page_limit,
        )
        source = "api"

    if not fields:
        fields = client.load_fallback_default_fields()
        source = "fallback"

    output_path = output or default_fields_cache_path(region, universe, delay)
    save_data_fields_cache(output_path, fields)

    dataset_path = dataset_output or f"data/cache/data_sets_{region}_{delay}_{universe}.json"
    dataset_payload = {
        "fetched_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": delay,
        "source": source,
        "dataset_count": len(datasets),
        "datasets": datasets,
    }
    dataset_file = Path(dataset_path)
    dataset_file.parent.mkdir(parents=True, exist_ok=True)
    dataset_file.write_text(json.dumps(dataset_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "fields_path": output_path,
        "dataset_path": str(dataset_file),
        "field_count": len(fields),
        "dataset_count": len(datasets),
        "region": region,
        "universe": universe,
        "delay": delay,
        "source": source,
    }


def generate_inspiration_text(
    llm_config_path: str,
    region: str,
    universe: str,
    delay: int,
    style_seed: str = "",
    seed_expressions: Optional[Sequence[str]] = None,
    max_chars: int = 420,
) -> str:
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))

    system_prompt = (
        "You are a quantitative alpha researcher. "
        "Generate a concise research inspiration for FASTEXPR templates. "
        "Output plain text only."
    )
    user_prompt = (
        f"Region: {region}\n"
        f"Universe: {universe or 'default'}\n"
        f"Delay: {delay}\n"
        "Task: Provide 1-2 short inspiration ideas to guide alpha template generation.\n"
        "Constraints:\n"
        "- Avoid code, avoid bullet lists.\n"
        "- Keep it under 3 sentences.\n"
        "- Be specific about signal intuition.\n"
    )
    if seed_expressions:
        samples = [x.strip() for x in seed_expressions if x and x.strip()]
        if samples:
            sample_text = "; ".join(samples[:3])
            user_prompt += f"\nSeed expressions (for intuition only): {sample_text}\n"
    if style_seed:
        user_prompt += f"\nStyle seed: {style_seed}\n"
    user_prompt += "\nReturn only the inspiration text."

    raw = llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.7)
    cleaned = _clean_inspiration_text(raw)
    if max_chars > 0 and len(cleaned) > max_chars:
        return cleaned[:max_chars].rstrip()
    return cleaned


def generate_templates(
    region: str,
    universe: str,
    delay: int,
    llm_config_path: str,
    fields_file: str = "",
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    count: int = 24,
    style_prompt: str = "",
    inspiration: str = "",
    output: str = "templates/generated_templates.json",
    operators_file: str = "",
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)

    fields, fields_cache = load_or_fetch_fields(
        region=region,
        universe=universe,
        delay=delay,
        fields_file=fields_file,
        credentials_path=credentials_path,
        username=username,
        password=password,
        required_if_missing=True,
    )

    operators = load_operators(operators_file) if operators_file else load_operators()
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    generator = TemplateGenerator(llm=llm, operators=operators)

    merged_style = merge_style_prompt(style_prompt, inspiration)
    templates = generator.generate_templates(
        region=region,
        data_fields=fields,
        count=max(1, int(count)),
        style_prompt=merged_style,
    )

    metadata = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": delay,
        "template_count": len(templates),
        "field_count": len(fields),
        "operator_count": len(operators),
        "fields_cache": fields_cache,
    }
    save_templates(output, templates, metadata)

    return {
        "output": output,
        "template_count": len(templates),
        "metadata": metadata,
    }


def generate_templates_iter(
    region: str,
    universe: str,
    delay: int,
    llm_config_path: str,
    fields_file: str = "",
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    count: int = 48,
    rounds: int = 3,
    max_fix_attempts: int = 1,
    style_prompt: str = "",
    inspiration: str = "",
    syntax_guide: str = "",
    output: str = "templates/generated_templates_iter.json",
    report_output: str = "results/gen_templates_iter_report.json",
    operators_file: str = "",
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)

    fields, fields_cache = load_or_fetch_fields(
        region=region,
        universe=universe,
        delay=delay,
        fields_file=fields_file,
        credentials_path=credentials_path,
        username=username,
        password=password,
        required_if_missing=True,
    )

    operators = load_operators(operators_file) if operators_file else load_operators()
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))

    merged_style = merge_style_prompt(style_prompt, inspiration)
    generator = IterativeTemplateGenerator(llm=llm, operators=operators)
    templates, iter_report = generator.generate(
        region=region,
        data_fields=fields,
        count=max(1, int(count)),
        rounds=max(1, int(rounds)),
        style_prompt=merged_style,
        syntax_guide=syntax_guide,
        max_fix_attempts=max(0, int(max_fix_attempts)),
    )

    metadata = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "region": region,
        "universe": universe,
        "delay": delay,
        "template_count": len(templates),
        "field_count": len(fields),
        "operator_count": len(operators),
        "mode": "iterative",
        "rounds": max(1, int(rounds)),
        "max_fix_attempts": max(0, int(max_fix_attempts)),
        "fields_cache": fields_cache,
    }
    save_templates(output, templates, metadata)

    report_payload = {
        "metadata": metadata,
        "iterative_report": iter_report,
    }
    report_path = Path(report_output)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "output": output,
        "report_output": str(report_path),
        "template_count": len(templates),
        "metadata": metadata,
    }


def optimize_loop(
    region: str,
    universe: str,
    delay: int,
    templates_file: str = "",
    expressions: Optional[Sequence[str]] = None,
    fields_file: str = "",
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    rounds: int = 3,
    variants_per_template: int = 8,
    max_simulations: int = 200,
    sharpe_threshold: float = 1.25,
    fitness_threshold: float = 1.0,
    output_dir: str = "results",
    neutralization: str = "",
    dry_run: bool = False,
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)
    neutralization = neutralization or get_default_neutralization(region)

    exprs: List[str] = []
    if expressions:
        exprs.extend([x.strip() for x in expressions if x and x.strip()])
    if templates_file:
        exprs.extend(load_templates(templates_file))
    exprs = [x for x in exprs if x]
    if not exprs:
        raise ValueError("No expressions provided for optimize loop")

    if fields_file and Path(fields_file).exists():
        fields = load_data_fields_cache(fields_file)
    elif dry_run:
        fields = []
    else:
        fields, _ = load_or_fetch_fields(
            region=region,
            universe=universe,
            delay=delay,
            fields_file=fields_file,
            credentials_path=credentials_path,
            username=username,
            password=password,
            required_if_missing=True,
        )

    operators = load_operators()
    mutator = ExpressionMutator(operators=operators)

    client = None
    if not dry_run:
        user, pwd = resolve_credentials(credentials_path, username, password, required=True)
        client = build_client(user, pwd)

    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )

    miner = FactorMiner(client=client, mutator=mutator, output_dir=output_dir)
    report = miner.mine(
        seed_expressions=exprs,
        available_field_ids=[f.field_id for f in fields if f.field_id],
        settings=settings,
        config=MiningConfig(
            rounds=max(1, int(rounds)),
            variants_per_template=max(1, int(variants_per_template)),
            max_simulations=max(1, int(max_simulations)),
            sharpe_threshold=float(sharpe_threshold),
            fitness_threshold=float(fitness_threshold),
            dry_run=bool(dry_run),
        ),
    )

    report["settings"] = {
        "region": region,
        "universe": universe,
        "delay": delay,
        "neutralization": neutralization,
        "dry_run": bool(dry_run),
    }
    report["expression_count"] = len(exprs)
    return report


def submit_concurrent(
    region: str,
    universe: str,
    delay: int,
    templates_file: str = "",
    expressions: Optional[Sequence[str]] = None,
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    max_submissions: int = 60,
    concurrency: int = 3,
    max_wait_sec: int = 240,
    poll_interval_sec: int = 5,
    output_dir: str = "results/submissions",
    neutralization: str = "",
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)
    neutralization = neutralization or get_default_neutralization(region)

    exprs: List[str] = []
    if expressions:
        exprs.extend([x.strip() for x in expressions if x and x.strip()])
    if templates_file:
        exprs.extend(load_templates(templates_file))
    exprs = [x for x in exprs if x]
    if not exprs:
        raise ValueError("No expressions provided for submit")

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)

    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )
    return submit_expressions_concurrent(
        expressions=exprs,
        username=user,
        password=pwd,
        settings=settings,
        max_submissions=max(1, int(max_submissions)),
        concurrency=max(1, int(concurrency)),
        max_wait_sec=max(30, int(max_wait_sec)),
        poll_interval_sec=max(1, int(poll_interval_sec)),
        output_dir=output_dir,
    )


def _clean_inspiration_text(raw: str) -> str:
    if not raw:
        return ""
    text = raw.replace("```", " ").strip()
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    cleaned = []
    for line in lines:
        line = line.lstrip("-•*0123456789. ").strip()
        if not line:
            continue
        cleaned.append(line)
    if not cleaned:
        return text.strip()
    merged = " ".join(cleaned[:2]).strip()
    return merged if merged else text.strip()
