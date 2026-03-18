"""Local producer-only pipeline: generate templates and enqueue to Supabase."""

from __future__ import annotations

import json
import logging
import socket
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence

from .config import load_llm_config
from .inspiration import merge_style_prompt
from .llm_client import OpenAICompatibleLLM
from .models import SimulationSettings
from .operator_store import load_operators
from .queue_supabase import SupabaseQueueClient
from .region_config import get_default_neutralization, get_default_universe
from .services import (
    _fetch_fields_for_dataset_ids,
    _load_seed_expressions,
    _load_tempmd_templates,
    _normalize_dataset_ids,
    _prepare_candidate_batch,
    default_fields_cache_path,
    generate_inspiration_text,
    resolve_credentials,
)
from .storage import load_data_fields_cache, save_data_fields_cache
from .template_generator import TemplateGenerator
from .worldquant_client import WorldQuantBrainClient


def _ensure_output_dir(path: str) -> Path:
    out = Path(path or "results/producer")
    out.mkdir(parents=True, exist_ok=True)
    return out


def _build_job_settings(
    *,
    region: str,
    universe: str,
    delay: int,
    neutralization: str,
) -> Dict[str, Any]:
    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )
    payload = settings.to_api_payload("__EXPR__")
    return dict(payload.get("settings", {}))


def produce_templates_only(
    *,
    region: str,
    universe: str,
    delay: int,
    llm_config_path: str,
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    template_count: int = 64,
    style_prompt: str = "",
    inspiration: str = "",
    timeout_sec: int = 60,
    max_retries: int = 5,
    disable_proxy: Optional[bool] = None,
    operator_file: str = "",
    strict_validation: bool = False,
    max_operator_count: int = 0,
    require_keyword_optional: bool = True,
    batch_size: int = 0,
    enforce_exact_batch: bool = False,
    required_theme_coverage: int = 0,
    common_operator_limit: int = 0,
    enforce_explore_theme_pairs: bool = False,
    template_guide_path: str = "",
    template_style_items: int = 0,
    template_seed_count: int = 0,
    seed_templates: str = "",
    dataset_ids: Optional[Sequence[str] | str] = None,
    dataset_field_max_pages: int = 5,
    dataset_field_page_limit: int = 50,
    output_dir: str = "results/producer",
    enqueue: bool = False,
    queue_priority: int = 0,
    queue_max_attempts: int = 6,
    queue_batch_table: str = "alpha_batches",
    queue_job_table: str = "alpha_jobs",
    supabase_url: str = "",
    supabase_service_key: str = "",
) -> Dict[str, Any]:
    region = str(region or "").upper() or "USA"
    universe = str(universe or "").strip() or get_default_universe(region)
    delay = int(delay)
    neutralization = get_default_neutralization(region)
    selected_dataset_ids = _normalize_dataset_ids(dataset_ids)

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)

    fields_cache = default_fields_cache_path(region, universe, delay, dataset_ids=selected_dataset_ids)
    if Path(fields_cache).exists():
        fields = load_data_fields_cache(fields_cache)
        logging.info("Using cached fields for producer: %s (count=%d)", fields_cache, len(fields))
    else:
        client = WorldQuantBrainClient(
            username=user,
            password=pwd,
            timeout_sec=max(5, int(timeout_sec)),
            max_retries=max(1, int(max_retries)),
            disable_proxy=disable_proxy,
        )
        if selected_dataset_ids:
            fields = _fetch_fields_for_dataset_ids(
                client,
                dataset_ids=selected_dataset_ids,
                region=region,
                universe=universe,
                delay=delay,
                max_pages=max(1, int(dataset_field_max_pages)),
                page_limit=max(1, int(dataset_field_page_limit)),
            )
        else:
            fields = client.fetch_data_fields(region=region, universe=universe, delay=delay)
        if not fields:
            fields = client.load_fallback_default_fields()
        save_data_fields_cache(fields_cache, fields)

    if not inspiration:
        seed_exprs = _load_seed_expressions(seed_templates)
        inspiration = generate_inspiration_text(
            llm_config_path=llm_config_path,
            region=region,
            universe=universe,
            delay=delay,
            style_seed=style_prompt,
            seed_expressions=seed_exprs,
        )

    operators = load_operators(operator_file)
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    generator = TemplateGenerator(llm=llm, operators=operators)
    template_lines = _load_tempmd_templates(template_guide_path, max_items=700) if template_guide_path else []
    style = merge_style_prompt(style_prompt, inspiration)
    target_count = int(batch_size) if int(batch_size) > 0 else int(template_count)

    expressions, preflight_reports = _prepare_candidate_batch(
        generator=generator,
        region=region,
        fields=fields,
        count=max(1, target_count),
        style_prompt=style,
        operators=operators,
        operator_file=operator_file,
        strict_validation=bool(strict_validation),
        max_operator_count=int(max_operator_count),
        require_keyword_optional=bool(require_keyword_optional),
        enforce_exact_batch=bool(enforce_exact_batch),
        required_theme_coverage=int(required_theme_coverage),
        common_operator_limit=int(common_operator_limit),
        enforce_explore_theme_pairs=bool(enforce_explore_theme_pairs),
        template_lines=template_lines,
        template_style_items=int(template_style_items),
        template_seed_count=int(template_seed_count),
    )

    settings_payload = _build_job_settings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )
    produced_at = time.strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()

    jobs_payload: List[Dict[str, Any]] = []
    for expr in expressions:
        jobs_payload.append(
            {
                "expression": expr,
                "settings": settings_payload,
                "region": region,
                "universe": universe,
                "delay": delay,
                "neutralization": neutralization,
                "language": str(settings_payload.get("language", "FASTEXPR")),
                "status": "queued",
                "priority": int(queue_priority),
                "max_attempts": max(1, int(queue_max_attempts)),
                "source": "local_producer",
                "source_host": host,
                "meta": {
                    "produced_at": produced_at,
                    "strict_validation": bool(strict_validation),
                },
            }
        )

    output_root = _ensure_output_dir(output_dir)
    ts = time.strftime("%Y%m%d_%H%M%S")
    output_file = output_root / f"produced_batch_{region}_{universe}_D{delay}_{ts}.json"
    local_payload = {
        "produced_at": produced_at,
        "region": region,
        "universe": universe,
        "delay": delay,
        "neutralization": neutralization,
        "count": len(expressions),
        "dataset_ids": selected_dataset_ids,
        "fields_cache": fields_cache,
        "inspiration": inspiration,
        "expressions": expressions,
        "jobs": jobs_payload,
    }
    output_file.write_text(json.dumps(local_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    queue_batch_id = ""
    enqueued_count = 0
    if enqueue:
        queue = SupabaseQueueClient(
            base_url=supabase_url,
            service_key=supabase_service_key,
            batch_table=queue_batch_table,
            job_table=queue_job_table,
        )
        batch = queue.create_batch(
            {
                "region": region,
                "universe": universe,
                "delay": delay,
                "neutralization": neutralization,
                "language": str(settings_payload.get("language", "FASTEXPR")),
                "template_count": len(expressions),
                "producer_host": host,
                "metadata": {
                    "output_file": str(output_file),
                    "fields_cache": fields_cache,
                    "dataset_ids": selected_dataset_ids,
                },
            }
        )
        queue_batch_id = str(batch.get("id", "")).strip()
        if queue_batch_id:
            for item in jobs_payload:
                item["batch_id"] = queue_batch_id
        inserted = queue.enqueue_jobs(jobs_payload)
        enqueued_count = len(inserted)

    return {
        "output_file": str(output_file),
        "count": len(expressions),
        "enqueued_count": enqueued_count,
        "queue_batch_id": queue_batch_id,
        "fields_cache": fields_cache,
        "dataset_ids": selected_dataset_ids,
        "preflight_reports": preflight_reports,
    }
