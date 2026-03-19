"""Minimal service layer for one-click flow."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
import socket
import threading
import time
import zipfile
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import requests

from .config import load_credentials, load_llm_config
from .expression_validator import validate_expression_report
from .inspiration import merge_style_prompt
from .llm_client import OpenAICompatibleLLM
from .models import DataField, SimulationSettings
from .operator_store import load_operators
from .region_config import get_default_neutralization, get_default_universe
from .storage import load_data_fields_cache, save_data_fields_cache
from .template_generator import TemplateGenerator
from .worldquant_client import WorldQuantBrainClient


def _normalize_dataset_ids(value: Optional[Sequence[str] | str]) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        raw = [x.strip() for x in value.split(",")]
    else:
        raw = [str(x).strip() for x in value]
    out: List[str] = []
    seen = set()
    for item in raw:
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _normalize_guide_paths(value: Optional[Sequence[str] | str]) -> List[str]:
    if value is None:
        return []
    raw: List[str] = []
    if isinstance(value, str):
        raw = re.split(r"[,\n]", value)
    else:
        for item in value:
            raw.extend(re.split(r"[,\n]", str(item or "")))
    out: List[str] = []
    seen = set()
    for item in raw:
        path = str(item or "").strip()
        if not path or path in seen:
            continue
        seen.add(path)
        out.append(path)
    return out


def _read_text_file(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return ""


def _read_text_from_zip(zip_path: Path, member: str) -> str:
    try:
        with zipfile.ZipFile(zip_path) as zf:
            with zf.open(member) as handle:
                return handle.read().decode("utf-8", errors="ignore")
    except Exception:
        return ""


def _summarize_ai_worker_guidance(raw: str, *, max_chars: int = 1100) -> str:
    text = (raw or "").strip()
    if not text:
        return ""
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    keywords = (
        "stable pnl",
        "economic sense",
        "simulation settings",
        "region",
        "delay",
        "universe",
        "neutralization",
        "winsorize",
        "zscore",
        "normalize",
        "group_neutralize",
        "regression_neut",
        "turnover",
        "correlation",
        "pitfall",
        "alpha",
        "pyramid",
    )

    picked: List[str] = []
    seen = set()
    for line in lines:
        norm = line.lstrip("-* ").strip()
        if not norm:
            continue
        low = norm.lower()
        if not any(k in low for k in keywords):
            continue
        if norm in seen:
            continue
        seen.add(norm)
        picked.append(f"- {norm}")
        if len(picked) >= 10:
            break

    if not picked:
        for line in lines[:8]:
            norm = line.lstrip("-* ").strip()
            if not norm or norm in seen:
                continue
            seen.add(norm)
            picked.append(f"- {norm}")

    out = "\n".join(picked).strip()
    if not out:
        return ""
    if len(out) > max_chars:
        return out[:max_chars].rstrip()
    return out


@lru_cache(maxsize=8)
def _load_ai_worker_guidance(source_path: str) -> str:
    source = (source_path or "").strip()
    if not source:
        return ""
    path = Path(source).expanduser()
    if not path.exists():
        return ""

    raw = ""
    if path.is_file() and path.suffix.lower() == ".zip":
        try:
            with zipfile.ZipFile(path) as zf:
                names = zf.namelist()
        except Exception:
            names = []
        target = ""
        for name in names:
            low = name.lower()
            if low.endswith("brain-consultant.md"):
                target = name
                break
        if not target:
            for name in names:
                low = name.lower()
                if low.endswith(".md") and ("consultant" in low or "brain" in low or "ai" in low):
                    target = name
                    break
        if target:
            raw = _read_text_from_zip(path, target)
    elif path.is_file():
        raw = _read_text_file(path)

    return _summarize_ai_worker_guidance(raw)


def _compose_generation_style(
    *,
    style_prompt: str,
    inspiration: str,
    ai_worker_guidance: str,
) -> str:
    chunks: List[str] = []
    base = (style_prompt or "").strip()
    if base:
        chunks.append(base)
    idea = (inspiration or "").strip()
    if idea:
        chunks.append(f"Inspiration:\n{idea}")
    worker = (ai_worker_guidance or "").strip()
    if worker:
        chunks.append("AI Worker Guidance:\n" + worker)
    return "\n\n".join(chunks).strip()


def default_fields_cache_path(
    region: str,
    universe: str,
    delay: int,
    dataset_ids: Optional[Sequence[str] | str] = None,
) -> str:
    norm_ids = _normalize_dataset_ids(dataset_ids)
    if not norm_ids:
        return f"data/cache/data_fields_{region}_{delay}_{universe}.json"
    digest_src = ",".join(sorted(norm_ids))
    digest = hashlib.sha1(digest_src.encode("utf-8")).hexdigest()[:12]
    return f"data/cache/data_fields_{region}_{delay}_{universe}_ds_{digest}.json"


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


class SupabaseQueueClient:
    def __init__(
        self,
        *,
        base_url: str,
        service_key: str,
        timeout_sec: int = 20,
        job_table: str = "alpha_jobs",
    ) -> None:
        base = str(base_url or "").strip().rstrip("/")
        key = str(service_key or "").strip()
        if not base:
            raise ValueError("base_url is required")
        if not key:
            raise ValueError("service_key is required")
        self.base_url = base
        self.service_key = key
        self.timeout_sec = max(3, int(timeout_sec))
        self.job_table = str(job_table or "alpha_jobs").strip()

    def _headers(self, prefer: str = "return=representation") -> Dict[str, str]:
        return {
            "apikey": self.service_key,
            "Authorization": f"Bearer {self.service_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Prefer": prefer,
        }

    def _post_rows(self, table: str, rows: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        items = [dict(x) for x in rows if isinstance(x, dict)]
        if not items:
            return []
        url = f"{self.base_url}/rest/v1/{table}"
        resp = requests.post(url, headers=self._headers(), json=items, timeout=self.timeout_sec)
        if resp.status_code >= 300:
            raise RuntimeError(f"Supabase insert failed ({resp.status_code}) table={table}: {resp.text}")
        payload = resp.json()
        return payload if isinstance(payload, list) else []

    def enqueue_jobs(self, jobs: Sequence[Dict[str, object]]) -> List[Dict[str, object]]:
        return self._post_rows(self.job_table, jobs)


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
) -> Dict[str, object]:
    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )
    payload = settings.to_api_payload("__EXPR__")
    block = payload.get("settings", {})
    return dict(block) if isinstance(block, dict) else {}


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
        "Write a concise, actionable alpha hypothesis that can be translated into FASTEXPR. "
        "Output plain text only (no lists, no code)."
    )
    user_prompt = (
        f"Region: {region}\n"
        f"Universe: {universe or 'default'}\n"
        f"Delay: {delay}\n"
        "Task: Provide 1-2 short sentences guiding alpha template generation.\n"
        "Constraints:\n"
        "- No bullet lists, no code.\n"
        "- 1-2 sentences only, under 320 characters.\n"
        "- Be specific about signal intuition and intended effect (e.g., value, momentum, quality, sentiment, risk).\n"
        "- Mention at least one concrete field type or dataset category when possible.\n"
    )
    if seed_expressions:
        samples = [x.strip() for x in seed_expressions if x and x.strip()]
        if samples:
            sample_text = "; ".join(samples[:3])
            user_prompt += f"\nSeed expressions (for intuition only): {sample_text}\n"
    if style_seed:
        user_prompt += f"\nStyle seed: {style_seed}\n"
    user_prompt += "\nReturn only the inspiration text."

    try:
        raw = llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.7)
    except Exception as exc:
        logging.warning("Inspiration generation failed: %s", exc)
        return ""
    cleaned = _clean_inspiration_text(raw)
    if max_chars > 0 and len(cleaned) > max_chars:
        return cleaned[:max_chars].rstrip()
    return cleaned


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


def _unique_expressions(items: Sequence[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        expr = (item or "").strip()
        if not expr:
            continue
        if expr in seen:
            continue
        seen.add(expr)
        out.append(expr)
    return out


def _load_seed_expressions(path: str) -> List[str]:
    if not path:
        return []
    src = Path(path)
    if not src.exists():
        return []
    try:
        payload = json.loads(src.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, dict) and isinstance(payload.get("templates"), list):
        items = payload.get("templates", [])
    else:
        items = payload if isinstance(payload, list) else []
    expressions: List[str] = []
    for item in items:
        if isinstance(item, dict) and item.get("expression"):
            expressions.append(str(item.get("expression", "")).strip())
        elif isinstance(item, str):
            expressions.append(item.strip())
    return _unique_expressions(expressions)


def _score_row(row: Dict[str, float]) -> float:
    return float(row.get("sharpe", 0.0)) + 0.5 * float(row.get("fitness", 0.0)) - 0.01 * float(row.get("turnover", 0.0))


def _select_top_rows(rows: Sequence[Dict[str, float]], top_k: int) -> List[Dict[str, float]]:
    if not rows:
        return []
    ranked = sorted(rows, key=_score_row, reverse=True)
    return ranked[: max(1, int(top_k))]


def _build_evolution_hint(rows: Sequence[Dict[str, float]]) -> str:
    lines = []
    for row in rows:
        expr = str(row.get("expression", "")).strip()
        if not expr:
            continue
        lines.append(
            f"- {expr} | sharpe={row.get('sharpe', 0.0):.3f}, "
            f"fitness={row.get('fitness', 0.0):.3f}, turnover={row.get('turnover', 0.0):.2f}"
        )
    if not lines:
        return ""
    return (
        "Evolution guidance:\n"
        "Use the best-performing expressions below as parents. "
        "Create improved variants by combining ideas, improving sharpe/fitness, "
        "and keeping turnover reasonable. Avoid duplicates.\n"
        + "\n".join(lines)
    )


def _negate_expression(expr: str) -> str:
    raw = (expr or "").strip()
    if not raw:
        return ""
    if raw.startswith("-(") and raw.endswith(")"):
        return raw[2:-1].strip()
    if raw.startswith("-") and not raw.startswith("-("):
        return raw[1:].strip()
    return f"-({raw})"


def _log_reverse_event(path: Path, row: Dict[str, float], negated: str) -> None:
    if not path:
        return
    payload = {
        "time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "expression": row.get("expression", ""),
        "sharpe": float(row.get("sharpe", 0.0)),
        "fitness": float(row.get("fitness", 0.0)),
        "turnover": float(row.get("turnover", 0.0)),
        "negated": negated,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def _collect_reverse_candidates(
    rows: Sequence[Dict[str, float]],
    sharpe_max: float,
    fitness_max: float,
    log_path: Optional[Path],
    seen: set,
    max_count: int,
) -> List[str]:
    candidates: List[str] = []
    if not rows:
        return candidates
    limit = int(max_count) if max_count is not None else 0
    for row in rows:
        try:
            sharpe = float(row.get("sharpe", 0.0))
            fitness = float(row.get("fitness", 0.0))
        except Exception:
            continue
        if sharpe >= sharpe_max or fitness >= fitness_max:
            continue
        expr = str(row.get("expression", "")).strip()
        if not expr:
            continue
        if expr.lstrip().startswith("-"):
            continue
        negated = _negate_expression(expr)
        if not negated or negated in seen:
            continue
        seen.add(negated)
        if log_path:
            _log_reverse_event(log_path, row, negated)
        candidates.append(negated)
        if limit > 0 and len(candidates) >= limit:
            break
    return candidates


def _summarize_results(rows: Sequence[Dict[str, float]]) -> Dict[str, float]:
    total = len(rows)
    if total == 0:
        return {
            "total": 0,
            "avg_sharpe": 0.0,
            "avg_fitness": 0.0,
            "avg_turnover": 0.0,
            "success": 0,
            "inactive": 0,
        }
    avg_sharpe = sum(float(r.get("sharpe", 0.0)) for r in rows) / total
    avg_fitness = sum(float(r.get("fitness", 0.0)) for r in rows) / total
    avg_turnover = sum(float(r.get("turnover", 0.0)) for r in rows) / total
    success = sum(1 for r in rows if r.get("success"))
    inactive = sum(1 for r in rows if float(r.get("turnover", 0.0)) <= 0.0)
    return {
        "total": total,
        "avg_sharpe": avg_sharpe,
        "avg_fitness": avg_fitness,
        "avg_turnover": avg_turnover,
        "success": success,
        "inactive": inactive,
    }


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
    template_guide_path: Optional[Sequence[str] | str] = "",
    template_style_items: int = 0,
    template_seed_count: int = 0,
    seed_templates: str = "",
    generate_inspiration: bool = False,
    ai_worker_file: str = "wqminer/constants/worker_prompt_compact.md",
    max_generate_attempts: int = 4,
    dataset_ids: Optional[Sequence[str] | str] = None,
    dataset_field_max_pages: int = 5,
    dataset_field_page_limit: int = 50,
    output_dir: str = "results/producer",
    enqueue: bool = False,
    queue_job_table: str = "alpha_jobs",
    supabase_url: str = "",
    supabase_service_key: str = "",
) -> Dict[str, object]:
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

    if not inspiration and bool(generate_inspiration):
        seed_exprs = _load_seed_expressions(seed_templates)
        inspiration = generate_inspiration_text(
            llm_config_path=llm_config_path,
            region=region,
            universe=universe,
            delay=delay,
            style_seed=style_prompt,
            seed_expressions=seed_exprs,
        )
    ai_worker_guidance = _load_ai_worker_guidance(ai_worker_file)
    if ai_worker_guidance:
        logging.info("Loaded AI worker guidance from %s", ai_worker_file)

    operators = load_operators(operator_file)
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    generator = TemplateGenerator(llm=llm, operators=operators)
    template_lines, loaded_guide_paths = _load_template_guides(template_guide_path, max_items=700)
    if template_lines:
        logging.info(
            "Loaded %s guide templates from %s",
            len(template_lines),
            ", ".join(loaded_guide_paths),
        )
    elif template_guide_path:
        logging.info("Template guide not found or empty: %s", template_guide_path)
    style = _compose_generation_style(
        style_prompt=style_prompt,
        inspiration=inspiration,
        ai_worker_guidance=ai_worker_guidance,
    )
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
        policy_prompt=ai_worker_guidance,
        max_generate_attempts=max(1, int(max_generate_attempts)),
    )

    settings_payload = _build_job_settings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )
    produced_at = time.strftime("%Y-%m-%d %H:%M:%S")
    host = socket.gethostname()

    jobs_payload: List[Dict[str, object]] = []
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

    enqueued_count = 0
    if enqueue:
        queue = SupabaseQueueClient(
            base_url=supabase_url,
            service_key=supabase_service_key,
            job_table=queue_job_table,
        )
        inserted = queue.enqueue_jobs(jobs_payload)
        enqueued_count = len(inserted)

    return {
        "output_file": str(output_file),
        "count": len(expressions),
        "enqueued_count": enqueued_count,
        "fields_cache": fields_cache,
        "dataset_ids": selected_dataset_ids,
        "preflight_reports": preflight_reports,
    }


def _resolve_parallel_runtime(
    *,
    concurrency_profile: str,
    concurrency: int,
    concurrency_cap: int,
    poll_interval_sec: int,
    template_count: int,
    batch_size: int,
) -> Dict[str, int | str]:
    profile = str(concurrency_profile or "advisor").strip().lower()
    requested = max(1, int(concurrency))
    cap = max(0, int(concurrency_cap))
    poll = max(1, int(poll_interval_sec))
    templates = max(1, int(template_count))
    fixed_batch = int(batch_size)

    if profile in {"advisor", "consultant", "aggressive", "turbo", "high"}:
        # Advisor accounts can run large parallel sets; lift legacy defaults.
        requested = max(requested, 56)
        if cap <= 0 or cap < requested:
            cap = requested
        poll = min(poll, 10)
        if fixed_batch <= 0:
            templates = max(templates, requested)
    elif profile in {"balanced", "standard"}:
        requested = max(requested, 16)
        if cap > 0 and cap < requested:
            cap = requested
        poll = min(poll, 15)
        if fixed_batch <= 0:
            templates = max(templates, requested)
    elif profile in {"safe", "legacy"}:
        requested = min(requested, 8)
        if cap > 0:
            cap = min(cap, requested)
        poll = max(poll, 15)
    else:
        # custom/auto: keep user values, but avoid cap<concurrency mismatch.
        if cap > 0 and cap < requested:
            cap = requested

    effective = requested if cap <= 0 else min(requested, cap)
    return {
        "profile": profile,
        "requested_concurrency": requested,
        "concurrency_cap": cap,
        "effective_concurrency": effective,
        "poll_interval_sec": poll,
        "template_count": templates,
    }


def generate_reflection_text(
    llm_config_path: str,
    region: str,
    universe: str,
    delay: int,
    round_index: int,
    rows: Sequence[Dict[str, float]],
    max_chars: int = 500,
) -> str:
    if not rows:
        return ""
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    summary = _summarize_results(rows)
    top_rows = _select_top_rows(rows, 5)
    lines = []
    for row in top_rows:
        expr = str(row.get("expression", "")).strip()
        if not expr:
            continue
        lines.append(
            f"- {expr} | sharpe={row.get('sharpe', 0.0):.3f}, "
            f"fitness={row.get('fitness', 0.0):.3f}, turnover={row.get('turnover', 0.0):.2f}"
        )

    system_prompt = (
        "You are a quantitative alpha researcher. "
        "Be blunt and aggressively prioritize hit-rate and throughput. "
        "No chain-of-thought, no lists, no code. 1-2 sentences only."
    )
    user_prompt = (
        f"Round: {round_index}\n"
        f"Region: {region}\n"
        f"Universe: {universe}\n"
        f"Delay: {delay}\n"
        f"Total: {summary['total']}\n"
        f"Success: {summary['success']}\n"
        f"Inactive (turnover<=0): {summary['inactive']}\n"
        f"Avg sharpe: {summary['avg_sharpe']:.3f}\n"
        f"Avg fitness: {summary['avg_fitness']:.3f}\n"
        f"Avg turnover: {summary['avg_turnover']:.2f}\n"
        "Top expressions:\n"
        + ("\n".join(lines) if lines else "none")
        + "\n\nReturn 1-2 sentences: diagnose performance and give 2 concrete next-step ideas plus 1 explicit "
        "avoid/stop direction. "
        "Each idea must name at least one operator (e.g., ts_rank, decay_linear, ts_delta, zscore, winsorize, "
        "group_neutralize) and one field family (news, price/volume, fundamentals, analyst, alternative). "
        "If inactive is high, explicitly say to avoid zero-activity signals."
    )

    try:
        raw = llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.4)
    except Exception as exc:
        logging.warning("Reflection generation failed: %s", exc)
        return ""
    cleaned = _clean_inspiration_text(raw)
    if max_chars > 0 and len(cleaned) > max_chars:
        return cleaned[:max_chars].rstrip()
    return cleaned


def _generate_expressions(
    generator: TemplateGenerator,
    region: str,
    fields: Sequence[DataField],
    count: int,
    style_prompt: str,
    policy_prompt: str = "",
) -> List[str]:
    templates = generator.generate_templates(
        region=region,
        data_fields=list(fields),
        count=max(1, int(count)),
        style_prompt=style_prompt,
        policy_prompt=policy_prompt,
    )
    expressions = [t.expression for t in templates if t and t.expression]
    return _unique_expressions(expressions)


def _fetch_fields_for_dataset_ids(
    client: WorldQuantBrainClient,
    *,
    dataset_ids: Sequence[str],
    region: str,
    universe: str,
    delay: int,
    max_pages: int = 5,
    page_limit: int = 50,
) -> List[DataField]:
    all_fields: Dict[str, DataField] = {}
    page_limit = max(1, int(page_limit))
    max_pages = max(1, int(max_pages))

    for idx, dataset_id in enumerate(dataset_ids, start=1):
        dsid = str(dataset_id or "").strip()
        if not dsid:
            continue
        logging.info("Fetching fields from selected dataset %s (%s/%s)", dsid, idx, len(dataset_ids))
        page = 1
        while page <= max_pages:
            try:
                rows = client.get_data_fields(
                    dataset_id=dsid,
                    region=region,
                    universe=universe,
                    delay=delay,
                    page=page,
                    limit=page_limit,
                )
            except requests.HTTPError:
                break
            if not rows:
                break
            for raw in rows:
                parsed = DataField.from_api(raw)
                if not parsed.field_id:
                    continue
                all_fields[parsed.field_id] = parsed
            if len(rows) < page_limit:
                break
            page += 1

    filtered: List[DataField] = []
    for field in all_fields.values():
        if field.region and field.region != region:
            continue
        if field.universe and field.universe != universe:
            continue
        if field.delay and int(field.delay) != int(delay):
            continue
        filtered.append(field)
    return sorted(filtered, key=lambda x: x.field_id)


def _normalize_template_line(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if "模板:" in text:
        text = text.split("模板:", 1)[1].strip()
    if "#" in text:
        text = text.split("#", 1)[0].strip()
    text = text.rstrip(";").strip()
    if not text:
        return ""
    if text.startswith("|") or text.startswith("```") or text.startswith("---"):
        return ""
    if text in {"```", "'''"}:
        return ""
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=\s*$", text):
        return ""
    return text


def _load_tempmd_templates(path: str, max_items: int = 500) -> List[str]:
    src = Path(path or "").expanduser()
    if not src.exists() or not src.is_file():
        return []
    try:
        content = src.read_text(encoding="utf-8")
    except Exception as exc:
        logging.warning("Failed to read template guide %s: %s", src, exc)
        return []

    out: List[str] = []
    seen = set()
    for line in content.splitlines():
        if "模板:" in line:
            cand = _normalize_template_line(line)
            if cand and cand not in seen:
                seen.add(cand)
                out.append(cand)
            if len(out) >= max_items:
                return out

    in_code = False
    block_lines: List[str] = []
    for raw in content.splitlines():
        line = raw.rstrip("\n")
        if line.strip().startswith("```"):
            if in_code:
                for b in block_lines:
                    cand = _normalize_template_line(b)
                    if not cand:
                        continue
                    if cand not in seen:
                        seen.add(cand)
                        out.append(cand)
                        if len(out) >= max_items:
                            return out
                block_lines = []
                in_code = False
            else:
                in_code = True
            continue
        if in_code:
            block_lines.append(line)
    return out


def _load_template_guides(
    paths: Optional[Sequence[str] | str],
    max_items: int = 500,
) -> Tuple[List[str], List[str]]:
    selected_paths = _normalize_guide_paths(paths)
    if not selected_paths:
        return [], []

    limit = max(1, int(max_items))
    loaded_paths: List[str] = []
    out: List[str] = []
    seen = set()

    for guide_path in selected_paths:
        if len(out) >= limit:
            break
        remaining = limit - len(out)
        rows = _load_tempmd_templates(guide_path, max_items=remaining)
        if not rows:
            continue
        loaded_paths.append(guide_path)
        for row in rows:
            cand = _normalize_template_line(row)
            if not cand or cand in seen:
                continue
            seen.add(cand)
            out.append(cand)
            if len(out) >= limit:
                break

    return out, loaded_paths


def _build_template_style_snippet(lines: Sequence[str], max_items: int = 16, max_chars: int = 2400) -> str:
    if not lines or max_items <= 0:
        return ""
    take = min(len(lines), max(1, int(max_items)))
    picks = random.sample(list(lines), take) if len(lines) > take else list(lines)
    snippet_lines: List[str] = []
    total_chars = 0
    for item in picks:
        if not item:
            continue
        line = f"- {item}"
        if total_chars + len(line) > max_chars:
            break
        snippet_lines.append(line)
        total_chars += len(line)
    if not snippet_lines:
        return ""
    return (
        "Template guide (from configured guide files):\n"
        "Follow these structures but only use operators/fields that are currently available.\n"
        + "\n".join(snippet_lines)
    )


def _field_pools_for_templates(fields: Sequence[DataField]) -> Dict[str, List[str]]:
    matrix_fields: List[str] = []
    vector_fields: List[str] = []
    sentiment_fields: List[str] = []
    analyst_fields: List[str] = []
    model_fields: List[str] = []
    fundamental_fields: List[str] = []
    option_fields: List[str] = []
    price_volume_fields: List[str] = []
    group_fields = ["industry", "sector", "subindustry", "market", "country"]

    for f in fields:
        fid = str(f.field_id or "").strip()
        if not fid:
            continue
        lfid = fid.lower()
        ftype = str(f.field_type or "").upper()
        if "VECTOR" in ftype or lfid.startswith(("anl4_", "oth41_", "scl12_", "nws", "mws")):
            vector_fields.append(fid)
        else:
            matrix_fields.append(fid)

        if lfid.startswith(("scl", "snt", "nws", "mws")):
            sentiment_fields.append(fid)
        if lfid.startswith(("anl4_", "oth41_", "analyst_")):
            analyst_fields.append(fid)
        if lfid.startswith(("mdl", "model")):
            model_fields.append(fid)
        if lfid.startswith(("fnd", "eps", "sales", "assets", "roe", "roa", "debt", "ebitda", "net_income", "gross_profit")):
            fundamental_fields.append(fid)
        if lfid.startswith(("option", "implied_volatility", "parkinson_volatility", "pcr_vol", "put_", "call_")):
            option_fields.append(fid)
        if lfid in {"open", "high", "low", "close", "returns", "volume", "adv20", "sharesout", "cap", "vwap"}:
            price_volume_fields.append(fid)

    if not matrix_fields:
        matrix_fields = ["close", "returns", "volume", "cap", "vwap", "adv20"]
    if not vector_fields:
        vector_fields = ["anl4_eps_mean", "scl12_alltype_buzzvec", "oth41_s_west_eps_ftm_chg_3m"]
    if not sentiment_fields:
        sentiment_fields = ["scl12_sentiment", "scl12_buzz"]
    if not analyst_fields:
        analyst_fields = ["anl4_eps_mean", "anl4_revenue_mean"]
    if not model_fields:
        model_fields = ["mdl175_01dtsv", "mdl175_01icc"]
    if not fundamental_fields:
        fundamental_fields = ["eps", "sales", "assets", "net_income", "roe", "roa"]
    if not option_fields:
        option_fields = ["implied_volatility_call_120", "pcr_vol_30", "put_delta", "call_delta"]
    if not price_volume_fields:
        price_volume_fields = ["close", "open", "high", "low", "returns", "volume", "adv20", "sharesout", "cap", "vwap"]

    return {
        "matrix": _unique_expressions(matrix_fields),
        "vector": _unique_expressions(vector_fields),
        "sentiment": _unique_expressions(sentiment_fields),
        "analyst": _unique_expressions(analyst_fields),
        "model": _unique_expressions(model_fields),
        "fundamental": _unique_expressions(fundamental_fields),
        "option": _unique_expressions(option_fields),
        "pv": _unique_expressions(price_volume_fields),
        "group": group_fields,
    }


def _choose_placeholder_value(
    token: str,
    *,
    operators: set,
    pools: Dict[str, List[str]],
) -> str:
    key = str(token or "").strip().lower()

    def pick(items: Sequence[str], default: str) -> str:
        values = [str(x).strip() for x in items if str(x).strip()]
        if not values:
            return default
        return random.choice(values)

    def pick_op(candidates: Sequence[str], default: str) -> str:
        available = [op for op in candidates if op in operators]
        return pick(available, default if default in operators else (available[0] if available else default))

    if key.startswith("ts_op"):
        return pick_op(
            [
                "ts_rank",
                "ts_zscore",
                "ts_delta",
                "ts_ir",
                "ts_mean",
                "ts_std_dev",
                "ts_decay_linear",
                "ts_decay_exp_window",
                "ts_corr",
            ],
            "ts_rank",
        )
    if key.startswith("group_op"):
        return pick_op(["group_rank", "group_neutralize", "group_zscore"], "group_rank")
    if key.startswith("vec_op"):
        return pick_op(["vec_avg", "vec_sum", "vec_max", "vec_min", "vec_stddev", "vec_count"], "vec_avg")

    if "vector" in key or "analyst" in key or key in {"sentiment_vec"}:
        return pick(pools.get("vector", []), "anl4_eps_mean")
    if "sentiment" in key:
        return pick(pools.get("sentiment", []), "scl12_sentiment")
    if "model" in key:
        return pick(pools.get("model", []), "mdl175_01dtsv")
    if "fundamental" in key or "profit_field" in key or "size_field" in key or "leverage_field" in key:
        return pick(pools.get("fundamental", []), "eps")
    if "option" in key or "greek" in key or "pcr" in key or "volatility" in key or "breakeven" in key:
        return pick(pools.get("option", []), "implied_volatility_call_120")
    if "price" in key or "ret_field" in key or "volume_field" in key:
        return pick(pools.get("pv", []), "close")
    if key.startswith("field"):
        return pick(pools.get("matrix", []), "close")
    if key in {"alpha", "signal"}:
        return "ts_rank(close, 22)"
    if key in {"group", "group1", "group2"}:
        return pick(pools.get("group", []), "industry")

    if key == "d":
        return pick(["5", "22", "66", "126", "252"], "22")
    if key in {"d1", "d2", "d3", "d_short", "d_long", "d_backfill", "decay_d"}:
        return pick(["5", "10", "22", "66", "126", "252"], "22")
    if key in {"k"}:
        return pick(["2", "3", "4", "0.3", "0.5"], "2")
    if "threshold" in key:
        return pick(["0.05", "0.1", "0.2", "0.5", "0.8"], "0.1")
    if key in {"std"}:
        return pick(["3", "4", "5"], "4")
    if key in {"range"}:
        return "0.1,1,0.1"
    if key in {"factor", "f"}:
        return pick(["0.04", "0.1", "0.5", "0.9"], "0.5")
    if key in {"target", "target_tvr"}:
        return pick(["0.1", "0.15", "0.2"], "0.1")
    if key in {"weight1", "weight2"}:
        return pick(["0.33", "0.5", "0.67"], "0.5")

    if key in {"low"}:
        return pick(["0.1", "0.25"], "0.1")
    if key in {"high"}:
        return pick(["100", "1000"], "100")
    if key in {"c"}:
        return pick(["0.05", "0.1"], "0.1")
    if key in {"p", "q"}:
        return pick(["0.25", "0.5", "0.75"], "0.5")
    if key in {"min_count"}:
        return pick(["2", "3", "5"], "3")
    if key in {"position"}:
        return pick(["0", "1"], "0")
    if key in {"days", "window"}:
        return pick(["1", "2", "5", "10"], "5")

    return "22" if key.startswith("d") else "close"


def _instantiate_template_line(line: str, *, operators: set, pools: Dict[str, List[str]]) -> str:
    text = _normalize_template_line(line)
    if not text:
        return ""
    # Skip pure assignment lines for seed expressions.
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", text):
        return ""

    def repl_angle(match: re.Match) -> str:
        token = match.group(1)
        return _choose_placeholder_value(token, operators=operators, pools=pools)

    def repl_brace(match: re.Match) -> str:
        token = match.group(1)
        return _choose_placeholder_value(token, operators=operators, pools=pools)

    text = re.sub(r"<\s*([A-Za-z0-9_]+)\s*/\s*>", repl_angle, text)
    text = re.sub(r"\{([A-Za-z0-9_]+)\}", repl_brace, text)
    text = " ".join(text.split())
    text = text.rstrip(";").strip()
    if not text:
        return ""
    if "<" in text or "{" in text:
        return ""
    if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\s*=", text):
        return ""
    if "模板" in text:
        return ""
    if not TemplateGenerator._is_balanced(text):
        return ""
    return text


def _template_line_compatible(line: str, *, operators: set) -> bool:
    text = _normalize_template_line(line)
    if not text:
        return False
    fn_names = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", text)
    for fn in fn_names:
        if fn not in operators:
            return False
    return True


def _render_template_seed_expressions(
    template_lines: Sequence[str],
    *,
    fields: Sequence[DataField],
    operators: Sequence[Dict],
    count: int,
) -> List[str]:
    if not template_lines or count <= 0:
        return []
    operator_names = {str(op.get("name", "")).strip() for op in operators if op.get("name")}
    pools = _field_pools_for_templates(fields)
    compatible_lines = [
        line
        for line in template_lines
        if _template_line_compatible(line, operators=operator_names)
    ]
    source_lines = compatible_lines if compatible_lines else list(template_lines)
    picks = random.sample(source_lines, min(len(source_lines), max(count * 3, count)))
    out: List[str] = []
    seen = set()
    for line in picks:
        expr = _instantiate_template_line(line, operators=operator_names, pools=pools)
        if not expr or expr in seen:
            continue
        seen.add(expr)
        out.append(expr)
        if len(out) >= count:
            break
    return out


_THEME_OPERATOR_GROUPS = {
    "A": {"trade_when", "keep", "if_else", "nan_mask"},
    "B": {
        "days_from_last_change",
        "filter",
        "group_backfill",
        "hump",
        "hump_decay",
        "jump_decay",
        "kth_element",
        "last_diff_value",
        "ts_backfill",
    },
    "C": {"clamp", "left_tail", "nan_out", "pasteurize", "purify", "replace", "right_tail", "tail", "truncate", "winsorize"},
    "D": {
        "group_multi_regression",
        "group_vector_neut",
        "group_vector_proj",
        "multi_regression",
        "regression_neut",
        "regression_proj",
        "ts_poly_regression",
        "ts_regression",
        "ts_theilsen",
        "ts_vector_neut",
        "ts_vector_proj",
        "vector_neut",
        "vector_proj",
    },
    "E": {"ts_co_kurtosis", "ts_co_skewness", "ts_corr", "ts_covariance", "ts_partial_corr", "ts_triple_corr"},
    "F": {
        "inst_pnl",
        "inst_tvr",
        "one_side",
        "rank_by_side",
        "scale",
        "scale_down",
        "ts_delta_limit",
        "ts_target_tvr_decay",
        "ts_target_tvr_delta_limit",
        "ts_target_tvr_hump",
    },
}

_COMMON_OPERATOR_SET = {
    "ts_sum",
    "ts_mean",
    "rank",
    "zscore",
    "winsorize",
    "ts_std_dev",
    "scale",
    "round",
    "trade_when",
}


def _batch_constraint_violations(
    expressions: Sequence[str],
    reports: Dict[str, Dict],
    *,
    target_count: int = 0,
    enforce_exact_batch: bool = False,
    required_theme_coverage: int = 0,
    common_operator_limit: int = 0,
    enforce_explore_theme_pairs: bool = False,
) -> List[str]:
    violations: List[str] = []
    total = len(expressions)
    if enforce_exact_batch and int(target_count) > 0 and total != int(target_count):
        violations.append(f"batch size {total} != required {int(target_count)}")
    if total == 0:
        violations.append("empty batch")
        return violations

    usage: Dict[str, int] = {}
    expr_ops: List[set] = []
    for expr in expressions:
        ops = set(reports.get(expr, {}).get("operators_used") or [])
        expr_ops.append(ops)
        for op in ops:
            usage[op] = usage.get(op, 0) + 1

    if int(common_operator_limit) > 0:
        capped = int(common_operator_limit)
        over = sorted((op, cnt) for op, cnt in usage.items() if op in _COMMON_OPERATOR_SET and cnt > capped)
        if over:
            violations.append(
                "common operator cap exceeded: "
                + ", ".join(f"{op}={cnt}" for op, cnt in over)
                + f" (limit={capped})"
            )

    if int(required_theme_coverage) > 0:
        covered = {
            theme
            for theme, theme_ops in _THEME_OPERATOR_GROUPS.items()
            if any(op in theme_ops for op in usage.keys())
        }
        need = int(required_theme_coverage)
        if len(covered) < need:
            violations.append(
                f"theme coverage {len(covered)} < required {need} (covered={','.join(sorted(covered)) or 'none'})"
            )

    if enforce_explore_theme_pairs and total >= 8:
        pair_seen = set()
        for idx in range(5, 8):
            ops = expr_ops[idx]
            themes = sorted([theme for theme, theme_ops in _THEME_OPERATOR_GROUPS.items() if ops.intersection(theme_ops)])
            if len(themes) < 2:
                violations.append(f"candidate #{idx + 1} has <2 theme operators")
                continue
            pair = tuple(themes[:2])
            if pair in pair_seen:
                violations.append(f"duplicate explore theme pair {pair} in candidate #{idx + 1}")
            pair_seen.add(pair)

    return violations


def _select_batch_from_pool(
    pool: Sequence[str],
    reports: Dict[str, Dict],
    *,
    target_count: int,
    enforce_exact_batch: bool,
    required_theme_coverage: int,
    common_operator_limit: int,
    enforce_explore_theme_pairs: bool,
) -> Tuple[List[str], List[str]]:
    if target_count <= 0:
        return [], ["target_count must be positive"]
    if len(pool) < target_count:
        return [], [f"valid pool {len(pool)} < required {target_count}"]

    trial_batches: List[List[str]] = [list(pool[:target_count])]
    if len(pool) > target_count:
        sample_tries = min(240, max(24, len(pool) * 10))
        for _ in range(sample_tries):
            trial_batches.append(random.sample(list(pool), target_count))

    best: List[str] = []
    best_violations: List[str] = []
    seen = set()
    for batch in trial_batches:
        sig = tuple(batch)
        if sig in seen:
            continue
        seen.add(sig)
        violations = _batch_constraint_violations(
            batch,
            reports,
            target_count=target_count,
            enforce_exact_batch=enforce_exact_batch,
            required_theme_coverage=required_theme_coverage,
            common_operator_limit=common_operator_limit,
            enforce_explore_theme_pairs=enforce_explore_theme_pairs,
        )
        if not violations:
            return batch, []
        if not best_violations or len(violations) < len(best_violations):
            best = batch
            best_violations = violations
    return [], (best_violations or ["unable to satisfy batch constraints"])


def _prepare_candidate_batch(
    *,
    generator: TemplateGenerator,
    region: str,
    fields: Sequence[DataField],
    count: int,
    style_prompt: str,
    operators: Sequence[Dict],
    operator_file: str = "",
    strict_validation: bool = False,
    max_operator_count: int = 0,
    require_keyword_optional: bool = True,
    enforce_exact_batch: bool = False,
    required_theme_coverage: int = 0,
    common_operator_limit: int = 0,
    enforce_explore_theme_pairs: bool = False,
    template_lines: Optional[Sequence[str]] = None,
    template_style_items: int = 0,
    template_seed_count: int = 0,
    policy_prompt: str = "",
    max_generate_attempts: int = 4,
) -> Tuple[List[str], Dict[str, Dict]]:
    target = max(1, int(count))
    pool: List[str] = []
    reports: Dict[str, Dict] = {}
    seen = set()
    last_violations: List[str] = []

    def add_candidate(expr: str) -> None:
        expr = str(expr or "").strip()
        if not expr or expr in seen:
            return
        seen.add(expr)
        report = validate_expression_report(
            expression=expr,
            operators=operators,
            operator_file=operator_file,
            region=region,
            max_operator_count=max_operator_count,
            require_known_operators=True,
            require_keyword_optional=require_keyword_optional,
        )
        reports[expr] = report
        if strict_validation and not report.get("is_valid"):
            logging.warning("Preflight reject: %s | %s", expr, "; ".join(report.get("errors", [])[:2]))
            return
        pool.append(expr)

    if template_lines and int(template_seed_count) > 0:
        seed_exprs = _render_template_seed_expressions(
            template_lines,
            fields=fields,
            operators=operators,
            count=int(template_seed_count),
        )
        for expr in seed_exprs:
            add_candidate(expr)
        if seed_exprs:
            logging.info("Template seed candidates added: %s", len(seed_exprs))
        selected, violations = _select_batch_from_pool(
            pool,
            reports,
            target_count=target,
            enforce_exact_batch=enforce_exact_batch,
            required_theme_coverage=required_theme_coverage,
            common_operator_limit=common_operator_limit,
            enforce_explore_theme_pairs=enforce_explore_theme_pairs,
        )
        if selected:
            return selected, {expr: reports.get(expr, {}) for expr in selected}
        last_violations = violations

    for attempt in range(1, max(1, int(max_generate_attempts)) + 1):
        needed = max(1, target - len(pool))
        request_count = max(needed, min(target * 2, needed + 4))
        attempt_style = style_prompt
        if template_lines and int(template_style_items) > 0:
            snippet = _build_template_style_snippet(
                template_lines,
                max_items=max(1, int(template_style_items)),
            )
            if snippet:
                attempt_style = merge_style_prompt(style_prompt, snippet)
        generated = _generate_expressions(
            generator,
            region,
            fields,
            request_count,
            attempt_style,
            policy_prompt=policy_prompt,
        )

        for expr in generated:
            add_candidate(expr)

        selected, violations = _select_batch_from_pool(
            pool,
            reports,
            target_count=target,
            enforce_exact_batch=enforce_exact_batch,
            required_theme_coverage=required_theme_coverage,
            common_operator_limit=common_operator_limit,
            enforce_explore_theme_pairs=enforce_explore_theme_pairs,
        )
        if selected:
            return selected, {expr: reports.get(expr, {}) for expr in selected}
        last_violations = violations
        logging.info(
            "Batch preflight attempt %s/%s pending: pool=%s target=%s reason=%s",
            attempt,
            max(1, int(max_generate_attempts)),
            len(pool),
            target,
            "; ".join(last_violations[:2]) if last_violations else "unknown",
        )

    if enforce_exact_batch:
        raise RuntimeError(
            "Cannot produce a compliant expression batch: "
            + ("; ".join(last_violations) if last_violations else "insufficient valid candidates")
        )

    fallback = list(pool[:target])
    if not fallback:
        raise RuntimeError("No expressions generated")
    return fallback, {expr: reports.get(expr, {}) for expr in fallback}


def _append_round_results_file(path: Path, rows: Sequence[Dict[str, float]], *, round_idx: int, stage: str) -> str:
    payload_time = time.strftime("%Y-%m-%d %H:%M:%S")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            payload = {
                "time": payload_time,
                "round": int(round_idx),
                "stage": stage,
                "expression": str(row.get("expression", "")).strip(),
                "alpha_id": str(row.get("alpha_id", "")).strip(),
                "sharpe": float(row.get("sharpe", 0.0)),
                "fitness": float(row.get("fitness", 0.0)),
                "turnover": float(row.get("turnover", 0.0)),
                "success": bool(row.get("success", False)),
                "link": str(row.get("link", "")).strip(),
            }
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
    return str(path)


def _evaluate_expressions(
    username: str,
    password: str,
    timeout_sec: int,
    max_retries: int,
    expressions: Sequence[str],
    settings: SimulationSettings,
    poll_interval_sec: int,
    max_wait_sec: int,
    concurrency: int,
    concurrency_cap: int = 0,
    disable_proxy: Optional[bool] = None,
    progress_cb: Optional[Callable[[Dict], None]] = None,
    round_idx: int = 0,
    stage: str = "simulate",
    row_cb: Optional[Callable[[Dict], None]] = None,
) -> List[Dict[str, float]]:
    return _evaluate_expressions_async(
        username=username,
        password=password,
        timeout_sec=timeout_sec,
        max_retries=max_retries,
        expressions=expressions,
        settings=settings,
        poll_interval_sec=poll_interval_sec,
        max_wait_sec=max_wait_sec,
        concurrency=concurrency,
        concurrency_cap=concurrency_cap,
        disable_proxy=disable_proxy,
        progress_cb=progress_cb,
        round_idx=round_idx,
        stage=stage,
        row_cb=row_cb,
    )


def _retry_failed_expressions(
    evaluator: Callable[..., List[Dict[str, float]]],
    *,
    expressions: Sequence[str],
    retry_rounds: int,
    retry_sleep_sec: int,
    stage: str,
    stop_event: Optional[threading.Event] = None,
    **kwargs,
) -> List[Dict[str, float]]:
    results = evaluator(expressions=expressions, stage=stage, **kwargs)
    by_expr = {str(row.get("expression", "")): row for row in results}
    pending = [expr for expr in expressions if not by_expr.get(expr, {}).get("success")]

    if int(retry_rounds) < 0:
        attempt = 0
        while pending:
            if stop_event is not None and stop_event.is_set():
                logging.info("Stop requested, exiting retries with %s pending", len(pending))
                break
            attempt += 1
            if retry_sleep_sec and int(retry_sleep_sec) > 0:
                time.sleep(int(retry_sleep_sec))
            logging.info("Retrying %s expressions (attempt %s/unbounded)", len(pending), attempt)
            retry_stage = f"{stage}_retry{attempt}"
            retry_results = evaluator(expressions=pending, stage=retry_stage, **kwargs)
            for row in retry_results:
                expr = str(row.get("expression", ""))
                by_expr[expr] = row
            pending = [expr for expr in pending if not by_expr.get(expr, {}).get("success")]
    else:
        for attempt in range(1, max(0, int(retry_rounds)) + 1):
            if not pending:
                break
            if stop_event is not None and stop_event.is_set():
                logging.info("Stop requested, exiting retries with %s pending", len(pending))
                break
            if retry_sleep_sec and int(retry_sleep_sec) > 0:
                time.sleep(int(retry_sleep_sec))
            logging.info("Retrying %s expressions (%s/%s)", len(pending), attempt, retry_rounds)
            retry_stage = f"{stage}_retry{attempt}"
            retry_results = evaluator(expressions=pending, stage=retry_stage, **kwargs)
            for row in retry_results:
                expr = str(row.get("expression", ""))
                by_expr[expr] = row
            pending = [expr for expr in pending if not by_expr.get(expr, {}).get("success")]

    if pending:
        logging.warning("Still failed after %s retries: %s", retry_rounds, len(pending))

    return [by_expr[expr] for expr in expressions if expr in by_expr]


def _evaluate_expressions_async(
    username: str,
    password: str,
    timeout_sec: int,
    max_retries: int,
    expressions: Sequence[str],
    settings: SimulationSettings,
    poll_interval_sec: int,
    max_wait_sec: int,
    concurrency: int,
    concurrency_cap: int = 0,
    disable_proxy: Optional[bool] = None,
    progress_cb: Optional[Callable[[Dict], None]] = None,
    round_idx: int = 0,
    stage: str = "simulate",
    row_cb: Optional[Callable[[Dict], None]] = None,
) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    total = len(expressions)
    if total == 0:
        return results

    seed = WorldQuantBrainClient(
        username=username,
        password=password,
        timeout_sec=max(5, int(timeout_sec)),
        max_retries=max(1, int(max_retries)),
        disable_proxy=disable_proxy,
    )
    seed.authenticate()

    completed = 0
    success = 0
    failed = 0
    if progress_cb:
        progress_cb(
            {
                "stage": stage,
                "round": round_idx,
                "total": total,
                "completed": 0,
                "success": 0,
                "failed": 0,
                "last": {},
            }
        )

    max_workers = max(1, min(int(concurrency), total))
    cap = int(concurrency_cap) if concurrency_cap else 0
    if cap > 0:
        max_workers = min(max_workers, cap)

    async def run_one(idx: int, expr: str) -> Dict[str, float]:
        try:
            result = await seed._clone_client().async_simulate_expression(
                expression=expr,
                settings=settings,
                poll_interval_sec=max(1, int(poll_interval_sec)),
                max_wait_sec=max(30, int(max_wait_sec)),
            )
            if result.success:
                row = {
                    "expression": expr,
                    "sharpe": float(result.sharpe),
                    "fitness": float(result.fitness),
                    "turnover": float(result.turnover),
                    "alpha_id": result.alpha_id,
                    "link": result.link,
                    "success": True,
                }
            else:
                row = {
                    "expression": expr,
                    "sharpe": 0.0,
                    "fitness": 0.0,
                    "turnover": 0.0,
                    "alpha_id": "",
                    "link": "",
                    "success": False,
                }
        except Exception as exc:
            logging.warning("Simulation failed (%s/%s): %s", idx, total, exc)
            row = {
                "expression": expr,
                "sharpe": 0.0,
                "fitness": 0.0,
                "turnover": 0.0,
                "alpha_id": "",
                "link": "",
                "success": False,
            }
        row["index"] = idx
        if row_cb:
            row_cb(row)
        return row

    async def runner() -> List[Dict[str, float]]:
        nonlocal completed, success, failed
        sem = asyncio.Semaphore(max_workers)

        async def run_one_async(idx: int, expr: str) -> Dict[str, float]:
            async with sem:
                return await run_one(idx, expr)

        tasks = [asyncio.create_task(run_one_async(idx, expr)) for idx, expr in enumerate(expressions, start=1)]
        out: List[Dict[str, float]] = []
        for fut in asyncio.as_completed(tasks):
            row = await fut
            out.append(row)
            completed += 1
            if row.get("success"):
                success += 1
            else:
                failed += 1
            if progress_cb:
                progress_cb(
                    {
                        "stage": stage,
                        "round": round_idx,
                        "total": total,
                        "completed": completed,
                        "success": success,
                        "failed": failed,
                        "last": {
                            "expression": row.get("expression", ""),
                            "sharpe": row.get("sharpe", 0.0),
                            "fitness": row.get("fitness", 0.0),
                            "turnover": row.get("turnover", 0.0),
                            "alpha_id": row.get("alpha_id", ""),
                        },
                    }
                )
        return out

    results = asyncio.run(runner())
    results_sorted = sorted(results, key=lambda x: int(x.get("index", 0)))
    for row in results_sorted:
        logging.info(
            "Simulated %s/%s sharpe=%.3f fitness=%.3f turnover=%.2f",
            row.get("index", 0),
            total,
            row.get("sharpe", 0.0),
            row.get("fitness", 0.0),
            row.get("turnover", 0.0),
        )
        row.pop("index", None)

    return results_sorted


def _write_results_json(path: Path, rows: Sequence[Dict[str, float]]) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = [
        {
            "expression": str(row.get("expression", "")).strip(),
            "sharpe": float(row.get("sharpe", 0.0)),
            "fitness": float(row.get("fitness", 0.0)),
            "turnover": float(row.get("turnover", 0.0)),
            "alpha_id": str(row.get("alpha_id", "")).strip(),
            "link": str(row.get("link", "")).strip(),
            "success": bool(row.get("success", False)),
        }
        for row in rows
        if row.get("expression")
    ]
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return str(path)


def _append_library(
    library_path: str,
    rows: Sequence[Dict[str, float]],
    sharpe_threshold: float,
    fitness_threshold: float,
) -> List[str]:
    if not library_path:
        return []
    src = Path(library_path)
    existing: List[str] = []
    if src.exists():
        try:
            payload = json.loads(src.read_text(encoding="utf-8"))
        except Exception:
            payload = []
        if isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict) and item.get("expression"):
                    existing.append(str(item.get("expression", "")).strip())
                elif isinstance(item, str):
                    existing.append(item.strip())

    seen = {x for x in existing if x}
    added: List[str] = []
    for row in rows:
        expr = str(row.get("expression", "")).strip()
        if not expr or expr in seen:
            continue
        if float(row.get("sharpe", 0.0)) >= sharpe_threshold and float(row.get("fitness", 0.0)) >= fitness_threshold:
            seen.add(expr)
            added.append(expr)

    if not added:
        return []

    merged = existing + added
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    return added


def _shorten_text(value: str, max_len: int = 140) -> str:
    text = " ".join((value or "").split())
    if max_len <= 0:
        return text
    if len(text) <= max_len:
        return text
    return text[: max_len - 3] + "..."


def _build_notify_url(base_url: str, message: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if not parsed.scheme and parsed.path:
        parsed = urlparse("https://" + raw)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["msg"] = message
    new_query = urlencode(query, doseq=True)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _send_notify(
    base_url: str,
    message: str,
    timeout_sec: float = 6.0,
    max_retries: int = 3,
) -> bool:
    url = _build_notify_url(base_url, message)
    if not url:
        return False
    for attempt in range(1, max_retries + 1):
        try:
            req = Request(url, headers={"User-Agent": "wqminer/notify"})
            with urlopen(req, timeout=timeout_sec) as resp:
                resp.read(1024)
            return True
        except Exception as exc:
            if attempt >= max_retries:
                logging.warning("Notify failed: %s", exc)
                return False
            backoff = min(6, 2 ** (attempt - 1))
            time.sleep(backoff)
    return False


def _format_notify_message(row: Dict[str, float], region: str, universe: str, delay: int, round_idx: int) -> str:
    sharpe = float(row.get("sharpe", 0.0))
    fitness = float(row.get("fitness", 0.0))
    turnover = float(row.get("turnover", 0.0))
    link = str(row.get("link", "")).strip()
    alpha_id = str(row.get("alpha_id", "")).strip()
    if not link and alpha_id:
        link = f"https://platform.worldquantbrain.com/alpha/{alpha_id}"
    lines = [
        f"sharpe={sharpe:.3f} fitness={fitness:.3f} turnover={turnover:.2f}",
        f"link={link}" if link else "link=",
    ]
    return "\n".join(lines)


def _maybe_notify_row(
    row: Dict[str, float],
    region: str,
    universe: str,
    delay: int,
    round_idx: int,
    notify_url: str,
    notified_seen: set,
) -> None:
    if not notify_url:
        return
    if not row.get("success"):
        return
    alpha_id = str(row.get("alpha_id", "")).strip()
    expr = str(row.get("expression", "")).strip()
    link = str(row.get("link", "")).strip()
    if not link and alpha_id:
        link = f"https://platform.worldquantbrain.com/alpha/{alpha_id}"
    key = alpha_id or expr
    if not key or key in notified_seen or not link:
        return
    row_view = dict(row)
    row_view["link"] = link
    row_view["alpha_id"] = alpha_id
    message = _format_notify_message(row_view, region, universe, delay, round_idx)
    if _send_notify(notify_url, message):
        notified_seen.add(key)
        logging.info("Notify sent: %s", key)


def run_one_click(
    region: str,
    universe: str,
    delay: int,
    llm_config_path: str,
    credentials_path: str = "",
    username: str = "",
    password: str = "",
    template_count: int = 20,
    style_prompt: str = "",
    inspiration: str = "",
    output_dir: str = "results/one_click",
    concurrency: int = 3,
    concurrency_profile: str = "advisor",
    async_mode: bool = False,
    timeout_sec: int = 60,
    max_retries: int = 5,
    poll_interval_sec: int = 30,
    max_wait_sec: int = 600,
    max_rounds: int = 0,
    sleep_between_rounds: int = 5,
    evolve_rounds: int = 0,
    evolve_count: int = 0,
    evolve_top_k: int = 6,
    concurrency_cap: int = 0,
    seed_templates: str = "",
    library_output: str = "",
    library_sharpe_min: float = 1.2,
    library_fitness_min: float = 1.0,
    reverse_sharpe_max: float = -1.2,
    reverse_fitness_max: float = -1.0,
    reverse_log: str = "",
    negate_max_per_round: int = 0,
    retry_failed_rounds: int = 2,
    retry_failed_sleep: int = 2,
    disable_proxy: Optional[bool] = None,
    notify_url: str = "",
    operator_file: str = "",
    strict_validation: bool = False,
    max_operator_count: int = 0,
    require_keyword_optional: bool = True,
    batch_size: int = 0,
    enforce_exact_batch: bool = False,
    required_theme_coverage: int = 0,
    common_operator_limit: int = 0,
    enforce_explore_theme_pairs: bool = False,
    template_guide_path: Optional[Sequence[str] | str] = "",
    template_style_items: int = 0,
    template_seed_count: int = 0,
    generate_inspiration: bool = False,
    ai_worker_file: str = "wqminer/constants/worker_prompt_compact.md",
    max_generate_attempts: int = 4,
    dataset_ids: Optional[Sequence[str] | str] = None,
    dataset_field_max_pages: int = 5,
    dataset_field_page_limit: int = 50,
    results_append_file: str = "",
    baseline_alpha_id: str = "",
    progress_cb: Optional[Callable[[Dict], None]] = None,
    stop_event: Optional[threading.Event] = None,
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)
    neutralization = get_default_neutralization(region)
    selected_dataset_ids = _normalize_dataset_ids(dataset_ids)
    runtime = _resolve_parallel_runtime(
        concurrency_profile=concurrency_profile,
        concurrency=concurrency,
        concurrency_cap=concurrency_cap,
        poll_interval_sec=poll_interval_sec,
        template_count=template_count,
        batch_size=batch_size,
    )
    effective_concurrency = int(runtime["effective_concurrency"])
    effective_cap = int(runtime["concurrency_cap"])
    effective_poll_interval = int(runtime["poll_interval_sec"])
    effective_template_count = int(runtime["template_count"])

    logging.info(
        "Parallel profile=%s requested=%s cap=%s effective=%s poll=%ss template_count=%s",
        runtime["profile"],
        runtime["requested_concurrency"],
        effective_cap,
        effective_concurrency,
        effective_poll_interval,
        effective_template_count,
    )

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)

    fields_cache = default_fields_cache_path(region, universe, delay, dataset_ids=selected_dataset_ids)
    if Path(fields_cache).exists():
        fields = load_data_fields_cache(fields_cache)
        logging.info("Using cached fields: %s (count=%d)", fields_cache, len(fields))
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
            logging.info(
                "Fetched fields from selected datasets: %s datasets, %s fields",
                len(selected_dataset_ids),
                len(fields),
            )
        else:
            fields = client.fetch_data_fields(region=region, universe=universe, delay=delay)
        if not fields:
            fields = client.load_fallback_default_fields()
        save_data_fields_cache(fields_cache, fields)

    if not inspiration and bool(generate_inspiration):
        seed_exprs = _load_seed_expressions(seed_templates)
        inspiration = generate_inspiration_text(
            llm_config_path=llm_config_path,
            region=region,
            universe=universe,
            delay=delay,
            style_seed=style_prompt,
            seed_expressions=seed_exprs,
        )
    ai_worker_guidance = _load_ai_worker_guidance(ai_worker_file)
    if ai_worker_guidance:
        logging.info("Loaded AI worker guidance from %s", ai_worker_file)

    operators = load_operators(operator_file)
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    generator = TemplateGenerator(llm=llm, operators=operators)
    template_lines, loaded_guide_paths = _load_template_guides(template_guide_path, max_items=700)
    if template_lines:
        logging.info(
            "Loaded %s guide templates from %s",
            len(template_lines),
            ", ".join(loaded_guide_paths),
        )
    elif template_guide_path:
        logging.info("Template guide not found or empty: %s", template_guide_path)

    base_style = _compose_generation_style(
        style_prompt=style_prompt,
        inspiration=inspiration,
        ai_worker_guidance=ai_worker_guidance,
    )
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)

    files: List[str] = []
    settings = SimulationSettings(
        region=region,
        universe=universe,
        delay=delay,
        neutralization=neutralization,
    )

    round_limit = max(0, int(max_rounds))
    if round_limit <= 0 and int(evolve_rounds) > 0:
        round_limit = int(evolve_rounds)

    round_idx = 0
    reflection = ""
    appended = 0
    results: List[Dict[str, float]] = []
    negated_seen: set = set()
    reverse_log_path = Path(reverse_log) if reverse_log else None
    append_path: Optional[Path] = None
    append_candidate = (results_append_file or "").strip()
    if append_candidate:
        append_path = Path(append_candidate)
    elif baseline_alpha_id:
        append_path = Path(f"{baseline_alpha_id}_optimization_results.txt")
    notify_url = (notify_url or "").strip()
    if notify_url:
        logging.info("Notify enabled: %s", notify_url.split("?")[0])
    else:
        logging.info("Notify disabled: no notify_url configured")
    notified_seen: set = set()
    evaluator = _evaluate_expressions_async if async_mode else _evaluate_expressions

    try:
        if progress_cb:
            progress_cb({"stage": "init", "round": 0, "total": 0, "completed": 0, "success": 0, "failed": 0, "last": {}})
        while True:
            if stop_event is not None and stop_event.is_set():
                logging.info("Stop requested, exiting before round %s", round_idx + 1)
                break
            round_idx += 1
            if int(batch_size) > 0:
                per_round = int(batch_size)
            else:
                base_count = evolve_count if evolve_count and int(evolve_count) > 0 else effective_template_count
                per_round = max(1, int(base_count))
            style = base_style if not reflection else merge_style_prompt(base_style, reflection)
            if progress_cb:
                progress_cb({"stage": "generate", "round": round_idx, "total": 0, "completed": 0, "success": 0, "failed": 0, "last": {}})
            expressions, _preflight_reports = _prepare_candidate_batch(
                generator=generator,
                region=region,
                fields=fields,
                count=per_round,
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
                policy_prompt=ai_worker_guidance,
                max_generate_attempts=max(1, int(max_generate_attempts)),
            )
            def row_cb(row: Dict[str, float]) -> None:
                _maybe_notify_row(row, region, universe, delay, round_idx, notify_url, notified_seen)
            if retry_failed_rounds and int(retry_failed_rounds) > 0:
                results = _retry_failed_expressions(
                    evaluator,
                    expressions=expressions,
                    retry_rounds=retry_failed_rounds,
                    retry_sleep_sec=retry_failed_sleep,
                    stage="simulate",
                    stop_event=stop_event,
                    username=user,
                    password=pwd,
                    timeout_sec=timeout_sec,
                    max_retries=max_retries,
                    settings=settings,
                    poll_interval_sec=effective_poll_interval,
                    max_wait_sec=max_wait_sec,
                    concurrency=effective_concurrency,
                    concurrency_cap=effective_cap,
                    disable_proxy=disable_proxy,
                    progress_cb=progress_cb,
                    round_idx=round_idx,
                    row_cb=row_cb,
                )
            else:
                results = evaluator(
                    username=user,
                    password=pwd,
                    timeout_sec=timeout_sec,
                    max_retries=max_retries,
                    expressions=expressions,
                    settings=settings,
                    poll_interval_sec=effective_poll_interval,
                    max_wait_sec=max_wait_sec,
                    concurrency=effective_concurrency,
                    concurrency_cap=effective_cap,
                    disable_proxy=disable_proxy,
                    progress_cb=progress_cb,
                    round_idx=round_idx,
                    stage="simulate",
                    row_cb=row_cb,
                )
            if append_path is not None:
                _append_round_results_file(append_path, results, round_idx=round_idx, stage="simulate")
            reverse_candidates = _collect_reverse_candidates(
                results,
                sharpe_max=reverse_sharpe_max,
                fitness_max=reverse_fitness_max,
                log_path=reverse_log_path,
                seen=negated_seen,
                max_count=negate_max_per_round,
            )
            if reverse_candidates:
                logging.info("Reverse factors detected: %d, evaluating negated expressions", len(reverse_candidates))
                if retry_failed_rounds and int(retry_failed_rounds) > 0:
                    negated_results = _retry_failed_expressions(
                        evaluator,
                        expressions=reverse_candidates,
                        retry_rounds=retry_failed_rounds,
                        retry_sleep_sec=retry_failed_sleep,
                        stage="negate",
                        stop_event=stop_event,
                        username=user,
                        password=pwd,
                        timeout_sec=timeout_sec,
                        max_retries=max_retries,
                        settings=settings,
                        poll_interval_sec=effective_poll_interval,
                        max_wait_sec=max_wait_sec,
                        concurrency=effective_concurrency,
                        concurrency_cap=effective_cap,
                        disable_proxy=disable_proxy,
                        progress_cb=progress_cb,
                        round_idx=round_idx,
                        row_cb=row_cb,
                    )
                else:
                    negated_results = evaluator(
                        username=user,
                        password=pwd,
                        timeout_sec=timeout_sec,
                        max_retries=max_retries,
                        expressions=reverse_candidates,
                        settings=settings,
                        poll_interval_sec=effective_poll_interval,
                        max_wait_sec=max_wait_sec,
                        concurrency=effective_concurrency,
                        concurrency_cap=effective_cap,
                        disable_proxy=disable_proxy,
                        progress_cb=progress_cb,
                        round_idx=round_idx,
                        stage="negate",
                        row_cb=row_cb,
                    )
                results.extend(negated_results)
                if append_path is not None:
                    _append_round_results_file(append_path, negated_results, round_idx=round_idx, stage="negate")
            files.append(_write_results_json(out_root / f"one_click_{ts}_round{round_idx:03}.json", results))
            added = _append_library(library_output, results, library_sharpe_min, library_fitness_min)
            appended += len(added)

            reflection = generate_reflection_text(
                llm_config_path=llm_config_path,
                region=region,
                universe=universe,
                delay=delay,
                round_index=round_idx,
                rows=results,
            )
            if reflection:
                logging.info("Reflection (round %s): %s", round_idx, reflection)

            if round_limit > 0 and round_idx >= round_limit:
                break
            if sleep_between_rounds and int(sleep_between_rounds) > 0:
                time.sleep(int(sleep_between_rounds))
    except KeyboardInterrupt:
        logging.info("Interrupted by user, stopping after round %s", round_idx)

    return {
        "files": files,
        "inspiration": inspiration,
        "final_count": len(results),
        "library_appended": appended,
        "results_append_file": str(append_path) if append_path else "",
        "fields_cache": fields_cache,
        "dataset_ids": selected_dataset_ids,
        "parallel_runtime": runtime,
    }
