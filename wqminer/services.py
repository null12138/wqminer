"""Minimal service layer for one-click flow."""

from __future__ import annotations

import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from .config import load_credentials, load_llm_config
from .inspiration import merge_style_prompt
from .llm_client import OpenAICompatibleLLM
from .models import DataField, SimulationSettings
from .operator_store import load_operators
from .region_config import get_default_neutralization, get_default_universe
from .storage import load_data_fields_cache, save_data_fields_cache
from .template_generator import TemplateGenerator
from .worldquant_client import WorldQuantBrainClient


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


def _summarize_results(rows: Sequence[Dict[str, float]]) -> Dict[str, float]:
    total = len(rows)
    if total == 0:
        return {"total": 0, "avg_sharpe": 0.0, "avg_fitness": 0.0, "avg_turnover": 0.0}
    avg_sharpe = sum(float(r.get("sharpe", 0.0)) for r in rows) / total
    avg_fitness = sum(float(r.get("fitness", 0.0)) for r in rows) / total
    avg_turnover = sum(float(r.get("turnover", 0.0)) for r in rows) / total
    return {
        "total": total,
        "avg_sharpe": avg_sharpe,
        "avg_fitness": avg_fitness,
        "avg_turnover": avg_turnover,
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
        "Provide a brief reflection and next-step guidance. "
        "No chain-of-thought. Keep it under 3 sentences."
    )
    user_prompt = (
        f"Round: {round_index}\n"
        f"Region: {region}\n"
        f"Universe: {universe}\n"
        f"Delay: {delay}\n"
        f"Total: {summary['total']}\n"
        f"Avg sharpe: {summary['avg_sharpe']:.3f}\n"
        f"Avg fitness: {summary['avg_fitness']:.3f}\n"
        f"Avg turnover: {summary['avg_turnover']:.2f}\n"
        "Top expressions:\n"
        + ("\n".join(lines) if lines else "none")
        + "\n\nReturn a short reflection and next guidance."
    )

    raw = llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.4)
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
) -> List[str]:
    templates = generator.generate_templates(
        region=region,
        data_fields=list(fields),
        count=max(1, int(count)),
        style_prompt=style_prompt,
    )
    expressions = [t.expression for t in templates if t and t.expression]
    return _unique_expressions(expressions)


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
) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    total = len(expressions)
    if total == 0:
        return results

    auth_lock = threading.Lock()
    local = threading.local()

    def get_client() -> WorldQuantBrainClient:
        client = getattr(local, "client", None)
        if client is None:
            client = WorldQuantBrainClient(
                username=username,
                password=password,
                timeout_sec=max(5, int(timeout_sec)),
                max_retries=max(1, int(max_retries)),
            )
            with auth_lock:
                client.authenticate()
            local.client = client
        return client

    def run_one(idx: int, expr: str) -> Dict[str, float]:
        try:
            result = get_client().simulate_expression(
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
                }
            else:
                row = {"expression": expr, "sharpe": 0.0, "fitness": 0.0, "turnover": 0.0}
        except Exception as exc:
            logging.warning("Simulation failed (%s/%s): %s", idx, total, exc)
            row = {"expression": expr, "sharpe": 0.0, "fitness": 0.0, "turnover": 0.0}
        row["index"] = idx
        return row

    max_workers = max(1, min(int(concurrency), total))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(run_one, idx, expr)
            for idx, expr in enumerate(expressions, start=1)
        ]
        for fut in as_completed(futures):
            results.append(fut.result())

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
) -> int:
    if not library_path:
        return 0
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
        return 0

    merged = existing + added
    src.parent.mkdir(parents=True, exist_ok=True)
    src.write_text(json.dumps(merged, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(added)


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
    timeout_sec: int = 60,
    max_retries: int = 5,
    poll_interval_sec: int = 30,
    max_wait_sec: int = 600,
    max_rounds: int = 0,
    sleep_between_rounds: int = 5,
    evolve_rounds: int = 0,
    evolve_count: int = 0,
    evolve_top_k: int = 6,
    seed_templates: str = "",
    library_output: str = "",
    library_sharpe_min: float = 1.2,
    library_fitness_min: float = 1.0,
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)
    neutralization = get_default_neutralization(region)

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)

    client = WorldQuantBrainClient(
        username=user,
        password=pwd,
        timeout_sec=max(5, int(timeout_sec)),
        max_retries=max(1, int(max_retries)),
    )
    client.authenticate()

    fields_cache = default_fields_cache_path(region, universe, delay)
    if Path(fields_cache).exists():
        fields = load_data_fields_cache(fields_cache)
        logging.info("Using cached fields: %s (count=%d)", fields_cache, len(fields))
    else:
        fields = client.fetch_data_fields(region=region, universe=universe, delay=delay)
        if not fields:
            fields = client.load_fallback_default_fields()
        save_data_fields_cache(fields_cache, fields)

    seed_exprs = _load_seed_expressions(seed_templates)
    if not inspiration:
        inspiration = generate_inspiration_text(
            llm_config_path=llm_config_path,
            region=region,
            universe=universe,
            delay=delay,
            style_seed=style_prompt,
            seed_expressions=seed_exprs,
        )

    operators = load_operators()
    llm = OpenAICompatibleLLM(load_llm_config(llm_config_path))
    generator = TemplateGenerator(llm=llm, operators=operators)

    base_style = merge_style_prompt(style_prompt, inspiration)
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

    try:
        while True:
            round_idx += 1
            per_round = evolve_count if evolve_count and int(evolve_count) > 0 else template_count
            style = base_style if not reflection else merge_style_prompt(base_style, reflection)
            expressions = _generate_expressions(generator, region, fields, per_round, style)
            results = _evaluate_expressions(
                username=user,
                password=pwd,
                timeout_sec=timeout_sec,
                max_retries=max_retries,
                expressions=expressions,
                settings=settings,
                poll_interval_sec=poll_interval_sec,
                max_wait_sec=max_wait_sec,
                concurrency=concurrency,
            )
            files.append(_write_results_json(out_root / f"one_click_{ts}_round{round_idx:03}.json", results))
            appended += _append_library(library_output, results, library_sharpe_min, library_fitness_min)

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
    }
