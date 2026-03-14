"""Minimal service layer for one-click flow."""

from __future__ import annotations

import json
import logging
import time
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
    client: WorldQuantBrainClient,
    expressions: Sequence[str],
    settings: SimulationSettings,
    poll_interval_sec: int,
    max_wait_sec: int,
) -> List[Dict[str, float]]:
    results: List[Dict[str, float]] = []
    total = len(expressions)
    for idx, expr in enumerate(expressions, start=1):
        try:
            result = client.simulate_expression(
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

        results.append(row)
        logging.info(
            "Simulated %s/%s sharpe=%.3f fitness=%.3f turnover=%.2f",
            idx,
            total,
            row["sharpe"],
            row["fitness"],
            row["turnover"],
        )
    return results


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
    poll_interval_sec: int = 30,
    max_wait_sec: int = 600,
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

    client = WorldQuantBrainClient(username=user, password=pwd)
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

    expressions = _generate_expressions(generator, region, fields, template_count, base_style)
    results = _evaluate_expressions(client, expressions, settings, poll_interval_sec, max_wait_sec)
    files.append(_write_results_json(out_root / f"one_click_{ts}_gen0.json", results))
    appended = _append_library(library_output, results, library_sharpe_min, library_fitness_min)

    rounds = max(0, int(evolve_rounds))
    if rounds > 0:
        per_round = evolve_count if evolve_count and int(evolve_count) > 0 else template_count
        for round_idx in range(1, rounds + 1):
            top_rows = _select_top_rows(results, evolve_top_k)
            evolution_hint = _build_evolution_hint(top_rows)
            if not evolution_hint:
                break
            style = merge_style_prompt(base_style, evolution_hint)
            expressions = _generate_expressions(generator, region, fields, per_round, style)
            results = _evaluate_expressions(client, expressions, settings, poll_interval_sec, max_wait_sec)
            files.append(_write_results_json(out_root / f"one_click_{ts}_gen{round_idx}.json", results))
            appended += _append_library(library_output, results, library_sharpe_min, library_fitness_min)

    return {
        "files": files,
        "inspiration": inspiration,
        "final_count": len(results),
        "library_appended": appended,
    }
