"""Minimal service layer for one-click flow."""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from urllib.request import Request, urlopen

from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Tuple

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
    progress_cb: Optional[Callable[[Dict], None]] = None,
    stop_event: Optional[threading.Event] = None,
) -> Dict:
    region = region.upper()
    universe = universe or get_default_universe(region)
    neutralization = get_default_neutralization(region)

    user, pwd = resolve_credentials(credentials_path, username, password, required=True)

    fields_cache = default_fields_cache_path(region, universe, delay)
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
    negated_seen: set = set()
    reverse_log_path = Path(reverse_log) if reverse_log else None
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
            per_round = evolve_count if evolve_count and int(evolve_count) > 0 else template_count
            style = base_style if not reflection else merge_style_prompt(base_style, reflection)
            if progress_cb:
                progress_cb({"stage": "generate", "round": round_idx, "total": 0, "completed": 0, "success": 0, "failed": 0, "last": {}})
            expressions = _generate_expressions(generator, region, fields, per_round, style)
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
                    poll_interval_sec=poll_interval_sec,
                    max_wait_sec=max_wait_sec,
                    concurrency=concurrency,
                    concurrency_cap=concurrency_cap,
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
                    poll_interval_sec=poll_interval_sec,
                    max_wait_sec=max_wait_sec,
                    concurrency=concurrency,
                    concurrency_cap=concurrency_cap,
                    disable_proxy=disable_proxy,
                    progress_cb=progress_cb,
                    round_idx=round_idx,
                    stage="simulate",
                    row_cb=row_cb,
                )
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
                        poll_interval_sec=poll_interval_sec,
                        max_wait_sec=max_wait_sec,
                        concurrency=concurrency,
                        concurrency_cap=concurrency_cap,
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
                        poll_interval_sec=poll_interval_sec,
                        max_wait_sec=max_wait_sec,
                        concurrency=concurrency,
                        concurrency_cap=concurrency_cap,
                        disable_proxy=disable_proxy,
                        progress_cb=progress_cb,
                        round_idx=round_idx,
                        stage="negate",
                        row_cb=row_cb,
                    )
                results.extend(negated_results)
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
    }
