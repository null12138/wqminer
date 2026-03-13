"""Minimal low-resource WebUI for WQMiner."""

from __future__ import annotations

import json
import os
import random
import threading
import time
import uuid
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from .. import services
from ..inspiration import list_inspirations, save_inspiration
from ..mutator import ExpressionMutator
from ..operator_store import load_operators
from ..storage import load_templates
from ..models import SimulationResult, SimulationSettings
from ..worldquant_client import WorldQuantBrainClient

os.environ.setdefault("WQMINER_METADATA_MIN_INTERVAL", "0.4")
os.environ.setdefault("WQMINER_METADATA_JITTER", "0.2")

BASE_DIR = Path(__file__).resolve().parents[2]
STATIC_DIR = Path(__file__).resolve().parent / "static"


class JobRunner:
    def __init__(self, max_workers: int = 1, max_jobs: int = 200):
        self._executor = ThreadPoolExecutor(max_workers=max(1, int(max_workers)))
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()
        self._max_jobs = max(50, int(max_jobs))
        self._cancelled: set[str] = set()

    def submit(self, job_type: str, params: Dict[str, Any]) -> str:
        job_id = uuid.uuid4().hex[:12]
        now = time.time()
        safe_params = _redact_params(params)
        job = {
            "id": job_id,
            "type": job_type,
            "status": "queued",
            "created_at": _format_ts(now),
            "started_at": "",
            "finished_at": "",
            "params": safe_params,
            "result": None,
            "error": "",
            "cancel_requested": False,
        }
        with self._lock:
            self._jobs[job_id] = job
            self._order.append(job_id)
            if len(self._order) > self._max_jobs:
                old = self._order.pop(0)
                self._jobs.pop(old, None)

        self._executor.submit(self._run_job, job_id, job_type, params)
        return job_id

    def _run_job(self, job_id: str, job_type: str, params: Dict[str, Any]) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            if job.get("cancel_requested"):
                job["status"] = "cancelled"
                job["finished_at"] = _format_ts(time.time())
                self._cancelled.add(job_id)
                return
            job["status"] = "running"
            job["started_at"] = _format_ts(time.time())

        try:
            result = dispatch_job(job_type, params, job_id=job_id, runner=self)
            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                if job["status"] != "cancelled":
                    job["status"] = "done"
                    job["finished_at"] = _format_ts(time.time())
                job["result"] = result
        except Exception as exc:
            with self._lock:
                job = self._jobs.get(job_id)
                if not job:
                    return
                job["status"] = "error"
                job["finished_at"] = _format_ts(time.time())
                job["error"] = str(exc)

    def get(self, job_id: str) -> Dict[str, Any]:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(job_id)
            return dict(job)

    def list(self) -> List[Dict[str, Any]]:
        with self._lock:
            items = [self._jobs[job_id] for job_id in self._order if job_id in self._jobs]
        return list(reversed(items))

    def cancel(self, job_id: str) -> bool:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                raise KeyError(job_id)
            if job["status"] in {"done", "error", "cancelled"}:
                return False
            job["cancel_requested"] = True
            if job["status"] == "queued":
                job["status"] = "cancelled"
                job["finished_at"] = _format_ts(time.time())
                self._cancelled.add(job_id)
            return True

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            if job_id in self._cancelled:
                return True
            job = self._jobs.get(job_id)
            if not job:
                return False
            return bool(job.get("cancel_requested"))

    def update_job(self, job_id: str, **fields: Any) -> None:
        with self._lock:
            job = self._jobs.get(job_id)
            if not job:
                return
            for key, val in fields.items():
                job[key] = val


app = FastAPI(title="WQMiner WebUI", docs_url=None, redoc_url=None)
app.mount("/ui", StaticFiles(directory=str(STATIC_DIR), html=True), name="ui")

_WORKERS = int(os.getenv("WQMINER_WEBUI_WORKERS", "1"))
_jobs = JobRunner(max_workers=_WORKERS)
_LOG_ROOTS = {
    "auto_runs": Path("results/auto_runs").resolve(),
    "results": Path("results").resolve(),
}


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/ui/index.html")


@app.get("/api/health")
def health() -> Dict[str, Any]:
    return {"ok": True, "workers": _WORKERS, "time": _format_ts(time.time())}


@app.get("/api/jobs")
def list_jobs() -> Dict[str, Any]:
    return {"jobs": _jobs.list()}


@app.get("/api/jobs/{job_id}")
def get_job(job_id: str) -> Dict[str, Any]:
    try:
        return _jobs.get(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")


@app.post("/api/jobs/{job_id}/cancel")
def cancel_job(job_id: str) -> Dict[str, Any]:
    try:
        ok = _jobs.cancel(job_id)
    except KeyError:
        raise HTTPException(status_code=404, detail="job not found")
    job = _jobs.get(job_id)
    return {"ok": ok, "status": job.get("status", ""), "job_id": job_id}


@app.post("/api/jobs")
async def create_job(request: Request) -> JSONResponse:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    job_type = str(payload.get("type", "")).strip()
    params = payload.get("params", {}) or {}
    if not job_type:
        raise HTTPException(status_code=400, detail="missing job type")
    if not isinstance(params, dict):
        raise HTTPException(status_code=400, detail="params must be object")

    job_id = _jobs.submit(job_type, params)
    return JSONResponse({"job_id": job_id})


@app.get("/api/inspirations")
def api_list_inspirations() -> Dict[str, Any]:
    return {"items": list_inspirations()}


@app.post("/api/inspirations")
async def api_save_inspiration(request: Request) -> Dict[str, Any]:
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="invalid payload")
    text = str(payload.get("text", "")).strip()
    if not text:
        raise HTTPException(status_code=400, detail="text is required")
    entry = save_inspiration(text)
    return {"item": entry}


@app.get("/api/logs")
def api_list_logs(scope: str = "auto_runs") -> Dict[str, Any]:
    roots = _select_log_roots(scope)
    items: List[Dict[str, Any]] = []
    for name, root in roots.items():
        if not root.exists():
            continue
        for path in sorted(root.glob("**/*")):
            if not path.is_file():
                continue
            if path.suffix.lower() not in {".jsonl", ".json", ".csv", ".txt"}:
                continue
            rel = str(path.relative_to(root))
            items.append(
                {
                    "path": f"{name}/{rel}",
                    "size": path.stat().st_size,
                    "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(path.stat().st_mtime)),
                }
            )
    items.sort(key=lambda x: x.get("mtime", ""), reverse=True)
    return {"logs": items}


@app.get("/api/logs/content")
def api_log_content(path: str, tail: int = 200) -> Dict[str, Any]:
    if not path:
        raise HTTPException(status_code=400, detail="path required")
    resolved = _resolve_log_path(path)
    if not resolved:
        raise HTTPException(status_code=404, detail="log not found")
    content, truncated = _read_log_content(resolved, tail=max(1, int(tail)))
    return {
        "path": path,
        "size": resolved.stat().st_size,
        "mtime": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(resolved.stat().st_mtime)),
        "content": content,
        "truncated": truncated,
    }


def dispatch_job(job_type: str, params: Dict[str, Any], job_id: str = "", runner: JobRunner | None = None) -> Dict[str, Any]:
    job_type = job_type.strip().lower()
    if job_type == "fetch_fields":
        return _job_fetch_fields(params)
    if job_type == "generate_templates":
        return _job_generate_templates(params)
    if job_type == "generate_templates_iter":
        return _job_generate_templates_iter(params)
    if job_type == "auto_loop":
        return _job_auto_loop(params, job_id=job_id, runner=runner)
    if job_type == "optimize_loop":
        return _job_optimize_loop(params)
    if job_type == "submit_concurrent":
        return _job_submit_concurrent(params)
    raise ValueError(f"Unsupported job type: {job_type}")


def _job_fetch_fields(params: Dict[str, Any]) -> Dict[str, Any]:
    region = _str(params.get("region", "USA"))
    universe = _str(params.get("universe", ""))
    delay = _int(params.get("delay", 1))

    return services.fetch_fields_and_cache(
        region=region,
        universe=universe,
        delay=delay,
        credentials_path=_path(params.get("credentials", ""), allow_outside=True),
        username=_str(params.get("username", "")),
        password=_str(params.get("password", "")),
        full=_bool(params.get("full", False)),
        max_datasets=_int(params.get("max_datasets", 10)),
        field_max_pages=_int(params.get("field_max_pages", 5)),
        dataset_max_pages=_int(params.get("dataset_max_pages", 1)),
        dataset_page_limit=_int(params.get("dataset_page_limit", 50)),
        output=_path(params.get("output", "")),
        dataset_output=_path(params.get("dataset_output", "")),
    )


def _job_generate_templates(params: Dict[str, Any]) -> Dict[str, Any]:
    inspiration = _str(params.get("inspiration", ""))
    if inspiration:
        save_inspiration(inspiration)

    return services.generate_templates(
        region=_str(params.get("region", "USA")),
        universe=_str(params.get("universe", "")),
        delay=_int(params.get("delay", 1)),
        llm_config_path=_path(params.get("llm_config", ""), allow_outside=True),
        fields_file=_path(params.get("fields_file", "")),
        credentials_path=_path(params.get("credentials", ""), allow_outside=True),
        username=_str(params.get("username", "")),
        password=_str(params.get("password", "")),
        count=_int(params.get("count", 24)),
        style_prompt=_str(params.get("style_prompt", "")),
        inspiration=inspiration,
        output=_path(params.get("output", "")) or "templates/generated_templates.json",
        operators_file=_path(params.get("operators_file", "")),
    )


def _job_generate_templates_iter(params: Dict[str, Any]) -> Dict[str, Any]:
    inspiration = _str(params.get("inspiration", ""))
    if inspiration:
        save_inspiration(inspiration)

    syntax_manual_path = _path(params.get("syntax_manual_path", ""))
    syntax_guide = _load_syntax_manual_excerpt(syntax_manual_path) if syntax_manual_path else ""

    return services.generate_templates_iter(
        region=_str(params.get("region", "USA")),
        universe=_str(params.get("universe", "")),
        delay=_int(params.get("delay", 1)),
        llm_config_path=_path(params.get("llm_config", ""), allow_outside=True),
        fields_file=_path(params.get("fields_file", "")),
        credentials_path=_path(params.get("credentials", ""), allow_outside=True),
        username=_str(params.get("username", "")),
        password=_str(params.get("password", "")),
        count=_int(params.get("count", 48)),
        rounds=_int(params.get("rounds", 3)),
        max_fix_attempts=_int(params.get("max_fix_attempts", 1)),
        style_prompt=_str(params.get("style_prompt", "")),
        inspiration=inspiration,
        syntax_guide=syntax_guide,
        output=_path(params.get("output", "")) or "templates/generated_templates_iter.json",
        report_output=_path(params.get("report_output", "")) or "results/gen_templates_iter_report.json",
        operators_file=_path(params.get("operators_file", "")),
    )


def _job_auto_loop(params: Dict[str, Any], job_id: str, runner: JobRunner | None) -> Dict[str, Any]:
    if not runner:
        raise ValueError("Runner is required for auto loop")

    region = _str(params.get("region", "USA"))
    universe = _str(params.get("universe", ""))
    delay = _int(params.get("delay", 1))
    llm_config = _path(params.get("llm_config", ""), allow_outside=True)
    fields_file = _path(params.get("fields_file", ""))
    credentials_path = _path(params.get("credentials", ""), allow_outside=True)
    username = _str(params.get("username", ""))
    password = _str(params.get("password", ""))
    style_prompt = _str(params.get("style_prompt", ""))
    syntax_manual_path = _path(params.get("syntax_manual_path", ""))
    inspiration_mode = _str(params.get("inspiration_mode", "auto")).lower()
    manual_inspiration = _str(params.get("inspiration", ""))

    templates_dir = _path(params.get("templates_dir", "templates")) or "templates"
    output_dir = _path(params.get("output_dir", "results/auto_runs")) or "results/auto_runs"
    templates_count = _int(params.get("count", 24))
    iterative = _bool(params.get("iterative", True))
    rounds = _int(params.get("rounds", 3))
    max_fix_attempts = _int(params.get("max_fix_attempts", 1))
    max_cycles = _int(params.get("max_cycles", 0))
    sleep_sec = max(5, _int(params.get("sleep_sec", 60)))

    variants_per_template = _int(params.get("variants_per_template", 8))
    max_simulations = _int(params.get("max_simulations", 0))
    max_submissions = _int(params.get("max_submissions", 0))
    if max_submissions <= 0:
        max_submissions = max_simulations
    sharpe_threshold = _float(params.get("sharpe_threshold", 0.0))
    fitness_threshold = _float(params.get("fitness_threshold", 0.0))
    neutralization = _str(params.get("neutralization", ""))
    dry_run = _bool(params.get("dry_run", False))
    cover_all_fields = _bool(params.get("cover_all_fields", True))
    coverage_limit = _int(params.get("coverage_limit", 0))
    coverage_batch_size = _int(params.get("coverage_batch_size", 200))
    next_seed_limit = _int(params.get("next_seed_limit", 200))
    concurrency = max(1, _int(params.get("concurrency", 3)))
    batch_size = _int(params.get("batch_size", concurrency))
    if batch_size <= 0:
        batch_size = concurrency
    max_wait_sec = _int(params.get("max_wait_sec", 240))
    poll_interval_sec = _int(params.get("poll_interval_sec", 5))

    syntax_guide = _load_syntax_manual_excerpt(syntax_manual_path) if syntax_manual_path else ""

    log_path = Path(output_dir) / f"auto_loop_{job_id}.jsonl"
    log_path.parent.mkdir(parents=True, exist_ok=True)

    cycle = 0
    last_summary: Dict[str, Any] = {}
    operators = load_operators()
    fields, _ = services.load_or_fetch_fields(
        region=region,
        universe=universe,
        delay=delay,
        fields_file=fields_file,
        credentials_path=credentials_path,
        username=username,
        password=password,
        required_if_missing=True,
    )
    field_ids = [f.field_id for f in fields if f.field_id]
    coverage_exprs: List[str] = []
    if cover_all_fields and field_ids:
        coverage_exprs = _build_coverage_expressions(field_ids, operators, coverage_limit=coverage_limit)
    base_templates: List[str] = []
    template_source = ""

    current_seeds: List[str] = []
    coverage_cursor = 0
    while True:
        if runner.is_cancelled(job_id):
            runner.update_job(job_id, status="cancelled", finished_at=_format_ts(time.time()))
            break

        cycle += 1
        ts = time.strftime("%Y%m%d_%H%M%S")

        inspiration = manual_inspiration
        inspiration_source = "manual"
        inspiration_error = ""
        seed_for_inspiration = []
        if base_templates:
            seed_for_inspiration = base_templates[:3]
        if inspiration_mode == "auto":
            try:
                inspiration = services.generate_inspiration_text(
                    llm_config_path=llm_config,
                    region=region,
                    universe=universe,
                    delay=delay,
                    style_seed=style_prompt,
                    seed_expressions=seed_for_inspiration,
                )
                inspiration_source = "auto"
            except Exception as exc:
                inspiration = _fallback_inspiration(style_prompt)
                inspiration_source = "fallback"
                inspiration_error = str(exc)
        elif inspiration_mode == "none":
            inspiration = ""
            inspiration_source = "none"

        if inspiration:
            try:
                save_inspiration(inspiration)
            except Exception:
                pass

        if cycle == 1:
            if iterative:
                templates_output = str(Path(templates_dir) / f"auto_templates_iter_{job_id}_{ts}.json")
                report_output = str(Path(output_dir) / f"auto_iter_report_{job_id}_{ts}.json")
                gen_result = services.generate_templates_iter(
                    region=region,
                    universe=universe,
                    delay=delay,
                    llm_config_path=llm_config,
                    fields_file=fields_file,
                    credentials_path=credentials_path,
                    username=username,
                    password=password,
                    count=templates_count,
                    rounds=rounds,
                    max_fix_attempts=max_fix_attempts,
                    style_prompt=style_prompt,
                    inspiration=inspiration,
                    syntax_guide=syntax_guide,
                    output=templates_output,
                    report_output=report_output,
                )
            else:
                templates_output = str(Path(templates_dir) / f"auto_templates_{job_id}_{ts}.json")
                gen_result = services.generate_templates(
                    region=region,
                    universe=universe,
                    delay=delay,
                    llm_config_path=llm_config,
                    fields_file=fields_file,
                    credentials_path=credentials_path,
                    username=username,
                    password=password,
                    count=templates_count,
                    style_prompt=style_prompt,
                    inspiration=inspiration,
                    output=templates_output,
                )
            template_source = gen_result.get("output", "")
            if template_source:
                base_templates = load_templates(template_source)

        coverage_batch = []
        if cover_all_fields and coverage_exprs:
            coverage_batch, coverage_cursor = _next_batch(
                coverage_exprs,
                coverage_cursor,
                coverage_batch_size,
            )

        if cycle == 1:
            evolve_candidates = base_templates
        else:
            evolve_candidates = current_seeds

        seed_exprs = _unique_list(coverage_batch + evolve_candidates)
        if max_submissions and max_submissions > 0:
            seed_exprs = seed_exprs[:max_submissions]

        if not seed_exprs:
            raise ValueError("No seed expressions available for simulation")

        max_submit = max_submissions if max_submissions > 0 else len(seed_exprs)
        settings = SimulationSettings(
            region=region,
            universe=universe,
            delay=delay,
            neutralization=neutralization,
        )
        sim_result = _simulate_in_batches(
            expressions=seed_exprs,
            username=username,
            password=password,
            settings=settings,
            batch_size=batch_size,
            concurrency=concurrency,
            max_wait_sec=max_wait_sec,
            poll_interval_sec=poll_interval_sec,
            output_dir=output_dir,
        )

        rows = sim_result.get("rows", []) or []
        selected = _select_for_evolution(rows, sharpe_threshold, fitness_threshold)
        if not selected:
            selected = evolve_candidates or seed_exprs
        current_seeds = _mutate_once(selected, field_ids, operators, variants_per_template)
        if next_seed_limit and next_seed_limit > 0:
            current_seeds = current_seeds[:next_seed_limit]

        summary = {
            "cycle": cycle,
            "time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "inspiration": inspiration,
            "inspiration_source": inspiration_source,
            "inspiration_error": inspiration_error,
            "templates_output": template_source,
            "template_count": len(base_templates),
            "coverage_count": len(coverage_exprs),
            "coverage_batch": len(coverage_batch),
            "coverage_cursor": coverage_cursor,
            "submitted_count": sim_result.get("requested_count"),
            "success_count": sim_result.get("success_count"),
            "batch_size": batch_size,
            "result_csv": sim_result.get("files", {}).get("csv"),
            "result_jsonl": sim_result.get("files", {}).get("jsonl"),
            "next_seed_count": len(current_seeds),
        }
        _append_jsonl(log_path, summary)

        last_summary = summary
        runner.update_job(
            job_id,
            result={
                "status": "running",
                "cycle": cycle,
                "last_summary": last_summary,
                "log_file": str(log_path),
            },
        )

        if max_cycles > 0 and cycle >= max_cycles:
            break

        if not _sleep_with_cancel(sleep_sec, runner, job_id):
            runner.update_job(job_id, status="cancelled", finished_at=_format_ts(time.time()))
            break

    return {
        "cycles": cycle,
        "log_file": str(log_path),
        "last_summary": last_summary,
    }


def _job_optimize_loop(params: Dict[str, Any]) -> Dict[str, Any]:
    expressions = _parse_expressions_text(_str(params.get("expressions_text", "")))

    return services.optimize_loop(
        region=_str(params.get("region", "USA")),
        universe=_str(params.get("universe", "")),
        delay=_int(params.get("delay", 1)),
        templates_file=_path(params.get("templates_file", "")),
        expressions=expressions,
        fields_file=_path(params.get("fields_file", "")),
        credentials_path=_path(params.get("credentials", ""), allow_outside=True),
        username=_str(params.get("username", "")),
        password=_str(params.get("password", "")),
        rounds=_int(params.get("rounds", 3)),
        variants_per_template=_int(params.get("variants_per_template", 8)),
        max_simulations=_int(params.get("max_simulations", 200)),
        sharpe_threshold=_float(params.get("sharpe_threshold", 1.25)),
        fitness_threshold=_float(params.get("fitness_threshold", 1.0)),
        output_dir=_path(params.get("output_dir", "results")) or "results",
        neutralization=_str(params.get("neutralization", "")),
        dry_run=_bool(params.get("dry_run", False)),
    )


def _job_submit_concurrent(params: Dict[str, Any]) -> Dict[str, Any]:
    expressions = _parse_expressions_text(_str(params.get("expressions_text", "")))

    return services.submit_concurrent(
        region=_str(params.get("region", "USA")),
        universe=_str(params.get("universe", "")),
        delay=_int(params.get("delay", 1)),
        templates_file=_path(params.get("templates_file", "")),
        expressions=expressions,
        credentials_path=_path(params.get("credentials", ""), allow_outside=True),
        username=_str(params.get("username", "")),
        password=_str(params.get("password", "")),
        max_submissions=_int(params.get("max_submissions", 60)),
        concurrency=_int(params.get("concurrency", 3)),
        max_wait_sec=_int(params.get("max_wait_sec", 240)),
        poll_interval_sec=_int(params.get("poll_interval_sec", 5)),
        output_dir=_path(params.get("output_dir", "results/submissions")) or "results/submissions",
        neutralization=_str(params.get("neutralization", "")),
    )


def _parse_expressions_text(text: str) -> List[str]:
    if not text:
        return []
    lines = [line.strip() for line in text.splitlines()]
    return [line for line in lines if line]


def _append_jsonl(path: Path, row: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _sleep_with_cancel(total_sec: int, runner: JobRunner, job_id: str) -> bool:
    remaining = max(0, int(total_sec))
    while remaining > 0:
        if runner.is_cancelled(job_id):
            return False
        step = min(5, remaining)
        time.sleep(step)
        remaining -= step
    return True


def _fallback_inspiration(style_prompt: str = "") -> str:
    seeds = [
        "Mean-reversion with liquidity shock: price pullback following abnormal volume surge.",
        "Momentum with volatility filter: follow price drift when recent volatility compresses.",
        "Cross-sectional value tilt: rank on valuation proxy vs. short-term price strength.",
        "Quality and stability: favor stable profitability with improving volume support.",
        "Event mean-reversion: fade extreme intraday moves that lack volume confirmation.",
    ]
    idx = int(time.time()) % len(seeds)
    idea = seeds[idx]
    if style_prompt:
        return f"{idea} Style seed: {style_prompt}"
    return idea


def _merge_elite(existing: List[str], new_exprs: List[str], max_keep: int) -> List[str]:
    if max_keep <= 0:
        return []
    seen = set()
    merged: List[str] = []
    for expr in new_exprs + existing:
        expr = (expr or "").strip()
        if not expr or expr in seen:
            continue
        merged.append(expr)
        seen.add(expr)
        if len(merged) >= max_keep:
            break
    return merged


def _build_coverage_expressions(
    field_ids: List[str],
    operators: List[Dict[str, Any]],
    coverage_limit: int = 0,
) -> List[str]:
    op_names = {str(op.get("name", "")).strip() for op in operators if op.get("name")}
    patterns: List[str] = []
    if "rank" in op_names and "ts_delta" in op_names:
        patterns.append("rank(ts_delta({field}, 1))")
    if "rank" in op_names and "ts_mean" in op_names:
        patterns.append("rank(ts_mean({field}, 20))")
    if "rank" in op_names and "ts_zscore" in op_names:
        patterns.append("rank(ts_zscore({field}, 20))")
    if "rank" in op_names and "ts_rank" in op_names:
        patterns.append("rank(ts_rank({field}, 20))")
    if "rank" in op_names:
        patterns.append("rank({field})")
    if "ts_delta" in op_names:
        patterns.append("ts_delta({field}, 1)")
    if not patterns:
        patterns.append("{field}")

    limit = coverage_limit if coverage_limit and coverage_limit > 0 else len(field_ids)
    out: List[str] = []
    for idx, field in enumerate(field_ids[:limit]):
        template = patterns[idx % len(patterns)]
        out.append(template.format(field=field))
    return out


def _select_for_evolution(rows: List[Dict[str, Any]], sharpe_th: float, fitness_th: float) -> List[str]:
    selected: List[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not row.get("success"):
            continue
        if sharpe_th > 0 and float(row.get("sharpe", 0.0)) < sharpe_th:
            continue
        if fitness_th > 0 and float(row.get("fitness", 0.0)) < fitness_th:
            continue
        expr = str(row.get("expression", "")).strip()
        if expr:
            selected.append(expr)
    return _unique_list(selected)


def _mutate_once(
    seeds: List[str],
    field_ids: List[str],
    operators: List[Dict[str, Any]],
    variants_per_template: int,
) -> List[str]:
    mutator = ExpressionMutator(operators=operators)
    out: List[str] = []
    for expr in seeds:
        variants = mutator.generate_variants(expr, field_ids, variants=max(1, variants_per_template))
        out.extend(variants)
    return _unique_list(out)


def _unique_list(items: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        if not item:
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _next_batch(items: List[str], cursor: int, batch_size: int) -> tuple[List[str], int]:
    if not items:
        return [], 0
    size = batch_size if batch_size and batch_size > 0 else len(items)
    if size >= len(items):
        return list(items), 0
    start = max(0, int(cursor)) % len(items)
    end = start + size
    if end <= len(items):
        batch = items[start:end]
        next_cursor = end % len(items)
        return batch, next_cursor
    batch = items[start:] + items[: end - len(items)]
    next_cursor = end % len(items)
    return batch, next_cursor


def _select_log_roots(scope: str) -> Dict[str, Path]:
    scope = (scope or "").lower()
    if scope == "all":
        return _LOG_ROOTS
    if scope in _LOG_ROOTS:
        return {scope: _LOG_ROOTS[scope]}
    return {"auto_runs": _LOG_ROOTS["auto_runs"]}


def _resolve_log_path(path: str) -> Path | None:
    if not path:
        return None
    parts = path.split("/", 1)
    if len(parts) != 2:
        return None
    scope, rel = parts
    roots = _select_log_roots(scope)
    root = roots.get(scope)
    if not root:
        return None
    candidate = (root / rel).resolve()
    if root not in candidate.parents and candidate != root:
        return None
    if not candidate.exists() or not candidate.is_file():
        return None
    if candidate.suffix.lower() not in {".jsonl", ".json", ".csv", ".txt"}:
        return None
    return candidate


def _read_log_content(path: Path, tail: int = 200) -> tuple[str, bool]:
    size = path.stat().st_size
    truncated = False
    if size <= 2 * 1024 * 1024:
        return path.read_text(encoding="utf-8", errors="ignore"), False

    dq: deque[str] = deque(maxlen=tail)
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            dq.append(line.rstrip("\n"))
    truncated = True
    return "\n".join(dq), truncated


def _simulate_in_batches(
    expressions: List[str],
    username: str,
    password: str,
    settings: SimulationSettings,
    batch_size: int,
    concurrency: int,
    max_wait_sec: int,
    poll_interval_sec: int,
    output_dir: str,
) -> Dict[str, Any]:
    exprs = _unique_list([x.strip() for x in expressions if x and x.strip()])
    if not exprs:
        raise ValueError("No expressions provided for simulation")

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_root / f"submit_{ts}.jsonl"
    out_csv = out_root / f"submit_{ts}.csv"
    out_json = out_root / f"submit_{ts}.json"

    rows: List[Dict[str, Any]] = []
    index = 1
    batch_size = max(1, int(batch_size))
    auth_lock = threading.Lock()
    local = threading.local()

    def get_client() -> WorldQuantBrainClient:
        client = getattr(local, "client", None)
        if client is None:
            client = WorldQuantBrainClient(username=username, password=password)
            with auth_lock:
                client.authenticate()
            local.client = client
        return client

    def run_one(idx: int, expr: str) -> Dict[str, Any]:
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            client = get_client()
            result = client.simulate_expression(
                expression=expr,
                settings=settings,
                poll_interval_sec=max(1, poll_interval_sec),
                max_wait_sec=max(30, max_wait_sec),
            )
            row = result.to_dict()
        except Exception as exc:
            row = SimulationResult(
                expression=expr,
                alpha_id="",
                success=False,
                error_message=f"exception: {exc}",
            ).to_dict()
        row["index"] = idx
        row["started_at"] = started
        row["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return row

    max_workers = max(1, min(int(concurrency), len(exprs)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for start in range(0, len(exprs), batch_size):
            batch = exprs[start : start + batch_size]
            futures = [executor.submit(run_one, index + i, expr) for i, expr in enumerate(batch)]
            for fut in as_completed(futures):
                row = fut.result()
                rows.append(row)
                with out_jsonl.open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
            index += len(batch)

    rows_sorted = sorted(rows, key=lambda x: int(x.get("index", 0)))
    if rows_sorted:
        from ..storage import write_csv

        write_csv(str(out_csv), rows_sorted)

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "requested_count": len(exprs),
        "success_count": sum(1 for r in rows_sorted if r.get("success")),
        "failure_count": sum(1 for r in rows_sorted if not r.get("success")),
        "concurrency": max(1, concurrency),
        "batch_size": batch_size,
        "max_wait_sec": max_wait_sec,
        "poll_interval_sec": poll_interval_sec,
        "files": {
            "jsonl": str(out_jsonl),
            "csv": str(out_csv),
            "json": str(out_json),
        },
        "rows": rows_sorted,
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


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


def _format_ts(ts: float) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int(value: Any) -> int:
    try:
        return int(str(value).strip())
    except Exception:
        return 0


def _float(value: Any) -> float:
    try:
        return float(str(value).strip())
    except Exception:
        return 0.0


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "on"}


def _path(value: Any, allow_outside: bool = False) -> str:
    raw = _str(value)
    if not raw:
        return ""
    path = Path(raw)
    if not path.is_absolute():
        path = (BASE_DIR / path).resolve()
    else:
        path = path.resolve()
    if not allow_outside:
        if BASE_DIR not in path.parents and path != BASE_DIR:
            raise ValueError("Path outside repo is not allowed")
    return str(path)


def _redact_params(params: Dict[str, Any]) -> Dict[str, Any]:
    redacted = {}
    for key, val in params.items():
        if key in {"password", "api_key"}:
            redacted[key] = "***"
        elif key in {"credentials"}:
            redacted[key] = "***"
        else:
            redacted[key] = val
    return redacted


def main() -> None:
    try:
        import uvicorn
    except Exception as exc:
        raise RuntimeError(f"uvicorn not available: {exc}")

    host = os.getenv("WQMINER_WEBUI_HOST", "127.0.0.1")
    port = int(os.getenv("WQMINER_WEBUI_PORT", "8000"))
    uvicorn.run("wqminer.webui.app:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
