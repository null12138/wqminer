#!/usr/bin/env python3
"""Run one OpenClaw-friendly optimization round for WorldQuant alphas.

This script delegates reflection/improvement to a user-provided OpenAI-compatible
LLM endpoint on every round, then submits candidates via wq_submitter_single.py.
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import glob
import json
import os
import re
import subprocess
import sys
import textwrap
import time
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple
from urllib.parse import urlparse, urlunparse

import requests


def _to_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def now_tag() -> str:
    return dt.datetime.now().strftime("%Y%m%d_%H%M%S")


def normalize_base_url(base_url: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return "https://api.openai.com/v1"
    if "://" not in raw:
        raw = "https://" + raw

    parsed = urlparse(raw)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path or ""

    if not netloc and parsed.path:
        netloc = parsed.path
        path = ""

    path = path.rstrip("/")
    if not path:
        path = "/v1"

    return urlunparse((scheme, netloc, path, "", "", "")).rstrip("/")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run one LLM-driven alpha optimization round")
    parser.add_argument("--workspace", default=".", help="Project root containing wqminer and submitter")
    parser.add_argument("--round-tag", default="", help="Optional round tag; default timestamp")

    parser.add_argument("--llm-config", default="", help="LLM config JSON path (OpenAI-compatible)")
    parser.add_argument("--disable-llm", action="store_true", help="Skip LLM generation and use deterministic neighbors only")
    parser.add_argument("--llm-candidate-count", type=int, default=36, help="Requested candidates per LLM attempt")
    parser.add_argument("--llm-attempts", type=int, default=3, help="LLM attempts per round")
    parser.add_argument(
        "--llm-temperature",
        type=float,
        default=None,
        help="Base temperature for LLM attempts (default: value from llm config/env)",
    )
    parser.add_argument(
        "--llm-max-tokens",
        type=int,
        default=0,
        help="Max tokens for LLM output (0 = use value from llm config/env)",
    )
    parser.add_argument("--llm-timeout", type=int, default=120, help="LLM HTTP timeout seconds")
    parser.add_argument("--llm-instruction", default="", help="Extra instruction appended to LLM prompt")

    parser.add_argument("--history-glob", default="results/submit_single/submit_*.csv", help="Glob for historical submit csv files")
    parser.add_argument("--output-dir", default="results/submit_single", help="Submit output directory")
    parser.add_argument("--templates-dir", default="templates", help="Directory for generated template files")
    parser.add_argument("--seed-template-file", action="append", default=[], help="Optional JSON/JSONL template source (repeatable)")
    parser.add_argument("--candidate-count", type=int, default=48, help="Final candidate count to submit")

    parser.add_argument("--target-sharpe", type=float, default=1.25)
    parser.add_argument("--target-fitness", type=float, default=1.0)
    parser.add_argument("--target-turnover-max", type=float, default=70.0)

    parser.add_argument("--username", default=os.getenv("WQ_USERNAME", ""))
    parser.add_argument("--password", default=os.getenv("WQ_PASSWORD", ""))
    parser.add_argument("--base-url", default="https://api.worldquantbrain.com")
    parser.add_argument("--region", default="USA")
    parser.add_argument("--universe", default="TOP3000")
    parser.add_argument("--delay", type=int, default=1)
    parser.add_argument("--neutralization", default="INDUSTRY")
    parser.add_argument("--max-submissions", type=int, default=48)
    parser.add_argument("--concurrency", type=int, default=2)
    parser.add_argument("--max-wait", type=int, default=300)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--submitter-script", default="wq_submitter_single.py")
    parser.add_argument("--python-bin", default=sys.executable or "python3")
    parser.add_argument("--dry-run-submit", action="store_true", help="Skip actual submit, only generate candidates/report")

    return parser.parse_args()


def resolve_path(workspace: Path, path_value: str) -> Path:
    target = Path(path_value)
    if target.is_absolute():
        return target
    return (workspace / target).resolve()


def load_llm_config(path: Path) -> Dict[str, object]:
    payload: Dict[str, object] = {}
    if path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))

    api_key = str(payload.get("api_key") or os.getenv("LLM_API_KEY", "")).strip()
    if not api_key:
        raise ValueError("Missing LLM API key in llm config or LLM_API_KEY env")

    base_url = normalize_base_url(str(payload.get("base_url") or os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")))
    model = str(payload.get("model") or os.getenv("LLM_MODEL", "gpt-4.1-mini")).strip()

    raw_temperature = payload.get("temperature", os.getenv("LLM_TEMPERATURE", ""))
    raw_max_tokens = payload.get("max_tokens", os.getenv("LLM_MAX_TOKENS", ""))

    return {
        "api_key": api_key,
        "base_url": base_url,
        "model": model,
        "temperature": _to_float(raw_temperature, 0.2),
        "max_tokens": max(400, _to_int(raw_max_tokens, 2400)),
    }


def load_operator_names(workspace: Path) -> set:
    op_file = workspace / "wqminer" / "constants" / "operatorRAW.json"
    if not op_file.exists():
        return set()
    payload = json.loads(op_file.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        return set()
    names = set()
    for item in payload:
        if isinstance(item, dict):
            name = str(item.get("name", "")).strip()
            if name:
                names.add(name.lower())
    return names


def call_llm_chat(
    llm_cfg: Dict[str, object],
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    max_tokens: int,
    timeout_sec: int,
    retries: int = 4,
) -> str:
    url = f"{llm_cfg['base_url']}/chat/completions"
    headers = {
        "Authorization": f"Bearer {llm_cfg['api_key']}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": llm_cfg["model"],
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    last_error: str = ""
    for attempt in range(1, max(1, retries) + 1):
        try:
            response = requests.post(url, headers=headers, json=payload, timeout=timeout_sec)
            if response.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                retry_after = response.headers.get("Retry-After", "")
                sleep_sec = int(retry_after) if retry_after.isdigit() else min(15, 2 ** (attempt - 1))
                time.sleep(max(1, sleep_sec))
                continue
            response.raise_for_status()
            data = response.json()
            choices = data.get("choices") or []
            if not choices:
                raise RuntimeError("LLM returned no choices")
            content = choices[0].get("message", {}).get("content", "")
            if not content:
                raise RuntimeError("LLM returned empty content")
            return str(content)
        except Exception as exc:  # noqa: BLE001
            last_error = str(exc)
            if attempt < retries:
                time.sleep(min(15, 2 ** (attempt - 1)))
                continue
            break

    raise RuntimeError(f"LLM request failed: {last_error}")


def load_history_rows(history_paths: Sequence[Path]) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for path in history_paths:
        try:
            with path.open("r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    expr = str(row.get("expression", "")).strip()
                    rows.append(
                        {
                            "source": str(path),
                            "expression": expr,
                            "alpha_id": str(row.get("alpha_id", "")).strip(),
                            "success": str(row.get("success", "")).lower() == "true",
                            "sharpe": _to_float(row.get("sharpe")),
                            "fitness": _to_float(row.get("fitness")),
                            "turnover": _to_float(row.get("turnover")),
                            "score": _to_float(row.get("score")),
                            "passed_checks": _to_int(row.get("passed_checks")),
                            "weight_check": str(row.get("weight_check", "")).strip(),
                            "error_message": str(row.get("error_message", "")).strip(),
                        }
                    )
        except Exception:  # noqa: BLE001
            continue
    return rows


def gap_to_target(row: Dict[str, object], target_sharpe: float, target_fitness: float, target_turnover: float) -> float:
    sharpe = _to_float(row.get("sharpe"))
    fitness = _to_float(row.get("fitness"))
    turnover = _to_float(row.get("turnover"))
    return max(0.0, target_sharpe - sharpe) + 1.2 * max(0.0, target_fitness - fitness) + 0.01 * max(0.0, turnover - target_turnover)


def summarize_families(rows: Sequence[Dict[str, object]]) -> List[str]:
    stats: Dict[str, Dict[str, float]] = {}

    def family(expr: str) -> str:
        if "ts_rank(ebit" in expr and "group_zscore" in expr:
            return "ebit-group-zscore"
        if "unsystematic_risk_last_30_days" in expr and "ebit" in expr:
            return "ebit-unsystematic-blend"
        if "systematic_risk_last_360_days" in expr and "ebit" in expr:
            return "ebit-systematic-blend"
        if "fscore_bfl_value" in expr and "ebit" in expr:
            return "ebit-fscore-blend"
        if any(token in expr for token in ("open", "close", "volume", "vwap", "returns")):
            return "price-volume-community"
        return "other"

    for row in rows:
        expr = str(row.get("expression", ""))
        fam = family(expr)
        if fam not in stats:
            stats[fam] = {"n": 0, "sh": 0.0, "fit": 0.0, "to": 0.0}
        stats[fam]["n"] += 1
        stats[fam]["sh"] += _to_float(row.get("sharpe"))
        stats[fam]["fit"] += _to_float(row.get("fitness"))
        stats[fam]["to"] += _to_float(row.get("turnover"))

    lines: List[str] = []
    for fam, agg in sorted(stats.items(), key=lambda item: item[1]["n"], reverse=True):
        n = max(1, int(agg["n"]))
        lines.append(
            f"- {fam}: n={n}, avg_sharpe={agg['sh']/n:.3f}, avg_fitness={agg['fit']/n:.3f}, avg_turnover={agg['to']/n:.2f}"
        )
    return lines


def build_history_digest(
    rows: Sequence[Dict[str, object]],
    target_sharpe: float,
    target_fitness: float,
    target_turnover: float,
) -> Tuple[str, List[Dict[str, object]], List[str], List[str]]:
    success_rows = [row for row in rows if row.get("success") and row.get("expression")]
    failed_rows = [row for row in rows if not row.get("success") and row.get("expression")]

    for row in success_rows:
        row["_gap"] = gap_to_target(row, target_sharpe, target_fitness, target_turnover)

    success_rows_sorted = sorted(success_rows, key=lambda row: _to_float(row.get("_gap")))
    top_close = success_rows_sorted[:20]

    top_lines: List[str] = []
    for row in top_close:
        top_lines.append(
            f"- sh={_to_float(row['sharpe']):.2f}, fit={_to_float(row['fitness']):.2f}, "
            f"to={_to_float(row['turnover']):.2f}, gap={_to_float(row['_gap']):.4f}, expr={row['expression']}"
        )

    failed_lines: List[str] = []
    for row in failed_rows[-10:]:
        failed_lines.append(f"- error={row.get('error_message','')}, expr={row.get('expression','')}")

    digest_parts = [
        f"history_success_count={len(success_rows)}",
        f"history_failure_count={len(failed_rows)}",
        "closest_to_target:",
        "\n".join(top_lines) if top_lines else "- none",
        "family_stats:",
        "\n".join(summarize_families(success_rows)) if success_rows else "- none",
        "recent_failures:",
        "\n".join(failed_lines) if failed_lines else "- none",
    ]

    tried_expressions = []
    seen = set()
    for row in success_rows_sorted[:120]:
        expr = str(row.get("expression", "")).strip()
        if expr and expr not in seen:
            seen.add(expr)
            tried_expressions.append(expr)

    failed_expressions = []
    seen_failed = set()
    for row in failed_rows[-80:]:
        expr = str(row.get("expression", "")).strip()
        if expr and expr not in seen_failed:
            seen_failed.add(expr)
            failed_expressions.append(expr)

    digest = "\n".join(digest_parts)
    return digest, top_close, tried_expressions, failed_expressions


def make_llm_prompts(
    history_digest: str,
    tried_expressions: Sequence[str],
    failed_expressions: Sequence[str],
    target_sharpe: float,
    target_fitness: float,
    target_turnover: float,
    candidate_count: int,
    extra_instruction: str,
) -> Tuple[str, str]:
    system_prompt = textwrap.dedent(
        """
        You are a FASTEXPR alpha optimization engine for WorldQuant.
        Reflect on historical results and propose new candidate expressions.
        Output only expressions, one expression per line.
        Do not output explanations, numbering, markdown, code fences, or placeholders.
        Keep syntax valid and parenthesis-balanced.
        Prefer concise expressions under 380 characters.
        """
    ).strip()

    banned_block = "\n".join(f"- {expr}" for expr in tried_expressions[:80])
    failed_block = "\n".join(f"- {expr}" for expr in failed_expressions[:40])

    user_prompt = textwrap.dedent(
        f"""
        Objective:
        - sharpe > {target_sharpe}
        - fitness > {target_fitness}
        - turnover < {target_turnover}

        Historical digest:
        {history_digest}

        Hard constraints:
        - Avoid exact duplicates from this tested list:
        {banned_block if banned_block else '- none'}
        - Avoid exact duplicates from these failed expressions:
        {failed_block if failed_block else '- none'}
        - Use ASCII punctuation only.
        - Prefer structures that balance Sharpe and Fitness while avoiding high turnover.

        {extra_instruction.strip() if extra_instruction.strip() else ''}

        Return exactly {candidate_count} FASTEXPR expressions, one per line.
        """
    ).strip()

    return system_prompt, user_prompt


def normalize_candidate_line(line: str) -> str:
    text = line.strip()
    if not text:
        return ""
    if text.startswith("```"):
        return ""
    if text.startswith("#"):
        return ""

    text = re.sub(r"^\s*[-*•]\s*", "", text)
    text = re.sub(r"^\s*\d+[\)\.\-:]\s*", "", text)
    text = text.strip().strip("`").strip().strip(";")

    if "=" in text and "(" in text and text.index("=") < text.index("("):
        left, right = text.split("=", 1)
        if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", left.strip()):
            text = right.strip()

    text = text.replace("Ts_Rank", "ts_rank")
    text = text.replace("Ts_", "ts_")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def is_balanced_parentheses(text: str) -> bool:
    depth = 0
    for ch in text:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
            if depth < 0:
                return False
    return depth == 0


def split_llm_chunks(raw_text: str) -> List[str]:
    chunks: List[str] = []
    buffer: List[str] = []
    depth = 0

    for raw_line in raw_text.splitlines():
        line = normalize_candidate_line(raw_line)
        if not line:
            continue

        buffer.append(line)
        depth += line.count("(") - line.count(")")

        if depth <= 0:
            chunk = normalize_candidate_line(" ".join(buffer))
            if chunk:
                chunks.append(chunk)
            buffer = []
            depth = 0

    if buffer:
        chunk = normalize_candidate_line(" ".join(buffer))
        if chunk:
            chunks.append(chunk)

    final_chunks: List[str] = []
    for chunk in chunks:
        if ";" in chunk:
            pieces = [normalize_candidate_line(x) for x in chunk.split(";")]
            for piece in pieces:
                if piece:
                    final_chunks.append(piece)
        else:
            final_chunks.append(chunk)

    return final_chunks


def list_unknown_calls(expression: str, operator_names_lower: set) -> List[str]:
    calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)
    unknown: List[str] = []
    for call in calls:
        call_l = call.lower()
        if call_l in operator_names_lower:
            continue
        if call_l not in unknown:
            unknown.append(call_l)
    return unknown


def is_valid_expression(expression: str, operator_names_lower: set) -> Tuple[bool, str]:
    expr = normalize_candidate_line(expression)
    if not expr:
        return False, "empty"
    if len(expr) < 8:
        return False, "too_short"
    if len(expr) > 700:
        return False, "too_long"
    if "http://" in expr or "https://" in expr:
        return False, "has_url"
    if "{" in expr or "}" in expr:
        return False, "has_placeholder"
    if "(" not in expr or ")" not in expr:
        return False, "no_call"
    if not is_balanced_parentheses(expr):
        return False, "unbalanced_parentheses"
    if any(ch in expr for ch in ("，", "；", "：", "（", "）")):
        return False, "non_ascii_punctuation"
    try:
        expr.encode("ascii")
    except UnicodeEncodeError:
        return False, "non_ascii"

    if operator_names_lower:
        unknown = list_unknown_calls(expr, operator_names_lower)
        if unknown:
            return False, f"unknown_calls:{','.join(unknown[:6])}"

    return True, "ok"


def dedupe_keep_order(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        text = item.strip()
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def load_seed_templates(path: Path) -> List[str]:
    if not path.exists():
        return []
    if path.suffix.lower() == ".jsonl":
        out: List[str] = []
        for line in path.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:  # noqa: BLE001
                continue
            if isinstance(obj, dict):
                expr = str(obj.get("expression", "")).strip()
                if expr:
                    out.append(expr)
        return dedupe_keep_order(out)

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return []

    raw = payload.get("templates", payload)
    out: List[str] = []
    if isinstance(raw, list):
        for item in raw:
            if isinstance(item, dict):
                expr = str(item.get("expression", "")).strip()
            else:
                expr = str(item).strip()
            if expr:
                out.append(expr)
    return dedupe_keep_order(out)


def generate_neighbors(top_rows: Sequence[Dict[str, object]]) -> List[str]:
    out: List[str] = []

    def add(expr: str) -> None:
        text = normalize_candidate_line(expr)
        if text and text not in out:
            out.append(text)

    fields = []
    for row in top_rows:
        expr = str(row.get("expression", ""))
        for field, win in re.findall(r"ts_rank\(([^,]+),\s*(\d+)\)", expr):
            field_clean = field.strip()
            win_num = _to_int(win)
            if win_num > 0 and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field_clean):
                fields.append((field_clean, win_num))

    if not fields:
        fields = [("ebit", 18), ("unsystematic_risk_last_30_days", 30), ("fscore_bfl_value", 90)]

    for field, win in fields[:30]:
        windows = sorted({max(2, win - 2), max(2, win - 1), win, win + 1, win + 2, win + 4})
        for w in windows:
            base = f"divide(ts_rank({field}, {w}), add(1, ts_count_nans({field}, {w})))"
            for group in ["sector", "industry", 'bucket(rank(cap), range="0,1,0.1")']:
                add(f"group_zscore({base}, {group})")
                add(f"group_zscore(ts_mean({base}, 3), {group})")
                add(f"group_zscore(ts_mean({base}, 5), {group})")
                add(f"group_zscore(ts_decay_linear({base}, 5), {group})")

            if field == "ebit":
                for rw in (20, 30, 45, 60):
                    risk = f"divide(ts_rank(unsystematic_risk_last_30_days, {rw}), add(1, ts_count_nans(unsystematic_risk_last_30_days, {rw})))"
                    add(f"group_zscore(add({base}, divide({risk}, 4)), sector)")
                    add(f"group_zscore(add(ts_mean({base}, 3), divide({risk}, 4)), sector)")
                    add(f"group_zscore(add({base}, divide({risk}, 4)), industry)")

                fv = "ts_rank(zscore(fscore_bfl_value), 90)"
                add(f"group_neutralize(rank(add(ts_mean({base}, 3), divide({fv}, 2))), sector)")
                add(f"group_neutralize(rank(add(ts_mean({base}, 3), divide({fv}, 2))), industry)")
                add(f"group_neutralize(rank(add(ts_mean({base}, 3), divide({fv}, 2))), market)")

    # Small stable fallback library from best-performing structures observed in practice.
    for w in [14, 16, 17, 18, 19, 20]:
        base = f"divide(ts_rank(ebit, {w}), add(1, ts_count_nans(ebit, {w})))"
        add(f"group_zscore({base}, sector)")
        add(f"group_zscore(ts_mean({base}, 3), sector)")
        add(f"group_zscore(ts_mean({base}, 5), sector)")
        add(f"group_zscore({base}, industry)")

    return out


def select_candidates(
    llm_candidates: Sequence[str],
    neighbor_candidates: Sequence[str],
    seed_candidates: Sequence[str],
    tried_expressions: set,
    operator_names_lower: set,
    desired_count: int,
) -> Tuple[List[str], Dict[str, int], Dict[str, str]]:
    combined = []
    combined.extend(llm_candidates)
    combined.extend(neighbor_candidates)
    combined.extend(seed_candidates)

    deduped = dedupe_keep_order(combined)

    final: List[str] = []
    reject_stats: Dict[str, int] = {}
    sample_rejects: Dict[str, str] = {}

    for expr in deduped:
        if expr in tried_expressions:
            reject_stats["duplicate_tested"] = reject_stats.get("duplicate_tested", 0) + 1
            continue

        ok, reason = is_valid_expression(expr, operator_names_lower)
        if not ok:
            reject_stats[reason] = reject_stats.get(reason, 0) + 1
            if reason not in sample_rejects:
                sample_rejects[reason] = expr
            continue

        final.append(expr)
        if len(final) >= desired_count:
            break

    return final, reject_stats, sample_rejects


def write_templates(path: Path, expressions: Sequence[str], meta: Dict[str, object]) -> None:
    payload = {"templates": list(expressions), "meta": meta}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def run_submitter(args: argparse.Namespace, workspace: Path, templates_file: Path, output_dir: Path, round_tag: str) -> Tuple[int, Path, Path, Path, Path]:
    submitter_script = resolve_path(workspace, args.submitter_script)
    if not submitter_script.exists():
        raise FileNotFoundError(f"submitter script not found: {submitter_script}")

    output_dir.mkdir(parents=True, exist_ok=True)
    log_path = output_dir / f"openclaw_round_{round_tag}.log"

    cmd = [
        args.python_bin,
        str(submitter_script),
        "--username",
        args.username,
        "--password",
        args.password,
        "--templates-file",
        str(templates_file),
        "--base-url",
        args.base_url,
        "--region",
        args.region,
        "--universe",
        args.universe,
        "--delay",
        str(args.delay),
        "--neutralization",
        args.neutralization,
        "--max-submissions",
        str(args.max_submissions),
        "--concurrency",
        str(args.concurrency),
        "--max-wait",
        str(args.max_wait),
        "--poll-interval",
        str(args.poll_interval),
        "--timeout",
        str(args.timeout),
        "--output-dir",
        str(output_dir),
    ]

    masked_cmd = []
    for idx, token in enumerate(cmd):
        if idx > 0 and cmd[idx - 1] == "--password":
            masked_cmd.append("******")
        else:
            masked_cmd.append(token)

    print("[submitter] running command:")
    print(" ".join(masked_cmd))

    started = time.time()
    found_json = Path("")
    found_csv = Path("")
    found_jsonl = Path("")

    with log_path.open("w", encoding="utf-8") as log_handle:
        process = subprocess.Popen(  # noqa: S603
            cmd,
            cwd=str(workspace),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )

        assert process.stdout is not None
        for line in process.stdout:
            sys.stdout.write(line)
            log_handle.write(line)
            stripped = line.strip()
            if stripped.endswith(".json") and "submit_" in Path(stripped).name:
                found_json = (workspace / stripped).resolve() if not Path(stripped).is_absolute() else Path(stripped)
            elif stripped.endswith(".csv") and "submit_" in Path(stripped).name:
                found_csv = (workspace / stripped).resolve() if not Path(stripped).is_absolute() else Path(stripped)
            elif stripped.endswith(".jsonl") and "submit_" in Path(stripped).name:
                found_jsonl = (workspace / stripped).resolve() if not Path(stripped).is_absolute() else Path(stripped)

        process.wait()
        code = int(process.returncode or 0)

    if not found_csv.exists() or not found_json.exists():
        latest_csv, latest_json, latest_jsonl = find_latest_submit_files(output_dir, started)
        if latest_csv:
            found_csv = latest_csv
        if latest_json:
            found_json = latest_json
        if latest_jsonl:
            found_jsonl = latest_jsonl

    return code, log_path, found_json, found_csv, found_jsonl


def find_latest_submit_files(output_dir: Path, started_ts: float) -> Tuple[Path, Path, Path]:
    csv_files = [Path(p) for p in glob.glob(str(output_dir / "submit_*.csv"))]
    json_files = [Path(p) for p in glob.glob(str(output_dir / "submit_*.json"))]
    jsonl_files = [Path(p) for p in glob.glob(str(output_dir / "submit_*.jsonl"))]

    def pick(files: Sequence[Path]) -> Path:
        recent = [p for p in files if p.exists() and p.stat().st_mtime >= started_ts - 2]
        if not recent:
            return Path("")
        return sorted(recent, key=lambda p: p.stat().st_mtime)[-1]

    return pick(csv_files), pick(json_files), pick(jsonl_files)


def analyze_submit(csv_path: Path, target_sharpe: float, target_fitness: float, target_turnover: float) -> Dict[str, object]:
    if not csv_path.exists():
        return {
            "csv_path": str(csv_path),
            "error": "csv_not_found",
            "success_count": 0,
            "failure_count": 0,
            "bar_hit_count": 0,
            "bar_hits": [],
            "top_near_bar": [],
        }

    rows = []
    with csv_path.open("r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            parsed = {
                "expression": str(row.get("expression", "")).strip(),
                "alpha_id": str(row.get("alpha_id", "")).strip(),
                "success": str(row.get("success", "")).lower() == "true",
                "sharpe": _to_float(row.get("sharpe")),
                "fitness": _to_float(row.get("fitness")),
                "turnover": _to_float(row.get("turnover")),
                "score": _to_float(row.get("score")),
                "passed_checks": _to_int(row.get("passed_checks")),
                "weight_check": str(row.get("weight_check", "")).strip(),
                "error_message": str(row.get("error_message", "")).strip(),
            }
            parsed["gap"] = gap_to_target(parsed, target_sharpe, target_fitness, target_turnover)
            rows.append(parsed)

    success_rows = [row for row in rows if row["success"]]
    fail_rows = [row for row in rows if not row["success"]]

    bar_hits = [
        row
        for row in success_rows
        if row["sharpe"] > target_sharpe and row["fitness"] > target_fitness and row["turnover"] < target_turnover
    ]

    top_near = sorted(success_rows, key=lambda row: row["gap"])[:12]

    return {
        "csv_path": str(csv_path),
        "success_count": len(success_rows),
        "failure_count": len(fail_rows),
        "bar_hit_count": len(bar_hits),
        "bar_hits": bar_hits,
        "top_near_bar": top_near,
        "max_sharpe": max((row["sharpe"] for row in success_rows), default=0.0),
        "max_fitness": max((row["fitness"] for row in success_rows), default=0.0),
        "avg_sharpe": (sum(row["sharpe"] for row in success_rows) / len(success_rows)) if success_rows else 0.0,
        "avg_fitness": (sum(row["fitness"] for row in success_rows) / len(success_rows)) if success_rows else 0.0,
        "avg_turnover": (sum(row["turnover"] for row in success_rows) / len(success_rows)) if success_rows else 0.0,
    }


def main() -> int:
    args = parse_args()
    workspace = Path(args.workspace).resolve()
    round_tag = args.round_tag.strip() or now_tag()

    output_dir = resolve_path(workspace, args.output_dir)
    templates_dir = resolve_path(workspace, args.templates_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    templates_dir.mkdir(parents=True, exist_ok=True)

    history_pattern = str(resolve_path(workspace, args.history_glob))
    history_paths = [Path(p) for p in sorted(glob.glob(history_pattern))]
    history_rows = load_history_rows(history_paths)

    history_digest, top_close, tried_exprs, failed_exprs = build_history_digest(
        history_rows,
        args.target_sharpe,
        args.target_fitness,
        args.target_turnover_max,
    )
    tried_set = set(expr.strip() for expr in tried_exprs if expr.strip())

    operator_names = load_operator_names(workspace)

    llm_prompt_path = output_dir / f"openclaw_round_{round_tag}_llm_prompt.txt"
    llm_raw_path = output_dir / f"openclaw_round_{round_tag}_llm_response.txt"

    llm_candidates: List[str] = []
    llm_errors: List[str] = []

    if not args.disable_llm:
        if not args.llm_config:
            raise ValueError("--llm-config is required unless --disable-llm is set")

        llm_cfg = load_llm_config(resolve_path(workspace, args.llm_config))
        configured_tokens = _to_int(llm_cfg.get("max_tokens"), 2400)
        effective_tokens = max(400, int(args.llm_max_tokens) if args.llm_max_tokens > 0 else configured_tokens)
        base_temperature = (
            float(args.llm_temperature)
            if args.llm_temperature is not None
            else _to_float(llm_cfg.get("temperature"), 0.2)
        )

        system_prompt, user_prompt = make_llm_prompts(
            history_digest=history_digest,
            tried_expressions=tried_exprs,
            failed_expressions=failed_exprs,
            target_sharpe=args.target_sharpe,
            target_fitness=args.target_fitness,
            target_turnover=args.target_turnover_max,
            candidate_count=max(8, args.llm_candidate_count),
            extra_instruction=args.llm_instruction,
        )
        llm_prompt_path.write_text(
            f"[system]\n{system_prompt}\n\n[user]\n{user_prompt}\n",
            encoding="utf-8",
        )

        raw_blocks: List[str] = []
        collected: List[str] = []

        for attempt in range(1, max(1, args.llm_attempts) + 1):
            try:
                raw = call_llm_chat(
                    llm_cfg=llm_cfg,
                    system_prompt=system_prompt,
                    user_prompt=user_prompt,
                    temperature=min(0.6, max(0.0, base_temperature + 0.05 * (attempt - 1))),
                    max_tokens=effective_tokens,
                    timeout_sec=max(20, args.llm_timeout),
                    retries=4,
                )
                raw_blocks.append(f"### attempt {attempt}\n{raw}\n")
                chunks = split_llm_chunks(raw)
                for chunk in chunks:
                    if chunk not in collected:
                        collected.append(chunk)
            except Exception as exc:  # noqa: BLE001
                llm_errors.append(f"attempt_{attempt}:{exc}")

        llm_raw_path.write_text("\n\n".join(raw_blocks), encoding="utf-8")

        for expr in dedupe_keep_order(collected):
            ok, reason = is_valid_expression(expr, operator_names)
            if ok:
                llm_candidates.append(expr)
            else:
                llm_errors.append(f"invalid:{reason}:{expr[:160]}")

    neighbors = generate_neighbors(top_close)

    seed_exprs: List[str] = []
    for path_value in args.seed_template_file:
        seed_path = resolve_path(workspace, path_value)
        seed_exprs.extend(load_seed_templates(seed_path))

    desired_count = max(1, args.candidate_count)
    final_candidates, reject_stats, reject_samples = select_candidates(
        llm_candidates=llm_candidates,
        neighbor_candidates=neighbors,
        seed_candidates=seed_exprs,
        tried_expressions=tried_set,
        operator_names_lower=operator_names,
        desired_count=desired_count,
    )

    min_required = min(8, desired_count)
    if len(final_candidates) < min_required:
        raise RuntimeError(
            "Only "
            f"{len(final_candidates)} valid new candidates produced (required >= {min_required}, "
            f"target={desired_count}). Increase llm attempts/candidate count or provide extra seed templates."
        )

    templates_file = templates_dir / f"openclaw_candidates_{round_tag}.json"
    write_templates(
        templates_file,
        final_candidates,
        meta={
            "round_tag": round_tag,
            "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "history_files": [str(path) for path in history_paths],
            "llm_candidate_count": len(llm_candidates),
            "neighbor_candidate_count": len(neighbors),
            "seed_candidate_count": len(seed_exprs),
            "final_candidate_count": len(final_candidates),
            "target_sharpe": args.target_sharpe,
            "target_fitness": args.target_fitness,
            "target_turnover_max": args.target_turnover_max,
        },
    )

    report: Dict[str, object] = {
        "round_tag": round_tag,
        "generated_at": dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "workspace": str(workspace),
        "targets": {
            "sharpe_gt": args.target_sharpe,
            "fitness_gt": args.target_fitness,
            "turnover_lt": args.target_turnover_max,
        },
        "history_file_count": len(history_paths),
        "history_row_count": len(history_rows),
        "templates_file": str(templates_file),
        "llm_prompt_file": str(llm_prompt_path),
        "llm_response_file": str(llm_raw_path),
        "generation": {
            "llm_candidates": len(llm_candidates),
            "neighbor_candidates": len(neighbors),
            "seed_candidates": len(seed_exprs),
            "final_candidates": len(final_candidates),
            "llm_errors": llm_errors,
            "reject_stats": reject_stats,
            "reject_samples": reject_samples,
        },
        "submit": {},
        "bar": {},
    }

    if args.dry_run_submit:
        report["submit"] = {"mode": "dry_run_submit", "output_dir": str(output_dir)}
        report_path = output_dir / f"openclaw_round_{round_tag}_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[dry-run] templates={templates_file}")
        print(f"REPORT_PATH={report_path}")
        return 0

    if not args.username or not args.password:
        raise ValueError("Need --username and --password for submit step (or WQ_USERNAME/WQ_PASSWORD env)")

    submit_code, submit_log, submit_json, submit_csv, submit_jsonl = run_submitter(
        args=args,
        workspace=workspace,
        templates_file=templates_file,
        output_dir=output_dir,
        round_tag=round_tag,
    )

    report["submit"] = {
        "exit_code": submit_code,
        "log": str(submit_log),
        "json": str(submit_json) if submit_json else "",
        "csv": str(submit_csv) if submit_csv else "",
        "jsonl": str(submit_jsonl) if submit_jsonl else "",
    }

    if submit_code != 0:
        report_path = output_dir / f"openclaw_round_{round_tag}_report.json"
        report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[error] submitter exit_code={submit_code}")
        print(f"REPORT_PATH={report_path}")
        return submit_code

    analysis = analyze_submit(submit_csv, args.target_sharpe, args.target_fitness, args.target_turnover_max)
    report["bar"] = analysis

    report_path = output_dir / f"openclaw_round_{round_tag}_report.json"
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    bar_hit_count = _to_int(analysis.get("bar_hit_count"))
    print(
        f"[round] success={analysis.get('success_count', 0)} fail={analysis.get('failure_count', 0)} "
        f"bar_hits={bar_hit_count} max_sharpe={_to_float(analysis.get('max_sharpe')):.2f} "
        f"max_fitness={_to_float(analysis.get('max_fitness')):.2f}"
    )

    if bar_hit_count > 0:
        print("[round] bar hit expressions:")
        for row in analysis.get("bar_hits", [])[:10]:
            print(
                f"- alpha={row.get('alpha_id','')} sh={_to_float(row.get('sharpe')):.2f} "
                f"fit={_to_float(row.get('fitness')):.2f} to={_to_float(row.get('turnover')):.2f}"
            )

    print(f"REPORT_PATH={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
