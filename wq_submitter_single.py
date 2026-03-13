#!/usr/bin/env python3
"""Single-file WorldQuant alpha submitter with concurrent workers.

Usage example:
python3 wq_submitter_single.py \
  --username "you@example.com" \
  --password "your_password" \
  --templates-file templates/swappable_filled_templates_ryc.json \
  --region USA --universe TOP3000 --delay 1 \
  --neutralization INDUSTRY \
  --max-submissions 60 \
  --concurrency 3 \
  --max-wait 240 \
  --poll-interval 5 \
  --output-dir results/submit_single
"""

from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Sequence
from urllib.parse import urljoin

import requests


class WQClient:
    def __init__(self, username: str, password: str, base_url: str, timeout_sec: int = 30):
        self.username = username
        self.password = password
        self.base_url = base_url.rstrip("/")
        self.timeout_sec = timeout_sec
        self.sess = requests.Session()
        self.sess.headers.update({"Accept": "application/json", "Content-Type": "application/json"})

    def authenticate(self, max_retries: int = 5) -> None:
        auth_header = "Basic " + base64.b64encode(f"{self.username}:{self.password}".encode("utf-8")).decode("utf-8")
        self.sess.headers["Authorization"] = auth_header

        last = None
        for attempt in range(1, max_retries + 1):
            response = self.sess.post(f"{self.base_url}/authentication", timeout=self.timeout_sec)
            last = response
            if response.status_code in (200, 201):
                token = response.headers.get("X-WQB-Session-Token")
                if token:
                    self.sess.headers["X-WQB-Session-Token"] = token
                return
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                self._sleep_with_retry_after(response, attempt)
                continue
            break
        if last is None:
            raise RuntimeError("authenticate_failed: no_response")
        raise RuntimeError(f"authenticate_failed: {last.status_code} {last.text}")

    def simulate_expression(
        self,
        expression: str,
        settings: Dict,
        poll_interval_sec: int,
        max_wait_sec: int,
    ) -> Dict:
        payload = {
            "type": "REGULAR",
            "regular": expression,
            "settings": settings,
        }
        submit = self._request("POST", "/simulations", json=payload)
        if submit.status_code != 201:
            return _error_row(expression, f"submit_failed: {submit.status_code} {submit.text}")

        location = submit.headers.get("Location", "")
        if not location:
            return _error_row(expression, "submit_failed: missing_location")

        progress_url = location if location.startswith("http") else urljoin(self.base_url + "/", location.lstrip("/"))
        alpha_id = ""
        deadline = time.time() + max_wait_sec

        while time.time() < deadline:
            progress = self._request("GET", progress_url, retry_auth=True)
            if progress.status_code == 200:
                body = progress.json()
                if body.get("alpha"):
                    alpha_id = str(body["alpha"]).rstrip("/").split("/")[-1]
                    break
            time.sleep(max(1, poll_interval_sec))

        if not alpha_id:
            return _error_row(expression, "simulation_timeout")

        detail = self._request("GET", f"/alphas/{alpha_id}", retry_auth=True)
        if detail.status_code != 200:
            return _error_row(expression, f"detail_failed: {detail.status_code} {detail.text}", alpha_id=alpha_id)

        payload = detail.json()
        is_block = payload.get("is", {})
        checks = is_block.get("checks", [])
        passed = sum(1 for x in checks if x.get("result") == "PASS")
        total = len(checks)
        weight_check = ""
        sub_u_sharpe = 0.0
        for check in checks:
            if check.get("name") == "CONCENTRATED_WEIGHT":
                weight_check = str(check.get("result", ""))
            if check.get("name") == "LOW_SUB_UNIVERSE_SHARPE":
                sub_u_sharpe = _to_float(check.get("value"))

        sharpe = _to_float(is_block.get("sharpe"))
        fitness = _to_float(is_block.get("fitness"))
        turnover = 100.0 * _to_float(is_block.get("turnover"))
        row = {
            "expression": expression,
            "alpha_id": alpha_id,
            "success": True,
            "sharpe": sharpe,
            "fitness": fitness,
            "turnover": turnover,
            "returns": _to_float(is_block.get("returns")),
            "drawdown": _to_float(is_block.get("drawdown")),
            "margin": _to_float(is_block.get("margin")),
            "passed_checks": passed,
            "total_checks": total,
            "weight_check": weight_check,
            "sub_universe_sharpe": sub_u_sharpe,
            "link": f"https://platform.worldquantbrain.com/alpha/{alpha_id}",
            "error_message": "",
            "score": sharpe + 0.5 * fitness - 0.01 * turnover,
        }
        return row

    def _request(self, method: str, url_or_path: str, retry_auth: bool = True, max_retries: int = 5, **kwargs):
        url = url_or_path if url_or_path.startswith("http") else f"{self.base_url}{url_or_path}"
        last = None
        for attempt in range(1, max_retries + 1):
            response = self.sess.request(method, url, timeout=self.timeout_sec, **kwargs)
            last = response
            if response.status_code == 401 and retry_auth and attempt < max_retries:
                self.authenticate()
                continue
            if response.status_code in (429, 500, 502, 503, 504) and attempt < max_retries:
                self._sleep_with_retry_after(response, attempt)
                continue
            return response
        return last

    @staticmethod
    def _sleep_with_retry_after(response: requests.Response, attempt: int):
        retry_after = response.headers.get("Retry-After")
        if retry_after and retry_after.isdigit():
            sec = max(1, int(retry_after))
        else:
            sec = min(30, 2 ** (attempt - 1))
        time.sleep(sec)


def _error_row(expression: str, message: str, alpha_id: str = "") -> Dict:
    return {
        "expression": expression,
        "alpha_id": alpha_id,
        "success": False,
        "sharpe": 0.0,
        "fitness": 0.0,
        "turnover": 0.0,
        "returns": 0.0,
        "drawdown": 0.0,
        "margin": 0.0,
        "passed_checks": 0,
        "total_checks": 0,
        "weight_check": "",
        "sub_universe_sharpe": 0.0,
        "link": "",
        "error_message": message,
        "score": 0.0,
    }


def _to_float(value) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def load_expressions(path: str) -> List[str]:
    src = Path(path)
    if not src.exists():
        raise FileNotFoundError(f"templates_file_not_found: {path}")

    if src.suffix.lower() == ".jsonl":
        out = []
        for line in src.read_text(encoding="utf-8", errors="ignore").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception:
                continue
            expr = ""
            if isinstance(obj, dict):
                expr = str(obj.get("expression", "")).strip()
            if expr:
                out.append(expr)
        return _unique(out)

    payload = json.loads(src.read_text(encoding="utf-8"))
    raw = payload.get("templates", payload)

    out = []
    if isinstance(raw, list):
        for item in raw:
            expr = ""
            if isinstance(item, dict):
                expr = str(item.get("expression", "")).strip()
            elif isinstance(item, str):
                expr = item.strip()
            if expr:
                out.append(expr)
    return _unique(out)


def _unique(items: Sequence[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out


def parse_args():
    parser = argparse.ArgumentParser(description="Single-file concurrent WorldQuant submitter")
    parser.add_argument("--username", default=os.getenv("WQ_USERNAME", ""))
    parser.add_argument("--password", default=os.getenv("WQ_PASSWORD", ""))
    parser.add_argument("--templates-file", required=True, help="JSON/JSONL template file")
    parser.add_argument("--base-url", default="https://api.worldquantbrain.com")
    parser.add_argument("--region", default="USA")
    parser.add_argument("--universe", default="TOP3000")
    parser.add_argument("--delay", type=int, default=1)
    parser.add_argument("--neutralization", default="INDUSTRY")
    parser.add_argument("--max-submissions", type=int, default=60)
    parser.add_argument("--concurrency", type=int, default=3)
    parser.add_argument("--max-wait", type=int, default=240)
    parser.add_argument("--poll-interval", type=int, default=5)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--output-dir", default="results/submit_single")
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.username or not args.password:
        raise ValueError("Need --username and --password (or env WQ_USERNAME/WQ_PASSWORD)")

    expressions = load_expressions(args.templates_file)
    if not expressions:
        raise ValueError("No expressions found in template file")
    if args.max_submissions > 0:
        expressions = expressions[: args.max_submissions]

    settings = {
        "region": args.region.upper(),
        "universe": args.universe,
        "instrumentType": "EQUITY",
        "delay": int(args.delay),
        "decay": 0,
        "neutralization": args.neutralization,
        "truncation": 0.08,
        "pasteurization": "ON",
        "unitHandling": "VERIFY",
        "nanHandling": "OFF",
        "maxTrade": "OFF",
        "language": "FASTEXPR",
        "visualization": False,
        "testPeriod": "P5Y0M0D",
    }

    out_root = Path(args.output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_root / f"submit_{ts}.jsonl"
    out_csv = out_root / f"submit_{ts}.csv"
    out_json = out_root / f"submit_{ts}.json"

    thread_local = threading.local()

    def get_client() -> WQClient:
        client = getattr(thread_local, "client", None)
        if client is None:
            client = WQClient(
                username=args.username,
                password=args.password,
                base_url=args.base_url,
                timeout_sec=max(5, int(args.timeout)),
            )
            client.authenticate()
            thread_local.client = client
        return client

    def run_one(index_expr):
        idx, expr = index_expr
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        try:
            client = get_client()
            row = client.simulate_expression(
                expression=expr,
                settings=settings,
                poll_interval_sec=max(1, int(args.poll_interval)),
                max_wait_sec=max(30, int(args.max_wait)),
            )
        except Exception as exc:
            row = _error_row(expr, f"exception: {exc}")
        row["index"] = idx
        row["started_at"] = started
        row["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        return row

    rows = []
    with ThreadPoolExecutor(max_workers=max(1, int(args.concurrency))) as executor:
        futures = [executor.submit(run_one, pair) for pair in enumerate(expressions, start=1)]
        for fut in as_completed(futures):
            row = fut.result()
            rows.append(row)
            with out_jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
            print(
                f"[{row['index']}/{len(expressions)}] success={row['success']} alpha={row.get('alpha_id','')} "
                f"sharpe={row.get('sharpe',0)} fitness={row.get('fitness',0)}",
                flush=True,
            )

    rows = sorted(rows, key=lambda x: int(x.get("index", 0)))
    fieldnames = []
    seen = set()
    for r in rows:
        for k in r.keys():
            if k not in seen:
                seen.add(k)
                fieldnames.append(k)

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "requested_count": len(expressions),
        "success_count": sum(1 for r in rows if r.get("success")),
        "failure_count": sum(1 for r in rows if not r.get("success")),
        "concurrency": max(1, int(args.concurrency)),
        "region": args.region.upper(),
        "universe": args.universe,
        "delay": int(args.delay),
        "neutralization": args.neutralization,
        "files": {
            "json": str(out_json),
            "jsonl": str(out_jsonl),
            "csv": str(out_csv),
        },
        "rows": rows,
    }
    out_json.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(str(out_json))
    print(str(out_csv))


if __name__ == "__main__":
    main()

