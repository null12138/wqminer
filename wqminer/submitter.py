"""Concurrent alpha submitter."""

from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
from typing import Dict, List, Sequence

from .models import SimulationResult, SimulationSettings
from .storage import write_csv
from .worldquant_client import WorldQuantBrainClient


def submit_expressions_concurrent(
    expressions: Sequence[str],
    username: str,
    password: str,
    settings: SimulationSettings,
    max_submissions: int,
    concurrency: int = 3,
    max_wait_sec: int = 240,
    poll_interval_sec: int = 5,
    output_dir: str = "results/submissions",
) -> Dict:
    exprs = [x.strip() for x in expressions if x and x.strip()]
    exprs = _unique(exprs)
    if max_submissions > 0:
        exprs = exprs[:max_submissions]

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_jsonl = out_root / f"submit_{ts}.jsonl"
    out_csv = out_root / f"submit_{ts}.csv"
    out_json = out_root / f"submit_{ts}.json"

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

    def run_one(index_expr):
        idx, expr = index_expr
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

    rows: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as executor:
        futures = [
            executor.submit(run_one, pair)
            for pair in enumerate(exprs, start=1)
        ]
        for fut in as_completed(futures):
            row = fut.result()
            rows.append(row)
            with out_jsonl.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")

    rows_sorted = sorted(rows, key=lambda x: int(x.get("index", 0)))
    write_csv(str(out_csv), rows_sorted)

    summary = {
        "generated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "requested_count": len(exprs),
        "success_count": sum(1 for r in rows_sorted if r.get("success")),
        "failure_count": sum(1 for r in rows_sorted if not r.get("success")),
        "concurrency": max(1, concurrency),
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


def _unique(items: Sequence[str]) -> List[str]:
    seen = set()
    out = []
    for item in items:
        if item not in seen:
            seen.add(item)
            out.append(item)
    return out
