#!/usr/bin/env python3
"""Simple web UI for history + factor query (no extra deps)."""

from __future__ import annotations

import argparse
import json
import logging
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, urlparse


HTML_PAGE = """<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WQMiner 查询</title>
  <style>
    :root {
      --bg: #0f1115;
      --panel: #171a21;
      --text: #e7eaf0;
      --muted: #9aa3b2;
      --accent: #5ad2c9;
      --border: #2a2f3a;
      --shadow: 0 12px 32px rgba(0,0,0,.35);
      font-family: "SF Pro Text", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    body { margin: 0; background: radial-gradient(1200px 600px at 10% 10%, #1a2233, #0b0e14 60%); color: var(--text); }
    .wrap { max-width: 980px; margin: 0 auto; padding: 32px 20px 40px; }
    h1 { margin: 0 0 12px; font-size: 24px; letter-spacing: .5px; }
    p { margin: 0 0 18px; color: var(--muted); }
    .panel {
      background: var(--panel);
      border: 1px solid var(--border);
      border-radius: 14px;
      box-shadow: var(--shadow);
      padding: 18px;
      margin-bottom: 16px;
    }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    input, button, select {
      padding: 10px 12px;
      border-radius: 10px;
      border: 1px solid var(--border);
      background: #0e121a;
      color: var(--text);
      font-size: 14px;
    }
    button { background: var(--accent); color: #0b0e14; font-weight: 600; cursor: pointer; }
    button:hover { filter: brightness(1.05); }
    pre {
      white-space: pre-wrap;
      background: #0b0e14;
      border: 1px solid var(--border);
      padding: 12px;
      border-radius: 10px;
      min-height: 120px;
      color: #d5d9e3;
    }
    .muted { color: var(--muted); font-size: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>WQMiner 查询</h1>
    <p>查询历史因子与模板库。默认读取 results/one_click 与 templates/library.json。</p>

    <div class="panel">
      <div class="row">
        <select id="mode">
          <option value="history">最新历史</option>
          <option value="top">Sharpe 排名</option>
          <option value="find">关键词查询</option>
        </select>
        <input id="keyword" placeholder="关键词（仅 find 模式）" />
        <input id="limit" type="number" value="12" min="1" max="100" />
        <button id="run">查询</button>
      </div>
      <div class="muted">history/top 使用 limit；find 使用关键词 + limit。</div>
    </div>

    <div class="panel">
      <pre id="output">等待查询...</pre>
    </div>
  </div>

  <script>
    const output = document.getElementById("output");
    const mode = document.getElementById("mode");
    const keyword = document.getElementById("keyword");
    const limit = document.getElementById("limit");
    const runBtn = document.getElementById("run");

    async function runQuery() {
      const m = mode.value;
      const n = parseInt(limit.value || "12", 10);
      const key = keyword.value.trim();
      let url = `/api/${m}?limit=${encodeURIComponent(n)}`;
      if (m === "find") {
        if (!key) {
          output.textContent = "请输入关键词。";
          return;
        }
        url += `&q=${encodeURIComponent(key)}`;
      }
      output.textContent = "查询中...";
      try {
        const resp = await fetch(url);
        const data = await resp.json();
        output.textContent = data.text || JSON.stringify(data, null, 2);
      } catch (err) {
        output.textContent = "查询失败: " + err;
      }
    }

    runBtn.addEventListener("click", runQuery);
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WQMiner web query")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8002, help="Port to bind")
    parser.add_argument("--results-dir", default="results/one_click", help="Results directory")
    parser.add_argument("--library", default="templates/library.json", help="Template library path")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def _load_results(results_dir: str) -> List[Dict[str, float]]:
    root = Path(results_dir)
    if not root.exists():
        return []
    records: List[Dict[str, float]] = []
    files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime)
    for path in files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(payload, list):
            continue
        ts = path.stat().st_mtime
        for idx, row in enumerate(payload):
            if not isinstance(row, dict):
                continue
            expr = str(row.get("expression", "")).strip()
            if not expr:
                continue
            try:
                sharpe = float(row.get("sharpe", 0.0))
                fitness = float(row.get("fitness", 0.0))
                turnover = float(row.get("turnover", 0.0))
            except Exception:
                sharpe = 0.0
                fitness = 0.0
                turnover = 0.0
            records.append(
                {
                    "expression": expr,
                    "sharpe": sharpe,
                    "fitness": fitness,
                    "turnover": turnover,
                    "_ts": ts,
                    "_idx": idx,
                }
            )
    records.sort(key=lambda r: (r.get("_ts", 0.0), r.get("_idx", 0)))
    return records


def _load_library(path: str) -> List[str]:
    src = Path(path)
    if not src.exists():
        return []
    try:
        payload = json.loads(src.read_text(encoding="utf-8"))
    except Exception:
        return []
    expressions: List[str] = []
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict) and item.get("expression"):
                expressions.append(str(item.get("expression", "")).strip())
            elif isinstance(item, str):
                expressions.append(item.strip())
    elif isinstance(payload, dict) and isinstance(payload.get("templates"), list):
        for item in payload.get("templates", []):
            if isinstance(item, dict) and item.get("expression"):
                expressions.append(str(item.get("expression", "")).strip())
    return [x for x in expressions if x]


def _format_rows(rows: List[Dict[str, float]], limit: int) -> str:
    if not rows:
        return "none"
    lines = []
    for row in rows[: max(1, int(limit))]:
        lines.append(
            f"{row.get('expression','')} | sharpe={row.get('sharpe',0.0):.3f} "
            f"fitness={row.get('fitness',0.0):.3f} turnover={row.get('turnover',0.0):.2f}"
        )
    return "\n".join(lines)


def _format_library(rows: List[str], limit: int) -> str:
    if not rows:
        return "none"
    return "\n".join(rows[: max(1, int(limit))])


class Handler(BaseHTTPRequestHandler):
    results_dir: str = "results/one_click"
    library_path: str = "templates/library.json"

    def _send_json(self, payload: Dict, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path.startswith("/api/"):
            qs = parse_qs(parsed.query)
            limit = int(qs.get("limit", ["12"])[0] or 12)
            records = _load_results(self.results_dir)
            if parsed.path == "/api/history":
                tail = records[-limit:] if records else []
                self._send_json({"text": _format_rows(tail, limit)})
                return
            if parsed.path == "/api/top":
                ranked = sorted(records, key=lambda r: r.get("sharpe", 0.0), reverse=True)
                self._send_json({"text": _format_rows(ranked, limit)})
                return
            if parsed.path == "/api/find":
                key = (qs.get("q", [""])[0] or "").strip().lower()
                if not key:
                    self._send_json({"text": "missing keyword"}, status=400)
                    return
                matches = [r for r in records if key in r.get("expression", "").lower()]
                library = _load_library(self.library_path)
                lib_matches = [x for x in library if key in x.lower()]
                text = "\n".join(
                    [
                        f"History matches: {len(matches)}",
                        _format_rows(matches[::-1], limit),
                        f"Library matches: {len(lib_matches)}",
                        _format_library(lib_matches, limit),
                    ]
                )
                self._send_json({"text": text})
                return

        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt: str, *args) -> None:
        logging.info("%s - %s", self.address_string(), fmt % args)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    Handler.results_dir = args.results_dir
    Handler.library_path = args.library

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    logging.info("Web query listening on http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
