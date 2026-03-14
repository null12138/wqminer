#!/usr/bin/env python3
"""One-page web console: query history + control local flow."""

from __future__ import annotations

import argparse
import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs, urlparse

from wqminer.config import load_run_config
from wqminer import services


HTML_PAGE = """<!doctype html>
<html lang="zh">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>WQMiner 控制台</title>
  <style>
    :root {
      --bg: #0f1115;
      --panel: #171a21;
      --text: #e7eaf0;
      --muted: #9aa3b2;
      --accent: #5ad2c9;
      --danger: #ff6b6b;
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
    button.danger { background: var(--danger); color: #0b0e14; }
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
    .status-pill {
      display: inline-block;
      padding: 4px 8px;
      border-radius: 999px;
      font-size: 12px;
      font-weight: 600;
      border: 1px solid var(--border);
      background: #0b0e14;
    }
    .status-run { color: #6be675; border-color: #1f6f3a; }
    .status-stop { color: #ff9f9f; border-color: #6f1f1f; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>WQMiner 控制台</h1>
    <p>一体化：Web 控制主流程 + 历史/模板查询。</p>

    <div class="panel">
      <div class="row">
        <button id="start">启动主流程</button>
        <button id="stop" class="danger">停止主流程</button>
        <span id="status" class="status-pill">状态: unknown</span>
      </div>
      <div class="muted" id="meta">加载中...</div>
    </div>

    <div class="panel">
      <h3 style="margin:0 0 8px; font-size:14px;">日志</h3>
      <pre id="log">等待日志...</pre>
    </div>

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
    const startBtn = document.getElementById("start");
    const stopBtn = document.getElementById("stop");
    const statusEl = document.getElementById("status");
    const metaEl = document.getElementById("meta");
    const logEl = document.getElementById("log");

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

    async function fetchStatus() {
      try {
        const resp = await fetch("/api/status");
        const data = await resp.json();
        const running = data.running;
        statusEl.textContent = running ? "状态: 运行中" : "状态: 已停止";
        statusEl.className = running ? "status-pill status-run" : "status-pill status-stop";
        metaEl.textContent = data.meta || "";
        logEl.textContent = data.log || "暂无日志";
      } catch (err) {
        statusEl.textContent = "状态: unknown";
        metaEl.textContent = "状态获取失败: " + err;
      }
    }

    startBtn.addEventListener("click", async () => {
      await fetch("/api/start", { method: "POST" });
      fetchStatus();
    });
    stopBtn.addEventListener("click", async () => {
      await fetch("/api/stop", { method: "POST" });
      fetchStatus();
    });
    runBtn.addEventListener("click", runQuery);

    fetchStatus();
    setInterval(fetchStatus, 3000);
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="WQMiner web console")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind")
    parser.add_argument("--port", type=int, default=8002, help="Port to bind")
    parser.add_argument("--config", default="run_config.json", help="Run config JSON path")
    parser.add_argument("--results-dir", default="", help="Override results dir for query")
    parser.add_argument("--library", default="", help="Override library path for query")
    parser.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARNING/ERROR")
    parser.add_argument("--log-limit", type=int, default=200, help="Log lines kept in memory")
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), format="%(asctime)s %(levelname)s %(message)s")


def _get(cfg: dict, key: str, default):
    value = cfg.get(key, default)
    return default if value is None else value


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


def _results_stats(results_dir: str) -> Dict[str, str]:
    root = Path(results_dir)
    if not root.exists():
        return {"count": "0", "latest": "none"}
    files = sorted(root.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return {"count": "0", "latest": "none"}
    return {"count": str(len(files)), "latest": files[-1].name}


class LogBufferHandler(logging.Handler):
    def __init__(self, limit: int = 200):
        super().__init__()
        self.limit = max(50, int(limit))
        self._lines: List[str] = []
        self._lock = threading.Lock()

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        with self._lock:
            self._lines.append(msg)
            if len(self._lines) > self.limit:
                self._lines = self._lines[-self.limit :]

    def get_text(self) -> str:
        with self._lock:
            return "\n".join(self._lines[-self.limit :])


class FlowController:
    def __init__(self, config_path: str, results_dir_override: str, library_override: str):
        self.config_path = config_path
        self.results_dir_override = results_dir_override
        self.library_override = library_override
        self.lock = threading.Lock()
        self.running = False
        self.thread: Optional[threading.Thread] = None
        self.stop_event: Optional[threading.Event] = None
        self.last_start = ""
        self.last_stop = ""
        self.last_error = ""
        self.last_summary: Optional[Dict] = None
        self.results_dir = results_dir_override or "results/one_click"
        self.library_path = library_override or "templates/library.json"

    def _now(self) -> str:
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def start(self) -> str:
        with self.lock:
            if self.running:
                return "already running"
            self.stop_event = threading.Event()
            self.running = True
            self.last_error = ""
            self.last_start = self._now()
            self.thread = threading.Thread(target=self._run, daemon=True)
            self.thread.start()
            return "started"

    def stop(self) -> str:
        with self.lock:
            if not self.running or not self.stop_event:
                return "not running"
            self.stop_event.set()
            return "stop requested"

    def _run(self) -> None:
        try:
            cfg = load_run_config(self.config_path)
            output_dir = self.results_dir_override or _get(cfg, "output_dir", "results/one_click")
            library_output = self.library_override or _get(cfg, "library_output", "templates/library.json")
            self.results_dir = output_dir
            self.library_path = library_output

            summary = services.run_one_click(
                region=_get(cfg, "region", "USA"),
                universe=_get(cfg, "universe", ""),
                delay=int(_get(cfg, "delay", 1)),
                llm_config_path=_get(cfg, "llm_config", "llm.json"),
                credentials_path=_get(cfg, "credentials", ""),
                username=_get(cfg, "username", ""),
                password=_get(cfg, "password", ""),
                template_count=int(_get(cfg, "template_count", 20)),
                style_prompt=_get(cfg, "style", ""),
                inspiration=_get(cfg, "inspiration", ""),
                output_dir=output_dir,
                concurrency=int(_get(cfg, "concurrency", 3)),
                timeout_sec=int(_get(cfg, "timeout_sec", 60)),
                max_retries=int(_get(cfg, "max_retries", 5)),
                poll_interval_sec=int(_get(cfg, "poll_interval", 30)),
                max_wait_sec=int(_get(cfg, "max_wait", 600)),
                max_rounds=int(_get(cfg, "max_rounds", 0)),
                sleep_between_rounds=int(_get(cfg, "sleep_between_rounds", 5)),
                evolve_rounds=int(_get(cfg, "evolve_rounds", 0)),
                evolve_count=int(_get(cfg, "evolve_count", 0)),
                evolve_top_k=int(_get(cfg, "evolve_top_k", 6)),
                seed_templates=_get(cfg, "seed_templates", ""),
                library_output=library_output,
                library_sharpe_min=float(_get(cfg, "library_sharpe_min", 1.2)),
                library_fitness_min=float(_get(cfg, "library_fitness_min", 1.0)),
                reverse_sharpe_max=float(_get(cfg, "reverse_sharpe_max", -1.2)),
                reverse_fitness_max=float(_get(cfg, "reverse_fitness_max", -1.0)),
                reverse_log=_get(cfg, "reverse_log", ""),
                negate_max_per_round=int(_get(cfg, "negate_max_per_round", 0)),
                stop_event=self.stop_event,
            )
            self.last_summary = summary
        except Exception as exc:
            self.last_error = str(exc)
            logging.exception("Flow failed: %s", exc)
        finally:
            with self.lock:
                self.running = False
                self.last_stop = self._now()


APP_STATE: Optional[FlowController] = None
LOG_BUFFER: Optional[LogBufferHandler] = None


class Handler(BaseHTTPRequestHandler):
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

        if parsed.path == "/api/status":
            state = APP_STATE
            if not state:
                self._send_json({"error": "state not ready"}, status=500)
                return
            stats = _results_stats(state.results_dir)
            meta = (
                f"running={state.running} | results={stats['count']} | latest={stats['latest']} | "
                f"start={state.last_start or '-'} | stop={state.last_stop or '-'} | "
                f"error={state.last_error or '-'}"
            )
            self._send_json(
                {
                    "running": state.running,
                    "meta": meta,
                    "log": LOG_BUFFER.get_text() if LOG_BUFFER else "",
                }
            )
            return

        if parsed.path.startswith("/api/"):
            state = APP_STATE
            if not state:
                self._send_json({"text": "state not ready"}, status=500)
                return
            qs = parse_qs(parsed.query)
            limit = int(qs.get("limit", ["12"])[0] or 12)
            records = _load_results(state.results_dir)
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
                library = _load_library(state.library_path)
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

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        state = APP_STATE
        if not state:
            self._send_json({"error": "state not ready"}, status=500)
            return
        if parsed.path == "/api/start":
            msg = state.start()
            self._send_json({"ok": True, "message": msg})
            return
        if parsed.path == "/api/stop":
            msg = state.stop()
            self._send_json({"ok": True, "message": msg})
            return
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt: str, *args) -> None:
        logging.info("%s - %s", self.address_string(), fmt % args)


def main() -> int:
    global APP_STATE, LOG_BUFFER
    args = parse_args()
    configure_logging(args.log_level)

    LOG_BUFFER = LogBufferHandler(limit=args.log_limit)
    LOG_BUFFER.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logging.getLogger().addHandler(LOG_BUFFER)

    APP_STATE = FlowController(args.config, args.results_dir, args.library)

    server = ThreadingHTTPServer((args.host, args.port), Handler)
    logging.info("Web console listening on http://%s:%s", args.host, args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logging.info("Shutting down")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
