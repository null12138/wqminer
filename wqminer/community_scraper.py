"""Community content scraper and FASTEXPR template extractor."""

from __future__ import annotations

import ast
import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import urljoin, urlparse

import requests

logger = logging.getLogger(__name__)


@dataclass
class PageCapture:
    url: str
    fetched_url: str
    status_code: int
    ok: bool
    error: str
    templates_found: int


class CommunityTemplateScraper:
    def __init__(self, operator_names: Iterable[str]):
        self.operator_names = sorted({name for name in operator_names if name}, key=len, reverse=True)

    def scrape(
        self,
        seed_urls: Sequence[str],
        input_files: Sequence[str],
        max_pages: int = 20,
        timeout_sec: int = 30,
        use_mirror: bool = True,
        use_playwright: bool = False,
        playwright_wait_sec: int = 8,
        playwright_storage_state: str = "",
        playwright_headless: bool = True,
        same_domain_only: bool = True,
    ) -> Dict:
        captures: List[PageCapture] = []
        expression_sources: Dict[str, Set[str]] = {}
        visited: Set[str] = set()
        queue: List[str] = [u for u in seed_urls if u]

        allowed_hosts = set()
        for url in queue:
            host = urlparse(url).netloc
            if host:
                allowed_hosts.add(host)

        while queue and len(captures) < max_pages:
            url = queue.pop(0)
            if not url or url in visited:
                continue
            visited.add(url)

            text, fetched_url, status_code, error = self._fetch_url(
                url,
                use_mirror=use_mirror,
                timeout_sec=timeout_sec,
                use_playwright=use_playwright,
                playwright_wait_sec=playwright_wait_sec,
                playwright_storage_state=playwright_storage_state,
                playwright_headless=playwright_headless,
            )
            expressions = self.extract_templates_from_text(text)

            capture = PageCapture(
                url=url,
                fetched_url=fetched_url,
                status_code=status_code,
                ok=(status_code == 200 and not error),
                error=error,
                templates_found=len(expressions),
            )
            captures.append(capture)

            for expr in expressions:
                expression_sources.setdefault(expr, set()).add(url)

            links = self._extract_links(text, base_url=url)
            for link in links:
                if link in visited or link in queue:
                    continue
                if same_domain_only and allowed_hosts:
                    link_host = urlparse(link).netloc
                    if link_host and link_host not in allowed_hosts:
                        continue
                queue.append(link)

        for file_path in input_files:
            expressions = self.extract_templates_from_file(file_path)
            source = f"file://{Path(file_path).resolve()}"
            for expr in expressions:
                expression_sources.setdefault(expr, set()).add(source)

        templates = []
        for expr in sorted(expression_sources.keys()):
            templates.append(
                {
                    "expression": expr,
                    "source_urls": sorted(expression_sources[expr]),
                    "operators_used": self._operators_used(expr),
                }
            )

        return {
            "summary": {
                "seed_url_count": len([u for u in seed_urls if u]),
                "input_file_count": len([f for f in input_files if f]),
                "pages_visited": len(captures),
                "pages_ok": sum(1 for c in captures if c.ok),
                "pages_blocked_or_failed": sum(1 for c in captures if not c.ok),
                "templates_total": len(templates),
            },
            "pages": [
                {
                    "url": c.url,
                    "fetched_url": c.fetched_url,
                    "status_code": c.status_code,
                    "ok": c.ok,
                    "error": c.error,
                    "templates_found": c.templates_found,
                }
                for c in captures
            ],
            "templates": templates,
        }

    def extract_templates_from_file(self, file_path: str) -> List[str]:
        path = Path(file_path)
        if not path.exists():
            return []

        text = path.read_text(encoding="utf-8", errors="ignore")
        expressions = set(self.extract_templates_from_text(text))

        if path.suffix.lower() == ".py":
            expressions.update(self._extract_from_python_literals(text))

        if path.suffix.lower() in {".json", ".jsonl"}:
            expressions.update(self._extract_from_json_literals(text))

        return sorted(expressions)

    def extract_templates_from_text(self, text: str) -> List[str]:
        if not text:
            return []

        chunks = [text]
        fenced = re.findall(r"```[a-zA-Z0-9_\-]*\n(.*?)```", text, flags=re.S)
        chunks.extend(fenced)

        out: Set[str] = set()
        for chunk in chunks:
            for raw_line in chunk.splitlines():
                for candidate in self._split_candidate_line(raw_line):
                    expr = self._normalize_candidate(candidate)
                    if not expr:
                        continue
                    if not self._is_expression_like(expr):
                        continue
                    if not self._is_balanced(expr):
                        continue
                    if not self._operators_used(expr):
                        continue
                    out.add(expr)

        return sorted(out)

    def _fetch_url(
        self,
        url: str,
        use_mirror: bool,
        timeout_sec: int,
        use_playwright: bool = False,
        playwright_wait_sec: int = 8,
        playwright_storage_state: str = "",
        playwright_headless: bool = True,
    ) -> Tuple[str, str, int, str]:
        if use_playwright:
            text, fetched_url, status_code, error = self._fetch_url_playwright(
                url=url,
                timeout_sec=timeout_sec,
                wait_sec=playwright_wait_sec,
                storage_state_path=playwright_storage_state,
                headless=playwright_headless,
            )
            if text and status_code == 200 and not error:
                return text, fetched_url, status_code, error

        targets = []
        if use_mirror:
            targets.append(self._mirror_url(url))
        targets.append(url)

        last_error = ""
        for target in targets:
            try:
                response = requests.get(
                    target,
                    timeout=timeout_sec,
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "text/html,application/json"},
                )
                if response.status_code == 200:
                    return response.text, target, response.status_code, ""
                last_error = f"http_{response.status_code}"
            except Exception as exc:  # pragma: no cover - network variation
                last_error = str(exc)
        return "", targets[-1], 0, last_error

    @staticmethod
    def _fetch_url_playwright(
        url: str,
        timeout_sec: int,
        wait_sec: int,
        storage_state_path: str = "",
        headless: bool = True,
    ) -> Tuple[str, str, int, str]:
        try:
            from playwright.sync_api import sync_playwright
        except Exception as exc:  # pragma: no cover - env variation
            return "", url, 0, f"playwright_not_available: {exc}"

        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=headless)
                context_kwargs = {}
                if storage_state_path:
                    state_file = Path(storage_state_path)
                    if state_file.exists():
                        context_kwargs["storage_state"] = str(state_file)
                context = browser.new_context(**context_kwargs)
                page = context.new_page()
                page.goto(url, wait_until="commit", timeout=timeout_sec * 1000)
                page.wait_for_timeout(max(0, wait_sec) * 1000)
                final_url = page.url
                title = page.title().strip()
                html = page.content()
                context.close()
                browser.close()

            if "just a moment" in title.lower() or "__cf_chl_rt_tk" in final_url:
                return html, final_url, 200, "cloudflare_challenge"
            return html, final_url, 200, ""
        except Exception as exc:  # pragma: no cover - network variation
            return "", url, 0, f"playwright_fetch_failed: {exc}"

    @staticmethod
    def _mirror_url(url: str) -> str:
        if url.startswith("https://"):
            return "https://r.jina.ai/http://" + url[len("https://") :]
        if url.startswith("http://"):
            return "https://r.jina.ai/http://" + url[len("http://") :]
        return "https://r.jina.ai/http://" + url

    def _extract_links(self, text: str, base_url: str) -> List[str]:
        links: Set[str] = set()

        for m in re.finditer(r"https?://[^\s\)\]\'\"<>]+", text):
            links.add(m.group(0))

        for m in re.finditer(r"/(?:hc/[a-z\-]+/)?community/(?:posts|topics)[^\s\)\]\'\"<>]*", text):
            links.add(urljoin(base_url, m.group(0)))

        return sorted(links)

    @staticmethod
    def _split_candidate_line(raw_line: str) -> List[str]:
        line = raw_line.strip()
        if not line:
            return []
        if line.startswith("#") or line.startswith("//"):
            return []

        line = re.sub(r"^[-*]\s+", "", line)
        line = re.sub(r"^\d+[\).]\s*", "", line)
        line = line.replace("`", "").strip()

        parts = [p.strip() for p in line.split(";") if p.strip()]
        return parts if parts else [line]

    def _normalize_candidate(self, line: str) -> str:
        expr = line.strip().rstrip(",")

        if len(expr) >= 2 and expr[0] in {'"', "'"} and expr[-1] == expr[0]:
            expr = expr[1:-1]

        if "=" in expr and "(" in expr and expr.index("=") < expr.index("("):
            left, right = expr.split("=", 1)
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", left.strip()):
                expr = right.strip()

        expr = expr.strip().strip(";")
        return re.sub(r"\s+", " ", expr)

    def _is_expression_like(self, expr: str) -> bool:
        if len(expr) < 8 or len(expr) > 400:
            return False
        if "http://" in expr or "https://" in expr:
            return False
        has_call = "(" in expr and ")" in expr
        has_math = any(sym in expr for sym in ["+", "-", "*", "/", "?", ":", "^"])
        return has_call or has_math

    @staticmethod
    def _is_balanced(expr: str) -> bool:
        depth = 0
        for ch in expr:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0

    def _operators_used(self, expr: str) -> List[str]:
        used = []
        lowered = expr
        for op in self.operator_names:
            if f"{op}(" in lowered:
                used.append(op)
        return used

    def _extract_from_python_literals(self, text: str) -> Set[str]:
        out: Set[str] = set()
        try:
            tree = ast.parse(text)
        except SyntaxError:
            return out

        for node in ast.walk(tree):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                for expr in self.extract_templates_from_text(node.value):
                    out.add(expr)
        return out

    def _extract_from_json_literals(self, text: str) -> Set[str]:
        out: Set[str] = set()

        def walk(v):
            if isinstance(v, str):
                for expr in self.extract_templates_from_text(v):
                    out.add(expr)
            elif isinstance(v, list):
                for item in v:
                    walk(item)
            elif isinstance(v, dict):
                for item in v.values():
                    walk(item)

        try:
            payload = json.loads(text)
            walk(payload)
            return out
        except Exception:
            pass

        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except Exception:
                continue
            walk(payload)

        return out
