"""Playwright-based interactive authentication helpers."""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path
from typing import Dict

logger = logging.getLogger(__name__)


def interactive_login_and_save_state(
    start_url: str,
    state_file: str,
    user_data_dir: str,
    wait_seconds: int = 240,
) -> Dict:
    """
    Open a persistent Playwright browser, let user manually pass challenge/login,
    then save storage state for future scraping.
    """
    try:
        from playwright.sync_api import sync_playwright
    except Exception as exc:
        return {
            "ok": False,
            "error": f"playwright_not_available: {exc}",
            "state_file": state_file,
        }

    state_path = Path(state_file)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    profile_dir = Path(user_data_dir)
    profile_dir.mkdir(parents=True, exist_ok=True)

    started_at = int(time.time())
    solved = False
    last_title = ""
    last_url = start_url

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            page = context.pages[0] if context.pages else context.new_page()
            page.goto(start_url, wait_until="domcontentloaded", timeout=120000)

            deadline = time.time() + max(30, wait_seconds)
            while time.time() < deadline:
                page.wait_for_timeout(3000)
                try:
                    last_title = (page.title() or "").strip()
                    last_url = page.url
                except Exception:
                    # page may navigate while querying title/url
                    continue
                title_lower = last_title.lower()

                challenge = ("just a moment" in title_lower) or ("__cf_chl_rt_tk" in last_url)
                signin = ("sign in" in title_lower) or ("/sign-in" in last_url)
                solved = (not challenge) and (not signin)

                remaining = int(max(0, deadline - time.time()))
                logger.info("Waiting manual login/challenge pass... remaining=%ss url=%s title=%s", remaining, last_url, last_title)

                if solved:
                    break

            context.storage_state(path=str(state_path))
        finally:
            context.close()

    report = {
        "ok": solved,
        "state_file": str(state_path),
        "user_data_dir": str(profile_dir),
        "started_at": started_at,
        "finished_at": int(time.time()),
        "last_url": last_url,
        "last_title": last_title,
        "error": "" if solved else "manual_login_not_confirmed_before_timeout",
    }
    state_path.with_suffix(state_path.suffix + ".report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return report
