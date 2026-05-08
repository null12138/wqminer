"""Inspiration helpers for template generation."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Dict, List


_DEFAULT_STORE = "data/cache/inspirations.json"


def merge_style_prompt(style_prompt: str, inspiration: str) -> str:
    base = (style_prompt or "").strip()
    idea = (inspiration or "").strip()
    if not idea:
        return base
    if not base:
        return f"Inspiration:\n{idea}".strip()
    return f"{base}\n\nInspiration:\n{idea}".strip()


def save_inspiration(text: str, path: str = _DEFAULT_STORE, max_keep: int = 80) -> Dict:
    idea = (text or "").strip()
    if not idea:
        raise ValueError("Inspiration text is empty")

    payload = []
    store = Path(path)
    if store.exists():
        try:
            payload = json.loads(store.read_text(encoding="utf-8"))
        except Exception:
            payload = []

    entry = {
        "id": f"idea_{int(time.time() * 1000)}",
        "text": idea,
        "created_at": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    payload = [entry] + [x for x in payload if isinstance(x, dict)]
    payload = payload[: max_keep]

    store.parent.mkdir(parents=True, exist_ok=True)
    store.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return entry


def list_inspirations(path: str = _DEFAULT_STORE, max_items: int = 50) -> List[Dict]:
    store = Path(path)
    if not store.exists():
        return []
    try:
        payload = json.loads(store.read_text(encoding="utf-8"))
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    items = [x for x in payload if isinstance(x, dict)]
    return items[: max_items]
