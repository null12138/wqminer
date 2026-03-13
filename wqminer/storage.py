"""Filesystem helpers for cache and artifacts."""

import csv
import json
from pathlib import Path
from typing import Dict, Iterable, List

from .models import DataField, TemplateCandidate


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def save_data_fields_cache(path: str, fields: List[DataField]) -> None:
    target = Path(path)
    ensure_parent(target)
    with open(target, "w", encoding="utf-8") as handle:
        json.dump([f.to_dict() for f in fields], handle, indent=2, ensure_ascii=False)


def load_data_fields_cache(path: str) -> List[DataField]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return [DataField.from_api(item) for item in payload]


def save_templates(path: str, templates: List[TemplateCandidate], metadata: Dict = None) -> None:
    target = Path(path)
    ensure_parent(target)
    payload = {
        "metadata": metadata or {},
        "templates": [t.to_dict() for t in templates],
    }
    with open(target, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, ensure_ascii=False)


def load_templates(path: str) -> List[str]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    raw = payload.get("templates", payload)
    if isinstance(raw, list):
        expressions = []
        for item in raw:
            if isinstance(item, dict) and item.get("expression"):
                expressions.append(item["expression"])
            elif isinstance(item, str):
                expressions.append(item)
        return expressions
    return []


def append_jsonl(path: str, rows: Iterable[Dict]) -> None:
    target = Path(path)
    ensure_parent(target)
    with open(target, "a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_csv(path: str, rows: List[Dict]) -> None:
    target = Path(path)
    ensure_parent(target)
    if not rows:
        return
    keys = list(rows[0].keys())
    with open(target, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)
