"""Filesystem helpers for cache."""

import json
from pathlib import Path
from typing import List

from .models import DataField


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
