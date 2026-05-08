"""Operator utilities."""

import json
from pathlib import Path
from typing import Dict, List


def operator_file_path() -> Path:
    return Path(__file__).resolve().parent / "constants" / "operatorRAW.json"


def load_operators(path: str = "") -> List[Dict]:
    target = Path(path) if path else operator_file_path()
    with open(target, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, list):
        raise ValueError(f"Invalid operator file: {target}")
    return payload


def operator_name_set(operators: List[Dict]) -> set:
    return {str(op.get("name", "")).strip() for op in operators if op.get("name")}
