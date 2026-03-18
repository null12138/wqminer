"""CLI wrapper for local FASTEXPR validation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import List

from .expression_validator import validate_expression_report
from .operator_store import load_operators


def _load_expressions(args: argparse.Namespace) -> List[str]:
    out: List[str] = []
    if args.expression:
        out.append(args.expression.strip())
    if args.file:
        src = Path(args.file)
        payload = src.read_text(encoding="utf-8")
        if src.suffix.lower() == ".json":
            raw = json.loads(payload)
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict) and item.get("expression"):
                        out.append(str(item.get("expression", "")).strip())
                    elif isinstance(item, str):
                        out.append(item.strip())
            elif isinstance(raw, dict) and isinstance(raw.get("expressions"), list):
                for item in raw["expressions"]:
                    out.append(str(item or "").strip())
        else:
            for line in payload.splitlines():
                line = line.strip()
                if line:
                    out.append(line)
    seen = set()
    uniq: List[str] = []
    for expr in out:
        if not expr or expr in seen:
            continue
        seen.add(expr)
        uniq.append(expr)
    return uniq


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Validate FASTEXPR expressions locally")
    p.add_argument("--expression", default="", help="Single expression to validate")
    p.add_argument("--file", default="", help="Text/JSON file containing expressions")
    p.add_argument("--operator-file", default="", help="Custom operator JSON path")
    p.add_argument("--region", default="", help="Optional region label for report")
    p.add_argument("--delay", type=int, default=None, help="Optional delay label for report")
    p.add_argument("--universe", default="", help="Optional universe label for report")
    p.add_argument("--max-operator-count", type=int, default=0, help="Estimated operator-count ceiling")
    p.add_argument("--allow-unknown-operators", action="store_true", help="Do not fail unknown operators")
    p.add_argument(
        "--allow-optional-positional",
        action="store_true",
        help="Allow optional parameters without name=value",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    expressions = _load_expressions(args)
    if not expressions:
        print(json.dumps({"error": "no expression supplied"}, ensure_ascii=False))
        return 1
    operators = load_operators(args.operator_file)
    reports = [
        validate_expression_report(
            expression=expr,
            operators=operators,
            region=args.region,
            delay=args.delay,
            universe=args.universe,
            max_operator_count=args.max_operator_count,
            require_known_operators=not args.allow_unknown_operators,
            require_keyword_optional=not args.allow_optional_positional,
        )
        for expr in expressions
    ]
    if len(reports) == 1:
        print(json.dumps(reports[0], ensure_ascii=False, indent=2))
        return 0 if reports[0].get("is_valid") else 2
    summary = {
        "total": len(reports),
        "valid": sum(1 for r in reports if r.get("is_valid")),
        "invalid": sum(1 for r in reports if not r.get("is_valid")),
        "reports": reports,
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0 if summary["invalid"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())

