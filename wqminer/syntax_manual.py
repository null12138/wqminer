"""Build FASTEXPR syntax manuals for LLM prompting."""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


@dataclass
class FastExprSyntaxManual:
    generated_at: str
    operator_count: int
    template_count: int
    categories: Dict[str, List[Dict]]
    top_operators: List[Dict]
    generation_rules: List[str]
    checker_rules: List[str]
    example_templates: List[str]
    common_errors: List[Dict]

    def to_dict(self) -> Dict:
        return {
            "generated_at": self.generated_at,
            "operator_count": self.operator_count,
            "template_count": self.template_count,
            "categories": self.categories,
            "top_operators": self.top_operators,
            "generation_rules": self.generation_rules,
            "checker_rules": self.checker_rules,
            "example_templates": self.example_templates,
            "common_errors": self.common_errors,
        }

    def to_markdown(self) -> str:
        lines: List[str] = []
        lines.append("# FASTEXPR Syntax Manual")
        lines.append("")
        lines.append(f"- Generated at: `{self.generated_at}`")
        lines.append(f"- Operators covered: `{self.operator_count}`")
        lines.append(f"- Template samples: `{self.template_count}`")
        lines.append("")

        lines.append("## Grammar Skeleton")
        lines.append("```text")
        lines.append("<expr> := <op>(<arg1>, <arg2>, ...) | <expr> <arith> <expr> | (<expr>) | if_else(<cond>, <a>, <b>)")
        lines.append("<cond> := greater(x,y) | less(x,y) | equal(x,y) | (<expr> > <expr>) | (<expr> && <expr>)")
        lines.append("<grouping> := market | sector | industry | subindustry | bucket(rank(cap), range=\"0,1,0.1\")")
        lines.append("```")
        lines.append("")

        lines.append("## Generation Rules")
        for rule in self.generation_rules:
            lines.append(f"- {rule}")
        lines.append("")

        lines.append("## Checker Rules")
        for rule in self.checker_rules:
            lines.append(f"- {rule}")
        lines.append("")

        lines.append("## Operator Cheat Sheet")
        for category, items in self.categories.items():
            lines.append(f"### {category}")
            for item in items:
                name = item.get("name", "")
                definition = item.get("definition", "")
                lines.append(f"- `{name}`: `{definition}`")
            lines.append("")

        lines.append("## High-Frequency Operators")
        for item in self.top_operators:
            lines.append(f"- `{item['name']}`: {item['count']}")
        lines.append("")

        lines.append("## Valid Template Examples")
        for expr in self.example_templates:
            lines.append(f"- `{expr}`")
        lines.append("")

        lines.append("## Common Errors")
        for err in self.common_errors:
            lines.append(f"- `{err['name']}`: {err['description']}")
        lines.append("")
        return "\n".join(lines).strip() + "\n"


def build_syntax_manual(
    operators: Sequence[Dict],
    expressions: Iterable[str],
    max_ops_per_category: int = 12,
    top_operator_k: int = 24,
    example_k: int = 18,
) -> FastExprSyntaxManual:
    expr_list = [x.strip() for x in expressions if x and x.strip()]
    op_names = {str(op.get("name", "")).strip() for op in operators if op.get("name")}
    op_names_lower = {x.lower() for x in op_names}

    categories = defaultdict(list)
    for op in operators:
        name = str(op.get("name", "")).strip()
        if not name:
            continue
        category = str(op.get("category", "Other")).strip() or "Other"
        categories[category].append(
            {
                "name": name,
                "definition": str(op.get("definition", "")).strip(),
                "description": str(op.get("description", "")).strip(),
            }
        )

    trimmed_categories: Dict[str, List[Dict]] = {}
    for category in sorted(categories.keys()):
        items = sorted(categories[category], key=lambda x: x["name"])
        trimmed_categories[category] = items[:max_ops_per_category]

    counter = Counter()
    for expr in expr_list:
        calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)
        for call in calls:
            if call in op_names:
                counter[call] += 1
            elif call.lower() in op_names_lower:
                counter[call.lower()] += 1

    top_operators = [{"name": name, "count": cnt} for name, cnt in counter.most_common(top_operator_k)]
    examples = _pick_examples(expr_list, op_names, k=example_k)

    return FastExprSyntaxManual(
        generated_at=time.strftime("%Y-%m-%d %H:%M:%S"),
        operator_count=len(op_names),
        template_count=len(expr_list),
        categories=trimmed_categories,
        top_operators=top_operators,
        generation_rules=[
            "Output exactly one FASTEXPR expression per line.",
            "Use only listed operators and listed data fields.",
            "Expression must be single-line and parenthesis-balanced.",
            "Do not output placeholders such as {datafield}.",
            "Do not output assignments, markdown, or explanations.",
            "Prefer concise compositions: rank/ts_* plus optional group neutralization.",
        ],
        checker_rules=[
            "Repair syntax only, do not change signal intent.",
            "Keep as much original structure as possible.",
            "Use known operators only.",
            "Keep expression single-line and balanced.",
            "If unrecoverable, return INVALID.",
        ],
        example_templates=examples,
        common_errors=[
            {"name": "placeholder", "description": "Unresolved tokens like {data} or <field>"},
            {"name": "unknown_operator", "description": "Functions outside allowed operator list"},
            {"name": "unbalanced_parentheses", "description": "Missing ')' or extra ')'"},
            {"name": "multi_statement", "description": "Multiple statements, assignments, or comments in one line"},
            {"name": "non_ascii_punctuation", "description": "Chinese punctuation such as ，；（）"},
        ],
    )


def write_syntax_manual_markdown(path: str, manual: FastExprSyntaxManual) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(manual.to_markdown(), encoding="utf-8")


def write_syntax_manual_json(path: str, manual: FastExprSyntaxManual) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(manual.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _pick_examples(expressions: Sequence[str], op_names: Iterable[str], k: int) -> List[str]:
    names = {str(x).strip() for x in op_names if str(x).strip()}
    out: List[str] = []
    seen = set()

    for expr in expressions:
        if expr in seen:
            continue
        if len(expr) < 8 or len(expr) > 260:
            continue
        if "{" in expr or "}" in expr:
            continue
        if expr.count("(") != expr.count(")"):
            continue
        calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)
        if not calls:
            continue
        known_calls = [x for x in calls if x in names]
        if not known_calls:
            continue
        out.append(expr)
        seen.add(expr)
        if len(out) >= k:
            break

    return out
