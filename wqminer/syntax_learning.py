"""FASTEXPR syntax learning utilities from template corpus."""

from __future__ import annotations

import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence


@dataclass
class SyntaxLearningReport:
    template_count: int
    operator_top: List[Dict]
    field_token_top: List[Dict]
    pattern_examples: Dict[str, List[str]]

    def to_dict(self) -> Dict:
        return {
            "template_count": self.template_count,
            "operator_top": self.operator_top,
            "field_token_top": self.field_token_top,
            "pattern_examples": self.pattern_examples,
        }


def learn_syntax_from_templates(expressions: Sequence[str], operator_names: Iterable[str], top_k: int = 25) -> SyntaxLearningReport:
    operator_set = {op for op in operator_names if op}

    op_counter: Counter = Counter()
    token_counter: Counter = Counter()

    patterns = {
        "momentum_like": [],
        "mean_reversion_like": [],
        "volume_price": [],
        "group_neutralization": [],
        "conditional_trade": [],
    }

    for expr in expressions:
        if not expr:
            continue

        calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)
        for call in calls:
            if call in operator_set:
                op_counter[call] += 1

        tokens = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\b", expr)
        for tok in tokens:
            if tok in operator_set:
                continue
            if tok.lower() in {
                "std",
                "lag",
                "rettype",
                "dense",
                "true",
                "false",
                "industry",
                "sector",
                "subindustry",
                "country",
                "market",
                "group",
                "bucket",
                "range",
            }:
                continue
            token_counter[tok] += 1

        e = expr.lower()
        if any(k in e for k in ["ts_delta", "ts_rank", "returns"]):
            _push_pattern(patterns["momentum_like"], expr)
        if any(k in e for k in ["ts_mean", "ts_zscore", "winsorize"]):
            _push_pattern(patterns["mean_reversion_like"], expr)
        if "volume" in e and any(k in e for k in ["close", "vwap", "open", "high", "low"]):
            _push_pattern(patterns["volume_price"], expr)
        if any(k in e for k in ["group_", "neutralize", "subindustry", "industry", "sector"]):
            _push_pattern(patterns["group_neutralization"], expr)
        if any(k in e for k in ["trade_when", "?", ":"]):
            _push_pattern(patterns["conditional_trade"], expr)

    operator_top = [{"name": k, "count": v} for k, v in op_counter.most_common(top_k)]
    field_token_top = [{"name": k, "count": v} for k, v in token_counter.most_common(top_k)]

    return SyntaxLearningReport(
        template_count=len([x for x in expressions if x]),
        operator_top=operator_top,
        field_token_top=field_token_top,
        pattern_examples=patterns,
    )


def write_syntax_markdown(path: str, report: SyntaxLearningReport) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)

    lines: List[str] = []
    lines.append("# FASTEXPR 语法学习总结")
    lines.append("")
    lines.append(f"模板样本数：`{report.template_count}`")
    lines.append("")

    lines.append("## 高频操作符")
    for item in report.operator_top:
        lines.append(f"- `{item['name']}`: {item['count']}")
    lines.append("")

    lines.append("## 高频字段/标识符")
    for item in report.field_token_top:
        lines.append(f"- `{item['name']}`: {item['count']}")
    lines.append("")

    lines.append("## 常见表达式结构示例")
    for name, examples in report.pattern_examples.items():
        lines.append(f"### {name}")
        if not examples:
            lines.append("- 无")
        else:
            for expr in examples[:8]:
                lines.append(f"- `{expr}`")
        lines.append("")

    lines.append("## 经验规则")
    lines.append("- 时序算子常见组合：`ts_delta/ts_mean/ts_rank/ts_corr`。")
    lines.append("- 排名归一化常见：`rank(...)` 与 `group_*` 中性化。")
    lines.append("- 价格-成交量联动是高频主题：`volume` 与 `close/vwap/open/high/low` 搭配。")
    lines.append("- 条件表达常用于开关仓：`trade_when` 或 `?:`。")

    target.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_syntax_json(path: str, report: SyntaxLearningReport) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")


def _push_pattern(items: List[str], expr: str, max_keep: int = 12) -> None:
    if expr in items:
        return
    items.append(expr)
    if len(items) > max_keep:
        del items[max_keep:]
