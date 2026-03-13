"""FASTEXPR syntax validation and normalization."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple


_BAD_PUNCTUATION = {"，", "；", "：", "（", "）"}
_NON_EXPR_KEYWORDS = {
    "alpha",
    "signal",
    "template",
    "expression",
    "return",
    "returns",
    "explanation",
}


@dataclass
class SyntaxIssue:
    code: str
    message: str

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message}


@dataclass
class SyntaxCheckResult:
    original_expression: str
    normalized_expression: str
    is_valid: bool
    operators_used: List[str]
    fields_used: List[str]
    issues: List[SyntaxIssue]

    def to_dict(self) -> dict:
        return {
            "original_expression": self.original_expression,
            "normalized_expression": self.normalized_expression,
            "is_valid": self.is_valid,
            "operators_used": self.operators_used,
            "fields_used": self.fields_used,
            "issues": [x.to_dict() for x in self.issues],
        }


class FastExprSyntaxValidator:
    def __init__(self, operator_names: Iterable[str], field_ids: Sequence[str] = ()):
        self.operator_names = sorted({str(x).strip() for x in operator_names if str(x).strip()}, key=len, reverse=True)
        self.operator_set = set(self.operator_names)
        self.operator_lower_to_name = {name.lower(): name for name in self.operator_names}
        self.field_ids = sorted({str(x).strip() for x in field_ids if str(x).strip()}, key=len, reverse=True)

    def with_fields(self, field_ids: Sequence[str]) -> "FastExprSyntaxValidator":
        return FastExprSyntaxValidator(operator_names=self.operator_names, field_ids=field_ids)

    def normalize_expression(self, expression: str) -> str:
        expr = (expression or "").strip()
        if not expr:
            return ""

        expr = expr.replace("```", " ").strip()
        expr = re.sub(r"^\d+[\).]\s*", "", expr)
        expr = expr.strip().strip(";")

        if "=" in expr and "(" in expr and expr.index("=") < expr.index("("):
            left, right = expr.split("=", 1)
            if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", left.strip()):
                expr = right.strip()

        # Drop obvious narrative prefixes from LLM output.
        expr_lower = expr.lower()
        for kw in _NON_EXPR_KEYWORDS:
            if expr_lower.startswith(kw + ":"):
                expr = expr.split(":", 1)[1].strip()
                break

        expr = re.sub(r"\s+", " ", expr)
        return expr.strip().strip(";")

    def validate(self, expression: str, require_known_field: bool = True) -> SyntaxCheckResult:
        original = expression or ""
        normalized = self.normalize_expression(original)
        issues: List[SyntaxIssue] = []

        if not normalized:
            issues.append(SyntaxIssue("empty", "Expression is empty after normalization"))
            return SyntaxCheckResult(original, normalized, False, [], [], issues)

        if len(normalized) < 6:
            issues.append(SyntaxIssue("too_short", "Expression is too short"))
        if len(normalized) > 600:
            issues.append(SyntaxIssue("too_long", "Expression is too long"))

        if any(ch in normalized for ch in _BAD_PUNCTUATION):
            issues.append(SyntaxIssue("bad_punctuation", "Use ASCII punctuation only"))

        if "http://" in normalized or "https://" in normalized:
            issues.append(SyntaxIssue("has_url", "Expression contains URL text"))

        if "{" in normalized or "}" in normalized:
            issues.append(SyntaxIssue("placeholder", "Template placeholder braces should be resolved"))

        if not self._is_balanced_parentheses(normalized):
            issues.append(SyntaxIssue("unbalanced_parentheses", "Parentheses are not balanced"))

        operators_used, unknown_calls = self._scan_operators(normalized)
        if not operators_used:
            issues.append(SyntaxIssue("no_operator", "No known operator call found"))
        if unknown_calls:
            issues.append(SyntaxIssue("unknown_operator", f"Unknown call(s): {', '.join(unknown_calls[:6])}"))

        fields_used = self._scan_fields(normalized)
        if require_known_field and self.field_ids and not fields_used:
            issues.append(SyntaxIssue("no_known_field", "Expression does not reference known fields"))

        return SyntaxCheckResult(
            original_expression=original,
            normalized_expression=normalized,
            is_valid=(len(issues) == 0),
            operators_used=operators_used,
            fields_used=fields_used,
            issues=issues,
        )

    @staticmethod
    def _is_balanced_parentheses(expr: str) -> bool:
        depth = 0
        for ch in expr:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0

    def _scan_operators(self, expr: str) -> Tuple[List[str], List[str]]:
        calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)
        used: List[str] = []
        unknown: List[str] = []

        for call in calls:
            if call in self.operator_set:
                if call not in used:
                    used.append(call)
                continue

            mapped = self.operator_lower_to_name.get(call.lower())
            if mapped:
                if mapped not in used:
                    used.append(mapped)
                continue

            if call not in unknown:
                unknown.append(call)

        return used, unknown

    def _scan_fields(self, expr: str) -> List[str]:
        found: List[str] = []
        for field_id in self.field_ids:
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(field_id)}(?![A-Za-z0-9_])", expr):
                found.append(field_id)
        return found
