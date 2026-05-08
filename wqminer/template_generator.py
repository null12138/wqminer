"""LLM-based FASTEXPR template generation."""

import random
import re
from typing import Dict, List

from .llm_client import OpenAICompatibleLLM
from .models import DataField, TemplateCandidate
from .operator_store import operator_name_set


class TemplateGenerator:
    def __init__(self, llm: OpenAICompatibleLLM, operators: List[Dict]):
        self.llm = llm
        self.operators = operators
        self.operator_names = operator_name_set(operators)

    def generate_templates(
        self,
        region: str,
        data_fields: List[DataField],
        count: int,
        style_prompt: str = "",
        policy_prompt: str = "",
        max_prompt_operators: int = 24,
        max_prompt_fields: int = 30,
    ) -> List[TemplateCandidate]:
        if not data_fields:
            raise ValueError("No data fields supplied for template generation")

        sampled_operators = random.sample(self.operators, min(max_prompt_operators, len(self.operators)))
        sampled_fields = random.sample(data_fields, min(max_prompt_fields, len(data_fields)))

        operator_lines = []
        for op in sampled_operators:
            name = op.get("name", "")
            definition = op.get("definition", "")
            desc = op.get("description", "")
            if name:
                operator_lines.append(f"- {name}: {definition}. {desc}")

        field_lines = [f"- {f.field_id}: {f.description or 'no description'}" for f in sampled_fields]

        compact_style = self._truncate(style_prompt, 1800)
        compact_policy = self._truncate(policy_prompt, 1800)

        system_prompt = (
            "You are an execution-focused WorldQuant BRAIN FASTEXPR worker. "
            "Prioritize throughput and validity first. "
            "Output ONLY valid FASTEXPR expressions, one per line, no markdown."
        )

        user_prompt = (
            f"Region: {region}\n"
            f"Task: Generate exactly {count} diverse FASTEXPR alpha expressions.\n"
            "Hard constraints (must obey):\n"
            "- Use ONLY the operators listed below (names must match exactly).\n"
            "- Use ONLY the field IDs listed below (spelling must match exactly).\n"
            "- Each line must be a complete, balanced expression.\n"
            "- Avoid undefined identifiers and avoid empty/constant-only expressions.\n"
            "- Keep each expression under 200 characters.\n"
            "- If any style/policy conflicts with hard constraints, follow hard constraints.\n"
            "- Prefer economically sensible and stable, neutralizable structures over noisy hacks.\n"
            f"Policy guidance:\n{compact_policy or 'none'}\n"
            f"Style requirements:\n{compact_style or 'none'}\n\n"
            "Operators:\n"
            + "\n".join(operator_lines)
            + "\n\nData fields:\n"
            + "\n".join(field_lines)
            + "\n\nReturn exactly one expression per line."
        )

        raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt)
        expressions = self._parse_expressions(raw)

        results: List[TemplateCandidate] = []
        seen = set()
        for expr in expressions:
            if expr in seen:
                continue
            seen.add(expr)

            fields_used = self.extract_fields_used(expr, data_fields)
            if not fields_used:
                continue

            operators_used = self.extract_operators_used(expr)
            if not self._is_balanced(expr):
                continue

            results.append(
                TemplateCandidate(
                    expression=expr,
                    source_prompt=style_prompt,
                    fields_used=fields_used,
                    operators_used=operators_used,
                )
            )
            if len(results) >= count:
                break

        return results

    def extract_fields_used(self, expression: str, data_fields: List[DataField]) -> List[str]:
        known = [f.field_id for f in data_fields if f.field_id]
        known.sort(key=len, reverse=True)
        found = []
        for field in known:
            if re.search(rf"(?<![A-Za-z0-9_]){re.escape(field)}(?![A-Za-z0-9_])", expression):
                found.append(field)
        return found

    def extract_operators_used(self, expression: str) -> List[str]:
        found = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)
        uniq = []
        for op in found:
            if op in self.operator_names and op not in uniq:
                uniq.append(op)
        return uniq

    def _parse_expressions(self, raw: str) -> List[str]:
        cleaned = raw.replace("```", "\n")
        lines = [line.strip() for line in cleaned.splitlines()]
        out = []
        for line in lines:
            if not line:
                continue
            line = re.sub(r"^\d+[\).]\s*", "", line)
            line = line.strip("`").strip()
            if not line:
                continue
            if any(prefix in line.lower() for prefix in ["here are", "explanation", "template"]):
                continue
            if not self._looks_like_expression(line):
                continue
            out.append(line)
        return out

    @staticmethod
    def _looks_like_expression(line: str) -> bool:
        has_call = "(" in line and ")" in line
        has_math = any(sym in line for sym in ["+", "-", "*", "/", "?", ":"])
        return has_call or has_math

    @staticmethod
    def _is_balanced(line: str) -> bool:
        depth = 0
        for ch in line:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
                if depth < 0:
                    return False
        return depth == 0

    @staticmethod
    def _truncate(text: str, limit: int) -> str:
        raw = (text or "").strip()
        if limit <= 0 or len(raw) <= limit:
            return raw
        return raw[:limit].rstrip()
