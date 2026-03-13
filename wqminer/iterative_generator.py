"""Iterative FASTEXPR template generation with LLM syntax repair."""

from __future__ import annotations

import random
import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

from .fast_expr_syntax import FastExprSyntaxValidator
from .llm_client import OpenAICompatibleLLM
from .models import DataField, TemplateCandidate


@dataclass
class IterativeRoundReport:
    round_index: int
    requested: int
    generated_raw: int
    accepted_direct: int
    accepted_fixed: int
    rejected: int
    total_collected: int

    def to_dict(self) -> Dict:
        return {
            "round_index": self.round_index,
            "requested": self.requested,
            "generated_raw": self.generated_raw,
            "accepted_direct": self.accepted_direct,
            "accepted_fixed": self.accepted_fixed,
            "rejected": self.rejected,
            "total_collected": self.total_collected,
        }


class IterativeTemplateGenerator:
    def __init__(self, llm: OpenAICompatibleLLM, operators: List[Dict], seed: int = 42):
        self.llm = llm
        self.operators = [op for op in operators if op.get("name")]
        self.operator_names = sorted({str(op["name"]).strip() for op in self.operators if op.get("name")})
        self.random = random.Random(seed)

    def generate(
        self,
        region: str,
        data_fields: Sequence[DataField],
        count: int,
        rounds: int,
        style_prompt: str = "",
        syntax_guide: str = "",
        max_fix_attempts: int = 1,
        max_prompt_operators: int = 24,
        max_prompt_fields: int = 36,
    ) -> Tuple[List[TemplateCandidate], Dict]:
        if not data_fields:
            raise ValueError("No data fields supplied for iterative generation")
        if count <= 0:
            return [], {"requested_count": count, "final_count": 0, "rounds": [], "rejections": []}

        validator = FastExprSyntaxValidator(
            operator_names=self.operator_names,
            field_ids=[f.field_id for f in data_fields if f.field_id],
        )
        sample_fields = [f for f in data_fields if f.field_id]
        collected: Dict[str, TemplateCandidate] = {}
        round_reports: List[IterativeRoundReport] = []
        rejection_log: List[Dict] = []

        for round_idx in range(1, max(1, rounds) + 1):
            if len(collected) >= count:
                break

            requested = max(4, count - len(collected))
            raw_expressions = self._generate_batch(
                region=region,
                data_fields=sample_fields,
                requested=requested,
                style_prompt=style_prompt,
                syntax_guide=syntax_guide,
                max_prompt_operators=max_prompt_operators,
                max_prompt_fields=max_prompt_fields,
            )

            accepted_direct = 0
            accepted_fixed = 0
            rejected = 0

            for raw_expr in raw_expressions:
                if len(collected) >= count:
                    break

                check = validator.validate(raw_expr, require_known_field=True)
                if check.is_valid:
                    accepted_direct += self._try_add_candidate(
                        collected=collected,
                        expression=check.normalized_expression,
                        source_prompt=f"iter_round_{round_idx}",
                        fields_used=check.fields_used,
                        operators_used=check.operators_used,
                    )
                    continue

                fixed = ""
                fixed_check = check
                candidate_expr = check.normalized_expression
                for _ in range(max(0, max_fix_attempts)):
                    fixed = self._repair_expression(
                        expression=candidate_expr,
                        region=region,
                        field_ids=[f.field_id for f in sample_fields],
                        syntax_guide=syntax_guide,
                    )
                    if not fixed:
                        break
                    fixed_check = validator.validate(fixed, require_known_field=True)
                    if fixed_check.is_valid:
                        break
                    candidate_expr = fixed_check.normalized_expression

                if fixed and fixed_check.is_valid:
                    accepted_fixed += self._try_add_candidate(
                        collected=collected,
                        expression=fixed_check.normalized_expression,
                        source_prompt=f"iter_round_{round_idx}:llm_fix",
                        fields_used=fixed_check.fields_used,
                        operators_used=fixed_check.operators_used,
                    )
                else:
                    rejected += 1
                    if len(rejection_log) < 50:
                        rejection_log.append(
                            {
                                "round": round_idx,
                                "expression": raw_expr,
                                "issues": [x.to_dict() for x in check.issues],
                            }
                        )

            round_reports.append(
                IterativeRoundReport(
                    round_index=round_idx,
                    requested=requested,
                    generated_raw=len(raw_expressions),
                    accepted_direct=accepted_direct,
                    accepted_fixed=accepted_fixed,
                    rejected=rejected,
                    total_collected=len(collected),
                )
            )

        templates = list(collected.values())[:count]
        report = {
            "requested_count": count,
            "final_count": len(templates),
            "rounds": [x.to_dict() for x in round_reports],
            "rejections": rejection_log,
        }
        return templates, report

    def _generate_batch(
        self,
        region: str,
        data_fields: Sequence[DataField],
        requested: int,
        style_prompt: str,
        syntax_guide: str,
        max_prompt_operators: int,
        max_prompt_fields: int,
    ) -> List[str]:
        sampled_operators = self.random.sample(self.operators, min(max_prompt_operators, len(self.operators)))
        sampled_fields = self.random.sample(list(data_fields), min(max_prompt_fields, len(data_fields)))

        operator_lines = []
        for op in sampled_operators:
            name = str(op.get("name", "")).strip()
            definition = str(op.get("definition", "")).strip()
            if name:
                operator_lines.append(f"- {name}: {definition}")

        field_lines = []
        for field in sampled_fields:
            field_lines.append(f"- {field.field_id}: {field.description or 'no description'}")

        syntax_hint = (syntax_guide or "").strip()
        if len(syntax_hint) > 3000:
            syntax_hint = syntax_hint[:3000]

        system_prompt = (
            "You are a WorldQuant Brain FASTEXPR template generator. "
            "Output only expressions, one expression per line, no markdown."
        )

        user_prompt = (
            f"Region: {region}\n"
            f"Generate {requested} FASTEXPR templates.\n"
            "Rules:\n"
            "- Use only operators from the provided operator list.\n"
            "- Use only data fields from the provided field list.\n"
            "- Expression must be single-line and syntactically complete.\n"
            "- Do not output placeholders like {datafield}.\n"
            "- Do not output explanations.\n"
            f"- Additional style: {style_prompt or 'none'}\n\n"
            "Operator list:\n"
            + "\n".join(operator_lines)
            + "\n\nField list:\n"
            + "\n".join(field_lines)
        )
        if syntax_hint:
            user_prompt += "\n\nSyntax manual excerpt:\n" + syntax_hint
        user_prompt += "\n\nReturn one expression per line."

        raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt)
        return self._parse_expressions(raw)

    def _repair_expression(
        self,
        expression: str,
        region: str,
        field_ids: Sequence[str],
        syntax_guide: str,
    ) -> str:
        sampled_fields = self.random.sample(list(field_ids), min(40, len(field_ids)))
        syntax_hint = (syntax_guide or "").strip()
        if len(syntax_hint) > 2500:
            syntax_hint = syntax_hint[:2500]

        system_prompt = (
            "You are a FASTEXPR syntax checker. "
            "Fix only syntax and formatting. Preserve economic intent."
        )
        user_prompt = (
            f"Region: {region}\n"
            "Task: Repair this expression so it is valid FASTEXPR.\n"
            "Rules:\n"
            "- Keep original signal logic as much as possible.\n"
            "- Do not introduce new variables or multi-line statements.\n"
            "- Use only known operators and known fields.\n"
            "- If it cannot be repaired, return exactly INVALID.\n\n"
            "Known operators:\n"
            + ", ".join(self.operator_names)
            + "\n\nKnown fields sample:\n"
            + ", ".join(sampled_fields)
            + "\n\nOriginal expression:\n"
            + expression
        )
        if syntax_hint:
            user_prompt += "\n\nSyntax manual excerpt:\n" + syntax_hint
        user_prompt += "\n\nOutput exactly one expression line or INVALID."

        fixed = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt, temperature=0.0).strip()
        fixed = fixed.splitlines()[0].strip() if fixed else ""
        fixed = fixed.strip().strip("`").strip()
        if fixed.upper() == "INVALID":
            return ""
        return fixed

    @staticmethod
    def _parse_expressions(raw: str) -> List[str]:
        if not raw:
            return []
        cleaned = raw.replace("```", "\n")
        lines = [line.strip() for line in cleaned.splitlines()]
        out: List[str] = []
        seen = set()
        for line in lines:
            if not line:
                continue
            line = re.sub(r"^\d+[\).]\s*", "", line)
            line = line.strip().strip("`")
            line = line.strip().rstrip(";")
            if not line:
                continue
            if not IterativeTemplateGenerator._looks_like_expression(line):
                continue
            if line not in seen:
                out.append(line)
                seen.add(line)
        return out

    @staticmethod
    def _looks_like_expression(line: str) -> bool:
        has_call = "(" in line and ")" in line
        has_math = any(sym in line for sym in ["+", "-", "*", "/", "?", ":", "^"])
        return has_call or has_math

    @staticmethod
    def _try_add_candidate(
        collected: Dict[str, TemplateCandidate],
        expression: str,
        source_prompt: str,
        fields_used: Iterable[str],
        operators_used: Iterable[str],
    ) -> int:
        if expression in collected:
            return 0
        collected[expression] = TemplateCandidate(
            expression=expression,
            source_prompt=source_prompt,
            fields_used=list(fields_used),
            operators_used=list(operators_used),
        )
        return 1

