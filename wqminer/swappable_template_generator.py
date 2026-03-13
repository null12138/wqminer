"""Generate swappable templates and expand them into concrete expressions."""

from __future__ import annotations

import json
import random
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from .fast_expr_syntax import FastExprSyntaxValidator
from .llm_client import OpenAICompatibleLLM
from .models import DataField, TemplateCandidate


PLACEHOLDER_RE = re.compile(r"\{([A-Za-z_][A-Za-z0-9_]*)\}")


@dataclass
class SwappableTemplate:
    template: str
    placeholders: List[str]
    source_prompt: str

    def to_dict(self) -> Dict:
        return {
            "template": self.template,
            "placeholders": self.placeholders,
            "source_prompt": self.source_prompt,
        }


class SwappableTemplateGenerator:
    def __init__(self, llm: OpenAICompatibleLLM, operators: Sequence[Dict], seed: int = 42):
        self.llm = llm
        self.operators = [op for op in operators if op.get("name")]
        self.operator_names = sorted({str(op.get("name", "")).strip() for op in self.operators if op.get("name")})
        self.random = random.Random(seed)

    def generate_swappable_templates(
        self,
        region: str,
        count: int,
        style_prompt: str = "",
        syntax_manual_excerpt: str = "",
        max_prompt_operators: int = 28,
        batch_size: int = 24,
        max_rounds: int = 10,
    ) -> List[SwappableTemplate]:
        out: List[SwappableTemplate] = []
        seen = set()

        rounds = 0
        while len(out) < count and rounds < max(1, max_rounds):
            rounds += 1
            need = min(max(1, batch_size), count - len(out))

            sampled = self.random.sample(self.operators, min(max_prompt_operators, len(self.operators)))
            operator_lines = []
            for op in sampled:
                name = str(op.get("name", "")).strip()
                definition = str(op.get("definition", "")).strip()
                if name:
                    operator_lines.append(f"- {name}: {definition}")

            manual = (syntax_manual_excerpt or "").strip()
            if len(manual) > 3500:
                manual = manual[:3500]

            system_prompt = (
                "You are a WorldQuant FASTEXPR template designer. "
                "Return only swappable expression templates."
            )
            user_prompt = (
                f"Region: {region}\n"
                f"Generate {need} swappable FASTEXPR templates.\n"
                "Rules:\n"
                "- Must output one template per line.\n"
                "- Use placeholders for fields and windows.\n"
                "- Allowed placeholders: {field_1},{field_2},{field_3},{group_1},{window_1},{window_2}.\n"
                "- Every template must include at least one {field_*} placeholder.\n"
                "- Output expression only. No explanation, no markdown, no assignment.\n"
                f"- Additional style: {style_prompt or 'none'}\n\n"
                "Operator list:\n"
                + "\n".join(operator_lines)
            )
            if manual:
                user_prompt += "\n\nSyntax manual excerpt:\n" + manual
            user_prompt += "\n\nReturn exactly one template expression per line."

            raw = self.llm.generate(system_prompt=system_prompt, user_prompt=user_prompt)
            lines = self._parse_lines(raw)

            for line in lines:
                placeholders = self._extract_placeholders(line)
                if not placeholders:
                    continue
                if not any(p.startswith("field_") for p in placeholders):
                    continue
                if line in seen:
                    continue
                seen.add(line)
                out.append(
                    SwappableTemplate(
                        template=line,
                        placeholders=placeholders,
                        source_prompt=style_prompt,
                    )
                )
                if len(out) >= count:
                    break
        return out

    def expand_templates(
        self,
        templates: Sequence[SwappableTemplate],
        data_fields: Sequence[DataField],
        max_expressions: int,
        fills_per_template: int = 12,
    ) -> Tuple[List[TemplateCandidate], Dict]:
        field_ids = [f.field_id for f in data_fields if f.field_id]
        validator = FastExprSyntaxValidator(operator_names=self.operator_names, field_ids=field_ids)
        if not field_ids:
            raise ValueError("No field ids available for template expansion")

        candidates: List[TemplateCandidate] = []
        seen_expr = set()

        total_attempts = 0
        valid_count = 0
        invalid_count = 0
        reject_examples: List[Dict] = []

        for tpl in templates:
            if len(candidates) >= max_expressions:
                break
            for _ in range(max(1, fills_per_template)):
                if len(candidates) >= max_expressions:
                    break

                total_attempts += 1
                expr = self._fill_one_template(tpl.template, field_ids)
                check = validator.validate(expr, require_known_field=True)
                if not check.is_valid:
                    invalid_count += 1
                    if len(reject_examples) < 30:
                        reject_examples.append(
                            {
                                "template": tpl.template,
                                "filled_expression": expr,
                                "issues": [x.to_dict() for x in check.issues],
                            }
                        )
                    continue

                norm = check.normalized_expression
                if norm in seen_expr:
                    continue
                seen_expr.add(norm)
                valid_count += 1
                candidates.append(
                    TemplateCandidate(
                        expression=norm,
                        source_prompt="swappable_fill",
                        fields_used=check.fields_used,
                        operators_used=check.operators_used,
                    )
                )

        report = {
            "template_count": len(templates),
            "fill_attempts": total_attempts,
            "valid_count": valid_count,
            "invalid_count": invalid_count,
            "output_count": len(candidates),
            "reject_examples": reject_examples,
        }
        return candidates, report

    @staticmethod
    def save_swappable_templates(path: str, templates: Sequence[SwappableTemplate], metadata: Dict = None) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "metadata": metadata or {},
            "templates": [x.to_dict() for x in templates],
        }
        target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    @staticmethod
    def _parse_lines(raw: str) -> List[str]:
        cleaned = (raw or "").replace("```", "\n")
        lines = [line.strip() for line in cleaned.splitlines() if line.strip()]
        out: List[str] = []
        seen = set()
        for line in lines:
            line = re.sub(r"^\d+[\).]\s*", "", line).strip()
            line = line.strip("`").strip().rstrip(";")
            if not line:
                continue
            if not ("(" in line and ")" in line):
                continue
            if line.lower().startswith(("here", "template", "explanation")):
                continue
            if line not in seen:
                out.append(line)
                seen.add(line)
        return out

    @staticmethod
    def _extract_placeholders(template: str) -> List[str]:
        found = []
        for m in PLACEHOLDER_RE.finditer(template):
            name = m.group(1)
            if name not in found:
                found.append(name)
        return found

    def _fill_one_template(self, template: str, field_ids: Sequence[str]) -> str:
        placeholders = self._extract_placeholders(template)
        if not placeholders:
            return template

        fields_sampled = self.random.sample(list(field_ids), min(6, len(field_ids)))
        if not fields_sampled:
            fields_sampled = list(field_ids)

        mapping: Dict[str, str] = {}
        used_fields = set()
        for name in placeholders:
            if name.startswith("field_"):
                # Prefer distinct fields per expression.
                candidates = [x for x in fields_sampled if x not in used_fields]
                if not candidates:
                    candidates = list(field_ids)
                picked = self.random.choice(candidates)
                mapping[name] = picked
                used_fields.add(picked)
            elif name.startswith("group_"):
                mapping[name] = self.random.choice(
                    [
                        "sector",
                        "industry",
                        "subindustry",
                        "market",
                        "bucket(rank(cap), range=\"0,1,0.1\")",
                    ]
                )
            elif name.startswith("window_"):
                mapping[name] = str(self.random.choice([3, 5, 10, 15, 20, 30, 60, 120, 252]))
            else:
                # Fallback: treat unknown placeholder as field.
                mapping[name] = self.random.choice(field_ids)

        expr = template
        for key, val in mapping.items():
            expr = expr.replace("{" + key + "}", val)
        return expr
