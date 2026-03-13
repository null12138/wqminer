"""Expression mutation logic for factor mining."""

import random
import re
from typing import Dict, Iterable, List, Sequence, Set


class ExpressionMutator:
    def __init__(self, operators: Sequence[Dict], seed: int = 42):
        self.random = random.Random(seed)
        self.operator_names = [str(op.get("name", "")).strip() for op in operators if op.get("name")]
        self.time_series_ops = [name for name in self.operator_names if name.startswith("ts_")]
        self.rank_like_ops = [name for name in self.operator_names if "rank" in name]

    def generate_variants(
        self,
        expression: str,
        field_ids: Sequence[str],
        variants: int,
    ) -> List[str]:
        pool: Set[str] = {expression}

        while len(pool) < variants + 1:
            choice = self.random.choice(["number", "field", "operator", "wrap"])
            mutated = expression

            if choice == "number":
                mutated = self.mutate_numbers(expression)
            elif choice == "field":
                mutated = self.mutate_field(expression, field_ids)
            elif choice == "operator":
                mutated = self.mutate_operator(expression)
            elif choice == "wrap":
                mutated = self.wrap_expression(expression)

            if mutated and mutated != expression:
                pool.add(mutated)

            if len(pool) > variants * 4:
                break

        ordered = list(pool)
        return ordered[: variants + 1]

    def mutate_numbers(self, expression: str) -> str:
        matches = list(re.finditer(r"\b\d+\b", expression))
        if not matches:
            return expression
        pick = self.random.choice(matches)
        value = int(pick.group(0))
        step = max(1, int(value * 0.2))
        delta = self.random.randint(-step, step)
        new_value = max(1, value + delta)
        return expression[: pick.start()] + str(new_value) + expression[pick.end() :]

    def mutate_field(self, expression: str, field_ids: Sequence[str]) -> str:
        if len(field_ids) < 2:
            return expression

        present = [f for f in field_ids if re.search(rf"(?<![A-Za-z0-9_]){re.escape(f)}(?![A-Za-z0-9_])", expression)]
        if not present:
            return expression

        old_field = self.random.choice(present)
        candidates = [f for f in field_ids if f != old_field]
        if not candidates:
            return expression
        new_field = self.random.choice(candidates)
        return re.sub(
            rf"(?<![A-Za-z0-9_]){re.escape(old_field)}(?![A-Za-z0-9_])",
            new_field,
            expression,
            count=1,
        )

    def mutate_operator(self, expression: str) -> str:
        calls = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)
        calls = [name for name in calls if name in self.operator_names]
        if not calls:
            return expression

        target = self.random.choice(calls)

        if target.startswith("ts_") and self.time_series_ops:
            replacement_pool = self.time_series_ops
        elif "rank" in target and self.rank_like_ops:
            replacement_pool = self.rank_like_ops
        else:
            replacement_pool = self.operator_names

        replacement_pool = [name for name in replacement_pool if name != target]
        if not replacement_pool:
            return expression

        replacement = self.random.choice(replacement_pool)
        return re.sub(rf"\b{re.escape(target)}\s*\(", f"{replacement}(", expression, count=1)

    def wrap_expression(self, expression: str) -> str:
        wrappers = []
        if "rank" in self.operator_names:
            wrappers.append("rank")
        if "ts_zscore" in self.operator_names:
            wrappers.append("ts_zscore")
        if "winsorize" in self.operator_names:
            wrappers.append("winsorize")
        if not wrappers:
            return expression

        wrapper = self.random.choice(wrappers)
        if wrapper == "ts_zscore":
            lookback = self.random.choice([20, 30, 60, 120])
            return f"ts_zscore({expression}, {lookback})"
        if wrapper == "winsorize":
            std = self.random.choice([2, 3, 4])
            return f"winsorize({expression}, std={std})"
        return f"{wrapper}({expression})"
