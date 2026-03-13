"""Factor mining loop: mutate templates and simulate."""

import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Sequence

from .models import SimulationResult, SimulationSettings
from .mutator import ExpressionMutator
from .storage import append_jsonl, write_csv


@dataclass
class MiningConfig:
    rounds: int = 3
    variants_per_template: int = 8
    max_simulations: int = 200
    sharpe_threshold: float = 1.25
    fitness_threshold: float = 1.0
    dry_run: bool = False


class FactorMiner:
    def __init__(self, client, mutator: ExpressionMutator, output_dir: str = "results"):
        self.client = client
        self.mutator = mutator
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def mine(
        self,
        seed_expressions: Sequence[str],
        available_field_ids: Sequence[str],
        settings: SimulationSettings,
        config: MiningConfig,
    ) -> Dict:
        ts = time.strftime("%Y%m%d_%H%M%S")
        jsonl_path = self.output_dir / f"mine_{ts}.jsonl"
        csv_path = self.output_dir / f"mine_{ts}.csv"

        all_results: List[SimulationResult] = []
        seen = set()
        frontier = [expr.strip() for expr in seed_expressions if expr and expr.strip()]

        for round_id in range(1, config.rounds + 1):
            if not frontier:
                break

            next_frontier: List[str] = []

            for expression in frontier:
                variants = self.mutator.generate_variants(
                    expression=expression,
                    field_ids=available_field_ids,
                    variants=config.variants_per_template,
                )

                for candidate in variants:
                    if candidate in seen:
                        continue
                    seen.add(candidate)

                    if config.dry_run:
                        result = self._mock_result(candidate)
                    else:
                        result = self.client.simulate_expression(candidate, settings)

                    all_results.append(result)
                    append_jsonl(str(jsonl_path), [self._enrich_row(result, round_id)])

                    if result.success and result.sharpe >= config.sharpe_threshold and result.fitness >= config.fitness_threshold:
                        next_frontier.append(candidate)

                    if len(all_results) >= config.max_simulations:
                        break

                if len(all_results) >= config.max_simulations:
                    break

            frontier = self._unique(next_frontier)
            if len(all_results) >= config.max_simulations:
                break

        ranked = sorted(
            all_results,
            key=lambda x: (x.success, x.score()),
            reverse=True,
        )

        rows = [r.to_dict() for r in ranked]
        write_csv(str(csv_path), rows)

        return {
            "total_simulations": len(all_results),
            "success_count": sum(1 for r in all_results if r.success),
            "result_jsonl": str(jsonl_path),
            "result_csv": str(csv_path),
            "top_results": rows[:10],
        }

    @staticmethod
    def _enrich_row(result: SimulationResult, round_id: int) -> Dict:
        row = result.to_dict()
        row["round"] = round_id
        return row

    @staticmethod
    def _unique(items: Sequence[str]) -> List[str]:
        seen = set()
        out = []
        for item in items:
            if item not in seen:
                seen.add(item)
                out.append(item)
        return out

    @staticmethod
    def _mock_result(expression: str) -> SimulationResult:
        pseudo = float((len(expression) % 19) + 1) / 10.0
        return SimulationResult(
            expression=expression,
            alpha_id="dry_run",
            success=True,
            sharpe=1.0 + pseudo,
            fitness=0.8 + 0.5 * pseudo,
            turnover=20.0 + 30.0 * (pseudo / 2.0),
            returns=0.02 + 0.01 * pseudo,
            drawdown=0.01,
            margin=0.0005,
            passed_checks=4,
            total_checks=5,
            weight_check="PASS",
            sub_universe_sharpe=0.9,
            link="",
        )
