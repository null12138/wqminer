import unittest

from wqminer.iterative_generator import IterativeTemplateGenerator
from wqminer.models import DataField


class FakeLLM:
    def generate(self, system_prompt: str, user_prompt: str, temperature=None) -> str:
        if "syntax checker" in system_prompt.lower():
            return "rank(ts_delta(close, 3))"
        return "\n".join(
            [
                "rank(ts_delta(close, 1))",
                "rank(bad_op({datafield}, 3))",
                "rank(ts_mean(volume, 20))",
            ]
        )


class IterativeGeneratorTest(unittest.TestCase):
    def test_generate_with_fix(self):
        operators = [
            {"name": "rank", "definition": "rank(x)"},
            {"name": "ts_delta", "definition": "ts_delta(x, d)"},
            {"name": "ts_mean", "definition": "ts_mean(x, d)"},
        ]
        fields = [DataField(field_id="close"), DataField(field_id="volume")]
        gen = IterativeTemplateGenerator(llm=FakeLLM(), operators=operators, seed=7)

        templates, report = gen.generate(
            region="USA",
            data_fields=fields,
            count=3,
            rounds=2,
            style_prompt="",
            syntax_guide="",
            max_fix_attempts=1,
        )

        self.assertEqual(len(templates), 3)
        exprs = {x.expression for x in templates}
        self.assertIn("rank(ts_delta(close, 3))", exprs)
        self.assertEqual(report["requested_count"], 3)
        self.assertGreaterEqual(report["final_count"], 3)


if __name__ == "__main__":
    unittest.main()
