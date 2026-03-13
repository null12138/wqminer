import unittest

from wqminer.models import DataField
from wqminer.swappable_template_generator import SwappableTemplateGenerator


class FakeLLM:
    def generate(self, system_prompt: str, user_prompt: str, temperature=None) -> str:
        return "\n".join(
            [
                "rank(ts_delta({field_1}, {window_1}))",
                "group_neutralize(ts_zscore({field_1}, {window_1}), {group_1})",
                "rank(ts_corr({field_1}, {field_2}, {window_2}))",
            ]
        )


class SwappableTemplateGeneratorTest(unittest.TestCase):
    def test_generate_and_expand(self):
        operators = [
            {"name": "rank", "definition": "rank(x)"},
            {"name": "ts_delta", "definition": "ts_delta(x,d)"},
            {"name": "group_neutralize", "definition": "group_neutralize(x,g)"},
            {"name": "ts_zscore", "definition": "ts_zscore(x,d)"},
            {"name": "ts_corr", "definition": "ts_corr(x,y,d)"},
        ]
        fields = [
            DataField(field_id="close"),
            DataField(field_id="volume"),
            DataField(field_id="returns"),
            DataField(field_id="cap"),
        ]

        g = SwappableTemplateGenerator(llm=FakeLLM(), operators=operators, seed=1)
        templates = g.generate_swappable_templates(region="USA", count=3)
        self.assertEqual(len(templates), 3)

        expanded, report = g.expand_templates(
            templates=templates,
            data_fields=fields,
            max_expressions=12,
            fills_per_template=5,
        )
        self.assertGreater(len(expanded), 0)
        self.assertGreater(report["valid_count"], 0)


if __name__ == "__main__":
    unittest.main()
