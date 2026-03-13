import unittest

from wqminer.models import DataField
from wqminer.template_generator import TemplateGenerator


class FakeLLM:
    def generate(self, system_prompt: str, user_prompt: str, temperature=None) -> str:
        return """
1. ts_rank(ts_delta(close, 1), 20)
2. rank(volume / ts_mean(volume, 20))
3. winsorize(ts_zscore(vwap - close, 30), std=3)
"""


class TemplateGeneratorTest(unittest.TestCase):
    def test_generate_templates(self):
        operators = [
            {"name": "ts_rank"},
            {"name": "ts_delta"},
            {"name": "rank"},
            {"name": "ts_mean"},
            {"name": "winsorize"},
            {"name": "ts_zscore"},
        ]
        fields = [
            DataField(field_id="close"),
            DataField(field_id="volume"),
            DataField(field_id="vwap"),
        ]

        generator = TemplateGenerator(llm=FakeLLM(), operators=operators)
        templates = generator.generate_templates(
            region="USA",
            data_fields=fields,
            count=3,
        )

        self.assertEqual(len(templates), 3)
        self.assertTrue(any("close" in t.expression for t in templates))


if __name__ == "__main__":
    unittest.main()
