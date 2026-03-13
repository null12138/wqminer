import unittest

from wqminer.mutator import ExpressionMutator
from wqminer.operator_store import load_operators


class MutatorTest(unittest.TestCase):
    def test_generate_variants(self):
        operators = load_operators()
        mutator = ExpressionMutator(operators)
        expr = "ts_rank(ts_delta(close, 1), 20)"
        fields = ["close", "open", "volume", "vwap"]
        variants = mutator.generate_variants(expr, fields, variants=5)
        self.assertGreaterEqual(len(variants), 2)
        self.assertIn(expr, variants)


if __name__ == "__main__":
    unittest.main()
