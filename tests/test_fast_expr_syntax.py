import unittest

from wqminer.fast_expr_syntax import FastExprSyntaxValidator


class FastExprSyntaxTest(unittest.TestCase):
    def test_valid_expression(self):
        v = FastExprSyntaxValidator(
            operator_names=["rank", "ts_delta", "ts_mean", "group_neutralize"],
            field_ids=["close", "volume", "cap"],
        )
        result = v.validate("rank(ts_delta(close, 5) / ts_mean(volume, 20))")
        self.assertTrue(result.is_valid)
        self.assertIn("rank", result.operators_used)
        self.assertIn("close", result.fields_used)

    def test_placeholder_and_unknown_operator(self):
        v = FastExprSyntaxValidator(
            operator_names=["rank", "ts_delta"],
            field_ids=["close"],
        )
        result = v.validate("rank(foo_op({datafield}, 5))")
        self.assertFalse(result.is_valid)
        issue_codes = {x.code for x in result.issues}
        self.assertIn("placeholder", issue_codes)
        self.assertIn("unknown_operator", issue_codes)

    def test_assignment_normalization(self):
        v = FastExprSyntaxValidator(
            operator_names=["rank", "ts_delta"],
            field_ids=["close"],
        )
        result = v.validate("alpha = rank(ts_delta(close, 1));")
        self.assertTrue(result.is_valid)
        self.assertEqual(result.normalized_expression, "rank(ts_delta(close, 1))")


if __name__ == "__main__":
    unittest.main()
