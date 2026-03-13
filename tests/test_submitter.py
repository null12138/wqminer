import unittest
from unittest.mock import patch

from wqminer.models import SimulationSettings
from wqminer.submitter import submit_expressions_concurrent


class FakeClient:
    def __init__(self, username: str, password: str, timeout_sec: int = 30):
        self.username = username
        self.password = password

    def authenticate(self):
        return None

    def simulate_expression(self, expression, settings, poll_interval_sec=5, max_wait_sec=240):
        from wqminer.models import SimulationResult

        return SimulationResult(
            expression=expression,
            alpha_id="A1",
            success=True,
            sharpe=0.5,
            fitness=0.2,
            turnover=10.0,
        )


class SubmitterTest(unittest.TestCase):
    @patch("wqminer.submitter.WorldQuantBrainClient", new=FakeClient)
    def test_submit_expressions_concurrent(self):
        settings = SimulationSettings(region="USA", universe="TOP3000", delay=1, neutralization="INDUSTRY")
        summary = submit_expressions_concurrent(
            expressions=["rank(close)", "rank(close)", "rank(volume)"],
            username="u",
            password="p",
            settings=settings,
            max_submissions=3,
            concurrency=3,
            max_wait_sec=30,
            poll_interval_sec=1,
            output_dir="results/test_submitter",
        )
        self.assertEqual(summary["requested_count"], 2)
        self.assertEqual(summary["success_count"], 2)


if __name__ == "__main__":
    unittest.main()
