import tempfile
import unittest
from pathlib import Path

from wqminer.community_scraper import CommunityTemplateScraper


class ScraperTest(unittest.TestCase):
    def setUp(self):
        self.scraper = CommunityTemplateScraper(["ts_rank", "ts_delta", "rank", "ts_mean", "winsorize"])

    def test_extract_templates_from_text(self):
        text = """
        1. ts_rank(ts_delta(close, 1), 20)
        noise line
        alpha = rank(volume / ts_mean(volume, 20));
        """
        templates = self.scraper.extract_templates_from_text(text)
        self.assertIn("ts_rank(ts_delta(close, 1), 20)", templates)
        self.assertIn("rank(volume / ts_mean(volume, 20))", templates)

    def test_extract_templates_from_python_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sample.py"
            path.write_text("X = ['ts_rank(ts_delta(close, 1), 20)']\n", encoding="utf-8")
            templates = self.scraper.extract_templates_from_file(str(path))
            self.assertIn("ts_rank(ts_delta(close, 1), 20)", templates)


if __name__ == "__main__":
    unittest.main()
