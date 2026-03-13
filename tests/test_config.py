import unittest

from wqminer.config import normalize_llm_base_url


class ConfigTest(unittest.TestCase):
    def test_normalize_host_only(self):
        self.assertEqual(normalize_llm_base_url("ryccpa.zeabur.app"), "https://ryccpa.zeabur.app/v1")

    def test_normalize_with_path(self):
        self.assertEqual(
            normalize_llm_base_url("https://example.com/custom/v1/"),
            "https://example.com/custom/v1",
        )


if __name__ == "__main__":
    unittest.main()
