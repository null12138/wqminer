import unittest
from unittest.mock import Mock, patch

from wqminer.config import LLMConfig
from wqminer.llm_client import OpenAICompatibleLLM


class LLMClientTest(unittest.TestCase):
    @patch("wqminer.llm_client.requests.post")
    def test_generate_success(self, mock_post):
        response = Mock()
        response.status_code = 200
        response.json.return_value = {"choices": [{"message": {"content": "rank(close)"}}]}
        response.raise_for_status.return_value = None
        mock_post.return_value = response

        llm = OpenAICompatibleLLM(
            LLMConfig(api_key="k", base_url="https://example.com/v1", model="m"),
            timeout_sec=3,
        )
        out = llm.generate(system_prompt="s", user_prompt="u")
        self.assertEqual(out, "rank(close)")

    @patch("wqminer.llm_client.requests.post")
    def test_generate_retry_on_429(self, mock_post):
        response_429 = Mock()
        response_429.status_code = 429
        response_429.headers = {}

        response_ok = Mock()
        response_ok.status_code = 200
        response_ok.json.return_value = {"choices": [{"message": {"content": "rank(close)"}}]}
        response_ok.raise_for_status.return_value = None

        mock_post.side_effect = [response_429, response_ok]

        llm = OpenAICompatibleLLM(
            LLMConfig(api_key="k", base_url="https://example.com/v1", model="m"),
            timeout_sec=3,
            max_retries=2,
        )
        out = llm.generate(system_prompt="s", user_prompt="u")
        self.assertEqual(out, "rank(close)")
        self.assertEqual(mock_post.call_count, 2)


if __name__ == "__main__":
    unittest.main()
