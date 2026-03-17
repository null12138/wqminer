"""OpenAI-compatible LLM client."""

import os
import random
import time
from typing import Optional

import requests

from .config import LLMConfig


class OpenAICompatibleLLM:
    def __init__(self, config: LLMConfig, timeout_sec: int = 90, max_retries: int = 4):
        self.config = config
        self.timeout_sec = timeout_sec
        self.max_retries = max(1, max_retries)
        self.sess = requests.Session()
        if "Authorization" not in self.sess.headers:
            self.sess.headers.update({"Content-Type": "application/json"})
        if (self.config.base_url or "").strip().lower().startswith("https://") and (
            (os.getenv("WQMINER_LLM_DISABLE_KEEPALIVE", "").strip().lower() in {"1", "true", "yes"})
        ):
            self.sess.headers.update({"Connection": "close"})

    def generate(self, system_prompt: str, user_prompt: str, temperature: Optional[float] = None) -> str:
        url = f"{self.config.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.config.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": self.config.temperature if temperature is None else temperature,
            "max_tokens": self.config.max_tokens,
        }
        last_error = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.sess.post(url, headers=headers, json=payload, timeout=self.timeout_sec)
            except requests.RequestException as exc:
                last_error = exc
                if attempt < self.max_retries:
                    backoff = min(12, 2 ** (attempt - 1))
                    time.sleep(backoff + random.uniform(0.0, 0.6))
                    continue
                raise

            if response.status_code in (429, 500, 502, 503, 504) and attempt < self.max_retries:
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    sleep_sec = max(1, int(retry_after))
                else:
                    sleep_sec = min(12, 2 ** (attempt - 1))
                time.sleep(sleep_sec + random.uniform(0.0, 0.6))
                continue

            response.raise_for_status()
            data = response.json()
            choices = data.get("choices", [])
            if not choices:
                raise RuntimeError("LLM returned no choices")
            content = choices[0].get("message", {}).get("content", "")
            if not content:
                raise RuntimeError("LLM returned empty content")
            return content

        if last_error:
            raise RuntimeError(f"LLM request failed after retries: {last_error}")
        raise RuntimeError("LLM request failed after retries")
