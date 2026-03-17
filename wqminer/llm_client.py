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
        use_responses = self._should_use_responses()
        url = f"{self.config.base_url}/responses" if use_responses else f"{self.config.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        reasoning_effort = (self.config.reasoning_effort or "").strip()
        allow_temperature = not reasoning_effort or reasoning_effort.lower() == "none"
        temp_value = self.config.temperature if temperature is None else temperature
        if use_responses:
            payload = {
                "model": self.config.model,
                "input": user_prompt,
                "instructions": system_prompt,
            }
            max_out = int(self.config.max_output_tokens or self.config.max_tokens)
            if max_out > 0:
                payload["max_output_tokens"] = max_out
            if reasoning_effort:
                payload["reasoning"] = {"effort": reasoning_effort}
            if (self.config.verbosity or "").strip():
                payload["text"] = {"verbosity": self.config.verbosity}
            if allow_temperature:
                payload["temperature"] = temp_value
        else:
            payload = {
                "model": self.config.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                "max_tokens": self.config.max_tokens,
            }
            if reasoning_effort:
                payload["reasoning_effort"] = reasoning_effort
            if (self.config.verbosity or "").strip():
                payload["verbosity"] = self.config.verbosity
            if allow_temperature:
                payload["temperature"] = temp_value
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
            if use_responses:
                content = self._extract_responses_text(data)
            else:
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

    def _should_use_responses(self) -> bool:
        if bool(self.config.use_responses):
            return True
        model = (self.config.model or "").lower()
        return "gpt-5" in model and "codex" in model

    @staticmethod
    def _extract_responses_text(payload: dict) -> str:
        direct = payload.get("output_text")
        if isinstance(direct, str) and direct.strip():
            return direct
        output = payload.get("output", [])
        if isinstance(output, list):
            chunks = []
            for item in output:
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "message":
                    content = item.get("content", [])
                    for part in content if isinstance(content, list) else []:
                        if not isinstance(part, dict):
                            continue
                        if part.get("type") == "output_text" and part.get("text"):
                            chunks.append(str(part.get("text")))
                elif item.get("type") == "output_text" and item.get("text"):
                    chunks.append(str(item.get("text")))
            if chunks:
                return "\n".join(chunks)
        return ""
