"""Config loaders for credentials and LLM settings."""

import json
import os
from dataclasses import dataclass
from typing import Dict, Tuple
from urllib.parse import urlparse, urlunparse


@dataclass
class LLMConfig:
    api_key: str
    model: str = "gpt-4.1-mini"
    base_url: str = "https://api.openai.com/v1"
    temperature: float = 0.4
    max_tokens: int = 1200


def load_json_file(path: str) -> Dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def load_credentials(path: str) -> Tuple[str, str]:
    payload = load_json_file(path)
    if isinstance(payload, list) and len(payload) >= 2:
        return str(payload[0]), str(payload[1])
    if isinstance(payload, dict):
        if payload.get("username") and payload.get("password"):
            return str(payload["username"]), str(payload["password"])
        if payload.get("email") and payload.get("password"):
            return str(payload["email"]), str(payload["password"])
    raise ValueError("Unsupported credentials format. Use [username, password] or {username,password}.")


def load_llm_config(path: str = "") -> LLMConfig:
    payload = {}
    if path:
        payload = load_json_file(path)

    api_key = payload.get("api_key") or os.getenv("LLM_API_KEY", "")
    model = payload.get("model") or os.getenv("LLM_MODEL", "gpt-4.1-mini")
    base_url = payload.get("base_url") or os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    temperature = float(payload.get("temperature", os.getenv("LLM_TEMPERATURE", "0.4")))
    max_tokens = int(payload.get("max_tokens", os.getenv("LLM_MAX_TOKENS", "1200")))

    if not api_key:
        raise ValueError("Missing LLM API key. Set in llm config or LLM_API_KEY env.")

    return LLMConfig(
        api_key=api_key,
        model=model,
        base_url=normalize_llm_base_url(base_url),
        temperature=temperature,
        max_tokens=max_tokens,
    )


def normalize_llm_base_url(base_url: str) -> str:
    raw = (base_url or "").strip()
    if not raw:
        return "https://api.openai.com/v1"

    if "://" not in raw:
        raw = "https://" + raw

    parsed = urlparse(raw)
    scheme = parsed.scheme or "https"
    netloc = parsed.netloc
    path = parsed.path or ""

    if not netloc and parsed.path:
        # Handle malformed host-only inputs like "example.com".
        netloc = parsed.path
        path = ""

    path = path.rstrip("/")
    if not path:
        path = "/v1"

    normalized = urlunparse((scheme, netloc, path, "", "", ""))
    return normalized.rstrip("/")


def load_run_config(path: str) -> Dict:
    payload = load_json_file(path)
    if not isinstance(payload, dict):
        raise ValueError("Run config must be a JSON object")
    return payload
