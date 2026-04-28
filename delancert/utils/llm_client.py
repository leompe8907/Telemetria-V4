from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional

import requests


@dataclass(frozen=True)
class LlmConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 20


def _env(name: str) -> Optional[str]:
    v = (os.getenv(name) or "").strip()
    return v or None


def get_llm_config() -> Optional[LlmConfig]:
    """
    Configura un cliente OpenAI-compatible.

    Env:
    - LLM_BASE_URL (ej: https://api.openai.com/v1)
    - LLM_API_KEY
    - LLM_MODEL (ej: gpt-4.1-mini)
    """
    base_url = _env("LLM_BASE_URL")
    api_key = _env("LLM_API_KEY")
    model = _env("LLM_MODEL")
    if not base_url or not api_key or not model:
        return None
    return LlmConfig(base_url=base_url.rstrip("/"), api_key=api_key, model=model)


def generate_text(*, system: str, user: str, max_tokens: int = 350) -> str:
    """
    Llamada mínima a /chat/completions (OpenAI-compatible).
    Si no hay config, levanta RuntimeError.
    """
    cfg = get_llm_config()
    if not cfg:
        raise RuntimeError("LLM no configurado (LLM_BASE_URL/LLM_API_KEY/LLM_MODEL).")

    url = f"{cfg.base_url}/chat/completions"
    headers = {"Authorization": f"Bearer {cfg.api_key}", "Content-Type": "application/json"}
    payload: dict[str, Any] = {
        "model": cfg.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0.2,
        "max_tokens": int(max_tokens),
    }
    r = requests.post(url, headers=headers, json=payload, timeout=cfg.timeout_seconds)
    r.raise_for_status()
    data = r.json()
    try:
        return (data["choices"][0]["message"]["content"] or "").strip()
    except Exception:
        return ""

