"""OpenAI-compatible провайдер (vLLM / llama.cpp server / Ollama / прокси для Anthropic)."""
from __future__ import annotations

import json
import os
from typing import List

import requests
from fastapi import HTTPException

from .base import LLMCompletion, LLMMessage


class LocalProvider:
    name = "local"

    def __init__(self, *, model: str | None = None, base_url: str | None = None, api_key: str | None = None):
        self.model = model or os.getenv("REASONER_MODEL", "qwen3-32b")
        self.base_url = (base_url or os.getenv("REASONER_API_BASE", "")).rstrip("/")
        self.api_key = api_key or os.getenv("REASONER_API_KEY", "")
        self.timeout = float(os.getenv("REASONER_TIMEOUT", "60"))
        if not self.base_url:
            raise RuntimeError("REASONER_API_BASE is not set")

    def complete(self, messages: List[LLMMessage], *, max_tokens: int, temperature: float) -> LLMCompletion:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        body = {
            "model": self.model,
            "messages": [{"role": m.role, "content": m.content} for m in messages],
            "max_tokens": max_tokens,
            "temperature": temperature,
            "stream": False,
            "response_format": {"type": "json_object"},
        }
        try:
            resp = requests.post(
                f"{self.base_url}/chat/completions",
                headers=headers,
                data=json.dumps(body),
                timeout=self.timeout,
            )
        except requests.RequestException as exc:
            raise HTTPException(status_code=502, detail=f"LLM provider unavailable: {exc}")
        if resp.status_code >= 400:
            raise HTTPException(
                status_code=502,
                detail=f"LLM provider error {resp.status_code}: {resp.text[:500]}",
            )
        data = resp.json()
        try:
            text = data["choices"][0]["message"]["content"]
        except (KeyError, IndexError) as exc:
            raise HTTPException(
                status_code=502, detail=f"LLM provider malformed response: {exc}"
            )
        usage = data.get("usage") or {}
        return LLMCompletion(
            text=text,
            prompt_tokens=usage.get("prompt_tokens"),
            completion_tokens=usage.get("completion_tokens"),
        )
