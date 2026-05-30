"""Anthropic Claude провайдер (эскалация L2)."""
from __future__ import annotations

import os
from typing import List

from fastapi import HTTPException

from .base import LLMCompletion, LLMMessage


class AnthropicProvider:
    name = "anthropic"

    def __init__(self, *, model: str | None = None, api_key: str | None = None, base_url: str | None = None):
        # Поддерживаем 2 режима: дефолтный (REASONER_MODEL/REASONER_API_KEY) и эскалация.
        # Для эскалации читаем REASONER_ESCALATION_*; иначе — REASONER_*.
        self.model = (
            model
            or os.getenv("REASONER_MODEL", "")
            or os.getenv("REASONER_ESCALATION_MODEL", "claude-sonnet-4-6")
        )
        self.api_key = (
            api_key
            or os.getenv("REASONER_API_KEY", "")
            or os.getenv("REASONER_ESCALATION_API_KEY", "")
            or os.getenv("ANTHROPIC_API_KEY", "")
            or os.getenv("ANTHROPIC_AUTH_TOKEN", "")
        )
        self.base_url = (
            base_url
            or os.getenv("ANTHROPIC_BASE_URL", "")
            or None
        )
        self.timeout = float(os.getenv("REASONER_TIMEOUT", "60"))
        if not self.api_key:
            raise RuntimeError(
                "Anthropic credentials not set (REASONER_API_KEY / REASONER_ESCALATION_API_KEY / ANTHROPIC_API_KEY / ANTHROPIC_AUTH_TOKEN)"
            )
        try:
            from anthropic import Anthropic  # noqa: F401
        except ImportError as exc:
            raise RuntimeError(
                "anthropic SDK is required for the AnthropicProvider; "
                "install via 'pip install anthropic' in the matcher image"
            ) from exc

    def complete(self, messages: List[LLMMessage], *, max_tokens: int, temperature: float) -> LLMCompletion:
        from anthropic import Anthropic, APIError

        client_kwargs: dict = {"api_key": self.api_key}
        if self.base_url:
            client_kwargs["base_url"] = self.base_url
        client = Anthropic(**client_kwargs)
        system = "\n\n".join(m.content for m in messages if m.role == "system")
        chat: list[dict[str, str]] = [
            {"role": m.role, "content": m.content}
            for m in messages
            if m.role in ("user", "assistant")
        ]
        try:
            resp = client.messages.create(
                model=self.model,
                max_tokens=max_tokens,
                temperature=temperature,
                system=system or None,
                messages=chat,
                timeout=self.timeout,
            )
        except APIError as exc:
            raise HTTPException(status_code=502, detail=f"Anthropic error: {exc}")
        # Складываем все text-блоки в одну строку
        text_parts = [block.text for block in resp.content if getattr(block, "type", "") == "text"]
        usage = getattr(resp, "usage", None)
        return LLMCompletion(
            text="".join(text_parts),
            prompt_tokens=getattr(usage, "input_tokens", None),
            completion_tokens=getattr(usage, "output_tokens", None),
        )
