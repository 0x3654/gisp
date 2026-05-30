"""Базовая абстракция провайдера."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Protocol


@dataclass
class LLMMessage:
    role: str  # "system" | "user" | "assistant"
    content: str


@dataclass
class LLMCompletion:
    text: str
    prompt_tokens: int | None = None
    completion_tokens: int | None = None


class LLMProvider(Protocol):
    name: str
    model: str

    def complete(self, messages: List[LLMMessage], *, max_tokens: int, temperature: float) -> LLMCompletion:
        ...
