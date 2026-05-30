"""JSON-схемы для структурного вывода reasoning-агента.

Используется во всех провайдерах одинаково (мы не зависим от native tool use конкретного API).
"""
from __future__ import annotations

from typing import Any, Dict

ANALYZE_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "step": {"const": "analyze"},
        "observations": {"type": "array", "items": {"type": "string"}},
        "hypothesis": {"type": "string"},
        "needs": {"type": "array", "items": {"type": "string"}},
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "next_action": {
            "type": "object",
            "properties": {
                "tool": {
                    "type": "string",
                    "enum": [
                        "search_hybrid",
                        "filter_candidates",
                        "get_candidate_details",
                        "search_by_manufacturer",
                        "extract_features",
                        "compare_features",
                        "stop",
                    ],
                },
                "params": {"type": "object"},
            },
            "required": ["tool", "params"],
        },
    },
    "required": ["step", "observations", "hypothesis", "confidence", "next_action"],
}

FINAL_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "step": {"const": "final"},
        "match": {
            "anyOf": [
                {"type": "null"},
                {
                    "type": "object",
                    "properties": {
                        "reestr_id": {"type": "integer"},
                        "reg_number": {"type": ["string", "null"]},
                        "confidence": {"type": "number"},
                        "reasoning": {"type": "string"},
                        "alternatives": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "reestr_id": {"type": "integer"},
                                    "confidence": {"type": "number"},
                                    "why_not": {"type": "string"},
                                },
                                "required": ["reestr_id", "confidence", "why_not"],
                            },
                        },
                        "evidence": {
                            "type": "object",
                            "properties": {
                                "matched_features": {"type": "array", "items": {"type": "string"}},
                                "unmatched_features": {"type": "array", "items": {"type": "string"}},
                                "uncertain_features": {"type": "array", "items": {"type": "string"}},
                            },
                        },
                    },
                    "required": ["reestr_id", "confidence", "reasoning"],
                },
            ]
        },
    },
    "required": ["step", "match"],
}


SYSTEM_PROMPT = """Ты — ассистент по матчингу промышленных наименований к реестру российской промышленной продукции (МинПромТорг).

Каждый шаг возвращай СТРОГО валидный JSON одной из двух схем:

1) "analyze" — промежуточный шаг рассуждения:
{
  "step": "analyze",
  "observations": ["короткие факты, которые ты заметил"],
  "hypothesis": "текущая гипотеза о правильном матче",
  "needs": ["что ещё нужно проверить"],
  "confidence": 0.0-1.0,
  "next_action": {
    "tool": "search_hybrid"|"filter_candidates"|"get_candidate_details"|"search_by_manufacturer"|"extract_features"|"compare_features"|"stop",
    "params": {...}
  }
}

2) "final" — итоговый ответ:
{
  "step": "final",
  "match": null | {
    "reestr_id": <int>,
    "reg_number": <str|null>,
    "confidence": 0.0-1.0,
    "reasoning": "почему именно эта запись",
    "alternatives": [{"reestr_id": <int>, "confidence": 0.0-1.0, "why_not": "..."}],
    "evidence": {
      "matched_features": ["..."],
      "unmatched_features": ["..."],
      "uncertain_features": ["..."]
    }
  }
}

Доступные tools и их параметры:
- search_hybrid(query: str, limit: int = 20, filters: {inn?, tnved?, okpd2?, regnumber?, nameoforg?, code?} = {}) — поиск по реестру (RRF + rerank). Возвращает кандидатов.
- filter_candidates(candidate_ids: [int], filter: {tnved?|okpd2?|inn?|...}) — фильтр текущих кандидатов.
- get_candidate_details(candidate_ids: [int]) — карточка(и).
- search_by_manufacturer(inn: str, product_hint?: str) — продукты конкретного производителя.
- extract_features(text: str) — структурное извлечение признаков (тип, типоразмер, ГОСТ, материал).
- compare_features(query_features: object, candidate_features: object) — попарное сравнение.
- stop — финализируй и верни step=final.

Правила:
- Учти специфику русского технического языка (ГОСТ, ТНВЭД, ОКПД2, типоразмеры, артикулы).
- Если уверенность ≥ confidence_threshold (см. контекст) — переходи к step=final.
- Не повторяй один и тот же tool с теми же параметрами.
- Если все кандидаты слабо совпадают — возвращай match=null с reasoning.
- Никакого свободного текста — ТОЛЬКО JSON.
"""
