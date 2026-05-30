"""Reasoning agent loop.

Архитектура (упрощённая):
  1. Делаем seed-call: /reestr/hybrid?rerank=true → top-K кандидатов.
  2. Если rerank score топ-1 >= confidence_threshold и gap > 0.2 → step=final (direct).
  3. Иначе входим в LLM-loop с tools:
     - На каждой итерации провайдер возвращает analyze или final JSON.
     - Tool dispatch, результат идёт обратно как user message.
     - Лимиты: max_iterations, max_tool_calls, token budget.
     - При неуверенности эскалация: L1 → L2 (Anthropic).
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, Iterator, List, Optional

from fastapi import HTTPException

from providers import AnthropicProvider, LocalProvider
from providers.base import LLMMessage, LLMProvider
from schemas import SYSTEM_PROMPT
from tools import dispatch_tool, search_hybrid

logger = logging.getLogger("uvicorn.error")


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.getenv(k, "").strip() or d)
    except ValueError:
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.getenv(k, "").strip() or d)
    except ValueError:
        return d


MAX_ITER = _env_int("REASONER_MAX_ITERATIONS", 5)
MAX_TOOL_CALLS = _env_int("REASONER_MAX_TOOL_CALLS", 10)
CONFIDENCE_THRESHOLD = _env_float("REASONER_CONFIDENCE_THRESHOLD", 0.7)
TOKEN_BUDGET = _env_int("REASONER_TOKEN_BUDGET", 32000)


@dataclass
class AgentEvent:
    type: str  # tool_call | tool_result | observation | hypothesis | final | error | meta
    payload: Dict[str, Any]


@dataclass
class AgentState:
    query: str
    context: Dict[str, Any]
    messages: List[LLMMessage] = field(default_factory=list)
    tool_calls: int = 0
    tokens_used: int = 0
    last_action_key: str = ""
    confidence_threshold: float = CONFIDENCE_THRESHOLD


def _make_provider(escalation: bool = False) -> LLMProvider:
    if escalation:
        return AnthropicProvider()
    provider_name = (os.getenv("REASONER_PROVIDER", "local") or "local").lower()
    if provider_name in ("anthropic", "claude"):
        return AnthropicProvider()
    return LocalProvider()


def _parse_json(text: str) -> Dict[str, Any]:
    text = (text or "").strip()
    # Снимаем markdown-обёртки на всякий случай
    if text.startswith("```"):
        text = text.strip("`")
        # уберём префикс "json\n"
        if text.lower().startswith("json"):
            text = text[4:].lstrip("\n")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        # Попробуем выделить первый JSON-объект простым поиском фигурных скобок
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            try:
                return json.loads(text[start : end + 1])
            except json.JSONDecodeError:
                pass
        raise


def _direct_decision(rerank_payload: Dict[str, Any], threshold: float) -> Dict[str, Any] | None:
    cands = rerank_payload.get("candidates") or []
    if not cands:
        return None
    top = cands[0]
    second_score = (cands[1] or {}).get("rerank_score") if len(cands) > 1 else None
    top_score = top.get("rerank_score")
    if top_score is None:
        return None
    if top_score < threshold:
        return None
    gap = (top_score - (second_score if second_score is not None else top_score - 1.0))
    if gap < 0.2:
        return None
    return {
        "step": "final",
        "match": {
            "reestr_id": top.get("id"),
            "reg_number": top.get("regnumber") or top.get("registernumber"),
            "confidence": float(top_score),
            "reasoning": "direct match by rerank score (top >= threshold, gap > 0.2)",
            "alternatives": [
                {
                    "reestr_id": c.get("id"),
                    "confidence": float(c.get("rerank_score") or 0.0),
                    "why_not": "lower rerank score",
                }
                for c in cands[1:4]
            ],
            "evidence": {
                "matched_features": ["rerank"],
                "unmatched_features": [],
                "uncertain_features": [],
            },
        },
    }


def _filters_from_context(ctx: Dict[str, Any]) -> Dict[str, Any]:
    """Извлекает прямые фильтры из контекста 1С (tnved, okpd2, inn).
    Поддерживает как новые ключи (tnved, okpd2, manufacturer_inn / inn),
    так и legacy `context.filters` подсловарь.
    """
    out: Dict[str, Any] = {}
    if not ctx:
        return out
    if ctx.get("tnved"):
        out["tnved"] = ctx["tnved"]
    if ctx.get("okpd2"):
        out["okpd2"] = ctx["okpd2"]
    # ИНН производителя/поставщика
    inn = ctx.get("manufacturer_inn") or ctx.get("inn")
    if inn:
        out["inn"] = inn
    # legacy/нестандартный sub-dict
    out.update({k: v for k, v in (ctx.get("filters") or {}).items() if v is not None})
    return out


def run_agent(*, query: str, context: Dict[str, Any]) -> Iterator[AgentEvent]:
    """Главный generator, эмиттит AgentEvent. Endpoint оборачивает их в SSE."""
    state = AgentState(query=query, context=context or {})
    filters = _filters_from_context(state.context)

    # 0) Seed-call: hybrid + rerank, с фильтрами из 1С контекста (tnved/okpd2/inn)
    yield AgentEvent(
        "tool_call",
        {"tool": "search_hybrid", "params": {"query": query, "limit": 20, "rerank": True, "filters": filters}},
    )
    t0 = time.perf_counter()
    try:
        seed = search_hybrid(query=query, limit=20, filters=filters, rerank=True)
    except HTTPException as exc:
        # soft-fail: при наличии фильтров иногда возвращает 0 кандидатов — пробуем без них
        if filters:
            try:
                seed = search_hybrid(query=query, limit=20, filters={}, rerank=True)
                yield AgentEvent(
                    "observation",
                    {"text": f"фильтры {filters} не дали кандидатов, повтор без фильтров"},
                )
            except HTTPException as exc2:
                yield AgentEvent("error", {"message": str(exc2.detail), "where": "seed search_hybrid (retry)"})
                return
        else:
            yield AgentEvent("error", {"message": str(exc.detail), "where": "seed search_hybrid"})
            return
    seed_elapsed = (time.perf_counter() - t0) * 1000
    top_candidates: List[Dict[str, Any]] = (seed.get("candidates") or [])[:10]
    yield AgentEvent(
        "tool_result",
        {
            "tool": "search_hybrid",
            "elapsed_ms": round(seed_elapsed, 1),
            "candidates_count": len(top_candidates),
            "top_5_ids": [c.get("id") for c in top_candidates[:5]],
        },
    )

    # 1) Можно ли решить сразу?
    direct = _direct_decision(seed, state.confidence_threshold)
    if direct is not None:
        direct["top_candidates"] = top_candidates
        yield AgentEvent("hypothesis", {"text": "direct match by rerank"})
        yield AgentEvent("final", direct)
        return

    # 2) Agent loop
    system = LLMMessage(role="system", content=SYSTEM_PROMPT)
    # явные подсказки от 1С — выносим из context, чтобы LLM не угадывал по знаниям мира
    context_for_llm: Dict[str, Any] = {}
    for k in ("tnved", "okpd2", "manufacturer_inn", "inn", "manufacturer_name", "article", "category"):
        if state.context.get(k):
            context_for_llm[k] = state.context[k]
    initial_user = LLMMessage(
        role="user",
        content=json.dumps(
            {
                "query": query,
                "context_from_1c": context_for_llm,
                "confidence_threshold": state.confidence_threshold,
                "initial_candidates": top_candidates,
            },
            ensure_ascii=False,
        ),
    )
    state.messages = [system, initial_user]

    try:
        provider: LLMProvider = _make_provider(escalation=False)
    except Exception as exc:  # noqa: BLE001
        yield AgentEvent("error", {"message": f"provider init failed: {exc}"})
        return
    yield AgentEvent("meta", {"provider": provider.name, "model": provider.model})

    last_confidence = 0.0
    escalated = False

    # Tentative-state: каждый раз когда LLM в next_action фокусируется на конкретном
    # candidate_id (через get_candidate_details / compare_features / filter_candidates),
    # запоминаем его как «текущий best». На forced stop / max_iterations / loop —
    # эмиттим как match (если уверенность достаточна), а не возвращаем null.
    tentative_id: Optional[int] = None
    tentative_conf: float = 0.0
    tentative_reasoning: str = ""

    def _candidate_by_id(cid: int) -> Optional[Dict[str, Any]]:
        for c in top_candidates:
            try:
                if int(c.get("id") or 0) == int(cid):
                    return c
            except Exception:
                pass
        return None

    def _best_effort_final(reason_prefix: str) -> Dict[str, Any]:
        """Собирает best-effort final из tentative_id / top-1 rerank."""
        if tentative_id and tentative_conf >= 0.5:
            cand = _candidate_by_id(tentative_id) or {}
            return {
                "step": "final",
                "match": {
                    "reestr_id": tentative_id,
                    "reg_number": cand.get("regnumber") or cand.get("registernumber"),
                    "confidence": round(tentative_conf, 3),
                    "reasoning": f"{reason_prefix}: {tentative_reasoning or 'using last focused candidate'}",
                    "alternatives": [],
                    "evidence": {
                        "matched_features": [],
                        "unmatched_features": [],
                        "uncertain_features": ["best-effort: цикл прерван до окончательного решения"],
                    },
                },
                "top_candidates": top_candidates,
            }
        # fallback: top-1 от rerank, если он сильный
        if top_candidates:
            top = top_candidates[0]
            score = float(top.get("rerank_score") or 0.0)
            if score >= 0.8:
                return {
                    "step": "final",
                    "match": {
                        "reestr_id": top.get("id"),
                        "reg_number": top.get("regnumber") or top.get("registernumber"),
                        "confidence": round(score, 3),
                        "reasoning": f"{reason_prefix}: top-1 от reranker (rerank_score={score:.3f})",
                        "alternatives": [],
                        "evidence": {
                            "matched_features": ["rerank"],
                            "unmatched_features": [],
                            "uncertain_features": [],
                        },
                    },
                    "top_candidates": top_candidates,
                }
        return {
            "step": "final",
            "match": None,
            "reasoning": f"{reason_prefix}; нет ни tentative-кандидата с conf>=0.5, ни сильного rerank top-1",
            "top_candidates": top_candidates,
        }

    for iteration in range(1, MAX_ITER + 1):
        try:
            completion = provider.complete(state.messages, max_tokens=1500, temperature=0.1)
        except HTTPException as exc:
            yield AgentEvent("error", {"message": str(exc.detail), "iteration": iteration})
            return
        state.tokens_used += (completion.completion_tokens or 0) + (completion.prompt_tokens or 0)
        if state.tokens_used > TOKEN_BUDGET:
            yield AgentEvent("error", {"message": "token budget exceeded", "tokens": state.tokens_used})
            return

        try:
            decision = _parse_json(completion.text)
        except json.JSONDecodeError as exc:
            yield AgentEvent("error", {"message": f"LLM returned non-JSON: {exc}", "raw": completion.text[:500]})
            # дадим ещё один шанс с retry-промптом
            state.messages.append(LLMMessage(role="assistant", content=completion.text))
            state.messages.append(
                LLMMessage(
                    role="user",
                    content="Ответ невалидный JSON. Верни СТРОГО один JSON-объект по схеме analyze или final.",
                )
            )
            continue

        step = (decision.get("step") or "").strip()
        if step == "final":
            decision["top_candidates"] = top_candidates
            yield AgentEvent("final", decision)
            return
        if step != "analyze":
            yield AgentEvent("error", {"message": f"unexpected step={step!r}", "raw": decision})
            return

        for obs in decision.get("observations") or []:
            yield AgentEvent("observation", {"text": str(obs)})
        if decision.get("hypothesis"):
            yield AgentEvent("hypothesis", {"text": str(decision["hypothesis"])})
        conf = float(decision.get("confidence") or 0.0)

        # Progress check: уверенность не растёт и иссяк бюджет действий — эскалируем или останавливаемся
        confidence_growing = conf > last_confidence
        last_confidence = conf

        action = decision.get("next_action") or {}
        tool_name = (action.get("tool") or "").strip()
        params = action.get("params") or {}
        action_key = f"{tool_name}|{json.dumps(params, sort_keys=True)}"

        # Запомним «текущий best» если LLM сфокусировался на конкретном candidate_id
        if tool_name in ("get_candidate_details", "compare_features", "filter_candidates"):
            ids = params.get("candidate_ids") or []
            if not ids and "candidate_id" in params:
                ids = [params["candidate_id"]]
            if ids:
                try:
                    tentative_id = int(ids[0])
                    if conf > 0:
                        tentative_conf = conf
                    if decision.get("hypothesis"):
                        tentative_reasoning = str(decision["hypothesis"])
                except (TypeError, ValueError):
                    pass

        if tool_name == "stop" or state.tool_calls >= MAX_TOOL_CALLS:
            reason = "forced stop" if tool_name == "stop" else f"max_tool_calls reached ({MAX_TOOL_CALLS})"
            yield AgentEvent("hypothesis", {"text": f"{reason}; emitting best-effort final"})
            yield AgentEvent("final", _best_effort_final(reason))
            return

        if action_key == state.last_action_key:
            # Зацикленность: эскалация на L2 если ещё не делали
            if not escalated:
                try:
                    provider = _make_provider(escalation=True)
                    escalated = True
                    yield AgentEvent("meta", {"escalated_to": provider.name, "model": provider.model})
                    continue
                except Exception as exc:  # noqa: BLE001
                    yield AgentEvent("error", {"message": f"escalation failed: {exc}"})
                    yield AgentEvent("final", _best_effort_final("loop detected, escalation failed"))
                    return
            else:
                yield AgentEvent(
                    "hypothesis",
                    {"text": "loop detected after escalation; emitting best-effort final"},
                )
                yield AgentEvent("final", _best_effort_final("loop after escalation"))
                return
        state.last_action_key = action_key

        # Tool dispatch
        yield AgentEvent("tool_call", {"tool": tool_name, "params": params})
        t0 = time.perf_counter()
        try:
            tool_result = dispatch_tool(tool_name, params)
        except HTTPException as exc:
            yield AgentEvent("error", {"message": str(exc.detail), "tool": tool_name})
            return
        elapsed = (time.perf_counter() - t0) * 1000
        state.tool_calls += 1

        summary: Dict[str, Any] = {"tool": tool_name, "elapsed_ms": round(elapsed, 1)}
        if "candidates" in tool_result:
            summary["candidates_count"] = len(tool_result["candidates"])
            summary["top_5_ids"] = [c.get("id") for c in tool_result["candidates"][:5]]
        yield AgentEvent("tool_result", summary)

        # Передаём результат обратно в диалог
        state.messages.append(LLMMessage(role="assistant", content=json.dumps(decision, ensure_ascii=False)))
        state.messages.append(
            LLMMessage(
                role="user",
                content=json.dumps({"tool_result": tool_result}, ensure_ascii=False),
            )
        )

        # При не растущей уверенности после нескольких итераций — эскалация
        if iteration >= 2 and not confidence_growing and conf < state.confidence_threshold and not escalated:
            try:
                provider = _make_provider(escalation=True)
                escalated = True
                yield AgentEvent("meta", {"escalated_to": provider.name, "model": provider.model})
            except Exception as exc:  # noqa: BLE001
                yield AgentEvent("error", {"message": f"escalation init failed: {exc}"})

    yield AgentEvent("hypothesis", {"text": "max_iterations exhausted; emitting best-effort final"})
    yield AgentEvent("final", _best_effort_final(f"max_iterations exhausted ({MAX_ITER})"))
