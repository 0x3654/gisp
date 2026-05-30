"""Общие хелперы для поисковых эндпоинтов API.

Используется существующим `/reestr/semantic` и (далее) `/reestr/hybrid`.
Логику эндпоинтов сюда не переносим — здесь только чистые утилиты.
"""
from __future__ import annotations

import logging
import os
import re
from datetime import date
from decimal import Decimal
from re import split as re_split
from typing import Dict, List, Tuple

import requests
from fastapi import HTTPException
from psycopg2 import connect

logger = logging.getLogger("uvicorn.error")

SEMANTIC_URL = os.getenv("SEMANTIC_URL", "http://semantic:8010/semantic_normalize")


def get_conn():
    return connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        options="-c client_encoding=UTF8",
    )


def serialize_dates(rows):
    """Приводит значения типа date к ISO-строкам. Мутирует список словарей."""
    for row in rows:
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = value.isoformat()
    return rows


def coerce_decimals(rows):
    """Заменяет Decimal на float (для совместимости с JSON)."""
    for row in rows:
        for key, value in row.items():
            if isinstance(value, Decimal):
                row[key] = float(value)
    return rows


def normalize_regnumber(val: str | None) -> str | None:
    if not val:
        return None
    v = val.strip().strip('"').strip()
    return v.replace('/', '\\')


def split_terms(value: str) -> list[str]:
    """Разделители: $ (И), ^ (ИЛИ)."""
    parts = re_split(r"[$^]", value)
    return [p.strip() for p in parts if p.strip()]


def build_filter_clauses(
    inn: str | None,
    tnved: str | None,
    okpd2: str | None,
    regnumber: str | None,
    nameoforg: str | None,
    code: str | None,
    *,
    alias: str = "r.",
) -> Tuple[List[str], List[str]]:
    """Собирает WHERE-условия и параметры для общего набора фильтров.

    `alias` — префикс таблицы (например, "r." для алиаса r или "" для одиночной таблицы).
    Семантика поддержки '|' / ',' / '$' / '^' остаётся как в текущем `/reestr/semantic`.
    """
    clauses: List[str] = []
    params: List[str] = []

    if code:
        code_values = [v.strip() for v in code.split("|") if v.strip()]
        if code_values:
            code_conditions = []
            for v in code_values:
                code_conditions.append(f"({alias}inn = %s OR {alias}tnved ILIKE %s)")
                params.extend([v, f"%{v}%"])
            clauses.append("(" + " OR ".join(code_conditions) + ")")

    if inn:
        if "|" in inn:
            inn_values = [v.strip() for v in inn.split("|") if v.strip()]
            inn_conditions = []
            for v in inn_values:
                inn_conditions.append(f"{alias}inn = %s")
                params.append(v)
            clauses.append("(" + " OR ".join(inn_conditions) + ")")
        elif "," in inn:
            inn_values = [v.strip() for v in inn.split(",") if v.strip()]
            for v in inn_values:
                clauses.append(f"{alias}inn = %s")
                params.append(v)
        else:
            clauses.append(f"{alias}inn = %s")
            params.append(inn)

    if tnved:
        if "|" in tnved:
            tnved_values = [v.strip() for v in tnved.split("|") if v.strip()]
            tnved_conditions = []
            for v in tnved_values:
                tnved_conditions.append(f"{alias}tnved ILIKE %s")
                params.append(f"%{v}%")
            clauses.append("(" + " OR ".join(tnved_conditions) + ")")
        elif "," in tnved:
            tnved_values = [v.strip() for v in tnved.split(",") if v.strip()]
            for v in tnved_values:
                clauses.append(f"{alias}tnved ILIKE %s")
                params.append(f"%{v}%")
        else:
            clauses.append(f"{alias}tnved ILIKE %s")
            params.append(f"%{tnved}%")

    if okpd2:
        clauses.append(f"{alias}okpd2 ILIKE %s")
        params.append(f"%{okpd2}%")

    if regnumber:
        clauses.append(f"({alias}regnumber = %s OR {alias}registernumber = %s)")
        params.extend([regnumber, regnumber])

    if nameoforg:
        if "^" in nameoforg:
            values = split_terms(nameoforg)
            conds = []
            for v in values:
                conds.append(f"{alias}nameoforg ILIKE %s")
                params.append(f"%{v}%")
            clauses.append("(" + " OR ".join(conds) + ")")
        else:
            values = split_terms(nameoforg)
            for v in values:
                clauses.append(f"{alias}nameoforg ILIKE %s")
                params.append(f"%{v}%")

    return clauses, params


def vector_literal(values: List[float]) -> str:
    """Формирует строковый литерал pgvector ([0.1, 0.2, ...])."""
    return "[" + ", ".join(str(float(v)) for v in values) + "]"


def parse_synonym_entry(entry: object) -> tuple[str | None, str | None]:
    source: str | None = None
    variant: str | None = None
    if isinstance(entry, dict):
        source = (entry.get("source") or "").strip() or None
        variant = (entry.get("variant") or "").strip() or None
        return source, variant
    if entry is None:
        return None, None
    raw = str(entry).strip()
    if "→" in raw:
        parts = raw.split("→", 1)
    elif "->" in raw:
        parts = raw.split("->", 1)
    else:
        return None, None
    source = parts[0].strip() or None
    variant_part = parts[1] if len(parts) > 1 else ""
    variant = variant_part.strip() or None
    return source, variant


def normalize_synonym_pairs(entries: List[object]) -> List[Dict[str, str]]:
    normalized: List[Dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    seen_sources: set[str] = set()
    for entry in entries:
        source, variant = parse_synonym_entry(entry)
        if not variant:
            continue
        if source and variant.lower() == source.lower():
            continue
        source_key = (source or "").lower()
        if source_key and source_key in seen_sources:
            continue
        key = (source_key, variant.lower())
        if key in seen_pairs:
            continue
        seen_pairs.add(key)
        if source_key:
            seen_sources.add(source_key)
        normalized.append({"source": source or "", "variant": variant})
    return normalized


def fetch_semantic_embedding(
    text: str, *, normalize: bool, debug: bool = False
) -> Tuple[str, List[float], List[str], List[str]]:
    """Запрос к semantic-сервису. Возвращает (normalized_text, embedding, synonyms, expansions)."""
    payload = {"text": text, "debug": debug, "normalize": normalize}
    if not normalize:
        payload["apply_synonyms"] = True
    try:
        resp = requests.post(SEMANTIC_URL, json=payload, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except requests.Timeout:
        raise HTTPException(
            status_code=504,
            detail="Semantic service timeout while building embedding.",
        )
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Semantic service unavailable: {exc}",
        )
    if not isinstance(data, dict):
        raise HTTPException(
            status_code=500,
            detail="Semantic service returned unexpected payload.",
        )
    embedding = data.get("embedding")
    if not embedding:
        raise HTTPException(
            status_code=500,
            detail="Semantic service did not provide embedding.",
        )
    normalized = data.get("normalized") or text
    synonyms = data.get("synonyms_applied") or []
    if not isinstance(embedding, list):
        raise HTTPException(
            status_code=500,
            detail="Semantic service returned embedding in invalid format.",
        )
    expansions = data.get("synonym_expansions") or []
    return normalized, embedding, synonyms, expansions


TOKEN_PATTERN = re.compile(r"[0-9A-Za-zА-Яа-яЁё№/\\\\\\*-]+")


def extract_query_tokens(text: str) -> Tuple[List[str], str | None]:
    """Извлекает токены из запроса + определяет «главный» (первый буквенный без цифр)."""
    raw_tokens = TOKEN_PATTERN.findall((text or "").lower())
    tokens: List[str] = []
    seen_tokens: set[str] = set()
    primary_token: str | None = None
    for raw in raw_tokens:
        tok = raw.strip()
        if len(tok) < 2:
            continue
        if tok not in seen_tokens:
            tokens.append(tok)
            seen_tokens.add(tok)
        if (
            primary_token is None
            and re.search(r"[A-Za-zА-Яа-яЁё]", tok)
            and not re.search(r"[0-9]", tok)
        ):
            primary_token = tok
    return tokens, primary_token


def _strip_digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def _is_simple_value(value: str | None) -> bool:
    return bool(value) and "|" not in value and "," not in value


def build_tnved_fallback_attempts(
    tnved: str | None,
    code: str | None,
) -> List[Dict[str, object]]:
    """Прогрессивный fallback ТНВЭД 10 → 8 → 6 → 4 + дополнительные стратегии.

    Возвращает список попыток с label/tnved/code/removed_filters. Первой всегда идёт
    "original" — попытка с исходными значениями.
    """
    attempts: List[Dict[str, object]] = []
    seen: set[tuple[str, str]] = set()

    def add(label: str, t: str | None, c: str | None, removed: List[str] | None = None) -> None:
        key = (t or "", c or "")
        if key in seen:
            return
        seen.add(key)
        attempts.append({"label": label, "tnved": t, "code": c, "removed_filters": removed or []})

    add("original", tnved, code)

    tnved_digits = _strip_digits(tnved) if _is_simple_value(tnved) else ""
    if tnved_digits:
        original_len = len(tnved_digits)
        for length in (10, 8, 6, 4):
            if length < original_len and length >= 4:
                add(f"tnved_prefix_{length}", tnved_digits[:length], code)

    code_digits = _strip_digits(code) if _is_simple_value(code) else ""
    if not tnved_digits and code_digits:
        for length in (10, 8, 6, 4):
            if len(code_digits) >= length and length >= 4:
                removed = ["code"] if code else []
                add(f"code_as_tnved_{length}", code_digits[:length], None, removed)

    if tnved_digits or tnved:
        add("tnved_removed", None, code, ["tnved"])

    return attempts


__all__ = [
    "SEMANTIC_URL",
    "get_conn",
    "serialize_dates",
    "coerce_decimals",
    "normalize_regnumber",
    "split_terms",
    "build_filter_clauses",
    "vector_literal",
    "parse_synonym_entry",
    "normalize_synonym_pairs",
    "fetch_semantic_embedding",
    "extract_query_tokens",
    "build_tnved_fallback_attempts",
    "TOKEN_PATTERN",
]
