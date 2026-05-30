"""Tools, доступные reasoning-агенту.

Tools — обёртки над существующим API (`/reestr/hybrid`, `/reestr` direct, `/reestr/semantic`).
Кэшировать здесь не пытаемся — кеш есть в reranker'е и semantic-сервисе.
"""
from __future__ import annotations

import datetime
import decimal
import os
import re
from typing import Any, Dict, List, Optional

import requests
from fastapi import HTTPException

API_BASE = os.getenv("REGISTRY_API_BASE", "http://api:8000").rstrip("/")
API_TIMEOUT = float(os.getenv("REGISTRY_API_TIMEOUT", "30"))

# Поля, которые мы возвращаем в результате tool-call'ов. Полные карточки берутся через get_candidate_details.
_DEFAULT_CARD_FIELDS = (
    "id",
    "regnumber",
    "registernumber",
    "productname",
    "nameoforg",
    "inn",
    "tnved",
    "okpd2",
    "docdate",
    "docvalidtill",
    "rerank_score",
    "rerank_rank",
    "total_score",
    "vec_distance",
    "fts_score",
    "trgm_score",
    "sources",
)


def _json_safe(value: Any) -> Any:
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if isinstance(value, decimal.Decimal):
        return float(value)
    return value


def _slim(row: Dict[str, Any], fields: tuple[str, ...] = _DEFAULT_CARD_FIELDS) -> Dict[str, Any]:
    return {k: _json_safe(row.get(k)) for k in fields if k in row}


def _get(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    try:
        resp = requests.get(
            f"{API_BASE}{endpoint}",
            params={k: v for k, v in params.items() if v is not None},
            timeout=API_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"registry API error: {exc}")


# ===================== Tools =====================

def search_hybrid(*, query: str, limit: int = 20, filters: Optional[Dict[str, Any]] = None, rerank: bool = True) -> Dict[str, Any]:
    params: Dict[str, Any] = {"text": query, "limit": limit, "rerank": "true" if rerank else "false"}
    for k in ("inn", "tnved", "okpd2", "regnumber", "nameoforg", "code"):
        v = (filters or {}).get(k)
        if v is not None:
            params[k] = v
    payload = _get("/reestr/hybrid", params)
    rows = payload.get("rows") or []
    return {
        "candidates": [_slim(r) for r in rows],
        "diagnostics": {
            "count": payload.get("count"),
            "rerank": (payload.get("hybrid") or {}).get("rerank"),
            "fallback_used": (payload.get("hybrid") or {}).get("fallback_used"),
        },
    }


def filter_candidates(*, candidate_ids: List[int], filter: Dict[str, Any]) -> Dict[str, Any]:
    """Локальный фильтр по уже извлечённым кандидатам (через /reestr direct).

    Поддерживает inn, tnved, okpd2, regnumber.
    """
    if not candidate_ids:
        return {"candidates": [], "diagnostics": {"reason": "no candidate_ids"}}
    # Достанем по id через direct API
    params = {"limit": min(len(candidate_ids), 200), "offset": 0}
    # direct endpoint требует хотя бы один фильтр; используем подзапрос по id-список через inn — не сработает.
    # Поэтому делаем выборку батчами: один запрос на id-set через /reestr/hybrid с фиктивным text="*".
    # Простейший путь: дёрнем /reestr/semantic с фильтрами + получим, потом сами отсечём.
    # Чтобы не усложнять — получаем детали через get_candidate_details и фильтруем здесь.
    details = get_candidate_details(candidate_ids=candidate_ids).get("candidates") or []
    out = []
    for row in details:
        ok = True
        for k, v in (filter or {}).items():
            if v is None:
                continue
            val = (row.get(k) or "")
            if k in ("tnved", "okpd2"):
                if v not in str(val):
                    ok = False
                    break
            elif str(val) != str(v):
                ok = False
                break
        if ok:
            out.append(row)
    return {"candidates": out, "diagnostics": {"input": len(details), "output": len(out)}}


def get_candidate_details(*, candidate_ids: List[int]) -> Dict[str, Any]:
    if not candidate_ids:
        return {"candidates": [], "diagnostics": {"reason": "no candidate_ids"}}
    # Делаем один SQL-запрос через direct API... но direct требует фильтр. Используем regnumber=null трюк нельзя.
    # Воркэраунд: используем /reestr?regnumber=<имя> не подходит. Сделаем серию /reestr/hybrid?text=<id>?
    # Чище — отдельный API endpoint /reestr/by_ids; пока эмулируем через psycopg напрямую:
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "registry"),
        user=os.getenv("POSTGRES_USER", "registry"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT * FROM registry.reestr WHERE id = ANY(%s::bigint[])",
                ([int(i) for i in candidate_ids],),
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    full_fields = _DEFAULT_CARD_FIELDS + (
        "ogrn", "orgaddr", "productmanufaddress", "nameofregulations",
        "docname", "docnum", "mptdep", "source_file", "registernumber",
    )
    return {"candidates": [_slim(dict(r), full_fields) for r in rows]}


def search_by_manufacturer(*, inn: str, product_hint: str | None = None) -> Dict[str, Any]:
    if not inn:
        raise HTTPException(status_code=400, detail="inn is required")
    if product_hint:
        return search_hybrid(query=product_hint, limit=30, filters={"inn": inn}, rerank=True)
    # Без подсказки — direct query через /reestr с filter inn
    payload = _get("/reestr", {"inn": inn, "limit": 30})
    rows = payload.get("rows") or []
    return {"candidates": [_slim(r) for r in rows], "diagnostics": {"count": payload.get("count")}}


def extract_features(*, text: str) -> Dict[str, Any]:
    """Грубое regex-извлечение типов/типоразмеров/ГОСТ-маркировок/материалов.

    Для русского технического языка делает лишь стартовую разметку — основная работа
    остаётся за LLM. Tool существует, чтобы предоставить общую базовую структуру.
    """
    t = text or ""
    features: Dict[str, Any] = {}
    # ГОСТ
    gosts = re.findall(r"ГОСТ[\s.]*([0-9]{3,5}[-/][0-9]{2,4})", t, flags=re.IGNORECASE)
    if gosts:
        features["gosts"] = gosts
    # ТНВЭД-цифры
    tnved_match = re.search(r"\b(\d{4}|\d{6}|\d{8}|\d{10})\b", t)
    if tnved_match:
        features["tnved_candidate"] = tnved_match.group(1)
    # Типоразмеры: "К 80-50-200", "1000В-4,5/65", "М10"
    sizes = re.findall(r"\b[A-ZА-Я]?\d{1,4}[-×x*\/]\d{1,4}(?:[-×x*\/]\d{1,4})?", t)
    if sizes:
        features["sizes"] = sizes
    # Резьба М...
    threads = re.findall(r"\bМ\d{1,3}\b", t)
    if threads:
        features["threads"] = threads
    # Материалы (простой словарик)
    materials = []
    for word in ("сталь", "латунь", "медь", "алюминий", "чугун", "нерж", "пластик", "пвх", "тэп"):
        if re.search(rf"\b{word}", t, flags=re.IGNORECASE):
            materials.append(word)
    if materials:
        features["materials"] = materials
    return {"features": features, "text": t}


def compare_features(*, query_features: Dict[str, Any], candidate_features: Dict[str, Any]) -> Dict[str, Any]:
    """Простое попарное сравнение наборов признаков."""
    matched: list[str] = []
    unmatched: list[str] = []
    uncertain: list[str] = []
    for key, qval in (query_features or {}).items():
        cval = (candidate_features or {}).get(key)
        if cval is None:
            uncertain.append(key)
            continue
        if isinstance(qval, list) and isinstance(cval, list):
            if set(qval) & set(cval):
                matched.append(key)
            else:
                unmatched.append(key)
        elif qval == cval:
            matched.append(key)
        else:
            unmatched.append(key)
    return {
        "matched": matched,
        "unmatched": unmatched,
        "uncertain": uncertain,
        "score": (len(matched) / max(1, len(matched) + len(unmatched) + len(uncertain))),
    }


TOOL_REGISTRY = {
    "search_hybrid": search_hybrid,
    "filter_candidates": filter_candidates,
    "get_candidate_details": get_candidate_details,
    "search_by_manufacturer": search_by_manufacturer,
    "extract_features": extract_features,
    "compare_features": compare_features,
}


def dispatch_tool(name: str, params: Dict[str, Any]) -> Dict[str, Any]:
    func = TOOL_REGISTRY.get(name)
    if func is None:
        raise HTTPException(status_code=400, detail=f"unknown tool: {name}")
    try:
        return func(**(params or {}))
    except HTTPException:
        raise
    except TypeError as exc:
        raise HTTPException(status_code=400, detail=f"bad params for {name}: {exc}")
