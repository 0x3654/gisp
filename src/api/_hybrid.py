"""Hybrid search: RRF-фьюжн pgvector + FTS(ru) + pg_trgm.

Изолировано от legacy `/reestr/semantic`. Параметры RRF и каналов конфигурируются env'ом.
"""
from __future__ import annotations

import logging
import os
import re
from typing import Any, Dict, List, Tuple

import requests
from fastapi import HTTPException

logger = logging.getLogger("uvicorn.error")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("invalid int env %s=%r; using %d", name, raw, default)
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        logger.warning("invalid float env %s=%r; using %s", name, raw, default)
        return default


RRF_K = _env_int("RRF_K", 60)
RRF_W_VEC = _env_float("RRF_WEIGHT_VEC", 1.0)
RRF_W_FTS = _env_float("RRF_WEIGHT_FTS", 1.2)
RRF_W_TRGM = _env_float("RRF_WEIGHT_TRGM", 1.2)
HYBRID_TOPK_PER_CHANNEL = _env_int("HYBRID_TOPK_PER_CHANNEL", 100)
HYBRID_DEFAULT_LIMIT = _env_int("HYBRID_DEFAULT_LIMIT", 50)
HYBRID_TRGM_THRESHOLD = _env_float("HYBRID_TRGM_THRESHOLD", 0.15)

RERANKER_URL = os.getenv("RERANKER_URL", "http://reranker:8020/rerank")
RERANKER_TIMEOUT = _env_float("RERANKER_TIMEOUT", 15.0)
RERANK_BEFORE_LIMIT = _env_int("RERANK_BEFORE_LIMIT", 50)

# Должно соответствовать generated column productname_normalized из 051_hybrid_*.sql:
#   regexp_replace(lower(coalesce(productname,'')), '[\s\-\/×x*]+', '', 'g')
_TRGM_NORM_RE = re.compile(r"[\s\-\/×x*]+")


def trgm_normalize_query(text: str) -> str:
    return _TRGM_NORM_RE.sub("", (text or "").lower())


def build_hybrid_sql(
    *,
    filter_clauses: List[str],
    topk_per_channel: int,
    rrf_k: int,
    w_vec: float,
    w_fts: float,
    w_trgm: float,
) -> str:
    """Возвращает SQL-шаблон для RRF-фьюжна.

    Веса и k подставляются как литералы (валидированные числа из env), остальное —
    параметры (`%s`) в следующем порядке (передавать в exec_params):
        1. embedding_literal      (vec CTE — qv)
        2. fts_query              (fts CTE — qf, plainto_tsquery)
        3. trgm_query_normalized  (trgm CTE — qt)
        4..k. filter_params       (применяются к vec WHERE)
        k+1..m. filter_params     (применяются к fts WHERE)
        m+1..n. filter_params     (применяются к trgm WHERE)
        n+1. limit
        n+2. offset
    """
    where_clause_vec = ""
    where_clause_fts = ""
    where_clause_trgm = ""
    if filter_clauses:
        and_clauses = " AND ".join(filter_clauses)
        where_clause_vec = f"WHERE {and_clauses}"
        # для fts/trgm @@ или % уже задают первую часть WHERE, добавим AND ...
        where_clause_fts = f"AND {and_clauses}"
        where_clause_trgm = f"AND {and_clauses}"

    # Защита: гарантируем числовой формат литералов
    rrf_k_lit = int(rrf_k)
    w_vec_lit = float(w_vec)
    w_fts_lit = float(w_fts)
    w_trgm_lit = float(w_trgm)
    topk_lit = int(topk_per_channel)

    return f"""
WITH qv AS (SELECT %s::vector AS v),
     qf AS (SELECT plainto_tsquery('russian', %s) AS q),
     qt AS (SELECT %s::text AS q),
     vec AS (
       SELECT si.reestr_id, (si.embedding <=> qv.v) AS distance
       FROM registry.semantic_items si
       JOIN registry.reestr r ON r.id = si.reestr_id
       CROSS JOIN qv
       {where_clause_vec}
       ORDER BY si.embedding <=> qv.v
       LIMIT {topk_lit}
     ),
     vec_ranked AS (
       SELECT reestr_id, row_number() OVER (ORDER BY distance) AS rank, distance
       FROM vec
     ),
     fts AS (
       SELECT r.id AS reestr_id, ts_rank_cd(r.search_tsv, qf.q) AS score
       FROM registry.reestr r
       CROSS JOIN qf
       WHERE r.search_tsv @@ qf.q
         {where_clause_fts}
       ORDER BY score DESC
       LIMIT {topk_lit}
     ),
     fts_ranked AS (
       SELECT reestr_id, row_number() OVER (ORDER BY score DESC) AS rank, score
       FROM fts
     ),
     trgm AS (
       SELECT r.id AS reestr_id, similarity(r.productname_normalized, qt.q) AS score
       FROM registry.reestr r
       CROSS JOIN qt
       WHERE r.productname_normalized %% qt.q
         {where_clause_trgm}
       ORDER BY score DESC
       LIMIT {topk_lit}
     ),
     trgm_ranked AS (
       SELECT reestr_id, row_number() OVER (ORDER BY score DESC) AS rank, score
       FROM trgm
     ),
     all_hits AS (
       SELECT reestr_id, ({w_vec_lit})::real / ({rrf_k_lit}::real + rank::real) AS rrf, 'vec'::text AS source
       FROM vec_ranked
       UNION ALL
       SELECT reestr_id, ({w_fts_lit})::real / ({rrf_k_lit}::real + rank::real), 'fts'::text
       FROM fts_ranked
       UNION ALL
       SELECT reestr_id, ({w_trgm_lit})::real / ({rrf_k_lit}::real + rank::real), 'trgm'::text
       FROM trgm_ranked
     ),
     fused AS (
       SELECT reestr_id,
              sum(rrf) AS total_score,
              array_agg(DISTINCT source ORDER BY source) AS sources
       FROM all_hits
       GROUP BY reestr_id
     )
SELECT
  r.*,
  f.total_score,
  f.sources,
  vr.distance AS vec_distance,
  fr.score    AS fts_score,
  tr.score    AS trgm_score
FROM fused f
JOIN registry.reestr r ON r.id = f.reestr_id
LEFT JOIN vec_ranked vr ON vr.reestr_id = f.reestr_id
LEFT JOIN fts_ranked fr ON fr.reestr_id = f.reestr_id
LEFT JOIN trgm_ranked tr ON tr.reestr_id = f.reestr_id
ORDER BY f.total_score DESC
LIMIT %s OFFSET %s
"""


def build_channel_counts_sql(filter_clauses: List[str]) -> str:
    """Облегчённый SQL для диагностики hit-count по каналам (без LIMIT 100, но с LIMIT 1000)."""
    where_clause_vec = ""
    where_clause_fts = ""
    where_clause_trgm = ""
    if filter_clauses:
        and_clauses = " AND ".join(filter_clauses)
        where_clause_vec = f"WHERE {and_clauses}"
        where_clause_fts = f"AND {and_clauses}"
        where_clause_trgm = f"AND {and_clauses}"

    return f"""
SELECT
  (
    SELECT count(*) FROM (
      SELECT si.reestr_id
      FROM registry.semantic_items si
      JOIN registry.reestr r ON r.id = si.reestr_id
      {where_clause_vec}
      LIMIT 1000
    ) z
  ) AS vec_hits,
  (
    SELECT count(*) FROM (
      SELECT r.id
      FROM registry.reestr r
      WHERE r.search_tsv @@ plainto_tsquery('russian', %s)
        {where_clause_fts}
      LIMIT 1000
    ) z
  ) AS fts_hits,
  (
    SELECT count(*) FROM (
      SELECT r.id
      FROM registry.reestr r
      WHERE r.productname_normalized %% %s
        {where_clause_trgm}
      LIMIT 1000
    ) z
  ) AS trgm_hits
"""


def call_reranker(query: str, candidates: List[Dict[str, Any]], top_k: int | None = None) -> Dict[str, Any]:
    """POST /rerank в сервис reranker. Возвращает payload как есть.

    Бросает HTTPException с 502/504 при недоступности сервиса.
    """
    if not candidates:
        return {"ranked": [], "elapsed_ms": 0.0, "model": "n/a", "cached": False}
    body: Dict[str, Any] = {
        "query": query,
        "candidates": [{"id": int(c["id"]), "text": str(c["text"])} for c in candidates],
    }
    if top_k is not None:
        body["top_k"] = int(top_k)
    try:
        resp = requests.post(RERANKER_URL, json=body, timeout=RERANKER_TIMEOUT)
        resp.raise_for_status()
        return resp.json()
    except requests.Timeout:
        raise HTTPException(status_code=504, detail="Reranker timeout")
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Reranker unavailable: {exc}")


__all__ = [
    "RRF_K",
    "RRF_W_VEC",
    "RRF_W_FTS",
    "RRF_W_TRGM",
    "HYBRID_TOPK_PER_CHANNEL",
    "HYBRID_DEFAULT_LIMIT",
    "HYBRID_TRGM_THRESHOLD",
    "RERANKER_URL",
    "RERANKER_TIMEOUT",
    "RERANK_BEFORE_LIMIT",
    "trgm_normalize_query",
    "build_hybrid_sql",
    "build_channel_counts_sql",
    "call_reranker",
]
