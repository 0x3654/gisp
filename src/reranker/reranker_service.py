"""FastAPI-сервис cross-encoder реранкера.

Endpoints:
  GET  /health            — readiness + backend info
  POST /rerank            — ранжирование пар (query, candidate.text)
"""
from __future__ import annotations

import copy
import hashlib
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from model_loader import BACKEND, MAX_LEN, get_reranker

logger = logging.getLogger("uvicorn.error")

CACHE_TTL = int(os.getenv("RERANKER_CACHE_TTL_SECONDS", "86400"))
MAX_PAIRS = int(os.getenv("RERANKER_MAX_PAIRS", "50"))


class Candidate(BaseModel):
    id: int
    text: str


class RerankRequest(BaseModel):
    query: str
    candidates: List[Candidate]
    top_k: Optional[int] = Field(default=None, ge=1)


class RerankResultItem(BaseModel):
    id: int
    score: float
    rank: int


class RerankResponse(BaseModel):
    ranked: List[RerankResultItem]
    elapsed_ms: float
    model: str
    cached: bool


_PG_CONN: psycopg2.extensions.connection | None = None


def _pg_conn() -> psycopg2.extensions.connection:
    global _PG_CONN
    if _PG_CONN is not None and _PG_CONN.closed == 0:
        return _PG_CONN
    _PG_CONN = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "registry"),
        user=os.getenv("POSTGRES_USER", "registry"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    _PG_CONN.autocommit = True
    return _PG_CONN


def _reset_pg() -> None:
    global _PG_CONN
    if _PG_CONN is not None:
        try:
            _PG_CONN.close()
        except Exception:
            pass
    _PG_CONN = None


def _cache_key(query: str, candidate_ids: List[int]) -> str:
    payload = json.dumps(
        {"q": query.strip(), "ids": list(candidate_ids), "backend": BACKEND, "max_len": MAX_LEN},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _cache_lookup(key: str) -> Optional[Dict[str, object]]:
    if CACHE_TTL <= 0:
        return None
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                "SELECT payload, updated_at FROM registry.rerank_cache WHERE cache_key = %s",
                (key,),
            )
            row = cur.fetchone()
    except psycopg2.Error:
        _reset_pg()
        return None
    if not row:
        return None
    payload, updated_at = row
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    if datetime.now(timezone.utc) - updated_at > timedelta(seconds=CACHE_TTL):
        return None
    return copy.deepcopy(payload)


def _cache_store(key: str, query: str, ids: List[int], payload: Dict[str, object]) -> None:
    if CACHE_TTL <= 0:
        return
    try:
        conn = _pg_conn()
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO registry.rerank_cache (cache_key, query_text, candidate_ids, payload, updated_at)
                VALUES (%s, %s, %s, %s, now())
                ON CONFLICT (cache_key) DO UPDATE SET
                  payload = EXCLUDED.payload,
                  updated_at = now()
                """,
                (key, query, ids, psycopg2.extras.Json(payload)),
            )
    except psycopg2.Error:
        _reset_pg()


app = FastAPI(title="GISP reranker")


@app.on_event("startup")
def _startup() -> None:
    try:
        rk = get_reranker()
        # warm-up
        rk.score([("warmup", "warmup candidate")])
        logger.info("reranker warm-up complete: backend=%s", rk.backend)
    except Exception:
        logger.exception("reranker startup failed")
        raise


@app.get("/health")
def health() -> Dict[str, object]:
    try:
        rk = get_reranker()
        return {"status": "ok", "backend": rk.backend, "max_len": MAX_LEN}
    except Exception as exc:  # noqa: BLE001
        return {"status": "error", "error": str(exc)}


@app.post("/rerank", response_model=RerankResponse)
def rerank(req: RerankRequest) -> RerankResponse:
    if not req.query.strip():
        raise HTTPException(status_code=400, detail="query must not be empty")
    if not req.candidates:
        return RerankResponse(ranked=[], elapsed_ms=0.0, model=BACKEND, cached=False)
    if len(req.candidates) > MAX_PAIRS * 4:
        raise HTTPException(
            status_code=400,
            detail=f"too many candidates: {len(req.candidates)} (max {MAX_PAIRS * 4})",
        )

    ids = [c.id for c in req.candidates]
    cache_key = _cache_key(req.query, ids)
    cached_payload = _cache_lookup(cache_key)
    if cached_payload:
        return RerankResponse(
            ranked=[RerankResultItem(**item) for item in cached_payload.get("ranked", [])],
            elapsed_ms=float(cached_payload.get("elapsed_ms", 0.0)),
            model=str(cached_payload.get("model", BACKEND)),
            cached=True,
        )

    rk = get_reranker()
    pairs: List[tuple[str, str]] = [(req.query, c.text) for c in req.candidates]
    t0 = time.perf_counter()
    # бьём на батчи по MAX_PAIRS чтобы не переполнить память при больших K
    scores: List[float] = []
    for i in range(0, len(pairs), MAX_PAIRS):
        batch = pairs[i : i + MAX_PAIRS]
        scores.extend(rk.score(batch))
    elapsed_ms = (time.perf_counter() - t0) * 1000

    # bge-reranker возвращает raw logit; превращаем в [0,1] через sigmoid (так модель тренирована BCE).
    probabilities = [1.0 / (1.0 + math.exp(-s)) for s in scores]
    ranked_pairs = sorted(
        zip(ids, probabilities), key=lambda kv: kv[1], reverse=True
    )
    top_k = req.top_k or len(ranked_pairs)
    ranked = [
        RerankResultItem(id=cid, score=float(s), rank=rank)
        for rank, (cid, s) in enumerate(ranked_pairs[:top_k], start=1)
    ]
    response = RerankResponse(
        ranked=ranked, elapsed_ms=round(elapsed_ms, 2), model=rk.backend, cached=False
    )
    _cache_store(cache_key, req.query, ids, response.model_dump())
    return response
