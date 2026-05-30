import logging
import os
import re
import time
from datetime import date
from decimal import Decimal
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from psycopg2.extras import RealDictCursor

from _search import (
    SEMANTIC_URL,  # noqa: F401 - сохраняем как часть публичной поверхности модуля
    build_filter_clauses,
    build_tnved_fallback_attempts,
    extract_query_tokens,
    fetch_semantic_embedding,
    get_conn,
    normalize_regnumber,
    normalize_synonym_pairs,
    serialize_dates,
    split_terms,
    vector_literal,
)
from _hybrid import (
    HYBRID_DEFAULT_LIMIT,
    HYBRID_TOPK_PER_CHANNEL,
    HYBRID_TRGM_THRESHOLD,
    RERANK_BEFORE_LIMIT,
    RRF_K,
    RRF_W_FTS,
    RRF_W_TRGM,
    RRF_W_VEC,
    build_channel_counts_sql,
    build_hybrid_sql,
    call_reranker,
    trgm_normalize_query,
)

logger = logging.getLogger("uvicorn.error")
FORCE_SEQSCAN = os.getenv("SEMANTIC_FORCE_SEQSCAN", "0") == "1"


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid value for %s: %s. Using default %d.", name, raw, default)
        return default


REESTR_MIN_LIMIT = max(1, _env_int("REESTR_MIN_LIMIT", 1))
REESTR_MAX_LIMIT = max(REESTR_MIN_LIMIT, _env_int("REESTR_MAX_LIMIT", 200))
REESTR_DEFAULT_LIMIT = min(
    REESTR_MAX_LIMIT,
    max(REESTR_MIN_LIMIT, _env_int("REESTR_DEFAULT_LIMIT", 20)),
)
REESTR_MIN_OFFSET = max(0, _env_int("REESTR_MIN_OFFSET", 0))
REESTR_DEFAULT_OFFSET = max(REESTR_MIN_OFFSET, _env_int("REESTR_DEFAULT_OFFSET", 0))
app = FastAPI()


@app.get("/reestr/semantic")
def get_reestr_semantic(
    request: Request,
    text: str = Query(..., description="Текст запроса для семантического поиска"),
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    inn: str | None = Query(None),
    tnved: str | None = Query(None),
    okpd2: str | None = Query(None),
    regnumber: str | None = Query(None),
    nameoforg: str | None = Query(None),
    code: str | None = Query(None),
):
    try:
        regnumber_norm = normalize_regnumber(regnumber)
        user_query_text = text
        normalized_text, embedding, raw_synonyms, expansions = fetch_semantic_embedding(
            text, normalize=False
        )
        synonyms = normalize_synonym_pairs(raw_synonyms)
        synonym_variant_applied: List[Dict[str, str]] = []
        lowered_text = (text or "").lower()
        synonyms_to_apply: List[Dict[str, str]] = []
        for pair in synonyms:
            variant = (pair.get("variant") or "").strip()
            if not variant:
                continue
            if variant.lower() in lowered_text:
                continue
            synonyms_to_apply.append(pair)
        if synonyms_to_apply:
            augmented_text = f"{text.rstrip()} {' '.join(p['variant'] for p in synonyms_to_apply)}".strip()
            try:
                (
                    normalized_text_new,
                    embedding_new,
                    raw_synonyms_new,
                    expansions_new,
                ) = fetch_semantic_embedding(augmented_text, normalize=False)
            except HTTPException:
                pass
            else:
                text = augmented_text
                normalized_text = normalized_text_new
                embedding = embedding_new
                expansions = expansions_new
                synonyms = normalize_synonym_pairs(raw_synonyms_new)
                synonym_variant_applied = [
                    {"source": p.get("source", ""), "variant": p.get("variant", "")}
                    for p in synonyms_to_apply
                ]
        embedding_literal = vector_literal(embedding)

        fetch_limit = max(limit * 2, offset + limit)
        attempts: List[Dict[str, object]] = build_tnved_fallback_attempts(tnved, code)

        query_template = """
            WITH query_vec AS (
                SELECT %s::vector AS embedding
            )
            SELECT
                r.*,
                s.normalized_text,
                s.synonyms,
                (s.embedding <=> query_vec.embedding) AS distance
            FROM registry.semantic_items AS s
            JOIN registry.reestr AS r
              ON r.id = s.reestr_id
            CROSS JOIN query_vec
            {where_clause}
            ORDER BY distance
            LIMIT %s OFFSET %s
        """

        attempt_history: List[Dict[str, object]] = []
        final_rows: List[dict] = []
        clauses: List[str] = []
        filter_values: List[str] = []
        final_attempt_index = 0
        last_clauses: List[str] = []
        last_filter_values: List[str] = []

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute("SET ivfflat.probes = %s", (100,))
                except Exception:
                    pass
                if FORCE_SEQSCAN:
                    try:
                        cur.execute("SET enable_indexscan = off")
                        cur.execute("SET enable_bitmapscan = off")
                    except Exception:
                        pass
                for idx, attempt in enumerate(attempts):
                    attempt_tnved = attempt["tnved"]
                    attempt_code = attempt["code"]
                    attempt_clauses, attempt_filter_values = build_filter_clauses(
                        inn,
                        attempt_tnved,
                        okpd2,
                        regnumber_norm,
                        nameoforg,
                        attempt_code,
                    )
                    last_clauses = attempt_clauses
                    last_filter_values = attempt_filter_values
                    where_clause = ""
                    if attempt_clauses:
                        where_clause = " WHERE " + " AND ".join(attempt_clauses)
                    current_limit = fetch_limit
                    rows_candidate: List[dict] = []
                    elapsed_exec = 0.0
                    while True:
                        query_sql = query_template.format(where_clause=where_clause)
                        exec_params: List[object] = [
                            embedding_literal,
                            *attempt_filter_values,
                            current_limit,
                            offset,
                        ]
                        start_exec = time.perf_counter()
                        cur.execute(query_sql, exec_params)
                        rows_candidate = cur.fetchall()
                        elapsed_exec = time.perf_counter() - start_exec
                        if logger.isEnabledFor(logging.INFO):
                            logger.info(
                                "semantic attempt=%s limit=%d rows=%d elapsed=%.3fs",
                                attempt["label"],
                                current_limit,
                                len(rows_candidate),
                                elapsed_exec,
                            )
                        if rows_candidate or current_limit >= 800:
                            break
                        current_limit = min(current_limit * 2, 800)
                    attempt_record: Dict[str, object] = {
                        "index": idx,
                        "label": attempt["label"],
                        "tnved": attempt_tnved,
                        "code": attempt_code,
                        "rows": len(rows_candidate),
                        "limit_used": current_limit,
                        "elapsed": round(elapsed_exec, 3),
                    }
                    removed_filters = attempt.get("removed_filters") or []
                    if removed_filters:
                        attempt_record["removed_filters"] = removed_filters
                    attempt_history.append(attempt_record)
                    if rows_candidate:
                        final_rows = rows_candidate
                        clauses = attempt_clauses
                        filter_values = attempt_filter_values
                        final_attempt_index = idx
                        break
                else:
                    clauses = last_clauses
                    filter_values = last_filter_values
                    final_rows = []

        rows = final_rows
        final_attempt = attempts[final_attempt_index]
        fallback_used = final_attempt_index > 0
        fallback_removed_filters: List[str] = list(
            final_attempt.get("removed_filters", [])
        )

        rows = serialize_dates(rows)
        for row in rows:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)

        tokens, primary_token = extract_query_tokens(text)

        base_token_set = set(tokens)
        synonym_terms: set[str] = set()
        primary_synonym_terms: set[str] = set()

        def register_synonym_term(term: str) -> None:
            normalized = (term or "").strip().lower()
            if not normalized:
                return
            if normalized in base_token_set:
                return
            synonym_terms.add(normalized)

        for item in expansions:
            register_synonym_term(item)
            for part in str(item).split():
                register_synonym_term(part)

        for pair in synonyms:
            if isinstance(pair, dict):
                source = (pair.get("source") or "").strip().lower()
                variant = (pair.get("variant") or "").strip().lower()
                pair_type = (pair.get("type") or "synonym").lower()
            else:
                source = ""
                variant = (str(pair) or "").strip().lower()
                pair_type = "synonym"
            if not variant:
                continue
            register_synonym_term(variant)
            for part in variant.split():
                register_synonym_term(part)
            if primary_token and source and source == primary_token:
                primary_synonym_terms.add(variant)
                for part in variant.split():
                    primary_synonym_terms.add(part)

        synonym_terms_list: List[str] = sorted(synonym_terms)
        primary_synonym_terms_list: List[str] = sorted(primary_synonym_terms)

        rows_by_id: dict[int, dict] = {}

        def enrich_row(row: dict) -> None:
            product = (row.get("productname") or "").lower()
            matches_base = 0
            matches_syn = 0
            if tokens:
                matches_base = sum(1 for token in tokens if token and token in product)
            if synonym_terms_list:
                matches_syn = sum(1 for token in synonym_terms_list if token and token in product)
            matches = matches_base + matches_syn
            if primary_token:
                primary_match = primary_token in product or any(
                    term in product for term in primary_synonym_terms_list
                )
            else:
                primary_match = matches > 0
            row["token_matches"] = matches
            row["token_matches_original"] = matches_base
            row["token_matches_synonyms"] = matches_syn
            row["primary_match"] = 1 if primary_match else 0
            for key, value in list(row.items()):
                if isinstance(value, date):
                    row[key] = value.isoformat()
                elif isinstance(value, Decimal):
                    row[key] = float(value)
            rows_by_id[row["id"]] = row

        for row in rows:
            enrich_row(row)

        filtered_rows = [
            row
            for row in rows_by_id.values()
            if row["token_matches"] > 0 and row["primary_match"] > 0
        ]
        filtered_count: int | None = None
        if filtered_rows:
            filtered_count = len(filtered_rows)
            candidate_rows = filtered_rows
        else:
            candidate_rows = list(rows_by_id.values())

        fallback_used = False
        if tokens and len(candidate_rows) < limit:
            remaining = max(fetch_limit, limit * 2)
            token_candidates = [
                tok for tok in tokens if re.search(r"[A-Za-zА-Яа-яЁё]", tok)
            ]
            base_clauses = list(clauses)
            base_params = list(filter_values)
            with get_conn() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    if FORCE_SEQSCAN:
                        try:
                            cur.execute("SET enable_indexscan = off")
                            cur.execute("SET enable_bitmapscan = off")
                        except Exception:
                            pass
                    for tok in token_candidates:
                        pattern = f"%{tok}%"
                        combined_clauses = base_clauses + [f"lower(r.productname) LIKE %s"]
                        where_sql = " WHERE " + " AND ".join(combined_clauses)
                        cur.execute(
                            f"""
                            SELECT
                                r.*,
                                s.normalized_text,
                                s.synonyms,
                                (s.embedding <=> %s::vector) AS distance
                            FROM registry.semantic_items AS s
                            JOIN registry.reestr AS r
                              ON r.id = s.reestr_id
                            {where_sql}
                            ORDER BY distance
                            LIMIT %s
                            """,
                            [embedding_literal, *base_params, pattern, remaining],
                        )
                        fallback_rows = cur.fetchall()
                        if not fallback_rows:
                            continue
                        fallback_used = True
                        for fr in fallback_rows:
                            rid = fr["id"]
                            if rid in rows_by_id:
                                continue
                            enrich_row(fr)
                            candidate_rows.append(fr)
                        if len(candidate_rows) >= remaining:
                            break

        if candidate_rows:
            candidate_rows.sort(
                key=lambda r: (
                    -r.get("token_matches", 0),
                    r.get("distance", float("inf")),
                )
            )
        else:
            candidate_rows = sorted(
                rows_by_id.values(), key=lambda r: r.get("distance", float("inf"))
            )
        rows = candidate_rows[:limit]

        final_active_filters = {
            "inn": inn,
            "tnved": final_attempt.get("tnved"),
            "okpd2": okpd2,
            "regnumber": regnumber_norm,
            "code": final_attempt.get("code"),
        }
        semantic_payload: Dict[str, object] = {
            "original_query": text,
            "user_query": user_query_text,
            "normalized_query": normalized_text,
            "synonyms": expansions,
            "synonym_pairs": synonyms,
            "mode": "raw",
            "tokens": tokens,
            "filtered_count": filtered_count,
            "primary_token": primary_token,
            "fallback_attempts": attempt_history,
            "fallback_used": fallback_used,
            "active_filters": final_active_filters,
        }
        if fallback_removed_filters:
            semantic_payload["fallback_removed_filters"] = fallback_removed_filters
        if synonym_variant_applied:
            semantic_payload["synonym_variant_applied"] = synonym_variant_applied

        return JSONResponse(
            content={
                "rows": rows,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
                "semantic": semantic_payload,
            },
            media_type="application/json",
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Exception occurred in /reestr/semantic endpoint")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/reestr/hybrid")
def get_reestr_hybrid(
    request: Request,
    text: str = Query(..., description="Текст запроса для hybrid-поиска"),
    limit: int = Query(HYBRID_DEFAULT_LIMIT, ge=1, le=200),
    offset: int = Query(0, ge=0),
    inn: str | None = Query(None),
    tnved: str | None = Query(None),
    okpd2: str | None = Query(None),
    regnumber: str | None = Query(None),
    nameoforg: str | None = Query(None),
    code: str | None = Query(None),
    diagnostics: bool = Query(False, description="Включить per-channel counters"),
    rerank: bool = Query(False, description="Прогнать top-K через cross-encoder reranker"),
):
    """RRF-фьюжн pgvector + FTS(ru) + pg_trgm. Опционально — cross-encoder reranker.

    Совместим по параметрам фильтров с `/reestr/semantic`. Возвращает строки реестра
    с дополнительными полями: total_score, sources, vec_distance, fts_score, trgm_score.
    При rerank=true добавляются rerank_score и rerank_rank.
    """
    try:
        regnumber_norm = normalize_regnumber(regnumber)
        normalized_text, embedding, raw_synonyms, expansions = fetch_semantic_embedding(
            text, normalize=False
        )
        synonyms = normalize_synonym_pairs(raw_synonyms)
        embedding_literal = vector_literal(embedding)
        trgm_query = trgm_normalize_query(text)
        fts_query = text  # plainto_tsquery сам токенизирует/стеммит

        attempts: List[Dict[str, object]] = build_tnved_fallback_attempts(tnved, code)
        attempt_history: List[Dict[str, object]] = []
        final_rows: List[dict] = []
        final_attempt_index = 0
        final_clauses: List[str] = []
        final_params: List[str] = []
        channel_counts: Dict[str, int] | None = None

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute("SET ivfflat.probes = %s", (100,))
                except Exception:
                    pass
                try:
                    cur.execute("SELECT set_limit(%s)", (HYBRID_TRGM_THRESHOLD,))
                except Exception:
                    pass
                if FORCE_SEQSCAN:
                    try:
                        cur.execute("SET enable_indexscan = off")
                        cur.execute("SET enable_bitmapscan = off")
                    except Exception:
                        pass

                # При rerank=true дёргаем БД с расширенным LIMIT (top-N для рерангера),
                # потом усекаем до пользовательского limit после реранка.
                sql_limit = max(limit + offset, RERANK_BEFORE_LIMIT) if rerank else limit
                sql_offset = 0 if rerank else offset
                for idx, attempt in enumerate(attempts):
                    a_tnved = attempt["tnved"]
                    a_code = attempt["code"]
                    a_clauses, a_params = build_filter_clauses(
                        inn, a_tnved, okpd2, regnumber_norm, nameoforg, a_code
                    )
                    sql_text = build_hybrid_sql(
                        filter_clauses=a_clauses,
                        topk_per_channel=HYBRID_TOPK_PER_CHANNEL,
                        rrf_k=RRF_K,
                        w_vec=RRF_W_VEC,
                        w_fts=RRF_W_FTS,
                        w_trgm=RRF_W_TRGM,
                    )
                    exec_params: List[object] = [
                        embedding_literal,
                        fts_query,
                        trgm_query,
                        *a_params,  # vec WHERE
                        *a_params,  # fts WHERE
                        *a_params,  # trgm WHERE
                        sql_limit,
                        sql_offset,
                    ]
                    start_exec = time.perf_counter()
                    cur.execute(sql_text, exec_params)
                    rows_candidate = cur.fetchall()
                    elapsed = time.perf_counter() - start_exec

                    attempt_record: Dict[str, object] = {
                        "index": idx,
                        "label": attempt["label"],
                        "tnved": a_tnved,
                        "code": a_code,
                        "rows": len(rows_candidate),
                        "elapsed": round(elapsed, 3),
                    }
                    removed_filters = attempt.get("removed_filters") or []
                    if removed_filters:
                        attempt_record["removed_filters"] = removed_filters
                    attempt_history.append(attempt_record)

                    if rows_candidate:
                        final_rows = rows_candidate
                        final_attempt_index = idx
                        final_clauses = a_clauses
                        final_params = a_params
                        break

                if diagnostics:
                    try:
                        diag_sql = build_channel_counts_sql(final_clauses)
                        diag_params = [
                            fts_query,
                            *final_params,
                            trgm_query,
                            *final_params,
                        ]
                        cur.execute(diag_sql, diag_params)
                        diag = cur.fetchone() or {}
                        channel_counts = {
                            "vec_hits": int(diag.get("vec_hits") or 0),
                            "fts_hits": int(diag.get("fts_hits") or 0),
                            "trgm_hits": int(diag.get("trgm_hits") or 0),
                        }
                    except Exception:
                        logger.exception("hybrid diagnostics query failed")
                        channel_counts = None

        _HIDDEN_HYBRID_COLS = ("search_tsv", "productname_normalized")
        rows = serialize_dates(final_rows)
        for row in rows:
            for hidden in _HIDDEN_HYBRID_COLS:
                row.pop(hidden, None)
            for key, value in list(row.items()):
                if isinstance(value, Decimal):
                    row[key] = float(value)
                elif isinstance(value, date):
                    row[key] = value.isoformat()
            if row.get("total_score") is not None:
                row["total_score"] = float(row["total_score"])
            if row.get("vec_distance") is not None:
                row["vec_distance"] = float(row["vec_distance"])
            if row.get("fts_score") is not None:
                row["fts_score"] = float(row["fts_score"])
            if row.get("trgm_score") is not None:
                row["trgm_score"] = float(row["trgm_score"])

        rerank_info: Dict[str, object] | None = None
        if rerank and rows:
            candidates_payload = [
                {"id": int(r["id"]), "text": str(r.get("productname") or "")}
                for r in rows
            ]
            start_rr = time.perf_counter()
            try:
                rr_resp = call_reranker(text, candidates_payload, top_k=None)
            except HTTPException as exc:
                # soft-fail: возвращаем результаты без реранка, помечаем причину
                logger.warning("rerank soft-failed: %s", exc.detail)
                rows = rows[offset : offset + limit]
                rerank_info = {
                    "applied": False,
                    "reason": "reranker_unavailable",
                    "detail": str(exc.detail),
                }
                rr_resp = None  # type: ignore[assignment]
            rr_elapsed = (time.perf_counter() - start_rr) * 1000
            if rr_resp is None:
                # soft-fail обработан выше (rerank_info уже выставлен, rows урезаны)
                rr_resp = {}  # type: ignore[assignment]
                ranked = []
            else:
                ranked = rr_resp.get("ranked") or []
                score_by_id: Dict[int, Dict[str, float]] = {}
                for item in ranked:
                    score_by_id[int(item["id"])] = {
                        "score": float(item["score"]),
                        "rank": int(item["rank"]),
                    }
                for r in rows:
                    rid = int(r["id"])
                    info = score_by_id.get(rid)
                    if info is not None:
                        r["rerank_score"] = info["score"]
                        r["rerank_rank"] = info["rank"]
                    else:
                        r["rerank_score"] = None
                        r["rerank_rank"] = None
                # сортируем по rerank_rank (ASC); элементы без rank уходят в хвост
                rows.sort(
                    key=lambda r: (r.get("rerank_rank") is None, r.get("rerank_rank") or 1_000_000)
                )
                # применяем пользовательский offset/limit поверх отсортированных
                rows = rows[offset : offset + limit]
                rerank_info = {
                    "applied": True,
                    "model": rr_resp.get("model"),
                    "cached": bool(rr_resp.get("cached", False)),
                    "elapsed_ms": round(rr_elapsed, 2),
                    "service_elapsed_ms": rr_resp.get("elapsed_ms"),
                    "candidates": len(candidates_payload),
                }
        elif rerank and not rows:
            rerank_info = {"applied": False, "reason": "no candidates"}

        final_attempt = attempts[final_attempt_index]
        hybrid_block: Dict[str, object] = {
            "original_query": text,
            "normalized_query": normalized_text,
            "trgm_query": trgm_query,
            "synonyms": expansions,
            "synonym_pairs": synonyms,
            "rrf": {
                "k": RRF_K,
                "weights": {"vec": RRF_W_VEC, "fts": RRF_W_FTS, "trgm": RRF_W_TRGM},
                "topk_per_channel": HYBRID_TOPK_PER_CHANNEL,
                "trgm_threshold": HYBRID_TRGM_THRESHOLD,
            },
            "fallback_used": final_attempt_index > 0,
            "fallback_attempts": attempt_history,
            "active_filters": {
                "inn": inn,
                "tnved": final_attempt.get("tnved"),
                "okpd2": okpd2,
                "regnumber": regnumber_norm,
                "code": final_attempt.get("code"),
                "nameoforg": nameoforg,
            },
        }
        removed_filters = final_attempt.get("removed_filters") or []
        if removed_filters:
            hybrid_block["fallback_removed_filters"] = list(removed_filters)
        if channel_counts is not None:
            hybrid_block["channels"] = channel_counts
        if rerank_info is not None:
            hybrid_block["rerank"] = rerank_info

        return JSONResponse(
            content={
                "rows": rows,
                "limit": limit,
                "offset": offset,
                "count": len(rows),
                "hybrid": hybrid_block,
            },
            media_type="application/json",
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Exception occurred in /reestr/hybrid endpoint")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/reestr")
def get_reestr(
    request: Request,
    inn: str | None = None,
    tnved: str | None = None,
    okpd2: str | None = None,
    productname: str | None = None,
    regnumber: str | None = None,
    nameoforg: str | None = None,
    limit: int = Query(
        REESTR_DEFAULT_LIMIT, ge=REESTR_MIN_LIMIT, le=REESTR_MAX_LIMIT
    ),
    offset: int = Query(REESTR_DEFAULT_OFFSET, ge=REESTR_MIN_OFFSET),
    code: str | None = None
):
    try:
        allowed = {"inn", "tnved", "okpd2", "productname", "regnumber", "nameoforg", "limit", "offset", "code"}
        passed = set(request.query_params.keys())
        unknown = passed - allowed
        if unknown:
            raise HTTPException(status_code=400, detail={
                "error": "Unknown query parameter(s)",
                "unknown": sorted(list(unknown)),
                "allowed": sorted(list(allowed))
            })

        regnumber = normalize_regnumber(regnumber)

        filters_provided = any([inn, tnved, okpd2, productname, regnumber, nameoforg, code])
        if not filters_provided:
            raise HTTPException(status_code=400, detail="At least one filter parameter is required (inn, tnved, okpd2, productname, regnumber, nameoforg, code).")

        query = "SELECT * FROM registry.reestr WHERE 1=1"
        params: list[object] = []

        # Обработка универсального кода (ищет и по inn, и по tnved)
        if code:
            code_values = [v.strip() for v in code.split("|") if v.strip()]
            code_conditions = []
            for v in code_values:
                code_conditions.append("(inn = %s OR tnved ILIKE %s)")
                params.extend([v, f"%{v}%"])
            query += " AND (" + " OR ".join(code_conditions) + ")"

        # Обработка inn с поддержкой | (ИЛИ) и , (И)
        if inn:
            if "|" in inn:
                inn_values = [v.strip() for v in inn.split("|") if v.strip()]
                inn_conditions = []
                for v in inn_values:
                    inn_conditions.append("inn = %s")
                    params.append(v)
                query += " AND (" + " OR ".join(inn_conditions) + ")"
            elif "," in inn:
                inn_values = [v.strip() for v in inn.split(",") if v.strip()]
                for v in inn_values:
                    query += " AND inn = %s"
                    params.append(v)
            else:
                query += " AND inn = %s"
                params.append(inn)

        # Обработка tnved с поддержкой | (ИЛИ) и , (И)
        if tnved:
            if "|" in tnved:
                tnved_values = [v.strip() for v in tnved.split("|") if v.strip()]
                tnved_conditions = []
                for v in tnved_values:
                    tnved_conditions.append("tnved ILIKE %s")
                    params.append(f"%{v}%")
                query += " AND (" + " OR ".join(tnved_conditions) + ")"
            elif "," in tnved:
                tnved_values = [v.strip() for v in tnved.split(",") if v.strip()]
                for v in tnved_values:
                    query += " AND tnved ILIKE %s"
                    params.append(f"%{v}%")
            else:
                query += " AND tnved ILIKE %s"
                params.append(f"%{tnved}%")

        if okpd2:
            query += " AND okpd2 ILIKE %s"
            params.append(f"%{okpd2}%")

        if productname:
            # $ — И, ^ — ИЛИ
            if "^" in productname:
                # ИЛИ (любой из терминов)
                values = split_terms(productname)
                conds = []
                for v in values:
                    conds.append("productname ILIKE %s")
                    params.append(f"%{v}%")
                query += " AND (" + " OR ".join(conds) + ")"
            else:
                # И (все термины)
                values = split_terms(productname)
                for v in values:
                    query += " AND productname ILIKE %s"
                    params.append(f"%{v}%")

        if regnumber:
            query += " AND (regnumber = %s OR registernumber = %s)"
            params.extend([regnumber, regnumber])

        if nameoforg:
            # $ — И, ^ — ИЛИ
            if "^" in nameoforg:
                # ИЛИ (любой из терминов)
                values = split_terms(nameoforg)
                conds = []
                for v in values:
                    conds.append("nameoforg ILIKE %s")
                    params.append(f"%{v}%")
                query += " AND (" + " OR ".join(conds) + ")"
            else:
                # И (все термины)
                values = split_terms(nameoforg)
                for v in values:
                    query += " AND nameoforg ILIKE %s"
                    params.append(f"%{v}%")

        query += " ORDER BY inn LIMIT %s OFFSET %s"
        params.extend([limit, offset])

        with get_conn() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                rows = cur.fetchall()

        rows = serialize_dates(rows)
        for row in rows:
            for key, value in row.items():
                if isinstance(value, Decimal):
                    row[key] = float(value)

        return JSONResponse(
            content={
                "rows": rows,
                "limit": limit,
                "offset": offset,
                "count": len(rows)
            },
            media_type="application/json"
        )
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Exception occurred in /reestr endpoint")
        return JSONResponse(status_code=500, content={"error": str(e)})

@app.get("/")
def serve_index():
    path = "/app/index.html"
    print("Serving file:", path, "exists:", os.path.exists(path))
    return FileResponse(path)
