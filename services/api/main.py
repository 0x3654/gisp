import os
import logging
import re
import time
from datetime import date
from re import split as re_split
from decimal import Decimal
from typing import List, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from psycopg2 import connect
from psycopg2.extras import RealDictCursor

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

def get_conn():
    return connect(
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        options="-c client_encoding=UTF8"
    )

def serialize_dates(rows):
    """
    Преобразует все значения типа date в iso-строки внутри списка словарей.
    """
    for row in rows:
        for key, value in row.items():
            if isinstance(value, date):
                row[key] = value.isoformat()
    return rows

def normalize_regnumber(val: str | None) -> str | None:
    if not val:
        return None
    v = val.strip().strip('"').strip()
    # приводим разделители к обратному слэшу
    v = v.replace('/', '\\')
    return v

def split_terms(value: str) -> list[str]:
    # Новый разделитель: $ (И), ^ (ИЛИ)
    parts = re_split(r"[$^]", value)
    return [p.strip() for p in parts if p.strip()]

def build_filter_clauses(
    inn: str | None,
    tnved: str | None,
    okpd2: str | None,
    regnumber: str | None,
    nameoforg: str | None,
    code: str | None,
) -> Tuple[List[str], List[str]]:
    clauses: List[str] = []
    params: List[str] = []
    alias = "r."

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

SEMANTIC_URL = os.getenv(
    "SEMANTIC_URL", "http://semantic_service:8010/semantic_normalize"
)


def _vector_literal(values: List[float]) -> str:
    return "[" + ", ".join(str(float(v)) for v in values) + "]"


def _fetch_semantic_embedding(
    text: str, *, normalize: bool, debug: bool = False
) -> Tuple[str, List[float], List[str], List[str]]:
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
        normalized_text, embedding, synonyms, expansions = _fetch_semantic_embedding(
            text, normalize=False
        )
        embedding_literal = _vector_literal(embedding)

        fetch_limit = max(limit * 2, offset + limit)

        def _is_simple_value(value: str | None) -> bool:
            return bool(value) and "|" not in value and "," not in value

        def _strip_digits(value: str | None) -> str:
            return re.sub(r"\D", "", value or "")

        attempts: List[Dict[str, object]] = []
        attempts_seen: set[tuple[str, str]] = set()

        def _add_attempt(
            label: str,
            tnved_value: str | None,
            code_value: str | None,
            removed_filters: List[str] | None = None,
        ) -> None:
            key = (tnved_value or "", code_value or "")
            if key in attempts_seen:
                return
            attempts_seen.add(key)
            attempts.append(
                {
                    "label": label,
                    "tnved": tnved_value,
                    "code": code_value,
                    "removed_filters": removed_filters or [],
                }
            )

        _add_attempt("original", tnved, code)

        tnved_digits = _strip_digits(tnved) if _is_simple_value(tnved) else ""
        if tnved_digits:
            original_len = len(tnved_digits)
            for length in (10, 8, 6, 4):
                if length < original_len and length >= 4:
                    candidate = tnved_digits[:length]
                    _add_attempt(f"tnved_prefix_{length}", candidate, code)

        code_digits = _strip_digits(code) if _is_simple_value(code) else ""
        if not tnved_digits and code_digits:
            for length in (10, 8, 6, 4):
                if len(code_digits) >= length and length >= 4:
                    candidate = code_digits[:length]
                    removed = ["code"] if code else []
                    _add_attempt(f"code_as_tnved_{length}", candidate, None, removed)

        if tnved_digits or tnved:
            _add_attempt("tnved_removed", None, code, ["tnved"])

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

        token_pattern = re.compile(r"[0-9A-Za-zА-Яа-яЁё№/\\\\\\*-]+")
        raw_tokens = token_pattern.findall(text.lower())
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
            source = (pair.get("source") or "").strip().lower()
            variant = (pair.get("variant") or "").strip().lower()
            if not variant:
                continue
            register_synonym_term(variant)
            for part in variant.split():
                register_synonym_term(part)
            if primary_token and source == primary_token:
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
