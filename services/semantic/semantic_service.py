import copy
import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

import psycopg2
import psycopg2.extras
import pymorphy3
from fastapi import FastAPI, HTTPException
from nltk.stem.snowball import RussianStemmer
from pydantic import BaseModel

from model_loader import MODEL_NAME, get_model

TOKEN_RE = re.compile(r"[a-zA-Zа-яА-Я0-9]+")
SYNS_FILE = Path(__file__).with_name("synonyms.json")
CACHE_TTL_SECONDS = int(os.getenv("SEMANTIC_CACHE_TTL_SECONDS", "604800"))
_CACHE_CONN = None


class SemanticRequest(BaseModel):
    text: str
    debug: bool = False
    normalize: bool = True
    apply_synonyms: bool = False


def load_synonyms(path: Path) -> Dict[str, List[str]]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid synonyms file: {exc}") from exc

    normalized: Dict[str, List[str]] = {}
    for key, value in raw.items():
        if not isinstance(key, str):
            continue
        canon = key.strip().lower()
        if not canon:
            continue
        if isinstance(value, list):
            variants = [v.strip() for v in value if isinstance(v, str) and v.strip()]
        else:
            variants = []
        normalized[canon] = variants
    return normalized


def normalize_token(token: str, morph: pymorphy3.MorphAnalyzer) -> str:
    if token.isdigit():
        return token
    parsed = morph.parse(token)
    if parsed:
        return parsed[0].normal_form
    return token


def collect_synonym_expansions(
    text_lower: str, synonyms: Dict[str, List[str]]
) -> Tuple[List[str], List[str]]:
    expansions: List[str] = []
    applied: List[str] = []
    seen: set[str] = set()

    for canonical, variants in synonyms.items():
        if not canonical:
            continue
        if canonical not in text_lower:
            continue
        if canonical not in seen:
            expansions.append(canonical)
            applied.append(f"{canonical}→{canonical}")
            seen.add(canonical)
        for variant in variants:
            variant_clean = variant.strip()
            if not variant_clean:
                continue
            variant_lower = variant_clean.lower()
            if variant_lower in seen:
                continue
            expansions.append(variant_clean)
            applied.append(f"{canonical}→{variant_clean}")
            seen.add(variant_lower)
    return expansions, applied


def get_embedding(text: str) -> List[float]:
    model = get_model()
    embedding = model.encode([text], convert_to_numpy=True, show_progress_bar=False)[0]
    return embedding.astype(float).tolist()


def _synonyms_version_hash(synonyms: Dict[str, List[str]]) -> str:
    payload = json.dumps(synonyms, ensure_ascii=False, sort_keys=True)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()


def _get_cache_conn():
    global _CACHE_CONN
    if _CACHE_CONN and _CACHE_CONN.closed == 0:
        return _CACHE_CONN
    conn = psycopg2.connect(
        dbname=os.getenv("POSTGRES_DB", "registry"),
        user=os.getenv("POSTGRES_USER", "registry"),
        password=os.getenv("POSTGRES_PASSWORD"),
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
    )
    conn.autocommit = True
    _CACHE_CONN = conn
    return conn


def _reset_cache_conn():
    global _CACHE_CONN
    if _CACHE_CONN:
        try:
            _CACHE_CONN.close()
        except Exception:
            pass
    _CACHE_CONN = None


def _cache_enabled() -> bool:
    return CACHE_TTL_SECONDS > 0


def _cache_lookup(key: str) -> Dict[str, object] | None:
    if not _cache_enabled():
        return None
    try:
        conn = _get_cache_conn()
    except Exception:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT payload, updated_at
                FROM registry.semantic_query_cache
                WHERE query_hash = %s
                """,
                (key,),
            )
            row = cur.fetchone()
    except psycopg2.Error:
        _reset_cache_conn()
        return None
    if not row:
        return None
    payload, updated_at = row
    if not isinstance(updated_at, datetime):
        return None
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    now_utc = datetime.now(timezone.utc)
    if now_utc - updated_at > timedelta(seconds=CACHE_TTL_SECONDS):
        return None
    return copy.deepcopy(payload)


def _cache_store(key: str, payload: Dict[str, object], *, original_text: str) -> None:
    if not _cache_enabled():
        return
    try:
        conn = _get_cache_conn()
    except Exception:
        return
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO registry.semantic_query_cache (query_hash, original_text, payload, updated_at)
                VALUES (%s, %s, %s, now())
                ON CONFLICT (query_hash)
                DO UPDATE SET payload = EXCLUDED.payload,
                              original_text = EXCLUDED.original_text,
                              updated_at = now()
                """,
                (key, original_text, psycopg2.extras.Json(payload)),
            )
    except psycopg2.Error:
        _reset_cache_conn()


def _make_cache_key(
    *,
    original: str,
    normalized: str,
    normalize_mode: bool,
    apply_synonyms: bool,
    synonyms_version: str,
) -> str:
    base = json.dumps(
        {
            "original": original.strip(),
            "normalized": normalized,
            "normalize": normalize_mode,
            "apply_synonyms": apply_synonyms,
            "synonyms_version": synonyms_version,
            "model": MODEL_NAME,
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def build_response(
    tokens: List[str],
    normalized_tokens: List[str],
    stems: List[str],
    synonyms_applied: List[str],
    debug_requested: bool,
    *,
    original_text: str,
    apply_synonyms: bool,
    synonyms_version: str,
    synonym_expansions: List[str] | None = None,
) -> Dict[str, object]:
    normalized_unique: List[str] = []
    seen = set()
    for token in normalized_tokens:
        if token not in seen:
            normalized_unique.append(token)
            seen.add(token)

    expansions = [exp for exp in (synonym_expansions or []) if exp]
    display_parts = normalized_unique.copy()
    if expansions:
        display_parts.extend(expansions)
    normalized_text = " ^ ".join(display_parts)

    embedding_tokens = normalized_unique.copy()
    embedding_tokens.extend(expansions)
    if not embedding_tokens:
        embedding_tokens = tokens
    embedding_input = " ".join(embedding_tokens).strip()
    cache_key = _make_cache_key(
        original=original_text,
        normalized=embedding_input,
        normalize_mode=True,
        apply_synonyms=apply_synonyms,
        synonyms_version=synonyms_version,
    )
    cached = _cache_lookup(cache_key)
    if cached:
        response = cached
    else:
        embedding = get_embedding(embedding_input)
        response = {
            "normalized": normalized_text,
            "embedding": embedding,
            "synonyms_applied": synonyms_applied,
            "synonym_expansions": expansions,
        }
        _cache_store(cache_key, response, original_text=original_text)

    if debug_requested:
        response = copy.deepcopy(response)
        response["debug"] = {
            "tokens": tokens,
            "stems": stems,
            "model": MODEL_NAME,
        }

    return response


def create_app() -> FastAPI:
    synonyms = load_synonyms(SYNS_FILE)
    synonyms_version = _synonyms_version_hash(synonyms)
    morph = pymorphy3.MorphAnalyzer()
    stemmer = RussianStemmer()
    app_instance = FastAPI()

    @app_instance.post("/semantic_normalize")
    def semantic_normalize(req: SemanticRequest) -> Dict[str, object]:
        text = req.text.strip()
        if not text:
            raise HTTPException(status_code=400, detail="Text must not be empty.")

        text_lower = text.lower()
        raw_tokens = TOKEN_RE.findall(text_lower)
        synonym_expansions: List[str] = []
        synonym_pairs: List[str] = []
        if req.apply_synonyms and raw_tokens:
            synonym_expansions, synonym_pairs = collect_synonym_expansions(
                text_lower, synonyms
            )

        if not req.normalize:
            if synonym_expansions:
                embedding_input = text + " " + " ".join(synonym_expansions)
            else:
                embedding_input = text

            cache_key = _make_cache_key(
                original=text,
                normalized=embedding_input,
                normalize_mode=False,
                apply_synonyms=req.apply_synonyms,
                synonyms_version=synonyms_version,
            )
            cached = _cache_lookup(cache_key)
            if cached:
                response = cached
            else:
                embedding = get_embedding(embedding_input)
                response = {
                    "normalized": text,
                    "embedding": embedding,
                    "synonyms_applied": synonym_pairs,
                    "synonym_expansions": synonym_expansions,
                    "embedding_augmented": synonym_expansions,
                }
                _cache_store(cache_key, response, original_text=text)
            if req.debug:
                response = copy.deepcopy(response)
                response["debug"] = {
                    "mode": "raw",
                    "model": MODEL_NAME,
                    "tokens": raw_tokens,
                }
            return response
        stems: List[str] = []
        normalized_tokens: List[str] = []

        for raw in raw_tokens:
            normalized = normalize_token(raw, morph)
            normalized_tokens.append(normalized)
            stems.append(stemmer.stem(raw))

        return build_response(
            tokens=raw_tokens,
            normalized_tokens=normalized_tokens,
            stems=stems,
            synonyms_applied=synonym_pairs,
            debug_requested=req.debug,
            original_text=text,
            apply_synonyms=req.apply_synonyms,
            synonyms_version=synonyms_version,
            synonym_expansions=synonym_expansions,
        )

    return app_instance


app = create_app()
