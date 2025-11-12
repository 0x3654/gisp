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
import pymorphy2
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
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid synonyms file: {exc}") from exc


def build_synonym_lookup(synonyms: Dict[str, List[str]]) -> Dict[str, str]:
    lookup: Dict[str, str] = {}
    for canonical, variants in synonyms.items():
        canonical_lower = canonical.lower()
        lookup[canonical_lower] = canonical_lower
        for variant in variants:
            lookup[variant.lower()] = canonical_lower
    return lookup


def normalize_token(token: str, morph: pymorphy2.MorphAnalyzer) -> str:
    if token.isdigit():
        return token
    parsed = morph.parse(token)
    if parsed:
        return parsed[0].normal_form
    return token


def apply_synonym(
    token: str, original: str, lookup: Dict[str, str]
) -> Tuple[str, str | None]:
    normalized_token = token.lower()
    original_token = original.lower()

    replacement = lookup.get(normalized_token) or lookup.get(original_token)
    if replacement and replacement != normalized_token:
        return replacement, f"{token}→{replacement}"
    return normalized_token if replacement else token, None


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
) -> Dict[str, object]:
    normalized_unique: List[str] = []
    seen = set()
    for token in normalized_tokens:
        if token not in seen:
            normalized_unique.append(token)
            seen.add(token)

    normalized_text = " ^ ".join(normalized_unique)
    embedding_input = normalized_text or " ".join(tokens)
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
    synonym_lookup = build_synonym_lookup(synonyms)
    synonyms_version = _synonyms_version_hash(synonyms)
    morph = pymorphy2.MorphAnalyzer()
    stemmer = RussianStemmer()
    app_instance = FastAPI()

    @app_instance.post("/semantic_normalize")
    def semantic_normalize(req: SemanticRequest) -> Dict[str, object]:
        text = req.text.strip()
        if not text:
            raise HTTPException(status_code=400, detail="Text must not be empty.")

        raw_tokens = TOKEN_RE.findall(text.lower())

        if not req.normalize:
            augmented_tokens: List[str] = []
            synonym_pairs: List[Dict[str, str]] = []
            synonym_expansions: List[str] = []

            if req.apply_synonyms and raw_tokens:
                seen_aug: set[str] = set()
                for raw in raw_tokens:
                    normalized = normalize_token(raw, morph)
                    normalized_lower = normalized.lower()
                    original_lower = raw.lower()

                    canonical = (
                        synonym_lookup.get(normalized_lower)
                        or synonym_lookup.get(original_lower)
                    )
                    if canonical:
                        variants = synonyms.get(canonical, [])
                    else:
                        variants = []

                    if canonical and canonical not in seen_aug and canonical != original_lower:
                        augmented_tokens.append(canonical)
                        synonym_expansions.append(canonical)
                        synonym_pairs.append(
                            {"source": raw, "variant": canonical, "type": "canonical"}
                        )
                        seen_aug.add(canonical)

                    for variant in variants:
                        variant_clean = variant.strip()
                        if not variant_clean:
                            continue
                        variant_lower = variant_clean.lower()
                        if variant_lower in {original_lower, canonical}:
                            continue
                        if variant_lower in seen_aug:
                            continue
                        augmented_tokens.append(variant_clean)
                        synonym_expansions.append(variant_clean)
                        synonym_pairs.append(
                            {
                                "source": raw,
                                "variant": variant_clean,
                                "type": "synonym",
                            }
                        )
                        seen_aug.add(variant_lower)

            if augmented_tokens:
                embedding_input = text + " " + " ".join(augmented_tokens)
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
                    "embedding_augmented": augmented_tokens,
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
        synonyms_applied: List[str] = []

        for raw in raw_tokens:
            normalized = normalize_token(raw, morph)
            normalized_with_synonym, replacement = apply_synonym(
                normalized, raw, synonym_lookup
            )
            normalized_tokens.append(normalized_with_synonym)
            if replacement:
                synonyms_applied.append(replacement)
            stems.append(stemmer.stem(raw))

        return build_response(
            tokens=raw_tokens,
            normalized_tokens=normalized_tokens,
            stems=stems,
            synonyms_applied=synonyms_applied,
            debug_requested=req.debug,
            original_text=text,
            apply_synonyms=req.apply_synonyms,
            synonyms_version=synonyms_version,
        )

    return app_instance


app = create_app()
