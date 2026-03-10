#!/usr/bin/env python3
"""
Обновляет таблицу registry.semantic_items, получая данные из сервиса semantic.

Конфигурация через переменные окружения:
  FORCE=1            - принудительно обновлять уже существующие embeddings
  DRY_RUN=1          - не писать в БД, только показать выборку
  SOURCE_FILES=a,b   - обрабатывать только записи с указанными source_file
  EMBED_IDS=1 2 3    - обрабатывать только указанные reestr.id
  LIMIT=N            - ограничить количество записей
  BATCH_SIZE=200     - размер батча (по умолчанию 200)
  SHARD_COUNT=1      - общее число шардов
  SHARD_INDEX=0      - номер текущего шарда
  SEMANTIC_URL       - URL сервиса semantic_normalize
"""

from __future__ import annotations

import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests

SEMANTIC_URL_DEFAULT = "http://semantic:8010/semantic_normalize"


@dataclass
class Config:
    force: bool = False
    dry_run: bool = False
    source_files: Optional[List[str]] = None
    ids: Optional[List[int]] = None
    limit: Optional[int] = None
    batch_size: int = 200
    shard_count: int = 1
    shard_index: int = 0
    semantic_url: str = SEMANTIC_URL_DEFAULT


def load_config() -> Config:
    def _bool(key: str) -> bool:
        return os.getenv(key, "0").strip() == "1"

    def _int(key: str, default: int) -> int:
        val = os.getenv(key, "").strip()
        return int(val) if val else default

    def _int_opt(key: str) -> Optional[int]:
        val = os.getenv(key, "").strip()
        return int(val) if val else None

    source_files_raw = os.getenv("SOURCE_FILES", "").strip()
    source_files = [f for f in source_files_raw.split(",") if f] if source_files_raw else None

    embed_ids_raw = os.getenv("EMBED_IDS", "").strip()
    ids = [int(i) for i in embed_ids_raw.split() if i] if embed_ids_raw else None

    cfg = Config(
        force=_bool("FORCE"),
        dry_run=_bool("DRY_RUN"),
        source_files=source_files,
        ids=ids,
        limit=_int_opt("LIMIT"),
        batch_size=_int("BATCH_SIZE", 200),
        shard_count=_int("SHARD_COUNT", 1),
        shard_index=_int("SHARD_INDEX", 0),
        semantic_url=os.getenv("SEMANTIC_URL", SEMANTIC_URL_DEFAULT),
    )

    if cfg.shard_count <= 0:
        sys.exit("SHARD_COUNT должен быть положительным числом.")
    if cfg.shard_index < 0 or cfg.shard_index >= cfg.shard_count:
        sys.exit("SHARD_INDEX должен быть в диапазоне [0, SHARD_COUNT-1].")

    return cfg


def db_connect() -> psycopg2.extensions.connection:
    cfg = {
        "host": os.getenv("POSTGRES_HOST", "postgres_registry"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "registry"),
        "user": os.getenv("POSTGRES_USER", "registry"),
        "password": os.getenv("POSTGRES_PASSWORD"),
    }
    missing = [key for key, value in cfg.items() if value is None]
    if missing:
        raise RuntimeError(f"Не заданы переменные окружения для подключения к БД: {', '.join(missing)}")
    return psycopg2.connect(**cfg)


def build_query(
    force: bool,
    source_files: Sequence[str] | None,
    limit: int | None,
    shard_count: int,
    shard_index: int,
    ids: Sequence[int] | None,
) -> Tuple[str, dict]:
    clauses = [
        "r.productname IS NOT NULL",
        "btrim(r.productname) <> ''",
    ]
    params: dict = {}
    if source_files:
        clauses.append("r.source_file = ANY(%(source_files)s)")
        params["source_files"] = list(source_files)
    if ids:
        clauses.append("r.id = ANY(%(ids)s)")
        params["ids"] = list(ids)
    if not force:
        clauses.append("s.reestr_id IS NULL")
    where = " AND ".join(clauses) if clauses else "TRUE"
    shard_clause = ""
    if shard_count > 1:
        shard_clause = " AND (r.id %% %(shard_count)s) = %(shard_index)s"
        params["shard_count"] = shard_count
        params["shard_index"] = shard_index

    query = f"""
        SELECT r.id, r.productname
        FROM registry.reestr AS r
        LEFT JOIN registry.semantic_items AS s
          ON s.reestr_id = r.id
        WHERE {where}{shard_clause}
        ORDER BY r.id
    """
    if limit:
        query += " LIMIT %(limit)s"
        params["limit"] = limit
    return query, params


def vector_literal(values: Iterable[float]) -> str:
    return "[" + ", ".join(str(float(v)) for v in values) + "]"


def fetch_rows(
    cursor: psycopg2.extensions.cursor,
    batch_size: int,
) -> Iterable[List[Tuple[int, str]]]:
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        yield rows


def upsert_embedding(
    cursor: psycopg2.extensions.cursor,
    reestr_id: int,
    normalized: str,
    synonyms: Sequence[str],
    embedding: Sequence[float],
) -> None:
    synonyms_json = psycopg2.extras.Json(
        list(synonyms),
        dumps=lambda obj: json.dumps(obj, ensure_ascii=False),
    )
    embedding_literal = vector_literal(embedding)
    cursor.execute(
        """
        INSERT INTO registry.semantic_items (reestr_id, normalized_text, synonyms, embedding)
        VALUES (%s, %s, %s::jsonb, %s::vector)
        ON CONFLICT (reestr_id) DO UPDATE
           SET normalized_text = EXCLUDED.normalized_text,
               synonyms        = EXCLUDED.synonyms,
               embedding       = EXCLUDED.embedding,
               updated_at      = now()
        """,
        (reestr_id, normalized, synonyms_json, embedding_literal),
    )


def fetch_semantic(
    session: requests.Session,
    url: str,
    text: str,
) -> dict:
    payload = {"text": text, "debug": False, "normalize": False}
    resp = session.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    cfg = load_config()

    if cfg.dry_run:
        sys.stderr.write("⚠️  Dry run: изменения записываться не будут.\n")

    conn = db_connect()
    conn.autocommit = False

    query, params = build_query(
        cfg.force,
        cfg.source_files,
        cfg.limit,
        cfg.shard_count,
        cfg.shard_index,
        cfg.ids,
    )
    total_selected = 0
    total_processed = 0
    total_errors = 0
    row_index = 0

    try:
        with conn.cursor() as select_cur:
            select_cur.execute(query, params)

            with conn.cursor() as write_cur, requests.Session() as session:
                batch_start = time.time()

                for rows in fetch_rows(select_cur, cfg.batch_size):
                    for reestr_id, productname in rows:
                        row_index += 1
                        total_selected += 1
                        original_text = (productname or "").strip()
                        if not original_text:
                            continue
                        if cfg.dry_run:
                            print(f"[DRY-RUN] id={reestr_id} name={original_text}", flush=True)
                            continue
                        savepoint = f"sp_{row_index}"
                        if not cfg.dry_run:
                            write_cur.execute(f"SAVEPOINT {savepoint}")
                        try:
                            data = fetch_semantic(session, cfg.semantic_url, original_text)
                            synonyms = data.get("synonyms_applied") or []
                            embedding = data.get("embedding")
                            if not isinstance(embedding, list):
                                raise ValueError("Ответ semantic не содержит embedding")
                            upsert_embedding(write_cur, reestr_id, original_text, synonyms, embedding)
                            if not cfg.dry_run:
                                write_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                            total_processed += 1
                        except Exception as exc:  # noqa: BLE001
                            total_errors += 1
                            if not cfg.dry_run:
                                write_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                                write_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                            sys.stderr.write(
                                f"❌ Ошибка обработки id={reestr_id}: {exc}\n"
                            )
                            continue

                    if not cfg.dry_run:
                        conn.commit()

                    elapsed = time.time() - batch_start
                    error_suffix = f" (ошибок: {total_errors})" if total_errors else ""
                    sys.stderr.write(
                        f"Обработано {total_processed} Время обработки: {elapsed:.1f} с.{error_suffix}\n"
                    )
                    batch_start = time.time()

    finally:
        conn.close()

    if total_errors:
        sys.stderr.write(f"Завершено с ошибками: {total_errors} записей.\n")
        return 1
    sys.stderr.write(f"Готово. Обработано {total_processed} записей.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
