#!/usr/bin/env python3
"""
Обновляет таблицу registry.semantic_items, получая данные из semantic_service.

Использование:
  python update_embeddings.py [--source-file FILE] [--limit N] [--batch-size N] [--force] [--dry-run]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Iterable, List, Sequence, Tuple

import psycopg2
import psycopg2.extras
import requests

_SCRIPT_PATH = Path(__file__).resolve()
_repo_hint = os.getenv("REPO_ROOT")
if _repo_hint:
    REPO_ROOT = Path(_repo_hint).resolve()
else:
    parents = _SCRIPT_PATH.parents
    REPO_ROOT = parents[2] if len(parents) >= 3 else _SCRIPT_PATH.parent

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

SEMANTIC_URL_DEFAULT = "http://semantic_service:8010/semantic_normalize"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Заполняет/обновляет embeddings в registry.semantic_items.")
    parser.add_argument(
        "--source-file",
        dest="source_files",
        action="append",
        help="Обрабатывать только записи с указанным source_file. Флаг можно повторять.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Ограничить количество обрабатываемых записей.",
    )
    parser.add_argument(
        "--id",
        dest="ids",
        action="append",
        type=int,
        help="Обрабатывать только указанные идентификаторы reestr.id (флаг можно повторять).",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Сколько записей обрабатывать за один запрос к БД (по умолчанию 200).",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        default=1,
        help="Разбить выборку на указанное число шардов и обрабатывать только текущий (--shard-index).",
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        default=0,
        help="Номер шарда (0..shard_count-1), для параллельного запуска нескольких экземпляров.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Принудительно обновлять embeddings даже если запись уже есть в registry.semantic_items.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не записывать изменения в БД, только вывести список выбранных записей.",
    )
    parser.add_argument(
        "--semantic-url",
        default=os.getenv("SEMANTIC_URL", SEMANTIC_URL_DEFAULT),
        help="URL сервиса semantic_normalize (по умолчанию %(default)s или переменная окружения SEMANTIC_URL).",
    )
    args = parser.parse_args()
    if args.shard_count <= 0:
        parser.error("--shard-count должен быть положительным числом.")
    if args.shard_index < 0 or args.shard_index >= args.shard_count:
        parser.error("--shard-index должен быть в диапазоне [0, shard_count-1].")
    return args


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
    payload = {"text": text, "debug": False}
    resp = session.post(url, json=payload, timeout=20)
    resp.raise_for_status()
    return resp.json()


def main() -> int:
    args = parse_args()

    if args.dry_run:
        sys.stderr.write("⚠️  Dry run: изменения записываться не будут.\n")

    conn = db_connect()
    conn.autocommit = False

    query, params = build_query(
        args.force,
        args.source_files,
        args.limit,
        args.shard_count,
        args.shard_index,
        args.ids,
    )
    sys.stderr.write(
        f"Выборка записей: force={args.force}, source_files={args.source_files}, limit={args.limit}, ids={args.ids}.\n"
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

                for rows in fetch_rows(select_cur, args.batch_size):
                    for reestr_id, productname in rows:
                        row_index += 1
                        total_selected += 1
                        productname = productname.strip()
                        if not productname:
                            continue
                        if args.dry_run:
                            print(f"[DRY-RUN] id={reestr_id} name={productname}", flush=True)
                            continue
                        savepoint = f"sp_{row_index}"
                        if not args.dry_run:
                            write_cur.execute(f"SAVEPOINT {savepoint}")
                        try:
                            data = fetch_semantic(session, args.semantic_url, productname)
                            normalized = (data.get("normalized") or "").strip()
                            synonyms = data.get("synonyms_applied") or []
                            embedding = data.get("embedding")
                            if not isinstance(embedding, list):
                                raise ValueError("Ответ semantic_service не содержит embedding")
                            upsert_embedding(write_cur, reestr_id, normalized, synonyms, embedding)
                            if not args.dry_run:
                                write_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                            total_processed += 1
                        except Exception as exc:  # noqa: BLE001
                            total_errors += 1
                            if not args.dry_run:
                                write_cur.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                                write_cur.execute(f"RELEASE SAVEPOINT {savepoint}")
                            sys.stderr.write(
                                f"❌ Ошибка обработки id={reestr_id}: {exc}\n"
                            )
                            continue

                    if not args.dry_run:
                        conn.commit()

                    elapsed = time.time() - batch_start
                    sys.stderr.write(
                        f"Обработано {total_processed} / отобрано {total_selected} записей "
                        f"(ошибок: {total_errors}). Время обработки партии: {elapsed:.1f} с.\n"
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
