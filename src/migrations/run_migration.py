#!/usr/bin/env python3
"""Раннер миграций для hybrid-поиска.

Шаги:
  1. Применяет DDL из services/migrations/*.sql (идемпотентно)
  2. Бэкфилит search_tsv / productname_normalized батчами (нагрузка на trigger мала, но
     явный UPDATE в батчах позволяет видеть прогресс и не держит один долгий транзакционный лок)
  3. Создаёт gin-индексы CONCURRENTLY (вне транзакции, минимум блокировок)
  4. ANALYZE

Конфиг через env:
  MIGRATION_FILES     — через запятую, по умолчанию "051_hybrid_backfill.sql"
  BATCH_SIZE          — размер батча для backfill (по умолчанию 10000)
  ONLY_MISSING        — 1 (по умолчанию): обновлять только строки с NULL search_tsv
  SKIP_BACKFILL       — 1: пропустить шаг backfill (только DDL + индексы)
  SKIP_INDEXES        — 1: пропустить CREATE INDEX CONCURRENTLY
"""
from __future__ import annotations

import os
import sys
import time
from pathlib import Path

import psycopg2
from psycopg2 import sql

MIGRATIONS_DIR = Path("/migrations")
DEFAULT_MIGRATION_FILES = ["051_hybrid_backfill.sql"]

INDEXES = [
    (
        "idx_reestr_search_tsv",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reestr_search_tsv "
        "ON registry.reestr USING gin(search_tsv)",
    ),
    (
        "idx_reestr_productname_trgm",
        "CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_reestr_productname_trgm "
        "ON registry.reestr USING gin(productname_normalized gin_trgm_ops)",
    ),
]


def _env_int(key: str, default: int) -> int:
    raw = os.getenv(key, "").strip()
    return int(raw) if raw else default


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


def db_connect(autocommit: bool = False) -> psycopg2.extensions.connection:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres_registry"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "registry"),
        user=os.getenv("POSTGRES_USER", "registry"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )
    conn.autocommit = autocommit
    return conn


def _log(msg: str) -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _strip_psql_metacommands(ddl: str) -> str:
    """Удаляет строки, начинающиеся с \\ (мета-команды psql вроде \\set, \\connect).
    psycopg2 их не понимает, а для контейнерного раннера они и не нужны.
    """
    cleaned: list[str] = []
    for line in ddl.splitlines():
        if line.lstrip().startswith("\\"):
            continue
        cleaned.append(line)
    return "\n".join(cleaned)


def apply_ddl(files: list[str]) -> None:
    for fname in files:
        path = MIGRATIONS_DIR / fname
        if not path.exists():
            raise FileNotFoundError(f"Migration file not found: {path}")
        _log(f"applying DDL: {fname}")
        ddl = _strip_psql_metacommands(path.read_text(encoding="utf-8"))
        with db_connect(autocommit=False) as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()
        _log(f"  {fname} ✓")


def backfill(batch_size: int, only_missing: bool) -> None:
    _log(f"backfill: batch_size={batch_size}, only_missing={only_missing}")
    where = "WHERE search_tsv IS NULL" if only_missing else ""
    count_sql = f"SELECT count(*) FROM registry.reestr {where}"
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(count_sql)
            total = cur.fetchone()[0]
    _log(f"  rows to update: {total}")
    if total == 0:
        _log("  nothing to backfill, skipping")
        return

    processed = 0
    start = time.perf_counter()
    # Берём ID-чанк -> обновляем -> коммитим. Trigger пересчитает обе колонки.
    # UPDATE с тем же значением — лёгкий, но мы используем productname=productname,
    # чтобы trigger гарантированно сработал (BEFORE UPDATE OF productname,nameoforg).
    while True:
        with db_connect() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    WITH batch AS (
                        SELECT id FROM registry.reestr
                        WHERE (%s = FALSE) OR search_tsv IS NULL
                        ORDER BY id
                        LIMIT %s OFFSET 0
                        FOR UPDATE SKIP LOCKED
                    )
                    UPDATE registry.reestr r
                    SET productname = r.productname
                    FROM batch b
                    WHERE r.id = b.id
                    RETURNING r.id
                    """,
                    (only_missing, batch_size),
                )
                updated = cur.rowcount
            conn.commit()
        processed += updated
        elapsed = time.perf_counter() - start
        rate = processed / elapsed if elapsed > 0 else 0
        pct = (processed / total) * 100 if total else 100
        _log(
            f"  batch processed={updated}  total={processed}/{total} "
            f"({pct:.1f}%)  rate={rate:.0f} rows/s"
        )
        if updated < batch_size:
            break

    _log(f"backfill done: {processed} rows in {time.perf_counter() - start:.1f}s")


def create_indexes() -> None:
    _log("creating gin indexes CONCURRENTLY")
    for name, ddl in INDEXES:
        _log(f"  {name} ...")
        # CONCURRENTLY требует autocommit
        conn = db_connect(autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(ddl)
        finally:
            conn.close()
        _log(f"  {name} ✓")


def analyze() -> None:
    _log("ANALYZE registry.reestr")
    conn = db_connect(autocommit=True)
    try:
        with conn.cursor() as cur:
            cur.execute("ANALYZE registry.reestr")
    finally:
        conn.close()
    _log("ANALYZE ✓")


def main() -> int:
    files_env = os.getenv("MIGRATION_FILES", "").strip()
    files = [f.strip() for f in files_env.split(",") if f.strip()] or DEFAULT_MIGRATION_FILES
    batch_size = _env_int("BATCH_SIZE", 10000)
    only_missing = _env_bool("ONLY_MISSING", True)
    skip_backfill = _env_bool("SKIP_BACKFILL", False)
    skip_indexes = _env_bool("SKIP_INDEXES", False)

    _log(
        f"start: files={files}, batch_size={batch_size}, only_missing={only_missing}, "
        f"skip_backfill={skip_backfill}, skip_indexes={skip_indexes}"
    )

    apply_ddl(files)

    if not skip_backfill:
        backfill(batch_size, only_missing)
    else:
        _log("backfill skipped via SKIP_BACKFILL")

    if not skip_indexes:
        create_indexes()
    else:
        _log("index creation skipped via SKIP_INDEXES")

    analyze()
    _log("migration complete")
    return 0


if __name__ == "__main__":
    sys.exit(main())
