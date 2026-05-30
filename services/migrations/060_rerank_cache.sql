-- Тот же кеш, отдельный файл для существующих БД (запускается через миграционный раннер).
\set ON_ERROR_STOP on

CREATE TABLE IF NOT EXISTS registry.rerank_cache (
    cache_key text PRIMARY KEY,
    query_text text NOT NULL,
    candidate_ids bigint[] NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rerank_cache_updated_at_idx
    ON registry.rerank_cache (updated_at);
