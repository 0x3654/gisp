CREATE TABLE IF NOT EXISTS registry.semantic_query_cache (
    query_hash text PRIMARY KEY,
    original_text text NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS semantic_query_cache_updated_at_idx
    ON registry.semantic_query_cache (updated_at);
