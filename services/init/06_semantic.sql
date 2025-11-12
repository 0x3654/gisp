SET search_path TO registry;

CREATE TABLE IF NOT EXISTS registry.semantic_items (
  reestr_id       BIGINT PRIMARY KEY REFERENCES registry.reestr(id) ON DELETE CASCADE,
  normalized_text TEXT NOT NULL,
  synonyms        JSONB,
  embedding       VECTOR(384) NOT NULL,
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS semantic_items_embedding_idx
  ON registry.semantic_items
  USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);
