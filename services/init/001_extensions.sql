-- Ensure the schema exists for pgvector objects
CREATE SCHEMA IF NOT EXISTS registry;

-- Create or move pgvector into the registry schema so dumps referencing registry.vector work
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    EXECUTE 'ALTER EXTENSION vector SET SCHEMA registry';
  ELSE
    EXECUTE 'CREATE EXTENSION vector WITH SCHEMA registry';
  END IF;
END
$$;

CREATE EXTENSION IF NOT EXISTS pg_prewarm;

\connect registry

CREATE SCHEMA IF NOT EXISTS registry;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'vector') THEN
    EXECUTE 'ALTER EXTENSION vector SET SCHEMA registry';
  ELSE
    EXECUTE 'CREATE EXTENSION vector WITH SCHEMA registry';
  END IF;
END
$$;

CREATE EXTENSION IF NOT EXISTS pg_prewarm;
