-- Колонки и индексы для hybrid-поиска (FTS + триграммы).
-- Этот файл выполняется ТОЛЬКО при первичной инициализации БД (docker-entrypoint-initdb.d).
-- Для уже работающих установок используется services/migrations/051_hybrid_backfill.sql + раннер.
\set ON_ERROR_STOP on
\connect registry

-- На свежей пустой таблице GENERATED ALWAYS без проблем с locks.
ALTER TABLE registry.reestr
  ADD COLUMN IF NOT EXISTS search_tsv tsvector
    GENERATED ALWAYS AS (
      setweight(to_tsvector('russian', coalesce(productname, '')), 'A') ||
      setweight(to_tsvector('russian', coalesce(nameoforg, '')), 'C')
    ) STORED,
  ADD COLUMN IF NOT EXISTS productname_normalized text
    GENERATED ALWAYS AS (
      regexp_replace(lower(coalesce(productname, '')), '[\s\-\/×x*]+', '', 'g')
    ) STORED;

CREATE INDEX IF NOT EXISTS idx_reestr_search_tsv
  ON registry.reestr USING gin(search_tsv);

CREATE INDEX IF NOT EXISTS idx_reestr_productname_trgm
  ON registry.reestr USING gin(productname_normalized gin_trgm_ops);
