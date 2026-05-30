-- Hybrid-поиск: миграция для существующих установок.
-- Стратегия: не использовать GENERATED ALWAYS (требует rewrite таблицы под ACCESS EXCLUSIVE),
-- вместо этого — обычные nullable-колонки + BEFORE-trigger.
-- Backfill и CREATE INDEX CONCURRENTLY делает раннер (run_migration.py) уже отдельными транзакциями.
\set ON_ERROR_STOP on

CREATE EXTENSION IF NOT EXISTS pg_trgm;

ALTER TABLE registry.reestr
  ADD COLUMN IF NOT EXISTS search_tsv tsvector;

ALTER TABLE registry.reestr
  ADD COLUMN IF NOT EXISTS productname_normalized text;

CREATE OR REPLACE FUNCTION registry.reestr_search_update() RETURNS trigger AS $$
BEGIN
  NEW.search_tsv :=
    setweight(to_tsvector('russian', coalesce(NEW.productname, '')), 'A') ||
    setweight(to_tsvector('russian', coalesce(NEW.nameoforg, '')), 'C');
  NEW.productname_normalized :=
    regexp_replace(lower(coalesce(NEW.productname, '')), '[\s\-\/×x*]+', '', 'g');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS reestr_search_trg ON registry.reestr;
CREATE TRIGGER reestr_search_trg
  BEFORE INSERT OR UPDATE OF productname, nameoforg
  ON registry.reestr
  FOR EACH ROW EXECUTE FUNCTION registry.reestr_search_update();
