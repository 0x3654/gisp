-- Расширения, нужные слою hybrid-поиска.
-- Файл выполняется только при первичной инициализации БД (docker-entrypoint-initdb.d);
-- для существующих установок аналогичный CREATE EXTENSION зашит в раннер миграции (см. Шаг 1).
\set ON_ERROR_STOP on
\connect registry

CREATE EXTENSION IF NOT EXISTS pg_trgm;
