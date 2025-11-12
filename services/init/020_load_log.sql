SET search_path TO registry;

CREATE TABLE IF NOT EXISTS registry.load_log (
  id bigserial primary key,
  file_name text not null,
  file_size bigint not null,
  file_sha256 text not null,
  loaded_at timestamptz not null default now(),
  rows_inserted integer not null,
  unique(file_name, file_sha256)
);

-- вспомогательная функция: есть ли уже запись
CREATE OR REPLACE FUNCTION registry.is_loaded(p_name text, p_sha text)
RETURNS boolean LANGUAGE sql STABLE AS $$
  SELECT EXISTS(
    SELECT 1 FROM registry.load_log
    WHERE file_name = p_name AND file_sha256 = p_sha
  );
$$;
