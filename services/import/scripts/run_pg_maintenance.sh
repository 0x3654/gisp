#!/usr/bin/env bash

set -euo pipefail

ENV_DUMP="/etc/environment"
if [[ -f "$ENV_DUMP" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "$ENV_DUMP"
  set +a
fi

: "${POSTGRES_HOST:=postgres_registry}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_USER:=registry}"
: "${POSTGRES_PASSWORD:=registry}"
: "${POSTGRES_DB:=registry}"
: "${LOG_DIR:=/var/log/registry}"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/maintenance_$(date '+%F_%H-%M').log"

exec >>"$LOG_FILE" 2>&1
echo "=============================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Начало обслуживания БД"
echo "=============================================="

export PGPASSWORD="$POSTGRES_PASSWORD"
PSQL_BASE=(psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1)

{
cat <<'SQL'
SET client_min_messages TO NOTICE;
VACUUM (ANALYZE, VERBOSE) registry.reestr;
VACUUM (ANALYZE, VERBOSE) registry.semantic_items;
VACUUM (ANALYZE, VERBOSE) registry.semantic_query_cache;
SQL
} | "${PSQL_BASE[@]}"

{
cat <<'SQL'
SELECT now() AS ts,
       relname,
       pg_size_pretty(pg_total_relation_size(relname)) AS total_size
FROM   (VALUES ('registry.reestr'::regclass),
               ('registry.semantic_items'::regclass),
               ('registry.semantic_query_cache'::regclass)) AS t(relname);
SQL
} | "${PSQL_BASE[@]}"

{
cat <<'SQL'
SELECT 'registry.semantic_items' AS relation, pg_prewarm('registry.semantic_items', 'buffer');
SELECT 'registry.reestr' AS relation, pg_prewarm('registry.reestr', 'buffer');
SQL
} | "${PSQL_BASE[@]}" || echo "[WARN] pg_prewarm вызов завершился с ошибкой (возможно, не загружен модуль)."

unset PGPASSWORD

echo ""
echo "=============================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Обслуживание БД завершено."
echo "=============================================="
