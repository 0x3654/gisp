#!/usr/bin/env bash

set -euo pipefail

SNAPSHOT_FILE="${1:-/app/registry_start_snapshot.sql.gz}"

: "${POSTGRES_HOST:=postgres_registry}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=registry}"
: "${POSTGRES_USER:=registry}"
: "${POSTGRES_PASSWORD:=}"

psql_exec() {
  PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
}

echo "🧹 Dropping staging tables..."
psql_exec <<'SQL'
DROP TABLE IF EXISTS registry.semantic_items CASCADE;
DROP TABLE IF EXISTS registry.load_log CASCADE;
DROP TABLE IF EXISTS registry.reestr CASCADE;
SQL

echo "📦 Importing snapshot..."
if [[ "$SNAPSHOT_FILE" == *.gz ]]; then
  gunzip -c "$SNAPSHOT_FILE" \
    | PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB"
else
  cat "$SNAPSHOT_FILE" \
    | PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
        -U "$POSTGRES_USER" -d "$POSTGRES_DB"
fi

echo "📈 Syncing sequences..."
psql_exec -c "SELECT setval('registry.reestr_id_seq', (SELECT max(id) FROM registry.reestr));"
psql_exec -c "SELECT setval('registry.load_log_id_seq', (SELECT max(id) FROM registry.load_log));"
