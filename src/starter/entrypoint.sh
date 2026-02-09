#!/usr/bin/env bash

set -euo pipefail

# Environment variables with defaults
: "${POSTGRES_HOST:=postgres_registry}"
: "${POSTGRES_PORT:=5432}"
: "${POSTGRES_DB:=registry}"
: "${POSTGRES_USER:=registry}"
: "${POSTGRES_PASSWORD:=}"
: "${SNAPSHOT_PATH:=/app/registry_start_snapshot.sql.gz}"
: "${STARTER_MAX_WAIT:=180}"
: "${FORCE:=0}"

wait_for_pg() {
  local elapsed=0
  echo "⏳ Waiting for PostgreSQL at ${POSTGRES_HOST}:${POSTGRES_PORT}..."
  until PGPASSWORD="$POSTGRES_PASSWORD" pg_isready -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" -d "$POSTGRES_DB" -U "$POSTGRES_USER" >/dev/null 2>&1; do
    sleep 2
    elapsed=$((elapsed + 2))
    if (( elapsed >= STARTER_MAX_WAIT )); then
      echo "❌ PostgreSQL is not ready after ${STARTER_MAX_WAIT}s."
      exit 1
    fi
  done
  echo "✅ PostgreSQL is ready."
}

existing_rows() {
  PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
    -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc \
    "SELECT count(*) FROM registry.reestr" 2>/dev/null || echo "0"
}

restore_snapshot() {
  local SNAPSHOT_FILE="${1:-/app/registry_start_snapshot.sql.gz}"

  psql_exec() {
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
      -U "$POSTGRES_USER" -d "$POSTGRES_DB" "$@"
  }

  echo "🧹 Ensuring schema exists and dropping staging tables..."
  psql_exec <<'SQL'
CREATE SCHEMA IF NOT EXISTS registry;
CREATE EXTENSION IF NOT EXISTS vector SCHEMA registry;
DROP TABLE IF EXISTS registry.semantic_items CASCADE;
DROP TABLE IF EXISTS registry.load_log CASCADE;
DROP TABLE IF EXISTS registry.reestr CASCADE;
SQL

  echo "📦 Importing snapshot..."
  if [[ "$SNAPSHOT_FILE" == *.gz ]]; then
    gunzip -c "$SNAPSHOT_FILE" | sed '/^\\restrict/d' \
      | PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
          -U "$POSTGRES_USER" -d "$POSTGRES_DB"
  else
    cat "$SNAPSHOT_FILE" | sed '/^\\restrict/d' \
      | PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -p "$POSTGRES_PORT" \
          -U "$POSTGRES_USER" -d "$POSTGRES_DB"
  fi

  echo "📈 Syncing sequences..."
  psql_exec -c "SELECT setval('registry.reestr_id_seq', (SELECT max(id) FROM registry.reestr));"
  psql_exec -c "SELECT setval('registry.load_log_id_seq', (SELECT max(id) FROM registry.load_log));"
}

# Main execution
wait_for_pg

ROWS="$(existing_rows)"
if [[ "$ROWS" =~ ^[0-9]+$ ]] && (( ROWS > 0 )) && [[ "$FORCE" != "1" ]]; then
  echo "ℹ️  registry.reestr already contains ${ROWS} rows. Skipping restore (set FORCE=1 to override)."
  exit 0
fi

if [[ "$FORCE" == "1" ]]; then
  echo "⚠️  FORCE=1 specified. Snapshot will overwrite existing data."
fi

if [[ ! -f "$SNAPSHOT_PATH" ]]; then
  echo "❌ Snapshot file not found at $SNAPSHOT_PATH"
  exit 1
fi

echo "⬇️  Restoring snapshot from $SNAPSHOT_PATH ..."
restore_snapshot "$SNAPSHOT_PATH"
echo "✅ Done."
