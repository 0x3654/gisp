#!/usr/bin/env bash

set -euo pipefail

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
/app/restore_snapshot.sh "$SNAPSHOT_PATH"
echo "✅ Done."
