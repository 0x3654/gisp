#!/usr/bin/env bash

set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE="${ENV_FILE:-$REPO_ROOT/.env}"
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

COMPOSE_CMD=${COMPOSE_CMD:-docker compose}
PG_SERVICE=${PG_SERVICE:-postgres_registry}
PGUSER=${PGUSER:-${POSTGRES_USER:-registry}}
PGDATABASE=${PGDATABASE:-${POSTGRES_DB:-registry}}
DUMP_FILE=${1:-dumps/registry_start_snapshot.sql.gz}

usage() {
  cat <<'EOF'
Usage: scripts/restore_start_snapshot.sh [dump_path]

Restores the canonical snapshot (registry.reestr, registry.load_log, registry.semantic_items)
into the running postgres_registry container. All related tables are dropped before import.

Arguments:
  dump_path  Optional path to the snapshot file (plain .sql or .sql.gz). Defaults to dumps/registry_start_snapshot.sql.gz.

Environment overrides:
  COMPOSE_CMD   docker compose command (default: "docker compose")
  PG_SERVICE    Service name (default: postgres_registry)
  PGUSER        Database user (default: registry)
  PGDATABASE    Database name (default: registry)
EOF
}

if [[ ${1:-} == "-h" || ${1:-} == "--help" ]]; then
  usage
  exit 0
fi

if [[ ! -f "$DUMP_FILE" ]]; then
  echo "[ERROR] Snapshot file not found: $DUMP_FILE" >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "[ERROR] docker binary is missing. Install Docker and try again." >&2
  exit 1
fi

if ! $COMPOSE_CMD ps >/dev/null 2>&1; then
  echo "[ERROR] '$COMPOSE_CMD ps' failed. Ensure you are in the repo root and Docker Compose is installed." >&2
  exit 1
fi

if [[ -z "$($COMPOSE_CMD ps -q "$PG_SERVICE" 2>/dev/null)" ]]; then
  echo "[ERROR] Service '$PG_SERVICE' is not running. Start it (docker compose up postgres_registry) and retry." >&2
  exit 1
fi

psql_exec() {
  $COMPOSE_CMD exec -T "$PG_SERVICE" \
    psql -U "$PGUSER" -d "$PGDATABASE" "$@"
}

echo "⚠️  Dropping registry tables before restore..."
psql_exec <<'SQL'
DROP TABLE IF EXISTS registry.semantic_items CASCADE;
DROP TABLE IF EXISTS registry.load_log CASCADE;
DROP TABLE IF EXISTS registry.reestr CASCADE;
SQL

if [[ "$DUMP_FILE" == *.gz ]]; then
  echo "⬇️  Restoring snapshot from $DUMP_FILE (gzipped)..."
  gunzip -c "$DUMP_FILE" \
    | $COMPOSE_CMD exec -T "$PG_SERVICE" \
        psql -U "$PGUSER" -d "$PGDATABASE"
else
  echo "⬇️  Restoring snapshot from $DUMP_FILE..."
  cat "$DUMP_FILE" \
    | $COMPOSE_CMD exec -T "$PG_SERVICE" \
        psql -U "$PGUSER" -d "$PGDATABASE"
fi

echo "🔁 Syncing sequences..."
psql_exec -c "SELECT setval('registry.reestr_id_seq', (SELECT max(id) FROM registry.reestr));"
psql_exec -c "SELECT setval('registry.load_log_id_seq', (SELECT max(id) FROM registry.load_log));"

echo "✅ Snapshot restore completed."
