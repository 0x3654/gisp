#!/usr/bin/env bash
set -euo pipefail

ARGS=()

if [[ "${FORCE:-0}" == "1" ]]; then
  ARGS+=(--force)
fi

if [[ "${DRY_RUN:-0}" == "1" ]]; then
  ARGS+=(--dry-run)
fi

if [[ -n "${SEMANTIC_URL_OVERRIDE:-}" ]]; then
  ARGS+=(--semantic-url "${SEMANTIC_URL_OVERRIDE}")
fi

if [[ -n "${SOURCE_FILES:-}" ]]; then
  IFS=',' read -r -a SF_ARR <<< "${SOURCE_FILES}"
  for sf in "${SF_ARR[@]}"; do
    if [[ -n "${sf}" ]]; then
      ARGS+=(--source-file "${sf}")
    fi
  done
fi

if [[ -n "${EMBED_IDS:-}" ]]; then
  for id in ${EMBED_IDS}; do
    ARGS+=(--id "${id}")
  done
fi

if [[ -n "${LIMIT:-}" ]]; then
  ARGS+=(--limit "${LIMIT}")
fi

ARGS+=(--batch-size "${BATCH_SIZE:-400}")
ARGS+=(--shard-count "${SHARD_COUNT:-1}")
ARGS+=(--shard-index "${SHARD_INDEX:-0}")

if [[ -n "${EXTRA_EMBED_ARGS:-}" ]]; then
  # shellcheck disable=SC2206
  EXTRA=(${EXTRA_EMBED_ARGS})
  ARGS+=("${EXTRA[@]}")
fi

exec python3 services/import/scripts/update_embeddings.py "${ARGS[@]}"
