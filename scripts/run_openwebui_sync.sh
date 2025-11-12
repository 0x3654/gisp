#!/usr/bin/env bash
# Helper script to run openwebui-sync as an ephemeral job
set -euo pipefail

REPO_ROOT="$(cd -- "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

docker compose run --rm openwebui-sync "$@"
