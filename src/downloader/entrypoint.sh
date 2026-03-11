#!/usr/bin/env bash
set -euo pipefail

: "${FILES_DIR:=/files}"
: "${MAX_CSV_FILES:=7}"
: "${TZ:=Europe/Moscow}"
: "${DOWNLOAD_METHOD:=direct}"
: "${SSH_HOST:=}"
: "${SSH_PORT:=22}"
: "${SSH_USER:=}"
: "${SSH_KEY_B64:=}"
: "${SSH_IDENTITY_FILE:=}"
: "${SOCKS_PORT:=1080}"

# If SSH key is provided as base64, decode it to a temp file
if [[ "$DOWNLOAD_METHOD" == "ssh-tunnel" && -n "$SSH_KEY_B64" ]]; then
  _tmp_key=$(mktemp)
  printf '%s' "$SSH_KEY_B64" | base64 -d > "$_tmp_key"
  chmod 600 "$_tmp_key"
  SSH_IDENTITY_FILE="$_tmp_key"
fi

# Export environment for Python scripts
export FILES_DIR MAX_CSV_FILES TZ DOWNLOAD_METHOD SSH_HOST SSH_PORT SSH_USER SSH_IDENTITY_FILE SOCKS_PORT

echo "=============================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск downloader..."
echo "=============================================="
echo "[INFO] FILES_DIR=$FILES_DIR"
echo "[INFO] MAX_CSV_FILES=$MAX_CSV_FILES"
echo "[INFO] TZ=$TZ"
echo "[INFO] DOWNLOAD_METHOD=$DOWNLOAD_METHOD"

if [[ "$DOWNLOAD_METHOD" == "ssh-tunnel" ]]; then
  echo "[INFO] SSH_USER=$SSH_USER"
  echo "[INFO] SSH_HOST=$SSH_HOST"
  echo "[INFO] SSH_PORT=$SSH_PORT"
  echo "[INFO] SSH_IDENTITY_FILE=$SSH_IDENTITY_FILE"
  echo "[INFO] SOCKS_PORT=$SOCKS_PORT"
fi

# Run download manager with SSH tunnel support
exec python3 /scripts/download_manager.py "$@"
