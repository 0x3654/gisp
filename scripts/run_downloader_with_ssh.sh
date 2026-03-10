#!/usr/bin/env bash
#
# run_downloader_with_ssh.sh - Run downloader with SSH tunnel support
#
# Usage:
#   ./scripts/run_downloader_with_ssh.sh                    # direct mode (production in Russia)
#   ./scripts/run_downloader_with_ssh.sh ssh-tunnel ru2     # SSH tunnel mode (development)
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DOWNLOAD_METHOD="${1:-direct}"
SSH_HOST="${2:-}"

echo "=========================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск downloader"
echo "=========================================="
echo "Режим: $DOWNLOAD_METHOD"

if [[ "$DOWNLOAD_METHOD" == "ssh-tunnel" ]]; then
  if [[ -z "$SSH_HOST" ]]; then
    echo "❌ Ошибка: для ssh-tunnel режима нужно указать SSH хост" >&2
    echo "Использование: $0 ssh-tunnel <ssh-host>" >&2
    exit 1
  fi

  # Find SSH key
  SSH_KEY=""
  if [[ -f "$HOME/.ssh/${SSH_HOST}_0x3654" ]]; then
    SSH_KEY="$HOME/.ssh/${SSH_HOST}_0x3654"
  elif [[ -f "$HOME/.ssh/id_rsa" ]]; then
    SSH_KEY="$HOME/.ssh/id_rsa"
  elif [[ -f "$HOME/.ssh/id_ed25519" ]]; then
    SSH_KEY="$HOME/.ssh/id_ed25519"
  else
    echo "❌ Ошибка: SSH ключи не найдены" >&2
    exit 1
  fi

  echo "SSH хост: $SSH_HOST"
  echo "SSH ключ: $SSH_KEY"
  echo ""
fi

cd "$PROJECT_ROOT"

# Build downloader image first (to include latest code)
echo "🔨 Сборка downloader образа..."
docker build -t gisp-downloader:test -f src/downloader/Dockerfile .

echo ""
echo "🚀 Запуск контейнера..."

if [[ "$DOWNLOAD_METHOD" == "ssh-tunnel" ]]; then
  # Run with SSH tunnel
  docker run --rm \
    --network host \
    -v "$PROJECT_ROOT/files:/files" \
    -v "$SSH_KEY:/ssh_key:ro" \
    -e TZ=Europe/Moscow \
    -e FILES_DIR=/files \
    -e MAX_CSV_FILES=7 \
    -e DOWNLOAD_METHOD=ssh-tunnel \
    -e SSH_HOST="$SSH_HOST" \
    -e SSH_PORT=22 \
    -e SSH_IDENTITY_FILE=/ssh_key \
    -e SOCKS_PORT=1080 \
    gisp-downloader:test \
    latest
else
  # Run in direct mode
  docker run --rm \
    --network host \
    -v "$PROJECT_ROOT/files:/files" \
    -e TZ=Europe/Moscow \
    -e FILES_DIR=/files \
    -e MAX_CSV_FILES=7 \
    -e DOWNLOAD_METHOD=direct \
    gisp-downloader:test \
    latest
fi

echo ""
echo "=========================================="
echo "✅ Готово!"
echo "=========================================="
echo "Файлы:"
ls -lh "$PROJECT_ROOT/files/"*.csv 2>/dev/null | tail -3 || echo "Нет"
echo "Маркеры:"
ls -lh "$PROJECT_ROOT/files/".ready_* 2>/dev/null || echo "Нет"
echo "=========================================="
