#!/usr/bin/env bash
#
# run_downloader_via_ssh_tunnel.sh - Запуск downloader через SSH tunnel
#

set -euo pipefail

REMOTE_HOST="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -z "$REMOTE_HOST" ]]; then
  echo "Использование: $0 user@server.ru" >&2
  exit 1
fi

SOCKS_PORT=1080
SSH_PID=""

cleanup() {
  local exit_code=$?
  if [[ -n "$SSH_PID" ]]; then
    kill "$SSH_PID" 2>/dev/null || true
    wait "$SSH_PID" 2>/dev/null || true
  fi
  exit $exit_code
}

trap cleanup EXIT INT TERM

echo "=========================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск downloader через SSH tunnel"
echo "=========================================="
echo "Удалённый сервер: $REMOTE_HOST"
echo "Локальный SOCKS proxy: localhost:$SOCKS_PORT"
echo ""

# Находим SSH ключ
echo "[1/5] Поиск SSH ключа..."
SSH_KEY=""
if [[ -f "$HOME/.ssh/id_rsa" ]]; then
  SSH_KEY="$HOME/.ssh/id_rsa"
elif [[ -f "$HOME/.ssh/id_ed25519" ]]; then
  SSH_KEY="$HOME/.ssh/id_ed25519"
else
  echo "❌ Ошибка: SSH ключи не найдены" >&2
  exit 1
fi
echo "✅ Ключ найден"

# Проверяем SSH соединение
echo "[2/5] Проверка SSH соединения..."
if ! ssh -o ConnectTimeout=5 -o ExitOnForwardFailure=yes -i "$SSH_KEY" -N -D "$SOCKS_PORT" "$REMOTE_HOST" &
then
  SSH_PID=$!
  sleep 2
  
  if ! kill -0 "$SSH_PID" 2>/dev/null; then
    echo "❌ Ошибка: не удалось создать SSH tunnel" >&2
    exit 1
  fi
  
  echo "✅ SSH tunnel создан"
else
  echo "❌ Ошибка: не удалось подключиться к $REMOTE_HOST" >&2
  exit 1
fi

# Запускаем downloader контейнер
echo "[3/5] Запуск downloader контейнера..."

docker run --rm \
  --network host \
  -v "$PROJECT_ROOT/files:/files" \
  -e TZ=Europe/Moscow \
  -e FILES_DIR=/files \
  -e MAX_CSV_FILES=7 \
  -e ALL_PROXY="socks5://localhost:$SOCKS_PORT" \
  -e all_proxy="socks5://localhost:$SOCKS_PORT" \
  gisp-downloader:test \
  latest

echo ""
echo "[4/5] Проверка скачанных файлов..."
echo "=========================================="
ls -lh "$PROJECT_ROOT/files/"*.csv 2>/dev/null | tail -3 || echo "Нет CSV файлов"
echo ""
ls -lh "$PROJECT_ROOT/files/".ready_* 2>/dev/null || echo "Нет маркеров"
echo "=========================================="

echo ""
echo "[5/5] Готово!"
echo ""
