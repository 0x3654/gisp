#!/usr/bin/env bash

set -euo pipefail

ENV_DUMP="/etc/environment"
if [[ -f "$ENV_DUMP" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "$ENV_DUMP"
  set +a
fi

: "${FILES_DIR:=/files}"
: "${LOG_DIR:=/var/log/registry}"
: "${MAX_LOG_FILES:=7}"
: "${MAX_MAINTENANCE_LOG_FILES:=7}"
: "${MAX_CSV_FILES:=7}"
: "${AUTO_EMBED:=1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-embeddings|--no-embeddings)
      AUTO_EMBED=0
      ;;
    --with-embeddings)
      AUTO_EMBED=1
      ;;
    -h|--help)
      cat <<'EOF'
Usage: run_import.sh [--skip-embeddings|--with-embeddings]

By default the importer recomputes semantic embeddings after each successful
load (AUTO_EMBED=1). Pass --skip-embeddings or set AUTO_EMBED=0 to import only
the tabular data and recalculate vectors later via the embeddings worker.
EOF
      exit 0
      ;;
    *)
      echo "[ERROR] Неизвестный параметр: $1" >&2
      echo "       Используйте --help для списка доступных опций." >&2
      exit 1
      ;;
  esac
  shift
done

export AUTO_EMBED

mkdir -p "$LOG_DIR" "$FILES_DIR"
LOG_FILE="$LOG_DIR/run_$(date '+%F_%H-%M').md"
# ------------------------------------------
send_telegram() {
  local message="$1"
  if [[ -n "${BOT_TOKEN:-}" && -n "${CHAT_ID:-}" ]]; then
    curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendDocument" \
      -F chat_id="${CHAT_ID}" \
      -F caption="${message}" \
      -F document=@"$LOG_FILE" > /dev/null || echo "[WARN] Не удалось отправить файл в Telegram" >> "$LOG_FILE"
  else
    echo "[WARN] Переменные Telegram не заданы" >> "$LOG_FILE"
  fi
}
# ------------------------------------------
status=0

record_failure() {
  local exit_code="$1"
  if [[ $status -eq 0 ]]; then
    status="$exit_code"
  fi
}

{
echo "=============================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск обновления..."
echo "=============================================="
if [[ "$AUTO_EMBED" != "1" ]]; then
  echo "[INFO] Автообновление эмбеддингов отключено (AUTO_EMBED=$AUTO_EMBED)"
fi

  if ! python3 /scripts/download_csvs.py new >/dev/null; then
    cmd_status=$?
    record_failure "$cmd_status"
    echo "[ERROR] Ошибка скачивания CSV ($cmd_status)"
  fi

  if ! /scripts/import_all.sh "$FILES_DIR"; then
    cmd_status=$?
    record_failure "$cmd_status"
    echo "[ERROR] Ошибка импорта ($cmd_status)"
  fi

  if [[ $status -eq 0 ]]; then
    echo -e "\n🔥 Удаляем старые файлы логов"
    old_logs=$(ls -1t "$LOG_DIR"/*.md 2>/dev/null | tail -n +$((MAX_LOG_FILES+1)))
    if [[ -n "$old_logs" ]]; then
      for f in $old_logs; do
        log_date=$(echo "$f" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1 | awk -F- '{print $3"."$2"."$1}')
        echo "$log_date  $f"
        rm -f "$f"
      done
    fi

    maintenance_logs=$(ls -1t "$LOG_DIR"/maintenance_*.log 2>/dev/null | tail -n +$((MAX_MAINTENANCE_LOG_FILES+1)))
    if [[ -n "$maintenance_logs" ]]; then
      echo -e "\n🔥 Удаляем старые maintenance-логи"
      for f in $maintenance_logs; do
        log_date=$(echo "$f" | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}' | head -1 | awk -F- '{print $3"."$2"."$1}')
        echo "$log_date  $f"
        rm -f "$f"
      done
    fi

    # Удаляем старые CSV-файлы, оставляя только два последних (последний всегда остаётся)
    old_csvs=$(ls -1t "$FILES_DIR"/*.csv 2>/dev/null | tail -n +$((MAX_CSV_FILES+1)))
    if [[ -n "$old_csvs" ]]; then
      echo -e "\n🔥 Удаляем старые CSV-файлы:"
      for f in $old_csvs; do
        echo "  $f"
        rm -f "$f"
      done
    fi
  else
    echo -e "\n[WARN] Пропускаем очистку логов и CSV из-за ошибки импорта"
  fi

echo ""
echo "=============================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Обновление завершено."
echo "=============================================="

} >>"$LOG_FILE" 2>&1 || true

if [[ $status -ne 0 ]]; then
  echo "[ERROR] Скрипт завершился с кодом $status" >> "$LOG_FILE"
  send_telegram "❌ Ошибка импорта (код $status):
$(date '+%d.%m.%Y %H:%M:%S')"
else
  # даже если формально успешный код, проверяем наличие ошибок в логе
  RAW_HOST=$(hostname -f 2>/dev/null || hostname)
  if [[ "$RAW_HOST" =~ ^[0-9a-f]{12}$ ]]; then
    RAW_HOST="registry-node-${RAW_HOST:0:6}"
  fi
  HOST_ID="${REGISTRY_NODE_NAME:-$RAW_HOST}"

  if grep -qiE "(psql: error|Traceback|Exception)" "$LOG_FILE"; then
    echo "[WARN] Обнаружены сообщения об ошибках в логе" >> "$LOG_FILE"
    send_telegram "⚠️ Ошибка импорта POSTGRES:
$(date '+%d.%m.%Y %H:%M:%S') (${HOST_ID})"
  else
    send_telegram "✅ Импорт завершён успешно:
$(date '+%d.%m.%Y %H:%M:%S') (${HOST_ID})"
  fi
fi

exit $status
