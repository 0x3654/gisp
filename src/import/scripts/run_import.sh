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

# Загружаем функции отправки Telegram
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "$SCRIPT_DIR/send_telegram.sh" ]]; then
  # shellcheck disable=SC1090
  source "$SCRIPT_DIR/send_telegram.sh"
else
  echo "[ERROR] Файл send_telegram.sh не найден" >&2
fi

# ------------------------------------------
extract_summary() {
  local log_file="$1"

  # Если есть ERROR - не извлекаем "успешную" статистику
  if grep -q "\[ERROR\]" "$log_file"; then
    grep -E "\[ERROR\].*" "$log_file" | sort | uniq | head -3
    return
  fi

  # Проверяем особые случаи
  if grep -q "⚠️  Не удалось скачать CSV\." "$log_file"; then
    grep -E "⚠️  Не удалось скачать CSV\.|✅ Файл уже загружен в базу|ℹ️  Новых дат для скачивания нет|CSV-файлы в каталоге.*не найдены" "$log_file" | sort | uniq | head -5
    return
  fi

  # Проверяем случай, когда CSV-файлы не найдены
  if grep -q "CSV-файлы в каталоге.*не найдены" "$log_file"; then
    grep -E "CSV-файлы в каталоге.*не найдены|ℹ️  Новых дат для скачивания нет" "$log_file" | sort | uniq | head -3
    return
  fi

  # Если был импорт - извлекаем статистику
  grep -E "^\([0-9]{2}\.[0-9]{2}\.[0-9]{4}\) 📦 Последний файл:|🔄 Синхронизация:|Эмбеддинги (обновлены|:)|⏱ Прошедшее время:|✅ Импорт завершён:" "$log_file" | sort | uniq | head -10
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

  if ! python3 /scripts/download_csvs.py latest; then
    cmd_status=$?
    record_failure "$cmd_status"
    # Ошибка уже выведена Python-скриптом с подробностями
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
  sync "$LOG_FILE" 2>/dev/null || true

  # Отправляем HTML уведомление об ошибке
  send_telegram_html "❌ Ошибка импорта (код $status)" "$LOG_FILE"
else
  # Формируем информативное сообщение на основе статистики из лога
  if grep -qiE "(psql: error|Traceback|Exception)" "$LOG_FILE"; then
    echo "[WARN] Обнаружены сообщения об ошибках в логе" >> "$LOG_FILE"
    sync "$LOG_FILE" 2>/dev/null || true
    # Отправляем HTML уведомление об ошибке POSTGRES
    send_telegram_html "⚠️ Ошибка импорта POSTGRES" "$LOG_FILE"
  else
    # Извлекаем статистику из лога
    SUMMARY=$(extract_summary "$LOG_FILE")
    if [[ -n "$SUMMARY" ]]; then
      # Отправляем HTML уведомление с успешным импортом
      send_telegram_html "📊 Импорт завершён успешно

${SUMMARY}" "$LOG_FILE"
    else
      # Heartbeat: нет новых файлов - передаём лог для извлечения статистики
      send_heartbeat "ℹ️ Новых файлов нет" "$LOG_FILE"
    fi
  fi
fi

exit $status
