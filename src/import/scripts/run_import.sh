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
: "${MAX_CSV_FILES:=7}"
: "${AUTO_EMBED:=0}"  # Отключено по умолчанию - embeddings работает отдельно

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

# Дублируем вывод в лог-файл и в stdout/stderr для отладки
exec > >(tee -a "$LOG_FILE") 2>&1

# ------------------------------------------
extract_summary() {
  local log_file="$1"

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

  /scripts/import_all.sh "$FILES_DIR" || {
    cmd_status=$?
    record_failure "$cmd_status"
    echo "[ERROR] Ошибка импорта ($cmd_status)"
  }

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

} || true

# Выводим summary в stdout для Semaphore/Ansible
echo ""
if [[ $status -eq 0 ]]; then
  echo "=== ✅ ИМПОРТ ЗАВЕРШЁН УСПЕШНО (код $status) ==="
  extract_summary "$LOG_FILE"
  echo "========================================"
else
  echo "=== ❌ ОШИБКА ИМПОРТА (код $status) ==="
  grep -E "\[ERROR\]" "$LOG_FILE" | head -5 || echo "Подробности в логе: $LOG_FILE"
  echo "========================================"
fi

exit $status
