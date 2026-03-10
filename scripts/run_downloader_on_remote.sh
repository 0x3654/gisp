#!/usr/bin/env bash
#
# run_downloader_on_remote.sh - Простой запуск downloader на удалённом сервере
#

set -euo pipefail

REMOTE_HOST="${1:-ru2}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "=========================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск downloader на $REMOTE_HOST"
echo "=========================================="

# Проверяем SSH
echo "[1/5] Проверка соединения..."
if ! ssh "$REMOTE_HOST" "echo 'OK'" >/dev/null 2>&1; then
  echo "❌ Нет SSH соединения" >&2
  exit 1
fi
echo "✅ SSH OK"

# Создаём директории
echo "[2/5] Подготовка директорий..."
ssh "$REMOTE_HOST" "mkdir -p /var/lib/gisp/files && chmod 777 /var/lib/gisp/files"

# Создаём временный скрипт на удалённом сервере
echo "[3/5] Создание downloader скрипта..."
ssh "$REMOTE_HOST" 'cat > /tmp/download_csvs.py' <<'PYTHON_EOF'
import hashlib
import os
import sys
from datetime import datetime, timedelta
import requests as req

BASE_URL = "https://minpromtorg.gov.ru/opendata/1000000012-ReestrProducts/data-{date}-structure-20210405.csv"
FILES_DIR = "/var/lib/gisp/files"

def download_latest():
    os.makedirs(FILES_DIR, exist_ok=True)

    # Находим последний файл
    dates = []
    for f in os.listdir(FILES_DIR):
        if f.startswith("data-") and f.endswith(".csv"):
            try:
                dates.append(datetime.strptime(f.split("-")[1], "%Y%m%d"))
            except: pass

    last = max(dates) if dates else None
    cur = datetime.today()

    if last:
        print(f"Последний файл: {last:%d.%m.%Y}")
        start = last + timedelta(days=1)
    else:
        print("Файлов нет, качаем сегодняшний")
        start = cur

    for attempt in range(7):
        d = start + timedelta(days=attempt)
        url = BASE_URL.format(date=d.strftime("%Y%m%d"))
        fn = f"data-{d.strftime('%Y%m%d')}-structure-20210405.csv"

        print(f"Попытка {attempt+1}/7: {fn}")

        try:
            r = req.get(url, timeout=120)
            if r.status_code == 404:
                print(f"  Файл не найден")
                continue

            r.raise_for_status()

            if not r.content:
                print("  Пустой ответ")
                continue

            # Сохраняем
            fp = os.path.join(FILES_DIR, fn)
            with open(fp, "wb") as f:
                f.write(r.content)

            mb = len(r.content) / (1024*1024)
            print(f"  ✅ Скачано ({mb:.1f} MB)")

            # Маркер
            marker = os.path.join(FILES_DIR, f".ready_{fn}")
            with open(marker, "w") as f:
                f.write(f"{datetime.now().isoformat()}\n")
            print(f"  📝 Маркер создан")

            return fp

        except req.RequestException as e:
            print(f"  ⚠️  Ошибка: {e}")

    print("❌ Не удалось скачать за последние 7 дней")
    sys.exit(1)

if __name__ == "__main__":
    download_latest()
PYTHON_EOF

echo "✅ Скрипт создан"

# Запускаем
echo "[4/5] Запуск скачивания..."
ssh "$REMOTE_HOST" "cd /tmp && python3 download_csvs.py"

# Синхронизируем
echo "[5/5] Синхронизация..."
mkdir -p "$PROJECT_ROOT/files"

rsync -avz --progress \
  --include="data-*.csv" \
  --include=".ready_*" \
  --exclude="*" \
  "$REMOTE_HOST:/var/lib/gisp/files/" \
  "$PROJECT_ROOT/files/"

echo ""
echo "=========================================="
echo "✅ Готово!"
echo "Файлы:"
ls -lh "$PROJECT_ROOT/files/"*.csv 2>/dev/null | tail -3 || echo "Нет"
echo "Маркеры:"
ls -lh "$PROJECT_ROOT/files/".ready_* 2>/dev/null || echo "Нет"
echo "=========================================="
