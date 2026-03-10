#!/usr/bin/env bash
#
# run_downloader_remote.sh - Быстрый запуск downloader на удалённом сервере
#
# Использование: ./run_downloader_remote.sh user@server.ru
#
# Простой скрипт для быстрого запуска downloader без systemd

set -euo pipefail

REMOTE_HOST="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -z "$REMOTE_HOST" ]]; then
  echo "Использование: $0 user@server.ru" >&2
  exit 1
fi

echo "=========================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Запуск downloader на $REMOTE_HOST"
echo "=========================================="

# Создаём временный скрипт на удалённом сервере
ssh "$REMOTE_HOST" bash <<'REMOTE_EOF'
set -euo pipefail

# Создаём директории
mkdir -p /tmp/gisp-downloader
cd /tmp/gisp-downloader

# Создаём downloader скрипт
cat > download_csvs.py <<'PYTHON_EOF'
import hashlib
import os
import sys
from datetime import datetime, timedelta
from typing import Optional, Tuple

import requests

BASE_URL = (
    "https://minpromtorg.gov.ru/opendata/"
    "1000000012-ReestrProducts/data-{date}-structure-20210405.csv"
)
FILES_DIR = os.getenv("FILES_DIR") or "/var/lib/gisp/files"
START_DATE = datetime(2024, 9, 5)
END_DATE = datetime.today()
USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/116.0.0.0 Safari/537.36"
)

def ensure_files_dir() -> None:
    if not FILES_DIR:
        print("❌ Переменная окружения FILES_DIR не задана.", file=sys.stderr)
        sys.exit(1)
    os.makedirs(FILES_DIR, exist_ok=True)

def existing_latest_date() -> Optional[datetime]:
    dates = []
    for fname in os.listdir(FILES_DIR or ""):
        if not fname.startswith("data-") or not fname.endswith(".csv"):
            continue
        try:
            date_str = fname.split("-")[1]
            dates.append(datetime.strptime(date_str, "%Y%m%d"))
        except Exception:
            continue
    return max(dates) if dates else None

def compose_filename(dt: datetime) -> Tuple[str, str]:
    date_str = dt.strftime("%Y%m%d")
    name = f"data-{date_str}-structure-20210405.csv"
    path = os.path.join(FILES_DIR, name)
    return name, path

def try_fetch(date_candidate: datetime) -> Optional[bytes]:
    url = BASE_URL.format(date=date_candidate.strftime("%Y%m%d"))
    headers = {"User-Agent": USER_AGENT}

    max_retries = 3
    base_timeout = 60

    for attempt in range(max_retries):
        try:
            timeout = base_timeout * (2 ** attempt)
            print(f"🔄 Попытка {attempt + 1}/{max_retries} для {date_candidate:%d.%m.%Y} (timeout={timeout}s)...", flush=True)
            response = requests.get(url, headers=headers, timeout=timeout)

            if response.status_code == 404:
                return None
            response.raise_for_status()

            if not response.content:
                raise RuntimeError("Получен пустой ответ от источника")

            print(f"✅ Успешно скачано за {attempt + 1} попытки", flush=True)
            return response.content

        except requests.HTTPError as http_err:
            if attempt == max_retries - 1:
                print(f"⚠️  HTTP ошибка {http_err.response.status_code}", flush=True)
            raise
        except requests.RequestException as req_err:
            if attempt < max_retries - 1:
                print(f"⚠️  Ошибка соединения: {req_err}", flush=True)
            if attempt == max_retries - 1:
                raise

    return None

def compute_sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def file_sha256(path: str) -> Optional[str]:
    if not os.path.exists(path):
        return None
    sha = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            sha.update(chunk)
    return sha.hexdigest()

def _next_required_date(last_existing: Optional[datetime]) -> datetime:
    if last_existing:
        return max(START_DATE, last_existing + timedelta(days=1))
    return START_DATE

def find_latest_payload(last_existing: Optional[datetime]) -> Optional[Tuple[datetime, bytes]]:
    current = END_DATE
    earliest_required = _next_required_date(last_existing)
    last_known_str = last_existing.strftime("%d.%m.%Y") if last_existing else "нет файлов"
    if earliest_required > current:
        print("ℹ️  Новых дат для скачивания нет.", file=sys.stderr)
        return None

    attempts = 0

    while current >= earliest_required:
        attempts += 1
        name, path = compose_filename(current)

        if os.path.exists(path):
            print(f"✅  Уже скачан: {name}")
            return None

        payload = try_fetch(current)
        if payload:
            return current, payload

        print(f"ℹ️  Дата {current:%d.%m.%Y} не доступна, пробуем предыдущий день...", flush=True)
        current -= timedelta(days=1)

    print("⚠️  Не удалось скачать CSV.", file=sys.stderr)
    return None

def save_payload(path: str, data: bytes) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(data)
    os.replace(tmp_path, path)

def create_marker(filename: str) -> None:
    """Create .ready_* marker file"""
    marker_path = os.path.join(FILES_DIR, f".ready_{filename}")
    with open(marker_path, "w") as f:
        f.write(f"Created at {datetime.now().isoformat()}\n")
    print(f"📝 Создан маркер: .ready_{filename}")

def download_latest() -> None:
    ensure_files_dir()
    last_existing = existing_latest_date()
    if last_existing and last_existing > END_DATE:
        last_existing = END_DATE

    if last_existing:
        print(f"ℹ️  Последний файл в папке: {last_existing:%d.%m.%Y}")
    else:
        print("ℹ️  CSV в папке не найдены.")

    result = find_latest_payload(last_existing)
    if not result:
        return

    remote_date, payload = result
    name, path = compose_filename(remote_date)

    save_payload(path, payload)
    size_mb = len(payload) / (1024 * 1024)

    print(f"💾  Скачан файл {name} ({size_mb:.2f} MB)")
    print(f"🕒  Дата источника: {remote_date:%d.%m.%Y}")

    # Create marker
    create_marker(name)

if __name__ == "__main__":
    try:
        download_latest()
    except Exception as e:
        print(f"[ERROR] Ошибка скачивания CSV: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
PYTHON_EOF

# Создаём директорию для файлов
mkdir -p /var/lib/gisp/files

# Запускаем downloader
echo "Запуск downloader..."
export FILES_DIR=/var/lib/gisp/files
python3 /tmp/gisp-downloader/download_csvs.py

echo ""
echo "=========================================="
echo "Скачанные файлы:"
ls -lh /var/lib/gisp/files/*.csv 2>/dev/null || echo "Нет файлов"
echo ""
echo "Маркеры:"
ls -lh /var/lib/gisp/files/.ready_* 2>/dev/null || echo "Нет маркеров"
echo "=========================================="
REMOTE_EOF

echo ""
echo "=========================================="
echo "✅ Скачать файлы с удалённого сервера:"
echo ""
echo "rsync -avz --progress \\"
echo "  --include='data-*.csv' --include='.ready_*' --exclude='*' \\"
echo "  $REMOTE_HOST:/var/lib/gisp/files/ \\"
echo "  $(cd "$PROJECT_ROOT/files" && pwd)/"
echo ""
