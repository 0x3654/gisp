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
FILES_DIR = os.getenv("FILES_DIR") or "/files"
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
    response = requests.get(url, headers=headers, timeout=30)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    if not response.content:
        raise RuntimeError("Получен пустой ответ от источника")
    return response.content


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
        print("ℹ️  Новых дат для скачивания нет: последний файл свежий.", file=sys.stderr)
        return None

    attempts = 0

    while current >= earliest_required:
        attempts += 1
        name, path = compose_filename(current)

        if os.path.exists(path):
            print(f"✅  Уже скачан: {name}, дальше искать не нужно.")
            return None

        try:
            payload = try_fetch(current)
        except requests.HTTPError as http_err:
            print(
                f"⚠️  Пропущена дата {current:%d.%m.%Y}: HTTP {http_err.response.status_code}",
                file=sys.stderr,
            )
        except requests.RequestException as req_err:
            print(f"⚠️  Пропущена дата {current:%d.%m.%Y}: {req_err}", file=sys.stderr)
        except RuntimeError as runtime_err:
            print(f"⚠️  Пропущена дата {current:%d.%m.%Y}: {runtime_err}", file=sys.stderr)
        else:
            if payload:
                return current, payload
        current -= timedelta(days=1)

    print("⚠️  Не удалось скачать новые CSV.", file=sys.stderr)
    print(f"Дата последнего файла: {last_known_str}", file=sys.stderr)
    print(f"Текущая дата: {END_DATE:%d.%m.%Y}", file=sys.stderr)
    if attempts > 0:
        print(
            f"Диапазон поиска: {earliest_required:%d.%m.%Y} — {END_DATE:%d.%m.%Y}, попыток: {attempts}",
            file=sys.stderr,
        )
    return None


def save_payload(path: str, data: bytes) -> None:
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "wb") as fh:
        fh.write(data)
    os.replace(tmp_path, path)


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
    remote_sha = compute_sha256(payload)

    local_sha = file_sha256(path)
    if local_sha == remote_sha:
        print(f"✅  Актуальный файл уже скачан: {name}")
        return

    save_payload(path, payload)
    size_mb = len(payload) / (1024 * 1024)

    print(f"💾  Скачан файл {name} ({size_mb:.2f} MB)")
    print(f"🕒  Дата источника: {remote_date:%d.%m.%Y}")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "latest"
    if mode not in {"latest", "new", "all"}:
        print("Использование: python download_csvs.py [latest]", file=sys.stderr)
        sys.exit(1)

    if mode in {"new", "all"}:
        print("ℹ️  Режимы 'new' и 'all' устарели, используем 'latest'.")

    download_latest()
