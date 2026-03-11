#!/usr/bin/env python3
"""
Download manager with SSH tunnel support for GISP CSV files.

Supports two modes:
- direct: Download directly (for servers in Russia)
- ssh-tunnel: Download through SSH SOCKS proxy (for servers outside Russia)
"""

import hashlib
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta
from typing import Optional, Tuple

import requests

# Configuration
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

# SSH configuration
DOWNLOAD_METHOD = os.getenv("DOWNLOAD_METHOD", "direct")  # direct | ssh-tunnel
SSH_HOST = os.getenv("SSH_HOST", "")
SSH_PORT = os.getenv("SSH_PORT", "22")
SSH_USER = os.getenv("SSH_USER", "")
SSH_IDENTITY_FILE = os.getenv("SSH_IDENTITY_FILE", "")
SOCKS_PORT = int(os.getenv("SOCKS_PORT", "1080"))

SSH_PROCESS = None


def ensure_files_dir() -> None:
    if not FILES_DIR:
        print("❌ Переменная окружения FILES_DIR не задана.", file=sys.stderr)
        sys.exit(1)
    os.makedirs(FILES_DIR, exist_ok=True)


def setup_ssh_tunnel() -> Optional[subprocess.Popen]:
    """
    Create SSH SOCKS tunnel in background.

    Returns: subprocess.Popen object or None if not using ssh-tunnel
    """
    if DOWNLOAD_METHOD != "ssh-tunnel":
        return None

    if not SSH_HOST:
        print("❌ SSH_HOST не задан для режима ssh-tunnel", file=sys.stderr)
        sys.exit(1)

    if not SSH_IDENTITY_FILE or not os.path.exists(SSH_IDENTITY_FILE):
        print(f"❌ SSH ключ не найден: {SSH_IDENTITY_FILE}", file=sys.stderr)
        sys.exit(1)

    # Build SSH command
    ssh_host_spec = f"{SSH_USER}@{SSH_HOST}" if SSH_USER else SSH_HOST
    ssh_cmd = [
        "ssh",
        "-i", SSH_IDENTITY_FILE,
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ExitOnForwardFailure=yes",
        "-N",
        "-D", str(SOCKS_PORT),
        "-p", SSH_PORT,
        ssh_host_spec
    ]

    print(f"🔐 Создание SSH туннеля: {ssh_host_spec}:{SSH_PORT} -> localhost:{SOCKS_PORT}")

    try:
        # Start SSH process
        process = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid if hasattr(os, 'setsid') else None
        )

        # Wait a bit for tunnel to establish
        time.sleep(2)

        # Check if process is still running
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            print(f"❌ Не удалось создать SSH туннель:", file=sys.stderr)
            if stderr:
                print(f"   {stderr.decode('utf-8', errors='replace')}", file=sys.stderr)
            sys.exit(1)

        print(f"✅ SSH туннель создан (PID: {process.pid})")
        return process

    except Exception as e:
        print(f"❌ Ошибка создания SSH туннеля: {e}", file=sys.stderr)
        sys.exit(1)


def teardown_ssh_tunnel(process: Optional[subprocess.Popen]) -> None:
    """Shutdown SSH tunnel gracefully."""
    if process is None:
        return

    print(f"🔒 Закрытие SSH туннеля (PID: {process.pid})")

    try:
        # Send SIGTERM to process group
        if hasattr(os, 'killpg'):
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
        else:
            process.terminate()

        # Wait for process to exit
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            # Force kill if doesn't exit
            if hasattr(os, 'killpg'):
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            else:
                process.kill()
            process.wait()

        print("✅ SSH туннель закрыт")

    except Exception as e:
        print(f"⚠️  Ошибка закрытия SSH туннеля: {e}", file=sys.stderr)


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


def get_session() -> requests.Session:
    """Create requests session with proxy configuration if needed."""
    session = requests.Session()

    if DOWNLOAD_METHOD == "ssh-tunnel":
        # socks5h:// for remote DNS resolution through proxy
        proxy_url = f"socks5h://127.0.0.1:{SOCKS_PORT}"
        session.proxies = {
            "http": proxy_url,
            "https": proxy_url
        }
        print(f"🔗 Используем прокси: {proxy_url}")

    return session


def try_fetch(date_candidate: datetime, session: requests.Session) -> Optional[bytes]:
    url = BASE_URL.format(date=date_candidate.strftime("%Y%m%d"))
    headers = {"User-Agent": USER_AGENT}

    # Retry logic with exponential backoff
    max_retries = 3
    # For SSH tunnel mode, use longer timeout (large files through proxy)
    quick_timeout = 20 if DOWNLOAD_METHOD == "ssh-tunnel" else 10
    base_timeout = 60   # Longer timeout for retries

    for attempt in range(max_retries):
        try:
            timeout = quick_timeout if attempt == 0 else base_timeout * (2 ** (attempt - 1))
            print(f"🔄 Попытка {attempt + 1}/{max_retries} для {date_candidate:%d.%m.%Y} (timeout={timeout}s)...", flush=True)
            response = session.get(url, headers=headers, timeout=timeout)

            if response.status_code == 404:
                return None
            response.raise_for_status()

            if not response.content:
                raise RuntimeError("Получен пустой ответ от источника")

            print(f"✅ Успешно скачано за {attempt + 1} попытки", flush=True)
            return response.content

        except requests.HTTPError as http_err:
            if attempt == max_retries - 1:
                print(f"⚠️  HTTP ошибка {http_err.response.status_code} на попытке {attempt + 1}", flush=True)
            if attempt < max_retries - 1:
                wait_time = 5 * (2 ** attempt)
                print(f"⏳ Повторная попытка через {wait_time} сек...", flush=True)
                time.sleep(wait_time)
            else:
                raise

        except requests.RequestException as req_err:
            if attempt < max_retries - 1:
                print(f"⚠️  Ошибка соединения на попытке {attempt + 1}: {req_err}", flush=True)
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


def find_latest_payload(last_existing: Optional[datetime], session: requests.Session) -> Optional[Tuple[datetime, bytes]]:
    current = END_DATE
    last_known_str = last_existing.strftime("%d.%m.%Y") if last_existing else "нет файлов"

    # If last file is today or yesterday, nothing to download
    if last_existing:
        days_diff = (END_DATE - last_existing).days
        if days_diff <= 1:
            print(f"ℹ️  Актуальный файл уже скачан: {last_known_str}", file=sys.stderr)
            return None
        earliest_required = last_existing + timedelta(days=1)
    else:
        earliest_required = START_DATE

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

        payload = try_fetch(current, session)
        if payload:
            return current, payload

        print(f"ℹ️  Дата {current:%d.%m.%Y} не доступна, пробуем предыдущий день...", flush=True)
        current -= timedelta(days=1)

    print("⚠️  Не удалось скачать CSV.", file=sys.stderr)
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


def cleanup_old_csvs() -> None:
    """Remove old CSV files, keeping only MAX_CSV_FILES most recent ones"""
    max_files = int(os.getenv("MAX_CSV_FILES", "7"))
    csv_files = []

    for fname in os.listdir(FILES_DIR):
        if fname.startswith("data-") and fname.endswith(".csv"):
            full_path = os.path.join(FILES_DIR, fname)
            csv_files.append((full_path, os.path.getmtime(full_path)))

    # Sort by modification time (newest first)
    csv_files.sort(key=lambda x: x[1], reverse=True)

    # Remove files beyond MAX_CSV_FILES
    if len(csv_files) > max_files:
        files_to_delete = csv_files[max_files:]
        for file_path, _ in files_to_delete:
            try:
                os.remove(file_path)
                print(f"🔥 Удален старый CSV: {os.path.basename(file_path)}")
            except Exception as e:
                print(f"⚠️  Ошибка удаления {file_path}: {e}", file=sys.stderr)


def download_latest() -> None:
    ensure_files_dir()

    # Setup SSH tunnel if needed
    global SSH_PROCESS
    SSH_PROCESS = setup_ssh_tunnel()

    try:
        # Create session with proxy configuration
        session = get_session()

        last_existing = existing_latest_date()
        if last_existing and last_existing > END_DATE:
            last_existing = END_DATE

        if last_existing:
            print(f"ℹ️  Последний файл в папке: {last_existing:%d.%m.%Y}")
        else:
            print("ℹ️  CSV в папке не найдены.")

        result = find_latest_payload(last_existing, session)
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

        # NOTE: Маркеры не создаются - import запускается Semaphore/Ansible

        # Cleanup old CSV files
        cleanup_old_csvs()

    finally:
        # Always teardown SSH tunnel
        teardown_ssh_tunnel(SSH_PROCESS)


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "latest"
    if mode not in {"latest", "new", "all"}:
        print("Использование: python download_manager.py [latest]", file=sys.stderr)
        sys.exit(1)

    if mode in {"new", "all"}:
        print("ℹ️  Режимы 'new' и 'all' устарели, используем 'latest'.")

    # Validate SSH configuration if using ssh-tunnel
    if DOWNLOAD_METHOD == "ssh-tunnel":
        print(f"🔐 Режим: SSH туннель через {SSH_HOST}")
    else:
        print("🌐 Режим: Прямое скачивание")

    try:
        download_latest()
    except Exception as e:
        print(f"[ERROR] Ошибка скачивания CSV: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
