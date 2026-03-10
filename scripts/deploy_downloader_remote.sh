#!/usr/bin/env bash
#
# deploy_downloader_remote.sh - Деплой downloader на удалённый сервер в РФ
#
# Использование:
#   ./deploy_downloader_remote.sh user@server.ru
#
# Что делает:
#   1. Копирует downloader скрипты на удалённый сервер
#   2. Создаёт systemd service для автозапуска
#   3. Настраивает cron для ежедневного запуска
#   4. Настраивает rsync для синхронизации файлов

set -euo pipefail

# Настройки
REMOTE_HOST="${1:-}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [[ -z "$REMOTE_HOST" ]]; then
  echo "Использование: $0 user@server.ru" >&2
  echo "" >&2
  echo "Пример:" >&2
  echo "  $0 root@192.168.1.100" >&2
  echo "  $0 user@gisp-server.ru" >&2
  exit 1
fi

echo "=========================================="
echo "[$(date '+%d.%m.%Y %H:%M:%S')] Деплой downloader на $REMOTE_HOST"
echo "=========================================="

# Проверяем SSH соединение
echo "[1/6] Проверка SSH соединения..."
if ! ssh -o ConnectTimeout=5 "$REMOTE_HOST" "echo 'SSH OK'" >/dev/null 2>&1; then
  echo "❌ Ошибка: не удалось подключиться к $REMOTE_HOST" >&2
  exit 1
fi
echo "✅ SSH соединение установлено"

# Создаём структуру директорий на удалённом сервере
echo "[2/6] Создание директорий на удалённом сервере..."
ssh "$REMOTE_HOST" bash <<'REMOTE_EOF'
set -euo pipefail

# Директории
mkdir -p /opt/gisp-downloader/{scripts,logs}
mkdir -p /var/lib/gisp/files

echo "✅ Директории созданы"
REMOTE_EOF

# Копируем скрипты
echo "[3/6] Копирование downloader скриптов..."
scp "$PROJECT_ROOT/src/downloader/scripts/download_csvs.py" \
    "$REMOTE_HOST:/opt/gisp-downloader/scripts/"

# Создаём systemd service
echo "[4/6] Создание systemd service..."
ssh "$REMOTE_HOST" bash <<'REMOTE_EOF'
set -euo pipefail

# Создаём systemd service
cat > /etc/systemd/system/gisp-downloader.service <<'SERVICE_EOF'
[Unit]
Description=GISP Downloader - Download CSV from minpromtorg.gov.ru
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=nobody
Group=nogroup
WorkingDirectory=/opt/gisp-downloader
Environment="FILES_DIR=/var/lib/gisp/files"
Environment="MAX_CSV_FILES=7"
Environment="TZ=Europe/Moscow"
ExecStart=/usr/bin/python3 /opt/gisp-downloader/scripts/download_csvs.py latest
StandardOutput=append:/var/log/gisp-downloader/downloader.log
StandardError=append:/var/log/gisp-downloader/downloader.log

# Hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/gisp/files /var/log/gisp-downloader

[Install]
WantedBy=multi-user.target
SERVICE_EOF

# Создаём директорию для логов
mkdir -p /var/log/gisp-downloader
chown nobody:nogroup /var/log/gisp-downloader

# Перезагружаем systemd
systemctl daemon-reload

echo "✅ Systemd service создан"
REMOTE_EOF

# Настраиваем cron
echo "[5/6] Настройка cron для ежедневного запуска..."
ssh "$REMOTE_HOST" bash <<'REMOTE_EOF'
set -euo pipefail

# Создаём cron задачу (запуск в 19:00 МСК)
cat > /etc/cron.d/gisp-downloader <<'CRON_EOF'
# GISP Downloader - Download CSV from minpromtorg.gov.ru
# Запуск каждый день в 19:00 МСК
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
0 19 * * * root systemctl start gisp-downloader.service
CRON_EOF

chmod 0644 /etc/cron.d/gisp-downloader

echo "✅ Cron настроен (19:00 МСК)"
REMOTE_EOF

# Создаём скрипт для rsync
echo "[6/6] Создание rsync скрипта..."
cat > "$SCRIPT_DIR/sync_files_from_remote.sh" <<'RSYNC_EOF'
#!/usr/bin/env bash
#
# sync_files_from_remote.sh - Синхронизация файлов с удалённого сервера
#
# Использование: ./sync_files_from_remote.sh user@server.ru

set -euo pipefail

REMOTE_HOST="${1:-}"
LOCAL_FILES_DIR="$(cd "$SCRIPT_DIR/../files" && pwd)"

if [[ -z "$REMOTE_HOST" ]]; then
  echo "Использование: $0 user@server.ru" >&2
  exit 1
fi

echo "=========================================="
echo "Синхронизация файлов с $REMOTE_HOST"
echo "=========================================="

# Синхронизируем CSV файлы и маркеры
rsync -avz --progress \
  --include="data-*.csv" \
  --include=".ready_*" \
  --exclude="*" \
  --remove-source-files \
  "$REMOTE_HOST:/var/lib/gisp/files/" \
  "$LOCAL_FILES_DIR/"

echo ""
echo "✅ Синхронизация завершена"
RSYNC_EOF

chmod +x "$SCRIPT_DIR/sync_files_from_remote.sh"

echo ""
echo "=========================================="
echo "✅ Деплой завершён!"
echo "=========================================="
echo ""
echo "Следующие шаги:"
echo ""
echo "1. Запустить downloader вручную (тест):"
echo "   ssh $REMOTE_HOST 'systemctl start gisp-downloader.service'"
echo ""
echo "2. Проверить логи:"
echo "   ssh $REMOTE_HOST 'journalctl -u gisp-downloader -f'"
echo "   ssh $REMOTE_HOST 'tail -f /var/log/gisp-downloader/downloader.log'"
echo ""
echo "3. Синхронизировать файлы:"
echo "   ./scripts/sync_files_from_remote.sh $REMOTE_HOST"
echo ""
echo "4. Добавить в cron на локальной машине:"
echo "   0 19 * * * cd /path/to/gisp && ./scripts/sync_files_from_remote.sh $REMOTE_HOST"
echo ""
