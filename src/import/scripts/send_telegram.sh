#!/usr/bin/env bash
# send_telegram.sh - отправка HTML сообщений в Telegram
#
# Переменные окружения:
#   BOT_TOKEN - токен Telegram бота (обязательно)
#   CHAT_ID   - ID чата для отправки (обязательно)
#   REGISTRY_NODE_NAME - имя ноды (по умолчанию hostname)

# Функция для отправки HTML-сообщения в Telegram
send_telegram_html() {
  local message="$1"
  local log_file="${2:-}"

  if [[ -z "${BOT_TOKEN:-}" || -z "${CHAT_ID:-}" ]]; then
    echo "[WARN] Переменные Telegram не заданы" >&2
    return 0
  fi

  echo "[INFO] Отправка уведомления в Telegram..." >&2

  # Формируем HTML сообщение
  local html_message
  html_message=$(format_html_message "$message" "$log_file")

  # Передаем сообщение напрямую - curl сам сделает URL encoding через --data-urlencode
  local response
  response=$(curl -s -X POST \
    "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
    -d chat_id="${CHAT_ID}" \
    -d parse_mode="HTML" \
    -d disable_web_page_preview="true" \
    --data-urlencode "text=${html_message}" 2>&1)

  if echo "$response" | grep -q '"ok":false'; then
    echo "[ERROR] Ошибка Telegram API: $response" >&2
    return 1
  elif [[ -n "$response" ]]; then
    echo "[INFO] Уведомление отправлено успешно" >&2
  else
    echo "[WARN] Пустой ответ от Telegram API" >&2
  fi
}

# Форматирование HTML сообщения с опциональным логом
format_html_message() {
  local summary="$1"
  local log_file="$2"

  # Получаем hostname и timestamp
  RAW_HOST=$(hostname -f 2>/dev/null || hostname)
  if [[ "$RAW_HOST" =~ ^[0-9a-f]{12}$ ]]; then
    RAW_HOST="registry-node-${RAW_HOST:0:6}"
  fi
  local hostname="${REGISTRY_NODE_NAME:-$RAW_HOST}"
  local timestamp=$(date '+%d.%m.%Y %H:%M:%S')

  # Если есть лог-файл
  if [[ -n "$log_file" && -f "$log_file" ]]; then
    # Выводим весь лог (экранируем HTML, сохраняем переносы строк)
    local log_content
    log_content=$(escape_html_pre < "$log_file")

    cat <<EOF
${summary}<blockquote expandable>
${timestamp} (${hostname})
${log_content}
</blockquote>
EOF
  else
    # Без лог-файла
    cat <<EOF
${summary}
EOF
  fi
}

# Экранирование HTML для вставки в <pre> тег
escape_html_pre() {
  sed -e 's/&/\&amp;/g' \
      -e 's/</\&lt;/g' \
      -e 's/>/\&gt;/g'
}

# Замена переносов строк на <br> для HTML
newlines_to_br() {
  sed 's/$/<br>/g' | sed '$ s/<br>$//'
}

# Функция для отправки heartbeat уведомления (короткое сообщение без лога)
send_heartbeat() {
  local status="$1"
  local log_file="${2:-}"

  # Получаем hostname и timestamp
  RAW_HOST=$(hostname -f 2>/dev/null || hostname)
  if [[ "$RAW_HOST" =~ ^[0-9a-f]{12}$ ]]; then
    RAW_HOST="registry-node-${RAW_HOST:0:6}"
  fi
  local hostname="${REGISTRY_NODE_NAME:-$RAW_HOST}"
  local timestamp=$(date '+%d.%m.%Y %H:%M:%S')

  local message
  if [[ -n "$log_file" && -f "$log_file" ]]; then
    # Выводим весь лог
    local log_content
    log_content=$(escape_html_pre < "$log_file")

    message="${status}<blockquote expandable>
${timestamp} (${hostname})
${log_content}
</blockquote>"
  elif [[ -n "$2" ]]; then
    # Если переданы детали как текст (обратная совместимость)
    local details="$2"
    local escaped_details
    escaped_details=$(echo "$details" | escape_html_pre)

    message="${status}<blockquote expandable>
${timestamp} (${hostname})
${escaped_details}
</blockquote>"
  else
    message="${status}"
  fi

  send_telegram_html "$message" ""
}
