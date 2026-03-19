#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
ENV_FILE="${ENV_FILE:-${REPO_ROOT}/.env}"
JSON_TOOL_PYTHON="${BOT_VENV_PYTHON:-python3}"

# Скрипт для установки Telegram webhook

# Загружаем переменные окружения
if [ -f "$ENV_FILE" ]; then
    export $(grep -v '^#' "$ENV_FILE" | xargs)
fi

# Проверяем наличие BOT_TOKEN
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ BOT_TOKEN не установлен в $ENV_FILE"
    exit 1
fi

WEBHOOK_URL="${WEBHOOK_URL:-}"
if [ -z "$WEBHOOK_URL" ]; then
    echo "❌ WEBHOOK_URL не установлен"
    echo "Укажи WEBHOOK_URL в окружении или через ENV_FILE"
    exit 1
fi

echo "🔧 Установка webhook для бота..."
echo "URL: $WEBHOOK_URL"

# Устанавливаем webhook
RESPONSE=$(curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"${WEBHOOK_URL}\", \"allowed_updates\": [\"message\", \"callback_query\"]}")

echo "Ответ Telegram:"
echo "$RESPONSE" | "$JSON_TOOL_PYTHON" -m json.tool

# Проверяем статус webhook
echo ""
echo "📊 Проверка статуса webhook..."
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo" | "$JSON_TOOL_PYTHON" -m json.tool

echo ""
echo "✅ Готово! Теперь:"
echo "1. Останови polling бота на сервере"
echo "2. Перезапусти админку (uvicorn app:app)"
echo "3. Проверь /bot/webhook/health"
