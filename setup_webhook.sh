#!/bin/bash

# Скрипт для установки Telegram webhook

# Загружаем переменные окружения
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Проверяем наличие BOT_TOKEN
if [ -z "$BOT_TOKEN" ]; then
    echo "❌ BOT_TOKEN не установлен в .env"
    exit 1
fi

# URL вебхука (замени на свой домен)
WEBHOOK_URL="${WEBHOOK_URL:-https://4abbf189760e.vps.myjino.ru/bot/webhook}"

echo "🔧 Установка webhook для бота..."
echo "URL: $WEBHOOK_URL"

# Устанавливаем webhook
RESPONSE=$(curl -s -X POST "https://api.telegram.org/bot${BOT_TOKEN}/setWebhook" \
    -H "Content-Type: application/json" \
    -d "{\"url\": \"${WEBHOOK_URL}\", \"allowed_updates\": [\"message\", \"callback_query\"]}")

echo "Ответ Telegram:"
echo "$RESPONSE" | python3 -m json.tool

# Проверяем статус webhook
echo ""
echo "📊 Проверка статуса webhook..."
curl -s "https://api.telegram.org/bot${BOT_TOKEN}/getWebhookInfo" | python3 -m json.tool

echo ""
echo "✅ Готово! Теперь:"
echo "1. Останови polling бота на сервере"
echo "2. Перезапусти админку (uvicorn app:app)"
echo "3. Проверь /bot/webhook/health"
