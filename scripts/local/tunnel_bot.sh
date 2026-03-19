#!/bin/bash
set -euo pipefail

# SSH туннель для бота на сервере Jino
# Пробрасывает Telegram webhook через локальный Mac

echo "🚇 Запуск SSH туннеля для бота..."

SERVER_HOST="${TUNNEL_SERVER_HOST:-}"
SERVER_PORT="${TUNNEL_SERVER_PORT:-}"
SERVER_USER="${TUNNEL_SERVER_USER:-}"
SERVER_PASS="${TUNNEL_SERVER_PASS:-}"
LOCAL_PORT="${TUNNEL_LOCAL_PORT:-8888}"
REMOTE_PORT="${TUNNEL_REMOTE_PORT:-18001}"

if [ -z "$SERVER_HOST" ] || [ -z "$SERVER_PORT" ] || [ -z "$SERVER_USER" ] || [ -z "$SERVER_PASS" ]; then
  echo "❌ Не заданы переменные туннеля."
  echo "Нужны: TUNNEL_SERVER_HOST, TUNNEL_SERVER_PORT, TUNNEL_SERVER_USER, TUNNEL_SERVER_PASS"
  exit 1
fi

echo "📡 Создаю обратный SSH туннель..."
echo "   Локальный порт: $LOCAL_PORT"
echo "   Удаленный порт: $REMOTE_PORT"

# Создаем обратный туннель (reverse tunnel)
# Сервер будет слушать на localhost:8888 и пробрасывать на Mac
sshpass -p "$SERVER_PASS" ssh -p "$SERVER_PORT" \
  -o StrictHostKeyChecking=no \
  -o ServerAliveInterval=60 \
  -o ServerAliveCountMax=3 \
  -R "$LOCAL_PORT:localhost:$REMOTE_PORT" \
  -N \
  "$SERVER_USER@$SERVER_HOST"

echo "❌ Туннель закрыт"
