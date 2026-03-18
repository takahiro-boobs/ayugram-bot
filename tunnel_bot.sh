#!/bin/bash

# SSH туннель для бота на сервере Jino
# Пробрасывает Telegram webhook через локальный Mac

echo "🚇 Запуск SSH туннеля для бота..."

# Параметры сервера
SERVER_HOST="4abbf189760e.vps.myjino.ru"
SERVER_PORT="49295"
SERVER_USER="root"
SERVER_PASS="5115101013Dan"

# Локальный порт для туннеля
LOCAL_PORT="8888"

# Удаленный порт админки на сервере
REMOTE_PORT="18001"

echo "📡 Создаю обратный SSH туннель..."
echo "   Локальный порт: $LOCAL_PORT"
echo "   Удаленный порт: $REMOTE_PORT"

# Создаем обратный туннель (reverse tunnel)
# Сервер будет слушать на localhost:8888 и пробрасывать на Mac
sshpass -p "$SERVER_PASS" ssh -p $SERVER_PORT \
  -o StrictHostKeyChecking=no \
  -o ServerAliveInterval=60 \
  -o ServerAliveCountMax=3 \
  -R $LOCAL_PORT:localhost:$REMOTE_PORT \
  -N \
  $SERVER_USER@$SERVER_HOST

echo "❌ Туннель закрыт"
