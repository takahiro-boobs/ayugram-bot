# Local Scripts

Локальные helper-скрипты вынесены в `scripts/local/` и больше не содержат зашитых секретов.

## `scripts/local/run_bot.sh`

Поддерживает переменные:

- `BOT_VENV_PYTHON` — путь к Python внутри локального venv
- `BOT_LOG_FILE` — путь к supervisor log
- `BOT_STOP_ON_CONFLICT_CODE` — код выхода, при котором supervisor не перезапускает бота

Пример:

```bash
BOT_VENV_PYTHON=/path/to/venv/bin/python ./scripts/local/run_bot.sh
```

## `scripts/local/setup_webhook.sh`

Поддерживает переменные:

- `ENV_FILE` — путь к env-файлу
- `BOT_VENV_PYTHON` — python для `json.tool`
- `WEBHOOK_URL` — webhook URL для Telegram

Пример:

```bash
ENV_FILE=.env WEBHOOK_URL=https://example.com/bot/webhook ./scripts/local/setup_webhook.sh
```

## `scripts/local/tunnel_bot.sh`

Поддерживает переменные:

- `TUNNEL_SERVER_HOST`
- `TUNNEL_SERVER_PORT`
- `TUNNEL_SERVER_USER`
- `TUNNEL_SERVER_PASS`
- `TUNNEL_LOCAL_PORT`
- `TUNNEL_REMOTE_PORT`

Пример:

```bash
TUNNEL_SERVER_HOST=example.com \
TUNNEL_SERVER_PORT=2222 \
TUNNEL_SERVER_USER=root \
TUNNEL_SERVER_PASS=secret \
./scripts/local/tunnel_bot.sh
```

## `scripts/local/instagram_helper_reverse_tunnel.sh`

Поддерживает переменные:

- `TUNNEL_SSH_KEY`
- `TUNNEL_SERVER_HOST`
- `TUNNEL_SERVER_PORT`
- `TUNNEL_SERVER_USER`
- `TUNNEL_REMOTE_BIND`
- `TUNNEL_LOCAL_TARGET`

Пример:

```bash
TUNNEL_SERVER_HOST=example.com \
TUNNEL_SERVER_PORT=2222 \
TUNNEL_SERVER_USER=root \
TUNNEL_SSH_KEY=~/.ssh/id_ed25519 \
./scripts/local/instagram_helper_reverse_tunnel.sh
```
