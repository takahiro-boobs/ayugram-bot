# Slezhka Project Map

Основной боевой контур оставлен в корне репозитория:

- `app.py` — FastAPI admin backend
- `web_app.py` — web-only entrypoint без embedded runtime worker
- `runtime_worker.py` — отдельный entrypoint для runtime queue worker
- `bot.py`, `review_bot.py`, `bot_webhook.py`
- `instagram_app_helper.py`, `instagram_helper.py`
- `db.py`, `mail_service.py`, `http_utils.py`, `twofa_utils.py`
- `settings.py` — централизованный env/config parsing
- `domain_states.py` — единый источник publish/audit/instagram state definitions
- `templates/`, `static/`, `render.yaml`, `requirements*.txt`

Поддерживающие зоны разложены по отдельным каталогам:

- `tests/` — весь автотестовый набор
- `scripts/publishing/` — publishing/n8n workflow assets и ops-утилиты
- `scripts/legacy/` — legacy/manual training-инструменты, которые сохранены, но выведены из активного контура
- `scripts/local/` — локальные shell-скрипты для ручной эксплуатации
- `docs/` — контракты и вспомогательная документация

## Runtime Topology

Новый рекомендуемый режим запуска разделяет web и background execution:

- `web_app.py` — HTTP/UI/API
- `runtime_worker.py` — runtime tasks, publish generation orchestration, mail reconcile
- `instagram_app_helper.py` — Android/helper automation и publish runner

`app.py` по-прежнему совместим с legacy-режимом и может поднимать embedded runtime worker через `EMBED_RUNTIME_WORKER=1`, но для deploy теперь рекомендуется отдельный worker process.

## Local Dev

Установка dev-зависимостей:

```bash
python3 -m pip install -r requirements-dev.txt
```

Быстрые команды:

```bash
make run-web
make run-runtime-worker
make run-helper
make test
```

Локальные runtime-артефакты больше не должны лежать в корне. Текущий backup создан вне репозитория в `~/SlezhkaRuntimeBackup/`.

Для локальных shell-скриптов переменные окружения и примеры запуска описаны в `docs/local_scripts.md`.
