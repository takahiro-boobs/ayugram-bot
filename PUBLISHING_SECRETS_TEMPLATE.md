# Publishing Secrets Matrix (Template)

Заполни значения вне git (в менеджере секретов или в Render dashboard).

| Key | Scope | Value | Rotated At | Notes |
|---|---|---|---|---|
| `HELPER_API_KEY` | admin + helper | `<set>` | `<date>` | Должен совпадать на сервере и helper-хосте |
| `PUBLISH_RUNNER_API_KEY` | admin + helper-runner | `<set>` | `<date>` | Можно равнять `HELPER_API_KEY` в v1 |
| `PUBLISH_SHARED_SECRET` | admin + n8n | `<set>` | `<date>` | Используется для HMAC подписи callback |
| `PUBLISH_N8N_WEBHOOK_URL` | admin | `<set>` | `<date>` | URL старта fixed workflow |
| `PUBLISH_BASE_URL` | admin | `<set>` | `<date>` | Публичный URL админки (с base path при необходимости) |
| `PUBLISH_STAGING_DIR` | admin | `<set>` | `<date>` | Каталог staging для batch artifacts |
