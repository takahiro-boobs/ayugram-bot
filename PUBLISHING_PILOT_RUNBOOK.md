# Publishing Pilot Runbook

Этот ранбук реализует план серверного запуска batch-публикаций (первый прогон 1 batch / 1 аккаунт) без изменения API.

## 1) Быстрый baseline сервера

```bash
python3 scripts/publishing/publishing_ops.py --env-file .env baseline
```

Ожидание:
- `summary.publishing_route_ready = true`
- `summary.runner_endpoint_ready = true`

Если один из пунктов `false`, сначала раскатать актуальный backend.

## 2) Снимок publish-таблиц (локально)

```bash
python3 scripts/publishing/publishing_ops.py --env-file .env snapshot-db --db-path admin.db
```

Используется как “нулевая точка” перед пилотом.

## 3) Проверка helper/runner на Mac

```bash
INSTAGRAM_APP_HELPER_BIND=127.0.0.1:18374 \
PUBLISH_RUNNER_ENABLED=1 \
python3 -m uvicorn instagram_app_helper:app --host 127.0.0.1 --port 18374
```

В отдельном терминале:

```bash
curl -sS http://127.0.0.1:18374/health | jq
```

Ожидание:
- `publish_runner_enabled = true`
- `base_url` указывает на боевую админку
- нет `401` в helper-логах при lease/status

## 4) Контролируемый пилот (callback руками)

1. Создать batch в админке (1 Instagram-аккаунт).
2. На сервере положить `.mp4` в `PUBLISH_STAGING_DIR/<batch_id>/`.
3. Отправить события:

```bash
python3 scripts/publishing/publishing_ops.py --env-file .env send-event \
  --event generation_started --batch-id <BATCH_ID>

python3 scripts/publishing/publishing_ops.py --env-file .env send-event \
  --event artifact_ready --batch-id <BATCH_ID> \
  --path <ABSOLUTE_OR_BATCH_RELATIVE_MP4_PATH> \
  --filename <VIDEO_NAME>.mp4

python3 scripts/publishing/publishing_ops.py --env-file .env send-event \
  --event generation_completed --batch-id <BATCH_ID>
```

4. Проверить lease:

```bash
python3 scripts/publishing/publishing_ops.py --env-file .env lease --runner-name pilot-runner
```

Ожидание: `200` (job есть) или `204` (в очереди пусто).

5. При ручной симуляции завершения job:

```bash
python3 scripts/publishing/publishing_ops.py --env-file .env set-status \
  --job-id <JOB_ID> --state published --detail "pilot success" \
  --last-file <VIDEO_NAME>.mp4 --runner-name pilot-runner
```

## 5) Критерий готовности

- Batch в UI имеет state `completed`.
- `published_jobs = 1`.
- Event log содержит: `batch_created -> generation_started -> artifact_ready -> leased -> ... -> published`.

## 6) Блокер full-auto (обязательная проверка)

Нужно документально подтвердить, как n8n кладёт файл в `PUBLISH_STAGING_DIR`.

- Если общий путь между n8n и backend есть: запускаем второй smoke без ручной подкладки.
- Если общего пути нет: фиксируем блокер `upload mechanism required`.
