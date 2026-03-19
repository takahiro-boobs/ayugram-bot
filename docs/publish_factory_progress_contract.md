# Publish Factory Progress Contract

This document describes the callback contract that the external video factory
must support for the live publish dashboard.

## Goal

After the operator clicks `Publish`, the admin backend should receive
account-scoped generation telemetry while the video is being produced.

The backend already supports:

- `generation_started`
- `generation_progress`
- `artifact_ready`
- `generation_completed`
- `generation_failed`

## Factory Input

The publish bridge sends the factory a JSON payload with these fields:

```json
{
  "topic": "отношения",
  "style": "милый + дерзкий",
  "messagesCount": 10,
  "dry_run": false,
  "simulate_video_fail": false,
  "async": false,
  "batch_id": 123,
  "account_id": 456,
  "username": "user_name",
  "account_login": "login_name",
  "emulator_serial": "emulator-5554",
  "workflow_key": "default",
  "progress_callback_url": "http://127.0.0.1:18001/api/internal/publishing/n8n",
  "shared_secret": "publish-shared-secret",
  "factory_timeout_seconds": 900
}
```

## Signed Callback

The factory must POST JSON to `progress_callback_url` with these headers:

- `Content-Type: application/json`
- `X-Publish-Timestamp: <unix-seconds>`
- `X-Publish-Signature: <hex-hmac-sha256>`

Signature formula:

```text
hex_hmac_sha256(shared_secret, X-Publish-Timestamp + "." + raw_json_body)
```

## Required Progress Event

The live dashboard expects real generation progress with this payload:

```json
{
  "event": "generation_progress",
  "batch_id": 123,
  "account_id": 456,
  "stage_key": "image_generation",
  "stage_label": "Генерация изображений",
  "progress_pct": 40,
  "detail": "Собрано 4/10 изображений",
  "meta": {
    "images_ready": 4
  }
}
```

Rules:

- `batch_id` and `account_id` are required.
- `stage_key` must be one of:
  - `workflow_started`
  - `script_generation`
  - `image_generation`
  - `video_render`
  - `artifact_packaging`
- `stage_label` is human-readable and is rendered in the dashboard.
- `progress_pct` must be a number in the `0..100` range.
- `detail` should describe the current sub-step.
- `meta` is optional and may contain arbitrary JSON.
- `factory_timeout_seconds` is the hard upper bound for one account run.
- The factory must end each account run with either a valid `mp4_path` or an
  explicit error payload. Silent `running` states are not allowed.

## Expected Sequence

Recommended happy path for one account:

1. `generation_started`
2. `generation_progress` with `stage_key=workflow_started`
3. `generation_progress` with `stage_key=script_generation`
4. `generation_progress` with `stage_key=image_generation`
5. `generation_progress` with `stage_key=video_render`
6. `generation_progress` with `stage_key=artifact_packaging`
7. `artifact_ready`
8. `generation_completed`

If generation fails for a single account, send:

```json
{
  "event": "generation_failed",
  "batch_id": 123,
  "account_id": 456,
  "detail": "ffmpeg render failed",
  "error_code": "DIALOG_INVALID_AFTER_RETRY",
  "raw_preview": "{\"messages\":[...}",
  "fixed_preview": "{\"messages\":[...]}",
  "parsed_keys": "messages",
  "factory_response_preview": "{\"error\":\"DIALOG_INVALID_AFTER_RETRY: ...\"}"
}
```

Rules:

- Only `detail` is required; the extra fields are optional diagnostics.
- Keep `detail` short and human-readable for batch/account status.
- Put bulky parser/debug context into the optional preview fields so the admin
  backend can preserve it in `publish_job_events.payload_json`.

If the factory itself cannot produce a valid MP4 within `factory_timeout_seconds`,
it must return an explicit error result so the bridge can emit
`generation_failed` for that account.

## Final Artifact Event

When the MP4 is ready, the factory or bridge must send:

```json
{
  "event": "artifact_ready",
  "batch_id": 123,
  "account_id": 456,
  "path": "/absolute/path/to/file.mp4",
  "filename": "456-user_name-1741549200.mp4",
  "detail": "Видео готово и поставлено в publish queue."
}
```

Notes:

- `path` must point to the exact generated artifact.
- The admin backend will reject a missing file.
- The publish runner must use this exact workflow artifact, not a same-named
  local fallback.
- No fallback publish video should be synthesized when generation fails. A bad
  generation run must stay failed.

## Local Diagnostics

Manual progress event test:

```bash
python3 scripts/publishing/publishing_ops.py send-event \
  --base-url http://127.0.0.1:8000 \
  --shared-secret "$PUBLISH_SHARED_SECRET" \
  --event generation_progress \
  --batch-id 123 \
  --account-id 456 \
  --stage-key image_generation \
  --stage-label "Генерация изображений" \
  --progress-pct 40 \
  --detail "Собрано 4/10 изображений"
```

Manual progress snapshot fetch:

```bash
python3 scripts/publishing/publishing_ops.py progress \
  --base-url http://127.0.0.1:8000 \
  --admin-user "$ADMIN_USER" \
  --admin-pass "$ADMIN_PASS" \
  --batch-id 123
```
