ACCOUNT_INSTAGRAM_LAUNCH_STATUS_LABELS = {
    "idle": "Не запускался",
    "login_submitted": "Логин отправлен",
    "manual_2fa_required": "Нужен 2FA",
    "challenge_required": "Нужен challenge",
    "invalid_password": "Неверный пароль",
    "helper_error": "Ошибка helper",
}
ACCOUNT_INSTAGRAM_LAUNCH_STATUS_KEYS = set(ACCOUNT_INSTAGRAM_LAUNCH_STATUS_LABELS)

ACCOUNT_INSTAGRAM_PUBLISH_STATUS_LABELS = {
    "idle": "Не запускался",
    "preparing": "Подготовка",
    "login_required": "Нужен вход",
    "manual_2fa_required": "Нужен 2FA",
    "email_code_required": "Нужен код с почты",
    "challenge_required": "Нужен challenge",
    "invalid_password": "Неверный пароль",
    "importing_media": "Импорт медиа",
    "opening_reel_flow": "Открываю Reel",
    "selecting_media": "Выбираю видео",
    "publishing": "Публикую",
    "published": "Опубликовано",
    "needs_review": "Нужна проверка",
    "no_source_video": "Нет видео",
    "publish_error": "Ошибка публикации",
}
ACCOUNT_INSTAGRAM_PUBLISH_STATUS_KEYS = set(ACCOUNT_INSTAGRAM_PUBLISH_STATUS_LABELS)

INSTAGRAM_AUDIT_BATCH_STATE_LABELS = {
    "queued": "В очереди",
    "running": "Выполняется",
    "completed": "Завершён",
    "completed_with_errors": "Завершён с проблемами",
    "failed": "Ошибка",
    "canceled": "Отменён",
}
INSTAGRAM_AUDIT_BATCH_STATE_KEYS = set(INSTAGRAM_AUDIT_BATCH_STATE_LABELS)

INSTAGRAM_AUDIT_ITEM_STATE_LABELS = {
    "queued": "В очереди",
    "launching": "Запускаю helper",
    "login_check": "Проверяю вход",
    "mail_check_if_needed": "Проверяю почту",
    "done": "Готово",
}
INSTAGRAM_AUDIT_ITEM_STATE_KEYS = set(INSTAGRAM_AUDIT_ITEM_STATE_LABELS)

INSTAGRAM_AUDIT_RESOLUTION_LABELS = {
    "login_ok": "Вход OK",
    "manual_2fa_required": "Нужен 2FA",
    "email_code_required": "Нужен код с почты",
    "challenge_required": "Нужен challenge",
    "invalid_password": "Неверный пароль",
    "helper_error": "Ошибка helper",
    "missing_credentials": "Нет данных входа",
    "missing_device": "Нет устройства",
}
INSTAGRAM_AUDIT_RESOLUTION_KEYS = set(INSTAGRAM_AUDIT_RESOLUTION_LABELS)

INSTAGRAM_AUDIT_MAIL_PROBE_STATE_LABELS = {
    "pending": "Не проверялась",
    "not_required": "Не требуется",
    "checking": "Проверяю почту",
    "ok": "Почта OK",
    "empty": "Писем нет",
    "auth_error": "Ошибка входа",
    "connect_error": "Ошибка подключения",
    "unsupported": "Неподдерживаемая почта",
    "not_configured": "Почта не настроена",
}
INSTAGRAM_AUDIT_MAIL_PROBE_STATE_KEYS = set(INSTAGRAM_AUDIT_MAIL_PROBE_STATE_LABELS)

PUBLISH_BATCH_STATE_LABELS = {
    "queued_to_worker": "Ждёт запуск",
    "worker_started": "Запускается",
    "generating": "Генерация",
    "publishing": "Публикация",
    "completed": "Завершён",
    "completed_needs_review": "Завершён, нужна проверка",
    "completed_with_errors": "Завершён с ошибками",
    "failed_generation": "Ошибка генерации",
    "canceled": "Отменён",
}
PUBLISH_BATCH_STATE_KEYS = set(PUBLISH_BATCH_STATE_LABELS)

PUBLISH_BATCH_ACCOUNT_STATE_LABELS = {
    "queued_for_generation": "Ждёт генерацию",
    "generating": "Генерируется видео",
    "generation_failed": "Генерация не удалась",
    "queued_for_publish": "Ждёт публикацию",
    "leased": "Взята в работу",
    "preparing": "Подготовка",
    "importing_media": "Импорт медиа",
    "opening_reel_flow": "Открываю Reel",
    "selecting_media": "Выбор видео",
    "publishing": "Публикация",
    "published": "Опубликовано",
    "needs_review": "Нужна проверка",
    "failed": "Ошибка публикации",
    "canceled": "Отменено",
}
PUBLISH_BATCH_ACCOUNT_STATE_KEYS = set(PUBLISH_BATCH_ACCOUNT_STATE_LABELS)

PUBLISH_JOB_STATE_LABELS = {
    "queued": "В очереди",
    "leased": "Взята в работу",
    "preparing": "Подготовка",
    "importing_media": "Импорт медиа",
    "opening_reel_flow": "Открываю Reel",
    "selecting_media": "Выбор видео",
    "publishing": "Публикация",
    "published": "Опубликовано",
    "needs_review": "Нужна проверка",
    "failed": "Ошибка",
    "canceled": "Отменено",
}
PUBLISH_JOB_STATE_KEYS = set(PUBLISH_JOB_STATE_LABELS)

PUBLISH_GENERATION_STAGE_LABELS = {
    "workflow_started": "Запуск workflow",
    "script_generation": "Генерация сценария",
    "image_generation": "Генерация изображений",
    "video_render": "Рендер видео",
    "artifact_packaging": "Подготовка файла",
}

PUBLISH_JOB_STATE_ORDER = {
    "queued": 10,
    "leased": 20,
    "preparing": 30,
    "importing_media": 40,
    "opening_reel_flow": 50,
    "selecting_media": 60,
    "publishing": 70,
    "published": 80,
    "needs_review": 80,
    "failed": 80,
    "canceled": 80,
}

ACTIVE_PUBLISH_JOB_STATES = {
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
}

ACTIVE_PUBLISH_BATCH_ACCOUNT_STATES = {
    "generating",
    "queued_for_publish",
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
}

TERMINAL_PUBLISH_BATCH_ACCOUNT_STATES = {
    "generation_failed",
    "published",
    "needs_review",
    "failed",
    "canceled",
}
