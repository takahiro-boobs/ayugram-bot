import csv
from contextlib import asynccontextmanager
import hashlib
import hmac
import json
import logging
import os
import re
import secrets
import shutil
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote, quote_plus, urlparse

from dotenv import load_dotenv
from fastapi import Body, Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_303_SEE_OTHER

import db
import http_utils
import mail_service
from domain_states import (
    ACCOUNT_INSTAGRAM_LAUNCH_STATUS_LABELS,
    ACCOUNT_INSTAGRAM_PUBLISH_STATUS_LABELS,
    INSTAGRAM_AUDIT_BATCH_STATE_LABELS,
    INSTAGRAM_AUDIT_ITEM_STATE_LABELS,
    INSTAGRAM_AUDIT_MAIL_PROBE_STATE_LABELS,
    INSTAGRAM_AUDIT_RESOLUTION_LABELS,
    PUBLISH_BATCH_ACCOUNT_STATE_LABELS,
    PUBLISH_BATCH_STATE_LABELS,
    PUBLISH_GENERATION_STAGE_LABELS,
    PUBLISH_JOB_STATE_LABELS,
)
from settings import load_web_settings

load_dotenv()

logger = logging.getLogger(__name__)
SETTINGS = load_web_settings()

ADMIN_USER = SETTINGS.admin_user
ADMIN_PASS = SETTINGS.admin_pass
BOT_TOKEN = SETTINGS.bot_token
BOT_USERNAME = SETTINGS.bot_username
ADMIN_TEST_CHAT_ID = SETTINGS.admin_test_chat_id
SESSION_SECRET = SETTINGS.session_secret
SESSION_MAX_AGE_SECONDS = SETTINGS.session_max_age_seconds
MAX_BROADCAST_MEDIA_BYTES = SETTINGS.max_broadcast_media_bytes
ADMIN_BASE_PATH_RAW = SETTINGS.admin_base_path_raw
HELPER_API_KEY = SETTINGS.helper_api_key
HELPER_TICKET_TTL_SECONDS = SETTINGS.helper_ticket_ttl_seconds
INSTAGRAM_APP_HELPER_OPEN_URL = SETTINGS.instagram_app_helper_open_url
INSTAGRAM_PUBLISH_SOURCE_DIR = SETTINGS.instagram_publish_source_dir
PUBLISH_VIDEO_EXTENSIONS = {".mp4", ".mov"}
PUBLISH_N8N_WEBHOOK_URL = SETTINGS.publish_n8n_webhook_url
PUBLISH_STAGING_DIR = SETTINGS.publish_staging_dir
PUBLISH_BASE_URL = SETTINGS.publish_base_url
PUBLISH_SHARED_SECRET = SETTINGS.publish_shared_secret
PUBLISH_WEBHOOK_MAX_AGE_SECONDS = SETTINGS.publish_webhook_max_age_seconds
PUBLISH_FACTORY_TIMEOUT_SECONDS = SETTINGS.publish_factory_timeout_seconds
PUBLISH_RUNNER_API_KEY = SETTINGS.publish_runner_api_key
PUBLISH_RUNNER_LEASE_SECONDS = SETTINGS.publish_runner_lease_seconds
PUBLISH_DEFAULT_WORKFLOW = SETTINGS.publish_default_workflow
MAIL_COLLECTOR_ENABLED = SETTINGS.mail_collector_enabled
MAIL_COLLECTOR_RECONCILE_SECONDS = SETTINGS.mail_collector_reconcile_seconds
MAIL_COLLECTOR_STALE_SYNC_SECONDS = SETTINGS.mail_collector_stale_sync_seconds
MAIL_COLLECTOR_WATCH_RENEW_MARGIN_SECONDS = SETTINGS.mail_collector_watch_renew_margin_seconds
MAIL_WEBHOOK_SECRET = SETTINGS.mail_webhook_secret
STRICT_CONFIG = SETTINGS.strict_config
EMBED_RUNTIME_WORKER = SETTINGS.embed_runtime_worker


def _config_warnings() -> list[str]:
    warnings: list[str] = []
    if not SESSION_SECRET or SESSION_SECRET == "change-me":
        warnings.append("SESSION_SECRET is default or empty")
    if not ADMIN_PASS or ADMIN_PASS == "admin":
        warnings.append("ADMIN_PASS is default or empty")
    if not PUBLISH_SHARED_SECRET:
        warnings.append("PUBLISH_SHARED_SECRET is empty")
    if not PUBLISH_RUNNER_API_KEY:
        warnings.append("PUBLISH_RUNNER_API_KEY is empty (publish runner disabled)")
    if not HELPER_API_KEY:
        warnings.append("HELPER_API_KEY is empty (helper callbacks disabled)")
    return warnings


def _validate_runtime_config() -> None:
    warnings = _config_warnings()
    for item in warnings:
        logger.warning("config_warning: %s", item)
    if STRICT_CONFIG and warnings:
        raise RuntimeError(f"Config validation failed: {', '.join(warnings)}")


def _normalize_base_path(raw: str) -> str:
    value = (raw or "").strip()
    if not value or value == "/":
        return ""
    if not value.startswith("/"):
        value = "/" + value
    value = value.rstrip("/")
    return value


ADMIN_BASE_PATH = _normalize_base_path(ADMIN_BASE_PATH_RAW)

ACCOUNT_TYPE_OPTIONS = [
    {"key": "youtube", "label": "YouTube"},
    {"key": "tiktok", "label": "TikTok"},
    {"key": "instagram", "label": "Instagram"},
]
ACCOUNT_TYPE_LABELS = {opt["key"]: opt["label"] for opt in ACCOUNT_TYPE_OPTIONS}
ACCOUNT_ROTATION_STATE_OPTIONS = [
    {"key": "review", "label": "На проверке"},
    {"key": "working", "label": "Рабочий"},
    {"key": "not_working", "label": "Нерабочий"},
]
ACCOUNT_ROTATION_STATE_LABELS = {opt["key"]: opt["label"] for opt in ACCOUNT_ROTATION_STATE_OPTIONS}
ACCOUNT_VIEWS_STATE_OPTIONS = [
    {"key": "unknown", "label": "Не задано"},
    {"key": "low", "label": "Мало просмотров"},
    {"key": "good", "label": "Норм просмотры"},
]
ACCOUNT_VIEWS_STATE_LABELS = {opt["key"]: opt["label"] for opt in ACCOUNT_VIEWS_STATE_OPTIONS}
ACCOUNT_LIST_SORT_OPTIONS = [
    {"key": "recent", "label": "Сначала новые"},
    {"key": "transitions_desc", "label": "Переходов: больше"},
    {"key": "transitions_asc", "label": "Переходов: меньше"},
]
ACCOUNT_MAIL_PROVIDER_OPTIONS = [
    {"key": "auto", "label": "Auto / IMAP"},
    {"key": "imap", "label": "IMAP"},
    {"key": "gmail_api", "label": "Gmail API"},
    {"key": "microsoft_graph", "label": "Microsoft Graph"},
]
ACCOUNT_MAIL_PROVIDER_LABELS = {opt["key"]: opt["label"] for opt in ACCOUNT_MAIL_PROVIDER_OPTIONS}
ACCOUNT_MAIL_STATUS_LABELS = {
    "never_checked": "Не проверялась",
    "ok": "Почта OK",
    "auth_error": "Ошибка входа",
    "connect_error": "Ошибка подключения",
    "empty": "Писем нет",
    "unsupported": "Неподдерживаемая почта",
}
ACCOUNT_MAIL_CHALLENGE_STATUS_LABELS = {
    "idle": "Нет challenge",
    "resolved": "Challenge найден",
    "not_found": "Код не найден",
    "ambiguous": "Найдено несколько писем",
    "mailbox_unavailable": "Почта недоступна",
    "unsupported": "Challenge не поддержан",
}
ACCOUNT_MAIL_CHALLENGE_KIND_LABELS = {
    "numeric_code": "Код из письма",
    "approval_link": "Ссылка подтверждения",
    "unsupported": "Неподдерживаемый сценарий",
}
RUNTIME_TASK_STATE_LABELS = {
    "queued": "В очереди",
    "running": "Выполняется",
    "retrying": "Повтор",
    "completed": "Завершена",
    "failed": "Ошибка",
    "canceled": "Отменена",
}
PUBLISH_PROGRESS_STEPS = [
    {"key": "workflow_started", "label": "Запуск"},
    {"key": "video_production", "label": "Генерация видео"},
    {"key": "publish_queue", "label": "Очередь публикации"},
    {"key": "instagram_publish", "label": "Публикация в Instagram"},
    {"key": "done", "label": "Готово"},
]
INSTAGRAM_AUDIT_MAIL_PROBE_LABELS = INSTAGRAM_AUDIT_MAIL_PROBE_STATE_LABELS
ACCOUNTS_IMPORT_MAX_BYTES = SETTINGS.accounts_import_max_bytes
INSTAGRAM_AUDIT_POLL_INTERVAL_SECONDS = SETTINGS.instagram_audit_poll_interval_seconds
INSTAGRAM_AUDIT_HELPER_POLL_SECONDS = SETTINGS.instagram_audit_helper_poll_seconds
INSTAGRAM_AUDIT_HELPER_IDLE_TIMEOUT_SECONDS = SETTINGS.instagram_audit_helper_idle_timeout_seconds
INSTAGRAM_AUDIT_LOGIN_TIMEOUT_SECONDS = SETTINGS.instagram_audit_login_timeout_seconds
INSTAGRAM_AUDIT_MAIL_FRESHNESS_SECONDS = SETTINGS.instagram_audit_mail_freshness_seconds
INSTAGRAM_MAIL_CHALLENGE_TIMEOUT_SECONDS = SETTINGS.instagram_mail_challenge_timeout_seconds
INSTAGRAM_MAIL_CHALLENGE_POLL_SECONDS = SETTINGS.instagram_mail_challenge_poll_seconds
INSTAGRAM_MAIL_CHALLENGE_LOOKBACK_SECONDS = SETTINGS.instagram_mail_challenge_lookback_seconds
INSTAGRAM_MAIL_CHALLENGE_FRESHNESS_SECONDS = SETTINGS.instagram_mail_challenge_freshness_seconds
INSTAGRAM_AUDIT_ITEM_RETRY_ATTEMPTS = SETTINGS.instagram_audit_item_retry_attempts
INSTAGRAM_AUDIT_FORCE_CLEAN_LOGIN = SETTINGS.instagram_audit_force_clean_login
RUNTIME_TASK_LEASE_SECONDS = SETTINGS.runtime_task_lease_seconds
RUNTIME_WORKER_HEARTBEAT_SECONDS = SETTINGS.runtime_worker_heartbeat_seconds
RUNTIME_TASK_RETRY_DELAY_SECONDS = SETTINGS.runtime_task_retry_delay_seconds
RUNTIME_RECONCILE_INTERVAL_SECONDS = SETTINGS.runtime_reconcile_interval_seconds
RUNTIME_WORKER_LIVE_TIMEOUT_SECONDS = SETTINGS.runtime_worker_live_timeout_seconds
RUNTIME_WORKER_NAME = SETTINGS.runtime_worker_name
RUNTIME_WORKER_IDLE_POLL_SECONDS = SETTINGS.runtime_worker_idle_poll_seconds

RUNTIME_WORKER_LOCK = threading.RLock()
RUNTIME_WORKER_STOP = threading.Event()
RUNTIME_WORKER_WAKEUP = threading.Event()
RUNTIME_WORKER_THREAD: Optional[threading.Thread] = None
MAIL_COLLECTOR_LOCK = threading.RLock()
MAIL_COLLECTOR_LAST_RECONCILE_AT = 0.0
RuntimeHeartbeat = Callable[[], None]


class RuntimeTaskError(RuntimeError):
    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = bool(retryable)


class InstagramAuditInfrastructureError(RuntimeTaskError):
    def __init__(self, message: str) -> None:
        super().__init__(message, retryable=True)

@asynccontextmanager
async def lifespan(_: FastAPI):
    _validate_runtime_config()
    db.init_db()
    Path(PUBLISH_STAGING_DIR).expanduser().mkdir(parents=True, exist_ok=True)
    if EMBED_RUNTIME_WORKER:
        start_runtime_worker_service()
    try:
        yield
    finally:
        if EMBED_RUNTIME_WORKER:
            stop_runtime_worker_service()


app = FastAPI(lifespan=lifespan)


class CompatJinja2Templates(Jinja2Templates):
    def TemplateResponse(self, *args: Any, **kwargs: Any) -> HTMLResponse:
        if args and isinstance(args[0], str):
            context = args[1] if len(args) > 1 else kwargs.get("context")
            if isinstance(context, dict) and "request" in context:
                request = context["request"]
                return super().TemplateResponse(request, args[0], context, *args[2:], **kwargs)
        if "name" in kwargs and "context" in kwargs and isinstance(kwargs["context"], dict) and "request" in kwargs["context"]:
            return super().TemplateResponse(
                kwargs["context"]["request"],
                kwargs["name"],
                kwargs["context"],
                **{key: value for key, value in kwargs.items() if key not in {"name", "context"}},
            )
        return super().TemplateResponse(*args, **kwargs)


templates = CompatJinja2Templates(directory="templates")
templates.env.filters["dt"] = lambda ts: datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M") if ts else "-"


def with_base(path: str) -> str:
    raw = (path or "").strip()
    if not raw:
        raw = "/"
    if raw.startswith(("http://", "https://")):
        return raw
    if not raw.startswith("/"):
        raw = "/" + raw
    if ADMIN_BASE_PATH and (raw == ADMIN_BASE_PATH or raw.startswith(ADMIN_BASE_PATH + "/")):
        return raw
    return f"{ADMIN_BASE_PATH}{raw}" if ADMIN_BASE_PATH else raw


templates.env.globals["urlp"] = with_base
templates.env.globals["admin_base_path"] = ADMIN_BASE_PATH

app.mount("/static", StaticFiles(directory="static"), name="static")
app.add_middleware(
    SessionMiddleware,
    secret_key=SESSION_SECRET,
    max_age=SESSION_MAX_AGE_SECONDS,
    same_site="lax",
)


def strip_base_from_scope(scope: dict) -> None:
    if not ADMIN_BASE_PATH:
        return
    path = str(scope.get("path") or "")
    if path == ADMIN_BASE_PATH:
        scope["path"] = "/"
        return
    prefix = ADMIN_BASE_PATH + "/"
    if path.startswith(prefix):
        stripped = path[len(ADMIN_BASE_PATH) :]
        scope["path"] = stripped if stripped else "/"


@app.middleware("http")
async def _admin_base_path_middleware(request: Request, call_next):
    if ADMIN_BASE_PATH:
        strip_base_from_scope(request.scope)
    return await call_next(request)


def _redirect(url: str, status_code: int = HTTP_303_SEE_OTHER) -> RedirectResponse:
    return RedirectResponse(url=with_base(url), status_code=status_code)


class NeedsAdminLogin(Exception):
    pass


class NeedsWorkerLogin(Exception):
    pass


@app.exception_handler(NeedsAdminLogin)
async def _needs_admin_login_handler(request: Request, exc: NeedsAdminLogin):
    return _redirect("/login", status_code=303)


@app.exception_handler(NeedsWorkerLogin)
async def _needs_worker_login_handler(request: Request, exc: NeedsWorkerLogin):
    return _redirect("/worker/login", status_code=303)


def require_auth(request: Request) -> None:
    if not request.session.get("admin"):
        raise NeedsAdminLogin()


def require_worker_auth(request: Request) -> None:
    worker_id = request.session.get("worker_id")
    if not worker_id:
        raise NeedsWorkerLogin()
    try:
        wid = int(worker_id)
    except Exception:
        request.session.pop("worker_id", None)
        request.session.pop("worker_name", None)
        raise NeedsWorkerLogin()
    if db.get_worker(wid) is None:
        request.session.pop("worker_id", None)
        request.session.pop("worker_name", None)
        raise NeedsWorkerLogin()


def require_helper_api_key(x_helper_api_key: Optional[str] = Header(None)) -> None:
    if not HELPER_API_KEY:
        raise HTTPException(status_code=503, detail="Helper API is not configured")
    if (x_helper_api_key or "").strip() != HELPER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid helper API key")


def require_publish_runner_api_key(
    x_runner_api_key: Optional[str] = Header(None),
    x_helper_api_key: Optional[str] = Header(None),
) -> None:
    expected = (PUBLISH_RUNNER_API_KEY or HELPER_API_KEY).strip()
    if not expected:
        raise HTTPException(status_code=503, detail="Publish runner API is not configured")
    provided = (x_runner_api_key or x_helper_api_key or "").strip()
    if provided != expected:
        raise HTTPException(status_code=401, detail="Invalid publish runner API key")


@app.get("/r/{code}")
def redirect_link(code: str, request: Request):
    code_clean = (code or "").strip()
    link = db.get_active_link(code_clean)
    if not link:
        return HTMLResponse("Not found", status_code=404)

    forwarded_for = (request.headers.get("x-forwarded-for") or "").strip()
    if forwarded_for:
        ip = forwarded_for.split(",", 1)[0].strip()
    else:
        ip = request.client.host if request.client else None

    account_id = int(link["account_id"]) if link["account_id"] is not None else None
    db.log_click(
        code=str(link["code"]),
        user_agent=request.headers.get("user-agent"),
        ip=ip,
        account_id=account_id,
    )
    return RedirectResponse(url=_build_bot_start_url(str(link["code"])), status_code=302)


def _normalize_account_type(raw: Optional[str]) -> Optional[str]:
    val = (raw or "").strip().lower()
    if not val:
        return None
    allowed = {opt["key"] for opt in ACCOUNT_TYPE_OPTIONS}
    if val not in allowed:
        raise ValueError("invalid account type")
    return val


def _normalize_rotation_state_filter(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return ""
    return db.normalize_account_rotation_state(value)


def _normalize_views_state_filter(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if not value:
        return ""
    return db.normalize_account_views_state(value)


def _normalize_account_list_sort(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower() or "recent"
    allowed = {opt["key"] for opt in ACCOUNT_LIST_SORT_OPTIONS}
    if value not in allowed:
        raise ValueError("invalid account list sort")
    return value


def _account_rotation_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "review"
    return ACCOUNT_ROTATION_STATE_LABELS.get(value, "На проверке"), value


def _account_views_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "unknown"
    return ACCOUNT_VIEWS_STATE_LABELS.get(value, "Не задано"), value


def _account_views_short_label(state: Optional[str]) -> str:
    value = (state or "").strip().lower()
    if value == "good":
        return "много"
    if value == "low":
        return "мало"
    return ""


def _account_mail_provider_label(provider: Optional[str]) -> str:
    value = (provider or "auto").strip().lower() or "auto"
    return ACCOUNT_MAIL_PROVIDER_LABELS.get(value, value.upper() or "AUTO")


def _account_mail_ready_meta(account: dict[str, Any]) -> tuple[str, str, str]:
    provider = str(account.get("mail_provider") or "auto").strip().lower() or "auto"
    email_value = db.sanitize_account_text_field(account.get("email"))
    if not email_value:
        return ("Почта не задана", "off", "Не заполнен email аккаунта.")
    if provider in {"gmail_api", "microsoft_graph"}:
        raw_auth = str(account.get("mail_auth_json") or "").strip()
        if not raw_auth:
            return ("Не готова", "review", "Для provider-native режима не заполнен mail_auth_json.")
        try:
            payload = json.loads(raw_auth)
        except Exception:
            return ("Неверный auth JSON", "off", "mail_auth_json не является валидным JSON-объектом.")
        if not isinstance(payload, dict) or not payload:
            return ("Не готова", "review", "mail_auth_json пустой или имеет неверный формат.")
        return ("Готова", "on", "Provider-native mail access настроен.")
    if db.sanitize_account_text_field(account.get("email_password")):
        return ("Готова", "on", "IMAP / app password настроен.")
    return ("Не готова", "review", "Для IMAP-режима не заполнен пароль почты.")


def _account_mail_status_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "never_checked"
    label = ACCOUNT_MAIL_STATUS_LABELS.get(value, "Не проверялась")
    status_class = {
        "ok": "on",
        "empty": "review",
        "never_checked": "unknown",
        "auth_error": "low",
        "connect_error": "off",
        "unsupported": "off",
    }.get(value, "unknown")
    return label, status_class


def _account_mail_challenge_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "idle"
    label = ACCOUNT_MAIL_CHALLENGE_STATUS_LABELS.get(value, "Нет challenge")
    status_class = {
        "idle": "unknown",
        "resolved": "on",
        "not_found": "review",
        "ambiguous": "review",
        "mailbox_unavailable": "off",
        "unsupported": "review",
    }.get(value, "unknown")
    return label, status_class


def _account_instagram_launch_status_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "idle"
    label = ACCOUNT_INSTAGRAM_LAUNCH_STATUS_LABELS.get(value, "Не запускался")
    status_class = {
        "idle": "unknown",
        "login_submitted": "wait",
        "manual_2fa_required": "wait",
        "challenge_required": "review",
        "invalid_password": "low",
        "helper_error": "off",
    }.get(value, "unknown")
    return label, status_class


def _account_instagram_publish_status_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "idle"
    label = ACCOUNT_INSTAGRAM_PUBLISH_STATUS_LABELS.get(value, "Не запускался")
    status_class = {
        "idle": "unknown",
        "preparing": "wait",
        "login_required": "review",
        "manual_2fa_required": "review",
        "email_code_required": "review",
        "challenge_required": "off",
        "invalid_password": "low",
        "importing_media": "wait",
        "opening_reel_flow": "wait",
        "selecting_media": "wait",
        "publishing": "wait",
        "published": "on",
        "needs_review": "review",
        "no_source_video": "review",
        "publish_error": "off",
    }.get(value, "unknown")
    return label, status_class


def _runtime_task_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued"
    label = RUNTIME_TASK_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued": "unknown",
        "running": "wait",
        "retrying": "review",
        "completed": "on",
        "failed": "off",
        "canceled": "off",
    }.get(value, "unknown")
    return label, status_class


def _instagram_audit_batch_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued"
    label = INSTAGRAM_AUDIT_BATCH_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued": "unknown",
        "running": "wait",
        "completed": "on",
        "completed_with_errors": "review",
        "failed": "off",
        "canceled": "off",
    }.get(value, "unknown")
    return label, status_class


def _instagram_audit_item_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued"
    label = INSTAGRAM_AUDIT_ITEM_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued": "unknown",
        "launching": "wait",
        "login_check": "wait",
        "mail_check_if_needed": "wait",
        "done": "on",
    }.get(value, "unknown")
    return label, status_class


def _instagram_audit_resolution_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower()
    label = INSTAGRAM_AUDIT_RESOLUTION_LABELS.get(value, "Ожидание") if value else "Ожидание"
    status_class = {
        "login_ok": "on",
        "manual_2fa_required": "review",
        "email_code_required": "review",
        "challenge_required": "review",
        "invalid_password": "low",
        "helper_error": "off",
        "missing_credentials": "off",
        "missing_device": "off",
    }.get(value, "unknown")
    return label, status_class


def _instagram_audit_joke(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower()
    if value == "login_ok":
        label = "рабочий"
    elif value == "manual_2fa_required":
        label = "просит 2FA"
    elif value == "email_code_required":
        label = "просит код с почты"
    elif value:
        label = "ручная проверка"
    else:
        label = "нет проверки"
    _, status_class = _instagram_audit_resolution_meta(value)
    return label, status_class


def _compact_account_block_reason(account: dict[str, Any]) -> str:
    rotation_state = str(account.get("rotation_state") or "").strip().lower()
    launch_status = str(account.get("instagram_launch_status") or "").strip().lower()
    publish_status = str(account.get("instagram_publish_status") or "").strip().lower()
    audit_status = str(account.get("latest_audit_resolution_state") or "").strip().lower()
    reason = str(account.get("rotation_state_reason") or "").strip()
    reason_lower = reason.lower()
    issues = [item.lower() for item in db.publish_account_readiness_issues(account)]

    if rotation_state == "working" and not reason and not issues:
        return ""

    if (
        launch_status == "manual_2fa_required"
        or publish_status == "manual_2fa_required"
        or audit_status == "manual_2fa_required"
        or "2fa" in reason_lower
    ):
        return "Нужен 2FA"
    if (
        launch_status == "invalid_password"
        or publish_status == "invalid_password"
        or audit_status == "invalid_password"
        or "неверн" in reason_lower and "парол" in reason_lower
        or "invalid password" in reason_lower
    ):
        return "Неверный пароль"
    if "trusted" in reason_lower or "another device" in reason_lower or "довер" in reason_lower:
        return "Доверенное устройство"
    if (
        publish_status == "email_code_required"
        or audit_status == "email_code_required"
        or any(token in reason_lower for token in ("почт", "email", "mail", "imap", "письм"))
    ):
        return "Проблема с почтой"
    if any(token in reason_lower for token in ("эмулятор", "emulator", "serial")):
        return "Нет эмулятора"
    if any("account login" in item or "account password" in item for item in issues):
        return "Нет логина/пароля"
    if any("emulator serial" in item for item in issues):
        return "Нет эмулятора"
    if "нет подтверждённой проверки входа" in reason_lower or "проверки входа" in reason_lower:
        return "Нет проверки"
    if launch_status == "challenge_required" or publish_status == "challenge_required" or audit_status == "challenge_required":
        return "Challenge"
    if rotation_state == "not_working":
        return "Нерабочий"
    return ""


def _instagram_audit_mail_probe_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "pending"
    label = INSTAGRAM_AUDIT_MAIL_PROBE_LABELS.get(value, "Не проверялась")
    status_class = {
        "pending": "unknown",
        "not_required": "unknown",
        "checking": "wait",
        "ok": "on",
        "empty": "review",
        "auth_error": "low",
        "connect_error": "off",
        "unsupported": "off",
        "not_configured": "review",
    }.get(value, "unknown")
    return label, status_class


def _publish_batch_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued_to_worker"
    label = PUBLISH_BATCH_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued_to_worker": "unknown",
        "worker_started": "wait",
        "generating": "wait",
        "publishing": "wait",
        "completed": "on",
        "completed_needs_review": "review",
        "completed_with_errors": "review",
        "failed_generation": "off",
        "canceled": "off",
    }.get(value, "unknown")
    return label, status_class


def _publish_job_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued"
    label = PUBLISH_JOB_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued": "unknown",
        "leased": "wait",
        "preparing": "wait",
        "importing_media": "wait",
        "opening_reel_flow": "wait",
        "selecting_media": "wait",
        "publishing": "wait",
        "published": "on",
        "needs_review": "review",
        "failed": "off",
        "canceled": "review",
    }.get(value, "unknown")
    return label, status_class


def _publish_batch_account_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "queued_for_generation"
    label = PUBLISH_BATCH_ACCOUNT_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "queued_for_generation": "unknown",
        "generating": "wait",
        "generation_failed": "off",
        "queued_for_publish": "unknown",
        "leased": "wait",
        "preparing": "wait",
        "importing_media": "wait",
        "opening_reel_flow": "wait",
        "selecting_media": "wait",
        "publishing": "wait",
        "published": "on",
        "needs_review": "review",
        "failed": "off",
        "canceled": "review",
    }.get(value, "unknown")
    return label, status_class


def _publish_batch_is_terminal(state: Optional[str]) -> bool:
    value = (state or "").strip().lower()
    return value in {"completed", "completed_needs_review", "completed_with_errors", "failed_generation", "canceled"}


def _account_identity_handle(account: dict | object) -> str:
    if isinstance(account, dict):
        username = account.get("username")
        account_login = account.get("account_login")
    else:
        username = getattr(account, "username", "")
        account_login = getattr(account, "account_login", "")
    return db.normalize_account_handle(str(username or "") or str(account_login or ""))


def _account_matches_handle(account: dict | object, raw_handle: Optional[str]) -> bool:
    expected = db.normalize_account_handle(raw_handle)
    if not expected:
        return False
    if isinstance(account, dict):
        candidates = [account.get("username"), account.get("account_login")]
    else:
        candidates = [getattr(account, "username", ""), getattr(account, "account_login", "")]
    normalized = {db.normalize_account_handle(str(value or "")) for value in candidates if str(value or "").strip()}
    return expected in normalized


def _accounts_redirect_url(
    q: str,
    account_type: str,
    worker_filter: str = "",
    rotation_state: str = "",
    views_state: str = "",
    sort: str = "recent",
) -> str:
    parts = []
    if q:
        parts.append(f"q={quote_plus(q)}")
    if account_type:
        parts.append(f"type={quote_plus(account_type)}")
    if worker_filter:
        parts.append(f"worker={quote_plus(worker_filter)}")
    if rotation_state:
        parts.append(f"rotation_state={quote_plus(rotation_state)}")
    if views_state:
        parts.append(f"views_state={quote_plus(views_state)}")
    if sort and sort != "recent":
        parts.append(f"sort={quote_plus(sort)}")
    if not parts:
        return "/accounts"
    return "/accounts?" + "&".join(parts)


def _worker_detail_redirect_url(worker_id: int, q: str, account_type: str, sort: str = "recent") -> str:
    parts = []
    if q:
        parts.append(f"q={quote_plus(q)}")
    if account_type:
        parts.append(f"type={quote_plus(account_type)}")
    if sort and sort != "recent":
        parts.append(f"sort={quote_plus(sort)}")
    base = f"/workers/{int(worker_id)}"
    if not parts:
        return base
    return base + "?" + "&".join(parts)


def _safe_next_url(next_url: Optional[str], fallback: str = "/accounts") -> str:
    raw = (next_url or "").strip()
    if raw.startswith("/") and not raw.startswith("//"):
        return raw
    return fallback


def _build_detail_url(path: str, return_to: Optional[str]) -> str:
    return_to_clean = _safe_next_url(return_to, fallback="")
    if not return_to_clean:
        return path
    sep = "&" if "?" in path else "?"
    return f"{path}{sep}return_to={quote(return_to_clean, safe='')}"


def _build_bot_start_url(code: str) -> str:
    return f"https://t.me/{BOT_USERNAME}?start={quote_plus((code or '').strip())}"


def _build_social_profile_url(account_type: str, username: str) -> Optional[str]:
    t = (account_type or "").strip().lower()
    handle = (username or "").strip()
    if handle.startswith("@"):
        handle = handle[1:]
    handle = handle.strip()
    if not handle:
        return None
    if any(ch.isspace() for ch in handle):
        return None

    encoded = quote(handle, safe="._-")
    if t == "instagram":
        return f"https://www.instagram.com/{encoded}/"
    if t == "tiktok":
        return f"https://www.tiktok.com/@{encoded}"
    if t == "youtube":
        return f"https://www.youtube.com/@{encoded}"
    return None


def _build_instagram_helper_open_url(ticket: str) -> str:
    base = (INSTAGRAM_APP_HELPER_OPEN_URL or "http://127.0.0.1:17374/open").strip() or "http://127.0.0.1:17374/open"
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}ticket={quote_plus((ticket or '').strip())}"


def _build_instagram_helper_local_url(path: str) -> str:
    base = (INSTAGRAM_APP_HELPER_OPEN_URL or "http://127.0.0.1:17374/open").strip() or "http://127.0.0.1:17374/open"
    base = base.split("?", 1)[0]
    if "/" in base.rsplit("://", 1)[-1]:
        base = base.rsplit("/", 1)[0]
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return f"{base}{suffix}"


def _instagram_audit_batch_is_terminal(state: Optional[str]) -> bool:
    value = (state or "").strip().lower()
    return value in {"completed", "completed_with_errors", "failed", "canceled"}


def _helper_request_headers() -> dict[str, str]:
    if not HELPER_API_KEY:
        raise RuntimeError("HELPER_API_KEY не настроен.")
    return {"X-Helper-Api-Key": HELPER_API_KEY, "Accept": "application/json"}


def _fetch_helper_emulator_inventory() -> dict[str, Any]:
    url = _build_instagram_helper_local_url("/api/helper/emulators")
    response = http_utils.request_with_retry(
        "GET",
        url,
        headers=_helper_request_headers(),
        timeout=20,
        allow_retry=True,
        log_context="instagram_audit_helper_inventory",
    )
    if response.status_code != 200:
        raise RuntimeError(f"Helper inventory returned {response.status_code}")
    payload = response.json()
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError(str(payload.get("detail") or "Helper inventory is unavailable"))
    return payload


def _helper_is_busy(payload: dict[str, Any]) -> bool:
    state = payload.get("state") if isinstance(payload, dict) else {}
    if isinstance(state, dict) and state.get("flow_running"):
        return True
    return False


def _wait_for_helper_idle(
    timeout_seconds: int = INSTAGRAM_AUDIT_HELPER_IDLE_TIMEOUT_SECONDS,
    *,
    heartbeat: Optional[RuntimeHeartbeat] = None,
) -> dict[str, Any]:
    deadline = time.time() + max(5, int(timeout_seconds))
    last_payload: dict[str, Any] = {}
    while time.time() < deadline:
        if heartbeat is not None:
            heartbeat()
        last_payload = _fetch_helper_emulator_inventory()
        if not _helper_is_busy(last_payload):
            return last_payload
        time.sleep(INSTAGRAM_AUDIT_HELPER_POLL_SECONDS)
    raise RuntimeError("Helper занят слишком долго. Дождись завершения текущего flow и повтори аудит.")


def _launch_instagram_helper_ticket(ticket: str) -> dict[str, Any]:
    url = _build_instagram_helper_local_url("/api/helper/launch-ticket")
    response = http_utils.request_with_retry(
        "POST",
        url,
        headers={**_helper_request_headers(), "Content-Type": "application/json"},
        json={"ticket": str(ticket or "").strip()},
        timeout=30,
        allow_retry=False,
        log_context="instagram_audit_helper_launch",
    )
    if response.status_code != 200:
        try:
            payload = response.json()
        except Exception:
            payload = {}
        raise RuntimeError(str(payload.get("detail") or f"Helper launch returned {response.status_code}"))
    payload = response.json()
    if not isinstance(payload, dict) or not payload.get("ok"):
        raise RuntimeError(str(payload.get("detail") or "Helper did not accept audit ticket"))
    return payload


def _account_has_instagram_login(account: dict[str, Any]) -> bool:
    return bool(str(account.get("account_login") or "").strip() and str(account.get("account_password") or "").strip())


def _mail_provider_requires_auth_json(provider: Optional[str]) -> bool:
    return (provider or "").strip().lower() in {"gmail_api", "microsoft_graph"}


def _mail_provider_requires_password(provider: Optional[str]) -> bool:
    return not _mail_provider_requires_auth_json(provider)


def _account_has_mail_credentials(account: dict[str, Any]) -> bool:
    if not db.sanitize_account_text_field(account.get("email")):
        return False
    provider = str(account.get("mail_provider") or "auto").strip().lower() or "auto"
    if _mail_provider_requires_auth_json(provider):
        raw_auth = str(account.get("mail_auth_json") or "").strip()
        if not raw_auth:
            return False
        try:
            payload = json.loads(raw_auth)
        except Exception:
            return False
        return isinstance(payload, dict) and bool(payload)
    return bool(db.sanitize_account_text_field(account.get("email_password")))


def _mail_form_values(
    *,
    email: str,
    email_password: str,
    mail_provider: Optional[str],
    mail_auth_json: Optional[str],
) -> tuple[str, str, str, str]:
    provider_value = db.normalize_account_mail_provider(mail_provider)
    auth_json_raw = (mail_auth_json or "").strip()
    auth_json_value = ""
    if auth_json_raw:
        auth_json_value = db.normalize_account_mail_auth_json(auth_json_raw)
    email_value = (email or "").strip()
    email_password_value = (email_password or "").strip()
    return email_value, email_password_value, provider_value, auth_json_value


def _validate_mail_form_fields(
    *,
    email: str,
    email_password: str,
    mail_provider: Optional[str],
    mail_auth_json: Optional[str],
) -> Optional[str]:
    try:
        email_value, email_password_value, provider_value, auth_json_value = _mail_form_values(
            email=email,
            email_password=email_password,
            mail_provider=mail_provider,
            mail_auth_json=mail_auth_json,
        )
    except ValueError as exc:
        return _account_form_error_message(str(exc))
    if not email_value:
        return "Заполни поле: Почта"
    if _mail_provider_requires_auth_json(provider_value):
        if not auth_json_value:
            return "Заполни поле: Mail auth JSON"
        return None
    if not email_password_value:
        return "Заполни поле: Пароль почты"
    return None


def _mail_messages_with_metadata(account_id: int, *, limit: int = mail_service.MAIL_FETCH_LIMIT) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    for row in db.list_account_mail_messages(int(account_id), limit=limit):
        item = dict(row)
        metadata_raw = str(item.get("metadata_json") or "").strip()
        metadata: dict[str, Any] = {}
        if metadata_raw:
            try:
                parsed = json.loads(metadata_raw)
            except Exception:
                parsed = {}
            if isinstance(parsed, dict):
                metadata = parsed
        item.update(
            {
                "provider_message_id": str(metadata.get("provider_message_id") or item.get("message_uid") or ""),
                "body_text": str(metadata.get("body_text") or ""),
                "body_html": str(metadata.get("body_html") or ""),
                "to_text": str(metadata.get("to_text") or ""),
                "cc_text": str(metadata.get("cc_text") or ""),
                "to_addresses": list(metadata.get("to_addresses") or []),
                "cc_addresses": list(metadata.get("cc_addresses") or []),
                "links": list(metadata.get("links") or []),
                "candidate_code": str(metadata.get("candidate_code") or ""),
                "candidate_link": str(metadata.get("candidate_link") or ""),
                "candidate_confidence": float(metadata.get("candidate_confidence") or 0.0),
            }
        )
        messages.append(item)
    return messages


def _mail_watch_payload(raw: Any) -> dict[str, Any]:
    return _parse_json_object(raw)


def _mail_watch_expiration_ts(raw: Any) -> Optional[int]:
    payload = _mail_watch_payload(raw)
    expiration = payload.get("expiration")
    if expiration in (None, ""):
        return None
    if isinstance(expiration, (int, float)):
        value = int(expiration)
    else:
        text = str(expiration).strip()
        if not text:
            return None
        if text.isdigit():
            value = int(text)
        else:
            try:
                value = int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
            except Exception:
                return None
    if value > 10**12:
        value //= 1000
    return value if value > 0 else None


def _mail_webhook_callback_url(provider: str) -> str:
    try:
        base = _runtime_admin_public_base_url()
    except RuntimeError:
        return ""
    suffix = "/api/internal/mail/webhooks/gmail" if provider == "gmail_api" else "/api/internal/mail/webhooks/microsoft"
    url = f"{base}{suffix}"
    if MAIL_WEBHOOK_SECRET:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}secret={quote_plus(MAIL_WEBHOOK_SECRET)}"
    return url


def _mail_provider_supports_background_sync(provider: Optional[str]) -> bool:
    return (provider or "").strip().lower() in {"gmail_api", "microsoft_graph"}


def _mail_message_candidate_metadata(message: dict[str, Any]) -> dict[str, Any]:
    sender = _collapse_mail_text(message.get("from_text")).lower()
    subject = _collapse_mail_text(message.get("subject")).lower()
    snippet = _collapse_mail_text(message.get("snippet")).lower()
    body_text = _collapse_mail_text(message.get("body_text")).lower()
    body_html = _collapse_mail_text(message.get("body_html")).lower()
    merged = " ".join(part for part in (sender, subject, snippet, body_text, body_html) if part).strip()
    code_candidate = _extract_mail_code_candidate(" ".join(part for part in (subject, body_text, body_html, snippet) if part))
    link_url = _select_instagram_approval_link(list(message.get("links") or []), merged)
    if code_candidate is not None:
        base_score = int(code_candidate.get("score") or 0) + (3 if any(marker in merged for marker in INSTAGRAM_MAIL_SENDER_MARKERS) else 0)
        return {
            "candidate_code": str(code_candidate.get("code") or ""),
            "candidate_link": "",
            "candidate_confidence": _mail_confidence_from_score(base_score),
        }
    if link_url:
        base_score = 6 + (2 if any(marker in merged for marker in INSTAGRAM_MAIL_APPROVAL_MARKERS) else 0)
        return {
            "candidate_code": "",
            "candidate_link": link_url,
            "candidate_confidence": _mail_confidence_from_score(base_score),
        }
    return {"candidate_code": "", "candidate_link": "", "candidate_confidence": 0.0}


def _enrich_mail_messages_for_cache(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    for raw_item in messages:
        item = dict(raw_item)
        item.update(_mail_message_candidate_metadata(item))
        enriched.append(item)
    return enriched


def _sync_account_mailbox(
    account: dict[str, Any],
    *,
    heartbeat: Optional[RuntimeHeartbeat] = None,
    force_refresh: bool = False,
    renew_watch: bool = True,
    reason: str = "",
) -> dict[str, Any]:
    account_id = int(account["id"])
    provider_value = str(account.get("mail_provider") or "auto").strip().lower() or "auto"
    email_value = str(account.get("email") or "").strip()
    email_password_value = str(account.get("email_password") or "").strip()
    auth_json_value = str(account.get("mail_auth_json") or "").strip()
    watch_json_value = str(account.get("mail_watch_json") or "").strip()
    now = int(time.time())

    if not _account_has_mail_credentials(account):
        _, _, readiness_detail = _account_mail_ready_meta(account)
        error_text = readiness_detail or "Почта аккаунта не настроена."
        db.update_account_mail_state(
            account_id,
            mail_provider=provider_value,
            mail_status="auth_error",
            mail_last_error=error_text,
            mail_auth_json=auth_json_value if _mail_provider_requires_auth_json(provider_value) else None,
            mail_watch_json=watch_json_value if _mail_provider_requires_auth_json(provider_value) else None,
        )
        return {
            "provider": provider_value,
            "status": "auth_error",
            "error": error_text,
            "messages": [],
            "auth_json": auth_json_value,
            "watch_json": watch_json_value,
            "watch_status": "skipped",
            "watch_error": "",
            "reason": reason,
        }

    if heartbeat is not None:
        heartbeat()
    result = mail_service.fetch_recent_messages(
        email_address=email_value,
        email_password=email_password_value,
        provider=provider_value,
        auth_json=auth_json_value,
        limit=mail_service.MAIL_FETCH_LIMIT,
        include_details=True,
    )
    if heartbeat is not None:
        heartbeat()

    provider_used = str(result.get("provider") or provider_value or "auto").strip().lower() or "auto"
    status_value = str(result.get("status") or "connect_error").strip().lower() or "connect_error"
    error_text = str(result.get("error") or "").strip()
    messages = _enrich_mail_messages_for_cache(list(result.get("messages") or []))
    updated_auth_json = str(result.get("auth_json") or auth_json_value or "").strip()
    effective_watch_json = watch_json_value
    successful_sync = status_value in {"ok", "empty"}

    db.update_account_mail_state(
        account_id,
        mail_provider=provider_used,
        mail_status=status_value,
        mail_last_synced_at=now if successful_sync else None,
        mail_last_error=error_text,
        mail_auth_json=updated_auth_json if _mail_provider_requires_auth_json(provider_used) else None,
        mail_watch_json=effective_watch_json if _mail_provider_requires_auth_json(provider_used) else None,
    )
    if successful_sync:
        db.replace_account_mail_messages(account_id, messages)

    watch_status = "skipped"
    watch_error = ""
    should_renew_watch = False
    if (
        renew_watch
        and successful_sync
        and _mail_provider_supports_background_sync(provider_used)
        and _account_has_mail_credentials(account)
    ):
        expires_at = _mail_watch_expiration_ts(effective_watch_json)
        should_renew_watch = (
            force_refresh
            or not effective_watch_json
            or expires_at is None
            or expires_at <= now + MAIL_COLLECTOR_WATCH_RENEW_MARGIN_SECONDS
        )
    if should_renew_watch:
        callback_url = _mail_webhook_callback_url(provider_used)
        if callback_url:
            if heartbeat is not None:
                heartbeat()
            watch_result = mail_service.renew_mail_watch(
                email_address=email_value,
                provider=provider_used,
                auth_json=updated_auth_json,
                watch_json=effective_watch_json,
                callback_url=callback_url,
                webhook_secret=MAIL_WEBHOOK_SECRET,
            )
            if heartbeat is not None:
                heartbeat()
            watch_status = str(watch_result.get("status") or "unsupported").strip().lower() or "unsupported"
            watch_error = str(watch_result.get("error") or "").strip()
            updated_auth_json = str(watch_result.get("auth_json") or updated_auth_json or "").strip()
            effective_watch_json = str(watch_result.get("watch_json") or effective_watch_json or "").strip()
            db.update_account_mail_state(
                account_id,
                mail_provider=provider_used,
                mail_status=status_value,
                mail_last_synced_at=now if successful_sync else None,
                mail_last_error=error_text,
                mail_auth_json=updated_auth_json if _mail_provider_requires_auth_json(provider_used) else None,
                mail_watch_json=effective_watch_json if _mail_provider_requires_auth_json(provider_used) else None,
            )

    account["mail_provider"] = provider_used
    account["mail_status"] = status_value
    account["mail_last_error"] = error_text
    account["mail_auth_json"] = updated_auth_json if _mail_provider_requires_auth_json(provider_used) else ""
    account["mail_watch_json"] = effective_watch_json if _mail_provider_requires_auth_json(provider_used) else ""
    if successful_sync:
        account["mail_last_synced_at"] = now

    return {
        "provider": provider_used,
        "status": status_value,
        "error": error_text,
        "messages": messages,
        "auth_json": updated_auth_json,
        "watch_json": effective_watch_json,
        "watch_status": watch_status,
        "watch_error": watch_error,
        "reason": reason,
    }


def _enqueue_mail_account_sync(
    account_id: int,
    *,
    reason: str = "",
    delay_seconds: int = 0,
    reactivate_if_terminal: bool = True,
) -> None:
    available_at = int(time.time()) + max(0, int(delay_seconds or 0))
    db.create_or_reactivate_runtime_task(
        task_type="mail_account_sync",
        entity_type="account",
        entity_id=int(account_id),
        payload={"account_id": int(account_id), "reason": (reason or "").strip()},
        max_attempts=3,
        available_at=available_at,
        reactivate_if_terminal=reactivate_if_terminal,
    )
    _ensure_runtime_worker_thread()
    RUNTIME_WORKER_WAKEUP.set()


def _maybe_run_mail_collector_reconcile() -> bool:
    global MAIL_COLLECTOR_LAST_RECONCILE_AT
    if not MAIL_COLLECTOR_ENABLED:
        return False
    now = time.time()
    with MAIL_COLLECTOR_LOCK:
        if now - MAIL_COLLECTOR_LAST_RECONCILE_AT < MAIL_COLLECTOR_RECONCILE_SECONDS:
            return False
        MAIL_COLLECTOR_LAST_RECONCILE_AT = now
    enqueued = 0
    for raw_account in db.list_accounts(limit=5000):
        account = dict(raw_account)
        provider_value = str(account.get("mail_provider") or "auto").strip().lower() or "auto"
        if not _mail_provider_supports_background_sync(provider_value):
            continue
        if not _account_has_mail_credentials(account):
            continue
        last_synced_at = int(account.get("mail_last_synced_at") or 0)
        watch_expiration = _mail_watch_expiration_ts(account.get("mail_watch_json"))
        stale = last_synced_at <= 0 or last_synced_at <= int(now) - MAIL_COLLECTOR_STALE_SYNC_SECONDS
        watch_due = watch_expiration is None or watch_expiration <= int(now) + MAIL_COLLECTOR_WATCH_RENEW_MARGIN_SECONDS
        if stale or watch_due:
            _enqueue_mail_account_sync(int(account["id"]), reason="collector_reconcile")
            enqueued += 1
    return enqueued > 0


def _mail_webhook_secret_valid(request: Request) -> bool:
    if not MAIL_WEBHOOK_SECRET:
        return True
    token = str(request.query_params.get("secret") or request.headers.get("X-Mail-Webhook-Secret") or "").strip()
    return bool(token) and secrets.compare_digest(token, MAIL_WEBHOOK_SECRET)


def _find_mail_account_by_email(email_address: str, *, provider: Optional[str] = None) -> Optional[dict[str, Any]]:
    email_value = str(email_address or "").strip().lower()
    provider_value = (provider or "").strip().lower()
    if not email_value:
        return None
    for row in db.list_accounts(limit=5000):
        account = dict(row)
        account_email = str(account.get("email") or "").strip().lower()
        if account_email != email_value:
            continue
        if provider_value and str(account.get("mail_provider") or "auto").strip().lower() != provider_value:
            continue
        return account
    return None


def _find_mail_account_by_subscription(subscription_id: str, *, client_state: str = "") -> Optional[dict[str, Any]]:
    subscription_value = str(subscription_id or "").strip()
    client_state_value = str(client_state or "").strip()
    if not subscription_value:
        return None
    for row in db.list_accounts(limit=5000):
        account = dict(row)
        if str(account.get("mail_provider") or "auto").strip().lower() != "microsoft_graph":
            continue
        watch_payload = _mail_watch_payload(account.get("mail_watch_json"))
        if str(watch_payload.get("subscription_id") or "").strip() != subscription_value:
            continue
        expected_client_state = str(watch_payload.get("client_state") or "").strip()
        if client_state_value and expected_client_state and client_state_value != expected_client_state:
            continue
        return account
    return None


def _helper_inventory_available_serials(payload: dict[str, Any]) -> list[str]:
    candidates = []
    for key in ("available_serials", "running_serials", "configured_serials"):
        raw = payload.get(key) if isinstance(payload, dict) else []
        if isinstance(raw, list):
            candidates.extend(str(item or "").strip() for item in raw)
    cleaned = sorted({item for item in candidates if item})
    return cleaned


def _pick_instagram_audit_serial(usage: dict[str, int]) -> str:
    if not usage:
        return ""
    return min(usage.keys(), key=lambda item: (int(usage.get(item, 0)), item))


def _prepare_instagram_emulator_serial_usage() -> dict[str, int]:
    try:
        inventory = _fetch_helper_emulator_inventory()
        available_serials = [
            serial
            for serial in _helper_inventory_available_serials(inventory)
            if db.is_valid_instagram_emulator_serial(serial)
        ]
    except Exception as exc:
        logger.info("instagram_serial_auto_assign_fallback: %s", exc)
        return {}
    return db.count_instagram_emulator_serial_usage(available_serials)


def _allocate_instagram_emulator_serial(usage: Optional[dict[str, int]] = None) -> str:
    usage_map = usage if isinstance(usage, dict) else {}
    chosen_serial = _pick_instagram_audit_serial(usage_map)
    if chosen_serial:
        usage_map[chosen_serial] = int(usage_map.get(chosen_serial, 0)) + 1
        return chosen_serial
    return "default"


def _account_needs_instagram_emulator_serial(account: dict[str, Any]) -> bool:
    current_serial = str(account.get("instagram_emulator_serial") or "").strip()
    if not current_serial:
        return True
    if current_serial.lower() == "default":
        return True
    return not db.is_valid_instagram_emulator_serial(current_serial)


def _backfill_instagram_emulator_serials(
    accounts: list[dict[str, Any]],
    *,
    usage: Optional[dict[str, int]] = None,
) -> dict[int, str]:
    pending_accounts = [
        account
        for account in accounts
        if str(account.get("type") or "").strip().lower() == "instagram"
        and int(account.get("id") or 0) > 0
        and _account_needs_instagram_emulator_serial(account)
    ]
    if not pending_accounts:
        return {}

    usage_map = usage if isinstance(usage, dict) else _prepare_instagram_emulator_serial_usage()
    assigned: dict[int, str] = {}
    for account in pending_accounts:
        account_id = int(account.get("id") or 0)
        assigned_serial = _allocate_instagram_emulator_serial(usage_map)
        db.update_account_instagram_emulator_serial(account_id, assigned_serial)
        account["instagram_emulator_serial"] = assigned_serial
        assigned[account_id] = assigned_serial
    return assigned


def _ensure_publish_account_serials(*, account_ids: Optional[list[int]] = None, limit: int = 500) -> None:
    if account_ids is None:
        accounts = [dict(row) for row in db.list_accounts(account_type="instagram", limit=limit)]
    else:
        accounts: list[dict[str, Any]] = []
        seen: set[int] = set()
        for raw_id in account_ids:
            try:
                account_id = int(raw_id)
            except Exception:
                continue
            if account_id <= 0 or account_id in seen:
                continue
            seen.add(account_id)
            row = db.get_account(account_id)
            if row is None:
                continue
            account = dict(row)
            if str(account.get("type") or "").strip().lower() == "instagram":
                accounts.append(account)
    if accounts:
        _backfill_instagram_emulator_serials(accounts)


def _prepare_instagram_audit_items(
    accounts: list[dict[str, Any]],
    *,
    available_serials: list[str],
) -> list[dict[str, Any]]:
    usage = db.count_instagram_emulator_serial_usage(available_serials)
    prepared: list[dict[str, Any]] = []
    timestamp = int(time.time())
    available_set = set(available_serials)
    for index, account in enumerate(accounts):
        account_id = int(account["id"])
        assigned_serial = str(account.get("instagram_emulator_serial") or "").strip()
        resolution_state = ""
        resolution_detail = ""
        item_state = "queued"
        mail_probe_state = "pending"
        if not _account_has_instagram_login(account):
            resolution_state = "missing_credentials"
            resolution_detail = "Не заполнены логин или пароль Instagram."
            item_state = "done"
            mail_probe_state = "not_required"
        else:
            serial_needs_reassign = (
                not assigned_serial
                or assigned_serial.lower() == "default"
                or assigned_serial not in available_set
            )
            if serial_needs_reassign:
                chosen_serial = _pick_instagram_audit_serial(usage)
                if not chosen_serial:
                    resolution_state = "missing_device"
                    resolution_detail = (
                        f"Назначенный serial {assigned_serial} сейчас недоступен в helper."
                        if assigned_serial and assigned_serial.lower() != "default"
                        else "Helper не отдал ни одного доступного emulator serial."
                    )
                    item_state = "done"
                    mail_probe_state = "not_required"
                else:
                    assigned_serial = chosen_serial
                    usage[chosen_serial] = int(usage.get(chosen_serial, 0)) + 1
                    db.update_account_instagram_emulator_serial(account_id, chosen_serial)
        prepared.append(
            {
                "account_id": account_id,
                "queue_position": index,
                "assigned_serial": assigned_serial,
                "item_state": item_state,
                "login_state": "",
                "login_detail": "",
                "mail_probe_state": mail_probe_state,
                "mail_probe_detail": "",
                "resolution_state": resolution_state,
                "resolution_detail": resolution_detail,
                "started_at": timestamp if item_state == "done" else None,
                "completed_at": timestamp if item_state == "done" else None,
            }
        )
    return prepared


INSTAGRAM_MAIL_SENDER_MARKERS = ("instagram", "meta")
INSTAGRAM_MAIL_CODE_MARKERS = (
    "security code",
    "confirmation code",
    "login code",
    "enter code",
    "code to log in",
    "use the following code",
    "confirm your identity",
    "код",
    "вход",
    "подтверд",
    "безопас",
)
INSTAGRAM_MAIL_APPROVAL_MARKERS = (
    "approve login",
    "confirm your account",
    "confirm this login",
    "confirm it's you",
    "help us confirm",
    "login attempt",
    "подтвердите вход",
    "подтвердите, что это вы",
)


def _collapse_mail_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _mask_mail_code(code: str) -> str:
    value = str(code or "").strip()
    if not value:
        return ""
    if len(value) <= 2:
        return "*" * len(value)
    visible = min(3, len(value) // 2)
    return value[:visible] + ("*" * max(1, len(value) - visible))


def _mail_challenge_result(
    *,
    status: str,
    kind: str = "unsupported",
    code: str = "",
    masked_code: str = "",
    link_url: str = "",
    message_uid: str = "",
    received_at: Optional[int] = None,
    confidence: float = 0.0,
    reason_code: str = "",
    reason_text: str = "",
) -> dict[str, Any]:
    return {
        "status": (status or "").strip(),
        "kind": (kind or "unsupported").strip(),
        "code": str(code or "").strip(),
        "masked_code": str(masked_code or "").strip(),
        "link_url": str(link_url or "").strip(),
        "message_uid": str(message_uid or "").strip(),
        "received_at": int(received_at) if received_at not in (None, "") else None,
        "confidence": round(max(0.0, min(1.0, float(confidence or 0.0))), 2),
        "reason_code": str(reason_code or "").strip(),
        "reason_text": str(reason_text or "").strip(),
    }


def _mail_confidence_from_score(score: float) -> float:
    return round(min(0.99, max(0.2, float(score or 0.0) / 14.0)), 2)


def _extract_mail_code_candidate(text: str) -> Optional[dict[str, Any]]:
    clean = _collapse_mail_text(text).lower()
    if not clean:
        return None
    candidates: dict[str, dict[str, Any]] = {}
    for match in re.finditer(r"(?<!\d)(\d{4,8})(?!\d)", clean):
        code = str(match.group(1) or "").strip()
        if not code:
            continue
        start = max(0, match.start() - 72)
        end = min(len(clean), match.end() + 72)
        window = clean[start:end]
        marker_score = 3 if any(marker in window for marker in INSTAGRAM_MAIL_CODE_MARKERS) else 0
        sender_score = 1 if any(marker in window for marker in INSTAGRAM_MAIL_SENDER_MARKERS) else 0
        length_score = 3 if len(code) == 6 else 1
        total_score = marker_score + sender_score + length_score
        current = candidates.get(code)
        if current is None or total_score > int(current["score"]):
            candidates[code] = {"code": code, "score": total_score, "window": window}
    if not candidates:
        return None
    ranked = sorted(candidates.values(), key=lambda item: (int(item["score"]), len(str(item["code"])) == 6, str(item["code"])), reverse=True)
    best = ranked[0]
    if len(str(best["code"])) != 6:
        if int(best["score"]) < 4:
            return None
        if sum(1 for item in ranked if int(item["score"]) == int(best["score"])) > 1:
            return None
    return best


def _select_instagram_approval_link(links: list[str], merged_text: str) -> str:
    merged = _collapse_mail_text(merged_text).lower()
    for raw_link in links or []:
        link = str(raw_link or "").strip()
        if not link:
            continue
        parsed = urlparse(link)
        host = parsed.netloc.lower()
        lowered = link.lower()
        if not any(marker in host for marker in ("instagram", "meta", "facebook", "fb")):
            continue
        if any(marker in lowered for marker in ("approve", "confirm", "challenge", "login", "verify", "security")):
            return link
        if any(marker in merged for marker in INSTAGRAM_MAIL_APPROVAL_MARKERS):
            return link
    return ""


def _inspect_instagram_mail_messages(
    messages: list[dict[str, Any]],
    *,
    account_email: str,
    challenge_started_at: int,
    screen_kind: str,
) -> dict[str, Any]:
    now = int(time.time())
    lower_account_email = str(account_email or "").strip().lower()
    base_cutoff = now - INSTAGRAM_MAIL_CHALLENGE_FRESHNESS_SECONDS
    challenge_cutoff = int(challenge_started_at or 0) - INSTAGRAM_MAIL_CHALLENGE_LOOKBACK_SECONDS
    freshness_cutoff = max(base_cutoff, challenge_cutoff)

    numeric_candidates: list[dict[str, Any]] = []
    approval_candidates: list[dict[str, Any]] = []
    unsupported_candidates: list[dict[str, Any]] = []
    instagramish_seen = False

    for item in messages:
        received_at = int(item.get("received_at") or 0)
        if received_at and received_at < freshness_cutoff:
            continue
        sender = _collapse_mail_text(item.get("from_text")).lower()
        subject = _collapse_mail_text(item.get("subject")).lower()
        snippet = _collapse_mail_text(item.get("snippet")).lower()
        body_text = _collapse_mail_text(item.get("body_text")).lower()
        body_html = _collapse_mail_text(item.get("body_html")).lower()
        merged = " ".join(part for part in (sender, subject, snippet, body_text, body_html) if part).strip()
        if not merged:
            continue
        has_sender_marker = any(marker in sender for marker in INSTAGRAM_MAIL_SENDER_MARKERS)
        has_instagram_marker = has_sender_marker or any(marker in merged for marker in INSTAGRAM_MAIL_SENDER_MARKERS)
        has_challenge_marker = any(marker in merged for marker in (*INSTAGRAM_MAIL_CODE_MARKERS, *INSTAGRAM_MAIL_APPROVAL_MARKERS))
        if not has_instagram_marker or not has_challenge_marker:
            continue

        instagramish_seen = True
        to_addresses = [str(addr or "").strip().lower() for addr in list(item.get("to_addresses") or [])]
        cc_addresses = [str(addr or "").strip().lower() for addr in list(item.get("cc_addresses") or [])]
        recipient_match = bool(lower_account_email and lower_account_email in {*to_addresses, *cc_addresses})
        base_score = 0
        base_score += 4 if has_sender_marker else 2
        if any(marker in merged for marker in INSTAGRAM_MAIL_CODE_MARKERS):
            base_score += 2
        if any(marker in merged for marker in INSTAGRAM_MAIL_APPROVAL_MARKERS):
            base_score += 1
        if recipient_match:
            base_score += 1
        if received_at:
            base_score += 2 if received_at >= int(challenge_started_at or 0) else 1

        code_candidate = _extract_mail_code_candidate(" ".join(part for part in (subject, body_text, body_html, snippet) if part))
        if code_candidate is not None:
            total_score = base_score + int(code_candidate["score"])
            numeric_candidates.append(
                {
                    "message_uid": str(item.get("message_uid") or "").strip(),
                    "received_at": received_at or None,
                    "subject": str(item.get("subject") or "").strip(),
                    "code": str(code_candidate["code"]),
                    "masked_code": _mask_mail_code(str(code_candidate["code"])),
                    "score": total_score,
                    "confidence": _mail_confidence_from_score(total_score),
                }
            )
            continue

        link_url = _select_instagram_approval_link(list(item.get("links") or []), merged)
        if link_url:
            total_score = base_score + 2
            approval_candidates.append(
                {
                    "message_uid": str(item.get("message_uid") or "").strip(),
                    "received_at": received_at or None,
                    "subject": str(item.get("subject") or "").strip(),
                    "link_url": link_url,
                    "score": total_score,
                    "confidence": _mail_confidence_from_score(total_score),
                }
            )
            continue

        unsupported_candidates.append(
            {
                "message_uid": str(item.get("message_uid") or "").strip(),
                "received_at": received_at or None,
                "subject": str(item.get("subject") or "").strip(),
                "score": base_score,
                "confidence": _mail_confidence_from_score(base_score),
            }
        )

    if numeric_candidates:
        ranked_numeric = sorted(
            numeric_candidates,
            key=lambda item: (int(item["score"]), int(item.get("received_at") or 0), str(item.get("message_uid") or "")),
            reverse=True,
        )
        top_score = int(ranked_numeric[0]["score"])
        top_candidates = [item for item in ranked_numeric if int(item["score"]) == top_score]
        top_keys = {(str(item.get("message_uid") or ""), str(item.get("code") or "")) for item in top_candidates}
        if len(top_keys) > 1:
            return _mail_challenge_result(
                status="ambiguous",
                kind="numeric_code",
                message_uid=str(ranked_numeric[0].get("message_uid") or ""),
                received_at=ranked_numeric[0].get("received_at"),
                confidence=ranked_numeric[0].get("confidence") or 0.0,
                reason_code="mail_ambiguous",
                reason_text="Нашлось несколько свежих писем Instagram/Meta с одинаково сильным кодом. Нужен ручной выбор.",
            )
        best = ranked_numeric[0]
        return _mail_challenge_result(
            status="resolved",
            kind="numeric_code",
            code=str(best["code"]),
            masked_code=str(best["masked_code"]),
            message_uid=str(best["message_uid"]),
            received_at=best.get("received_at"),
            confidence=best.get("confidence") or 0.0,
            reason_code="mail_code_ready",
            reason_text=f"Найден свежий код Instagram/Meta: {str(best.get('subject') or '(без темы)')}.",
        )

    if approval_candidates:
        ranked_approval = sorted(
            approval_candidates,
            key=lambda item: (int(item["score"]), int(item.get("received_at") or 0), str(item.get("message_uid") or "")),
            reverse=True,
        )
        best = ranked_approval[0]
        return _mail_challenge_result(
            status="resolved",
            kind="approval_link",
            link_url=str(best["link_url"]),
            message_uid=str(best["message_uid"]),
            received_at=best.get("received_at"),
            confidence=best.get("confidence") or 0.0,
            reason_code="approval_link_ready",
            reason_text="Письмо Instagram/Meta найдено, но в нём ссылка подтверждения, а не код.",
        )

    if unsupported_candidates:
        best = sorted(
            unsupported_candidates,
            key=lambda item: (int(item["score"]), int(item.get("received_at") or 0), str(item.get("message_uid") or "")),
            reverse=True,
        )[0]
        return _mail_challenge_result(
            status="unsupported",
            kind="unsupported",
            message_uid=str(best["message_uid"]),
            received_at=best.get("received_at"),
            confidence=best.get("confidence") or 0.0,
            reason_code="challenge_not_email_based",
            reason_text="Письмо Instagram/Meta найдено, но в нём нет надёжного кода или подходящей ссылки для fully-auto сценария.",
        )

    if instagramish_seen and screen_kind == "approval":
        return _mail_challenge_result(
            status="unsupported",
            kind="unsupported",
            reason_code="challenge_not_email_based",
            reason_text="Instagram прислал challenge, но письмо не содержит кода, пригодного для авто-ввода.",
        )

    return _mail_challenge_result(
        status="not_found",
        kind="unsupported",
        reason_code="mail_not_found",
        reason_text="Свежих писем Instagram/Meta с кодом или ссылкой подтверждения не найдено.",
    )


def _persist_account_mail_challenge(account_id: int, result: dict[str, Any]) -> None:
    db.update_account_mail_challenge_state(
        int(account_id),
        status=str(result.get("status") or "idle"),
        kind=str(result.get("kind") or ""),
        reason_code=str(result.get("reason_code") or ""),
        reason_text=str(result.get("reason_text") or ""),
        message_uid=str(result.get("message_uid") or ""),
        received_at=result.get("received_at"),
        masked_code=str(result.get("masked_code") or ""),
        confidence=float(result.get("confidence") or 0.0),
    )


def _resolve_instagram_mail_challenge(
    account: dict[str, Any],
    *,
    challenge_started_at: Optional[int],
    screen_kind: str,
    timeout_seconds: Optional[int] = None,
    heartbeat: Optional[RuntimeHeartbeat] = None,
) -> dict[str, Any]:
    account_id = int(account["id"])
    if not _account_has_mail_credentials(account):
        _, _, readiness_detail = _account_mail_ready_meta(account)
        result = _mail_challenge_result(
            status="mailbox_unavailable",
            kind="unsupported",
            reason_code="mailbox_missing_credentials",
            reason_text=readiness_detail or "У аккаунта не заполнены почта или пароль почты.",
        )
        _persist_account_mail_challenge(account_id, result)
        return result

    timeout_value = max(5, int(timeout_seconds or INSTAGRAM_MAIL_CHALLENGE_TIMEOUT_SECONDS))
    deadline = time.time() + timeout_value
    poll_seconds = max(2, INSTAGRAM_MAIL_CHALLENGE_POLL_SECONDS)
    challenge_start = int(challenge_started_at or time.time())
    last_result = _mail_challenge_result(
        status="not_found",
        kind="unsupported",
        reason_code="mail_not_found",
        reason_text="Свежих писем Instagram/Meta с кодом или ссылкой подтверждения не найдено.",
    )
    last_connect_error = ""
    cached_messages = _mail_messages_with_metadata(account_id, limit=mail_service.MAIL_FETCH_LIMIT)
    if cached_messages:
        cached_result = _inspect_instagram_mail_messages(
            cached_messages,
            account_email=str(account.get("email") or "").strip(),
            challenge_started_at=challenge_start,
            screen_kind=screen_kind,
        )
        if cached_result["status"] in {"resolved", "ambiguous", "unsupported"}:
            _persist_account_mail_challenge(account_id, cached_result)
            return cached_result

    while True:
        result = _sync_account_mailbox(
            account,
            heartbeat=heartbeat,
            force_refresh=True,
            renew_watch=True,
            reason="instagram_mail_challenge",
        )

        mail_status = str(result.get("status") or "connect_error")
        messages = list(result.get("messages") or [])

        if mail_status == "auth_error":
            final = _mail_challenge_result(
                status="mailbox_unavailable",
                kind="unsupported",
                reason_code="mailbox_auth_failed",
                reason_text=str(result.get("error") or "Не удалось войти в почту аккаунта."),
            )
            _persist_account_mail_challenge(account_id, final)
            return final
        if mail_status == "unsupported":
            final = _mail_challenge_result(
                status="mailbox_unavailable",
                kind="unsupported",
                reason_code="mailbox_unavailable",
                reason_text=str(result.get("error") or "Почтовый провайдер не поддерживается."),
            )
            _persist_account_mail_challenge(account_id, final)
            return final
        if mail_status == "connect_error":
            last_connect_error = str(result.get("error") or "Не удалось подключиться к почте.")
        else:
            last_connect_error = ""

        if mail_status == "ok":
            analyzed = _inspect_instagram_mail_messages(
                messages,
                account_email=str(account.get("email") or "").strip(),
                challenge_started_at=challenge_start,
                screen_kind=screen_kind,
            )
            last_result = analyzed
            if analyzed["status"] in {"resolved", "ambiguous", "unsupported"}:
                _persist_account_mail_challenge(account_id, analyzed)
                return analyzed
        else:
            last_result = _mail_challenge_result(
                status="not_found",
                kind="unsupported",
                reason_code="mail_not_found",
                reason_text="Свежих писем Instagram/Meta с кодом или ссылкой подтверждения не найдено.",
            )

        if time.time() >= deadline:
            break
        time.sleep(min(poll_seconds, max(0.0, deadline - time.time())))

    if last_connect_error:
        last_result = _mail_challenge_result(
            status="mailbox_unavailable",
            kind="unsupported",
            reason_code="mailbox_unavailable",
            reason_text=last_connect_error,
        )
    _persist_account_mail_challenge(account_id, last_result)
    return last_result


def _instagram_message_match(messages: list[dict[str, Any]]) -> dict[str, Any]:
    match = _inspect_instagram_mail_messages(
        messages,
        account_email="",
        challenge_started_at=int(time.time()),
        screen_kind="unknown",
    )
    if match["status"] == "resolved" and match["kind"] == "numeric_code":
        return {
            "matched": True,
            "subject": "",
            "received_at": match.get("received_at"),
            "detail": str(match.get("reason_text") or "Найдено свежее письмо Instagram/Meta с кодом."),
        }
    return {"matched": False, "detail": str(match.get("reason_text") or "Свежих писем Instagram/Meta не найдено.")}


def _run_instagram_mail_probe(
    account: dict[str, Any],
    *,
    heartbeat: Optional[RuntimeHeartbeat] = None,
) -> dict[str, Any]:
    account_id = int(account["id"])
    if not _account_has_mail_credentials(account):
        _, _, readiness_detail = _account_mail_ready_meta(account)
        return {
            "mail_probe_state": "not_configured",
            "mail_probe_detail": readiness_detail or "У аккаунта не заполнены почта или пароль почты.",
            "matched_email_code": False,
        }
    result = _sync_account_mailbox(
        account,
        heartbeat=heartbeat,
        force_refresh=True,
        renew_watch=True,
        reason="instagram_mail_probe",
    )
    mail_status = str(result.get("status") or "connect_error")
    messages = list(result.get("messages") or [])
    if mail_status == "ok":
        match = _inspect_instagram_mail_messages(
            messages,
            account_email=str(account.get("email") or "").strip(),
            challenge_started_at=int(time.time()),
            screen_kind="unknown",
        )
        _persist_account_mail_challenge(account_id, match)
        return {
            "mail_probe_state": "ok",
            "mail_probe_detail": str(match.get("reason_text") or "Почта проверена."),
            "matched_email_code": match["status"] == "resolved" and match["kind"] == "numeric_code",
            "matched_subject": str(match.get("subject") or "").strip(),
        }
    if mail_status == "empty":
        _persist_account_mail_challenge(
            account_id,
            _mail_challenge_result(
                status="not_found",
                kind="unsupported",
                reason_code="mail_not_found",
                reason_text="Во входящих письма не найдены.",
            ),
        )
        return {
            "mail_probe_state": "empty",
            "mail_probe_detail": "Во входящих письма не найдены.",
            "matched_email_code": False,
        }
    if mail_status == "auth_error":
        _persist_account_mail_challenge(
            account_id,
            _mail_challenge_result(
                status="mailbox_unavailable",
                kind="unsupported",
                reason_code="mailbox_auth_failed",
                reason_text=str(result.get("error") or "Не удалось войти в почту."),
            ),
        )
        return {
            "mail_probe_state": "auth_error",
            "mail_probe_detail": str(result.get("error") or "Не удалось войти в почту."),
            "matched_email_code": False,
        }
    if mail_status == "unsupported":
        _persist_account_mail_challenge(
            account_id,
            _mail_challenge_result(
                status="mailbox_unavailable",
                kind="unsupported",
                reason_code="mailbox_unavailable",
                reason_text=str(result.get("error") or "Почтовый провайдер не поддерживается."),
            ),
        )
        return {
            "mail_probe_state": "unsupported",
            "mail_probe_detail": str(result.get("error") or "Почтовый провайдер не поддерживается."),
            "matched_email_code": False,
        }
    _persist_account_mail_challenge(
        account_id,
        _mail_challenge_result(
            status="mailbox_unavailable",
            kind="unsupported",
            reason_code="mailbox_unavailable",
            reason_text=str(result.get("error") or "Не удалось проверить почту."),
        ),
    )
    return {
        "mail_probe_state": "connect_error",
        "mail_probe_detail": str(result.get("error") or "Не удалось проверить почту."),
        "matched_email_code": False,
    }


def _extract_diagnostic_path(detail: str) -> str:
    marker = "Диагностика:"
    text = str(detail or "").strip()
    if marker not in text:
        return ""
    after = text.split(marker, 1)[1].strip()
    return after.split()[0].strip() if after else ""


def _wait_for_instagram_login_result(
    account_id: int,
    *,
    since_ts: int,
    timeout_seconds: int,
    heartbeat: Optional[RuntimeHeartbeat] = None,
) -> dict[str, Any]:
    deadline = time.time() + max(10, int(timeout_seconds))
    terminal_states = {"login_submitted", "manual_2fa_required", "challenge_required", "invalid_password", "helper_error"}
    while time.time() < deadline:
        if heartbeat is not None:
            heartbeat()
        row = db.get_account(int(account_id))
        if row:
            account = dict(row)
            updated_at = int(account.get("instagram_launch_updated_at") or 0)
            state = str(account.get("instagram_launch_status") or "idle").strip().lower()
            if updated_at >= int(since_ts) and state in terminal_states:
                return {
                    "login_state": state,
                    "login_detail": str(account.get("instagram_launch_detail") or "").strip(),
                    "updated_at": updated_at,
                }
        time.sleep(INSTAGRAM_AUDIT_HELPER_POLL_SECONDS)
    raise TimeoutError("Helper не прислал итоговый статус входа за отведённое время.")


def _instagram_audit_resolution_from_login(login_state: str) -> str:
    mapping = {
        "login_submitted": "login_ok",
        "manual_2fa_required": "manual_2fa_required",
        "challenge_required": "challenge_required",
        "invalid_password": "invalid_password",
        "helper_error": "helper_error",
    }
    return mapping.get((login_state or "").strip().lower(), "helper_error")


def _instagram_audit_progress_pct(item_state: str) -> int:
    return {
        "queued": 0,
        "launching": 15,
        "login_check": 45,
        "mail_check_if_needed": 72,
        "done": 100,
    }.get((item_state or "").strip().lower(), 0)


def _instagram_audit_live_progress_pct(item_state: str, *, updated_at: Any) -> int:
    value = (item_state or "").strip().lower()
    base_pct = _instagram_audit_progress_pct(value)
    upper_bound = {
        "launching": 39,
        "login_check": 69,
        "mail_check_if_needed": 96,
    }.get(value, base_pct)
    return _smooth_live_progress(base_pct, updated_at=updated_at, upper_bound=upper_bound)


def _run_instagram_audit_batch(batch_id: int, *, heartbeat: Optional[RuntimeHeartbeat] = None) -> None:
    batch = db.get_instagram_audit_batch(batch_id)
    if batch is None or _instagram_audit_batch_is_terminal(batch["state"]):
        return
    if heartbeat is not None:
        heartbeat()
    db.reset_instagram_audit_inflight_items(batch_id)
    db.update_instagram_audit_batch_state(batch_id, "running", detail="Массовая проверка входов запущена.", started_at=int(time.time()))
    items = [dict(row) for row in db.list_instagram_audit_items(batch_id)]
    for item in items:
        if str(item.get("item_state") or "") == "done":
            continue
        if heartbeat is not None:
            heartbeat()
        _run_instagram_audit_item(batch_id, item, heartbeat=heartbeat)
    summary = db.refresh_instagram_audit_batch_state(batch_id)
    final_state = str(summary.get("state") or "")
    if final_state == "completed":
        db.update_instagram_audit_batch_state(batch_id, final_state, detail="Проверка входов завершена. Проблем не найдено.")
    elif final_state == "completed_with_errors":
        db.update_instagram_audit_batch_state(batch_id, final_state, detail="Проверка входов завершена. Есть аккаунты, требующие ручных шагов.")


def _schedule_instagram_audit_item_retry(
    batch_id: int,
    *,
    item_id: int,
    account_id: int,
    assigned_serial: str,
    detail: str,
) -> None:
    detail_value = str(detail or "").strip() or "Helper недоступен во время проверки Instagram."
    retry_detail = f"Инфраструктурный сбой. Повторю автоматически: {detail_value}"
    db.update_instagram_audit_item(
        item_id,
        item_state="queued",
        assigned_serial=assigned_serial,
        login_state="",
        login_detail=detail_value,
        mail_probe_state="pending",
        mail_probe_detail="",
        resolution_state="",
        resolution_detail="",
        diagnostic_path="",
    )
    db.append_instagram_audit_event(
        batch_id,
        audit_item_id=item_id,
        account_id=account_id,
        state="queued",
        detail=retry_detail,
        payload={
            "assigned_serial": assigned_serial,
            "failure_kind": "infrastructure",
            "retryable": True,
        },
    )
    db.refresh_instagram_audit_batch_state(batch_id, detail=retry_detail)
    raise InstagramAuditInfrastructureError(detail_value)


def _run_instagram_audit_item(
    batch_id: int,
    item: dict[str, Any],
    *,
    heartbeat: Optional[RuntimeHeartbeat] = None,
) -> None:
    account_id = int(item["account_id"])
    item_id = int(item["id"])
    account_row = db.get_account(account_id)
    if account_row is None:
        db.update_instagram_audit_item(
            item_id,
            item_state="done",
            resolution_state="helper_error",
            resolution_detail="Аккаунт не найден.",
            mail_probe_state="not_required",
            completed_at=int(time.time()),
        )
        db.append_instagram_audit_event(batch_id, audit_item_id=item_id, account_id=account_id, state="done", detail="Аккаунт не найден.", payload={})
        db.refresh_instagram_audit_batch_state(batch_id)
        return
    account = dict(account_row)
    if not _account_has_instagram_login(account):
        detail = "Не заполнены логин или пароль Instagram."
        db.update_instagram_audit_item(
            item_id,
            item_state="done",
            resolution_state="missing_credentials",
            resolution_detail=detail,
            mail_probe_state="not_required",
            completed_at=int(time.time()),
        )
        db.append_instagram_audit_event(batch_id, audit_item_id=item_id, account_id=account_id, state="done", detail=detail, payload={"resolution_state": "missing_credentials"})
        db.refresh_instagram_audit_batch_state(batch_id, detail=detail)
        return
    assigned_serial = str(item.get("assigned_serial") or account.get("instagram_emulator_serial") or "").strip()
    if not assigned_serial:
        detail = "Для аккаунта не удалось назначить emulator serial."
        db.update_instagram_audit_item(
            item_id,
            item_state="done",
            resolution_state="missing_device",
            resolution_detail=detail,
            mail_probe_state="not_required",
            completed_at=int(time.time()),
        )
        db.append_instagram_audit_event(batch_id, audit_item_id=item_id, account_id=account_id, state="done", detail=detail, payload={"resolution_state": "missing_device"})
        db.refresh_instagram_audit_batch_state(batch_id, detail=detail)
        return

    start_ts = int(time.time())
    detail = f"Запускаю helper для @{_account_identity_handle(account)} на {assigned_serial}."
    db.update_instagram_audit_item(
        item_id,
        item_state="launching",
        assigned_serial=assigned_serial,
        started_at=start_ts,
        resolution_state="",
        resolution_detail="",
    )
    db.append_instagram_audit_event(batch_id, audit_item_id=item_id, account_id=account_id, state="launching", detail=detail, payload={"assigned_serial": assigned_serial})
    db.refresh_instagram_audit_batch_state(batch_id, detail=detail)
    db.update_account_instagram_launch_state(account_id, "idle", "Instagram audit запущен. Ожидаю ответ helper.")

    try:
        _wait_for_helper_idle(heartbeat=heartbeat)
        created = db.create_helper_launch_ticket(
            account_id=account_id,
            target="instagram_audit_login",
            created_by_admin=ADMIN_USER,
            ttl_seconds=HELPER_TICKET_TTL_SECONDS,
        )
        db.update_instagram_audit_item(item_id, item_state="login_check", assigned_serial=assigned_serial)
        db.append_instagram_audit_event(
            batch_id,
            audit_item_id=item_id,
            account_id=account_id,
            state="login_check",
            detail="Helper принял задачу. Жду итоговый login status.",
            payload={"ticket": str(created["ticket"]), "assigned_serial": assigned_serial},
        )
        db.refresh_instagram_audit_batch_state(batch_id, detail="Helper проверяет вход Instagram.")
        _launch_instagram_helper_ticket(str(created["ticket"]))
        login_result = _wait_for_instagram_login_result(
            account_id,
            since_ts=start_ts,
            timeout_seconds=INSTAGRAM_AUDIT_LOGIN_TIMEOUT_SECONDS,
            heartbeat=heartbeat,
        )
    except Exception as exc:
        _schedule_instagram_audit_item_retry(
            batch_id,
            item_id=item_id,
            account_id=account_id,
            assigned_serial=assigned_serial,
            detail=str(exc),
        )

    login_state = str(login_result["login_state"] or "").strip().lower()
    login_detail = str(login_result.get("login_detail") or "").strip()
    diagnostic_path = _extract_diagnostic_path(login_detail)
    resolution_state = _instagram_audit_resolution_from_login(login_state)
    resolution_detail = login_detail
    mail_probe_state = "not_required"
    mail_probe_detail = "Проверка почты не нужна."

    if login_state == "challenge_required":
        db.update_instagram_audit_item(
            item_id,
            item_state="mail_check_if_needed",
            assigned_serial=assigned_serial,
            login_state=login_state,
            login_detail=login_detail,
            diagnostic_path=diagnostic_path,
            mail_probe_state="checking",
            mail_probe_detail="Challenge обнаружен. Проверяю почту аккаунта.",
        )
        db.append_instagram_audit_event(
            batch_id,
            audit_item_id=item_id,
            account_id=account_id,
            state="mail_check_if_needed",
            detail="Instagram запросил challenge. Запускаю mail probe.",
            payload={"login_state": login_state},
        )
        db.refresh_instagram_audit_batch_state(batch_id, detail="Для challenge-аккаунта проверяю почту.")
        try:
            mail_probe = _run_instagram_mail_probe(account, heartbeat=heartbeat)
        except Exception as exc:
            _schedule_instagram_audit_item_retry(
                batch_id,
                item_id=item_id,
                account_id=account_id,
                assigned_serial=assigned_serial,
                detail=str(exc),
            )
        mail_probe_state = str(mail_probe["mail_probe_state"])
        mail_probe_detail = str(mail_probe["mail_probe_detail"])
        if mail_probe.get("matched_email_code"):
            resolution_state = "email_code_required"
            resolution_detail = f"{login_detail} {mail_probe_detail}".strip()
        else:
            resolution_state = "challenge_required"
            resolution_detail = f"{login_detail} {mail_probe_detail}".strip()

    completed_at = int(time.time())
    db.update_instagram_audit_item(
        item_id,
        item_state="done",
        assigned_serial=assigned_serial,
        login_state=login_state,
        login_detail=login_detail,
        mail_probe_state=mail_probe_state,
        mail_probe_detail=mail_probe_detail,
        resolution_state=resolution_state,
        resolution_detail=resolution_detail,
        diagnostic_path=diagnostic_path,
        completed_at=completed_at,
    )
    db.append_instagram_audit_event(
        batch_id,
        audit_item_id=item_id,
        account_id=account_id,
        state="done",
        detail=resolution_detail,
        payload={
            "resolution_state": resolution_state,
            "login_state": login_state,
            "mail_probe_state": mail_probe_state,
            "assigned_serial": assigned_serial,
        },
    )
    db.refresh_instagram_audit_batch_state(batch_id, detail=resolution_detail)


def _runtime_task_is_retryable(task_type: str, exc: Exception) -> bool:
    if isinstance(exc, RuntimeTaskError):
        return bool(exc.retryable)
    if isinstance(exc, ValueError):
        return False
    return task_type in {
        "publish_batch_start",
        "instagram_audit_batch_run",
        "publish_reconcile",
        "instagram_audit_reconcile",
        "mail_account_sync",
    }


def _runtime_task_heartbeat(worker_name: str, task_id: int) -> None:
    db.upsert_runtime_worker_heartbeat(worker_name, current_task_id=int(task_id))
    if not db.heartbeat_runtime_task(int(task_id), worker_name=worker_name, lease_seconds=RUNTIME_TASK_LEASE_SECONDS):
        raise RuntimeTaskError("Runtime task lease lost.", retryable=True)


def _finalize_failed_runtime_task(task: dict[str, Any], error_text: str) -> None:
    task_type = str(task.get("task_type") or "").strip().lower()
    entity_id = int(task.get("entity_id") or 0)
    if entity_id <= 0:
        return
    if task_type == "publish_batch_start":
        try:
            db.mark_publish_generation_failed(entity_id, f"Runtime worker failed to start n8n workflow: {error_text}")
        except Exception as exc:
            logger.warning("runtime_publish_batch_finalize_failed: batch_id=%s error=%s", entity_id, exc)
    elif task_type == "instagram_audit_batch_run":
        try:
            timestamp = int(time.time())
            final_detail = f"Проверка завершена с инфраструктурной ошибкой: {error_text}".strip()
            finalized_total = db.finalize_unfinished_instagram_audit_items_as_helper_error(
                entity_id,
                final_detail,
                completed_at=timestamp,
            )
            summary = db.refresh_instagram_audit_batch_state(entity_id, detail=final_detail)
            if finalized_total > 0 or str(summary.get("state") or "") in {"completed", "completed_with_errors"}:
                db.append_instagram_audit_event(
                    entity_id,
                    audit_item_id=None,
                    account_id=None,
                    state="failed",
                    detail=final_detail,
                    payload={
                        "failure_kind": "infrastructure",
                        "finalized_unresolved": finalized_total,
                        "resolution_state": "helper_error",
                    },
                )
            else:
                db.update_instagram_audit_batch_state(entity_id, "failed", detail=error_text, completed_at=timestamp)
                db.append_instagram_audit_event(
                    entity_id,
                    audit_item_id=None,
                    account_id=None,
                    state="failed",
                    detail=error_text,
                    payload={},
                )
        except Exception as exc:
            logger.warning("runtime_audit_batch_finalize_failed: batch_id=%s error=%s", entity_id, exc)
    elif task_type == "mail_account_sync":
        try:
            db.update_account_mail_state(
                entity_id,
                mail_status="connect_error",
                mail_last_error=error_text,
            )
        except Exception as exc:
            logger.warning("runtime_mail_sync_finalize_failed: account_id=%s error=%s", entity_id, exc)


def _process_runtime_task(
    task: dict[str, Any],
    *,
    worker_name: str,
    heartbeat: RuntimeHeartbeat,
) -> None:
    task_type = str(task.get("task_type") or "").strip().lower()
    entity_id = int(task.get("entity_id") or 0)
    payload = _parse_json_object(task.get("payload_json"))
    if entity_id <= 0 and task_type not in {"publish_reconcile", "instagram_audit_reconcile"}:
        raise RuntimeTaskError("Runtime task entity_id is invalid.", retryable=False)

    if task_type == "publish_batch_start":
        heartbeat()
        _advance_publish_batch_runtime(entity_id, worker_name=worker_name, heartbeat=heartbeat)
        return
    if task_type == "instagram_audit_batch_run":
        _run_instagram_audit_batch(entity_id, heartbeat=heartbeat)
        return
    if task_type == "publish_reconcile":
        heartbeat()
        _run_publish_generation_watchdog()
        return
    if task_type == "instagram_audit_reconcile":
        heartbeat()
        for batch_id in db.list_pending_instagram_audit_batch_ids(limit=20):
            heartbeat()
            _enqueue_instagram_audit_batch(int(batch_id))
        return
    if task_type == "mail_account_sync":
        heartbeat()
        account_row = db.get_account(entity_id)
        if account_row is None:
            raise RuntimeTaskError("Account not found for mail sync.", retryable=False)
        _sync_account_mailbox(
            dict(account_row),
            heartbeat=heartbeat,
            force_refresh=True,
            renew_watch=True,
            reason=str(payload.get("reason") or ""),
        )
        return
    raise RuntimeTaskError(f"Unsupported runtime task type: {task_type or 'unknown'}", retryable=False)


def _run_runtime_task_once(*, worker_name: str = RUNTIME_WORKER_NAME) -> bool:
    db.upsert_runtime_worker_heartbeat(worker_name, current_task_id=None)
    task = db.lease_next_runtime_task(worker_name=worker_name, lease_seconds=RUNTIME_TASK_LEASE_SECONDS)
    if task is None:
        return False

    task_id = int(task["id"])
    last_error = ""

    def heartbeat() -> None:
        _runtime_task_heartbeat(worker_name, task_id)

    try:
        heartbeat()
        _process_runtime_task(task, worker_name=worker_name, heartbeat=heartbeat)
        if not db.complete_runtime_task(task_id, worker_name=worker_name):
            raise RuntimeTaskError("Runtime task completion lost its lease.", retryable=True)
    except Exception as exc:
        last_error = str(exc)
        retryable = _runtime_task_is_retryable(str(task.get("task_type") or ""), exc)
        try:
            updated = db.fail_runtime_task(
                task_id,
                worker_name=worker_name,
                error=last_error,
                retryable=retryable,
                retry_delay_seconds=RUNTIME_TASK_RETRY_DELAY_SECONDS,
            )
        except ValueError as state_exc:
            logger.warning("runtime_task_fail_transition_skipped: task_id=%s error=%s", task_id, state_exc)
        else:
            if str(updated.get("state") or "") == "failed":
                _finalize_failed_runtime_task(task, last_error)
        logger.exception("runtime_task_failed: task_id=%s type=%s error=%s", task_id, task.get("task_type"), exc)
    finally:
        db.upsert_runtime_worker_heartbeat(worker_name, current_task_id=None, last_error=last_error)
    return True


def _runtime_worker_main() -> None:
    while not RUNTIME_WORKER_STOP.is_set():
        try:
            did_work = _run_runtime_task_once(worker_name=RUNTIME_WORKER_NAME)
        except Exception as exc:
            error_text = str(exc).lower()
            if RUNTIME_WORKER_STOP.is_set() or "unable to open database file" in error_text or "no such table: runtime_" in error_text:
                break
            logger.exception("runtime_worker_cycle_failed: worker=%s error=%s", RUNTIME_WORKER_NAME, exc)
            try:
                db.upsert_runtime_worker_heartbeat(RUNTIME_WORKER_NAME, current_task_id=None, last_error=str(exc))
            except Exception:
                break
            did_work = False
        if did_work:
            continue
        try:
            if _maybe_run_mail_collector_reconcile():
                continue
        except Exception as exc:
            error_text = str(exc).lower()
            if not (
                RUNTIME_WORKER_STOP.is_set()
                or "unable to open database file" in error_text
                or "no such table: runtime_" in error_text
            ):
                logger.exception("mail_collector_reconcile_failed: worker=%s error=%s", RUNTIME_WORKER_NAME, exc)
        RUNTIME_WORKER_WAKEUP.wait(RUNTIME_WORKER_IDLE_POLL_SECONDS)
        RUNTIME_WORKER_WAKEUP.clear()


def _ensure_runtime_worker_thread() -> None:
    global RUNTIME_WORKER_THREAD
    with RUNTIME_WORKER_LOCK:
        if RUNTIME_WORKER_THREAD is not None and RUNTIME_WORKER_THREAD.is_alive():
            return
        RUNTIME_WORKER_THREAD = threading.Thread(
            target=_runtime_worker_main,
            daemon=True,
            name=RUNTIME_WORKER_NAME,
        )
        RUNTIME_WORKER_THREAD.start()


def _stop_runtime_worker_thread() -> None:
    global RUNTIME_WORKER_THREAD
    RUNTIME_WORKER_STOP.set()
    RUNTIME_WORKER_WAKEUP.set()
    with RUNTIME_WORKER_LOCK:
        thread = RUNTIME_WORKER_THREAD
        RUNTIME_WORKER_THREAD = None
    if thread is not None and thread.is_alive():
        thread.join(timeout=1.0)


def start_runtime_worker_service() -> None:
    RUNTIME_WORKER_STOP.clear()
    RUNTIME_WORKER_WAKEUP.clear()
    _ensure_runtime_worker_thread()


def stop_runtime_worker_service() -> None:
    _stop_runtime_worker_thread()


def run_runtime_worker_forever() -> int:
    _validate_runtime_config()
    db.init_db()
    Path(PUBLISH_STAGING_DIR).expanduser().mkdir(parents=True, exist_ok=True)
    RUNTIME_WORKER_STOP.clear()
    RUNTIME_WORKER_WAKEUP.clear()
    try:
        _runtime_worker_main()
    except KeyboardInterrupt:
        return 130
    finally:
        RUNTIME_WORKER_STOP.set()
        RUNTIME_WORKER_WAKEUP.set()
    return 0


def _enqueue_publish_batch_start(batch_id: int) -> None:
    db.create_or_reactivate_runtime_task(
        task_type="publish_batch_start",
        entity_type="publish_batch",
        entity_id=int(batch_id),
        payload={"batch_id": int(batch_id)},
        max_attempts=3,
        reactivate_if_terminal=True,
    )
    _ensure_runtime_worker_thread()
    RUNTIME_WORKER_WAKEUP.set()


def _enqueue_instagram_audit_batch(batch_id: int) -> None:
    db.create_or_reactivate_runtime_task(
        task_type="instagram_audit_batch_run",
        entity_type="instagram_audit_batch",
        entity_id=int(batch_id),
        payload={"audit_batch_id": int(batch_id)},
        max_attempts=INSTAGRAM_AUDIT_ITEM_RETRY_ATTEMPTS,
    )
    _ensure_runtime_worker_thread()
    RUNTIME_WORKER_WAKEUP.set()


def _admin_public_base_url(request: Request) -> str:
    if PUBLISH_BASE_URL:
        return PUBLISH_BASE_URL
    base = str(request.base_url).rstrip("/")
    return f"{base}{ADMIN_BASE_PATH}" if ADMIN_BASE_PATH else base


def _runtime_admin_public_base_url() -> str:
    if PUBLISH_BASE_URL:
        return PUBLISH_BASE_URL
    raise RuntimeError("PUBLISH_BASE_URL не настроен. Runtime worker не может построить callback URL.")


def _absolute_admin_url(request: Request, path: str) -> str:
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return _admin_public_base_url(request) + suffix


def _absolute_runtime_admin_url(path: str) -> str:
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return _runtime_admin_public_base_url() + suffix


def _publish_internal_callback_url(path: str) -> str:
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    parsed = urlparse(PUBLISH_N8N_WEBHOOK_URL or "")
    if parsed.hostname in {"127.0.0.1", "localhost"}:
        return f"http://127.0.0.1:18001{suffix}"
    return ""


def _publish_staging_root() -> Path:
    root = Path(PUBLISH_STAGING_DIR).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _publish_batch_stage_path(batch_id: int) -> Path:
    return _publish_staging_root() / str(int(batch_id))


def _publish_batch_stage_dir(batch_id: int) -> Path:
    directory = _publish_batch_stage_path(int(batch_id))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _normalize_publish_upload_filename(filename: str) -> str:
    raw_name = Path((filename or "").strip() or "source.mp4").name
    suffix = Path(raw_name).suffix.lower()
    if suffix not in PUBLISH_VIDEO_EXTENSIONS:
        raise ValueError("Поддерживаются только .mp4 и .mov файлы.")
    stem = re.sub(r"[^A-Za-z0-9._-]+", "_", Path(raw_name).stem).strip("._") or "source"
    return f"{stem}{suffix}"


def _store_publish_batch_upload(batch_id: int, media_file: UploadFile) -> tuple[Path, int]:
    filename = _normalize_publish_upload_filename(getattr(media_file, "filename", "") or "source.mp4")
    batch_dir = _publish_batch_stage_dir(int(batch_id))
    target_path = batch_dir / filename
    if target_path.exists():
        target_path = batch_dir / f"{target_path.stem}-{int(time.time())}{target_path.suffix}"

    size_bytes = 0
    with target_path.open("wb") as handle:
        while True:
            chunk = media_file.file.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)
            size_bytes += len(chunk)
    if size_bytes <= 0:
        try:
            target_path.unlink()
        except Exception:
            pass
        raise ValueError("Файл пустой.")
    return target_path, size_bytes


def _normalize_publish_artifact_path(batch_id: int, raw_path: str) -> Path:
    value = (raw_path or "").strip()
    if not value:
        raise ValueError("Artifact path is required")
    batch_dir = _publish_batch_stage_dir(int(batch_id)).resolve()
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = batch_dir / candidate
    resolved = candidate.resolve()
    if resolved != batch_dir and batch_dir not in resolved.parents:
        raise ValueError("Artifact path must stay inside publish staging dir")
    return resolved


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _cleanup_publish_batch_stage_dir(batch_id: int) -> dict[str, Any]:
    stage_dir = _publish_batch_stage_path(int(batch_id))
    payload = {"path": str(stage_dir)}
    if not stage_dir.exists():
        return {"removed": False, **payload}
    shutil.rmtree(stage_dir)
    db.append_publish_job_event(
        int(batch_id),
        state="staging_cleaned",
        detail=f"Staging папка удалена после завершения batch: {stage_dir}.",
        payload=payload,
    )
    return {"removed": True, **payload}


def _maybe_cleanup_publish_batch_stage_dir(batch_id: int, batch_state: Optional[str], *, job_id: Optional[int] = None) -> dict[str, Any] | None:
    if not _publish_batch_is_terminal(batch_state):
        return None
    try:
        return _cleanup_publish_batch_stage_dir(int(batch_id))
    except Exception as exc:
        db.append_publish_job_event(
            int(batch_id),
            state="staging_cleanup_failed",
            detail=f"Не удалось удалить staging папку: {exc}",
            payload={"batch_id": int(batch_id)},
            job_id=int(job_id) if job_id is not None else None,
        )
        return None


def _publish_signature(timestamp: str, body: bytes) -> str:
    secret = (PUBLISH_SHARED_SECRET or "").encode("utf-8")
    signed = timestamp.encode("utf-8") + b"." + body
    return hmac.new(secret, signed, hashlib.sha256).hexdigest()


def _signed_publish_headers(body: bytes) -> dict[str, str]:
    timestamp = str(int(time.time()))
    return {
        "X-Publish-Timestamp": timestamp,
        "X-Publish-Signature": _publish_signature(timestamp, body),
        "Content-Type": "application/json",
    }


def _publish_event_hash(payload: dict[str, Any]) -> str:
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _run_publish_generation_watchdog(batch_id: Optional[int] = None) -> int:
    timeout_seconds = max(30, int(PUBLISH_FACTORY_TIMEOUT_SECONDS or 0))
    results = db.fail_stale_generation_accounts(batch_id=batch_id, timeout_seconds=timeout_seconds)
    for item in results:
        logger.warning(
            "publish_generation_timeout: batch_id=%s account_id=%s timeout_seconds=%s",
            item.get("batch_id"),
            item.get("account_id"),
            item.get("timeout_seconds"),
        )
    return len(results)


def _verify_signed_publish_request(body: bytes, timestamp: Optional[str], signature: Optional[str]) -> None:
    if not PUBLISH_SHARED_SECRET:
        raise HTTPException(status_code=503, detail="Publish shared secret is not configured")
    ts_raw = (timestamp or "").strip()
    sig_raw = (signature or "").strip().lower()
    if not ts_raw or not sig_raw:
        raise HTTPException(status_code=401, detail="Missing publish signature")
    try:
        ts_value = int(ts_raw)
    except Exception as exc:
        raise HTTPException(status_code=401, detail="Invalid publish timestamp") from exc
    if abs(int(time.time()) - ts_value) > PUBLISH_WEBHOOK_MAX_AGE_SECONDS:
        raise HTTPException(status_code=401, detail="Publish signature expired")
    expected = _publish_signature(ts_raw, body)
    if not hmac.compare_digest(expected, sig_raw):
        raise HTTPException(status_code=401, detail="Invalid publish signature")


def _parse_owner_worker_id(raw: Optional[str], *, allow_none_token: bool = True) -> Optional[int]:
    value = (raw or "").strip()
    if not value:
        return None
    if allow_none_token and value.lower() in {"none", "null", "без"}:
        return None
    owner_id = int(value)
    if owner_id <= 0:
        raise ValueError("invalid worker id")
    if db.get_worker(owner_id) is None:
        raise ValueError("worker not found")
    return owner_id


def _decode_accounts_import(raw_bytes: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "cp1251", "latin-1"):
        try:
            return raw_bytes.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw_bytes.decode("utf-8", errors="replace")


def _guess_accounts_import_delimiter(lines: list[str]) -> str:
    sample = "\n".join(lines[:5]).strip()
    if sample:
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|:")
            if dialect.delimiter in {",", ";", "\t", "|", ":"}:
                return dialect.delimiter
        except Exception:
            pass
    counts = {delim: sum(line.count(delim) for line in lines[:10]) for delim in (",", ";", "\t", "|", ":")}
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ";"


def _normalize_header_token(value: str) -> str:
    return "".join(ch for ch in (value or "").strip().lower() if ch.isalnum())


def _looks_like_accounts_import_header(cells: list[str]) -> bool:
    tokens = {_normalize_header_token(cell) for cell in cells if (cell or "").strip()}
    if not tokens:
        return False
    known = {
        "login",
        "логин",
        "accountlogin",
        "username",
        "user",
        "password",
        "pass",
        "пароль",
        "email",
        "mail",
        "почта",
        "emailpassword",
        "mailpassword",
        "парольпочты",
        "2fa",
        "twofa",
        "otp",
    }
    return len(tokens & known) >= 2


def _build_import_username(account_login: str) -> str:
    value = (account_login or "").strip()
    if value.startswith("@"):
        value = value[1:].strip()
    return value or "imported"


def _split_accounts_import_line(line: str, preferred_delimiter: str) -> list[str]:
    candidates = [preferred_delimiter, ";", ",", "\t", "|", ":"]
    seen: set[str] = set()
    for delimiter in candidates:
        if delimiter in seen:
            continue
        seen.add(delimiter)
        row = next(csv.reader([line], delimiter=delimiter))
        cells = [(cell or "").strip() for cell in row]
        while cells and not cells[-1]:
            cells.pop()
        if 4 <= len(cells) <= 5:
            return cells
    row = next(csv.reader([line], delimiter=preferred_delimiter))
    cells = [(cell or "").strip() for cell in row]
    while cells and not cells[-1]:
        cells.pop()
    return cells


def _parse_accounts_import_upload(raw_bytes: bytes) -> tuple[list[dict], list[str]]:
    if not raw_bytes:
        return [], ["Файл пустой."]

    text = _decode_accounts_import(raw_bytes)
    lines = [line for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
    if not lines:
        return [], ["Файл пустой."]

    delimiter = _guess_accounts_import_delimiter(lines)
    rows = [_split_accounts_import_line(line, delimiter) for line in lines]
    if not rows:
        return [], ["Файл пустой."]

    parsed: list[dict] = []
    errors: list[str] = []
    start_index = 0
    if rows and _looks_like_accounts_import_header(rows[0]):
        start_index = 1

    for offset, row in enumerate(rows[start_index:], start=start_index + 1):
        cells = [(cell or "").strip() for cell in row]
        while cells and not cells[-1]:
            cells.pop()
        if not any(cells):
            continue
        if len(cells) < 4 or len(cells) > 5:
            errors.append(f"Строка {offset}: ожидается 4 или 5 колонок.")
            continue

        account_login, account_password, email, email_password = cells[:4]
        twofa = cells[4] if len(cells) >= 5 else ""

        if not account_login or not account_password or not email or not email_password:
            errors.append(f"Строка {offset}: обязательные поля пустые.")
            continue

        parsed.append(
            {
                "line": offset,
                "account_login": account_login,
                "account_password": account_password,
                "username": _build_import_username(account_login),
                "email": email,
                "email_password": email_password,
                "twofa": twofa,
            }
        )

    if not parsed and not errors:
        errors.append("Файл не содержит валидных строк.")
    return parsed, errors


def _duplicate_account_message(account_row: dict) -> str:
    type_label = ACCOUNT_TYPE_LABELS.get(str(account_row.get("type") or ""), str(account_row.get("type") or "").upper())
    owner_name = str(account_row.get("owner_worker_name") or "").strip()
    owner_username = str(account_row.get("owner_worker_username") or "").strip()
    owner_label = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "без работника")
    return f"Аккаунт {type_label} с логином {account_row.get('account_login')} уже есть в системе. Владелец: {owner_label}."


def _account_form_error_message(raw_error: str) -> str:
    if raw_error == "invalid emulator serial":
        return "Неверный Instagram emulator serial. Укажи serial вроде emulator-5554 или default."
    if raw_error == "invalid mail provider":
        return "Неверный mail provider."
    if raw_error == "invalid mail auth json":
        return "Mail auth JSON должен быть валидным JSON-объектом."
    if raw_error == "invalid twofa secret":
        return "2FA должен быть валидным base32 secret или otpauth:// URI из Authenticator."
    if raw_error == "invalid mail watch json":
        return "Mail watch JSON повреждён."
    if raw_error == "worker not found":
        return "Неверный работник"
    return raw_error


def _claim_request_feedback(created: bool) -> str:
    if created:
        return "Аккаунт уже есть в системе. Заявка отправлена администратору."
    return "Заявка на этот аккаунт уже отправлена администратору."


def _claim_request_error_message(raw_error: str) -> str:
    if raw_error == "already assigned":
        return "Этот аккаунт уже закреплён за тобой."
    if raw_error == "account not found":
        return "Исходный аккаунт не найден."
    if raw_error == "worker not found":
        return "Работник не найден."
    return raw_error


def _claim_status_meta(status: str) -> tuple[str, str]:
    value = (status or "").strip().lower()
    if value == "approved":
        return "Одобрено", "on"
    if value == "rejected":
        return "Отклонено", "off"
    return "Ожидает", "wait"


def _worker_filter_meta(raw: Optional[str]) -> tuple[str, Optional[int], bool]:
    value = (raw or "").strip()
    if not value:
        return "", None, False
    if value.lower() == "none":
        return "none", None, True
    worker_id = int(value)
    if worker_id <= 0:
        raise ValueError("invalid worker filter")
    if db.get_worker(worker_id) is None:
        raise ValueError("worker not found")
    return str(worker_id), int(worker_id), False


def _accounts_page_response(
    request: Request,
    *,
    q: str,
    account_type: str,
    worker_filter: str = "",
    rotation_state: str = "",
    views_state: str = "",
    sort: str = "recent",
    error: Optional[str] = None,
    success: Optional[str] = None,
    import_summary: Optional[dict] = None,
    import_errors: Optional[list[str]] = None,
    status_code: int = 200,
) -> HTMLResponse:
    db.sync_instagram_auto_rotation_states(limit=1000)
    try:
        worker_filter_value, worker_filter_id, unassigned_only = _worker_filter_meta(worker_filter)
    except ValueError:
        worker_filter_value, worker_filter_id, unassigned_only = "", None, False
        error = error or "Неверный фильтр работника"
    try:
        rotation_filter_value = _normalize_rotation_state_filter(rotation_state)
    except ValueError:
        rotation_filter_value = ""
        error = error or "Неверный статус аккаунта"
    try:
        views_filter_value = _normalize_views_state_filter(views_state)
    except ValueError:
        views_filter_value = ""
        error = error or "Неверный статус просмотров"
    try:
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        sort_value = "recent"
        error = error or "Неверная сортировка"

    list_url = _accounts_redirect_url(q, account_type, worker_filter_value, rotation_filter_value, views_filter_value, sort_value)
    raw_rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=worker_filter_id,
        rotation_state=rotation_filter_value or None,
        views_state=views_filter_value or None,
        sort_by=sort_value,
        limit=500,
    )
    rows = []
    for raw in raw_rows:
        account = dict(raw)
        account_id = int(account["id"])
        account["card_id"] = f"account-{account_id}"
        account["detail_url"] = _build_detail_url(f"/accounts/{account_id}", f"{list_url}#{account['card_id']}")
        if unassigned_only and account.get("owner_worker_id") is not None:
            continue
        account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
        code = str(account.get("primary_link_code") or "").strip()
        account["primary_bot_url"] = _build_bot_start_url(code) if code else ""
        account["profile_url"] = _build_social_profile_url(str(account.get("type") or ""), str(account.get("username") or ""))
        account["has_profile_url"] = bool(account["profile_url"])
        rotation_label, rotation_class = _account_rotation_state_meta(account.get("rotation_state"))
        views_label, views_class = _account_views_state_meta(account.get("views_state"))
        mail_label, mail_class = _account_mail_status_meta(account.get("mail_status"))
        launch_label, launch_class = _account_instagram_launch_status_meta(account.get("instagram_launch_status"))
        publish_label, publish_class = _account_instagram_publish_status_meta(account.get("instagram_publish_status"))
        views_short = _account_views_short_label(account.get("views_state"))
        account["rotation_state_label"] = rotation_label
        account["rotation_state_class"] = rotation_class
        account["rotation_state_reason"] = db.account_rotation_display_reason(account)
        account["compact_block_reason"] = _compact_account_block_reason(account)
        account["rotation_state_source"] = str(account.get("rotation_state_source") or "manual").strip().lower() or "manual"
        account["views_state_label"] = views_label
        account["views_state_class"] = views_class
        account["views_short_label"] = views_short
        account["mail_status_label"] = mail_label
        account["mail_status_class"] = mail_class
        account["mail_provider_label"] = _account_mail_provider_label(account.get("mail_provider"))
        ready_label, ready_class, ready_detail = _account_mail_ready_meta(account)
        account["mail_ready_label"] = ready_label
        account["mail_ready_class"] = ready_class
        account["mail_ready_detail"] = ready_detail
        account["instagram_launch_status_label"] = launch_label
        account["instagram_launch_status_class"] = launch_class
        account["instagram_publish_status_label"] = publish_label
        account["instagram_publish_status_class"] = publish_class
        account["identity_handle"] = _account_identity_handle(account)
        owner_name = str(account.get("owner_worker_name") or "").strip()
        owner_username = str(account.get("owner_worker_username") or "").strip()
        account["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
        latest_audit = db.get_latest_instagram_audit_for_account(int(account["id"]))
        if latest_audit:
            latest_audit_dict = dict(latest_audit)
            resolution_label, _ = _instagram_audit_resolution_meta(latest_audit_dict.get("resolution_state"))
            joke_label, joke_class = _instagram_audit_joke(latest_audit_dict.get("resolution_state"))
            account["latest_audit_url"] = with_base(f"/accounts/instagram/audits/{int(latest_audit_dict['audit_batch_id'])}")
            account["latest_audit_label"] = resolution_label
            account["latest_audit_updated_at"] = int(latest_audit_dict.get("updated_at") or 0)
            account["audit_joke_label"] = joke_label
            account["audit_joke_class"] = joke_class
        else:
            account["latest_audit_url"] = ""
            account["latest_audit_label"] = ""
            account["latest_audit_updated_at"] = 0
            if str(account.get("type") or "").strip().lower() == "instagram":
                account["audit_joke_label"] = "нет проверки"
            else:
                account["audit_joke_label"] = "не требуется"
            account["audit_joke_class"] = "unknown"
        rows.append(account)

    overview = db.accounts_overview()
    workers = [dict(w) for w in db.list_workers_compact(limit=500)]
    claim_requests = []
    for raw in db.list_account_claim_requests(status="pending", limit=200):
        row = dict(raw)
        row["type_label"] = ACCOUNT_TYPE_LABELS.get(str(row.get("account_type") or ""), str(row.get("account_type") or "").upper())
        owner_name = str(row.get("owner_worker_name") or "").strip()
        owner_username = str(row.get("owner_worker_username") or "").strip()
        row["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else "Без работника"
        requested_name = str(row.get("requested_worker_name") or "").strip()
        requested_username = str(row.get("requested_worker_username") or "").strip()
        row["requested_label"] = f"{requested_name} (@{requested_username})" if requested_username else requested_name
        claim_requests.append(row)
    return templates.TemplateResponse(
        "accounts.html",
        {
            "request": request,
            "accounts": rows,
            "overview": overview,
            "q": q,
            "type": account_type,
            "worker": worker_filter_value,
            "rotation_state": rotation_filter_value,
            "views_state": views_filter_value,
            "sort": sort_value,
            "accounts_list_url": list_url,
            "workers": workers,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "rotation_state_options": ACCOUNT_ROTATION_STATE_OPTIONS,
            "views_state_options": ACCOUNT_VIEWS_STATE_OPTIONS,
            "mail_provider_options": ACCOUNT_MAIL_PROVIDER_OPTIONS,
            "sort_options": ACCOUNT_LIST_SORT_OPTIONS,
            "error": error,
            "success": success,
            "claim_requests": claim_requests,
            "import_summary": import_summary or None,
            "import_errors": import_errors or [],
        },
        status_code=status_code,
    )


def _account_detail_page_response(
    request: Request,
    *,
    account_id: int,
    owner_worker_id: Optional[int] = None,
    return_to: Optional[str] = None,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    return_to_clean = _safe_next_url(return_to, fallback="")
    back_url = return_to_clean or "/accounts"
    detail_self_url = _build_detail_url(f"/accounts/{int(account_id)}", return_to_clean)
    account_row = db.get_account(int(account_id), owner_worker_id=owner_worker_id)
    if not account_row:
        return templates.TemplateResponse(
            "account_detail.html",
            {
                "request": request,
                "account": None,
                "links": [],
                "stats": {"starts_unique_total": 0, "starts_total": 0, "first_touch_total": 0, "links_total": 0},
                "type_options": ACCOUNT_TYPE_OPTIONS,
                "workers": [dict(w) for w in db.list_workers_compact(limit=500)],
                "back_url": back_url,
                "detail_self_url": detail_self_url,
                "return_to": return_to_clean,
                "error": "Аккаунт не найден",
                "success": None,
            },
            status_code=404,
        )

    account = dict(account_row)
    account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
    account["profile_url"] = _build_social_profile_url(str(account.get("type") or ""), str(account.get("username") or ""))
    account["has_profile_url"] = bool(account["profile_url"])
    account["can_launch_instagram"] = bool(
        HELPER_API_KEY
        and str(account.get("type") or "").strip().lower() == "instagram"
        and str(account.get("account_login") or "").strip()
        and str(account.get("account_password") or "").strip()
    )
    rotation_label, rotation_class = _account_rotation_state_meta(account.get("rotation_state"))
    views_label, views_class = _account_views_state_meta(account.get("views_state"))
    mail_label, mail_class = _account_mail_status_meta(account.get("mail_status"))
    mail_challenge_label, mail_challenge_class = _account_mail_challenge_meta(account.get("mail_challenge_status"))
    launch_label, launch_class = _account_instagram_launch_status_meta(account.get("instagram_launch_status"))
    publish_label, publish_class = _account_instagram_publish_status_meta(account.get("instagram_publish_status"))
    views_short = _account_views_short_label(account.get("views_state"))
    account["rotation_state_label"] = rotation_label
    account["rotation_state_class"] = rotation_class
    account["rotation_state_reason"] = db.account_rotation_display_reason(account)
    account["rotation_state_source"] = str(account.get("rotation_state_source") or "manual").strip().lower() or "manual"
    account["views_state_label"] = views_label
    account["views_state_class"] = views_class
    account["views_short_label"] = views_short
    account["mail_status_label"] = mail_label
    account["mail_status_class"] = mail_class
    account["mail_provider_label"] = _account_mail_provider_label(account.get("mail_provider"))
    ready_label, ready_class, ready_detail = _account_mail_ready_meta(account)
    account["mail_ready_label"] = ready_label
    account["mail_ready_class"] = ready_class
    account["mail_ready_detail"] = ready_detail
    account["mail_challenge_label"] = mail_challenge_label
    account["mail_challenge_class"] = mail_challenge_class
    account["mail_challenge_kind_label"] = ACCOUNT_MAIL_CHALLENGE_KIND_LABELS.get(str(account.get("mail_challenge_kind") or "").strip(), "")
    account["instagram_launch_status_label"] = launch_label
    account["instagram_launch_status_class"] = launch_class
    account["instagram_publish_status_label"] = publish_label
    account["instagram_publish_status_class"] = publish_class
    account["identity_handle"] = _account_identity_handle(account)
    account["can_publish_instagram"] = bool(account["can_launch_instagram"])
    links = [dict(r) for r in db.list_account_links_with_stats(int(account["id"]), owner_worker_id=owner_worker_id)]
    for link in links:
        link["bot_url"] = _build_bot_start_url(str(link.get("code") or ""))
    stats = db.account_stats(int(account["id"]), owner_worker_id=owner_worker_id)
    mail_messages = _mail_messages_with_metadata(int(account["id"]), limit=mail_service.MAIL_FETCH_LIMIT)
    latest_mail = mail_messages[0] if mail_messages else None
    workers = [dict(w) for w in db.list_workers_compact(limit=500)]
    owner_name = str(account.get("owner_worker_name") or "").strip()
    owner_username = str(account.get("owner_worker_username") or "").strip()
    account["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
    latest_reel_row = db.get_latest_instagram_reel_post_for_account(int(account["id"]))
    latest_reel = None
    if latest_reel_row is not None:
        latest_reel = _decorate_instagram_reel_post(
            dict(latest_reel_row),
            snapshots=[dict(item) for item in db.list_instagram_reel_metric_snapshots(int(latest_reel_row["id"]))],
        )
    latest_audit = db.get_latest_instagram_audit_for_account(int(account["id"]))
    latest_audit_data = dict(latest_audit) if latest_audit else None
    if latest_audit_data:
        resolution_label, resolution_class = _instagram_audit_resolution_meta(latest_audit_data.get("resolution_state"))
        joke_label, joke_class = _instagram_audit_joke(latest_audit_data.get("resolution_state"))
        latest_audit_data["resolution_label"] = resolution_label
        latest_audit_data["resolution_class"] = resolution_class
        latest_audit_data["joke_label"] = joke_label
        latest_audit_data["joke_class"] = joke_class
        latest_audit_data["url"] = with_base(f"/accounts/instagram/audits/{int(latest_audit_data['audit_batch_id'])}")
        latest_audit_data["updated_at_label"] = _format_timestamp_label(latest_audit_data.get("updated_at"))
    if str(account.get("type") or "").strip().lower() == "instagram":
        fallback_label = "нет проверки"
    else:
        fallback_label = "не требуется"
    if latest_audit_data:
        account["audit_joke_label"] = latest_audit_data["joke_label"]
        account["audit_joke_class"] = latest_audit_data["joke_class"]
    else:
        account["audit_joke_label"] = fallback_label
        account["audit_joke_class"] = "unknown"

    return templates.TemplateResponse(
        "account_detail.html",
        {
            "request": request,
            "account": account,
            "links": links,
            "stats": stats,
            "mail_messages": mail_messages,
            "latest_mail": latest_mail,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "rotation_state_options": ACCOUNT_ROTATION_STATE_OPTIONS,
            "views_state_options": ACCOUNT_VIEWS_STATE_OPTIONS,
            "mail_provider_options": ACCOUNT_MAIL_PROVIDER_OPTIONS,
            "workers": workers,
            "back_url": back_url,
            "detail_self_url": detail_self_url,
            "return_to": return_to_clean,
            "latest_audit": latest_audit_data,
            "latest_reel": latest_reel,
            "instagram_publish_source_dir": INSTAGRAM_PUBLISH_SOURCE_DIR,
            "instagram_publish_source_info_url": _build_instagram_helper_local_url("/publish-source/latest"),
            "error": error,
            "success": success,
        },
        status_code=status_code,
    )


def _decorate_instagram_audit_batch(raw: dict[str, Any]) -> dict[str, Any]:
    batch = dict(raw)
    label, css = _instagram_audit_batch_state_meta(batch.get("state"))
    batch["state_label"] = label
    batch["state_class"] = css
    batch["is_terminal"] = _instagram_audit_batch_is_terminal(batch.get("state"))
    return batch


def _instagram_audit_event_meta(state: str, payload: dict[str, Any]) -> tuple[str, str]:
    value = (state or "").strip().lower()
    if value == "done":
        resolution = str(payload.get("resolution_state") or "").strip().lower()
        return _instagram_audit_resolution_meta(resolution)
    if value == "mail_check_if_needed":
        return ("Проверяю почту", "wait")
    if value == "login_check":
        return ("Проверяю вход", "wait")
    if value == "launching":
        return ("Запускаю helper", "wait")
    return (_instagram_audit_item_state_meta(value)[0], "unknown")


def _instagram_audit_sort_key(item: dict[str, Any]) -> tuple[int, int, str]:
    state = str(item.get("item_state") or "").strip().lower()
    resolution = str(item.get("resolution_state") or "").strip().lower()
    if state != "done":
        return (0, int(item.get("queue_position") or 0), "")
    priority = {
        "manual_2fa_required": 0,
        "email_code_required": 1,
        "challenge_required": 2,
        "invalid_password": 3,
        "helper_error": 4,
        "missing_credentials": 5,
        "missing_device": 6,
        "login_ok": 7,
    }.get(resolution, 8)
    return (1, priority, str(item.get("username") or ""))


def _build_instagram_audit_snapshot(batch_id: int) -> Optional[dict[str, Any]]:
    batch_row = db.get_instagram_audit_batch(int(batch_id))
    if batch_row is None:
        return None
    batch = _decorate_instagram_audit_batch(dict(batch_row))
    now_ts = int(time.time())
    item_rows = [dict(row) for row in db.list_instagram_audit_items(int(batch_id))]
    event_rows = [dict(row) for row in db.list_instagram_audit_events(int(batch_id), limit=100)]

    selected = len(item_rows)
    done = sum(1 for row in item_rows if str(row.get("item_state") or "") == "done")
    success = sum(1 for row in item_rows if str(row.get("resolution_state") or "") == "login_ok")
    issues = sum(1 for row in item_rows if str(row.get("resolution_state") or "") not in {"", "login_ok"})
    active_item = next((row for row in item_rows if str(row.get("item_state") or "") != "done"), None)
    batch_state = str(batch.get("state") or "").strip().lower()
    if batch["is_terminal"]:
        if batch_state == "completed":
            phase_label = "Готово"
            phase_subtitle = str(batch.get("detail") or "").strip() or "Аудит завершён."
        elif batch_state == "completed_with_errors":
            phase_label = "Готово с ручными шагами"
            phase_subtitle = str(batch.get("detail") or "").strip() or "Аудит завершён с проблемами."
        elif batch_state == "failed":
            phase_label = "Ошибка"
            phase_subtitle = str(batch.get("detail") or "").strip() or "Проверка завершилась ошибкой."
        elif batch_state == "canceled":
            phase_label = "Отменён"
            phase_subtitle = str(batch.get("detail") or "").strip() or "Проверка была отменена."
        else:
            phase_label = "Готово"
            phase_subtitle = str(batch.get("detail") or "").strip() or "Аудит завершён."
    elif active_item is not None:
        phase_label = _instagram_audit_item_state_meta(active_item.get("item_state"))[0]
        phase_subtitle = str(active_item.get("resolution_detail") or active_item.get("login_detail") or active_item.get("mail_probe_detail") or batch.get("detail") or "").strip()
    else:
        phase_label = "В очереди"
        phase_subtitle = str(batch.get("detail") or "").strip() or "Ожидаю запуск проверки."

    steps = []
    active_step = str(active_item.get("item_state") if active_item else ("done" if batch["is_terminal"] else "queued") or "queued")
    step_order = ["queued", "launching", "login_check", "mail_check_if_needed", "done"]
    current_index = step_order.index(active_step) if active_step in step_order else 0
    for idx, key in enumerate(step_order):
        status = "pending"
        if batch["is_terminal"] and key == "done":
            status = "done"
        elif idx < current_index:
            status = "done"
        elif idx == current_index:
            status = "active"
        steps.append({"key": key, "label": INSTAGRAM_AUDIT_ITEM_STATE_LABELS.get(key, key), "status": status})

    cards: list[dict[str, Any]] = []
    resolution_counts = {key: 0 for key in INSTAGRAM_AUDIT_RESOLUTION_LABELS}
    for row in item_rows:
        resolution = str(row.get("resolution_state") or "").strip().lower()
        if resolution in resolution_counts:
            resolution_counts[resolution] += 1
        item_label, item_class = _instagram_audit_item_state_meta(row.get("item_state"))
        login_label, login_class = _account_instagram_launch_status_meta(row.get("login_state"))
        mail_label, mail_class = _instagram_audit_mail_probe_meta(row.get("mail_probe_state"))
        resolution_label, resolution_class = _instagram_audit_resolution_meta(row.get("resolution_state"))
        owner_name = str(row.get("owner_worker_name") or "").strip()
        owner_username = str(row.get("owner_worker_username") or "").strip()
        owner_label = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
        cards.append(
            {
                "id": int(row["id"]),
                "account_id": int(row["account_id"]),
                "queue_position": int(row.get("queue_position") or 0),
                "username": str(row.get("username") or "").strip(),
                "account_login": str(row.get("account_login") or "").strip(),
                "assigned_serial": str(row.get("assigned_serial") or "").strip(),
                "item_state": str(row.get("item_state") or ""),
                "item_state_label": item_label,
                "item_state_class": item_class,
                "login_state": str(row.get("login_state") or ""),
                "login_state_label": login_label,
                "login_state_class": login_class,
                "mail_probe_state": str(row.get("mail_probe_state") or ""),
                "mail_probe_state_label": mail_label,
                "mail_probe_state_class": mail_class,
                "resolution_state": str(row.get("resolution_state") or ""),
                "resolution_label": resolution_label,
                "resolution_class": resolution_class,
                "detail": str(row.get("resolution_detail") or row.get("login_detail") or row.get("mail_probe_detail") or "").strip(),
                "login_detail": str(row.get("login_detail") or "").strip(),
                "mail_probe_detail": str(row.get("mail_probe_detail") or "").strip(),
                "diagnostic_path": str(row.get("diagnostic_path") or "").strip(),
                "updated_at_label": _format_timestamp_label(row.get("updated_at")),
                "updated_at_relative_label": _format_relative_age_label(row.get("updated_at"), now_ts=now_ts),
                "started_at_label": _format_timestamp_label(row.get("started_at")),
                "completed_at_label": _format_timestamp_label(row.get("completed_at")),
                "progress_pct": _instagram_audit_live_progress_pct(str(row.get("item_state") or ""), updated_at=row.get("updated_at")),
                "open_url": with_base(f"/accounts/{int(row['account_id'])}"),
                "owner_label": owner_label,
                "is_active": str(row.get("item_state") or "") != "done",
            }
        )
    cards.sort(key=_instagram_audit_sort_key)

    if batch["is_terminal"]:
        progress_pct = 100 if selected else 0
    else:
        progress_pct = int(round(sum(int(card.get("progress_pct") or 0) for card in cards) / len(cards))) if cards else 0

    recent_activity = []
    for row in event_rows[:10]:
        payload = _parse_json_object(row.get("payload_json"))
        title, tone_class = _instagram_audit_event_meta(str(row.get("state") or ""), payload)
        recent_activity.append(
            {
                "id": int(row["id"]),
                "title": title,
                "tone_class": tone_class,
                "detail": str(row.get("detail") or "").strip(),
                "account_username": str(row.get("account_username") or row.get("account_login") or "").strip(),
                "created_at_label": _format_timestamp_label(row.get("created_at")),
            }
        )

    latest_event_ts = max((_unix_timestamp(row.get("created_at")) for row in event_rows), default=0)
    batch_touch_ts = max(_unix_timestamp(batch.get("updated_at")), latest_event_ts, _unix_timestamp(batch.get("created_at")))

    return {
        "batch": {
            **batch,
            "progress_pct": progress_pct,
            "phase_label": phase_label,
            "phase_subtitle": phase_subtitle,
            "created_at_label": _format_timestamp_label(batch.get("created_at")),
            "updated_at_label": _format_timestamp_label(batch.get("updated_at")),
            "updated_at_relative_label": _format_relative_age_label(batch_touch_ts, now_ts=now_ts),
            "started_at_label": _format_timestamp_label(batch.get("started_at")),
            "completed_at_label": _format_timestamp_label(batch.get("completed_at")),
            "steps": steps,
            "counts": {
                "selected": selected,
                "done": done,
                "success": success,
                "issues": issues,
                "manual_2fa_required": resolution_counts["manual_2fa_required"],
                "email_code_required": resolution_counts["email_code_required"],
                "challenge_required": resolution_counts["challenge_required"],
                "invalid_password": resolution_counts["invalid_password"],
                "helper_error": resolution_counts["helper_error"] + resolution_counts["missing_credentials"] + resolution_counts["missing_device"],
            },
        },
        "items": cards,
        "recent_activity": recent_activity,
        "events": [
            {
                "id": int(row["id"]),
                "state": str(row.get("state") or ""),
                "detail": str(row.get("detail") or "").strip(),
                "payload_preview": json.dumps(_parse_json_object(row.get("payload_json")), ensure_ascii=False),
                "account_username": str(row.get("account_username") or row.get("account_login") or "").strip(),
                "created_at_label": _format_timestamp_label(row.get("created_at")),
            }
            for row in event_rows
        ],
        "progress_url": with_base(f"/api/accounts/instagram/audits/{int(batch_id)}/progress"),
        "poll_interval_seconds": max(2, int(INSTAGRAM_AUDIT_POLL_INTERVAL_SECONDS or 2)),
    }


def _instagram_audit_detail_page_response(
    request: Request,
    *,
    audit_id: int,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    batch_row = db.get_instagram_audit_batch(int(audit_id))
    snapshot = _build_instagram_audit_snapshot(int(audit_id)) if batch_row else None
    batch = _decorate_instagram_audit_batch(dict(batch_row)) if batch_row else None
    return templates.TemplateResponse(
        "instagram_audit_detail.html",
        {
            "request": request,
            "audit_id": int(audit_id),
            "batch": batch,
            "dashboard_snapshot": snapshot,
            "error": error,
            "success": success,
        },
        status_code=status_code,
    )


def _decorate_publish_batch(raw: dict) -> dict:
    batch = dict(raw)
    label, css = _publish_batch_state_meta(batch.get("state"))
    batch["state_label"] = label
    batch["state_class"] = css
    batch["is_terminal"] = _publish_batch_is_terminal(batch.get("state"))
    batch["error_accounts"] = int(batch.get("generation_failed_accounts") or 0) + int(batch.get("failed_accounts") or 0) + int(batch.get("canceled_accounts") or 0)
    batch["active_accounts"] = int(batch.get("generating_accounts") or 0) + int(batch.get("queued_publish_accounts") or 0) + int(batch.get("active_publish_accounts") or 0)
    return batch


def _format_timestamp_label(ts: Any) -> str:
    try:
        value = int(ts or 0)
    except Exception:
        return "—"
    if value <= 0:
        return "—"
    return datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M")


def _unix_timestamp(value: Any) -> int:
    try:
        timestamp = int(value or 0)
    except Exception:
        return 0
    return timestamp if timestamp > 0 else 0


def _format_relative_age_label(ts: Any, *, now_ts: Optional[int] = None) -> str:
    timestamp = _unix_timestamp(ts)
    if timestamp <= 0:
        return "нет данных"
    now_value = int(now_ts if now_ts is not None else time.time())
    delta = max(0, now_value - timestamp)
    if delta <= 4:
        return "только что"
    if delta < 60:
        return f"{delta} сек назад"
    minutes = delta // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    days = hours // 24
    return f"{days} д назад"


def _smooth_live_progress(
    base_pct: Any,
    *,
    updated_at: Any,
    upper_bound: Any,
    warmup_seconds: int = 4,
    step_seconds: int = 4,
) -> int:
    try:
        base_value = int(round(float(base_pct)))
    except Exception:
        base_value = 0
    try:
        upper_value = int(round(float(upper_bound)))
    except Exception:
        upper_value = base_value
    base_value = max(0, min(100, base_value))
    upper_value = max(base_value, min(100, upper_value))
    updated_ts = _unix_timestamp(updated_at)
    if updated_ts <= 0 or upper_value <= base_value:
        return base_value
    elapsed = max(0, int(time.time()) - updated_ts)
    if elapsed < warmup_seconds:
        return base_value
    drift = 1 + ((elapsed - warmup_seconds) // max(1, step_seconds))
    return min(upper_value, base_value + drift)


def _parse_json_object(raw: Any) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    try:
        value = json.loads(text)
    except Exception:
        return {}
    return value if isinstance(value, dict) else {}


def _publish_generation_progress_percent(progress_pct: Any) -> int:
    try:
        progress_value = float(progress_pct)
    except Exception:
        progress_value = 0.0
    progress_value = max(0.0, min(100.0, progress_value))
    return int(round(5.0 + (progress_value * 0.5)))


def _publish_account_progress_for_state(state: str) -> int:
    value = (state or "").strip().lower()
    return {
        "queued_for_generation": 0,
        "generating": 20,
        "queued_for_publish": 60,
        "leased": 70,
        "preparing": 70,
        "importing_media": 78,
        "opening_reel_flow": 84,
        "selecting_media": 90,
        "publishing": 96,
        "published": 100,
        "needs_review": 100,
    }.get(value, 0)


def _publish_live_progress_ceiling(state: str) -> int:
    value = (state or "").strip().lower()
    return {
        "generating": 58,
        "queued_for_publish": 68,
        "leased": 76,
        "preparing": 76,
        "importing_media": 82,
        "opening_reel_flow": 88,
        "selecting_media": 94,
        "publishing": 99,
    }.get(value, 0)


def _publish_live_progress_for_state(base_pct: int, state: str, *, updated_at: Any) -> int:
    upper_bound = _publish_live_progress_ceiling(state)
    if upper_bound <= base_pct:
        return int(max(0, min(100, base_pct)))
    return _smooth_live_progress(base_pct, updated_at=updated_at, upper_bound=upper_bound)


def _publish_payload_phase(payload: dict[str, Any]) -> str:
    return str(payload.get("publish_phase") or "").strip().lower()


def _publish_payload_event_kind(payload: dict[str, Any]) -> str:
    return str(payload.get("event_kind") or "").strip().lower()


def _publish_payload_elapsed_seconds(payload: dict[str, Any]) -> int:
    try:
        return max(0, int(float(payload.get("elapsed_seconds") or 0)))
    except Exception:
        return 0


def _publish_payload_upload_progress(payload: dict[str, Any]) -> Optional[int]:
    raw = payload.get("upload_progress_pct")
    if raw in (None, ""):
        return None
    try:
        value = int(float(raw))
    except Exception:
        return None
    return max(0, min(100, value))


def _publish_payload_bool(payload: dict[str, Any], key: str) -> bool:
    value = payload.get(key)
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _publish_payload_int(payload: dict[str, Any], key: str) -> Optional[int]:
    raw = payload.get(key)
    if raw in (None, ""):
        return None
    try:
        return int(float(raw))
    except Exception:
        return None


def _format_elapsed_seconds_label(seconds: int) -> str:
    value = max(0, int(seconds))
    if value < 60:
        return f"{value} сек назад"
    minutes = value // 60
    if minutes < 60:
        return f"{minutes} мин назад"
    hours = minutes // 60
    if hours < 24:
        return f"{hours} ч назад"
    days = hours // 24
    return f"{days} дн назад"


def _format_duration_short_label(seconds: int) -> str:
    value = max(0, int(seconds))
    hours, remainder = divmod(value, 3600)
    minutes, secs = divmod(remainder, 60)
    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours} ч")
    if minutes > 0:
        parts.append(f"{minutes} мин")
    if secs > 0 or not parts:
        parts.append(f"{secs} сек")
    return " ".join(parts)


def _format_clock_label(ts: Any) -> str:
    try:
        value = int(ts or 0)
    except Exception:
        return ""
    if value <= 0:
        return ""
    return datetime.fromtimestamp(value).strftime("%H:%M:%S")


def _instagram_reel_collection_stage_label(stage: Any) -> str:
    value = str(stage or "").strip().lower()
    return {
        "t30m": "30м",
        "t6h": "6ч",
        "t24h": "24ч",
        "t72h": "72ч",
        "done": "Готово",
    }.get(value, "—")


def _instagram_reel_collection_state_meta(state: Any) -> tuple[str, str]:
    value = str(state or "").strip().lower()
    mapping = {
        "scheduled": ("Ожидает сбор", "wait"),
        "leased": ("Собирается", "wait"),
        "collected": ("Собрано", "on"),
        "partial": ("Частично", "review"),
        "unavailable": ("Недоступно", "review"),
        "not_found": ("Reel не найден", "review"),
        "failed": ("Ошибка", "off"),
    }
    return mapping.get(value, ("—", "unknown"))


def _instagram_reel_snapshot_status_meta(status: Any) -> tuple[str, str]:
    value = str(status or "").strip().lower()
    mapping = {
        "ok": ("Полный", "on"),
        "partial": ("Частичный", "review"),
        "unavailable": ("Недоступно", "review"),
        "not_found": ("Не найден", "review"),
        "failed": ("Ошибка", "off"),
    }
    return mapping.get(value, ("—", "unknown"))


def _format_metric_compact_value(value: Any) -> str:
    try:
        number = int(value)
    except Exception:
        return "—"
    if number < 1000:
        return str(number)
    if number < 1_000_000:
        compact = round(number / 1000.0, 1)
        return f"{compact:.1f}K".replace(".0K", "K")
    compact = round(number / 1_000_000.0, 1)
    return f"{compact:.1f}M".replace(".0M", "M")


def _format_metric_seconds_value(value: Any) -> str:
    try:
        seconds = float(value)
    except Exception:
        return "—"
    if seconds < 0:
        return "—"
    rounded = int(round(seconds))
    if rounded < 60:
        return f"{rounded}с"
    minutes, secs = divmod(rounded, 60)
    if minutes < 60:
        return f"{minutes}м {secs}с" if secs else f"{minutes}м"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}ч {minutes}м" if minutes else f"{hours}ч"


def _format_metric_percent_value(value: Any) -> str:
    try:
        pct = float(value)
    except Exception:
        return "—"
    return f"{pct:.1f}%".replace(".0%", "%")


def _decorate_instagram_reel_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    status_label, status_class = _instagram_reel_snapshot_status_meta(row.get("status"))
    row["status_label"] = status_label
    row["status_class"] = status_class
    row["window_label"] = _instagram_reel_collection_stage_label(row.get("window_key"))
    row["collected_at_label"] = _format_timestamp_label(row.get("collected_at"))
    row["plays_label"] = _format_metric_compact_value(row.get("plays_count"))
    row["likes_label"] = _format_metric_compact_value(row.get("likes_count"))
    row["comments_label"] = _format_metric_compact_value(row.get("comments_count"))
    row["shares_label"] = _format_metric_compact_value(row.get("shares_count"))
    row["saves_label"] = _format_metric_compact_value(row.get("saves_count"))
    row["accounts_reached_label"] = _format_metric_compact_value(row.get("accounts_reached_count"))
    row["watch_time_label"] = _format_metric_seconds_value(row.get("watch_time_seconds"))
    row["avg_watch_time_label"] = _format_metric_seconds_value(row.get("avg_watch_time_seconds"))
    row["three_second_views_label"] = _format_metric_compact_value(row.get("three_second_views_count"))
    row["completion_rate_label"] = _format_metric_percent_value(row.get("completion_rate_pct"))
    return row


def _decorate_instagram_reel_post(raw: dict[str, Any], *, snapshots: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    row = dict(raw)
    state_label, state_class = _instagram_reel_collection_state_meta(row.get("collection_state"))
    latest_status_label, latest_status_class = _instagram_reel_snapshot_status_meta(row.get("latest_snapshot_status"))
    row["collection_stage_label"] = _instagram_reel_collection_stage_label(row.get("collection_stage"))
    row["collection_state_label"] = state_label
    row["collection_state_class"] = state_class
    row["latest_snapshot_status_label"] = latest_status_label
    row["latest_snapshot_status_class"] = latest_status_class
    row["published_at_label"] = _format_timestamp_label(row.get("published_at"))
    row["next_collect_at_label"] = _format_timestamp_label(row.get("next_collect_at"))
    row["last_collected_at_label"] = _format_timestamp_label(row.get("last_collected_at") or row.get("latest_snapshot_collected_at"))
    row["latest_snapshot_window_label"] = _instagram_reel_collection_stage_label(row.get("latest_snapshot_window_key"))
    row["plays_label"] = _format_metric_compact_value(row.get("latest_snapshot_plays_count"))
    row["likes_label"] = _format_metric_compact_value(row.get("latest_snapshot_likes_count"))
    row["comments_label"] = _format_metric_compact_value(row.get("latest_snapshot_comments_count"))
    row["shares_label"] = _format_metric_compact_value(row.get("latest_snapshot_shares_count"))
    row["saves_label"] = _format_metric_compact_value(row.get("latest_snapshot_saves_count"))
    row["accounts_reached_label"] = _format_metric_compact_value(row.get("latest_snapshot_accounts_reached_count"))
    row["watch_time_label"] = _format_metric_seconds_value(row.get("latest_snapshot_watch_time_seconds"))
    row["avg_watch_time_label"] = _format_metric_seconds_value(row.get("latest_snapshot_avg_watch_time_seconds"))
    row["three_second_views_label"] = _format_metric_compact_value(row.get("latest_snapshot_three_second_views_count"))
    row["completion_rate_label"] = _format_metric_percent_value(row.get("latest_snapshot_completion_rate_pct"))
    row["compact_summary"] = " · ".join(
        item
        for item in (
            f"Просмотры {row['plays_label']}" if row["plays_label"] != "—" else "",
            f"Лайки {row['likes_label']}" if row["likes_label"] != "—" else "",
            f"Комменты {row['comments_label']}" if row["comments_label"] != "—" else "",
            row["last_collected_at_label"] if row["last_collected_at_label"] else "",
        )
        if item
    )
    row["history"] = [_decorate_instagram_reel_snapshot(dict(item)) for item in (snapshots or [])]
    return row


def _publish_payload_mail_challenge(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("mail_challenge") if isinstance(payload, dict) else None
    return dict(value) if isinstance(value, dict) else {}


def _publish_mail_event_meta(event_kind: str) -> tuple[str, str]:
    value = (event_kind or "").strip().lower()
    mapping = {
        "mail_challenge_checking": ("Проверяю почту", "wait"),
        "challenge_email_option_selected": ("Выбран email", "wait"),
        "mail_code_found": ("Код из письма найден", "wait"),
        "mail_code_applied": ("Код из письма введён", "on"),
        "mail_code_not_found": ("Код из письма не найден", "review"),
        "mail_code_rejected": ("Код из письма не принят", "review"),
        "approval_link_opened": ("Открываю ссылку из письма", "wait"),
        "approval_link_applied": ("Ссылка из письма применена", "on"),
        "approval_link_failed": ("Ссылка из письма не сработала", "review"),
        "challenge_phone_only": ("Доступен только phone", "review"),
        "challenge_manual_recovery_only": ("Нужен manual recovery", "review"),
        "mailbox_unavailable": ("Почта недоступна", "off"),
        "challenge_requires_link": ("Нужна ссылка из письма", "review"),
        "challenge_screen_unsupported": ("Challenge не поддержан", "review"),
    }
    return mapping.get(value, ("", ""))


def _publish_phase_label_from_payload(payload: dict[str, Any]) -> str:
    mail_label, _ = _publish_mail_event_meta(_publish_payload_event_kind(payload))
    if mail_label:
        return mail_label
    return {
        "waiting_upload_start": "Запускаю загрузку",
        "uploading": "Загружается в Instagram",
        "waiting_confirmation": "Жду подтверждение публикации",
        "waiting_profile_verification_window": "Жду окно проверки профиля",
        "verifying_profile": "Проверяю в профиле",
    }.get(_publish_payload_phase(payload), "")


def _publish_phase_detail_from_payload(payload: dict[str, Any], fallback_detail: str = "") -> str:
    parts: list[str] = []
    progress_pct = _publish_payload_upload_progress(payload)
    last_activity = str(payload.get("last_activity") or "").strip()
    elapsed_seconds = _publish_payload_elapsed_seconds(payload)
    phase = _publish_payload_phase(payload)
    event_kind = _publish_payload_event_kind(payload)
    mail_challenge = _publish_payload_mail_challenge(payload)
    matched_age_seconds = _publish_payload_int(payload, "matched_age_seconds")
    verification_attempt = _publish_payload_int(payload, "verification_attempt")
    seconds_until_profile_check = _publish_payload_int(payload, "seconds_until_profile_check")
    share_clicked_at = _publish_payload_int(payload, "share_clicked_at")
    verification_starts_at = _publish_payload_int(payload, "verification_starts_at")
    first_profile_check_at = _publish_payload_int(payload, "first_profile_check_at")
    profile_surface_state = str(payload.get("profile_surface_state") or "").strip()
    diagnostics_path = str(payload.get("diagnostics_path") or "").strip()
    mail_reason_text = str(mail_challenge.get("reason_text") or "").strip()
    masked_code = str(mail_challenge.get("masked_code") or "").strip()
    confidence_value = float(mail_challenge.get("confidence") or 0.0) if mail_challenge else 0.0
    if event_kind and _publish_mail_event_meta(event_kind)[0]:
        if mail_reason_text:
            parts.append(mail_reason_text)
        elif last_activity:
            parts.append(last_activity)
        if masked_code:
            parts.append(f"Код: {masked_code}")
        if confidence_value > 0:
            parts.append(f"Уверенность: {int(round(confidence_value * 100))}%")
        if elapsed_seconds > 0:
            parts.append(f"Прошло {elapsed_seconds} сек")
        return " · ".join(parts) if parts else fallback_detail
    if progress_pct is not None:
        parts.append(f"Загрузка: {progress_pct}%")
    if phase == "waiting_profile_verification_window" and seconds_until_profile_check is not None:
        parts.append(f"До проверки профиля: {_format_duration_short_label(seconds_until_profile_check)}")
    if phase == "waiting_profile_verification_window" and verification_starts_at:
        parts.append(f"Старт окна: {_format_clock_label(verification_starts_at)}")
    if phase == "verifying_profile" and matched_age_seconds is not None:
        parts.append(f"Найден reel: {_format_elapsed_seconds_label(matched_age_seconds)}")
    if phase == "verifying_profile" and verification_attempt:
        parts.append(f"Проверка #{verification_attempt}")
    if phase == "verifying_profile" and first_profile_check_at:
        parts.append(f"Первый вход в профиль: {_format_clock_label(first_profile_check_at)}")
    if phase in {"waiting_profile_verification_window", "verifying_profile"} and share_clicked_at:
        parts.append(f"Share: {_format_clock_label(share_clicked_at)}")
    if phase == "verifying_profile" and profile_surface_state and not last_activity:
        parts.append(f"Состояние: {profile_surface_state}")
    if last_activity:
        parts.append(last_activity)
    if mail_reason_text and mail_reason_text not in parts:
        parts.append(mail_reason_text)
    if masked_code:
        parts.append(f"Код: {masked_code}")
    if confidence_value > 0:
        parts.append(f"Уверенность: {int(round(confidence_value * 100))}%")
    if phase == "verifying_profile" and diagnostics_path:
        parts.append("Диагностика сохранена")
    if elapsed_seconds > 0:
        parts.append(f"Прошло {elapsed_seconds} сек")
    if parts:
        return " · ".join(parts)
    return fallback_detail


def _publish_progress_for_publishing_payload(payload: dict[str, Any]) -> int:
    phase = _publish_payload_phase(payload)
    progress_pct = _publish_payload_upload_progress(payload)
    accepted_by_instagram = _publish_payload_bool(payload, "accepted_by_instagram")
    if phase == "waiting_upload_start":
        return 92
    if phase == "uploading":
        if progress_pct is not None:
            return max(93, min(98, 92 + int(round(progress_pct * 0.06))))
        return 95
    if phase == "waiting_confirmation":
        return 99 if accepted_by_instagram else 97
    if phase == "waiting_profile_verification_window":
        return 98 if accepted_by_instagram else 97
    if phase == "verifying_profile":
        return 99
    return 96


def _publish_event_progress_for_state(state: str, payload: dict[str, Any]) -> int:
    value = (state or "").strip().lower()
    if value == "generation_progress":
        return _publish_generation_progress_percent(payload.get("progress_pct"))
    if value in {"generation_started", "batch_created"}:
        return 20 if value == "generation_started" else 0
    if value == "artifact_ready":
        return 60
    if value == "publishing" and payload:
        return _publish_progress_for_publishing_payload(payload)
    if value in {"leased", "preparing", "importing_media", "opening_reel_flow", "selecting_media", "publishing", "published"}:
        return _publish_account_progress_for_state(value)
    return 0


def _publish_account_sort_group(state: str) -> int:
    value = (state or "").strip().lower()
    if value in {"generating", "queued_for_publish", "leased", "preparing", "importing_media", "opening_reel_flow", "selecting_media", "publishing"}:
        return 0
    if value == "queued_for_generation":
        return 1
    if value in {"published", "needs_review"}:
        return 2
    if value in {"generation_failed", "failed", "canceled"}:
        return 3
    return 4


def _publish_recent_event_meta(state: str, payload: dict[str, Any]) -> tuple[str, str]:
    value = (state or "").strip().lower()
    mail_title, mail_tone = _publish_mail_event_meta(_publish_payload_event_kind(payload))
    if mail_title:
        return mail_title, mail_tone
    if value == "generation_progress":
        stage_label = str(payload.get("stage_label") or "").strip()
        return stage_label or "Генерация видео", "wait"
    if value == "generation_started":
        return "Запуск workflow", "wait"
    if value == "generation_completed":
        return "Генерация завершена", "on"
    if value == "generation_failed":
        return "Ошибка генерации", "off"
    if value == "artifact_ready":
        return "Видео готово", "on"
    if value == "batch_created":
        return "Пакет создан", "unknown"
    if value == "publishing":
        event_kind = _publish_payload_event_kind(payload)
        if event_kind == "publishing_started":
            return "Запуск upload", "wait"
        if event_kind == "uploading_detected" or _publish_payload_phase(payload) == "uploading":
            return "Загрузка в Instagram", "wait"
        if event_kind == "publish_confirmation_wait" or _publish_payload_phase(payload) == "waiting_confirmation":
            return "Жду подтверждение", "wait"
        if event_kind == "profile_verification_scheduled" or _publish_payload_phase(payload) == "waiting_profile_verification_window":
            return "Жду окно проверки профиля", "wait"
        if event_kind == "profile_verification_started" or _publish_payload_phase(payload) == "verifying_profile":
            return "Проверяю в профиле", "wait"
        if event_kind == "profile_verification_retry":
            return "Повторно проверяю профиль", "wait"
        if event_kind == "profile_verified":
            return "Профиль подтвердил Reel", "on"
        if event_kind == "needs_review":
            return "Нужна проверка", "review"
    if value == "published" and _publish_payload_event_kind(payload) == "profile_verified":
        return "Профиль подтвердил Reel", "on"
    if value == "failed":
        reason_code = str(payload.get("reason_code") or "").strip().lower()
        if reason_code == "publish_confirmation_timeout":
            return "Таймаут публикации", "off"
        if reason_code == "publish_not_started":
            return "Upload не стартовал", "off"
    if value in PUBLISH_JOB_STATE_LABELS:
        return PUBLISH_JOB_STATE_LABELS[value], _publish_job_state_meta(value)[1]
    if value in PUBLISH_BATCH_ACCOUNT_STATE_LABELS:
        return PUBLISH_BATCH_ACCOUNT_STATE_LABELS[value], _publish_batch_account_state_meta(value)[1]
    if value in PUBLISH_BATCH_STATE_LABELS:
        return PUBLISH_BATCH_STATE_LABELS[value], _publish_batch_state_meta(value)[1]
    return value or "Событие", "unknown"


def _build_publish_dashboard_snapshot(batch_id: int) -> Optional[dict[str, Any]]:
    batch_row = db.get_publish_batch(int(batch_id))
    if batch_row is None:
        return None

    batch = _decorate_publish_batch(dict(batch_row))
    batch_stage_dir = _publish_batch_stage_path(int(batch_id))
    batch["stage_dir_exists"] = batch_stage_dir.exists()
    now_ts = int(time.time())
    batch["created_at_label"] = _format_timestamp_label(batch.get("created_at"))
    batch["updated_at_label"] = _format_timestamp_label(batch.get("updated_at"))
    batch["generation_started_at_label"] = _format_timestamp_label(batch.get("generation_started_at"))
    batch["generation_completed_at_label"] = _format_timestamp_label(batch.get("generation_completed_at"))
    batch["completed_at_label"] = _format_timestamp_label(batch.get("completed_at"))

    accounts_raw = [_decorate_publish_account(dict(raw)) for raw in db.list_publish_batch_accounts(int(batch_id))]
    reel_posts_by_job_id: dict[int, dict[str, Any]] = {}
    for raw in db.list_instagram_reel_posts_for_batch(int(batch_id)):
        post = dict(raw)
        decorated_post = _decorate_instagram_reel_post(
            post,
            snapshots=[dict(item) for item in db.list_instagram_reel_metric_snapshots(int(post["id"]))],
        )
        publish_job_id = int(decorated_post.get("publish_job_id") or 0)
        if publish_job_id > 0:
            reel_posts_by_job_id[publish_job_id] = decorated_post
    artifacts_raw: list[dict[str, Any]] = []
    for raw in db.list_publish_artifacts(int(batch_id)):
        row = dict(raw)
        size_bytes = int(row.get("size_bytes") or 0)
        row["size_label"] = f"{size_bytes / (1024 * 1024):.1f} MB" if size_bytes else "—"
        duration = row.get("duration_seconds")
        row["duration_label"] = f"{float(duration):.1f} s" if duration not in (None, "") else "—"
        row["created_at_label"] = _format_timestamp_label(row.get("created_at"))
        row["download_url"] = with_base(f"/publishing/batches/{int(batch_id)}/artifacts/{int(row['id'])}/download")
        artifacts_raw.append(row)

    jobs_raw: list[dict[str, Any]] = []
    for raw in db.list_publish_jobs(int(batch_id)):
        row = dict(raw)
        label, css = _publish_job_state_meta(row.get("state"))
        row["state_label"] = label
        row["state_class"] = css
        row["created_at_label"] = _format_timestamp_label(row.get("created_at"))
        row["started_at_label"] = _format_timestamp_label(row.get("started_at"))
        row["completed_at_label"] = _format_timestamp_label(row.get("completed_at"))
        row["identity_handle"] = _account_identity_handle({"username": row.get("account_username"), "account_login": row.get("account_login")})
        row["download_url"] = with_base(f"/publishing/batches/{int(batch_id)}/artifacts/{int(row['artifact_id'])}/download")
        jobs_raw.append(row)

    parsed_events: list[dict[str, Any]] = []
    latest_generation_progress: dict[int, dict[str, Any]] = {}
    latest_publish_progress: dict[int, dict[str, Any]] = {}
    account_events: dict[int, list[dict[str, Any]]] = {}
    for raw in db.list_publish_job_events(int(batch_id), limit=500):
        row = dict(raw)
        payload = _parse_json_object(row.get("payload_json"))
        row["payload"] = payload
        row["created_at_label"] = _format_timestamp_label(row.get("created_at"))
        row["payload_preview"] = (
            str(row.get("payload_json") or "")[:220] + ("…" if len(str(row.get("payload_json") or "")) > 220 else "")
            if str(row.get("payload_json") or "").strip()
            else ""
        )
        title, tone = _publish_recent_event_meta(str(row.get("state") or ""), payload)
        row["title"] = title
        row["tone_class"] = tone
        state_value = str(row.get("state") or "").strip().lower()
        if payload and (
            state_value == "publishing"
            or bool(_publish_payload_mail_challenge(payload))
            or bool(_publish_mail_event_meta(_publish_payload_event_kind(payload))[0])
        ):
            row["display_detail"] = _publish_phase_detail_from_payload(payload, str(row.get("detail") or "").strip())
        else:
            row["display_detail"] = str(row.get("detail") or "").strip()
        parsed_events.append(row)
        try:
            account_id = int(row.get("account_id") or 0)
        except Exception:
            account_id = 0
        if account_id > 0:
            account_events.setdefault(account_id, []).append(row)
            if str(row.get("state") or "").strip().lower() == "generation_progress" and account_id not in latest_generation_progress:
                latest_generation_progress[account_id] = row
            if (
                account_id not in latest_publish_progress
                and str(row.get("state") or "").strip().lower()
                in {"leased", "preparing", "importing_media", "opening_reel_flow", "selecting_media", "publishing", "published", "needs_review", "failed"}
                and any(
                    key in payload
                    for key in (
                        "publish_phase",
                        "accepted_by_instagram",
                        "elapsed_seconds",
                        "upload_progress_pct",
                        "last_activity",
                        "event_kind",
                        "reason_code",
                        "mail_challenge",
                    )
                )
            ):
                latest_publish_progress[account_id] = row

    latest_event_ts = max((_unix_timestamp(row.get("created_at")) for row in parsed_events), default=0)
    batch_touch_ts = max(_unix_timestamp(batch.get("updated_at")), latest_event_ts, _unix_timestamp(batch.get("created_at")))
    batch["updated_at_relative_label"] = _format_relative_age_label(batch_touch_ts, now_ts=now_ts)

    account_cards: list[dict[str, Any]] = []
    for account in accounts_raw:
        account_id = int(account["id"])
        batch_state = str(account.get("batch_state") or "").strip().lower() or "queued_for_generation"
        progress_event = latest_generation_progress.get(account_id)
        publish_event = latest_publish_progress.get(account_id)
        publish_payload = (publish_event or {}).get("payload") or {}
        history_max = _publish_account_progress_for_state(batch_state) if batch_state not in {"generation_failed", "failed", "canceled"} else 0
        for event in account_events.get(account_id, []):
            history_max = max(history_max, _publish_event_progress_for_state(str(event.get("state") or ""), event.get("payload") or {}))

        if batch_state == "generating" and progress_event is not None:
            progress_pct = _publish_generation_progress_percent((progress_event.get("payload") or {}).get("progress_pct"))
            progress_pct = _publish_live_progress_for_state(progress_pct, batch_state, updated_at=progress_event.get("created_at"))
            phase_label = str((progress_event.get("payload") or {}).get("stage_label") or "").strip() or "Генерация видео"
            phase_detail = str(progress_event.get("detail") or "").strip() or str(account.get("detail") or "").strip()
            phase_step_key = "video_production"
        elif batch_state == "queued_for_generation":
            progress_pct = 0
            phase_label = "Запуск workflow" if batch.get("generation_started_at") else "Ожидает запуска"
            phase_detail = str(account.get("detail") or "").strip() or "Аккаунт ждёт запуска генерации."
            phase_step_key = "workflow_started" if not batch.get("generation_started_at") else "video_production"
        elif batch_state == "generating":
            progress_pct = _publish_live_progress_for_state(20, batch_state, updated_at=account.get("updated_at"))
            phase_label = "Генерация видео"
            phase_detail = str(account.get("detail") or "").strip() or "Видео сейчас генерируется."
            phase_step_key = "video_production"
        elif batch_state == "queued_for_publish":
            progress_pct = _publish_live_progress_for_state(60, batch_state, updated_at=account.get("updated_at"))
            phase_label = "Видео готово"
            phase_detail = str(account.get("detail") or "").strip() or "Видео поставлено в очередь публикации."
            phase_step_key = "publish_queue"
        elif batch_state in {"leased", "preparing"}:
            progress_pct = _publish_live_progress_for_state(70, batch_state, updated_at=account.get("updated_at"))
            phase_label = _publish_phase_label_from_payload(publish_payload) or "Подготовка публикации"
            phase_detail = _publish_phase_detail_from_payload(
                publish_payload,
                str(account.get("job_detail") or account.get("detail") or "").strip() or "Runner готовит публикацию.",
            )
            phase_step_key = "instagram_publish"
        elif batch_state == "importing_media":
            progress_pct = _publish_live_progress_for_state(78, batch_state, updated_at=account.get("updated_at"))
            phase_label = "Импорт в эмулятор"
            phase_detail = str(account.get("job_detail") or account.get("detail") or "").strip() or "Видео импортируется в эмулятор."
            phase_step_key = "instagram_publish"
        elif batch_state == "opening_reel_flow":
            progress_pct = _publish_live_progress_for_state(84, batch_state, updated_at=account.get("updated_at"))
            phase_label = "Открываю Reel"
            phase_detail = str(account.get("job_detail") or account.get("detail") or "").strip() or "Открывается поток публикации Reel."
            phase_step_key = "instagram_publish"
        elif batch_state == "selecting_media":
            progress_pct = _publish_live_progress_for_state(90, batch_state, updated_at=account.get("updated_at"))
            phase_label = "Выбор видео"
            phase_detail = str(account.get("job_detail") or account.get("detail") or "").strip() or "Видео выбирается внутри Instagram."
            phase_step_key = "instagram_publish"
        elif batch_state == "publishing":
            if publish_payload:
                payload_progress = _publish_progress_for_publishing_payload(publish_payload)
                progress_pct = _publish_live_progress_for_state(
                    max(history_max, payload_progress),
                    batch_state,
                    updated_at=publish_event.get("created_at") if publish_event is not None else account.get("updated_at"),
                )
                phase_label = _publish_phase_label_from_payload(publish_payload) or "Публикация Reel"
                phase_detail = _publish_phase_detail_from_payload(
                    publish_payload,
                    str(account.get("job_detail") or account.get("detail") or "").strip() or "Instagram публикует Reel.",
                )
            else:
                progress_pct = _publish_live_progress_for_state(96, batch_state, updated_at=account.get("updated_at"))
                phase_label = "Публикация Reel"
                phase_detail = str(account.get("job_detail") or account.get("detail") or "").strip() or "Instagram публикует Reel."
            phase_step_key = "instagram_publish"
        elif batch_state == "published":
            progress_pct = 100
            phase_label = "Готово"
            phase_detail = str(account.get("job_detail") or account.get("detail") or "").strip() or "Видео опубликовано."
            phase_step_key = "done"
        elif batch_state == "needs_review":
            progress_pct = 100
            phase_label = "Нужна проверка"
            phase_detail = (
                str(account.get("job_detail") or "").strip()
                or str(account.get("instagram_publish_detail") or "").strip()
                or str(account.get("detail") or "").strip()
                or "Upload завершён, но профиль не подтвердил новый Reel."
            )
            phase_step_key = "done"
        elif batch_state == "generation_failed":
            progress_pct = max(history_max, 20)
            phase_label = "Ошибка генерации"
            phase_detail = str(account.get("detail") or "").strip() or "Видео не удалось сгенерировать."
            phase_step_key = "video_production"
        elif batch_state in {"failed", "canceled"}:
            progress_pct = max(history_max, 70 if account.get("has_job") or account.get("has_artifact") else 20)
            publish_status_value = str(account.get("instagram_publish_status") or "").strip().lower()
            if publish_status_value in {"manual_2fa_required", "email_code_required", "challenge_required", "invalid_password"}:
                phase_label = str(account.get("instagram_publish_status_label") or "").strip() or (
                    "Ошибка публикации" if batch_state == "failed" else "Отменено"
                )
            else:
                phase_label = "Ошибка публикации" if batch_state == "failed" else "Отменено"
            phase_detail = _publish_phase_detail_from_payload(
                publish_payload,
                str(account.get("job_detail") or "").strip()
                or str(account.get("mail_challenge_reason_text") or "").strip()
                or str(account.get("instagram_publish_detail") or "").strip()
                or str(account.get("detail") or "").strip()
                or ("Публикация завершилась ошибкой." if batch_state == "failed" else "Публикация была отменена."),
            )
            phase_step_key = "instagram_publish"
        else:
            progress_pct = history_max
            phase_label = account.get("batch_state_label") or "Ожидание"
            phase_detail = str(account.get("detail") or "").strip()
            phase_step_key = "workflow_started"

        account["progress_pct"] = int(max(0, min(100, progress_pct)))
        account["phase_label"] = phase_label
        account["phase_detail"] = phase_detail
        account["phase_step_key"] = phase_step_key
        account["publish_phase"] = _publish_payload_phase(publish_payload)
        account["accepted_by_instagram"] = _publish_payload_bool(publish_payload, "accepted_by_instagram")
        account["elapsed_seconds"] = _publish_payload_elapsed_seconds(publish_payload)
        account["upload_progress_pct"] = _publish_payload_upload_progress(publish_payload)
        account["last_activity"] = str(publish_payload.get("last_activity") or "").strip()
        account["artifact_download_url"] = (
            with_base(f"/publishing/batches/{int(batch_id)}/artifacts/{int(account['artifact_id'])}/download")
            if account.get("artifact_id")
            else ""
        )
        reel_post = reel_posts_by_job_id.get(int(account.get("job_id") or 0))
        if reel_post is not None:
            account["latest_reel"] = reel_post
            account["reel_metrics_summary"] = str(reel_post.get("compact_summary") or "").strip()
            account["reel_metrics_history"] = list(reel_post.get("history") or [])
        else:
            account["latest_reel"] = None
            account["reel_metrics_summary"] = ""
            account["reel_metrics_history"] = []
        account["queue_position"] = int(account.get("queue_position") or 0)
        account["open_url"] = with_base(f"/accounts/{account_id}")
        account["updated_at_label"] = _format_timestamp_label(account.get("updated_at"))
        account_touch_ts = max(
            _unix_timestamp(account.get("updated_at")),
            _unix_timestamp(progress_event.get("created_at")) if progress_event is not None else 0,
            _unix_timestamp(publish_event.get("created_at")) if publish_event is not None else 0,
        )
        account["updated_at_relative_label"] = _format_relative_age_label(account_touch_ts, now_ts=now_ts)
        account["sort_group"] = _publish_account_sort_group(batch_state)
        account["is_failed"] = batch_state in {"generation_failed", "failed", "canceled"}
        account["is_active"] = account["sort_group"] == 0
        account_cards.append(account)

    account_cards.sort(
        key=lambda item: (
            int(item.get("queue_position") or 2147483647),
            int(item.get("id") or 0),
        )
    )

    if batch_state in {"completed", "completed_needs_review", "completed_with_errors", "failed_generation", "canceled"}:
        overall_progress_pct = 100 if account_cards else 0
    elif account_cards:
        overall_progress_pct = int(round(sum(int(item.get("progress_pct") or 0) for item in account_cards) / len(account_cards)))
    else:
        overall_progress_pct = 0

    active_publish_account = next((item for item in account_cards if str(item.get("batch_state") or "") in {"leased", "preparing", "importing_media", "opening_reel_flow", "selecting_media", "publishing"}), None)
    queued_publish_account = next((item for item in account_cards if str(item.get("batch_state") or "") == "queued_for_publish"), None)
    generating_account = next((item for item in account_cards if str(item.get("batch_state") or "") == "generating"), None)
    queued_generation_account = next((item for item in account_cards if str(item.get("batch_state") or "") == "queued_for_generation"), None)

    batch_state = str(batch.get("state") or "").strip().lower()
    if batch_state == "completed":
        batch_phase_key = "done"
        batch_phase_label = "Готово"
        batch_phase_subtitle = "Все выбранные аккаунты уже опубликовали видео."
    elif batch_state == "completed_needs_review":
        batch_phase_key = "done"
        batch_phase_label = "Нужна проверка"
        batch_phase_subtitle = str(batch.get("detail") or "").strip() or "Публикация завершена, но часть аккаунтов требует ручной проверки."
    elif batch_state in {"completed_with_errors", "failed_generation"}:
        batch_phase_key = "done"
        batch_phase_label = "Готово с ошибками"
        batch_phase_subtitle = str(batch.get("detail") or "").strip() or "Часть аккаунтов завершилась с ошибками."
    elif batch_state == "canceled":
        batch_phase_key = "done"
        batch_phase_label = "Отменено"
        batch_phase_subtitle = str(batch.get("detail") or "").strip() or "Пакет публикации был отменён."
    elif active_publish_account is not None:
        batch_phase_key = "instagram_publish"
        batch_phase_label = "Публикация в Instagram"
        batch_phase_subtitle = str(active_publish_account.get("phase_label") or "").strip()
        if active_publish_account.get("phase_detail"):
            batch_phase_subtitle = f"{batch_phase_subtitle} · {active_publish_account['phase_detail']}"
    elif queued_publish_account is not None:
        batch_phase_key = "publish_queue"
        batch_phase_label = "Очередь публикации"
        batch_phase_subtitle = str(queued_publish_account.get("phase_detail") or "").strip() or "Видео готово и ждёт runner-а."
    elif generating_account is not None:
        batch_phase_key = "video_production"
        batch_phase_label = "Генерация видео"
        batch_phase_subtitle = str(generating_account.get("phase_label") or "").strip()
        if generating_account.get("phase_detail"):
            batch_phase_subtitle = f"{batch_phase_subtitle} · {generating_account['phase_detail']}"
    elif queued_generation_account is not None:
        batch_phase_key = "workflow_started"
        batch_phase_label = "Запуск workflow"
        batch_phase_subtitle = str(batch.get("detail") or "").strip() or "Ожидается старт генерации."
    else:
        batch_phase_key = "workflow_started"
        batch_phase_label = "Запуск workflow"
        batch_phase_subtitle = str(batch.get("detail") or "").strip() or "Пакет отправлен в workflow."

    step_index = {item["key"]: idx for idx, item in enumerate(PUBLISH_PROGRESS_STEPS)}
    active_step_index = step_index.get(batch_phase_key, 0)
    steps: list[dict[str, Any]] = []
    for index, item in enumerate(PUBLISH_PROGRESS_STEPS):
        status = "pending"
        if batch_state == "completed":
            status = "completed"
        elif batch_state in {"completed_needs_review", "completed_with_errors", "failed_generation", "canceled"}:
            if batch_state == "failed_generation":
                if item["key"] == "workflow_started":
                    status = "completed"
                elif item["key"] in {"video_production", "done"}:
                    status = "error"
                else:
                    status = "pending"
            elif batch_state == "completed_needs_review":
                if item["key"] == "done":
                    status = "active"
                elif item["key"] == "instagram_publish":
                    status = "completed"
                elif index < active_step_index:
                    status = "completed"
            elif item["key"] == "done":
                status = "error"
            elif batch_state == "completed_with_errors" and item["key"] == "instagram_publish":
                status = "error"
            elif index < active_step_index:
                status = "completed"
        else:
            if index < active_step_index:
                status = "completed"
            elif index == active_step_index:
                status = "active"
        steps.append({**item, "status": status})

    counts = {
        "selected": int(batch.get("accounts_total") or 0),
        "generating": int(batch.get("queued_generation_accounts") or 0) + int(batch.get("generating_accounts") or 0),
        "ready": int(batch.get("queued_publish_accounts") or 0),
        "publishing": int(batch.get("active_publish_accounts") or 0),
        "published": int(batch.get("published_accounts") or 0),
        "needs_review": int(batch.get("needs_review_accounts") or 0),
        "failed": int(batch.get("error_accounts") or 0),
    }

    recent_activity: list[dict[str, Any]] = []
    for row in parsed_events[:10]:
        recent_activity.append(
            {
                "id": int(row["id"]),
                "title": row["title"],
                "detail": str(row.get("display_detail") or "").strip(),
                "account_username": str(row.get("account_username") or "").strip(),
                "source_name": str(row.get("source_name") or "").strip(),
                "created_at_label": row["created_at_label"],
                "tone_class": row["tone_class"],
            }
        )

    raw_events: list[dict[str, Any]] = []
    for row in parsed_events[:200]:
        raw_events.append(
            {
                "id": int(row["id"]),
                "state": str(row.get("state") or "").strip(),
                "title": row["title"],
                "detail": str(row.get("display_detail") or "").strip(),
                "account_username": str(row.get("account_username") or "").strip(),
                "source_name": str(row.get("source_name") or "").strip(),
                "payload_preview": row["payload_preview"],
                "created_at_label": row["created_at_label"],
                "tone_class": row["tone_class"],
            }
        )

    return {
        "batch": {
            "id": int(batch["id"]),
            "state": batch_state,
            "state_label": batch["state_label"],
            "state_class": batch["state_class"],
            "detail": str(batch.get("detail") or "").strip(),
            "workflow_key": str(batch.get("workflow_key") or ""),
            "created_at_label": batch["created_at_label"],
            "updated_at_label": batch["updated_at_label"],
            "updated_at_relative_label": batch["updated_at_relative_label"],
            "generation_started_at_label": batch["generation_started_at_label"],
            "generation_completed_at_label": batch["generation_completed_at_label"],
            "completed_at_label": batch["completed_at_label"],
            "is_terminal": bool(batch["is_terminal"]),
            "progress_pct": overall_progress_pct,
            "phase_key": batch_phase_key,
            "phase_label": batch_phase_label,
            "phase_subtitle": batch_phase_subtitle,
            "counts": counts,
            "steps": steps,
            "stage_dir": str(batch_stage_dir),
            "stage_dir_exists": bool(batch["stage_dir_exists"]),
            "artifacts_total": int(batch.get("artifacts_total") or 0),
            "jobs_total": int(batch.get("jobs_total") or 0),
        },
        "accounts": account_cards,
        "artifacts": artifacts_raw,
        "jobs": jobs_raw,
        "recent_activity": recent_activity,
        "events": raw_events,
        "poll_interval_seconds": 0 if batch["is_terminal"] else 2,
        "progress_url": with_base(f"/api/publishing/batches/{int(batch_id)}/progress"),
    }


def _decorate_publish_account(raw: dict[str, Any]) -> dict[str, Any]:
    row = dict(raw)
    rotation_label, rotation_class = _account_rotation_state_meta(row.get("rotation_state"))
    launch_label, launch_class = _account_instagram_launch_status_meta(row.get("instagram_launch_status"))
    publish_label, publish_class = _account_instagram_publish_status_meta(row.get("instagram_publish_status"))
    mail_label, mail_class = _account_mail_status_meta(row.get("mail_status"))
    mail_ready_label, mail_ready_class, mail_ready_detail = _account_mail_ready_meta(row)
    mail_challenge_label, mail_challenge_class = _account_mail_challenge_meta(row.get("mail_challenge_status"))
    row["rotation_state_label"] = rotation_label
    row["rotation_state_class"] = rotation_class
    row["rotation_state_reason"] = db.account_rotation_display_reason(row)
    row["compact_block_reason"] = _compact_account_block_reason(row)
    row["instagram_launch_status_label"] = launch_label
    row["instagram_launch_status_class"] = launch_class
    row["instagram_publish_status_label"] = publish_label
    row["instagram_publish_status_class"] = publish_class
    row["mail_enabled"] = db.account_mail_automation_ready(row)
    row["mail_status_label"] = mail_label
    row["mail_status_class"] = mail_class
    row["mail_provider_label"] = _account_mail_provider_label(row.get("mail_provider"))
    row["mail_ready_label"] = mail_ready_label
    row["mail_ready_class"] = mail_ready_class
    row["mail_ready_detail"] = mail_ready_detail
    row["mail_challenge_label"] = mail_challenge_label
    row["mail_challenge_class"] = mail_challenge_class
    row["mail_challenge_kind_label"] = ACCOUNT_MAIL_CHALLENGE_KIND_LABELS.get(str(row.get("mail_challenge_kind") or "").strip(), "")
    owner_name = str(row.get("owner_worker_name") or "").strip()
    owner_username = str(row.get("owner_worker_username") or "").strip()
    row["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
    row["identity_handle"] = _account_identity_handle(row)
    row["publish_warnings"] = db.publish_account_automation_warnings(row)
    row["twofa_ready"] = db.account_twofa_automation_ready(row)
    batch_state = str(row.get("state") or "").strip().lower()
    if batch_state:
        batch_label, batch_class = _publish_batch_account_state_meta(batch_state)
        row["batch_state"] = batch_state
        row["batch_state_label"] = batch_label
        row["batch_state_class"] = batch_class
        row["batch_state_terminal"] = batch_state in {"generation_failed", "published", "needs_review", "failed", "canceled"}
    else:
        row["batch_state"] = ""
        row["batch_state_label"] = ""
        row["batch_state_class"] = "unknown"
        row["batch_state_terminal"] = False
    row["is_published"] = row["batch_state"] == "published" or str(row.get("instagram_publish_status") or "").strip().lower() == "published"
    row["has_artifact"] = bool(row.get("artifact_id"))
    row["has_job"] = bool(row.get("job_id"))
    job_state = str(row.get("job_state") or "").strip().lower()
    if job_state:
        job_label, job_class = _publish_job_state_meta(job_state)
        row["job_state_label"] = job_label
        row["job_state_class"] = job_class
    else:
        row["job_state_label"] = ""
        row["job_state_class"] = "unknown"
    return row


def _publish_account_selection_blockers(account: dict[str, Any]) -> list[str]:
    blockers = list(db.publish_account_readiness_issues(account))
    rotation_reason = str(account.get("rotation_state_reason") or "").strip()
    if rotation_reason and rotation_reason not in blockers:
        blockers.append(rotation_reason)
    if blockers:
        return blockers

    launch_status = str(account.get("instagram_launch_status") or "idle").strip().lower()
    publish_status = str(account.get("instagram_publish_status") or "idle").strip().lower()
    audit_status = str(account.get("latest_audit_resolution_state") or "").strip().lower()
    if launch_status == "login_submitted" or audit_status == "login_ok" or publish_status in {"published", "needs_review"}:
        return []
    return ["Нет подтверждённой проверки входа Instagram. Сначала запусти Instagram audit или live-login."]


def _publish_account_selection_context(limit: int = 500) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    _ensure_publish_account_serials(limit=limit)
    db.sync_instagram_auto_rotation_states(limit=max(limit, 1000))
    ready_accounts: list[dict[str, Any]] = []
    blocked_accounts: list[dict[str, Any]] = []
    rows = [_decorate_publish_account(dict(raw)) for raw in db.list_publish_ready_accounts(limit=limit)]
    rows.extend(_decorate_publish_account(dict(raw)) for raw in db.list_publish_blocked_accounts(limit=limit))
    for row in rows:
        blockers = _publish_account_selection_blockers(row)
        if blockers:
            row["publish_blockers"] = blockers
            compact_reason = str(row.get("compact_block_reason") or "").strip()
            if not compact_reason:
                compact_source = dict(row)
                compact_source["rotation_state_reason"] = " ".join(str(item) for item in blockers if item).strip()
                compact_reason = _compact_account_block_reason(compact_source)
            row["compact_block_reason"] = compact_reason or "Нерабочий"
            blocked_accounts.append(row)
        else:
            ready_accounts.append(row)
    return ready_accounts, blocked_accounts


def _selected_publish_accounts(account_ids: list[int], *, limit: int = 500) -> tuple[list[dict[str, Any]], list[int]]:
    _ensure_publish_account_serials(account_ids=account_ids, limit=limit)
    accounts, _ = _publish_account_selection_context(limit=limit)
    by_id = {int(row["id"]): row for row in accounts}
    selected: list[dict[str, Any]] = []
    missing: list[int] = []
    seen: set[int] = set()
    for raw_id in account_ids:
        account_id = int(raw_id)
        if account_id in seen:
            continue
        seen.add(account_id)
        row = by_id.get(account_id)
        if row is None:
            missing.append(account_id)
            continue
        selected.append(row)
    return selected, missing


def _parse_ordered_publish_account_ids(raw_values: Optional[list[str]]) -> list[int]:
    ordered_ids: list[int] = []
    seen_ids: set[int] = set()
    for raw in raw_values or []:
        value = (raw or "").strip()
        if not value:
            continue
        try:
            account_id = int(value)
        except Exception as exc:
            raise ValueError("invalid account id") from exc
        if account_id <= 0 or account_id in seen_ids:
            continue
        seen_ids.add(account_id)
        ordered_ids.append(account_id)
    return ordered_ids


def _publishing_page_response(
    request: Request,
    *,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    accounts, blocked_accounts = _publish_account_selection_context(limit=500)
    batches = [_decorate_publish_batch(dict(raw)) for raw in db.list_publish_batches(limit=20)]
    return templates.TemplateResponse(
        "publishing.html",
        {
            "request": request,
            "batches": batches,
            "error": error,
            "success": success,
            "n8n_webhook_configured": bool(PUBLISH_N8N_WEBHOOK_URL),
            "staging_root": str(_publish_staging_root()),
            "publish_workflow_key": PUBLISH_DEFAULT_WORKFLOW,
            "ready_accounts_count": len(accounts),
            "warning_accounts_count": sum(1 for account in accounts if account["publish_warnings"]),
            "blocked_accounts_count": len(blocked_accounts),
        },
        status_code=status_code,
    )


def _publishing_start_page_response(
    request: Request,
    *,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
    selected_ids: Optional[set[int]] = None,
) -> HTMLResponse:
    accounts, blocked_accounts = _publish_account_selection_context(limit=500)
    selected = selected_ids or set()
    for row in accounts:
        row["selected"] = int(row["id"]) in selected
    selected_count = sum(1 for row in accounts if row["selected"])
    return templates.TemplateResponse(
        "publishing_start.html",
        {
            "request": request,
            "accounts": accounts,
            "blocked_accounts": blocked_accounts,
            "error": error,
            "success": success,
            "n8n_webhook_configured": bool(PUBLISH_N8N_WEBHOOK_URL),
            "staging_root": str(_publish_staging_root()),
            "publish_workflow_key": PUBLISH_DEFAULT_WORKFLOW,
            "ready_accounts_count": len(accounts),
            "selected_accounts_count": selected_count,
            "blocked_accounts_count": len(blocked_accounts),
        },
        status_code=status_code,
    )


def _publishing_confirm_page_response(
    request: Request,
    *,
    account_ids: list[int],
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    accounts, missing = _selected_publish_accounts(account_ids, limit=500)
    if missing:
        return _publishing_start_page_response(
            request,
            error=f"Некоторые аккаунты больше недоступны для запуска: {', '.join(str(item) for item in missing)}.",
            status_code=400,
            selected_ids=set(account_ids),
        )
    return templates.TemplateResponse(
        "publishing_confirm.html",
        {
            "request": request,
            "accounts": accounts,
            "account_ids": [int(row["id"]) for row in accounts],
            "accounts_total": len(accounts),
            "warning_accounts_count": sum(1 for row in accounts if row["publish_warnings"]),
            "error": error,
            "success": success,
            "n8n_webhook_configured": bool(PUBLISH_N8N_WEBHOOK_URL),
            "staging_root": str(_publish_staging_root()),
            "publish_workflow_key": PUBLISH_DEFAULT_WORKFLOW,
        },
        status_code=status_code,
    )


def _publishing_batch_detail_page_response(
    request: Request,
    *,
    batch_id: int,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    snapshot = _build_publish_dashboard_snapshot(int(batch_id))
    if snapshot is None:
        return templates.TemplateResponse(
            "publishing_batch_detail.html",
            {
                "request": request,
                "batch": None,
                "dashboard_snapshot": None,
                "dashboard_snapshot_json": "{}",
                "error": error or "Пакет не найден",
                "success": success,
                "poll_interval_seconds": 0,
                "batch_stage_dir": str(_publish_batch_stage_path(int(batch_id))),
            },
            status_code=404,
        )

    return templates.TemplateResponse(
        "publishing_batch_detail.html",
        {
            "request": request,
            "batch": snapshot["batch"],
            "dashboard_snapshot": snapshot,
            "dashboard_snapshot_json": json.dumps(snapshot, ensure_ascii=False),
            "error": error,
            "success": success,
            "poll_interval_seconds": int(snapshot["poll_interval_seconds"]),
            "batch_stage_dir": str(snapshot["batch"]["stage_dir"]),
        },
        status_code=status_code,
    )


def _build_publish_generation_payload(
    batch_id: int,
    *,
    callback_url: str,
    internal_callback_url: str,
    account_id: int,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    if not PUBLISH_N8N_WEBHOOK_URL:
        raise RuntimeError("PUBLISH_N8N_WEBHOOK_URL не настроен.")
    batch_row = db.get_publish_batch(int(batch_id))
    if batch_row is None:
        raise RuntimeError("Пакет не найден.")
    target_account: dict[str, Any] | None = None
    for row in db.list_publish_batch_accounts(int(batch_id)):
        if int(row["id"]) != int(account_id):
            continue
        target_account = {
            "account_id": int(row["id"]),
            "username": str(row["username"] or ""),
            "account_login": str(row["account_login"] or ""),
            "emulator_serial": str(row["instagram_emulator_serial"] or ""),
            "queue_position": int(row["queue_position"] or 0),
        }
        break
    if target_account is None:
        raise RuntimeError("Аккаунт для generation не найден в batch.")
    batch_dir = _publish_batch_stage_dir(int(batch_id))
    payload = {
        "event": "start_generation",
        "batch_id": int(batch_id),
        "workflow_key": str(batch_row["workflow_key"] or PUBLISH_DEFAULT_WORKFLOW),
        "callback_url": callback_url,
        "internal_callback_url": internal_callback_url,
        "progress_callback_url": internal_callback_url or callback_url,
        "shared_secret": PUBLISH_SHARED_SECRET,
        "factory_timeout_seconds": max(30, int(PUBLISH_FACTORY_TIMEOUT_SECONDS or 0)),
        "staging_dir": str(batch_dir),
        "generator_defaults": {
            "topic": "отношения",
            "style": "милый + дерзкий",
            "messagesCount": 10,
            "dry_run": False,
            "simulate_video_fail": False,
            "async": False,
        },
        "accounts": [target_account],
    }
    return payload, batch_dir, target_account


def _post_publish_generation_payload(
    batch_id: int,
    payload: dict[str, Any],
    batch_dir: Path,
    *,
    account: dict[str, Any],
) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    response = http_utils.request_with_retry(
        "POST",
        PUBLISH_N8N_WEBHOOK_URL,
        data=body,
        headers=_signed_publish_headers(body),
        timeout=25,
        allow_retry=False,
        log_context="n8n_publish_start",
    )
    response.raise_for_status()
    response_note = (response.text or "").strip()
    account_handle = f"@{str(account.get('username') or '').strip()}" if str(account.get("username") or "").strip() else f"account_id={int(account['account_id'])}"
    detail = f"n8n принял generation для {account_handle}. Папка batch: {batch_dir}."
    if response_note:
        detail = f"{detail} Ответ: {response_note[:140]}"
    db.mark_publish_generation_started(int(batch_id), detail=detail, account_id=int(account["account_id"]))


def _publish_batch_can_start_next_generation(batch_row: dict[str, Any]) -> bool:
    state = str(batch_row.get("state") or "").strip().lower()
    if state in {"completed", "completed_needs_review", "completed_with_errors", "failed_generation", "canceled"}:
        return False
    if int(batch_row.get("queued_generation_accounts") or 0) <= 0:
        return False
    if int(batch_row.get("generating_accounts") or 0) > 0:
        return False
    if int(batch_row.get("queued_publish_accounts") or 0) > 0:
        return False
    if int(batch_row.get("active_publish_accounts") or 0) > 0:
        return False
    if int(batch_row.get("queued_jobs") or 0) > 0:
        return False
    if int(batch_row.get("leased_jobs") or 0) > 0:
        return False
    if int(batch_row.get("running_jobs") or 0) > 0:
        return False
    return True


def _advance_publish_batch_runtime(batch_id: int, *, worker_name: str, heartbeat: RuntimeHeartbeat) -> bool:
    batch_row = db.get_publish_batch(int(batch_id))
    if batch_row is None:
        raise RuntimeError("Пакет не найден.")
    batch = dict(batch_row)
    if not _publish_batch_can_start_next_generation(batch):
        return False
    next_account = db.get_publish_next_generation_account(int(batch_id))
    if next_account is None:
        return False
    heartbeat()
    queue_position = int(next_account["queue_position"]) + 1
    account_handle = _account_identity_handle({"username": next_account["username"], "account_login": next_account["account_login"]})
    db.mark_publish_batch_worker_started(
        int(batch_id),
        f"Runtime worker {worker_name} запускает generation для #{queue_position} {account_handle}.",
    )
    _trigger_publish_generation_runtime(int(batch_id), account_id=int(next_account["account_id"]))
    return True


def _trigger_publish_generation(request: Request, batch_id: int, *, account_id: int) -> None:
    callback_url = _absolute_admin_url(request, "/api/internal/publishing/n8n")
    internal_callback_url = _publish_internal_callback_url("/api/internal/publishing/n8n")
    payload, batch_dir, target_account = _build_publish_generation_payload(
        int(batch_id),
        callback_url=callback_url,
        internal_callback_url=internal_callback_url,
        account_id=int(account_id),
    )
    _post_publish_generation_payload(int(batch_id), payload, batch_dir, account=target_account)


def _trigger_publish_generation_runtime(batch_id: int, *, account_id: int) -> None:
    callback_url = _absolute_runtime_admin_url("/api/internal/publishing/n8n")
    internal_callback_url = _publish_internal_callback_url("/api/internal/publishing/n8n")
    payload, batch_dir, target_account = _build_publish_generation_payload(
        int(batch_id),
        callback_url=callback_url,
        internal_callback_url=internal_callback_url,
        account_id=int(account_id),
    )
    _post_publish_generation_payload(int(batch_id), payload, batch_dir, account=target_account)


def _resolve_publish_callback_account_id(batch_id: int, event: str, account_id: Optional[int]) -> Optional[int]:
    if account_id is not None:
        return int(account_id)
    if event not in {"generation_started", "generation_progress", "generation_failed", "artifact_ready"}:
        return None
    batch_accounts = [dict(row) for row in db.list_publish_batch_accounts(int(batch_id))]
    generating_accounts = [
        int(row["account_id"])
        for row in batch_accounts
        if str(row.get("state") or "").strip().lower() == "generating"
    ]
    unique_ids = sorted(set(generating_accounts))
    if len(unique_ids) == 1:
        return unique_ids[0]
    if event == "generation_started":
        queued_candidates = [
            int(row["account_id"])
            for row in batch_accounts
            if str(row.get("state") or "").strip().lower() in {"queued_for_generation", "generating"}
        ]
        queued_unique_ids = sorted(set(queued_candidates))
        if len(queued_unique_ids) == 1:
            return queued_unique_ids[0]
    raise HTTPException(status_code=400, detail=f"account_id is required for {event}")


def _parse_broadcast_filters(
    scope: str,
    stage_key: Optional[str],
    stage_mode: Optional[str],
):
    scope_clean = (scope or "all").strip()
    stage_key_clean = (stage_key or "").strip() or None
    stage_mode_clean = (stage_mode or "reached").strip().lower()
    if stage_mode_clean not in ("reached", "exact"):
        stage_mode_clean = "reached"

    installed: Optional[int]
    if scope_clean == "installed":
        installed = 1
    elif scope_clean == "not_installed":
        installed = 0
    else:
        installed = None

    return scope_clean, installed, stage_key_clean, stage_mode_clean


def _prepare_broadcast_media(media_kind: Optional[str], media_file: Optional[UploadFile]) -> tuple[Optional[dict], Optional[str]]:
    if media_file is None or not getattr(media_file, "filename", ""):
        return None, None

    filename = (media_file.filename or "media").strip() or "media"
    raw = media_file.file.read()
    if not raw:
        return None, "Файл пустой."
    if len(raw) > MAX_BROADCAST_MEDIA_BYTES:
        mb = MAX_BROADCAST_MEDIA_BYTES // (1024 * 1024)
        return None, f"Файл слишком большой. Максимум {mb} MB."

    hint = (media_kind or "").strip().lower()
    ctype = (media_file.content_type or "").lower()
    ext = os.path.splitext(filename.lower())[1]
    photo_ext = {".jpg", ".jpeg", ".png", ".webp", ".gif"}
    video_ext = {".mp4", ".mov", ".m4v", ".webm", ".mkv"}

    kind: Optional[str] = None
    if hint in ("photo", "video"):
        kind = hint
    elif ctype.startswith("image/") or ext in photo_ext:
        kind = "photo"
    elif ctype.startswith("video/") or ext in video_ext:
        kind = "video"

    if kind not in ("photo", "video"):
        return None, "Поддерживаются только фото и видео."

    return {
        "kind": kind,
        "filename": filename,
        "content_type": ctype or "application/octet-stream",
        "content": raw,
    }, None


def _send_message(user_id: int, text: str, media: Optional[dict] = None) -> bool:
    if not BOT_TOKEN:
        return False

    text_clean = (text or "").strip()
    try:
        if media:
            kind = media.get("kind")
            method = "sendPhoto" if kind == "photo" else "sendVideo"
            field = "photo" if kind == "photo" else "video"
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"
            data = {"chat_id": user_id}
            if text_clean:
                data["caption"] = text_clean
                data["parse_mode"] = "HTML"
            files = {
                field: (
                    media.get("filename") or "media",
                    media.get("content") or b"",
                    media.get("content_type") or "application/octet-stream",
                )
            }
            resp = http_utils.request_with_retry(
                "POST",
                url,
                data=data,
                files=files,
                timeout=40,
                allow_retry=False,
                log_context="telegram_send_media",
            )
        else:
            if not text_clean:
                return False
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            resp = http_utils.request_with_retry(
                "POST",
                url,
                data={"chat_id": user_id, "text": text_clean, "parse_mode": "HTML"},
                timeout=10,
                allow_retry=False,
                log_context="telegram_send_message",
            )
        return resp.status_code == 200
    except Exception:
        return False


def _broadcast_page_response(
    request: Request,
    *,
    sent: Optional[int],
    failed: Optional[int],
    filters: dict,
    recipients: int,
    test_chat_id: str,
    message: str,
    mode: Optional[str] = None,
    error: Optional[str] = None,
    media_kind: str = "",
) -> HTMLResponse:
    step_options = db.admin_funnel_step_options()
    history = [dict(r) for r in db.list_broadcast_runs(limit=12)]
    return templates.TemplateResponse(
        "broadcast.html",
        {
            "request": request,
            "sent": sent,
            "failed": failed,
            "step_options": step_options,
            "filters": filters,
            "recipients": int(recipients or 0),
            "history": history,
            "test_chat_id": str(test_chat_id or ""),
            "message": message or "",
            "mode": mode,
            "error": error,
            "media_kind": media_kind or "",
        },
    )


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    if request.session.get("admin"):
        return _redirect("/", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": None},
    )


@app.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    if secrets.compare_digest(username, ADMIN_USER) and secrets.compare_digest(password, ADMIN_PASS):
        request.session["admin"] = True
        return _redirect("/", status_code=303)
    return templates.TemplateResponse(
        "login.html",
        {"request": request, "error": "Неверный логин или пароль"},
        status_code=401,
    )


@app.get("/logout")
def logout(request: Request):
    request.session.pop("admin", None)
    return _redirect("/login", status_code=303)


@app.get("/", response_class=HTMLResponse)
def index(request: Request, _: None = Depends(require_auth)):
    funnel = db.admin_funnel_overview()
    workers_overview = db.workers_overview()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "funnel": funnel,
            "workers_overview": workers_overview,
        },
    )


@app.get("/api/stats/funnel")
def funnel_stats_api(_: None = Depends(require_auth)):
    return JSONResponse(db.admin_funnel_overview())


@app.get("/users", response_class=HTMLResponse)
def users_page(
    request: Request,
    q: Optional[str] = None,
    step: Optional[str] = None,
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    step_key = (step or "").strip() or None
    users = db.list_users_with_funnel_progress(q=query, reached_step=step_key, limit=300)
    step_options = db.admin_funnel_step_options()
    return templates.TemplateResponse(
        "users.html",
        {
            "request": request,
            "users": users,
            "q": query,
            "step": step_key or "",
            "step_options": step_options,
        },
    )


@app.get("/accounts", response_class=HTMLResponse)
def accounts_page(
    request: Request,
    q: Optional[str] = None,
    type: Optional[str] = None,
    worker: Optional[str] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    sort: Optional[str] = None,
    created: Optional[str] = None,
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    worker_filter = (worker or "").strip()
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    try:
        account_type = _normalize_account_type(type) or ""
        rotation_state_value = _normalize_rotation_state_filter(rotation_state)
        views_state_value = _normalize_views_state_filter(views_state)
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type="",
            worker_filter=worker_filter,
            sort="recent",
            error="Неверный фильтр аккаунтов",
            status_code=400,
        )
    return _accounts_page_response(
        request,
        q=query,
        account_type=account_type,
        worker_filter=worker_filter,
        rotation_state=rotation_state_value,
        views_state=views_state_value,
        sort=sort_value,
        success="Аккаунт добавлен. Ссылка создана автоматически." if created_flag else None,
    )


@app.post("/accounts/instagram/audits", response_class=HTMLResponse)
def instagram_audit_create(
    request: Request,
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    filter_rotation_state: Optional[str] = Form(None),
    filter_views_state: Optional[str] = Form(None),
    filter_sort: Optional[str] = Form(None),
    account_ids: Optional[list[str]] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    worker_filter = (filter_worker or "").strip()
    filter_sort_raw = (filter_sort or "").strip()
    try:
        requested_type = _normalize_account_type(filter_type) if filter_type else ""
        worker_filter_value, worker_filter_id, unassigned_only = _worker_filter_meta(worker_filter)
        rotation_state_value = _normalize_rotation_state_filter(filter_rotation_state)
        views_state_value = _normalize_views_state_filter(filter_views_state)
        sort_value = _normalize_account_list_sort(filter_sort_raw)
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type="instagram",
            worker_filter=worker_filter,
            sort="recent",
            error="Неверные фильтры аудита аккаунтов.",
            status_code=400,
        )
    if requested_type and requested_type != "instagram":
        return _accounts_page_response(
            request,
            q=query,
            account_type=requested_type,
            worker_filter=worker_filter,
            rotation_state=rotation_state_value,
            views_state=views_state_value,
            sort=sort_value,
            error="Массовый аудит доступен только для Instagram-аккаунтов.",
            status_code=400,
        )
    try:
        inventory = _fetch_helper_emulator_inventory()
    except Exception as exc:
        return _accounts_page_response(
            request,
            q=query,
            account_type="instagram",
            worker_filter=worker_filter_value,
            rotation_state=rotation_state_value,
            views_state=views_state_value,
            sort=sort_value,
            error=f"Helper недоступен: {exc}",
            status_code=503,
        )

    selected_ids: set[int] = set()
    for raw in account_ids or []:
        value = (raw or "").strip()
        if not value:
            continue
        try:
            selected_ids.add(int(value))
        except Exception:
            return _accounts_page_response(
                request,
                q=query,
                account_type="instagram",
                worker_filter=worker_filter_value,
                rotation_state=rotation_state_value,
                views_state=views_state_value,
                sort=sort_value,
                error="Неверный account_id в запросе аудита.",
                status_code=400,
            )

    account_rows = [dict(row) for row in db.list_accounts(
        q=query,
        account_type="instagram",
        owner_worker_id=worker_filter_id,
        rotation_state=rotation_state_value or None,
        views_state=views_state_value or None,
        limit=500,
    )]
    if unassigned_only:
        account_rows = [row for row in account_rows if row.get("owner_worker_id") is None]
    if selected_ids:
        account_rows = [row for row in account_rows if int(row["id"]) in selected_ids]
    if not account_rows:
        return _accounts_page_response(
            request,
            q=query,
            account_type="instagram",
            worker_filter=worker_filter_value,
            rotation_state=rotation_state_value,
            views_state=views_state_value,
            sort=sort_value,
            error="Для аудита не найдено ни одного Instagram-аккаунта.",
            status_code=400,
        )

    prepared_items = _prepare_instagram_audit_items(account_rows, available_serials=_helper_inventory_available_serials(inventory))
    created = db.create_instagram_audit_batch(prepared_items, created_by_admin=ADMIN_USER)
    batch_id = int(created["batch_id"])
    if any(str(item.get("item_state") or "") == "queued" for item in prepared_items):
        _enqueue_instagram_audit_batch(batch_id)
    return _redirect(f"/accounts/instagram/audits/{batch_id}", status_code=HTTP_303_SEE_OTHER)


@app.get("/accounts/instagram/audits/{audit_id}", response_class=HTMLResponse)
def instagram_audit_detail_page(
    request: Request,
    audit_id: int,
    _: None = Depends(require_auth),
):
    return _instagram_audit_detail_page_response(request, audit_id=int(audit_id))


@app.get("/api/accounts/instagram/audits/{audit_id}/progress")
def instagram_audit_progress_api(
    audit_id: int,
    _: None = Depends(require_auth),
):
    snapshot = _build_instagram_audit_snapshot(int(audit_id))
    if snapshot is None:
        raise HTTPException(status_code=404, detail="audit not found")
    return JSONResponse(snapshot)


@app.get("/accounts/{account_id}", response_class=HTMLResponse)
def account_detail_page(
    request: Request,
    account_id: int,
    created: Optional[str] = None,
    return_to: Optional[str] = None,
    _: None = Depends(require_auth),
):
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    return _account_detail_page_response(
        request,
        account_id=int(account_id),
        return_to=return_to,
        success="Аккаунт добавлен. Ссылка создана автоматически." if created_flag else None,
    )


@app.post("/accounts/{account_id}/rotation-state")
def account_rotation_state_update(
    request: Request,
    account_id: int,
    rotation_state: str = Form(...),
    next_url: Optional[str] = Form(None),
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    fallback_url = _build_detail_url(f"/accounts/{int(account_id)}", return_to)
    account = db.get_account(int(account_id))
    if account is None:
        raise HTTPException(status_code=404, detail="Not Found")

    try:
        rotation_state_value = _normalize_rotation_state_filter(rotation_state) or "review"
    except ValueError:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Неверный статус аккаунта",
            status_code=400,
        )

    db.update_account_rotation_state(int(account_id), rotation_state_value)
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.get("/publishing", response_class=HTMLResponse)
def publishing_page(request: Request, _: None = Depends(require_auth)):
    return _publishing_page_response(request)


@app.get("/publishing/start", response_class=HTMLResponse)
def publishing_start_page(request: Request, _: None = Depends(require_auth)):
    return _publishing_start_page_response(request)


@app.post("/publishing/prepare", response_class=HTMLResponse)
def publishing_prepare_page(
    request: Request,
    account_ids: Optional[list[str]] = Form(None),
    _: None = Depends(require_auth),
):
    try:
        selected_ids = _parse_ordered_publish_account_ids(account_ids)
    except ValueError:
        return _publishing_start_page_response(request, error="Неверный account_id в списке.", status_code=400)
    if not selected_ids:
        return _publishing_start_page_response(request, error="Выбери хотя бы один Instagram-аккаунт.", status_code=400)
    return _publishing_confirm_page_response(request, account_ids=selected_ids)


@app.post("/publishing/batches", response_class=HTMLResponse)
def publishing_batch_create(
    request: Request,
    account_ids: Optional[list[str]] = Form(None),
    launch_mode: str = Form("generated"),
    _: None = Depends(require_auth),
):
    launch_mode_value = (launch_mode or "generated").strip().lower() or "generated"
    if launch_mode_value not in {"generated", "existing_video"}:
        try:
            fallback_ids = set(_parse_ordered_publish_account_ids(account_ids))
        except ValueError:
            fallback_ids = set()
        return _publishing_start_page_response(
            request,
            error="Неизвестный режим запуска публикации.",
            status_code=400,
            selected_ids=fallback_ids if fallback_ids else None,
        )

    if launch_mode_value == "generated" and not PUBLISH_N8N_WEBHOOK_URL:
        try:
            fallback_ids = set(_parse_ordered_publish_account_ids(account_ids))
        except ValueError:
            fallback_ids = set()
        return _publishing_start_page_response(
            request,
            error="PUBLISH_N8N_WEBHOOK_URL не настроен. Fully-auto запуск сейчас недоступен.",
            status_code=503,
            selected_ids=fallback_ids if fallback_ids else None,
        )

    try:
        selected_ids = _parse_ordered_publish_account_ids(account_ids)
    except ValueError:
        return _publishing_start_page_response(request, error="Неверный account_id в batch.", status_code=400)
    if not selected_ids:
        return _publishing_start_page_response(request, error="Выбери хотя бы один Instagram-аккаунт для batch.", status_code=400)

    selected_accounts, missing = _selected_publish_accounts(selected_ids, limit=500)
    if missing:
        return _publishing_start_page_response(
            request,
            error=f"Некоторые аккаунты больше недоступны для запуска: {', '.join(str(item) for item in missing)}.",
            status_code=400,
            selected_ids=set(selected_ids),
        )

    try:
        created = db.create_publish_batch(
            selected_ids,
            created_by_admin=ADMIN_USER,
            workflow_key=PUBLISH_DEFAULT_WORKFLOW,
        )
    except ValueError as exc:
        return _publishing_confirm_page_response(
            request,
            account_ids=[int(row["id"]) for row in selected_accounts],
            error=str(exc),
            status_code=400,
        )

    batch_id = int(created["batch_id"])
    if launch_mode_value == "existing_video":
        db.update_publish_batch_state(
            batch_id,
            "queued_to_worker",
            detail="Batch создан для публикации готового видео. Жду загрузку mp4 в staging.",
        )
    else:
        try:
            _enqueue_publish_batch_start(batch_id)
        except Exception as exc:
            db.mark_publish_generation_failed(batch_id, f"Не удалось поставить batch в runtime queue: {exc}")
            return _publishing_batch_detail_page_response(
                request,
                batch_id=batch_id,
                error=f"Пакет создан, но runtime worker не поставил запуск в очередь: {exc}",
                status_code=502,
            )
    return _redirect(f"/publishing/batches/{batch_id}", status_code=HTTP_303_SEE_OTHER)


@app.get("/publishing/batches/{batch_id}", response_class=HTMLResponse)
def publishing_batch_detail_page(
    request: Request,
    batch_id: int,
    _: None = Depends(require_auth),
):
    return _publishing_batch_detail_page_response(request, batch_id=int(batch_id))


@app.get("/api/publishing/batches/{batch_id}/progress")
def publishing_batch_progress_api(
    batch_id: int,
    _: None = Depends(require_auth),
):
    _run_publish_generation_watchdog(int(batch_id))
    snapshot = _build_publish_dashboard_snapshot(int(batch_id))
    if snapshot is None:
        raise HTTPException(status_code=404, detail="batch not found")
    return JSONResponse(snapshot)


@app.get("/publishing/batches/{batch_id}/artifacts/{artifact_id}/download")
def publishing_batch_artifact_download(
    batch_id: int,
    artifact_id: int,
    _: None = Depends(require_auth),
):
    artifact = db.get_publish_artifact(int(batch_id), int(artifact_id))
    if artifact is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    try:
        source_path = _normalize_publish_artifact_path(int(batch_id), str(artifact["path"] or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not source_path.exists() or not source_path.is_file():
        raise HTTPException(status_code=404, detail="artifact file not found")
    filename = str(artifact["filename"] or source_path.name).strip() or source_path.name
    return FileResponse(path=source_path, filename=filename, media_type="application/octet-stream")


@app.post("/api/publishing/batches/{batch_id}/artifacts/upload")
def publishing_batch_artifact_upload(
    batch_id: int,
    media_file: UploadFile = File(...),
    account_id: Optional[int] = Form(None),
    _: None = Depends(require_auth),
):
    if db.get_publish_batch(int(batch_id)) is None:
        raise HTTPException(status_code=404, detail="batch not found")

    try:
        target_path, size_bytes = _store_publish_batch_upload(int(batch_id), media_file)
        result = db.register_publish_artifact(
            int(batch_id),
            path=str(target_path),
            filename=target_path.name,
            checksum=_file_sha256(target_path),
            size_bytes=size_bytes,
            duration_seconds=None,
            account_id=int(account_id) if account_id is not None else None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"upload failed: {exc}") from exc

    return JSONResponse(
        {
            "ok": True,
            "batch_id": int(batch_id),
            "account_id": int(account_id) if account_id is not None else None,
            "path": str(target_path),
            "filename": target_path.name,
            "size_bytes": size_bytes,
            **result,
        }
    )


@app.post("/accounts/{account_id}/launch/instagram")
def account_launch_instagram(
    request: Request,
    account_id: int,
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    account = db.get_account(int(account_id))
    if account is None:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )
    if not HELPER_API_KEY:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="HELPER_API_KEY не настроен. Автовход пока недоступен.",
            status_code=503,
        )
    if str(account["type"] or "").strip().lower() != "instagram":
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Автовход доступен только для Instagram.",
            status_code=400,
        )
    if not str(account["account_login"] or "").strip() or not str(account["account_password"] or "").strip():
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Для автовхода нужны логин и пароль аккаунта.",
            status_code=400,
        )

    created = db.create_helper_launch_ticket(
        account_id=int(account_id),
        target="instagram_app_login",
        created_by_admin=ADMIN_USER,
        ttl_seconds=HELPER_TICKET_TTL_SECONDS,
    )
    return RedirectResponse(url=_build_instagram_helper_open_url(str(created["ticket"])), status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/{account_id}/publish/latest-reel")
def account_publish_latest_reel(
    request: Request,
    account_id: int,
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    account = db.get_account(int(account_id))
    if account is None:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )
    if not HELPER_API_KEY:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="HELPER_API_KEY не настроен. Публикация пока недоступна.",
            status_code=503,
        )
    if str(account["type"] or "").strip().lower() != "instagram":
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Автопубликация доступна только для Instagram.",
            status_code=400,
        )
    if not str(account["account_login"] or "").strip() or not str(account["account_password"] or "").strip():
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Для публикации нужны логин и пароль аккаунта.",
            status_code=400,
        )

    db.update_account_instagram_publish_state(
        int(account_id),
        "preparing",
        "Запуск публикации поставлен в очередь. Helper проверит локальную папку и откроет Reel flow.",
        last_file=str(account["instagram_publish_last_file"] or ""),
    )
    created = db.create_helper_launch_ticket(
        account_id=int(account_id),
        target="instagram_publish_latest_reel",
        created_by_admin=ADMIN_USER,
        ttl_seconds=HELPER_TICKET_TTL_SECONDS,
    )
    return RedirectResponse(url=_build_instagram_helper_open_url(str(created["ticket"])), status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/{account_id}/mail/check", response_class=HTMLResponse)
def account_mail_check(
    request: Request,
    account_id: int,
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    account = db.get_account(int(account_id))
    if account is None:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )

    account_dict = dict(account)
    if not _account_has_mail_credentials(account_dict):
        _, _, readiness_detail = _account_mail_ready_meta(account_dict)
        db.update_account_mail_state(
            int(account_id),
            mail_provider=str(account["mail_provider"] or "auto"),
            mail_status="auth_error",
            mail_last_error=readiness_detail or "Почта аккаунта не настроена.",
        )
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error=readiness_detail or "Заполни почтовые реквизиты перед проверкой.",
            status_code=400,
        )

    result = _sync_account_mailbox(
        account_dict,
        force_refresh=True,
        renew_watch=True,
        reason="admin_mail_check",
    )

    status_value = str(result.get("status") or "")
    if status_value == "ok":
        messages = list(result.get("messages") or [])
        latest_message = messages[0] if messages else None
        latest_label = ""
        if latest_message and latest_message.get("received_at"):
            latest_label = f" Последнее письмо: {datetime.fromtimestamp(int(latest_message['received_at'])).strftime('%Y-%m-%d %H:%M')}."
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            success=f"Почта проверена. Писем найдено: {len(messages)}.{latest_label}",
        )
    if status_value == "empty":
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            success="Почта проверена. Во входящих письма не найдены.",
        )
    return _account_detail_page_response(
        request,
        account_id=int(account_id),
        return_to=return_to,
        error=str(result.get("error") or "Не удалось проверить почту."),
        status_code=400,
    )


@app.get("/api/helper/launch-ticket/{ticket}")
def helper_launch_ticket_get(ticket: str, target: Optional[str] = None, _: None = Depends(require_helper_api_key)):
    try:
        payload = db.consume_helper_launch_ticket(ticket, target=target)
    except ValueError as exc:
        msg = str(exc)
        if msg == "ticket expired":
            return JSONResponse({"detail": "Ticket expired"}, status_code=410)
        if msg == "ticket used":
            return JSONResponse({"detail": "Ticket already used"}, status_code=409)
        if msg == "account not found":
            return JSONResponse({"detail": "Account not found"}, status_code=404)
        return JSONResponse({"detail": "Ticket not found"}, status_code=404)

    account = payload["account"]
    response_payload = {
        "ticket": payload["ticket"],
        "target": payload["target"],
        "account_id": payload["account_id"],
        "account_login": account["account_login"],
        "account_password": account["account_password"],
        "twofa": account["twofa"],
        "mail_enabled": bool(account.get("mail_enabled")),
        "mail_address": str(account.get("email") or "").strip(),
        "mail_provider": str(account.get("mail_provider") or "auto").strip() or "auto",
        "instagram_emulator_serial": str(account["instagram_emulator_serial"] or ""),
        "username": account["username"],
        "profile_url": _build_social_profile_url("instagram", account["username"]) or "https://www.instagram.com/",
    }
    if str(payload.get("target") or "") == "instagram_audit_login" and INSTAGRAM_AUDIT_FORCE_CLEAN_LOGIN:
        response_payload["force_clean_login"] = True
    return JSONResponse(response_payload)


@app.post("/api/helper/accounts/{account_id}/mail-challenge/resolve")
def helper_account_mail_challenge_resolve(
    account_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_helper_api_key),
):
    account_row = db.get_account(int(account_id))
    if account_row is None:
        return JSONResponse({"detail": "Account not found"}, status_code=404)
    account = dict(account_row)

    challenge_started_at_raw = payload.get("challenge_started_at")
    try:
        challenge_started_at = int(challenge_started_at_raw or time.time())
    except Exception:
        return JSONResponse({"detail": "Invalid challenge_started_at"}, status_code=400)

    screen_kind = str(payload.get("screen_kind") or "unknown").strip().lower() or "unknown"
    if screen_kind not in {"numeric_code", "channel_choice", "phone_only", "approval", "unknown"}:
        return JSONResponse({"detail": "Invalid screen_kind"}, status_code=400)

    timeout_raw = payload.get("timeout_seconds")
    try:
        timeout_seconds = int(timeout_raw or INSTAGRAM_MAIL_CHALLENGE_TIMEOUT_SECONDS)
    except Exception:
        return JSONResponse({"detail": "Invalid timeout_seconds"}, status_code=400)

    ticket = str(payload.get("ticket") or "").strip()
    logger.info(
        "helper_mail_challenge_resolve: account_id=%s ticket=%s screen_kind=%s timeout=%s",
        int(account_id),
        ticket or "-",
        screen_kind,
        timeout_seconds,
    )
    resolved = _resolve_instagram_mail_challenge(
        account,
        challenge_started_at=challenge_started_at,
        screen_kind=screen_kind,
        timeout_seconds=timeout_seconds,
    )
    return JSONResponse(resolved)


@app.post("/api/internal/mail/webhooks/gmail")
async def gmail_mail_webhook(request: Request):
    if not _mail_webhook_secret_valid(request):
        raise HTTPException(status_code=403, detail="Invalid mail webhook secret")
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    notification = mail_service.parse_gmail_push_notification(payload)
    email_address = str(notification.get("email_address") or "").strip().lower()
    if not email_address:
        return JSONResponse({"ok": True, "ignored": True, "reason": "missing_email_address"})
    account = _find_mail_account_by_email(email_address, provider="gmail_api")
    if account is None:
        return JSONResponse({"ok": True, "ignored": True, "reason": "account_not_found"})
    watch_payload = _mail_watch_payload(account.get("mail_watch_json"))
    history_id = str(notification.get("history_id") or "").strip()
    if history_id:
        watch_payload["history_id"] = history_id
        watch_payload["updated_at"] = int(time.time())
        db.update_account_mail_state(
            int(account["id"]),
            mail_provider="gmail_api",
            mail_watch_json=mail_service.dump_json_payload(watch_payload),
            mail_auth_json=str(account.get("mail_auth_json") or ""),
        )
    _enqueue_mail_account_sync(int(account["id"]), reason="gmail_push")
    return JSONResponse({"ok": True, "account_id": int(account["id"]), "provider": "gmail_api"})


@app.api_route("/api/internal/mail/webhooks/microsoft", methods=["GET", "POST"])
async def microsoft_mail_webhook(request: Request):
    validation_token = str(request.query_params.get("validationToken") or "").strip()
    if validation_token:
        return PlainTextResponse(validation_token)
    if not _mail_webhook_secret_valid(request):
        raise HTTPException(status_code=403, detail="Invalid mail webhook secret")
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc
    notifications = mail_service.parse_microsoft_notifications(payload)
    enqueued_accounts: list[int] = []
    for item in notifications:
        subscription_id = str(item.get("subscription_id") or "").strip()
        client_state = str(item.get("client_state") or "").strip()
        account = _find_mail_account_by_subscription(subscription_id, client_state=client_state)
        if account is None:
            continue
        account_id = int(account["id"])
        if account_id not in enqueued_accounts:
            _enqueue_mail_account_sync(account_id, reason="microsoft_notification")
            enqueued_accounts.append(account_id)
    return JSONResponse({"ok": True, "enqueued_account_ids": enqueued_accounts})


@app.post("/api/helper/accounts/{account_id}/instagram-status")
def helper_account_instagram_status_update(
    account_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_helper_api_key),
):
    account = db.get_account(int(account_id))
    if account is None:
        return JSONResponse({"detail": "Account not found"}, status_code=404)

    requested_handle = str(payload.get("handle") or "").strip()
    if requested_handle and not _account_matches_handle(dict(account), requested_handle):
        return JSONResponse({"detail": "Account handle mismatch"}, status_code=409)

    try:
        status_value = db.normalize_instagram_launch_status(str(payload.get("state") or "idle"))
    except ValueError:
        return JSONResponse({"detail": "Invalid status"}, status_code=400)

    detail = str(payload.get("detail") or "").strip()
    db.update_account_instagram_launch_state(int(account_id), status_value, detail)
    mail_challenge_payload = payload.get("mail_challenge")
    if isinstance(mail_challenge_payload, dict) and mail_challenge_payload:
        try:
            db.update_account_mail_challenge_state(
                int(account_id),
                status=str(mail_challenge_payload.get("status") or "idle"),
                kind=str(mail_challenge_payload.get("kind") or ""),
                reason_code=str(mail_challenge_payload.get("reason_code") or ""),
                reason_text=str(mail_challenge_payload.get("reason_text") or ""),
                message_uid=str(mail_challenge_payload.get("message_uid") or ""),
                received_at=mail_challenge_payload.get("received_at"),
                masked_code=str(mail_challenge_payload.get("masked_code") or ""),
                confidence=float(mail_challenge_payload.get("confidence") or 0.0),
            )
        except Exception:
            logger.warning("helper_mail_challenge_state_rejected: account_id=%s payload=%s", int(account_id), mail_challenge_payload)
    return JSONResponse(
        {
            "ok": True,
            "account_id": int(account_id),
            "handle": _account_identity_handle(dict(account)),
            "status": status_value,
        }
    )


@app.post("/api/helper/accounts/{account_id}/instagram-publish-status")
def helper_account_instagram_publish_status_update(
    account_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_helper_api_key),
):
    account = db.get_account(int(account_id))
    if account is None:
        return JSONResponse({"detail": "Account not found"}, status_code=404)

    requested_handle = str(payload.get("handle") or "").strip()
    if requested_handle and not _account_matches_handle(dict(account), requested_handle):
        return JSONResponse({"detail": "Account handle mismatch"}, status_code=409)

    try:
        status_value = db.normalize_instagram_publish_status(str(payload.get("state") or "idle"))
    except ValueError:
        return JSONResponse({"detail": "Invalid status"}, status_code=400)

    detail = str(payload.get("detail") or "").strip()
    last_file = str(payload.get("last_file") or "").strip()
    db.update_account_instagram_publish_state(int(account_id), status_value, detail, last_file=last_file)
    if status_value == "published":
        helper_ticket = str(payload.get("helper_ticket") or "").strip()
        if helper_ticket:
            db.upsert_instagram_reel_post_for_standalone(
                account_id=int(account_id),
                helper_ticket=helper_ticket,
                source_name=last_file,
                source_path=str(payload.get("source_path") or "").strip(),
                payload={
                    "helper_ticket": helper_ticket,
                    "reel_fingerprint": str(payload.get("reel_fingerprint") or "").strip(),
                    "reel_signature_text": str(payload.get("reel_signature_text") or "").strip(),
                    "matched_slot": payload.get("matched_slot"),
                    "matched_age_seconds": payload.get("matched_age_seconds"),
                    "published_at": payload.get("published_at"),
                },
            )
    return JSONResponse(
        {
            "ok": True,
            "account_id": int(account_id),
            "handle": _account_identity_handle(dict(account)),
            "status": status_value,
            "last_file": last_file,
        }
    )


@app.post("/api/internal/publishing/n8n")
async def publishing_n8n_callback(
    request: Request,
    x_publish_timestamp: Optional[str] = Header(None),
    x_publish_signature: Optional[str] = Header(None),
):
    raw_body = await request.body()
    _verify_signed_publish_request(raw_body, x_publish_timestamp, x_publish_signature)
    try:
        payload = json.loads(raw_body.decode("utf-8") or "{}")
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON payload") from exc

    event = str(payload.get("event") or "").strip().lower()
    try:
        batch_id = int(payload.get("batch_id") or 0)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid batch_id") from exc
    if batch_id <= 0:
        raise HTTPException(status_code=400, detail="batch_id is required")
    if db.get_publish_batch(batch_id) is None:
        raise HTTPException(status_code=404, detail="Batch not found")
    _run_publish_generation_watchdog(int(batch_id))
    account_id_raw = payload.get("account_id")
    account_id: int | None = None
    if account_id_raw not in (None, ""):
        try:
            account_id = int(account_id_raw)
        except Exception as exc:
            raise HTTPException(status_code=400, detail="Invalid account_id") from exc

    detail = str(payload.get("detail") or "").strip()
    event_hash = _publish_event_hash(payload)
    if event_hash and db.publish_event_hash_exists(batch_id, event_hash):
        return JSONResponse(
            {"ok": True, "event": event, "batch_id": batch_id, "account_id": account_id, "duplicate": True}
        )
    account_id = _resolve_publish_callback_account_id(batch_id, event, account_id)
    if event != "generation_progress":
        logger.info("publish_callback: event=%s batch_id=%s account_id=%s", event, batch_id, account_id)

    account_state: Optional[str] = None
    if account_id is not None:
        account_state = db.get_publish_batch_account_state(batch_id, account_id)
    if account_state:
        if event in {"generation_started", "generation_progress"} and account_state not in {"queued_for_generation", "generating"}:
            return JSONResponse(
                {
                    "ok": True,
                    "event": event,
                    "batch_id": batch_id,
                    "account_id": account_id,
                    "ignored": True,
                    "reason": "state already advanced",
                    "state": account_state,
                }
            )
        if event == "generation_failed" and account_state not in {"queued_for_generation", "generating", "generation_failed"}:
            return JSONResponse(
                {
                    "ok": True,
                    "event": event,
                    "batch_id": batch_id,
                    "account_id": account_id,
                    "ignored": True,
                    "reason": "state already advanced",
                    "state": account_state,
                }
            )
    if event == "generation_started":
        try:
            metrics = db.mark_publish_generation_started(
                batch_id,
                detail=detail or None,
                account_id=account_id,
                event_hash=event_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response_payload = {"ok": True, "event": event, "batch_id": batch_id, "account_id": account_id, "state": metrics.get("state")}
        cleanup = _maybe_cleanup_publish_batch_stage_dir(batch_id, str(metrics.get("state") or ""))
        if cleanup is not None:
            response_payload["cleanup"] = cleanup
        return JSONResponse(response_payload)
    if event == "generation_completed":
        try:
            metrics = db.mark_publish_generation_completed(batch_id, detail=detail or None, event_hash=event_hash)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response_payload = {"ok": True, "event": event, "batch_id": batch_id, "state": metrics.get("state")}
        cleanup = _maybe_cleanup_publish_batch_stage_dir(batch_id, str(metrics.get("state") or ""))
        if cleanup is not None:
            response_payload["cleanup"] = cleanup
        return JSONResponse(response_payload)
    if event == "generation_failed":
        diagnostic_payload: dict[str, str] = {}
        for key, max_length in (
            ("error_code", 120),
            ("raw_preview", 2000),
            ("fixed_preview", 2000),
            ("parsed_keys", 400),
            ("factory_response_preview", 2000),
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                diagnostic_payload[key] = value[:max_length]
        try:
            metrics = db.mark_publish_generation_failed(
                batch_id,
                detail or "n8n сообщил об ошибке генерации.",
                account_id=account_id,
                payload=diagnostic_payload or None,
                event_hash=event_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response_payload = {"ok": True, "event": event, "batch_id": batch_id, "account_id": account_id, "state": metrics.get("state")}
        cleanup = _maybe_cleanup_publish_batch_stage_dir(batch_id, str(metrics.get("state") or ""))
        if cleanup is not None:
            response_payload["cleanup"] = cleanup
        if account_id is not None:
            _enqueue_publish_batch_start(batch_id)
        return JSONResponse(response_payload)
    if event == "generation_progress":
        if account_id is None:
            raise HTTPException(status_code=400, detail="account_id is required for generation_progress")
        stage_key = str(payload.get("stage_key") or "").strip().lower()
        stage_label = str(payload.get("stage_label") or "").strip()
        if not stage_key:
            raise HTTPException(status_code=400, detail="stage_key is required")
        if stage_key not in PUBLISH_GENERATION_STAGE_LABELS:
            raise HTTPException(status_code=400, detail="Unsupported generation stage_key")
        if not stage_label:
            raise HTTPException(status_code=400, detail="stage_label is required")
        try:
            progress_pct = float(payload.get("progress_pct"))
        except Exception as exc:
            raise HTTPException(status_code=400, detail="progress_pct must be a number") from exc
        if progress_pct < 0 or progress_pct > 100:
            raise HTTPException(status_code=400, detail="progress_pct must be in range 0..100")
        meta = payload.get("meta")
        try:
            metrics = db.mark_publish_generation_progress(
                batch_id,
                account_id=account_id,
                stage_key=stage_key,
                stage_label=stage_label,
                progress_pct=progress_pct,
                detail=detail or stage_label,
                meta=meta,
                event_hash=event_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        return JSONResponse(
            {
                "ok": True,
                "event": event,
                "batch_id": batch_id,
                "account_id": account_id,
                "state": metrics.get("state"),
                "stage_key": stage_key,
                "stage_label": stage_label,
                "progress_pct": progress_pct,
            }
        )
    if event == "artifact_ready":
        try:
            normalized_path = _normalize_publish_artifact_path(batch_id, str(payload.get("path") or ""))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        if not normalized_path.exists() or not normalized_path.is_file():
            raise HTTPException(status_code=409, detail="Artifact file not found")
        filename = str(payload.get("filename") or normalized_path.name).strip() or normalized_path.name
        checksum = str(payload.get("checksum") or "").strip() or _file_sha256(normalized_path)
        size_bytes = payload.get("size_bytes")
        duration_seconds = payload.get("duration_seconds")
        if size_bytes in (None, ""):
            size_bytes = int(normalized_path.stat().st_size)
        try:
            result = db.register_publish_artifact(
                batch_id,
                path=str(normalized_path),
                filename=filename,
                checksum=checksum,
                size_bytes=int(size_bytes) if size_bytes not in (None, "") else None,
                duration_seconds=float(duration_seconds) if duration_seconds not in (None, "") else None,
                account_id=account_id,
                event_hash=event_hash,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        response_payload = {"ok": True, "event": event, "batch_id": batch_id, "account_id": account_id, **result}
        cleanup = _maybe_cleanup_publish_batch_stage_dir(batch_id, str(result.get("state") or ""))
        if cleanup is not None:
            response_payload["cleanup"] = cleanup
        return JSONResponse(response_payload)
    raise HTTPException(status_code=400, detail="Unsupported publish event")


@app.post("/api/internal/publishing/jobs/lease")
def publishing_job_lease(
    payload: Optional[dict] = Body(None),
    _: None = Depends(require_publish_runner_api_key),
):
    _run_publish_generation_watchdog()
    runner_name = str((payload or {}).get("runner_name") or "publish-runner").strip() or "publish-runner"
    job = db.lease_next_publish_job(runner_name=runner_name, lease_seconds=PUBLISH_RUNNER_LEASE_SECONDS)
    if job is None:
        return Response(status_code=204)
    return JSONResponse({"ok": True, "job": job})


@app.get("/api/internal/publishing/jobs/{job_id}/artifact")
def publishing_job_artifact_download(
    job_id: int,
    _: None = Depends(require_publish_runner_api_key),
):
    job = db.get_publish_job(int(job_id))
    if job is None:
        raise HTTPException(status_code=404, detail="job not found")
    try:
        source_path = _normalize_publish_artifact_path(int(job["batch_id"]), str(job["source_path"] or ""))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if not source_path.exists() or not source_path.is_file():
        raise HTTPException(status_code=404, detail="artifact file not found")
    filename = str(job["source_name"] or job["artifact_filename"] or source_path.name).strip() or source_path.name
    return FileResponse(path=source_path, filename=filename, media_type="application/octet-stream")


@app.post("/api/internal/publishing/jobs/{job_id}/status")
def publishing_job_status_update(
    job_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_publish_runner_api_key),
):
    state_raw = str(payload.get("state") or "").strip()
    if not state_raw:
        raise HTTPException(status_code=400, detail="state is required")
    publish_payload = {
        "emulator_serial": str(payload.get("emulator_serial") or "").strip(),
        "source_path": str(payload.get("source_path") or "").strip(),
        "account_publish_state": str(payload.get("account_publish_state") or "").strip(),
    }
    for key in (
        "publish_phase",
        "accepted_by_instagram",
        "elapsed_seconds",
        "last_activity",
        "upload_progress_pct",
        "event_kind",
        "reason_code",
        "verification_attempt",
        "verification_window_minutes",
        "checked_slots",
        "matched_slot",
        "matched_age_seconds",
        "reel_fingerprint",
        "reel_signature_text",
        "published_at",
        "helper_ticket",
        "baseline_available",
        "seconds_until_profile_check",
        "share_clicked_at",
        "verification_starts_at",
        "verification_deadline_at",
        "first_profile_check_at",
        "profile_surface_state",
        "keyboard_visible",
        "comment_sheet_visible",
        "clips_viewer_visible",
        "quick_capture_visible",
        "timestamp_readable",
        "diagnostics_path",
        "timings",
    ):
        if key in payload:
            publish_payload[key] = payload.get(key)
    if isinstance(payload.get("mail_challenge"), dict) and payload.get("mail_challenge"):
        publish_payload["mail_challenge"] = payload.get("mail_challenge")
    try:
        result = db.update_publish_job_state(
            int(job_id),
            state=state_raw,
            detail=str(payload.get("detail") or "").strip(),
            last_file=str(payload.get("last_file") or "").strip() or None,
            runner_name=str(payload.get("runner_name") or "").strip() or None,
            payload=publish_payload,
            lease_seconds=PUBLISH_RUNNER_LEASE_SECONDS,
        )
    except ValueError as exc:
        msg = str(exc)
        status_code = 409 if msg == "job already finished" else 404 if msg == "job not found" else 400
        raise HTTPException(status_code=status_code, detail=msg) from exc
    logger.info("publish_job_status: job_id=%s batch_id=%s state=%s", job_id, result.get("batch_id"), state_raw)
    cleanup = _maybe_cleanup_publish_batch_stage_dir(
        int(result["batch_id"]),
        str(result.get("batch_state") or result.get("state") or ""),
        job_id=int(job_id),
    )
    if str(result.get("job_state") or "").strip().lower() in {"published", "needs_review", "failed", "canceled"}:
        _enqueue_publish_batch_start(int(result["batch_id"]))
    response_payload = {"ok": True, **result}
    if cleanup is not None:
        response_payload["cleanup"] = cleanup
    return JSONResponse(response_payload)


@app.post("/api/internal/reel-metrics/lease")
def instagram_reel_metrics_lease(
    payload: Optional[dict] = Body(None),
    _: None = Depends(require_publish_runner_api_key),
):
    runner_name = str((payload or {}).get("runner_name") or "publish-runner").strip() or "publish-runner"
    post = db.lease_next_instagram_reel_post(runner_name=runner_name, lease_seconds=PUBLISH_RUNNER_LEASE_SECONDS)
    if post is None:
        return Response(status_code=204)

    account_row = db.get_account(int(post["account_id"]))
    if account_row is None:
        db.record_instagram_reel_metric_snapshot(
            int(post["id"]),
            window_key=str(post["collection_stage"] or "t30m"),
            status="failed",
            retryable=True,
            error_detail="Account not found for reel metrics collection.",
        )
        return Response(status_code=204)

    account = dict(account_row)
    return JSONResponse(
        {
            "ok": True,
            "post": {
                "id": int(post["id"]),
                "origin_kind": str(post.get("origin_kind") or ""),
                "account_id": int(post["account_id"]),
                "publish_batch_id": int(post["publish_batch_id"]) if post.get("publish_batch_id") is not None else None,
                "publish_job_id": int(post["publish_job_id"]) if post.get("publish_job_id") is not None else None,
                "publish_artifact_id": int(post["publish_artifact_id"]) if post.get("publish_artifact_id") is not None else None,
                "helper_ticket": str(post.get("helper_ticket") or ""),
                "source_name": str(post.get("source_name") or ""),
                "source_path": str(post.get("source_path") or ""),
                "reel_fingerprint": str(post.get("reel_fingerprint") or ""),
                "reel_signature_text": str(post.get("reel_signature_text") or ""),
                "matched_slot": int(post["matched_slot"]) if post.get("matched_slot") is not None else None,
                "matched_age_seconds": int(post["matched_age_seconds"]) if post.get("matched_age_seconds") is not None else None,
                "published_at": int(post["published_at"]),
                "window_key": str(post.get("collection_stage") or "t30m"),
                "collection_state": str(post.get("collection_state") or ""),
                "next_collect_at": int(post["next_collect_at"]) if post.get("next_collect_at") is not None else None,
                "account_login": str(account.get("account_login") or ""),
                "account_password": str(account.get("account_password") or ""),
                "twofa": str(account.get("twofa") or ""),
                "username": str(account.get("username") or ""),
                "instagram_emulator_serial": str(account.get("instagram_emulator_serial") or ""),
            },
        }
    )


@app.post("/api/internal/reel-metrics/posts/{post_id}/snapshot")
def instagram_reel_metrics_snapshot_update(
    post_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_publish_runner_api_key),
):
    window_key = str(payload.get("window_key") or "").strip()
    if not window_key:
        raise HTTPException(status_code=400, detail="window_key is required")
    status_value = str(payload.get("status") or "").strip()
    if not status_value:
        raise HTTPException(status_code=400, detail="status is required")
    try:
        updated = db.record_instagram_reel_metric_snapshot(
            int(post_id),
            window_key=window_key,
            status=status_value,
            collected_at=payload.get("collected_at"),
            plays_count=payload.get("plays_count"),
            likes_count=payload.get("likes_count"),
            comments_count=payload.get("comments_count"),
            shares_count=payload.get("shares_count"),
            saves_count=payload.get("saves_count"),
            accounts_reached_count=payload.get("accounts_reached_count"),
            watch_time_seconds=payload.get("watch_time_seconds"),
            avg_watch_time_seconds=payload.get("avg_watch_time_seconds"),
            three_second_views_count=payload.get("three_second_views_count"),
            completion_rate_pct=payload.get("completion_rate_pct"),
            raw_text_json=payload.get("raw_text_json"),
            diagnostics_path=str(payload.get("diagnostics_path") or "").strip(),
            retryable=bool(payload.get("retryable")),
            error_detail=str(payload.get("error_detail") or "").strip(),
        )
    except ValueError as exc:
        message = str(exc)
        status_code = 404 if message == "instagram reel post not found" else 400
        raise HTTPException(status_code=status_code, detail=message) from exc
    return JSONResponse({"ok": True, "post": updated})


@app.post("/accounts", response_class=HTMLResponse)
def account_create(
    request: Request,
    type: str = Form(...),
    account_login: str = Form(...),
    account_password: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    email_password: str = Form(""),
    mail_provider: str = Form("auto"),
    mail_auth_json: str = Form(""),
    proxy: Optional[str] = Form(None),
    twofa: Optional[str] = Form(None),
    instagram_emulator_serial: Optional[str] = Form(None),
    rotation_state: Optional[str] = Form("review"),
    views_state: Optional[str] = Form("unknown"),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    filter_rotation_state: Optional[str] = Form(None),
    filter_views_state: Optional[str] = Form(None),
    filter_sort: Optional[str] = Form(None),
    owner_worker_id: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    filter_rotation_state_raw = (filter_rotation_state or "").strip()
    filter_views_state_raw = (filter_views_state or "").strip()
    filter_sort_raw = (filter_sort or "").strip()

    try:
        account_type = _normalize_account_type(type)
        rotation_state_value = _normalize_rotation_state_filter(rotation_state) or "review"
        views_state_value = _normalize_views_state_filter(views_state) or "unknown"
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Неверные параметры аккаунта",
            status_code=400,
        )

    if not account_type:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Выбери тип аккаунта",
            status_code=400,
        )

    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Имя профиля": username,
        "Почта": email,
    }
    for label, value in required.items():
        if not (value or "").strip():
            return _accounts_page_response(
                request,
                q=query,
                account_type=filter_type_raw,
                worker_filter=filter_worker_raw,
                rotation_state=filter_rotation_state_raw,
                views_state=filter_views_state_raw,
                sort=filter_sort_raw,
                error=f"Заполни поле: {label}",
                status_code=400,
            )
    mail_validation_error = _validate_mail_form_fields(
        email=email,
        email_password=email_password,
        mail_provider=mail_provider,
        mail_auth_json=mail_auth_json,
    )
    if mail_validation_error:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error=mail_validation_error,
            status_code=400,
        )

    try:
        owner_id = _parse_owner_worker_id(owner_worker_id, allow_none_token=True)
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Неверный работник",
            status_code=400,
        )

    duplicate = db.find_duplicate_account(account_type, account_login)
    if duplicate is not None:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error=_duplicate_account_message(dict(duplicate)),
            status_code=400,
        )

    resolved_emulator_serial = (instagram_emulator_serial or "").strip()
    if account_type == "instagram" and not resolved_emulator_serial:
        resolved_emulator_serial = _allocate_instagram_emulator_serial(_prepare_instagram_emulator_serial_usage())

    try:
        created_info = db.create_account_with_default_link(
            account_type=account_type,
            account_login=account_login,
            account_password=account_password,
            username=username,
            email=email,
            email_password=email_password,
            mail_provider=mail_provider,
            mail_auth_json=mail_auth_json,
            proxy=proxy,
            twofa=twofa,
            instagram_emulator_serial=resolved_emulator_serial,
            rotation_state=rotation_state_value,
            views_state=views_state_value,
            owner_worker_id=owner_id,
            default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{(username or '').strip() or 'account'}",
            target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
        )
    except ValueError as exc:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error=_account_form_error_message(str(exc)),
            status_code=400,
        )
    new_id = int(created_info["account_id"])
    return _redirect(f"/accounts/{new_id}?created=1", status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/import", response_class=HTMLResponse)
async def accounts_import(
    request: Request,
    import_type: str = Form(...),
    import_owner_worker_id: Optional[str] = Form(None),
    import_file: UploadFile = File(...),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    filter_rotation_state: Optional[str] = Form(None),
    filter_views_state: Optional[str] = Form(None),
    filter_sort: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    filter_rotation_state_raw = (filter_rotation_state or "").strip()
    filter_views_state_raw = (filter_views_state or "").strip()
    filter_sort_raw = (filter_sort or "").strip()

    try:
        account_type = _normalize_account_type(import_type)
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Неверный тип аккаунта для импорта",
            status_code=400,
        )

    if not account_type:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Выбери тип аккаунта для импорта",
            status_code=400,
        )

    try:
        owner_id = _parse_owner_worker_id(import_owner_worker_id, allow_none_token=True)
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Неверный работник для импорта",
            status_code=400,
        )

    if import_file is None or not (import_file.filename or "").strip():
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Выбери файл для импорта",
            status_code=400,
        )

    raw_bytes = await import_file.read()
    if len(raw_bytes) > ACCOUNTS_IMPORT_MAX_BYTES:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error=f"Файл слишком большой. Лимит: {ACCOUNTS_IMPORT_MAX_BYTES // 1024 // 1024} MB",
            status_code=400,
        )

    rows, parse_errors = _parse_accounts_import_upload(raw_bytes)
    if not rows:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Не удалось импортировать файл",
            import_errors=parse_errors,
            status_code=400,
        )

    imported = 0
    import_errors = list(parse_errors)
    serial_usage = _prepare_instagram_emulator_serial_usage() if account_type == "instagram" else None
    for row in rows:
        duplicate = db.find_duplicate_account(account_type, row["account_login"])
        if duplicate is not None:
            import_errors.append(f"Строка {row['line']}: {_duplicate_account_message(dict(duplicate))}")
            continue
        try:
            db.create_account_with_default_link(
                account_type=account_type,
                account_login=row["account_login"],
                account_password=row["account_password"],
                username=row["username"],
                email=row["email"],
                email_password=row["email_password"],
                proxy="",
                twofa=row["twofa"],
                instagram_emulator_serial=_allocate_instagram_emulator_serial(serial_usage) if account_type == "instagram" else "",
                owner_worker_id=owner_id,
                default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{row['username'] or 'account'}",
                target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
            )
            imported += 1
        except Exception as exc:
            import_errors.append(f"Строка {row['line']}: {str(exc)}")

    summary = {
        "imported": imported,
        "failed": len(import_errors),
        "total": len(rows) + len(parse_errors),
        "filename": (import_file.filename or "").strip(),
        "username_rule": "username создан из логина",
    }
    success = f"Импортировано аккаунтов: {imported}."
    if imported == 0:
        return _accounts_page_response(
            request,
            q=query,
            account_type=filter_type_raw,
            worker_filter=filter_worker_raw,
            rotation_state=filter_rotation_state_raw,
            views_state=filter_views_state_raw,
            sort=filter_sort_raw,
            error="Импорт не создал ни одного аккаунта",
            import_summary=summary,
            import_errors=import_errors,
            status_code=400,
        )

    return _accounts_page_response(
        request,
        q=query,
        account_type=filter_type_raw,
        worker_filter=filter_worker_raw,
        rotation_state=filter_rotation_state_raw,
        views_state=filter_views_state_raw,
        sort=filter_sort_raw,
        success=success,
        import_summary=summary,
        import_errors=import_errors,
    )


@app.post("/accounts/{account_id}/update", response_class=HTMLResponse)
def account_update(
    request: Request,
    account_id: int,
    type: str = Form(...),
    account_login: str = Form(...),
    account_password: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    email_password: str = Form(""),
    mail_provider: str = Form("auto"),
    mail_auth_json: str = Form(""),
    proxy: Optional[str] = Form(None),
    twofa: Optional[str] = Form(None),
    instagram_emulator_serial: Optional[str] = Form(None),
    rotation_state: Optional[str] = Form("review"),
    views_state: Optional[str] = Form("unknown"),
    owner_worker_id: Optional[str] = Form(None),
    next_url: Optional[str] = Form(None),
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    fallback_url = _build_detail_url(f"/accounts/{int(account_id)}", return_to)

    try:
        account_type = _normalize_account_type(type)
        rotation_state_value = _normalize_rotation_state_filter(rotation_state) or "review"
        views_state_value = _normalize_views_state_filter(views_state) or "unknown"
    except ValueError:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Неверные параметры аккаунта",
            status_code=400,
        )

    if not account_type:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Выбери тип аккаунта",
            status_code=400,
        )

    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Имя профиля": username,
        "Почта": email,
    }
    for label, value in required.items():
        if not (value or "").strip():
            return _account_detail_page_response(
                request,
                account_id=int(account_id),
                return_to=return_to,
                error=f"Заполни поле: {label}",
                status_code=400,
            )
    mail_validation_error = _validate_mail_form_fields(
        email=email,
        email_password=email_password,
        mail_provider=mail_provider,
        mail_auth_json=mail_auth_json,
    )
    if mail_validation_error:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error=mail_validation_error,
            status_code=400,
        )

    try:
        owner_id = _parse_owner_worker_id(owner_worker_id, allow_none_token=True)
    except ValueError:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Неверный работник",
            status_code=400,
        )

    duplicate = db.find_duplicate_account(account_type, account_login, exclude_account_id=int(account_id))
    if duplicate is not None:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error=_duplicate_account_message(dict(duplicate)),
            status_code=400,
        )

    try:
        db.update_account(
            account_id=int(account_id),
            account_type=account_type,
            account_login=account_login,
            account_password=account_password,
            username=username,
            email=email,
            email_password=email_password,
            mail_provider=mail_provider,
            mail_auth_json=mail_auth_json,
            proxy=proxy,
            twofa=twofa,
            instagram_emulator_serial=instagram_emulator_serial,
            rotation_state=rotation_state_value,
            views_state=views_state_value,
            owner_worker_id=owner_id,
        )
    except ValueError as exc:
        msg = str(exc)
        if msg == "duplicate account":
            duplicate = db.find_duplicate_account(account_type, account_login, exclude_account_id=int(account_id))
            if duplicate is not None:
                msg = _duplicate_account_message(dict(duplicate))
        else:
            msg = _account_form_error_message(msg)
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error=msg,
            status_code=400,
        )
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/claim-requests/{request_id}/approve")
def account_claim_request_approve(request_id: int, next_url: Optional[str] = Form(None), _: None = Depends(require_auth)):
    db.resolve_account_claim_request(int(request_id), approve=True)
    dest = _safe_next_url(next_url, fallback="/accounts")
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/claim-requests/{request_id}/reject")
def account_claim_request_reject(request_id: int, next_url: Optional[str] = Form(None), _: None = Depends(require_auth)):
    db.resolve_account_claim_request(int(request_id), approve=False)
    dest = _safe_next_url(next_url, fallback="/accounts")
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/{account_id}/delete")
def account_delete(
    request: Request,
    account_id: int,
    next_url: Optional[str] = Form(None),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    filter_rotation_state: Optional[str] = Form(None),
    filter_views_state: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    db.delete_account(int(account_id))
    fallback_url = _accounts_redirect_url(
        query,
        filter_type_raw,
        filter_worker_raw,
        (filter_rotation_state or "").strip(),
        (filter_views_state or "").strip(),
    )
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/{account_id}/links", response_class=HTMLResponse)
def account_link_create(
    request: Request,
    account_id: int,
    name: Optional[str] = Form(None),
    next_url: Optional[str] = Form(None),
    return_to: Optional[str] = Form(None),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    fallback_url = _build_detail_url(f"/accounts/{int(account_id)}", return_to)
    if not db.get_account(int(account_id)):
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )

    existing_links = db.list_account_links_with_stats(int(account_id))
    if existing_links:
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Дополнительные ссылки отключены. Для аккаунта доступна только одна рабочая ссылка.",
            status_code=409,
        )

    name_clean = (name or "").strip() or "Основная ссылка"

    try:
        db.create_account_link(
            account_id=int(account_id),
            name=name_clean,
            custom_code=None,
            target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
        )
    except ValueError as exc:
        msg = str(exc)
        if msg == "invalid link code":
            msg = "Код: только латиница/цифры/_ и длина 4-32."
        elif msg == "code already exists":
            msg = "Такой код уже существует."
        elif msg == "account not found":
            msg = "Аккаунт не найден."
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error=msg,
            status_code=400,
        )

    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/links/{code}/toggle")
def account_link_toggle(
    request: Request,
    code: str,
    active: str = Form(...),
    next_url: Optional[str] = Form(None),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    on = (active or "").strip().lower() in {"1", "true", "yes", "on"}
    db.toggle_link_active(code, on)
    fallback_url = _accounts_redirect_url(query, filter_type_raw, filter_worker_raw)
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/accounts/links/{code}/delete")
def account_link_delete(
    request: Request,
    code: str,
    next_url: Optional[str] = Form(None),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_worker: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    db.soft_delete_link(code)
    fallback_url = _accounts_redirect_url(query, filter_type_raw, filter_worker_raw)
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


def _worker_accounts_redirect_url(q: str, account_type: str, sort: str = "recent") -> str:
    parts = []
    if q:
        parts.append(f"q={quote_plus(q)}")
    if account_type:
        parts.append(f"type={quote_plus(account_type)}")
    if sort and sort != "recent":
        parts.append(f"sort={quote_plus(sort)}")
    if not parts:
        return "/worker/accounts"
    return "/worker/accounts?" + "&".join(parts)


def _worker_accounts_page_response(
    request: Request,
    *,
    worker_id: int,
    q: str,
    account_type: str,
    sort: str = "recent",
    error: Optional[str] = None,
    success: Optional[str] = None,
    import_summary: Optional[dict] = None,
    import_errors: Optional[list[str]] = None,
    status_code: int = 200,
) -> HTMLResponse:
    try:
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        sort_value = "recent"
        error = error or "Неверная сортировка"
    list_url = _worker_accounts_redirect_url(q, account_type, sort_value)
    rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=int(worker_id),
        sort_by=sort_value,
        limit=500,
    )
    accounts = []
    for raw in rows:
        account = dict(raw)
        account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
        rotation_label, rotation_class = _account_rotation_state_meta(account.get("rotation_state"))
        account["rotation_state_label"] = rotation_label
        account["rotation_state_class"] = rotation_class
        account["rotation_state_reason"] = db.account_rotation_display_reason(account)
        account["rotation_state_source"] = str(account.get("rotation_state_source") or "manual").strip().lower() or "manual"
        code = str(account.get("primary_link_code") or "").strip()
        account["primary_bot_url"] = _build_bot_start_url(code) if code else ""
        account["card_id"] = f"account-{int(account['id'])}"
        account["detail_url"] = _build_detail_url(
            f"/worker/accounts/{int(account['id'])}",
            f"{list_url}#{account['card_id']}",
        )
        accounts.append(account)

    claim_requests = []
    for raw in db.list_account_claim_requests(status=None, requested_by_worker_id=int(worker_id), limit=100):
        row = dict(raw)
        row["type_label"] = ACCOUNT_TYPE_LABELS.get(str(row.get("account_type") or ""), str(row.get("account_type") or "").upper())
        row["status_label"], row["status_class"] = _claim_status_meta(str(row.get("status") or "pending"))
        owner_name = str(row.get("owner_worker_name") or "").strip()
        owner_username = str(row.get("owner_worker_username") or "").strip()
        row["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
        claim_requests.append(row)

    worker_row = db.get_worker(int(worker_id))
    worker_name = str(worker_row["name"]) if worker_row else "Работник"
    worker_stats = db.worker_detail_overview(int(worker_id))
    return templates.TemplateResponse(
        "worker_accounts.html",
        {
            "request": request,
            "worker_id": int(worker_id),
            "worker_name": worker_name,
            "accounts": accounts,
            "stats": worker_stats,
            "q": q,
            "type": account_type,
            "sort": sort_value,
            "accounts_list_url": list_url,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "sort_options": ACCOUNT_LIST_SORT_OPTIONS,
            "claim_requests": claim_requests,
            "error": error,
            "success": success,
            "import_summary": import_summary or None,
            "import_errors": import_errors or [],
        },
        status_code=status_code,
    )


def _worker_account_detail_page_response(
    request: Request,
    *,
    worker_id: int,
    account_id: int,
    return_to: Optional[str] = None,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    return_to_clean = _safe_next_url(return_to, fallback="")
    back_url = return_to_clean or "/worker/accounts"
    detail_self_url = _build_detail_url(f"/worker/accounts/{int(account_id)}", return_to_clean)
    account_row = db.get_account(int(account_id), owner_worker_id=int(worker_id))
    if not account_row:
        return templates.TemplateResponse(
            "worker_account_detail.html",
            {
                "request": request,
                "account": None,
                "links": [],
                "stats": {"starts_unique_total": 0, "starts_total": 0, "first_touch_total": 0, "links_total": 0},
                "type_options": ACCOUNT_TYPE_OPTIONS,
                "back_url": back_url,
                "detail_self_url": detail_self_url,
                "return_to": return_to_clean,
                "error": "Аккаунт не найден",
                "success": None,
            },
            status_code=404,
        )

    account = dict(account_row)
    account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
    rotation_label, rotation_class = _account_rotation_state_meta(account.get("rotation_state"))
    account["rotation_state_label"] = rotation_label
    account["rotation_state_class"] = rotation_class
    account["rotation_state_reason"] = db.account_rotation_display_reason(account)
    account["rotation_state_source"] = str(account.get("rotation_state_source") or "manual").strip().lower() or "manual"
    links = [dict(r) for r in db.list_account_links_with_stats(int(account["id"]), owner_worker_id=int(worker_id))]
    for link in links:
        link["bot_url"] = _build_bot_start_url(str(link.get("code") or ""))
    stats = db.account_stats(int(account["id"]), owner_worker_id=int(worker_id))

    return templates.TemplateResponse(
        "worker_account_detail.html",
        {
            "request": request,
            "account": account,
            "links": links,
            "stats": stats,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "back_url": back_url,
            "detail_self_url": detail_self_url,
            "return_to": return_to_clean,
            "error": error,
            "success": success,
        },
        status_code=status_code,
    )


def _worker_detail_page_response(
    request: Request,
    *,
    worker_id: int,
    q: str,
    account_type: str,
    sort: str = "recent",
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    worker_row = db.get_worker(int(worker_id))
    if not worker_row:
        return templates.TemplateResponse(
            "worker_detail.html",
            {
                "request": request,
                "worker": None,
                "accounts": [],
                "stats": {"accounts_total": 0, "starts_total": 0, "starts_unique_total": 0, "first_touch_total": 0},
                "q": q,
                "type": account_type,
                "type_options": ACCOUNT_TYPE_OPTIONS,
                "error": error or "Работник не найден",
                "success": None,
            },
            status_code=404,
        )

    worker = dict(worker_row)
    stats = db.worker_detail_overview(int(worker_id))
    try:
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        sort_value = "recent"
        error = error or "Неверная сортировка"
    list_url = _worker_detail_redirect_url(worker_id, q, account_type, sort_value)
    accounts_rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=int(worker_id),
        sort_by=sort_value,
        limit=500,
    )
    accounts = []
    for raw in accounts_rows:
        account = dict(raw)
        account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
        rotation_label, rotation_class = _account_rotation_state_meta(account.get("rotation_state"))
        account["rotation_state_label"] = rotation_label
        account["rotation_state_class"] = rotation_class
        account["rotation_state_reason"] = db.account_rotation_display_reason(account)
        account["rotation_state_source"] = str(account.get("rotation_state_source") or "manual").strip().lower() or "manual"
        code = str(account.get("primary_link_code") or "").strip()
        account["primary_bot_url"] = _build_bot_start_url(code) if code else ""
        account["card_id"] = f"account-{int(account['id'])}"
        account["detail_url"] = _build_detail_url(
            f"/accounts/{int(account['id'])}",
            f"{list_url}#{account['card_id']}",
        )
        accounts.append(account)

    return templates.TemplateResponse(
        "worker_detail.html",
        {
            "request": request,
            "worker": worker,
            "accounts": accounts,
            "stats": stats,
            "q": q,
            "type": account_type,
            "sort": sort_value,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "sort_options": ACCOUNT_LIST_SORT_OPTIONS,
            "error": error,
            "success": success,
        },
        status_code=status_code,
    )


@app.get("/workers", response_class=HTMLResponse)
def workers_page(request: Request, q: Optional[str] = None, created: Optional[str] = None, _: None = Depends(require_auth)):
    query = (q or "").strip()
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    workers = [dict(w) for w in db.list_workers_compact(q=query, limit=500)]
    overview = db.workers_overview()
    return templates.TemplateResponse(
        "workers.html",
        {
            "request": request,
            "workers": workers,
            "overview": overview,
            "q": query,
            "error": None,
            "success": "Работник создан." if created_flag else None,
        },
    )


@app.post("/workers", response_class=HTMLResponse)
def worker_create(
    request: Request,
    name: str = Form(...),
    username: str = Form(...),
    password: str = Form(...),
    q: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    try:
        db.create_worker(name=name, username=username, password=password)
    except ValueError as exc:
        workers = [dict(w) for w in db.list_workers_compact(q=query, limit=500)]
        overview = db.workers_overview()
        msg = str(exc)
        if msg == "username exists":
            msg = "Такой логин уже существует."
        elif msg == "name required":
            msg = "Заполни имя работника."
        elif msg == "username required":
            msg = "Заполни логин работника."
        elif msg == "password required":
            msg = "Заполни пароль работника."
        return templates.TemplateResponse(
            "workers.html",
            {
                "request": request,
                "workers": workers,
                "overview": overview,
                "q": query,
                "error": msg,
                "success": None,
            },
            status_code=400,
        )
    return _redirect("/workers?created=1", status_code=HTTP_303_SEE_OTHER)


@app.get("/workers/{worker_id}", response_class=HTMLResponse)
def worker_detail_page(
    request: Request,
    worker_id: int,
    q: Optional[str] = None,
    type: Optional[str] = None,
    sort: Optional[str] = None,
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    try:
        account_type = _normalize_account_type(type) or ""
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        return _worker_detail_page_response(
            request,
            worker_id=int(worker_id),
            q=query,
            account_type="",
            sort="recent",
            error="Неверный фильтр аккаунтов",
            status_code=400,
        )
    return _worker_detail_page_response(
        request,
        worker_id=int(worker_id),
        q=query,
        account_type=account_type,
        sort=sort_value,
    )


@app.post("/workers/{worker_id}/update", response_class=HTMLResponse)
def worker_update(
    request: Request,
    worker_id: int,
    name: str = Form(...),
    username: str = Form(...),
    password: str = Form(""),
    _: None = Depends(require_auth),
):
    try:
        changed = db.update_worker(int(worker_id), name=name, username=username, password_or_empty=password)
        if not changed:
            return _worker_detail_page_response(
                request,
                worker_id=int(worker_id),
                q="",
                account_type="",
                error="Работник не найден",
                status_code=404,
            )
    except ValueError as exc:
        msg = str(exc)
        if msg == "username exists":
            msg = "Такой логин уже существует."
        elif msg == "name required":
            msg = "Заполни имя работника."
        elif msg == "username required":
            msg = "Заполни логин работника."
        return _worker_detail_page_response(
            request,
            worker_id=int(worker_id),
            q="",
            account_type="",
            error=msg,
            status_code=400,
        )
    return _redirect(f"/workers/{int(worker_id)}", status_code=HTTP_303_SEE_OTHER)


@app.post("/workers/{worker_id}/delete")
def worker_delete(worker_id: int, _: None = Depends(require_auth)):
    db.delete_worker(int(worker_id))
    return _redirect("/workers", status_code=HTTP_303_SEE_OTHER)


@app.get("/worker/login", response_class=HTMLResponse)
def worker_login_page(request: Request):
    if request.session.get("worker_id"):
        return _redirect("/worker/accounts", status_code=303)
    return templates.TemplateResponse("worker_login.html", {"request": request, "error": None})


@app.post("/worker/login", response_class=HTMLResponse)
def worker_login_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    username_clean = (username or "").strip()
    password_clean = (password or "").strip()
    worker = db.verify_worker_password(username_clean, password_clean)
    if not worker:
        return templates.TemplateResponse(
            "worker_login.html",
            {"request": request, "error": "Неверный логин или пароль"},
            status_code=401,
        )
    request.session["worker_id"] = int(worker["id"])
    request.session["worker_name"] = str(worker["name"] or "")
    return _redirect("/worker/accounts", status_code=303)


@app.get("/worker/logout")
def worker_logout(request: Request):
    request.session.pop("worker_id", None)
    request.session.pop("worker_name", None)
    return _redirect("/worker/login", status_code=303)


@app.get("/worker/accounts", response_class=HTMLResponse)
def worker_accounts_page(
    request: Request,
    q: Optional[str] = None,
    type: Optional[str] = None,
    sort: Optional[str] = None,
    created: Optional[str] = None,
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    try:
        account_type = _normalize_account_type(type) or ""
        sort_value = _normalize_account_list_sort(sort)
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type="",
            sort="recent",
            error="Неверный фильтр аккаунтов",
            status_code=400,
        )
    return _worker_accounts_page_response(
        request,
        worker_id=worker_id,
        q=query,
        account_type=account_type,
        sort=sort_value,
        success="Аккаунт добавлен. Ссылка создана автоматически." if created_flag else None,
    )


@app.get("/worker/accounts/{account_id}", response_class=HTMLResponse)
def worker_account_detail_page(
    request: Request,
    account_id: int,
    created: Optional[str] = None,
    return_to: Optional[str] = None,
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    return _worker_account_detail_page_response(
        request,
        worker_id=worker_id,
        account_id=int(account_id),
        return_to=return_to,
        success="Аккаунт добавлен. Ссылка создана автоматически." if created_flag else None,
    )


@app.post("/worker/accounts", response_class=HTMLResponse)
def worker_account_create(
    request: Request,
    type: str = Form(...),
    account_login: str = Form(...),
    account_password: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    email_password: str = Form(...),
    proxy: Optional[str] = Form(None),
    twofa: Optional[str] = Form(None),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_sort: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_sort_raw = (filter_sort or "").strip()
    try:
        account_type = _normalize_account_type(type)
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error="Неверный тип аккаунта",
            status_code=400,
        )

    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Имя профиля": username,
        "Почта": email,
        "Пароль почты": email_password,
    }
    for label, value in required.items():
        if not (value or "").strip():
            return _worker_accounts_page_response(
                request,
                worker_id=worker_id,
                q=query,
                account_type=filter_type_raw,
                sort=filter_sort_raw,
                error=f"Заполни поле: {label}",
                status_code=400,
            )

    duplicate = db.find_duplicate_account(account_type, account_login)
    if duplicate is not None:
        duplicate_row = dict(duplicate)
        owner_worker_id = duplicate_row.get("owner_worker_id")
        if owner_worker_id is not None and int(owner_worker_id) == worker_id:
            return _worker_accounts_page_response(
                request,
                worker_id=worker_id,
                q=query,
                account_type=filter_type_raw,
                sort=filter_sort_raw,
                error="Этот аккаунт уже добавлен у тебя.",
                status_code=400,
            )
        try:
            request_info = db.create_account_claim_request(int(duplicate_row["id"]), worker_id)
            message = _claim_request_feedback(bool(request_info.get("created")))
        except ValueError as exc:
            message = _claim_request_error_message(str(exc))
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            success=message,
        )

    assigned_serial = _allocate_instagram_emulator_serial(_prepare_instagram_emulator_serial_usage()) if account_type == "instagram" else ""
    created_info = db.create_account_with_default_link(
        account_type=account_type,
        account_login=account_login,
        account_password=account_password,
        username=username,
        email=email,
        email_password=email_password,
        proxy=proxy,
        twofa=twofa,
        instagram_emulator_serial=assigned_serial,
        owner_worker_id=worker_id,
        default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{(username or '').strip() or 'account'}",
        target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
    )
    new_id = int(created_info["account_id"])
    return_to = f"{_worker_accounts_redirect_url(query, filter_type_raw, filter_sort_raw)}#account-{new_id}"
    return _redirect(_build_detail_url(f"/worker/accounts/{new_id}?created=1", return_to), status_code=HTTP_303_SEE_OTHER)


@app.post("/worker/accounts/import", response_class=HTMLResponse)
async def worker_accounts_import(
    request: Request,
    import_type: str = Form(...),
    import_file: UploadFile = File(...),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    filter_sort: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_sort_raw = (filter_sort or "").strip()
    try:
        account_type = _normalize_account_type(import_type)
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error="Неверный тип аккаунта для импорта",
            status_code=400,
        )

    if import_file is None or not (import_file.filename or "").strip():
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error="Выбери файл для импорта",
            status_code=400,
        )

    raw_bytes = await import_file.read()
    if len(raw_bytes) > ACCOUNTS_IMPORT_MAX_BYTES:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error=f"Файл слишком большой. Лимит: {ACCOUNTS_IMPORT_MAX_BYTES // 1024 // 1024} MB",
            status_code=400,
        )

    rows, parse_errors = _parse_accounts_import_upload(raw_bytes)
    if not rows:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error="Не удалось импортировать файл",
            import_errors=parse_errors,
            status_code=400,
        )

    imported = 0
    requests_created = 0
    requests_existing = 0
    import_errors = list(parse_errors)
    serial_usage = _prepare_instagram_emulator_serial_usage() if account_type == "instagram" else None
    for row in rows:
        duplicate = db.find_duplicate_account(account_type, row["account_login"])
        if duplicate is not None:
            duplicate_row = dict(duplicate)
            owner_worker_id = duplicate_row.get("owner_worker_id")
            if owner_worker_id is not None and int(owner_worker_id) == worker_id:
                import_errors.append(f"Строка {row['line']}: этот аккаунт уже есть у тебя.")
                continue
            try:
                request_info = db.create_account_claim_request(int(duplicate_row["id"]), worker_id)
                if bool(request_info.get("created")):
                    requests_created += 1
                else:
                    requests_existing += 1
            except ValueError as exc:
                import_errors.append(f"Строка {row['line']}: {_claim_request_error_message(str(exc))}")
            continue
        try:
            db.create_account_with_default_link(
                account_type=account_type,
                account_login=row["account_login"],
                account_password=row["account_password"],
                username=row["username"],
                email=row["email"],
                email_password=row["email_password"],
                proxy="",
                twofa=row["twofa"],
                instagram_emulator_serial=_allocate_instagram_emulator_serial(serial_usage) if account_type == "instagram" else "",
                owner_worker_id=worker_id,
                default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{row['username'] or 'account'}",
                target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
            )
            imported += 1
        except Exception as exc:
            import_errors.append(f"Строка {row['line']}: {str(exc)}")

    summary = {
        "imported": imported,
        "requested": requests_created,
        "already_requested": requests_existing,
        "failed": len(import_errors),
        "filename": (import_file.filename or "").strip(),
    }
    success_parts = []
    if imported:
        success_parts.append(f"Импортировано: {imported}")
    if requests_created:
        success_parts.append(f"заявок отправлено: {requests_created}")
    if requests_existing:
        success_parts.append(f"уже ожидали: {requests_existing}")
    success = ". ".join(success_parts) + "." if success_parts else None
    if imported == 0 and requests_created == 0 and requests_existing == 0:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            sort=filter_sort_raw,
            error="Импорт не обработал ни одной строки",
            import_summary=summary,
            import_errors=import_errors,
            status_code=400,
        )

    return _worker_accounts_page_response(
        request,
        worker_id=worker_id,
        q=query,
        account_type=filter_type_raw,
        sort=filter_sort_raw,
        success=success,
        import_summary=summary,
        import_errors=import_errors,
    )


@app.post("/worker/accounts/{account_id}/update", response_class=HTMLResponse)
def worker_account_update(
    request: Request,
    account_id: int,
    type: str = Form(...),
    account_login: str = Form(...),
    account_password: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    email_password: str = Form(...),
    proxy: Optional[str] = Form(None),
    twofa: Optional[str] = Form(None),
    next_url: Optional[str] = Form(None),
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    fallback_url = _build_detail_url(f"/worker/accounts/{int(account_id)}", return_to)
    existing_account_row = db.get_account(int(account_id), owner_worker_id=worker_id)
    if existing_account_row is None:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )
    existing_account = dict(existing_account_row)
    try:
        account_type = _normalize_account_type(type)
    except ValueError:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error="Неверный тип аккаунта",
            status_code=400,
        )
    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Имя профиля": username,
        "Почта": email,
        "Пароль почты": email_password,
    }
    for label, value in required.items():
        if not (value or "").strip():
            return _worker_account_detail_page_response(
                request,
                worker_id=worker_id,
                account_id=int(account_id),
                return_to=return_to,
                error=f"Заполни поле: {label}",
                status_code=400,
            )

    duplicate = db.find_duplicate_account(account_type, account_login, exclude_account_id=int(account_id))
    if duplicate is not None:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error=_duplicate_account_message(dict(duplicate)),
            status_code=400,
        )

    try:
        db.update_account(
            account_id=int(account_id),
            account_type=account_type,
            account_login=account_login,
            account_password=account_password,
            username=username,
            email=email,
            email_password=email_password,
            mail_provider=str(existing_account.get("mail_provider") or "auto"),
            mail_auth_json=str(existing_account.get("mail_auth_json") or ""),
            proxy=proxy,
            twofa=twofa,
            owner_worker_id=worker_id,
        )
    except ValueError as exc:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error=_account_form_error_message(str(exc)),
            status_code=400,
        )
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/worker/accounts/{account_id}/delete")
def worker_account_delete(
    request: Request,
    account_id: int,
    next_url: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    if db.get_account(int(account_id), owner_worker_id=worker_id) is None:
        raise HTTPException(status_code=404, detail="Not Found")
    db.delete_account(int(account_id), owner_worker_id=worker_id)
    dest = _safe_next_url(next_url, fallback="/worker/accounts")
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/worker/accounts/{account_id}/links", response_class=HTMLResponse)
def worker_account_link_create(
    request: Request,
    account_id: int,
    name: Optional[str] = Form(None),
    next_url: Optional[str] = Form(None),
    return_to: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    if db.get_account(int(account_id), owner_worker_id=worker_id) is None:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )
    existing_links = db.list_account_links_with_stats(int(account_id), owner_worker_id=worker_id)
    if existing_links:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error="Дополнительные ссылки отключены. Для аккаунта доступна только одна рабочая ссылка.",
            status_code=409,
        )
    try:
        db.create_account_link(
            account_id=int(account_id),
            name=(name or "").strip() or "Основная ссылка",
            custom_code=None,
            target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
            owner_worker_id=worker_id,
        )
    except ValueError as exc:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error=str(exc),
            status_code=400,
        )
    fallback_url = _build_detail_url(f"/worker/accounts/{int(account_id)}", return_to)
    dest = _safe_next_url(next_url, fallback=fallback_url)
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


def _worker_owns_link(worker_id: int, code: str) -> bool:
    link = db.get_link(code)
    if not link:
        return False
    account_id = link["account_id"]
    if account_id is None:
        return False
    return db.get_account(int(account_id), owner_worker_id=int(worker_id)) is not None


@app.post("/worker/accounts/links/{code}/toggle")
def worker_account_link_toggle(
    request: Request,
    code: str,
    active: str = Form(...),
    next_url: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    if not _worker_owns_link(worker_id, code):
        raise HTTPException(status_code=404, detail="Not Found")
    on = (active or "").strip().lower() in {"1", "true", "yes", "on"}
    db.toggle_link_active(code, on)
    dest = _safe_next_url(next_url, fallback="/worker/accounts")
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.post("/worker/accounts/links/{code}/delete")
def worker_account_link_delete(
    request: Request,
    code: str,
    next_url: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    if not _worker_owns_link(worker_id, code):
        raise HTTPException(status_code=404, detail="Not Found")
    db.soft_delete_link(code)
    dest = _safe_next_url(next_url, fallback="/worker/accounts")
    return _redirect(dest, status_code=HTTP_303_SEE_OTHER)


@app.get("/broadcast", response_class=HTMLResponse)
def broadcast_page(request: Request, _: None = Depends(require_auth)):
    recipients = db.count_users_for_broadcast(
        installed=None,
        partner_id=None,
        manager_id=None,
        stage_key=None,
        stage_mode="reached",
    )
    return _broadcast_page_response(
        request,
        sent=None,
        failed=None,
        filters={"scope": "all", "stage_key": "", "stage_mode": "reached"},
        recipients=int(recipients or 0),
        test_chat_id=ADMIN_TEST_CHAT_ID,
        message="",
        mode=None,
        error=None,
        media_kind="",
    )


@app.get("/broadcast/count")
def broadcast_count(
    request: Request,
    scope: str = "all",
    stage_key: Optional[str] = None,
    stage_mode: Optional[str] = "reached",
    _: None = Depends(require_auth),
):
    scope_clean, installed, stage_key_clean, stage_mode_clean = _parse_broadcast_filters(
        scope, stage_key, stage_mode
    )
    n = db.count_users_for_broadcast(
        installed=installed,
        partner_id=None,
        manager_id=None,
        stage_key=stage_key_clean,
        stage_mode=stage_mode_clean,
    )
    return JSONResponse({"recipients": int(n or 0)})


@app.post("/broadcast/test", response_class=HTMLResponse)
def broadcast_test_send(
    request: Request,
    message: str = Form(""),
    test_chat_id: str = Form(...),
    scope: str = Form("all"),
    stage_key: Optional[str] = Form(None),
    stage_mode: Optional[str] = Form("reached"),
    media_kind: Optional[str] = Form(None),
    media_file: Optional[UploadFile] = File(None),
    _: None = Depends(require_auth),
):
    scope_clean, installed, stage_key_clean, stage_mode_clean = _parse_broadcast_filters(
        scope, stage_key, stage_mode
    )
    msg = (message or "").strip()
    media, media_error = _prepare_broadcast_media(media_kind, media_file)

    recipients = db.count_users_for_broadcast(
        installed=installed,
        partner_id=None,
        manager_id=None,
        stage_key=stage_key_clean,
        stage_mode=stage_mode_clean,
    )

    if media_error:
        return _broadcast_page_response(
            request,
            sent=None,
            failed=None,
            filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
            recipients=int(recipients or 0),
            test_chat_id=test_chat_id,
            message=msg,
            mode="test",
            error=media_error,
            media_kind=media_kind or "",
        )

    if not msg and not media:
        return _broadcast_page_response(
            request,
            sent=None,
            failed=None,
            filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
            recipients=int(recipients or 0),
            test_chat_id=test_chat_id,
            message=msg,
            mode="test",
            error="Добавь текст или фото/видео.",
            media_kind=media_kind or "",
        )

    try:
        test_uid = int(str(test_chat_id).strip())
    except Exception:
        return _broadcast_page_response(
            request,
            sent=None,
            failed=None,
            filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
            recipients=int(recipients or 0),
            test_chat_id=test_chat_id,
            message=msg,
            mode="test",
            error="Неверный test chat_id.",
            media_kind=media_kind or "",
        )

    ok = _send_message(test_uid, msg, media=media)
    scope_record = scope_clean
    if stage_key_clean:
        scope_record = f"{scope_clean}|{stage_mode_clean}:{stage_key_clean}"

    stored_message = msg
    if media:
        marker = f"[{media['kind']}: {media.get('filename') or 'media'}]"
        stored_message = (msg + "\n" + marker).strip()

    db.add_broadcast_run(
        scope=scope_record,
        partner_id=None,
        manager_id=None,
        recipients=int(recipients or 0),
        sent=1 if ok else 0,
        failed=0 if ok else 1,
        is_test=True,
        message=stored_message,
    )

    return _broadcast_page_response(
        request,
        sent=1 if ok else 0,
        failed=0 if ok else 1,
        filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
        recipients=int(recipients or 0),
        test_chat_id=str(test_chat_id),
        message=msg,
        mode="test",
        error=None,
        media_kind=media_kind or "",
    )


@app.post("/broadcast", response_class=HTMLResponse)
def broadcast_send(
    request: Request,
    message: str = Form(""),
    scope: str = Form("all"),
    stage_key: Optional[str] = Form(None),
    stage_mode: Optional[str] = Form("reached"),
    media_kind: Optional[str] = Form(None),
    media_file: Optional[UploadFile] = File(None),
    _: None = Depends(require_auth),
):
    scope_clean, installed, stage_key_clean, stage_mode_clean = _parse_broadcast_filters(
        scope, stage_key, stage_mode
    )
    msg = (message or "").strip()
    media, media_error = _prepare_broadcast_media(media_kind, media_file)

    recipients = db.count_users_for_broadcast(
        installed=installed,
        partner_id=None,
        manager_id=None,
        stage_key=stage_key_clean,
        stage_mode=stage_mode_clean,
    )

    if media_error:
        return _broadcast_page_response(
            request,
            sent=None,
            failed=None,
            filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
            recipients=int(recipients or 0),
            test_chat_id=ADMIN_TEST_CHAT_ID,
            message=msg,
            mode="send",
            error=media_error,
            media_kind=media_kind or "",
        )

    if not msg and not media:
        return _broadcast_page_response(
            request,
            sent=None,
            failed=None,
            filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
            recipients=int(recipients or 0),
            test_chat_id=ADMIN_TEST_CHAT_ID,
            message=msg,
            mode="send",
            error="Добавь текст или фото/видео.",
            media_kind=media_kind or "",
        )

    users = db.list_users_for_broadcast(
        installed=installed,
        partner_id=None,
        manager_id=None,
        stage_key=stage_key_clean,
        stage_mode=stage_mode_clean,
        limit=20000,
    )

    sent = 0
    failed = 0
    for u in users:
        if _send_message(int(u["user_id"]), msg, media=media):
            sent += 1
        else:
            failed += 1

    scope_record = scope_clean
    if stage_key_clean:
        scope_record = f"{scope_clean}|{stage_mode_clean}:{stage_key_clean}"

    stored_message = msg
    if media:
        marker = f"[{media['kind']}: {media.get('filename') or 'media'}]"
        stored_message = (msg + "\n" + marker).strip()

    db.add_broadcast_run(
        scope=scope_record,
        partner_id=None,
        manager_id=None,
        recipients=len(users),
        sent=sent,
        failed=failed,
        is_test=False,
        message=stored_message,
    )

    return _broadcast_page_response(
        request,
        sent=sent,
        failed=failed,
        filters={"scope": scope_clean, "stage_key": stage_key_clean or "", "stage_mode": stage_mode_clean},
        recipients=len(users),
        test_chat_id=ADMIN_TEST_CHAT_ID,
        message=msg,
        mode="send",
        error=None,
        media_kind=media_kind or "",
    )


# ============================================================================
# TELEGRAM BOT WEBHOOK
# ============================================================================

@app.post("/bot/webhook")
async def telegram_webhook(request: Request):
    """
    Webhook endpoint для получения обновлений от Telegram
    """
    try:
        import bot_webhook
        
        # Получаем JSON данные от Telegram
        update_data = await request.json()
        
        # Обрабатываем обновление
        result = await bot_webhook.process_update(update_data)
        
        return JSONResponse(content=result)
    except Exception as e:
        logger.error(f"Ошибка webhook: {e}", exc_info=True)
        return JSONResponse(content={"status": "error", "message": str(e)}, status_code=500)


@app.get("/bot/webhook/health")
async def webhook_health():
    """
    Проверка работоспособности webhook
    """
    return JSONResponse(content={
        "status": "ok",
        "bot_token_configured": bool(BOT_TOKEN),
        "webhook_mode": True
    })
