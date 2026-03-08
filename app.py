import csv
import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote, quote_plus

import requests
from dotenv import load_dotenv
from fastapi import Body, Depends, FastAPI, File, Form, Header, HTTPException, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.status import HTTP_303_SEE_OTHER

import db
import mail_service

load_dotenv()

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USERNAME = (os.getenv("BOT_USERNAME", "checkayugrambot") or "checkayugrambot").strip().lstrip("@")
ADMIN_TEST_CHAT_ID = os.getenv("ADMIN_TEST_CHAT_ID", "").strip()
SESSION_SECRET = os.getenv("SESSION_SECRET", "change-me")
SESSION_MAX_AGE_SECONDS = int(os.getenv("SESSION_MAX_AGE_SECONDS", str(60 * 60 * 24 * 30)))
MAX_BROADCAST_MEDIA_BYTES = int(os.getenv("MAX_BROADCAST_MEDIA_BYTES", str(45 * 1024 * 1024)))
ADMIN_BASE_PATH_RAW = os.getenv("ADMIN_BASE_PATH", "")
HELPER_API_KEY = os.getenv("HELPER_API_KEY", "").strip()
HELPER_TICKET_TTL_SECONDS = int(os.getenv("HELPER_TICKET_TTL_SECONDS", "120"))
INSTAGRAM_APP_HELPER_OPEN_URL = (
    os.getenv(
        "INSTAGRAM_APP_HELPER_OPEN_URL",
        os.getenv("INSTAGRAM_HELPER_OPEN_URL", "http://127.0.0.1:17374/open"),
    )
    or "http://127.0.0.1:17374/open"
).strip()
INSTAGRAM_PUBLISH_SOURCE_DIR = (
    os.getenv("INSTAGRAM_PUBLISH_SOURCE_DIR", "/Users/daniildatlov/Desktop/видео ауграм ")
    or "/Users/daniildatlov/Desktop/видео ауграм "
)
PUBLISH_N8N_WEBHOOK_URL = (os.getenv("PUBLISH_N8N_WEBHOOK_URL", "") or "").strip()
PUBLISH_STAGING_DIR = (
    os.getenv("PUBLISH_STAGING_DIR", str(Path.home() / "SlezhkaPublishStaging"))
    or str(Path.home() / "SlezhkaPublishStaging")
).strip()
PUBLISH_BASE_URL = (os.getenv("PUBLISH_BASE_URL", "") or "").strip().rstrip("/")
PUBLISH_SHARED_SECRET = (
    os.getenv("PUBLISH_SHARED_SECRET", HELPER_API_KEY or SESSION_SECRET)
    or HELPER_API_KEY
    or SESSION_SECRET
).strip()
PUBLISH_WEBHOOK_MAX_AGE_SECONDS = int(os.getenv("PUBLISH_WEBHOOK_MAX_AGE_SECONDS", "300"))
PUBLISH_RUNNER_API_KEY = (os.getenv("PUBLISH_RUNNER_API_KEY", HELPER_API_KEY) or HELPER_API_KEY).strip()
PUBLISH_RUNNER_LEASE_SECONDS = int(os.getenv("PUBLISH_RUNNER_LEASE_SECONDS", "900"))
PUBLISH_DEFAULT_WORKFLOW = (os.getenv("PUBLISH_DEFAULT_WORKFLOW", "default") or "default").strip()


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
ACCOUNT_MAIL_STATUS_LABELS = {
    "never_checked": "Не проверялась",
    "ok": "Почта OK",
    "auth_error": "Ошибка входа",
    "connect_error": "Ошибка подключения",
    "empty": "Писем нет",
    "unsupported": "Неподдерживаемая почта",
}
ACCOUNT_INSTAGRAM_LAUNCH_STATUS_LABELS = {
    "idle": "Не запускался",
    "login_submitted": "Логин отправлен",
    "manual_2fa_required": "Нужен 2FA",
    "challenge_required": "Нужен challenge",
    "invalid_password": "Неверный пароль",
    "helper_error": "Ошибка helper",
}
ACCOUNT_INSTAGRAM_PUBLISH_STATUS_LABELS = {
    "idle": "Не запускался",
    "preparing": "Подготовка",
    "login_required": "Нужен вход",
    "importing_media": "Импорт медиа",
    "opening_reel_flow": "Открываю Reel",
    "selecting_media": "Выбираю видео",
    "publishing": "Публикую",
    "published": "Опубликовано",
    "no_source_video": "Нет видео",
    "publish_error": "Ошибка публикации",
}
PUBLISH_BATCH_STATE_LABELS = {
    "generating": "Генерация",
    "publishing": "Публикация",
    "completed": "Завершён",
    "completed_with_errors": "Завершён с ошибками",
    "failed_generation": "Ошибка генерации",
    "canceled": "Отменён",
}
PUBLISH_JOB_STATE_LABELS = {
    "queued": "В очереди",
    "leased": "Runner взял job",
    "preparing": "Подготовка",
    "importing_media": "Импорт медиа",
    "opening_reel_flow": "Открываю Reel",
    "selecting_media": "Выбор видео",
    "publishing": "Публикация",
    "published": "Опубликовано",
    "failed": "Ошибка",
    "canceled": "Отменено",
}
ACCOUNTS_IMPORT_MAX_BYTES = int(os.getenv("ACCOUNTS_IMPORT_MAX_BYTES", str(2 * 1024 * 1024)))

app = FastAPI()
templates = Jinja2Templates(directory="templates")
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


@app.on_event("startup")
def _startup() -> None:
    db.init_db()
    Path(PUBLISH_STAGING_DIR).expanduser().mkdir(parents=True, exist_ok=True)


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


def _account_rotation_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "review"
    return ACCOUNT_ROTATION_STATE_LABELS.get(value, "На проверке"), value


def _account_views_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "unknown"
    return ACCOUNT_VIEWS_STATE_LABELS.get(value, "Не задано"), value


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
        "importing_media": "wait",
        "opening_reel_flow": "wait",
        "selecting_media": "wait",
        "publishing": "wait",
        "published": "on",
        "no_source_video": "review",
        "publish_error": "off",
    }.get(value, "unknown")
    return label, status_class


def _publish_batch_state_meta(state: Optional[str]) -> tuple[str, str]:
    value = (state or "").strip().lower() or "generating"
    label = PUBLISH_BATCH_STATE_LABELS.get(value, "Неизвестно")
    status_class = {
        "generating": "wait",
        "publishing": "wait",
        "completed": "on",
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
        "failed": "off",
        "canceled": "review",
    }.get(value, "unknown")
    return label, status_class


def _publish_batch_is_terminal(state: Optional[str]) -> bool:
    value = (state or "").strip().lower()
    return value in {"completed", "completed_with_errors", "failed_generation", "canceled"}


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
    if not parts:
        return "/accounts"
    return "/accounts?" + "&".join(parts)


def _worker_detail_redirect_url(worker_id: int, q: str, account_type: str) -> str:
    parts = []
    if q:
        parts.append(f"q={quote_plus(q)}")
    if account_type:
        parts.append(f"type={quote_plus(account_type)}")
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


def _admin_public_base_url(request: Request) -> str:
    if PUBLISH_BASE_URL:
        return PUBLISH_BASE_URL
    base = str(request.base_url).rstrip("/")
    return f"{base}{ADMIN_BASE_PATH}" if ADMIN_BASE_PATH else base


def _absolute_admin_url(request: Request, path: str) -> str:
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return _admin_public_base_url(request) + suffix


def _publish_staging_root() -> Path:
    root = Path(PUBLISH_STAGING_DIR).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _publish_batch_stage_dir(batch_id: int) -> Path:
    directory = _publish_staging_root() / str(int(batch_id))
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def _normalize_publish_artifact_path(batch_id: int, raw_path: str) -> Path:
    value = (raw_path or "").strip()
    if not value:
        raise ValueError("Artifact path is required")
    batch_dir = _publish_batch_stage_dir(int(batch_id)).resolve()
    staging_root = _publish_staging_root().resolve()
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = batch_dir / candidate
    resolved = candidate.resolve()
    if batch_dir not in resolved.parents and resolved != batch_dir and staging_root not in resolved.parents:
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
    error: Optional[str] = None,
    success: Optional[str] = None,
    import_summary: Optional[dict] = None,
    import_errors: Optional[list[str]] = None,
    status_code: int = 200,
) -> HTMLResponse:
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

    raw_rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=worker_filter_id,
        rotation_state=rotation_filter_value or None,
        views_state=views_filter_value or None,
        limit=500,
    )
    rows = []
    for raw in raw_rows:
        account = dict(raw)
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
        account["rotation_state_label"] = rotation_label
        account["rotation_state_class"] = rotation_class
        account["views_state_label"] = views_label
        account["views_state_class"] = views_class
        account["mail_status_label"] = mail_label
        account["mail_status_class"] = mail_class
        account["instagram_launch_status_label"] = launch_label
        account["instagram_launch_status_class"] = launch_class
        account["instagram_publish_status_label"] = publish_label
        account["instagram_publish_status_class"] = publish_class
        account["identity_handle"] = _account_identity_handle(account)
        owner_name = str(account.get("owner_worker_name") or "").strip()
        owner_username = str(account.get("owner_worker_username") or "").strip()
        account["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
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
            "accounts_list_url": _accounts_redirect_url(q, account_type, worker_filter_value, rotation_filter_value, views_filter_value),
            "workers": workers,
            "type_options": ACCOUNT_TYPE_OPTIONS,
            "rotation_state_options": ACCOUNT_ROTATION_STATE_OPTIONS,
            "views_state_options": ACCOUNT_VIEWS_STATE_OPTIONS,
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
    launch_label, launch_class = _account_instagram_launch_status_meta(account.get("instagram_launch_status"))
    publish_label, publish_class = _account_instagram_publish_status_meta(account.get("instagram_publish_status"))
    account["rotation_state_label"] = rotation_label
    account["rotation_state_class"] = rotation_class
    account["views_state_label"] = views_label
    account["views_state_class"] = views_class
    account["mail_status_label"] = mail_label
    account["mail_status_class"] = mail_class
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
    mail_messages = [dict(r) for r in db.list_account_mail_messages(int(account["id"]), limit=mail_service.MAIL_FETCH_LIMIT)]
    latest_mail = mail_messages[0] if mail_messages else None
    workers = [dict(w) for w in db.list_workers_compact(limit=500)]
    owner_name = str(account.get("owner_worker_name") or "").strip()
    owner_username = str(account.get("owner_worker_username") or "").strip()
    account["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")

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
            "workers": workers,
            "back_url": back_url,
            "detail_self_url": detail_self_url,
            "return_to": return_to_clean,
            "instagram_publish_source_dir": INSTAGRAM_PUBLISH_SOURCE_DIR,
            "instagram_publish_source_info_url": _build_instagram_helper_local_url("/publish-source/latest"),
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
    return batch


def _publishing_page_response(
    request: Request,
    *,
    error: Optional[str] = None,
    success: Optional[str] = None,
    status_code: int = 200,
) -> HTMLResponse:
    accounts = []
    for raw in db.list_publish_ready_accounts(limit=500):
        row = dict(raw)
        publish_label, publish_class = _account_instagram_publish_status_meta(row.get("instagram_publish_status"))
        row["instagram_publish_status_label"] = publish_label
        row["instagram_publish_status_class"] = publish_class
        owner_name = str(row.get("owner_worker_name") or "").strip()
        owner_username = str(row.get("owner_worker_username") or "").strip()
        row["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
        row["identity_handle"] = _account_identity_handle(row)
        accounts.append(row)

    batches = [_decorate_publish_batch(dict(raw)) for raw in db.list_publish_batches(limit=20)]
    return templates.TemplateResponse(
        "publishing.html",
        {
            "request": request,
            "accounts": accounts,
            "batches": batches,
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
    batch_row = db.get_publish_batch(int(batch_id))
    if batch_row is None:
        return templates.TemplateResponse(
            "publishing_batch_detail.html",
            {
                "request": request,
                "batch": None,
                "accounts": [],
                "artifacts": [],
                "jobs": [],
                "events": [],
                "error": error or "Batch не найден",
                "success": success,
                "poll_interval_seconds": 0,
                "batch_stage_dir": str(_publish_batch_stage_dir(int(batch_id))),
            },
            status_code=404,
        )

    batch = _decorate_publish_batch(dict(batch_row))
    accounts = []
    for raw in db.list_publish_batch_accounts(int(batch_id)):
        row = dict(raw)
        publish_label, publish_class = _account_instagram_publish_status_meta(row.get("instagram_publish_status"))
        row["instagram_publish_status_label"] = publish_label
        row["instagram_publish_status_class"] = publish_class
        owner_name = str(row.get("owner_worker_name") or "").strip()
        owner_username = str(row.get("owner_worker_username") or "").strip()
        row["owner_label"] = f"{owner_name} (@{owner_username})" if owner_username else (owner_name or "Без работника")
        accounts.append(row)

    artifacts = []
    for raw in db.list_publish_artifacts(int(batch_id)):
        row = dict(raw)
        size_bytes = int(row.get("size_bytes") or 0)
        row["size_label"] = f"{size_bytes / (1024 * 1024):.1f} MB" if size_bytes else "—"
        duration = row.get("duration_seconds")
        row["duration_label"] = f"{float(duration):.1f} s" if duration not in (None, "") else "—"
        artifacts.append(row)

    jobs = []
    for raw in db.list_publish_jobs(int(batch_id)):
        row = dict(raw)
        label, css = _publish_job_state_meta(row.get("state"))
        row["state_label"] = label
        row["state_class"] = css
        row["identity_handle"] = _account_identity_handle({"username": row.get("account_username"), "account_login": row.get("account_login")})
        jobs.append(row)

    events = []
    for raw in db.list_publish_job_events(int(batch_id), limit=200):
        row = dict(raw)
        payload_json = str(row.get("payload_json") or "").strip()
        row["payload_preview"] = payload_json[:220] + ("…" if len(payload_json) > 220 else "") if payload_json else ""
        events.append(row)

    return templates.TemplateResponse(
        "publishing_batch_detail.html",
        {
            "request": request,
            "batch": batch,
            "accounts": accounts,
            "artifacts": artifacts,
            "jobs": jobs,
            "events": events,
            "error": error,
            "success": success,
            "poll_interval_seconds": 5 if not batch["is_terminal"] else 0,
            "batch_stage_dir": str(_publish_batch_stage_dir(int(batch_id))),
        },
        status_code=status_code,
    )


def _trigger_publish_generation(request: Request, batch_id: int) -> None:
    if not PUBLISH_N8N_WEBHOOK_URL:
        raise RuntimeError("PUBLISH_N8N_WEBHOOK_URL не настроен.")
    batch_row = db.get_publish_batch(int(batch_id))
    if batch_row is None:
        raise RuntimeError("Batch не найден.")
    accounts = [
        {
            "account_id": int(row["id"]),
            "username": str(row["username"] or ""),
            "account_login": str(row["account_login"] or ""),
            "emulator_serial": str(row["instagram_emulator_serial"] or ""),
        }
        for row in db.list_publish_batch_accounts(int(batch_id))
    ]
    batch_dir = _publish_batch_stage_dir(int(batch_id))
    payload = {
        "event": "start_generation",
        "batch_id": int(batch_id),
        "workflow_key": str(batch_row["workflow_key"] or PUBLISH_DEFAULT_WORKFLOW),
        "callback_url": _absolute_admin_url(request, "/api/internal/publishing/n8n"),
        "staging_dir": str(batch_dir),
        "accounts": accounts,
    }
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    response = requests.post(
        PUBLISH_N8N_WEBHOOK_URL,
        data=body,
        headers=_signed_publish_headers(body),
        timeout=25,
    )
    response.raise_for_status()
    response_note = (response.text or "").strip()
    detail = f"n8n принял batch. Папка batch: {batch_dir}."
    if response_note:
        detail = f"{detail} Ответ: {response_note[:140]}"
    db.mark_publish_generation_started(int(batch_id), detail=detail)


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
            resp = requests.post(url, data=data, files=files, timeout=40)
        else:
            if not text_clean:
                return False
            url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
            resp = requests.post(
                url,
                data={"chat_id": user_id, "text": text_clean, "parse_mode": "HTML"},
                timeout=10,
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
    except ValueError:
        return _accounts_page_response(
            request,
            q=query,
            account_type="",
            worker_filter=worker_filter,
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
        success="Аккаунт добавлен. Ссылка создана автоматически." if created_flag else None,
    )


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


@app.get("/publishing", response_class=HTMLResponse)
def publishing_page(request: Request, _: None = Depends(require_auth)):
    return _publishing_page_response(request)


@app.post("/publishing/batches", response_class=HTMLResponse)
def publishing_batch_create(
    request: Request,
    account_ids: Optional[list[str]] = Form(None),
    _: None = Depends(require_auth),
):
    selected_ids: list[int] = []
    for raw in account_ids or []:
        value = (raw or "").strip()
        if not value:
            continue
        try:
            selected_ids.append(int(value))
        except Exception:
            return _publishing_page_response(request, error="Неверный account_id в batch.", status_code=400)
    if not selected_ids:
        return _publishing_page_response(request, error="Выбери хотя бы один Instagram-аккаунт для batch.", status_code=400)

    try:
        created = db.create_publish_batch(
            selected_ids,
            created_by_admin=ADMIN_USER,
            workflow_key=PUBLISH_DEFAULT_WORKFLOW,
        )
    except ValueError as exc:
        return _publishing_page_response(request, error=str(exc), status_code=400)

    batch_id = int(created["batch_id"])
    try:
        _trigger_publish_generation(request, batch_id)
    except Exception as exc:
        db.mark_publish_generation_failed(batch_id, f"Не удалось стартовать n8n workflow: {exc}")
        return _publishing_batch_detail_page_response(
            request,
            batch_id=batch_id,
            error=f"Batch создан, но n8n не стартовал: {exc}",
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

    email_value = str(account["email"] or "").strip()
    email_password_value = str(account["email_password"] or "").strip()
    if not email_value or not email_password_value:
        db.update_account_mail_state(
            int(account_id),
            mail_provider=str(account["mail_provider"] or "auto"),
            mail_status="auth_error",
            mail_last_error="Не заполнены почта или пароль почты.",
        )
        return _account_detail_page_response(
            request,
            account_id=int(account_id),
            return_to=return_to,
            error="Заполни почту и пароль почты перед проверкой.",
            status_code=400,
        )

    result = mail_service.fetch_recent_messages(
        email_address=email_value,
        email_password=email_password_value,
        provider=str(account["mail_provider"] or "auto"),
        limit=mail_service.MAIL_FETCH_LIMIT,
    )
    db.update_account_mail_state(
        int(account_id),
        mail_provider=str(result.get("provider") or account["mail_provider"] or "auto"),
        mail_status=str(result.get("status") or "connect_error"),
        mail_last_error=str(result.get("error") or ""),
    )
    if str(result.get("status") or "") in {"ok", "empty"}:
        db.replace_account_mail_messages(int(account_id), list(result.get("messages") or []))

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
    return JSONResponse(
        {
            "ticket": payload["ticket"],
            "target": payload["target"],
            "account_id": payload["account_id"],
            "account_login": account["account_login"],
            "account_password": account["account_password"],
            "twofa": account["twofa"],
            "username": account["username"],
            "profile_url": _build_social_profile_url("instagram", account["username"]) or "https://www.instagram.com/",
        }
    )


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

    detail = str(payload.get("detail") or "").strip()
    if event == "generation_started":
        metrics = db.mark_publish_generation_started(batch_id, detail=detail or None)
        return JSONResponse({"ok": True, "event": event, "batch_id": batch_id, "state": metrics.get("state")})
    if event == "generation_completed":
        metrics = db.mark_publish_generation_completed(batch_id, detail=detail or None)
        return JSONResponse({"ok": True, "event": event, "batch_id": batch_id, "state": metrics.get("state")})
    if event == "generation_failed":
        db.mark_publish_generation_failed(batch_id, detail or "n8n сообщил об ошибке генерации.")
        batch = db.get_publish_batch(batch_id)
        return JSONResponse({"ok": True, "event": event, "batch_id": batch_id, "state": str(batch["state"] or "failed_generation")})
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
        result = db.register_publish_artifact(
            batch_id,
            path=str(normalized_path),
            filename=filename,
            checksum=checksum,
            size_bytes=int(size_bytes) if size_bytes not in (None, "") else None,
            duration_seconds=float(duration_seconds) if duration_seconds not in (None, "") else None,
        )
        return JSONResponse({"ok": True, "event": event, "batch_id": batch_id, **result})
    raise HTTPException(status_code=400, detail="Unsupported publish event")


@app.post("/api/internal/publishing/jobs/lease")
def publishing_job_lease(
    payload: Optional[dict] = Body(None),
    _: None = Depends(require_publish_runner_api_key),
):
    runner_name = str((payload or {}).get("runner_name") or "publish-runner").strip() or "publish-runner"
    job = db.lease_next_publish_job(runner_name=runner_name, lease_seconds=PUBLISH_RUNNER_LEASE_SECONDS)
    if job is None:
        return Response(status_code=204)
    return JSONResponse({"ok": True, "job": job})


@app.post("/api/internal/publishing/jobs/{job_id}/status")
def publishing_job_status_update(
    job_id: int,
    payload: dict = Body(...),
    _: None = Depends(require_publish_runner_api_key),
):
    state_raw = str(payload.get("state") or "").strip()
    if not state_raw:
        raise HTTPException(status_code=400, detail="state is required")
    try:
        result = db.update_publish_job_state(
            int(job_id),
            state=state_raw,
            detail=str(payload.get("detail") or "").strip(),
            last_file=str(payload.get("last_file") or "").strip() or None,
            runner_name=str(payload.get("runner_name") or "").strip() or None,
            payload={
                "emulator_serial": str(payload.get("emulator_serial") or "").strip(),
                "source_path": str(payload.get("source_path") or "").strip(),
            },
            lease_seconds=PUBLISH_RUNNER_LEASE_SECONDS,
        )
    except ValueError as exc:
        msg = str(exc)
        status_code = 409 if msg == "job already finished" else 404 if msg == "job not found" else 400
        raise HTTPException(status_code=status_code, detail=msg) from exc
    return JSONResponse({"ok": True, **result})


@app.post("/accounts", response_class=HTMLResponse)
def account_create(
    request: Request,
    type: str = Form(...),
    account_login: str = Form(...),
    account_password: str = Form(...),
    username: str = Form(...),
    email: str = Form(...),
    email_password: str = Form(...),
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
    owner_worker_id: Optional[str] = Form(None),
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    filter_rotation_state_raw = (filter_rotation_state or "").strip()
    filter_views_state_raw = (filter_views_state or "").strip()

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
            error="Выбери тип аккаунта",
            status_code=400,
        )

    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Username": username,
        "Почта": email,
        "Пароль почты": email_password,
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
                error=f"Заполни поле: {label}",
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
            error=_duplicate_account_message(dict(duplicate)),
            status_code=400,
        )

    created_info = db.create_account_with_default_link(
        account_type=account_type,
        account_login=account_login,
        account_password=account_password,
        username=username,
        email=email,
        email_password=email_password,
        proxy=proxy,
        twofa=twofa,
        instagram_emulator_serial=instagram_emulator_serial,
        rotation_state=rotation_state_value,
        views_state=views_state_value,
        owner_worker_id=owner_id,
        default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{(username or '').strip() or 'account'}",
        target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
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
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    filter_worker_raw = (filter_worker or "").strip()
    filter_rotation_state_raw = (filter_rotation_state or "").strip()
    filter_views_state_raw = (filter_views_state or "").strip()

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
            error="Не удалось импортировать файл",
            import_errors=parse_errors,
            status_code=400,
        )

    imported = 0
    import_errors = list(parse_errors)
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
    email_password: str = Form(...),
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
        "Username": username,
        "Почта": email,
        "Пароль почты": email_password,
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


def _worker_accounts_redirect_url(q: str, account_type: str) -> str:
    parts = []
    if q:
        parts.append(f"q={quote_plus(q)}")
    if account_type:
        parts.append(f"type={quote_plus(account_type)}")
    if not parts:
        return "/worker/accounts"
    return "/worker/accounts?" + "&".join(parts)


def _worker_accounts_page_response(
    request: Request,
    *,
    worker_id: int,
    q: str,
    account_type: str,
    error: Optional[str] = None,
    success: Optional[str] = None,
    import_summary: Optional[dict] = None,
    import_errors: Optional[list[str]] = None,
    status_code: int = 200,
) -> HTMLResponse:
    list_url = _worker_accounts_redirect_url(q, account_type)
    rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=int(worker_id),
        limit=500,
    )
    accounts = []
    for raw in rows:
        account = dict(raw)
        account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
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
            "accounts_list_url": list_url,
            "type_options": ACCOUNT_TYPE_OPTIONS,
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
    list_url = _worker_detail_redirect_url(worker_id, q, account_type)
    accounts_rows = db.list_accounts_compact(
        q=q,
        account_type=account_type or None,
        owner_worker_id=int(worker_id),
        limit=500,
    )
    accounts = []
    for raw in accounts_rows:
        account = dict(raw)
        account["type_label"] = ACCOUNT_TYPE_LABELS.get(str(account.get("type") or ""), str(account.get("type") or "").upper())
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
            "type_options": ACCOUNT_TYPE_OPTIONS,
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
    _: None = Depends(require_auth),
):
    query = (q or "").strip()
    try:
        account_type = _normalize_account_type(type) or ""
    except ValueError:
        return _worker_detail_page_response(
            request,
            worker_id=int(worker_id),
            q=query,
            account_type="",
            error="Неверный тип аккаунта",
            status_code=400,
        )
    return _worker_detail_page_response(
        request,
        worker_id=int(worker_id),
        q=query,
        account_type=account_type,
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
    created: Optional[str] = None,
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    created_flag = (created or "").strip().lower() in {"1", "true", "yes", "ok"}
    try:
        account_type = _normalize_account_type(type) or ""
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type="",
            error="Неверный тип аккаунта",
            status_code=400,
        )
    return _worker_accounts_page_response(
        request,
        worker_id=worker_id,
        q=query,
        account_type=account_type,
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
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    try:
        account_type = _normalize_account_type(type)
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            error="Неверный тип аккаунта",
            status_code=400,
        )

    required = {
        "Логин аккаунта": account_login,
        "Пароль аккаунта": account_password,
        "Username": username,
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
            success=message,
        )

    created_info = db.create_account_with_default_link(
        account_type=account_type,
        account_login=account_login,
        account_password=account_password,
        username=username,
        email=email,
        email_password=email_password,
        proxy=proxy,
        twofa=twofa,
        owner_worker_id=worker_id,
        default_link_name=f"{ACCOUNT_TYPE_LABELS.get(account_type, account_type.title())} @{(username or '').strip() or 'account'}",
        target_url=f"https://t.me/{BOT_USERNAME}?start={{code}}",
    )
    new_id = int(created_info["account_id"])
    return_to = f"{_worker_accounts_redirect_url(query, filter_type_raw)}#account-{new_id}"
    return _redirect(_build_detail_url(f"/worker/accounts/{new_id}?created=1", return_to), status_code=HTTP_303_SEE_OTHER)


@app.post("/worker/accounts/import", response_class=HTMLResponse)
async def worker_accounts_import(
    request: Request,
    import_type: str = Form(...),
    import_file: UploadFile = File(...),
    q: Optional[str] = Form(None),
    filter_type: Optional[str] = Form(None),
    _: None = Depends(require_worker_auth),
):
    worker_id = int(request.session["worker_id"])
    query = (q or "").strip()
    filter_type_raw = (filter_type or "").strip()
    try:
        account_type = _normalize_account_type(import_type)
    except ValueError:
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
            error="Неверный тип аккаунта для импорта",
            status_code=400,
        )

    if import_file is None or not (import_file.filename or "").strip():
        return _worker_accounts_page_response(
            request,
            worker_id=worker_id,
            q=query,
            account_type=filter_type_raw,
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
            error="Не удалось импортировать файл",
            import_errors=parse_errors,
            status_code=400,
        )

    imported = 0
    requests_created = 0
    requests_existing = 0
    import_errors = list(parse_errors)
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
    if db.get_account(int(account_id), owner_worker_id=worker_id) is None:
        return _worker_account_detail_page_response(
            request,
            worker_id=worker_id,
            account_id=int(account_id),
            return_to=return_to,
            error="Аккаунт не найден",
            status_code=404,
        )
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
        "Username": username,
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
            error=str(exc),
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
