import atexit
import logging
import os
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import http_utils
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright

load_dotenv()

HELPER_BIND = (os.getenv("HELPER_BIND", "127.0.0.1:17373") or "127.0.0.1:17373").strip()
HELPER_HOST, _, HELPER_PORT = HELPER_BIND.partition(":")
HELPER_PORT_INT = int(HELPER_PORT or "17373")
SLEZHKA_ADMIN_BASE_URL = (
    os.getenv("SLEZHKA_ADMIN_BASE_URL", "http://4abbf189760e.vps.myjino.ru/slezhka")
    or "http://4abbf189760e.vps.myjino.ru/slezhka"
).strip().rstrip("/")
HELPER_API_KEY = (os.getenv("HELPER_API_KEY", "") or "").strip()
PROFILE_ROOT = Path.home() / "Library" / "Application Support" / "SlezhkaHelper" / "profiles_mobile"
LOG_DIR = Path.home() / "Library" / "Logs" / "SlezhkaHelper"
LOG_DIR.mkdir(parents=True, exist_ok=True)
PROFILE_ROOT.mkdir(parents=True, exist_ok=True)

LOG_FILE = LOG_DIR / "instagram-helper.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("instagram_helper")

app = FastAPI(title="Slezhka Instagram Helper")

LOGIN_FORM_WAIT_SECONDS = 24
MANUAL_SESSION_STATES = {"manual_step_required", "login_clicked"}
INSTAGRAM_LOGIN_URL = "https://www.instagram.com/accounts/login/?force_authentication=1"
MOBILE_DEVICE_NAME = "iPhone 13"

TASK_QUEUE = queue.Queue()
WORKER_THREAD: Optional[threading.Thread] = None
REGISTRY_LOCK = threading.RLock()


@dataclass
class SessionEntry:
    account_id: int
    context: Optional[Any] = None
    page: Optional[Any] = None
    state: str = "queued"
    flow_running: bool = False
    last_activity_at: float = field(default_factory=time.time)


SESSIONS: dict[int, SessionEntry] = {}


def _build_ticket_url(ticket: str) -> str:
    encoded = quote((ticket or "").strip(), safe="")
    return f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/launch-ticket/{encoded}?target=instagram_login"


def _render_status_page(title: str, message: str, detail: str = "") -> HTMLResponse:
    extra = f"<p>{detail}</p>" if detail else ""
    return HTMLResponse(
        f"""
        <!doctype html>
        <html lang="ru">
          <head>
            <meta charset="utf-8">
            <title>{title}</title>
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <style>
              body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; background: #f4f5f7; color: #111; }}
              .wrap {{ max-width: 640px; margin: 48px auto; padding: 24px; }}
              .card {{ background: #fff; border-radius: 20px; padding: 24px; box-shadow: 0 18px 44px rgba(0,0,0,.08); }}
              h1 {{ margin: 0 0 12px; font-size: 24px; }}
              p {{ margin: 0 0 10px; line-height: 1.5; }}
              code {{ background: #f0f2f5; padding: 2px 8px; border-radius: 999px; }}
            </style>
          </head>
          <body>
            <div class="wrap">
              <div class="card">
                <h1>{title}</h1>
                <p>{message}</p>
                {extra}
                <p>Helper открывает мобильный Instagram в режиме iPhone Safari.</p>
                <p>Окно браузера не будет закрыто автоматически.</p>
                <p>Лог helper: <code>~/Library/Logs/SlezhkaHelper/instagram-helper.log</code></p>
              </div>
            </div>
          </body>
        </html>
        """
    )


def _fetch_ticket_payload(ticket: str) -> dict:
    if not HELPER_API_KEY:
        raise RuntimeError("HELPER_API_KEY is not configured")
    response = http_utils.request_with_retry(
        "GET",
        _build_ticket_url(ticket),
        headers={"X-Helper-Api-Key": HELPER_API_KEY},
        timeout=25,
        allow_retry=True,
        log_context="helper_ticket_fetch",
    )
    if response.status_code == 410:
        raise RuntimeError("Ticket expired")
    if response.status_code == 409:
        raise RuntimeError("Ticket already used")
    if response.status_code == 404:
        raise RuntimeError("Ticket not found")
    if response.status_code == 401:
        raise RuntimeError("Invalid HELPER_API_KEY")
    response.raise_for_status()
    return response.json()


def _first_visible(page, selectors: list[str]):
    for selector in selectors:
        locator = page.locator(selector).first
        try:
            locator.wait_for(state="visible", timeout=1500)
            return locator
        except Exception:
            continue
    return None


def _click_if_visible(page, selectors: list[str]) -> bool:
    locator = _first_visible(page, selectors)
    if locator is None:
        return False
    try:
        locator.click(timeout=2500)
        return True
    except Exception:
        return False


def _safe_body_text(page) -> str:
    try:
        return (page.locator("body").first.text_content(timeout=2500) or "").lower()
    except Exception:
        return ""


def _handle_cookie_banner(page) -> None:
    _click_if_visible(
        page,
        [
            "button:has-text('Allow all cookies')",
            "button:has-text('Accept all')",
            "button:has-text('Разрешить все cookie')",
            "button:has-text('Разрешить все cookies')",
            "button:has-text('Разрешить все')",
        ],
    )


def _wait_for_login_form(page, timeout_seconds: int = LOGIN_FORM_WAIT_SECONDS) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        _handle_cookie_banner(page)
        username_input = _first_visible(
            page,
            [
                "input[name='username']",
                "input[name='email']",
                "input[type='text']",
            ],
        )
        password_input = _first_visible(
            page,
            [
                "input[name='password']",
                "input[name='pass']",
                "input[type='password']",
            ],
        )
        if username_input is not None and password_input is not None:
            return True
        page.wait_for_timeout(700)
    return False


def _fill_login_form(page, login: str, password: str, account_id: int) -> None:
    user_input = _first_visible(
        page,
        [
            "input[name='username']",
            "input[name='email']",
            "input[type='text']",
        ],
    )
    pass_input = _first_visible(
        page,
        [
            "input[name='password']",
            "input[name='pass']",
            "input[type='password']",
        ],
    )
    if user_input is None or pass_input is None:
        raise RuntimeError("Instagram login inputs were not found")
    user_input.wait_for(state="visible", timeout=15000)
    user_input.fill("")
    user_input.fill(login)
    pass_input.fill("")
    pass_input.fill(password)
    logger.info("credentials_filled: account_id=%s", account_id)
    submit = _first_visible(
        page,
        [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Log in')",
            "button:has-text('Войти')",
        ],
    )
    if submit is not None:
        submit.click(timeout=5000)
    else:
        pass_input.press("Enter")
    logger.info("login_clicked: account_id=%s", account_id)
    page.wait_for_timeout(1200)


def _bring_page_to_front(page) -> None:
    try:
        page.bring_to_front()
    except Exception:
        pass


def _close_context_if_needed(context: Any) -> None:
    try:
        if context is not None:
            context.close()
    except Exception:
        pass


def _mobile_launch_options(playwright_runtime) -> dict[str, Any]:
    device = playwright_runtime.devices.get(MOBILE_DEVICE_NAME, {})
    viewport = device.get("viewport") or {"width": 390, "height": 844}
    user_agent = device.get(
        "user_agent",
        (
            "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) "
            "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 "
            "Mobile/15E148 Safari/604.1"
        ),
    )
    return {
        "user_agent": user_agent,
        "viewport": viewport,
        "screen": viewport,
        "device_scale_factor": device.get("device_scale_factor", 3),
        "is_mobile": True,
        "has_touch": True,
        "locale": device.get("locale", "en-US"),
        "timezone_id": device.get("timezone_id", "Europe/Moscow"),
    }


def _open_clean_login_page(entry: SessionEntry, page, account_id: int) -> None:
    try:
        entry.context.clear_cookies()
    except Exception:
        logger.info("helper_error: account_id=%s clear_cookies_failed", account_id)
    page.goto(INSTAGRAM_LOGIN_URL, wait_until="domcontentloaded", timeout=60000)
    logger.info("login_page_opened: account_id=%s", account_id)
    _handle_cookie_banner(page)


def _worker_set_state(account_id: int, **updates) -> Optional[SessionEntry]:
    with REGISTRY_LOCK:
        entry = SESSIONS.get(account_id)
        if entry is None:
            return None
        for key, value in updates.items():
            setattr(entry, key, value)
        entry.last_activity_at = time.time()
        return entry


def _worker_get_entry(account_id: int) -> Optional[SessionEntry]:
    with REGISTRY_LOCK:
        return SESSIONS.get(account_id)


def _worker_create_session(playwright_runtime, account_id: int) -> SessionEntry:
    profile_dir = PROFILE_ROOT / str(account_id)
    profile_dir.mkdir(parents=True, exist_ok=True)
    mobile_options = _mobile_launch_options(playwright_runtime)
    context = playwright_runtime.chromium.launch_persistent_context(
        user_data_dir=str(profile_dir),
        headless=False,
        args=["--disable-blink-features=AutomationControlled"],
        **mobile_options,
    )
    page = context.pages[0] if context.pages else context.new_page()
    entry = SessionEntry(account_id=account_id, context=context, page=page, state="session_opened", flow_running=True)
    with REGISTRY_LOCK:
        SESSIONS[account_id] = entry
    logger.info("session_opened: account_id=%s mode=mobile profile=%s", account_id, profile_dir)
    return entry


def _worker_session_is_usable(entry: SessionEntry) -> bool:
    try:
        if entry.context is None:
            return False
        page = entry.page
        if page is not None and not page.is_closed():
            _ = page.url
            return True
        for candidate in entry.context.pages:
            if not candidate.is_closed():
                entry.page = candidate
                _ = candidate.url
                return True
        entry.page = entry.context.new_page()
        return True
    except Exception:
        return False


def _worker_get_or_create_session(playwright_runtime, account_id: int) -> tuple[SessionEntry, bool]:
    entry = _worker_get_entry(account_id)
    if entry is not None and _worker_session_is_usable(entry):
        return entry, True
    if entry is not None:
        _close_context_if_needed(entry.context)
        with REGISTRY_LOCK:
            SESSIONS.pop(account_id, None)
    return _worker_create_session(playwright_runtime, account_id), False


def _drive_instagram_flow(entry: SessionEntry, payload: dict, reused: bool) -> str:
    account_id = int(payload["account_id"])
    login = str(payload.get("account_login") or "").strip()
    password = str(payload.get("account_password") or "").strip()

    if not login or not password:
        logger.info("helper_error: account_id=%s missing_credentials", account_id)
        return "helper_error"

    page = entry.page if entry.page is not None and not entry.page.is_closed() else entry.context.new_page()
    entry.page = page
    _bring_page_to_front(page)

    if reused:
        logger.info("session_reused: account_id=%s", account_id)

    _open_clean_login_page(entry, page, account_id)

    if not _wait_for_login_form(page):
        try:
            page.evaluate(
                "() => { try { localStorage.clear(); sessionStorage.clear(); } catch (e) {} }"
            )
        except Exception:
            pass
        _open_clean_login_page(entry, page, account_id)

    if not _wait_for_login_form(page):
        logger.info("manual_step_required: account_id=%s login_form_unavailable", account_id)
        return "manual_step_required"

    _fill_login_form(page, login, password, account_id)
    logger.info("manual_step_required: account_id=%s", account_id)
    return "login_clicked"


def _worker_run_payload_flow(playwright_runtime, payload: dict) -> None:
    account_id = int(payload["account_id"])
    try:
        entry, reused = _worker_get_or_create_session(playwright_runtime, account_id)
        final_state = _drive_instagram_flow(entry, payload, reused)
        _worker_set_state(account_id, state=final_state, flow_running=False)
    except PlaywrightTimeoutError as exc:
        logger.exception("Playwright timeout for account_id=%s: %s", account_id, exc)
        _worker_set_state(account_id, state="timeout", flow_running=False)
    except Exception as exc:
        logger.exception("Helper flow failed for account_id=%s: %s", account_id, exc)
        _worker_set_state(account_id, state="error", flow_running=False)


def _playwright_worker_main() -> None:
    playwright_runtime = sync_playwright().start()
    try:
        while True:
            task = TASK_QUEUE.get()
            try:
                if not task:
                    continue
                kind = task.get("kind")
                if kind == "shutdown":
                    break
                if kind == "open_payload":
                    _worker_run_payload_flow(playwright_runtime, task["payload"])
            except Exception as exc:
                logger.exception("Worker task failed: %s", exc)
            finally:
                TASK_QUEUE.task_done()
    finally:
        with REGISTRY_LOCK:
            entries = list(SESSIONS.values())
        for entry in entries:
            _close_context_if_needed(entry.context)
        with REGISTRY_LOCK:
            SESSIONS.clear()
        try:
            playwright_runtime.stop()
        except Exception:
            pass


def _ensure_worker_thread() -> None:
    global WORKER_THREAD
    with REGISTRY_LOCK:
        if WORKER_THREAD is not None and WORKER_THREAD.is_alive():
            return
        WORKER_THREAD = threading.Thread(target=_playwright_worker_main, daemon=True, name="instagram-helper-worker")
        WORKER_THREAD.start()


def _request_shutdown() -> None:
    try:
        TASK_QUEUE.put_nowait({"kind": "shutdown"})
    except Exception:
        pass


atexit.register(_request_shutdown)


@app.get("/health")
def health() -> JSONResponse:
    with REGISTRY_LOCK:
        sessions = [
            {
                "account_id": account_id,
                "state": entry.state,
                "flow_running": entry.flow_running,
                "last_activity_at": int(entry.last_activity_at),
            }
            for account_id, entry in sorted(SESSIONS.items())
        ]
        worker_alive = bool(WORKER_THREAD is not None and WORKER_THREAD.is_alive())
    return JSONResponse(
        {
            "ok": True,
            "bind": f"{HELPER_HOST}:{HELPER_PORT_INT}",
            "base_url": SLEZHKA_ADMIN_BASE_URL,
            "log_file": str(LOG_FILE),
            "worker_alive": worker_alive,
            "queue_size": TASK_QUEUE.qsize(),
            "sessions": sessions,
        }
    )


@app.get("/open", response_class=HTMLResponse)
def open_ticket(ticket: str = Query(..., min_length=8)) -> HTMLResponse:
    if not HELPER_API_KEY:
        raise HTTPException(status_code=503, detail="HELPER_API_KEY is not configured")

    try:
        payload = _fetch_ticket_payload(ticket)
    except Exception as exc:
        logger.exception("Failed to fetch launch ticket %s: %s", ticket, exc)
        return _render_status_page(
            "Не удалось запустить Instagram helper",
            "Helper не смог получить данные аккаунта.",
            str(exc),
        )

    _ensure_worker_thread()

    account_id = int(payload["account_id"])
    title = "Запускаю Instagram helper"
    message = "Локальный helper открывает мобильный Instagram, подставляет данные аккаунта и нажимает вход."
    detail = "Откроется mobile-окно в стиле iPhone Safari. После нажатия входа окно останется открытым, дальше работаешь вручную."

    with REGISTRY_LOCK:
        existing = SESSIONS.get(account_id)
        if existing is not None and existing.flow_running:
            return _render_status_page(
                "Instagram уже открывается",
                "Для этого аккаунта запуск уже идёт. Дождись окна браузера.",
            )
        if existing is not None and existing.state in MANUAL_SESSION_STATES:
            title = "Instagram уже открыт"
            message = "Для этого аккаунта уже есть открытая mobile-сессия. Helper заново активирует окно и отправит форму входа."
        elif existing is not None:
            title = "Использую mobile-сессию"
            message = "Для этого аккаунта уже есть mobile-профиль. Helper переиспользует его."

        if existing is None:
            SESSIONS[account_id] = SessionEntry(account_id=account_id, state="queued", flow_running=True)
        else:
            existing.flow_running = True
            existing.last_activity_at = time.time()

    TASK_QUEUE.put({"kind": "open_payload", "payload": payload})
    return _render_status_page(title, message, detail)
