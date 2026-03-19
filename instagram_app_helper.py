import atexit
from contextlib import asynccontextmanager
from datetime import datetime
from html import unescape
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import threading
import time
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import quote
from xml.etree import ElementTree as ET

from dotenv import load_dotenv
from fastapi import Body, Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

import http_utils
from settings import load_helper_settings
from twofa_utils import (
    current_totp_code as _current_totp_code,
    extract_twofa_profile as _extract_twofa_profile,
    is_valid_twofa_secret as _twofa_secret_is_valid,
    normalize_twofa_secret as _normalize_twofa_secret_value,
    seconds_until_totp_rollover as _seconds_until_totp_rollover,
)

try:
    import pyotp
except Exception as exc:  # pragma: no cover - import-time fallback
    pyotp = None
    PYOTP_IMPORT_ERROR = exc
else:
    PYOTP_IMPORT_ERROR = None

load_dotenv()

SETTINGS = load_helper_settings()

try:
    import adbutils  # type: ignore
except Exception as exc:  # pragma: no cover - import-time fallback
    adbutils = None
    ADBUTILS_IMPORT_ERROR = exc
else:
    ADBUTILS_IMPORT_ERROR = None

try:
    import uiautomator2 as u2  # type: ignore
except Exception as exc:  # pragma: no cover - import-time fallback
    u2 = None
    UIAUTOMATOR2_IMPORT_ERROR = exc
else:
    UIAUTOMATOR2_IMPORT_ERROR = None

HELPER_BIND = SETTINGS.helper_bind
HELPER_HOST, _, HELPER_PORT = HELPER_BIND.partition(":")
HELPER_PORT_INT = int(HELPER_PORT or "17374")
SLEZHKA_ADMIN_BASE_URL = SETTINGS.slezhka_admin_base_url
HELPER_API_KEY = SETTINGS.helper_api_key
ANDROID_AVD_NAME = SETTINGS.android_avd_name
ADB_PATH_RAW = SETTINGS.adb_path_raw
EMULATOR_PATH_RAW = SETTINGS.emulator_path_raw
EMULATOR_NO_WINDOW = SETTINGS.emulator_no_window
INSTAGRAM_PACKAGE = SETTINGS.instagram_package
INSTAGRAM_PUBLISH_SOURCE_DIR = SETTINGS.instagram_publish_source_dir
INSTAGRAM_PUBLISH_MEDIA_DIR = SETTINGS.instagram_publish_media_dir
PUBLISH_VIDEO_EXTENSIONS = {".mp4", ".mov"}
PUBLISH_RUNNER_CACHE_DIR = SETTINGS.publish_runner_cache_dir
PUBLISH_RUNNER_DOWNLOADS_DIR = SETTINGS.publish_runner_downloads_dir
PUBLISH_DIAGNOSTICS_DIR = SETTINGS.publish_diagnostics_dir
STRICT_CONFIG = SETTINGS.strict_config
EMULATOR_STABILIZE_SECONDS = SETTINGS.emulator_stabilize_seconds
USE_EMULATOR_SNAPSHOTS = SETTINGS.use_emulator_snapshots
PUBLISH_RUNNER_ENABLED = SETTINGS.publish_runner_enabled
PUBLISH_RUNNER_POLL_SECONDS = SETTINGS.publish_runner_poll_seconds
PUBLISH_SUCCESS_WAIT_SECONDS = SETTINGS.publish_success_wait_seconds
PUBLISH_HEARTBEAT_SECONDS = SETTINGS.publish_heartbeat_seconds
PUBLISH_UPLOAD_START_WAIT_SECONDS = SETTINGS.publish_upload_start_wait_seconds
PUBLISH_PROFILE_VERIFY_START_DELAY_SECONDS = SETTINGS.publish_profile_verify_start_delay_seconds
PUBLISH_PROFILE_VERIFY_SECONDS = SETTINGS.publish_profile_verify_seconds
PUBLISH_PROFILE_VERIFY_INTERVAL_SECONDS = SETTINGS.publish_profile_verify_interval_seconds
PUBLISH_PROFILE_FRESHNESS_SECONDS = SETTINGS.publish_profile_freshness_seconds
PUBLISH_PROFILE_CHECK_SLOTS = SETTINGS.publish_profile_check_slots
PUBLISH_PROFILE_BASELINE_SLOTS = SETTINGS.publish_profile_baseline_slots
PUBLISH_RUNNER_NAME = SETTINGS.publish_runner_name
PUBLISH_RUNNER_API_KEY = SETTINGS.publish_runner_api_key
SERIAL_TO_AVD_MAP_RAW = SETTINGS.serial_to_avd_map_raw

LOG_DIR = Path.home() / "Library" / "Logs" / "SlezhkaHelper"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "instagram-app-helper.log"
logger = logging.getLogger("instagram_app_helper")
if not logger.handlers:
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
logger.propagate = False

def _helper_startup_config_check() -> None:
    _validate_runtime_config()


def _startup_helper() -> None:
    _ensure_worker_thread()
    _ensure_runner_thread()


@asynccontextmanager
async def lifespan(_: FastAPI):
    _helper_startup_config_check()
    _startup_helper()
    yield


app = FastAPI(title="Slezhka Instagram App Helper", lifespan=lifespan)


def require_helper_api_key(x_helper_api_key: Optional[str] = Header(None)) -> None:
    if not HELPER_API_KEY:
        raise HTTPException(status_code=503, detail="HELPER_API_KEY is not configured")
    if (x_helper_api_key or "").strip() != HELPER_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid HELPER_API_KEY")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

TASK_QUEUE: "queue.Queue[dict[str, Any]]" = queue.Queue()
WORKER_THREAD: Optional[threading.Thread] = None
RUNNER_THREAD: Optional[threading.Thread] = None
STATE_LOCK = threading.RLock()
EMULATOR_PROCESSES: dict[str, subprocess.Popen] = {}


@dataclass
class HelperState:
    account_id: Optional[int] = None
    target: str = "instagram_app_login"
    state: str = "idle"
    detail: str = ""
    flow_running: bool = False
    emulator_serial: str = ""
    last_activity_at: float = field(default_factory=time.time)


RUNTIME_STATE = HelperState()

LOGIN_WAIT_SECONDS = 40
BOOT_WAIT_SECONDS = 220
MAIL_CHALLENGE_TIMEOUT_SECONDS = SETTINGS.instagram_mail_challenge_timeout_seconds
MAIL_CHALLENGE_RETRY_SECONDS = SETTINGS.instagram_mail_challenge_retry_seconds
MAIL_CHALLENGE_RESEND_WAIT_SECONDS = SETTINGS.instagram_mail_challenge_resend_wait_seconds


class PublishFlowError(RuntimeError):
    def __init__(
        self,
        stage: str,
        detail: str,
        *,
        last_file: str = "",
        serial: str = "",
        reason_code: str = "",
        account_publish_state: str = "",
        payload: Optional[dict[str, Any]] = None,
    ) -> None:
        super().__init__(detail)
        self.stage = (stage or "publishing").strip()
        self.detail = (detail or "Instagram publish flow failed.").strip()
        self.last_file = (last_file or "").strip()
        self.serial = (serial or "").strip()
        self.reason_code = (reason_code or "").strip()
        self.account_publish_state = (account_publish_state or "").strip()
        self.payload = dict(payload or {})


class ReelMetricsFlowError(RuntimeError):
    def __init__(
        self,
        detail: str,
        *,
        snapshot_reported: bool = False,
        serial: str = "",
    ) -> None:
        super().__init__(detail)
        self.detail = (detail or "Instagram reel metrics collection failed.").strip()
        self.snapshot_reported = bool(snapshot_reported)
        self.serial = (serial or "").strip()


@dataclass
class PublishWaitResult:
    outcome: str
    publish_phase: str = "waiting_upload_start"
    accepted_by_instagram: bool = False
    elapsed_seconds: int = 0
    upload_progress_pct: Optional[int] = None
    last_activity: str = ""
    reason_code: str = ""
    success: bool = False
    event_kind: str = ""


@dataclass
class ProfileReelCandidate:
    slot_index: int
    age_seconds: Optional[int] = None
    age_label: str = ""
    fingerprint: str = ""
    signature_text: str = ""
    opened: bool = False
    success_markers: bool = False


@dataclass
class ProfileVerificationResult:
    verified: bool = False
    needs_review: bool = False
    reason_code: str = ""
    detail: str = ""
    publish_phase: str = "verifying_profile"
    matched_slot: Optional[int] = None
    matched_age_seconds: Optional[int] = None
    matched_fingerprint: str = ""
    matched_signature_text: str = ""
    verification_attempt: int = 0
    baseline_available: bool = False
    checked_slots: int = 0
    event_kind: str = ""
    seconds_until_profile_check: Optional[int] = None
    share_clicked_at: Optional[int] = None
    verification_starts_at: Optional[int] = None
    verification_deadline_at: Optional[int] = None
    first_profile_check_at: Optional[int] = None
    profile_surface_state: str = ""
    keyboard_visible: bool = False
    comment_sheet_visible: bool = False
    clips_viewer_visible: bool = False
    quick_capture_visible: bool = False
    timestamp_readable: bool = False
    diagnostics_path: str = ""
    published_at: Optional[int] = None


def _sdk_candidates() -> list[Path]:
    candidates: list[Path] = []
    android_home = (os.getenv("ANDROID_SDK_ROOT", "") or os.getenv("ANDROID_HOME", "")).strip()
    if android_home:
        candidates.append(Path(android_home))
    candidates.append(Path.home() / "Library" / "Android" / "sdk")
    return candidates


def _resolve_binary(raw_path: str, command_name: str, relative_parts: tuple[str, ...]) -> Optional[str]:
    if raw_path:
        candidate = Path(raw_path).expanduser()
        if candidate.exists():
            return str(candidate)
    which = shutil.which(command_name)
    if which:
        return which
    for root in _sdk_candidates():
        candidate = root.joinpath(*relative_parts)
        if candidate.exists():
            return str(candidate)
    return None


def _resolve_adb_path() -> Optional[str]:
    return _resolve_binary(ADB_PATH_RAW, "adb", ("platform-tools", "adb"))


def _resolve_emulator_path() -> Optional[str]:
    return _resolve_binary(EMULATOR_PATH_RAW, "emulator", ("emulator", "emulator"))


def _build_emulator_launch_command(
    emulator_path: str,
    avd_name: str,
    *,
    port: Optional[int] = None,
    no_window: bool = False,
) -> list[str]:
    command = [emulator_path, "-avd", avd_name]
    if port is not None:
        command.extend(["-port", str(port)])
    command.extend(
        [
            "-netdelay",
            "none",
            "-netspeed",
            "full",
            "-gpu",
            "swiftshader_indirect",
            "-no-boot-anim",
            "-no-metrics",
        ]
    )
    if no_window:
        command.extend(["-no-window", "-no-audio"])
    return command


def _emulator_launch_commands(emulator_path: str, avd_name: str, *, port: Optional[int] = None) -> list[list[str]]:
    commands = [
        _build_emulator_launch_command(
            emulator_path,
            avd_name,
            port=port,
            no_window=EMULATOR_NO_WINDOW,
        )
    ]
    if not EMULATOR_NO_WINDOW:
        headless_command = _build_emulator_launch_command(
            emulator_path,
            avd_name,
            port=port,
            no_window=True,
        )
        if headless_command != commands[0]:
            commands.append(headless_command)
    return commands


def _serial_to_avd_map() -> dict[str, str]:
    if not SERIAL_TO_AVD_MAP_RAW:
        return {}
    try:
        payload = json.loads(SERIAL_TO_AVD_MAP_RAW)
    except Exception:
        logger.warning("serial_to_avd_map_invalid_json")
        return {}
    if not isinstance(payload, dict):
        logger.warning("serial_to_avd_map_invalid_type")
        return {}
    mapping: dict[str, str] = {}
    for key, value in payload.items():
        serial = str(key or "").strip()
        avd = str(value or "").strip()
        if serial and avd:
            mapping[serial] = avd
    return mapping


def _serial_emulator_port(serial: str) -> Optional[int]:
    raw = (serial or "").strip()
    if not raw.startswith("emulator-"):
        return None
    try:
        port = int(raw.split("-", 1)[1])
    except Exception:
        return None
    return port if port > 0 else None


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
              .wrap {{ max-width: 720px; margin: 48px auto; padding: 24px; }}
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
                <p>Helper запускает Instagram app в Android emulator.</p>
                <p>После нажатия входа эмулятор останется открытым. Дальнейшие шаги делаешь вручную.</p>
                <p>Лог helper: <code>~/Library/Logs/SlezhkaHelper/instagram-app-helper.log</code></p>
              </div>
            </div>
          </body>
        </html>
        """
    )


def _state_snapshot() -> dict[str, Any]:
    with STATE_LOCK:
        return {
            "account_id": RUNTIME_STATE.account_id,
            "target": RUNTIME_STATE.target,
            "state": RUNTIME_STATE.state,
            "detail": RUNTIME_STATE.detail,
            "flow_running": RUNTIME_STATE.flow_running,
            "emulator_serial": RUNTIME_STATE.emulator_serial,
            "last_activity_at": int(RUNTIME_STATE.last_activity_at),
        }


def _set_state(**updates: Any) -> None:
    with STATE_LOCK:
        for key, value in updates.items():
            setattr(RUNTIME_STATE, key, value)
        RUNTIME_STATE.last_activity_at = time.time()


def _run(
    cmd: list[str],
    *,
    timeout: int = 30,
    check: bool = True,
    capture_output: bool = True,
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd,
        timeout=timeout,
        check=False,
        text=True,
        capture_output=capture_output,
    )
    if check and result.returncode != 0:
        stdout = (result.stdout or "").strip()
        stderr = (result.stderr or "").strip()
        msg = stderr or stdout or f"command failed: {' '.join(cmd)}"
        raise RuntimeError(msg)
    return result


def _build_ticket_url(ticket: str, target: str) -> str:
    encoded = quote((ticket or "").strip(), safe="")
    encoded_target = quote((target or "").strip(), safe="")
    return f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/launch-ticket/{encoded}?target={encoded_target}"


def _fetch_ticket_payload(ticket: str, target: str) -> dict:
    if not HELPER_API_KEY:
        raise RuntimeError("HELPER_API_KEY is not configured")
    response = http_utils.request_with_retry(
        "GET",
        _build_ticket_url(ticket, target),
        headers={"X-Helper-Api-Key": HELPER_API_KEY},
        timeout=25,
        allow_retry=True,
        log_context="helper_fetch_ticket",
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


def _push_account_launch_status(
    account_id: int,
    status: str,
    detail: str,
    handle: str,
    *,
    mail_challenge: Optional[dict[str, Any]] = None,
) -> None:
    if not HELPER_API_KEY:
        return
    try:
        payload: dict[str, Any] = {
            "state": (status or "").strip(),
            "detail": (detail or "").strip(),
            "handle": (handle or "").strip(),
        }
        if isinstance(mail_challenge, dict) and mail_challenge:
            payload["mail_challenge"] = mail_challenge
        response = http_utils.request_with_retry(
            "POST",
            f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/accounts/{int(account_id)}/instagram-status",
            headers={"X-Helper-Api-Key": HELPER_API_KEY},
            json=payload,
            timeout=25,
            allow_retry=True,
            log_context="helper_push_launch_status",
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning("status_push_failed: account_id=%s status=%s error=%s", account_id, status, exc)


def _push_account_publish_status(
    account_id: int,
    status: str,
    detail: str,
    handle: str,
    *,
    last_file: str = "",
    source_path: str = "",
    helper_ticket: str = "",
    telemetry: Optional[dict[str, Any]] = None,
) -> None:
    if not HELPER_API_KEY:
        return
    try:
        request_payload: dict[str, Any] = {
            "state": (status or "").strip(),
            "detail": (detail or "").strip(),
            "handle": (handle or "").strip(),
            "last_file": (last_file or "").strip(),
            "source_path": (source_path or "").strip(),
            "helper_ticket": (helper_ticket or "").strip(),
        }
        if isinstance(telemetry, dict):
            for key, value in telemetry.items():
                request_payload[key] = value
        response = http_utils.request_with_retry(
            "POST",
            f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/accounts/{int(account_id)}/instagram-publish-status",
            headers={"X-Helper-Api-Key": HELPER_API_KEY},
            json=request_payload,
            timeout=25,
            allow_retry=True,
            log_context="helper_push_publish_status",
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning(
            "publish_status_push_failed: account_id=%s status=%s last_file=%s error=%s",
            account_id,
            status,
            last_file,
            exc,
        )


def _resolve_account_mail_challenge(
    account_id: int,
    *,
    ticket: str,
    challenge_started_at: int,
    screen_kind: str,
    timeout_seconds: int,
) -> dict[str, Any]:
    if not HELPER_API_KEY:
        raise RuntimeError("HELPER_API_KEY is not configured")
    response = http_utils.request_with_retry(
        "POST",
        f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/accounts/{int(account_id)}/mail-challenge/resolve",
        headers={"X-Helper-Api-Key": HELPER_API_KEY},
        json={
            "ticket": (ticket or "").strip(),
            "challenge_started_at": int(challenge_started_at or time.time()),
            "screen_kind": (screen_kind or "unknown").strip(),
            "timeout_seconds": max(5, int(timeout_seconds or MAIL_CHALLENGE_TIMEOUT_SECONDS)),
        },
        timeout=max(20, int(timeout_seconds or MAIL_CHALLENGE_TIMEOUT_SECONDS) + 10),
        allow_retry=False,
        log_context="helper_mail_challenge_resolve",
    )
    if response.status_code == 404:
        raise RuntimeError("Account not found")
    if response.status_code == 401:
        raise RuntimeError("Invalid HELPER_API_KEY")
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise RuntimeError("Invalid mail challenge response")
    return payload


def _lease_publish_job() -> Optional[dict[str, Any]]:
    if not PUBLISH_RUNNER_API_KEY:
        raise RuntimeError("PUBLISH_RUNNER_API_KEY is not configured")
    response = http_utils.request_with_retry(
        "POST",
        f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/publishing/jobs/lease",
        headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
        json={"runner_name": PUBLISH_RUNNER_NAME},
        timeout=25,
        allow_retry=False,
        log_context="publish_job_lease",
    )
    if response.status_code == 204:
        return None
    response.raise_for_status()
    payload = response.json()
    job = payload.get("job")
    return job if isinstance(job, dict) else None


def _lease_reel_metric_post() -> Optional[dict[str, Any]]:
    if not PUBLISH_RUNNER_API_KEY:
        raise RuntimeError("PUBLISH_RUNNER_API_KEY is not configured")
    response = http_utils.request_with_retry(
        "POST",
        f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/reel-metrics/lease",
        headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
        json={"runner_name": PUBLISH_RUNNER_NAME},
        timeout=25,
        allow_retry=False,
        log_context="reel_metrics_lease",
    )
    if response.status_code == 204:
        return None
    response.raise_for_status()
    payload = response.json()
    post = payload.get("post")
    return post if isinstance(post, dict) else None


def _push_reel_metric_snapshot(
    post_id: int,
    *,
    window_key: str,
    status: str,
    payload: Optional[dict[str, Any]] = None,
) -> None:
    if not PUBLISH_RUNNER_API_KEY:
        return
    request_payload: dict[str, Any] = {
        "window_key": (window_key or "").strip(),
        "status": (status or "").strip(),
    }
    if isinstance(payload, dict):
        for key, value in payload.items():
            request_payload[key] = value
    response = http_utils.request_with_retry(
        "POST",
        f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/reel-metrics/posts/{int(post_id)}/snapshot",
        headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
        json=request_payload,
        timeout=25,
        allow_retry=True,
        log_context="reel_metrics_snapshot",
    )
    response.raise_for_status()


def _push_publish_job_status(
    job_id: int,
    state: str,
    detail: str,
    *,
    last_file: str = "",
    serial: str = "",
    source_path: str = "",
    account_publish_state: str = "",
    payload: Optional[dict[str, Any]] = None,
) -> None:
    if not PUBLISH_RUNNER_API_KEY:
        return
    try:
        request_payload: dict[str, Any] = {
            "state": (state or "").strip(),
            "detail": (detail or "").strip(),
            "last_file": (last_file or "").strip(),
            "runner_name": PUBLISH_RUNNER_NAME,
            "emulator_serial": (serial or "").strip(),
            "source_path": (source_path or "").strip(),
            "account_publish_state": (account_publish_state or "").strip(),
        }
        if isinstance(payload, dict):
            for key, value in payload.items():
                request_payload[key] = value
        response = http_utils.request_with_retry(
            "POST",
            f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/publishing/jobs/{int(job_id)}/status",
            headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
            json=request_payload,
            timeout=25,
            allow_retry=True,
            log_context="publish_job_status",
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning("publish_job_status_push_failed: job_id=%s state=%s error=%s", job_id, state, exc)


def _source_dir_path() -> Path:
    return Path(INSTAGRAM_PUBLISH_SOURCE_DIR).expanduser()


def _publish_runner_cache_dir() -> Path:
    root = Path(PUBLISH_RUNNER_CACHE_DIR).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _publish_runner_downloads_dir() -> Path:
    root = Path(PUBLISH_RUNNER_DOWNLOADS_DIR).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _config_warnings() -> list[str]:
    warnings: list[str] = []
    if not HELPER_API_KEY:
        warnings.append("HELPER_API_KEY is empty")
    if not SLEZHKA_ADMIN_BASE_URL:
        warnings.append("SLEZHKA_ADMIN_BASE_URL is empty")
    if PUBLISH_RUNNER_ENABLED and not PUBLISH_RUNNER_API_KEY:
        warnings.append("PUBLISH_RUNNER_API_KEY is empty while runner is enabled")
    return warnings


def _validate_runtime_config() -> None:
    warnings = _config_warnings()
    for item in warnings:
        logger.warning("config_warning: %s", item)
    if STRICT_CONFIG and warnings:
        raise RuntimeError(f"Config validation failed: {', '.join(warnings)}")


def _publish_diagnostics_dir() -> Path:
    root = Path(PUBLISH_DIAGNOSTICS_DIR).expanduser()
    root.mkdir(parents=True, exist_ok=True)
    return root


def _capture_publish_diagnostics(
    serial: str,
    label: str,
    *,
    device: Any = None,
    batch_id: Optional[int] = None,
    job_id: Optional[int] = None,
    account_id: Optional[int] = None,
) -> dict[str, str]:
    adb_path = _resolve_adb_path()
    if not adb_path or not serial:
        return {}
    ts = time.strftime("%Y%m%d_%H%M%S")
    parts = [ts, (label or "publish_failure").strip() or "publish_failure"]
    if batch_id:
        parts.append(f"batch{int(batch_id)}")
    if job_id:
        parts.append(f"job{int(job_id)}")
    if account_id:
        parts.append(f"acc{int(account_id)}")
    prefix = "_".join(parts)
    root = _publish_diagnostics_dir()
    paths: dict[str, str] = {}
    screenshot_path = root / f"{prefix}_screen.png"
    window_path = root / f"{prefix}_window.txt"
    activity_path = root / f"{prefix}_activity.txt"
    hierarchy_path = root / f"{prefix}_hierarchy.xml"
    nodes_path = root / f"{prefix}_nodes.json"
    try:
        result = subprocess.run(
            [adb_path, "-s", serial, "exec-out", "screencap", "-p"],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode == 0 and result.stdout:
            screenshot_path.write_bytes(result.stdout)
            paths["screenshot"] = str(screenshot_path)
    except Exception as exc:
        logger.warning("publish_diagnostics_screenshot_failed: serial=%s error=%s", serial, exc)
    try:
        window_dump = _adb_shell(serial, "dumpsys", "window", timeout=20, check=False)
        window_path.write_text(window_dump.stdout or "", encoding="utf-8")
        paths["dumpsys_window"] = str(window_path)
    except Exception as exc:
        logger.warning("publish_diagnostics_window_failed: serial=%s error=%s", serial, exc)
    try:
        activity_dump = _adb_shell(serial, "dumpsys", "activity", "top", timeout=20, check=False)
        activity_path.write_text(activity_dump.stdout or "", encoding="utf-8")
        paths["dumpsys_activity"] = str(activity_path)
    except Exception as exc:
        logger.warning("publish_diagnostics_activity_failed: serial=%s error=%s", serial, exc)
    if device is not None:
        try:
            try:
                hierarchy = device.dump_hierarchy(compressed=False)
            except TypeError:
                hierarchy = device.dump_hierarchy()
            if hierarchy:
                hierarchy_path.write_text(str(hierarchy), encoding="utf-8")
                paths["hierarchy"] = str(hierarchy_path)
        except Exception as exc:
            logger.warning("publish_diagnostics_hierarchy_failed: serial=%s error=%s", serial, exc)
        try:
            nodes_path.write_text(
                json.dumps(_dump_ui_nodes(device), ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            paths["node_snapshot"] = str(nodes_path)
        except Exception as exc:
            logger.warning("publish_diagnostics_nodes_failed: serial=%s error=%s", serial, exc)
    if paths:
        logger.info("publish_failure_diagnostics_saved: serial=%s paths=%s", serial, paths)
    return paths


def _diagnostics_primary_path(paths: Optional[dict[str, str]]) -> str:
    payload = dict(paths or {})
    for key in ("screenshot", "hierarchy", "node_snapshot", "dumpsys_activity", "dumpsys_window"):
        value = str(payload.get(key) or "").strip()
        if value:
            return value
    return ""


def _latest_source_video_info() -> Optional[dict[str, Any]]:
    source_dir = _source_dir_path()
    if not source_dir.exists() or not source_dir.is_dir():
        return None
    candidates = [
        path
        for path in source_dir.iterdir()
        if path.is_file() and path.suffix.lower() in PUBLISH_VIDEO_EXTENSIONS
    ]
    if not candidates:
        return None
    latest = max(candidates, key=lambda item: item.stat().st_mtime)
    stat = latest.stat()
    modified_at = int(stat.st_mtime)
    return {
        "path": str(latest),
        "name": latest.name,
        "size_bytes": int(stat.st_size),
        "modified_at": modified_at,
        "modified_at_label": time.strftime("%Y-%m-%d %H:%M", time.localtime(modified_at)),
    }


def _source_video_info_from_path(source_path: str) -> dict[str, Any]:
    source = Path(source_path).expanduser()
    if not source.exists() or not source.is_file():
        raise RuntimeError("Source video not found.")
    if source.suffix.lower() not in PUBLISH_VIDEO_EXTENSIONS:
        raise RuntimeError("Unsupported source video extension.")
    stat = source.stat()
    modified_at = int(stat.st_mtime)
    return {
        "path": str(source),
        "name": source.name,
        "size_bytes": int(stat.st_size),
        "modified_at": modified_at,
        "modified_at_label": time.strftime("%Y-%m-%d %H:%M", time.localtime(modified_at)),
    }


def _persist_downloaded_publish_job_source(job_id: int, source_name: str, cached_path: Path) -> Path:
    target_name = Path(source_name).name.strip() or cached_path.name
    safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", target_name).strip("._") or cached_path.name
    target_path = _publish_runner_downloads_dir() / f"job-{int(job_id)}-{safe_name}"
    shutil.copy2(cached_path, target_path)
    return target_path


def _download_publish_job_source(job_id: int, source_name: str) -> dict[str, Any]:
    if not PUBLISH_RUNNER_API_KEY:
        raise RuntimeError("PUBLISH_RUNNER_API_KEY is not configured")
    target_name = Path(source_name).name.strip() or f"publish-job-{int(job_id)}.mp4"
    target_dir = _publish_runner_cache_dir() / str(int(job_id))
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / target_name
    response = http_utils.request_with_retry(
        "GET",
        f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/publishing/jobs/{int(job_id)}/artifact",
        headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
        stream=True,
        timeout=180,
        allow_retry=True,
        log_context="publish_job_artifact_download",
    )
    response.raise_for_status()
    with target_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    info = _source_video_info_from_path(str(target_path))
    info["downloaded"] = True
    info["saved_to"] = str(_persist_downloaded_publish_job_source(int(job_id), target_name, target_path))
    return info


def _delete_downloaded_publish_job_source(path_value: str) -> None:
    path = Path(path_value).expanduser()
    if path.exists() and path.is_file():
        path.unlink()
    try:
        parent = path.parent
        if parent.exists() and parent.is_dir() and not any(parent.iterdir()):
            parent.rmdir()
    except Exception:
        pass


def _resolve_publish_job_source(job_id: int, source_path: str) -> dict[str, Any]:
    """
    Publish jobs come from server-side staging paths. If that path is not
    available on this host, download them through admin API. Batch jobs must
    use the exact workflow-generated artifact and never fallback by filename
    to an unrelated local video.
    """
    try:
        return _source_video_info_from_path(source_path)
    except RuntimeError as exc:
        local_error = str(exc)
    else:  # pragma: no cover - kept for readability
        local_error = ""
    download_error = ""
    if int(job_id or 0) > 0:
        try:
            info = _download_publish_job_source(int(job_id), Path(source_path).name or source_path)
            info["downloaded_from"] = source_path
            return info
        except Exception as exc:
            download_error = str(exc).strip()
            logger.warning("publish_job_source_download_failed: job_id=%s source_path=%s error=%s", job_id, source_path, exc)
    detail_parts = []
    if local_error:
        detail_parts.append(local_error)
    if download_error:
        detail_parts.append(f"download failed: {download_error}")
    detail = "; ".join(part for part in detail_parts if part)
    if detail:
        raise RuntimeError(f"Workflow-generated source video is unavailable on this Mac ({detail}).")
    raise RuntimeError("Workflow-generated source video is unavailable on this Mac.")


def _preflight() -> None:
    _validate_runtime_config()
    if not HELPER_API_KEY:
        raise RuntimeError("HELPER_API_KEY is not configured")
    if u2 is None:
        raise RuntimeError(f"uiautomator2 is not installed: {UIAUTOMATOR2_IMPORT_ERROR}")
    if adbutils is None:
        raise RuntimeError(f"adbutils is not installed: {ADBUTILS_IMPORT_ERROR}")
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found. Install Android platform-tools or set ADB_PATH.")
    emulator_path = _resolve_emulator_path()
    if not emulator_path:
        raise RuntimeError("Android emulator not found. Install Android Emulator or set EMULATOR_PATH.")
    if not ANDROID_AVD_NAME:
        raise RuntimeError("ANDROID_AVD_NAME is not configured.")
    avds = _list_avds()
    if ANDROID_AVD_NAME not in avds:
        raise RuntimeError(
            f"AVD '{ANDROID_AVD_NAME}' is not ready yet. Available: {', '.join(avds) or 'none'}."
        )


def _list_avds() -> list[str]:
    emulator_path = _resolve_emulator_path()
    if not emulator_path:
        return []
    result = _run([emulator_path, "-list-avds"], timeout=20, check=False)
    return [line.strip() for line in (result.stdout or "").splitlines() if line.strip()]


def _list_running_emulators() -> list[str]:
    if adbutils is not None:
        try:
            devices = adbutils.adb.device_list()
            serials = [str(device.serial) for device in devices if str(device.serial).startswith("emulator-")]
            if serials:
                return serials
        except Exception:
            pass

    adb_path = _resolve_adb_path()
    if not adb_path:
        return []
    result = _run([adb_path, "devices"], timeout=15, check=False)
    serials: list[str] = []
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if not line or line.startswith("List of devices attached"):
            continue
        if "\tdevice" in line:
            serial = line.split("\t", 1)[0].strip()
            if serial.startswith("emulator-"):
                serials.append(serial)
    return serials


def _publish_boundary_reset_needed(serial_hint: str = "") -> bool:
    hint = (serial_hint or "").strip()
    running = _list_running_emulators()
    if running:
        if not hint or hint.lower() == "default":
            return True
        return hint in running or bool(running)
    for process in EMULATOR_PROCESSES.values():
        try:
            if process is not None and process.poll() is None:
                return True
        except Exception:
            continue
    return False


def _terminate_tracked_emulator_processes(timeout_seconds: float = 20.0) -> None:
    deadline = time.time() + max(1.0, float(timeout_seconds or 0))
    for key, process in list(EMULATOR_PROCESSES.items()):
        if process is None:
            EMULATOR_PROCESSES.pop(key, None)
            continue
        try:
            if process.poll() is None:
                process.terminate()
        except Exception:
            pass
    while time.time() < deadline:
        alive = False
        for key, process in list(EMULATOR_PROCESSES.items()):
            if process is None:
                EMULATOR_PROCESSES.pop(key, None)
                continue
            try:
                if process.poll() is None:
                    alive = True
                    continue
            except Exception:
                alive = True
                continue
            EMULATOR_PROCESSES.pop(key, None)
        if not alive:
            return
        time.sleep(0.5)
    for key, process in list(EMULATOR_PROCESSES.items()):
        if process is None:
            EMULATOR_PROCESSES.pop(key, None)
            continue
        try:
            if process.poll() is None:
                process.kill()
        except Exception:
            pass
        EMULATOR_PROCESSES.pop(key, None)


def _reset_publish_emulator_boundary(serial_hint: str = "", *, clear_instagram: bool = True) -> list[str]:
    hint = (serial_hint or "").strip()
    running = sorted(set(_list_running_emulators()))
    targets = list(running)
    if hint and hint.lower() != "default" and hint not in targets:
        targets.append(hint)
    adb_path = _resolve_adb_path()
    stopped: list[str] = []
    for serial in targets:
        if serial in running:
            if clear_instagram:
                _adb_shell(serial, "am", "force-stop", INSTAGRAM_PACKAGE, timeout=15, check=False)
                try:
                    _clear_instagram_data(serial)
                except Exception as exc:
                    logger.warning("publish_job_clear_instagram_failed: serial=%s error=%s", serial, exc)
            if adb_path:
                _run([adb_path, "-s", serial, "emu", "kill"], timeout=20, check=False)
            stopped.append(serial)
    _terminate_tracked_emulator_processes()
    if stopped:
        deadline = time.time() + 60.0
        while time.time() < deadline:
            current = set(_list_running_emulators())
            if not current.intersection(stopped):
                break
            time.sleep(1.0)
    _set_state(emulator_serial="")
    return stopped


def _wait_for_device_serial(previous: set[str], timeout_seconds: int = 150) -> str:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current = set(_list_running_emulators())
        new_serials = [serial for serial in current if serial not in previous]
        if new_serials:
            return sorted(new_serials)[0]
        if current and not previous:
            return sorted(current)[0]
        time.sleep(2)
    raise RuntimeError("Android emulator did not appear in adb devices.")


def _wait_for_boot(serial: str) -> None:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    _run([adb_path, "-s", serial, "wait-for-device"], timeout=60, check=False)
    deadline = time.time() + BOOT_WAIT_SECONDS
    while time.time() < deadline:
        result = _run([adb_path, "-s", serial, "shell", "getprop", "sys.boot_completed"], timeout=20, check=False)
        if (result.stdout or "").strip() == "1":
            _run([adb_path, "-s", serial, "shell", "input", "keyevent", "82"], timeout=15, check=False)
            return
        time.sleep(3)
    raise RuntimeError("Android emulator boot timed out.")


def _device_display_size(device: Optional[Any] = None) -> tuple[int, int]:
    width = 1080
    height = 1920
    if device is None:
        return width, height
    try:
        info = getattr(device, "info", {}) or {}
        width = int(info.get("displayWidth") or width)
        height = int(info.get("displayHeight") or height)
    except Exception:
        pass
    return width, height


def _system_anr_windows(serial: str) -> list[str]:
    result = _adb_shell(serial, "dumpsys", "window", timeout=40, check=False)
    return [
        line.strip()
        for line in (result.stdout or "").splitlines()
        if "Application Not Responding:" in line
    ]


def _disable_emulator_noise(serial: str) -> None:
    # These background services are unnecessary for our single-purpose emulator
    # and are a frequent source of cold-boot ANR dialogs.
    _adb_shell(serial, "pm", "disable-user", "--user", "0", "com.google.android.apps.wellbeing", timeout=30, check=False)
    for setting in ("window_animation_scale", "transition_animation_scale", "animator_duration_scale"):
        _adb_shell(serial, "settings", "put", "global", setting, "0", timeout=20, check=False)


def _dismiss_system_dialogs(device: Optional[Any], serial: str, timeout_seconds: float = 6.0) -> None:
    deadline = time.time() + timeout_seconds
    wait_selectors = [
        {"textMatches": "(?i)(wait|подождать)"},
        {"resourceId": "android:id/aerr_wait"},
        {"resourceId": "android:id/button1"},
    ]
    primary_selectors = [
        {"textMatches": "(?i)(ok|ок|got it|понятно|continue|продолжить)"},
        {"textMatches": "(?i)(not now|не сейчас)"},
        {"textMatches": "(?i)(allow|разрешить|while using the app|только во время использования)"},
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_foreground_only_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_one_time_button"},
    ]
    close_selectors = [
        {"textMatches": "(?i)(close app|закрыть приложение)"},
        {"resourceId": "android:id/aerr_close"},
    ]
    secondary_selectors = [
        {"textMatches": "(?i)(close app|закрыть приложение|don[’']?t allow|don't allow|не разрешать|запретить)"},
        {"resourceId": "android:id/button2"},
        {"resourceId": "com.android.permissioncontroller:id/permission_deny_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_deny_and_dont_ask_again_button"},
    ]
    while time.time() < deadline:
        clicked = False
        anr_windows = _system_anr_windows(serial)
        prefer_close = any(INSTAGRAM_PACKAGE in line for line in anr_windows)
        if device is not None:
            if prefer_close and _click_first(device, close_selectors, timeout_seconds=0.6):
                logger.info("system_dialog_dismissed: serial=%s action=anr_close", serial)
                clicked = True
            elif _click_first(device, wait_selectors, timeout_seconds=0.6):
                logger.info("system_dialog_dismissed: serial=%s action=anr_wait_button", serial)
                clicked = True
            elif _click_first(device, primary_selectors, timeout_seconds=0.6):
                logger.info("system_dialog_dismissed: serial=%s action=primary", serial)
                clicked = True
            elif _click_first(device, secondary_selectors, timeout_seconds=0.6):
                logger.info("system_dialog_dismissed: serial=%s action=secondary", serial)
                clicked = True
        if clicked:
            time.sleep(1.0)
            continue

        if anr_windows:
            width, height = _device_display_size(device)
            if prefer_close:
                _adb_tap(serial, width // 2, max(240, int(height * 0.50)))
            else:
                _adb_tap(serial, max(140, int(width * 0.16)), max(240, int(height * 0.56)))
            time.sleep(0.5)
            _adb_shell(serial, "input", "keyevent", "66", timeout=15, check=False)
            logger.info("system_dialog_dismissed: serial=%s action=%s", serial, "anr_close_tap" if prefer_close else "anr_wait")
            time.sleep(1.2)
            continue
        break


def _instagram_app_rate_dialog_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_title_area"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_rate_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_rate_later_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_cancel_button"},
        {"textMatches": "(?i)^rate instagram$"},
        {"textMatches": "(?i)^remind me later$"},
        {"textMatches": "(?i)^no, thanks$"},
    ]
    return _ig_find_first(device, selectors, timeout_seconds=0.4) is not None


def _dismiss_instagram_app_rate_dialog(device: Any, serial: str) -> bool:
    if not _instagram_app_rate_dialog_visible(device):
        return False
    dismiss_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_rate_later_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/appirater_cancel_button"},
        {"textMatches": "(?i)^remind me later$"},
        {"textMatches": "(?i)^no, thanks$"},
    ]
    if _ig_click_first(device, dismiss_selectors, timeout_seconds=0.8, serial=serial):
        logger.info("instagram_interstitial_dismissed: serial=%s action=app_rate_dialog", serial)
        time.sleep(1.0)
        return True
    width, height = _device_display_size(device)
    _adb_tap(serial, width // 2, int(height * 0.58))
    time.sleep(1.0)
    if not _instagram_app_rate_dialog_visible(device):
        logger.info("instagram_interstitial_dismissed: serial=%s action=app_rate_dialog_fallback", serial)
        return True
    return False


def _stabilize_emulator(serial: str) -> None:
    _disable_emulator_noise(serial)
    device = None
    try:
        device = _connect_ui(serial)
    except Exception:
        device = None

    deadline = time.time() + EMULATOR_STABILIZE_SECONDS
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.2)
        if not _system_anr_windows(serial):
            break
        time.sleep(1.0)

    _adb_shell(serial, "input", "keyevent", "3", timeout=10, check=False)
    time.sleep(1.0)


def _ensure_emulator_ready(preferred_serial: str = "") -> str:
    preferred = (preferred_serial or "").strip()
    if preferred.lower() == "default":
        preferred = ""
    existing = sorted(_list_running_emulators())
    if preferred and preferred in existing:
        serial = preferred
        _set_state(state="emulator_ready", emulator_serial=serial, detail=f"Эмулятор уже запущен: {serial}")
        logger.info("emulator_ready: serial=%s reused=true preferred=true", serial)
        _wait_for_boot(serial)
        _stabilize_emulator(serial)
        return serial
    if existing and not preferred:
        serial = existing[0]
        _set_state(state="emulator_ready", emulator_serial=serial, detail=f"Эмулятор уже запущен: {serial}")
        logger.info("emulator_ready: serial=%s reused=true preferred=false", serial)
        _wait_for_boot(serial)
        _stabilize_emulator(serial)
        return serial

    emulator_path = _resolve_emulator_path()
    if not emulator_path:
        raise RuntimeError("Android emulator not found.")

    avd_name = ANDROID_AVD_NAME
    if preferred:
        avd_name = _serial_to_avd_map().get(preferred, "").strip()
        if not avd_name:
            raise RuntimeError(f"Эмулятор {preferred} не запущен и для него не настроен AVD mapping.")
        port = _serial_emulator_port(preferred)
        if port is None:
            raise RuntimeError(f"Не удалось определить порт из serial {preferred}.")
        launch_commands = _emulator_launch_commands(emulator_path, avd_name, port=port)
    else:
        if not avd_name:
            raise RuntimeError("ANDROID_AVD_NAME is not configured.")
        launch_commands = _emulator_launch_commands(emulator_path, avd_name)

    avds = _list_avds()
    if avd_name not in avds:
        raise RuntimeError(f"AVD '{avd_name}' not found. Available: {', '.join(avds) or 'none'}")

    _set_state(state="emulator_starting", detail=f"Запускаю AVD: {avd_name}", emulator_serial=preferred)
    logger.info(
        "emulator_starting: avd=%s preferred_serial=%s launch_mode=%s",
        avd_name,
        preferred or "-",
        "headless" if "-no-window" in launch_commands[0] else "windowed",
    )
    previous = set(existing)
    process_key = preferred or avd_name
    process = EMULATOR_PROCESSES.get(process_key)
    launch_index = 0
    command = list(launch_commands[launch_index])
    if process is None or process.poll() is not None:
        if not USE_EMULATOR_SNAPSHOTS:
            command.extend(["-no-snapshot-load", "-no-snapshot-save"])
        EMULATOR_PROCESSES[process_key] = subprocess.Popen(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
    serial = preferred or _wait_for_device_serial(previous)
    max_attempts = max(2, len(launch_commands) + (1 if len(launch_commands) > 1 else 0))
    for attempt in range(max_attempts):
        try:
            _wait_for_boot(serial)
            _stabilize_emulator(serial)
            _set_state(state="emulator_ready", emulator_serial=serial, detail=f"Эмулятор готов: {serial}")
            logger.info("emulator_ready: serial=%s reused=false preferred=%s", serial, bool(preferred))
            return serial
        except Exception as exc:
            logger.warning(
                "emulator_boot_failed: serial=%s attempt=%s launch_mode=%s error=%s",
                serial,
                attempt + 1,
                "headless" if "-no-window" in command else "windowed",
                exc,
            )
            if attempt >= max_attempts - 1:
                raise
            _reset_publish_emulator_boundary(serial, clear_instagram=False)
            previous = set(_list_running_emulators())
            if launch_index + 1 < len(launch_commands):
                launch_index += 1
            command = list(launch_commands[launch_index])
            if not USE_EMULATOR_SNAPSHOTS:
                command.extend(["-no-snapshot-load", "-no-snapshot-save"])
            EMULATOR_PROCESSES[process_key] = subprocess.Popen(
                command,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
            serial = preferred or _wait_for_device_serial(previous)
    raise RuntimeError("Android emulator did not become ready.")


def _ensure_instagram_installed(serial: str) -> None:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    result = _run([adb_path, "-s", serial, "shell", "pm", "path", INSTAGRAM_PACKAGE], timeout=25, check=False)
    if result.returncode != 0 or not (result.stdout or "").strip():
        raise RuntimeError(f"Instagram app is not installed in emulator ({INSTAGRAM_PACKAGE}).")


def _open_play_store_listing(serial: str) -> None:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    intents = [
        [
            adb_path,
            "-s",
            serial,
            "shell",
            "am",
            "start",
            "-a",
            "android.intent.action.VIEW",
            "-d",
            f"market://details?id={INSTAGRAM_PACKAGE}",
            "com.android.vending",
        ],
        [
            adb_path,
            "-s",
            serial,
            "shell",
            "am",
            "start",
            "-a",
            "android.intent.action.VIEW",
            "-d",
            f"https://play.google.com/store/apps/details?id={INSTAGRAM_PACKAGE}",
        ],
    ]
    for command in intents:
        result = _run(command, timeout=25, check=False)
        output = ((result.stdout or "") + "\n" + (result.stderr or "")).strip().lower()
        if result.returncode == 0 and "error" not in output and "exception" not in output:
            return
    raise RuntimeError("Failed to open Google Play listing for Instagram.")


def _clear_instagram_data(serial: str) -> None:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    result = _run([adb_path, "-s", serial, "shell", "pm", "clear", INSTAGRAM_PACKAGE], timeout=35, check=False)
    output = ((result.stdout or "") + "\n" + (result.stderr or "")).strip().lower()
    if result.returncode != 0 or "success" not in output:
        raise RuntimeError(output or "Failed to clear Instagram app data.")
    _adb_shell(serial, "am", "force-stop", INSTAGRAM_PACKAGE, timeout=15, check=False)


def _launch_instagram_app(serial: str) -> None:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    last_detail = ""
    for attempt in range(1, 4):
        _adb_shell(serial, "input", "keyevent", "224", timeout=10, check=False)  # WAKEUP
        _adb_shell(serial, "wm", "dismiss-keyguard", timeout=10, check=False)
        _adb_shell(serial, "input", "keyevent", "82", timeout=10, check=False)  # MENU / unlock
        _adb_shell(serial, "input", "keyevent", "3", timeout=10, check=False)  # HOME
        time.sleep(0.8)
        _adb_shell(serial, "am", "force-stop", INSTAGRAM_PACKAGE, timeout=15, check=False)
        resolved = _run(
            [adb_path, "-s", serial, "shell", "cmd", "package", "resolve-activity", "--brief", INSTAGRAM_PACKAGE],
            timeout=20,
            check=False,
        )
        launch_activity = ""
        for line in (resolved.stdout or "").splitlines():
            line = line.strip()
            if line.startswith(INSTAGRAM_PACKAGE + "/"):
                launch_activity = line
                break
        if launch_activity:
            result = _run(
                [adb_path, "-s", serial, "shell", "am", "start", "-W", "-n", launch_activity],
                timeout=30,
                check=False,
            )
            output = ((result.stdout or "") + "\n" + (result.stderr or "")).strip().lower()
            if result.returncode != 0 or "error:" in output or "exception" in output:
                last_detail = (output or "Failed to start Instagram app activity.").strip()
        else:
            last_detail = "Instagram launch activity is not resolved."

        if not _instagram_is_foreground(serial):
            _adb_shell(serial, "am", "start", "-n", f"{INSTAGRAM_PACKAGE}/.activity.MainTabActivity", timeout=25, check=False)
            time.sleep(1.4)
        if not _instagram_is_foreground(serial):
            _run(
                [adb_path, "-s", serial, "shell", "monkey", "-p", INSTAGRAM_PACKAGE, "-c", "android.intent.category.LAUNCHER", "1"],
                timeout=25,
                check=False,
            )

        wait_seconds = 10.0 + (attempt * 5.0)
        if _wait_for_instagram_foreground(serial, timeout_seconds=wait_seconds):
            _set_state(state="app_opened", detail="Instagram app открыт в эмуляторе.")
            logger.info("app_opened: serial=%s package=%s attempt=%s", serial, INSTAGRAM_PACKAGE, attempt)
            return

        top_activity = _current_top_activity(serial)
        focus_window = _current_focus_window(serial)
        last_detail = (
            f"attempt={attempt} top_activity={top_activity or '-'} focus={focus_window or '-'} launch_detail={last_detail or '-'}"
        )
        logger.warning("instagram_foreground_wait_failed: serial=%s %s", serial, last_detail)
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
        time.sleep(1.0)

    raise RuntimeError(f"Instagram app did not come to foreground. {last_detail}".strip())


def _adb_shell(serial: str, *args: str, timeout: int = 30, check: bool = True) -> subprocess.CompletedProcess:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    return _run([adb_path, "-s", serial, "shell", *args], timeout=timeout, check=check)


def _adb_push(serial: str, source_path: str, target_path: str, timeout: int = 120) -> subprocess.CompletedProcess:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    return _run([adb_path, "-s", serial, "push", source_path, target_path], timeout=timeout, check=True)


def _current_top_activity(serial: str) -> str:
    result = _adb_shell(serial, "dumpsys", "activity", "top", timeout=35, check=False)
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if "ACTIVITY " in line and INSTAGRAM_PACKAGE in line:
            return line
    return ""


def _current_focus_window(serial: str) -> str:
    result = _adb_shell(serial, "dumpsys", "window", timeout=35, check=False)
    for line in (result.stdout or "").splitlines():
        line = line.strip()
        if line.startswith("mCurrentFocus=") or line.startswith("mFocusedApp="):
            if INSTAGRAM_PACKAGE in line:
                return line
    return ""


def _instagram_is_foreground(serial: str) -> bool:
    return bool(_current_top_activity(serial) or _current_focus_window(serial))


def _wait_for_instagram_foreground(serial: str, timeout_seconds: float = 15.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _instagram_is_foreground(serial):
            return True
        time.sleep(0.6)
    return False


def _adb_tap(serial: str, x: int, y: int) -> None:
    _adb_shell(serial, "input", "tap", str(int(x)), str(int(y)), timeout=15, check=False)


def _adb_swipe(serial: str, x1: int, y1: int, x2: int, y2: int, duration_ms: int = 350) -> None:
    _adb_shell(
        serial,
        "input",
        "swipe",
        str(int(x1)),
        str(int(y1)),
        str(int(x2)),
        str(int(y2)),
        str(max(120, int(duration_ms))),
        timeout=20,
        check=False,
    )


def _adb_input_text(serial: str, value: str) -> None:
    safe_value = (value or "").replace(" ", "%s")
    _adb_shell(serial, "input", "text", safe_value, timeout=20, check=False)


def _ensure_media_dir(serial: str) -> None:
    _adb_shell(serial, "mkdir", "-p", INSTAGRAM_PUBLISH_MEDIA_DIR, timeout=25, check=False)


def _grant_instagram_media_permissions(serial: str) -> None:
    permissions = [
        "android.permission.READ_MEDIA_IMAGES",
        "android.permission.READ_MEDIA_VIDEO",
        "android.permission.READ_EXTERNAL_STORAGE",
        "android.permission.WRITE_EXTERNAL_STORAGE",
        "android.permission.CAMERA",
        "android.permission.RECORD_AUDIO",
    ]
    for permission in permissions:
        _adb_shell(serial, "pm", "grant", INSTAGRAM_PACKAGE, permission, timeout=15, check=False)


def _import_video_into_emulator(serial: str, source_path: str) -> str:
    source = Path(source_path)
    if not source.exists():
        raise RuntimeError("Source video not found.")
    _ensure_media_dir(serial)
    target_path = f"{INSTAGRAM_PUBLISH_MEDIA_DIR.rstrip('/')}/{source.name}"
    _adb_push(serial, str(source), target_path, timeout=240)
    _adb_shell(
        serial,
        "am",
        "broadcast",
        "-a",
        "android.intent.action.MEDIA_SCANNER_SCAN_FILE",
        "-d",
        f"file://{target_path}",
        timeout=30,
        check=False,
    )
    _adb_shell(serial, "cmd", "media_session", "dispatch", "play", timeout=15, check=False)
    _adb_shell(serial, "cmd", "media", "rescan", target_path, timeout=45, check=False)
    return target_path


def _delete_local_source_video(path_value: str) -> None:
    path = Path(path_value)
    if path.exists() and path.is_file():
        path.unlink()


def _connect_ui(serial: str):
    if u2 is None:
        raise RuntimeError("uiautomator2 is not installed.")
    device = u2.connect(serial)
    try:
        device.implicitly_wait(3.0)
    except Exception:
        pass
    return device


def _obj_exists(obj: Any, timeout_seconds: float = 0.5) -> bool:
    exists_attr = getattr(obj, "exists", None)
    try:
        return bool(exists_attr)
    except Exception:
        pass
    if callable(exists_attr):
        try:
            return bool(exists_attr())
        except Exception:
            return False
    try:
        return bool(exists_attr)
    except Exception:
        return False


def _find_first(device: Any, selectors: list[dict[str, Any]], timeout_seconds: float = 1.5) -> Optional[Any]:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        for selector in selectors:
            obj = device(**selector)
            if _obj_exists(obj, timeout_seconds=0.3):
                return obj
        time.sleep(0.3)
    return None


def _safe_click(obj: Any) -> bool:
    try:
        obj.click()
        return True
    except Exception:
        return False


def _tap_object(serial: str, obj: Any) -> bool:
    center = _node_center(obj)
    if center is None:
        return _safe_click(obj)
    _adb_tap(serial, center[0], center[1])
    return True


def _instagram_selector(selector: dict[str, Any]) -> dict[str, Any]:
    prepared = dict(selector)
    if "packageName" in prepared:
        return prepared
    resource_id = str(prepared.get("resourceId") or "")
    if resource_id and ":" in resource_id and not resource_id.startswith(f"{INSTAGRAM_PACKAGE}:"):
        return prepared
    prepared["packageName"] = INSTAGRAM_PACKAGE
    return prepared


def _instagram_selectors(selectors: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [_instagram_selector(selector) for selector in selectors]


def _click_first(
    device: Any,
    selectors: list[dict[str, Any]],
    timeout_seconds: float = 2.0,
    *,
    serial: str = "",
) -> bool:
    obj = _find_first(device, selectors, timeout_seconds=timeout_seconds)
    if obj is None:
        return False
    if serial:
        return _tap_object(serial, obj)
    return _safe_click(obj)


def _ig_find_first(device: Any, selectors: list[dict[str, Any]], timeout_seconds: float = 1.5) -> Optional[Any]:
    return _find_first(device, _instagram_selectors(selectors), timeout_seconds=timeout_seconds)


def _ig_click_first(
    device: Any,
    selectors: list[dict[str, Any]],
    timeout_seconds: float = 2.0,
    *,
    serial: str = "",
) -> bool:
    return _click_first(
        device,
        _instagram_selectors(selectors),
        timeout_seconds=timeout_seconds,
        serial=serial,
    )


def _node_center(obj: Any) -> Optional[tuple[int, int]]:
    try:
        info = getattr(obj, "info", {}) or {}
    except Exception:
        info = {}
    bounds = info.get("bounds")
    if isinstance(bounds, dict):
        left = int(bounds.get("left") or 0)
        top = int(bounds.get("top") or 0)
        right = int(bounds.get("right") or 0)
        bottom = int(bounds.get("bottom") or 0)
        if right > left and bottom > top:
            return ((left + right) // 2, (top + bottom) // 2)
    return None


def _obj_text(obj: Any) -> str:
    try:
        info = getattr(obj, "info", {}) or {}
    except Exception:
        info = {}
    candidates = [
        info.get("text"),
        info.get("contentDescription"),
        info.get("description"),
        getattr(obj, "text", None),
        getattr(obj, "description", None),
    ]
    for candidate in candidates:
        if callable(candidate):
            try:
                candidate = candidate()
            except Exception:
                continue
        value = str(candidate or "").strip()
        if value:
            return value
    return ""


def _obj_selected(obj: Any) -> bool:
    try:
        info = getattr(obj, "info", {}) or {}
    except Exception:
        return False
    return bool(info.get("selected") or info.get("checked"))


def _parse_bounds_text(raw: Any) -> Optional[tuple[int, int, int, int]]:
    value = str(raw or "").strip()
    match = re.fullmatch(r"\[(\d+),(\d+)\]\[(\d+),(\d+)\]", value)
    if not match:
        return None
    left, top, right, bottom = (int(part) for part in match.groups())
    if right <= left or bottom <= top:
        return None
    return left, top, right, bottom


def _dump_ui_nodes(device: Any) -> list[dict[str, Any]]:
    try:
        try:
            hierarchy = device.dump_hierarchy(compressed=False)
        except TypeError:
            hierarchy = device.dump_hierarchy()
    except Exception:
        return []
    if not hierarchy:
        return []
    try:
        root = ET.fromstring(str(hierarchy))
    except Exception:
        return []
    nodes: list[dict[str, Any]] = []
    for elem in root.iter():
        if elem.tag != "node":
            continue
        bounds = _parse_bounds_text(elem.attrib.get("bounds"))
        text = str(elem.attrib.get("text") or "").strip()
        description = str(elem.attrib.get("content-desc") or "").strip()
        resource_id = str(elem.attrib.get("resource-id") or "").strip()
        if not any((text, description, resource_id, bounds)):
            continue
        nodes.append(
            {
                "text": text,
                "description": description,
                "resource_id": resource_id,
                "class_name": str(elem.attrib.get("class") or "").strip(),
                "clickable": str(elem.attrib.get("clickable") or "").strip().lower() == "true",
                "selected": str(elem.attrib.get("selected") or "").strip().lower() == "true",
                "bounds": bounds,
            }
        )
    return nodes


def _visible_screen_texts(device: Any) -> list[str]:
    texts: list[str] = []
    seen: set[str] = set()
    for node in _dump_ui_nodes(device):
        for raw in (node.get("text"), node.get("description")):
            value = str(raw or "").strip()
            if not value:
                continue
            key = value.casefold()
            if key in seen:
                continue
            seen.add(key)
            texts.append(value)
    return texts


def _compact_ui_text(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip())


def _focus_and_type(device: Any, serial: str, target: Any, value: str) -> None:
    try:
        target.click()
    except Exception:
        center = _node_center(target)
        if center is not None:
            _adb_tap(serial, center[0], center[1])
    time.sleep(0.4)

    if re.fullmatch(r"[A-Za-z0-9_.@\-]+", value or ""):
        _adb_input_text(serial, value)
        return

    try:
        device.send_keys(value, clear=True)
        return
    except Exception:
        pass

    try:
        target.set_text(value)
        return
    except Exception:
        pass

    raise RuntimeError("Не удалось ввести текст в поле Instagram.")


def _wait_until(predicate, timeout_seconds: float = 8.0, interval: float = 0.5) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            if predicate():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def _open_login_entrypoint(device: Any, serial: str) -> bool:
    existing_account_selectors = [
        {"textMatches": "(?i)(log in|login|войти)"},
        {"descriptionMatches": "(?i)(log in|login|войти)"},
        {"textMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"descriptionMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*log.?in.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*login.*button.*"},
    ]
    if _click_first(device, existing_account_selectors, timeout_seconds=1.0, serial=serial):
        logger.info("login_entry_tapped: serial=%s action=button", serial)
        time.sleep(1.8)
        return True

    landing_markers = [
        {"textMatches": "(?i)(create new account|sign up|from meta|already have an account)"},
        {"descriptionMatches": "(?i)(create new account|sign up|from meta|already have an account)"},
    ]
    signup_flow_markers = [
        {"textMatches": "(?i)(what['’]s your mobile number\\?|mobile number|sign up with email)"},
        {"descriptionMatches": "(?i)(what['’]s your mobile number\\?|mobile number|sign up with email)"},
        {"textMatches": "(?i)(create new account|sign up)"},
        {"descriptionMatches": "(?i)(create new account|sign up)"},
    ]
    if _find_first(device, signup_flow_markers, timeout_seconds=0.8) is not None:
        width, height = _device_display_size(device)
        _adb_tap(serial, width // 2, int(height * 0.93))
        logger.info("login_entry_tapped: serial=%s action=existing_account_link", serial)
        time.sleep(2.0)
        return True
    if _ig_find_first(device, landing_markers, timeout_seconds=0.8) is not None:
        width, height = _device_display_size(device)
        _adb_tap(serial, width // 2, int(height * 0.93))
        logger.info("login_entry_tapped: serial=%s action=fallback_tap", serial)
        time.sleep(2.0)
        return True
    return False


def _wait_for_login_screen(device: Any, serial: str) -> tuple[Any, Any]:
    deadline = time.time() + LOGIN_WAIT_SECONDS
    username_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/login_username"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/login_username_row"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*username.*"},
        {"textMatches": "(?i)(username, email or mobile number|phone number, username, or email|username|email or mobile number)"},
        {"className": "android.widget.EditText", "instance": 0},
    ]
    password_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/password"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*password.*"},
        {"textMatches": "(?i)^password$"},
        {"className": "android.widget.EditText", "instance": 1},
    ]
    landing_markers = [
        {"textMatches": "(?i)(create new account|sign up|from meta|already have an account|log in|cancel|forgot password)"},
        {"descriptionMatches": "(?i)(create new account|sign up|from meta|already have an account|log in|cancel|forgot password)"},
    ]
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.0)
        if not _instagram_is_foreground(serial):
            _launch_instagram_app(serial)

        username = _ig_find_first(device, username_selectors, timeout_seconds=0.8)
        password = _ig_find_first(device, password_selectors, timeout_seconds=0.8)
        if username is not None and password is not None:
            return username, password

        if _open_login_entrypoint(device, serial):
            continue

        try:
            maybe_continue = device(textMatches="(?i)(allow|continue|not now|ок|продолжить|разрешить|не сейчас|cancel|отмена)")
            if _obj_exists(maybe_continue, timeout_seconds=0.2):
                maybe_continue.click()
        except Exception:
            pass

        activity = _current_top_activity(serial)
        focus = _current_focus_window(serial)
        if (
            "ModalActivity" in activity or "ModalActivity" in focus
        ) and _find_first(device, landing_markers, timeout_seconds=0.4) is None:
            logger.info("login_modal_recover: serial=%s activity=%s focus=%s", serial, activity or "-", focus or "-")
            _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
            time.sleep(1.2)
            if not _instagram_is_foreground(serial):
                _launch_instagram_app(serial)
            continue
        time.sleep(0.6)
    raise RuntimeError("Instagram login screen did not appear.")


def _fill_credentials_and_submit(device: Any, serial: str, login: str, password: str) -> None:
    username_field, password_field = _wait_for_login_screen(device, serial)
    _focus_and_type(device, serial, username_field, login)
    _focus_and_type(device, serial, password_field, password)
    # Instagram keeps the keyboard open after filling the password field,
    # which hides the "Log in" button on small emulator screens.
    try:
        device.press("back")
        time.sleep(0.8)
    except Exception:
        pass
    logger.info("credentials_filled")
    _set_state(state="credentials_filled", detail="Логин и пароль введены в Instagram app.")

    submit = _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/button_text", "textMatches": "(?i)(log in|войти|login)"},
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/next_button"},
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*login.*button.*"},
            {"textMatches": "(?i)(log in|войти|login)"},
        ],
        timeout_seconds=6,
    )
    if submit is None:
        width, height = _device_display_size(device)
        _adb_tap(serial, width // 2, max(240, height - 250))
        time.sleep(1.0)
        submit = _ig_find_first(
            device,
            [
                {"resourceId": f"{INSTAGRAM_PACKAGE}:id/button_text", "textMatches": "(?i)(log in|войти|login)"},
                {"resourceId": f"{INSTAGRAM_PACKAGE}:id/next_button"},
                {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*login.*button.*"},
                {"textMatches": "(?i)(log in|войти|login)"},
            ],
            timeout_seconds=2,
        )
    if submit is None:
        _adb_shell(serial, "input", "keyevent", "66", timeout=10, check=False)
    else:
        submit.click()
    logger.info("login_clicked")
    _set_state(state="login_clicked", detail="Кнопка входа нажата. Дальше работай вручную в приложении.")


def _login_form_visible(device: Any) -> bool:
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/login_username"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/password"},
        {"textMatches": "(?i)(username, email or mobile number|mobile number or email|phone number, username, or email)"},
        {"textMatches": "(?i)^password$"},
        {"textMatches": "(?i)^log in$"},
    ]
    return _ig_find_first(device, selectors, timeout_seconds=0.6) is not None


def _signed_out_surface_visible(device: Any, serial: str) -> bool:
    selectors = [
        {"textMatches": "(?i)(create new account|sign up|sign up with email)"},
        {"descriptionMatches": "(?i)(create new account|sign up|sign up with email)"},
        {"textMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"descriptionMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"textMatches": "(?i)(what's your mobile number\\?|mobile number)"},
        {"descriptionMatches": "(?i)(what's your mobile number\\?|mobile number)"},
    ]
    activity = _current_top_activity(serial)
    focus = _current_focus_window(serial)
    if "BloksSignedOutFragmentActivity" in activity or "BloksSignedOutFragmentActivity" in focus:
        return True
    return _ig_find_first(device, selectors, timeout_seconds=0.6) is not None


def _profile_surface_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    markers = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_header_actions_top_row"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/(profile_header_.*|.*edit_profile.*|.*share_profile.*|row_profile_header_.*)"},
        {"textMatches": "(?i)(edit profile|share profile|professional dashboard|view archive|threads)"},
    ]
    if _ig_find_first(device, markers, timeout_seconds=0.6) is not None:
        return True
    profile_tab = _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab"},
            {"descriptionMatches": "(?i)^profile$"},
            {"descriptionMatches": "(?i)(profile tab|open profile)"},
        ],
        timeout_seconds=0.4,
    )
    if profile_tab is not None and _obj_selected(profile_tab):
        return True
    return False


def _bottom_nav_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/home_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/search_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab"},
    ]
    hits = 0
    for selector in selectors:
        if _ig_find_first(device, [selector], timeout_seconds=0.15) is not None:
            hits += 1
    return hits >= 2


def _post_publish_feed_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    if not _bottom_nav_visible(device):
        return False
    if _profile_surface_visible(device):
        return False
    if _clips_viewer_visible(device) or _feed_contextual_viewer_visible(device):
        return False
    if _quick_capture_visible(device) or _comment_sheet_visible(device):
        return False
    return True


def _post_publish_share_sheet_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"textMatches": "(?i)(add to story|copy link|links you share are unique|learn more)"},
        {"descriptionMatches": "(?i)(add to story|copy link|sms|whatsapp)"},
        {"textMatches": "(?i)(whatsapp|sms|friends|reels)"},
        {"textMatches": "(?i)(search|поиск)"},
    ]
    hits = 0
    for selector in selectors:
        if _ig_find_first(device, [selector], timeout_seconds=0.2) is not None:
            hits += 1
    if hits >= 2:
        return True

    resource_suffixes = {
        ":id/direct_private_share_container_view",
        ":id/direct_private_share_search_box",
        ":id/link_tracking_disclosure_text_view",
        ":id/direct_private_share_bottom_control_container",
        ":id/direct_external_share_container_view",
        ":id/direct_external_reshare_row",
    }
    text_markers = {
        "links you share are unique",
        "add to story",
        "copy link",
        "whatsapp",
        "sms",
        "search",
    }
    resource_hits: set[str] = set()
    text_hits: set[str] = set()
    for node in _dump_ui_nodes(device):
        resource_id = str(node.get("resource_id") or "").strip()
        if any(resource_id.endswith(suffix) for suffix in resource_suffixes):
            resource_hits.add(resource_id)
        combined = " ".join(str(node.get(key) or "").strip() for key in ("text", "description")).lower()
        if not combined:
            continue
        for marker in text_markers:
            if marker in combined:
                text_hits.add(marker)
    return len(resource_hits) >= 2 or (bool(resource_hits) and len(text_hits) >= 2)


def _published_reel_viewer_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"textMatches": "(?i)^insights$"},
        {"descriptionMatches": "(?i)^insights$"},
        {"textMatches": "(?i)^view insights$"},
        {"descriptionMatches": "(?i)^view insights$"},
        {"textMatches": "(?i)^boost$"},
        {"descriptionMatches": "(?i)^boost$"},
        {"textMatches": "(?i)^boost reel$"},
        {"descriptionMatches": "(?i)^boost reel$"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*insight.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*boost.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*promot.*"},
    ]
    markers: set[str] = set()
    for selector in selectors:
        joined = " ".join(str(selector.get(key) or "").lower() for key in ("textMatches", "descriptionMatches", "resourceIdMatches"))
        hit = _ig_find_first(device, [selector], timeout_seconds=0.2)
        if hit is None:
            continue
        if "insight" in joined:
            markers.add("insights")
        if "boost" in joined or "promot" in joined:
            markers.add("boost")
    if len(markers) >= 2 and (_bottom_nav_visible(device) or _reel_viewer_visible(device)):
        return True

    text_markers: set[str] = set()
    for node in _dump_ui_nodes(device):
        combined = " ".join(str(node.get(key) or "").strip() for key in ("text", "description")).lower()
        resource_id = str(node.get("resource_id") or "").strip().lower()
        if "insight" in combined or "insight" in resource_id:
            text_markers.add("insights")
        if "boost" in combined or "boost" in resource_id or "promot" in combined or "promot" in resource_id:
            text_markers.add("boost")
    return len(text_markers) >= 2 and (_bottom_nav_visible(device) or _reel_viewer_visible(device))


def _keyboard_visible(serial: str) -> bool:
    result = _adb_shell(serial, "dumpsys", "window", "windows", timeout=25, check=False)
    output = str(result.stdout or "")
    if "mInputMethodWindow=Window" not in output and "InputMethod" not in output:
        return False
    if re.search(r"Window #\d+ Window\{[^}]+ InputMethod\}:.*?mHasSurface=true.*?isVisible=true", output, re.S):
        return True
    return bool(re.search(r"Window #\d+ Window\{[^}]+ InputMethod\}:.*?mHasSurface=true", output, re.S))


def _clips_viewer_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_viewer_container"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_viewer_action_bar"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_viewer_view_pager"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_author_profile_pic"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_caption_component"},
    ]
    return _ig_find_first(device, selectors, timeout_seconds=0.5) is not None


def _feed_contextual_viewer_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    if _bottom_nav_visible(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/row_feed_profile_header"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/row_feed_photo_profile_name"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/row_feed_photo_profile_imageview"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/comment_button"},
    ]
    hits = 0
    for selector in selectors:
        if _ig_find_first(device, [selector], timeout_seconds=0.2) is not None:
            hits += 1
    return hits >= 2


def _reel_viewer_visible(device: Any) -> bool:
    return _clips_viewer_visible(device) or _feed_contextual_viewer_visible(device)


def _comment_sheet_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_container"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/layout_comment_thread_edittext_multiline"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/comment_composer_parent_updated"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_close_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/comment_composer_text_view"},
    ]
    return _ig_find_first(device, selectors, timeout_seconds=0.5) is not None


def _quick_capture_visible(device: Any) -> bool:
    if device is None or not callable(device):
        return False
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/quick_capture_fragment_container"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/quick_capture_root_container"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/camera_home_button"},
    ]
    return _ig_find_first(device, selectors, timeout_seconds=0.5) is not None


def _profile_surface_flags(device: Any, serial: str) -> dict[str, Any]:
    keyboard_visible = _keyboard_visible(serial)
    comment_sheet_visible = _comment_sheet_visible(device)
    clips_viewer_visible = _clips_viewer_visible(device)
    quick_capture_visible = _quick_capture_visible(device)
    share_sheet_visible = _post_publish_share_sheet_visible(device)
    instagram_dialog_visible = _instagram_app_rate_dialog_visible(device)
    profile_visible = _profile_surface_visible(device)
    post_publish_feed_visible = _post_publish_feed_visible(device)
    if profile_visible and not any((keyboard_visible, comment_sheet_visible, clips_viewer_visible, quick_capture_visible, share_sheet_visible)):
        state = "profile"
    elif instagram_dialog_visible:
        state = "instagram_dialog"
    elif share_sheet_visible:
        state = "post_publish_share_sheet"
    elif comment_sheet_visible:
        state = "comment_sheet"
    elif clips_viewer_visible or _feed_contextual_viewer_visible(device):
        state = "clips_viewer"
    elif quick_capture_visible:
        state = "quick_capture"
    elif keyboard_visible:
        state = "keyboard"
    elif post_publish_feed_visible:
        state = "post_publish_feed"
    elif _instagram_is_foreground(serial):
        state = "instagram_main"
    else:
        state = "not_foreground"
    return {
        "profile_surface_state": state,
        "profile_visible": profile_visible,
        "keyboard_visible": keyboard_visible,
        "comment_sheet_visible": comment_sheet_visible,
        "clips_viewer_visible": clips_viewer_visible or _feed_contextual_viewer_visible(device),
        "quick_capture_visible": quick_capture_visible,
        "share_sheet_visible": share_sheet_visible,
        "instagram_dialog_visible": instagram_dialog_visible,
        "post_publish_feed_visible": post_publish_feed_visible,
    }


def _profile_nav_selectors() -> list[dict[str, Any]]:
    return [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab"},
        {"descriptionMatches": "(?i)^profile$"},
        {"descriptionMatches": "(?i)(profile tab|open profile)"},
    ]


def _tap_profile_tab(device: Any, serial: str) -> bool:
    if _ig_click_first(device, _profile_nav_selectors(), timeout_seconds=0.8, serial=serial):
        return True
    _tap_bottom_nav_slot(device, serial, 4)
    return True


def _promote_surface_to_profile(device: Any, serial: str) -> bool:
    if _profile_surface_visible(device):
        return True
    if not (_post_publish_feed_visible(device) or _bottom_nav_visible(device)):
        return False
    _tap_profile_tab(device, serial)
    time.sleep(1.0)
    return _profile_surface_visible(device)


def _close_keyboard(device: Any, serial: str) -> bool:
    if not _keyboard_visible(serial):
        return False
    try:
        device.press("back")
    except Exception:
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
    time.sleep(0.8)
    return True


def _close_comment_sheet(device: Any, serial: str) -> bool:
    handled = False
    close_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_close_button"},
        {"descriptionMatches": "(?i)(close|dismiss|закрыть)"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_end_nav_button_icon"},
    ]
    if _ig_click_first(device, close_selectors, timeout_seconds=0.8, serial=serial):
        handled = True
        time.sleep(1.0)
        if not _comment_sheet_visible(device):
            return True
    handle = _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_drag_handle"},
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/bottom_sheet_drag_handle_prism"},
        ],
        timeout_seconds=0.6,
    )
    width, height = _device_display_size(device)
    if handle is not None:
        center = _node_center(handle)
        if center is not None:
            _adb_swipe(serial, center[0], center[1], center[0], min(height - 240, center[1] + int(height * 0.42)), 320)
            handled = True
            time.sleep(1.0)
            if not _comment_sheet_visible(device):
                return True
    _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
    time.sleep(0.9)
    return handled or not _comment_sheet_visible(device)


def _close_post_publish_share_sheet(device: Any, serial: str) -> bool:
    if not _post_publish_share_sheet_visible(device):
        return False
    _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
    time.sleep(1.0)
    return not _post_publish_share_sheet_visible(device)


def _close_clips_viewer(device: Any, serial: str) -> bool:
    handled = False
    back_selectors = [
        {"descriptionMatches": "(?i)(back|go back|close|dismiss)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*back.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*close.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*left.*button.*"},
    ]
    if _ig_click_first(device, back_selectors, timeout_seconds=0.8, serial=serial):
        handled = True
        time.sleep(1.0)
        if not _reel_viewer_visible(device):
            return True
    _adb_tap(serial, 64, 112)
    handled = True
    time.sleep(1.0)
    if not _reel_viewer_visible(device):
        return True
    _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
    time.sleep(1.0)
    return handled or not _reel_viewer_visible(device)


def _exit_quick_capture(device: Any, serial: str) -> bool:
    handled = False
    if _ig_click_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/camera_home_button"},
            {"descriptionMatches": "(?i)(close camera|camera home|back)"},
        ],
        timeout_seconds=0.8,
        serial=serial,
    ):
        handled = True
        time.sleep(1.0)
        if not _quick_capture_visible(device):
            return True
    _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
    time.sleep(1.0)
    return handled or not _quick_capture_visible(device)


def _force_main_tab(serial: str) -> None:
    _adb_shell(serial, "am", "start", "-n", f"{INSTAGRAM_PACKAGE}/.activity.MainTabActivity", timeout=20, check=False)
    time.sleep(1.4)


def _recover_to_profile_surface(device: Any, serial: str, timeout_seconds: float = 20.0) -> bool:
    deadline = time.time() + timeout_seconds
    forced_main_tab_attempts = 0
    last_state = ""
    while time.time() < deadline:
        flags = _profile_surface_flags(device, serial)
        state = str(flags.get("profile_surface_state") or "")
        if state != last_state:
            logger.info("profile_recovery_step: serial=%s state=%s flags=%s", serial, state or "-", flags)
            last_state = state
        if bool(flags.get("profile_visible")) and not any(
            (
                bool(flags.get("keyboard_visible")),
                bool(flags.get("comment_sheet_visible")),
                bool(flags.get("clips_viewer_visible")),
                bool(flags.get("quick_capture_visible")),
            )
        ):
            logger.info("profile_recovery_success: serial=%s state=%s", serial, state or "profile")
            return True
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.8)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.6)
        if bool(flags.get("instagram_dialog_visible")):
            time.sleep(0.8)
            continue
        if bool(flags.get("share_sheet_visible")):
            if bool(flags.get("clips_viewer_visible")) or bool(flags.get("post_publish_feed_visible")) or _bottom_nav_visible(device):
                if _promote_surface_to_profile(device, serial):
                    logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_from_share_sheet", serial)
                    return True
            _close_post_publish_share_sheet(device, serial)
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_share_sheet_close", serial)
                return True
            if bool(flags.get("clips_viewer_visible")):
                _close_clips_viewer(device, serial)
                if _promote_surface_to_profile(device, serial):
                    logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_share_sheet_clips_viewer", serial)
                    return True
            if bool(flags.get("comment_sheet_visible")):
                _close_comment_sheet(device, serial)
                if _promote_surface_to_profile(device, serial):
                    logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_share_sheet_comment_sheet", serial)
                    return True
            if bool(flags.get("keyboard_visible")):
                _close_keyboard(device, serial)
                if _promote_surface_to_profile(device, serial):
                    logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_share_sheet_keyboard", serial)
                    return True
            continue
        if bool(flags.get("keyboard_visible")):
            _close_keyboard(device, serial)
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_keyboard", serial)
                return True
            continue
        if bool(flags.get("comment_sheet_visible")):
            _close_comment_sheet(device, serial)
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_comment_sheet", serial)
                return True
            continue
        if bool(flags.get("clips_viewer_visible")):
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_from_clips_viewer", serial)
                return True
            _close_clips_viewer(device, serial)
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_clips_viewer", serial)
                return True
            continue
        if bool(flags.get("quick_capture_visible")):
            _exit_quick_capture(device, serial)
            if _promote_surface_to_profile(device, serial):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_after_quick_capture", serial)
                return True
            continue
        if bool(flags.get("post_publish_feed_visible")):
            _tap_profile_tab(device, serial)
            time.sleep(1.0)
            if _profile_surface_visible(device):
                logger.info("profile_recovery_success: serial=%s state=profile_tab_opened_from_feed", serial)
                return True
            if forced_main_tab_attempts < 2:
                _force_main_tab(serial)
                forced_main_tab_attempts += 1
            continue
        if not _instagram_is_foreground(serial):
            _launch_instagram_app(serial)
            continue
        _tap_profile_tab(device, serial)
        time.sleep(1.0)
        if _profile_surface_visible(device):
            logger.info("profile_recovery_success: serial=%s state=profile_tab_opened", serial)
            return True
        if forced_main_tab_attempts < 2:
            _force_main_tab(serial)
            forced_main_tab_attempts += 1
            continue
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
        time.sleep(0.8)
    logger.warning("profile_recovery_failed: serial=%s state=%s", serial, last_state or "-")
    return False


def _open_profile_tab(device: Any, serial: str, timeout_seconds: float = 10.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _recover_to_profile_surface(device, serial, timeout_seconds=min(6.0, max(1.0, deadline - time.time()))):
            return True
        _tap_profile_tab(device, serial)
        time.sleep(1.0)
        if _profile_surface_visible(device):
            return True
    return _profile_surface_visible(device)


def _profile_reels_tab_selected(device: Any) -> bool:
    reel_keywords = ("reels", "clips")
    selectors = [
        {
            "resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab_icon_view",
            "descriptionMatches": "(?i)^reels$",
        },
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*(clips|reels).*tab.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*(clips|reels).*"},
        {"descriptionMatches": "(?i)^reels$"},
        {"descriptionMatches": "(?i)(reels tab|clips tab)"},
        {"textMatches": "(?i)^reels$"},
    ]
    for selector in selectors:
        target = _ig_find_first(device, [selector], timeout_seconds=0.3)
        if target is not None and _obj_selected(target):
            return True
    icon_nodes: list[dict[str, Any]] = []
    for node in _dump_ui_nodes(device):
        if str(node.get("resource_id") or "") != f"{INSTAGRAM_PACKAGE}:id/profile_tab_icon_view":
            continue
        icon_nodes.append(node)
        label = f"{node.get('text') or ''} {node.get('description') or ''}".strip().casefold()
        if any(keyword in label for keyword in reel_keywords) and bool(node.get("selected")):
            return True
    icon_nodes.sort(key=lambda item: (int(item.get("bounds", [0, 0, 0, 0])[0]), int(item.get("bounds", [0, 0, 0, 0])[1])))
    if len(icon_nodes) >= 2 and bool(icon_nodes[1].get("selected")):
        return True
    return False


def _profile_reels_grid_candidates(device: Any) -> list[tuple[int, int, int, int]]:
    width, height = _device_display_size(device)
    candidates: list[tuple[int, int, int, int]] = []
    for node in _dump_ui_nodes(device):
        bounds = node.get("bounds")
        if not bounds:
            continue
        left, top, right, bottom = bounds
        node_width = right - left
        node_height = bottom - top
        if top < int(height * 0.28) or bottom > int(height * 0.86):
            continue
        if node_width < int(width * 0.18) or node_width > int(width * 0.45):
            continue
        if node_height < int(height * 0.14) or node_height > int(height * 0.42):
            continue
        resource_id = str(node.get("resource_id") or "").lower()
        class_name = str(node.get("class_name") or "").lower()
        description = f"{node.get('text') or ''} {node.get('description') or ''}".lower()
        score = 0
        if node.get("clickable"):
            score += 2
        if any(token in resource_id for token in ("thumbnail", "image", "media", "clips", "reel")):
            score += 3
        if any(token in class_name for token in ("imageview", "frame", "viewgroup")):
            score += 1
        if "reel" in description or "video" in description:
            score += 1
        if score <= 0:
            continue
        candidates.append((score, left, top, right, bottom))
    candidates.sort(key=lambda item: (-item[0], item[2], item[1]))
    return candidates


def _profile_reels_grid_visible(device: Any) -> bool:
    explicit_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_grid_recyclerview"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/preview_clip_thumbnail"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_grid_shimmer_container"},
    ]
    if _ig_find_first(device, explicit_selectors, timeout_seconds=0.3) is not None:
        return True
    return bool(_profile_reels_grid_candidates(device))


def _open_profile_reels_tab(device: Any, serial: str, timeout_seconds: float = 12.0) -> bool:
    if not _open_profile_tab(device, serial, timeout_seconds=min(timeout_seconds, 8.0)):
        return False
    deadline = time.time() + timeout_seconds
    selectors = [
        {
            "resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab_icon_view",
            "descriptionMatches": "(?i)^reels$",
        },
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*(clips|reels).*tab.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*(clips|reels).*"},
        {"descriptionMatches": "(?i)^reels$"},
        {"descriptionMatches": "(?i)(reels tab|clips tab)"},
        {"textMatches": "(?i)^reels$"},
    ]
    width, height = _device_display_size(device)
    fallback_x = width // 2
    fallback_y = max(240, int(height * 0.52))
    tab_strip = _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tabs_container"},
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab_layout"},
        ],
        timeout_seconds=0.5,
    )
    if tab_strip is not None:
        center = _node_center(tab_strip)
        bounds = getattr(tab_strip, "info", {}).get("bounds") if hasattr(tab_strip, "info") else None
        if isinstance(bounds, dict):
            left = int(bounds.get("left", 0) or 0)
            right = int(bounds.get("right", width) or width)
            top = int(bounds.get("top", fallback_y) or fallback_y)
            bottom = int(bounds.get("bottom", fallback_y) or fallback_y)
            fallback_x = left + max(1, (right - left) // 2)
            fallback_y = top + max(1, (bottom - top) // 2)
        elif center is not None:
            fallback_x, fallback_y = center
    while time.time() < deadline:
        reels_grid_visible = _profile_reels_grid_visible(device)
        if reels_grid_visible and (_profile_reels_tab_selected(device) or reels_grid_visible):
            logger.info("profile_reels_tab_opened: serial=%s method=selected_grid", serial)
            return True
        if _comment_sheet_visible(device) or _reel_viewer_visible(device) or _quick_capture_visible(device):
            _recover_to_profile_surface(device, serial, timeout_seconds=6.0)
            continue
        if _ig_click_first(device, selectors, timeout_seconds=0.8, serial=serial):
            time.sleep(1.0)
            reels_grid_visible = _profile_reels_grid_visible(device)
            if reels_grid_visible and (_profile_reels_tab_selected(device) or reels_grid_visible):
                logger.info("profile_reels_tab_opened: serial=%s method=selector", serial)
                return True
            continue
        _adb_tap(serial, fallback_x, fallback_y)
        time.sleep(1.0)
        reels_grid_visible = _profile_reels_grid_visible(device)
        if reels_grid_visible and (_profile_reels_tab_selected(device) or reels_grid_visible):
            logger.info("profile_reels_tab_opened: serial=%s method=fallback_tap", serial)
            return True
    reels_grid_visible = _profile_reels_grid_visible(device)
    return reels_grid_visible and (_profile_reels_tab_selected(device) or reels_grid_visible)


def _refresh_profile_reels_tab(device: Any, serial: str) -> None:
    if not _open_profile_reels_tab(device, serial, timeout_seconds=6.0):
        return
    width, height = _device_display_size(device)
    _adb_swipe(
        serial,
        width // 2,
        max(280, int(height * 0.36)),
        width // 2,
        min(height - 220, int(height * 0.74)),
        280,
    )
    time.sleep(1.4)


def _profile_reels_grid_centers(device: Any, *, limit: int) -> list[tuple[int, int]]:
    width, height = _device_display_size(device)
    candidates = _profile_reels_grid_candidates(device)
    centers: list[tuple[int, int]] = []
    for _, left, top, right, bottom in candidates:
        center = ((left + right) // 2, (top + bottom) // 2)
        if any(abs(center[0] - existing[0]) < 40 and abs(center[1] - existing[1]) < 40 for existing in centers):
            continue
        centers.append(center)
        if len(centers) >= max(1, int(limit)):
            break
    if len(centers) >= max(1, int(limit)):
        centers.sort(key=lambda item: (item[1], item[0]))
        return centers[: max(1, int(limit))]
    fallback_y = max(360, int(height * 0.48))
    fallback = [
        (int(width * (1 / 6)), fallback_y),
        (int(width * (3 / 6)), fallback_y),
        (int(width * (5 / 6)), fallback_y),
    ]
    return fallback[: max(1, int(limit))]


def _profile_reels_slot_centers(device: Any) -> list[tuple[int, int]]:
    return _profile_reels_grid_centers(device, limit=PUBLISH_PROFILE_CHECK_SLOTS)


def _scroll_profile_reels_grid(device: Any, serial: str) -> None:
    width, height = _device_display_size(device)
    _adb_swipe(
        serial,
        width // 2,
        min(height - 320, int(height * 0.76)),
        width // 2,
        max(320, int(height * 0.34)),
        320,
    )
    time.sleep(1.2)


def _close_surface_to_profile(device: Any, serial: str, timeout_seconds: float = 8.0) -> bool:
    return _recover_to_profile_surface(device, serial, timeout_seconds=timeout_seconds)


def _parse_relative_age_seconds(raw: str) -> Optional[int]:
    value = re.sub(r"\s+", " ", str(raw or "").strip().lower())
    if not value:
        return None
    if value in {"just now", "только что", "now", "сейчас"}:
        return 0
    patterns = [
        (r"\b(\d+)\s*(?:s|sec|secs|second|seconds)\s*ago\b", 1),
        (r"\b(\d+)\s*(?:m|min|mins|minute|minutes)\s*ago\b", 60),
        (r"\b(\d+)\s*(?:h|hr|hrs|hour|hours)\s*ago\b", 3600),
        (r"\b(\d+)\s*(?:d|day|days)\s*ago\b", 86400),
        (r"\b(\d+)\s*(?:w|wk|wks|week|weeks)\s*ago\b", 604800),
        (r"\b(\d+)\s*(?:s|sec|secs)\b", 1),
        (r"\b(\d+)\s*(?:m|min|mins)\b", 60),
        (r"\b(\d+)\s*(?:h|hr|hrs)\b", 3600),
        (r"\b(\d+)\s*(?:d|day|days)\b", 86400),
        (r"\b(\d+)\s*(?:w|wk|wks|week|weeks)\b", 604800),
        (r"\b(\d+)\s*(?:сек|секунда|секунды|секунд)\s*назад\b", 1),
        (r"\b(\d+)\s*(?:мин|минута|минуты|минут)\s*назад\b", 60),
        (r"\b(\d+)\s*(?:ч|час|часа|часов)\s*назад\b", 3600),
        (r"\b(\d+)\s*(?:д|дн|день|дня|дней)\s*назад\b", 86400),
        (r"\b(\d+)\s*(?:нед|неделю|недели|недель)\s*назад\b", 604800),
        (r"\b(\d+)\s*(?:нед|неделю|недели|недель)\b", 604800),
    ]
    for pattern, multiplier in patterns:
        match = re.search(pattern, value)
        if match:
            try:
                return max(0, int(match.group(1)) * multiplier)
            except Exception:
                return None
    return None


def _extract_relative_age_from_texts(texts: list[str]) -> tuple[Optional[int], str]:
    best_age: Optional[int] = None
    best_label = ""
    for text in texts:
        age_seconds = _parse_relative_age_seconds(text)
        if age_seconds is None:
            continue
        if best_age is None or age_seconds < best_age:
            best_age = age_seconds
            best_label = str(text or "").strip()
    return best_age, best_label


def _screen_text_snapshot(device: Any) -> list[dict[str, Any]]:
    snapshot: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for node in _dump_ui_nodes(device):
        resource_id = str(node.get("resource_id") or "").strip()
        bounds = node.get("bounds")
        for field in ("text", "description"):
            value = str(node.get(field) or "").strip()
            if not value:
                continue
            key = (field, value.casefold(), resource_id.casefold())
            if key in seen:
                continue
            seen.add(key)
            snapshot.append(
                {
                    "value": value,
                    "field": field,
                    "resource_id": resource_id,
                    "class_name": str(node.get("class_name") or "").strip(),
                    "bounds": bounds,
                }
            )
    return snapshot


def _viewer_text_candidates(device: Any) -> list[str]:
    width, height = _device_display_size(device)
    targeted: list[str] = []
    general: list[str] = []
    seen: set[str] = set()
    for entry in _screen_text_snapshot(device):
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        lowered = value.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        resource_id = str(entry.get("resource_id") or "").casefold()
        bounds = entry.get("bounds")
        near_viewer_meta = False
        if isinstance(bounds, tuple) and len(bounds) == 4:
            _, top, _, bottom = bounds
            near_viewer_meta = top < int(height * 0.26) or bottom > int(height * 0.52)
        if any(token in resource_id for token in ("time", "timestamp", "date", "meta", "subtitle", "caption", "header", "profile", "comment")) or near_viewer_meta:
            targeted.append(value)
        general.append(value)
    ordered = targeted + [value for value in general if value not in targeted]
    return ordered


def _snapshot_entry_center(entry: dict[str, Any]) -> Optional[tuple[int, int]]:
    bounds = entry.get("bounds")
    if not isinstance(bounds, tuple) or len(bounds) != 4:
        return None
    left, top, right, bottom = bounds
    return ((int(left) + int(right)) // 2, (int(top) + int(bottom)) // 2)


def _parse_metric_count(raw: Any) -> Optional[int]:
    value = str(raw or "").strip().lower()
    if not value:
        return None
    value = value.replace("\xa0", " ").replace("тыс.", "k").replace("тыс", "k").replace("млн", "m")
    value = re.sub(r"\s+", "", value)
    match = re.search(r"(\d[\d.,]*)([kmb])?\b", value)
    if match is None:
        return None
    number_raw = str(match.group(1) or "").strip()
    suffix = str(match.group(2) or "").strip().lower()
    if not number_raw:
        return None
    if suffix:
        try:
            return int(round(float(number_raw.replace(",", ".")) * {"k": 1_000, "m": 1_000_000, "b": 1_000_000_000}.get(suffix, 1)))
        except Exception:
            return None
    digits_only = re.sub(r"[^\d]", "", number_raw)
    if not digits_only:
        return None
    try:
        return int(digits_only)
    except Exception:
        return None


def _parse_metric_percent(raw: Any) -> Optional[float]:
    value = str(raw or "").strip().replace("\xa0", " ")
    match = re.search(r"(\d+(?:[.,]\d+)?)\s*%", value)
    if match is None:
        return None
    try:
        return float(str(match.group(1)).replace(",", "."))
    except Exception:
        return None


def _parse_metric_duration_seconds(raw: Any) -> Optional[float]:
    value = str(raw or "").strip().lower().replace("\xa0", " ")
    if not value:
        return None
    if re.fullmatch(r"\d{1,2}:\d{2}(?::\d{2})?", value):
        parts = [int(part) for part in value.split(":")]
        if len(parts) == 2:
            return float(parts[0] * 60 + parts[1])
        if len(parts) == 3:
            return float(parts[0] * 3600 + parts[1] * 60 + parts[2])
    hours_match = re.search(r"(\d+)\s*(?:h|hr|hrs|hour|hours|ч|час|часа|часов)", value)
    minutes_match = re.search(r"(\d+)\s*(?:m|min|mins|minute|minutes|мин|минута|минуты|минут)", value)
    seconds_match = re.search(r"(\d+)\s*(?:s|sec|secs|second|seconds|сек|секунда|секунды|секунд)", value)
    if not any((hours_match, minutes_match, seconds_match)):
        return None
    hours = int(hours_match.group(1)) if hours_match else 0
    minutes = int(minutes_match.group(1)) if minutes_match else 0
    seconds = int(seconds_match.group(1)) if seconds_match else 0
    return float(hours * 3600 + minutes * 60 + seconds)


def _metric_label_matches(value: str, patterns: tuple[str, ...]) -> bool:
    lowered = str(value or "").strip().lower()
    if not lowered:
        return False
    return any(re.search(pattern, lowered) for pattern in patterns)


def _metric_value_near_label(
    entries: list[dict[str, Any]],
    label_patterns: tuple[str, ...],
    *,
    parser: Callable[[Any], Any],
) -> Any:
    numeric_entries: list[dict[str, Any]] = []
    for entry in entries:
        parsed = parser(entry.get("value"))
        center = _snapshot_entry_center(entry)
        if parsed is None or center is None:
            continue
        numeric_entries.append({**entry, "_center": center, "_parsed": parsed})

    best_score: float | None = None
    best_value: Any = None
    for entry in entries:
        if not _metric_label_matches(str(entry.get("value") or ""), label_patterns):
            continue
        center = _snapshot_entry_center(entry)
        if center is None:
            continue
        for numeric in numeric_entries:
            candidate_center = numeric["_center"]
            dy = abs(int(candidate_center[1]) - int(center[1]))
            if dy > 180:
                continue
            dx = int(candidate_center[0]) - int(center[0])
            score = float(dy) + (abs(dx) / 10.0)
            if dx < -40:
                score += 40.0
            if best_score is None or score < best_score:
                best_score = score
                best_value = numeric["_parsed"]
    return best_value


def _extract_reel_metrics_from_entries(entries: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "plays_count": _metric_value_near_label(entries, (r"\b(views|plays|просмотры|просмотров)\b",), parser=_parse_metric_count),
        "likes_count": _metric_value_near_label(entries, (r"\b(likes|лайки|лайков|нравится)\b",), parser=_parse_metric_count),
        "comments_count": _metric_value_near_label(entries, (r"\b(comments|comment|комментарии|комментариев)\b",), parser=_parse_metric_count),
        "shares_count": _metric_value_near_label(entries, (r"\b(shares|share|репосты|отправки|поделились)\b",), parser=_parse_metric_count),
        "saves_count": _metric_value_near_label(entries, (r"\b(saves|saved|сохранения|сохранено)\b",), parser=_parse_metric_count),
        "accounts_reached_count": _metric_value_near_label(entries, (r"(accounts reached|охваченные аккаунты|охват аккаунтов)",), parser=_parse_metric_count),
        "watch_time_seconds": _metric_value_near_label(entries, (r"(watch time|время просмотра)",), parser=_parse_metric_duration_seconds),
        "avg_watch_time_seconds": _metric_value_near_label(entries, (r"(average watch time|avg watch time|среднее время просмотра)",), parser=_parse_metric_duration_seconds),
        "three_second_views_count": _metric_value_near_label(entries, (r"(3[\s-]?second views|3[\s-]?second plays|3[\s-]?секунд)",), parser=_parse_metric_count),
        "completion_rate_pct": _metric_value_near_label(entries, (r"(completion rate|досмотр|completion)",), parser=_parse_metric_percent),
    }


def _reel_metrics_surface_visible(device: Any) -> bool:
    markers = 0
    for entry in _screen_text_snapshot(device):
        value = str(entry.get("value") or "").strip()
        if not value:
            continue
        if _metric_label_matches(value, (r"(accounts reached|охваченные аккаунты|охват аккаунтов)",)):
            markers += 1
        if _metric_label_matches(value, (r"(watch time|время просмотра)",)):
            markers += 1
        if _metric_label_matches(value, (r"(average watch time|avg watch time|среднее время просмотра)",)):
            markers += 1
        if _metric_label_matches(value, (r"(completion rate|досмотр|completion)",)):
            markers += 1
        if markers >= 2:
            return True
    return False


def _open_reel_metrics_surface(device: Any, serial: str, timeout_seconds: float = 8.0) -> bool:
    if _ig_click_first(
        device,
        [
            {"textMatches": "(?i)^view insights$"},
            {"descriptionMatches": "(?i)^view insights$"},
            {"textMatches": "(?i)^insights$"},
            {"descriptionMatches": "(?i)^insights$"},
            {"textMatches": "(?i)(ваша статистика|статистика)"},
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*insight.*"},
        ],
        timeout_seconds=1.0,
        serial=serial,
    ):
        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            _dismiss_system_dialogs(device, serial, timeout_seconds=0.4)
            if _reel_metrics_surface_visible(device):
                return True
            time.sleep(0.4)
    return _reel_metrics_surface_visible(device)


def _serialize_profile_reel_candidate(candidate: Optional[ProfileReelCandidate]) -> dict[str, Any]:
    if candidate is None:
        return {}
    return {
        "slot_index": int(candidate.slot_index),
        "age_seconds": int(candidate.age_seconds) if candidate.age_seconds is not None else None,
        "age_label": str(candidate.age_label or ""),
        "fingerprint": str(candidate.fingerprint or ""),
        "signature_text": str(candidate.signature_text or ""),
        "opened": bool(candidate.opened),
        "success_markers": bool(candidate.success_markers),
    }


def _merge_reel_metric_maps(*parts: Optional[dict[str, Any]]) -> dict[str, Any]:
    merged: dict[str, Any] = {
        "plays_count": None,
        "likes_count": None,
        "comments_count": None,
        "shares_count": None,
        "saves_count": None,
        "accounts_reached_count": None,
        "watch_time_seconds": None,
        "avg_watch_time_seconds": None,
        "three_second_views_count": None,
        "completion_rate_pct": None,
    }
    for item in parts:
        if not isinstance(item, dict):
            continue
        for key, value in item.items():
            if key in merged and value not in (None, ""):
                merged[key] = value
    return merged


def _open_located_reel_for_metrics(device: Any, serial: str, center: tuple[int, int]) -> bool:
    for attempt in range(2):
        if attempt > 0 and not _open_profile_reels_tab(device, serial, timeout_seconds=8.0):
            return False
        _adb_tap(serial, int(center[0]), int(center[1]))
        if _wait_until(
            lambda: _normalize_open_reel_surface(device, serial, timeout_seconds=1.2),
            timeout_seconds=6.0,
            interval=0.6,
        ):
            return True
    return False


def _normalize_open_reel_surface(device: Any, serial: str, timeout_seconds: float = 8.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.6)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.6)
        if _comment_sheet_visible(device):
            _close_comment_sheet(device, serial)
            continue
        if _quick_capture_visible(device):
            _exit_quick_capture(device, serial)
            continue
        if _reel_viewer_visible(device):
            return True
        if _keyboard_visible(serial):
            _close_keyboard(device, serial)
            continue
        if _profile_surface_visible(device):
            return False
        time.sleep(0.4)
    return _reel_viewer_visible(device)


def _reel_signature_from_texts(texts: list[str], *, expected_handle: str = "") -> tuple[str, str]:
    handle = str(expected_handle or "").strip().lstrip("@").casefold()
    ignored_exact = {
        "",
        "reels",
        "view insights",
        "boost reel",
        "поделиться",
        "нравится",
        "комментарии",
        "replies",
        "follow",
        "message",
        "audio",
    }
    parts: list[str] = []
    seen: set[str] = set()
    for raw in texts:
        value = re.sub(r"\s+", " ", str(raw or "").strip())
        if not value:
            continue
        lowered = value.casefold()
        if lowered in ignored_exact:
            continue
        if _parse_relative_age_seconds(value) is not None:
            continue
        normalized = re.sub(r"[^0-9a-zA-Zа-яА-Я_@# ]+", " ", value).strip()
        normalized = re.sub(r"\s+", " ", normalized)
        lowered_normalized = normalized.casefold()
        if not lowered_normalized or len(lowered_normalized) < 4:
            continue
        if handle and lowered_normalized in {handle, f"@{handle}"}:
            continue
        if lowered_normalized in seen:
            continue
        seen.add(lowered_normalized)
        parts.append(normalized)
        if len(parts) >= 6:
            break
    signature = " | ".join(parts[:4])
    return signature.casefold(), signature


def _inspect_profile_reel_center(
    device: Any,
    serial: str,
    center: tuple[int, int],
    *,
    slot_index: int = 0,
    expected_handle: str = "",
) -> ProfileReelCandidate:
    _adb_tap(serial, center[0], center[1])
    logger.info("profile_slot_opened: serial=%s slot=%s", serial, slot_index)
    viewer_ready = _wait_until(lambda: _normalize_open_reel_surface(device, serial, timeout_seconds=1.2), timeout_seconds=6.0, interval=0.6)
    if not viewer_ready:
        _recover_to_profile_surface(device, serial, timeout_seconds=6.0)
        return ProfileReelCandidate(slot_index=slot_index, opened=False)
    texts = _viewer_text_candidates(device)
    age_seconds, age_label = _extract_relative_age_from_texts(texts)
    fingerprint, signature_text = _reel_signature_from_texts(texts, expected_handle=expected_handle)
    success_markers = any(
        re.search(r"(?i)(view insights|boost reel|ваша статистика|продвигать reels?)", value or "")
        for value in texts
    )
    if age_seconds is None:
        width, height = _device_display_size(device)
        _adb_tap(serial, width // 2, int(height * 0.42))
        time.sleep(0.8)
        if _normalize_open_reel_surface(device, serial, timeout_seconds=2.0):
            texts = _viewer_text_candidates(device)
            age_seconds, age_label = _extract_relative_age_from_texts(texts)
            if not fingerprint:
                fingerprint, signature_text = _reel_signature_from_texts(texts, expected_handle=expected_handle)
            success_markers = success_markers or any(
                re.search(r"(?i)(view insights|boost reel|ваша статистика|продвигать reels?)", value or "")
                for value in texts
            )
    opened = _reel_viewer_visible(device) or bool(texts)
    if age_seconds is not None:
        logger.info("profile_slot_timestamp_read: serial=%s slot=%s age_seconds=%s label=%s", serial, slot_index, age_seconds, age_label or "-")
    elif opened:
        logger.warning("profile_slot_timestamp_unreadable: serial=%s slot=%s", serial, slot_index)
    _recover_to_profile_surface(device, serial, timeout_seconds=6.0)
    _open_profile_reels_tab(device, serial, timeout_seconds=6.0)
    return ProfileReelCandidate(
        slot_index=slot_index,
        age_seconds=age_seconds,
        age_label=age_label,
        fingerprint=fingerprint,
        signature_text=signature_text,
        opened=opened,
        success_markers=success_markers,
    )


def _inspect_profile_reel_slot(
    device: Any,
    serial: str,
    slot_index: int,
    *,
    expected_handle: str = "",
) -> ProfileReelCandidate:
    centers = _profile_reels_grid_centers(device, limit=PUBLISH_PROFILE_CHECK_SLOTS)
    if slot_index < 0 or slot_index >= len(centers):
        return ProfileReelCandidate(slot_index=slot_index)
    return _inspect_profile_reel_center(
        device,
        serial,
        centers[slot_index],
        slot_index=slot_index,
        expected_handle=expected_handle,
    )


def _capture_profile_reels_baseline(device: Any, serial: str, *, expected_handle: str = "") -> dict[str, Any]:
    if not _open_profile_reels_tab(device, serial, timeout_seconds=12.0):
        return {"available": False, "candidates": []}
    candidates: list[ProfileReelCandidate] = []
    baseline_slots = max(1, min(PUBLISH_PROFILE_CHECK_SLOTS, PUBLISH_PROFILE_BASELINE_SLOTS))
    for slot_index in range(baseline_slots):
        candidates.append(_inspect_profile_reel_slot(device, serial, slot_index, expected_handle=expected_handle))
    return {
        "available": any(candidate.opened for candidate in candidates),
        "candidates": candidates,
    }


def _candidate_matches_baseline(candidate: ProfileReelCandidate, baseline_candidates: list[ProfileReelCandidate]) -> bool:
    if not candidate.fingerprint:
        return False
    for baseline_candidate in baseline_candidates:
        if baseline_candidate.fingerprint and baseline_candidate.fingerprint == candidate.fingerprint:
            return True
    return False


def _candidate_confirms_new_upload(
    candidate: ProfileReelCandidate,
    *,
    baseline_candidates: list[ProfileReelCandidate],
    baseline_available: bool,
    baseline_fresh: list[ProfileReelCandidate],
    freshness_seconds: int,
) -> bool:
    age_seconds = candidate.age_seconds
    if age_seconds is None or int(age_seconds) > int(freshness_seconds):
        return False
    if not _candidate_matches_baseline(candidate, baseline_candidates):
        return True
    if not baseline_available:
        return True
    if not baseline_fresh:
        return True
    return False


def _reel_candidate_matches_target(
    candidate: ProfileReelCandidate,
    *,
    target_fingerprint: str,
    target_signature_text: str,
    published_at: Optional[int],
) -> bool:
    fingerprint_value = str(target_fingerprint or "").strip().casefold()
    if fingerprint_value and candidate.fingerprint and candidate.fingerprint == fingerprint_value:
        return True
    signature_value = str(target_signature_text or "").strip().casefold()
    candidate_signature = str(candidate.signature_text or "").strip().casefold()
    if not signature_value or not candidate_signature:
        return False
    signature_match = (
        signature_value == candidate_signature
        or signature_value in candidate_signature
        or candidate_signature in signature_value
    )
    if not signature_match:
        return False
    if candidate.age_seconds is None:
        return False
    try:
        published_ts = int(published_at or 0)
    except Exception:
        published_ts = 0
    if published_ts <= 0:
        return False
    expected_age = max(0, int(time.time()) - published_ts)
    tolerance_seconds = max(2 * 60 * 60, min(24 * 60 * 60, int(max(expected_age * 0.35, 20 * 60))))
    return abs(int(candidate.age_seconds) - expected_age) <= tolerance_seconds


def _metrics_have_any_value(metrics: dict[str, Any]) -> bool:
    return any(value not in (None, "", 0, 0.0) for value in metrics.values())


def _locate_reel_for_metrics(
    device: Any,
    serial: str,
    *,
    reel_fingerprint: str,
    reel_signature_text: str,
    published_at: Optional[int],
    expected_handle: str = "",
    max_slots: int = 12,
    max_screens: int = 3,
) -> tuple[Optional[tuple[int, int]], Optional[ProfileReelCandidate]]:
    if not _open_profile_reels_tab(device, serial, timeout_seconds=12.0):
        return None, None
    inspected = 0
    per_screen_limit = max(1, min(4, int(max_slots)))
    for screen_index in range(max(1, int(max_screens))):
        centers = _profile_reels_grid_centers(device, limit=max(1, int(max_slots)))
        if not centers:
            break
        for local_index, center in enumerate(centers[:per_screen_limit]):
            candidate = _inspect_profile_reel_center(
                device,
                serial,
                center,
                slot_index=screen_index * per_screen_limit + local_index,
                expected_handle=expected_handle,
            )
            inspected += 1
            if _reel_candidate_matches_target(
                candidate,
                target_fingerprint=reel_fingerprint,
                target_signature_text=reel_signature_text,
                published_at=published_at,
            ):
                return center, candidate
            if inspected >= max(1, int(max_slots)):
                return None, None
        if screen_index + 1 >= max(1, int(max_screens)):
            break
        _scroll_profile_reels_grid(device, serial)
    return None, None


def _sanitize_metric_snapshot_entries(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sanitized: list[dict[str, Any]] = []
    for entry in entries[:80]:
        bounds = entry.get("bounds")
        sanitized.append(
            {
                "value": str(entry.get("value") or "").strip(),
                "field": str(entry.get("field") or "").strip(),
                "resource_id": str(entry.get("resource_id") or "").strip(),
                "class_name": str(entry.get("class_name") or "").strip(),
                "bounds": list(bounds) if isinstance(bounds, tuple) and len(bounds) == 4 else None,
            }
        )
    return sanitized


def _format_compact_number(value: Any) -> str:
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


def _merge_reel_metric_values(*metrics_list: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for metrics in metrics_list:
        if not isinstance(metrics, dict):
            continue
        for key, value in metrics.items():
            if value in (None, ""):
                continue
            merged[key] = value
    return merged


def _reel_metrics_detail(post: dict[str, Any], status: str, metrics: Optional[dict[str, Any]] = None) -> str:
    source_name = str(post.get("source_name") or "").strip() or f"post #{int(post.get('id') or 0)}"
    window_key = str(post.get("window_key") or post.get("collection_stage") or "t30m").strip()
    window_label = {
        "t30m": "30м",
        "t6h": "6ч",
        "t24h": "24ч",
        "t72h": "72ч",
    }.get(window_key, window_key or "окно")
    metrics = dict(metrics or {})
    if status == "ok":
        return (
            f"Собрал Insights для {source_name} ({window_label}). "
            f"Views: {_format_compact_number(metrics.get('plays_count'))}. "
            f"Likes: {_format_compact_number(metrics.get('likes_count'))}. "
            f"Comments: {_format_compact_number(metrics.get('comments_count'))}."
        )
    if status == "partial":
        return (
            f"Собрал базовые метрики для {source_name} ({window_label}), "
            "но расширенные Insights недоступны."
        )
    if status == "unavailable":
        return f"Reel {source_name} открыт, но метрики ({window_label}) сейчас недоступны."
    if status == "not_found":
        return f"Не нашёл Reel {source_name} в профиле аккаунта для окна {window_label}."
    return f"Не удалось собрать метрики Reel {source_name} ({window_label})."


def _reel_metrics_login_failure_outcome(login_state: str) -> tuple[str, bool]:
    state_value = (login_state or "").strip().lower()
    if state_value == "helper_error":
        return ("failed", True)
    return ("unavailable", False)


def _open_reel_viewer_at_center(device: Any, serial: str, center: tuple[int, int]) -> bool:
    return _open_located_reel_for_metrics(device, serial, center)


def _collect_reel_metrics_from_open_viewer(device: Any, serial: str) -> tuple[str, dict[str, Any], dict[str, Any]]:
    viewer_entries = _screen_text_snapshot(device)
    viewer_metrics = _extract_reel_metrics_from_entries(viewer_entries)
    insights_entries: list[dict[str, Any]] = []
    insights_metrics: dict[str, Any] = {}
    insights_opened = _open_reel_metrics_surface(device, serial, timeout_seconds=8.0)
    if insights_opened:
        time.sleep(1.0)
        insights_entries = _screen_text_snapshot(device)
        insights_metrics = _extract_reel_metrics_from_entries(insights_entries)

    merged_metrics = _merge_reel_metric_values(viewer_metrics, insights_metrics)
    advanced_keys = (
        "accounts_reached_count",
        "watch_time_seconds",
        "avg_watch_time_seconds",
        "three_second_views_count",
        "completion_rate_pct",
    )
    has_advanced_metrics = any(merged_metrics.get(key) not in (None, "", 0, 0.0) for key in advanced_keys)
    has_any_metrics = _metrics_have_any_value(merged_metrics)
    if has_advanced_metrics:
        status = "ok"
    elif has_any_metrics:
        status = "partial"
    else:
        status = "unavailable"
    raw_payload = {
        "viewer": _sanitize_metric_snapshot_entries(viewer_entries),
        "insights": _sanitize_metric_snapshot_entries(insights_entries),
        "insights_opened": bool(insights_opened),
    }
    return status, merged_metrics, raw_payload


def _format_duration_short(seconds: int) -> str:
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


def _profile_verification_detail(source_name: str, result: ProfileVerificationResult) -> str:
    if result.publish_phase == "waiting_profile_verification_window":
        parts = [f"Жду окно проверки профиля для {source_name}."]
        if result.seconds_until_profile_check is not None:
            parts.append(f"Первая проверка через {_format_duration_short(result.seconds_until_profile_check)}.")
    else:
        parts = [f"Проверяю Reel {source_name} в профиле."]
    if result.detail:
        parts.append(result.detail)
    if result.matched_age_seconds is not None:
        parts.append(f"Возраст Reel: {_format_duration_short(result.matched_age_seconds)}.")
    if result.verification_attempt > 0:
        parts.append(f"Попытка #{int(result.verification_attempt)}.")
    if result.first_profile_check_at:
        parts.append(
            "Первый вход в профиль: "
            f"{datetime.fromtimestamp(int(result.first_profile_check_at)).strftime('%H:%M:%S')}."
        )
    if result.diagnostics_path and "Диагностика:" not in " ".join(parts):
        parts.append(f"Диагностика: {result.diagnostics_path}.")
    return " ".join(part.strip() for part in parts if str(part).strip())


def _estimate_reel_published_at(
    verification_result: ProfileVerificationResult,
    *,
    fallback_published_at: Optional[int] = None,
) -> int:
    if verification_result.published_at is not None:
        try:
            explicit_value = int(verification_result.published_at)
        except Exception:
            explicit_value = 0
        if explicit_value > 0:
            return explicit_value
    matched_age_seconds = verification_result.matched_age_seconds
    if matched_age_seconds is not None:
        try:
            estimated = int(time.time()) - max(0, int(matched_age_seconds))
        except Exception:
            estimated = 0
        if estimated > 0:
            return estimated
    try:
        fallback_value = int(fallback_published_at or 0)
    except Exception:
        fallback_value = 0
    return fallback_value if fallback_value > 0 else int(time.time())


def _build_reel_publish_telemetry(
    verification_result: ProfileVerificationResult,
    *,
    helper_ticket: str = "",
    fallback_published_at: Optional[int] = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "reel_fingerprint": str(verification_result.matched_fingerprint or "").strip(),
        "reel_signature_text": str(verification_result.matched_signature_text or "").strip(),
        "matched_slot": verification_result.matched_slot,
        "matched_age_seconds": verification_result.matched_age_seconds,
        "published_at": _estimate_reel_published_at(
            verification_result,
            fallback_published_at=fallback_published_at,
        ),
    }
    ticket_value = str(helper_ticket or "").strip()
    if ticket_value:
        payload["helper_ticket"] = ticket_value
    return payload


def _confirm_publish_via_profile(
    device: Any,
    serial: str,
    *,
    source_name: str,
    baseline: Optional[dict[str, Any]] = None,
    expected_handle: str = "",
    elapsed_since_share_seconds: int = 0,
    share_clicked_at: Optional[int] = None,
    start_delay_seconds: int = int(PUBLISH_PROFILE_VERIFY_START_DELAY_SECONDS),
    timeout_seconds: float = float(PUBLISH_PROFILE_VERIFY_SECONDS),
    interval_seconds: float = float(PUBLISH_PROFILE_VERIFY_INTERVAL_SECONDS),
    freshness_seconds: int = int(PUBLISH_PROFILE_FRESHNESS_SECONDS),
    diagnostics_context: Optional[dict[str, Any]] = None,
    on_update: Optional[Callable[[ProfileVerificationResult], None]] = None,
) -> ProfileVerificationResult:
    baseline_candidates = list((baseline or {}).get("candidates") or [])
    baseline_available = bool((baseline or {}).get("available")) and bool(baseline_candidates)
    baseline_fresh = [
        candidate
        for candidate in baseline_candidates
        if candidate.age_seconds is not None and int(candidate.age_seconds) <= int(freshness_seconds)
    ]
    local_started_at = time.monotonic()
    share_epoch = int(share_clicked_at) if share_clicked_at is not None else None
    verification_starts_at = int(share_epoch + start_delay_seconds) if share_epoch is not None else None
    verification_deadline_at = int(share_epoch + timeout_seconds) if share_epoch is not None else None
    attempt = 0
    first_profile_check_at: Optional[int] = None
    diagnostics_payload: dict[str, str] = {}
    timestamp_unreadable_streak = 0

    def _emit(result: ProfileVerificationResult) -> None:
        if on_update is not None:
            on_update(result)

    def _elapsed_now() -> int:
        return max(0, int(round(float(elapsed_since_share_seconds) + max(0.0, time.monotonic() - local_started_at))))

    def _seconds_until_start() -> int:
        return max(0, int(start_delay_seconds) - _elapsed_now())

    def _seconds_until_deadline() -> int:
        return max(0, int(timeout_seconds) - _elapsed_now())

    def _surface_kwargs() -> dict[str, Any]:
        flags = _profile_surface_flags(device, serial)
        return {
            "profile_surface_state": str(flags.get("profile_surface_state") or ""),
            "keyboard_visible": bool(flags.get("keyboard_visible")),
            "comment_sheet_visible": bool(flags.get("comment_sheet_visible")),
            "clips_viewer_visible": bool(flags.get("clips_viewer_visible")),
            "quick_capture_visible": bool(flags.get("quick_capture_visible")),
            "diagnostics_path": _diagnostics_primary_path(diagnostics_payload),
        }

    def _build_result(
        *,
        verified: bool = False,
        needs_review: bool = False,
        reason_code: str = "",
        detail: str = "",
        publish_phase: str = "verifying_profile",
        matched_slot: Optional[int] = None,
        matched_age_seconds: Optional[int] = None,
        matched_fingerprint: str = "",
        matched_signature_text: str = "",
        verification_attempt: int = 0,
        event_kind: str = "",
        seconds_until_profile_check: Optional[int] = None,
        timestamp_readable: bool = False,
    ) -> ProfileVerificationResult:
        return ProfileVerificationResult(
            verified=verified,
            needs_review=needs_review,
            reason_code=reason_code,
            detail=detail,
            publish_phase=publish_phase,
            matched_slot=matched_slot,
            matched_age_seconds=matched_age_seconds,
            matched_fingerprint=str(matched_fingerprint or "").strip(),
            matched_signature_text=str(matched_signature_text or "").strip(),
            verification_attempt=verification_attempt,
            baseline_available=baseline_available,
            checked_slots=PUBLISH_PROFILE_CHECK_SLOTS,
            event_kind=event_kind,
            seconds_until_profile_check=seconds_until_profile_check,
            share_clicked_at=share_epoch,
            verification_starts_at=verification_starts_at,
            verification_deadline_at=verification_deadline_at,
            first_profile_check_at=first_profile_check_at,
            timestamp_readable=timestamp_readable,
            published_at=share_epoch,
            **_surface_kwargs(),
        )

    def _capture_soft_diagnostics(label: str) -> None:
        nonlocal diagnostics_payload
        if diagnostics_payload:
            return
        diagnostics_payload = _capture_publish_diagnostics(
            serial,
            label,
            device=device,
            batch_id=int(diagnostics_context.get("batch_id") or 0) if diagnostics_context else None,
            job_id=int(diagnostics_context.get("job_id") or 0) if diagnostics_context else None,
            account_id=int(diagnostics_context.get("account_id") or 0) if diagnostics_context else None,
        )

    last_result = _build_result(
        needs_review=True,
        reason_code="publish_profile_inconclusive",
        detail="Профиль ещё не подтвердил новый Reel.",
        publish_phase="waiting_profile_verification_window" if _seconds_until_start() > 0 else "verifying_profile",
        event_kind="profile_verification_scheduled" if _seconds_until_start() > 0 else "profile_verification_started",
        seconds_until_profile_check=_seconds_until_start() if _seconds_until_start() > 0 else 0,
    )

    logger.info(
        "profile_verification_schedule: serial=%s source=%s share_clicked_at=%s verification_starts_at=%s "
        "verification_deadline_at=%s elapsed_since_share_seconds=%s seconds_until_start=%s",
        serial,
        source_name,
        share_epoch,
        verification_starts_at,
        verification_deadline_at,
        elapsed_since_share_seconds,
        _seconds_until_start(),
    )

    while _seconds_until_start() > 0 and _seconds_until_deadline() > 0:
        remaining_to_start = _seconds_until_start()
        last_result = _build_result(
            needs_review=True,
            reason_code="publish_profile_verification_scheduled",
            detail="Upload принят, жду окно проверки профиля перед подтверждением публикации.",
            publish_phase="waiting_profile_verification_window",
            event_kind="profile_verification_scheduled",
            seconds_until_profile_check=remaining_to_start,
        )
        _emit(last_result)
        time.sleep(min(float(interval_seconds), max(0.1, float(remaining_to_start)), max(0.1, float(_seconds_until_deadline()))))

    if _seconds_until_deadline() <= 0:
        last_result = _build_result(
            needs_review=True,
            reason_code="publish_profile_inconclusive",
            detail="Окно проверки профиля истекло до подтверждения публикации.",
            publish_phase="verifying_profile",
            event_kind="needs_review",
        )
        logger.warning(
            "profile_verification_deadline_expired: serial=%s source=%s share_clicked_at=%s verification_starts_at=%s "
            "verification_deadline_at=%s elapsed_since_share_seconds=%s",
            serial,
            source_name,
            share_epoch,
            verification_starts_at,
            verification_deadline_at,
            _elapsed_now(),
        )
        return last_result

    _emit(
        _build_result(
            needs_review=True,
            reason_code="publish_profile_verification_started",
            detail="Начинаю проверку профиля и последних Reel.",
            publish_phase="verifying_profile",
            event_kind="profile_verification_started",
        )
    )

    while _seconds_until_deadline() > 0:
        attempt += 1
        if first_profile_check_at is None:
            first_profile_check_at = int(time.time())
            logger.info(
                "profile_verification_first_check: serial=%s source=%s share_clicked_at=%s verification_starts_at=%s "
                "verification_deadline_at=%s first_profile_check_at=%s elapsed_since_share_seconds=%s attempt=%s",
                serial,
                source_name,
                share_epoch,
                verification_starts_at,
                verification_deadline_at,
                first_profile_check_at,
                _elapsed_now(),
                attempt,
            )
        if _published_reel_viewer_visible(device):
            viewer_texts = _viewer_text_candidates(device)
            viewer_age_seconds, _ = _extract_relative_age_from_texts(viewer_texts)
            viewer_fingerprint, viewer_signature_text = _reel_signature_from_texts(
                viewer_texts,
                expected_handle=expected_handle,
            )
            result = _build_result(
                verified=True,
                reason_code="publish_viewer_verified",
                detail="Instagram оставил открытым опубликованный Reel с кнопками View insights / Boost post.",
                publish_phase="verifying_profile",
                matched_age_seconds=viewer_age_seconds,
                matched_fingerprint=viewer_fingerprint,
                matched_signature_text=viewer_signature_text,
                verification_attempt=attempt,
                event_kind="published_reel_viewer_verified",
                timestamp_readable=viewer_age_seconds is not None,
            )
            logger.info(
                "profile_verification_verified_from_viewer: serial=%s source=%s attempt=%s matched_age_seconds=%s "
                "share_clicked_at=%s first_profile_check_at=%s elapsed_since_share_seconds=%s",
                serial,
                source_name,
                attempt,
                viewer_age_seconds,
                share_epoch,
                first_profile_check_at,
                _elapsed_now(),
            )
            return result
        _emit(
            _build_result(
                needs_review=True,
                reason_code="publish_profile_verification_started",
                detail="Возвращаюсь в профиль.",
                publish_phase="verifying_profile",
                verification_attempt=attempt,
                event_kind="profile_verification_retry",
            )
        )
        if not _recover_to_profile_surface(device, serial, timeout_seconds=10.0):
            _capture_soft_diagnostics("profile_verification_navigation_failed")
            last_result = _build_result(
                needs_review=True,
                reason_code="publish_profile_navigation_failed",
                detail="Не удалось вернуться в профиль аккаунта для проверки публикации.",
                publish_phase="verifying_profile",
                verification_attempt=attempt,
                event_kind="profile_verification_retry",
            )
            _emit(last_result)
            time.sleep(min(float(interval_seconds), max(0.1, float(_seconds_until_deadline()))))
            continue
        _emit(
            _build_result(
                needs_review=True,
                reason_code="publish_profile_verification_started",
                detail="Открываю Reels в профиле.",
                publish_phase="verifying_profile",
                verification_attempt=attempt,
                event_kind="profile_verification_retry",
            )
        )
        if not _open_profile_reels_tab(device, serial, timeout_seconds=12.0):
            _capture_soft_diagnostics("profile_verification_navigation_failed")
            last_result = _build_result(
                needs_review=True,
                reason_code="publish_profile_navigation_failed",
                detail="Не удалось открыть вкладку Reels в профиле для проверки.",
                publish_phase="verifying_profile",
                verification_attempt=attempt,
                event_kind="profile_verification_retry",
            )
            _emit(last_result)
            time.sleep(min(float(interval_seconds), max(0.1, float(_seconds_until_deadline()))))
            continue
        _refresh_profile_reels_tab(device, serial)
        current_candidates: list[ProfileReelCandidate] = []
        matched_candidate: Optional[ProfileReelCandidate] = None
        for slot_index in range(PUBLISH_PROFILE_CHECK_SLOTS):
            _emit(
                _build_result(
                    needs_review=True,
                    reason_code="publish_profile_verification_started",
                    detail=f"Открываю Reel #{slot_index + 1} и считываю время публикации.",
                    publish_phase="verifying_profile",
                    verification_attempt=attempt,
                    event_kind="profile_verification_retry",
                )
            )
            candidate = _inspect_profile_reel_slot(device, serial, slot_index, expected_handle=expected_handle)
            current_candidates.append(candidate)
            if _candidate_confirms_new_upload(
                candidate,
                baseline_candidates=baseline_candidates,
                baseline_available=baseline_available,
                baseline_fresh=baseline_fresh,
                freshness_seconds=freshness_seconds,
            ):
                matched_candidate = candidate
                break

        fresh_candidates = [
            candidate
            for candidate in current_candidates
            if candidate.age_seconds is not None and int(candidate.age_seconds) <= int(freshness_seconds)
        ]

        if matched_candidate is not None:
            timestamp_unreadable_streak = 0
            logger.info(
                "profile_verification_verified: serial=%s source=%s attempt=%s matched_slot=%s matched_age_seconds=%s "
                "share_clicked_at=%s first_profile_check_at=%s elapsed_since_share_seconds=%s",
                serial,
                source_name,
                attempt,
                matched_candidate.slot_index,
                matched_candidate.age_seconds,
                share_epoch,
                first_profile_check_at,
                _elapsed_now(),
            )
            return _build_result(
                verified=True,
                reason_code="publish_profile_verified",
                detail=(
                    f"В профиле найден свежий Reel ({matched_candidate.age_label})."
                    if matched_candidate.age_label
                    else "В профиле найден свежий Reel."
                ),
                publish_phase="verifying_profile",
                matched_slot=int(matched_candidate.slot_index),
                matched_age_seconds=matched_candidate.age_seconds,
                matched_fingerprint=matched_candidate.fingerprint,
                matched_signature_text=matched_candidate.signature_text,
                verification_attempt=attempt,
                event_kind="profile_verified",
                timestamp_readable=True,
            )

        if fresh_candidates:
            last_reason = "publish_profile_inconclusive"
            last_detail = "В первых Reel есть свежий ролик, но он слишком похож на baseline и не подтверждает новый upload."
        else:
            last_reason = "publish_profile_not_fresh"
            last_detail = "Свежий Reel в первых 3 публикациях пока не появился."
        if all(candidate.age_seconds is None for candidate in current_candidates if candidate.opened):
            last_reason = "publish_profile_timestamp_unreadable"
            last_detail = "Reels открылись, но не удалось прочитать время публикации."
        if last_reason == "publish_profile_timestamp_unreadable":
            timestamp_unreadable_streak += 1
            if timestamp_unreadable_streak >= 2:
                _capture_soft_diagnostics("profile_verification_timestamp_unreadable")
        else:
            timestamp_unreadable_streak = 0
        if _diagnostics_primary_path(diagnostics_payload):
            last_detail = f"{last_detail} Диагностика: {_diagnostics_primary_path(diagnostics_payload)}".strip()
        last_result = _build_result(
            needs_review=True,
            reason_code=last_reason,
            detail=last_detail,
            publish_phase="verifying_profile",
            verification_attempt=attempt,
            event_kind="profile_verification_retry",
            timestamp_readable=last_reason != "publish_profile_timestamp_unreadable",
        )
        _emit(last_result)
        time.sleep(min(float(interval_seconds), max(0.1, float(_seconds_until_deadline()))))

    last_result.publish_phase = "verifying_profile"
    last_result.event_kind = "needs_review"
    last_result.first_profile_check_at = first_profile_check_at
    last_result.diagnostics_path = _diagnostics_primary_path(diagnostics_payload)
    if last_result.diagnostics_path and "Диагностика:" not in last_result.detail:
        last_result.detail = f"{last_result.detail} Диагностика: {last_result.diagnostics_path}".strip()
    logger.warning(
        "profile_verification_needs_review: serial=%s source=%s reason=%s attempts=%s share_clicked_at=%s "
        "verification_starts_at=%s verification_deadline_at=%s first_profile_check_at=%s elapsed_since_share_seconds=%s",
        serial,
        source_name,
        last_result.reason_code or "publish_profile_inconclusive",
        attempt,
        share_epoch,
        verification_starts_at,
        verification_deadline_at,
        first_profile_check_at,
        _elapsed_now(),
    )
    return last_result


def _open_profile_menu(device: Any, serial: str, timeout_seconds: float = 8.0) -> bool:
    deadline = time.time() + timeout_seconds
    menu_selectors = [
        {"descriptionMatches": "(?i)(options|menu|more options|settings and activity|settings and privacy)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*menu.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*options.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*more.*"},
    ]
    menu_surface_markers = [
        {"textMatches": "(?i)(settings and privacy|settings|accounts center|your activity|saved)"},
        {"descriptionMatches": "(?i)(settings and privacy|settings|accounts center)"},
        {"textMatches": "(?i)(log out|logout|выйти)"},
    ]
    width, height = _device_display_size(device)
    while time.time() < deadline:
        if _ig_find_first(device, menu_surface_markers, timeout_seconds=0.6) is not None:
            return True
        if _ig_click_first(device, menu_selectors, timeout_seconds=0.8, serial=serial):
            time.sleep(1.2)
            continue
        _adb_tap(serial, max(120, width - 74), max(120, int(height * 0.085)))
        time.sleep(1.2)
        if _ig_find_first(device, menu_surface_markers, timeout_seconds=0.8) is not None:
            return True
    return _ig_find_first(device, menu_surface_markers, timeout_seconds=0.6) is not None


def _logout_instagram_ui(device: Any, serial: str, timeout_seconds: float = 26.0) -> bool:
    if _login_form_visible(device) or _signed_out_surface_visible(device, serial):
        return True
    if not _open_profile_tab(device, serial, timeout_seconds=8.0):
        return False

    settings_selectors = [
        {"textMatches": "(?i)(settings and privacy|settings|settings and activity)"},
        {"descriptionMatches": "(?i)(settings and privacy|settings|settings and activity)"},
    ]
    logout_selectors = [
        {"textMatches": "(?i)(log out|logout|выйти)"},
        {"descriptionMatches": "(?i)(log out|logout|выйти)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*logout.*"},
    ]
    width, height = _device_display_size(device)
    deadline = time.time() + timeout_seconds
    settings_open_attempted = False
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.8)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.6)
        if _login_form_visible(device) or _signed_out_surface_visible(device, serial):
            return True

        logout = _find_first(device, _instagram_selectors(logout_selectors), timeout_seconds=0.8)
        if logout is not None and _tap_object(serial, logout):
            time.sleep(1.5)
            confirm = _find_first(device, _instagram_selectors(logout_selectors), timeout_seconds=1.0)
            if confirm is not None:
                _tap_object(serial, confirm)
                time.sleep(1.2)
            if _wait_until(
                lambda: _login_form_visible(device) or _signed_out_surface_visible(device, serial),
                timeout_seconds=10.0,
                interval=0.6,
            ):
                return True
            continue

        if not settings_open_attempted and _open_profile_menu(device, serial, timeout_seconds=4.0):
            settings_open_attempted = True
            settings_target = _find_first(device, _instagram_selectors(settings_selectors), timeout_seconds=1.2)
            if settings_target is not None:
                _tap_object(serial, settings_target)
                time.sleep(1.3)
                continue

        _adb_swipe(
            serial,
            width // 2,
            int(height * 0.84),
            width // 2,
            int(height * 0.26),
            420,
        )
        time.sleep(1.0)
    return _login_form_visible(device) or _signed_out_surface_visible(device, serial)


def _ensure_signed_out_instagram_session(device: Any, serial: str, allow_destructive_fallback: bool = True) -> str:
    if _login_form_visible(device) or _signed_out_surface_visible(device, serial):
        return "already_signed_out"
    if _logout_instagram_ui(device, serial, timeout_seconds=24.0):
        return "ui_logout"
    if _login_form_visible(device) or _signed_out_surface_visible(device, serial):
        return "signed_out_recovered"
    if _open_login_entrypoint(device, serial) and _wait_until(lambda: _login_form_visible(device), timeout_seconds=5.0, interval=0.5):
        return "login_entrypoint"
    if allow_destructive_fallback:
        _clear_instagram_data(serial)
        return "app_data_cleared"
    return "logout_failed"


def _wait_for_logged_in_surface(device: Any, serial: str, timeout_seconds: float = 20.0) -> bool:
    selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/tab_bar"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/feed_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/profile_header_actions_top_row"},
        {"textMatches": "(?i)(edit profile|share profile|professional dashboard)"},
    ]
    nav_selectors = [
        {"descriptionMatches": "(?i)^home$"},
        {"descriptionMatches": "(?i)(search and explore|search)"},
        {"descriptionMatches": "(?i)^reels$"},
        {"descriptionMatches": "(?i)^profile$"},
        {"descriptionMatches": "(?i)^messages?$"},
    ]
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        current_activity = _current_top_activity(serial)
        if "InstagramMainActivity" in current_activity:
            return True
        if _login_form_visible(device):
            return False
        if _signed_out_surface_visible(device, serial):
            return False
        if not _instagram_is_foreground(serial):
            time.sleep(0.4)
            continue
        if _ig_find_first(device, selectors, timeout_seconds=0.8) is not None:
            return True
        nav_hits = 0
        for selector in nav_selectors:
            if _ig_find_first(device, [selector], timeout_seconds=0.2) is not None:
                nav_hits += 1
        if nav_hits >= 2:
            return True
        if (
            ("MainTabActivity" in current_activity or "InstagramMainActivity" in current_activity)
            and not _login_form_visible(device)
            and not _signed_out_surface_visible(device, serial)
        ):
            return True
        time.sleep(0.4)
    return False


def _post_login_coordinate_fallback(serial: str) -> None:
    # Coordinates tuned for 1080x1920 emulator layout.
    for x, y in (
        (540, 1040),  # Don't allow / secondary action
        (540, 1180),  # Not now / save info
        (540, 1510),  # Got it / onboarding CTA
        (540, 1630),  # Got it / onboarding CTA (current layout)
        (540, 1730),  # Save your login info? -> Not now
        (540, 870),   # Allow / primary action
    ):
        _adb_tap(serial, x, y)
        time.sleep(0.6)


def _recover_logged_in_surface(device: Any, serial: str, timeout_seconds: float = 12.0) -> bool:
    if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.0):
        return True
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        activity = _current_top_activity(serial)
        if "ModalActivity" in activity or not activity:
            _post_login_coordinate_fallback(serial)
            if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.0):
                return True
            _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
            time.sleep(1.2)
            if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.0):
                return True
            _adb_shell(serial, "am", "start", "-n", "com.instagram.android/.activity.MainTabActivity", timeout=20, check=False)
            time.sleep(2.0)
            if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.0):
                return True
        else:
            if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.0):
                return True
        time.sleep(0.8)
    return False


def _dismiss_save_login_info_prompt(device: Any, serial: str) -> bool:
    prompt_markers = [
        {"textMatches": "(?i)(save your login info|save login info|save your login information)"},
        {"descriptionMatches": "(?i)(save your login info|save login info|save your login information)"},
    ]
    dismiss_selectors = [
        {"textMatches": "(?i)^not now$"},
        {"descriptionMatches": "(?i)^not now$"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/igds_headline_secondary_action_button"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*secondary.*action.*button.*"},
    ]
    if _ig_find_first(device, prompt_markers, timeout_seconds=0.5) is None:
        return False
    if _ig_click_first(device, dismiss_selectors, timeout_seconds=0.5, serial=serial):
        logger.info("manual_step_required: dismissed save-login-info prompt")
        return True
    for node in _dump_ui_nodes(device):
        labels = [
            _compact_ui_text(node.get("text")),
            _compact_ui_text(node.get("description")),
        ]
        if not any(re.fullmatch(r"(?i)not now", label) for label in labels if label):
            continue
        if not _tap_challenge_node(serial, node):
            continue
        logger.info("manual_step_required: dismissed save-login-info prompt via node")
        return True
    return False


def _tap_post_login_dismiss_node(device: Any, serial: str) -> bool:
    dismiss_patterns = [
        r"(?i)not now",
        r"(?i)не сейчас\.?",
        r"(?i)don[’']?t allow",
        r"(?i)не разрешать",
        r"(?i)запретить",
        r"(?i)got it",
        r"(?i)понятно",
        r"(?i)ок(?:ей)?",
        r"(?i)okay",
    ]
    for node in _dump_ui_nodes(device):
        labels = [
            _compact_ui_text(node.get("text")),
            _compact_ui_text(node.get("description")),
        ]
        if not any(
            re.fullmatch(pattern, label)
            for label in labels
            if label
            for pattern in dismiss_patterns
        ):
            continue
        if not _tap_challenge_node(serial, node):
            continue
        logger.info("manual_step_required: dismissed post-login prompt via node")
        return True
    return False


def _handle_post_login_prompts(device: Any, serial: str, timeout_seconds: float = 18.0) -> bool:
    dismiss_selectors = [
        {"textMatches": "(?i)(not now|не сейчас|не сейчас\\.)"},
        {"descriptionMatches": "(?i)(not now|не сейчас)"},
        {"textMatches": "(?i)(don[’']?t allow|don't allow|не разрешать|запретить)"},
        {"descriptionMatches": "(?i)(don[’']?t allow|don't allow|не разрешать|запретить)"},
        {"resourceId": "com.android.permissioncontroller:id/permission_deny_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_deny_and_dont_ask_again_button"},
        {"textMatches": "(?i)(got it|понятно|ок|okay)"},
        {"descriptionMatches": "(?i)(got it|понятно|ок|okay)"},
    ]
    prompt_markers = [
        {"textMatches": "(?i)(save your login info|save login info|save your login information)"},
        {"descriptionMatches": "(?i)(save your login info|save login info|save your login information)"},
        {"textMatches": "(?i)(turn on notifications|enable notifications)"},
        {"descriptionMatches": "(?i)(turn on notifications|enable notifications)"},
        {"textMatches": "(?i)(allow instagram to send you notifications)"},
        {"descriptionMatches": "(?i)(allow instagram to send you notifications)"},
        {"textMatches": "(?i)(save info|notification|reels and messages)"},
        {"descriptionMatches": "(?i)(save info|notification|reels and messages)"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/igds_promo_dialog_headline"},
    ]
    deadline = time.time() + timeout_seconds
    clicked_any = False
    while time.time() < deadline:
        if _dismiss_save_login_info_prompt(device, serial):
            clicked_any = True
            time.sleep(1.2)
            continue

        prompt_visible = _ig_find_first(device, prompt_markers, timeout_seconds=0.5) is not None
        dismiss = _find_first(device, dismiss_selectors, timeout_seconds=0.5)
        if dismiss is not None:
            try:
                dismiss.click()
                clicked_any = True
                logger.info("manual_step_required: auto-dismissed post-login prompt")
                time.sleep(1.2)
                continue
            except Exception:
                pass
        if prompt_visible and _tap_post_login_dismiss_node(device, serial):
            clicked_any = True
            time.sleep(1.2)
            continue

        if _wait_for_logged_in_surface(device, serial, timeout_seconds=0.8) and not prompt_visible:
            return True

        if prompt_visible:
            time.sleep(1.0)
            continue

        _post_login_coordinate_fallback(serial)
        if _wait_for_logged_in_surface(device, serial, timeout_seconds=1.0):
            return True

        time.sleep(0.5)

    if clicked_any:
        if _wait_for_logged_in_surface(device, serial, timeout_seconds=5.0):
            return True
    return _recover_logged_in_surface(device, serial, timeout_seconds=8.0)


def _normalize_twofa_secret(raw_value: str) -> str:
    return _normalize_twofa_secret_value(raw_value)


def _twofa_prompt_visible(device: Any) -> bool:
    return _find_first(
        device,
        [
            {"textMatches": "(?i)(two-factor|two factor|security code|confirmation code|enter code|login code|authentication app)"},
            {"textMatches": "(?i)(код безопасности|код подтверждения|двухфактор)"},
        ],
        timeout_seconds=1.0,
    ) is not None


def _human_check_visible(device: Any) -> bool:
    return _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(confirm you'?re a human|confirm you are a human|i.?m not a robot|verify you'?re human|verify you are human|security check|captcha)"},
            {"textMatches": "(?i)(подтверд.*что вы человек|подтверд.*что ты человек|проверка безопасности|капча|подтверд.*человек)"},
        ],
        timeout_seconds=1.5,
    ) is not None


def _unfamiliar_device_challenge_visible(device: Any) -> bool:
    return _ig_find_first(
        device,
        [
            {"textMatches": "(?i)try another device to continue"},
            {"textMatches": "(?i)can'?t try another device\\?"},
            {"textMatches": "(?i)this must be a device you'?ve used to log into this account before"},
            {"textMatches": "(?i)we can'?t match the device you'?re using to the account you'?re trying to recover"},
        ],
        timeout_seconds=1.5,
    ) is not None


def _generate_current_twofa_code(secret: str) -> str:
    profile = _extract_twofa_profile(secret)
    if profile is None:
        raise RuntimeError("invalid TOTP profile")

    if pyotp is not None:
        digest = getattr(hashlib, str(profile.get("algorithm") or "SHA1").lower(), None)
        if digest is not None:
            totp = pyotp.TOTP(
                str(profile["secret"]),
                digits=int(profile["digits"]),
                interval=int(profile["period"]),
                digest=digest,
            )
            code = str(totp.now() or "").strip()
            digits = max(4, int(getattr(totp, "digits", profile["digits"]) or profile["digits"]))
            if code.isdigit():
                code = code.zfill(digits)
            if code and code.isdigit() and len(code) == digits:
                return code

    code = str(_current_totp_code(secret) or "").strip()
    digits = int(profile["digits"])
    if code.isdigit():
        code = code.zfill(digits)
    if not code or not code.isdigit() or len(code) != digits:
        raise RuntimeError(f"invalid TOTP code generated: {code!r}")
    return code


def _wait_for_fresh_twofa_window(twofa_secret: str, serial: str, *, min_validity_seconds: float = 8.0) -> None:
    try:
        remaining = float(_seconds_until_totp_rollover(twofa_secret))
    except Exception:
        return
    threshold = max(2.0, float(min_validity_seconds or 0))
    if remaining > threshold:
        return
    wait_seconds = min(remaining + 0.8, 31.0)
    logger.info(
        "twofa_wait_for_next_window: serial=%s remaining=%.2fs wait=%.2fs",
        serial,
        remaining,
        wait_seconds,
    )
    time.sleep(wait_seconds)


def _set_instagram_code_field_value(device: Any, serial: str, field: Any, value: str) -> bool:
    try:
        field.click()
    except Exception:
        pass
    time.sleep(0.2)

    try:
        field.set_text("")
        time.sleep(0.2)
    except Exception:
        try:
            device.send_keys("", clear=True)
            time.sleep(0.2)
        except Exception:
            pass

    if value.isdigit():
        for digit in value:
            _adb_shell(serial, "input", "keyevent", str(7 + int(digit)), timeout=10, check=False)
            time.sleep(0.05)
        return True

    try:
        device.send_keys(value, clear=True)
        return True
    except Exception:
        pass

    try:
        field.set_text(value)
        return True
    except Exception:
        pass

    if re.fullmatch(r"[A-Za-z0-9_.@\\-]+", value or ""):
        _adb_input_text(serial, value)
        return True
    return False


def _maybe_submit_twofa(device: Any, serial: str, twofa_secret: str) -> bool:
    secret = _normalize_twofa_secret(twofa_secret)
    if not secret or not _twofa_secret_is_valid(twofa_secret):
        return False

    if not _twofa_prompt_visible(device):
        return False

    field = _find_first(
        device,
        [
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*code.*"},
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*security.*"},
            {"className": "android.widget.EditText", "instance": 0},
        ],
        timeout_seconds=5.0,
    )
    if field is None:
        return False

    confirm_selectors = [
        {"textMatches": "(?i)(confirm|continue|next|done|submit|войти|подтвердить|продолжить|далее|готово)"},
        {"descriptionMatches": "(?i)(confirm|continue|next|done|submit|войти|подтвердить|продолжить|далее|готово)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*confirm.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*continue.*"},
    ]

    submitted = False
    for attempt in range(1, 3):
        if attempt == 1:
            _wait_for_fresh_twofa_window(secret, serial)
        else:
            try:
                wait_seconds = min(float(_seconds_until_totp_rollover(secret)) + 0.8, 31.0)
            except Exception:
                wait_seconds = 1.0
            logger.info("twofa_retry_wait: serial=%s attempt=%s wait=%.2fs", serial, attempt, wait_seconds)
            time.sleep(wait_seconds)

        try:
            code = _generate_current_twofa_code(secret)
        except Exception as exc:
            logger.warning("twofa_code_generation_failed: serial=%s attempt=%s error=%s", serial, attempt, exc)
            return submitted

        try:
            field.click()
        except Exception:
            pass
        if not _set_instagram_code_field_value(device, serial, field, code):
            logger.warning("twofa_field_input_failed: serial=%s attempt=%s", serial, attempt)
            return submitted
        try:
            device.press("back")
        except Exception:
            pass

        if not _click_first(device, confirm_selectors, timeout_seconds=3.0):
            _adb_shell(serial, "input", "keyevent", "66", timeout=10, check=False)
        submitted = True
        logger.info("twofa_submitted: serial=%s attempt=%s digits=%s", serial, attempt, len(code))
        time.sleep(3.0)
        if not _twofa_prompt_visible(device):
            return True
        logger.warning("twofa_prompt_still_visible: serial=%s attempt=%s", serial, attempt)
    return True


def _join_detail_text(*parts: str) -> str:
    cleaned = [str(part or "").strip() for part in parts if str(part or "").strip()]
    return " ".join(cleaned).strip()


def _find_instagram_code_field(device: Any, *, timeout_seconds: float = 2.5) -> Any:
    return _find_first(
        device,
        [
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*code.*"},
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*security.*"},
            {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*confirmation.*"},
            {"className": "android.widget.EditText", "instance": 0},
        ],
        timeout_seconds=timeout_seconds,
    )


def _submit_instagram_email_code(device: Any, serial: str, code: str) -> bool:
    value = str(code or "").strip()
    if not value:
        return False
    field = _find_instagram_code_field(device, timeout_seconds=4.0)
    if field is None:
        return False
    try:
        field.click()
    except Exception:
        pass
    if not _set_instagram_code_field_value(device, serial, field, value):
        return False
    try:
        device.press("back")
    except Exception:
        pass

    confirm_selectors = [
        {"textMatches": "(?i)(confirm|continue|next|done|submit|verify|войти|подтвердить|продолжить|далее|готово)"},
        {"descriptionMatches": "(?i)(confirm|continue|next|done|submit|verify|войти|подтвердить|продолжить|далее|готово)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*confirm.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*continue.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*next.*"},
    ]
    if not _click_first(device, confirm_selectors, timeout_seconds=3.0):
        _adb_shell(serial, "input", "keyevent", "66", timeout=10, check=False)
    logger.info("mail_code_submitted: serial=%s", serial)
    time.sleep(3.0)
    return True


def _mail_code_challenge_visible(device: Any) -> bool:
    return _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(check your email|get a new code|enter the code we sent|code we sent to)"},
            {"textMatches": "(?i)(код из письма|проверьте почту|новый код|отправили код)"},
        ],
        timeout_seconds=1.5,
    ) is not None


def _sorted_challenge_nodes(nodes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def _key(node: dict[str, Any]) -> tuple[int, int]:
        bounds = node.get("bounds") or (0, 0, 0, 0)
        return int(bounds[1]), int(bounds[0])

    return sorted(nodes, key=_key)


def _challenge_delivery_option_nodes(device: Any) -> dict[str, Any]:
    email_nodes: list[dict[str, Any]] = []
    phone_nodes: list[dict[str, Any]] = []
    continue_nodes: list[dict[str, Any]] = []
    manual_recovery_nodes: list[dict[str, Any]] = []

    for node in _dump_ui_nodes(device):
        label = _compact_ui_text(f"{node.get('text') or ''} {node.get('description') or ''}")
        if not label:
            continue
        lowered = label.casefold()
        if (
            "don't have access" in lowered
            or "dont have access" in lowered
            or "нет доступа" in lowered
            or "lost access" in lowered
        ):
            manual_recovery_nodes.append(node)
            continue
        if any(token in lowered for token in ("continue", "продолжить", "далее", "отправить", "send")):
            continue_nodes.append(node)
        if re.search(r"(^|\b)(email|e-mail)(\b|$)", lowered) or re.search(r"[a-z0-9*._%+\-]+@[a-z0-9*.\-]+", lowered):
            email_nodes.append(node)
            continue
        if (
            any(token in lowered for token in ("phone number", "mobile number", "text message", "sms", "номер телефона", "телефон"))
            or re.search(r"\+\d[\d*()\s-]{4,}", label)
        ):
            phone_nodes.append(node)

    return {
        "email_nodes": _sorted_challenge_nodes(email_nodes),
        "phone_nodes": _sorted_challenge_nodes(phone_nodes),
        "continue_nodes": _sorted_challenge_nodes(continue_nodes),
        "manual_recovery_nodes": _sorted_challenge_nodes(manual_recovery_nodes),
    }


def _tap_challenge_node(serial: str, node: dict[str, Any]) -> bool:
    bounds = node.get("bounds")
    if not bounds:
        return False
    left, top, right, bottom = bounds
    if right <= left or bottom <= top:
        return False
    _adb_tap(serial, (left + right) // 2, (top + bottom) // 2)
    return True


def _tap_instagram_challenge_email_option(device: Any, serial: str) -> bool:
    selectors = [
        {"textMatches": "(?i)^email$"},
        {"textMatches": "(?i)^e-mail$"},
        {"textMatches": "(?i)(send to email|by email|код на почту|по email|по e-mail)"},
        {"textMatches": "(?i).+@.+"},
    ]
    if _ig_click_first(device, selectors, timeout_seconds=1.0, serial=serial):
        logger.info("challenge_email_option_tapped: serial=%s action=selector", serial)
        return True

    option_nodes = _challenge_delivery_option_nodes(device)
    for node in option_nodes["email_nodes"]:
        if _tap_challenge_node(serial, node):
            logger.info("challenge_email_option_tapped: serial=%s action=node", serial)
            return True
    return False


def _press_instagram_challenge_continue(device: Any, serial: str) -> bool:
    selectors = [
        {"textMatches": "(?i)(continue|next|send|confirm|done|submit)"},
        {"textMatches": "(?i)(продолжить|далее|отправить|подтвердить|готово)"},
        {"descriptionMatches": "(?i)(continue|next|send|confirm|done|submit)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*continue.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*confirm.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*next.*"},
    ]
    if _ig_click_first(device, selectors, timeout_seconds=1.2, serial=serial):
        logger.info("challenge_continue_clicked: serial=%s", serial)
        return True

    option_nodes = _challenge_delivery_option_nodes(device)
    for node in option_nodes["continue_nodes"]:
        if _tap_challenge_node(serial, node):
            logger.info("challenge_continue_clicked: serial=%s action=node", serial)
            return True
    return False


def _select_instagram_email_challenge_option(device: Any, serial: str) -> tuple[bool, str]:
    if _tap_instagram_challenge_email_option(device, serial):
        time.sleep(0.8)
        _press_instagram_challenge_continue(device, serial)
        time.sleep(1.5)
        return True, "На challenge-экране выбран email-канал."

    alternate_selectors = [
        {"textMatches": "(?i)(try another way|choose another way|use a different method|another way)"},
        {"textMatches": "(?i)(i didn't get the code|didn't get the code|get a new code)"},
        {"textMatches": "(?i)(попробовать другой способ|другой способ|не приш[её]л код|получить новый код)"},
    ]
    if not _ig_click_first(device, alternate_selectors, timeout_seconds=1.2, serial=serial):
        return False, ""

    logger.info("challenge_email_option_tapped: serial=%s action=alternate", serial)
    time.sleep(1.2)
    if not _tap_instagram_challenge_email_option(device, serial):
        return False, ""
    time.sleep(0.8)
    _press_instagram_challenge_continue(device, serial)
    time.sleep(1.5)
    return True, "Через Try another way выбран email-канал."


def _open_instagram_approval_link(serial: str, link_url: str) -> str:
    adb_path = _resolve_adb_path()
    if not adb_path:
        raise RuntimeError("adb not found")
    normalized_link = unescape(str(link_url or "").strip())
    if not normalized_link:
        raise RuntimeError("Approval link is empty")

    commands = [
        [
            adb_path,
            "-s",
            serial,
            "shell",
            "am",
            "start",
            "-W",
            "-a",
            "android.intent.action.VIEW",
            "-d",
            normalized_link,
            INSTAGRAM_PACKAGE,
        ],
        [
            adb_path,
            "-s",
            serial,
            "shell",
            "am",
            "start",
            "-W",
            "-a",
            "android.intent.action.VIEW",
            "-d",
            normalized_link,
        ],
    ]
    last_output = ""
    for command in commands:
        result = _run(command, timeout=30, check=False)
        output = _compact_ui_text(f"{result.stdout or ''} {result.stderr or ''}").lower()
        last_output = output
        if result.returncode == 0 and "error" not in output and "exception" not in output:
            logger.info("approval_link_opened: serial=%s url=%s", serial, normalized_link)
            return normalized_link
    raise RuntimeError(f"Не удалось открыть approval link в эмуляторе: {last_output or normalized_link}")


def _request_instagram_new_email_code(device: Any, serial: str) -> bool:
    primary_selectors = [
        {"textMatches": "(?i)(get a new code|resend code|send a new code|send code again|request a new code)"},
        {"textMatches": "(?i)(i didn't get the code|didn't get the code|try another way)"},
        {"textMatches": "(?i)(получить новый код|отправить код ещё раз|отправить код еще раз|не приш[её]л код|попробовать другой способ)"},
    ]
    if not _ig_click_first(device, primary_selectors, timeout_seconds=1.5, serial=serial):
        return False
    logger.info("mail_code_resend_requested: serial=%s action=primary", serial)
    time.sleep(1.5)

    email_channel_selectors = [
        {"textMatches": "(?i)(email|email code|send email|send to email|by email)"},
        {"textMatches": "(?i)(почт|код на почту|по email|по e-mail)"},
    ]
    if _ig_click_first(device, email_channel_selectors, timeout_seconds=1.2, serial=serial):
        logger.info("mail_code_resend_requested: serial=%s action=email_channel", serial)
        time.sleep(1.0)

    confirm_selectors = [
        {"textMatches": "(?i)(continue|confirm|send|done|next|ok)"},
        {"textMatches": "(?i)(продолжить|подтвердить|отправить|готово|далее|ок)"},
    ]
    if _ig_click_first(device, confirm_selectors, timeout_seconds=0.8, serial=serial):
        logger.info("mail_code_resend_requested: serial=%s action=confirm", serial)
    time.sleep(float(MAIL_CHALLENGE_RESEND_WAIT_SECONDS))
    return True


def _classify_login_challenge_screen_once(device: Any, serial: str) -> tuple[str, str]:
    auth_app_prompt = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(go to your authentication app|authentication app|two-factor authentication app|google authenticator|duo mobile|authenticator app)"},
            {"textMatches": "(?i)(приложени[ея].*аутентифик|код из приложения|google authenticator|duo mobile)"},
        ],
        timeout_seconds=1.5,
    )
    if auth_app_prompt is not None:
        return ("manual_2fa", "Instagram просит код из приложения аутентификации.")

    if _human_check_visible(device):
        return ("human_check", "Instagram показал Confirm you're a human / CAPTCHA. Автоматический обход отключён, нужна ручная проверка аккаунта.")

    field = _find_instagram_code_field(device, timeout_seconds=1.5)
    if field is not None:
        return ("numeric_code", "На экране challenge есть поле ввода кода.")

    code_prompt = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(security code|confirmation code|enter code|login code|check your email|6-digit code)"},
            {"textMatches": "(?i)(код безопасности|код подтверждения|код из письма|проверьте почту)"},
        ],
        timeout_seconds=1.5,
    )
    if code_prompt is not None:
        return ("numeric_code", "Instagram просит код из письма, но поле ввода найдено не сразу.")

    delivery_options = _challenge_delivery_option_nodes(device)
    has_email = bool(delivery_options["email_nodes"])
    has_phone = bool(delivery_options["phone_nodes"])
    has_continue = bool(delivery_options["continue_nodes"])
    has_manual_recovery = bool(delivery_options["manual_recovery_nodes"])
    if has_email and (has_phone or has_continue):
        return ("channel_choice", "Instagram предлагает выбрать канал подтверждения. Переключаюсь на email.")
    if has_phone and not has_email and (has_continue or has_manual_recovery):
        return ("phone_only", "Instagram предлагает только phone/manual recovery без email-варианта.")

    approval_prompt = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(approve login|confirm it's you|help us confirm|suspicious login|login attempt|secure your account)"},
            {"textMatches": "(?i)(подтверд.*это вы|подозрительн.*вход|подтверд.*вход|безопасн.*вход)"},
        ],
        timeout_seconds=1.5,
    )
    if approval_prompt is not None:
        return ("approval", "Instagram просит подтверждение входа без поля ввода кода.")

    if not _instagram_is_foreground(serial):
        return ("unknown", "Instagram ушёл из foreground во время challenge.")
    return ("unknown", "Экран challenge не удалось надёжно классифицировать.")


def _classify_login_challenge_screen(device: Any, serial: str) -> tuple[str, str]:
    result = _classify_login_challenge_screen_once(device, serial)
    if result[0] != "unknown":
        return result
    time.sleep(1.0)
    return _classify_login_challenge_screen_once(device, serial)


def _mail_challenge_snapshot(
    base: Optional[dict[str, Any]] = None,
    *,
    status: str = "",
    kind: str = "",
    reason_code: str = "",
    reason_text: str = "",
) -> dict[str, Any]:
    result = dict(base or {})
    if status:
        result["status"] = status
    if kind:
        result["kind"] = kind
    if reason_code:
        result["reason_code"] = reason_code
    if reason_text:
        result["reason_text"] = reason_text
    return {
        "status": str(result.get("status") or "idle").strip(),
        "kind": str(result.get("kind") or "unsupported").strip(),
        "reason_code": str(result.get("reason_code") or "").strip(),
        "reason_text": str(result.get("reason_text") or "").strip(),
        "message_uid": str(result.get("message_uid") or "").strip(),
        "received_at": int(result["received_at"]) if result.get("received_at") not in (None, "") else None,
        "masked_code": str(result.get("masked_code") or "").strip(),
        "confidence": float(result.get("confidence") or 0.0),
    }


def _override_terminal_challenge_state(
    device: Any,
    serial: str,
    payload: dict[str, Any],
) -> Optional[tuple[str, str, dict[str, Any]]]:
    screen_kind, screen_detail = _classify_login_challenge_screen(device, serial)
    twofa_secret = str(payload.get("twofa") or "").strip()

    if screen_kind == "manual_2fa":
        if twofa_secret and _maybe_submit_twofa(device, serial, twofa_secret):
            next_state, next_detail = _detect_post_login_state(device, serial, twofa_secret)
            if next_state != "challenge_required":
                snapshot = _mail_challenge_snapshot(
                    status="resolved" if next_state == "login_submitted" else "unsupported",
                    kind="numeric_code",
                    reason_code="twofa_app_code_applied" if next_state == "login_submitted" else "manual_2fa_required",
                    reason_text="Код из приложения аутентификации введён автоматически."
                    if next_state == "login_submitted"
                    else next_detail,
                )
                return (next_state, next_detail, snapshot)
        detail = "Instagram просит код из приложения аутентификации."
        if not twofa_secret:
            detail = "Instagram запросил 2FA-код из приложения аутентификации, но в account.twofa нет секрета."
        snapshot = _mail_challenge_snapshot(
            status="unsupported",
            kind="unsupported",
            reason_code="manual_2fa_required",
            reason_text=detail,
        )
        return ("manual_2fa_required", _join_detail_text(screen_detail, detail), snapshot)

    if screen_kind == "human_check":
        snapshot = _mail_challenge_snapshot(
            status="unsupported",
            kind="unsupported",
            reason_code="human_check_required",
            reason_text=screen_detail,
        )
        return ("challenge_required", screen_detail, snapshot)

    return None


def _apply_instagram_approval_link(serial: str, link_url: str, twofa_secret: str) -> tuple[str, str, str]:
    opened_link = _open_instagram_approval_link(serial, link_url)
    time.sleep(4.0)
    device = _connect_ui(serial)
    _dismiss_system_dialogs(device, serial, timeout_seconds=2.0)
    if _wait_for_logged_in_surface(device, serial, timeout_seconds=4.0):
        return ("login_submitted", "Ссылка подтверждения открыта автоматически, Instagram подтвердил вход.", opened_link)
    if _handle_post_login_prompts(device, serial, timeout_seconds=10.0):
        return ("login_submitted", "Ссылка подтверждения открыта автоматически, Instagram подтвердил вход.", opened_link)
    next_state, next_detail = _detect_post_login_state(device, serial, twofa_secret)
    return (next_state, next_detail, opened_link)


def _attempt_mail_challenge_login(
    device: Any,
    serial: str,
    payload: dict[str, Any],
    *,
    challenge_started_at: int,
    initial_detail: str,
    on_update: Optional[Callable[[str, str, dict[str, Any]], None]] = None,
) -> tuple[str, str, dict[str, Any]]:
    account_id = int(payload.get("account_id") or 0)
    ticket = str(payload.get("ticket") or "").strip()

    def _emit_update(event_kind: str, detail: str, snapshot: Optional[dict[str, Any]] = None) -> None:
        if on_update is None:
            return
        try:
            on_update(event_kind, (detail or "").strip(), _mail_challenge_snapshot(snapshot or {}))
        except Exception as exc:
            logger.warning("mail_challenge_update_failed: serial=%s event_kind=%s error=%s", serial, event_kind, exc)

    if account_id <= 0:
        snapshot = _mail_challenge_snapshot(
            status="unsupported",
            kind="unsupported",
            reason_code="challenge_screen_unsupported",
            reason_text="Не удалось определить account_id для mail challenge.",
        )
        _emit_update("challenge_screen_unsupported", snapshot["reason_text"], snapshot)
        return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

    screen_kind, screen_detail = _classify_login_challenge_screen(device, serial)
    delivery_options = _challenge_delivery_option_nodes(device)
    if screen_kind == "phone_only":
        reason_code = "challenge_manual_recovery_only" if delivery_options["manual_recovery_nodes"] else "challenge_phone_only"
        snapshot = _mail_challenge_snapshot(
            status="unsupported",
            kind="unsupported",
            reason_code=reason_code,
            reason_text="Instagram предлагает только phone/manual recovery без email-варианта.",
        )
        _emit_update(reason_code, snapshot["reason_text"], snapshot)
        return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

    if screen_kind in {"manual_2fa", "human_check"}:
        override_result = _override_terminal_challenge_state(device, serial, payload)
        if override_result is not None:
            state, detail, snapshot = override_result
            _emit_update(snapshot["reason_code"] or "challenge_screen_unsupported", snapshot["reason_text"], snapshot)
            return (state, _join_detail_text(initial_detail, detail), snapshot)

    if screen_kind in {"channel_choice", "approval"}:
        selected_email, selection_detail = _select_instagram_email_challenge_option(device, serial)
        if selected_email:
            selection_snapshot = _mail_challenge_snapshot(
                status="idle",
                kind="numeric_code",
                reason_code="challenge_email_option_selected",
                reason_text=selection_detail or "На challenge-экране выбран email-канал.",
            )
            _emit_update("challenge_email_option_selected", selection_snapshot["reason_text"], selection_snapshot)
            if _wait_for_logged_in_surface(device, serial, timeout_seconds=2.5):
                return (
                    "login_submitted",
                    _join_detail_text(selection_snapshot["reason_text"], "Instagram подтвердил вход после выбора email-канала."),
                    selection_snapshot,
                )
            screen_kind, screen_detail = _classify_login_challenge_screen(device, serial)
            delivery_options = _challenge_delivery_option_nodes(device)
            if screen_kind == "phone_only":
                reason_code = "challenge_manual_recovery_only" if delivery_options["manual_recovery_nodes"] else "challenge_phone_only"
                snapshot = _mail_challenge_snapshot(
                    status="unsupported",
                    kind="unsupported",
                    reason_code=reason_code,
                    reason_text="После выбора способа подтверждения Instagram оставил только phone/manual recovery.",
                )
                _emit_update(reason_code, snapshot["reason_text"], snapshot)
                return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)
        elif screen_kind == "channel_choice":
            snapshot = _mail_challenge_snapshot(
                status="unsupported",
                kind="unsupported",
                reason_code="challenge_screen_unsupported",
                reason_text="Instagram показал выбор канала подтверждения, но helper не смог переключиться на email.",
            )
            _emit_update("challenge_screen_unsupported", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

    if not bool(payload.get("mail_enabled")):
        snapshot = _mail_challenge_snapshot(
            status="mailbox_unavailable",
            kind="unsupported",
            reason_code="mailbox_missing_credentials",
            reason_text="Для аккаунта не настроены почта и пароль почты.",
        )
        _emit_update("mailbox_unavailable", snapshot["reason_text"], snapshot)
        return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

    if screen_kind not in {"numeric_code", "approval"}:
        snapshot = _mail_challenge_snapshot(
            status="unsupported",
            kind="unsupported",
            reason_code="challenge_screen_unsupported",
            reason_text=screen_detail,
        )
        _emit_update("challenge_screen_unsupported", snapshot["reason_text"], snapshot)
        return ("challenge_required", _join_detail_text(initial_detail, screen_detail), snapshot)

    next_lookup_from = int(challenge_started_at or time.time())
    mail_snapshot: dict[str, Any] = {}
    resend_attempted = False
    twofa_secret = str(payload.get("twofa") or "")
    _emit_update(
        "mail_challenge_checking",
        "Instagram запросил код из письма. Проверяю почту аккаунта.",
        {
            "status": "idle",
            "kind": "numeric_code",
            "reason_code": "mail_challenge_checking",
            "reason_text": "Проверяю свежие письма Instagram/Meta для этого аккаунта.",
        },
    )
    for attempt in range(2):
        resolve_timeout = MAIL_CHALLENGE_TIMEOUT_SECONDS if attempt == 0 else MAIL_CHALLENGE_RETRY_SECONDS
        try:
            resolved = _resolve_account_mail_challenge(
                account_id,
                ticket=ticket,
                challenge_started_at=next_lookup_from,
                screen_kind=screen_kind,
                timeout_seconds=resolve_timeout,
            )
        except Exception as exc:
            snapshot = _mail_challenge_snapshot(
                status="mailbox_unavailable",
                kind="unsupported",
                reason_code="mailbox_unavailable",
                reason_text=f"Не удалось запросить mail resolver у админки: {exc}",
            )
            _emit_update("mailbox_unavailable", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

        mail_snapshot = _mail_challenge_snapshot(resolved)
        status_value = str(resolved.get("status") or "").strip().lower()
        kind_value = str(resolved.get("kind") or "").strip().lower()
        if status_value != "resolved":
            reason_code = str(mail_snapshot.get("reason_code") or "").strip().lower()
            if reason_code == "mail_not_found" and screen_kind == "numeric_code" and attempt == 0:
                _emit_update(
                    "mail_code_not_found",
                    "Свежий код из письма пока не найден. Прошу Instagram отправить письмо повторно.",
                    mail_snapshot,
                )
                resend_attempted = _request_instagram_new_email_code(device, serial)
                next_lookup_from = max(next_lookup_from, int(time.time()))
                continue
            if reason_code == "mail_not_found" and resend_attempted:
                mail_snapshot = _mail_challenge_snapshot(
                    resolved,
                    status="not_found",
                    kind="numeric_code",
                    reason_code="mail_not_found_after_resend",
                    reason_text="Попросил Instagram отправить новый код, но свежего письма так и не появилось.",
                )
            override_result = _override_terminal_challenge_state(device, serial, payload)
            if override_result is not None:
                state, detail, snapshot = override_result
                _emit_update(snapshot["reason_code"] or "challenge_screen_unsupported", snapshot["reason_text"], snapshot)
                return (state, _join_detail_text(initial_detail, detail), snapshot)
            reason_text = str(mail_snapshot.get("reason_text") or "Не удалось получить код из почты.")
            _emit_update(
                "mailbox_unavailable" if reason_code == "mailbox_unavailable" else "mail_code_not_found",
                reason_text,
                mail_snapshot,
            )
            return ("challenge_required", _join_detail_text(initial_detail, reason_text), mail_snapshot)

        if kind_value == "approval_link":
            link_url = str(resolved.get("link_url") or "").strip()
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="resolved",
                kind="approval_link",
                reason_code="approval_link_opened",
                reason_text="Нашёл письмо Instagram/Meta со ссылкой подтверждения. Открываю её автоматически.",
            )
            _emit_update("approval_link_opened", snapshot["reason_text"], snapshot)
            try:
                next_state, next_detail, opened_link = _apply_instagram_approval_link(serial, link_url, twofa_secret)
            except Exception as exc:
                snapshot = _mail_challenge_snapshot(
                    resolved,
                    status="resolved",
                    kind="approval_link",
                    reason_code="approval_link_failed",
                    reason_text=f"Письмо найдено, но approval link не удалось открыть автоматически: {exc}",
                )
                _emit_update("approval_link_failed", snapshot["reason_text"], snapshot)
                return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)
            if next_state == "login_submitted":
                snapshot = _mail_challenge_snapshot(
                    resolved,
                    status="resolved",
                    kind="approval_link",
                    reason_code="approval_link_applied",
                    reason_text="Ссылка подтверждения открыта автоматически, Instagram подтвердил вход.",
                )
                _emit_update("approval_link_applied", snapshot["reason_text"], snapshot)
                return (
                    "login_submitted",
                    _join_detail_text("Ссылка из письма открыта автоматически.", next_detail),
                    snapshot,
                )
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="resolved",
                kind="approval_link",
                reason_code="approval_link_failed",
                reason_text=_join_detail_text(
                    "Ссылка подтверждения открыта автоматически, но Instagram всё ещё требует ручной шаг.",
                    next_detail,
                ),
            )
            _emit_update("approval_link_failed", snapshot["reason_text"], snapshot)
            if next_state != "challenge_required":
                return (next_state, next_detail, snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

        if kind_value != "numeric_code":
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="unsupported",
                kind="approval_link" if kind_value == "approval_link" else "unsupported",
                reason_code="challenge_requires_link",
                reason_text="Почта нашлась, но Instagram прислал ссылку подтверждения вместо кода.",
            )
            _emit_update("challenge_requires_link", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

        code_value = str(resolved.get("code") or "").strip()
        if not code_value:
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="not_found",
                kind="numeric_code",
                reason_code="mail_not_found",
                reason_text="Mail resolver не вернул сам код, хотя письмо было найдено.",
            )
            _emit_update("mail_code_not_found", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

        _emit_update("mail_code_found", "Нашёл свежий код из письма. Ввожу его в Instagram.", mail_snapshot)
        if not _submit_instagram_email_code(device, serial, code_value):
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="unsupported",
                kind="numeric_code",
                reason_code="challenge_screen_unsupported",
                reason_text="На экране challenge не удалось найти поле ввода кода.",
            )
            _emit_update("challenge_screen_unsupported", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)

        next_state, next_detail = _detect_post_login_state(device, serial, twofa_secret)
        if next_state == "login_submitted":
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="resolved",
                kind="numeric_code",
                reason_code="mail_code_applied",
                reason_text="Код из почты введён автоматически, Instagram подтвердил вход.",
            )
            _emit_update("mail_code_applied", snapshot["reason_text"], snapshot)
            return ("login_submitted", _join_detail_text("Код из почты введён автоматически.", next_detail), snapshot)
        if next_state != "challenge_required":
            return (next_state, next_detail, mail_snapshot)

        screen_kind, screen_detail = _classify_login_challenge_screen(device, serial)
        if screen_kind != "numeric_code":
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="unsupported",
                kind="unsupported",
                reason_code="challenge_screen_unsupported",
                reason_text=screen_detail,
            )
            _emit_update("challenge_screen_unsupported", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, screen_detail), snapshot)
        if attempt == 1:
            snapshot = _mail_challenge_snapshot(
                resolved,
                status="resolved",
                kind="numeric_code",
                reason_code="mail_code_rejected",
                reason_text="Instagram не принял код из письма. Более свежего письма не найдено.",
            )
            _emit_update("mail_code_rejected", snapshot["reason_text"], snapshot)
            return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)
        next_lookup_from = max(next_lookup_from, int(resolved.get("received_at") or 0) + 1)

    snapshot = _mail_challenge_snapshot(
        mail_snapshot,
        status="resolved",
        kind="numeric_code",
        reason_code="mail_code_rejected",
        reason_text="Instagram не принял код из письма.",
    )
    _emit_update("mail_code_rejected", snapshot["reason_text"], snapshot)
    return ("challenge_required", _join_detail_text(initial_detail, snapshot["reason_text"]), snapshot)


def _detect_post_login_state(device: Any, serial: str, twofa_secret: str = "") -> tuple[str, str]:
    time.sleep(3)
    _dismiss_system_dialogs(device, serial, timeout_seconds=2.0)
    invalid_password = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(incorrect password|password you entered is incorrect|try again)"},
            {"textMatches": "(?i)(неверн.*парол|парол.*невер)"},
        ],
        timeout_seconds=2.5,
    )
    if invalid_password is not None:
        return ("invalid_password", "Instagram отклонил пароль. Проверь account_password у этого аккаунта.")

    if _maybe_submit_twofa(device, serial, twofa_secret):
        if _handle_post_login_prompts(device, serial, timeout_seconds=20.0):
            if _signed_out_surface_visible(device, serial):
                return ("login_failed", "Instagram остался в signed-out flow после 2FA. Повторю вход заново.")
            return ("login_submitted", "Вход выполнен. Instagram открылся после автоматического 2FA.")

    if _mail_code_challenge_visible(device):
        return ("challenge_required", "Instagram запросил код из письма. Попробую получить его автоматически.")

    if _human_check_visible(device):
        return ("challenge_required", "Instagram показал Confirm you're a human / CAPTCHA. Автоматический обход отключён, нужна ручная проверка аккаунта.")

    if _unfamiliar_device_challenge_visible(device):
        return (
            "challenge_required",
            "Instagram заблокировал вход экраном Try another device to continue. Для этого аккаунта нужен вход с ранее доверенного устройства.",
        )

    manual_2fa = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(two-factor|two factor|security code|confirmation code|enter code|login code|authentication app)"},
            {"textMatches": "(?i)(код безопасности|код подтверждения|двухфактор)"},
        ],
        timeout_seconds=2.5,
    )
    if manual_2fa is not None:
        detail = "Instagram запросил 2FA. Заверши этот шаг вручную в приложении."
        if not twofa_secret:
            detail = "Instagram запросил 2FA. В account.twofa нет секрета, поэтому заверши шаг вручную."
        return ("manual_2fa_required", detail)

    screen_kind = "unknown"
    screen_detail = ""
    if callable(device):
        try:
            screen_kind, screen_detail = _classify_login_challenge_screen(device, serial)
        except Exception as exc:
            logger.warning("login_challenge_classification_failed: serial=%s error=%s", serial, exc)
    if screen_kind != "unknown":
        return ("challenge_required", screen_detail)

    challenge = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(confirm it's you|help us confirm|suspicious login|approve login|challenge|secure your account)"},
            {"textMatches": "(?i)(подтверд.*это вы|подозрительн.*вход|подтверд.*вход)"},
        ],
        timeout_seconds=2.5,
    )
    if challenge is not None:
        return ("challenge_required", "Instagram запросил challenge или подтверждение входа. Fully-auto publish остановлен для этого аккаунта.")

    if _signed_out_surface_visible(device, serial):
        return ("login_failed", "Instagram вернулся в signed-out экран вместо домашней ленты. Повторю вход заново.")

    if _handle_post_login_prompts(device, serial, timeout_seconds=18.0):
        if _signed_out_surface_visible(device, serial):
            return ("login_failed", "Instagram вернулся в signed-out экран вместо домашней ленты. Повторю вход заново.")
        return ("login_submitted", "Вход выполнен. Instagram открыт на основном экране.")

    if _login_form_visible(device):
        return ("invalid_password", "Instagram вернулся на экран входа. Проверь логин, пароль или состояние сети.")

    if _signed_out_surface_visible(device, serial):
        return ("login_failed", "Instagram не завершил вход и остался в signed-out flow. Повторю вход заново.")
    if not _instagram_is_foreground(serial):
        return ("challenge_required", "Instagram ушёл из foreground после отправки логина. Fully-auto publish остановлен до ручной проверки этого аккаунта.")
    return ("challenge_required", "Логин отправлен, но Instagram не подтвердил домашний экран автоматически. Fully-auto publish остановлен до ручной проверки этого аккаунта.")


def _finalize_login_state(state: str, detail: str) -> tuple[str, str]:
    state_value = (state or "").strip()
    detail_value = (detail or "").strip()
    if state_value == "login_failed":
        return (
            "challenge_required",
            detail_value or "Instagram не завершил fully-auto вход и остался в signed-out flow.",
        )
    if state_value == "manual_step_required":
        return (
            "challenge_required",
            detail_value or "Instagram запросил ручной шаг после логина. Fully-auto publish остановлен.",
        )
    return state_value, detail_value


def _publish_status_from_login_state(login_state: str, mail_challenge: Optional[dict[str, Any]] = None) -> str:
    value = (login_state or "").strip().lower()
    if value == "challenge_required" and isinstance(mail_challenge, dict):
        kind = str(mail_challenge.get("kind") or "").strip().lower()
        reason_code = str(mail_challenge.get("reason_code") or "").strip().lower()
        if kind == "numeric_code" or reason_code in {
            "mailbox_missing_credentials",
            "mailbox_unavailable",
            "mail_not_found",
            "mail_ambiguous",
            "mail_code_rejected",
        }:
            return "email_code_required"
    if value in {"manual_2fa_required", "challenge_required", "invalid_password"}:
        return value
    if value == "helper_error":
        return "publish_error"
    return "login_required"


def _allow_media_permissions(device: Any, serial: str = "", timeout_seconds: float = 10.0) -> None:
    allow_selectors = [
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_foreground_only_button"},
        {"resourceId": "com.android.permissioncontroller:id/permission_allow_one_time_button"},
    ]
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if not _click_first(device, allow_selectors, timeout_seconds=0.6, serial=serial):
            break
        logger.info("manual_step_required: auto-allowed media permission")
        time.sleep(1.0)


def _dismiss_instagram_interstitials(device: Any, serial: str, timeout_seconds: float = 6.0) -> bool:
    dismiss_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/upsell_close"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/auxiliary_button", "textMatches": "(?i)^not now$"},
        {"textMatches": "(?i)^keep editing$"},
        {"descriptionMatches": "(?i)^keep editing$"},
        {"textMatches": "(?i)(not now|не сейчас|later|skip|got it|понятно)"},
        {"descriptionMatches": "(?i)(not now|не сейчас|later|skip|got it|понятно)"},
        {"textMatches": "(?i)(don[’']?t allow|don't allow|не разрешать|запретить)"},
        {"descriptionMatches": "(?i)(don[’']?t allow|don't allow|не разрешать|запретить)"},
    ]
    clicked_any = False
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _dismiss_instagram_app_rate_dialog(device, serial):
            clicked_any = True
            continue
        target = _find_first(device, _instagram_selectors(dismiss_selectors), timeout_seconds=0.5)
        if target is None:
            break
        if not _tap_object(serial, target):
            break
        clicked_any = True
        time.sleep(1.0)
    return clicked_any


def _tap_bottom_nav_slot(device: Any, serial: str, slot_index: int, slots: int = 5) -> None:
    width, height = _device_display_size(device)
    slot_index = max(0, min(slot_index, slots - 1))
    x = int(width * ((slot_index + 0.5) / slots))
    y = max(240, height - 110)
    _adb_tap(serial, x, y)


def _home_feed_visible(device: Any) -> bool:
    if _ig_find_first(device, [{"descriptionMatches": "(?i)instagram home feed"}], timeout_seconds=0.5) is not None:
        return True
    feed_tab = _ig_find_first(device, [{"resourceId": f"{INSTAGRAM_PACKAGE}:id/feed_tab"}], timeout_seconds=0.5)
    if feed_tab is not None and _obj_selected(feed_tab):
        return True
    return False


def _discard_creation_draft_prompt(device: Any, serial: str, timeout_seconds: float = 2.0) -> bool:
    target = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(start over|discard|delete draft)"},
            {"descriptionMatches": "(?i)(start over|discard|delete draft)"},
        ],
        timeout_seconds=timeout_seconds,
    )
    if target is None:
        return False
    if not _tap_object(serial, target):
        return False
    time.sleep(1.0)
    return True


def _ensure_home_feed(device: Any, serial: str, timeout_seconds: float = 24.0) -> None:
    deadline = time.time() + timeout_seconds
    main_tab_activity = f"{INSTAGRAM_PACKAGE}/.activity.MainTabActivity"
    forced_main_tab_attempts = 0
    home_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/feed_tab"},
        {"descriptionMatches": "(?i)^home$"},
    ]
    while time.time() < deadline:
        if not _instagram_is_foreground(serial):
            _launch_instagram_app(serial)
            time.sleep(2.0)
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.2)
        if _discard_creation_draft_prompt(device, serial, timeout_seconds=0.8):
            continue
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=1.2)
        if _home_feed_visible(device):
            _ig_click_first(
                device,
                [{"resourceId": f"{INSTAGRAM_PACKAGE}:id/upsell_close"}],
                timeout_seconds=0.5,
                serial=serial,
            )
            return
        if _ig_click_first(device, home_selectors, timeout_seconds=1.0, serial=serial):
            time.sleep(1.2)
            continue
        if forced_main_tab_attempts < 2:
            _adb_shell(serial, "am", "start", "-n", main_tab_activity, timeout=20, check=False)
            forced_main_tab_attempts += 1
            time.sleep(1.8)
            continue
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
        time.sleep(1.0)
    if _home_feed_visible(device):
        return
    raise RuntimeError("Не удалось перейти на домашнюю ленту Instagram.")


def _create_surface_visible(device: Any) -> bool:
    tab_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/cam_dest_feed"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/cam_dest_story"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/cam_dest_clips"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/cam_dest_live"},
        {"textMatches": "(?i)^post$"},
        {"textMatches": "(?i)^story$"},
        {"textMatches": "(?i)^reel$"},
        {"textMatches": "(?i)^live$"},
    ]
    tab_hits = 0
    for selector in tab_selectors:
        if _ig_find_first(device, [selector], timeout_seconds=0.2) is not None:
            tab_hits += 1
    if tab_hits >= 2:
        return True
    surface_markers = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/gallery_root_container"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/gallery_title_text", "textMatches": "(?i)new reel"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/gallery_grid_item_thumbnail"},
        {"textMatches": "(?i)(next|далее)"},
        {"descriptionMatches": "(?i)(next|далее)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*thumbnail.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*gallery.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*media.*thumbnail.*"},
    ]
    return _ig_find_first(device, surface_markers, timeout_seconds=0.4) is not None


def _instagram_task_has_create_surface(serial: str) -> bool:
    result = _adb_shell(serial, "dumpsys", "activity", "activities", timeout=40, check=False)
    output = str(result.stdout or "")
    if "com.instagram.android/com.instagram.mainactivity.InstagramMainActivity" not in output:
        return False
    markers = (
        "app:id/gallery_root_container",
        "app:id/gallery_grid_item_thumbnail",
        "app:id/gallery_title_text",
        "app:id/staged_gallery_container",
    )
    hits = sum(1 for marker in markers if marker in output)
    return hits >= 2


def _bring_instagram_task_to_front(serial: str) -> bool:
    launch_components = (
        f"{INSTAGRAM_PACKAGE}/com.instagram.mainactivity.InstagramMainActivity",
        f"{INSTAGRAM_PACKAGE}/.activity.MainTabActivity",
    )
    for component in launch_components:
        _adb_shell(serial, "am", "start", "-n", component, timeout=20, check=False)
        time.sleep(1.0)
        if _instagram_is_foreground(serial):
            return True
    _adb_shell(
        serial,
        "monkey",
        "-p",
        INSTAGRAM_PACKAGE,
        "-c",
        "android.intent.category.LAUNCHER",
        "1",
        timeout=25,
        check=False,
    )
    return _wait_for_instagram_foreground(serial, timeout_seconds=4.0)


def _reel_creation_surface_visible(device: Any) -> bool:
    title = _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/gallery_title_text", "textMatches": "(?i)new reel"},
            {"textMatches": "(?i)^new reel$"},
        ],
        timeout_seconds=0.5,
    )
    if title is not None:
        return True
    reel_tab = _ig_find_first(device, [{"resourceId": f"{INSTAGRAM_PACKAGE}:id/cam_dest_clips"}], timeout_seconds=0.5)
    if reel_tab is not None and _obj_selected(reel_tab):
        return True
    return False


def _wait_for_create_surface(device: Any, serial: str, timeout_seconds: float = 12.0) -> bool:
    deadline = time.time() + timeout_seconds
    last_recover_at = 0.0
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.8)
        _allow_media_permissions(device, serial, timeout_seconds=1.2)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=1.0)
        if _create_surface_visible(device):
            return True
        if not _instagram_is_foreground(serial):
            now = time.time()
            if now - last_recover_at >= 1.5:
                hidden_surface = _instagram_task_has_create_surface(serial)
                logger.warning(
                    "create_surface_foreground_lost: serial=%s activity=%s focus=%s hidden_surface=%s",
                    serial,
                    _current_top_activity(serial) or "-",
                    _current_focus_window(serial) or "-",
                    hidden_surface,
                )
                _bring_instagram_task_to_front(serial)
                last_recover_at = now
            time.sleep(0.6)
            continue
        time.sleep(0.5)
    return False


def _switch_creation_mode_to_reel(device: Any, serial: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if _reel_creation_surface_visible(device):
            return
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.8)
        _allow_media_permissions(device, serial, timeout_seconds=1.0)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.8)
        if not _instagram_is_foreground(serial):
            _bring_instagram_task_to_front(serial)
            time.sleep(1.0)
            continue
        if _reel_creation_surface_visible(device):
            return
        if _create_surface_visible(device):
            width, height = _device_display_size(device)
            # Coordinates confirmed on SlezhkaPixel13 for the bottom REEL tab in the create sheet.
            _adb_tap(serial, int(width * 0.81), int(height * 0.926))
            time.sleep(1.5)
            if _reel_creation_surface_visible(device):
                return
        time.sleep(0.4)
    raise RuntimeError("Не удалось переключить Instagram composer в режим Reel.")


def _recover_reel_creation_flow(device: Any, serial: str, *, reason: str = "") -> bool:
    for attempt in range(1, 3):
        hidden_surface = _instagram_task_has_create_surface(serial)
        logger.warning(
            "reel_create_flow_recover: serial=%s attempt=%s reason=%s activity=%s focus=%s hidden_surface=%s",
            serial,
            attempt,
            reason or "-",
            _current_top_activity(serial) or "-",
            _current_focus_window(serial) or "-",
            hidden_surface,
        )
        if hidden_surface:
            _bring_instagram_task_to_front(serial)
            _dismiss_system_dialogs(device, serial, timeout_seconds=1.0)
            _allow_media_permissions(device, serial, timeout_seconds=1.0)
            _dismiss_instagram_interstitials(device, serial, timeout_seconds=1.0)
            if _wait_for_create_surface(device, serial, timeout_seconds=3.5):
                return True
        if not _instagram_is_foreground(serial) and not _bring_instagram_task_to_front(serial):
            _launch_instagram_app(serial)
        _ensure_create_flow_open(device, serial)
        _switch_creation_mode_to_reel(device, serial, timeout_seconds=6.0)
        if _wait_for_create_surface(device, serial, timeout_seconds=5.0):
            return True
    return False


def _share_surface_visible(device: Any) -> bool:
    return _ig_find_first(
        device,
        [
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/share_button"},
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_share_button"},
            {"resourceId": f"{INSTAGRAM_PACKAGE}:id/creation_share_button"},
            {"textMatches": "(?i)(share|поделиться|publish|опубликовать)"},
            {"descriptionMatches": "(?i)(share|поделиться|publish|опубликовать)"},
        ],
        timeout_seconds=0.5,
    ) is not None


def _handle_publish_confirmation_prompt(device: Any, serial: str, timeout_seconds: float = 8.0) -> bool:
    prompt_markers = [
        {"textMatches": "(?i)update on your original audio"},
        {"textMatches": "(?i)original audio from this and any future reels"},
    ]
    action_selectors = [
        {"textMatches": "(?i)^turn off and share$"},
        {"descriptionMatches": "(?i)^turn off and share$"},
        {"textMatches": "(?i)^share$"},
        {"descriptionMatches": "(?i)^share$"},
    ]
    handled = False
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        prompt_visible = _ig_find_first(device, prompt_markers, timeout_seconds=0.5) is not None
        current_activity = _current_top_activity(serial)
        if not prompt_visible and "ModalActivity" not in current_activity:
            break
        action = _ig_find_first(device, action_selectors, timeout_seconds=0.5)
        if action is not None and _tap_object(serial, action):
            handled = True
            time.sleep(1.5)
            continue
        time.sleep(0.4)
    return handled


def _extract_publish_progress_pct(device: Any) -> Optional[int]:
    selectors = [
        {"textMatches": r"(?i)\b\d{1,3}%\b"},
        {"descriptionMatches": r"(?i)\b\d{1,3}%\b"},
        {"textMatches": r"(?i)(uploading|processing|sharing)[^\\n]*\b\d{1,3}%\b"},
        {"descriptionMatches": r"(?i)(uploading|processing|sharing)[^\\n]*\b\d{1,3}%\b"},
    ]
    target = _ig_find_first(device, selectors, timeout_seconds=0.2)
    if target is None:
        return None
    match = re.search(r"(\d{1,3})\s*%", _obj_text(target))
    if not match:
        return None
    try:
        value = int(match.group(1))
    except Exception:
        return None
    return max(0, min(100, value))


def _publish_wait_detail(source_name: str, result: PublishWaitResult) -> str:
    phase_label = {
        "waiting_upload_start": "Жду старта загрузки",
        "uploading": "Видео загружается",
        "waiting_confirmation": "Жду подтверждение публикации",
    }.get(result.publish_phase, "Публикация Reel")
    parts = [f"{phase_label} для {source_name}."]
    if result.last_activity:
        parts.append(result.last_activity)
    if result.upload_progress_pct is not None:
        parts.append(f"Прогресс загрузки: {int(result.upload_progress_pct)}%.")
    if result.elapsed_seconds > 0:
        parts.append(f"Прошло {int(result.elapsed_seconds)} сек.")
    return " ".join(part.strip() for part in parts if str(part).strip())


def _ensure_create_flow_open(device: Any, serial: str) -> None:
    create_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/action_bar_left_button"},
        {"descriptionMatches": "(?i)(create a post, story, reel or live video)"},
        {"descriptionMatches": "(?i)(create|создать|new post|camera)"},
        {"textMatches": "(?i)(create|создать|new post)"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/creation_tab"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_creation_entry_point"},
    ]
    for attempt in range(3):
        _ensure_home_feed(device, serial)
        _grant_instagram_media_permissions(serial)
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.5)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=1.2)
        if _create_surface_visible(device):
            return
        # This top-left action is stable on SlezhkaPixel13 and avoids flaky UIAutomator clicks.
        _adb_tap(serial, 48, 136)
        time.sleep(1.2)
        if not _wait_for_create_surface(device, serial, timeout_seconds=4.0):
            _ig_click_first(device, create_selectors, timeout_seconds=1.0, serial=serial)
        if not _wait_for_create_surface(device, serial, timeout_seconds=4.0):
            if attempt < 2:
                _adb_shell(serial, "am", "start", "-n", f"{INSTAGRAM_PACKAGE}/.activity.MainTabActivity", timeout=20, check=False)
                time.sleep(1.5)
                continue
            raise RuntimeError("Не удалось открыть create flow в Instagram.")
        return


def _open_reel_creation_flow(device: Any, serial: str) -> None:
    _ensure_create_flow_open(device, serial)
    _switch_creation_mode_to_reel(device, serial)
    if not _wait_for_create_surface(device, serial, timeout_seconds=8.0):
        if _recover_reel_creation_flow(device, serial, reason="reel_surface_missing_after_switch"):
            return
        raise RuntimeError("Instagram не открыл экран выбора медиа для Reel.")


def _select_reel_media(device: Any, serial: str) -> None:
    deadline = time.time() + 20.0
    thumbnail_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/gallery_grid_item_thumbnail"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*thumbnail.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*gallery.*image.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*media.*thumbnail.*"},
        {"className": "android.widget.ImageView", "instance": 0},
        {"className": "android.widget.ImageView", "instance": 1},
        {"className": "android.widget.ImageView", "instance": 2},
    ]
    next_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_right_action_button"},
        {"textMatches": "(?i)(next|далее)"},
        {"descriptionMatches": "(?i)(next|далее)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*next.*"},
    ]
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.0)
        _allow_media_permissions(device, serial, timeout_seconds=1.2)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.8)
        if _ig_find_first(device, next_selectors, timeout_seconds=0.5) is not None:
            return
        if _ig_click_first(device, thumbnail_selectors, timeout_seconds=1.5, serial=serial):
            time.sleep(1.2)
            continue
        _adb_tap(serial, 180, 470)
        time.sleep(1.0)
    raise RuntimeError("Не удалось выбрать импортированное видео для Reel.")


def _advance_reel_next(device: Any, serial: str, steps: int = 2) -> None:
    next_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_right_action_button"},
        {"textMatches": "(?i)(next|далее|continue|продолжить)"},
        {"descriptionMatches": "(?i)(next|далее|continue|продолжить)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*next.*"},
    ]
    for _ in range(steps):
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.2)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.8)
        if _share_surface_visible(device):
            return
        target = _ig_find_first(device, next_selectors, timeout_seconds=8.0)
        if target is None:
            if _share_surface_visible(device):
                return
            raise RuntimeError("Instagram не открыл следующий экран Reel.")
        if not _tap_object(serial, target):
            _adb_tap(serial, 980, 92)
        time.sleep(2.2)
    if not _wait_until(lambda: _share_surface_visible(device), timeout_seconds=10.0, interval=0.6):
        raise RuntimeError("Instagram не дошёл до экрана Share для Reel.")


def _share_reel(device: Any, serial: str) -> None:
    share_selectors = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/share_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_share_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/creation_share_button"},
        {"textMatches": "(?i)(share|поделиться|publish|опубликовать)"},
        {"descriptionMatches": "(?i)(share|поделиться|publish|опубликовать)"},
    ]
    _dismiss_system_dialogs(device, serial, timeout_seconds=1.2)
    _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.8)
    target = _ig_find_first(device, share_selectors, timeout_seconds=10.0)
    if target is None:
        raise RuntimeError("Не удалось найти кнопку Share для публикации Reel.")
    if not _tap_object(serial, target):
        _adb_tap(serial, 980, 92)
    time.sleep(1.5)
    _handle_publish_confirmation_prompt(device, serial, timeout_seconds=8.0)


def _wait_for_publish_success(
    device: Any,
    serial: str,
    timeout_seconds: float = 180.0,
    *,
    heartbeat_seconds: float = float(PUBLISH_HEARTBEAT_SECONDS),
    on_update: Optional[Callable[[PublishWaitResult], None]] = None,
) -> PublishWaitResult:
    explicit_success_markers = [
        {"descriptionMatches": "(?i)view insights"},
        {"descriptionMatches": "(?i)boost reel"},
        {"textMatches": "(?i)view insights"},
        {"textMatches": "(?i)boost reel"},
        {"textMatches": "(?i)(your reel has been shared|shared your reel|your reel is live|your reel has been posted)"},
        {"textMatches": "(?i)(опубликовано|поделились)"},
    ]
    uploading_markers = [
        {"textMatches": "(?i)(uploading|processing|your reel will be shared|finishing up|preparing|sharing to reels|sharing reel|sharing to reel|posting reel|publishing reel)"},
        {"descriptionMatches": "(?i)(sharing to reels|sharing reel|sharing to reel|uploading|processing)"},
        {"textMatches": "(?i)(загружается|обрабатывается|публикуется|подготовка|публикация reel|отправка reel)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*upload.*progress.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*progress.*bar.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*uploading.*"},
    ]
    blocking_markers = [
        {"textMatches": "(?i)(draft|save draft|processing failed|couldn.t post|couldn't post|retry)"},
        {"textMatches": "(?i)(черновик|ошибка|не удалось опубликовать|повторить)"},
    ]
    composer_markers = [
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/share_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/clips_share_button"},
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/creation_share_button"},
        {"textMatches": "(?i)(share|поделиться|publish|опубликовать)"},
        {"textMatches": "(?i)(next|далее|continue|продолжить)"},
        {"textMatches": "(?i)^post$"},
        {"textMatches": "(?i)^story$"},
        {"textMatches": "(?i)^reel$"},
        {"textMatches": "(?i)^live$"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*thumbnail.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*gallery.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*media.*thumbnail.*"},
    ]
    wait_started_at = time.monotonic()
    deadline = wait_started_at + timeout_seconds
    upload_start_deadline = wait_started_at + min(float(PUBLISH_UPLOAD_START_WAIT_SECONDS), max(20.0, timeout_seconds))
    stable_surface_hits = 0
    share_clicked_at = wait_started_at
    last_log_at = share_clicked_at
    last_update_at = 0.0
    accepted_by_instagram = False
    publish_phase = "waiting_upload_start"
    waiting_confirmation_since: Optional[float] = None
    upload_progress_pct: Optional[int] = None
    last_activity = "Нажал Share, жду старта загрузки в Instagram."

    def _snapshot(
        outcome: str,
        *,
        reason_code: str = "",
        success: bool = False,
        event_kind: str = "",
    ) -> PublishWaitResult:
        return PublishWaitResult(
            outcome=outcome,
            publish_phase=publish_phase,
            accepted_by_instagram=accepted_by_instagram,
            elapsed_seconds=int(max(0, round(time.monotonic() - share_clicked_at))),
            upload_progress_pct=upload_progress_pct,
            last_activity=last_activity,
            reason_code=reason_code,
            success=success,
            event_kind=event_kind,
        )

    def _emit_update(*, force: bool = False, outcome: Optional[str] = None, event_kind: str = "") -> None:
        nonlocal last_update_at
        if on_update is None:
            return
        now = time.monotonic()
        if not force and now - last_update_at < max(0.05, float(heartbeat_seconds or 0)):
            return
        snapshot = _snapshot(outcome or ("uploading_detected" if accepted_by_instagram else "waiting"), event_kind=event_kind)
        last_update_at = now
        on_update(snapshot)

    while time.monotonic() < deadline:
        elapsed = time.monotonic() - share_clicked_at
        if time.monotonic() - last_log_at >= 15.0:
            current_activity_log = _current_top_activity(serial)
            logger.info(
                "wait_publish_progress: serial=%s elapsed=%.0fs phase=%s activity=%s accepted=%s upload_progress_pct=%s",
                serial,
                elapsed,
                publish_phase,
                current_activity_log,
                accepted_by_instagram,
                upload_progress_pct,
            )
            last_log_at = time.monotonic()

        _dismiss_system_dialogs(device, serial, timeout_seconds=0.5)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.5)
        _handle_publish_confirmation_prompt(device, serial, timeout_seconds=1.2)
        if not _instagram_is_foreground(serial):
            last_activity = "Instagram ушёл из foreground, продолжаю ждать подтверждение публикации."
            _emit_update()
            time.sleep(1.0)
            continue
        current_activity = _current_top_activity(serial)
        if _ig_find_first(device, blocking_markers, timeout_seconds=0.6) is not None:
            logger.warning("wait_publish_blocked: serial=%s elapsed=%.0fs", serial, elapsed)
            last_activity = "Instagram показал блокирующий экран или ошибку публикации."
            return _snapshot("publish_blocked", reason_code="publish_blocked", event_kind="publish_blocked")
        if _ig_find_first(device, explicit_success_markers, timeout_seconds=0.8) is not None:
            logger.info("wait_publish_success_explicit: serial=%s elapsed=%.0fs", serial, elapsed)
            accepted_by_instagram = True
            publish_phase = "waiting_confirmation"
            upload_progress_pct = 100
            last_activity = "Instagram явно подтвердил публикацию Reel. Перехожу к проверке профиля."
            return _snapshot("publish_confirmed", success=True, event_kind="publish_confirmation_wait")
        if _published_reel_viewer_visible(device):
            logger.info("wait_publish_published_reel_viewer: serial=%s elapsed=%.0fs", serial, elapsed)
            accepted_by_instagram = True
            publish_phase = "waiting_confirmation"
            upload_progress_pct = 100
            last_activity = "Instagram открыл опубликованный Reel с кнопками Insights/Boost. Перехожу к проверке профиля."
            return _snapshot("publish_confirmed", success=True, event_kind="published_reel_viewer")
        if _post_publish_share_sheet_visible(device):
            logger.info("wait_publish_post_share_sheet_detected: serial=%s elapsed=%.0fs", serial, elapsed)
            accepted_by_instagram = True
            publish_phase = "waiting_confirmation"
            upload_progress_pct = 100
            last_activity = "Instagram открыл share sheet после публикации Reel. Перехожу к проверке профиля."
            return _snapshot("publish_confirmed", success=True, event_kind="post_publish_share_sheet")

        upload_progress_pct = _extract_publish_progress_pct(device)
        uploading_visible = (
            _ig_find_first(device, uploading_markers, timeout_seconds=0.6) is not None
            or upload_progress_pct is not None
        )
        composer_visible = _ig_find_first(device, composer_markers, timeout_seconds=0.4) is not None
        if uploading_visible:
            stable_surface_hits = 0
            if not accepted_by_instagram:
                accepted_by_instagram = True
                publish_phase = "uploading"
                logger.info("wait_publish_uploading_detected: serial=%s elapsed=%.0fs", serial, elapsed)
                _emit_update(force=True, outcome="uploading_detected", event_kind="uploading_detected")
            publish_phase = "uploading"
            waiting_confirmation_since = None
            if upload_progress_pct is not None:
                last_activity = f"Instagram загружает Reel: {int(upload_progress_pct)}%."
            else:
                last_activity = "Instagram принял Reel в загрузку."
        else:
            upload_progress_pct = None
            if accepted_by_instagram:
                if publish_phase != "waiting_confirmation":
                    publish_phase = "waiting_confirmation"
                    waiting_confirmation_since = time.monotonic()
                    stable_surface_hits = 0
                    last_activity = "Upload завершён, жду подтверждение публикации от Instagram."
                    _emit_update(force=True, outcome="uploading_detected", event_kind="publish_confirmation_wait")
                else:
                    last_activity = "Жду подтверждение публикации от Instagram."

                if (
                    not composer_visible
                    and ("MainTabActivity" in current_activity or "InstagramMainActivity" in current_activity or _home_feed_visible(device))
                ):
                    stable_surface_hits += 1
                    if stable_surface_hits >= 4 and time.monotonic() - (waiting_confirmation_since or share_clicked_at) >= 8.0:
                        logger.info("wait_publish_success_stable_main: serial=%s elapsed=%.0fs", serial, elapsed)
                        last_activity = "Instagram вернулся на основной экран после публикации Reel. Перехожу к проверке профиля."
                        return _snapshot("publish_confirmed", success=True, event_kind="publish_confirmation_wait")
                else:
                    stable_surface_hits = 0
            else:
                stable_surface_hits = 0
                if time.monotonic() >= upload_start_deadline:
                    logger.warning("wait_publish_not_started: serial=%s elapsed=%.0fs", serial, elapsed)
                    last_activity = "Instagram не начал загрузку Reel после нажатия Share."
                    return _snapshot("publish_timeout", reason_code="publish_not_started", event_kind="publish_not_started")

        _emit_update()
        time.sleep(1.0)
    if accepted_by_instagram:
        logger.warning("wait_publish_confirmation_timeout: serial=%s elapsed=%.0fs", serial, timeout_seconds)
        publish_phase = "waiting_confirmation"
        upload_progress_pct = None
        last_activity = "Instagram слишком долго не подтвердил завершение публикации Reel."
        return _snapshot("publish_timeout", reason_code="publish_confirmation_timeout", event_kind="publish_confirmation_timeout")
    logger.warning("wait_publish_timeout: serial=%s elapsed=%.0fs accepted=%s", serial, timeout_seconds, accepted_by_instagram)
    last_activity = "Instagram не начал загрузку Reel после нажатия Share."
    return _snapshot("publish_timeout", reason_code="publish_not_started", event_kind="publish_not_started")


def _run_login_flow(
    payload: dict[str, Any],
    *,
    push_status: bool = True,
    finalize_runtime: bool = True,
    preferred_serial: str = "",
    on_mail_challenge_update: Optional[Callable[[str, str, dict[str, Any]], None]] = None,
) -> dict[str, Any]:
    account_id = int(payload["account_id"])
    login = str(payload.get("account_login") or "").strip()
    password = str(payload.get("account_password") or "").strip()
    expected_handle = str(payload.get("username") or payload.get("account_login") or "").strip()
    force_clean_login = bool(payload.get("force_clean_login"))
    if not login or not password:
        raise RuntimeError("Missing account_login or account_password.")

    serial = _ensure_emulator_ready(preferred_serial=preferred_serial)
    if force_clean_login:
        logger.info("login_force_clean: account_id=%s serial=%s", account_id, serial)
    try:
        _ensure_instagram_installed(serial)
    except RuntimeError as exc:
        if "not installed in emulator" in str(exc).lower():
            _open_play_store_listing(serial)
            _set_state(
                account_id=account_id,
                target=str(payload.get("target") or "instagram_app_login"),
                state="manual_step_required",
                detail="Instagram не установлен. Открыл страницу приложения в Google Play внутри эмулятора. Войди в Google и установи Instagram один раз.",
                flow_running=not finalize_runtime,
                emulator_serial=serial,
            )
            if push_status:
                _push_account_launch_status(
                    account_id,
                    "helper_error",
                    "Instagram не установлен в эмуляторе. Сначала установи приложение, затем повтори запуск.",
                    expected_handle,
                )
            logger.info("manual_step_required: account_id=%s serial=%s reason=install_instagram", account_id, serial)
            return {
                "account_id": account_id,
                "serial": serial,
                "device": None,
                "state": "helper_error",
                "detail": "Instagram не установлен в эмуляторе. Сначала установи приложение, затем повтори запуск.",
                "handle": expected_handle,
            }
        raise
    next_state = "helper_error"
    next_detail = "Instagram login did not start."
    device = None
    mail_challenge_snapshot: dict[str, Any] = {}
    for attempt in range(2):
        if force_clean_login and attempt == 0:
            _clear_instagram_data(serial)
        _launch_instagram_app(serial)
        device = _connect_ui(serial)
        _dismiss_system_dialogs(device, serial, timeout_seconds=2.0)
        if not force_clean_login and _wait_for_logged_in_surface(device, serial, timeout_seconds=2.5):
            next_state = "login_submitted"
            next_detail = "Instagram уже открыт в активной сессии. Повторный relogin не понадобился."
            logger.info("login_session_reused: account_id=%s serial=%s attempt=%s", account_id, serial, attempt + 1)
            break
        session_prepare = _ensure_signed_out_instagram_session(device, serial, allow_destructive_fallback=True)
        logger.info("login_session_prepared: account_id=%s serial=%s action=%s attempt=%s", account_id, serial, session_prepare, attempt + 1)
        if session_prepare == "app_data_cleared":
            _launch_instagram_app(serial)
            device = _connect_ui(serial)
            _dismiss_system_dialogs(device, serial, timeout_seconds=2.0)
        challenge_started_at = int(time.time())
        _fill_credentials_and_submit(device, serial, login, password)
        next_state, next_detail = _detect_post_login_state(device, serial, str(payload.get("twofa") or ""))
        mail_challenge_snapshot = {}
        if next_state == "challenge_required":
            next_state, next_detail, mail_challenge_snapshot = _attempt_mail_challenge_login(
                device,
                serial,
                payload,
                challenge_started_at=challenge_started_at,
                initial_detail=next_detail,
                on_update=on_mail_challenge_update,
            )
        if next_state != "login_failed" or attempt == 1:
            break
        logger.info("login_retry: account_id=%s serial=%s attempt=%s", account_id, serial, attempt + 2)
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.5)
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
        time.sleep(1.5)
    next_state, next_detail = _finalize_login_state(next_state, next_detail)
    if next_state in {"manual_2fa_required", "challenge_required", "invalid_password", "helper_error"}:
        diagnostics = _capture_publish_diagnostics(serial, f"login_audit_{next_state}", account_id=account_id)
        screenshot_path = str(diagnostics.get("screenshot") or "").strip()
        if screenshot_path:
            next_detail = f"{next_detail} Диагностика: {screenshot_path}".strip()
    if push_status:
        _push_account_launch_status(account_id, next_state, next_detail, expected_handle, mail_challenge=mail_challenge_snapshot)
    _set_state(
        account_id=account_id,
        target=str(payload.get("target") or "instagram_app_login"),
        state=next_state,
        detail=next_detail,
        flow_running=not finalize_runtime,
        emulator_serial=serial,
    )
    logger.info("%s: account_id=%s serial=%s", next_state, account_id, serial)
    return {
        "account_id": account_id,
        "serial": serial,
        "device": device,
        "state": next_state,
        "detail": next_detail,
        "mail_challenge": dict(mail_challenge_snapshot),
        "handle": expected_handle,
    }


def _run_publish_flow(payload: dict[str, Any]) -> None:
    account_id = int(payload["account_id"])
    expected_handle = str(payload.get("username") or payload.get("account_login") or "").strip()
    helper_ticket = str(payload.get("ticket") or "").strip()
    source_path_override = str(payload.get("source_path") or "").strip()
    delete_source_on_success = bool(payload.get("delete_source_on_success", True))
    preferred_serial = str(payload.get("instagram_emulator_serial") or payload.get("emulator_serial") or "").strip()
    latest = _source_video_info_from_path(source_path_override) if source_path_override else _latest_source_video_info()
    if latest is None:
        detail = (
            f"В папке {INSTAGRAM_PUBLISH_SOURCE_DIR} нет .mp4/.mov файлов."
            if not source_path_override
            else f"Исходный файл {source_path_override} не найден."
        )
        _push_account_publish_status(account_id, "no_source_video", detail, expected_handle)
        _set_state(
            account_id=account_id,
            target="instagram_publish_latest_reel",
            state="no_source_video",
            detail=detail,
            flow_running=False,
        )
        logger.info("no_source_video: account_id=%s", account_id)
        return

    source_path = str(latest["path"])
    source_name = str(latest["name"])
    serial = ""
    current_stage = "preparing"
    profile_baseline: dict[str, Any] = {"available": False, "candidates": []}
    share_clicked_at_epoch: Optional[int] = None

    def _publish_progress(stage: str, detail: str, *, flow_running: bool = True) -> None:
        _push_account_publish_status(
            account_id,
            stage,
            detail,
            expected_handle,
            last_file=source_name,
        )
        _set_state(
            account_id=account_id,
            target="instagram_publish_latest_reel",
            state=stage,
            detail=detail,
            flow_running=flow_running,
            emulator_serial=serial,
        )

    try:
        _publish_progress("preparing", f"Выбран локальный файл {source_name}. Подготавливаю Instagram app.")

        login_result = _run_login_flow(
            {**payload, "target": "instagram_publish_latest_reel"},
            push_status=True,
            finalize_runtime=False,
            preferred_serial=preferred_serial,
        )
        serial = str(login_result.get("serial") or "")
        if login_result["state"] != "login_submitted":
            detail = f"Публикация остановлена: {login_result['detail']}"
            publish_status = _publish_status_from_login_state(
                str(login_result["state"]),
                login_result.get("mail_challenge"),
            )
            _push_account_publish_status(
                account_id,
                publish_status,
                detail,
                expected_handle,
                last_file=source_name,
            )
            _set_state(
                account_id=account_id,
                target="instagram_publish_latest_reel",
                state=publish_status,
                detail=detail,
                flow_running=False,
                emulator_serial=serial,
            )
            return

        device = login_result["device"]
        try:
            profile_baseline = _capture_profile_reels_baseline(device, serial, expected_handle=expected_handle)
            logger.info(
                "publish_profile_baseline: account_id=%s serial=%s available=%s candidates=%s",
                account_id,
                serial,
                bool(profile_baseline.get("available")),
                len(profile_baseline.get("candidates") or []),
            )
        except Exception as exc:
            logger.warning("publish_profile_baseline_failed: account_id=%s serial=%s error=%s", account_id, serial, exc)
            profile_baseline = {"available": False, "candidates": []}
        current_stage = "importing_media"
        _publish_progress(current_stage, f"Импортирую {source_name} в эмулятор.")
        imported_path = _import_video_into_emulator(serial, source_path)
        logger.info("media_imported: account_id=%s serial=%s source=%s target=%s", account_id, serial, source_path, imported_path)

        current_stage = "opening_reel_flow"
        _publish_progress(current_stage, f"Открываю Reel flow для {source_name}.")
        _open_reel_creation_flow(device, serial)

        current_stage = "selecting_media"
        _publish_progress(current_stage, f"Выбираю видео {source_name}.")
        _select_reel_media(device, serial)
        _advance_reel_next(device, serial, steps=2)

        current_stage = "publishing"
        _publish_progress(current_stage, f"Публикую Reel {source_name}.")
        share_clicked_at_epoch = int(time.time())
        _share_reel(device, serial)
        wait_result = _wait_for_publish_success(
            device,
            serial,
            timeout_seconds=float(PUBLISH_SUCCESS_WAIT_SECONDS),
            on_update=lambda result: _publish_progress(current_stage, _publish_wait_detail(source_name, result)),
        )
        if not wait_result.success and not wait_result.accepted_by_instagram:
            raise PublishFlowError(
                current_stage,
                _publish_wait_detail(source_name, wait_result),
                last_file=source_name,
                serial=serial,
                reason_code=wait_result.reason_code,
                payload={
                    "publish_phase": wait_result.publish_phase,
                    "accepted_by_instagram": wait_result.accepted_by_instagram,
                    "elapsed_seconds": wait_result.elapsed_seconds,
                    "last_activity": wait_result.last_activity,
                    "upload_progress_pct": wait_result.upload_progress_pct,
                    "event_kind": wait_result.event_kind,
                },
            )

        try:
            verification_result = _confirm_publish_via_profile(
                device,
                serial,
                source_name=source_name,
                baseline=profile_baseline,
                expected_handle=expected_handle,
                elapsed_since_share_seconds=wait_result.elapsed_seconds,
                share_clicked_at=share_clicked_at_epoch,
                diagnostics_context={"account_id": account_id},
                on_update=lambda result: _publish_progress(current_stage, _profile_verification_detail(source_name, result)),
            )
        except Exception as exc:
            logger.warning("publish_profile_verification_failed: account_id=%s serial=%s error=%s", account_id, serial, exc)
            verification_result = ProfileVerificationResult(
                needs_review=True,
                reason_code="publish_profile_navigation_failed",
                detail="Upload принят, но автоматическая проверка профиля не завершилась.",
                publish_phase="verifying_profile",
                baseline_available=bool(profile_baseline.get("available")),
                checked_slots=PUBLISH_PROFILE_CHECK_SLOTS,
                event_kind="needs_review",
                share_clicked_at=share_clicked_at_epoch,
                verification_starts_at=(int(share_clicked_at_epoch + PUBLISH_PROFILE_VERIFY_START_DELAY_SECONDS) if share_clicked_at_epoch else None),
                verification_deadline_at=(int(share_clicked_at_epoch + PUBLISH_PROFILE_VERIFY_SECONDS) if share_clicked_at_epoch else None),
                first_profile_check_at=None,
            )

        if verification_result.needs_review and not verification_result.verified:
            detail = (
                "Instagram принял Reel в публикацию, но новый ролик не удалось подтвердить автоматически. "
                "Нужна ручная проверка профиля."
            )
            if verification_result.detail:
                detail = f"{detail} {verification_result.detail}".strip()
            _push_account_publish_status(
                account_id,
                "needs_review",
                detail,
                expected_handle,
                last_file=source_name,
            )
            _set_state(
                account_id=account_id,
                target="instagram_publish_latest_reel",
                state="needs_review",
                detail=detail,
                flow_running=False,
                emulator_serial=serial,
            )
            logger.warning(
                "publish_needs_review: account_id=%s serial=%s reason=%s",
                account_id,
                serial,
                verification_result.reason_code or "publish_profile_inconclusive",
            )
            return

        if delete_source_on_success:
            _delete_local_source_video(source_path)
            detail = f"Reel опубликован и подтверждён через профиль. Исходный файл {source_name} удалён из локальной папки."
        else:
            detail = f"Reel опубликован и подтверждён через профиль. Staged файл {source_name} оставлен на месте."
        publish_telemetry = _build_reel_publish_telemetry(
            verification_result,
            helper_ticket=helper_ticket,
            fallback_published_at=(
                int(share_clicked_at_epoch) + int(wait_result.elapsed_seconds)
                if share_clicked_at_epoch is not None
                else int(time.time())
            ),
        )
        _push_account_publish_status(
            account_id,
            "published",
            detail,
            expected_handle,
            last_file=source_name,
            source_path=source_path,
            helper_ticket=helper_ticket,
            telemetry=publish_telemetry,
        )
        _set_state(
            account_id=account_id,
            target="instagram_publish_latest_reel",
            state="published",
            detail=detail,
            flow_running=False,
            emulator_serial=serial,
        )
        logger.info("published: account_id=%s serial=%s file=%s", account_id, serial, source_name)
    except PublishFlowError as exc:
        _capture_publish_diagnostics(exc.serial or serial, "publish_flow_failed", account_id=account_id)
        raise
    except Exception as exc:
        _capture_publish_diagnostics(serial, "publish_flow_error", account_id=account_id)
        raise PublishFlowError(
            current_stage,
            str(exc),
            last_file=source_name,
            serial=serial,
        ) from exc


def _run_publish_job(job: dict[str, Any]) -> None:
    job_id = int(job["id"])
    account_id = int(job["account_id"])
    batch_id = int(job["batch_id"])
    source_path = str(job.get("source_path") or "").strip()
    source_name = Path(source_path).name if source_path else ""
    preferred_serial = str(job.get("emulator_serial") or "").strip()
    serial = ""
    current_stage = "preparing"
    downloaded_source_path = ""
    timings: dict[str, float] = {}
    profile_baseline: dict[str, Any] = {"available": False, "candidates": []}
    share_clicked_at_epoch: Optional[int] = None
    payload = {
        "account_id": account_id,
        "account_login": str(job.get("account_login") or "").strip(),
        "account_password": str(job.get("account_password") or "").strip(),
        "username": str(job.get("username") or "").strip(),
        "twofa": str(job.get("twofa") or "").strip(),
        "mail_enabled": bool(job.get("mail_enabled")),
        "mail_address": str(job.get("mail_address") or "").strip(),
        "mail_provider": str(job.get("mail_provider") or "auto").strip() or "auto",
        "target": "publish_batch_job",
        "force_clean_login": True,
    }
    terminal_state = "failed"
    terminal_detail = "Публикация остановлена до завершения job."
    terminal_account_publish_state = ""
    terminal_payload: Optional[dict[str, Any]] = None
    terminal_last_file = source_name
    terminal_serial = ""

    def _record_timing(name: str, started_at: float) -> None:
        timings[name] = round(max(0.0, time.monotonic() - started_at), 2)

    def _telemetry_payload(extra: Optional[dict[str, Any]] = None) -> Optional[dict[str, Any]]:
        payload_value: dict[str, Any] = {}
        if timings:
            payload_value["timings"] = {key: float(value) for key, value in timings.items()}
        if isinstance(extra, dict):
            for key, value in extra.items():
                payload_value[key] = value
        return payload_value or None

    def _job_progress(
        stage: str,
        detail: str,
        *,
        flow_running: bool = True,
        payload: Optional[dict[str, Any]] = None,
        account_publish_state: str = "",
    ) -> None:
        _push_publish_job_status(
            job_id,
            stage,
            detail,
            last_file=source_name,
            serial=serial,
            source_path=source_path,
            account_publish_state=account_publish_state,
            payload=_telemetry_payload(payload),
        )
        _set_state(
            account_id=account_id,
            target="publish_batch_job",
            state=stage,
            detail=f"[batch {batch_id} job {job_id}] {detail}",
            flow_running=flow_running,
            emulator_serial=serial,
        )

    def _set_terminal(
        state: str,
        detail: str,
        *,
        account_publish_state: str = "",
        payload: Optional[dict[str, Any]] = None,
        last_file: str = "",
        serial_value: str = "",
    ) -> None:
        nonlocal terminal_state
        nonlocal terminal_detail
        nonlocal terminal_account_publish_state
        nonlocal terminal_payload
        nonlocal terminal_last_file
        nonlocal terminal_serial
        terminal_state = (state or "failed").strip()
        terminal_detail = (detail or "").strip()
        terminal_account_publish_state = (account_publish_state or "").strip()
        terminal_payload = payload
        terminal_last_file = (last_file or source_name).strip()
        terminal_serial = (serial_value or serial or preferred_serial).strip()

    def _reset_boundary(detail: str) -> None:
        target_serial = terminal_serial or serial or preferred_serial
        if not _publish_boundary_reset_needed(target_serial):
            return
        _job_progress(
            "preparing",
            detail,
            payload={
                "publish_phase": "cleanup_emulator",
                "event_kind": "publish_job_emulator_shutdown_started",
            },
        )
        logger.info(
            "publish_job_emulator_shutdown_started: batch_id=%s job_id=%s account_id=%s serial=%s",
            batch_id,
            job_id,
            account_id,
            target_serial or "-",
        )
        _reset_publish_emulator_boundary(target_serial, clear_instagram=True)
        logger.info(
            "publish_job_emulator_shutdown_completed: batch_id=%s job_id=%s account_id=%s serial=%s",
            batch_id,
            job_id,
            account_id,
            target_serial or "-",
        )

    try:
        source_started_at = time.monotonic()
        source_info = _resolve_publish_job_source(job_id, source_path)
        _record_timing("resolve_source_seconds", source_started_at)
        downloaded_from = str(source_info.get("downloaded_from") or "").strip()
        if downloaded_from:
            timings["download_source_seconds"] = timings.get("resolve_source_seconds", 0.0)
            logger.info(
                "publish_job_source_downloaded: job_id=%s source_path=%s cached_path=%s",
                job_id,
                downloaded_from,
                source_info["path"],
            )
        saved_to = str(source_info.get("saved_to") or "").strip()
        if saved_to:
            logger.info(
                "publish_job_source_saved_to_mac: job_id=%s cached_path=%s saved_path=%s",
                job_id,
                source_info["path"],
                saved_to,
            )
        source_path = str(source_info["path"])
        source_name = str(source_info["name"])
        if source_info.get("downloaded"):
            downloaded_source_path = source_path
        _job_progress("preparing", f"Batch job #{job_id}: готовлю Instagram app для {source_name}.")
        if _publish_boundary_reset_needed(preferred_serial):
            _reset_boundary("Закрываю прошлый эмулятор перед следующим аккаунтом.")
        _job_progress(
            "preparing",
            f"Запускаю чистый эмулятор и готовлю новый вход для {source_name}.",
            payload={
                "publish_phase": "fresh_boot",
                "event_kind": "publish_job_fresh_boot_started",
            },
        )
        logger.info(
            "publish_job_fresh_boot_started: batch_id=%s job_id=%s account_id=%s preferred_serial=%s",
            batch_id,
            job_id,
            account_id,
            preferred_serial or "-",
        )
        logger.info(
            "publish_job_clean_login_started: batch_id=%s job_id=%s account_id=%s",
            batch_id,
            job_id,
            account_id,
        )
        login_started_at = time.monotonic()
        login_result = _run_login_flow(
            payload,
            push_status=False,
            finalize_runtime=False,
            preferred_serial=preferred_serial,
            on_mail_challenge_update=lambda event_kind, detail, mail_snapshot: _job_progress(
                "preparing",
                detail,
                payload={
                    "publish_phase": "clean_login",
                    "event_kind": event_kind,
                    "mail_challenge": mail_snapshot,
                },
            ),
        )
        _record_timing("login_seconds", login_started_at)
        serial = str(login_result.get("serial") or "")
        terminal_serial = serial
        logger.info(
            "publish_job_fresh_boot_completed: batch_id=%s job_id=%s account_id=%s serial=%s",
            batch_id,
            job_id,
            account_id,
            serial or "-",
        )
        if login_result["state"] != "login_submitted":
            detail = f"Публикация остановлена: {login_result['detail']}"
            publish_status = _publish_status_from_login_state(
                str(login_result["state"]),
                login_result.get("mail_challenge"),
            )
            failure_payload = {"reason_code": publish_status}
            if isinstance(login_result.get("mail_challenge"), dict) and login_result.get("mail_challenge"):
                failure_payload["mail_challenge"] = dict(login_result["mail_challenge"])
            raise PublishFlowError(
                "failed",
                detail,
                last_file=source_name,
                serial=serial,
                reason_code=publish_status,
                account_publish_state=publish_status,
                payload=_telemetry_payload(failure_payload),
            )

        logger.info(
            "publish_job_clean_login_completed: batch_id=%s job_id=%s account_id=%s serial=%s",
            batch_id,
            job_id,
            account_id,
            serial or "-",
        )
        _job_progress(
            "preparing",
            f"Чистый эмулятор готов. Вхожу в новый аккаунт для {source_name}.",
            payload={
                "publish_phase": "clean_login",
                "event_kind": "publish_job_clean_login_completed",
            },
        )

        device = login_result["device"]
        baseline_started_at = time.monotonic()
        try:
            profile_baseline = _capture_profile_reels_baseline(
                device,
                serial,
                expected_handle=str(payload.get("username") or payload.get("account_login") or "").strip(),
            )
        except Exception as exc:
            logger.warning("job_profile_baseline_failed: batch_id=%s job_id=%s serial=%s error=%s", batch_id, job_id, serial, exc)
            profile_baseline = {"available": False, "candidates": []}
        _record_timing("capture_profile_baseline_seconds", baseline_started_at)
        current_stage = "importing_media"
        _job_progress(current_stage, f"Импортирую {source_name} в эмулятор.")
        import_started_at = time.monotonic()
        imported_path = _import_video_into_emulator(serial, source_path)
        _record_timing("import_into_emulator_seconds", import_started_at)
        logger.info(
            "job_media_imported: batch_id=%s job_id=%s account_id=%s serial=%s source=%s target=%s",
            batch_id,
            job_id,
            account_id,
            serial,
            source_path,
            imported_path,
        )

        current_stage = "opening_reel_flow"
        _job_progress(current_stage, f"Открываю Reel flow для {source_name}.")
        open_reel_started_at = time.monotonic()
        _open_reel_creation_flow(device, serial)
        _record_timing("open_reel_flow_seconds", open_reel_started_at)

        current_stage = "selecting_media"
        _job_progress(current_stage, f"Выбираю видео {source_name}.")
        media_select_started_at = time.monotonic()
        _select_reel_media(device, serial)
        _advance_reel_next(device, serial, steps=2)
        _record_timing("media_select_seconds", media_select_started_at)

        current_stage = "publishing"
        _job_progress(
            current_stage,
            f"Публикую Reel {source_name}.",
            payload={
                "publish_phase": "waiting_upload_start",
                "accepted_by_instagram": False,
                "elapsed_seconds": 0,
                "last_activity": "Нажал Share, жду старта загрузки в Instagram.",
                "upload_progress_pct": None,
                "event_kind": "publishing_started",
            },
        )
        share_started_at = time.monotonic()
        share_clicked_at_epoch = int(time.time())
        _share_reel(device, serial)
        _record_timing("tap_share_seconds", share_started_at)

        def _publish_wait_update(result: PublishWaitResult) -> None:
            if result.event_kind == "uploading_detected" and "time_to_upload_detected_seconds" not in timings:
                timings["time_to_upload_detected_seconds"] = float(result.elapsed_seconds)
            _job_progress(
                current_stage,
                _publish_wait_detail(source_name, result),
                payload={
                    "publish_phase": result.publish_phase,
                    "accepted_by_instagram": result.accepted_by_instagram,
                    "elapsed_seconds": result.elapsed_seconds,
                    "last_activity": result.last_activity,
                    "upload_progress_pct": result.upload_progress_pct,
                    "event_kind": result.event_kind or "publish_wait_heartbeat",
                },
            )

        wait_result = _wait_for_publish_success(
            device,
            serial,
            timeout_seconds=float(PUBLISH_SUCCESS_WAIT_SECONDS),
            on_update=_publish_wait_update,
        )
        if wait_result.accepted_by_instagram and "time_to_upload_detected_seconds" not in timings:
            timings["time_to_upload_detected_seconds"] = float(wait_result.elapsed_seconds)
        if not wait_result.success and not wait_result.accepted_by_instagram:
            raise PublishFlowError(
                current_stage,
                _publish_wait_detail(source_name, wait_result),
                last_file=source_name,
                serial=serial,
                reason_code=wait_result.reason_code,
                payload=_telemetry_payload(
                    {
                        "publish_phase": wait_result.publish_phase,
                        "accepted_by_instagram": wait_result.accepted_by_instagram,
                        "elapsed_seconds": wait_result.elapsed_seconds,
                        "last_activity": wait_result.last_activity,
                        "upload_progress_pct": wait_result.upload_progress_pct,
                        "event_kind": wait_result.event_kind,
                        "reason_code": wait_result.reason_code,
                    }
                ),
            )

        profile_verify_started_at = time.monotonic()

        def _profile_verification_update(result: ProfileVerificationResult) -> None:
            elapsed_total = max(wait_result.elapsed_seconds, int(max(0.0, round(time.monotonic() - share_started_at))))
            _job_progress(
                current_stage,
                _profile_verification_detail(source_name, result),
                payload={
                    "publish_phase": result.publish_phase or "verifying_profile",
                    "accepted_by_instagram": True,
                    "elapsed_seconds": elapsed_total,
                    "last_activity": result.detail or "Проверяю Reel в профиле аккаунта.",
                    "upload_progress_pct": None,
                    "event_kind": result.event_kind or "profile_verification_retry",
                    "reason_code": result.reason_code,
                    "verification_attempt": result.verification_attempt,
                    "verification_window_minutes": max(1, int(PUBLISH_PROFILE_VERIFY_SECONDS // 60)),
                    "checked_slots": result.checked_slots or PUBLISH_PROFILE_CHECK_SLOTS,
                    "matched_slot": result.matched_slot,
                    "matched_age_seconds": result.matched_age_seconds,
                    "reel_fingerprint": result.matched_fingerprint,
                    "reel_signature_text": result.matched_signature_text,
                    "baseline_available": result.baseline_available,
                    "seconds_until_profile_check": result.seconds_until_profile_check,
                    "share_clicked_at": result.share_clicked_at,
                    "verification_starts_at": result.verification_starts_at,
                    "verification_deadline_at": result.verification_deadline_at,
                    "first_profile_check_at": result.first_profile_check_at,
                    "profile_surface_state": result.profile_surface_state,
                    "keyboard_visible": result.keyboard_visible,
                    "comment_sheet_visible": result.comment_sheet_visible,
                    "clips_viewer_visible": result.clips_viewer_visible,
                    "quick_capture_visible": result.quick_capture_visible,
                    "timestamp_readable": result.timestamp_readable,
                    "diagnostics_path": result.diagnostics_path,
                },
            )

        try:
            verification_result = _confirm_publish_via_profile(
                device,
                serial,
                source_name=source_name,
                baseline=profile_baseline,
                expected_handle=str(payload.get("username") or payload.get("account_login") or "").strip(),
                elapsed_since_share_seconds=wait_result.elapsed_seconds,
                share_clicked_at=share_clicked_at_epoch,
                diagnostics_context={"batch_id": batch_id, "job_id": job_id, "account_id": account_id},
                on_update=_profile_verification_update,
            )
        except Exception as exc:
            logger.warning(
                "job_profile_verification_failed: batch_id=%s job_id=%s serial=%s error=%s",
                batch_id,
                job_id,
                serial,
                exc,
            )
            verification_result = ProfileVerificationResult(
                needs_review=True,
                reason_code="publish_profile_navigation_failed",
                detail="Upload принят, но автоматическая проверка профиля не завершилась.",
                publish_phase="verifying_profile",
                baseline_available=bool(profile_baseline.get("available")),
                checked_slots=PUBLISH_PROFILE_CHECK_SLOTS,
                event_kind="needs_review",
                share_clicked_at=share_clicked_at_epoch,
                verification_starts_at=(int(share_clicked_at_epoch + PUBLISH_PROFILE_VERIFY_START_DELAY_SECONDS) if share_clicked_at_epoch else None),
                verification_deadline_at=(int(share_clicked_at_epoch + PUBLISH_PROFILE_VERIFY_SECONDS) if share_clicked_at_epoch else None),
                first_profile_check_at=None,
            )
        _record_timing("verify_profile_seconds", profile_verify_started_at)
        elapsed_total = max(wait_result.elapsed_seconds, int(max(0.0, round(time.monotonic() - share_started_at))))

        if verification_result.needs_review and not verification_result.verified:
            detail = (
                "Instagram принял Reel в публикацию, но профиль не дал надёжного подтверждения. "
                "Нужна ручная проверка."
            )
            if verification_result.detail:
                detail = f"{detail} {verification_result.detail}".strip()
            _set_terminal(
                "needs_review",
                detail,
                account_publish_state="needs_review",
                payload=_telemetry_payload(
                    {
                        "publish_phase": verification_result.publish_phase or "verifying_profile",
                        "accepted_by_instagram": True,
                        "elapsed_seconds": elapsed_total,
                        "last_activity": verification_result.detail or "Профиль не подтвердил новый Reel.",
                        "upload_progress_pct": None,
                        "event_kind": "needs_review",
                        "reason_code": verification_result.reason_code or "publish_profile_inconclusive",
                        "verification_attempt": verification_result.verification_attempt,
                        "verification_window_minutes": max(1, int(PUBLISH_PROFILE_VERIFY_SECONDS // 60)),
                        "checked_slots": verification_result.checked_slots or PUBLISH_PROFILE_CHECK_SLOTS,
                        "matched_slot": verification_result.matched_slot,
                        "matched_age_seconds": verification_result.matched_age_seconds,
                        "reel_fingerprint": verification_result.matched_fingerprint,
                        "reel_signature_text": verification_result.matched_signature_text,
                        "baseline_available": verification_result.baseline_available,
                        "seconds_until_profile_check": verification_result.seconds_until_profile_check,
                        "share_clicked_at": verification_result.share_clicked_at,
                        "verification_starts_at": verification_result.verification_starts_at,
                        "verification_deadline_at": verification_result.verification_deadline_at,
                        "first_profile_check_at": verification_result.first_profile_check_at,
                        "profile_surface_state": verification_result.profile_surface_state,
                        "keyboard_visible": verification_result.keyboard_visible,
                        "comment_sheet_visible": verification_result.comment_sheet_visible,
                        "clips_viewer_visible": verification_result.clips_viewer_visible,
                        "quick_capture_visible": verification_result.quick_capture_visible,
                        "timestamp_readable": verification_result.timestamp_readable,
                        "diagnostics_path": verification_result.diagnostics_path,
                    }
                ),
                serial_value=serial,
            )
            logger.warning(
                "job_publish_needs_review: batch_id=%s job_id=%s account_id=%s reason=%s",
                batch_id,
                job_id,
                account_id,
                verification_result.reason_code or "publish_profile_inconclusive",
            )
        else:
            timings["time_to_publish_confirmed_seconds"] = float(elapsed_total)
            detail = f"Reel опубликован и подтверждён через профиль. Staged файл {source_name} оставлен в batch-папке."
            _set_terminal(
                "published",
                detail,
                payload=_telemetry_payload(
                    {
                        "publish_phase": verification_result.publish_phase or "verifying_profile",
                        "accepted_by_instagram": True,
                        "elapsed_seconds": elapsed_total,
                        "last_activity": verification_result.detail or "Профиль подтвердил новый Reel.",
                        "upload_progress_pct": None,
                        "event_kind": "profile_verified",
                        "reason_code": "publish_profile_verified",
                        "verification_attempt": verification_result.verification_attempt,
                        "verification_window_minutes": max(1, int(PUBLISH_PROFILE_VERIFY_SECONDS // 60)),
                        "checked_slots": verification_result.checked_slots or PUBLISH_PROFILE_CHECK_SLOTS,
                        "matched_slot": verification_result.matched_slot,
                        "matched_age_seconds": verification_result.matched_age_seconds,
                        "reel_fingerprint": verification_result.matched_fingerprint,
                        "reel_signature_text": verification_result.matched_signature_text,
                        "published_at": _estimate_reel_published_at(
                            verification_result,
                            fallback_published_at=(
                                int(share_clicked_at_epoch) + int(wait_result.elapsed_seconds)
                                if share_clicked_at_epoch is not None
                                else int(time.time())
                            ),
                        ),
                        "baseline_available": verification_result.baseline_available,
                        "seconds_until_profile_check": verification_result.seconds_until_profile_check,
                        "share_clicked_at": verification_result.share_clicked_at,
                        "verification_starts_at": verification_result.verification_starts_at,
                        "verification_deadline_at": verification_result.verification_deadline_at,
                        "first_profile_check_at": verification_result.first_profile_check_at,
                        "profile_surface_state": verification_result.profile_surface_state,
                        "keyboard_visible": verification_result.keyboard_visible,
                        "comment_sheet_visible": verification_result.comment_sheet_visible,
                        "clips_viewer_visible": verification_result.clips_viewer_visible,
                        "quick_capture_visible": verification_result.quick_capture_visible,
                        "timestamp_readable": verification_result.timestamp_readable,
                        "diagnostics_path": verification_result.diagnostics_path,
                    }
                ),
                serial_value=serial,
            )
            logger.info("job_publish_timings: batch_id=%s job_id=%s timings=%s", batch_id, job_id, json.dumps(timings, ensure_ascii=False, sort_keys=True))
            logger.info("job_published: batch_id=%s job_id=%s account_id=%s serial=%s file=%s", batch_id, job_id, account_id, serial, source_name)
    except PublishFlowError as exc:
        _capture_publish_diagnostics(
            exc.serial or serial,
            "publish_job_failed",
            batch_id=batch_id,
            job_id=job_id,
            account_id=account_id,
        )
        _set_terminal(
            "failed",
            exc.detail,
            account_publish_state=exc.account_publish_state,
            payload=exc.payload or _telemetry_payload({"reason_code": exc.reason_code}),
            last_file=exc.last_file or source_name,
            serial_value=exc.serial or serial,
        )
        logger.exception("job_publish_failed: batch_id=%s job_id=%s account_id=%s error=%s", batch_id, job_id, account_id, exc.detail)
    except Exception as exc:
        _capture_publish_diagnostics(
            serial,
            "publish_job_error",
            batch_id=batch_id,
            job_id=job_id,
            account_id=account_id,
        )
        detail = str(exc)
        _set_terminal(
            "failed",
            detail,
            payload=_telemetry_payload({"reason_code": "publish_job_unhandled_error"}),
            last_file=source_name,
            serial_value=serial,
        )
        logger.exception("job_publish_unhandled_error: batch_id=%s job_id=%s account_id=%s error=%s", batch_id, job_id, account_id, exc)
    finally:
        try:
            _reset_boundary("Закрываю эмулятор и очищаю Instagram после завершения job.")
        except Exception as cleanup_exc:
            logger.warning(
                "publish_job_boundary_reset_failed: batch_id=%s job_id=%s account_id=%s error=%s",
                batch_id,
                job_id,
                account_id,
                cleanup_exc,
            )
        try:
            _push_publish_job_status(
                job_id,
                terminal_state,
                terminal_detail,
                last_file=terminal_last_file or source_name,
                serial=terminal_serial or serial,
                source_path=source_path,
                account_publish_state=terminal_account_publish_state,
                payload=terminal_payload,
            )
        finally:
            _set_state(
                account_id=None,
                target="publish_batch_job",
                state="idle",
                detail="",
                flow_running=False,
                emulator_serial="",
            )
        if downloaded_source_path:
            _delete_downloaded_publish_job_source(downloaded_source_path)


def _run_collect_reel_metrics(post: dict[str, Any]) -> None:
    post_id = int(post["id"])
    account_id = int(post["account_id"])
    batch_id = int(post.get("publish_batch_id") or 0)
    job_id = int(post.get("publish_job_id") or 0)
    window_key = str(post.get("window_key") or post.get("collection_stage") or "t30m").strip() or "t30m"
    source_name = str(post.get("source_name") or "").strip()
    preferred_serial = str(post.get("instagram_emulator_serial") or "").strip()
    expected_handle = str(post.get("username") or post.get("account_login") or "").strip()
    reel_fingerprint = str(post.get("reel_fingerprint") or "").strip()
    reel_signature_text = str(post.get("reel_signature_text") or "").strip()
    published_at_raw = post.get("published_at")
    serial = preferred_serial

    def _set_metrics_state(state: str, detail: str, *, flow_running: bool = True) -> None:
        prefix = f"[reel metrics post {post_id}] "
        _set_state(
            account_id=account_id,
            target="instagram_collect_reel_metrics",
            state=state,
            detail=prefix + (detail or "").strip(),
            flow_running=flow_running,
            emulator_serial=serial,
        )

    def _push_snapshot(
        status: str,
        *,
        payload: Optional[dict[str, Any]] = None,
        retryable: bool = False,
        error_detail: str = "",
    ) -> None:
        snapshot_payload = dict(payload or {})
        if retryable:
            snapshot_payload["retryable"] = True
        if error_detail:
            snapshot_payload["error_detail"] = error_detail
        _push_reel_metric_snapshot(
            post_id,
            window_key=window_key,
            status=status,
            payload=snapshot_payload,
        )

    diagnostics_path = ""
    try:
        _set_metrics_state("collecting", f"Готовлю сбор метрик для {source_name or f'post #{post_id}'} ({window_key}).")
        login_result = _run_login_flow(
            {
                "account_id": account_id,
                "account_login": str(post.get("account_login") or "").strip(),
                "account_password": str(post.get("account_password") or "").strip(),
                "username": expected_handle,
                "twofa": str(post.get("twofa") or "").strip(),
                "target": "instagram_collect_reel_metrics",
                "instagram_emulator_serial": preferred_serial,
            },
            push_status=False,
            finalize_runtime=False,
            preferred_serial=preferred_serial,
        )
        serial = str(login_result.get("serial") or preferred_serial or "")
        if login_result["state"] != "login_submitted":
            snapshot_status, retryable = _reel_metrics_login_failure_outcome(str(login_result.get("state") or ""))
            diagnostics = _capture_publish_diagnostics(
                serial,
                "reel_metrics_failed" if snapshot_status == "failed" else "reel_metrics_unavailable",
                batch_id=batch_id or None,
                job_id=job_id or None,
                account_id=account_id,
            )
            diagnostics_path = _diagnostics_primary_path(diagnostics)
            detail = f"Сбор метрик остановлен: {login_result['detail']}"
            _push_snapshot(
                snapshot_status,
                payload={
                    "raw_text_json": {
                        "login_state": str(login_result.get("state") or ""),
                        "detail": str(login_result.get("detail") or ""),
                        "mail_challenge": login_result.get("mail_challenge") or {},
                    },
                    "diagnostics_path": diagnostics_path,
                },
                retryable=retryable,
                error_detail=detail,
            )
            _set_metrics_state(snapshot_status, detail, flow_running=False)
            return

        device = login_result["device"]
        _set_metrics_state("collecting", f"Ищу Reel {source_name or f'post #{post_id}'} в профиле аккаунта.")
        center, candidate = _locate_reel_for_metrics(
            device,
            serial,
            reel_fingerprint=reel_fingerprint,
            reel_signature_text=reel_signature_text,
            published_at=int(published_at_raw) if published_at_raw is not None else None,
            expected_handle=expected_handle,
            max_slots=12,
            max_screens=3,
        )
        if center is None or candidate is None:
            diagnostics = _capture_publish_diagnostics(
                serial,
                "reel_metrics_not_found",
                batch_id=batch_id or None,
                job_id=job_id or None,
                account_id=account_id,
            )
            diagnostics_path = _diagnostics_primary_path(diagnostics)
            detail = _reel_metrics_detail(post, "not_found")
            _push_snapshot(
                "not_found",
                payload={
                    "raw_text_json": {
                        "reel_fingerprint": reel_fingerprint,
                        "reel_signature_text": reel_signature_text,
                        "published_at": published_at_raw,
                    },
                    "diagnostics_path": diagnostics_path,
                },
                error_detail=detail,
            )
            _set_metrics_state("not_found", detail, flow_running=False)
            return

        if not _open_profile_reels_tab(device, serial, timeout_seconds=6.0):
            raise RuntimeError("Не удалось вернуть вкладку Reels перед сбором метрик.")
        if not _open_reel_viewer_at_center(device, serial, center):
            raise RuntimeError("Не удалось повторно открыть Reel для сбора метрик.")

        status, metrics, raw_payload = _collect_reel_metrics_from_open_viewer(device, serial)
        detail = _reel_metrics_detail(post, status, metrics)
        if status == "unavailable":
            diagnostics = _capture_publish_diagnostics(
                serial,
                "reel_metrics_unavailable",
                batch_id=batch_id or None,
                job_id=job_id or None,
                account_id=account_id,
            )
            diagnostics_path = _diagnostics_primary_path(diagnostics)
        _push_snapshot(
            status,
            payload={
                **metrics,
                "raw_text_json": {
                    "matched_candidate": _serialize_profile_reel_candidate(candidate),
                    **raw_payload,
                },
                "diagnostics_path": diagnostics_path,
            },
            error_detail=detail if status in {"unavailable", "not_found"} else "",
        )
        _set_metrics_state(status, detail, flow_running=False)
    except Exception as exc:
        snapshot_reported = False
        if not diagnostics_path:
            diagnostics = _capture_publish_diagnostics(
                serial,
                "reel_metrics_failed",
                batch_id=batch_id or None,
                job_id=job_id or None,
                account_id=account_id,
            )
            diagnostics_path = _diagnostics_primary_path(diagnostics)
        try:
            _push_snapshot(
                "failed",
                payload={
                    "raw_text_json": {
                        "reel_fingerprint": reel_fingerprint,
                        "reel_signature_text": reel_signature_text,
                        "published_at": published_at_raw,
                    },
                    "diagnostics_path": diagnostics_path,
                },
                retryable=True,
                error_detail=str(exc),
            )
            snapshot_reported = True
        except Exception as snapshot_exc:
            logger.warning("reel_metrics_snapshot_failed: post_id=%s error=%s", post_id, snapshot_exc)
        _set_metrics_state("failed", str(exc), flow_running=False)
        raise ReelMetricsFlowError(str(exc), snapshot_reported=snapshot_reported, serial=serial) from exc
    finally:
        try:
            if serial and "device" in locals():
                _recover_to_profile_surface(device, serial, timeout_seconds=4.0)
        except Exception:
            pass


def _run_payload_flow(payload: dict[str, Any]) -> None:
    target = str(payload.get("target") or "").strip()
    if target in {"instagram_app_login", "instagram_audit_login"}:
        preferred_serial = str(payload.get("instagram_emulator_serial") or payload.get("emulator_serial") or "").strip()
        _run_login_flow(payload, push_status=True, preferred_serial=preferred_serial)
        return
    if target == "instagram_publish_latest_reel":
        _run_publish_flow(payload)
        return
    raise RuntimeError(f"Unsupported helper target: {target or 'unknown'}")


def _worker_main() -> None:
    while True:
        task = TASK_QUEUE.get()
        try:
            if not task:
                continue
            kind = task.get("kind")
            if kind == "shutdown":
                break
            if kind == "open_payload":
                try:
                    _run_payload_flow(task["payload"])
                except Exception as exc:
                    logger.exception("helper_error: %s", exc)
                    payload = task.get("payload") or {}
                    account_id = int(payload.get("account_id") or 0)
                    if account_id > 0:
                        expected_handle = str(payload.get("username") or payload.get("account_login") or "").strip()
                        target = str(payload.get("target") or "").strip()
                        if target == "instagram_publish_latest_reel":
                            stage = "publishing"
                            detail = str(exc)
                            last_file = ""
                            exc_serial = ""
                            if isinstance(exc, PublishFlowError):
                                stage = exc.stage
                                detail = exc.detail
                                last_file = exc.last_file
                                exc_serial = exc.serial
                            elif latest := _latest_source_video_info():
                                last_file = str(latest.get("name") or "")
                            _push_account_publish_status(
                                account_id,
                                stage,
                                detail,
                                expected_handle,
                                last_file=last_file,
                            )
                        else:
                            exc_serial = exc.serial if isinstance(exc, PublishFlowError) else ""
                            _push_account_launch_status(account_id, "helper_error", str(exc), expected_handle)
                    next_state = "helper_error"
                    next_detail = str(exc)
                    emulator_serial = exc_serial
                    if isinstance(exc, PublishFlowError):
                        next_state = exc.stage
                        next_detail = exc.detail
                        emulator_serial = exc.serial
                    _set_state(state=next_state, detail=next_detail, flow_running=False, emulator_serial=emulator_serial)
        finally:
            TASK_QUEUE.task_done()


def _runner_main() -> None:
    while True:
        with STATE_LOCK:
            flow_running = bool(RUNTIME_STATE.flow_running)
        if flow_running:
            time.sleep(1.0)
            continue
        try:
            job = _lease_publish_job()
        except Exception as exc:
            logger.warning("publish_runner_lease_failed: error=%s", exc)
            time.sleep(PUBLISH_RUNNER_POLL_SECONDS)
            continue
        if job:
            batch_id = int(job.get("batch_id") or 0)
            job_id = int(job.get("id") or 0)
            account_id = int(job.get("account_id") or 0)
            source_name = str(job.get("source_name") or job.get("artifact_filename") or "").strip()
            serial = str(job.get("emulator_serial") or "").strip()
            _set_state(
                account_id=account_id,
                target="publish_batch_job",
                state="leased",
                detail=f"[batch {batch_id} job {job_id}] Runner взял job {source_name}.",
                flow_running=True,
                emulator_serial=serial,
            )
            try:
                _run_publish_job(job)
            except Exception as exc:
                logger.exception("publish_runner_job_failed: batch_id=%s job_id=%s error=%s", batch_id, job_id, exc)
                _push_publish_job_status(
                    job_id,
                    "failed",
                    str(exc),
                    last_file=source_name,
                    serial=serial,
                    source_path=str(job.get("source_path") or ""),
                )
                _set_state(
                    account_id=account_id,
                    target="publish_batch_job",
                    state="failed",
                    detail=f"[batch {batch_id} job {job_id}] {exc}",
                    flow_running=False,
                    emulator_serial=serial,
                )
            time.sleep(1.0)
            continue

        try:
            post = _lease_reel_metric_post()
        except Exception as exc:
            logger.warning("reel_metrics_lease_failed: error=%s", exc)
            time.sleep(PUBLISH_RUNNER_POLL_SECONDS)
            continue
        if not post:
            time.sleep(PUBLISH_RUNNER_POLL_SECONDS)
            continue

        post_id = int(post.get("id") or 0)
        account_id = int(post.get("account_id") or 0)
        source_name = str(post.get("source_name") or "").strip()
        serial = str(post.get("instagram_emulator_serial") or "").strip()
        window_key = str(post.get("window_key") or post.get("collection_stage") or "t30m").strip()
        _set_state(
            account_id=account_id,
            target="instagram_collect_reel_metrics",
            state="leased",
            detail=f"[reel metrics post {post_id}] Runner взял окно {window_key} для {source_name or 'reel'}.",
            flow_running=True,
            emulator_serial=serial,
        )
        try:
            _run_collect_reel_metrics(post)
        except Exception as exc:
            logger.exception("reel_metrics_collect_failed: post_id=%s error=%s", post_id, exc)
            snapshot_reported = isinstance(exc, ReelMetricsFlowError) and bool(exc.snapshot_reported)
            if not snapshot_reported:
                try:
                    _push_reel_metric_snapshot(
                        post_id,
                        window_key=window_key,
                        status="failed",
                        payload={
                            "retryable": True,
                            "error_detail": str(exc),
                        },
                    )
                except Exception as snapshot_exc:
                    logger.warning("reel_metrics_collect_failed_snapshot_push: post_id=%s error=%s", post_id, snapshot_exc)
            _set_state(
                account_id=account_id,
                target="instagram_collect_reel_metrics",
                state="failed",
                detail=f"[reel metrics post {post_id}] {exc}",
                flow_running=False,
                emulator_serial=getattr(exc, "serial", "") or serial,
            )
        time.sleep(1.0)


def _ensure_worker_thread() -> None:
    global WORKER_THREAD
    with STATE_LOCK:
        if WORKER_THREAD is not None and WORKER_THREAD.is_alive():
            return
        WORKER_THREAD = threading.Thread(target=_worker_main, daemon=True, name="instagram-app-helper-worker")
        WORKER_THREAD.start()


def _ensure_runner_thread() -> None:
    global RUNNER_THREAD
    if not PUBLISH_RUNNER_ENABLED:
        return
    with STATE_LOCK:
        if RUNNER_THREAD is not None and RUNNER_THREAD.is_alive():
            return
        RUNNER_THREAD = threading.Thread(target=_runner_main, daemon=True, name="instagram-app-helper-runner")
        RUNNER_THREAD.start()


def _request_shutdown() -> None:
    try:
        TASK_QUEUE.put_nowait({"kind": "shutdown"})
    except Exception:
        pass


atexit.register(_request_shutdown)

@app.get("/health")
def health() -> JSONResponse:
    snapshot = _state_snapshot()
    running = _list_running_emulators()
    anr_windows = _system_anr_windows(running[0]) if running else []
    return JSONResponse(
        {
            "ok": True,
            "bind": f"{HELPER_HOST}:{HELPER_PORT_INT}",
            "base_url": SLEZHKA_ADMIN_BASE_URL,
            "log_file": str(LOG_FILE),
            "adb_path": _resolve_adb_path(),
            "emulator_path": _resolve_emulator_path(),
            "avd_name": ANDROID_AVD_NAME,
            "adbutils_ready": adbutils is not None,
            "uiautomator2_ready": u2 is not None,
            "pyotp_ready": pyotp is not None,
            "publish_runner_enabled": PUBLISH_RUNNER_ENABLED,
            "publish_runner_name": PUBLISH_RUNNER_NAME,
            "running_emulators": running,
            "anr_windows": anr_windows,
            "source_dir": INSTAGRAM_PUBLISH_SOURCE_DIR,
            "publish_runner_downloads_dir": PUBLISH_RUNNER_DOWNLOADS_DIR,
            "latest_video": _latest_source_video_info(),
            "state": snapshot,
        }
    )


@app.get("/api/helper/emulators")
def helper_emulators(_: None = Depends(require_helper_api_key)) -> JSONResponse:
    snapshot = _state_snapshot()
    running = _list_running_emulators()
    configured = sorted(_serial_to_avd_map().keys())
    if not configured and ANDROID_AVD_NAME:
        configured = ["default"]
    available = sorted({*running, *configured})
    return JSONResponse(
        {
            "ok": True,
            "running_serials": running,
            "configured_serials": configured,
            "available_serials": available,
            "serial_to_avd_map": _serial_to_avd_map(),
            "state": snapshot,
        }
    )


@app.post("/api/helper/launch-ticket")
def helper_launch_ticket(payload: dict[str, Any] = Body(...), _: None = Depends(require_helper_api_key)) -> JSONResponse:
    try:
        _preflight()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    ticket = str(payload.get("ticket") or "").strip()
    if len(ticket) < 8:
        raise HTTPException(status_code=400, detail="ticket is required")
    target_candidates = ["instagram_publish_latest_reel", "instagram_audit_login", "instagram_app_login"]
    fetched_payload = None
    last_error: Optional[Exception] = None
    for target in target_candidates:
        try:
            fetched_payload = _fetch_ticket_payload(ticket, target)
            break
        except Exception as exc:
            last_error = exc
    if fetched_payload is None:
        raise HTTPException(status_code=404, detail=str(last_error or "Ticket not found"))
    target = str(fetched_payload.get("target") or "").strip()
    if target not in {"instagram_app_login", "instagram_audit_login", "instagram_publish_latest_reel"}:
        raise HTTPException(status_code=400, detail=f"Unsupported target: {target or 'unknown'}")
    _ensure_worker_thread()
    with STATE_LOCK:
        if RUNTIME_STATE.flow_running:
            raise HTTPException(status_code=409, detail="Helper is busy")
        _set_state(
            account_id=int(fetched_payload["account_id"]),
            target=target,
            state="queued",
            detail="Задача поставлена в очередь. Helper начинает обработку.",
            flow_running=True,
            emulator_serial=str(fetched_payload.get("instagram_emulator_serial") or fetched_payload.get("emulator_serial") or "").strip(),
        )
    TASK_QUEUE.put({"kind": "open_payload", "payload": fetched_payload})
    return JSONResponse(
        {
            "ok": True,
            "target": target,
            "account_id": int(fetched_payload["account_id"]),
            "handle": str(fetched_payload.get("username") or fetched_payload.get("account_login") or "").strip(),
        }
    )


@app.get("/publish-source/latest")
def publish_source_latest() -> JSONResponse:
    latest = _latest_source_video_info()
    if latest is None:
        return JSONResponse(
            {
                "ok": True,
                "source_dir": INSTAGRAM_PUBLISH_SOURCE_DIR,
                "latest_video": None,
                "status_text": "Локальная папка доступна, но подходящих .mp4/.mov файлов пока нет.",
            }
        )
    return JSONResponse(
        {
            "ok": True,
            "source_dir": INSTAGRAM_PUBLISH_SOURCE_DIR,
            "latest_video": latest,
            "status_text": f"Helper видит файл {latest['name']} и готов использовать его для публикации.",
        }
    )


@app.get("/open", response_class=HTMLResponse)
def open_ticket(ticket: str = Query(..., min_length=8)) -> HTMLResponse:
    try:
        _preflight()
    except Exception as exc:
        logger.exception("Preflight failed: %s", exc)
        return _render_status_page(
            "Instagram app helper не готов",
            "Локальная среда Android helper пока не настроена.",
            str(exc),
        )

    try:
        target_candidates = ["instagram_publish_latest_reel", "instagram_audit_login", "instagram_app_login"]
        payload = None
        last_error: Optional[Exception] = None
        for target in target_candidates:
            try:
                payload = _fetch_ticket_payload(ticket, target)
                break
            except Exception as exc:
                last_error = exc
        if payload is None:
            raise last_error or RuntimeError("Ticket not found")
    except Exception as exc:
        logger.exception("Failed to fetch launch ticket %s: %s", ticket, exc)
        return _render_status_page(
            "Не удалось запустить Instagram app helper",
            "Helper не смог получить данные аккаунта.",
            str(exc),
        )

    target = str(payload.get("target") or "").strip()
    if target not in {"instagram_app_login", "instagram_audit_login", "instagram_publish_latest_reel"}:
        return _render_status_page(
            "Неверный тип helper-ticket",
            "Этот helper принимает только Instagram login/publish ticket.",
            f"Получен target: {payload.get('target')!r}",
        )

    _ensure_worker_thread()
    account_id = int(payload["account_id"])
    with STATE_LOCK:
        if RUNTIME_STATE.flow_running:
            return _render_status_page(
                "Instagram app уже открывается",
                "Сейчас уже выполняется один запуск helper. Дождись, пока эмулятор откроется.",
            )
        _set_state(
            account_id=account_id,
            target=target,
            state="queued",
            detail="Задача поставлена в очередь. Откроется Android emulator и Instagram app.",
            flow_running=True,
        )

    TASK_QUEUE.put({"kind": "open_payload", "payload": payload})
    if target == "instagram_publish_latest_reel":
        return _render_status_page(
            "Запускаю публикацию Reel",
            "Локальный helper проверит папку на этом Mac, импортирует самый новый ролик в эмулятор и попробует опубликовать его как Reel.",
            "После подтверждённого успеха локальный исходный файл будет удалён. Если helper увидит проблему, файл останется на месте.",
        )
    if target == "instagram_audit_login":
        return _render_status_page(
            "Запускаю Instagram audit helper",
            "Локальный helper откроет Android emulator, Instagram app и проверит, проходит ли вход автоматически.",
            "Если Instagram попросит 2FA, challenge или код с почты, helper зафиксирует это в админке и оставит диагностический снимок экрана.",
        )
    return _render_status_page(
        "Запускаю Instagram app helper",
        "Локальный helper откроет Android emulator, Instagram app, введёт логин/пароль и нажмёт вход.",
        "Если Instagram ещё не установлен, helper откроет страницу приложения в Google Play. После этого helper остановится, а эмулятор останется открытым для ручных действий.",
    )
