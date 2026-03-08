import atexit
import json
import logging
import os
import queue
import re
import shutil
import subprocess
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse

try:
    import pyotp
except Exception as exc:  # pragma: no cover - import-time fallback
    pyotp = None
    PYOTP_IMPORT_ERROR = exc
else:
    PYOTP_IMPORT_ERROR = None

load_dotenv()

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

HELPER_BIND = (os.getenv("INSTAGRAM_APP_HELPER_BIND", "127.0.0.1:17374") or "127.0.0.1:17374").strip()
HELPER_HOST, _, HELPER_PORT = HELPER_BIND.partition(":")
HELPER_PORT_INT = int(HELPER_PORT or "17374")
SLEZHKA_ADMIN_BASE_URL = (
    os.getenv("SLEZHKA_ADMIN_BASE_URL", "http://4abbf189760e.vps.myjino.ru/slezhka")
    or "http://4abbf189760e.vps.myjino.ru/slezhka"
).strip().rstrip("/")
HELPER_API_KEY = (os.getenv("HELPER_API_KEY", "") or "").strip()
ANDROID_AVD_NAME = (os.getenv("ANDROID_AVD_NAME", "") or "").strip()
ADB_PATH_RAW = (os.getenv("ADB_PATH", "") or "").strip()
EMULATOR_PATH_RAW = (os.getenv("EMULATOR_PATH", "") or "").strip()
INSTAGRAM_PACKAGE = (os.getenv("INSTAGRAM_ANDROID_PACKAGE", "com.instagram.android") or "com.instagram.android").strip()
INSTAGRAM_PUBLISH_SOURCE_DIR = (
    os.getenv("INSTAGRAM_PUBLISH_SOURCE_DIR", "/Users/daniildatlov/Desktop/видео ауграм ")
    or "/Users/daniildatlov/Desktop/видео ауграм "
)
INSTAGRAM_PUBLISH_MEDIA_DIR = (
    os.getenv("INSTAGRAM_PUBLISH_MEDIA_DIR", "/sdcard/Movies/Videoogram")
    or "/sdcard/Movies/Videoogram"
).strip()
PUBLISH_VIDEO_EXTENSIONS = {".mp4", ".mov"}
EMULATOR_STABILIZE_SECONDS = int(os.getenv("INSTAGRAM_APP_EMULATOR_STABILIZE_SECONDS", "12"))
USE_EMULATOR_SNAPSHOTS = (os.getenv("INSTAGRAM_APP_USE_SNAPSHOTS", "0") or "0").strip().lower() in {
    "1",
    "true",
    "yes",
    "on",
}
PUBLISH_RUNNER_ENABLED = (os.getenv("PUBLISH_RUNNER_ENABLED", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
PUBLISH_RUNNER_POLL_SECONDS = max(3, int(os.getenv("PUBLISH_RUNNER_POLL_SECONDS", "10")))
PUBLISH_RUNNER_NAME = (os.getenv("PUBLISH_RUNNER_NAME", "instagram-app-helper-runner") or "instagram-app-helper-runner").strip()
PUBLISH_RUNNER_API_KEY = (os.getenv("PUBLISH_RUNNER_API_KEY", HELPER_API_KEY) or HELPER_API_KEY).strip()
SERIAL_TO_AVD_MAP_RAW = (os.getenv("INSTAGRAM_RUNNER_SERIAL_TO_AVD_JSON", "") or "").strip()

LOG_DIR = Path.home() / "Library" / "Logs" / "SlezhkaHelper"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOG_DIR / "instagram-app-helper.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger("instagram_app_helper")

app = FastAPI(title="Slezhka Instagram App Helper")
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


class PublishFlowError(RuntimeError):
    def __init__(self, stage: str, detail: str, *, last_file: str = "", serial: str = "") -> None:
        super().__init__(detail)
        self.stage = (stage or "publishing").strip()
        self.detail = (detail or "Instagram publish flow failed.").strip()
        self.last_file = (last_file or "").strip()
        self.serial = (serial or "").strip()


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
    response = requests.get(
        _build_ticket_url(ticket, target),
        headers={"X-Helper-Api-Key": HELPER_API_KEY},
        timeout=25,
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


def _push_account_launch_status(account_id: int, status: str, detail: str, handle: str) -> None:
    if not HELPER_API_KEY:
        return
    try:
        response = requests.post(
            f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/accounts/{int(account_id)}/instagram-status",
            headers={"X-Helper-Api-Key": HELPER_API_KEY},
            json={
                "state": (status or "").strip(),
                "detail": (detail or "").strip(),
                "handle": (handle or "").strip(),
            },
            timeout=25,
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
) -> None:
    if not HELPER_API_KEY:
        return
    try:
        response = requests.post(
            f"{SLEZHKA_ADMIN_BASE_URL}/api/helper/accounts/{int(account_id)}/instagram-publish-status",
            headers={"X-Helper-Api-Key": HELPER_API_KEY},
            json={
                "state": (status or "").strip(),
                "detail": (detail or "").strip(),
                "handle": (handle or "").strip(),
                "last_file": (last_file or "").strip(),
            },
            timeout=25,
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


def _lease_publish_job() -> Optional[dict[str, Any]]:
    if not PUBLISH_RUNNER_API_KEY:
        raise RuntimeError("PUBLISH_RUNNER_API_KEY is not configured")
    response = requests.post(
        f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/publishing/jobs/lease",
        headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
        json={"runner_name": PUBLISH_RUNNER_NAME},
        timeout=25,
    )
    if response.status_code == 204:
        return None
    response.raise_for_status()
    payload = response.json()
    job = payload.get("job")
    return job if isinstance(job, dict) else None


def _push_publish_job_status(
    job_id: int,
    state: str,
    detail: str,
    *,
    last_file: str = "",
    serial: str = "",
    source_path: str = "",
) -> None:
    if not PUBLISH_RUNNER_API_KEY:
        return
    try:
        response = requests.post(
            f"{SLEZHKA_ADMIN_BASE_URL}/api/internal/publishing/jobs/{int(job_id)}/status",
            headers={"X-Runner-Api-Key": PUBLISH_RUNNER_API_KEY},
            json={
                "state": (state or "").strip(),
                "detail": (detail or "").strip(),
                "last_file": (last_file or "").strip(),
                "runner_name": PUBLISH_RUNNER_NAME,
                "emulator_serial": (serial or "").strip(),
                "source_path": (source_path or "").strip(),
            },
            timeout=25,
        )
        response.raise_for_status()
    except Exception as exc:
        logger.warning("publish_job_status_push_failed: job_id=%s state=%s error=%s", job_id, state, exc)


def _source_dir_path() -> Path:
    return Path(INSTAGRAM_PUBLISH_SOURCE_DIR).expanduser()


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


def _preflight() -> None:
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
    command: list[str]
    if preferred:
        avd_name = _serial_to_avd_map().get(preferred, "").strip()
        if not avd_name:
            raise RuntimeError(f"Эмулятор {preferred} не запущен и для него не настроен AVD mapping.")
        port = _serial_emulator_port(preferred)
        if port is None:
            raise RuntimeError(f"Не удалось определить порт из serial {preferred}.")
        command = [
            emulator_path,
            "-avd",
            avd_name,
            "-port",
            str(port),
            "-netdelay",
            "none",
            "-netspeed",
            "full",
            "-gpu",
            "swiftshader_indirect",
        ]
    else:
        if not avd_name:
            raise RuntimeError("ANDROID_AVD_NAME is not configured.")
        command = [
            emulator_path,
            "-avd",
            avd_name,
            "-netdelay",
            "none",
            "-netspeed",
            "full",
            "-gpu",
            "swiftshader_indirect",
        ]

    avds = _list_avds()
    if avd_name not in avds:
        raise RuntimeError(f"AVD '{avd_name}' not found. Available: {', '.join(avds) or 'none'}")

    _set_state(state="emulator_starting", detail=f"Запускаю AVD: {avd_name}", emulator_serial=preferred)
    logger.info("emulator_starting: avd=%s preferred_serial=%s", avd_name, preferred or "-")
    previous = set(existing)
    process_key = preferred or avd_name
    process = EMULATOR_PROCESSES.get(process_key)
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
    _wait_for_boot(serial)
    _stabilize_emulator(serial)
    _set_state(state="emulator_ready", emulator_serial=serial, detail=f"Эмулятор готов: {serial}")
    logger.info("emulator_ready: serial=%s reused=false preferred=%s", serial, bool(preferred))
    return serial


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
            [adb_path, "-s", serial, "shell", "am", "start", "-n", launch_activity],
            timeout=25,
            check=False,
        )
        output = ((result.stdout or "") + "\n" + (result.stderr or "")).strip().lower()
        if result.returncode != 0 or "error:" in output or "exception" in output:
            raise RuntimeError("Failed to start Instagram app activity.")
    else:
        _run(
            [adb_path, "-s", serial, "shell", "monkey", "-p", INSTAGRAM_PACKAGE, "-c", "android.intent.category.LAUNCHER", "1"],
            timeout=25,
            check=False,
        )
    if not _wait_for_instagram_foreground(serial, timeout_seconds=12.0):
        raise RuntimeError("Instagram app did not come to foreground.")
    _set_state(state="app_opened", detail="Instagram app открыт в эмуляторе.")
    logger.info("app_opened: serial=%s package=%s", serial, INSTAGRAM_PACKAGE)


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


def _obj_selected(obj: Any) -> bool:
    try:
        info = getattr(obj, "info", {}) or {}
    except Exception:
        return False
    return bool(info.get("selected") or info.get("checked"))


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
    login_selectors = [
        {"textMatches": "(?i)(log in|login|войти)"},
        {"descriptionMatches": "(?i)(log in|login|войти)"},
        {"textMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"descriptionMatches": "(?i)(i already have an account|already have an account\\??)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*log.?in.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*login.*button.*"},
    ]
    if _ig_click_first(device, login_selectors, timeout_seconds=1.0):
        logger.info("login_entry_tapped: serial=%s action=button", serial)
        time.sleep(1.8)
        return True

    landing_markers = [
        {"textMatches": "(?i)(create new account|sign up|from meta|already have an account)"},
        {"descriptionMatches": "(?i)(create new account|sign up|from meta|already have an account)"},
    ]
    if _ig_find_first(device, landing_markers, timeout_seconds=0.8) is not None:
        width, height = _device_display_size(device)
        _adb_tap(serial, width // 2, max(240, height - 260))
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
        {"resourceId": f"{INSTAGRAM_PACKAGE}:id/igds_headline_primary_action_button"},
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
    return re.sub(r"[^A-Z2-7]", "", (raw_value or "").upper())


def _maybe_submit_twofa(device: Any, serial: str, twofa_secret: str) -> bool:
    secret = _normalize_twofa_secret(twofa_secret)
    if not secret or pyotp is None:
        return False

    prompt_markers = [
        {"textMatches": "(?i)(two-factor|two factor|security code|confirmation code|enter code|login code|authentication app)"},
        {"textMatches": "(?i)(код безопасности|код подтверждения|двухфактор)"},
    ]
    if _find_first(device, prompt_markers, timeout_seconds=2.0) is None:
        return False

    try:
        code = pyotp.TOTP(secret).now()
    except Exception as exc:
        logger.warning("twofa_code_generation_failed: serial=%s error=%s", serial, exc)
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

    try:
        field.click()
    except Exception:
        pass
    field.set_text(code)
    try:
        device.press("back")
    except Exception:
        pass

    confirm_selectors = [
        {"textMatches": "(?i)(confirm|continue|next|done|submit|войти|подтвердить|продолжить|далее|готово)"},
        {"descriptionMatches": "(?i)(confirm|continue|next|done|submit|войти|подтвердить|продолжить|далее|готово)"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*confirm.*"},
        {"resourceIdMatches": f"{INSTAGRAM_PACKAGE}:id/.*continue.*"},
    ]
    if not _click_first(device, confirm_selectors, timeout_seconds=3.0):
        _adb_shell(serial, "input", "keyevent", "66", timeout=10, check=False)
    logger.info("twofa_submitted: serial=%s", serial)
    time.sleep(3.0)
    return True


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
        if twofa_secret and pyotp is None:
            detail = f"Instagram запросил 2FA, но pyotp недоступен: {PYOTP_IMPORT_ERROR}."
        elif not twofa_secret:
            detail = "Instagram запросил 2FA. В account.twofa нет секрета, поэтому заверши шаг вручную."
        return ("manual_2fa_required", detail)

    challenge = _ig_find_first(
        device,
        [
            {"textMatches": "(?i)(confirm it's you|help us confirm|suspicious login|approve login|challenge|secure your account)"},
            {"textMatches": "(?i)(подтверд.*это вы|подозрительн.*вход|подтверд.*вход)"},
        ],
        timeout_seconds=2.5,
    )
    if challenge is not None:
        return ("challenge_required", "Instagram запросил challenge или подтверждение входа. Продолжай вручную.")

    if _signed_out_surface_visible(device, serial):
        return ("login_failed", "Instagram вернулся в signed-out экран вместо домашней ленты.")

    if _handle_post_login_prompts(device, serial, timeout_seconds=18.0):
        if _signed_out_surface_visible(device, serial):
            return ("login_failed", "Instagram вернулся в signed-out экран вместо домашней ленты.")
        return ("login_submitted", "Вход выполнен. Instagram открыт на основном экране.")

    if _login_form_visible(device):
        return ("invalid_password", "Instagram вернулся на экран входа. Проверь логин, пароль или состояние сети.")

    if _signed_out_surface_visible(device, serial):
        return ("login_failed", "Instagram не завершил вход и остался в signed-out flow.")
    if not _instagram_is_foreground(serial):
        return ("login_failed", "Instagram ушёл из foreground после отправки логина.")
    return ("manual_step_required", "Логин отправлен, но Instagram не подтвердил домашний экран автоматически.")


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
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.8)
        _allow_media_permissions(device, serial, timeout_seconds=1.2)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=1.0)
        if _create_surface_visible(device):
            return True
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


def _wait_for_publish_success(device: Any, serial: str, timeout_seconds: float = 45.0) -> bool:
    explicit_success_markers = [
        {"descriptionMatches": "(?i)view insights"},
        {"descriptionMatches": "(?i)boost reel"},
        {"textMatches": "(?i)view insights"},
        {"textMatches": "(?i)boost reel"},
        {"textMatches": "(?i)(your reel has been shared|shared your reel|your reel is live|your reel has been posted)"},
        {"textMatches": "(?i)(опубликовано|поделились)"},
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
    deadline = time.time() + timeout_seconds
    stable_surface_hits = 0
    share_clicked_at = time.time()
    while time.time() < deadline:
        _dismiss_system_dialogs(device, serial, timeout_seconds=0.5)
        _dismiss_instagram_interstitials(device, serial, timeout_seconds=0.5)
        _handle_publish_confirmation_prompt(device, serial, timeout_seconds=1.2)
        if not _instagram_is_foreground(serial):
            time.sleep(1.0)
            continue
        if _ig_find_first(device, blocking_markers, timeout_seconds=0.6) is not None:
            return False
        if _ig_find_first(device, explicit_success_markers, timeout_seconds=0.8) is not None:
            return True
        if _home_feed_visible(device) and _ig_find_first(device, composer_markers, timeout_seconds=0.4) is None:
            stable_surface_hits += 1
            if stable_surface_hits >= 4 and (time.time() - share_clicked_at) >= 8.0:
                return True
        else:
            stable_surface_hits = 0
        time.sleep(1.0)
    return False


def _run_login_flow(
    payload: dict[str, Any],
    *,
    push_status: bool = True,
    finalize_runtime: bool = True,
    preferred_serial: str = "",
) -> dict[str, Any]:
    account_id = int(payload["account_id"])
    login = str(payload.get("account_login") or "").strip()
    password = str(payload.get("account_password") or "").strip()
    expected_handle = str(payload.get("username") or payload.get("account_login") or "").strip()
    if not login or not password:
        raise RuntimeError("Missing account_login or account_password.")

    serial = _ensure_emulator_ready(preferred_serial=preferred_serial)
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
    _clear_instagram_data(serial)
    next_state = "helper_error"
    next_detail = "Instagram login did not start."
    device = None
    for attempt in range(2):
        _launch_instagram_app(serial)
        device = _connect_ui(serial)
        _dismiss_system_dialogs(device, serial, timeout_seconds=2.0)
        _fill_credentials_and_submit(device, serial, login, password)
        next_state, next_detail = _detect_post_login_state(device, serial, str(payload.get("twofa") or ""))
        if next_state != "login_failed" or attempt == 1:
            break
        logger.info("login_retry: account_id=%s serial=%s attempt=%s", account_id, serial, attempt + 2)
        _dismiss_system_dialogs(device, serial, timeout_seconds=1.5)
        _adb_shell(serial, "input", "keyevent", "4", timeout=10, check=False)
        time.sleep(1.5)
    if push_status:
        _push_account_launch_status(account_id, next_state, next_detail, expected_handle)
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
        "handle": expected_handle,
    }


def _run_publish_flow(payload: dict[str, Any]) -> None:
    account_id = int(payload["account_id"])
    expected_handle = str(payload.get("username") or payload.get("account_login") or "").strip()
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
            _push_account_publish_status(
                account_id,
                "login_required",
                detail,
                expected_handle,
                last_file=source_name,
            )
            _set_state(
                account_id=account_id,
                target="instagram_publish_latest_reel",
                state="login_required",
                detail=detail,
                flow_running=False,
                emulator_serial=serial,
            )
            return

        device = login_result["device"]
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
        _share_reel(device, serial)
        if not _wait_for_publish_success(device, serial, timeout_seconds=60.0):
            raise PublishFlowError(
                current_stage,
                f"Instagram не подтвердил публикацию Reel для файла {source_name}.",
                last_file=source_name,
                serial=serial,
            )

        if delete_source_on_success:
            _delete_local_source_video(source_path)
            detail = f"Reel опубликован. Исходный файл {source_name} удалён из локальной папки."
        else:
            detail = f"Reel опубликован. Staged файл {source_name} оставлен на месте."
        _push_account_publish_status(
            account_id,
            "published",
            detail,
            expected_handle,
            last_file=source_name,
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
    except PublishFlowError:
        raise
    except Exception as exc:
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
    payload = {
        "account_id": account_id,
        "account_login": str(job.get("account_login") or "").strip(),
        "account_password": str(job.get("account_password") or "").strip(),
        "username": str(job.get("username") or "").strip(),
        "twofa": str(job.get("twofa") or "").strip(),
        "target": "publish_batch_job",
    }

    def _job_progress(stage: str, detail: str, *, flow_running: bool = True) -> None:
        _push_publish_job_status(
            job_id,
            stage,
            detail,
            last_file=source_name,
            serial=serial,
            source_path=source_path,
        )
        _set_state(
            account_id=account_id,
            target="publish_batch_job",
            state=stage,
            detail=f"[batch {batch_id} job {job_id}] {detail}",
            flow_running=flow_running,
            emulator_serial=serial,
        )

    try:
        source_info = _source_video_info_from_path(source_path)
        source_name = str(source_info["name"])
        _job_progress("preparing", f"Batch job #{job_id}: готовлю Instagram app для {source_name}.")
        login_result = _run_login_flow(
            payload,
            push_status=False,
            finalize_runtime=False,
            preferred_serial=preferred_serial,
        )
        serial = str(login_result.get("serial") or "")
        if login_result["state"] != "login_submitted":
            detail = f"Публикация остановлена: {login_result['detail']}"
            _push_publish_job_status(
                job_id,
                "failed",
                detail,
                last_file=source_name,
                serial=serial,
                source_path=source_path,
            )
            _set_state(
                account_id=account_id,
                target="publish_batch_job",
                state="failed",
                detail=f"[batch {batch_id} job {job_id}] {detail}",
                flow_running=False,
                emulator_serial=serial,
            )
            return

        device = login_result["device"]
        current_stage = "importing_media"
        _job_progress(current_stage, f"Импортирую {source_name} в эмулятор.")
        imported_path = _import_video_into_emulator(serial, source_path)
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
        _open_reel_creation_flow(device, serial)

        current_stage = "selecting_media"
        _job_progress(current_stage, f"Выбираю видео {source_name}.")
        _select_reel_media(device, serial)
        _advance_reel_next(device, serial, steps=2)

        current_stage = "publishing"
        _job_progress(current_stage, f"Публикую Reel {source_name}.")
        _share_reel(device, serial)
        if not _wait_for_publish_success(device, serial, timeout_seconds=60.0):
            raise PublishFlowError(
                current_stage,
                f"Instagram не подтвердил публикацию Reel для файла {source_name}.",
                last_file=source_name,
                serial=serial,
            )

        detail = f"Reel опубликован. Staged файл {source_name} оставлен в batch-папке."
        _push_publish_job_status(
            job_id,
            "published",
            detail,
            last_file=source_name,
            serial=serial,
            source_path=source_path,
        )
        _set_state(
            account_id=account_id,
            target="publish_batch_job",
            state="published",
            detail=f"[batch {batch_id} job {job_id}] {detail}",
            flow_running=False,
            emulator_serial=serial,
        )
        logger.info("job_published: batch_id=%s job_id=%s account_id=%s serial=%s file=%s", batch_id, job_id, account_id, serial, source_name)
    except PublishFlowError as exc:
        _push_publish_job_status(
            job_id,
            "failed",
            exc.detail,
            last_file=exc.last_file or source_name,
            serial=exc.serial or serial,
            source_path=source_path,
        )
        _set_state(
            account_id=account_id,
            target="publish_batch_job",
            state="failed",
            detail=f"[batch {batch_id} job {job_id}] {exc.detail}",
            flow_running=False,
            emulator_serial=exc.serial or serial,
        )
        logger.exception("job_publish_failed: batch_id=%s job_id=%s account_id=%s error=%s", batch_id, job_id, account_id, exc.detail)
    except Exception as exc:
        detail = str(exc)
        _push_publish_job_status(
            job_id,
            "failed",
            detail,
            last_file=source_name,
            serial=serial,
            source_path=source_path,
        )
        _set_state(
            account_id=account_id,
            target="publish_batch_job",
            state="failed",
            detail=f"[batch {batch_id} job {job_id}] {detail}",
            flow_running=False,
            emulator_serial=serial,
        )
        logger.exception("job_publish_unhandled_error: batch_id=%s job_id=%s account_id=%s error=%s", batch_id, job_id, account_id, exc)


def _run_payload_flow(payload: dict[str, Any]) -> None:
    target = str(payload.get("target") or "").strip()
    if target == "instagram_app_login":
        _run_login_flow(payload, push_status=True)
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
                            serial = ""
                            if isinstance(exc, PublishFlowError):
                                stage = exc.stage
                                detail = exc.detail
                                last_file = exc.last_file
                                serial = exc.serial
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
                            _push_account_launch_status(account_id, "helper_error", str(exc), expected_handle)
                    next_state = "helper_error"
                    next_detail = str(exc)
                    emulator_serial = ""
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
        if not job:
            time.sleep(PUBLISH_RUNNER_POLL_SECONDS)
            continue
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


@app.on_event("startup")
def _startup_helper() -> None:
    _ensure_worker_thread()
    _ensure_runner_thread()


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
            "latest_video": _latest_source_video_info(),
            "state": snapshot,
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
        target_candidates = ["instagram_publish_latest_reel", "instagram_app_login"]
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
    if target not in {"instagram_app_login", "instagram_publish_latest_reel"}:
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
    return _render_status_page(
        "Запускаю Instagram app helper",
        "Локальный helper откроет Android emulator, Instagram app, введёт логин/пароль и нажмёт вход.",
        "Если Instagram ещё не установлен, helper откроет страницу приложения в Google Play. После этого helper остановится, а эмулятор останется открытым для ручных действий.",
    )
