#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import shlex
import subprocess
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import requests
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import instagram_app_helper as ig_helper


ACTIVE_PUBLISH_STATES = {
    "queued",
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
}
ACTIVE_BATCH_ACCOUNT_STATES = {
    "queued_for_generation",
    "generating",
    "queued_for_publish",
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
}
TERMINAL_ACCOUNT_STATES = {"published", "needs_review", "failed", "canceled", "generation_failed"}
LOG_EVENT_MARKERS = {
    "job_media_imported",
    "wait_publish_uploading_detected",
    "wait_publish_post_share_sheet_detected",
    "wait_publish_success_explicit",
    "profile_verification_schedule",
    "profile_verification_first_check",
    "profile_recovery_step",
    "profile_recovery_success",
    "profile_reels_tab_opened",
    "profile_slot_opened",
    "profile_slot_timestamp_read",
    "profile_slot_timestamp_unreadable",
    "profile_verification_verified",
    "profile_verification_needs_review",
    "job_publish_needs_review",
    "job_published",
}


@dataclass
class RemoteAccountInfo:
    account_id: int
    username: str
    account_type: str
    rotation_state: str
    emulator_serial: str


@dataclass
class ProgressState:
    batch_state: str = ""
    batch_phase: str = ""
    account_state: str = ""
    publish_phase: str = ""
    phase_label: str = ""
    phase_detail: str = ""
    latest_activity_title: str = ""
    latest_activity_detail: str = ""


@dataclass
class CanaryCaptureRecord:
    index: int
    label: str
    reason: str
    created_at: str
    directory: str
    batch_state: str = ""
    account_state: str = ""
    publish_phase: str = ""
    helper_state: str = ""
    helper_detail: str = ""
    matched_age_seconds: Optional[int] = None
    share_clicked_at: Optional[int] = None
    verification_starts_at: Optional[int] = None
    verification_deadline_at: Optional[int] = None
    first_profile_check_at: Optional[int] = None
    recent_log_markers: list[str] = field(default_factory=list)


def _join_url(base_url: str, path: str) -> str:
    base = (base_url or "").rstrip("/") + "/"
    return urljoin(base, path.lstrip("/"))


def _resolve_config_value(cli_value: str, env_key: str, fallback: str = "") -> str:
    value = str(cli_value or "").strip()
    if value:
        return value
    env_value = str(os.getenv(env_key, "") or "").strip()
    if env_value:
        return env_value
    return fallback


def _is_loopback_url(value: str) -> bool:
    try:
        parsed = urlparse((value or "").strip())
    except Exception:
        return False
    host = (parsed.hostname or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _helper_base_url_matches_expected(expected_base_url: str, helper_reported_base_url: str) -> bool:
    expected = (expected_base_url or "").rstrip("/")
    reported = (helper_reported_base_url or "").rstrip("/")
    if not expected or not reported:
        return False
    if reported == expected:
        return True
    if not _is_loopback_url(reported):
        return False
    expected_parsed = urlparse(expected)
    reported_parsed = urlparse(reported)
    expected_path = (expected_parsed.path or "").rstrip("/")
    reported_path = (reported_parsed.path or "").rstrip("/")
    return bool(expected_path) and expected_path == reported_path


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", (value or "").strip())
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug or "capture"


def _now_compact() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _extract_ready_accounts(html: str) -> list[dict[str, str]]:
    pattern = re.compile(
        r'<input(?=[^>]*name="account_ids")(?=[^>]*id="(?P<input_id>[^"]+)")(?=[^>]*value="(?P<account_id>\d+)")[^>]*>'
        r'.*?<label[^>]*for="(?P=input_id)"[^>]*>\s*@(?P<username>[^<]+)\s*</label>',
        re.IGNORECASE | re.DOTALL,
    )
    results: list[dict[str, str]] = []
    for match in pattern.finditer(html or ""):
        results.append(
            {
                "account_id": match.group("account_id").strip(),
                "username": match.group("username").strip(),
            }
        )
    return results


def _parse_batch_id_from_url(url: str) -> Optional[int]:
    match = re.search(r"/publishing/batches/(\d+)", url or "")
    if not match:
        return None
    try:
        return int(match.group(1))
    except Exception:
        return None


def _tail_from_offset(path: Path, offset: int) -> tuple[list[str], int]:
    if not path.exists():
        return [], offset
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        handle.seek(offset)
        data = handle.read()
        new_offset = handle.tell()
    if not data:
        return [], new_offset
    return data.splitlines(), new_offset


def _tail_lines(path: Path, limit: int = 200) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="ignore").splitlines()[-max(1, int(limit)) :]


def _extract_log_markers(lines: list[str]) -> list[str]:
    seen: list[str] = []
    for line in lines:
        for marker in LOG_EVENT_MARKERS:
            if marker in line and marker not in seen:
                seen.append(marker)
    return seen


def _extract_latest_helper_timestamps(lines: list[str]) -> dict[str, Optional[int]]:
    values: dict[str, Optional[int]] = {
        "share_clicked_at": None,
        "verification_starts_at": None,
        "verification_deadline_at": None,
        "first_profile_check_at": None,
        "matched_age_seconds": None,
    }
    for line in lines:
        for key in ("share_clicked_at", "verification_starts_at", "verification_deadline_at", "first_profile_check_at", "matched_age_seconds"):
            match = re.search(rf"{key}=([0-9]+|None)", line)
            if match:
                raw = match.group(1)
                values[key] = int(raw) if raw.isdigit() else None
    return values


def _publish_signature(shared_secret: str, timestamp: str, body: bytes) -> str:
    secret = (shared_secret or "").encode("utf-8")
    signed = timestamp.encode("utf-8") + b"." + body
    return hmac.new(secret, signed, hashlib.sha256).hexdigest()


def _state_signature(snapshot: dict[str, Any], account_id: int) -> ProgressState:
    batch = dict(snapshot.get("batch") or {})
    accounts = list(snapshot.get("accounts") or [])
    account = next((item for item in accounts if int(item.get("id") or 0) == int(account_id)), {})
    latest_activity = dict((snapshot.get("recent_activity") or [{}])[0] or {})
    return ProgressState(
        batch_state=str(batch.get("state") or ""),
        batch_phase=str(batch.get("phase_key") or ""),
        account_state=str(account.get("batch_state") or ""),
        publish_phase=str(account.get("publish_phase") or ""),
        phase_label=str(account.get("phase_label") or ""),
        phase_detail=str(account.get("phase_detail") or ""),
        latest_activity_title=str(latest_activity.get("title") or ""),
        latest_activity_detail=str(latest_activity.get("detail") or ""),
    )


def _should_upload_existing_video(
    *,
    launch_mode: str,
    resuming_existing_batch: bool,
    upload_existing_video_on_resume: bool = False,
) -> bool:
    if str(launch_mode or "").strip() != "existing_video":
        return False
    if resuming_existing_batch and not upload_existing_video_on_resume:
        return False
    return True


def _classify_failure(snapshot: dict[str, Any], helper_health: dict[str, Any], log_lines: list[str]) -> str:
    accounts = list(snapshot.get("accounts") or [])
    account = dict(accounts[0] or {}) if accounts else {}
    batch_state = str((snapshot.get("batch") or {}).get("state") or "").lower()
    account_state = str(account.get("batch_state") or "").lower()
    phase_detail = str(account.get("phase_detail") or "").lower()
    publish_phase = str(account.get("publish_phase") or "").lower()
    helper_state = str((helper_health.get("state") or {}).get("state") or "").lower()
    joined = "\n".join(log_lines).lower()

    if batch_state == "failed_generation" or account_state == "generation_failed":
        return "generation_failed"
    if "publish_profile_navigation_failed" in joined or "не удалось открыть профиль" in phase_detail:
        return "navigation_recovery"
    if "profile_slot_timestamp_unreadable" in joined or "не удалось прочитать время" in phase_detail:
        return "timestamp_read"
    if "profile_reels_tab_opened" not in joined and ("reels" in phase_detail or "вкладку reels" in phase_detail):
        return "reels_tab_open"
    if any(marker in joined for marker in ("profile_slot_opened", "clips_viewer", "comment_sheet", "quick_capture")):
        return "slot_open_or_viewer_normalize"
    if publish_phase in {"waiting_confirmation", "waiting_profile_verification_window", "verifying_profile"}:
        return "publish_timing_or_state"
    if helper_state in {"published", "needs_review"} and not account:
        return "ui_snapshot_only"
    return "publish_timing_or_state"


class AdminClient:
    def __init__(self, base_url: str, username: str, password: str, timeout: int = 25):
        self.base_url = (base_url or "").rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.username = username
        self.password = password

    def _request(self, method: str, path: str, *, retry_budget_seconds: float = 15.0, **kwargs: Any) -> requests.Response:
        last_error: Optional[Exception] = None
        started_at = time.monotonic()
        attempt = 0
        while True:
            attempt += 1
            try:
                response = self.session.request(
                    method.upper(),
                    _join_url(self.base_url, path),
                    timeout=self.timeout,
                    **kwargs,
                )
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                elapsed = max(0.0, time.monotonic() - started_at)
                if elapsed >= max(1.0, float(retry_budget_seconds or 0.0)):
                    raise
                sleep_seconds = min(10.0, max(1.0, float(attempt) * 2.0))
                remaining = max(0.0, float(retry_budget_seconds) - elapsed)
                time.sleep(min(sleep_seconds, remaining or sleep_seconds))
        assert last_error is not None
        raise last_error

    def login(self) -> None:
        response = self._request(
            "POST",
            "/login",
            data={"username": self.username, "password": self.password},
            allow_redirects=True,
        )
        if not response.url.rstrip("/").endswith("/"):
            pass
        if "/login" in response.url and "Неверный логин или пароль" in response.text:
            raise RuntimeError("Admin login failed.")

    def publishing_start_html(self) -> str:
        response = self._request("GET", "/publishing/start")
        return response.text

    def create_batch(self, account_id: int, *, launch_mode: str = "generated") -> int:
        response = self._request(
            "POST",
            "/publishing/batches",
            data=[("account_ids", str(int(account_id))), ("launch_mode", str(launch_mode or "generated"))],
            allow_redirects=True,
        )
        batch_id = _parse_batch_id_from_url(response.url)
        if batch_id is None:
            raise RuntimeError("Could not parse batch_id from publish response.")
        return batch_id

    def upload_artifact(self, batch_id: int, source_path: str, *, account_id: Optional[int] = None) -> dict[str, Any]:
        source = Path(source_path).expanduser()
        if not source.exists() or not source.is_file():
            raise RuntimeError(f"Source video not found: {source}")
        with source.open("rb") as handle:
            files = {"media_file": (source.name, handle, "video/mp4")}
            data: list[tuple[str, str]] = []
            if account_id is not None:
                data.append(("account_id", str(int(account_id))))
            response = self._request(
                "POST",
                f"/api/publishing/batches/{int(batch_id)}/artifacts/upload",
                data=data,
                files=files,
            )
        return response.json()

    def publish_callback(self, payload: dict[str, Any], *, shared_secret: str) -> dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        timestamp = str(int(time.time()))
        response = self._request(
            "POST",
            "/api/internal/publishing/n8n",
            data=body,
            headers={
                "X-Publish-Timestamp": timestamp,
                "X-Publish-Signature": _publish_signature(shared_secret, timestamp, body),
                "Content-Type": "application/json",
            },
        )
        return response.json()

    def batch_progress(self, batch_id: int) -> dict[str, Any]:
        response = self._request(
            "GET",
            f"/api/publishing/batches/{int(batch_id)}/progress",
            retry_budget_seconds=120.0,
        )
        return response.json()


class HelperClient:
    def __init__(self, base_url: str, api_key: str, timeout: int = 20):
        self.base_url = (base_url or "").rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        last_error: Optional[Exception] = None
        for attempt in range(1, 6):
            try:
                response = self.session.request(
                    method.upper(),
                    _join_url(self.base_url, path),
                    timeout=self.timeout,
                    **kwargs,
                )
                response.raise_for_status()
                return response
            except requests.RequestException as exc:
                last_error = exc
                if attempt >= 5:
                    raise
                time.sleep(min(5, attempt))
        assert last_error is not None
        raise last_error

    def health(self) -> dict[str, Any]:
        response = self._request("GET", "/health")
        return response.json()

    def emulators(self) -> dict[str, Any]:
        response = self._request(
            "GET",
            "/api/helper/emulators",
            headers={"X-Helper-API-Key": self.api_key},
        )
        return response.json()

    def publish_source_latest(self) -> dict[str, Any]:
        response = self._request("GET", "/publish-source/latest")
        return response.json()


def _ssh_run(target: str, *, port: int, key_path: str, command: str, timeout: int = 30) -> str:
    cmd = [
        "ssh",
        "-i",
        key_path,
        "-p",
        str(port),
        target,
        command,
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=True)
    return result.stdout


def _scp_to_remote(
    local_path: Path,
    remote_path: str,
    *,
    target: str,
    port: int,
    key_path: str,
    timeout: int = 180,
    remote_owner: str = "slezhka",
    remote_group: str = "slezhka",
) -> None:
    remote_parent = str(Path(remote_path).parent)
    _ssh_run(
        target,
        port=port,
        key_path=key_path,
        command=(
            f"mkdir -p {shlex.quote(remote_parent)}"
            f" && chown {shlex.quote(remote_owner)}:{shlex.quote(remote_group)} {shlex.quote(remote_parent)}"
            f" && chmod 2770 {shlex.quote(remote_parent)}"
        ),
        timeout=min(30, timeout),
    )
    cmd = [
        "scp",
        "-i",
        key_path,
        "-P",
        str(port),
        str(local_path),
        f"{target}:{remote_path}",
    ]
    subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=True)
    _ssh_run(
        target,
        port=port,
        key_path=key_path,
        command=(
            f"chown {shlex.quote(remote_owner)}:{shlex.quote(remote_group)} {shlex.quote(remote_path)}"
            f" && chmod 0640 {shlex.quote(remote_path)}"
        ),
        timeout=min(30, timeout),
    )


def _lookup_remote_account(
    username: str,
    *,
    target: str,
    port: int,
    key_path: str,
    remote_db_path: str,
) -> RemoteAccountInfo:
    sql = (
        "SELECT id, username, type, rotation_state, instagram_emulator_serial "
        "FROM accounts WHERE username = ? LIMIT 1"
    )
    payload = (
        "python3 - <<'PY'\n"
        "import json, sqlite3\n"
        f"conn=sqlite3.connect('file:{remote_db_path}?mode=ro', uri=True, timeout=1)\n"
        "conn.row_factory=sqlite3.Row\n"
        "cur=conn.cursor()\n"
        f"cur.execute({sql!r}, ({username!r},))\n"
        "row=cur.fetchone()\n"
        "print(json.dumps(dict(row) if row else None, ensure_ascii=False))\n"
        "PY"
    )
    raw = _ssh_run(target, port=port, key_path=key_path, command=payload)
    data = json.loads(raw.strip() or "null")
    if not isinstance(data, dict):
        raise RuntimeError(f"Account @{username} not found in remote DB.")
    return RemoteAccountInfo(
        account_id=int(data["id"]),
        username=str(data.get("username") or ""),
        account_type=str(data.get("type") or ""),
        rotation_state=str(data.get("rotation_state") or ""),
        emulator_serial=str(data.get("instagram_emulator_serial") or ""),
    )


def _remote_active_job_count(
    *,
    target: str,
    port: int,
    key_path: str,
    remote_db_path: str,
) -> int:
    sql = (
        "SELECT COUNT(*) AS cnt FROM publish_jobs "
        "WHERE state IN ('queued','leased','preparing','importing_media','opening_reel_flow','selecting_media','publishing')"
    )
    payload = (
        "python3 - <<'PY'\n"
        "import sqlite3\n"
        f"conn=sqlite3.connect('file:{remote_db_path}?mode=ro', uri=True, timeout=1)\n"
        "conn.row_factory=sqlite3.Row\n"
        "cur=conn.cursor()\n"
        f"cur.execute({sql!r})\n"
        "print(int(cur.fetchone()['cnt']))\n"
        "PY"
    )
    raw = _ssh_run(target, port=port, key_path=key_path, command=payload)
    return int((raw or "0").strip() or 0)


def _remote_active_batches_for_account(
    account_id: int,
    *,
    target: str,
    port: int,
    key_path: str,
    remote_db_path: str,
) -> list[dict[str, Any]]:
    states_sql = ",".join(repr(state) for state in sorted(ACTIVE_BATCH_ACCOUNT_STATES))
    sql = (
        "SELECT batch_id, state, detail, updated_at "
        "FROM publish_batch_accounts "
        f"WHERE account_id = ? AND state IN ({states_sql}) "
        "ORDER BY batch_id DESC"
    )
    payload = (
        "python3 - <<'PY'\n"
        "import json, sqlite3\n"
        f"conn=sqlite3.connect('file:{remote_db_path}?mode=ro', uri=True, timeout=1)\n"
        "conn.row_factory=sqlite3.Row\n"
        "cur=conn.cursor()\n"
        f"cur.execute({sql!r}, ({int(account_id)!r},))\n"
        "rows=[dict(row) for row in cur.fetchall()]\n"
        "print(json.dumps(rows, ensure_ascii=False))\n"
        "PY"
    )
    raw = _ssh_run(target, port=port, key_path=key_path, command=payload)
    data = json.loads(raw.strip() or "[]")
    if not isinstance(data, list):
        return []
    return [dict(item) for item in data if isinstance(item, dict)]


def _save_ui_capture(target_dir: Path, serial: str) -> dict[str, str]:
    target_dir.mkdir(parents=True, exist_ok=True)
    adb_path = ig_helper._resolve_adb_path()
    if not adb_path or not serial:
        return {}
    paths: dict[str, str] = {}
    device = None
    try:
        device = ig_helper._connect_ui(serial)
    except Exception:
        device = None
    try:
        result = subprocess.run(
            [adb_path, "-s", serial, "exec-out", "screencap", "-p"],
            capture_output=True,
            timeout=30,
            check=False,
        )
        if result.returncode == 0 and result.stdout:
            path = target_dir / "screen.png"
            path.write_bytes(result.stdout)
            paths["screenshot"] = str(path)
    except Exception:
        pass
    for name, args in (
        ("window.txt", ("dumpsys", "window")),
        ("activity.txt", ("dumpsys", "activity", "top")),
    ):
        try:
            result = ig_helper._adb_shell(serial, *args, timeout=25, check=False)
            path = target_dir / name
            path.write_text(result.stdout or "", encoding="utf-8")
            paths[name] = str(path)
        except Exception:
            pass
    if device is not None:
        try:
            try:
                hierarchy = device.dump_hierarchy(compressed=False)
            except TypeError:
                hierarchy = device.dump_hierarchy()
            if hierarchy:
                path = target_dir / "hierarchy.xml"
                path.write_text(str(hierarchy), encoding="utf-8")
                paths["hierarchy"] = str(path)
        except Exception:
            pass
        try:
            nodes = ig_helper._dump_ui_nodes(device)
            path = target_dir / "nodes.json"
            _write_json(path, nodes)
            paths["nodes"] = str(path)
        except Exception:
            pass
    try:
        _write_json(target_dir / "anr_windows.json", ig_helper._system_anr_windows(serial))
    except Exception:
        pass
    return paths


def _summarize_progress(snapshot: dict[str, Any], account_id: int) -> dict[str, Any]:
    batch = dict(snapshot.get("batch") or {})
    accounts = list(snapshot.get("accounts") or [])
    account = next((dict(item) for item in accounts if int(item.get("id") or 0) == int(account_id)), {})
    return {
        "batch_id": int(batch.get("id") or 0),
        "batch_state": str(batch.get("state") or ""),
        "batch_phase": str(batch.get("phase_key") or ""),
        "batch_progress_pct": int(batch.get("progress_pct") or 0),
        "account_id": int(account.get("id") or 0),
        "account_state": str(account.get("batch_state") or ""),
        "publish_phase": str(account.get("publish_phase") or ""),
        "phase_label": str(account.get("phase_label") or ""),
        "phase_detail": str(account.get("phase_detail") or ""),
        "progress_pct": int(account.get("progress_pct") or 0),
        "accepted_by_instagram": account.get("accepted_by_instagram"),
        "elapsed_seconds": account.get("elapsed_seconds"),
        "upload_progress_pct": account.get("upload_progress_pct"),
        "last_activity": account.get("last_activity"),
    }


def _capture_bundle(
    bundle_dir: Path,
    *,
    label: str,
    reason: str,
    capture_index: int,
    snapshot: dict[str, Any],
    helper_health: dict[str, Any],
    helper_emulators: dict[str, Any],
    publish_source: dict[str, Any],
    new_log_lines: list[str],
    log_tail_lines: list[str],
    timeline_log_lines: list[str],
    serial: str,
    account_id: int,
) -> CanaryCaptureRecord:
    capture_dir = bundle_dir / f"{capture_index:02d}_{_slugify(label)}"
    capture_dir.mkdir(parents=True, exist_ok=True)
    _write_json(capture_dir / "progress_snapshot.json", snapshot)
    _write_json(capture_dir / "helper_health.json", helper_health)
    _write_json(capture_dir / "helper_emulators.json", helper_emulators)
    _write_json(capture_dir / "publish_source_latest.json", publish_source)
    _write_text(capture_dir / "helper_log_tail.log", "\n".join(log_tail_lines))
    _write_text(capture_dir / "helper_log_new.log", "\n".join(new_log_lines))
    ui_paths = _save_ui_capture(capture_dir / "emulator", serial) if serial else {}
    timeline_tail = list(timeline_log_lines[-500:])
    timestamps = _extract_latest_helper_timestamps(timeline_tail)
    summary = _summarize_progress(snapshot, account_id)
    _write_json(
        capture_dir / "summary.json",
        {
            "label": label,
            "reason": reason,
            "created_at": datetime.now().isoformat(),
            "summary": summary,
            "timestamps": timestamps,
            "ui_paths": ui_paths,
            "log_markers": _extract_log_markers(new_log_lines or timeline_tail),
        },
    )
    return CanaryCaptureRecord(
        index=capture_index,
        label=label,
        reason=reason,
        created_at=datetime.now().isoformat(),
        directory=str(capture_dir),
        batch_state=summary["batch_state"],
        account_state=summary["account_state"],
        publish_phase=summary["publish_phase"],
        helper_state=str((helper_health.get("state") or {}).get("state") or ""),
        helper_detail=str((helper_health.get("state") or {}).get("detail") or ""),
        matched_age_seconds=timestamps.get("matched_age_seconds"),
        share_clicked_at=timestamps.get("share_clicked_at"),
        verification_starts_at=timestamps.get("verification_starts_at"),
        verification_deadline_at=timestamps.get("verification_deadline_at"),
        first_profile_check_at=timestamps.get("first_profile_check_at"),
        recent_log_markers=_extract_log_markers(new_log_lines or timeline_tail),
    )


def _print(msg: str) -> None:
    print(msg, flush=True)


def _run_canary(args: argparse.Namespace) -> int:
    load_dotenv(args.env_file)
    bundle_dir = Path(args.bundle_root).expanduser() / f"{_now_compact()}-{_slugify(args.username)}"
    bundle_dir.mkdir(parents=True, exist_ok=True)

    base_url = _resolve_config_value(args.base_url, "SLEZHKA_ADMIN_BASE_URL", "http://127.0.0.1:38001/slezhka")
    helper_base_url = _resolve_config_value(args.helper_base_url, "PUBLISH_CANARY_HELPER_BASE_URL", "http://127.0.0.1:17374")
    helper_api_key = _resolve_config_value(args.helper_api_key, "HELPER_API_KEY")
    publish_shared_secret = _resolve_config_value(args.publish_shared_secret, "PUBLISH_SHARED_SECRET", helper_api_key)
    admin_user = _resolve_config_value(args.admin_user, "ADMIN_USER", "admin")
    admin_pass = _resolve_config_value(args.admin_pass, "ADMIN_PASS")
    if not admin_pass:
        raise RuntimeError("ADMIN_PASS is required.")
    if not helper_api_key:
        raise RuntimeError("HELPER_API_KEY is required.")
    if not publish_shared_secret:
        raise RuntimeError("PUBLISH_SHARED_SECRET or HELPER_API_KEY is required for artifact_ready callback.")
    if _is_loopback_url(base_url):
        raise RuntimeError(f"Canary requires direct admin base_url, got loopback URL: {base_url}")
    resuming_existing_batch = int(args.batch_id or 0) > 0

    admin = AdminClient(base_url, admin_user, admin_pass, timeout=args.http_timeout)
    helper = HelperClient(helper_base_url, helper_api_key, timeout=args.http_timeout)

    _print(f"[preflight] bundle={bundle_dir}")
    helper_health = helper.health()
    helper_emulators = helper.emulators()
    publish_source = helper.publish_source_latest()
    if not helper_health.get("ok"):
        raise RuntimeError("Helper health check failed.")
    if bool((helper_health.get("state") or {}).get("flow_running")) and not (int(args.batch_id or 0) > 0 and bool(args.allow_active_jobs)):
        raise RuntimeError("Helper is busy. Stop the active flow before canary.")
    helper_reported_base_url = str(helper_health.get("base_url") or "").rstrip("/")
    if not _helper_base_url_matches_expected(base_url, helper_reported_base_url):
        raise RuntimeError(
            "Helper is pointed at a different admin base_url. "
            f"health.base_url={helper_reported_base_url or '-'} expected={base_url.rstrip('/')}"
        )
    if _should_upload_existing_video(
        launch_mode=args.launch_mode,
        resuming_existing_batch=resuming_existing_batch,
        upload_existing_video_on_resume=bool(args.upload_existing_video_on_resume),
    ) and not publish_source.get("latest_video"):
        raise RuntimeError("Helper does not see any current source video.")

    remote_info: Optional[RemoteAccountInfo] = None
    if args.ssh_target:
        remote_info = _lookup_remote_account(
            args.username,
            target=args.ssh_target,
            port=args.ssh_port,
            key_path=args.ssh_key,
            remote_db_path=args.remote_db_path,
        )
        _print(
            f"[preflight] remote account id={remote_info.account_id} serial={remote_info.emulator_serial or '-'} "
            f"rotation={remote_info.rotation_state or '-'}"
        )
        if not args.allow_active_jobs:
            active_jobs = _remote_active_job_count(
                target=args.ssh_target,
                port=args.ssh_port,
                key_path=args.ssh_key,
                remote_db_path=args.remote_db_path,
            )
            if active_jobs > 0:
                raise RuntimeError(f"There are still {active_jobs} active publish jobs on the server.")
            active_batches = _remote_active_batches_for_account(
                remote_info.account_id,
                target=args.ssh_target,
                port=args.ssh_port,
                key_path=args.ssh_key,
                remote_db_path=args.remote_db_path,
            )
            if active_batches:
                batch_labels = ", ".join(
                    f"batch {int(item.get('batch_id') or 0)}:{str(item.get('state') or '-')}"
                    for item in active_batches[:5]
                )
                raise RuntimeError(
                    "There are still active publish batches for this account. "
                    f"Finish/cancel them before clean canary: {batch_labels}"
                )

    account_id: Optional[int] = int(remote_info.account_id) if remote_info is not None else None
    preferred_serial = str(args.preferred_serial or (remote_info.emulator_serial if remote_info is not None else "") or "").strip()
    admin.login()
    start_html = admin.publishing_start_html()
    ready_accounts = _extract_ready_accounts(start_html)
    _write_text(bundle_dir / "publishing_start.html", start_html)
    _write_json(bundle_dir / "ready_accounts.json", ready_accounts)
    ready_match = next((item for item in ready_accounts if str(item.get("username") or "") == args.username), None)
    if ready_match is None:
        raise RuntimeError(f"@{args.username} is not present in ready accounts on /publishing/start.")
    if account_id is None:
        account_id = int(ready_match["account_id"])
    if not preferred_serial:
        available = list(helper_emulators.get("available_serials") or [])
        if len(available) == 1:
            preferred_serial = str(available[0])
    if not preferred_serial:
        raise RuntimeError("Could not determine target emulator serial for canary.")

    if not args.skip_emulator_start:
        _print(f"[preflight] ensuring emulator {preferred_serial}")
        serial = ig_helper._ensure_emulator_ready(preferred_serial=preferred_serial)
        _print(f"[preflight] emulator ready: {serial}")
    else:
        serial = preferred_serial

    log_file = Path(str(helper_health.get("log_file") or "")).expanduser()
    log_offset = log_file.stat().st_size if log_file.exists() else 0
    captures: list[CanaryCaptureRecord] = []
    helper_lines_seen: list[str] = []

    preflight_capture = _capture_bundle(
        bundle_dir,
        label="preflight",
        reason="preflight_ready",
        capture_index=1,
        snapshot={"batch": {}, "accounts": [], "recent_activity": []},
        helper_health=helper.health(),
        helper_emulators=helper.emulators(),
        publish_source=helper.publish_source_latest(),
        new_log_lines=[],
        log_tail_lines=_tail_lines(log_file, limit=args.log_tail_lines),
        timeline_log_lines=[],
        serial=serial,
        account_id=account_id,
    )
    captures.append(preflight_capture)
    _write_json(bundle_dir / "manifest.json", {"captures": [asdict(item) for item in captures]})

    batch_id = int(args.batch_id or 0)
    upload_existing_video = _should_upload_existing_video(
        launch_mode=args.launch_mode,
        resuming_existing_batch=resuming_existing_batch,
        upload_existing_video_on_resume=bool(args.upload_existing_video_on_resume),
    )
    if batch_id > 0:
        _print(f"[run] resuming existing batch_id={batch_id}")
        if args.launch_mode == "existing_video" and not upload_existing_video:
            _print("[run] resume mode detected; skipping existing-video upload by default")
    else:
        _print(
            f"[run] creating production batch for @{args.username} account_id={account_id} "
            f"launch_mode={args.launch_mode}"
        )
        batch_id = admin.create_batch(account_id, launch_mode=args.launch_mode)
        _print(f"[run] batch_id={batch_id}")
    if upload_existing_video:
        latest_video = dict(publish_source.get("latest_video") or {})
        source_path = str(latest_video.get("path") or "").strip()
        try:
            upload_result = admin.upload_artifact(batch_id, source_path, account_id=account_id)
        except requests.HTTPError as exc:
            response = exc.response
            if response is None or response.status_code != 404 or not args.ssh_target:
                raise
            snapshot = admin.batch_progress(batch_id)
            stage_dir = str((snapshot.get("batch") or {}).get("stage_dir") or "").strip()
            if not stage_dir:
                raise RuntimeError("Could not determine remote batch stage_dir for SSH artifact fallback.") from exc
            remote_path = str(Path(stage_dir) / (latest_video.get("name") or Path(source_path).name))
            _scp_to_remote(
                Path(source_path).expanduser(),
                remote_path,
                target=args.ssh_target,
                port=args.ssh_port,
                key_path=args.ssh_key,
            )
            payload = {
                "event": "artifact_ready",
                "batch_id": int(batch_id),
                "account_id": int(account_id) if account_id is not None else None,
                "path": remote_path,
                "filename": Path(remote_path).name,
                "size_bytes": int(Path(source_path).expanduser().stat().st_size),
            }
            upload_result = admin.publish_callback(payload, shared_secret=publish_shared_secret)
            upload_result["path"] = remote_path
        _print(
            f"[run] uploaded existing video {latest_video.get('name') or Path(source_path).name} "
            f"to batch {batch_id}: jobs_created={upload_result.get('jobs_created')}"
        )

    snapshot = admin.batch_progress(batch_id)
    helper_health = helper.health()
    helper_emulators = helper.emulators()
    publish_source = helper.publish_source_latest()
    log_tail = _tail_lines(log_file, limit=args.log_tail_lines)
    captures.append(
        _capture_bundle(
            bundle_dir,
            label="batch-created" if not args.batch_id else "batch-resumed",
            reason="batch_created" if not args.batch_id else "batch_resumed",
            capture_index=len(captures) + 1,
            snapshot=snapshot,
            helper_health=helper_health,
            helper_emulators=helper_emulators,
            publish_source=publish_source,
            new_log_lines=[],
            log_tail_lines=log_tail,
            timeline_log_lines=helper_lines_seen,
            serial=serial,
            account_id=account_id,
        )
    )
    _write_json(bundle_dir / "manifest.json", {"batch_id": batch_id, "captures": [asdict(item) for item in captures]})

    started_at = time.time()
    last_capture_at = time.time()
    previous_state = _state_signature(snapshot, account_id)
    terminal_snapshot = snapshot
    while True:
        if time.time() - started_at > args.timeout_seconds:
            raise RuntimeError(f"Canary timed out after {args.timeout_seconds} seconds.")

        time.sleep(max(1, int(snapshot.get("poll_interval_seconds") or args.poll_seconds or 2)))
        snapshot = admin.batch_progress(batch_id)
        terminal_snapshot = snapshot
        helper_health = helper.health()
        helper_emulators = helper.emulators()
        publish_source = helper.publish_source_latest()
        new_lines, log_offset = _tail_from_offset(log_file, log_offset)
        if new_lines:
            helper_lines_seen.extend(new_lines)
        recent_tail = _tail_lines(log_file, limit=args.log_tail_lines)
        current_state = _state_signature(snapshot, account_id)
        log_markers = _extract_log_markers(new_lines)
        should_capture = False
        capture_label = "poll"
        capture_reason = "periodic"

        if current_state != previous_state:
            should_capture = True
            capture_label = current_state.publish_phase or current_state.account_state or current_state.batch_phase or "state-change"
            capture_reason = "progress_state_changed"
        elif log_markers:
            should_capture = True
            capture_label = log_markers[-1]
            capture_reason = "helper_log_marker"
        elif time.time() - last_capture_at >= args.periodic_capture_seconds:
            should_capture = True
            capture_label = current_state.publish_phase or current_state.account_state or "heartbeat"
            capture_reason = "periodic_capture"

        batch = dict(snapshot.get("batch") or {})
        accounts = list(snapshot.get("accounts") or [])
        account = next((dict(item) for item in accounts if int(item.get("id") or 0) == int(account_id)), {})
        account_state = str(account.get("batch_state") or "")
        batch_terminal = bool(batch.get("is_terminal"))
        account_terminal = account_state in TERMINAL_ACCOUNT_STATES
        if batch_terminal or account_terminal:
            should_capture = True
            capture_label = account_state or str(batch.get("state") or "terminal")
            capture_reason = "terminal_state"

        if should_capture:
            captures.append(
                _capture_bundle(
                    bundle_dir,
                    label=capture_label,
                    reason=capture_reason,
                    capture_index=len(captures) + 1,
                    snapshot=snapshot,
                    helper_health=helper_health,
                    helper_emulators=helper_emulators,
                    publish_source=publish_source,
                    new_log_lines=new_lines,
                    log_tail_lines=recent_tail,
                    timeline_log_lines=helper_lines_seen,
                    serial=serial,
                    account_id=account_id,
                )
            )
            _write_json(bundle_dir / "manifest.json", {"batch_id": batch_id, "captures": [asdict(item) for item in captures]})
            last_capture_at = time.time()
            summary = _summarize_progress(snapshot, account_id)
            _print(
                "[run] "
                f"batch={summary['batch_state']} account={summary['account_state']} phase={summary['publish_phase'] or summary['phase_label']} "
                f"progress={summary['progress_pct']}%"
            )

        previous_state = current_state
        if batch_terminal or account_terminal:
            break

    final_health = helper.health()
    final_logs = _tail_lines(log_file, limit=args.log_tail_lines)
    classification = ""
    accounts = list(terminal_snapshot.get("accounts") or [])
    account = next((dict(item) for item in accounts if int(item.get("id") or 0) == int(account_id)), {})
    final_state = str(account.get("batch_state") or "")
    if final_state and final_state != "published":
        classification = _classify_failure(terminal_snapshot, final_health, final_logs)

    final_payload = {
        "bundle_dir": str(bundle_dir),
        "batch_id": batch_id,
        "username": args.username,
        "account_id": account_id,
        "emulator_serial": serial,
        "final_state": final_state or str((terminal_snapshot.get("batch") or {}).get("state") or ""),
        "classification": classification,
        "captures": [asdict(item) for item in captures],
    }
    _write_json(bundle_dir / "final_summary.json", final_payload)
    _print(f"[done] batch_id={batch_id} final_state={final_payload['final_state']} classification={classification or 'success'}")
    _print(f"[done] evidence={bundle_dir}")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a controlled one-account publish canary with evidence bundle capture.")
    parser.add_argument("--env-file", default=".env")
    parser.add_argument("--username", default="ayugram_sed")
    parser.add_argument("--base-url", default="")
    parser.add_argument("--admin-user", default="")
    parser.add_argument("--admin-pass", default="")
    parser.add_argument("--helper-base-url", default="")
    parser.add_argument("--helper-api-key", default="")
    parser.add_argument("--publish-shared-secret", default="")
    parser.add_argument("--bundle-root", default=str(ROOT / "reports" / "publish-canary"))
    parser.add_argument("--http-timeout", type=int, default=25)
    parser.add_argument("--timeout-seconds", type=int, default=5400)
    parser.add_argument("--poll-seconds", type=int, default=2)
    parser.add_argument("--periodic-capture-seconds", type=int, default=60)
    parser.add_argument("--log-tail-lines", type=int, default=200)
    parser.add_argument("--batch-id", type=int, default=0)
    parser.add_argument("--launch-mode", choices=("generated", "existing_video"), default="generated")
    parser.add_argument("--upload-existing-video-on-resume", action="store_true")
    parser.add_argument("--preferred-serial", default="")
    parser.add_argument("--skip-emulator-start", action="store_true")
    parser.add_argument("--allow-active-jobs", action="store_true")
    parser.add_argument("--ssh-target", default="")
    parser.add_argument("--ssh-port", type=int, default=49297)
    parser.add_argument("--ssh-key", default=str(Path.home() / ".ssh" / "codex_tvf_ed25519"))
    parser.add_argument("--remote-db-path", default="/srv/slezhka/shared/admin.db")
    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    try:
        return _run_canary(args)
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
