#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import requests
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import http_utils

DEFAULT_TIMEOUT = 25


@dataclass
class EnvConfig:
    base_url: str
    admin_user: str
    admin_pass: str
    helper_api_key: str
    runner_api_key: str
    shared_secret: str


def _normalize_base_url(raw: str) -> str:
    value = (raw or "").strip().rstrip("/")
    if not value:
        raise ValueError("Base URL is required")
    if not value.startswith(("http://", "https://")):
        raise ValueError("Base URL must start with http:// or https://")
    return value


def _join_url(base_url: str, path: str) -> str:
    suffix = (path or "").strip()
    if not suffix.startswith("/"):
        suffix = "/" + suffix
    return f"{base_url}{suffix}"


def _masked(value: str) -> str:
    clean = (value or "").strip()
    if not clean:
        return ""
    if len(clean) <= 8:
        return "*" * len(clean)
    return f"{clean[:4]}...{clean[-2:]}"


def _load_env(env_file: Optional[str]) -> None:
    if env_file:
        load_dotenv(env_file)
        return
    load_dotenv()


def _env_config(args: argparse.Namespace) -> EnvConfig:
    raw_base = args.base_url or os.getenv("SLEZHKA_ADMIN_BASE_URL") or os.getenv("PUBLISH_BASE_URL") or ""
    base_url = _normalize_base_url(raw_base)
    helper_api_key = (args.helper_api_key or os.getenv("HELPER_API_KEY") or "").strip()
    runner_api_key = (
        args.runner_api_key
        or os.getenv("PUBLISH_RUNNER_API_KEY")
        or helper_api_key
    ).strip()
    shared_secret = (
        args.shared_secret
        or os.getenv("PUBLISH_SHARED_SECRET")
        or helper_api_key
    ).strip()
    admin_user = (args.admin_user or os.getenv("ADMIN_USER") or "admin").strip()
    admin_pass = (args.admin_pass or os.getenv("ADMIN_PASS") or "admin").strip()
    return EnvConfig(
        base_url=base_url,
        admin_user=admin_user,
        admin_pass=admin_pass,
        helper_api_key=helper_api_key,
        runner_api_key=runner_api_key,
        shared_secret=shared_secret,
    )


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def _try_request(
    method: str,
    url: str,
    *,
    timeout: int,
    session: Optional[requests.Session] = None,
    **kwargs: Any,
) -> dict[str, Any]:
    try:
        response = http_utils.request_with_retry(
            method,
            url,
            session=session,
            timeout=timeout,
            allow_retry=True,
            log_context="publishing_ops",
            **kwargs,
        )
    except Exception as exc:
        return {"ok": False, "error": str(exc)}
    return {
        "ok": True,
        "status_code": int(response.status_code),
        "location": response.headers.get("location", ""),
        "text_preview": (response.text or "")[:220],
        "headers": dict(response.headers),
    }


def cmd_baseline(args: argparse.Namespace) -> int:
    cfg = _env_config(args)
    report: dict[str, Any] = {
        "base_url": cfg.base_url,
        "keys": {
            "helper_api_key": bool(cfg.helper_api_key),
            "runner_api_key": bool(cfg.runner_api_key),
            "shared_secret": bool(cfg.shared_secret),
            "helper_api_key_masked": _masked(cfg.helper_api_key),
            "runner_api_key_masked": _masked(cfg.runner_api_key),
            "shared_secret_masked": _masked(cfg.shared_secret),
        },
    }
    timeout = int(args.timeout or DEFAULT_TIMEOUT)

    report["login_page"] = _try_request("GET", _join_url(cfg.base_url, "/login"), timeout=timeout, allow_redirects=False)
    report["publishing_guest"] = _try_request(
        "GET",
        _join_url(cfg.base_url, "/publishing"),
        timeout=timeout,
        allow_redirects=False,
    )

    session = requests.Session()
    report["login_post"] = _try_request(
        "POST",
        _join_url(cfg.base_url, "/login"),
        timeout=timeout,
        session=session,
        allow_redirects=False,
        data={"username": cfg.admin_user, "password": cfg.admin_pass},
    )
    report["publishing_after_login"] = _try_request(
        "GET",
        _join_url(cfg.base_url, "/publishing"),
        timeout=timeout,
        session=session,
        allow_redirects=False,
    )

    helper_headers: dict[str, str] = {}
    if cfg.helper_api_key:
        helper_headers["X-Helper-Api-Key"] = cfg.helper_api_key
    report["helper_probe"] = _try_request(
        "GET",
        _join_url(cfg.base_url, "/api/helper/launch-ticket/not-a-ticket?target=instagram_app_login"),
        timeout=timeout,
        headers=helper_headers,
        allow_redirects=False,
    )

    runner_headers: dict[str, str] = {}
    if cfg.runner_api_key:
        runner_headers["X-Runner-Api-Key"] = cfg.runner_api_key
    report["runner_lease_probe"] = _try_request(
        "POST",
        _join_url(cfg.base_url, "/api/internal/publishing/jobs/lease"),
        timeout=timeout,
        headers=runner_headers,
        json={"runner_name": args.runner_name},
        allow_redirects=False,
    )

    publishing_ready = (
        report["publishing_after_login"].get("ok")
        and int(report["publishing_after_login"].get("status_code", 0)) != 404
    )
    runner_endpoint_ready = (
        report["runner_lease_probe"].get("ok")
        and int(report["runner_lease_probe"].get("status_code", 0)) != 404
    )
    report["summary"] = {
        "publishing_route_ready": bool(publishing_ready),
        "runner_endpoint_ready": bool(runner_endpoint_ready),
    }
    _print_json(report)

    if publishing_ready and runner_endpoint_ready:
        return 0
    return 2


def _sign_payload(shared_secret: str, timestamp: str, body: bytes) -> str:
    signed = timestamp.encode("utf-8") + b"." + body
    return hmac.new(shared_secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()


def _event_payload(args: argparse.Namespace) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "event": args.event,
        "batch_id": int(args.batch_id),
    }
    if args.account_id is not None:
        payload["account_id"] = int(args.account_id)
    if args.detail:
        payload["detail"] = args.detail
    if args.event == "artifact_ready":
        if not args.path:
            raise ValueError("--path is required for artifact_ready")
        payload["path"] = args.path
        if args.filename:
            payload["filename"] = args.filename
        if args.checksum:
            payload["checksum"] = args.checksum
        if args.size_bytes is not None:
            payload["size_bytes"] = int(args.size_bytes)
        if args.duration_seconds is not None:
            payload["duration_seconds"] = float(args.duration_seconds)
    if args.event == "generation_progress":
        if args.account_id is None:
            raise ValueError("--account-id is required for generation_progress")
        if not args.stage_key:
            raise ValueError("--stage-key is required for generation_progress")
        if not args.stage_label:
            raise ValueError("--stage-label is required for generation_progress")
        if args.progress_pct is None:
            raise ValueError("--progress-pct is required for generation_progress")
        payload["stage_key"] = args.stage_key
        payload["stage_label"] = args.stage_label
        payload["progress_pct"] = float(args.progress_pct)
        if args.meta_json:
            try:
                payload["meta"] = json.loads(args.meta_json)
            except Exception as exc:
                raise ValueError(f"--meta-json must be valid JSON: {exc}") from exc
    return payload


def cmd_send_event(args: argparse.Namespace) -> int:
    cfg = _env_config(args)
    if not cfg.shared_secret:
        raise ValueError("Publish shared secret is missing. Provide --shared-secret or set PUBLISH_SHARED_SECRET.")
    payload = _event_payload(args)
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
    timestamp = str(int(time.time()))
    signature = _sign_payload(cfg.shared_secret, timestamp, body)
    headers = {
        "Content-Type": "application/json",
        "X-Publish-Timestamp": timestamp,
        "X-Publish-Signature": signature,
    }
    timeout = int(args.timeout or DEFAULT_TIMEOUT)
    response = http_utils.request_with_retry(
        "POST",
        _join_url(cfg.base_url, "/api/internal/publishing/n8n"),
        data=body,
        headers=headers,
        timeout=timeout,
        allow_retry=False,
        log_context="publishing_ops_send_event",
    )
    report = {
        "url": _join_url(cfg.base_url, "/api/internal/publishing/n8n"),
        "payload": payload,
        "timestamp": timestamp,
        "signature_masked": _masked(signature),
        "status_code": int(response.status_code),
        "response_text": (response.text or "")[:400],
    }
    _print_json(report)
    return 0 if response.status_code < 400 else 1


def cmd_lease(args: argparse.Namespace) -> int:
    cfg = _env_config(args)
    if not cfg.runner_api_key:
        raise ValueError("Runner API key is missing. Provide --runner-api-key or set PUBLISH_RUNNER_API_KEY/HELPER_API_KEY.")
    timeout = int(args.timeout or DEFAULT_TIMEOUT)
    response = http_utils.request_with_retry(
        "POST",
        _join_url(cfg.base_url, "/api/internal/publishing/jobs/lease"),
        headers={"X-Runner-Api-Key": cfg.runner_api_key},
        json={"runner_name": args.runner_name},
        timeout=timeout,
        allow_retry=False,
        log_context="publishing_ops_lease",
    )
    response_json: Any = None
    try:
        response_json = response.json()
    except Exception:
        response_json = None
    payload = {
        "status_code": int(response.status_code),
        "response_text": (response.text or "")[:500],
        "response_json": response_json,
    }
    _print_json(payload)
    return 0 if response.status_code in {200, 204} else 1


def cmd_status(args: argparse.Namespace) -> int:
    cfg = _env_config(args)
    if not cfg.runner_api_key:
        raise ValueError("Runner API key is missing. Provide --runner-api-key or set PUBLISH_RUNNER_API_KEY/HELPER_API_KEY.")
    timeout = int(args.timeout or DEFAULT_TIMEOUT)
    body = {
        "state": args.state,
        "detail": args.detail or "",
        "last_file": args.last_file or "",
        "runner_name": args.runner_name,
        "source_path": args.source_path or "",
        "emulator_serial": args.emulator_serial or "",
    }
    response = http_utils.request_with_retry(
        "POST",
        _join_url(cfg.base_url, f"/api/internal/publishing/jobs/{int(args.job_id)}/status"),
        headers={"X-Runner-Api-Key": cfg.runner_api_key},
        json=body,
        timeout=timeout,
        allow_retry=False,
        log_context="publishing_ops_status",
    )
    response_json: Any = None
    try:
        response_json = response.json()
    except Exception:
        response_json = None
    payload = {
        "status_code": int(response.status_code),
        "request": body,
        "response_text": (response.text or "")[:500],
        "response_json": response_json,
    }
    _print_json(payload)
    return 0 if response.status_code < 400 else 1


def cmd_snapshot(args: argparse.Namespace) -> int:
    db_path = (args.db_path or os.getenv("ADMIN_DB_PATH") or "admin.db").strip()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    report: dict[str, Any] = {"db_path": db_path}
    for table in ("publish_batches", "publish_jobs", "publish_artifacts", "publish_batch_accounts"):
        try:
            cur.execute(f"SELECT COUNT(*) AS c FROM {table}")
            report[f"{table}_count"] = int(cur.fetchone()["c"])
        except Exception as exc:
            report[f"{table}_error"] = str(exc)

    try:
        cur.execute(
            """
            SELECT id, state, detail, created_at, updated_at
            FROM publish_batches
            ORDER BY id DESC
            LIMIT ?
            """,
            (int(args.limit),),
        )
        report["latest_batches"] = [dict(row) for row in cur.fetchall()]
    except Exception as exc:
        report["latest_batches_error"] = str(exc)
    conn.close()
    _print_json(report)
    return 0


def cmd_progress(args: argparse.Namespace) -> int:
    cfg = _env_config(args)
    timeout = int(args.timeout or DEFAULT_TIMEOUT)
    session = requests.Session()
    login = http_utils.request_with_retry(
        "POST",
        _join_url(cfg.base_url, "/login"),
        data={"username": cfg.admin_user, "password": cfg.admin_pass},
        allow_redirects=False,
        timeout=timeout,
        session=session,
        allow_retry=False,
        log_context="publishing_ops_login",
    )
    if login.status_code not in {302, 303}:
        report = {
            "status_code": int(login.status_code),
            "detail": "login failed",
            "response_text": (login.text or "")[:400],
        }
        _print_json(report)
        return 1

    response = http_utils.request_with_retry(
        "GET",
        _join_url(cfg.base_url, f"/api/publishing/batches/{int(args.batch_id)}/progress"),
        headers={"Accept": "application/json"},
        timeout=timeout,
        session=session,
        allow_retry=True,
        log_context="publishing_ops_progress",
    )
    response_json: Any = None
    try:
        response_json = response.json()
    except Exception:
        response_json = None
    report = {
        "status_code": int(response.status_code),
        "response_json": response_json,
        "response_text": (response.text or "")[:500],
    }
    _print_json(report)
    return 0 if response.status_code < 400 else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Operational helper for publishing rollout checks and signed callbacks.",
    )
    parser.add_argument("--env-file", default=None, help="Optional .env path")
    parser.add_argument("--base-url", default=None, help="Admin base URL, for example https://host/slezhka")
    parser.add_argument("--admin-user", default=None, help="Admin username for /login smoke")
    parser.add_argument("--admin-pass", default=None, help="Admin password for /login smoke")
    parser.add_argument("--helper-api-key", default=None, help="Helper API key")
    parser.add_argument("--runner-api-key", default=None, help="Runner API key")
    parser.add_argument("--shared-secret", default=None, help="Publish shared secret for webhook signature")
    parser.add_argument("--timeout", type=int, default=DEFAULT_TIMEOUT, help="HTTP timeout in seconds")

    sub = parser.add_subparsers(dest="command", required=True)

    p_baseline = sub.add_parser("baseline", help="Run baseline smoke checks against server endpoints")
    p_baseline.add_argument("--runner-name", default="publishing-ops-baseline")
    p_baseline.set_defaults(func=cmd_baseline)

    p_event = sub.add_parser("send-event", help="Send signed n8n callback event")
    p_event.add_argument(
        "--event",
        required=True,
        choices=["generation_started", "artifact_ready", "generation_completed", "generation_failed", "generation_progress"],
    )
    p_event.add_argument("--batch-id", required=True, type=int)
    p_event.add_argument("--account-id", type=int, default=None)
    p_event.add_argument("--detail", default="")
    p_event.add_argument("--path", default="")
    p_event.add_argument("--filename", default="")
    p_event.add_argument("--checksum", default="")
    p_event.add_argument("--size-bytes", type=int, default=None)
    p_event.add_argument("--duration-seconds", type=float, default=None)
    p_event.add_argument("--stage-key", default="")
    p_event.add_argument("--stage-label", default="")
    p_event.add_argument("--progress-pct", type=float, default=None)
    p_event.add_argument("--meta-json", default="")
    p_event.set_defaults(func=cmd_send_event)

    p_lease = sub.add_parser("lease", help="Lease next publish job as runner")
    p_lease.add_argument("--runner-name", default="publishing-ops-runner")
    p_lease.set_defaults(func=cmd_lease)

    p_status = sub.add_parser("set-status", help="Push publish job status")
    p_status.add_argument("--job-id", required=True, type=int)
    p_status.add_argument("--state", required=True)
    p_status.add_argument("--detail", default="")
    p_status.add_argument("--last-file", default="")
    p_status.add_argument("--source-path", default="")
    p_status.add_argument("--emulator-serial", default="")
    p_status.add_argument("--runner-name", default="publishing-ops-runner")
    p_status.set_defaults(func=cmd_status)

    p_snapshot = sub.add_parser("snapshot-db", help="Show local publishing DB snapshot")
    p_snapshot.add_argument("--db-path", default=None)
    p_snapshot.add_argument("--limit", type=int, default=10)
    p_snapshot.set_defaults(func=cmd_snapshot)

    p_progress = sub.add_parser("progress", help="Fetch authenticated batch progress snapshot from admin API")
    p_progress.add_argument("--batch-id", required=True, type=int)
    p_progress.set_defaults(func=cmd_progress)

    return parser


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    _load_env(args.env_file)
    try:
        return int(args.func(args))
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
