from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


TRUE_VALUES = {"1", "true", "yes", "on"}


def _env_str(name: str, default: str = "") -> str:
    return (os.getenv(name, default) or default).strip()


def _env_bool(name: str, default: bool = False) -> bool:
    raw_default = "1" if default else "0"
    return _env_str(name, raw_default).lower() in TRUE_VALUES


def _env_int(name: str, default: int, *, minimum: int | None = None) -> int:
    value = int(_env_str(name, str(default)) or default)
    if minimum is not None:
        value = max(minimum, value)
    return value


def _env_float(name: str, default: float, *, minimum: float | None = None) -> float:
    value = float(_env_str(name, str(default)) or default)
    if minimum is not None:
        value = max(minimum, value)
    return value


@dataclass(frozen=True)
class WebSettings:
    admin_user: str
    admin_pass: str
    bot_token: str
    bot_username: str
    admin_test_chat_id: str
    session_secret: str
    session_max_age_seconds: int
    max_broadcast_media_bytes: int
    admin_base_path_raw: str
    helper_api_key: str
    helper_ticket_ttl_seconds: int
    instagram_app_helper_open_url: str
    instagram_publish_source_dir: str
    publish_n8n_webhook_url: str
    publish_staging_dir: str
    publish_base_url: str
    publish_shared_secret: str
    publish_webhook_max_age_seconds: int
    publish_factory_timeout_seconds: int
    publish_runner_api_key: str
    publish_runner_lease_seconds: int
    publish_default_workflow: str
    mail_collector_enabled: bool
    mail_collector_reconcile_seconds: int
    mail_collector_stale_sync_seconds: int
    mail_collector_watch_renew_margin_seconds: int
    mail_webhook_secret: str
    strict_config: bool
    accounts_import_max_bytes: int
    instagram_audit_poll_interval_seconds: int
    instagram_audit_helper_poll_seconds: int
    instagram_audit_helper_idle_timeout_seconds: int
    instagram_audit_login_timeout_seconds: int
    instagram_audit_mail_freshness_seconds: int
    instagram_mail_challenge_timeout_seconds: int
    instagram_mail_challenge_poll_seconds: int
    instagram_mail_challenge_lookback_seconds: int
    instagram_mail_challenge_freshness_seconds: int
    instagram_audit_item_retry_attempts: int
    instagram_audit_force_clean_login: bool
    runtime_task_lease_seconds: int
    runtime_worker_heartbeat_seconds: int
    runtime_task_retry_delay_seconds: int
    runtime_reconcile_interval_seconds: int
    runtime_worker_live_timeout_seconds: int
    runtime_worker_name: str
    runtime_worker_idle_poll_seconds: float
    embed_runtime_worker: bool


@dataclass(frozen=True)
class HelperSettings:
    helper_bind: str
    slezhka_admin_base_url: str
    helper_api_key: str
    android_avd_name: str
    adb_path_raw: str
    emulator_path_raw: str
    emulator_no_window: bool
    instagram_package: str
    instagram_publish_source_dir: str
    instagram_publish_media_dir: str
    publish_runner_cache_dir: str
    publish_runner_downloads_dir: str
    publish_diagnostics_dir: str
    strict_config: bool
    emulator_stabilize_seconds: int
    use_emulator_snapshots: bool
    publish_runner_enabled: bool
    publish_runner_poll_seconds: int
    publish_success_wait_seconds: int
    publish_heartbeat_seconds: int
    publish_upload_start_wait_seconds: int
    publish_profile_verify_start_delay_seconds: int
    publish_profile_verify_seconds: int
    publish_profile_verify_interval_seconds: int
    publish_profile_freshness_seconds: int
    publish_profile_check_slots: int
    publish_profile_baseline_slots: int
    publish_runner_name: str
    publish_runner_api_key: str
    serial_to_avd_map_raw: str
    instagram_mail_challenge_timeout_seconds: int
    instagram_mail_challenge_retry_seconds: int
    instagram_mail_challenge_resend_wait_seconds: int


def _default_publish_source_dir() -> str:
    return str(Path.home() / "SlezhkaPublishSource")


def load_web_settings() -> WebSettings:
    helper_api_key = _env_str("HELPER_API_KEY", "")
    session_secret = _env_str("SESSION_SECRET", "change-me")
    publish_shared_secret = (
        _env_str("PUBLISH_SHARED_SECRET", helper_api_key or session_secret)
        or helper_api_key
        or session_secret
    )
    publish_runner_api_key = _env_str("PUBLISH_RUNNER_API_KEY", helper_api_key) or helper_api_key
    default_publish_source_dir = _default_publish_source_dir()

    runtime_worker_heartbeat_seconds = _env_int("RUNTIME_WORKER_HEARTBEAT_SECONDS", 15, minimum=5)

    return WebSettings(
        admin_user=_env_str("ADMIN_USER", "admin"),
        admin_pass=_env_str("ADMIN_PASS", "admin"),
        bot_token=_env_str("BOT_TOKEN", ""),
        bot_username=_env_str("BOT_USERNAME", "checkayugrambot").lstrip("@") or "checkayugrambot",
        admin_test_chat_id=_env_str("ADMIN_TEST_CHAT_ID", ""),
        session_secret=session_secret,
        session_max_age_seconds=_env_int("SESSION_MAX_AGE_SECONDS", 60 * 60 * 24 * 30),
        max_broadcast_media_bytes=_env_int("MAX_BROADCAST_MEDIA_BYTES", 45 * 1024 * 1024),
        admin_base_path_raw=_env_str("ADMIN_BASE_PATH", ""),
        helper_api_key=helper_api_key,
        helper_ticket_ttl_seconds=_env_int("HELPER_TICKET_TTL_SECONDS", 120),
        instagram_app_helper_open_url=_env_str(
            "INSTAGRAM_APP_HELPER_OPEN_URL",
            _env_str("INSTAGRAM_HELPER_OPEN_URL", "http://127.0.0.1:17374/open") or "http://127.0.0.1:17374/open",
        )
        or "http://127.0.0.1:17374/open",
        instagram_publish_source_dir=_env_str("INSTAGRAM_PUBLISH_SOURCE_DIR", default_publish_source_dir)
        or default_publish_source_dir,
        publish_n8n_webhook_url=_env_str("PUBLISH_N8N_WEBHOOK_URL", ""),
        publish_staging_dir=_env_str("PUBLISH_STAGING_DIR", str(Path.home() / "SlezhkaPublishStaging"))
        or str(Path.home() / "SlezhkaPublishStaging"),
        publish_base_url=_env_str("PUBLISH_BASE_URL", "").rstrip("/"),
        publish_shared_secret=publish_shared_secret,
        publish_webhook_max_age_seconds=_env_int("PUBLISH_WEBHOOK_MAX_AGE_SECONDS", 300),
        publish_factory_timeout_seconds=_env_int("PUBLISH_FACTORY_TIMEOUT_SECONDS", 900),
        publish_runner_api_key=publish_runner_api_key,
        publish_runner_lease_seconds=_env_int("PUBLISH_RUNNER_LEASE_SECONDS", 900),
        publish_default_workflow=_env_str("PUBLISH_DEFAULT_WORKFLOW", "default") or "default",
        mail_collector_enabled=_env_bool("MAIL_COLLECTOR_ENABLED", True),
        mail_collector_reconcile_seconds=_env_int("MAIL_COLLECTOR_RECONCILE_SECONDS", 60, minimum=15),
        mail_collector_stale_sync_seconds=_env_int("MAIL_COLLECTOR_STALE_SYNC_SECONDS", 10 * 60, minimum=30),
        mail_collector_watch_renew_margin_seconds=_env_int(
            "MAIL_COLLECTOR_WATCH_RENEW_MARGIN_SECONDS", 15 * 60, minimum=60
        ),
        mail_webhook_secret=_env_str("MAIL_WEBHOOK_SECRET", publish_shared_secret or helper_api_key or session_secret),
        strict_config=_env_bool("STRICT_CONFIG", False),
        accounts_import_max_bytes=_env_int("ACCOUNTS_IMPORT_MAX_BYTES", 2 * 1024 * 1024),
        instagram_audit_poll_interval_seconds=_env_int("INSTAGRAM_AUDIT_POLL_INTERVAL_SECONDS", 2, minimum=2),
        instagram_audit_helper_poll_seconds=_env_int("INSTAGRAM_AUDIT_HELPER_POLL_SECONDS", 4, minimum=2),
        instagram_audit_helper_idle_timeout_seconds=_env_int(
            "INSTAGRAM_AUDIT_HELPER_IDLE_TIMEOUT_SECONDS", 180, minimum=30
        ),
        instagram_audit_login_timeout_seconds=_env_int("INSTAGRAM_AUDIT_LOGIN_TIMEOUT_SECONDS", 240, minimum=30),
        instagram_audit_mail_freshness_seconds=_env_int(
            "INSTAGRAM_AUDIT_MAIL_FRESHNESS_SECONDS", 30 * 60, minimum=300
        ),
        instagram_mail_challenge_timeout_seconds=_env_int("INSTAGRAM_MAIL_CHALLENGE_TIMEOUT_SECONDS", 90, minimum=15),
        instagram_mail_challenge_poll_seconds=_env_int("INSTAGRAM_MAIL_CHALLENGE_POLL_SECONDS", 10, minimum=5),
        instagram_mail_challenge_lookback_seconds=_env_int(
            "INSTAGRAM_MAIL_CHALLENGE_LOOKBACK_SECONDS", 120, minimum=120
        ),
        instagram_mail_challenge_freshness_seconds=_env_int(
            "INSTAGRAM_MAIL_CHALLENGE_FRESHNESS_SECONDS", 900, minimum=300
        ),
        instagram_audit_item_retry_attempts=_env_int("INSTAGRAM_AUDIT_ITEM_RETRY_ATTEMPTS", 3, minimum=1),
        instagram_audit_force_clean_login=_env_bool("INSTAGRAM_AUDIT_FORCE_CLEAN_LOGIN", False),
        runtime_task_lease_seconds=_env_int("RUNTIME_TASK_LEASE_SECONDS", 300, minimum=60),
        runtime_worker_heartbeat_seconds=runtime_worker_heartbeat_seconds,
        runtime_task_retry_delay_seconds=_env_int("RUNTIME_TASK_RETRY_DELAY_SECONDS", 30, minimum=10),
        runtime_reconcile_interval_seconds=_env_int("RUNTIME_RECONCILE_INTERVAL_SECONDS", 60, minimum=30),
        runtime_worker_live_timeout_seconds=_env_int(
            "RUNTIME_WORKER_LIVE_TIMEOUT_SECONDS", 45, minimum=runtime_worker_heartbeat_seconds * 2
        ),
        runtime_worker_name=_env_str("RUNTIME_WORKER_NAME", "runtime-local-worker") or "runtime-local-worker",
        runtime_worker_idle_poll_seconds=_env_float("RUNTIME_WORKER_IDLE_POLL_SECONDS", 0.5, minimum=0.1),
        embed_runtime_worker=_env_bool("EMBED_RUNTIME_WORKER", True),
    )


def load_helper_settings() -> HelperSettings:
    helper_api_key = _env_str("HELPER_API_KEY", "")
    default_publish_source_dir = _default_publish_source_dir()

    return HelperSettings(
        helper_bind=_env_str("INSTAGRAM_APP_HELPER_BIND", "127.0.0.1:17374") or "127.0.0.1:17374",
        slezhka_admin_base_url=_env_str("SLEZHKA_ADMIN_BASE_URL", "http://4abbf189760e.vps.myjino.ru/slezhka").rstrip("/"),
        helper_api_key=helper_api_key,
        android_avd_name=_env_str("ANDROID_AVD_NAME", ""),
        adb_path_raw=_env_str("ADB_PATH", ""),
        emulator_path_raw=_env_str("EMULATOR_PATH", ""),
        emulator_no_window=_env_bool("INSTAGRAM_APP_EMULATOR_NO_WINDOW", False),
        instagram_package=_env_str("INSTAGRAM_ANDROID_PACKAGE", "com.instagram.android") or "com.instagram.android",
        instagram_publish_source_dir=_env_str("INSTAGRAM_PUBLISH_SOURCE_DIR", default_publish_source_dir)
        or default_publish_source_dir,
        instagram_publish_media_dir=_env_str("INSTAGRAM_PUBLISH_MEDIA_DIR", "/sdcard/Movies/Videoogram")
        or "/sdcard/Movies/Videoogram",
        publish_runner_cache_dir=_env_str(
            "PUBLISH_RUNNER_CACHE_DIR",
            str(Path.home() / "Library" / "Caches" / "SlezhkaHelper" / "publish_jobs"),
        )
        or str(Path.home() / "Library" / "Caches" / "SlezhkaHelper" / "publish_jobs"),
        publish_runner_downloads_dir=_env_str(
            "PUBLISH_RUNNER_DOWNLOADS_DIR",
            str(Path.home() / "Downloads" / "SlezhkaPublishArtifacts"),
        )
        or str(Path.home() / "Downloads" / "SlezhkaPublishArtifacts"),
        publish_diagnostics_dir=_env_str(
            "PUBLISH_DIAGNOSTICS_DIR",
            str(Path.home() / "Downloads" / "SlezhkaPublishDiagnostics"),
        )
        or str(Path.home() / "Downloads" / "SlezhkaPublishDiagnostics"),
        strict_config=_env_bool("STRICT_CONFIG", False),
        emulator_stabilize_seconds=_env_int("INSTAGRAM_APP_EMULATOR_STABILIZE_SECONDS", 12),
        use_emulator_snapshots=_env_bool("INSTAGRAM_APP_USE_SNAPSHOTS", False),
        publish_runner_enabled=_env_bool("PUBLISH_RUNNER_ENABLED", False),
        publish_runner_poll_seconds=_env_int("PUBLISH_RUNNER_POLL_SECONDS", 10, minimum=3),
        publish_success_wait_seconds=_env_int("PUBLISH_SUCCESS_WAIT_SECONDS", 1200, minimum=60),
        publish_heartbeat_seconds=_env_int("PUBLISH_HEARTBEAT_SECONDS", 10, minimum=5),
        publish_upload_start_wait_seconds=_env_int("PUBLISH_UPLOAD_START_WAIT_SECONDS", 90, minimum=20),
        publish_profile_verify_start_delay_seconds=_env_int(
            "PUBLISH_PROFILE_VERIFY_START_DELAY_SECONDS", 10 * 60, minimum=60
        ),
        publish_profile_verify_seconds=_env_int("PUBLISH_PROFILE_VERIFY_SECONDS", 30 * 60, minimum=10 * 60),
        publish_profile_verify_interval_seconds=_env_int("PUBLISH_PROFILE_VERIFY_INTERVAL_SECONDS", 30, minimum=10),
        publish_profile_freshness_seconds=_env_int("PUBLISH_PROFILE_FRESHNESS_SECONDS", 30 * 60, minimum=5 * 60),
        publish_profile_check_slots=_env_int("PUBLISH_PROFILE_CHECK_SLOTS", 3, minimum=1),
        publish_profile_baseline_slots=_env_int("PUBLISH_PROFILE_BASELINE_SLOTS", 1, minimum=1),
        publish_runner_name=_env_str("PUBLISH_RUNNER_NAME", "instagram-app-helper-runner")
        or "instagram-app-helper-runner",
        publish_runner_api_key=_env_str("PUBLISH_RUNNER_API_KEY", helper_api_key) or helper_api_key,
        serial_to_avd_map_raw=_env_str("INSTAGRAM_RUNNER_SERIAL_TO_AVD_JSON", ""),
        instagram_mail_challenge_timeout_seconds=_env_int("INSTAGRAM_MAIL_CHALLENGE_TIMEOUT_SECONDS", 90, minimum=15),
        instagram_mail_challenge_retry_seconds=_env_int("INSTAGRAM_MAIL_CHALLENGE_RETRY_SECONDS", 30, minimum=10),
        instagram_mail_challenge_resend_wait_seconds=_env_int(
            "INSTAGRAM_MAIL_CHALLENGE_RESEND_WAIT_SECONDS", 8, minimum=5
        ),
    )
