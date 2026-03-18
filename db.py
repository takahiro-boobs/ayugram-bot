import json
import os
import sqlite3
import time
import hashlib
import re
import secrets
from typing import Any, Dict, List, Optional

from twofa_utils import is_valid_twofa_secret, normalize_twofa_secret

DB_PATH = os.getenv("ADMIN_DB_PATH", "admin.db")
MODEL_COMMISSION_PCT = float(os.getenv("MODEL_COMMISSION_PCT", "25"))
MANAGER_COMMISSION_PCT = float(os.getenv("MANAGER_COMMISSION_PCT", "25"))
ACCOUNT_TYPE_KEYS = {"youtube", "tiktok", "instagram"}
ACCOUNT_ROTATION_STATE_KEYS = {"working", "not_working", "review"}
ACCOUNT_ROTATION_SOURCE_KEYS = {"manual", "auto"}
ACCOUNT_VIEWS_STATE_KEYS = {"low", "good", "unknown"}
ACCOUNT_MAIL_PROVIDER_KEYS = {"auto", "imap", "gmail_api", "microsoft_graph"}
ACCOUNT_MAIL_STATUS_KEYS = {"never_checked", "ok", "auth_error", "connect_error", "empty", "unsupported"}
ACCOUNT_MAIL_CHALLENGE_STATUS_KEYS = {"idle", "resolved", "not_found", "ambiguous", "mailbox_unavailable", "unsupported"}
ACCOUNT_TEXT_PLACEHOLDER_KEYS = {"NO_EMAIL", "NO MAIL", "NO_MAIL", "NONE", "NULL", "N/A", "NA", "-", "—"}
HELPER_TICKET_TARGET_KEYS = {"instagram_login", "instagram_app_login", "instagram_audit_login", "instagram_publish_latest_reel"}
INSTAGRAM_LAUNCH_STATUS_KEYS = {
    "idle",
    "login_submitted",
    "manual_2fa_required",
    "challenge_required",
    "invalid_password",
    "helper_error",
}
INSTAGRAM_PUBLISH_STATUS_KEYS = {
    "idle",
    "preparing",
    "login_required",
    "manual_2fa_required",
    "email_code_required",
    "challenge_required",
    "invalid_password",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
    "published",
    "needs_review",
    "no_source_video",
    "publish_error",
}
INSTAGRAM_AUDIT_BATCH_STATE_KEYS = {
    "queued",
    "running",
    "completed",
    "completed_with_errors",
    "failed",
    "canceled",
}
INSTAGRAM_AUDIT_ITEM_STATE_KEYS = {
    "queued",
    "launching",
    "login_check",
    "mail_check_if_needed",
    "done",
}
INSTAGRAM_AUDIT_RESOLUTION_KEYS = {
    "login_ok",
    "manual_2fa_required",
    "email_code_required",
    "challenge_required",
    "invalid_password",
    "helper_error",
    "missing_credentials",
    "missing_device",
}
INSTAGRAM_AUDIT_MAIL_PROBE_STATE_KEYS = {
    "pending",
    "not_required",
    "checking",
    "ok",
    "empty",
    "auth_error",
    "connect_error",
    "unsupported",
    "not_configured",
}
RUNTIME_TASK_TYPE_KEYS = {
    "publish_batch_start",
    "instagram_audit_batch_run",
    "publish_reconcile",
    "instagram_audit_reconcile",
    "mail_account_sync",
}
RUNTIME_TASK_ENTITY_TYPE_KEYS = {
    "publish_batch",
    "instagram_audit_batch",
    "account",
    "system",
}
RUNTIME_TASK_STATE_KEYS = {
    "queued",
    "running",
    "retrying",
    "completed",
    "failed",
    "canceled",
}
PUBLISH_BATCH_STATE_KEYS = {
    "queued_to_worker",
    "worker_started",
    "generating",
    "publishing",
    "completed",
    "completed_needs_review",
    "completed_with_errors",
    "failed_generation",
    "canceled",
}
PUBLISH_BATCH_ACCOUNT_STATE_KEYS = {
    "queued_for_generation",
    "generating",
    "generation_failed",
    "queued_for_publish",
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
    "published",
    "needs_review",
    "failed",
    "canceled",
}
PUBLISH_JOB_STATE_KEYS = {
    "queued",
    "leased",
    "preparing",
    "importing_media",
    "opening_reel_flow",
    "selecting_media",
    "publishing",
    "published",
    "needs_review",
    "failed",
    "canceled",
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

ADMIN_FUNNEL_STEPS: List[Dict[str, Any]] = [
    {"key": "start", "label": "Нажал /start", "events": ("start",)},
    {"key": "target_entered", "label": "Ввёл username", "events": ("target_entered", "nickname_submitted")},
    {"key": "dialogs_shown", "label": "Открыл список диалогов", "events": ("dialogs_shown", "view_results")},
    {"key": "dialog_opened", "label": "Открыл диалог", "events": ("dialog_opened",)},
    {"key": "unlock_clicked", "label": "Нажал «полный диалог»", "events": ("unlock_clicked",)},
    {"key": "install_ayugram_clicked", "label": "Нажал «установить»", "events": ("install_ayugram_clicked",)},
    {"key": "manager_redirect_clicked", "label": "Перешёл к менеджеру", "events": ("manager_redirect_clicked",)},
]


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
    except Exception:
        pass
    return conn


def init_db() -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            installed INTEGER NOT NULL DEFAULT 0,
            installed_at INTEGER,
            ref_code TEXT,
            partner_id INTEGER,
            manager_id INTEGER,
            attributed_at INTEGER,
            created_at INTEGER,
            last_seen INTEGER
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            amount_rub REAL NOT NULL,
            note TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS earning_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payment_id INTEGER NOT NULL UNIQUE,
            user_id INTEGER NOT NULL,
            partner_id INTEGER,
            manager_id INTEGER,
            partner_amount_rub REAL NOT NULL,
            manager_amount_rub REAL NOT NULL,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_payments_created_at ON payments(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_earning_lines_partner_id ON earning_lines(partner_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_earning_lines_manager_id ON earning_lines(manager_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_earning_lines_user_id ON earning_lines(user_id)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_type TEXT NOT NULL,
            meta_json TEXT,
            code TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_user_type_time ON events(user_id, event_type, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_events_type_time ON events(event_type, created_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS links (
            code TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            target_url TEXT NOT NULL,
            partner_id INTEGER,
            account_id INTEGER,
            is_active INTEGER NOT NULL DEFAULT 1,
            is_deleted INTEGER NOT NULL DEFAULT 0,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS clicks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT NOT NULL,
            partner_id INTEGER,
            account_id INTEGER,
            user_agent TEXT,
            ip TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clicks_code_time ON clicks(code, created_at)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS partners (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            pass_hash TEXT NOT NULL,
            pass_salt TEXT NOT NULL,
            plain_password TEXT,
            earned_rub REAL NOT NULL DEFAULT 0,
            manager_id INTEGER,
            approved INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS managers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            pass_hash TEXT NOT NULL,
            pass_salt TEXT NOT NULL,
            plain_password TEXT,
            paid_out_rub REAL NOT NULL DEFAULT 0,
            approved INTEGER NOT NULL DEFAULT 1,
            created_at INTEGER NOT NULL
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS manager_invites (
            code TEXT PRIMARY KEY,
            manager_id INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            used_at INTEGER,
            used_partner_id INTEGER,
            uses_count INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_manager_invites_manager_id ON manager_invites(manager_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_manager_invites_used_at ON manager_invites(used_at)")

    # migrations for existing tables
    cur.execute("PRAGMA table_info(manager_invites)")
    inv_cols = [row["name"] for row in cur.fetchall()]
    if "uses_count" not in inv_cols:
        cur.execute("ALTER TABLE manager_invites ADD COLUMN uses_count INTEGER NOT NULL DEFAULT 0")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS broadcasts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at INTEGER NOT NULL,
            scope TEXT NOT NULL,
            partner_id INTEGER,
            manager_id INTEGER,
            recipients INTEGER NOT NULL DEFAULT 0,
            sent INTEGER NOT NULL DEFAULT 0,
            failed INTEGER NOT NULL DEFAULT 0,
            is_test INTEGER NOT NULL DEFAULT 0,
            message TEXT NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_broadcasts_created_at ON broadcasts(created_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            account_login TEXT NOT NULL,
            account_password TEXT NOT NULL,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            email_password TEXT NOT NULL,
            proxy TEXT,
            twofa TEXT,
            rotation_state TEXT NOT NULL DEFAULT 'review',
            views_state TEXT NOT NULL DEFAULT 'unknown',
            mail_provider TEXT NOT NULL DEFAULT 'auto',
            mail_auth_json TEXT NOT NULL DEFAULT '',
            mail_status TEXT NOT NULL DEFAULT 'never_checked',
            mail_last_checked_at INTEGER,
            mail_last_synced_at INTEGER,
            mail_last_error TEXT,
            mail_watch_json TEXT NOT NULL DEFAULT '',
            mail_challenge_status TEXT NOT NULL DEFAULT 'idle',
            mail_challenge_kind TEXT NOT NULL DEFAULT '',
            mail_challenge_reason_code TEXT NOT NULL DEFAULT '',
            mail_challenge_reason_text TEXT NOT NULL DEFAULT '',
            mail_challenge_message_uid TEXT NOT NULL DEFAULT '',
            mail_challenge_received_at INTEGER,
            mail_challenge_masked_code TEXT NOT NULL DEFAULT '',
            mail_challenge_confidence REAL NOT NULL DEFAULT 0,
            mail_challenge_updated_at INTEGER,
            instagram_emulator_serial TEXT,
            instagram_launch_status TEXT NOT NULL DEFAULT 'idle',
            instagram_launch_detail TEXT,
            instagram_launch_updated_at INTEGER,
            instagram_publish_status TEXT NOT NULL DEFAULT 'idle',
            instagram_publish_detail TEXT,
            instagram_publish_updated_at INTEGER,
            instagram_publish_last_file TEXT,
            owner_worker_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_type ON accounts(type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_username ON accounts(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_created_at ON accounts(created_at)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS workers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            username TEXT NOT NULL UNIQUE,
            pass_hash TEXT NOT NULL,
            pass_salt TEXT NOT NULL,
            plain_password TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_workers_username ON workers(username)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_workers_created_at ON workers(created_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS account_claim_requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            requested_by_worker_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_account_claim_requests_status ON account_claim_requests(status, created_at)")
    cur.execute(
        "CREATE INDEX IF NOT EXISTS idx_account_claim_requests_account_worker ON account_claim_requests(account_id, requested_by_worker_id, status)"
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS helper_launch_tickets (
            ticket TEXT PRIMARY KEY,
            account_id INTEGER NOT NULL,
            target TEXT NOT NULL,
            created_at INTEGER NOT NULL,
            expires_at INTEGER NOT NULL,
            used_at INTEGER,
            created_by_admin TEXT
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_helper_launch_tickets_expires_at ON helper_launch_tickets(expires_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_helper_launch_tickets_account_target ON helper_launch_tickets(account_id, target, used_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS account_mail_messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            account_id INTEGER NOT NULL,
            message_uid TEXT NOT NULL,
            from_text TEXT NOT NULL,
            subject TEXT NOT NULL,
            received_at INTEGER,
            snippet TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_account_mail_messages_uid ON account_mail_messages(account_id, message_uid)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_account_mail_messages_received ON account_mail_messages(account_id, received_at DESC, id DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instagram_audit_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL DEFAULT 'queued',
            detail TEXT NOT NULL DEFAULT '',
            selected_accounts INTEGER NOT NULL DEFAULT 0,
            created_by_admin TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            started_at INTEGER,
            completed_at INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instagram_audit_batches_state ON instagram_audit_batches(state, updated_at DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instagram_audit_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            audit_batch_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            queue_position INTEGER NOT NULL DEFAULT 0,
            item_state TEXT NOT NULL DEFAULT 'queued',
            assigned_serial TEXT NOT NULL DEFAULT '',
            login_state TEXT NOT NULL DEFAULT '',
            login_detail TEXT NOT NULL DEFAULT '',
            mail_probe_state TEXT NOT NULL DEFAULT 'pending',
            mail_probe_detail TEXT NOT NULL DEFAULT '',
            resolution_state TEXT NOT NULL DEFAULT '',
            resolution_detail TEXT NOT NULL DEFAULT '',
            diagnostic_path TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            started_at INTEGER,
            completed_at INTEGER
        )
        """
    )
    cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_instagram_audit_items_batch_account ON instagram_audit_items(audit_batch_id, account_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instagram_audit_items_batch_state ON instagram_audit_items(audit_batch_id, item_state, queue_position)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instagram_audit_items_account_id ON instagram_audit_items(account_id, updated_at DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS instagram_audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            audit_batch_id INTEGER NOT NULL,
            audit_item_id INTEGER,
            account_id INTEGER,
            state TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_instagram_audit_events_batch_created ON instagram_audit_events(audit_batch_id, created_at DESC, id DESC)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            natural_key TEXT NOT NULL UNIQUE,
            task_type TEXT NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            state TEXT NOT NULL DEFAULT 'queued',
            payload_json TEXT NOT NULL DEFAULT '{}',
            lease_owner TEXT,
            lease_expires_at INTEGER,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            max_attempts INTEGER NOT NULL DEFAULT 3,
            last_error TEXT NOT NULL DEFAULT '',
            available_at INTEGER NOT NULL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            started_at INTEGER,
            completed_at INTEGER,
            last_heartbeat_at INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_tasks_state_available ON runtime_tasks(state, available_at, created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_tasks_entity ON runtime_tasks(entity_type, entity_id, task_type)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_tasks_lease ON runtime_tasks(state, lease_expires_at)")
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS runtime_workers (
            worker_name TEXT PRIMARY KEY,
            current_task_id INTEGER,
            last_heartbeat_at INTEGER,
            last_error TEXT NOT NULL DEFAULT '',
            first_seen_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_runtime_workers_heartbeat ON runtime_workers(last_heartbeat_at, updated_at)")

    cur.execute("PRAGMA table_info(accounts)")
    account_cols = [row["name"] for row in cur.fetchall()]
    if "instagram_launch_status" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_launch_status TEXT NOT NULL DEFAULT 'idle'")
    if "instagram_launch_detail" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_launch_detail TEXT")
    if "instagram_launch_updated_at" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_launch_updated_at INTEGER")
    if "instagram_publish_status" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_publish_status TEXT NOT NULL DEFAULT 'idle'")
    if "instagram_publish_detail" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_publish_detail TEXT")
    if "instagram_publish_updated_at" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_publish_updated_at INTEGER")
    if "instagram_publish_last_file" not in account_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_publish_last_file TEXT")

    cur.execute("PRAGMA table_info(users)")
    users_cols = [row["name"] for row in cur.fetchall()]
    if "installed" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN installed INTEGER NOT NULL DEFAULT 0")
    if "installed_at" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN installed_at INTEGER")
    if "ref_code" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN ref_code TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_users_ref_code ON users(ref_code)")
    if "partner_id" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN partner_id INTEGER")
    if "manager_id" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN manager_id INTEGER")
    if "attributed_at" not in users_cols:
        cur.execute("ALTER TABLE users ADD COLUMN attributed_at INTEGER")

    # one-time migration: old "paid_rub"/"paid_at" columns -> payments rows
    if "paid_rub" in users_cols:
        try:
            cur.execute(
                """
                SELECT user_id, paid_rub, paid_at
                FROM users
                WHERE paid_rub IS NOT NULL AND paid_rub > 0
                """
            )
            for row in cur.fetchall():
                user_id = int(row[0])
                amt = float(row[1] or 0)
                paid_at = int(row[2] or 0) if row[2] is not None else 0
                if amt <= 0:
                    continue
                cur.execute("SELECT COUNT(*) FROM payments WHERE user_id = ?", (user_id,))
                if int(cur.fetchone()[0]) > 0:
                    continue
                cur.execute(
                    "INSERT INTO payments (user_id, amount_rub, note, created_at) VALUES (?, ?, ?, ?)",
                    (user_id, amt, "migrated", paid_at or int(time.time())),
                )
        except Exception:
            # best-effort migration; safe to ignore if schema differs
            pass

    cur.execute("PRAGMA table_info(events)")
    if "code" not in [row["name"] for row in cur.fetchall()]:
        cur.execute("ALTER TABLE events ADD COLUMN code TEXT")

    cur.execute("PRAGMA table_info(links)")
    links_cols = [row["name"] for row in cur.fetchall()]
    if "partner_id" not in links_cols:
        cur.execute("ALTER TABLE links ADD COLUMN partner_id INTEGER")
    if "account_id" not in links_cols:
        cur.execute("ALTER TABLE links ADD COLUMN account_id INTEGER")
    if "is_active" not in links_cols:
        cur.execute("ALTER TABLE links ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
    if "is_deleted" not in links_cols:
        cur.execute("ALTER TABLE links ADD COLUMN is_deleted INTEGER NOT NULL DEFAULT 0")
    if "updated_at" not in links_cols:
        cur.execute("ALTER TABLE links ADD COLUMN updated_at INTEGER")
    cur.execute(
        "UPDATE links SET updated_at = COALESCE(updated_at, created_at, ?) WHERE updated_at IS NULL OR updated_at = 0",
        (int(time.time()),),
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_links_account_active ON links(account_id, is_deleted, is_active, created_at)")

    cur.execute("PRAGMA table_info(clicks)")
    clicks_cols = [row["name"] for row in cur.fetchall()]
    if "partner_id" not in clicks_cols:
        cur.execute("ALTER TABLE clicks ADD COLUMN partner_id INTEGER")
    if "account_id" not in clicks_cols:
        cur.execute("ALTER TABLE clicks ADD COLUMN account_id INTEGER")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_clicks_code_time ON clicks(code, created_at)")

    cur.execute("PRAGMA table_info(accounts)")
    accounts_cols = [row["name"] for row in cur.fetchall()]
    if "twofa" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN twofa TEXT")
    if "rotation_state" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN rotation_state TEXT NOT NULL DEFAULT 'review'")
    if "rotation_state_source" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN rotation_state_source TEXT NOT NULL DEFAULT 'manual'")
    if "rotation_state_reason" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN rotation_state_reason TEXT NOT NULL DEFAULT ''")
    if "views_state" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN views_state TEXT NOT NULL DEFAULT 'unknown'")
    if "mail_provider" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_provider TEXT NOT NULL DEFAULT 'auto'")
    if "mail_auth_json" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_auth_json TEXT NOT NULL DEFAULT ''")
    if "mail_status" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_status TEXT NOT NULL DEFAULT 'never_checked'")
    if "mail_last_checked_at" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_last_checked_at INTEGER")
    if "mail_last_synced_at" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_last_synced_at INTEGER")
    if "mail_last_error" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_last_error TEXT")
    if "mail_watch_json" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_watch_json TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_status" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_status TEXT NOT NULL DEFAULT 'idle'")
    if "mail_challenge_kind" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_kind TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_reason_code" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_reason_code TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_reason_text" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_reason_text TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_message_uid" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_message_uid TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_received_at" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_received_at INTEGER")
    if "mail_challenge_masked_code" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_masked_code TEXT NOT NULL DEFAULT ''")
    if "mail_challenge_confidence" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_confidence REAL NOT NULL DEFAULT 0")
    if "mail_challenge_updated_at" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN mail_challenge_updated_at INTEGER")
    if "instagram_emulator_serial" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN instagram_emulator_serial TEXT")
    if "owner_worker_id" not in accounts_cols:
        cur.execute("ALTER TABLE accounts ADD COLUMN owner_worker_id INTEGER")
    cur.execute("UPDATE accounts SET rotation_state = 'review' WHERE rotation_state IS NULL OR TRIM(rotation_state) = ''")
    cur.execute("UPDATE accounts SET rotation_state_source = 'manual' WHERE rotation_state_source IS NULL OR TRIM(rotation_state_source) = ''")
    cur.execute("UPDATE accounts SET rotation_state_reason = '' WHERE rotation_state_reason IS NULL")
    cur.execute("UPDATE accounts SET views_state = 'unknown' WHERE views_state IS NULL OR TRIM(views_state) = ''")
    cur.execute("UPDATE accounts SET mail_provider = 'auto' WHERE mail_provider IS NULL OR TRIM(mail_provider) = ''")
    cur.execute("UPDATE accounts SET mail_auth_json = '' WHERE mail_auth_json IS NULL")
    cur.execute("UPDATE accounts SET mail_status = 'never_checked' WHERE mail_status IS NULL OR TRIM(mail_status) = ''")
    cur.execute("UPDATE accounts SET mail_watch_json = '' WHERE mail_watch_json IS NULL")
    cur.execute("UPDATE accounts SET mail_challenge_status = 'idle' WHERE mail_challenge_status IS NULL OR TRIM(mail_challenge_status) = ''")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_owner_worker_id ON accounts(owner_worker_id, updated_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_rotation_state ON accounts(rotation_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_rotation_state_source ON accounts(rotation_state_source)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_views_state ON accounts(views_state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_mail_provider ON accounts(mail_provider)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_mail_status ON accounts(mail_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_mail_challenge_status ON accounts(mail_challenge_status)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_accounts_instagram_emulator_serial ON accounts(instagram_emulator_serial)")

    cur.execute("PRAGMA table_info(account_mail_messages)")
    account_mail_cols = [row["name"] for row in cur.fetchall()]
    if "metadata_json" not in account_mail_cols:
        cur.execute("ALTER TABLE account_mail_messages ADD COLUMN metadata_json TEXT NOT NULL DEFAULT ''")
    cur.execute("UPDATE account_mail_messages SET metadata_json = '' WHERE metadata_json IS NULL")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_batches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            state TEXT NOT NULL DEFAULT 'generating',
            detail TEXT,
            workflow_key TEXT NOT NULL DEFAULT 'default',
            created_by_admin TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            generation_started_at INTEGER,
            generation_completed_at INTEGER,
            completed_at INTEGER,
            canceled_at INTEGER
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_batches_created_at ON publish_batches(created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_batches_state ON publish_batches(state)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_batch_accounts (
            batch_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            queue_position INTEGER NOT NULL DEFAULT 0,
            state TEXT NOT NULL DEFAULT 'queued_for_generation',
            detail TEXT,
            artifact_id INTEGER,
            job_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (batch_id, account_id)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_artifacts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            path TEXT NOT NULL,
            filename TEXT NOT NULL,
            checksum TEXT,
            size_bytes INTEGER,
            duration_seconds REAL,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (batch_id, path)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_artifacts_batch_id ON publish_artifacts(batch_id)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            artifact_id INTEGER NOT NULL,
            account_id INTEGER NOT NULL,
            emulator_serial TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'queued',
            detail TEXT,
            source_path TEXT NOT NULL,
            source_name TEXT NOT NULL,
            leased_by TEXT,
            leased_at INTEGER,
            lease_expires_at INTEGER,
            started_at INTEGER,
            completed_at INTEGER,
            last_file TEXT,
            last_error TEXT,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            UNIQUE (batch_id, artifact_id, account_id)
        )
        """
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_jobs_batch_state ON publish_jobs(batch_id, state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_jobs_emulator_state ON publish_jobs(emulator_serial, state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_jobs_account_id ON publish_jobs(account_id)")

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS publish_job_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id INTEGER NOT NULL,
            job_id INTEGER,
            account_id INTEGER,
            state TEXT NOT NULL,
            detail TEXT,
            payload_json TEXT,
            event_hash TEXT,
            created_at INTEGER NOT NULL
        )
        """
    )

    cur.execute("PRAGMA table_info(publish_batch_accounts)")
    publish_batch_accounts_cols = [row["name"] for row in cur.fetchall()]
    if "state" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN state TEXT NOT NULL DEFAULT 'queued_for_generation'")
    if "queue_position" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN queue_position INTEGER")
    if "detail" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN detail TEXT")
    if "artifact_id" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN artifact_id INTEGER")
    if "job_id" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN job_id INTEGER")
    if "updated_at" not in publish_batch_accounts_cols:
        cur.execute("ALTER TABLE publish_batch_accounts ADD COLUMN updated_at INTEGER")
    cur.execute(
        """
        UPDATE publish_batch_accounts
        SET state = COALESCE(NULLIF(TRIM(state), ''), 'queued_for_generation'),
            queue_position = COALESCE(
                queue_position,
                (
                    SELECT COUNT(*)
                    FROM publish_batch_accounts pba_prev
                    WHERE pba_prev.batch_id = publish_batch_accounts.batch_id
                      AND (
                        COALESCE(pba_prev.created_at, 0) < COALESCE(publish_batch_accounts.created_at, 0)
                        OR (
                            COALESCE(pba_prev.created_at, 0) = COALESCE(publish_batch_accounts.created_at, 0)
                            AND pba_prev.account_id <= publish_batch_accounts.account_id
                        )
                      )
                ) - 1
            ),
            updated_at = COALESCE(updated_at, created_at, ?)
        """,
        (int(time.time()),),
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_batch_accounts_account_id ON publish_batch_accounts(account_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_batch_accounts_batch_state ON publish_batch_accounts(batch_id, state)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_batch_accounts_batch_queue ON publish_batch_accounts(batch_id, queue_position, account_id)")

    cur.execute("PRAGMA table_info(publish_job_events)")
    publish_job_events_cols = [row["name"] for row in cur.fetchall()]
    if "account_id" not in publish_job_events_cols:
        cur.execute("ALTER TABLE publish_job_events ADD COLUMN account_id INTEGER")
    if "event_hash" not in publish_job_events_cols:
        cur.execute("ALTER TABLE publish_job_events ADD COLUMN event_hash TEXT")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_job_events_batch_id ON publish_job_events(batch_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_job_events_job_id ON publish_job_events(job_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_job_events_batch_account ON publish_job_events(batch_id, account_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_job_events_account_id ON publish_job_events(account_id, created_at DESC)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_publish_job_events_event_hash ON publish_job_events(event_hash)")

    cur.execute("PRAGMA table_info(instagram_audit_items)")
    instagram_audit_items_cols = [row["name"] for row in cur.fetchall()]
    if instagram_audit_items_cols:
        if "resolution_detail" not in instagram_audit_items_cols:
            cur.execute("ALTER TABLE instagram_audit_items ADD COLUMN resolution_detail TEXT NOT NULL DEFAULT ''")
        if "diagnostic_path" not in instagram_audit_items_cols:
            cur.execute("ALTER TABLE instagram_audit_items ADD COLUMN diagnostic_path TEXT NOT NULL DEFAULT ''")
        cur.execute(
            """
            UPDATE instagram_audit_items
            SET item_state = COALESCE(NULLIF(TRIM(item_state), ''), 'queued'),
                mail_probe_state = COALESCE(NULLIF(TRIM(mail_probe_state), ''), 'pending'),
                assigned_serial = COALESCE(assigned_serial, ''),
                login_state = COALESCE(login_state, ''),
                login_detail = COALESCE(login_detail, ''),
                mail_probe_detail = COALESCE(mail_probe_detail, ''),
                resolution_state = COALESCE(resolution_state, ''),
                resolution_detail = COALESCE(resolution_detail, ''),
                diagnostic_path = COALESCE(diagnostic_path, '')
            """
        )

    cur.execute("PRAGMA table_info(partners)")
    partners_cols = [row["name"] for row in cur.fetchall()]
    if "earned_rub" not in partners_cols:
        cur.execute("ALTER TABLE partners ADD COLUMN earned_rub REAL NOT NULL DEFAULT 0")
    if "manager_id" not in partners_cols:
        cur.execute("ALTER TABLE partners ADD COLUMN manager_id INTEGER")
    if "approved" not in partners_cols:
        cur.execute("ALTER TABLE partners ADD COLUMN approved INTEGER NOT NULL DEFAULT 1")
    if "plain_password" not in partners_cols:
        cur.execute("ALTER TABLE partners ADD COLUMN plain_password TEXT")

    cur.execute("PRAGMA table_info(managers)")
    managers_cols = [row["name"] for row in cur.fetchall()]
    if "approved" not in managers_cols:
        cur.execute("ALTER TABLE managers ADD COLUMN approved INTEGER NOT NULL DEFAULT 1")
    if "plain_password" not in managers_cols:
        cur.execute("ALTER TABLE managers ADD COLUMN plain_password TEXT")

    conn.commit()
    conn.close()

    # Ensure existing managers/partners have a visible password. (We store plain_password by request.)
    # For legacy rows with NULL password, we generate one and update hash+salt accordingly.
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT id FROM managers WHERE plain_password IS NULL OR plain_password = ''")
        mids = [int(r[0]) for r in cur.fetchall()]
        for mid in mids:
            pw = secrets.token_urlsafe(10).replace("-", "").replace("_", "")[:12]
            hp = _hash_password(pw)
            cur.execute(
                "UPDATE managers SET plain_password = ?, pass_hash = ?, pass_salt = ? WHERE id = ?",
                (pw, hp["hash"], hp["salt"], int(mid)),
            )

        cur.execute("SELECT id FROM partners WHERE plain_password IS NULL OR plain_password = ''")
        pids = [int(r[0]) for r in cur.fetchall()]
        for pid in pids:
            pw = secrets.token_urlsafe(10).replace("-", "").replace("_", "")[:12]
            hp = _hash_password(pw)
            cur.execute(
                "UPDATE partners SET plain_password = ?, pass_hash = ?, pass_salt = ? WHERE id = ?",
                (pw, hp["hash"], hp["salt"], int(pid)),
            )
        conn.commit()
        conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass

    # Backfill/refresh earning lines for payments (best-effort, idempotent).
    try:
        conn = _connect()
        cur = conn.cursor()
        # Backfill missing rows AND refresh rows where attribution changed later.
        cur.execute(
            """
            SELECT p.id
            FROM payments p
            JOIN users u ON u.user_id = p.user_id
            LEFT JOIN earning_lines el ON el.payment_id = p.id
            WHERE el.payment_id IS NULL
               OR (u.partner_id IS NOT NULL AND (el.partner_id IS NULL OR el.partner_id != u.partner_id))
               OR (u.manager_id IS NOT NULL AND (el.manager_id IS NULL OR el.manager_id != u.manager_id))
            ORDER BY p.id ASC
            """
        )
        payment_ids = [int(r[0]) for r in cur.fetchall()]
        conn.close()
        for pid in payment_ids:
            upsert_earning_line_for_payment(pid)
    except Exception:
        pass


def upsert_user(user_id: int, username: Optional[str], first_name: str, last_name: Optional[str]) -> None:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO users (user_id, username, first_name, last_name, created_at, last_seen)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_name=excluded.last_name,
            last_seen=excluded.last_seen
        """,
        (user_id, username, first_name, last_name, now, now),
    )
    conn.commit()
    conn.close()


def log_event(
    user_id: Optional[int],
    event_type: str,
    meta: Optional[Dict[str, Any]] = None,
    code: Optional[str] = None,
) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events (user_id, event_type, meta_json, code, created_at) VALUES (?, ?, ?, ?, ?)",
        (user_id, event_type, json.dumps(meta or {}, ensure_ascii=False), code, int(time.time())),
    )
    conn.commit()
    conn.close()


LINK_CODE_RE = re.compile(r"^[A-Za-z0-9_]{4,32}$")
LINK_ALPHABET = "abcdefghijklmnopqrstuvwxyz0123456789"


def _normalize_link_code(code: str) -> str:
    value = (code or "").strip()
    if not value:
        raise ValueError("empty link code")
    if not LINK_CODE_RE.fullmatch(value):
        raise ValueError("invalid link code")
    return value.lower()


def generate_link_code(length: int = 8) -> str:
    n = max(4, min(32, int(length or 8)))
    conn = _connect()
    cur = conn.cursor()
    try:
        for _ in range(400):
            code = "".join(secrets.choice(LINK_ALPHABET) for _ in range(n))
            cur.execute("SELECT 1 FROM links WHERE code = ? COLLATE NOCASE LIMIT 1", (code,))
            if cur.fetchone() is None:
                return code
    finally:
        conn.close()
    raise RuntimeError("failed to generate unique link code")


def _generate_unique_link_code_with_cursor(cur: sqlite3.Cursor, length: int = 6, attempts: int = 400) -> str:
    n = max(4, min(32, int(length or 6)))
    for _ in range(max(1, int(attempts))):
        code = "".join(secrets.choice(LINK_ALPHABET) for _ in range(n))
        cur.execute("SELECT 1 FROM links WHERE code = ? COLLATE NOCASE LIMIT 1", (code,))
        if cur.fetchone() is None:
            return code
    raise RuntimeError("failed to generate unique link code")


def create_link(
    code: str,
    name: str,
    target_url: str,
    partner_id: Optional[int] = None,
    account_id: Optional[int] = None,
    is_active: bool = True,
) -> None:
    code_clean = _normalize_link_code(code)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO links (
            code, name, target_url, partner_id, account_id, is_active, is_deleted, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?)
        """,
        (
            code_clean,
            (name or "").strip(),
            (target_url or "").strip(),
            partner_id,
            account_id,
            1 if is_active else 0,
            now,
            now,
        ),
    )
    conn.commit()
    conn.close()


def list_links() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code, name, target_url, partner_id, account_id,
               COALESCE(is_active, 1) AS is_active,
               COALESCE(is_deleted, 0) AS is_deleted,
               created_at,
               COALESCE(updated_at, created_at) AS updated_at
        FROM links
        WHERE COALESCE(is_deleted, 0) = 0
        ORDER BY created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_links_with_clicks() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            l.code,
            l.name,
            l.target_url,
            COALESCE(c.clicks, 0) AS clicks
        FROM links l
        LEFT JOIN (
            SELECT code, COUNT(*) AS clicks
            FROM clicks
            GROUP BY code
        ) c ON c.code = l.code
        WHERE COALESCE(l.is_deleted, 0) = 0
        ORDER BY l.created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_link(code: str) -> Optional[sqlite3.Row]:
    code_clean = (code or "").strip()
    if not code_clean:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code, name, target_url, partner_id, account_id,
               COALESCE(is_active, 1) AS is_active,
               COALESCE(is_deleted, 0) AS is_deleted,
               created_at,
               COALESCE(updated_at, created_at) AS updated_at
        FROM links
        WHERE code = ? COLLATE NOCASE
        LIMIT 1
        """,
        (code_clean,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_active_link(code: str) -> Optional[sqlite3.Row]:
    code_clean = (code or "").strip()
    if not code_clean:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code, name, target_url, partner_id, account_id,
               COALESCE(is_active, 1) AS is_active,
               COALESCE(is_deleted, 0) AS is_deleted
        FROM links
        WHERE code = ? COLLATE NOCASE
          AND COALESCE(is_deleted, 0) = 0
          AND COALESCE(is_active, 1) = 1
        LIMIT 1
        """,
        (code_clean,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def delete_link(code: str) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM links WHERE code = ? COLLATE NOCASE", ((code or "").strip(),))
    conn.commit()
    conn.close()


def log_click(
    code: str,
    user_agent: Optional[str],
    ip: Optional[str],
    partner_id: Optional[int] = None,
    account_id: Optional[int] = None,
) -> None:
    code_clean = (code or "").strip()
    if not code_clean:
        return
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO clicks (code, partner_id, account_id, user_agent, ip, created_at) VALUES (?, ?, ?, ?, ?, ?)",
        (code_clean, partner_id, account_id, user_agent, ip, int(time.time())),
    )
    conn.commit()
    conn.close()


def create_account_link(
    account_id: int,
    name: str,
    custom_code: Optional[str],
    target_url: str,
    owner_worker_id: Optional[int] = None,
) -> sqlite3.Row:
    account = get_account(int(account_id), owner_worker_id=owner_worker_id)
    if not account:
        raise ValueError("account not found")

    name_clean = (name or "").strip()
    if not name_clean:
        raise ValueError("name required")

    target_template = (target_url or "").strip() or "https://t.me/checkayugrambot?start={code}"

    if custom_code and custom_code.strip():
        code = _normalize_link_code(custom_code)
    else:
        code = generate_link_code(6)

    target_final = target_template.replace("{code}", code)
    now = int(time.time())

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("SELECT 1 FROM links WHERE code = ? COLLATE NOCASE LIMIT 1", (code,))
        if cur.fetchone() is not None:
            raise ValueError("code already exists")
        cur.execute(
            """
            INSERT INTO links (
                code, name, target_url, partner_id, account_id, is_active, is_deleted, created_at, updated_at
            )
            VALUES (?, ?, ?, NULL, ?, 1, 0, ?, ?)
            """,
            (code, name_clean, target_final, int(account_id), now, now),
        )
        conn.commit()
    except sqlite3.IntegrityError as exc:
        raise ValueError("code already exists") from exc
    finally:
        conn.close()

    row = get_link(code)
    if row is None:
        raise RuntimeError("link not found after create")
    return row


def list_account_links_with_stats(
    account_id: int,
    owner_worker_id: Optional[int] = None,
) -> List[sqlite3.Row]:
    if get_account(int(account_id), owner_worker_id=owner_worker_id) is None:
        return []
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            l.code,
            l.name,
            l.target_url,
            COALESCE(l.is_active, 1) AS is_active,
            COALESCE(l.is_deleted, 0) AS is_deleted,
            l.created_at,
            COALESCE(l.updated_at, l.created_at) AS updated_at,
            COALESCE(c.clicks_total, 0) AS clicks_total,
            CASE
                WHEN COALESCE(c.clicks_total, 0) > 0 THEN COALESCE(c.clicks_total, 0)
                ELSE COALESCE(s.starts_total, 0)
            END AS display_clicks_total,
            COALESCE(s.starts_total, 0) AS starts_total,
            COALESCE(s.starts_unique, 0) AS starts_unique,
            COALESCE(ft.first_touch_users, 0) AS first_touch_users,
            CASE
                WHEN COALESCE(c.clicks_total, 0) = 0 THEN 0.0
                ELSE ROUND((COALESCE(s.starts_unique, 0) * 100.0) / c.clicks_total, 2)
            END AS click_to_start_unique_pct
        FROM links l
        LEFT JOIN (
            SELECT LOWER(code) AS code_key, COUNT(*) AS clicks_total
            FROM clicks
            WHERE code IS NOT NULL AND code != ''
            GROUP BY LOWER(code)
        ) c ON c.code_key = LOWER(l.code)
        LEFT JOIN (
            SELECT
                LOWER(code) AS code_key,
                COUNT(*) AS starts_total,
                COUNT(DISTINCT CASE WHEN user_id IS NOT NULL THEN user_id END) AS starts_unique
            FROM events
            WHERE event_type = 'start'
              AND code IS NOT NULL
              AND code != ''
            GROUP BY LOWER(code)
        ) s ON s.code_key = LOWER(l.code)
        LEFT JOIN (
            SELECT LOWER(ref_code) AS code_key, COUNT(*) AS first_touch_users
            FROM users
            WHERE ref_code IS NOT NULL
              AND ref_code != ''
            GROUP BY LOWER(ref_code)
        ) ft ON ft.code_key = LOWER(l.code)
        WHERE l.account_id = ?
          AND COALESCE(l.is_deleted, 0) = 0
        ORDER BY l.created_at DESC, l.code DESC
        """,
        (int(account_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def toggle_link_active(code: str, active: bool) -> bool:
    code_clean = (code or "").strip()
    if not code_clean:
        return False
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE links
        SET is_active = ?, updated_at = ?
        WHERE code = ? COLLATE NOCASE
          AND COALESCE(is_deleted, 0) = 0
        """,
        (1 if active else 0, int(time.time()), code_clean),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def soft_delete_link(code: str) -> bool:
    code_clean = (code or "").strip()
    if not code_clean:
        return False
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE links
        SET is_deleted = 1,
            is_active = 0,
            updated_at = ?
        WHERE code = ? COLLATE NOCASE
          AND COALESCE(is_deleted, 0) = 0
        """,
        (int(time.time()), code_clean),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def stats_summary() -> Dict[str, int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM events")
    events_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM links")
    links_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM clicks")
    clicks_count = cur.fetchone()[0]
    conn.close()
    return {
        "users": users_count,
        "events": events_count,
        "links": links_count,
        "clicks": clicks_count,
    }


def list_users(limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.installed,
            u.installed_at,
            u.ref_code,
            u.partner_id,
            u.manager_id,
            u.attributed_at,
            COALESCE(p.username, '') AS partner_username,
            COALESCE(m.username, '') AS manager_username,
            COALESCE(m.name, '') AS manager_name,
            COALESCE(pay.total, 0) AS paid_total,
            COALESCE(pay.cnt, 0) AS payments_count,
            u.created_at,
            u.last_seen
        FROM users u
        LEFT JOIN partners p ON p.id = u.partner_id
        LEFT JOIN managers m ON m.id = u.manager_id
        LEFT JOIN (
            SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
            FROM payments
            GROUP BY user_id
        ) pay ON pay.user_id = u.user_id
        ORDER BY u.last_seen DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_users_by_partner(partner_id: int, limit: int = 400) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.installed,
            u.installed_at,
            u.ref_code,
            u.partner_id,
            u.manager_id,
            u.attributed_at,
            COALESCE(p.username, '') AS partner_username,
            COALESCE(m.username, '') AS manager_username,
            COALESCE(m.name, '') AS manager_name,
            COALESCE(pay.total, 0) AS paid_total,
            COALESCE(pay.cnt, 0) AS payments_count,
            u.created_at,
            u.last_seen
        FROM users u
        LEFT JOIN partners p ON p.id = u.partner_id
        LEFT JOIN managers m ON m.id = u.manager_id
        LEFT JOIN (
            SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
            FROM payments
            GROUP BY user_id
        ) pay ON pay.user_id = u.user_id
        WHERE u.partner_id = ?
        ORDER BY u.last_seen DESC
        LIMIT ?
        """,
        (int(partner_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_users_by_filter(filter_name: str, limit: int = 200) -> List[sqlite3.Row]:
    f = (filter_name or "").strip().lower()
    conn = _connect()
    cur = conn.cursor()
    if f == "no_partner":
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.installed,
                u.installed_at,
                u.ref_code,
                u.partner_id,
                u.manager_id,
                u.attributed_at,
                COALESCE(p.username, '') AS partner_username,
                COALESCE(m.username, '') AS manager_username,
                COALESCE(m.name, '') AS manager_name,
                COALESCE(pay.total, 0) AS paid_total,
                COALESCE(pay.cnt, 0) AS payments_count,
                u.created_at,
                u.last_seen
            FROM users u
            LEFT JOIN partners p ON p.id = u.partner_id
            LEFT JOIN managers m ON m.id = u.manager_id
            LEFT JOIN (
                SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
                FROM payments
                GROUP BY user_id
            ) pay ON pay.user_id = u.user_id
            WHERE u.partner_id IS NULL
            ORDER BY u.last_seen DESC
            LIMIT ?
            """,
            (int(limit),),
        )
    elif f == "no_manager":
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.installed,
                u.installed_at,
                u.ref_code,
                u.partner_id,
                u.manager_id,
                u.attributed_at,
                COALESCE(p.username, '') AS partner_username,
                COALESCE(m.username, '') AS manager_username,
                COALESCE(m.name, '') AS manager_name,
                COALESCE(pay.total, 0) AS paid_total,
                COALESCE(pay.cnt, 0) AS payments_count,
                u.created_at,
                u.last_seen
            FROM users u
            LEFT JOIN partners p ON p.id = u.partner_id
            LEFT JOIN managers m ON m.id = u.manager_id
            LEFT JOIN (
                SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
                FROM payments
                GROUP BY user_id
            ) pay ON pay.user_id = u.user_id
            WHERE u.manager_id IS NULL
            ORDER BY u.last_seen DESC
            LIMIT ?
            """,
            (int(limit),),
        )
    elif f == "paid_not_installed":
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.installed,
                u.installed_at,
                u.ref_code,
                u.partner_id,
                u.manager_id,
                u.attributed_at,
                COALESCE(p.username, '') AS partner_username,
                COALESCE(m.username, '') AS manager_username,
                COALESCE(m.name, '') AS manager_name,
                COALESCE(pay.total, 0) AS paid_total,
                COALESCE(pay.cnt, 0) AS payments_count,
                u.created_at,
                u.last_seen
            FROM users u
            JOIN (
                SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
                FROM payments
                GROUP BY user_id
            ) pay ON pay.user_id = u.user_id
            LEFT JOIN partners p ON p.id = u.partner_id
            LEFT JOIN managers m ON m.id = u.manager_id
            WHERE u.installed = 0
            ORDER BY u.last_seen DESC
            LIMIT ?
            """,
            (int(limit),),
        )
    else:
        conn.close()
        return []

    rows = cur.fetchall()
    conn.close()
    return rows


def attention_overview() -> Dict[str, int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM managers WHERE approved = 0")
    pending_managers = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM partners WHERE approved = 0")
    pending_partners = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM partners WHERE approved = 1 AND (manager_id IS NULL OR manager_id = 0)")
    models_no_manager = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM users WHERE partner_id IS NULL")
    users_no_partner = int(cur.fetchone()[0] or 0)
    cur.execute("SELECT COUNT(*) FROM users WHERE manager_id IS NULL")
    users_no_manager = int(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COUNT(DISTINCT u.user_id)
        FROM users u
        JOIN payments p ON p.user_id = u.user_id
        WHERE u.installed = 0
        """
    )
    paid_not_installed = int(cur.fetchone()[0] or 0)
    conn.close()
    return {
        "pending_managers": pending_managers,
        "pending_partners": pending_partners,
        "models_no_manager": models_no_manager,
        "users_no_partner": users_no_partner,
        "users_no_manager": users_no_manager,
        "paid_not_installed": paid_not_installed,
    }

def list_users_for_broadcast(
    *,
    installed: Optional[int] = None,
    partner_id: Optional[int] = None,
    manager_id: Optional[int] = None,
    stage_key: Optional[str] = None,
    stage_mode: str = "reached",
    limit: int = 20000,
) -> List[sqlite3.Row]:
    where = []
    args: List[Any] = []
    if installed is not None:
        where.append("installed = ?")
        args.append(int(installed))
    if partner_id is not None:
        where.append("partner_id = ?")
        args.append(int(partner_id))
    if manager_id is not None:
        where.append("manager_id = ?")
        args.append(int(manager_id))

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"SELECT user_id, COALESCE(username,'') AS username FROM users {clause} ORDER BY last_seen DESC LIMIT ?",
        (*args, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    if stage_key:
        user_ids = [int(r["user_id"]) for r in rows if r["user_id"] is not None]
        allowed = _filter_user_ids_by_funnel_stage(user_ids, stage_key, stage_mode)
        rows = [r for r in rows if int(r["user_id"]) in allowed]
    return rows


def count_users_for_broadcast(
    *,
    installed: Optional[int] = None,
    partner_id: Optional[int] = None,
    manager_id: Optional[int] = None,
    stage_key: Optional[str] = None,
    stage_mode: str = "reached",
) -> int:
    where = []
    args: List[Any] = []
    if installed is not None:
        where.append("installed = ?")
        args.append(int(installed))
    if partner_id is not None:
        where.append("partner_id = ?")
        args.append(int(partner_id))
    if manager_id is not None:
        where.append("manager_id = ?")
        args.append(int(manager_id))
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    conn = _connect()
    cur = conn.cursor()
    if not stage_key:
        cur.execute(f"SELECT COUNT(*) FROM users {clause}", tuple(args))
        n = int(cur.fetchone()[0] or 0)
        conn.close()
        return n

    cur.execute(f"SELECT user_id FROM users {clause}", tuple(args))
    user_ids = [int(r[0]) for r in cur.fetchall() if r[0] is not None]
    conn.close()
    allowed = _filter_user_ids_by_funnel_stage(user_ids, stage_key, stage_mode)
    return int(len(allowed))


def get_user(user_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.installed,
            u.installed_at,
            u.ref_code,
            u.partner_id,
            u.manager_id,
            u.attributed_at,
            COALESCE(p.username, '') AS partner_username,
            COALESCE(m.username, '') AS manager_username,
            COALESCE(m.name, '') AS manager_name,
            COALESCE(pay.total, 0) AS paid_total,
            COALESCE(pay.cnt, 0) AS payments_count
        FROM users u
        LEFT JOIN partners p ON p.id = u.partner_id
        LEFT JOIN managers m ON m.id = u.manager_id
        LEFT JOIN (
            SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
            FROM payments
            GROUP BY user_id
        ) pay ON pay.user_id = u.user_id
        WHERE u.user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def search_users(query: str, limit: int = 200) -> List[sqlite3.Row]:
    q = (query or "").strip()
    if q.startswith("@"):
        q = q[1:]
    if not q:
        return list_users(limit=limit)

    conn = _connect()
    cur = conn.cursor()
    if q.isdigit():
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.installed,
                u.installed_at,
                u.ref_code,
                u.partner_id,
                u.manager_id,
                u.attributed_at,
                COALESCE(p.username, '') AS partner_username,
                COALESCE(m.username, '') AS manager_username,
                COALESCE(m.name, '') AS manager_name,
                COALESCE(pay.total, 0) AS paid_total,
                COALESCE(pay.cnt, 0) AS payments_count,
                u.created_at,
                u.last_seen
            FROM users u
            LEFT JOIN partners p ON p.id = u.partner_id
            LEFT JOIN managers m ON m.id = u.manager_id
            LEFT JOIN (
                SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
                FROM payments
                GROUP BY user_id
            ) pay ON pay.user_id = u.user_id
            WHERE u.user_id = ?
            ORDER BY u.last_seen DESC
            LIMIT ?
            """,
            (int(q), limit),
        )
    else:
        like = f"%{q.lower()}%"
        cur.execute(
            """
            SELECT
                u.user_id,
                u.username,
                u.first_name,
                u.last_name,
                u.installed,
                u.installed_at,
                u.ref_code,
                u.partner_id,
                u.manager_id,
                u.attributed_at,
                COALESCE(p.username, '') AS partner_username,
                COALESCE(m.username, '') AS manager_username,
                COALESCE(m.name, '') AS manager_name,
                COALESCE(pay.total, 0) AS paid_total,
                COALESCE(pay.cnt, 0) AS payments_count,
                u.created_at,
                u.last_seen
            FROM users u
            LEFT JOIN partners p ON p.id = u.partner_id
            LEFT JOIN managers m ON m.id = u.manager_id
            LEFT JOIN (
                SELECT user_id, SUM(amount_rub) AS total, COUNT(*) AS cnt
                FROM payments
                GROUP BY user_id
            ) pay ON pay.user_id = u.user_id
            WHERE LOWER(COALESCE(u.username, '')) LIKE ?
               OR LOWER(COALESCE(u.first_name, '')) LIKE ?
               OR LOWER(COALESCE(u.last_name, '')) LIKE ?
            ORDER BY u.last_seen DESC
            LIMIT ?
            """,
            (like, like, like, limit),
        )
    rows = cur.fetchall()
    conn.close()
    return rows


def users_overview() -> Dict[str, float]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total = int(cur.fetchone()[0])
    cur.execute("SELECT COUNT(*) FROM users WHERE installed = 1")
    installed = int(cur.fetchone()[0])
    cur.execute("SELECT COALESCE(SUM(amount_rub), 0) FROM payments")
    revenue = float(cur.fetchone()[0] or 0)
    conn.close()
    return {"total": total, "installed": installed, "revenue_rub": revenue}


def _normalize_account_type(account_type: Optional[str]) -> str:
    value = (account_type or "").strip().lower()
    if value not in ACCOUNT_TYPE_KEYS:
        raise ValueError("invalid account type")
    return value


def normalize_account_rotation_state(raw: Optional[str]) -> str:
    value = (raw or "review").strip().lower() or "review"
    if value not in ACCOUNT_ROTATION_STATE_KEYS:
        raise ValueError("invalid rotation state")
    return value


def normalize_account_rotation_source(raw: Optional[str]) -> str:
    value = (raw or "manual").strip().lower() or "manual"
    if value not in ACCOUNT_ROTATION_SOURCE_KEYS:
        raise ValueError("invalid rotation source")
    return value


def normalize_account_views_state(raw: Optional[str]) -> str:
    value = (raw or "unknown").strip().lower() or "unknown"
    if value not in ACCOUNT_VIEWS_STATE_KEYS:
        raise ValueError("invalid views state")
    return value


def normalize_account_mail_provider(raw: Optional[str]) -> str:
    value = (raw or "auto").strip().lower() or "auto"
    if value not in ACCOUNT_MAIL_PROVIDER_KEYS:
        raise ValueError("invalid mail provider")
    return value


def normalize_account_text_field(raw: Any) -> str:
    return re.sub(r"\s+", " ", str(raw or "").strip())


def account_text_field_is_missing(raw: Any) -> bool:
    value = normalize_account_text_field(raw)
    if not value:
        return True
    return value.upper() in ACCOUNT_TEXT_PLACEHOLDER_KEYS


def sanitize_account_text_field(raw: Any) -> str:
    value = normalize_account_text_field(raw)
    if account_text_field_is_missing(value):
        return ""
    return value


def _normalize_json_text(raw: Any, *, error_message: str) -> str:
    text = (raw or "").strip() if isinstance(raw, str) else ""
    if raw in (None, ""):
        return ""
    if not text and not isinstance(raw, str):
        try:
            parsed = json.loads(json.dumps(raw))
        except Exception as exc:
            raise ValueError(error_message) from exc
    else:
        try:
            parsed = json.loads(text)
        except Exception as exc:
            raise ValueError(error_message) from exc
    if not isinstance(parsed, dict):
        raise ValueError(error_message)
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"), sort_keys=True)


def normalize_account_mail_auth_json(raw: Any) -> str:
    return _normalize_json_text(raw, error_message="invalid mail auth json")


def normalize_account_mail_watch_json(raw: Any) -> str:
    return _normalize_json_text(raw, error_message="invalid mail watch json")


def normalize_account_mail_status(raw: Optional[str]) -> str:
    value = (raw or "never_checked").strip().lower() or "never_checked"
    if value not in ACCOUNT_MAIL_STATUS_KEYS:
        raise ValueError("invalid mail status")
    return value


def normalize_account_mail_challenge_status(raw: Optional[str]) -> str:
    value = (raw or "idle").strip().lower() or "idle"
    if value not in ACCOUNT_MAIL_CHALLENGE_STATUS_KEYS:
        raise ValueError("invalid mail challenge status")
    return value


def _normalize_helper_ticket_target(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if value not in HELPER_TICKET_TARGET_KEYS:
        raise ValueError("invalid helper target")
    return value


def normalize_account_login(raw: Optional[str]) -> str:
    return (raw or "").strip().lower()


def normalize_account_handle(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if value.startswith("@"):
        value = value[1:]
    return value


def normalize_instagram_launch_status(raw: Optional[str]) -> str:
    value = (raw or "idle").strip().lower() or "idle"
    if value not in INSTAGRAM_LAUNCH_STATUS_KEYS:
        raise ValueError("invalid instagram launch status")
    return value


def normalize_instagram_publish_status(raw: Optional[str]) -> str:
    value = (raw or "idle").strip().lower() or "idle"
    if value not in INSTAGRAM_PUBLISH_STATUS_KEYS:
        raise ValueError("invalid instagram publish status")
    return value


def normalize_instagram_audit_batch_state(raw: Optional[str]) -> str:
    value = (raw or "queued").strip().lower() or "queued"
    if value not in INSTAGRAM_AUDIT_BATCH_STATE_KEYS:
        raise ValueError("invalid instagram audit batch state")
    return value


def normalize_instagram_audit_item_state(raw: Optional[str]) -> str:
    value = (raw or "queued").strip().lower() or "queued"
    if value not in INSTAGRAM_AUDIT_ITEM_STATE_KEYS:
        raise ValueError("invalid instagram audit item state")
    return value


def normalize_instagram_audit_resolution(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if value and value not in INSTAGRAM_AUDIT_RESOLUTION_KEYS:
        raise ValueError("invalid instagram audit resolution")
    return value


def normalize_instagram_audit_mail_probe_state(raw: Optional[str]) -> str:
    value = (raw or "pending").strip().lower() or "pending"
    if value not in INSTAGRAM_AUDIT_MAIL_PROBE_STATE_KEYS:
        raise ValueError("invalid instagram audit mail probe state")
    return value


def normalize_runtime_task_type(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if value not in RUNTIME_TASK_TYPE_KEYS:
        raise ValueError("invalid runtime task type")
    return value


def normalize_runtime_task_entity_type(raw: Optional[str]) -> str:
    value = (raw or "").strip().lower()
    if value not in RUNTIME_TASK_ENTITY_TYPE_KEYS:
        raise ValueError("invalid runtime task entity type")
    return value


def normalize_runtime_task_state(raw: Optional[str]) -> str:
    value = (raw or "queued").strip().lower() or "queued"
    if value not in RUNTIME_TASK_STATE_KEYS:
        raise ValueError("invalid runtime task state")
    return value


def _publish_account_field(account: Any, key: str, default: Any = "") -> Any:
    if isinstance(account, dict):
        return account.get(key, default)
    try:
        return account[key]
    except Exception:
        return default


def normalize_instagram_emulator_serial(raw: Optional[str]) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    if value.lower() == "default":
        return "default"
    if value.isdigit():
        raise ValueError("invalid emulator serial")
    return value


def is_valid_instagram_emulator_serial(raw: Optional[str]) -> bool:
    try:
        normalize_instagram_emulator_serial(raw)
    except ValueError:
        return False
    return True


def publish_account_readiness_issues(account: Any, *, include_rotation_state: bool = True) -> List[str]:
    issues: List[str] = []
    login = str(_publish_account_field(account, "account_login") or "").strip()
    password_present = bool(_publish_account_field(account, "has_account_password", 0))
    if not password_present:
        password_present = bool(str(_publish_account_field(account, "account_password") or "").strip())
    emulator_serial = str(_publish_account_field(account, "instagram_emulator_serial") or "").strip()
    rotation_state = normalize_account_rotation_state(_publish_account_field(account, "rotation_state", "review"))
    if not login:
        issues.append("Не заполнен account login.")
    if not password_present:
        issues.append("Не заполнен account password.")
    if not emulator_serial:
        issues.append("Не заполнен Instagram emulator serial.")
    elif not is_valid_instagram_emulator_serial(emulator_serial):
        issues.append("Instagram emulator serial заполнен неверно.")
    if include_rotation_state and rotation_state == "not_working":
        issues.append("Аккаунт помечен как нерабочий и исключён из автопубликации.")
    return issues


def account_mail_automation_ready(account: Any) -> bool:
    address = sanitize_account_text_field(_publish_account_field(account, "email"))
    if not address:
        return False
    try:
        provider = normalize_account_mail_provider(_publish_account_field(account, "mail_provider", "auto"))
    except ValueError:
        return False
    if provider in {"gmail_api", "microsoft_graph"}:
        try:
            auth_json = normalize_account_mail_auth_json(_publish_account_field(account, "mail_auth_json"))
        except ValueError:
            return False
        return bool(auth_json)
    return bool(sanitize_account_text_field(_publish_account_field(account, "email_password")))


def normalize_account_twofa_secret(raw_value: Optional[str]) -> str:
    value = (raw_value or "").strip()
    if not value:
        return ""
    normalized = normalize_twofa_secret(value)
    if not normalized or not is_valid_twofa_secret(value):
        raise ValueError("invalid twofa secret")
    return normalized


def account_twofa_automation_ready(account: Any) -> bool:
    raw_value = _publish_account_field(account, "twofa")
    return is_valid_twofa_secret(raw_value)


def publish_account_automation_warnings(account: Any) -> List[str]:
    warnings: List[str] = []
    twofa_secret = str(_publish_account_field(account, "twofa") or "").strip()
    if not twofa_secret:
        warnings.append("2FA не заполнен. Если Instagram запросит код, публикация остановится со статусом «Требует 2FA».")
    elif not account_twofa_automation_ready(account):
        warnings.append(
            "2FA заполнен в неподдерживаемом формате. Вставь base32 secret или otpauth:// URI, иначе автологин остановится со статусом «Требует 2FA»."
        )
    try:
        provider = normalize_account_mail_provider(_publish_account_field(account, "mail_provider", "auto"))
    except ValueError:
        provider = "auto"
    mail_status = str(_publish_account_field(account, "mail_status") or "never_checked").strip().lower()
    if not account_mail_automation_ready(account):
        if provider in {"gmail_api", "microsoft_graph"}:
            warnings.append(
                "Почта не авторизована для auto-code. Если Instagram запросит код из письма, авто-подтверждение не сработает."
            )
        else:
            warnings.append(
                "Почта не готова для auto-code. Если Instagram запросит код из письма, публикация потребует ручного шага."
            )
    elif mail_status == "never_checked":
        warnings.append("Почта ещё не проверялась. Перед canary лучше сделать Mail check для этого аккаунта.")
    elif mail_status == "auth_error":
        warnings.append("Почта не проходит авторизацию. Если Instagram запросит код, auto-code может не сработать.")
    elif mail_status == "connect_error":
        warnings.append("Почта сейчас недоступна. Если Instagram запросит код, auto-code может не сработать.")
    elif mail_status == "unsupported":
        warnings.append("Провайдер почты пока не подтверждён для auto-code. При mail challenge может понадобиться ручной шаг.")
    return warnings


def _account_auto_rotation_failure_reason(account: Any, status: str, detail: str) -> str:
    status_value = (status or "").strip().lower()
    detail_value = (detail or "").strip()
    mail_reason = str(_publish_account_field(account, "mail_challenge_reason_text") or "").strip()
    mail_status = str(_publish_account_field(account, "mail_status") or "never_checked").strip().lower()

    if status_value == "invalid_password":
        return detail_value or "Instagram отклонил пароль. Проверь account_password."
    if status_value == "manual_2fa_required":
        if not account_twofa_automation_ready(account):
            return detail_value or "Instagram запросил 2FA, но для аккаунта не настроен валидный 2FA secret."
        return detail_value or "Instagram запросил 2FA, и helper не смог пройти этот шаг автоматически."
    if status_value == "email_code_required":
        if mail_reason:
            return mail_reason
        if not account_mail_automation_ready(account):
            return detail_value or "Instagram запросил код с почты, но почта не настроена для auto-code."
        if mail_status == "auth_error":
            return detail_value or "Instagram запросил код с почты, но почта не проходит авторизацию."
        if mail_status == "connect_error":
            return detail_value or "Instagram запросил код с почты, но почта сейчас недоступна."
        if mail_status == "unsupported":
            return detail_value or "Instagram запросил код с почты, но провайдер почты не поддержан для auto-code."
        return detail_value or "Instagram запросил код с почты, но получить его автоматически не удалось."
    if status_value == "challenge_required":
        return mail_reason or detail_value or "Instagram запросил challenge или подтверждение входа."
    return detail_value


def _account_auto_rotation_publish_candidate(account: Any) -> Optional[Dict[str, Any]]:
    updated_at = int(_publish_account_field(account, "instagram_publish_updated_at") or 0)
    if updated_at <= 0:
        return None
    status = normalize_instagram_publish_status(_publish_account_field(account, "instagram_publish_status", "idle"))
    detail = str(_publish_account_field(account, "instagram_publish_detail") or "").strip()

    if status in {"published", "needs_review"}:
        return {"state": "working", "reason": "", "updated_at": updated_at, "source": "publish"}
    if status in {"invalid_password", "manual_2fa_required", "email_code_required", "challenge_required"}:
        return {
            "state": "not_working",
            "reason": _account_auto_rotation_failure_reason(account, status, detail),
            "updated_at": updated_at,
            "source": "publish",
        }
    return None


def _account_auto_rotation_launch_candidate(account: Any) -> Optional[Dict[str, Any]]:
    updated_at = int(_publish_account_field(account, "instagram_launch_updated_at") or 0)
    if updated_at <= 0:
        return None
    status = normalize_instagram_launch_status(_publish_account_field(account, "instagram_launch_status", "idle"))
    detail = str(_publish_account_field(account, "instagram_launch_detail") or "").strip()

    if status == "login_submitted":
        return {"state": "working", "reason": "", "updated_at": updated_at, "source": "launch"}
    if status in {"invalid_password", "manual_2fa_required", "challenge_required"}:
        return {
            "state": "not_working",
            "reason": _account_auto_rotation_failure_reason(account, status, detail),
            "updated_at": updated_at,
            "source": "launch",
        }
    return None


def _account_auto_rotation_audit_candidate(account: Any) -> Optional[Dict[str, Any]]:
    updated_at = int(_publish_account_field(account, "latest_audit_updated_at") or 0)
    if updated_at <= 0:
        return None
    resolution = str(_publish_account_field(account, "latest_audit_resolution_state") or "").strip().lower()
    if not resolution:
        return None
    detail = str(_publish_account_field(account, "latest_audit_resolution_detail") or "").strip()

    if resolution == "login_ok":
        return {"state": "working", "reason": "", "updated_at": updated_at, "source": "audit"}
    if resolution in {"manual_2fa_required", "email_code_required", "challenge_required", "invalid_password"}:
        return {
            "state": "not_working",
            "reason": detail or _account_auto_rotation_failure_reason(account, resolution, detail),
            "updated_at": updated_at,
            "source": "audit",
        }
    if resolution == "missing_credentials":
        return {
            "state": "not_working",
            "reason": detail or "Не заполнены логин или пароль Instagram.",
            "updated_at": updated_at,
            "source": "audit",
        }
    if resolution == "missing_device":
        return {
            "state": "not_working",
            "reason": detail or "Для аккаунта не удалось назначить emulator serial.",
            "updated_at": updated_at,
            "source": "audit",
        }
    return None


def _account_auto_rotation_config_candidate(account: Any) -> Optional[Dict[str, Any]]:
    issues = publish_account_readiness_issues(account, include_rotation_state=False)
    if str(_publish_account_field(account, "twofa") or "").strip() and not account_twofa_automation_ready(account):
        issues.append("2FA заполнен в неверном формате. Нужен валидный base32 secret или otpauth:// URI.")
    updated_at = int(_publish_account_field(account, "updated_at") or 0)
    if not issues:
        return {"state": "working", "reason": "", "updated_at": updated_at, "source": "config"}
    return {
        "state": "not_working",
        "reason": " ".join(item for item in issues if item).strip(),
        "updated_at": updated_at,
        "source": "config",
    }


def account_auto_rotation_candidate(account: Any) -> Optional[Dict[str, Any]]:
    account_type = str(_publish_account_field(account, "type") or "").strip().lower()
    if account_type and account_type != "instagram":
        return None
    candidates = [
        candidate
        for candidate in (
            _account_auto_rotation_publish_candidate(account),
            _account_auto_rotation_launch_candidate(account),
            _account_auto_rotation_audit_candidate(account),
            _account_auto_rotation_config_candidate(account),
        )
        if candidate is not None
    ]
    if not candidates:
        return None
    source_priority = {"config": 0, "launch": 1, "audit": 2, "publish": 3}
    candidates.sort(key=lambda item: (int(item.get("updated_at") or 0), source_priority.get(str(item.get("source") or ""), 0)))
    return candidates[-1]


def account_rotation_display_reason(account: Any) -> str:
    stored_reason = str(_publish_account_field(account, "rotation_state_reason") or "").strip()
    if stored_reason:
        return stored_reason
    candidate = account_auto_rotation_candidate(account)
    if candidate is not None and str(candidate.get("state") or "") == "not_working":
        return str(candidate.get("reason") or "").strip()
    current_state = normalize_account_rotation_state(_publish_account_field(account, "rotation_state", "review"))
    current_source = normalize_account_rotation_source(_publish_account_field(account, "rotation_state_source", "manual"))
    if current_state == "not_working" and current_source == "manual":
        return "Статус аккаунта установлен вручную."
    return ""


def _get_account_rotation_fields_with_cursor(cur: sqlite3.Cursor, account_id: int) -> Optional[sqlite3.Row]:
    cur.execute(
        """
        SELECT
            id,
            type,
            account_login,
            account_password,
            COALESCE(twofa, '') AS twofa,
            COALESCE(email, '') AS email,
            email_password,
            COALESCE(mail_provider, 'auto') AS mail_provider,
            COALESCE(mail_auth_json, '') AS mail_auth_json,
            COALESCE(mail_status, 'never_checked') AS mail_status,
            COALESCE(mail_last_error, '') AS mail_last_error,
            COALESCE(mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(instagram_launch_status, 'idle') AS instagram_launch_status,
            COALESCE(instagram_launch_detail, '') AS instagram_launch_detail,
            instagram_launch_updated_at,
            COALESCE(instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(instagram_publish_detail, '') AS instagram_publish_detail,
            instagram_publish_updated_at,
            COALESCE((
                SELECT ai.resolution_state
                FROM instagram_audit_items ai
                WHERE ai.account_id = accounts.id
                ORDER BY ai.updated_at DESC, ai.id DESC
                LIMIT 1
            ), '') AS latest_audit_resolution_state,
            COALESCE((
                SELECT ai.resolution_detail
                FROM instagram_audit_items ai
                WHERE ai.account_id = accounts.id
                ORDER BY ai.updated_at DESC, ai.id DESC
                LIMIT 1
            ), '') AS latest_audit_resolution_detail,
            (
                SELECT ai.updated_at
                FROM instagram_audit_items ai
                WHERE ai.account_id = accounts.id
                ORDER BY ai.updated_at DESC, ai.id DESC
                LIMIT 1
            ) AS latest_audit_updated_at,
            COALESCE(rotation_state, 'review') AS rotation_state,
            COALESCE(rotation_state_source, 'manual') AS rotation_state_source,
            COALESCE(rotation_state_reason, '') AS rotation_state_reason,
            updated_at
        FROM accounts
        WHERE id = ?
        LIMIT 1
        """,
        (int(account_id),),
    )
    return cur.fetchone()


def _sync_account_auto_rotation_state_with_cursor(cur: sqlite3.Cursor, account_id: int, *, now: Optional[int] = None) -> bool:
    row = _get_account_rotation_fields_with_cursor(cur, int(account_id))
    if row is None:
        return False
    account = dict(row)
    current_state = normalize_account_rotation_state(str(account.get("rotation_state") or "review"))
    current_source = normalize_account_rotation_source(str(account.get("rotation_state_source") or "manual"))
    candidate = account_auto_rotation_candidate(account)
    if candidate is None:
        return False
    next_state = normalize_account_rotation_state(str(candidate.get("state") or "review"))
    if current_state == "not_working" and current_source == "manual":
        return False
    if next_state == "working" and current_state == "not_working" and current_source == "manual":
        return False

    reason_value = str(candidate.get("reason") or "").strip() if next_state == "not_working" else ""
    timestamp = int(now or time.time())
    if (
        current_state == next_state
        and current_source == "auto"
        and str(account.get("rotation_state_reason") or "").strip() == reason_value
    ):
        return False
    cur.execute(
        """
        UPDATE accounts
        SET rotation_state = ?,
            rotation_state_source = 'auto',
            rotation_state_reason = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (next_state, reason_value, timestamp, int(account_id)),
    )
    return cur.rowcount > 0


def normalize_publish_batch_state(raw: Optional[str]) -> str:
    value = (raw or "generating").strip().lower() or "generating"
    if value not in PUBLISH_BATCH_STATE_KEYS:
        raise ValueError("invalid publish batch state")
    return value


def normalize_publish_batch_account_state(raw: Optional[str]) -> str:
    value = (raw or "queued_for_generation").strip().lower() or "queued_for_generation"
    if value not in PUBLISH_BATCH_ACCOUNT_STATE_KEYS:
        raise ValueError("invalid publish batch account state")
    return value


def normalize_publish_job_state(raw: Optional[str]) -> str:
    value = (raw or "queued").strip().lower() or "queued"
    if value not in PUBLISH_JOB_STATE_KEYS:
        raise ValueError("invalid publish job state")
    return value


def find_duplicate_account(
    account_type: str,
    account_login: str,
    *,
    exclude_account_id: Optional[int] = None,
) -> Optional[sqlite3.Row]:
    t = _normalize_account_type(account_type)
    login_normalized = normalize_account_login(account_login)
    if not login_normalized:
        return None
    conn = _connect()
    cur = conn.cursor()
    sql = """
        SELECT
            a.id,
            a.type,
            a.account_login,
            a.username,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            a.updated_at
        FROM accounts a
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        WHERE a.type = ?
          AND LOWER(TRIM(COALESCE(a.account_login, ''))) = ?
    """
    args: List[Any] = [t, login_normalized]
    if exclude_account_id is not None:
        sql += " AND a.id <> ?"
        args.append(int(exclude_account_id))
    sql += " ORDER BY a.updated_at DESC, a.id DESC LIMIT 1"
    cur.execute(sql, tuple(args))
    row = cur.fetchone()
    conn.close()
    return row


def list_accounts(
    q: Optional[str] = None,
    account_type: Optional[str] = None,
    owner_worker_id: Optional[int] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    limit: int = 500,
) -> List[sqlite3.Row]:
    where: List[str] = []
    args: List[Any] = []
    query = (q or "").strip().lower()
    if query:
        like = f"%{query}%"
        where.append(
            "("
            "LOWER(COALESCE(account_login, '')) LIKE ? OR "
            "LOWER(COALESCE(username, '')) LIKE ? OR "
            "LOWER(COALESCE(email, '')) LIKE ? OR "
            "LOWER(COALESCE(proxy, '')) LIKE ?"
            ")"
        )
        args.extend([like, like, like, like])
    if account_type:
        at = _normalize_account_type(account_type)
        where.append("a.type = ?")
        args.append(at)
    if owner_worker_id is not None:
        where.append("a.owner_worker_id = ?")
        args.append(int(owner_worker_id))
    if rotation_state:
        where.append("COALESCE(a.rotation_state, 'review') = ?")
        args.append(normalize_account_rotation_state(rotation_state))
    if views_state:
        where.append("COALESCE(a.views_state, 'unknown') = ?")
        args.append(normalize_account_views_state(views_state))

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            a.id,
            a.type,
            a.account_login,
            a.account_password,
            a.username,
            a.email,
            a.email_password,
            COALESCE(a.proxy, '') AS proxy,
            COALESCE(a.twofa, '') AS twofa,
            COALESCE(a.rotation_state, 'review') AS rotation_state,
            COALESCE(a.rotation_state_source, 'manual') AS rotation_state_source,
            COALESCE(a.rotation_state_reason, '') AS rotation_state_reason,
            COALESCE(a.views_state, 'unknown') AS views_state,
            COALESCE(a.mail_provider, 'auto') AS mail_provider,
            COALESCE(a.mail_auth_json, '') AS mail_auth_json,
            COALESCE(a.mail_status, 'never_checked') AS mail_status,
            a.mail_last_checked_at,
            a.mail_last_synced_at,
            COALESCE(a.mail_last_error, '') AS mail_last_error,
            COALESCE(a.mail_watch_json, '') AS mail_watch_json,
            COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
            COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
            COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
            COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(a.mail_challenge_message_uid, '') AS mail_challenge_message_uid,
            a.mail_challenge_received_at,
            COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
            COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
            a.mail_challenge_updated_at,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(a.instagram_launch_status, 'idle') AS instagram_launch_status,
            COALESCE(a.instagram_launch_detail, '') AS instagram_launch_detail,
            a.instagram_launch_updated_at,
            COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
            a.instagram_publish_updated_at,
            COALESCE(a.instagram_publish_last_file, '') AS instagram_publish_last_file,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            a.created_at,
            a.updated_at
        FROM accounts a
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        {clause}
        ORDER BY a.updated_at DESC, a.id DESC
        LIMIT ?
        """,
        (*args, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_accounts_compact(
    q: Optional[str] = None,
    account_type: Optional[str] = None,
    owner_worker_id: Optional[int] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    sort_by: Optional[str] = None,
    limit: int = 500,
) -> List[sqlite3.Row]:
    where: List[str] = []
    args: List[Any] = []
    query = (q or "").strip().lower()
    if query:
        like = f"%{query}%"
        where.append(
            "("
            "LOWER(COALESCE(a.account_login, '')) LIKE ? OR "
            "LOWER(COALESCE(a.username, '')) LIKE ? OR "
            "LOWER(COALESCE(a.email, '')) LIKE ? OR "
            "LOWER(COALESCE(a.proxy, '')) LIKE ?"
            ")"
        )
        args.extend([like, like, like, like])
    if account_type:
        at = _normalize_account_type(account_type)
        where.append("a.type = ?")
        args.append(at)
    if owner_worker_id is not None:
        where.append("a.owner_worker_id = ?")
        args.append(int(owner_worker_id))
    if rotation_state:
        where.append("COALESCE(a.rotation_state, 'review') = ?")
        args.append(normalize_account_rotation_state(rotation_state))
    if views_state:
        where.append("COALESCE(a.views_state, 'unknown') = ?")
        args.append(normalize_account_views_state(views_state))

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    sort_value = (sort_by or "recent").strip().lower() or "recent"
    if sort_value == "transitions_desc":
        order_clause = "ORDER BY starts_unique_total DESC, a.updated_at DESC, a.id DESC"
    elif sort_value == "transitions_asc":
        order_clause = "ORDER BY starts_unique_total ASC, a.updated_at DESC, a.id DESC"
    else:
        order_clause = "ORDER BY a.updated_at DESC, a.id DESC"
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            a.id,
            a.type,
            a.account_login,
            a.username,
            COALESCE(a.twofa, '') AS twofa,
            COALESCE(a.rotation_state, 'review') AS rotation_state,
            COALESCE(a.rotation_state_source, 'manual') AS rotation_state_source,
            COALESCE(a.rotation_state_reason, '') AS rotation_state_reason,
            COALESCE(a.views_state, 'unknown') AS views_state,
            COALESCE(a.mail_provider, 'auto') AS mail_provider,
            COALESCE(a.mail_status, 'never_checked') AS mail_status,
            a.mail_last_checked_at,
            a.mail_last_synced_at,
            COALESCE(a.mail_last_error, '') AS mail_last_error,
            COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
            COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
            COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
            COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(a.mail_challenge_message_uid, '') AS mail_challenge_message_uid,
            a.mail_challenge_received_at,
            COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
            COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
            a.mail_challenge_updated_at,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(a.instagram_launch_status, 'idle') AS instagram_launch_status,
            COALESCE(a.instagram_launch_detail, '') AS instagram_launch_detail,
            a.instagram_launch_updated_at,
            COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
            a.instagram_publish_updated_at,
            COALESCE(a.instagram_publish_last_file, '') AS instagram_publish_last_file,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            a.updated_at,
            COALESCE((
                SELECT COUNT(*)
                FROM links l
                WHERE l.account_id = a.id
                  AND COALESCE(l.is_deleted, 0) = 0
            ), 0) AS links_total,
            COALESCE((
                SELECT COUNT(DISTINCT e.user_id)
                FROM events e
                JOIN links l ON LOWER(l.code) = LOWER(e.code)
                WHERE l.account_id = a.id
                  AND COALESCE(l.is_deleted, 0) = 0
                  AND e.event_type = 'start'
                  AND e.user_id IS NOT NULL
            ), 0) AS starts_unique_total,
            COALESCE((
                SELECT l1.code
                FROM links l1
                WHERE l1.account_id = a.id
                  AND COALESCE(l1.is_deleted, 0) = 0
                ORDER BY l1.created_at DESC, l1.code DESC
                LIMIT 1
            ), (
                SELECT l2.code
                FROM links l2
                WHERE l2.account_id = a.id
                ORDER BY l2.created_at DESC, l2.code DESC
                LIMIT 1
            )) AS primary_link_code
        FROM accounts a
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        {clause}
        {order_clause}
        LIMIT ?
        """,
        (*args, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_account(account_id: int, owner_worker_id: Optional[int] = None) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    if owner_worker_id is None:
        cur.execute(
            """
            SELECT
                a.id,
                a.type,
                a.account_login,
                a.account_password,
                a.username,
                a.email,
                a.email_password,
                COALESCE(a.proxy, '') AS proxy,
                COALESCE(a.twofa, '') AS twofa,
                COALESCE(a.rotation_state, 'review') AS rotation_state,
                COALESCE(a.rotation_state_source, 'manual') AS rotation_state_source,
                COALESCE(a.rotation_state_reason, '') AS rotation_state_reason,
                COALESCE(a.views_state, 'unknown') AS views_state,
                COALESCE(a.mail_provider, 'auto') AS mail_provider,
                COALESCE(a.mail_auth_json, '') AS mail_auth_json,
                COALESCE(a.mail_status, 'never_checked') AS mail_status,
                a.mail_last_checked_at,
                a.mail_last_synced_at,
                COALESCE(a.mail_last_error, '') AS mail_last_error,
                COALESCE(a.mail_watch_json, '') AS mail_watch_json,
                COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
                COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
                COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
                COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
                COALESCE(a.mail_challenge_message_uid, '') AS mail_challenge_message_uid,
                a.mail_challenge_received_at,
                COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
                COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
                a.mail_challenge_updated_at,
                COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
                COALESCE(a.instagram_launch_status, 'idle') AS instagram_launch_status,
                COALESCE(a.instagram_launch_detail, '') AS instagram_launch_detail,
                a.instagram_launch_updated_at,
                COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
                COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
                a.instagram_publish_updated_at,
                COALESCE(a.instagram_publish_last_file, '') AS instagram_publish_last_file,
                a.owner_worker_id,
                COALESCE(w.name, '') AS owner_worker_name,
                COALESCE(w.username, '') AS owner_worker_username,
                a.created_at,
                a.updated_at
            FROM accounts a
            LEFT JOIN workers w ON w.id = a.owner_worker_id
            WHERE a.id = ?
            """,
            (int(account_id),),
        )
    else:
        cur.execute(
            """
            SELECT
                a.id,
                a.type,
                a.account_login,
                a.account_password,
                a.username,
                a.email,
                a.email_password,
                COALESCE(a.proxy, '') AS proxy,
                COALESCE(a.twofa, '') AS twofa,
                COALESCE(a.rotation_state, 'review') AS rotation_state,
                COALESCE(a.rotation_state_source, 'manual') AS rotation_state_source,
                COALESCE(a.rotation_state_reason, '') AS rotation_state_reason,
                COALESCE(a.views_state, 'unknown') AS views_state,
                COALESCE(a.mail_provider, 'auto') AS mail_provider,
                COALESCE(a.mail_auth_json, '') AS mail_auth_json,
                COALESCE(a.mail_status, 'never_checked') AS mail_status,
                a.mail_last_checked_at,
                a.mail_last_synced_at,
                COALESCE(a.mail_last_error, '') AS mail_last_error,
                COALESCE(a.mail_watch_json, '') AS mail_watch_json,
                COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
                COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
                COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
                COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
                COALESCE(a.mail_challenge_message_uid, '') AS mail_challenge_message_uid,
                a.mail_challenge_received_at,
                COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
                COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
                a.mail_challenge_updated_at,
                COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
                COALESCE(a.instagram_launch_status, 'idle') AS instagram_launch_status,
                COALESCE(a.instagram_launch_detail, '') AS instagram_launch_detail,
                a.instagram_launch_updated_at,
                COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
                COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
                a.instagram_publish_updated_at,
                COALESCE(a.instagram_publish_last_file, '') AS instagram_publish_last_file,
                a.owner_worker_id,
                COALESCE(w.name, '') AS owner_worker_name,
                COALESCE(w.username, '') AS owner_worker_username,
                a.created_at,
                a.updated_at
            FROM accounts a
            LEFT JOIN workers w ON w.id = a.owner_worker_id
            WHERE a.id = ?
              AND a.owner_worker_id = ?
            """,
            (int(account_id), int(owner_worker_id)),
        )
    row = cur.fetchone()
    conn.close()
    return row


def account_stats(account_id: int, owner_worker_id: Optional[int] = None) -> Dict[str, int]:
    account_id_int = int(account_id)
    if get_account(account_id_int, owner_worker_id=owner_worker_id) is None:
        return {
            "links_total": 0,
            "starts_total": 0,
            "starts_unique_total": 0,
            "first_touch_total": 0,
        }
    conn = _connect()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT COUNT(*) AS links_total
        FROM links
        WHERE account_id = ?
          AND COALESCE(is_deleted, 0) = 0
        """,
        (account_id_int,),
    )
    row_links = cur.fetchone()

    cur.execute(
        """
        SELECT
            COUNT(*) AS starts_total,
            COUNT(DISTINCT CASE WHEN e.user_id IS NOT NULL THEN e.user_id END) AS starts_unique_total
        FROM events e
        JOIN links l ON LOWER(l.code) = LOWER(e.code)
        WHERE l.account_id = ?
          AND COALESCE(l.is_deleted, 0) = 0
          AND e.event_type = 'start'
        """,
        (account_id_int,),
    )
    row_starts = cur.fetchone()

    cur.execute(
        """
        SELECT COUNT(*) AS first_touch_total
        FROM users u
        WHERE EXISTS (
            SELECT 1
            FROM links l
            WHERE l.account_id = ?
              AND COALESCE(l.is_deleted, 0) = 0
              AND LOWER(l.code) = LOWER(COALESCE(u.ref_code, ''))
        )
        """,
        (account_id_int,),
    )
    row_touch = cur.fetchone()
    conn.close()

    return {
        "links_total": int((row_links["links_total"] if row_links else 0) or 0),
        "starts_total": int((row_starts["starts_total"] if row_starts else 0) or 0),
        "starts_unique_total": int((row_starts["starts_unique_total"] if row_starts else 0) or 0),
        "first_touch_total": int((row_touch["first_touch_total"] if row_touch else 0) or 0),
    }


def list_account_mail_messages(account_id: int, limit: int = 10) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            account_id,
            message_uid,
            from_text,
            subject,
            received_at,
            snippet,
            COALESCE(metadata_json, '') AS metadata_json,
            created_at
        FROM account_mail_messages
        WHERE account_id = ?
        ORDER BY COALESCE(received_at, 0) DESC, id DESC
        LIMIT ?
        """,
        (int(account_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def update_account_mail_state(
    account_id: int,
    *,
    mail_provider: Optional[str] = None,
    mail_status: Optional[str] = None,
    mail_last_checked_at: Optional[int] = None,
    mail_last_synced_at: Optional[int] = None,
    mail_last_error: Optional[str] = None,
    mail_auth_json: Optional[str] = None,
    mail_watch_json: Optional[str] = None,
) -> bool:
    now = int(mail_last_checked_at or time.time())
    sync_at = int(mail_last_synced_at) if mail_last_synced_at not in (None, "") else None
    provider_value = normalize_account_mail_provider(mail_provider) if mail_provider is not None else None
    status_value = normalize_account_mail_status(mail_status) if mail_status is not None else None
    auth_json_value = normalize_account_mail_auth_json(mail_auth_json) if mail_auth_json is not None else None
    watch_json_value = normalize_account_mail_watch_json(mail_watch_json) if mail_watch_json is not None else None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE accounts
        SET mail_provider = COALESCE(?, mail_provider),
            mail_auth_json = COALESCE(?, mail_auth_json),
            mail_status = COALESCE(?, mail_status),
            mail_last_checked_at = ?,
            mail_last_synced_at = COALESCE(?, mail_last_synced_at),
            mail_last_error = ?,
            mail_watch_json = COALESCE(?, mail_watch_json)
        WHERE id = ?
        """,
        (
            provider_value,
            auth_json_value,
            status_value,
            now,
            sync_at,
            (mail_last_error or "").strip(),
            watch_json_value,
            int(account_id),
        ),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def update_account_mail_challenge_state(
    account_id: int,
    *,
    status: Optional[str] = None,
    kind: Optional[str] = None,
    reason_code: Optional[str] = None,
    reason_text: Optional[str] = None,
    message_uid: Optional[str] = None,
    received_at: Optional[int] = None,
    masked_code: Optional[str] = None,
    confidence: Optional[float] = None,
    updated_at: Optional[int] = None,
) -> bool:
    now = int(updated_at or time.time())
    status_value = normalize_account_mail_challenge_status(status)
    confidence_value = 0.0
    if confidence not in (None, ""):
        try:
            confidence_value = max(0.0, min(1.0, float(confidence)))
        except Exception:
            confidence_value = 0.0
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE accounts
        SET mail_challenge_status = ?,
            mail_challenge_kind = ?,
            mail_challenge_reason_code = ?,
            mail_challenge_reason_text = ?,
            mail_challenge_message_uid = ?,
            mail_challenge_received_at = ?,
            mail_challenge_masked_code = ?,
            mail_challenge_confidence = ?,
            mail_challenge_updated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            status_value,
            (kind or "").strip(),
            (reason_code or "").strip(),
            (reason_text or "").strip(),
            (message_uid or "").strip(),
            int(received_at) if received_at not in (None, "") else None,
            (masked_code or "").strip(),
            confidence_value,
            now,
            now,
            int(account_id),
        ),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def _update_account_mail_challenge_state_with_cursor(
    cur: sqlite3.Cursor,
    account_id: int,
    *,
    status: str,
    kind: str = "",
    reason_code: str = "",
    reason_text: str = "",
    message_uid: str = "",
    received_at: Optional[Any] = None,
    masked_code: str = "",
    confidence: float = 0.0,
    updated_at: Optional[int] = None,
) -> None:
    now = int(updated_at or time.time())
    status_value = normalize_account_mail_challenge_status(status)
    confidence_value = 0.0
    if confidence not in (None, ""):
        try:
            confidence_value = max(0.0, min(1.0, float(confidence)))
        except Exception:
            confidence_value = 0.0
    cur.execute(
        """
        UPDATE accounts
        SET mail_challenge_status = ?,
            mail_challenge_kind = ?,
            mail_challenge_reason_code = ?,
            mail_challenge_reason_text = ?,
            mail_challenge_message_uid = ?,
            mail_challenge_received_at = ?,
            mail_challenge_masked_code = ?,
            mail_challenge_confidence = ?,
            mail_challenge_updated_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            status_value,
            (kind or "").strip(),
            (reason_code or "").strip(),
            (reason_text or "").strip(),
            (message_uid or "").strip(),
            int(received_at) if received_at not in (None, "") else None,
            (masked_code or "").strip(),
            confidence_value,
            now,
            now,
            int(account_id),
        ),
    )


def replace_account_mail_messages(account_id: int, messages: List[Dict[str, Any]]) -> None:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM account_mail_messages WHERE account_id = ?", (int(account_id),))
    for item in messages:
        cur.execute(
            """
            INSERT INTO account_mail_messages (
                account_id,
                message_uid,
                from_text,
                subject,
                received_at,
                snippet,
                metadata_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(account_id),
                str(item.get("message_uid") or ""),
                str(item.get("from_text") or ""),
                str(item.get("subject") or ""),
                int(item.get("received_at") or 0) if item.get("received_at") else None,
                str(item.get("snippet") or ""),
                json.dumps(
                    {
                        "provider_message_id": str(item.get("provider_message_id") or ""),
                        "body_text": str(item.get("body_text") or ""),
                        "body_html": str(item.get("body_html") or ""),
                        "to_text": str(item.get("to_text") or ""),
                        "cc_text": str(item.get("cc_text") or ""),
                        "to_addresses": list(item.get("to_addresses") or []),
                        "cc_addresses": list(item.get("cc_addresses") or []),
                        "links": list(item.get("links") or []),
                        "candidate_code": str(item.get("candidate_code") or ""),
                        "candidate_link": str(item.get("candidate_link") or ""),
                        "candidate_confidence": float(item.get("candidate_confidence") or 0.0),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                    sort_keys=True,
                ),
                now,
            ),
        )
    conn.commit()
    conn.close()


def update_account_instagram_emulator_serial(account_id: int, instagram_emulator_serial: str) -> bool:
    now = int(time.time())
    serial_value = normalize_instagram_emulator_serial(instagram_emulator_serial)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE accounts
        SET instagram_emulator_serial = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (serial_value, now, int(account_id)),
    )
    changed = cur.rowcount > 0
    if changed:
        _sync_account_auto_rotation_state_with_cursor(cur, int(account_id), now=now)
    conn.commit()
    conn.close()
    return changed


def count_instagram_emulator_serial_usage(serials: List[str]) -> Dict[str, int]:
    cleaned = sorted({str(item or "").strip() for item in serials if str(item or "").strip()})
    if not cleaned:
        return {}
    placeholders = ",".join("?" for _ in cleaned)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            instagram_emulator_serial,
            COUNT(*) AS total
        FROM accounts
        WHERE instagram_emulator_serial IN ({placeholders})
        GROUP BY instagram_emulator_serial
        """,
        tuple(cleaned),
    )
    rows = cur.fetchall()
    conn.close()
    usage = {serial: 0 for serial in cleaned}
    for row in rows:
        usage[str(row["instagram_emulator_serial"] or "").strip()] = int(row["total"] or 0)
    return usage


def _append_instagram_audit_event_with_cursor(
    cur: sqlite3.Cursor,
    *,
    audit_batch_id: int,
    audit_item_id: Optional[int],
    account_id: Optional[int],
    state: str,
    detail: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    created_at: Optional[int] = None,
) -> int:
    timestamp = int(created_at or time.time())
    payload_json = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    cur.execute(
        """
        INSERT INTO instagram_audit_events (
            audit_batch_id,
            audit_item_id,
            account_id,
            state,
            detail,
            payload_json,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(audit_batch_id),
            int(audit_item_id) if audit_item_id is not None else None,
            int(account_id) if account_id is not None else None,
            (state or "").strip(),
            (detail or "").strip(),
            payload_json,
            timestamp,
        ),
    )
    return int(cur.lastrowid)


def append_instagram_audit_event(
    audit_batch_id: int,
    *,
    audit_item_id: Optional[int],
    account_id: Optional[int],
    state: str,
    detail: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    created_at: Optional[int] = None,
) -> int:
    conn = _connect()
    cur = conn.cursor()
    event_id = _append_instagram_audit_event_with_cursor(
        cur,
        audit_batch_id=int(audit_batch_id),
        audit_item_id=audit_item_id,
        account_id=account_id,
        state=state,
        detail=detail,
        payload=payload,
        created_at=created_at,
    )
    conn.commit()
    conn.close()
    return event_id


def create_instagram_audit_batch(
    items: List[Dict[str, Any]],
    *,
    created_by_admin: Optional[str],
) -> Dict[str, Any]:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO instagram_audit_batches (
            state,
            detail,
            selected_accounts,
            created_by_admin,
            created_at,
            updated_at
        )
        VALUES ('queued', '', ?, ?, ?, ?)
        """,
        (len(items), (created_by_admin or "").strip(), now, now),
    )
    batch_id = int(cur.lastrowid)
    for index, item in enumerate(items):
        item_state = normalize_instagram_audit_item_state(str(item.get("item_state") or "queued"))
        mail_probe_state = normalize_instagram_audit_mail_probe_state(str(item.get("mail_probe_state") or "pending"))
        resolution_state = normalize_instagram_audit_resolution(str(item.get("resolution_state") or ""))
        detail = str(item.get("resolution_detail") or item.get("login_detail") or item.get("mail_probe_detail") or "").strip()
        started_at = int(item["started_at"]) if item.get("started_at") else None
        completed_at = int(item["completed_at"]) if item.get("completed_at") else None
        cur.execute(
            """
            INSERT INTO instagram_audit_items (
                audit_batch_id,
                account_id,
                queue_position,
                item_state,
                assigned_serial,
                login_state,
                login_detail,
                mail_probe_state,
                mail_probe_detail,
                resolution_state,
                resolution_detail,
                diagnostic_path,
                created_at,
                updated_at,
                started_at,
                completed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch_id,
                int(item["account_id"]),
                int(item.get("queue_position") or index),
                item_state,
                str(item.get("assigned_serial") or "").strip(),
                str(item.get("login_state") or "").strip(),
                str(item.get("login_detail") or "").strip(),
                mail_probe_state,
                str(item.get("mail_probe_detail") or "").strip(),
                resolution_state,
                str(item.get("resolution_detail") or detail).strip(),
                str(item.get("diagnostic_path") or "").strip(),
                now,
                now,
                started_at,
                completed_at,
            ),
        )
        item_id = int(cur.lastrowid)
        _append_instagram_audit_event_with_cursor(
            cur,
            audit_batch_id=batch_id,
            audit_item_id=item_id,
            account_id=int(item["account_id"]),
            state=item_state,
            detail=detail or ("Задача поставлена в очередь." if item_state == "queued" else ""),
            payload={
                "assigned_serial": str(item.get("assigned_serial") or "").strip(),
                "resolution_state": resolution_state,
                "mail_probe_state": mail_probe_state,
            },
            created_at=now,
        )
        _sync_account_auto_rotation_state_with_cursor(cur, int(item["account_id"]), now=now)
    conn.commit()
    conn.close()
    refresh_instagram_audit_batch_state(batch_id)
    return {"batch_id": batch_id, "selected_accounts": len(items)}


def get_instagram_audit_batch(batch_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            state,
            detail,
            selected_accounts,
            created_by_admin,
            created_at,
            updated_at,
            started_at,
            completed_at
        FROM instagram_audit_batches
        WHERE id = ?
        """,
        (int(batch_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_instagram_audit_items(batch_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.id,
            i.audit_batch_id,
            i.account_id,
            i.queue_position,
            i.item_state,
            i.assigned_serial,
            i.login_state,
            i.login_detail,
            i.mail_probe_state,
            i.mail_probe_detail,
            i.resolution_state,
            i.resolution_detail,
            i.diagnostic_path,
            i.created_at,
            i.updated_at,
            i.started_at,
            i.completed_at,
            COALESCE(a.type, '') AS account_type,
            COALESCE(a.account_login, '') AS account_login,
            COALESCE(a.username, '') AS username,
            COALESCE(a.email, '') AS email,
            COALESCE(a.instagram_emulator_serial, '') AS account_instagram_emulator_serial,
            COALESCE(a.instagram_launch_status, 'idle') AS instagram_launch_status,
            COALESCE(a.instagram_launch_detail, '') AS instagram_launch_detail,
            a.instagram_launch_updated_at,
            COALESCE(a.mail_status, 'never_checked') AS account_mail_status,
            COALESCE(a.mail_last_error, '') AS account_mail_last_error,
            a.mail_last_checked_at,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username
        FROM instagram_audit_items i
        JOIN accounts a ON a.id = i.account_id
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        WHERE i.audit_batch_id = ?
        ORDER BY i.queue_position ASC, i.id ASC
        """,
        (int(batch_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_instagram_audit_item(batch_id: int, account_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            audit_batch_id,
            account_id,
            queue_position,
            item_state,
            assigned_serial,
            login_state,
            login_detail,
            mail_probe_state,
            mail_probe_detail,
            resolution_state,
            resolution_detail,
            diagnostic_path,
            created_at,
            updated_at,
            started_at,
            completed_at
        FROM instagram_audit_items
        WHERE audit_batch_id = ?
          AND account_id = ?
        LIMIT 1
        """,
        (int(batch_id), int(account_id)),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_instagram_audit_events(batch_id: int, limit: int = 100) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            e.id,
            e.audit_batch_id,
            e.audit_item_id,
            e.account_id,
            e.state,
            e.detail,
            e.payload_json,
            e.created_at,
            COALESCE(a.username, '') AS account_username,
            COALESCE(a.account_login, '') AS account_login
        FROM instagram_audit_events e
        LEFT JOIN accounts a ON a.id = e.account_id
        WHERE e.audit_batch_id = ?
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT ?
        """,
        (int(batch_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_pending_instagram_audit_batch_ids(limit: int = 20) -> List[int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM instagram_audit_batches
        WHERE state IN ('queued', 'running')
        ORDER BY created_at ASC, id ASC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = [int(row["id"]) for row in cur.fetchall()]
    conn.close()
    return rows


def reset_instagram_audit_inflight_items(batch_id: int) -> int:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE instagram_audit_items
        SET item_state = 'queued',
            updated_at = ?
        WHERE audit_batch_id = ?
          AND item_state IN ('launching', 'login_check', 'mail_check_if_needed')
        """,
        (now, int(batch_id)),
    )
    changed = int(cur.rowcount or 0)
    conn.commit()
    conn.close()
    if changed:
        refresh_instagram_audit_batch_state(batch_id, detail="После рестарта batch поставлен обратно в очередь.")
    return changed


def update_instagram_audit_batch_state(
    batch_id: int,
    state: str,
    *,
    detail: Optional[str] = None,
    started_at: Optional[int] = None,
    completed_at: Optional[int] = None,
) -> bool:
    state_value = normalize_instagram_audit_batch_state(state)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    if detail is None:
        cur.execute("SELECT detail, started_at, completed_at FROM instagram_audit_batches WHERE id = ?", (int(batch_id),))
        row = cur.fetchone()
        detail_value = str((row["detail"] if row else "") or "").strip()
        started_value = int((row["started_at"] if row and row["started_at"] is not None else 0) or 0) or None
        completed_value = int((row["completed_at"] if row and row["completed_at"] is not None else 0) or 0) or None
    else:
        detail_value = str(detail or "").strip()
        started_value = started_at
        completed_value = completed_at
    if started_at is not None:
        started_value = int(started_at)
    if completed_at is not None:
        completed_value = int(completed_at)
    cur.execute(
        """
        UPDATE instagram_audit_batches
        SET state = ?,
            detail = ?,
            started_at = ?,
            completed_at = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            state_value,
            detail_value,
            started_value,
            completed_value,
            now,
            int(batch_id),
        ),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def update_instagram_audit_item(
    item_id: int,
    *,
    item_state: Optional[str] = None,
    assigned_serial: Optional[str] = None,
    login_state: Optional[str] = None,
    login_detail: Optional[str] = None,
    mail_probe_state: Optional[str] = None,
    mail_probe_detail: Optional[str] = None,
    resolution_state: Optional[str] = None,
    resolution_detail: Optional[str] = None,
    diagnostic_path: Optional[str] = None,
    started_at: Optional[int] = None,
    completed_at: Optional[int] = None,
) -> bool:
    updates: List[str] = []
    args: List[Any] = []
    if item_state is not None:
        updates.append("item_state = ?")
        args.append(normalize_instagram_audit_item_state(item_state))
    if assigned_serial is not None:
        updates.append("assigned_serial = ?")
        args.append(str(assigned_serial or "").strip())
    if login_state is not None:
        updates.append("login_state = ?")
        args.append(str(login_state or "").strip())
    if login_detail is not None:
        updates.append("login_detail = ?")
        args.append(str(login_detail or "").strip())
    if mail_probe_state is not None:
        updates.append("mail_probe_state = ?")
        args.append(normalize_instagram_audit_mail_probe_state(mail_probe_state))
    if mail_probe_detail is not None:
        updates.append("mail_probe_detail = ?")
        args.append(str(mail_probe_detail or "").strip())
    if resolution_state is not None:
        updates.append("resolution_state = ?")
        args.append(normalize_instagram_audit_resolution(resolution_state))
    if resolution_detail is not None:
        updates.append("resolution_detail = ?")
        args.append(str(resolution_detail or "").strip())
    if diagnostic_path is not None:
        updates.append("diagnostic_path = ?")
        args.append(str(diagnostic_path or "").strip())
    if started_at is not None:
        updates.append("started_at = ?")
        args.append(int(started_at))
    if completed_at is not None:
        updates.append("completed_at = ?")
        args.append(int(completed_at))
    if not updates:
        return False
    timestamp = int(time.time())
    updates.append("updated_at = ?")
    args.append(timestamp)
    args.append(int(item_id))
    conn = _connect()
    cur = conn.cursor()
    cur.execute(f"UPDATE instagram_audit_items SET {', '.join(updates)} WHERE id = ?", tuple(args))
    changed = cur.rowcount > 0
    if changed:
        cur.execute("SELECT account_id FROM instagram_audit_items WHERE id = ? LIMIT 1", (int(item_id),))
        row = cur.fetchone()
        account_id = int(row["account_id"] or 0) if row is not None and row["account_id"] is not None else 0
        if account_id > 0:
            _sync_account_auto_rotation_state_with_cursor(cur, account_id, now=timestamp)
    conn.commit()
    conn.close()
    return changed


def refresh_instagram_audit_batch_state(batch_id: int, *, detail: Optional[str] = None) -> Dict[str, Any]:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN item_state = 'done' THEN 1 ELSE 0 END) AS done_total,
            SUM(CASE WHEN item_state IN ('launching', 'login_check', 'mail_check_if_needed') THEN 1 ELSE 0 END) AS active_total,
            SUM(CASE WHEN item_state = 'queued' THEN 1 ELSE 0 END) AS queued_total,
            SUM(CASE WHEN resolution_state = 'login_ok' THEN 1 ELSE 0 END) AS ok_total,
            SUM(CASE WHEN resolution_state IN ('manual_2fa_required', 'email_code_required', 'challenge_required', 'invalid_password', 'helper_error', 'missing_credentials', 'missing_device') THEN 1 ELSE 0 END) AS issue_total
        FROM instagram_audit_items
        WHERE audit_batch_id = ?
        """,
        (int(batch_id),),
    )
    metrics = cur.fetchone()
    cur.execute("SELECT detail, started_at FROM instagram_audit_batches WHERE id = ?", (int(batch_id),))
    batch_row = cur.fetchone()
    total = int((metrics["total"] if metrics else 0) or 0)
    done_total = int((metrics["done_total"] if metrics else 0) or 0)
    active_total = int((metrics["active_total"] if metrics else 0) or 0)
    issue_total = int((metrics["issue_total"] if metrics else 0) or 0)
    started_at_value = int((batch_row["started_at"] if batch_row and batch_row["started_at"] is not None else 0) or 0) or None
    detail_value = str(detail if detail is not None else ((batch_row["detail"] if batch_row else "") or "")).strip()

    if total > 0 and done_total >= total:
        next_state = "completed_with_errors" if issue_total > 0 else "completed"
        completed_at = now
    elif active_total > 0 or done_total > 0 or started_at_value:
        next_state = "running"
        completed_at = None
    else:
        next_state = "queued"
        completed_at = None

    cur.execute(
        """
        UPDATE instagram_audit_batches
        SET state = ?,
            detail = ?,
            updated_at = ?,
            started_at = COALESCE(started_at, ?),
            completed_at = ?
        WHERE id = ?
        """,
        (
            next_state,
            detail_value,
            now,
            started_at_value or (now if next_state == "running" else None),
            completed_at,
            int(batch_id),
        ),
    )
    conn.commit()
    conn.close()
    return {
        "state": next_state,
        "total": total,
        "done_total": done_total,
        "active_total": active_total,
        "issue_total": issue_total,
    }


def get_latest_instagram_audit_for_account(account_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            i.audit_batch_id,
            i.account_id,
            i.item_state,
            i.assigned_serial,
            i.login_state,
            i.mail_probe_state,
            i.resolution_state,
            i.resolution_detail,
            i.diagnostic_path,
            i.updated_at,
            b.state AS batch_state
        FROM instagram_audit_items i
        JOIN instagram_audit_batches b ON b.id = i.audit_batch_id
        WHERE i.account_id = ?
        ORDER BY i.updated_at DESC, i.id DESC
        LIMIT 1
        """,
        (int(account_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def _runtime_task_natural_key(task_type: str, entity_type: str, entity_id: int) -> str:
    return f"{normalize_runtime_task_type(task_type)}:{normalize_runtime_task_entity_type(entity_type)}:{int(entity_id)}"


def create_or_reactivate_runtime_task(
    *,
    task_type: str,
    entity_type: str,
    entity_id: int,
    payload: Optional[Dict[str, Any]] = None,
    max_attempts: int = 3,
    available_at: Optional[int] = None,
    reactivate_if_terminal: bool = False,
) -> Dict[str, Any]:
    task_type_value = normalize_runtime_task_type(task_type)
    entity_type_value = normalize_runtime_task_entity_type(entity_type)
    natural_key = _runtime_task_natural_key(task_type_value, entity_type_value, int(entity_id))
    now = int(time.time())
    available_value = int(available_at if available_at is not None else now)
    payload_json = json.dumps(payload or {}, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            INSERT OR IGNORE INTO runtime_tasks (
                natural_key,
                task_type,
                entity_type,
                entity_id,
                state,
                payload_json,
                attempt_count,
                max_attempts,
                last_error,
                available_at,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, 'queued', ?, 0, ?, '', ?, ?, ?)
            """,
            (
                natural_key,
                task_type_value,
                entity_type_value,
                int(entity_id),
                payload_json,
                max(1, int(max_attempts or 1)),
                available_value,
                now,
                now,
            ),
        )
        cur.execute(
            """
            SELECT
                id,
                natural_key,
                task_type,
                entity_type,
                entity_id,
                state,
                payload_json,
                lease_owner,
                lease_expires_at,
                attempt_count,
                max_attempts,
                last_error,
                available_at,
                created_at,
                updated_at,
                started_at,
                completed_at,
                last_heartbeat_at
            FROM runtime_tasks
            WHERE natural_key = ?
            LIMIT 1
            """,
            (natural_key,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError("runtime task not created")
        if reactivate_if_terminal and str(row["state"] or "") in {"completed", "failed", "canceled"}:
            cur.execute(
                """
                UPDATE runtime_tasks
                SET state = 'queued',
                    payload_json = ?,
                    attempt_count = 0,
                    max_attempts = ?,
                    last_error = '',
                    available_at = ?,
                    updated_at = ?,
                    started_at = NULL,
                    completed_at = NULL,
                    lease_owner = NULL,
                    lease_expires_at = NULL,
                    last_heartbeat_at = NULL
                WHERE id = ?
                """,
                (payload_json, max(1, int(max_attempts or 1)), available_value, now, int(row["id"])),
            )
            cur.execute("SELECT * FROM runtime_tasks WHERE id = ?", (int(row["id"]),))
            row = cur.fetchone()
        conn.commit()
        return dict(row)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_runtime_task(task_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT * FROM runtime_tasks WHERE id = ?", (int(task_id),))
    row = cur.fetchone()
    conn.close()
    return row


def get_runtime_task_for_entity(task_type: str, entity_type: str, entity_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM runtime_tasks
        WHERE task_type = ?
          AND entity_type = ?
          AND entity_id = ?
        LIMIT 1
        """,
        (
            normalize_runtime_task_type(task_type),
            normalize_runtime_task_entity_type(entity_type),
            int(entity_id),
        ),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_runtime_tasks(limit: int = 100) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT *
        FROM runtime_tasks
        ORDER BY updated_at DESC, id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _expire_stale_runtime_tasks_with_cursor(cur: sqlite3.Cursor, *, now: Optional[int] = None) -> int:
    timestamp = int(now or time.time())
    cur.execute(
        """
        SELECT
            id,
            attempt_count,
            max_attempts
        FROM runtime_tasks
        WHERE state = 'running'
          AND COALESCE(lease_expires_at, 0) > 0
          AND COALESCE(lease_expires_at, 0) < ?
        """,
        (timestamp,),
    )
    rows = cur.fetchall()
    expired = 0
    for row in rows:
        task_id = int(row["id"])
        attempt_count = int(row["attempt_count"] or 0)
        max_attempts = max(1, int(row["max_attempts"] or 1))
        next_state = "failed" if attempt_count >= max_attempts else "retrying"
        completed_at = timestamp if next_state == "failed" else None
        cur.execute(
            """
            UPDATE runtime_tasks
            SET state = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                updated_at = ?,
                completed_at = CASE WHEN ? IS NOT NULL THEN ? ELSE completed_at END,
                available_at = CASE WHEN ? = 'retrying' THEN ? ELSE available_at END
            WHERE id = ?
            """,
            (
                next_state,
                "Runtime lease expired",
                timestamp,
                completed_at,
                completed_at,
                next_state,
                timestamp,
                task_id,
            ),
        )
        expired += 1
    return expired


def lease_next_runtime_task(
    *,
    worker_name: str,
    lease_seconds: int = 300,
    now: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    runner = (worker_name or "").strip() or "runtime-worker"
    lease_ttl = max(30, int(lease_seconds or 0))
    timestamp = int(now or time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        _expire_stale_runtime_tasks_with_cursor(cur, now=timestamp)
        cur.execute(
            """
            SELECT *
            FROM runtime_tasks
            WHERE state IN ('queued', 'retrying')
              AND COALESCE(available_at, 0) <= ?
            ORDER BY
              CASE WHEN task_type IN ('publish_reconcile', 'instagram_audit_reconcile') THEN 1 ELSE 0 END ASC,
              available_at ASC,
              created_at ASC,
              id ASC
            LIMIT 1
            """,
            (timestamp,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None
        cur.execute(
            """
            UPDATE runtime_tasks
            SET state = 'running',
                lease_owner = ?,
                lease_expires_at = ?,
                attempt_count = COALESCE(attempt_count, 0) + 1,
                updated_at = ?,
                started_at = COALESCE(started_at, ?),
                completed_at = NULL,
                last_heartbeat_at = ?
            WHERE id = ?
            """,
            (runner, timestamp + lease_ttl, timestamp, timestamp, timestamp, int(row["id"])),
        )
        cur.execute("SELECT * FROM runtime_tasks WHERE id = ?", (int(row["id"]),))
        leased = cur.fetchone()
        conn.commit()
        return dict(leased) if leased is not None else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def heartbeat_runtime_task(task_id: int, *, worker_name: str, lease_seconds: int = 300) -> bool:
    runner = (worker_name or "").strip() or "runtime-worker"
    now = int(time.time())
    lease_ttl = max(30, int(lease_seconds or 0))
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE runtime_tasks
        SET lease_expires_at = ?,
            last_heartbeat_at = ?,
            updated_at = ?
        WHERE id = ?
          AND state = 'running'
          AND COALESCE(lease_owner, '') = ?
        """,
        (now + lease_ttl, now, now, int(task_id), runner),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def complete_runtime_task(task_id: int, *, worker_name: str, last_error: str = "") -> bool:
    runner = (worker_name or "").strip() or "runtime-worker"
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE runtime_tasks
        SET state = 'completed',
            lease_owner = NULL,
            lease_expires_at = NULL,
            last_error = ?,
            updated_at = ?,
            completed_at = ?
        WHERE id = ?
          AND state = 'running'
          AND COALESCE(lease_owner, '') = ?
        """,
        ((last_error or "").strip(), now, now, int(task_id), runner),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def fail_runtime_task(
    task_id: int,
    *,
    worker_name: str,
    error: str,
    retryable: bool,
    retry_delay_seconds: int = 30,
) -> Dict[str, Any]:
    runner = (worker_name or "").strip() or "runtime-worker"
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT * FROM runtime_tasks WHERE id = ?", (int(task_id),))
        row = cur.fetchone()
        if row is None:
            raise ValueError("runtime task not found")
        if str(row["state"] or "") != "running":
            raise ValueError("runtime task is not running")
        if str(row["lease_owner"] or "") != runner:
            raise ValueError("runtime task is owned by another worker")
        attempt_count = int(row["attempt_count"] or 0)
        max_attempts = max(1, int(row["max_attempts"] or 1))
        next_state = "retrying" if retryable and attempt_count < max_attempts else "failed"
        completed_at = now if next_state == "failed" else None
        available_at = now + max(5, int(retry_delay_seconds or 0)) if next_state == "retrying" else int(row["available_at"] or now)
        cur.execute(
            """
            UPDATE runtime_tasks
            SET state = ?,
                lease_owner = NULL,
                lease_expires_at = NULL,
                last_error = ?,
                available_at = ?,
                updated_at = ?,
                completed_at = CASE WHEN ? IS NOT NULL THEN ? ELSE NULL END
            WHERE id = ?
              AND state = 'running'
              AND COALESCE(lease_owner, '') = ?
            """,
            (
                next_state,
                (error or "").strip(),
                available_at,
                now,
                completed_at,
                completed_at,
                int(task_id),
                runner,
            ),
        )
        if cur.rowcount <= 0:
            raise ValueError("runtime task update failed")
        cur.execute("SELECT * FROM runtime_tasks WHERE id = ?", (int(task_id),))
        updated = cur.fetchone()
        conn.commit()
        return dict(updated) if updated is not None else {}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reschedule_runtime_task(
    task_id: int,
    *,
    worker_name: str,
    delay_seconds: int,
    last_error: str = "",
) -> bool:
    runner = (worker_name or "").strip() or "runtime-worker"
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE runtime_tasks
            SET state = 'queued',
                lease_owner = NULL,
                lease_expires_at = NULL,
                attempt_count = 0,
                last_error = ?,
                available_at = ?,
                updated_at = ?,
                started_at = NULL,
                completed_at = NULL,
                last_heartbeat_at = NULL
            WHERE id = ?
              AND state = 'running'
              AND COALESCE(lease_owner, '') = ?
            """,
            (
                (last_error or "").strip(),
                now + max(5, int(delay_seconds or 0)),
                now,
                int(task_id),
                runner,
            ),
        )
        changed = cur.rowcount > 0
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_runtime_worker_heartbeat(
    worker_name: str,
    *,
    current_task_id: Optional[int] = None,
    last_error: Optional[str] = None,
    now: Optional[int] = None,
) -> None:
    worker = (worker_name or "").strip() or "runtime-worker"
    timestamp = int(now or time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO runtime_workers (worker_name, current_task_id, last_heartbeat_at, last_error, first_seen_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(worker_name) DO UPDATE SET
            current_task_id = excluded.current_task_id,
            last_heartbeat_at = excluded.last_heartbeat_at,
            last_error = CASE
                WHEN TRIM(excluded.last_error) <> '' THEN excluded.last_error
                ELSE runtime_workers.last_error
            END,
            updated_at = excluded.updated_at
        """,
        (
            worker,
            int(current_task_id) if current_task_id is not None else None,
            timestamp,
            (last_error or "").strip(),
            timestamp,
            timestamp,
        ),
    )
    conn.commit()
    conn.close()


def runtime_health_snapshot(*, live_timeout_seconds: int = 45) -> Dict[str, Any]:
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN state = 'queued' THEN 1 ELSE 0 END) AS queued_total,
            SUM(CASE WHEN state = 'retrying' THEN 1 ELSE 0 END) AS retrying_total,
            SUM(CASE WHEN state = 'running' THEN 1 ELSE 0 END) AS running_total,
            SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) AS failed_total
        FROM runtime_tasks
        """
    )
    counts = cur.fetchone()
    cur.execute(
        """
        SELECT MIN(available_at) AS oldest_queued_at
        FROM runtime_tasks
        WHERE state IN ('queued', 'retrying')
        """
    )
    oldest_row = cur.fetchone()
    cur.execute(
        """
        SELECT
            worker_name,
            current_task_id,
            last_heartbeat_at,
            last_error,
            first_seen_at,
            updated_at
        FROM runtime_workers
        ORDER BY last_heartbeat_at DESC, updated_at DESC
        """
    )
    workers = [dict(row) for row in cur.fetchall()]
    recent_failed = [dict(row) for row in list_runtime_tasks(limit=20) if str(row["state"] or "") == "failed"][:5]
    conn.close()
    oldest_queued_at = int((oldest_row["oldest_queued_at"] if oldest_row and oldest_row["oldest_queued_at"] is not None else 0) or 0) or None
    live_workers = [row for row in workers if int(row.get("last_heartbeat_at") or 0) >= now - max(10, int(live_timeout_seconds))]
    return {
        "workers": workers,
        "live_workers": live_workers,
        "counts": {
            "queued": int((counts["queued_total"] if counts else 0) or 0),
            "retrying": int((counts["retrying_total"] if counts else 0) or 0),
            "running": int((counts["running_total"] if counts else 0) or 0),
            "failed": int((counts["failed_total"] if counts else 0) or 0),
        },
        "oldest_queued_at": oldest_queued_at,
        "recent_failed": recent_failed,
    }


def create_account(
    account_type: str,
    account_login: str,
    account_password: str,
    username: str,
    email: str,
    email_password: str,
    proxy: Optional[str],
    twofa: Optional[str],
    mail_provider: Optional[str] = None,
    mail_auth_json: Optional[str] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    owner_worker_id: Optional[int] = None,
    instagram_emulator_serial: Optional[str] = None,
) -> int:
    t = _normalize_account_type(account_type)
    rotation_state_value = normalize_account_rotation_state(rotation_state)
    views_state_value = normalize_account_views_state(views_state)
    mail_provider_value = normalize_account_mail_provider(mail_provider)
    mail_auth_json_value = normalize_account_mail_auth_json(mail_auth_json)
    twofa_value = normalize_account_twofa_secret(twofa)
    login_clean = (account_login or "").strip()
    emulator_serial_clean = normalize_instagram_emulator_serial(instagram_emulator_serial)
    duplicate = find_duplicate_account(t, login_clean)
    if duplicate is not None:
        raise ValueError("duplicate account")
    now = int(time.time())
    owner_id = int(owner_worker_id) if owner_worker_id is not None else None
    if owner_id is not None and get_worker(owner_id) is None:
        raise ValueError("worker not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO accounts (
            type,
            account_login,
            account_password,
            username,
            email,
            email_password,
            proxy,
            twofa,
            mail_provider,
            mail_auth_json,
            instagram_emulator_serial,
            rotation_state,
            rotation_state_source,
            rotation_state_reason,
            views_state,
            owner_worker_id,
            created_at,
            updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            t,
            login_clean,
            (account_password or "").strip(),
            (username or "").strip(),
            sanitize_account_text_field(email),
            sanitize_account_text_field(email_password),
            (proxy or "").strip(),
            twofa_value,
            mail_provider_value,
            mail_auth_json_value,
            emulator_serial_clean,
            rotation_state_value,
            "manual",
            "",
            views_state_value,
            owner_id,
            now,
            now,
        ),
    )
    new_id = int(cur.lastrowid)
    _sync_account_auto_rotation_state_with_cursor(cur, new_id, now=now)
    conn.commit()
    conn.close()
    return new_id


def create_account_with_default_link(
    account_type: str,
    account_login: str,
    account_password: str,
    username: str,
    email: str,
    email_password: str,
    proxy: Optional[str],
    twofa: Optional[str],
    mail_provider: Optional[str] = None,
    mail_auth_json: Optional[str] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    owner_worker_id: Optional[int] = None,
    instagram_emulator_serial: Optional[str] = None,
    default_link_name: Optional[str] = None,
    target_url: str = "https://t.me/checkayugrambot?start={code}",
) -> Dict[str, Any]:
    t = _normalize_account_type(account_type)
    rotation_state_value = normalize_account_rotation_state(rotation_state)
    views_state_value = normalize_account_views_state(views_state)
    mail_provider_value = normalize_account_mail_provider(mail_provider)
    mail_auth_json_value = normalize_account_mail_auth_json(mail_auth_json)
    now = int(time.time())
    owner_id = int(owner_worker_id) if owner_worker_id is not None else None
    if owner_id is not None and get_worker(owner_id) is None:
        raise ValueError("worker not found")
    login_clean = (account_login or "").strip()
    duplicate = find_duplicate_account(t, login_clean)
    if duplicate is not None:
        raise ValueError("duplicate account")
    account_pass_clean = (account_password or "").strip()
    username_clean = (username or "").strip()
    email_clean = sanitize_account_text_field(email)
    email_pass_clean = sanitize_account_text_field(email_password)
    proxy_clean = (proxy or "").strip()
    twofa_clean = normalize_account_twofa_secret(twofa)
    emulator_serial_clean = normalize_instagram_emulator_serial(instagram_emulator_serial)
    link_name = (default_link_name or "").strip() or f"{t} @{username_clean or 'account'}"
    target_template = (target_url or "").strip() or "https://t.me/checkayugrambot?start={code}"

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN")
        cur.execute(
            """
            INSERT INTO accounts (
                type,
                account_login,
                account_password,
                username,
                email,
                email_password,
                proxy,
                twofa,
                mail_provider,
                mail_auth_json,
                instagram_emulator_serial,
                rotation_state,
                rotation_state_source,
                rotation_state_reason,
                views_state,
                owner_worker_id,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                t,
                login_clean,
                account_pass_clean,
                username_clean,
                email_clean,
                email_pass_clean,
                proxy_clean,
                twofa_clean,
                mail_provider_value,
                mail_auth_json_value,
                emulator_serial_clean,
                rotation_state_value,
                "manual",
                "",
                views_state_value,
                owner_id,
                now,
                now,
            ),
        )
        account_id = int(cur.lastrowid)
        _sync_account_auto_rotation_state_with_cursor(cur, account_id, now=now)
        code = _generate_unique_link_code_with_cursor(cur, length=6)
        cur.execute(
            """
            INSERT INTO links (
                code, name, target_url, partner_id, account_id, is_active, is_deleted, created_at, updated_at
            )
            VALUES (?, ?, ?, NULL, ?, 1, 0, ?, ?)
            """,
            (code, link_name, target_template.replace("{code}", code), account_id, now, now),
        )
        conn.commit()
        return {"account_id": account_id, "link_code": code}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_account(
    account_id: int,
    account_type: str,
    account_login: str,
    account_password: str,
    username: str,
    email: str,
    email_password: str,
    proxy: Optional[str],
    twofa: Optional[str],
    mail_provider: Optional[str] = None,
    mail_auth_json: Optional[str] = None,
    rotation_state: Optional[str] = None,
    views_state: Optional[str] = None,
    owner_worker_id: Optional[int] = None,
    instagram_emulator_serial: Optional[str] = None,
) -> bool:
    t = _normalize_account_type(account_type)
    rotation_state_value = normalize_account_rotation_state(rotation_state)
    views_state_value = normalize_account_views_state(views_state)
    mail_provider_value = normalize_account_mail_provider(mail_provider)
    mail_auth_json_value = normalize_account_mail_auth_json(mail_auth_json)
    twofa_value = normalize_account_twofa_secret(twofa)
    login_clean = (account_login or "").strip()
    emulator_serial_clean = normalize_instagram_emulator_serial(instagram_emulator_serial)
    duplicate = find_duplicate_account(t, login_clean, exclude_account_id=int(account_id))
    if duplicate is not None:
        raise ValueError("duplicate account")
    now = int(time.time())
    owner_id = int(owner_worker_id) if owner_worker_id is not None else None
    if owner_id is not None and get_worker(owner_id) is None:
        raise ValueError("worker not found")
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE accounts
        SET type = ?,
            account_login = ?,
            account_password = ?,
            username = ?,
            email = ?,
            email_password = ?,
            proxy = ?,
            twofa = ?,
            mail_provider = ?,
            mail_auth_json = ?,
            instagram_emulator_serial = ?,
            rotation_state = ?,
            rotation_state_source = 'manual',
            rotation_state_reason = '',
            views_state = ?,
            owner_worker_id = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            t,
            login_clean,
            (account_password or "").strip(),
            (username or "").strip(),
            sanitize_account_text_field(email),
            sanitize_account_text_field(email_password),
            (proxy or "").strip(),
            twofa_value,
            mail_provider_value,
            mail_auth_json_value,
            emulator_serial_clean,
            rotation_state_value,
            views_state_value,
            owner_id,
            now,
            int(account_id),
        ),
    )
    changed = cur.rowcount > 0
    if changed:
        _sync_account_auto_rotation_state_with_cursor(cur, int(account_id), now=now)
    conn.commit()
    conn.close()
    return changed


def delete_account(account_id: int, owner_worker_id: Optional[int] = None) -> bool:
    conn = _connect()
    cur = conn.cursor()
    params: List[Any] = [int(time.time()), int(account_id)]
    links_where = "account_id = ?"
    if owner_worker_id is not None:
        links_where += " AND account_id IN (SELECT id FROM accounts WHERE id = ? AND owner_worker_id = ?)"
        params = [int(time.time()), int(account_id), int(account_id), int(owner_worker_id)]
    cur.execute(
        f"""
        UPDATE links
        SET is_deleted = 1,
            is_active = 0,
            updated_at = ?
        WHERE {links_where}
          AND COALESCE(is_deleted, 0) = 0
        """,
        tuple(params),
    )
    if owner_worker_id is None:
        cur.execute("DELETE FROM accounts WHERE id = ?", (int(account_id),))
    else:
        cur.execute(
            "DELETE FROM accounts WHERE id = ? AND owner_worker_id = ?",
            (int(account_id), int(owner_worker_id)),
        )
    changed = cur.rowcount > 0
    if changed:
        cur.execute("DELETE FROM account_mail_messages WHERE account_id = ?", (int(account_id),))
    conn.commit()
    conn.close()
    return changed


def update_account_rotation_state(account_id: int, rotation_state: Optional[str] = None, *, reason: Optional[str] = None) -> bool:
    rotation_state_value = normalize_account_rotation_state(rotation_state)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE accounts
        SET rotation_state = ?,
            rotation_state_source = 'manual',
            rotation_state_reason = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (
            rotation_state_value,
            (reason or "").strip(),
            now,
            int(account_id),
        ),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def update_account_instagram_launch_state(account_id: int, status: str, detail: Optional[str] = None) -> bool:
    status_value = normalize_instagram_launch_status(status)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE accounts
            SET instagram_launch_status = ?,
                instagram_launch_detail = ?,
                instagram_launch_updated_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (status_value, (detail or "").strip(), now, now, int(account_id)),
        )
        changed = cur.rowcount > 0
        _sync_account_auto_rotation_state_with_cursor(cur, int(account_id), now=now)
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def update_account_instagram_publish_state(
    account_id: int,
    status: str,
    detail: Optional[str] = None,
    *,
    last_file: Optional[str] = None,
) -> bool:
    status_value = normalize_instagram_publish_status(status)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE accounts
            SET instagram_publish_status = ?,
                instagram_publish_detail = ?,
                instagram_publish_updated_at = ?,
                instagram_publish_last_file = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                status_value,
                (detail or "").strip(),
                now,
                (last_file or "").strip(),
                now,
                int(account_id),
            ),
        )
        changed = cur.rowcount > 0
        _sync_account_auto_rotation_state_with_cursor(cur, int(account_id), now=now)
        conn.commit()
        return changed
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _publish_job_state_to_account_publish_state(job_state: str, payload: Optional[Dict[str, Any]] = None) -> str:
    value = normalize_publish_job_state(job_state)
    explicit_state = ""
    if isinstance(payload, dict):
        explicit_state = str(payload.get("account_publish_state") or "").strip().lower()
    if explicit_state:
        try:
            return normalize_instagram_publish_status(explicit_state)
        except ValueError:
            pass
    if value in INSTAGRAM_PUBLISH_STATUS_KEYS:
        return value
    if value == "leased":
        return "preparing"
    if value in {"failed", "canceled"}:
        return "publish_error"
    return "preparing"


def _publish_job_state_to_batch_account_state(job_state: str) -> str:
    value = normalize_publish_job_state(job_state)
    if value == "queued":
        return "queued_for_publish"
    return normalize_publish_batch_account_state(value)


def _is_publish_job_state_regression(current_state: str, next_state: str) -> bool:
    current_value = normalize_publish_job_state(current_state)
    next_value = normalize_publish_job_state(next_state)
    if next_value in {"failed", "canceled"}:
        return False
    return PUBLISH_JOB_STATE_ORDER.get(next_value, 0) < PUBLISH_JOB_STATE_ORDER.get(current_value, 0)


def _append_publish_job_event_with_cursor(
    cur: sqlite3.Cursor,
    *,
    batch_id: int,
    state: str,
    detail: Optional[str],
    payload: Optional[Dict[str, Any]] = None,
    job_id: Optional[int] = None,
    account_id: Optional[int] = None,
    event_hash: Optional[str] = None,
    created_at: Optional[int] = None,
) -> None:
    cur.execute(
        """
        INSERT INTO publish_job_events (batch_id, job_id, account_id, state, detail, payload_json, event_hash, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(batch_id),
            int(job_id) if job_id is not None else None,
            int(account_id) if account_id is not None else None,
            (state or "").strip(),
            (detail or "").strip(),
            json.dumps(payload, ensure_ascii=False, sort_keys=True) if payload is not None else None,
            (event_hash or "").strip() or None,
            int(created_at or time.time()),
        ),
    )


def publish_event_hash_exists(batch_id: int, event_hash: str) -> bool:
    value = (event_hash or "").strip()
    if not value:
        return False
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM publish_job_events
        WHERE batch_id = ? AND event_hash = ?
        LIMIT 1
        """,
        (int(batch_id), value),
    )
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def _set_publish_batch_account_state_with_cursor(
    cur: sqlite3.Cursor,
    *,
    batch_id: int,
    account_id: int,
    state: str,
    detail: Optional[str] = None,
    artifact_id: Optional[int] = None,
    job_id: Optional[int] = None,
    updated_at: Optional[int] = None,
) -> None:
    state_value = normalize_publish_batch_account_state(state)
    timestamp = int(updated_at or time.time())
    cur.execute(
        """
        UPDATE publish_batch_accounts
        SET state = ?,
            detail = ?,
            artifact_id = COALESCE(?, artifact_id),
            job_id = COALESCE(?, job_id),
            updated_at = ?
        WHERE batch_id = ? AND account_id = ?
        """,
        (
            state_value,
            (detail or "").strip(),
            int(artifact_id) if artifact_id is not None else None,
            int(job_id) if job_id is not None else None,
            timestamp,
            int(batch_id),
            int(account_id),
        ),
    )
    if cur.rowcount <= 0:
        raise ValueError("batch account not found")


def _publish_batch_account_metrics_with_cursor(cur: sqlite3.Cursor, batch_id: int) -> Dict[str, int]:
    cur.execute(
        """
        SELECT
            COALESCE(COUNT(*), 0) AS accounts_total,
            COALESCE(SUM(CASE WHEN state = 'queued_for_generation' THEN 1 ELSE 0 END), 0) AS queued_generation_accounts,
            COALESCE(SUM(CASE WHEN state = 'generating' THEN 1 ELSE 0 END), 0) AS generating_accounts,
            COALESCE(SUM(CASE WHEN state = 'queued_for_publish' THEN 1 ELSE 0 END), 0) AS queued_publish_accounts,
            COALESCE(SUM(CASE WHEN state IN ('leased', 'preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing') THEN 1 ELSE 0 END), 0) AS active_publish_accounts,
            COALESCE(SUM(CASE WHEN state = 'published' THEN 1 ELSE 0 END), 0) AS published_accounts,
            COALESCE(SUM(CASE WHEN state = 'needs_review' THEN 1 ELSE 0 END), 0) AS needs_review_accounts,
            COALESCE(SUM(CASE WHEN state = 'generation_failed' THEN 1 ELSE 0 END), 0) AS generation_failed_accounts,
            COALESCE(SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END), 0) AS failed_accounts,
            COALESCE(SUM(CASE WHEN state = 'canceled' THEN 1 ELSE 0 END), 0) AS canceled_accounts,
            COALESCE(SUM(CASE WHEN state IN ('generation_failed', 'published', 'needs_review', 'failed', 'canceled') THEN 1 ELSE 0 END), 0) AS terminal_accounts
        FROM publish_batch_accounts
        WHERE batch_id = ?
        """,
        (int(batch_id),),
    )
    row = cur.fetchone()
    if row is None:
        return {
            "accounts_total": 0,
            "queued_generation_accounts": 0,
            "generating_accounts": 0,
            "queued_publish_accounts": 0,
            "active_publish_accounts": 0,
            "published_accounts": 0,
            "needs_review_accounts": 0,
            "generation_failed_accounts": 0,
            "failed_accounts": 0,
            "canceled_accounts": 0,
            "terminal_accounts": 0,
        }
    return {key: int(row[key] or 0) for key in row.keys()}


def _publish_batch_metrics_with_cursor(cur: sqlite3.Cursor, batch_id: int) -> Dict[str, int]:
    cur.execute(
        """
        SELECT
            COALESCE((SELECT COUNT(*) FROM publish_artifacts WHERE batch_id = ?), 0) AS artifacts_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ?), 0) AS jobs_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'queued'), 0) AS queued_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'leased'), 0) AS leased_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'published'), 0) AS published_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'needs_review'), 0) AS needs_review_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'failed'), 0) AS failed_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state = 'canceled'), 0) AS canceled_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs WHERE batch_id = ? AND state IN ('preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing')), 0) AS running_jobs
        """,
        (int(batch_id),) * 9,
    )
    row = cur.fetchone()
    account_metrics = _publish_batch_account_metrics_with_cursor(cur, int(batch_id))
    if row is None:
        metrics = {
            "artifacts_total": 0,
            "jobs_total": 0,
            "queued_jobs": 0,
            "leased_jobs": 0,
            "published_jobs": 0,
            "needs_review_jobs": 0,
            "failed_jobs": 0,
            "canceled_jobs": 0,
            "running_jobs": 0,
        }
    else:
        metrics = {key: int(row[key] or 0) for key in row.keys()}
    metrics.update(account_metrics)
    metrics["error_accounts"] = (
        int(metrics.get("generation_failed_accounts", 0))
        + int(metrics.get("failed_accounts", 0))
        + int(metrics.get("canceled_accounts", 0))
    )
    metrics["review_accounts"] = int(metrics.get("needs_review_accounts", 0))
    return metrics


def _refresh_publish_batch_state_with_cursor(cur: sqlite3.Cursor, batch_id: int, *, now: Optional[int] = None) -> Dict[str, Any]:
    timestamp = int(now or time.time())
    cur.execute(
        """
        SELECT
            id,
            state,
            detail,
            generation_started_at,
            generation_completed_at,
            completed_at,
            canceled_at
        FROM publish_batches
        WHERE id = ?
        """,
        (int(batch_id),),
    )
    batch = cur.fetchone()
    if batch is None:
        raise ValueError("batch not found")

    current_state = normalize_publish_batch_state(str(batch["state"] or "queued_to_worker"))
    if current_state in {"failed_generation", "completed", "completed_needs_review", "completed_with_errors", "canceled"}:
        metrics = _publish_batch_metrics_with_cursor(cur, int(batch_id))
        return {"state": current_state, **metrics}

    metrics = _publish_batch_metrics_with_cursor(cur, int(batch_id))
    next_state = current_state
    completed_at = batch["completed_at"]
    detail = str(batch["detail"] or "").strip()
    accounts_total = int(metrics.get("accounts_total", 0))
    terminal_accounts = int(metrics.get("terminal_accounts", 0))
    published_accounts = int(metrics.get("published_accounts", 0))
    needs_review_accounts = int(metrics.get("needs_review_accounts", 0))
    generation_failed_accounts = int(metrics.get("generation_failed_accounts", 0))
    failed_accounts = int(metrics.get("failed_accounts", 0))
    canceled_accounts = int(metrics.get("canceled_accounts", 0))
    jobs_total = int(metrics.get("jobs_total", 0))

    all_accounts_terminal = accounts_total > 0 and terminal_accounts >= accounts_total
    publish_started = (
        jobs_total > 0
        or int(metrics.get("queued_publish_accounts", 0)) > 0
        or int(metrics.get("active_publish_accounts", 0)) > 0
        or published_accounts > 0
        or needs_review_accounts > 0
        or failed_accounts > 0
        or canceled_accounts > 0
    )

    if all_accounts_terminal:
        if published_accounts == accounts_total:
            next_state = "completed"
            completed_at = timestamp
        elif (
            needs_review_accounts > 0
            and generation_failed_accounts == 0
            and failed_accounts == 0
            and canceled_accounts == 0
        ):
            next_state = "completed_needs_review"
            completed_at = timestamp
        elif published_accounts > 0 or jobs_total > 0:
            next_state = "completed_with_errors"
            completed_at = timestamp
        elif generation_failed_accounts == accounts_total:
            next_state = "failed_generation"
            completed_at = timestamp
            detail = detail or "Генерация не создала ни одного publish job."
        else:
            next_state = "completed_with_errors"
            completed_at = timestamp
    elif publish_started:
        next_state = "publishing"
        completed_at = None
    elif current_state in {"queued_to_worker", "worker_started"} and not batch["generation_started_at"]:
        next_state = current_state
        completed_at = None
    else:
        next_state = "generating"
        completed_at = None

    cur.execute(
        """
        UPDATE publish_batches
        SET state = ?,
            detail = ?,
            updated_at = ?,
            completed_at = ?
        WHERE id = ?
        """,
        (next_state, detail, timestamp, completed_at, int(batch_id)),
    )
    return {"state": next_state, **metrics}


def list_publish_ready_accounts(limit: int = 500) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            a.id,
            a.type,
            a.account_login,
            a.username,
            COALESCE(a.email, '') AS email,
            a.email_password,
            COALESCE(a.mail_provider, 'auto') AS mail_provider,
            COALESCE(a.mail_auth_json, '') AS mail_auth_json,
            COALESCE(a.mail_status, 'never_checked') AS mail_status,
            COALESCE(a.mail_last_error, '') AS mail_last_error,
            COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
            COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
            COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
            COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
            COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
            a.mail_challenge_updated_at,
            COALESCE(a.twofa, '') AS twofa,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
            a.instagram_publish_updated_at,
            COALESCE(a.rotation_state, 'review') AS rotation_state,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            CASE WHEN TRIM(COALESCE(a.account_password, '')) <> '' THEN 1 ELSE 0 END AS has_account_password,
            a.updated_at
        FROM accounts a
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        WHERE a.type = 'instagram'
        ORDER BY a.updated_at DESC, a.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return [row for row in rows if not publish_account_readiness_issues(row)]


def list_publish_blocked_accounts(limit: int = 500) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            a.id,
            a.type,
            a.account_login,
            a.username,
            COALESCE(a.email, '') AS email,
            a.email_password,
            COALESCE(a.mail_provider, 'auto') AS mail_provider,
            COALESCE(a.mail_auth_json, '') AS mail_auth_json,
            COALESCE(a.mail_status, 'never_checked') AS mail_status,
            COALESCE(a.mail_last_error, '') AS mail_last_error,
            COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
            COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
            COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
            COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
            COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
            a.mail_challenge_updated_at,
            COALESCE(a.twofa, '') AS twofa,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
            a.instagram_publish_updated_at,
            COALESCE(a.rotation_state, 'review') AS rotation_state,
            a.owner_worker_id,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            CASE WHEN TRIM(COALESCE(a.account_password, '')) <> '' THEN 1 ELSE 0 END AS has_account_password,
            a.updated_at
        FROM accounts a
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        WHERE a.type = 'instagram'
        ORDER BY a.updated_at DESC, a.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return [row for row in rows if publish_account_readiness_issues(row)]


def create_publish_batch(
    account_ids: List[int],
    *,
    created_by_admin: Optional[str],
    workflow_key: str = "default",
) -> Dict[str, Any]:
    unique_ids: List[int] = []
    seen_ids: set[int] = set()
    for raw_account_id in account_ids:
        account_id = int(raw_account_id)
        if account_id <= 0 or account_id in seen_ids:
            continue
        seen_ids.add(account_id)
        unique_ids.append(account_id)
    if not unique_ids:
        raise ValueError("no accounts selected")

    conn = _connect()
    cur = conn.cursor()
    placeholders = ",".join("?" for _ in unique_ids)
    cur.execute(
        f"""
        SELECT
            id,
            type,
            account_login,
            account_password,
            username,
            COALESCE(rotation_state, 'review') AS rotation_state,
            COALESCE(instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(twofa, '') AS twofa
        FROM accounts
        WHERE id IN ({placeholders})
        """,
        tuple(unique_ids),
    )
    rows = {int(row["id"]): row for row in cur.fetchall()}
    missing = [account_id for account_id in unique_ids if account_id not in rows]
    if missing:
        conn.close()
        raise ValueError("account not found")
    for account_id in unique_ids:
        row = rows[account_id]
        if str(row["type"] or "").strip().lower() != "instagram":
            conn.close()
            raise ValueError("account is not instagram")
        issues = publish_account_readiness_issues(row)
        if issues:
            conn.close()
            raise ValueError(f"account {account_id} is not ready for fully-auto publish: {' '.join(issues)}")

    now = int(time.time())
    try:
        cur.execute("BEGIN")
        cur.execute(
            """
            INSERT INTO publish_batches (state, detail, workflow_key, created_by_admin, created_at, updated_at)
            VALUES ('queued_to_worker', 'Batch создан. Жду runtime worker для старта n8n workflow.', ?, ?, ?, ?)
            """,
            ((workflow_key or "default").strip() or "default", (created_by_admin or "").strip(), now, now),
        )
        batch_id = int(cur.lastrowid)
        for queue_position, account_id in enumerate(unique_ids):
            cur.execute(
                """
                INSERT INTO publish_batch_accounts (
                    batch_id, account_id, queue_position, state, detail, created_at, updated_at
                )
                VALUES (?, ?, ?, 'queued_for_generation', 'Ожидает очереди генерации.', ?, ?)
                """,
                (batch_id, int(account_id), int(queue_position), now, now),
            )
        _append_publish_job_event_with_cursor(
            cur,
            batch_id=batch_id,
            state="batch_created",
            detail=f"Batch создан для {len(unique_ids)} аккаунтов.",
            payload={"account_ids": unique_ids, "workflow_key": (workflow_key or "default").strip() or "default"},
            created_at=now,
        )
        conn.commit()
    finally:
        conn.close()
    return {"batch_id": batch_id, "account_ids": unique_ids}


def get_publish_batch(batch_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            b.id,
            b.state,
            COALESCE(b.detail, '') AS detail,
            COALESCE(b.workflow_key, 'default') AS workflow_key,
            COALESCE(b.created_by_admin, '') AS created_by_admin,
            b.created_at,
            b.updated_at,
            b.generation_started_at,
            b.generation_completed_at,
            b.completed_at,
            b.canceled_at,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id), 0) AS accounts_total,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'queued_for_generation'), 0) AS queued_generation_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'generating'), 0) AS generating_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'queued_for_publish'), 0) AS queued_publish_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state IN ('leased', 'preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing')), 0) AS active_publish_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'published'), 0) AS published_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'needs_review'), 0) AS needs_review_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'generation_failed'), 0) AS generation_failed_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'failed'), 0) AS failed_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'canceled'), 0) AS canceled_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_artifacts pa WHERE pa.batch_id = b.id), 0) AS artifacts_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id), 0) AS jobs_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'queued'), 0) AS queued_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'leased'), 0) AS leased_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state IN ('preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing')), 0) AS running_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'published'), 0) AS published_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'needs_review'), 0) AS needs_review_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'failed'), 0) AS failed_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'canceled'), 0) AS canceled_jobs
        FROM publish_batches b
        WHERE b.id = ?
        LIMIT 1
        """,
        (int(batch_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_publish_batches(limit: int = 25) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            b.id,
            b.state,
            COALESCE(b.detail, '') AS detail,
            COALESCE(b.workflow_key, 'default') AS workflow_key,
            COALESCE(b.created_by_admin, '') AS created_by_admin,
            b.created_at,
            b.updated_at,
            b.generation_started_at,
            b.generation_completed_at,
            b.completed_at,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id), 0) AS accounts_total,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'queued_for_generation'), 0) AS queued_generation_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'generating'), 0) AS generating_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'queued_for_publish'), 0) AS queued_publish_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state IN ('leased', 'preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing')), 0) AS active_publish_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'published'), 0) AS published_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'needs_review'), 0) AS needs_review_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'generation_failed'), 0) AS generation_failed_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'failed'), 0) AS failed_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_batch_accounts pba WHERE pba.batch_id = b.id AND pba.state = 'canceled'), 0) AS canceled_accounts,
            COALESCE((SELECT COUNT(*) FROM publish_artifacts pa WHERE pa.batch_id = b.id), 0) AS artifacts_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id), 0) AS jobs_total,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'published'), 0) AS published_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'needs_review'), 0) AS needs_review_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'failed'), 0) AS failed_jobs,
            COALESCE((SELECT COUNT(*) FROM publish_jobs pj WHERE pj.batch_id = b.id AND pj.state = 'canceled'), 0) AS canceled_jobs
        FROM publish_batches b
        ORDER BY b.created_at DESC, b.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_publish_batch_accounts(batch_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            pba.account_id,
            a.id,
            a.type,
            a.account_login,
            a.username,
            COALESCE(a.email, '') AS email,
            a.email_password,
            COALESCE(a.mail_provider, 'auto') AS mail_provider,
            COALESCE(a.mail_auth_json, '') AS mail_auth_json,
            COALESCE(a.mail_status, 'never_checked') AS mail_status,
            COALESCE(a.mail_last_error, '') AS mail_last_error,
            COALESCE(a.mail_challenge_status, 'idle') AS mail_challenge_status,
            COALESCE(a.mail_challenge_kind, '') AS mail_challenge_kind,
            COALESCE(a.mail_challenge_reason_code, '') AS mail_challenge_reason_code,
            COALESCE(a.mail_challenge_reason_text, '') AS mail_challenge_reason_text,
            COALESCE(a.mail_challenge_message_uid, '') AS mail_challenge_message_uid,
            a.mail_challenge_received_at,
            COALESCE(a.mail_challenge_masked_code, '') AS mail_challenge_masked_code,
            COALESCE(a.mail_challenge_confidence, 0) AS mail_challenge_confidence,
            a.mail_challenge_updated_at,
            COALESCE(a.twofa, '') AS twofa,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial,
            COALESCE(a.instagram_publish_status, 'idle') AS instagram_publish_status,
            COALESCE(a.instagram_publish_detail, '') AS instagram_publish_detail,
            a.instagram_publish_updated_at,
            COALESCE(w.name, '') AS owner_worker_name,
            COALESCE(w.username, '') AS owner_worker_username,
            COALESCE(pba.queue_position, 0) AS queue_position,
            pba.state,
            COALESCE(pba.detail, '') AS detail,
            pba.artifact_id,
            pba.job_id,
            pba.created_at,
            pba.updated_at,
            COALESCE(pa.filename, '') AS artifact_filename,
            COALESCE(pa.path, '') AS artifact_path,
            COALESCE(pj.state, '') AS job_state,
            COALESCE(pj.detail, '') AS job_detail
        FROM publish_batch_accounts pba
        JOIN accounts a ON a.id = pba.account_id
        LEFT JOIN workers w ON w.id = a.owner_worker_id
        LEFT JOIN publish_artifacts pa ON pa.id = pba.artifact_id
        LEFT JOIN publish_jobs pj ON pj.id = pba.job_id
        WHERE pba.batch_id = ?
        ORDER BY COALESCE(pba.queue_position, 2147483647) ASC, pba.created_at ASC, a.id ASC
        """,
        (int(batch_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_publish_next_generation_account(batch_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            pba.account_id,
            COALESCE(pba.queue_position, 0) AS queue_position,
            pba.state,
            a.id,
            a.username,
            a.account_login,
            COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial
        FROM publish_batch_accounts pba
        JOIN accounts a ON a.id = pba.account_id
        WHERE pba.batch_id = ?
          AND pba.state = 'queued_for_generation'
        ORDER BY COALESCE(pba.queue_position, 2147483647) ASC, pba.created_at ASC, pba.account_id ASC
        LIMIT 1
        """,
        (int(batch_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_publish_batch_account_state(batch_id: int, account_id: int) -> Optional[str]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT state
        FROM publish_batch_accounts
        WHERE batch_id = ? AND account_id = ?
        LIMIT 1
        """,
        (int(batch_id), int(account_id)),
    )
    row = cur.fetchone()
    conn.close()
    if row is None:
        return None
    return normalize_publish_batch_account_state(str(row["state"] or "queued_for_generation"))


def list_publish_artifacts(batch_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            batch_id,
            path,
            filename,
            COALESCE(checksum, '') AS checksum,
            size_bytes,
            duration_seconds,
            created_at,
            updated_at
        FROM publish_artifacts
        WHERE batch_id = ?
        ORDER BY created_at ASC, id ASC
        """,
        (int(batch_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_publish_artifact(batch_id: int, artifact_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            id,
            batch_id,
            path,
            filename,
            COALESCE(checksum, '') AS checksum,
            size_bytes,
            duration_seconds,
            created_at,
            updated_at
        FROM publish_artifacts
        WHERE batch_id = ? AND id = ?
        LIMIT 1
        """,
        (int(batch_id), int(artifact_id)),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_publish_jobs(batch_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            j.id,
            j.batch_id,
            j.artifact_id,
            j.account_id,
            j.emulator_serial,
            j.state,
            COALESCE(j.detail, '') AS detail,
            j.source_path,
            j.source_name,
            COALESCE(j.leased_by, '') AS leased_by,
            j.leased_at,
            j.lease_expires_at,
            j.started_at,
            j.completed_at,
            COALESCE(j.last_file, '') AS last_file,
            COALESCE(j.last_error, '') AS last_error,
            j.created_at,
            j.updated_at,
            COALESCE(a.username, '') AS account_username,
            COALESCE(a.account_login, '') AS account_login,
            COALESCE(pa.filename, '') AS artifact_filename
        FROM publish_jobs j
        JOIN accounts a ON a.id = j.account_id
        JOIN publish_artifacts pa ON pa.id = j.artifact_id
        WHERE j.batch_id = ?
        ORDER BY j.created_at ASC, j.id ASC
        """,
        (int(batch_id),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_publish_job(job_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            j.id,
            j.batch_id,
            j.artifact_id,
            j.account_id,
            j.emulator_serial,
            j.state,
            COALESCE(j.detail, '') AS detail,
            j.source_path,
            j.source_name,
            COALESCE(j.leased_by, '') AS leased_by,
            j.leased_at,
            j.lease_expires_at,
            j.started_at,
            j.completed_at,
            COALESCE(j.last_file, '') AS last_file,
            COALESCE(j.last_error, '') AS last_error,
            j.created_at,
            j.updated_at,
            COALESCE(a.username, '') AS account_username,
            COALESCE(a.account_login, '') AS account_login,
            COALESCE(pa.filename, '') AS artifact_filename
        FROM publish_jobs j
        JOIN accounts a ON a.id = j.account_id
        JOIN publish_artifacts pa ON pa.id = j.artifact_id
        WHERE j.id = ?
        LIMIT 1
        """,
        (int(job_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def list_publish_job_events(batch_id: int, limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            e.id,
            e.batch_id,
            e.job_id,
            COALESCE(e.account_id, j.account_id) AS account_id,
            e.state,
            COALESCE(e.detail, '') AS detail,
            COALESCE(e.payload_json, '') AS payload_json,
            e.created_at,
            COALESCE(a.username, '') AS account_username,
            COALESCE(j.source_name, '') AS source_name
        FROM publish_job_events e
        LEFT JOIN publish_jobs j ON j.id = e.job_id
        LEFT JOIN accounts a ON a.id = COALESCE(e.account_id, j.account_id)
        WHERE e.batch_id = ?
        ORDER BY e.created_at DESC, e.id DESC
        LIMIT ?
        """,
        (int(batch_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def append_publish_job_event(
    batch_id: int,
    *,
    state: str,
    detail: Optional[str],
    payload: Optional[Dict[str, Any]] = None,
    job_id: Optional[int] = None,
    account_id: Optional[int] = None,
) -> None:
    conn = _connect()
    cur = conn.cursor()
    _append_publish_job_event_with_cursor(
        cur,
        batch_id=int(batch_id),
        job_id=int(job_id) if job_id is not None else None,
        account_id=int(account_id) if account_id is not None else None,
        state=(state or "").strip(),
        detail=detail,
        payload=payload,
        created_at=int(time.time()),
    )
    conn.commit()
    conn.close()


def update_publish_batch_state(batch_id: int, state: str, detail: Optional[str] = None) -> bool:
    state_value = normalize_publish_batch_state(state)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    completed_at = now if state_value in {"completed", "completed_needs_review", "completed_with_errors", "failed_generation", "canceled"} else None
    generation_started_at = now if state_value == "generating" else None
    generation_completed_at = now if state_value in {"publishing", "completed", "completed_needs_review", "completed_with_errors", "failed_generation"} else None
    cur.execute(
        """
        UPDATE publish_batches
        SET state = ?,
            detail = ?,
            updated_at = ?,
            generation_started_at = COALESCE(generation_started_at, ?),
            generation_completed_at = CASE
                WHEN ? IS NOT NULL THEN COALESCE(generation_completed_at, ?)
                ELSE generation_completed_at
            END,
            completed_at = ?,
            canceled_at = CASE WHEN ? = 'canceled' THEN COALESCE(canceled_at, ?) ELSE canceled_at END
        WHERE id = ?
        """,
        (
            state_value,
            (detail or "").strip(),
            now,
            generation_started_at,
            generation_completed_at,
            generation_completed_at,
            completed_at,
            state_value,
            now,
            int(batch_id),
        ),
    )
    changed = cur.rowcount > 0
    if changed:
        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(batch_id),
            state=state_value,
            detail=detail,
            payload={"source": "manual"},
            created_at=now,
        )
    conn.commit()
    conn.close()
    return changed


def fail_stale_generation_accounts(
    *,
    batch_id: Optional[int] = None,
    timeout_seconds: int,
    now: Optional[int] = None,
) -> List[Dict[str, Any]]:
    timeout_value = int(timeout_seconds or 0)
    if timeout_value <= 0:
        return []
    timestamp = int(now or time.time())
    cutoff = timestamp - timeout_value
    conn = _connect()
    cur = conn.cursor()
    results: List[Dict[str, Any]] = []
    try:
        cur.execute("BEGIN IMMEDIATE")
        filters = [int(cutoff)]
        where = "state = 'generating' AND updated_at < ?"
        if batch_id is not None:
            where += " AND batch_id = ?"
            filters.append(int(batch_id))
        cur.execute(
            f"""
            SELECT batch_id, account_id, updated_at
            FROM publish_batch_accounts
            WHERE {where}
            """,
            tuple(filters),
        )
        rows = cur.fetchall()
        if not rows:
            conn.rollback()
            return []
        by_batch: Dict[int, List[int]] = {}
        detail_value = f"Generation timed out after {timeout_value} seconds."
        for row in rows:
            batch_id_value = int(row["batch_id"])
            account_id_value = int(row["account_id"])
            cur.execute(
                """
                UPDATE publish_batch_accounts
                SET state = 'generation_failed',
                    detail = ?,
                    updated_at = ?
                WHERE batch_id = ? AND account_id = ? AND state = 'generating'
                """,
                (detail_value, timestamp, batch_id_value, account_id_value),
            )
            _append_publish_job_event_with_cursor(
                cur,
                batch_id=batch_id_value,
                account_id=account_id_value,
                state="generation_failed",
                detail=detail_value,
                payload={"timeout_seconds": timeout_value, "cutoff": cutoff},
                created_at=timestamp,
            )
            by_batch.setdefault(batch_id_value, []).append(account_id_value)
            results.append(
                {
                    "batch_id": batch_id_value,
                    "account_id": account_id_value,
                    "timeout_seconds": timeout_value,
                }
            )
        for batch_id_value, account_ids in by_batch.items():
            cur.execute(
                """
                UPDATE publish_batches
                SET detail = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (f"Generation timed out for {len(account_ids)} account(s).", timestamp, batch_id_value),
            )
            _refresh_publish_batch_state_with_cursor(cur, batch_id_value, now=timestamp)
        conn.commit()
        return results
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_publish_generation_started(
    batch_id: int,
    detail: Optional[str] = None,
    *,
    account_id: Optional[int] = None,
    event_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    detail_value = (detail or "").strip() or "n8n начал генерацию видео."
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE publish_batches
        SET detail = ?,
            updated_at = ?,
            generation_started_at = COALESCE(generation_started_at, ?)
        WHERE id = ?
        """,
        (detail_value, now, now, int(batch_id)),
    )
    if cur.rowcount <= 0:
        conn.close()
        raise ValueError("batch not found")
    if account_id is not None:
        _set_publish_batch_account_state_with_cursor(
            cur,
            batch_id=int(batch_id),
            account_id=int(account_id),
            state="generating",
            detail=detail_value,
            updated_at=now,
        )
    _append_publish_job_event_with_cursor(
        cur,
        batch_id=int(batch_id),
        state="generation_started",
        detail=detail_value,
        payload={"account_id": int(account_id)} if account_id is not None else None,
        account_id=int(account_id) if account_id is not None else None,
        event_hash=event_hash,
        created_at=now,
    )
    metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
    conn.commit()
    conn.close()
    return metrics


def mark_publish_batch_worker_started(batch_id: int, detail: Optional[str] = None) -> Dict[str, Any]:
    now = int(time.time())
    detail_value = (detail or "").strip() or "Runtime worker начал запуск n8n workflow."
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE publish_batches
        SET state = CASE
                WHEN state IN ('queued_to_worker', 'worker_started') THEN 'worker_started'
                ELSE state
            END,
            detail = ?,
            updated_at = ?
        WHERE id = ?
        """,
        (detail_value, now, int(batch_id)),
    )
    if cur.rowcount <= 0:
        conn.close()
        raise ValueError("batch not found")
    metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
    conn.commit()
    conn.close()
    return metrics


def mark_publish_generation_completed(
    batch_id: int,
    detail: Optional[str] = None,
    *,
    event_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    detail_value = (detail or "").strip() or "n8n закончил генерацию видео."
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE publish_batches
        SET detail = ?,
            updated_at = ?,
            generation_started_at = COALESCE(generation_started_at, ?),
            generation_completed_at = COALESCE(generation_completed_at, ?)
        WHERE id = ?
        """,
        (detail_value, now, now, now, int(batch_id)),
    )
    if cur.rowcount <= 0:
        conn.close()
        raise ValueError("batch not found")
    _append_publish_job_event_with_cursor(
        cur,
        batch_id=int(batch_id),
        state="generation_completed",
        detail=detail_value,
        event_hash=event_hash,
        created_at=now,
    )
    metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
    conn.commit()
    conn.close()
    return metrics


def mark_publish_generation_failed(
    batch_id: int,
    detail: Optional[str],
    *,
    account_id: Optional[int] = None,
    payload: Optional[Dict[str, Any]] = None,
    event_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    detail_value = (detail or "").strip() or "n8n вернул ошибку генерации."
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            UPDATE publish_batches
            SET detail = ?,
                updated_at = ?,
                generation_started_at = COALESCE(generation_started_at, ?),
                generation_completed_at = CASE
                    WHEN ? IS NULL THEN COALESCE(generation_completed_at, ?)
                    ELSE generation_completed_at
                END
            WHERE id = ?
            """,
            (detail_value, now, now, account_id, now, int(batch_id)),
        )
        if cur.rowcount <= 0:
            raise ValueError("batch not found")

        if account_id is not None:
            _set_publish_batch_account_state_with_cursor(
                cur,
                batch_id=int(batch_id),
                account_id=int(account_id),
                state="generation_failed",
                detail=detail_value,
                updated_at=now,
            )
        else:
            cur.execute(
                """
                UPDATE publish_batch_accounts
                SET state = 'generation_failed',
                    detail = ?,
                    updated_at = ?
                WHERE batch_id = ?
                  AND state IN ('queued_for_generation', 'generating')
                """,
                (detail_value, now, int(batch_id)),
            )
            cur.execute(
                """
                UPDATE publish_batches
                SET generation_completed_at = COALESCE(generation_completed_at, ?)
                WHERE id = ?
                """,
                (now, int(batch_id)),
            )

        event_payload = dict(payload or {})
        if account_id is not None:
            event_payload["account_id"] = int(account_id)
        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(batch_id),
            state="generation_failed",
            detail=detail_value,
            payload=event_payload or None,
            account_id=int(account_id) if account_id is not None else None,
            event_hash=event_hash,
            created_at=now,
        )
        metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
        conn.commit()
        return metrics
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def mark_publish_generation_progress(
    batch_id: int,
    *,
    account_id: int,
    stage_key: str,
    stage_label: str,
    progress_pct: float,
    detail: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    event_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    stage_key_value = (stage_key or "").strip()
    stage_label_value = (stage_label or "").strip()
    progress_value = max(0.0, min(100.0, float(progress_pct)))
    detail_value = (detail or "").strip() or stage_label_value or "Получен generation progress."
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            SELECT id
            FROM publish_batches
            WHERE id = ?
            LIMIT 1
            """,
            (int(batch_id),),
        )
        if cur.fetchone() is None:
            raise ValueError("batch not found")

        cur.execute(
            """
            SELECT state
            FROM publish_batch_accounts
            WHERE batch_id = ? AND account_id = ?
            LIMIT 1
            """,
            (int(batch_id), int(account_id)),
        )
        account_row = cur.fetchone()
        if account_row is None:
            raise ValueError("batch account not found")

        current_state = normalize_publish_batch_account_state(str(account_row["state"] or "queued_for_generation"))
        if current_state in {"queued_for_generation", "generating"}:
            _set_publish_batch_account_state_with_cursor(
                cur,
                batch_id=int(batch_id),
                account_id=int(account_id),
                state="generating",
                detail=detail_value,
                updated_at=now,
            )
        else:
            cur.execute(
                """
                UPDATE publish_batch_accounts
                SET detail = ?,
                    updated_at = ?
                WHERE batch_id = ? AND account_id = ?
                """,
                (detail_value, now, int(batch_id), int(account_id)),
            )

        cur.execute(
            """
            UPDATE publish_batches
            SET detail = ?,
                updated_at = ?,
                generation_started_at = COALESCE(generation_started_at, ?)
            WHERE id = ?
            """,
            (detail_value, now, now, int(batch_id)),
        )

        payload = {
            "account_id": int(account_id),
            "stage_key": stage_key_value,
            "stage_label": stage_label_value,
            "progress_pct": progress_value,
        }
        if meta is not None:
            payload["meta"] = meta

        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(batch_id),
            account_id=int(account_id),
            state="generation_progress",
            detail=detail_value,
            payload=payload,
            event_hash=event_hash,
            created_at=now,
        )
        metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
        conn.commit()
        return metrics
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def register_publish_artifact(
    batch_id: int,
    *,
    path: str,
    filename: str,
    checksum: Optional[str] = None,
    size_bytes: Optional[int] = None,
    duration_seconds: Optional[float] = None,
    account_id: Optional[int] = None,
    event_hash: Optional[str] = None,
) -> Dict[str, Any]:
    now = int(time.time())
    path_value = (path or "").strip()
    filename_value = (filename or "").strip()
    if not path_value or not filename_value:
        raise ValueError("artifact path required")

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute("SELECT id FROM publish_batches WHERE id = ?", (int(batch_id),))
        if cur.fetchone() is None:
            raise ValueError("batch not found")

        cur.execute(
            """
            SELECT id
            FROM publish_artifacts
            WHERE batch_id = ? AND path = ?
            LIMIT 1
            """,
            (int(batch_id), path_value),
        )
        existing = cur.fetchone()
        created = existing is None
        if created:
            cur.execute(
                """
                INSERT INTO publish_artifacts (
                    batch_id, path, filename, checksum, size_bytes, duration_seconds, created_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(batch_id),
                    path_value,
                    filename_value,
                    (checksum or "").strip() or None,
                    int(size_bytes) if size_bytes is not None else None,
                    float(duration_seconds) if duration_seconds is not None else None,
                    now,
                    now,
                ),
            )
            artifact_id = int(cur.lastrowid)
        else:
            artifact_id = int(existing["id"])
            cur.execute(
                """
                UPDATE publish_artifacts
                SET filename = ?,
                    checksum = ?,
                    size_bytes = ?,
                    duration_seconds = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    filename_value,
                    (checksum or "").strip() or None,
                    int(size_bytes) if size_bytes is not None else None,
                    float(duration_seconds) if duration_seconds is not None else None,
                    now,
                    artifact_id,
                ),
            )

        account_filters = [int(batch_id)]
        account_where = "WHERE pba.batch_id = ?"
        if account_id is not None:
            account_where += " AND pba.account_id = ?"
            account_filters.append(int(account_id))
        cur.execute(
            f"""
            SELECT
                pba.account_id,
                pba.artifact_id,
                pba.job_id,
                pba.state,
                COALESCE(a.instagram_emulator_serial, '') AS instagram_emulator_serial
            FROM publish_batch_accounts pba
            JOIN accounts a ON a.id = pba.account_id
            {account_where}
            ORDER BY pba.account_id ASC
            """,
            tuple(account_filters),
        )
        account_rows = cur.fetchall()
        if account_id is not None and not account_rows:
            raise ValueError("batch account not found")
        jobs_created = 0
        job_ids: List[int] = []
        target_account_ids: List[int] = []
        for row in account_rows:
            emulator_serial = str(row["instagram_emulator_serial"] or "").strip()
            if not emulator_serial:
                continue
            target_account_id = int(row["account_id"])
            cur.execute(
                """
                INSERT OR IGNORE INTO publish_jobs (
                    batch_id,
                    artifact_id,
                    account_id,
                    emulator_serial,
                    state,
                    detail,
                    source_path,
                    source_name,
                    created_at,
                    updated_at
                )
                VALUES (?, ?, ?, ?, 'queued', 'Ожидает публикации.', ?, ?, ?, ?)
                """,
                (
                    int(batch_id),
                    artifact_id,
                    target_account_id,
                    emulator_serial,
                    path_value,
                    filename_value,
                    now,
                    now,
                ),
            )
            if cur.rowcount > 0:
                jobs_created += 1
            cur.execute(
                """
                SELECT id
                FROM publish_jobs
                WHERE batch_id = ? AND artifact_id = ? AND account_id = ?
                LIMIT 1
                """,
                (int(batch_id), artifact_id, target_account_id),
            )
            job_row = cur.fetchone()
            job_id_value = int(job_row["id"]) if job_row is not None else None
            current_state = normalize_publish_batch_account_state(str(row["state"] or "queued_for_generation"))
            should_advance = current_state in {"queued_for_generation", "generating", "generation_failed", "queued_for_publish"}
            if should_advance:
                _set_publish_batch_account_state_with_cursor(
                    cur,
                    batch_id=int(batch_id),
                    account_id=target_account_id,
                    state="queued_for_publish",
                    detail=f"Видео {filename_value} готово. Ожидает lease runner-а.",
                    artifact_id=artifact_id,
                    job_id=job_id_value,
                    updated_at=now,
                )
            else:
                cur.execute(
                    """
                    UPDATE publish_batch_accounts
                    SET artifact_id = COALESCE(artifact_id, ?),
                        job_id = COALESCE(job_id, ?),
                        updated_at = ?
                    WHERE batch_id = ? AND account_id = ?
                    """,
                    (artifact_id, job_id_value, now, int(batch_id), target_account_id),
                )
            if job_id_value is not None:
                job_ids.append(job_id_value)
            target_account_ids.append(target_account_id)
        detail_value = (
            f"Получен файл {filename_value} для account_id={int(account_id)}. Создано jobs: {jobs_created}."
            if account_id is not None
            else f"Получен файл {filename_value}. Создано jobs: {jobs_created}."
        )

        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(batch_id),
            state="artifact_ready",
            detail=detail_value,
            payload={
                "artifact_id": artifact_id,
                "path": path_value,
                "filename": filename_value,
                "jobs_created": jobs_created,
                "created": created,
                "account_id": int(account_id) if account_id is not None else None,
                "job_ids": job_ids,
            },
            account_id=int(account_id) if account_id is not None else None,
            event_hash=event_hash,
            created_at=now,
        )
        metrics = _refresh_publish_batch_state_with_cursor(cur, int(batch_id), now=now)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {
        "artifact_id": artifact_id,
        "created": created,
        "jobs_created": jobs_created,
        "job_ids": job_ids,
        "account_ids": target_account_ids,
        **metrics,
    }


def _expire_stale_publish_jobs_with_cursor(cur: sqlite3.Cursor, *, now: int) -> int:
    active_states = tuple(sorted(ACTIVE_PUBLISH_JOB_STATES))
    if not active_states:
        return 0
    placeholders = ", ".join("?" for _ in active_states)
    cur.execute(
        f"""
        SELECT
            id,
            batch_id,
            account_id,
            source_name,
            COALESCE(last_file, '') AS last_file,
            COALESCE(lease_expires_at, 0) AS lease_expires_at
        FROM publish_jobs
        WHERE state IN ({placeholders})
          AND COALESCE(lease_expires_at, 0) < ?
        """,
        (*active_states, int(now)),
    )
    rows = cur.fetchall()
    if not rows:
        return 0
    expired = 0
    detail_value = "Publish job lease expired; marking failed."
    for row in rows:
        job_id = int(row["id"])
        batch_id = int(row["batch_id"])
        account_id = int(row["account_id"])
        last_file_value = (str(row["last_file"] or "").strip() or str(row["source_name"] or "").strip())

        cur.execute(
            """
            UPDATE publish_jobs
            SET state = 'failed',
                detail = ?,
                completed_at = ?,
                last_file = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (detail_value, int(now), last_file_value, detail_value, int(now), job_id),
        )
        cur.execute(
            """
            UPDATE accounts
            SET instagram_publish_status = ?,
                instagram_publish_detail = ?,
                instagram_publish_updated_at = ?,
                instagram_publish_last_file = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                _publish_job_state_to_account_publish_state("failed"),
                detail_value,
                int(now),
                last_file_value,
                int(now),
                account_id,
            ),
        )
        _sync_account_auto_rotation_state_with_cursor(cur, account_id, now=int(now))
        _set_publish_batch_account_state_with_cursor(
            cur,
            batch_id=batch_id,
            account_id=account_id,
            state="failed",
            detail=detail_value,
            job_id=job_id,
            updated_at=int(now),
        )
        _append_publish_job_event_with_cursor(
            cur,
            batch_id=batch_id,
            job_id=job_id,
            account_id=account_id,
            state="failed",
            detail=detail_value,
            payload={"expired": True},
            created_at=int(now),
        )
        _refresh_publish_batch_state_with_cursor(cur, batch_id, now=int(now))
        expired += 1
    return expired


def lease_next_publish_job(
    *,
    runner_name: str,
    lease_seconds: int = 600,
) -> Optional[Dict[str, Any]]:
    runner = (runner_name or "").strip() or "publish-runner"
    lease_ttl = max(60, int(lease_seconds or 0))
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        expired_jobs = _expire_stale_publish_jobs_with_cursor(cur, now=now)
        cur.execute(
            """
            SELECT j.id
            FROM publish_jobs j
            WHERE (
                j.state = 'queued'
                OR (j.state = 'leased' AND COALESCE(j.lease_expires_at, 0) < ?)
            )
              AND NOT EXISTS (
                SELECT 1
                FROM publish_jobs active
                WHERE active.emulator_serial = j.emulator_serial
                  AND active.id <> j.id
                  AND (
                    active.state IN ('preparing', 'importing_media', 'opening_reel_flow', 'selecting_media', 'publishing')
                    OR (active.state = 'leased' AND COALESCE(active.lease_expires_at, 0) >= ?)
                  )
              )
            ORDER BY j.created_at ASC, j.id ASC
            LIMIT 1
            """,
            (now, now),
        )
        candidate = cur.fetchone()
        if candidate is None:
            if expired_jobs > 0:
                conn.commit()
            else:
                conn.rollback()
            return None
        job_id = int(candidate["id"])
        cur.execute(
            """
            UPDATE publish_jobs
            SET state = 'leased',
                detail = ?,
                leased_by = ?,
                leased_at = ?,
                lease_expires_at = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (f"Runner {runner} взял job в работу.", runner, now, now + lease_ttl, now, job_id),
        )
        cur.execute(
            """
            SELECT
                j.id,
                j.batch_id,
                j.artifact_id,
                j.account_id,
                j.emulator_serial,
                j.state,
                COALESCE(j.detail, '') AS detail,
                j.source_path,
                j.source_name,
                COALESCE(j.leased_by, '') AS leased_by,
                j.leased_at,
                j.lease_expires_at,
                a.account_login,
                a.account_password,
                COALESCE(a.username, '') AS username,
                COALESCE(a.twofa, '') AS twofa,
                COALESCE(a.email, '') AS email,
                a.email_password,
                COALESCE(a.mail_provider, 'auto') AS mail_provider,
                COALESCE(a.mail_auth_json, '') AS mail_auth_json,
                pa.filename AS artifact_filename,
                b.workflow_key
            FROM publish_jobs j
            JOIN accounts a ON a.id = j.account_id
            JOIN publish_artifacts pa ON pa.id = j.artifact_id
            JOIN publish_batches b ON b.id = j.batch_id
            WHERE j.id = ?
            LIMIT 1
            """,
            (job_id,),
        )
        row = cur.fetchone()
        if row is None:
            conn.rollback()
            return None
        job_payload = dict(row)
        job_payload["mail_enabled"] = account_mail_automation_ready(job_payload)
        job_payload["mail_address"] = str(job_payload.pop("email", "") or "")
        job_payload["mail_provider"] = str(job_payload.get("mail_provider") or "auto")
        job_payload.pop("email_password", None)
        job_payload.pop("mail_auth_json", None)
        _set_publish_batch_account_state_with_cursor(
            cur,
            batch_id=int(row["batch_id"]),
            account_id=int(row["account_id"]),
            state="leased",
            detail=f"Runner {runner} взял job в lease.",
            artifact_id=int(row["artifact_id"]),
            job_id=job_id,
            updated_at=now,
        )
        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(row["batch_id"]),
            job_id=job_id,
            state="leased",
            detail=f"Runner {runner} взял job.",
            payload={"runner_name": runner, "lease_seconds": lease_ttl},
            account_id=int(row["account_id"]),
            created_at=now,
        )
        _refresh_publish_batch_state_with_cursor(cur, int(row["batch_id"]), now=now)
        conn.commit()
        return job_payload
    finally:
        conn.close()


def update_publish_job_state(
    job_id: int,
    *,
    state: str,
    detail: Optional[str] = None,
    last_file: Optional[str] = None,
    runner_name: Optional[str] = None,
    payload: Optional[Dict[str, Any]] = None,
    lease_seconds: int = 600,
) -> Dict[str, Any]:
    state_value = normalize_publish_job_state(state)
    timestamp = int(time.time())
    lease_ttl = max(60, int(lease_seconds or 0))
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute("BEGIN IMMEDIATE")
        cur.execute(
            """
            SELECT
                id,
                batch_id,
                account_id,
                state,
                source_name,
                COALESCE(last_file, '') AS last_file,
                COALESCE(detail, '') AS detail
            FROM publish_jobs
            WHERE id = ?
            LIMIT 1
            """,
            (int(job_id),),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("job not found")
        current_state = normalize_publish_job_state(str(row["state"] or "queued"))
        if current_state in {"published", "needs_review", "failed", "canceled"} and state_value != current_state:
            raise ValueError("job already finished")
        if _is_publish_job_state_regression(current_state, state_value):
            metrics = _refresh_publish_batch_state_with_cursor(cur, int(row["batch_id"]), now=timestamp)
            conn.commit()
            return {
                "job_id": int(job_id),
                "batch_id": int(row["batch_id"]),
                "job_state": current_state,
                "batch_state": str(metrics.get("state") or ""),
                **metrics,
            }

        last_file_value = (last_file or "").strip() or str(row["last_file"] or "").strip() or str(row["source_name"] or "").strip()
        detail_value = (detail or "").strip()
        started_at = timestamp if state_value in ACTIVE_PUBLISH_JOB_STATES and current_state not in ACTIVE_PUBLISH_JOB_STATES else None
        completed_at = timestamp if state_value in {"published", "needs_review", "failed", "canceled"} else None
        lease_expires_at = timestamp + lease_ttl if state_value in ACTIVE_PUBLISH_JOB_STATES else None
        last_error = detail_value if state_value in {"failed", "canceled"} else ""

        cur.execute(
            """
            UPDATE publish_jobs
            SET state = ?,
                detail = ?,
                leased_by = COALESCE(?, leased_by),
                lease_expires_at = ?,
                started_at = COALESCE(started_at, ?),
                completed_at = CASE WHEN ? IS NOT NULL THEN ? ELSE completed_at END,
                last_file = ?,
                last_error = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                state_value,
                detail_value,
                (runner_name or "").strip() or None,
                lease_expires_at,
                started_at,
                completed_at,
                completed_at,
                last_file_value,
                last_error,
                timestamp,
                int(job_id),
            ),
        )

        cur.execute(
            """
            UPDATE accounts
            SET instagram_publish_status = ?,
                instagram_publish_detail = ?,
                instagram_publish_updated_at = ?,
                instagram_publish_last_file = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                _publish_job_state_to_account_publish_state(state_value, payload),
                detail_value,
                timestamp,
                last_file_value,
                timestamp,
                int(row["account_id"]),
            ),
        )
        mail_challenge_payload = payload.get("mail_challenge") if isinstance(payload, dict) else None
        if isinstance(mail_challenge_payload, dict) and mail_challenge_payload:
            try:
                _update_account_mail_challenge_state_with_cursor(
                    cur,
                    int(row["account_id"]),
                    status=str(mail_challenge_payload.get("status") or "idle"),
                    kind=str(mail_challenge_payload.get("kind") or ""),
                    reason_code=str(mail_challenge_payload.get("reason_code") or ""),
                    reason_text=str(mail_challenge_payload.get("reason_text") or ""),
                    message_uid=str(mail_challenge_payload.get("message_uid") or ""),
                    received_at=mail_challenge_payload.get("received_at"),
                    masked_code=str(mail_challenge_payload.get("masked_code") or ""),
                    confidence=float(mail_challenge_payload.get("confidence") or 0.0),
                    updated_at=timestamp,
                )
            except Exception:
                pass
        _sync_account_auto_rotation_state_with_cursor(cur, int(row["account_id"]), now=timestamp)
        _set_publish_batch_account_state_with_cursor(
            cur,
            batch_id=int(row["batch_id"]),
            account_id=int(row["account_id"]),
            state=_publish_job_state_to_batch_account_state(state_value),
            detail=detail_value,
            job_id=int(job_id),
            updated_at=timestamp,
        )

        _append_publish_job_event_with_cursor(
            cur,
            batch_id=int(row["batch_id"]),
            job_id=int(job_id),
            state=state_value,
            detail=detail_value,
            payload=payload,
            account_id=int(row["account_id"]),
            created_at=timestamp,
        )
        metrics = _refresh_publish_batch_state_with_cursor(cur, int(row["batch_id"]), now=timestamp)
        conn.commit()
        return {
            "job_id": int(job_id),
            "batch_id": int(row["batch_id"]),
            "job_state": state_value,
            "batch_state": str(metrics.get("state") or ""),
            **metrics,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def create_account_claim_request(account_id: int, requested_by_worker_id: int) -> Dict[str, Any]:
    account = get_account(int(account_id))
    if account is None:
        raise ValueError("account not found")
    worker = get_worker(int(requested_by_worker_id))
    if worker is None:
        raise ValueError("worker not found")
    if account["owner_worker_id"] is not None and int(account["owner_worker_id"]) == int(requested_by_worker_id):
        raise ValueError("already assigned")

    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM account_claim_requests
        WHERE account_id = ?
          AND requested_by_worker_id = ?
          AND status = 'pending'
        ORDER BY id DESC
        LIMIT 1
        """,
        (int(account_id), int(requested_by_worker_id)),
    )
    existing = cur.fetchone()
    if existing:
        conn.close()
        return {"request_id": int(existing["id"]), "created": False}

    cur.execute(
        """
        INSERT INTO account_claim_requests (account_id, requested_by_worker_id, status, created_at, updated_at)
        VALUES (?, ?, 'pending', ?, ?)
        """,
        (int(account_id), int(requested_by_worker_id), now, now),
    )
    request_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return {"request_id": request_id, "created": True}


def create_helper_launch_ticket(
    *,
    account_id: int,
    target: str,
    created_by_admin: Optional[str],
    ttl_seconds: int = 120,
) -> Dict[str, Any]:
    account = get_account(int(account_id))
    if account is None:
        raise ValueError("account not found")
    target_key = _normalize_helper_ticket_target(target)
    ttl = max(15, int(ttl_seconds or 0))
    now = int(time.time())
    ticket = secrets.token_urlsafe(24)

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO helper_launch_tickets (
            ticket, account_id, target, created_at, expires_at, used_at, created_by_admin
        )
        VALUES (?, ?, ?, ?, ?, NULL, ?)
        """,
        (ticket, int(account_id), target_key, now, now + ttl, (created_by_admin or "").strip() or None),
    )
    conn.commit()
    conn.close()
    return {
        "ticket": ticket,
        "account_id": int(account_id),
        "target": target_key,
        "created_at": now,
        "expires_at": now + ttl,
    }


def consume_helper_launch_ticket(ticket: str, *, target: Optional[str] = None) -> Dict[str, Any]:
    ticket_value = (ticket or "").strip()
    if not ticket_value:
        raise ValueError("ticket not found")
    target_key = _normalize_helper_ticket_target(target) if target else None
    now = int(time.time())

    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT ticket, account_id, target, created_at, expires_at, used_at, created_by_admin
            FROM helper_launch_tickets
            WHERE ticket = ?
            LIMIT 1
            """,
            (ticket_value,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError("ticket not found")
        if target_key and str(row["target"] or "") != target_key:
            raise ValueError("ticket not found")
        if row["used_at"]:
            raise ValueError("ticket used")
        if int(row["expires_at"] or 0) < now:
            raise ValueError("ticket expired")

        cur.execute(
            "UPDATE helper_launch_tickets SET used_at = ? WHERE ticket = ? AND used_at IS NULL",
            (now, ticket_value),
        )
        if cur.rowcount <= 0:
            raise ValueError("ticket used")
        conn.commit()
    finally:
        conn.close()

    account = get_account(int(row["account_id"]))
    if account is None:
        raise ValueError("account not found")
    account_dict = dict(account)
    return {
        "ticket": str(row["ticket"]),
        "account_id": int(row["account_id"]),
        "target": str(row["target"]),
        "created_at": int(row["created_at"] or 0),
        "expires_at": int(row["expires_at"] or 0),
        "created_by_admin": str(row["created_by_admin"] or "").strip(),
        "account": {
            "id": int(account_dict["id"]),
            "type": str(account_dict.get("type") or ""),
            "username": str(account_dict.get("username") or ""),
            "account_login": str(account_dict.get("account_login") or ""),
            "account_password": str(account_dict.get("account_password") or ""),
            "mail_enabled": account_mail_automation_ready(account_dict),
            "email": str(account_dict.get("email") or ""),
            "mail_provider": str(account_dict.get("mail_provider") or "auto"),
            "twofa": str(account_dict.get("twofa") or ""),
            "instagram_emulator_serial": str(account_dict.get("instagram_emulator_serial") or ""),
        },
    }


def list_account_claim_requests(
    status: Optional[str] = "pending",
    *,
    requested_by_worker_id: Optional[int] = None,
    limit: int = 200,
) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    where_parts: List[str] = []
    args: List[Any] = []
    if status:
        where_parts.append("acr.status = ?")
        args.append((status or "").strip())
    if requested_by_worker_id is not None:
        where_parts.append("acr.requested_by_worker_id = ?")
        args.append(int(requested_by_worker_id))
    where = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    cur.execute(
        f"""
        SELECT
            acr.id,
            acr.account_id,
            acr.requested_by_worker_id,
            acr.status,
            acr.created_at,
            acr.updated_at,
            a.type AS account_type,
            a.account_login,
            a.username AS account_username,
            a.owner_worker_id,
            COALESCE(owner.name, '') AS owner_worker_name,
            COALESCE(owner.username, '') AS owner_worker_username,
            COALESCE(req.name, '') AS requested_worker_name,
            COALESCE(req.username, '') AS requested_worker_username
        FROM account_claim_requests acr
        JOIN accounts a ON a.id = acr.account_id
        LEFT JOIN workers owner ON owner.id = a.owner_worker_id
        JOIN workers req ON req.id = acr.requested_by_worker_id
        {where}
        ORDER BY acr.created_at DESC, acr.id DESC
        LIMIT ?
        """,
        (*args, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def resolve_account_claim_request(request_id: int, approve: bool) -> bool:
    rid = int(request_id)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, account_id, requested_by_worker_id, status
        FROM account_claim_requests
        WHERE id = ?
        LIMIT 1
        """,
        (rid,),
    )
    row = cur.fetchone()
    if row is None or str(row["status"]) != "pending":
        conn.close()
        return False

    if approve:
        cur.execute(
            "UPDATE accounts SET owner_worker_id = ?, updated_at = ? WHERE id = ?",
            (int(row["requested_by_worker_id"]), now, int(row["account_id"])),
        )
        new_status = "approved"
    else:
        new_status = "rejected"

    cur.execute(
        "UPDATE account_claim_requests SET status = ?, updated_at = ? WHERE id = ?",
        (new_status, now, rid),
    )
    conn.commit()
    conn.close()
    return True


def accounts_overview() -> Dict[str, int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN type = 'youtube' THEN 1 ELSE 0 END) AS youtube,
            SUM(CASE WHEN type = 'tiktok' THEN 1 ELSE 0 END) AS tiktok,
            SUM(CASE WHEN type = 'instagram' THEN 1 ELSE 0 END) AS instagram
        FROM accounts
        """
    )
    row = cur.fetchone()
    conn.close()
    return {
        "total": int(row["total"] or 0) if row else 0,
        "youtube": int(row["youtube"] or 0) if row else 0,
        "tiktok": int(row["tiktok"] or 0) if row else 0,
        "instagram": int(row["instagram"] or 0) if row else 0,
    }


def workers_overview() -> Dict[str, int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS workers_total FROM workers")
    workers_total = int((cur.fetchone() or {"workers_total": 0})["workers_total"] or 0)
    cur.execute("SELECT COUNT(*) AS assigned_accounts_total FROM accounts WHERE owner_worker_id IS NOT NULL")
    assigned_accounts_total = int((cur.fetchone() or {"assigned_accounts_total": 0})["assigned_accounts_total"] or 0)
    cur.execute(
        """
        SELECT COUNT(DISTINCT e.user_id) AS worker_starts_unique_total
        FROM events e
        JOIN links l ON LOWER(l.code) = LOWER(e.code)
        JOIN accounts a ON a.id = l.account_id
        WHERE e.event_type = 'start'
          AND e.user_id IS NOT NULL
          AND COALESCE(l.is_deleted, 0) = 0
          AND a.owner_worker_id IS NOT NULL
        """
    )
    worker_starts_unique_total = int((cur.fetchone() or {"worker_starts_unique_total": 0})["worker_starts_unique_total"] or 0)
    conn.close()
    return {
        "workers_total": workers_total,
        "assigned_accounts_total": assigned_accounts_total,
        "worker_starts_unique_total": worker_starts_unique_total,
    }


def worker_detail_overview(worker_id: int) -> Dict[str, int]:
    wid = int(worker_id)
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS accounts_total FROM accounts WHERE owner_worker_id = ?", (wid,))
    accounts_total = int((cur.fetchone() or {"accounts_total": 0})["accounts_total"] or 0)
    cur.execute(
        """
        SELECT
            COUNT(*) AS starts_total,
            COUNT(DISTINCT CASE WHEN e.user_id IS NOT NULL THEN e.user_id END) AS starts_unique_total
        FROM events e
        JOIN links l ON LOWER(l.code) = LOWER(e.code)
        JOIN accounts a ON a.id = l.account_id
        WHERE e.event_type = 'start'
          AND COALESCE(l.is_deleted, 0) = 0
          AND a.owner_worker_id = ?
        """,
        (wid,),
    )
    row_starts = cur.fetchone()
    starts_total = int((row_starts["starts_total"] if row_starts else 0) or 0)
    starts_unique_total = int((row_starts["starts_unique_total"] if row_starts else 0) or 0)
    cur.execute(
        """
        SELECT COUNT(*) AS first_touch_total
        FROM users u
        WHERE EXISTS (
            SELECT 1
            FROM links l
            JOIN accounts a ON a.id = l.account_id
            WHERE a.owner_worker_id = ?
              AND COALESCE(l.is_deleted, 0) = 0
              AND LOWER(l.code) = LOWER(COALESCE(u.ref_code, ''))
        )
        """,
        (wid,),
    )
    first_touch_total = int((cur.fetchone() or {"first_touch_total": 0})["first_touch_total"] or 0)
    conn.close()
    return {
        "accounts_total": accounts_total,
        "starts_total": starts_total,
        "starts_unique_total": starts_unique_total,
        "first_touch_total": first_touch_total,
    }


def list_workers_compact(q: Optional[str] = None, limit: int = 300) -> List[sqlite3.Row]:
    where: List[str] = []
    args: List[Any] = []
    query = (q or "").strip().lower()
    if query:
        like = f"%{query}%"
        where.append(
            "("
            "LOWER(COALESCE(w.name, '')) LIKE ? OR "
            "LOWER(COALESCE(w.username, '')) LIKE ?"
            ")"
        )
        args.extend([like, like])
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT
            w.id,
            w.name,
            w.username,
            w.plain_password,
            w.created_at,
            w.updated_at,
            COALESCE((
                SELECT COUNT(*)
                FROM accounts a
                WHERE a.owner_worker_id = w.id
            ), 0) AS accounts_total,
            COALESCE((
                SELECT COUNT(DISTINCT e.user_id)
                FROM events e
                JOIN links l ON LOWER(l.code) = LOWER(e.code)
                JOIN accounts a ON a.id = l.account_id
                WHERE a.owner_worker_id = w.id
                  AND e.event_type = 'start'
                  AND e.user_id IS NOT NULL
                  AND COALESCE(l.is_deleted, 0) = 0
            ), 0) AS starts_unique_total
        FROM workers w
        {clause}
        ORDER BY w.updated_at DESC, w.id DESC
        LIMIT ?
        """,
        (*args, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_worker(worker_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, username, pass_hash, pass_salt, plain_password, created_at, updated_at
        FROM workers
        WHERE id = ?
        """,
        (int(worker_id),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def normalize_worker_username(raw: str) -> str:
    value = (raw or "").strip()
    value = value.lstrip("@").strip()
    return value.lower()


def get_worker_by_username(username: str) -> Optional[sqlite3.Row]:
    normalized = normalize_worker_username(username)
    if not normalized:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, name, username, pass_hash, pass_salt, plain_password, created_at, updated_at
        FROM workers
        WHERE LOWER(LTRIM(TRIM(username), '@')) = ?
        ORDER BY id DESC
        LIMIT 1
        """,
        (normalized,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def create_worker(name: str, username: str, password: str) -> int:
    name_clean = (name or "").strip()
    username_clean = (username or "").strip()
    username_normalized = normalize_worker_username(username_clean)
    password_clean = (password or "").strip()
    if not name_clean:
        raise ValueError("name required")
    if not username_normalized:
        raise ValueError("username required")
    if not password_clean:
        raise ValueError("password required")
    hp = _hash_password(password_clean)
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id
            FROM workers
            WHERE LOWER(LTRIM(TRIM(username), '@')) = ?
            LIMIT 1
            """,
            (username_normalized,),
        )
        if cur.fetchone():
            raise ValueError("username exists")
        cur.execute(
            """
            INSERT INTO workers (name, username, pass_hash, pass_salt, plain_password, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (name_clean, username_clean, hp["hash"], hp["salt"], password_clean, now, now),
        )
        worker_id = int(cur.lastrowid)
        conn.commit()
        return worker_id
    except sqlite3.IntegrityError as exc:
        raise ValueError("username exists") from exc
    finally:
        conn.close()


def update_worker(worker_id: int, name: str, username: str, password_or_empty: str) -> bool:
    name_clean = (name or "").strip()
    username_clean = (username or "").strip()
    username_normalized = normalize_worker_username(username_clean)
    password_clean = (password_or_empty or "").strip()
    if not name_clean:
        raise ValueError("name required")
    if not username_normalized:
        raise ValueError("username required")
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            SELECT id
            FROM workers
            WHERE id <> ?
              AND LOWER(LTRIM(TRIM(username), '@')) = ?
            LIMIT 1
            """,
            (int(worker_id), username_normalized),
        )
        if cur.fetchone():
            raise ValueError("username exists")
        if password_clean:
            hp = _hash_password(password_clean)
            cur.execute(
                """
                UPDATE workers
                SET name = ?,
                    username = ?,
                    pass_hash = ?,
                    pass_salt = ?,
                    plain_password = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (name_clean, username_clean, hp["hash"], hp["salt"], password_clean, now, int(worker_id)),
            )
        else:
            cur.execute(
                """
                UPDATE workers
                SET name = ?,
                    username = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (name_clean, username_clean, now, int(worker_id)),
            )
        changed = cur.rowcount > 0
        conn.commit()
        return changed
    except sqlite3.IntegrityError as exc:
        raise ValueError("username exists") from exc
    finally:
        conn.close()


def delete_worker(worker_id: int) -> bool:
    wid = int(worker_id)
    conn = _connect()
    cur = conn.cursor()
    now = int(time.time())
    cur.execute("UPDATE accounts SET owner_worker_id = NULL, updated_at = ? WHERE owner_worker_id = ?", (now, wid))
    cur.execute(
        "UPDATE account_claim_requests SET status = 'rejected', updated_at = ? WHERE requested_by_worker_id = ? AND status = 'pending'",
        (now, wid),
    )
    cur.execute("DELETE FROM workers WHERE id = ?", (wid,))
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    return changed


def admin_funnel_step_options() -> List[Dict[str, str]]:
    return [{"key": str(step["key"]), "label": str(step["label"])} for step in ADMIN_FUNNEL_STEPS]


def _funnel_alias_map() -> tuple[Dict[str, str], List[str]]:
    alias_to_key: Dict[str, str] = {}
    event_names: List[str] = []
    for step in ADMIN_FUNNEL_STEPS:
        for evt in step["events"]:
            evt_name = str(evt)
            alias_to_key[evt_name] = str(step["key"])
            event_names.append(evt_name)
    return alias_to_key, event_names


def _strict_funnel_last_idx(step_times: Dict[str, int]) -> int:
    last_idx = -1
    prev_ts: Optional[int] = None
    for idx, step in enumerate(ADMIN_FUNNEL_STEPS):
        ts = step_times.get(str(step["key"]))
        if ts is None:
            break
        if prev_ts is not None and ts < prev_ts:
            break
        last_idx = idx
        prev_ts = ts
    return last_idx


def _filter_user_ids_by_funnel_stage(
    user_ids: List[int],
    stage_key: Optional[str],
    stage_mode: str = "reached",
) -> set[int]:
    ids = [int(uid) for uid in user_ids if uid is not None]
    if not ids:
        return set()

    target = (stage_key or "").strip()
    step_order = {str(s["key"]): idx for idx, s in enumerate(ADMIN_FUNNEL_STEPS)}
    target_idx = step_order.get(target)
    if target_idx is None:
        return set()

    mode = (stage_mode or "reached").strip().lower()
    if mode not in ("reached", "exact"):
        mode = "reached"

    alias_to_key, event_names = _funnel_alias_map()
    if not event_names:
        return set()

    conn = _connect()
    cur = conn.cursor()
    user_steps: Dict[int, Dict[str, int]] = {}
    evt_ph = ",".join("?" for _ in event_names)
    chunk_size = 400
    for start_idx in range(0, len(ids), chunk_size):
        uid_chunk = ids[start_idx : start_idx + chunk_size]
        user_ph = ",".join("?" for _ in uid_chunk)
        cur.execute(
            f"""
            SELECT user_id, event_type, MIN(created_at) AS first_ts
            FROM events
            WHERE user_id IN ({user_ph})
              AND event_type IN ({evt_ph})
            GROUP BY user_id, event_type
            """,
            (*uid_chunk, *event_names),
        )
        for row in cur.fetchall():
            uid = int(row["user_id"])
            event_type = str(row["event_type"] or "")
            step = alias_to_key.get(event_type)
            if not step:
                continue
            ts = int(row["first_ts"] or 0)
            bucket = user_steps.setdefault(uid, {})
            prev = bucket.get(step)
            if prev is None or ts < prev:
                bucket[step] = ts
    conn.close()

    out: set[int] = set()
    for uid in ids:
        last_idx = _strict_funnel_last_idx(user_steps.get(uid, {}))
        if mode == "exact":
            if last_idx == target_idx:
                out.add(uid)
        else:
            if last_idx >= target_idx:
                out.add(uid)
    return out


def list_users_with_funnel_progress(
    q: Optional[str] = None,
    reached_step: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    query = (q or "").strip()
    if query.startswith("@"):
        query = query[1:]

    conn = _connect()
    cur = conn.cursor()

    where: List[str] = []
    args: List[Any] = []
    if query:
        if query.isdigit():
            where.append("u.user_id = ?")
            args.append(int(query))
        else:
            like = f"%{query.lower()}%"
            where.append(
                "(LOWER(COALESCE(u.username, '')) LIKE ? OR LOWER(COALESCE(u.first_name, '')) LIKE ? OR LOWER(COALESCE(u.last_name, '')) LIKE ?)"
            )
            args.extend([like, like, like])

    clause = ("WHERE " + " AND ".join(where)) if where else ""
    fetch_limit = max(int(limit), 2000) if reached_step else int(limit)
    cur.execute(
        f"""
        SELECT
            u.user_id,
            u.username,
            u.first_name,
            u.last_name,
            u.last_seen
        FROM users u
        {clause}
        ORDER BY u.last_seen DESC
        LIMIT ?
        """,
        (*args, fetch_limit),
    )
    base_rows = [dict(r) for r in cur.fetchall()]
    if not base_rows:
        conn.close()
        return []

    user_ids = [int(r["user_id"]) for r in base_rows if r.get("user_id") is not None]
    alias_to_key, event_names = _funnel_alias_map()
    user_steps: Dict[int, Dict[str, int]] = {}

    if user_ids and event_names:
        evt_ph = ",".join("?" for _ in event_names)
        chunk_size = 400
        for start_idx in range(0, len(user_ids), chunk_size):
            uid_chunk = user_ids[start_idx : start_idx + chunk_size]
            user_ph = ",".join("?" for _ in uid_chunk)
            cur.execute(
                f"""
                SELECT user_id, event_type, MIN(created_at) AS first_ts
                FROM events
                WHERE user_id IN ({user_ph})
                  AND event_type IN ({evt_ph})
                GROUP BY user_id, event_type
                """,
                (*uid_chunk, *event_names),
            )
            for row in cur.fetchall():
                uid = int(row["user_id"])
                event_type = str(row["event_type"] or "")
                step_key = alias_to_key.get(event_type)
                if not step_key:
                    continue
                ts = int(row["first_ts"] or 0)
                bucket = user_steps.setdefault(uid, {})
                prev_ts = bucket.get(step_key)
                if prev_ts is None or ts < prev_ts:
                    bucket[step_key] = ts

    conn.close()

    step_order = {str(s["key"]): idx for idx, s in enumerate(ADMIN_FUNNEL_STEPS)}
    selected_idx = step_order.get((reached_step or "").strip())

    out: List[Dict[str, Any]] = []
    for row in base_rows:
        uid = int(row["user_id"])
        last_idx = _strict_funnel_last_idx(user_steps.get(uid, {}))
        reached_order = last_idx + 1
        if selected_idx is not None and reached_order < (selected_idx + 1):
            continue

        if last_idx >= 0:
            row["funnel_step_key"] = str(ADMIN_FUNNEL_STEPS[last_idx]["key"])
            row["funnel_step_label"] = str(ADMIN_FUNNEL_STEPS[last_idx]["label"])
        else:
            row["funnel_step_key"] = "none"
            row["funnel_step_label"] = "Не начал"
        row["funnel_step_order"] = reached_order
        out.append(row)
        if len(out) >= int(limit):
            break
    return out


def search_partners(query: str, limit: int = 50) -> List[sqlite3.Row]:
    q = (query or "").strip()
    if q.startswith("@"):
        q = q[1:]
    if not q:
        return []
    like = f"%{q.lower()}%"
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.name,
            p.username,
            p.plain_password,
            p.manager_id,
            p.approved,
            COALESCE(m.username, '') AS manager_username,
            COALESCE(m.name, '') AS manager_name
        FROM partners p
        LEFT JOIN managers m ON m.id = p.manager_id
        WHERE LOWER(COALESCE(p.username, '')) LIKE ?
           OR LOWER(COALESCE(p.name, '')) LIKE ?
        ORDER BY p.created_at DESC
        LIMIT ?
        """,
        (like, like, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def search_managers(query: str, limit: int = 50) -> List[sqlite3.Row]:
    q = (query or "").strip()
    if q.startswith("@"):
        q = q[1:]
    if not q:
        return []
    like = f"%{q.lower()}%"
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            m.id,
            m.name,
            m.username,
            m.plain_password,
            m.approved
        FROM managers m
        WHERE LOWER(COALESCE(m.username, '')) LIKE ?
           OR LOWER(COALESCE(m.name, '')) LIKE ?
        ORDER BY m.created_at DESC
        LIMIT ?
        """,
        (like, like, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def search_links(query: str, limit: int = 50) -> List[sqlite3.Row]:
    q = (query or "").strip()
    if not q:
        return []
    like = f"%{q.lower()}%"
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            l.code,
            l.name,
            l.target_url,
            l.partner_id,
            l.created_at,
            COALESCE(p.username, '') AS partner_username,
            COALESCE(p.name, '') AS partner_name,
            (SELECT COUNT(*) FROM clicks c WHERE c.code = l.code) AS clicks,
            (SELECT COUNT(*) FROM events e WHERE e.event_type = 'start' AND e.code = l.code) AS starts,
            (SELECT COUNT(DISTINCT e.user_id) FROM events e WHERE e.event_type = 'start' AND e.code = l.code AND e.user_id IS NOT NULL) AS uniq_users
        FROM links l
        LEFT JOIN partners p ON p.id = l.partner_id
        WHERE LOWER(COALESCE(l.code, '')) LIKE ?
           OR LOWER(COALESCE(l.name, '')) LIKE ?
           OR LOWER(COALESCE(l.target_url, '')) LIKE ?
        ORDER BY l.created_at DESC
        LIMIT ?
        """,
        (like, like, like, int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def add_broadcast_run(
    *,
    scope: str,
    partner_id: Optional[int],
    manager_id: Optional[int],
    recipients: int,
    sent: int,
    failed: int,
    is_test: bool,
    message: str,
) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO broadcasts (created_at, scope, partner_id, manager_id, recipients, sent, failed, is_test, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            int(time.time()),
            (scope or "all").strip(),
            int(partner_id) if partner_id is not None else None,
            int(manager_id) if manager_id is not None else None,
            int(recipients),
            int(sent),
            int(failed),
            1 if is_test else 0,
            (message or "").strip(),
        ),
    )
    conn.commit()
    conn.close()


def list_broadcast_runs(limit: int = 20) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            b.id,
            b.created_at,
            b.scope,
            b.partner_id,
            b.manager_id,
            b.recipients,
            b.sent,
            b.failed,
            b.is_test,
            b.message,
            COALESCE(p.username, '') AS partner_username,
            COALESCE(p.name, '') AS partner_name,
            COALESCE(m.username, '') AS manager_username,
            COALESCE(m.name, '') AS manager_name
        FROM broadcasts b
        LEFT JOIN partners p ON p.id = b.partner_id
        LEFT JOIN managers m ON m.id = b.manager_id
        ORDER BY b.created_at DESC, b.id DESC
        LIMIT ?
        """,
        (int(limit),),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def update_user_installed(user_id: int, installed: bool) -> None:
    conn = _connect()
    cur = conn.cursor()
    now = int(time.time())
    if installed:
        cur.execute(
            """
            UPDATE users
            SET installed = 1,
                installed_at = COALESCE(installed_at, ?)
            WHERE user_id = ?
            """,
            (now, user_id),
        )
    else:
        cur.execute(
            "UPDATE users SET installed = 0, installed_at = NULL WHERE user_id = ?",
            (user_id,),
        )
    conn.commit()
    conn.close()
    # If install flag changes, earnings should be recalculated for all existing payments.
    recompute_user_earning_lines(int(user_id))


def add_payment(user_id: int, amount_rub: float, note: Optional[str] = None) -> Optional[int]:
    amount = float(amount_rub or 0)
    if amount <= 0:
        return None
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO payments (user_id, amount_rub, note, created_at) VALUES (?, ?, ?, ?)",
        (user_id, amount, (note or "").strip() or None, int(time.time())),
    )
    payment_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    upsert_earning_line_for_payment(payment_id)
    return payment_id


def list_payments(user_id: int, limit: int = 100) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, amount_rub, note, created_at FROM payments WHERE user_id = ? ORDER BY created_at DESC LIMIT ?",
        (user_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_payment(payment_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id, user_id, amount_rub, note, created_at FROM payments WHERE id = ?", (payment_id,))
    row = cur.fetchone()
    conn.close()
    return row


def delete_payment(payment_id: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM earning_lines WHERE payment_id = ?", (payment_id,))
    cur.execute("DELETE FROM payments WHERE id = ?", (payment_id,))
    conn.commit()
    conn.close()


def delete_user(user_id: int) -> None:
    """
    Fully remove a user from the system.
    - Deletes payments + earning_lines
    - Deletes the user row
    - Leaves clicks/events as history (they don't affect payments).
    """
    uid = int(user_id)
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM payments WHERE user_id = ?", (uid,))
    payment_ids = [int(r[0]) for r in cur.fetchall()]
    if payment_ids:
        cur.executemany("DELETE FROM earning_lines WHERE payment_id = ?", [(pid,) for pid in payment_ids])
        cur.executemany("DELETE FROM payments WHERE id = ?", [(pid,) for pid in payment_ids])
    cur.execute("DELETE FROM users WHERE user_id = ?", (uid,))
    conn.commit()
    conn.close()


def update_payment(payment_id: int, amount_rub: float, note: Optional[str] = None) -> None:
    amount = float(amount_rub or 0)
    if amount <= 0:
        return
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE payments SET amount_rub = ?, note = ? WHERE id = ?",
        (amount, (note or "").strip() or None, int(payment_id)),
    )
    conn.commit()
    conn.close()
    upsert_earning_line_for_payment(int(payment_id))


def upsert_earning_line_for_payment(payment_id: int) -> None:
    """Create/update earning line for a payment (manager + model shares).

    Rules:
    - Count ALL payments.
    - Only if user.installed = 1.
    - Only if partner/manager are approved.
    """
    pay = get_payment(int(payment_id))
    if not pay:
        return
    user = get_user(int(pay["user_id"]))
    if not user:
        return

    user_id = int(pay["user_id"])
    amount = float(pay["amount_rub"] or 0)
    installed = int(user["installed"] or 0) == 1
    partner_id = int(user["partner_id"]) if user["partner_id"] is not None else None
    manager_id = int(user["manager_id"]) if user["manager_id"] is not None else None

    partner_ok = False
    if partner_id is not None:
        p = get_partner(partner_id)
        partner_ok = bool(p) and int(p["approved"] or 0) == 1

    manager_ok = False
    if manager_id is not None:
        m = get_manager(manager_id)
        manager_ok = bool(m) and int(m["approved"] or 0) == 1

    partner_share = amount * MODEL_COMMISSION_PCT / 100.0 if installed and partner_ok else 0.0
    manager_share = amount * MANAGER_COMMISSION_PCT / 100.0 if installed and manager_ok else 0.0

    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO earning_lines
            (payment_id, user_id, partner_id, manager_id, partner_amount_rub, manager_amount_rub, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(payment_id) DO UPDATE SET
            user_id=excluded.user_id,
            partner_id=excluded.partner_id,
            manager_id=excluded.manager_id,
            partner_amount_rub=excluded.partner_amount_rub,
            manager_amount_rub=excluded.manager_amount_rub
        """,
        (int(payment_id), user_id, partner_id, manager_id, float(partner_share), float(manager_share), int(time.time())),
    )
    conn.commit()
    conn.close()


def recompute_user_earning_lines(user_id: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id FROM payments WHERE user_id = ?", (int(user_id),))
    ids = [int(r[0]) for r in cur.fetchall()]
    conn.close()
    for pid in ids:
        upsert_earning_line_for_payment(pid)


def list_partner_payments(partner_id: int, limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.user_id,
            COALESCE(u.username, '') AS username,
            u.installed,
            p.amount_rub,
            COALESCE(el.partner_amount_rub, 0) AS earned_rub,
            p.note,
            p.created_at
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        LEFT JOIN earning_lines el ON el.payment_id = p.id
        WHERE u.partner_id = ?
        ORDER BY p.created_at DESC, p.id DESC
        LIMIT ?
        """,
        (partner_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_manager_payments(manager_id: int, limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.user_id,
            COALESCE(u.username, '') AS username,
            u.installed,
            COALESCE(pt.name, '') AS partner_name,
            COALESCE(pt.username, '') AS partner_username,
            p.amount_rub,
            COALESCE(el.manager_amount_rub, 0) AS earned_rub,
            p.note,
            p.created_at
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        LEFT JOIN partners pt ON pt.id = u.partner_id
        LEFT JOIN earning_lines el ON el.payment_id = p.id
        WHERE u.manager_id = ?
        ORDER BY p.created_at DESC, p.id DESC
        LIMIT ?
        """,
        (manager_id, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def set_user_attribution(
    user_id: int,
    code: str,
    partner_id: Optional[int],
    manager_id: Optional[int],
) -> bool:
    code_clean = (code or "").strip()
    if not code_clean:
        return False
    now = int(time.time())
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE users
        SET ref_code = ?,
            partner_id = ?,
            manager_id = ?,
            attributed_at = ?
        WHERE user_id = ?
          AND (ref_code IS NULL OR ref_code = '')
        """,
        (code_clean, partner_id, manager_id, now, user_id),
    )
    changed = cur.rowcount > 0
    conn.commit()
    conn.close()
    if changed:
        recompute_user_earning_lines(int(user_id))
    return changed


def list_events(limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, user_id, event_type, meta_json, code, created_at FROM events ORDER BY created_at DESC LIMIT ?",
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def _hash_password(password: str, salt: Optional[str] = None) -> Dict[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    digest = hashlib.sha256((salt + password).encode("utf-8")).hexdigest()
    return {"salt": salt, "hash": digest}


def create_partner(
    name: str,
    username: str,
    password: str,
    manager_id: Optional[int] = None,
    approved: int = 1,
) -> None:
    now = int(time.time())
    hp = _hash_password(password)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO partners (name, username, pass_hash, pass_salt, plain_password, earned_rub, manager_id, approved, created_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
        (name, username, hp["hash"], hp["salt"], password, manager_id, int(approved), now),
    )
    conn.commit()
    conn.close()


def create_partner_returning_id(
    name: str,
    username: str,
    password: str,
    manager_id: Optional[int] = None,
    approved: int = 1,
) -> int:
    now = int(time.time())
    hp = _hash_password(password)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO partners (name, username, pass_hash, pass_salt, plain_password, earned_rub, manager_id, approved, created_at) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)",
        (name, username, hp["hash"], hp["salt"], password, manager_id, int(approved), now),
    )
    partner_id = int(cur.lastrowid)
    conn.commit()
    conn.close()
    return partner_id


def set_partner_password(partner_id: int, password: str) -> None:
    hp = _hash_password(password)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE partners SET pass_hash = ?, pass_salt = ? WHERE id = ?",
        (hp["hash"], hp["salt"], int(partner_id)),
    )
    conn.commit()
    conn.close()


def set_partner_manager(partner_id: int, manager_id: Optional[int]) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE partners SET manager_id = ? WHERE id = ?", (manager_id, partner_id))
    # Backfill for users attributed to this partner before manager was assigned.
    if manager_id is not None:
        cur.execute(
            """
            UPDATE users
            SET manager_id = ?
            WHERE partner_id = ? AND (manager_id IS NULL OR manager_id = 0)
            """,
            (manager_id, partner_id),
        )
    conn.commit()
    conn.close()
    # Manager share depends on user.manager_id. Recompute for users of this partner.
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE partner_id = ?", (int(partner_id),))
        user_ids = [int(r[0]) for r in cur.fetchall()]
        conn.close()
        for uid in user_ids:
            recompute_user_earning_lines(uid)
    except Exception:
        pass


def list_partners() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, username, earned_rub, manager_id, approved, created_at FROM partners ORDER BY created_at DESC"
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_partner_by_username(username: str) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, username, pass_hash, pass_salt, plain_password, earned_rub, manager_id, approved FROM partners WHERE username = ?",
        (username,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_partner(partner_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, username, plain_password, earned_rub, manager_id, approved FROM partners WHERE id = ?",
        (partner_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def set_partner_approved(partner_id: int, approved: bool) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE partners SET approved = ? WHERE id = ?", (1 if approved else 0, partner_id))
    conn.commit()
    conn.close()
    # Approval affects whether partner share is counted.
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE partner_id = ?", (int(partner_id),))
        user_ids = [int(r[0]) for r in cur.fetchall()]
        conn.close()
        for uid in user_ids:
            recompute_user_earning_lines(uid)
    except Exception:
        pass


def update_partner_earned(partner_id: int, earned_rub: float) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE partners SET earned_rub = ? WHERE id = ?", (earned_rub, partner_id))
    conn.commit()
    conn.close()


def delete_partner(partner_id: int) -> None:
    """
    Fully remove a model (partner).

    What we do:
    - Remove partner links + partner clicks.
    - Detach users from this partner (and also manager_id if it matches the partner's manager).
    - Delete partner row.
    - Recompute earnings for affected users so earning_lines gets cleaned up.
    """
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT manager_id FROM partners WHERE id = ?", (int(partner_id),))
    row = cur.fetchone()
    manager_id = int(row[0]) if row and row[0] is not None else None

    cur.execute("SELECT user_id FROM users WHERE partner_id = ?", (int(partner_id),))
    user_ids = [int(r[0]) for r in cur.fetchall()]

    # Remove links and clicks for this model (events kept as history).
    cur.execute("DELETE FROM links WHERE partner_id = ?", (int(partner_id),))
    cur.execute("DELETE FROM clicks WHERE partner_id = ?", (int(partner_id),))

    # Detach users.
    if manager_id is None:
        cur.execute("UPDATE users SET partner_id = NULL WHERE partner_id = ?", (int(partner_id),))
    else:
        cur.execute(
            "UPDATE users SET partner_id = NULL, manager_id = NULL WHERE partner_id = ? AND manager_id = ?",
            (int(partner_id), int(manager_id)),
        )
        cur.execute(
            "UPDATE users SET partner_id = NULL WHERE partner_id = ? AND (manager_id IS NULL OR manager_id != ?)",
            (int(partner_id), int(manager_id)),
        )

    # Delete the partner itself.
    cur.execute("DELETE FROM partners WHERE id = ?", (int(partner_id),))
    conn.commit()
    conn.close()

    # Recompute for affected users (will also clean earning_lines attribution).
    for uid in user_ids:
        try:
            recompute_user_earning_lines(int(uid))
        except Exception:
            pass


def create_manager(name: str, username: str, password: str, *, approved: int = 1) -> None:
    now = int(time.time())
    hp = _hash_password(password)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO managers (name, username, pass_hash, pass_salt, plain_password, approved, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
        (name, username, hp["hash"], hp["salt"], password, int(approved), now),
    )
    conn.commit()
    conn.close()


def set_manager_password(manager_id: int, password: str) -> None:
    hp = _hash_password(password)
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "UPDATE managers SET pass_hash = ?, pass_salt = ? WHERE id = ?",
        (hp["hash"], hp["salt"], int(manager_id)),
    )
    conn.commit()
    conn.close()


def list_managers() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name, username, plain_password, paid_out_rub, approved, created_at FROM managers ORDER BY created_at DESC")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_manager(manager_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT id, name, username, plain_password, paid_out_rub, approved FROM managers WHERE id = ?", (manager_id,))
    row = cur.fetchone()
    conn.close()
    return row


def update_manager_paid_out(manager_id: int, paid_out_rub: float) -> None:
    paid = float(paid_out_rub or 0)
    if paid < 0:
        paid = 0.0
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE managers SET paid_out_rub = ? WHERE id = ?", (paid, manager_id))
    conn.commit()
    conn.close()

def set_manager_approved(manager_id: int, approved: bool) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("UPDATE managers SET approved = ? WHERE id = ?", (1 if approved else 0, manager_id))
    conn.commit()
    conn.close()
    # Approval affects whether manager share is counted.
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT user_id FROM users WHERE manager_id = ?", (int(manager_id),))
        user_ids = [int(r[0]) for r in cur.fetchall()]
        conn.close()
        for uid in user_ids:
            recompute_user_earning_lines(uid)
    except Exception:
        pass


def delete_manager(manager_id: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    # Detach models and users so we don't keep dangling ids.
    cur.execute("UPDATE partners SET manager_id = NULL WHERE manager_id = ?", (manager_id,))
    cur.execute("UPDATE users SET manager_id = NULL WHERE manager_id = ?", (manager_id,))
    cur.execute("DELETE FROM managers WHERE id = ?", (manager_id,))
    conn.commit()
    conn.close()
    # Detaching manager changes manager share -> recompute for affected users.
    try:
        conn = _connect()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT user_id FROM earning_lines WHERE manager_id = ?", (int(manager_id),))
        user_ids = [int(r[0]) for r in cur.fetchall()]
        conn.close()
        for uid in user_ids:
            recompute_user_earning_lines(uid)
    except Exception:
        pass


def managers_overview() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            m.id,
            m.name,
            m.username,
            m.plain_password,
            m.paid_out_rub,
            m.approved,
            COALESCE(pm.models, 0) AS models,
            COALESCE(ut.users_total, 0) AS users_total,
            COALESCE(ut.installed_users, 0) AS installed_users,
            COALESCE(rv.revenue_all, 0) AS revenue_all,
            COALESCE(rv.revenue_installed, 0) AS revenue_installed,
            COALESCE(el.earned_rub, 0) AS manager_earned_rub
        FROM managers m
        LEFT JOIN (
            SELECT manager_id, COUNT(*) AS models
            FROM partners
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) pm ON pm.manager_id = m.id
        LEFT JOIN (
            SELECT manager_id,
                   COUNT(*) AS users_total,
                   SUM(CASE WHEN installed = 1 THEN 1 ELSE 0 END) AS installed_users
            FROM users
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) ut ON ut.manager_id = m.id
        LEFT JOIN (
            SELECT u.manager_id AS manager_id,
                   SUM(p.amount_rub) AS revenue_all,
                   SUM(CASE WHEN u.installed = 1 THEN p.amount_rub ELSE 0 END) AS revenue_installed
            FROM payments p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.manager_id IS NOT NULL
            GROUP BY u.manager_id
        ) rv ON rv.manager_id = m.id
        LEFT JOIN (
            SELECT manager_id, SUM(manager_amount_rub) AS earned_rub
            FROM earning_lines
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) el ON el.manager_id = m.id
        ORDER BY m.created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def manager_rollup(manager_id: int) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            m.id,
            m.name,
            m.username,
            m.plain_password,
            m.paid_out_rub,
            m.approved,
            COALESCE(pm.models, 0) AS models,
            COALESCE(ut.users_total, 0) AS users_total,
            COALESCE(ut.installed_users, 0) AS installed_users,
            COALESCE(rv.revenue_all, 0) AS revenue_all,
            COALESCE(rv.revenue_installed, 0) AS revenue_installed,
            COALESCE(el.earned_rub, 0) AS manager_earned_rub
        FROM managers m
        LEFT JOIN (
            SELECT manager_id, COUNT(*) AS models
            FROM partners
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) pm ON pm.manager_id = m.id
        LEFT JOIN (
            SELECT manager_id,
                   COUNT(*) AS users_total,
                   SUM(CASE WHEN installed = 1 THEN 1 ELSE 0 END) AS installed_users
            FROM users
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) ut ON ut.manager_id = m.id
        LEFT JOIN (
            SELECT u.manager_id AS manager_id,
                   SUM(p.amount_rub) AS revenue_all,
                   SUM(CASE WHEN u.installed = 1 THEN p.amount_rub ELSE 0 END) AS revenue_installed
            FROM payments p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.manager_id IS NOT NULL
            GROUP BY u.manager_id
        ) rv ON rv.manager_id = m.id
        LEFT JOIN (
            SELECT manager_id, SUM(manager_amount_rub) AS earned_rub
            FROM earning_lines
            WHERE manager_id IS NOT NULL
            GROUP BY manager_id
        ) el ON el.manager_id = m.id
        WHERE m.id = ?
        """,
        (manager_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def partners_overview() -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.name,
            p.username,
            p.plain_password,
            p.earned_rub,
            p.manager_id,
            p.approved,
            COALESCE(m.name, '') AS manager_name,
            COALESCE(m.plain_password, '') AS manager_password,
            COALESCE(ut.users_total, 0) AS users_total,
            COALESCE(ut.installed_users, 0) AS installed_users,
            COALESCE(rv.revenue_all, 0) AS revenue_all,
            COALESCE(rv.revenue_installed, 0) AS revenue_installed,
            COALESCE(el.earned_rub, 0) AS partner_earned_rub,
            COALESCE(l.links, 0) AS links,
            COALESCE(c.clicks, 0) AS clicks,
            COALESCE(s.starts, 0) AS starts,
            COALESCE(u.uniq_users, 0) AS uniq_users
        FROM partners p
        LEFT JOIN managers m ON m.id = p.manager_id
        LEFT JOIN (
            SELECT partner_id,
                   COUNT(*) AS users_total,
                   SUM(CASE WHEN installed = 1 THEN 1 ELSE 0 END) AS installed_users
            FROM users
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) ut ON ut.partner_id = p.id
        LEFT JOIN (
            SELECT u.partner_id AS partner_id,
                   SUM(pay.amount_rub) AS revenue_all,
                   SUM(CASE WHEN u.installed = 1 THEN pay.amount_rub ELSE 0 END) AS revenue_installed
            FROM payments pay
            JOIN users u ON u.user_id = pay.user_id
            WHERE u.partner_id IS NOT NULL
            GROUP BY u.partner_id
        ) rv ON rv.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, SUM(partner_amount_rub) AS earned_rub
            FROM earning_lines
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) el ON el.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, COUNT(*) AS links
            FROM links
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) l ON l.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, COUNT(*) AS clicks
            FROM clicks
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) c ON c.partner_id = p.id
        LEFT JOIN (
            SELECT l.partner_id, COUNT(*) AS starts
            FROM events e
            JOIN links l ON l.code = e.code
            WHERE e.event_type = 'start' AND l.partner_id IS NOT NULL
            GROUP BY l.partner_id
        ) s ON s.partner_id = p.id
        LEFT JOIN (
            SELECT l.partner_id, COUNT(DISTINCT e.user_id) AS uniq_users
            FROM events e
            JOIN links l ON l.code = e.code
            WHERE e.event_type = 'start' AND l.partner_id IS NOT NULL AND e.user_id IS NOT NULL
            GROUP BY l.partner_id
        ) u ON u.partner_id = p.id
        ORDER BY p.created_at DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def partners_overview_by_manager(manager_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            p.id,
            p.name,
            p.username,
            p.earned_rub,
            p.manager_id,
            p.approved,
            COALESCE(m.name, '') AS manager_name,
            COALESCE(ut.users_total, 0) AS users_total,
            COALESCE(ut.installed_users, 0) AS installed_users,
            COALESCE(rv.revenue_all, 0) AS revenue_all,
            COALESCE(rv.revenue_installed, 0) AS revenue_installed,
            COALESCE(el.earned_rub, 0) AS partner_earned_rub,
            COALESCE(mel.earned_rub, 0) AS manager_earned_rub,
            COALESCE(l.links, 0) AS links,
            COALESCE(c.clicks, 0) AS clicks,
            COALESCE(s.starts, 0) AS starts,
            COALESCE(u.uniq_users, 0) AS uniq_users
        FROM partners p
        LEFT JOIN managers m ON m.id = p.manager_id
        LEFT JOIN (
            SELECT partner_id,
                   COUNT(*) AS users_total,
                   SUM(CASE WHEN installed = 1 THEN 1 ELSE 0 END) AS installed_users
            FROM users
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) ut ON ut.partner_id = p.id
        LEFT JOIN (
            SELECT u.partner_id AS partner_id,
                   SUM(pay.amount_rub) AS revenue_all,
                   SUM(CASE WHEN u.installed = 1 THEN pay.amount_rub ELSE 0 END) AS revenue_installed
            FROM payments pay
            JOIN users u ON u.user_id = pay.user_id
            WHERE u.partner_id IS NOT NULL
            GROUP BY u.partner_id
        ) rv ON rv.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, SUM(partner_amount_rub) AS earned_rub
            FROM earning_lines
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) el ON el.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, SUM(manager_amount_rub) AS earned_rub
            FROM earning_lines
            WHERE partner_id IS NOT NULL AND manager_id = ?
            GROUP BY partner_id
        ) mel ON mel.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, COUNT(*) AS links
            FROM links
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) l ON l.partner_id = p.id
        LEFT JOIN (
            SELECT partner_id, COUNT(*) AS clicks
            FROM clicks
            WHERE partner_id IS NOT NULL
            GROUP BY partner_id
        ) c ON c.partner_id = p.id
        LEFT JOIN (
            SELECT l.partner_id, COUNT(*) AS starts
            FROM events e
            JOIN links l ON l.code = e.code
            WHERE e.event_type = 'start' AND l.partner_id IS NOT NULL
            GROUP BY l.partner_id
        ) s ON s.partner_id = p.id
        LEFT JOIN (
            SELECT l.partner_id, COUNT(DISTINCT e.user_id) AS uniq_users
            FROM events e
            JOIN links l ON l.code = e.code
            WHERE e.event_type = 'start' AND l.partner_id IS NOT NULL AND e.user_id IS NOT NULL
            GROUP BY l.partner_id
        ) u ON u.partner_id = p.id
        WHERE p.manager_id = ?
        ORDER BY p.created_at DESC
        """,
        (int(manager_id), int(manager_id)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def admin_overview() -> Dict[str, float]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    users_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE installed = 1")
    installed_users_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM partners WHERE approved = 1")
    partners_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM managers WHERE approved = 1")
    managers_count = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(amount_rub), 0) FROM payments")
    revenue_total = float(cur.fetchone()[0] or 0)
    cur.execute("SELECT COALESCE(SUM(paid_out_rub), 0) FROM managers")
    managers_paid_out_total = float(cur.fetchone()[0] or 0)

    # Earned totals: sum per-payment splits; only count approved entities.
    cur.execute(
        """
        SELECT COALESCE(SUM(el.partner_amount_rub), 0)
        FROM earning_lines el
        JOIN partners p ON p.id = el.partner_id
        WHERE p.approved = 1
        """
    )
    partner_earned_total = float(cur.fetchone()[0] or 0)
    cur.execute(
        """
        SELECT COALESCE(SUM(el.manager_amount_rub), 0)
        FROM earning_lines el
        JOIN managers m ON m.id = el.manager_id
        WHERE m.approved = 1
        """
    )
    manager_earned_total = float(cur.fetchone()[0] or 0)

    # Commission base: ALL payments (only if installed=1) AND only for approved models/managers.
    cur.execute(
        """
        SELECT
            COALESCE(SUM(CASE WHEN u.installed = 1 AND pt.approved = 1 THEN p.amount_rub ELSE 0 END), 0) AS model_base,
            COALESCE(SUM(CASE WHEN u.installed = 1 AND mg.approved = 1 THEN p.amount_rub ELSE 0 END), 0) AS manager_base
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        LEFT JOIN partners pt ON pt.id = u.partner_id
        LEFT JOIN managers mg ON mg.id = u.manager_id
        """
    )
    row = cur.fetchone()
    model_base = float(row[0] or 0) if row else 0.0
    manager_base = float(row[1] or 0) if row else 0.0
    conn.close()
    return {
        "users": int(users_count),
        "installed_users": int(installed_users_count),
        "partners": int(partners_count),
        "managers": int(managers_count),
        "revenue_rub": revenue_total,
        # Backward-compatible key (older templates used it)
        "partners_earned_rub": partner_earned_total,
        # Keys used by app.py (new dashboards)
        "partner_earned_rub": partner_earned_total,
        "manager_earned_rub": manager_earned_total,
        "managers_paid_out_rub": managers_paid_out_total,
        "model_base_rub": model_base,
        "manager_base_rub": manager_base,
    }


def admin_funnel_overview() -> Dict[str, Any]:
    """Builds strict step-by-step all-time funnel based on unique users."""
    step_defs = ADMIN_FUNNEL_STEPS
    alias_to_key, event_names = _funnel_alias_map()

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = int(cur.fetchone()[0] or 0)

    user_steps: Dict[int, Dict[str, int]] = {}
    if event_names:
        placeholders = ",".join("?" for _ in event_names)
        cur.execute(
            f"""
            SELECT user_id, event_type, MIN(created_at) AS first_ts
            FROM events
            WHERE user_id IS NOT NULL AND event_type IN ({placeholders})
            GROUP BY user_id, event_type
            ORDER BY user_id ASC
            """,
            tuple(event_names),
        )
        for row in cur.fetchall():
            user_id = int(row["user_id"])
            event_type = str(row["event_type"] or "")
            step_key = alias_to_key.get(event_type)
            if not step_key:
                continue
            ts = int(row["first_ts"] or 0)
            bucket = user_steps.setdefault(user_id, {})
            prev_ts = bucket.get(step_key)
            if prev_ts is None or ts < prev_ts:
                bucket[step_key] = ts

    conn.close()

    counts = [0 for _ in step_defs]
    for step_times in user_steps.values():
        last_idx = _strict_funnel_last_idx(step_times)
        for idx in range(last_idx + 1):
            counts[idx] += 1

    start_users = int(counts[0] if counts else 0)
    final_users = int(counts[-1] if counts else 0)

    steps_payload: List[Dict[str, Any]] = []
    prev_users = 0
    for idx, step in enumerate(step_defs):
        users = int(counts[idx])
        if idx == 0:
            pct_from_start = 100.0 if start_users else 0.0
            pct_from_prev = 100.0 if start_users else 0.0
        else:
            pct_from_start = round((users / start_users) * 100.0, 2) if start_users else 0.0
            pct_from_prev = round((users / prev_users) * 100.0, 2) if prev_users else 0.0
        steps_payload.append(
            {
                "key": step["key"],
                "label": step["label"],
                "users": users,
                "pct_from_start": pct_from_start,
                "pct_from_prev": pct_from_prev,
            }
        )
        prev_users = users

    return {
        "updated_at": int(time.time()),
        "total_users": int(total_users),
        "start_users": start_users,
        "final_users": final_users,
        "steps": steps_payload,
    }


def partner_rollup(partner_id: int) -> Dict[str, float]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(earned_rub, 0), manager_id FROM partners WHERE id = ?", (partner_id,))
    row = cur.fetchone()
    paid_out = float(row[0] or 0) if row else 0.0
    manager_id = int(row[1]) if row and row[1] is not None else None

    cur.execute("SELECT COUNT(*) FROM links WHERE partner_id = ?", (partner_id,))
    links = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM clicks WHERE partner_id = ?", (partner_id,))
    clicks = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE partner_id = ?", (partner_id,))
    users_total = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE partner_id = ? AND installed = 1", (partner_id,))
    installed_users = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COALESCE(SUM(p.amount_rub), 0)
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        WHERE u.partner_id = ?
        """,
        (partner_id,),
    )
    revenue_all = float(cur.fetchone()[0] or 0)

    # Commission base: ALL payments (only if installed=1).
    cur.execute(
        """
        SELECT COALESCE(SUM(p.amount_rub), 0)
        FROM payments p
        JOIN users u ON u.user_id = p.user_id
        WHERE u.partner_id = ? AND u.installed = 1
        """,
        (partner_id,),
    )
    revenue_installed = float(cur.fetchone()[0] or 0)

    cur.execute(
        """
        SELECT COALESCE(SUM(partner_amount_rub), 0)
        FROM earning_lines
        WHERE partner_id = ?
        """,
        (partner_id,),
    )
    partner_earned_rub = float(cur.fetchone()[0] or 0)

    cur.execute(
        """
        SELECT COUNT(*)
        FROM events e
        JOIN links l ON l.code = e.code
        WHERE e.event_type = 'start' AND l.partner_id = ?
        """,
        (partner_id,),
    )
    starts = cur.fetchone()[0]

    cur.execute(
        """
        SELECT COUNT(DISTINCT e.user_id)
        FROM events e
        JOIN links l ON l.code = e.code
        WHERE e.event_type = 'start' AND l.partner_id = ? AND e.user_id IS NOT NULL
        """,
        (partner_id,),
    )
    uniq_users = cur.fetchone()[0]

    conn.close()
    return {
        "links": int(links),
        "clicks": int(clicks),
        "starts": int(starts),
        "uniq_users": int(uniq_users),
        "users_total": int(users_total),
        "installed_users": int(installed_users),
        "revenue_all": revenue_all,
        "revenue_installed": revenue_installed,
        "partner_earned_rub": partner_earned_rub,
        "paid_out_rub": paid_out,
        "manager_id": manager_id,
    }


def verify_partner_password(username: str, password: str) -> Optional[sqlite3.Row]:
    partner = get_partner_by_username(username)
    if not partner:
        return None
    hp = _hash_password(password, partner["pass_salt"])
    if hp["hash"] != partner["pass_hash"]:
        return None
    return partner


def get_manager_by_username(username: str) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, username, pass_hash, pass_salt, plain_password, paid_out_rub, approved FROM managers WHERE username = ?",
        (username,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def verify_manager_password(username: str, password: str) -> Optional[sqlite3.Row]:
    manager = get_manager_by_username(username)
    if not manager:
        return None
    hp = _hash_password(password, manager["pass_salt"])
    if hp["hash"] != manager["pass_hash"]:
        return None
    return manager


def verify_worker_password(username: str, password: str) -> Optional[sqlite3.Row]:
    worker = get_worker_by_username(username)
    if not worker:
        return None
    password_clean = (password or "").strip()
    hp = _hash_password(password_clean, worker["pass_salt"])
    if hp["hash"] != worker["pass_hash"]:
        return None
    return worker


def create_manager_invite(manager_id: int) -> str:
    """Reusable invite code for a model to self-register and auto-attach to a manager."""
    code = secrets.token_urlsafe(8).replace("-", "").replace("_", "")[:10].lower()
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO manager_invites (code, manager_id, created_at, uses_count) VALUES (?, ?, ?, 0)",
        (code, int(manager_id), int(time.time())),
    )
    conn.commit()
    conn.close()
    return code


def list_manager_invites(manager_id: int, limit: int = 200) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT code, manager_id, created_at, used_at, used_partner_id, uses_count
        FROM manager_invites
        WHERE manager_id = ?
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (int(manager_id), int(limit)),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_manager_invite(code: str) -> Optional[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, manager_id, created_at, used_at, used_partner_id, uses_count FROM manager_invites WHERE code = ?",
        ((code or "").strip(),),
    )
    row = cur.fetchone()
    conn.close()
    return row


def use_manager_invite(code: str, partner_id: int) -> bool:
    """Record invite usage (multi-use). Returns True if invite exists."""
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE manager_invites
        SET used_at = ?, used_partner_id = ?, uses_count = COALESCE(uses_count, 0) + 1
        WHERE code = ?
        """,
        (int(time.time()), int(partner_id), (code or "").strip()),
    )
    ok = cur.rowcount > 0
    conn.commit()
    conn.close()
    return ok


def delete_manager_invite(code: str, manager_id: int) -> None:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("DELETE FROM manager_invites WHERE code = ? AND manager_id = ?", ((code or "").strip(), int(manager_id)))
    conn.commit()
    conn.close()


def list_partner_links(partner_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        "SELECT code, name, target_url, created_at FROM links WHERE partner_id = ? ORDER BY created_at DESC",
        (partner_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_partner_links_with_clicks(partner_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            l.code,
            l.name,
            l.target_url,
            COALESCE(c.clicks, 0) AS clicks
        FROM links l
        LEFT JOIN (
            SELECT code, COUNT(*) AS clicks
            FROM clicks
            WHERE partner_id = ?
            GROUP BY code
        ) c ON c.code = l.code
        WHERE l.partner_id = ?
        ORDER BY l.created_at DESC
        """,
        (partner_id, partner_id),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def partner_link_stats(partner_id: int) -> List[sqlite3.Row]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            l.code,
            l.name,
            l.target_url,
            l.created_at,
            COALESCE(c.clicks, 0) AS clicks,
            COALESCE(s.starts, 0) AS starts,
            COALESCE(u.uniq_users, 0) AS uniq_users,
            COALESCE(ut.users_total, 0) AS users_total,
            COALESCE(ut.installed_users, 0) AS installed_users,
            COALESCE(rv_all.revenue_all, 0) AS revenue_all,
            COALESCE(rv_inst.revenue_installed, 0) AS revenue_installed
        FROM links l
        LEFT JOIN (
            SELECT code, COUNT(*) AS clicks
            FROM clicks
            WHERE partner_id = ?
            GROUP BY code
        ) c ON c.code = l.code
        LEFT JOIN (
            SELECT code, COUNT(*) AS starts
            FROM events
            WHERE event_type = 'start' AND code IS NOT NULL
            GROUP BY code
        ) s ON s.code = l.code
        LEFT JOIN (
            SELECT code, COUNT(DISTINCT user_id) AS uniq_users
            FROM events
            WHERE event_type = 'start' AND code IS NOT NULL AND user_id IS NOT NULL
            GROUP BY code
        ) u ON u.code = l.code
        LEFT JOIN (
            SELECT ref_code AS code,
                   COUNT(*) AS users_total,
                   SUM(CASE WHEN installed = 1 THEN 1 ELSE 0 END) AS installed_users
            FROM users
            WHERE ref_code IS NOT NULL AND ref_code != ''
            GROUP BY ref_code
        ) ut ON ut.code = l.code
        LEFT JOIN (
            SELECT u.ref_code AS code,
                   SUM(p.amount_rub) AS revenue_all
            FROM payments p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.ref_code IS NOT NULL AND u.ref_code != ''
            GROUP BY u.ref_code
        ) rv_all ON rv_all.code = l.code
        LEFT JOIN (
            SELECT
                u.ref_code AS code,
                SUM(p.amount_rub) AS revenue_installed
            FROM payments p
            JOIN users u ON u.user_id = p.user_id
            WHERE u.ref_code IS NOT NULL AND u.ref_code != '' AND u.installed = 1
            GROUP BY u.ref_code
        ) rv_inst ON rv_inst.code = l.code
        WHERE l.partner_id = ?
        ORDER BY l.created_at DESC
        """,
        (partner_id, partner_id),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def partner_summary(partner_id: int) -> Dict[str, int]:
    conn = _connect()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM links WHERE partner_id = ?", (partner_id,))
    total_links = cur.fetchone()[0]
    cur.execute(
        """
        SELECT COUNT(*)
        FROM events e
        JOIN links l ON l.code = e.code
        WHERE e.event_type = 'start' AND l.partner_id = ?
        """,
        (partner_id,),
    )
    starts = cur.fetchone()[0]
    conn.close()
    return {"total_links": total_links, "starts": starts}
