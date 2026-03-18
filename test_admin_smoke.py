import importlib
import base64
import json
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


class AdminSmokeTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = str(Path(self.temp_dir.name) / "test_admin.db")
        self.staging_dir = str(Path(self.temp_dir.name) / "staging")
        self._old_env: dict[str, str | None] = {}
        overrides = {
            "ADMIN_DB_PATH": self.db_path,
            "ADMIN_USER": "admin",
            "ADMIN_PASS": "secret",
            "SESSION_SECRET": "session-secret",
            "HELPER_API_KEY": "helper-key",
            "PUBLISH_RUNNER_API_KEY": "runner-key",
            "PUBLISH_SHARED_SECRET": "publish-secret",
            "PUBLISH_N8N_WEBHOOK_URL": "https://n8n.example/hook",
            "PUBLISH_STAGING_DIR": self.staging_dir,
            "PUBLISH_BASE_URL": "http://testserver",
            "ADMIN_BASE_PATH": "",
            "ADMIN_TEST_CHAT_ID": "42",
        }
        for key, value in overrides.items():
            self._old_env[key] = os.environ.get(key)
            os.environ[key] = value

        import db as db_module
        import app as app_module

        self.db = importlib.reload(db_module)
        self.app_module = importlib.reload(app_module)
        self.db.init_db()
        self.client = TestClient(self.app_module.app)
        login = self.client.post(
            "/login",
            data={"username": os.environ["ADMIN_USER"], "password": os.environ["ADMIN_PASS"]},
            follow_redirects=False,
        )
        self.assertEqual(login.status_code, 303)
        self.worker_client: TestClient | None = None

    def tearDown(self) -> None:
        if self.worker_client is not None:
            self.worker_client.close()
        self.client.close()
        self.temp_dir.cleanup()
        for key, old_value in self._old_env.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value

    def _create_user(self, user_id: int, username: str, *, events: list[str] | None = None) -> None:
        self.db.upsert_user(user_id, username, "Test", None)
        for event in events or []:
            self.db.log_event(user_id, event)

    def _create_worker_via_admin(self, *, name: str, username: str, password: str) -> int:
        response = self.client.post(
            "/workers",
            data={"name": name, "username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        workers = [dict(row) for row in self.db.list_workers_compact(q=username, limit=10)]
        self.assertEqual(len(workers), 1)
        return int(workers[0]["id"])

    def _create_account_via_admin(
        self,
        *,
        login: str,
        username: str,
        owner_worker_id: str = "",
        serial: str = "emulator-5554",
    ) -> int:
        response = self.client.post(
            "/accounts",
            data={
                "type": "instagram",
                "account_login": login,
                "account_password": "pass123",
                "username": username,
                "email": f"{username}@example.com",
                "email_password": "mailpass",
                "proxy": "",
                "twofa": "JBSWY3DPEHPK3PXP",
                "instagram_emulator_serial": serial,
                "rotation_state": "review",
                "views_state": "unknown",
                "owner_worker_id": owner_worker_id,
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        location = response.headers["location"]
        self.assertTrue(location.startswith("/accounts/"))
        return int(location.split("/accounts/", 1)[1].split("?", 1)[0])

    def _login_worker(self, username: str, password: str) -> TestClient:
        client = TestClient(self.app_module.app)
        login = client.post(
            "/worker/login",
            data={"username": username, "password": password},
            follow_redirects=False,
        )
        self.assertEqual(login.status_code, 303)
        self.worker_client = client
        return client

    def test_worker_login_page_renders_for_logged_out_user(self) -> None:
        guest_client = TestClient(self.app_module.app)
        try:
            response = guest_client.get("/worker/login")
        finally:
            guest_client.close()
        self.assertEqual(response.status_code, 200)
        self.assertIn("Кабинет работника", response.text)

    def test_stats_and_users_pages_render_and_filter(self) -> None:
        self._create_user(1, "alpha", events=["start"])
        self._create_user(2, "beta")

        index_page = self.client.get("/")
        self.assertEqual(index_page.status_code, 200)
        self.assertIn("Обзор", index_page.text)
        self.assertIn("Воронка", index_page.text)

        stats_response = self.client.get("/api/stats/funnel")
        self.assertEqual(stats_response.status_code, 200)
        self.assertEqual(stats_response.json()["start_users"], 1)

        users_page = self.client.get("/users", params={"q": "alpha", "step": "start"})
        self.assertEqual(users_page.status_code, 200)
        self.assertIn("@alpha", users_page.text)
        self.assertNotIn("@beta", users_page.text)

    def test_workers_admin_and_worker_account_pages_smoke(self) -> None:
        worker_id = self._create_worker_via_admin(name="Smoke Worker", username="smoke_worker", password="worker-pass")

        workers_page = self.client.get("/workers")
        self.assertEqual(workers_page.status_code, 200)
        self.assertIn("Smoke Worker", workers_page.text)
        self.assertIn("Переходов:", workers_page.text)

        update_response = self.client.post(
            f"/workers/{worker_id}/update",
            data={"name": "Smoke Worker 2", "username": "smoke_worker", "password": ""},
            follow_redirects=False,
        )
        self.assertEqual(update_response.status_code, 303)

        worker_detail = self.client.get(f"/workers/{worker_id}")
        self.assertEqual(worker_detail.status_code, 200)
        self.assertIn("Smoke Worker 2", worker_detail.text)

        account_id = self._create_account_via_admin(
            login="worker_login_1",
            username="worker_account_1",
            owner_worker_id=str(worker_id),
        )

        worker_client = self._login_worker("smoke_worker", "worker-pass")
        worker_accounts = worker_client.get("/worker/accounts")
        self.assertEqual(worker_accounts.status_code, 200)
        self.assertIn("worker_account_1", worker_accounts.text)

        worker_account_detail = worker_client.get(f"/worker/accounts/{account_id}")
        self.assertEqual(worker_account_detail.status_code, 200)
        self.assertIn("Ссылки аккаунта", worker_account_detail.text)

        update_account = worker_client.post(
            f"/worker/accounts/{account_id}/update",
            data={
                "type": "instagram",
                "account_login": "worker_login_1",
                "account_password": "pass123",
                "username": "worker_account_2",
                "email": "worker_account_2@example.com",
                "email_password": "mailpass",
                "proxy": "",
                "twofa": "JBSWY3DPEHPK3PXP",
                "next_url": f"/worker/accounts/{account_id}",
                "return_to": "/worker/accounts",
            },
            follow_redirects=False,
        )
        self.assertEqual(update_account.status_code, 303)
        self.assertEqual(dict(self.db.get_account(account_id))["username"], "worker_account_2")

    def test_accounts_crud_and_broadcast_pages_smoke(self) -> None:
        account_id = self._create_account_via_admin(login="admin_login_1", username="admin_account_1")

        accounts_page = self.client.get("/accounts", params={"q": "admin_account_1"})
        self.assertEqual(accounts_page.status_code, 200)
        self.assertIn("admin_account_1", accounts_page.text)
        self.assertIn("Проверить входы Instagram", accounts_page.text)

        detail_page = self.client.get(f"/accounts/{account_id}")
        self.assertEqual(detail_page.status_code, 200)
        self.assertIn("Публикация Reel", detail_page.text)
        self.assertIn("Почта", detail_page.text)

        update_response = self.client.post(
            f"/accounts/{account_id}/update",
            data={
                "type": "instagram",
                "account_login": "admin_login_1",
                "account_password": "pass123",
                "username": "admin_account_2",
                "email": "admin_account_2@example.com",
                "email_password": "mailpass",
                "proxy": "",
                "twofa": "otpauth://totp/Instagram:admin_account_2?secret=JBSWY3DPEHPK3PXP&issuer=Instagram",
                "instagram_emulator_serial": "emulator-5554",
                "rotation_state": "review",
                "views_state": "unknown",
                "owner_worker_id": "",
                "next_url": f"/accounts/{account_id}",
                "return_to": "/accounts",
            },
            follow_redirects=False,
        )
        self.assertEqual(update_response.status_code, 303)
        self.assertEqual(dict(self.db.get_account(account_id))["username"], "admin_account_2")
        self.assertEqual(dict(self.db.get_account(account_id))["twofa"], "JBSWY3DPEHPK3PXP")

    def test_accounts_update_rejects_invalid_twofa(self) -> None:
        account_id = self._create_account_via_admin(login="admin_login_invalid_twofa", username="admin_invalid_twofa")

        update_response = self.client.post(
            f"/accounts/{account_id}/update",
            data={
                "type": "instagram",
                "account_login": "admin_login_invalid_twofa",
                "account_password": "pass123",
                "username": "admin_invalid_twofa",
                "email": "admin_invalid_twofa@example.com",
                "email_password": "mailpass",
                "proxy": "",
                "twofa": "otpauth://totp/Instagram:admin_invalid_twofa?issuer=Instagram",
                "instagram_emulator_serial": "emulator-5554",
                "rotation_state": "review",
                "views_state": "unknown",
                "owner_worker_id": "",
                "next_url": f"/accounts/{account_id}",
                "return_to": "/accounts",
            },
            follow_redirects=False,
        )
        self.assertEqual(update_response.status_code, 400)
        self.assertIn("валидным base32 secret или otpauth:// URI", update_response.text)

        delete_response = self.client.post(
            f"/accounts/{account_id}/delete",
            data={"next_url": "/accounts"},
            follow_redirects=False,
        )
        self.assertEqual(delete_response.status_code, 303)
        self.assertIsNone(self.db.get_account(account_id))

        self._create_user(10, "broadcast_alpha", events=["start"])
        self._create_user(11, "broadcast_beta")

        broadcast_page = self.client.get("/broadcast")
        self.assertEqual(broadcast_page.status_code, 200)
        self.assertIn("Рассылка", broadcast_page.text)
        self.assertIn("Получателей", broadcast_page.text)

        count_response = self.client.get("/broadcast/count", params={"scope": "all", "stage_mode": "reached"})
        self.assertEqual(count_response.status_code, 200)
        self.assertEqual(count_response.json()["recipients"], 2)

        with patch.object(self.app_module, "_send_message", return_value=False):
            test_send = self.client.post(
                "/broadcast/test",
                data={"message": "Тестовое сообщение", "test_chat_id": "42", "scope": "all", "stage_mode": "reached"},
            )
        self.assertEqual(test_send.status_code, 200)
        self.assertIn("Ошибок", test_send.text)

        with patch.object(self.app_module, "_send_message", side_effect=[True, False]):
            send_response = self.client.post(
                "/broadcast",
                data={"message": "Боевой прогон", "scope": "all", "stage_mode": "reached"},
            )
        self.assertEqual(send_response.status_code, 200)
        last_run = dict(self.db.list_broadcast_runs(limit=1)[0])
        self.assertEqual(last_run["sent"], 1)
        self.assertEqual(last_run["failed"], 1)

    def test_accounts_page_shows_auto_reason_for_not_working_account(self) -> None:
        account_id = self._create_account_via_admin(login="admin_reason_1", username="admin_reason_user")
        self.db.update_account_instagram_publish_state(
            account_id,
            "invalid_password",
            "Instagram отклонил пароль. Проверь account_password у этого аккаунта.",
            last_file="ready.mp4",
        )

        accounts_page = self.client.get("/accounts", params={"q": "admin_reason_user"})
        self.assertEqual(accounts_page.status_code, 200)
        self.assertIn("Instagram отклонил пароль", accounts_page.text)

        detail_page = self.client.get(f"/accounts/{account_id}")
        self.assertEqual(detail_page.status_code, 200)
        self.assertIn("Instagram отклонил пароль", detail_page.text)

    def test_accounts_reject_numeric_only_emulator_serial(self) -> None:
        response = self.client.post(
            "/accounts",
            data={
                "type": "instagram",
                "account_login": "bad_serial_login",
                "account_password": "pass123",
                "username": "bad_serial_user",
                "email": "bad_serial_user@example.com",
                "email_password": "mailpass",
                "proxy": "",
                "twofa": "JBSWY3DPEHPK3PXP",
                "instagram_emulator_serial": "3",
                "rotation_state": "review",
                "views_state": "unknown",
                "owner_worker_id": "",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 400)
        self.assertIn("Неверный Instagram emulator serial", response.text)
        self.assertIsNone(self.db.find_duplicate_account("instagram", "bad_serial_login"))

    def test_helper_launch_ticket_exposes_safe_mail_metadata(self) -> None:
        account_id = self._create_account_via_admin(login="ticket_login_1", username="ticket_account_1")
        ticket = self.db.create_helper_launch_ticket(
            account_id=account_id,
            target="instagram_app_login",
            created_by_admin="admin",
        )

        response = self.client.get(
            f"/api/helper/launch-ticket/{ticket['ticket']}",
            params={"target": "instagram_app_login"},
            headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["account_id"], account_id)
        self.assertTrue(payload["mail_enabled"])
        self.assertEqual(payload["mail_address"], "ticket_account_1@example.com")
        self.assertEqual(payload["mail_provider"], "auto")
        self.assertNotIn("email_password", payload)

    def test_helper_mail_challenge_resolve_returns_numeric_code_and_persists_state(self) -> None:
        account_id = self._create_account_via_admin(login="mail_login_1", username="mail_account_1")
        now = int(time.time())
        resolved_messages = [
            {
                "message_uid": "mail-uid-1",
                "from_text": "Instagram <security@mail.instagram.com>",
                "subject": "Security code",
                "received_at": now,
                "snippet": "Use 123456 to log in.",
                "body_text": "Use 123456 to log in to Instagram.",
                "body_html": "<p>Use 123456 to log in to Instagram.</p>",
                "to_text": "mail_account_1@example.com",
                "cc_text": "",
                "to_addresses": ["mail_account_1@example.com"],
                "cc_addresses": [],
                "links": [],
            }
        ]

        with patch.object(
            self.app_module.mail_service,
            "fetch_recent_messages",
            return_value={"provider": "imap", "status": "ok", "error": "", "messages": resolved_messages},
        ):
            response = self.client.post(
                f"/api/helper/accounts/{account_id}/mail-challenge/resolve",
                json={
                    "ticket": "test-ticket",
                    "challenge_started_at": now,
                    "screen_kind": "numeric_code",
                    "timeout_seconds": 5,
                },
                headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "resolved")
        self.assertEqual(payload["kind"], "numeric_code")
        self.assertEqual(payload["code"], "123456")
        self.assertEqual(payload["masked_code"], "123***")

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["mail_challenge_status"], "resolved")
        self.assertEqual(account["mail_challenge_kind"], "numeric_code")
        self.assertEqual(account["mail_challenge_reason_code"], "mail_code_ready")
        self.assertEqual(account["mail_challenge_masked_code"], "123***")
        self.assertEqual(account["mail_challenge_message_uid"], "mail-uid-1")
        self.assertGreater(float(account["mail_challenge_confidence"]), 0)

    def test_helper_mail_challenge_resolve_detects_verify_your_account_html_template(self) -> None:
        account_id = self._create_account_via_admin(login="mail_login_2", username="mail_account_2")
        now = int(time.time())
        resolved_messages = [
            {
                "message_uid": "mail-uid-verify-1",
                "from_text": "Instagram <security@mail.instagram.com>",
                "subject": "Verify your account",
                "received_at": now,
                "snippet": "",
                "body_text": "",
                "body_html": (
                    "Hi mail_account_2, Someone tried to log in to your Instagram account. "
                    "If this was you, please use the following code to confirm your identity: 121687 "
                    "If this wasn't you, please reset your password to secure your account."
                ),
                "to_text": "mail_account_2@example.com",
                "cc_text": "",
                "to_addresses": ["mail_account_2@example.com"],
                "cc_addresses": [],
                "links": [
                    "https://www.instagram.com/accounts/password/reset/",
                    "https://instagram.com/accounts/remove/revoke_wrong_email/?uidb36=test&token=test",
                ],
            }
        ]

        with patch.object(
            self.app_module.mail_service,
            "fetch_recent_messages",
            return_value={"provider": "imap", "status": "ok", "error": "", "messages": resolved_messages},
        ):
            response = self.client.post(
                f"/api/helper/accounts/{account_id}/mail-challenge/resolve",
                json={
                    "ticket": "test-ticket",
                    "challenge_started_at": now,
                    "screen_kind": "numeric_code",
                    "timeout_seconds": 5,
                },
                headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "resolved")
        self.assertEqual(payload["kind"], "numeric_code")
        self.assertEqual(payload["code"], "121687")
        self.assertEqual(payload["masked_code"], "121***")

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["mail_challenge_status"], "resolved")
        self.assertEqual(account["mail_challenge_kind"], "numeric_code")
        self.assertEqual(account["mail_challenge_reason_code"], "mail_code_ready")
        self.assertEqual(account["mail_challenge_masked_code"], "121***")
        self.assertEqual(account["mail_challenge_message_uid"], "mail-uid-verify-1")
        self.assertGreater(float(account["mail_challenge_confidence"]), 0)

    def test_helper_mail_challenge_resolve_uses_cached_mail_before_refresh(self) -> None:
        account_id = self._create_account_via_admin(login="mail_cached_login_1", username="mail_cached_account_1")
        now = int(time.time())
        self.db.replace_account_mail_messages(
            account_id,
            [
                {
                    "message_uid": "cached-mail-uid-1",
                    "provider_message_id": "cached-mail-uid-1",
                    "from_text": "Instagram <security@mail.instagram.com>",
                    "subject": "Security code",
                    "received_at": now,
                    "snippet": "Use 654321 to log in.",
                    "body_text": "Use 654321 to log in to Instagram.",
                    "body_html": "<p>Use 654321 to log in to Instagram.</p>",
                    "to_text": "mail_cached_account_1@example.com",
                    "cc_text": "",
                    "to_addresses": ["mail_cached_account_1@example.com"],
                    "cc_addresses": [],
                    "links": [],
                }
            ],
        )

        with patch.object(self.app_module.mail_service, "fetch_recent_messages", side_effect=AssertionError("refresh should not be called")):
            response = self.client.post(
                f"/api/helper/accounts/{account_id}/mail-challenge/resolve",
                json={
                    "ticket": "cached-ticket",
                    "challenge_started_at": now,
                    "screen_kind": "numeric_code",
                    "timeout_seconds": 5,
                },
                headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "resolved")
        self.assertEqual(payload["kind"], "numeric_code")
        self.assertEqual(payload["code"], "654321")

    def test_helper_mail_challenge_resolve_accepts_channel_choice_screen_kind(self) -> None:
        account_id = self._create_account_via_admin(login="mail_channel_login_1", username="mail_channel_account_1")
        now = int(time.time())
        resolved_payload = {
            "status": "resolved",
            "kind": "numeric_code",
            "code": "112233",
            "masked_code": "112***",
            "message_uid": "mail-uid-channel-1",
            "received_at": now,
            "confidence": 0.87,
            "reason_code": "mail_code_ready",
            "reason_text": "Найден свежий код.",
        }

        with patch.object(self.app_module, "_resolve_instagram_mail_challenge", return_value=resolved_payload) as resolve_mock:
            response = self.client.post(
                f"/api/helper/accounts/{account_id}/mail-challenge/resolve",
                json={
                    "ticket": "channel-ticket",
                    "challenge_started_at": now,
                    "screen_kind": "channel_choice",
                    "timeout_seconds": 5,
                },
                headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["status"], "resolved")
        self.assertEqual(payload["kind"], "numeric_code")
        resolve_mock.assert_called_once()
        self.assertEqual(resolve_mock.call_args.kwargs["screen_kind"], "channel_choice")

    def test_accounts_create_accepts_gmail_api_without_email_password(self) -> None:
        response = self.client.post(
            "/accounts",
            data={
                "type": "instagram",
                "account_login": "gmail_api_login",
                "account_password": "pass123",
                "username": "gmail_api_user",
                "email": "gmail_api_user@example.com",
                "email_password": "",
                "mail_provider": "gmail_api",
                "mail_auth_json": json.dumps(
                    {
                        "client_id": "gmail-client",
                        "client_secret": "gmail-secret",
                        "refresh_token": "gmail-refresh",
                    }
                ),
                "proxy": "",
                "twofa": "",
                "instagram_emulator_serial": "emulator-5554",
                "rotation_state": "review",
                "views_state": "unknown",
                "owner_worker_id": "",
            },
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        account_id = int(response.headers["location"].split("/accounts/", 1)[1].split("?", 1)[0])
        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["mail_provider"], "gmail_api")
        self.assertEqual(json.loads(account["mail_auth_json"])["refresh_token"], "gmail-refresh")
        self.assertEqual(account["email_password"], "")

        detail_page = self.client.get(f"/accounts/{account_id}")
        self.assertEqual(detail_page.status_code, 200)
        self.assertIn("Gmail API", detail_page.text)

    def test_gmail_webhook_enqueues_mail_sync_task(self) -> None:
        account_id = self._create_account_via_admin(login="gmail_webhook_login", username="gmail_webhook_user")
        self.db.update_account(
            account_id=account_id,
            account_type="instagram",
            account_login="gmail_webhook_login",
            account_password="pass123",
            username="gmail_webhook_user",
            email="gmail_webhook_user@example.com",
            email_password="",
            mail_provider="gmail_api",
            mail_auth_json=json.dumps(
                {
                    "client_id": "gmail-client",
                    "client_secret": "gmail-secret",
                    "refresh_token": "gmail-refresh",
                    "topic_name": "projects/demo/topics/mail",
                }
            ),
            proxy="",
            twofa="",
            instagram_emulator_serial="emulator-5554",
            rotation_state="review",
            views_state="unknown",
            owner_worker_id=None,
        )
        payload = {
            "message": {
                "data": base64.b64encode(
                    json.dumps({"emailAddress": "gmail_webhook_user@example.com", "historyId": "777"}).encode("utf-8")
                ).decode("utf-8")
            }
        }
        with patch.object(self.app_module, "_enqueue_mail_account_sync") as mocked:
            response = self.client.post(
                f"/api/internal/mail/webhooks/gmail?secret={os.environ['PUBLISH_SHARED_SECRET']}",
                json=payload,
            )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["account_id"], account_id)
        mocked.assert_called_once_with(account_id, reason="gmail_push")

    def test_publishing_pages_render_stable_copy(self) -> None:
        self._create_account_via_admin(login="publish_login_1", username="publish_account_1")

        publishing_page = self.client.get("/publishing")
        self.assertEqual(publishing_page.status_code, 200)
        self.assertIn("Начать публикацию", publishing_page.text)

        start_page = self.client.get("/publishing/start")
        self.assertEqual(start_page.status_code, 200)
        self.assertIn("Запустить", start_page.text)
        self.assertNotIn(">Publish<", start_page.text)
