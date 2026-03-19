import importlib
import hashlib
import json
import os
import re
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


class PublishingBatchTests(unittest.TestCase):
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

    def tearDown(self) -> None:
        self.client.close()
        for attempt in range(3):
            try:
                self.temp_dir.cleanup()
                break
            except OSError:
                if attempt == 2:
                    raise
                time.sleep(0.05)
        for key, old_value in self._old_env.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value

    def _create_instagram_account(
        self,
        login: str,
        username: str,
        serial: str,
        *,
        twofa: str = "JBSWY3DPEHPK3PXP",
        rotation_state: str = "review",
    ) -> int:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login=login,
            account_password="pass123",
            username=username,
            email=f"{username}@example.com",
            email_password="mailpass",
            proxy="",
            twofa=twofa,
            rotation_state=rotation_state,
            instagram_emulator_serial=serial,
            default_link_name=f"Instagram @{username}",
        )
        account_id = int(created["account_id"])
        if serial and rotation_state != "not_working":
            self.db.update_account_instagram_launch_state(
                account_id,
                "login_submitted",
                "Тестовая live-проверка входа выполнена.",
            )
        return account_id

    def _mark_instagram_login_ok(self, account_id: int, *, detail: str = "Тестовая live-проверка входа выполнена.") -> None:
        self.db.update_account_instagram_launch_state(account_id, "login_submitted", detail)

    def _sign_payload(self, payload: dict) -> dict[str, object]:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")
        timestamp = str(int(time.time()))
        return {
            "body": body,
            "headers": {
                "X-Publish-Timestamp": timestamp,
                "X-Publish-Signature": self.app_module._publish_signature(timestamp, body),
                "Content-Type": "application/json",
            },
        }

    def _post_signed_callback(self, payload: dict):
        signed = self._sign_payload(payload)
        return self.client.post("/api/internal/publishing/n8n", content=signed["body"], headers=signed["headers"])

    def _batch_accounts(self, batch_id: int) -> list[dict]:
        return [dict(row) for row in self.db.list_publish_batch_accounts(batch_id)]

    def _progress_snapshot(self, batch_id: int) -> dict:
        response = self.client.get(f"/api/publishing/batches/{batch_id}/progress")
        self.assertEqual(response.status_code, 200)
        return response.json()

    def _wait_until(self, predicate, *, timeout: float = 2.0, interval: float = 0.02, message: str = "condition not reached") -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            if predicate():
                return
            time.sleep(interval)
        self.fail(message)

    def _create_audit_account(
        self,
        login: str,
        username: str,
        *,
        password: str = "pass123",
        serial: str = "",
        email: str = "",
        email_password: str = "",
    ) -> int:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login=login,
            account_password=password,
            username=username,
            email=email or f"{username}@example.com",
            email_password=email_password or "mailpass",
            proxy="",
            twofa="",
            instagram_emulator_serial=serial,
            default_link_name=f"Instagram @{username}",
        )
        return int(created["account_id"])

    def test_batch_create_triggers_n8n_webhook(self) -> None:
        acc1 = self._create_instagram_account("login1", "user1", "emulator-5554")
        acc2 = self._create_instagram_account("login2", "user2", "emulator-5556")

        class DummyResponse:
            status_code = 200
            text = "accepted"

            def raise_for_status(self) -> None:
                return None

        with patch.object(self.app_module.http_utils, "request_with_retry", return_value=DummyResponse()) as mocked:
            response = self.client.post(
                "/publishing/batches",
                data={"account_ids": [str(acc1), str(acc2)]},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            location = response.headers["location"]
            self.assertTrue(location.startswith("/publishing/batches/"))
            batch_id = int(location.rsplit("/", 1)[-1])
            self._wait_until(
                lambda: mocked.call_count == 1 and str(self.db.get_publish_batch(batch_id)["state"]) == "generating",
                message="runtime worker did not start n8n workflow",
            )

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["state"], "generating")
        self.assertEqual(batch["accounts_total"], 2)
        self.assertIn("n8n принял generation", batch["detail"])
        runtime_task = dict(self.db.get_runtime_task_for_entity("publish_batch_start", "publish_batch", batch_id))
        self.assertEqual(runtime_task["state"], "completed")

        mocked.assert_called_once()
        call_args = mocked.call_args
        self.assertEqual(call_args.args[0], "POST")
        self.assertEqual(call_args.args[1], os.environ["PUBLISH_N8N_WEBHOOK_URL"])
        payload = json.loads(call_args.kwargs["data"].decode("utf-8"))
        self.assertEqual(payload["batch_id"], batch_id)
        self.assertEqual(len(payload["accounts"]), 1)
        self.assertEqual(int(payload["accounts"][0]["account_id"]), acc1)
        self.assertEqual(payload["callback_url"], "http://testserver/api/internal/publishing/n8n")
        self.assertEqual(payload["progress_callback_url"], "http://testserver/api/internal/publishing/n8n")
        self.assertEqual(payload["factory_timeout_seconds"], 900)
        expected_defaults = self.app_module._publish_generator_defaults(batch_id, acc1)
        self.assertEqual(payload["generator_defaults"]["topic"], expected_defaults["topic"])
        self.assertEqual(payload["generator_defaults"]["style"], expected_defaults["style"])
        self.assertEqual(payload["generator_defaults"]["messagesCount"], expected_defaults["messagesCount"])
        self.assertFalse(payload["generator_defaults"]["async"])
        batch_accounts = self._batch_accounts(batch_id)
        self.assertEqual([int(row["account_id"]) for row in batch_accounts], [acc1, acc2])
        self.assertEqual([int(row["queue_position"]) for row in batch_accounts], [0, 1])
        self.assertEqual(batch_accounts[0]["state"], "generating")
        self.assertEqual(batch_accounts[1]["state"], "queued_for_generation")

    def test_batch_create_existing_video_mode_waits_for_uploaded_artifact(self) -> None:
        account_id = self._create_instagram_account("login-existing", "user-existing", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["state"], "queued_to_worker")
        self.assertIn("готового видео", batch["detail"])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)
        upload_payload = upload.json()
        self.assertTrue(upload_payload["ok"])
        self.assertEqual(upload_payload["jobs_created"], 1)
        self.assertTrue(Path(upload_payload["path"]).exists())

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["state"], "publishing")
        self.assertEqual(batch["jobs_total"], 1)
        batch_accounts = self._batch_accounts(batch_id)
        self.assertEqual(len(batch_accounts), 1)
        self.assertEqual(batch_accounts[0]["state"], "queued_for_publish")
        self.assertEqual(batch_accounts[0]["artifact_id"], upload_payload["artifact_id"])
        self.assertEqual(batch_accounts[0]["job_id"], upload_payload["job_ids"][0])

    def test_publish_job_lease_includes_mail_automation_fields(self) -> None:
        account_id = self._create_instagram_account("login-mail", "user-mail", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job = lease.json()["job"]

        self.assertTrue(job["mail_enabled"])
        self.assertEqual(job["mail_address"], "user-mail@example.com")
        self.assertEqual(job["mail_provider"], "auto")
        self.assertNotIn("email_password", job)
        self.assertNotIn("mail_auth_json", job)

    def test_instagram_audit_create_assigns_serials_and_marks_missing_credentials(self) -> None:
        acc_ok = self._create_audit_account("audit_ok", "audit_ok", serial="")
        acc_missing = self._create_audit_account("audit_missing", "audit_missing", password="", serial="")
        acc_default = self._create_audit_account("audit_default", "audit_default", serial="default")

        with (
            patch.object(
                self.app_module,
                "_fetch_helper_emulator_inventory",
                return_value={"ok": True, "available_serials": ["emulator-5554", "emulator-5556"], "state": {"flow_running": False}},
            ),
            patch.object(self.app_module, "_enqueue_instagram_audit_batch") as enqueue_mock,
        ):
            response = self.client.post(
                "/accounts/instagram/audits",
                data={"filter_type": "instagram"},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        location = response.headers["location"]
        self.assertTrue(location.startswith("/accounts/instagram/audits/"))
        batch_id = int(location.rsplit("/", 1)[-1])
        items = [dict(row) for row in self.db.list_instagram_audit_items(batch_id)]
        self.assertEqual(len(items), 3)

        by_account = {int(item["account_id"]): item for item in items}
        valid_serials = {"emulator-5554", "emulator-5556"}
        self.assertEqual(by_account[acc_ok]["item_state"], "queued")
        self.assertIn(by_account[acc_ok]["assigned_serial"], valid_serials)
        self.assertIn(str(self.db.get_account(acc_ok)["instagram_emulator_serial"]), valid_serials)
        self.assertEqual(str(self.db.get_account(acc_ok)["rotation_state"]), "working")

        self.assertEqual(by_account[acc_default]["item_state"], "queued")
        self.assertIn(by_account[acc_default]["assigned_serial"], valid_serials)
        self.assertIn(str(self.db.get_account(acc_default)["instagram_emulator_serial"]), valid_serials)

        self.assertEqual(by_account[acc_missing]["item_state"], "done")
        self.assertEqual(by_account[acc_missing]["resolution_state"], "missing_credentials")
        missing_account = dict(self.db.get_account(acc_missing))
        self.assertEqual(missing_account["rotation_state"], "not_working")
        self.assertEqual(missing_account["rotation_state_source"], "auto")
        self.assertIn("логин или пароль Instagram", missing_account["rotation_state_reason"])
        enqueue_mock.assert_called_once_with(batch_id)

    def test_instagram_audit_done_item_marks_account_not_working_with_reason(self) -> None:
        account_id = self._create_audit_account("audit_mail", "audit_mail", serial="emulator-5554", email_password="")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "queued",
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        item = dict(self.db.get_instagram_audit_item(batch_id, account_id))

        changed = self.db.update_instagram_audit_item(
            int(item["id"]),
            item_state="done",
            login_state="challenge_required",
            login_detail="Instagram запросил challenge.",
            mail_probe_state="not_configured",
            mail_probe_detail="Для IMAP-режима не заполнен пароль почты.",
            resolution_state="email_code_required",
            resolution_detail="Instagram запросил challenge. Для IMAP-режима не заполнен пароль почты.",
            completed_at=int(time.time()),
        )
        self.assertTrue(changed)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("не заполнен пароль почты", account["rotation_state_reason"])

    def test_instagram_audit_progress_snapshot_reports_email_code_resolution(self) -> None:
        account_id = self._create_audit_account("audit_progress", "audit_progress", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "done",
                    "login_state": "challenge_required",
                    "login_detail": "Instagram запросил challenge.",
                    "mail_probe_state": "ok",
                    "mail_probe_detail": "Найдено свежее письмо Instagram/Meta: Security code",
                    "resolution_state": "email_code_required",
                    "resolution_detail": "Нужен код из почты.",
                    "started_at": int(time.time()),
                    "completed_at": int(time.time()),
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])

        response = self.client.get(f"/api/accounts/instagram/audits/{batch_id}/progress")
        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertEqual(snapshot["batch"]["state"], "completed_with_errors")
        self.assertEqual(snapshot["batch"]["counts"]["email_code_required"], 1)
        self.assertEqual(snapshot["items"][0]["resolution_state"], "email_code_required")
        self.assertIn("почты", snapshot["items"][0]["detail"])

    def test_instagram_audit_done_phone_only_challenge_marks_account_not_working_with_reason(self) -> None:
        account_id = self._create_audit_account("audit_phone_only", "audit_phone_only", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "queued",
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        item = dict(self.db.get_instagram_audit_item(batch_id, account_id))

        changed = self.db.update_instagram_audit_item(
            int(item["id"]),
            item_state="done",
            login_state="challenge_required",
            login_detail="Instagram предлагает только phone/manual recovery без email-варианта.",
            mail_probe_state="not_required",
            mail_probe_detail="Проверка почты не нужна.",
            resolution_state="challenge_required",
            resolution_detail="Instagram предлагает только phone/manual recovery без email-варианта.",
            completed_at=int(time.time()),
        )
        self.assertTrue(changed)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("phone/manual recovery", account["rotation_state_reason"])

    def test_instagram_audit_progress_snapshot_drifts_forward_between_state_changes(self) -> None:
        account_id = self._create_audit_account("audit_live", "audit_live", serial="emulator-5554")
        now_ts = int(time.time())
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "launching",
                    "login_state": "",
                    "login_detail": "",
                    "mail_probe_state": "pending",
                    "mail_probe_detail": "",
                    "resolution_state": "",
                    "resolution_detail": "",
                    "started_at": now_ts,
                    "completed_at": None,
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])

        snapshot = self.client.get(f"/api/accounts/instagram/audits/{batch_id}/progress").json()
        self.assertEqual(snapshot["items"][0]["progress_pct"], 15)

        with patch.object(self.app_module.time, "time", return_value=now_ts + 18):
            drifted = self.client.get(f"/api/accounts/instagram/audits/{batch_id}/progress").json()

        self.assertGreater(drifted["items"][0]["progress_pct"], 15)
        self.assertLess(drifted["items"][0]["progress_pct"], 45)

    def test_instagram_audit_progress_snapshot_marks_failed_batch_as_error(self) -> None:
        account_id = self._create_audit_account("audit_failed", "audit_failed", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "launching",
                    "login_state": "",
                    "login_detail": "",
                    "mail_probe_state": "pending",
                    "mail_probe_detail": "",
                    "resolution_state": "",
                    "resolution_detail": "",
                    "started_at": int(time.time()),
                    "completed_at": None,
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        self.db.update_instagram_audit_batch_state(batch_id, "failed", detail="helper crashed", completed_at=int(time.time()))

        response = self.client.get(f"/api/accounts/instagram/audits/{batch_id}/progress")
        self.assertEqual(response.status_code, 200)
        snapshot = response.json()
        self.assertEqual(snapshot["batch"]["state"], "failed")
        self.assertEqual(snapshot["batch"]["phase_label"], "Ошибка")

    def test_instagram_audit_infrastructure_error_retries_instead_of_finalizing_item(self) -> None:
        account_id = self._create_audit_account("audit_retry", "audit_retry", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "queued",
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        self.db.create_or_reactivate_runtime_task(
            task_type="instagram_audit_batch_run",
            entity_type="instagram_audit_batch",
            entity_id=batch_id,
            payload={"audit_batch_id": batch_id},
            max_attempts=3,
        )

        with (
            patch.object(self.app_module, "_wait_for_helper_idle", return_value={"ok": True, "state": {"flow_running": False}}),
            patch.object(self.app_module.db, "create_helper_launch_ticket", return_value={"ticket": "ticket-1"}),
            patch.object(self.app_module, "_launch_instagram_helper_ticket", side_effect=RuntimeError("helper unavailable")),
        ):
            processed = self.app_module._run_runtime_task_once(worker_name="test-runtime")

        self.assertTrue(processed)
        task = dict(self.db.get_runtime_task_for_entity("instagram_audit_batch_run", "instagram_audit_batch", batch_id))
        self.assertEqual(task["state"], "retrying")
        item = dict(self.db.get_instagram_audit_item(batch_id, account_id))
        self.assertEqual(item["item_state"], "queued")
        self.assertEqual(item["resolution_state"], "")
        self.assertIn("helper unavailable", str(item["login_detail"]))
        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "working")

    def test_instagram_audit_retry_run_skips_already_completed_items(self) -> None:
        first_id = self._create_audit_account("audit_done", "audit_done", serial="emulator-5554")
        second_id = self._create_audit_account("audit_queue", "audit_queue", serial="emulator-5556")
        now_ts = int(time.time())
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": first_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "done",
                    "login_state": "login_submitted",
                    "login_detail": "Вход выполнен.",
                    "mail_probe_state": "not_required",
                    "mail_probe_detail": "Проверка почты не нужна.",
                    "resolution_state": "login_ok",
                    "resolution_detail": "Вход выполнен.",
                    "started_at": now_ts,
                    "completed_at": now_ts,
                },
                {
                    "account_id": second_id,
                    "queue_position": 1,
                    "assigned_serial": "emulator-5556",
                    "item_state": "queued",
                },
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        seen_ids: list[int] = []

        def _fake_run(batch: int, item: dict[str, object], *, heartbeat=None) -> None:
            self.assertEqual(batch, batch_id)
            seen_ids.append(int(item["account_id"]))

        with patch.object(self.app_module, "_run_instagram_audit_item", side_effect=_fake_run):
            self.app_module._run_instagram_audit_batch(batch_id)

        self.assertEqual(seen_ids, [second_id])

    def test_instagram_audit_exhausted_retry_finalizes_helper_error_without_not_working(self) -> None:
        account_id = self._create_audit_account("audit_helper_fail", "audit_helper_fail", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "queued",
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])
        self.db.create_or_reactivate_runtime_task(
            task_type="instagram_audit_batch_run",
            entity_type="instagram_audit_batch",
            entity_id=batch_id,
            payload={"audit_batch_id": batch_id},
            max_attempts=1,
        )

        with (
            patch.object(self.app_module, "_wait_for_helper_idle", return_value={"ok": True, "state": {"flow_running": False}}),
            patch.object(self.app_module.db, "create_helper_launch_ticket", return_value={"ticket": "ticket-1"}),
            patch.object(self.app_module, "_launch_instagram_helper_ticket", side_effect=RuntimeError("helper unavailable forever")),
        ):
            processed = self.app_module._run_runtime_task_once(worker_name="test-runtime")

        self.assertTrue(processed)
        task = dict(self.db.get_runtime_task_for_entity("instagram_audit_batch_run", "instagram_audit_batch", batch_id))
        self.assertEqual(task["state"], "failed")
        batch = dict(self.db.get_instagram_audit_batch(batch_id))
        self.assertEqual(batch["state"], "completed_with_errors")
        item = dict(self.db.get_instagram_audit_item(batch_id, account_id))
        self.assertEqual(item["item_state"], "done")
        self.assertEqual(item["resolution_state"], "helper_error")
        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "working")

    def test_artifact_ready_without_account_id_targets_only_current_generating_account(self) -> None:
        acc1 = self._create_instagram_account("same1", "same_user1", "emulator-5554")
        acc2 = self._create_instagram_account("same2", "same_user2", "emulator-5554")
        created = self.db.create_publish_batch([acc1, acc2], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])
        self.db.mark_publish_generation_started(batch_id, account_id=acc1, detail="factory started")

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        video_path = batch_dir / "video1.mp4"
        video_path.write_bytes(b"fake-video-content")

        artifact_resp = self._post_signed_callback({"event": "artifact_ready", "batch_id": batch_id, "path": "video1.mp4"})
        self.assertEqual(artifact_resp.status_code, 200)
        duplicate_resp = self._post_signed_callback({"event": "artifact_ready", "batch_id": batch_id, "path": "video1.mp4"})
        self.assertEqual(duplicate_resp.status_code, 200)

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["artifacts_total"], 1)
        self.assertEqual(batch["jobs_total"], 1)
        self.assertEqual(batch["state"], "publishing")
        batch_accounts = {int(row["account_id"]): row for row in self._batch_accounts(batch_id)}
        self.assertEqual(batch_accounts[acc1]["state"], "queued_for_publish")
        self.assertEqual(batch_accounts[acc2]["state"], "queued_for_generation")
        self.assertNotEqual(int(batch_accounts[acc1]["artifact_id"]), 0)
        self.assertIsNone(batch_accounts[acc2]["artifact_id"])

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease1 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease1.status_code, 200)
        job1 = lease1.json()["job"]
        download = self.client.get(f"/api/internal/publishing/jobs/{job1['id']}/artifact", headers=headers)
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.content, b"fake-video-content")
        lease2 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease2.status_code, 204)

        status1 = self.client.post(
            f"/api/internal/publishing/jobs/{job1['id']}/status",
            json={"state": "published", "detail": "ok", "last_file": "video1.mp4", "runner_name": "runner-1"},
            headers=headers,
        )
        self.assertEqual(status1.status_code, 200)
        lease2 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease2.status_code, 204)
        next_batch_accounts = {int(row["account_id"]): row for row in self._batch_accounts(batch_id)}
        self.assertEqual(next_batch_accounts[acc1]["state"], "published")
        self.assertEqual(next_batch_accounts[acc2]["state"], "queued_for_generation")

    def test_published_first_account_restarts_runtime_and_generates_second_account(self) -> None:
        acc1 = self._create_instagram_account("serial1", "serial_user1", "emulator-5554")
        acc2 = self._create_instagram_account("serial2", "serial_user2", "emulator-5556")

        class DummyResponse:
            status_code = 200
            text = "accepted"

            def raise_for_status(self) -> None:
                return None

        with patch.object(self.app_module.http_utils, "request_with_retry", return_value=DummyResponse()) as mocked:
            response = self.client.post(
                "/publishing/batches",
                data={"account_ids": [str(acc1), str(acc2)]},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            batch_id = int(response.headers["location"].rsplit("/", 1)[-1])
            self._wait_until(lambda: mocked.call_count == 1, message="first account generation did not start")

            batch_dir = Path(self.staging_dir) / str(batch_id)
            batch_dir.mkdir(parents=True, exist_ok=True)
            (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

            self.assertEqual(
                self._post_signed_callback({"event": "artifact_ready", "batch_id": batch_id, "path": "video1.mp4"}).status_code,
                200,
            )

            headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
            lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
            self.assertEqual(lease.status_code, 200)
            job_id = int(lease.json()["job"]["id"])

            status = self.client.post(
                f"/api/internal/publishing/jobs/{job_id}/status",
                json={"state": "published", "detail": "ok", "last_file": "video1.mp4", "runner_name": "runner-1"},
                headers=headers,
            )
            self.assertEqual(status.status_code, 200)

            self._wait_until(
                lambda: mocked.call_count == 2 and self.db.get_publish_batch_account_state(batch_id, acc2) == "generating",
                message="second account generation did not start after first publish finished",
            )

        second_payload = json.loads(mocked.call_args_list[1].kwargs["data"].decode("utf-8"))
        self.assertEqual(len(second_payload["accounts"]), 1)
        self.assertEqual(int(second_payload["accounts"][0]["account_id"]), acc2)

    def test_generation_failed_account_advances_batch_to_next_generation(self) -> None:
        acc1 = self._create_instagram_account("serial_fail1", "serial_fail_user1", "emulator-5554")
        acc2 = self._create_instagram_account("serial_fail2", "serial_fail_user2", "emulator-5556")

        class DummyResponse:
            status_code = 200
            text = "accepted"

            def raise_for_status(self) -> None:
                return None

        with patch.object(self.app_module.http_utils, "request_with_retry", return_value=DummyResponse()) as mocked:
            response = self.client.post(
                "/publishing/batches",
                data={"account_ids": [str(acc1), str(acc2)]},
                follow_redirects=False,
            )
            self.assertEqual(response.status_code, 303)
            batch_id = int(response.headers["location"].rsplit("/", 1)[-1])
            self._wait_until(lambda: mocked.call_count == 1, message="first account generation did not start")

            failed = self._post_signed_callback(
                {"event": "generation_failed", "batch_id": batch_id, "account_id": acc1, "detail": "factory error"}
            )
            self.assertEqual(failed.status_code, 200)

            self._wait_until(
                lambda: mocked.call_count == 2 and self.db.get_publish_batch_account_state(batch_id, acc2) == "generating",
                message="second account generation did not start after first generation failure",
            )

        second_payload = json.loads(mocked.call_args_list[1].kwargs["data"].decode("utf-8"))
        self.assertEqual(int(second_payload["accounts"][0]["account_id"]), acc2)

    def test_generation_failed_callback_persists_diagnostic_payload(self) -> None:
        account_id = self._create_instagram_account("diag_fail", "diag_fail_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        response = self._post_signed_callback(
            {
                "event": "generation_failed",
                "batch_id": batch_id,
                "account_id": account_id,
                "detail": "factory error",
                "error_code": "DIALOG_INVALID_AFTER_RETRY",
                "raw_preview": '{"messages":[{"sender":"Кирилл"',
                "fixed_preview": '{"messages":[]}',
                "parsed_keys": "messages",
                "factory_response_preview": '{"error":"DIALOG_INVALID_AFTER_RETRY: JSON_PARSE_FAILED"}',
            }
        )
        self.assertEqual(response.status_code, 200)

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["state"], "failed_generation")
        self.assertEqual(batch["detail"], "factory error")

        account = self._batch_accounts(batch_id)[0]
        self.assertEqual(account["state"], "generation_failed")
        self.assertEqual(account["detail"], "factory error")

        event = dict(self.db.list_publish_job_events(batch_id, limit=1)[0])
        self.assertEqual(event["state"], "generation_failed")
        payload = json.loads(event["payload_json"])
        self.assertEqual(payload["account_id"], account_id)
        self.assertEqual(payload["error_code"], "DIALOG_INVALID_AFTER_RETRY")
        self.assertEqual(payload["raw_preview"], '{"messages":[{"sender":"Кирилл"')
        self.assertEqual(payload["fixed_preview"], '{"messages":[]}')
        self.assertEqual(payload["parsed_keys"], "messages")
        self.assertEqual(
            payload["factory_response_preview"],
            '{"error":"DIALOG_INVALID_AFTER_RETRY: JSON_PARSE_FAILED"}',
        )

    def test_enqueue_instagram_audit_batch_runs_via_runtime_worker(self) -> None:
        account_id = self._create_audit_account("audit_runtime", "audit_runtime", serial="emulator-5554")
        created = self.db.create_instagram_audit_batch(
            [
                {
                    "account_id": account_id,
                    "queue_position": 0,
                    "assigned_serial": "emulator-5554",
                    "item_state": "queued",
                    "login_state": "",
                    "login_detail": "",
                    "mail_probe_state": "pending",
                    "mail_probe_detail": "",
                    "resolution_state": "",
                    "resolution_detail": "",
                    "started_at": None,
                    "completed_at": None,
                }
            ],
            created_by_admin="admin",
        )
        batch_id = int(created["batch_id"])

        with patch.object(self.app_module, "_run_instagram_audit_batch", return_value=None) as run_mock:
            self.app_module._enqueue_instagram_audit_batch(batch_id)
            self._wait_until(
                lambda: run_mock.call_count == 1,
                message="runtime worker did not execute instagram audit batch",
            )

        runtime_task = dict(self.db.get_runtime_task_for_entity("instagram_audit_batch_run", "instagram_audit_batch", batch_id))
        self.assertEqual(runtime_task["state"], "completed")

    def test_authenticated_batch_artifact_download_returns_workflow_file(self) -> None:
        account_id = self._create_instagram_account("download1", "download_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        video_path = batch_dir / "workflow-video.mp4"
        video_path.write_bytes(b"workflow-video-content")

        response = self._post_signed_callback(
            {
                "event": "artifact_ready",
                "batch_id": batch_id,
                "account_id": account_id,
                "path": str(video_path),
                "filename": "workflow-video.mp4",
            }
        )
        self.assertEqual(response.status_code, 200)
        artifact_id = int(response.json()["artifact_id"])

        download = self.client.get(f"/publishing/batches/{batch_id}/artifacts/{artifact_id}/download")
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.content, b"workflow-video-content")
        self.assertIn("workflow-video.mp4", download.headers.get("content-disposition", ""))

    def test_artifact_ready_rejects_file_from_another_batch_directory(self) -> None:
        account_id = self._create_instagram_account("download2", "cross_batch_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        other_batch_dir = Path(self.staging_dir) / "999"
        other_batch_dir.mkdir(parents=True, exist_ok=True)
        foreign_video = other_batch_dir / "foreign-video.mp4"
        foreign_video.write_bytes(b"foreign-video-content")

        response = self._post_signed_callback(
            {
                "event": "artifact_ready",
                "batch_id": batch_id,
                "account_id": account_id,
                "path": str(foreign_video),
                "filename": "foreign-video.mp4",
            }
        )

        self.assertEqual(response.status_code, 400)
        self.assertIn("Artifact path must stay inside publish staging dir", response.text)

    def test_runner_artifact_download_rejects_source_path_outside_own_batch_directory(self) -> None:
        account_id = self._create_instagram_account("download3", "runner_guard_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        good_video = batch_dir / "runner-video.mp4"
        good_video.write_bytes(b"runner-video-content")

        artifact_resp = self._post_signed_callback(
            {
                "event": "artifact_ready",
                "batch_id": batch_id,
                "account_id": account_id,
                "path": str(good_video),
                "filename": "runner-video.mp4",
            }
        )
        self.assertEqual(artifact_resp.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        foreign_batch_dir = Path(self.staging_dir) / "1000"
        foreign_batch_dir.mkdir(parents=True, exist_ok=True)
        foreign_video = foreign_batch_dir / "foreign-runner-video.mp4"
        foreign_video.write_bytes(b"foreign-runner-video-content")

        conn = self.db._connect()
        cur = conn.cursor()
        cur.execute("UPDATE publish_jobs SET source_path = ? WHERE id = ?", (str(foreign_video), job_id))
        conn.commit()
        conn.close()

        download = self.client.get(f"/api/internal/publishing/jobs/{job_id}/artifact", headers=headers)
        self.assertEqual(download.status_code, 400)
        self.assertIn("Artifact path must stay inside publish staging dir", download.text)

    def test_batch_detail_renders_live_dashboard_without_full_reload(self) -> None:
        account_id = self._create_instagram_account("dash1", "dash_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        page = self.client.get(f"/publishing/batches/{batch_id}")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Статус пакета", page.text)
        self.assertIn('id="dashboard-progress-fill"', page.text)
        self.assertIn('id="dashboard-steps"', page.text)
        self.assertIn('id="dashboard-accounts"', page.text)
        self.assertIn("Последние события", page.text)
        self.assertIn("журнал событий", page.text)
        self.assertNotIn("window.location.reload()", page.text)

    def test_generation_progress_callback_updates_progress_snapshot(self) -> None:
        account_id = self._create_instagram_account("tele1", "telemetry_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        progress = self._post_signed_callback(
            {
                "event": "generation_progress",
                "batch_id": batch_id,
                "account_id": account_id,
                "stage_key": "image_generation",
                "stage_label": "Генерация изображений",
                "progress_pct": 40,
                "detail": "Собрано 4/10 изображений",
                "meta": {"images_ready": 4},
            }
        )
        self.assertEqual(progress.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        self.assertEqual(snapshot["batch"]["phase_label"], "Генерация видео")
        self.assertIn("Генерация изображений", snapshot["batch"]["phase_subtitle"])
        self.assertEqual(snapshot["accounts"][0]["phase_label"], "Генерация изображений")
        self.assertEqual(snapshot["accounts"][0]["progress_pct"], 25)
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Генерация изображений")

        events = [dict(row) for row in self.db.list_publish_job_events(batch_id, limit=10)]
        self.assertEqual(events[0]["state"], "generation_progress")

    def test_generation_progress_snapshot_drifts_forward_between_callbacks(self) -> None:
        account_id = self._create_instagram_account("tele_live", "telemetry_live", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])
        now_ts = int(time.time())

        with patch.object(self.app_module.time, "time", return_value=now_ts):
            response = self._post_signed_callback(
                {
                    "event": "generation_progress",
                    "batch_id": batch_id,
                    "account_id": account_id,
                    "stage_key": "image_generation",
                    "stage_label": "Генерация изображений",
                    "progress_pct": 40,
                    "detail": "Собрано 4/10 изображений",
                }
            )
        self.assertEqual(response.status_code, 200)

        baseline = self._progress_snapshot(batch_id)
        self.assertEqual(baseline["accounts"][0]["progress_pct"], 25)

        with patch.object(self.app_module.time, "time", return_value=now_ts + 18):
            drifted = self._progress_snapshot(batch_id)

        self.assertGreater(drifted["accounts"][0]["progress_pct"], 25)
        self.assertLess(drifted["accounts"][0]["progress_pct"], 60)

    def test_progress_snapshot_falls_back_to_coarse_video_generation_without_telemetry(self) -> None:
        account_id = self._create_instagram_account("tele2", "coarse_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        response = self._post_signed_callback(
            {"event": "generation_started", "batch_id": batch_id, "account_id": account_id, "detail": "factory started"}
        )
        self.assertEqual(response.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        self.assertEqual(snapshot["accounts"][0]["phase_label"], "Генерация видео")
        self.assertEqual(snapshot["accounts"][0]["progress_pct"], 20)
        self.assertEqual(snapshot["batch"]["phase_label"], "Генерация видео")

    def test_progress_snapshot_moves_monotonically_through_publish_states(self) -> None:
        account_id = self._create_instagram_account("tele3", "progress_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        artifact_resp = self._post_signed_callback(
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"}
        )
        self.assertEqual(artifact_resp.status_code, 200)
        self.assertEqual(self._post_signed_callback({"event": "generation_completed", "batch_id": batch_id}).status_code, 200)

        progress_values = [self._progress_snapshot(batch_id)["accounts"][0]["progress_pct"]]
        self.assertEqual(progress_values[-1], 60)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])
        progress_values.append(self._progress_snapshot(batch_id)["accounts"][0]["progress_pct"])

        for state, expected in (
            ("importing_media", 78),
            ("selecting_media", 90),
            ("publishing", 96),
            ("published", 100),
        ):
            status = self.client.post(
                f"/api/internal/publishing/jobs/{job_id}/status",
                json={"state": state, "detail": state, "last_file": "video1.mp4", "runner_name": "runner-1"},
                headers=headers,
            )
            self.assertEqual(status.status_code, 200)
            progress_values.append(self._progress_snapshot(batch_id)["accounts"][0]["progress_pct"])
            self.assertEqual(progress_values[-1], expected)

        self.assertEqual(progress_values, sorted(progress_values))

    def test_duplicate_generation_progress_is_ignored(self) -> None:
        account_id = self._create_instagram_account("tele4", "dupe_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])
        payload = {
            "event": "generation_progress",
            "batch_id": batch_id,
            "account_id": account_id,
            "stage_key": "image_generation",
            "stage_label": "Генерация изображений",
            "progress_pct": 40,
            "detail": "Собрано 4/10 изображений",
        }
        first = self._post_signed_callback(payload)
        second = self._post_signed_callback(payload)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)

        events = [dict(row) for row in self.db.list_publish_job_events(batch_id, limit=20)]
        progress_events = [event for event in events if event.get("state") == "generation_progress"]
        self.assertEqual(len(progress_events), 1)

    def test_generation_timeout_watchdog_marks_failed(self) -> None:
        account_id = self._create_instagram_account("tele5", "timeout_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        started = self._post_signed_callback(
            {"event": "generation_started", "batch_id": batch_id, "account_id": account_id, "detail": "started"}
        )
        self.assertEqual(started.status_code, 200)

        conn = self.db._connect()
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE publish_batch_accounts
            SET updated_at = ?
            WHERE batch_id = ? AND account_id = ?
            """,
            (int(time.time()) - 120, batch_id, account_id),
        )
        conn.commit()
        conn.close()

        self.app_module.PUBLISH_FACTORY_TIMEOUT_SECONDS = 1
        snapshot = self._progress_snapshot(batch_id)
        self.assertEqual(snapshot["accounts"][0]["batch_state"], "generation_failed")

    def test_progress_snapshot_aggregates_mixed_accounts_and_terminal_errors(self) -> None:
        acc1 = self._create_instagram_account("tele4", "mix_user1", "emulator-5554")
        acc2 = self._create_instagram_account("tele5", "mix_user2", "emulator-5556")
        created = self.db.create_publish_batch([acc1, acc2], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        self.assertEqual(
            self._post_signed_callback({"event": "artifact_ready", "batch_id": batch_id, "account_id": acc1, "path": "video1.mp4"}).status_code,
            200,
        )
        self.assertEqual(
            self._post_signed_callback({"event": "generation_failed", "batch_id": batch_id, "account_id": acc2, "detail": "factory error"}).status_code,
            200,
        )
        self.assertEqual(self._post_signed_callback({"event": "generation_completed", "batch_id": batch_id}).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])
        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={"state": "published", "detail": "done", "last_file": "video1.mp4", "runner_name": "runner-1"},
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        self.assertEqual(snapshot["batch"]["state"], "completed_with_errors")
        self.assertEqual(snapshot["batch"]["phase_label"], "Готово с ошибками")
        self.assertEqual(snapshot["batch"]["counts"]["published"], 1)
        self.assertEqual(snapshot["batch"]["counts"]["failed"], 1)
        account_cards = {int(item["id"]): item for item in snapshot["accounts"]}
        self.assertEqual(account_cards[acc1]["progress_pct"], 100)
        self.assertEqual(account_cards[acc2]["phase_label"], "Ошибка генерации")

    def test_invalid_publish_signature_is_rejected(self) -> None:
        created = self.db.create_publish_batch(
            [self._create_instagram_account("sig1", "siguser1", "emulator-5554")],
            created_by_admin="admin",
            workflow_key="default",
        )
        batch_id = int(created["batch_id"])
        response = self.client.post(
            "/api/internal/publishing/n8n",
            content=json.dumps({"event": "generation_started", "batch_id": batch_id}).encode("utf-8"),
            headers={
                "X-Publish-Timestamp": str(int(time.time())),
                "X-Publish-Signature": "bad-signature",
                "Content-Type": "application/json",
            },
        )
        self.assertEqual(response.status_code, 401)

    def test_account_scoped_callbacks_publish_one_account_and_keep_other_generation_failure_local(self) -> None:
        acc1 = self._create_instagram_account("acc1", "batch_user1", "emulator-5554")
        acc2 = self._create_instagram_account("acc2", "batch_user2", "emulator-5556")
        created = self.db.create_publish_batch([acc1, acc2], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        self.assertEqual(
            self._post_signed_callback({"event": "generation_started", "batch_id": batch_id, "account_id": acc1, "detail": "factory started"}).status_code,
            200,
        )
        artifact_resp = self._post_signed_callback({"event": "artifact_ready", "batch_id": batch_id, "account_id": acc1, "path": "video1.mp4"})
        self.assertEqual(artifact_resp.status_code, 200)
        self.assertEqual(artifact_resp.json()["jobs_created"], 1)
        self.assertEqual(self._post_signed_callback({"event": "generation_failed", "batch_id": batch_id, "account_id": acc2, "detail": "factory error"}).status_code, 200)
        self.assertEqual(self._post_signed_callback({"event": "generation_completed", "batch_id": batch_id}).status_code, 200)

        mid_batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(mid_batch["jobs_total"], 1)
        self.assertEqual(mid_batch["state"], "publishing")
        batch_accounts = {int(row["account_id"]): row for row in self._batch_accounts(batch_id)}
        self.assertEqual(batch_accounts[acc1]["state"], "queued_for_publish")
        self.assertEqual(batch_accounts[acc2]["state"], "generation_failed")

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={"state": "published", "detail": "ok", "last_file": "video1.mp4", "runner_name": "runner-1"},
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        final_batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(final_batch["state"], "completed_with_errors")
        self.assertEqual(final_batch["published_accounts"], 1)
        self.assertEqual(final_batch["generation_failed_accounts"], 1)
        self.assertFalse(batch_dir.exists())

    def test_accounts_without_twofa_stay_ready_with_warning(self) -> None:
        no_twofa_id = self._create_instagram_account("blocked1", "blocked_user", "emulator-5554", twofa="")
        ready_id = self._create_instagram_account("ready1", "ready_user", "emulator-5556")

        ready_ids = {int(row["id"]) for row in self.db.list_publish_ready_accounts()}
        blocked_ids = {int(row["id"]) for row in self.db.list_publish_blocked_accounts()}

        self.assertEqual(ready_ids, {no_twofa_id, ready_id})
        self.assertEqual(blocked_ids, set())

        created = self.db.create_publish_batch([no_twofa_id], created_by_admin="admin", workflow_key="default")
        self.assertGreater(int(created["batch_id"]), 0)

        page = self.client.get("/publishing/start")
        self.assertEqual(page.status_code, 200)
        self.assertIn("blocked_user", page.text)
        self.assertIn("2FA не заполнен", page.text)

    def test_accounts_without_mail_automation_stay_ready_and_confirm_page_shows_warning(self) -> None:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login="mail_warn_login",
            account_password="pass123",
            username="mail_warn_user",
            email="mail_warn_user@example.com",
            email_password="",
            proxy="",
            twofa="JBSWY3DPEHPK3PXP",
            instagram_emulator_serial="emulator-5554",
            default_link_name="Instagram @mail_warn_user",
        )
        account_id = int(created["account_id"])
        self._mark_instagram_login_ok(account_id)

        ready_ids = {int(row["id"]) for row in self.db.list_publish_ready_accounts()}
        blocked_ids = {int(row["id"]) for row in self.db.list_publish_blocked_accounts()}
        self.assertIn(account_id, ready_ids)
        self.assertNotIn(account_id, blocked_ids)

        start_page = self.client.get("/publishing/start")
        self.assertEqual(start_page.status_code, 200)
        self.assertIn("mail_warn_user", start_page.text)
        self.assertIn("Почта не готова для auto-code", start_page.text)
        self.assertNotIn("Для IMAP-режима не заполнен пароль почты.", start_page.text)

        confirm_page = self.client.post(
            "/publishing/prepare",
            data={"account_ids": [str(account_id)]},
        )
        self.assertEqual(confirm_page.status_code, 200)
        self.assertIn("mail_warn_user", confirm_page.text)
        self.assertIn("Mail: Auto / IMAP", confirm_page.text)
        self.assertIn("Не готова", confirm_page.text)
        self.assertIn("Почта не готова для auto-code", confirm_page.text)
        self.assertIn("Для IMAP-режима не заполнен пароль почты.", confirm_page.text)

    def test_placeholder_mail_values_are_treated_as_missing_mail(self) -> None:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login="mail_placeholder_login",
            account_password="pass123",
            username="mail_placeholder_user",
            email="NO_EMAIL",
            email_password="NO_EMAIL",
            proxy="",
            twofa="JBSWY3DPEHPK3PXP",
            instagram_emulator_serial="emulator-5554",
            default_link_name="Instagram @mail_placeholder_user",
        )
        account_id = int(created["account_id"])
        self._mark_instagram_login_ok(account_id)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["email"], "")
        self.assertEqual(account["email_password"], "")
        self.assertFalse(self.db.account_mail_automation_ready(account))

        start_page = self.client.get("/publishing/start")
        self.assertEqual(start_page.status_code, 200)
        self.assertIn("mail_placeholder_user", start_page.text)
        self.assertNotIn("Почта не задана", start_page.text)
        self.assertNotIn("Не заполнен email аккаунта.", start_page.text)
        self.assertIn("Почта не готова для auto-code", start_page.text)

    def test_publishing_start_requires_confirmed_instagram_login(self) -> None:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login="no_live_check_login",
            account_password="pass123",
            username="no_live_check_user",
            email="no_live_check_user@example.com",
            email_password="mailpass",
            proxy="",
            twofa="JBSWY3DPEHPK3PXP",
            instagram_emulator_serial="emulator-5554",
            default_link_name="Instagram @no_live_check_user",
        )
        account_id = int(created["account_id"])

        ready_ids = {int(row["id"]) for row in self.db.list_publish_ready_accounts()}
        self.assertIn(account_id, ready_ids)

        start_page = self.client.get("/publishing/start")
        self.assertEqual(start_page.status_code, 200)
        self.assertNotIn("@no_live_check_user</label>", start_page.text)
        self.assertIn("no_live_check_user", start_page.text)
        self.assertIn("Причина: Нет проверки", start_page.text)

    def test_publishing_start_auto_assigns_missing_serial_from_helper_inventory(self) -> None:
        account_id = self._create_instagram_account("auto_serial_1", "auto_serial_1", "", twofa="")

        with patch.object(
            self.app_module,
            "_fetch_helper_emulator_inventory",
            return_value={"ok": True, "available_serials": ["emulator-5554", "emulator-5556"], "state": {"flow_running": False}},
        ):
            page = self.client.get("/publishing/start")

        self.assertEqual(page.status_code, 200)
        self.assertEqual(str(self.db.get_account(account_id)["instagram_emulator_serial"]), "emulator-5554")
        self.assertIn("auto_serial_1", page.text)
        self.assertIn("Готово к запуску: 0", page.text)
        self.assertIn("Причина: Нет проверки", page.text)

    def test_publishing_start_falls_back_to_default_serial_when_helper_unavailable(self) -> None:
        account_id = self._create_instagram_account("auto_default_1", "auto_default_1", "", twofa="")

        with patch.object(self.app_module, "_fetch_helper_emulator_inventory", side_effect=RuntimeError("helper unavailable")):
            page = self.client.get("/publishing/start")

        self.assertEqual(page.status_code, 200)
        self.assertEqual(str(self.db.get_account(account_id)["instagram_emulator_serial"]), "default")
        self.assertIn("Причина: Нет проверки", page.text)
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        self.assertGreater(int(created["batch_id"]), 0)

    def test_admin_account_create_auto_assigns_real_serial_when_blank(self) -> None:
        with patch.object(
            self.app_module,
            "_fetch_helper_emulator_inventory",
            return_value={"ok": True, "available_serials": ["emulator-5554", "emulator-5556"], "state": {"flow_running": False}},
        ):
            response = self.client.post(
                "/accounts",
                data={
                    "type": "instagram",
                    "account_login": "created_auto_serial",
                    "account_password": "pass123",
                    "username": "created_auto_serial",
                    "email": "created_auto_serial@example.com",
                    "email_password": "mailpass",
                    "twofa": "",
                    "rotation_state": "review",
                    "views_state": "unknown",
                },
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        duplicate = dict(self.db.find_duplicate_account("instagram", "created_auto_serial"))
        account = dict(self.db.get_account(int(duplicate["id"])))
        self.assertEqual(account["instagram_emulator_serial"], "emulator-5554")

    def test_publishing_start_page_renders_one_click_checkbox_selection(self) -> None:
        ready_id = self._create_instagram_account("ready3", "ready_checkbox", "emulator-5554")

        page = self.client.get("/publishing/start")
        self.assertEqual(page.status_code, 200)
        self.assertIn('action="/publishing/batches"', page.text)
        self.assertRegex(page.text, rf'id="publish-account-{ready_id}"\s+name="account_ids"\s+value="{ready_id}"')
        self.assertIn('type="checkbox"', page.text)
        self.assertIsNone(re.search(rf'id="publish-account-{ready_id}"[^>]*checked', page.text, re.S))
        self.assertIn("Выбрать все", page.text)
        self.assertIn("Снять все", page.text)
        self.assertIn('Выбрано: <span id="selected-count">0</span>', page.text)
        self.assertIn("Готово к запуску: 1", page.text)
        self.assertIn("Запустить", page.text)
        self.assertIn('id="publish-start-btn"', page.text)
        self.assertIn("disabled", page.text)

    def test_publishing_prepare_preserves_selected_account_order(self) -> None:
        first_id = self._create_instagram_account("order1", "order_alpha", "emulator-5554")
        second_id = self._create_instagram_account("order2", "order_beta", "emulator-5556")

        page = self.client.post(
            "/publishing/prepare",
            data={"account_ids": [str(second_id), str(first_id)]},
        )
        self.assertEqual(page.status_code, 200)

        second_pos = page.text.index("@order_beta")
        first_pos = page.text.index("@order_alpha")
        self.assertLess(second_pos, first_pos)
        hidden_values = re.findall(r'name="account_ids" value="(\d+)"', page.text)
        self.assertEqual(hidden_values, [str(second_id), str(first_id)])

    def test_publishing_start_page_disables_publish_when_no_ready_accounts(self) -> None:
        self.db.create_account_with_default_link(
            account_type="instagram",
            account_login="blocked3",
            account_password="",
            username="blocked_publish",
            email="blocked_publish@example.com",
            email_password="mailpass",
            proxy="",
            twofa="",
            instagram_emulator_serial="default",
            default_link_name="Instagram @blocked_publish",
        )

        page = self.client.get("/publishing/start")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Нет готовых аккаунтов", page.text)
        self.assertIn("Запустить", page.text)
        self.assertIn("disabled", page.text)

    def test_batch_is_not_created_when_n8n_webhook_missing(self) -> None:
        account_id = self._create_instagram_account("ready4", "ready_without_n8n", "emulator-5554")
        previous_value = os.environ.get("PUBLISH_N8N_WEBHOOK_URL")
        os.environ["PUBLISH_N8N_WEBHOOK_URL"] = ""
        try:
            import app as app_module

            self.app_module = importlib.reload(app_module)
            self.client.close()
            self.client = TestClient(self.app_module.app)
            login = self.client.post(
                "/login",
                data={"username": os.environ["ADMIN_USER"], "password": os.environ["ADMIN_PASS"]},
                follow_redirects=False,
            )
            self.assertEqual(login.status_code, 303)

            before = self.db.list_publish_batches(limit=20)
            response = self.client.post("/publishing/batches", data={"account_ids": [str(account_id)]})
            after = self.db.list_publish_batches(limit=20)
        finally:
            if previous_value is None:
                os.environ.pop("PUBLISH_N8N_WEBHOOK_URL", None)
            else:
                os.environ["PUBLISH_N8N_WEBHOOK_URL"] = previous_value
            import app as app_module

            self.app_module = importlib.reload(app_module)
            self.client.close()
            self.client = TestClient(self.app_module.app)
            login = self.client.post(
                "/login",
                data={"username": os.environ["ADMIN_USER"], "password": os.environ["ADMIN_PASS"]},
                follow_redirects=False,
            )
            self.assertEqual(login.status_code, 303)

        self.assertEqual(response.status_code, 503)
        self.assertIn("PUBLISH_N8N_WEBHOOK_URL не настроен", response.text)
        self.assertEqual(len(after), len(before))

    def test_accounts_missing_required_fields_stay_blocked(self) -> None:
        blocked_id = self._create_instagram_account("blocked2", "blocked_required", "")
        ready_id = self._create_instagram_account("ready2", "ready_required", "emulator-5554")

        ready_ids = {int(row["id"]) for row in self.db.list_publish_ready_accounts()}
        blocked_ids = {int(row["id"]) for row in self.db.list_publish_blocked_accounts()}

        self.assertIn(ready_id, ready_ids)
        self.assertNotIn(blocked_id, ready_ids)
        self.assertIn(blocked_id, blocked_ids)

        with self.assertRaisesRegex(ValueError, "account .* not ready"):
            self.db.create_publish_batch([blocked_id], created_by_admin="admin", workflow_key="default")

    def test_not_working_accounts_stay_blocked_from_publish(self) -> None:
        blocked_id = self._create_instagram_account(
            "blocked-state",
            "blocked_state",
            "emulator-5554",
            rotation_state="not_working",
        )
        ready_id = self._create_instagram_account("ready-state", "ready_state", "emulator-5556")

        ready_ids = {int(row["id"]) for row in self.db.list_publish_ready_accounts()}
        blocked_ids = {int(row["id"]) for row in self.db.list_publish_blocked_accounts()}

        self.assertIn(ready_id, ready_ids)
        self.assertNotIn(blocked_id, ready_ids)
        self.assertIn(blocked_id, blocked_ids)

        page = self.client.get("/publishing/start")
        self.assertEqual(page.status_code, 200)
        self.assertIn("@ready_state", page.text)
        self.assertNotIn("@blocked_state</label>", page.text)
        self.assertIn("@blocked_state", page.text)
        self.assertIn("Причина: Нерабочий", page.text)

        with self.assertRaisesRegex(ValueError, "account .* not ready"):
            self.db.create_publish_batch([blocked_id], created_by_admin="admin", workflow_key="default")

    def test_manual_not_working_lock_is_preserved_after_success_status(self) -> None:
        account_id = self._create_instagram_account(
            "manual-lock",
            "manual_lock",
            "emulator-5554",
            rotation_state="not_working",
        )

        changed = self.db.update_account_instagram_publish_state(
            account_id,
            "published",
            "Публикация завершилась успешно.",
            last_file="ready.mp4",
        )
        self.assertTrue(changed)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "manual")

    def test_live_failure_is_not_overridden_by_config_fallback_on_resync(self) -> None:
        account_id = self._create_instagram_account("livefail1", "live_fail_user", "emulator-5554")

        changed = self.db.update_account_instagram_launch_state(
            account_id,
            "challenge_required",
            "Instagram запросил challenge.",
        )
        self.assertTrue(changed)

        changed_count = self.db.sync_instagram_auto_rotation_states(account_ids=[account_id])
        self.assertEqual(changed_count, 0)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("challenge", account["rotation_state_reason"].lower())

    def test_publish_ready_account_is_auto_marked_working_on_create(self) -> None:
        account_id = self._create_instagram_account("ready-auto", "ready_auto", "emulator-5554", twofa="")

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertEqual(account["rotation_state_reason"], "")

    def test_account_config_fix_promotes_auto_status_back_to_working(self) -> None:
        account_id = self._create_audit_account("fixme", "fix_me_user", serial="")

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("Instagram emulator serial", account["rotation_state_reason"])

        changed = self.db.update_account(
            account_id,
            account["type"],
            account["account_login"],
            account["account_password"],
            account["username"],
            account["email"],
            account["email_password"],
            account["proxy"],
            account["twofa"],
            account["mail_provider"],
            account["mail_auth_json"],
            "review",
            account["views_state"],
            account["owner_worker_id"],
            "emulator-5554",
        )
        self.assertTrue(changed)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["rotation_state"], "working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertEqual(account["rotation_state_reason"], "")

    def test_non_instagram_account_keeps_manual_rotation_state(self) -> None:
        account_id = self.db.create_account(
            "tiktok",
            "tt_login",
            "pass123",
            "tt_user",
            "tt@example.com",
            "mailpass",
            "",
            "",
            rotation_state="review",
        )

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["type"], "tiktok")
        self.assertEqual(account["rotation_state"], "review")
        self.assertEqual(account["rotation_state_source"], "manual")
        self.assertEqual(account["rotation_state_reason"], "")

    def test_failed_job_can_set_specific_account_publish_status(self) -> None:
        account_id = self._create_instagram_account("fail1", "fail_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            response = self._post_signed_callback(payload)
            self.assertEqual(response.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "failed",
                "detail": "invalid password from helper",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "account_publish_state": "invalid_password",
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        account = dict(self.db.get_account(account_id))
        batch = dict(self.db.get_publish_batch(batch_id))
        batch_account = self._batch_accounts(batch_id)[0]
        self.assertEqual(account["instagram_publish_status"], "invalid_password")
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("invalid password", account["rotation_state_reason"])
        self.assertEqual(batch_account["state"], "failed")
        self.assertEqual(batch["state"], "completed_with_errors")

    def test_publish_job_mail_challenge_progress_persists_into_snapshot(self) -> None:
        account_id = self._create_instagram_account("mailprogress1", "mail_progress_user", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "preparing",
                "detail": "Код из письма введён автоматически.",
                "last_file": "ready.mp4",
                "runner_name": "runner-1",
                "publish_phase": "clean_login",
                "event_kind": "mail_code_applied",
                "elapsed_seconds": 12,
                "mail_challenge": {
                    "status": "resolved",
                    "kind": "numeric_code",
                    "reason_code": "mail_code_applied",
                    "reason_text": "Код из письма введён автоматически.",
                    "masked_code": "123***",
                    "confidence": 0.92,
                },
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["mail_challenge_status"], "resolved")
        self.assertEqual(account["mail_challenge_kind"], "numeric_code")
        self.assertEqual(account["mail_challenge_reason_code"], "mail_code_applied")
        self.assertEqual(account["mail_challenge_masked_code"], "123***")

        snapshot = self._progress_snapshot(batch_id)
        account_card = snapshot["accounts"][0]
        self.assertEqual(account_card["phase_label"], "Код из письма введён")
        self.assertIn("Код: 123***", account_card["phase_detail"])
        self.assertIn("Уверенность: 92%", account_card["phase_detail"])
        self.assertEqual(account_card["mail_provider_label"], "Auto / IMAP")
        self.assertEqual(account_card["mail_ready_label"], "Готова")
        self.assertEqual(account_card["mail_challenge_status"], "resolved")
        self.assertEqual(account_card["mail_challenge_kind_label"], "Код из письма")
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Код из письма введён")
        self.assertIn("Код: 123***", snapshot["recent_activity"][0]["detail"])

    def test_publish_job_approval_link_progress_persists_into_snapshot(self) -> None:
        account_id = self._create_instagram_account("maillink1", "mail_link_user", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "preparing",
                "detail": "Ссылка подтверждения открыта автоматически, Instagram подтвердил вход.",
                "last_file": "ready.mp4",
                "runner_name": "runner-1",
                "publish_phase": "clean_login",
                "event_kind": "approval_link_applied",
                "elapsed_seconds": 19,
                "mail_challenge": {
                    "status": "resolved",
                    "kind": "approval_link",
                    "reason_code": "approval_link_applied",
                    "reason_text": "Ссылка подтверждения открыта автоматически, Instagram подтвердил вход.",
                    "confidence": 0.76,
                },
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        account_card = snapshot["accounts"][0]
        self.assertEqual(account_card["phase_label"], "Ссылка из письма применена")
        self.assertIn("Instagram подтвердил вход", account_card["phase_detail"])
        self.assertEqual(account_card["mail_challenge_kind_label"], "Ссылка подтверждения")
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Ссылка из письма применена")

    def test_failed_mail_challenge_marks_email_code_required_and_keeps_mail_reason(self) -> None:
        account_id = self._create_instagram_account("mailfail1", "mail_fail_user", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "failed",
                "detail": "Свежих писем не найдено.",
                "last_file": "ready.mp4",
                "runner_name": "runner-1",
                "account_publish_state": "email_code_required",
                "publish_phase": "clean_login",
                "event_kind": "mail_code_not_found",
                "elapsed_seconds": 15,
                "reason_code": "email_code_required",
                "mail_challenge": {
                    "status": "not_found",
                    "kind": "numeric_code",
                    "reason_code": "mail_not_found",
                    "reason_text": "Свежих писем не найдено.",
                    "confidence": 0.4,
                },
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        account = dict(self.db.get_account(account_id))
        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(account["instagram_publish_status"], "email_code_required")
        self.assertEqual(account["rotation_state"], "not_working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertIn("Свежих писем не найдено.", account["rotation_state_reason"])
        self.assertEqual(account["mail_challenge_status"], "not_found")
        self.assertEqual(account["mail_challenge_reason_text"], "Свежих писем не найдено.")
        self.assertEqual(batch["state"], "completed_with_errors")

        snapshot = self._progress_snapshot(batch_id)
        account_card = snapshot["accounts"][0]
        self.assertEqual(account_card["phase_label"], "Нужен код с почты")
        self.assertIn("Свежих писем не найдено.", account_card["phase_detail"])
        self.assertEqual(account_card["instagram_publish_status"], "email_code_required")
        self.assertEqual(account_card["mail_challenge_status"], "not_found")
        mail_events = [event for event in snapshot["recent_activity"] if event["title"] == "Код из письма не найден"]
        self.assertTrue(mail_events)
        self.assertIn("Свежих писем не найдено.", mail_events[0]["detail"])

    def test_publishing_snapshot_uses_upload_phase_payload(self) -> None:
        account_id = self._create_instagram_account("phase1", "phase_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "publishing",
                "detail": "Видео загружается в Instagram.",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "publish_phase": "uploading",
                "accepted_by_instagram": True,
                "elapsed_seconds": 45,
                "last_activity": "Instagram загружает Reel: 37%.",
                "upload_progress_pct": 37,
                "event_kind": "uploading_detected",
                "timings": {"tap_share_seconds": 1.2, "time_to_upload_detected_seconds": 12.0},
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        account = snapshot["accounts"][0]
        self.assertEqual(account["phase_label"], "Загружается в Instagram")
        self.assertIn("37%", account["phase_detail"])
        self.assertEqual(account["publish_phase"], "uploading")
        self.assertTrue(account["accepted_by_instagram"])
        self.assertEqual(account["upload_progress_pct"], 37)
        self.assertGreaterEqual(account["progress_pct"], 93)
        self.assertLess(account["progress_pct"], 100)
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Загрузка в Instagram")
        self.assertIn("37%", snapshot["recent_activity"][0]["detail"])

    def test_publishing_snapshot_uses_profile_verification_phase(self) -> None:
        account_id = self._create_instagram_account("verify1", "verify_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "publishing",
                "detail": "Проверяю появление свежего Reel в профиле.",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "publish_phase": "verifying_profile",
                "accepted_by_instagram": True,
                "elapsed_seconds": 103,
                "last_activity": "Свежий Reel ещё не найден, повторяю проверку.",
                "event_kind": "profile_verification_retry",
                "reason_code": "publish_profile_inconclusive",
                "verification_attempt": 2,
                "verification_window_minutes": 30,
                "checked_slots": 3,
                "baseline_available": True,
                "first_profile_check_at": 1760000600,
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        account = snapshot["accounts"][0]
        self.assertEqual(account["phase_label"], "Проверяю в профиле")
        self.assertEqual(account["publish_phase"], "verifying_profile")
        self.assertIn("Проверка #2", account["phase_detail"])
        self.assertIn("Первый вход в профиль:", account["phase_detail"])
        self.assertGreaterEqual(account["progress_pct"], 99)
        self.assertLessEqual(account["progress_pct"], 99)
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Проверяю в профиле")

    def test_publishing_snapshot_shows_profile_recovery_diagnostics_hint(self) -> None:
        account_id = self._create_instagram_account("verifydiag1", "verify_diag_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "publishing",
                "detail": "Возвращаюсь в профиль.",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "publish_phase": "verifying_profile",
                "accepted_by_instagram": True,
                "elapsed_seconds": 812,
                "last_activity": "Возвращаюсь в профиль.",
                "event_kind": "profile_verification_retry",
                "reason_code": "publish_profile_navigation_failed",
                "verification_attempt": 3,
                "verification_window_minutes": 30,
                "checked_slots": 3,
                "baseline_available": True,
                "profile_surface_state": "comment_sheet",
                "comment_sheet_visible": True,
                "clips_viewer_visible": True,
                "keyboard_visible": True,
                "diagnostics_path": "/tmp/diag/profile_recovery.png",
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        account = snapshot["accounts"][0]
        self.assertIn("Возвращаюсь в профиль.", account["phase_detail"])
        self.assertIn("Диагностика сохранена", account["phase_detail"])

    def test_publishing_snapshot_uses_profile_verification_wait_window_phase(self) -> None:
        account_id = self._create_instagram_account("verifywait1", "verify_wait_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "publishing",
                "detail": "Upload принят, жду окно проверки профиля перед подтверждением публикации.",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "publish_phase": "waiting_profile_verification_window",
                "accepted_by_instagram": True,
                "elapsed_seconds": 180,
                "last_activity": "Upload принят, жду окно проверки профиля перед подтверждением публикации.",
                "event_kind": "profile_verification_scheduled",
                "reason_code": "publish_profile_verification_scheduled",
                "seconds_until_profile_check": 420,
                "share_clicked_at": 1760000000,
                "verification_starts_at": 1760000420,
                "verification_window_minutes": 30,
                "checked_slots": 3,
                "baseline_available": True,
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        snapshot = self._progress_snapshot(batch_id)
        account = snapshot["accounts"][0]
        self.assertEqual(account["phase_label"], "Жду окно проверки профиля")
        self.assertEqual(account["publish_phase"], "waiting_profile_verification_window")
        self.assertIn("7 мин", account["phase_detail"])
        self.assertIn("Share:", account["phase_detail"])
        self.assertIn("Старт окна:", account["phase_detail"])
        self.assertEqual(snapshot["recent_activity"][0]["title"], "Жду окно проверки профиля")

    def test_needs_review_job_sets_completed_needs_review_batch(self) -> None:
        account_id = self._create_instagram_account("review1", "review_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "needs_review",
                "detail": "Upload принят, но профиль не дал надёжного подтверждения.",
                "last_file": "video1.mp4",
                "runner_name": "runner-1",
                "account_publish_state": "needs_review",
                "publish_phase": "verifying_profile",
                "accepted_by_instagram": True,
                "elapsed_seconds": 181,
                "last_activity": "Свежий Reel не удалось подтвердить в профиле.",
                "event_kind": "needs_review",
                "reason_code": "publish_profile_inconclusive",
                "verification_attempt": 3,
                "verification_window_minutes": 30,
                "checked_slots": 3,
                "baseline_available": True,
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        account = dict(self.db.get_account(account_id))
        batch = dict(self.db.get_publish_batch(batch_id))
        batch_account = self._batch_accounts(batch_id)[0]
        job = dict(self.db.get_publish_job(job_id))
        self.assertEqual(account["instagram_publish_status"], "needs_review")
        self.assertEqual(account["rotation_state"], "working")
        self.assertEqual(account["rotation_state_source"], "auto")
        self.assertEqual(account["rotation_state_reason"], "")
        self.assertEqual(batch_account["state"], "needs_review")
        self.assertEqual(job["state"], "needs_review")
        self.assertEqual(batch["state"], "completed_needs_review")
        snapshot = self._progress_snapshot(batch_id)
        self.assertEqual(snapshot["batch"]["phase_label"], "Нужна проверка")
        self.assertEqual(snapshot["accounts"][0]["phase_label"], "Нужна проверка")

    def test_publishing_heartbeat_extends_job_lease(self) -> None:
        account_id = self._create_instagram_account("lease1", "lease_user", "emulator-5554")
        created = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        (batch_dir / "video1.mp4").write_bytes(b"fake-video-content")

        for payload in (
            {"event": "generation_started", "batch_id": batch_id},
            {"event": "artifact_ready", "batch_id": batch_id, "account_id": account_id, "path": "video1.mp4"},
            {"event": "generation_completed", "batch_id": batch_id},
        ):
            self.assertEqual(self._post_signed_callback(payload).status_code, 200)

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        with (
            patch.object(self.app_module, "PUBLISH_RUNNER_LEASE_SECONDS", 60),
            patch.object(self.app_module.time, "time", return_value=1000),
            patch.object(self.db.time, "time", return_value=1000),
        ):
            lease = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        job_id = int(lease.json()["job"]["id"])

        with (
            patch.object(self.app_module, "PUBLISH_RUNNER_LEASE_SECONDS", 60),
            patch.object(self.app_module.time, "time", return_value=1030),
            patch.object(self.db.time, "time", return_value=1030),
        ):
            status = self.client.post(
                f"/api/internal/publishing/jobs/{job_id}/status",
                json={
                    "state": "publishing",
                    "detail": "Жду подтверждение публикации.",
                    "last_file": "video1.mp4",
                    "runner_name": "runner-1",
                    "publish_phase": "waiting_confirmation",
                    "accepted_by_instagram": True,
                    "elapsed_seconds": 30,
                    "last_activity": "Upload завершён, жду подтверждение публикации от Instagram.",
                    "event_kind": "publish_confirmation_wait",
                },
                headers=headers,
            )
        self.assertEqual(status.status_code, 200)

        with patch.object(self.db.time, "time", return_value=1085):
            self.assertIsNone(self.db.lease_next_publish_job(runner_name="runner-2", lease_seconds=60))
        mid_job = dict(self.db.get_publish_job(job_id))
        self.assertEqual(mid_job["state"], "publishing")
        self.assertEqual(int(mid_job["lease_expires_at"]), 1090)

        with patch.object(self.db.time, "time", return_value=1091):
            self.assertIsNone(self.db.lease_next_publish_job(runner_name="runner-2", lease_seconds=60))
        expired_job = dict(self.db.get_publish_job(job_id))
        self.assertEqual(expired_job["state"], "failed")

    def test_published_job_creates_reel_post_and_progress_snapshot_exposes_metrics(self) -> None:
        account_id = self._create_instagram_account("login-reel", "user-reel", "emulator-5554")

        response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(response.status_code, 303)
        batch_id = int(response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)
        upload_payload = upload.json()
        job_id = int(upload_payload["job_ids"][0])
        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}

        status = self.client.post(
            f"/api/internal/publishing/jobs/{job_id}/status",
            json={
                "state": "published",
                "detail": "Reel подтверждён через профиль.",
                "last_file": "ready.mp4",
                "runner_name": "runner-1",
                "source_path": upload_payload["path"],
                "publish_phase": "verifying_profile",
                "accepted_by_instagram": True,
                "elapsed_seconds": 42,
                "event_kind": "profile_verified",
                "matched_slot": 1,
                "matched_age_seconds": 120,
                "reel_fingerprint": "finger-1",
                "reel_signature_text": "new reel",
                "published_at": 1710000000,
            },
            headers=headers,
        )
        self.assertEqual(status.status_code, 200)

        posts = [dict(row) for row in self.db.list_instagram_reel_posts_for_batch(batch_id)]
        self.assertEqual(len(posts), 1)
        self.assertEqual(int(posts[0]["publish_job_id"]), job_id)
        self.assertEqual(posts[0]["reel_fingerprint"], "finger-1")
        self.assertEqual(posts[0]["reel_signature_text"], "new reel")
        self.assertEqual(int(posts[0]["published_at"]), 1710000000)

        self.db.record_instagram_reel_metric_snapshot(
            int(posts[0]["id"]),
            window_key="t30m",
            status="partial",
            plays_count=1200,
            likes_count=45,
            comments_count=6,
            collected_at=1710001800,
        )
        snapshot = self._progress_snapshot(batch_id)
        account = snapshot["accounts"][0]
        self.assertEqual(account["latest_reel"]["plays_label"], "1.2K")
        self.assertIn("Просмотры 1.2K", account["reel_metrics_summary"])
        self.assertEqual(account["reel_metrics_history"][0]["window_label"], "30м")

        page = self.client.get(f"/publishing/batches/{batch_id}")
        self.assertEqual(page.status_code, 200)
        self.assertIn("Просмотры 1.2K", page.text)

    def test_standalone_publish_callback_creates_single_reel_post_by_helper_ticket(self) -> None:
        account_id = self._create_instagram_account("login-standalone", "user-standalone", "emulator-5554")
        headers = {"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]}
        payload = {
            "state": "published",
            "detail": "Standalone publish success.",
            "handle": "user-standalone",
            "last_file": "video.mp4",
            "source_path": "/tmp/video.mp4",
            "helper_ticket": "ticket-123",
            "reel_fingerprint": "finger-standalone",
            "reel_signature_text": "standalone reel",
            "matched_slot": 0,
            "matched_age_seconds": 90,
            "published_at": 1710000100,
        }

        first = self.client.post(f"/api/helper/accounts/{account_id}/instagram-publish-status", json=payload, headers=headers)
        second = self.client.post(f"/api/helper/accounts/{account_id}/instagram-publish-status", json=payload, headers=headers)
        self.assertEqual(first.status_code, 200)
        self.assertEqual(second.status_code, 200)

        latest = dict(self.db.get_latest_instagram_reel_post_for_account(account_id))
        self.assertEqual(latest["origin_kind"], "standalone")
        self.assertEqual(latest["helper_ticket"], "ticket-123")
        self.assertEqual(latest["reel_fingerprint"], "finger-standalone")

        conn = self.db._connect()
        try:
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) AS total FROM instagram_reel_posts WHERE helper_ticket = ?", ("ticket-123",))
            total = int(cur.fetchone()["total"])
        finally:
            conn.close()
        self.assertEqual(total, 1)

    def test_reel_metrics_lease_and_snapshot_advances_window(self) -> None:
        account_id = self._create_instagram_account("login-metrics", "user-metrics", "emulator-5554")
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="metrics-ticket-1",
            source_name="video.mp4",
            source_path="/tmp/video.mp4",
            payload={
                "helper_ticket": "metrics-ticket-1",
                "reel_fingerprint": "finger-1",
                "reel_signature_text": "metrics reel",
                "published_at": 100,
            },
        )
        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}

        lease = self.client.post("/api/internal/reel-metrics/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)
        leased_post = lease.json()["post"]
        self.assertEqual(leased_post["id"], int(post["id"]))
        self.assertEqual(leased_post["window_key"], "t30m")
        self.assertEqual(leased_post["account_login"], "login-metrics")

        snapshot = self.client.post(
            f"/api/internal/reel-metrics/posts/{int(post['id'])}/snapshot",
            json={
                "window_key": "t30m",
                "status": "partial",
                "plays_count": 1200,
                "likes_count": 45,
                "comments_count": 6,
                "collected_at": 2000,
            },
            headers=headers,
        )
        self.assertEqual(snapshot.status_code, 200)
        updated = dict(self.db.get_instagram_reel_post(int(post["id"])))
        self.assertEqual(updated["collection_stage"], "t6h")
        self.assertEqual(updated["collection_state"], "partial")
        self.assertEqual(int(updated["next_collect_at"]), 100 + 6 * 60 * 60)

    def test_reel_metrics_retryable_failure_reschedules_same_window(self) -> None:
        account_id = self._create_instagram_account("login-metrics-retry", "user-metrics-retry", "emulator-5554")
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="metrics-ticket-2",
            source_name="video.mp4",
            source_path="/tmp/video.mp4",
            payload={
                "helper_ticket": "metrics-ticket-2",
                "reel_fingerprint": "finger-2",
                "reel_signature_text": "metrics reel 2",
                "published_at": 100,
            },
        )
        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}

        lease = self.client.post("/api/internal/reel-metrics/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease.status_code, 200)

        snapshot = self.client.post(
            f"/api/internal/reel-metrics/posts/{int(post['id'])}/snapshot",
            json={
                "window_key": "t30m",
                "status": "failed",
                "retryable": True,
                "error_detail": "adb timeout",
                "collected_at": 2000,
            },
            headers=headers,
        )
        self.assertEqual(snapshot.status_code, 200)
        updated = dict(self.db.get_instagram_reel_post(int(post["id"])))
        self.assertEqual(updated["collection_stage"], "t30m")
        self.assertEqual(updated["collection_state"], "scheduled")
        self.assertEqual(int(updated["next_collect_at"]), 2600)

    def test_manual_account_status_blocks_fully_auto_publish_batch(self) -> None:
        account_id = self._create_instagram_account("manual-status", "manual_status", "emulator-5554")
        account = dict(self.db.get_account(account_id))

        changed = self.db.update_account(
            account_id,
            account["type"],
            account["account_login"],
            account["account_password"],
            account["username"],
            account["email"],
            account["email_password"],
            account["proxy"],
            account["twofa"],
            account["mail_provider"],
            account["mail_auth_json"],
            "working",
            account["views_state"],
            account["owner_worker_id"],
            account["instagram_emulator_serial"],
            "limited_recommendations",
        )
        self.assertTrue(changed)

        refreshed = dict(self.db.get_account(account_id))
        self.assertEqual(refreshed["account_status_manual"], "limited_recommendations")
        issues = self.db.publish_account_readiness_issues(refreshed)
        self.assertTrue(any("limited recommendations" in item.lower() for item in issues))

        with self.assertRaisesRegex(ValueError, "limited recommendations"):
            self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")

    def test_auto_account_status_limited_recommendations_blocks_fully_auto_publish_batch(self) -> None:
        account_id = self._create_instagram_account("auto-status", "auto_status", "emulator-5554")

        changed = self.db.update_account_instagram_account_status_auto(
            account_id,
            "limited_recommendations",
            "Helper found limited recommendations.",
            checked_at=1710000000,
            diagnostics_path="/tmp/account-status.png",
        )
        self.assertTrue(changed)

        snapshot = self.db.get_account_shadow_snapshot(account_id)
        self.assertEqual(snapshot["shadow_status"], "shadowban_suspected")
        self.assertTrue(snapshot["shadow_confirmed_by_instagram"])

        refreshed = dict(self.db.get_account(account_id))
        issues = self.db.publish_account_readiness_issues(refreshed)
        self.assertTrue(any("ограничение рекомендаций" in item.lower() for item in issues))

        with self.assertRaisesRegex(ValueError, "ограничение рекомендаций|limited recommendations"):
            self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")

    def test_auto_account_status_inconclusive_warns_but_does_not_block_publish_batch(self) -> None:
        account_id = self._create_instagram_account("auto-risk", "auto_risk", "emulator-5554")
        self.db.update_account_instagram_account_status_auto(
            account_id,
            "inconclusive",
            "Helper could not classify the surface.",
            checked_at=1710000200,
        )

        snapshot = self.db.get_account_shadow_snapshot(account_id)
        self.assertEqual(snapshot["shadow_status"], "risk")
        self.assertFalse(snapshot["shadow_confirmed_by_instagram"])

        refreshed = dict(self.db.get_account(account_id))
        self.assertEqual(self.db.publish_account_readiness_issues(refreshed), [])

        batch = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        dashboard = self._progress_snapshot(int(batch["batch_id"]))
        self.assertEqual(dashboard["accounts"][0]["shadow_status"], "risk")
        self.assertTrue(dashboard["accounts"][0]["shadow_reasons"])

    def test_reel_shadow_snapshot_stays_risk_without_comparable_baseline(self) -> None:
        account_id = self._create_instagram_account("reel-risk", "reel_risk", "emulator-5554")
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="shadow-risk-ticket-1",
            source_name="risk.mp4",
            source_path="/tmp/risk.mp4",
            payload={"published_at": 100, "helper_ticket": "shadow-risk-ticket-1"},
        )
        self.db.record_instagram_reel_metric_snapshot(
            int(post["id"]),
            window_key="t30m",
            status="ok",
            collected_at=2000,
            accounts_reached_count=90,
        )

        snapshot = self.db.get_instagram_reel_shadow_snapshot(int(post["id"]))
        self.assertEqual(snapshot["shadow_status"], "risk")
        self.assertTrue(snapshot["insufficient_data"])
        self.assertEqual(snapshot["comparable_window"], "t30m")

    def test_reel_shadow_snapshot_escalates_with_comparable_baseline_collapse(self) -> None:
        account_id = self._create_instagram_account("reel-red", "reel_red", "emulator-5554")
        baseline_reaches = [1200, 1000]
        for index, reach in enumerate(baseline_reaches, start=1):
            post = self.db.upsert_instagram_reel_post_for_standalone(
                account_id=account_id,
                helper_ticket=f"shadow-red-ticket-{index}",
                source_name=f"baseline-{index}.mp4",
                source_path=f"/tmp/baseline-{index}.mp4",
                payload={"published_at": index * 100, "helper_ticket": f"shadow-red-ticket-{index}"},
            )
            self.db.record_instagram_reel_metric_snapshot(
                int(post["id"]),
                window_key="t30m",
                status="ok",
                collected_at=3000 + index,
                accounts_reached_count=max(1, reach // 4),
            )
            self.db.record_instagram_reel_metric_snapshot(
                int(post["id"]),
                window_key="t6h",
                status="ok",
                collected_at=4000 + index,
                accounts_reached_count=max(1, reach // 2),
            )
            self.db.record_instagram_reel_metric_snapshot(
                int(post["id"]),
                window_key="t24h",
                status="ok",
                collected_at=5000 + index,
                accounts_reached_count=reach,
            )

        latest_post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="shadow-red-ticket-latest",
            source_name="latest.mp4",
            source_path="/tmp/latest.mp4",
            payload={"published_at": 400, "helper_ticket": "shadow-red-ticket-latest"},
        )
        self.db.record_instagram_reel_metric_snapshot(
            int(latest_post["id"]),
            window_key="t30m",
            status="ok",
            collected_at=7000,
            accounts_reached_count=60,
        )
        self.db.record_instagram_reel_metric_snapshot(
            int(latest_post["id"]),
            window_key="t6h",
            status="ok",
            collected_at=8000,
            accounts_reached_count=90,
        )
        self.db.record_instagram_reel_metric_snapshot(
            int(latest_post["id"]),
            window_key="t24h",
            status="ok",
            collected_at=9000,
            accounts_reached_count=120,
        )

        snapshot = self.db.get_instagram_reel_shadow_snapshot(int(latest_post["id"]))
        self.assertEqual(snapshot["shadow_status"], "shadowban_suspected")
        self.assertFalse(snapshot["insufficient_data"])
        self.assertEqual(snapshot["comparable_window"], "t24h")
        self.assertLess(float(snapshot["reach_ratio"] or 1.0), 0.2)

    def test_helper_account_status_callback_persists_and_renders_account_detail(self) -> None:
        account_id = self._create_instagram_account("callback-status", "callback_status", "emulator-5554")
        headers = {"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]}

        response = self.client.post(
            f"/api/helper/accounts/{account_id}/instagram-account-status",
            json={
                "status": "limited_recommendations",
                "detail": "Instagram shows limited recommendations.",
                "checked_at": 1710000300,
                "diagnostics_path": "/tmp/account-status.png",
            },
            headers=headers,
        )
        self.assertEqual(response.status_code, 200)

        snapshot = self.db.get_account_shadow_snapshot(account_id)
        self.assertTrue(snapshot["shadow_confirmed_by_instagram"])
        self.assertEqual(snapshot["account_status_auto"], "limited_recommendations")
        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["account_status_auto"], "limited_recommendations")
        self.assertEqual(account["account_status_auto_diagnostics_path"], "/tmp/account-status.png")

        detail = self.client.get(f"/accounts/{account_id}")
        self.assertEqual(detail.status_code, 200)
        self.assertIn("Auto limited recommendations", detail.text)
        self.assertIn("/tmp/account-status.png", detail.text)
        self.assertIn("Shadowban suspected", detail.text)

    def test_duplicate_video_hash_blocks_second_publish_job(self) -> None:
        account_id = self._create_instagram_account("dup-login", "dup_user", "emulator-5554")
        stage_path = Path(self.temp_dir.name) / "duplicate_clip.mp4"
        payload_hash = hashlib.sha256(b"duplicate-content").hexdigest()
        stage_path.write_bytes(b"duplicate-content")

        first_batch = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        first_artifact = self.db.register_publish_artifact(first_batch["batch_id"], path=str(stage_path), filename=stage_path.name)
        self.assertEqual(first_artifact["jobs_created"], 1)
        self.assertEqual(first_artifact["jobs_blocked"], 0)

        self.db.update_publish_job_state(
            int(first_artifact["job_ids"][0]),
            state="published",
            detail="ok",
            payload={"published_at": 1, "video_sha256": payload_hash},
        )

        second_batch = self.db.create_publish_batch([account_id], created_by_admin="admin", workflow_key="default")
        second_artifact = self.db.register_publish_artifact(second_batch["batch_id"], path=str(stage_path), filename=stage_path.name)
        self.assertEqual(second_artifact["jobs_created"], 0)
        self.assertEqual(second_artifact["jobs_blocked"], 1)

        second_job = dict(self.db.list_publish_jobs(second_batch["batch_id"])[0])
        self.assertEqual(second_job["state"], "canceled")
        self.assertIn("уже публиковался", second_job["policy_reason"])

    def test_launch_state_stores_session_mode_and_serial(self) -> None:
        account_id = self._create_audit_account("session-login", "session_user", serial="emulator-5554")

        changed = self.db.update_account_instagram_launch_state(
            account_id,
            "login_submitted",
            "Сессия переиспользована.",
            session_mode="session_reuse",
            serial="emulator-5554",
        )
        self.assertTrue(changed)

        account = dict(self.db.get_account(account_id))
        self.assertEqual(account["last_session_mode"], "session_reuse")
        self.assertEqual(account["last_login_serial"], "emulator-5554")
        self.assertGreater(int(account["last_stable_login_at"] or 0), 0)


if __name__ == "__main__":
    unittest.main()
