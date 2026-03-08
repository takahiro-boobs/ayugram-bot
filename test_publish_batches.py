import importlib
import json
import os
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
        self.temp_dir.cleanup()
        for key, old_value in self._old_env.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value

    def _create_instagram_account(self, login: str, username: str, serial: str) -> int:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login=login,
            account_password="pass123",
            username=username,
            email=f"{username}@example.com",
            email_password="mailpass",
            proxy="",
            twofa="",
            instagram_emulator_serial=serial,
            default_link_name=f"Instagram @{username}",
        )
        return int(created["account_id"])

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

    def test_batch_create_triggers_n8n_webhook(self) -> None:
        acc1 = self._create_instagram_account("login1", "user1", "emulator-5554")
        acc2 = self._create_instagram_account("login2", "user2", "emulator-5556")

        class DummyResponse:
            status_code = 200
            text = "accepted"

            def raise_for_status(self) -> None:
                return None

        with patch.object(self.app_module.requests, "post", return_value=DummyResponse()) as mocked:
            response = self.client.post(
                "/publishing/batches",
                data={"account_ids": [str(acc1), str(acc2)]},
                follow_redirects=False,
            )

        self.assertEqual(response.status_code, 303)
        location = response.headers["location"]
        self.assertTrue(location.startswith("/publishing/batches/"))
        batch_id = int(location.rsplit("/", 1)[-1])
        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["state"], "generating")
        self.assertEqual(batch["accounts_total"], 2)
        self.assertIn("n8n принял batch", batch["detail"])

        mocked.assert_called_once()
        call_args = mocked.call_args
        self.assertEqual(call_args.args[0], os.environ["PUBLISH_N8N_WEBHOOK_URL"])
        payload = json.loads(call_args.kwargs["data"].decode("utf-8"))
        self.assertEqual(payload["batch_id"], batch_id)
        self.assertEqual(len(payload["accounts"]), 2)
        self.assertEqual(payload["callback_url"], "http://testserver/api/internal/publishing/n8n")

    def test_signed_callbacks_and_job_leasing_are_idempotent(self) -> None:
        acc1 = self._create_instagram_account("same1", "same_user1", "emulator-5554")
        acc2 = self._create_instagram_account("same2", "same_user2", "emulator-5554")
        created = self.db.create_publish_batch([acc1, acc2], created_by_admin="admin", workflow_key="default")
        batch_id = int(created["batch_id"])

        batch_dir = Path(self.staging_dir) / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        video_path = batch_dir / "video1.mp4"
        video_path.write_bytes(b"fake-video-content")

        generation_started = self._sign_payload({"event": "generation_started", "batch_id": batch_id})
        started_resp = self.client.post("/api/internal/publishing/n8n", content=generation_started["body"], headers=generation_started["headers"])
        self.assertEqual(started_resp.status_code, 200)

        artifact_ready = self._sign_payload({"event": "artifact_ready", "batch_id": batch_id, "path": "video1.mp4"})
        artifact_resp = self.client.post("/api/internal/publishing/n8n", content=artifact_ready["body"], headers=artifact_ready["headers"])
        self.assertEqual(artifact_resp.status_code, 200)
        duplicate_resp = self.client.post("/api/internal/publishing/n8n", content=artifact_ready["body"], headers=artifact_ready["headers"])
        self.assertEqual(duplicate_resp.status_code, 200)

        generation_completed = self._sign_payload({"event": "generation_completed", "batch_id": batch_id})
        completed_resp = self.client.post("/api/internal/publishing/n8n", content=generation_completed["body"], headers=generation_completed["headers"])
        self.assertEqual(completed_resp.status_code, 200)

        batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(batch["artifacts_total"], 1)
        self.assertEqual(batch["jobs_total"], 2)
        self.assertEqual(batch["state"], "publishing")

        headers = {"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]}
        lease1 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease1.status_code, 200)
        job1 = lease1.json()["job"]
        lease2 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease2.status_code, 204)

        status1 = self.client.post(
            f"/api/internal/publishing/jobs/{job1['id']}/status",
            json={"state": "published", "detail": "ok", "last_file": "video1.mp4", "runner_name": "runner-1"},
            headers=headers,
        )
        self.assertEqual(status1.status_code, 200)

        lease3 = self.client.post("/api/internal/publishing/jobs/lease", json={"runner_name": "runner-1"}, headers=headers)
        self.assertEqual(lease3.status_code, 200)
        job2 = lease3.json()["job"]
        self.assertNotEqual(job1["id"], job2["id"])

        status2 = self.client.post(
            f"/api/internal/publishing/jobs/{job2['id']}/status",
            json={"state": "published", "detail": "ok", "last_file": "video1.mp4", "runner_name": "runner-1"},
            headers=headers,
        )
        self.assertEqual(status2.status_code, 200)

        final_batch = dict(self.db.get_publish_batch(batch_id))
        self.assertEqual(final_batch["state"], "completed")
        self.assertEqual(final_batch["published_jobs"], 2)

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


if __name__ == "__main__":
    unittest.main()
