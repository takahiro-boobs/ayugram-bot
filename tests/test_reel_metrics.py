from __future__ import annotations

import importlib
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient


class ReelMetricsAppDbTests(unittest.TestCase):
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

    def _create_instagram_account(
        self,
        login: str = "login1",
        username: str = "user1",
        serial: str = "emulator-5554",
    ) -> int:
        created = self.db.create_account_with_default_link(
            account_type="instagram",
            account_login=login,
            account_password="pass123",
            username=username,
            email=f"{username}@example.com",
            email_password="mailpass",
            proxy="",
            twofa="JBSWY3DPEHPK3PXP",
            rotation_state="review",
            instagram_emulator_serial=serial,
            default_link_name=f"Instagram @{username}",
        )
        account_id = int(created["account_id"])
        self.db.update_account_instagram_launch_state(account_id, "login_submitted", "Login OK for tests.")
        return account_id

    def _count_reel_posts_by_ticket(self, helper_ticket: str) -> int:
        conn = self.db._connect()
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) AS total FROM instagram_reel_posts WHERE helper_ticket = ?",
            (str(helper_ticket),),
        )
        row = cur.fetchone()
        conn.close()
        return int(row["total"] or 0)

    def test_standalone_publish_success_upserts_single_reel_post(self) -> None:
        account_id = self._create_instagram_account()
        payload = {
            "state": "published",
            "detail": "Published from helper.",
            "handle": "user1",
            "last_file": "video.mp4",
            "source_path": "/tmp/video.mp4",
            "helper_ticket": "ticket-standalone-1",
            "reel_fingerprint": "fp-1",
            "reel_signature_text": "sig-1",
            "matched_slot": 2,
            "matched_age_seconds": 120,
            "published_at": 1_710_000_000,
        }

        response = self.client.post(
            f"/api/helper/accounts/{account_id}/instagram-publish-status",
            json=payload,
            headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
        )
        self.assertEqual(response.status_code, 200)

        duplicate = self.client.post(
            f"/api/helper/accounts/{account_id}/instagram-publish-status",
            json=payload,
            headers={"X-Helper-Api-Key": os.environ["HELPER_API_KEY"]},
        )
        self.assertEqual(duplicate.status_code, 200)

        self.assertEqual(self._count_reel_posts_by_ticket("ticket-standalone-1"), 1)
        latest = dict(self.db.get_latest_instagram_reel_post_for_account(account_id))
        self.assertEqual(latest["origin_kind"], "standalone")
        self.assertEqual(latest["helper_ticket"], "ticket-standalone-1")
        self.assertEqual(latest["reel_fingerprint"], "fp-1")
        self.assertEqual(latest["reel_signature_text"], "sig-1")
        self.assertEqual(latest["matched_slot"], 2)
        self.assertEqual(latest["matched_age_seconds"], 120)
        self.assertEqual(latest["collection_stage"], "t30m")
        self.assertEqual(latest["collection_state"], "scheduled")

    def test_reel_metrics_lease_returns_due_post_and_credentials(self) -> None:
        account_id = self._create_instagram_account()
        published_at = int(time.time()) - (7 * 60 * 60)
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="ticket-lease-1",
            source_name="reel.mp4",
            source_path="/tmp/reel.mp4",
            payload={
                "helper_ticket": "ticket-lease-1",
                "reel_fingerprint": "fp-lease",
                "reel_signature_text": "sig-lease",
                "published_at": published_at,
            },
        )

        response = self.client.post(
            "/api/internal/reel-metrics/lease",
            json={"runner_name": "runner-test"},
            headers={"X-Runner-Api-Key": os.environ["PUBLISH_RUNNER_API_KEY"]},
        )
        self.assertEqual(response.status_code, 200)
        leased = response.json()["post"]
        self.assertEqual(int(leased["id"]), int(post["id"]))
        self.assertEqual(leased["window_key"], "t30m")
        self.assertEqual(leased["account_login"], "login1")
        self.assertEqual(leased["account_password"], "pass123")
        self.assertEqual(leased["username"], "user1")
        self.assertEqual(leased["reel_fingerprint"], "fp-lease")
        self.assertEqual(leased["reel_signature_text"], "sig-lease")

    def test_reel_metric_snapshot_progresses_windows_and_retries_failures(self) -> None:
        account_id = self._create_instagram_account()
        published_at = 1_710_000_000
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="ticket-progression-1",
            source_name="reel.mp4",
            source_path="/tmp/reel.mp4",
            payload={"helper_ticket": "ticket-progression-1", "published_at": published_at},
        )
        post_id = int(post["id"])

        first_retry = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t30m",
            status="failed",
            retryable=True,
            collected_at=published_at + 30 * 60,
            error_detail="adb timeout",
        )
        self.assertEqual(first_retry["collection_stage"], "t30m")
        self.assertEqual(first_retry["collection_state"], "scheduled")
        self.assertEqual(first_retry["next_collect_at"], published_at + 30 * 60 + 10 * 60)

        second_retry = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t30m",
            status="failed",
            retryable=True,
            collected_at=published_at + 50 * 60,
            error_detail="adb timeout again",
        )
        self.assertEqual(second_retry["collection_stage"], "t30m")
        self.assertEqual(second_retry["collection_state"], "scheduled")
        self.assertEqual(second_retry["next_collect_at"], published_at + 50 * 60 + 30 * 60)

        terminal_failure = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t30m",
            status="failed",
            retryable=True,
            collected_at=published_at + 90 * 60,
            error_detail="still broken",
        )
        self.assertEqual(terminal_failure["collection_stage"], "t30m")
        self.assertEqual(terminal_failure["collection_state"], "failed")

        recovered = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t30m",
            status="ok",
            collected_at=published_at + 2 * 60 * 60,
            plays_count=1234,
            likes_count=56,
            comments_count=7,
        )
        self.assertEqual(recovered["collection_stage"], "t6h")
        self.assertEqual(recovered["collection_state"], "collected")
        self.assertEqual(recovered["next_collect_at"], published_at + 6 * 60 * 60)

        progressed = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t6h",
            status="partial",
            collected_at=published_at + 6 * 60 * 60,
            plays_count=1400,
        )
        self.assertEqual(progressed["collection_stage"], "t24h")
        self.assertEqual(progressed["collection_state"], "partial")
        self.assertEqual(progressed["next_collect_at"], published_at + 24 * 60 * 60)

        progressed = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t24h",
            status="unavailable",
            collected_at=published_at + 24 * 60 * 60,
        )
        self.assertEqual(progressed["collection_stage"], "t72h")
        self.assertEqual(progressed["collection_state"], "unavailable")
        self.assertEqual(progressed["next_collect_at"], published_at + 72 * 60 * 60)

        finished = self.db.record_instagram_reel_metric_snapshot(
            post_id,
            window_key="t72h",
            status="not_found",
            collected_at=published_at + 72 * 60 * 60,
        )
        self.assertEqual(finished["collection_stage"], "done")
        self.assertEqual(finished["collection_state"], "not_found")
        self.assertIsNone(finished["next_collect_at"])

    def test_batch_progress_snapshot_exposes_reel_metrics_summary(self) -> None:
        account_id = self._create_instagram_account()
        create_response = self.client.post(
            "/publishing/batches",
            data={"account_ids": [str(account_id)], "launch_mode": "existing_video"},
            follow_redirects=False,
        )
        self.assertEqual(create_response.status_code, 303)
        batch_id = int(create_response.headers["location"].rsplit("/", 1)[-1])

        upload = self.client.post(
            f"/api/publishing/batches/{batch_id}/artifacts/upload",
            data={"account_id": str(account_id)},
            files={"media_file": ("ready.mp4", b"ready-video", "video/mp4")},
        )
        self.assertEqual(upload.status_code, 200)
        job_id = int(upload.json()["job_ids"][0])

        published_at = 1_710_000_000
        self.db.update_publish_job_state(
            job_id,
            state="published",
            detail="Published from runner.",
            payload={
                "reel_fingerprint": "fp-batch",
                "reel_signature_text": "sig-batch",
                "matched_slot": 0,
                "matched_age_seconds": 120,
                "published_at": published_at,
            },
        )
        post = dict(self.db.get_latest_instagram_reel_post_for_account(account_id))
        self.db.record_instagram_reel_metric_snapshot(
            int(post["id"]),
            window_key="t30m",
            status="ok",
            collected_at=published_at + 30 * 60,
            plays_count=1234,
            likes_count=56,
            comments_count=7,
        )

        snapshot = self.client.get(f"/api/publishing/batches/{batch_id}/progress").json()
        account = snapshot["accounts"][0]
        self.assertIn("Просмотры 1.2K", account["reel_metrics_summary"])
        self.assertEqual(account["latest_reel"]["latest_snapshot_window_label"], "30м")
        self.assertEqual(len(account["reel_metrics_history"]), 1)

    def test_account_detail_page_shows_latest_reel_block(self) -> None:
        account_id = self._create_instagram_account()
        post = self.db.upsert_instagram_reel_post_for_standalone(
            account_id=account_id,
            helper_ticket="ticket-detail-1",
            source_name="detail.mp4",
            source_path="/tmp/detail.mp4",
            payload={"helper_ticket": "ticket-detail-1", "published_at": 1_710_000_000},
        )
        self.db.record_instagram_reel_metric_snapshot(
            int(post["id"]),
            window_key="t30m",
            status="ok",
            collected_at=1_710_001_800,
            plays_count=432,
            likes_count=21,
            comments_count=3,
        )

        response = self.client.get(f"/accounts/{account_id}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Последний Reel", response.text)
        self.assertIn("Просмотры: 432", response.text)
        self.assertIn("Аналитика", response.text)


class ReelMetricsHelperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import instagram_app_helper as helper_module

        cls.helper = importlib.reload(helper_module)

    def test_runner_prefers_publish_jobs_before_reel_metrics(self) -> None:
        publish_job = {
            "id": 15,
            "batch_id": 3,
            "account_id": 9,
            "source_name": "video.mp4",
            "emulator_serial": "emulator-5554",
            "source_path": "/tmp/video.mp4",
        }
        with (
            patch.object(self.helper, "_lease_publish_job", return_value=publish_job),
            patch.object(self.helper, "_lease_reel_metric_post") as lease_reel_mock,
            patch.object(self.helper, "_run_publish_job") as run_publish_mock,
            patch.object(self.helper, "_push_publish_job_status"),
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.helper._runner_main()
        run_publish_mock.assert_called_once_with(publish_job)
        lease_reel_mock.assert_not_called()

    def test_runner_leases_reel_metrics_when_publish_queue_is_empty(self) -> None:
        reel_post = {
            "id": 41,
            "account_id": 12,
            "source_name": "reel.mp4",
            "instagram_emulator_serial": "emulator-5556",
            "window_key": "t6h",
        }
        with (
            patch.object(self.helper, "_lease_publish_job", return_value=None),
            patch.object(self.helper, "_lease_reel_metric_post", return_value=reel_post),
            patch.object(self.helper, "_run_collect_reel_metrics") as run_metrics_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.helper._runner_main()
        run_metrics_mock.assert_called_once_with(reel_post)

    def test_runner_does_not_duplicate_failed_snapshot_when_collector_already_reported(self) -> None:
        reel_post = {
            "id": 42,
            "account_id": 12,
            "source_name": "reel.mp4",
            "instagram_emulator_serial": "emulator-5556",
            "window_key": "t24h",
        }
        flow_error = self.helper.ReelMetricsFlowError(
            "collector failed",
            snapshot_reported=True,
            serial="emulator-5556",
        )
        with (
            patch.object(self.helper, "_lease_publish_job", return_value=None),
            patch.object(self.helper, "_lease_reel_metric_post", return_value=reel_post),
            patch.object(self.helper, "_run_collect_reel_metrics", side_effect=flow_error),
            patch.object(self.helper, "_push_reel_metric_snapshot") as push_snapshot_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.helper._runner_main()
        push_snapshot_mock.assert_not_called()

    def test_runner_pushes_failed_snapshot_when_collector_fails_before_reporting(self) -> None:
        reel_post = {
            "id": 43,
            "account_id": 12,
            "source_name": "reel.mp4",
            "instagram_emulator_serial": "emulator-5556",
            "window_key": "t72h",
        }
        with (
            patch.object(self.helper, "_lease_publish_job", return_value=None),
            patch.object(self.helper, "_lease_reel_metric_post", return_value=reel_post),
            patch.object(self.helper, "_run_collect_reel_metrics", side_effect=RuntimeError("collector failed early")),
            patch.object(self.helper, "_push_reel_metric_snapshot") as push_snapshot_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper.time, "sleep", side_effect=KeyboardInterrupt),
        ):
            with self.assertRaises(KeyboardInterrupt):
                self.helper._runner_main()
        push_snapshot_mock.assert_called_once()
        self.assertEqual(push_snapshot_mock.call_args.kwargs["status"], "failed")
        self.assertTrue(push_snapshot_mock.call_args.kwargs["payload"]["retryable"])

    def test_run_collect_reel_metrics_marks_not_found_without_retry(self) -> None:
        post = {
            "id": 7,
            "account_id": 5,
            "source_name": "reel.mp4",
            "window_key": "t30m",
            "account_login": "login7",
            "account_password": "secret",
            "username": "user7",
            "twofa": "",
            "instagram_emulator_serial": "emulator-5554",
            "reel_fingerprint": "fp-7",
            "reel_signature_text": "sig-7",
            "published_at": 1_710_000_000,
        }
        device = object()
        login_result = {
            "state": "login_submitted",
            "detail": "Login ok.",
            "serial": "emulator-5554",
            "device": device,
        }
        with (
            patch.object(self.helper, "_run_login_flow", return_value=login_result),
            patch.object(self.helper, "_locate_reel_for_metrics", return_value=(None, None)),
            patch.object(self.helper, "_capture_publish_diagnostics", return_value={"screenshot": "/tmp/not-found.png"}),
            patch.object(self.helper, "_push_reel_metric_snapshot") as push_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper, "_recover_to_profile_surface"),
        ):
            self.helper._run_collect_reel_metrics(post)

        self.assertEqual(push_mock.call_args.kwargs["status"], "not_found")
        payload = push_mock.call_args.kwargs["payload"]
        self.assertEqual(payload["diagnostics_path"], "/tmp/not-found.png")
        self.assertEqual(payload["raw_text_json"]["reel_fingerprint"], "fp-7")

    def test_run_collect_reel_metrics_retries_when_login_flow_hits_helper_error(self) -> None:
        post = {
            "id": 8,
            "account_id": 5,
            "source_name": "reel.mp4",
            "window_key": "t30m",
            "account_login": "login8",
            "account_password": "secret",
            "username": "user8",
            "twofa": "",
            "instagram_emulator_serial": "emulator-5554",
            "reel_fingerprint": "fp-8",
            "reel_signature_text": "sig-8",
            "published_at": 1_710_000_000,
        }
        login_result = {
            "state": "helper_error",
            "detail": "Instagram not installed.",
            "serial": "emulator-5554",
            "device": None,
        }
        with (
            patch.object(self.helper, "_run_login_flow", return_value=login_result),
            patch.object(self.helper, "_capture_publish_diagnostics", return_value={"screenshot": "/tmp/login-error.png"}),
            patch.object(self.helper, "_push_reel_metric_snapshot") as push_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper, "_recover_to_profile_surface"),
        ):
            self.helper._run_collect_reel_metrics(post)

        self.assertEqual(push_mock.call_args.kwargs["status"], "failed")
        self.assertTrue(push_mock.call_args.kwargs["payload"]["retryable"])
        self.assertEqual(push_mock.call_args.kwargs["payload"]["diagnostics_path"], "/tmp/login-error.png")
        self.assertEqual(push_mock.call_args.kwargs["payload"]["raw_text_json"]["login_state"], "helper_error")

    def test_run_collect_reel_metrics_saves_partial_snapshot_without_retry(self) -> None:
        post = {
            "id": 9,
            "account_id": 6,
            "source_name": "reel.mp4",
            "window_key": "t6h",
            "account_login": "login9",
            "account_password": "secret",
            "username": "user9",
            "twofa": "",
            "instagram_emulator_serial": "emulator-5554",
            "reel_fingerprint": "fp-9",
            "reel_signature_text": "sig-9",
            "published_at": 1_710_000_000,
        }
        device = object()
        candidate = self.helper.ProfileReelCandidate(
            slot_index=1,
            age_seconds=3600,
            age_label="1 hour ago",
            fingerprint="fp-9",
            signature_text="sig-9",
            opened=True,
        )
        login_result = {
            "state": "login_submitted",
            "detail": "Login ok.",
            "serial": "emulator-5554",
            "device": device,
        }
        with (
            patch.object(self.helper, "_run_login_flow", return_value=login_result),
            patch.object(self.helper, "_locate_reel_for_metrics", return_value=((120, 320), candidate)),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_open_reel_viewer_at_center", return_value=True),
            patch.object(
                self.helper,
                "_collect_reel_metrics_from_open_viewer",
                return_value=(
                    "partial",
                    {"plays_count": 345, "likes_count": 22, "comments_count": 4},
                    {"viewer": [{"value": "345"}], "insights": [], "insights_opened": False},
                ),
            ),
            patch.object(self.helper, "_push_reel_metric_snapshot") as push_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper, "_recover_to_profile_surface"),
        ):
            self.helper._run_collect_reel_metrics(post)

        self.assertEqual(push_mock.call_args.kwargs["status"], "partial")
        payload = push_mock.call_args.kwargs["payload"]
        self.assertEqual(payload["plays_count"], 345)
        self.assertEqual(payload["likes_count"], 22)
        self.assertEqual(payload["comments_count"], 4)
        self.assertEqual(payload["raw_text_json"]["matched_candidate"]["fingerprint"], "fp-9")
        self.assertFalse(payload.get("retryable"))


if __name__ == "__main__":
    unittest.main()
