from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import requests

from scripts.publishing.publish_canary import (
    AdminClient,
    _build_parser,
    _classify_failure,
    _extract_latest_helper_timestamps,
    _extract_log_markers,
    _extract_ready_accounts,
    _helper_base_url_matches_expected,
    _is_loopback_url,
    _parse_batch_id_from_url,
    _resolve_config_value,
    _scp_to_remote,
    _should_upload_existing_video,
    _state_signature,
)


class PublishCanaryHelpersTests(unittest.TestCase):
    def test_extract_ready_accounts(self) -> None:
        html = """
        <input class="publish-select-check" type="checkbox" id="publish-account-5" name="account_ids" value="5">
        <div class="publish-select-main">
          <label class="listitem__title" for="publish-account-5">@ayugram_sed</label>
        </div>
        <input class="publish-select-check" type="checkbox" id="publish-account-6" name="account_ids" value="6">
        <label class="listitem__title" for="publish-account-6">@other_handle</label>
        """
        self.assertEqual(
            _extract_ready_accounts(html),
            [
                {"account_id": "5", "username": "ayugram_sed"},
                {"account_id": "6", "username": "other_handle"},
            ],
        )

    def test_parse_batch_id_from_url(self) -> None:
        self.assertEqual(_parse_batch_id_from_url("http://127.0.0.1:38001/slezhka/publishing/batches/44"), 44)
        self.assertIsNone(_parse_batch_id_from_url("http://127.0.0.1:38001/slezhka/publishing/start"))

    def test_loopback_url_detection(self) -> None:
        self.assertTrue(_is_loopback_url("http://127.0.0.1:38001/slezhka"))
        self.assertTrue(_is_loopback_url("http://localhost:18001/slezhka"))
        self.assertFalse(_is_loopback_url("http://4abbf189760e.vps.myjino.ru/slezhka"))

    def test_helper_base_url_matches_expected_when_helper_uses_loopback_tunnel(self) -> None:
        self.assertTrue(
            _helper_base_url_matches_expected(
                "http://4abbf189760e.vps.myjino.ru/slezhka",
                "http://127.0.0.1:38001/slezhka",
            )
        )

    def test_helper_base_url_rejects_different_path(self) -> None:
        self.assertFalse(
            _helper_base_url_matches_expected(
                "http://4abbf189760e.vps.myjino.ru/slezhka",
                "http://127.0.0.1:38001/other",
            )
        )

    def test_resolve_config_value_prefers_cli_then_env(self) -> None:
        self.assertEqual(_resolve_config_value("http://direct", "MISSING_KEY", "fallback"), "http://direct")
        self.assertEqual(_resolve_config_value("", "MISSING_KEY", "fallback"), "fallback")

    def test_extract_log_markers_and_timestamps(self) -> None:
        lines = [
            "2026-03-16 20:00:00 INFO wait_publish_uploading_detected: serial=emulator-5554 elapsed=12s",
            "2026-03-16 20:10:00 INFO profile_verification_schedule: serial=emulator-5554 source=video.mp4 share_clicked_at=1773677000 verification_starts_at=1773677600 verification_deadline_at=1773678800 elapsed_since_share_seconds=601 seconds_until_start=0",
            "2026-03-16 20:10:05 INFO profile_verification_first_check: serial=emulator-5554 source=video.mp4 share_clicked_at=1773677000 verification_starts_at=1773677600 verification_deadline_at=1773678800 first_profile_check_at=1773677605 elapsed_since_share_seconds=605 attempt=1",
            "2026-03-16 20:10:08 INFO profile_verification_verified: serial=emulator-5554 source=video.mp4 attempt=1 matched_slot=0 matched_age_seconds=120 share_clicked_at=1773677000 first_profile_check_at=1773677605 elapsed_since_share_seconds=608",
        ]
        self.assertEqual(
            _extract_log_markers(lines),
            [
                "wait_publish_uploading_detected",
                "profile_verification_schedule",
                "profile_verification_first_check",
                "profile_verification_verified",
            ],
        )
        self.assertEqual(
            _extract_latest_helper_timestamps(lines),
            {
                "share_clicked_at": 1773677000,
                "verification_starts_at": 1773677600,
                "verification_deadline_at": 1773678800,
                "first_profile_check_at": 1773677605,
                "matched_age_seconds": 120,
            },
        )

    def test_state_signature_for_single_account(self) -> None:
        snapshot = {
            "batch": {"state": "running", "phase_key": "instagram_publish"},
            "accounts": [
                {
                    "id": 5,
                    "batch_state": "publishing",
                    "publish_phase": "verifying_profile",
                    "phase_label": "Проверяю в профиле",
                    "phase_detail": "Открываю Reels",
                }
            ],
            "recent_activity": [{"title": "Проверяю Reel", "detail": "Открываю Reels"}],
        }
        sig = _state_signature(snapshot, 5)
        self.assertEqual(sig.account_state, "publishing")
        self.assertEqual(sig.publish_phase, "verifying_profile")
        self.assertEqual(sig.latest_activity_title, "Проверяю Reel")

    def test_classify_failure_navigation(self) -> None:
        snapshot = {
            "accounts": [
                {
                    "id": 5,
                    "batch_state": "needs_review",
                    "publish_phase": "verifying_profile",
                    "phase_detail": "Не удалось открыть профиль и вкладку Reels для проверки.",
                }
            ]
        }
        helper_health = {"state": {"state": "needs_review"}}
        logs = ["profile_verification_needs_review: reason=publish_profile_navigation_failed"]
        self.assertEqual(_classify_failure(snapshot, helper_health, logs), "navigation_recovery")

    def test_classify_failure_timestamp(self) -> None:
        snapshot = {
            "accounts": [
                {
                    "id": 5,
                    "batch_state": "needs_review",
                    "publish_phase": "verifying_profile",
                    "phase_detail": "Reels открылись, но не удалось прочитать время публикации.",
                }
            ]
        }
        helper_health = {"state": {"state": "needs_review"}}
        logs = ["profile_slot_timestamp_unreadable: serial=emulator-5554 slot=0"]
        self.assertEqual(_classify_failure(snapshot, helper_health, logs), "timestamp_read")

    def test_classify_failure_generation_failed(self) -> None:
        snapshot = {
            "batch": {"state": "failed_generation"},
            "accounts": [
                {
                    "id": 5,
                    "batch_state": "generation_failed",
                    "phase_detail": "page.screenshot: Protocol error",
                }
            ],
        }
        helper_health = {"state": {"state": "idle"}}
        logs: list[str] = []
        self.assertEqual(_classify_failure(snapshot, helper_health, logs), "generation_failed")

    def test_scp_to_remote_normalizes_remote_permissions(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / "clip.mp4"
            local_path.write_bytes(b"video")
            ssh_calls: list[str] = []

            def _fake_ssh_run(target: str, *, port: int, key_path: str, command: str, timeout: int = 30) -> str:
                self.assertEqual(target, "root@example.test")
                self.assertEqual(port, 49297)
                self.assertEqual(key_path, "/tmp/test-key")
                ssh_calls.append(command)
                return ""

            with (
                patch("scripts.publishing.publish_canary._ssh_run", side_effect=_fake_ssh_run),
                patch("scripts.publishing.publish_canary.subprocess.run") as subprocess_run,
            ):
                _scp_to_remote(
                    local_path,
                    "/srv/slezhka/shared/publish_staging/77/clip.mp4",
                    target="root@example.test",
                    port=49297,
                    key_path="/tmp/test-key",
                )

            self.assertEqual(len(ssh_calls), 2)
            self.assertIn("mkdir -p /srv/slezhka/shared/publish_staging/77", ssh_calls[0])
            self.assertIn("chown slezhka:slezhka /srv/slezhka/shared/publish_staging/77", ssh_calls[0])
            self.assertIn("chmod 2770 /srv/slezhka/shared/publish_staging/77", ssh_calls[0])
            self.assertIn("chown slezhka:slezhka /srv/slezhka/shared/publish_staging/77/clip.mp4", ssh_calls[1])
            self.assertIn("chmod 0640 /srv/slezhka/shared/publish_staging/77/clip.mp4", ssh_calls[1])
            subprocess_run.assert_called_once()
            scp_cmd = subprocess_run.call_args.args[0]
            self.assertEqual(scp_cmd[:5], ["scp", "-i", "/tmp/test-key", "-P", "49297"])
            self.assertEqual(scp_cmd[-2], str(local_path))
            self.assertEqual(scp_cmd[-1], "root@example.test:/srv/slezhka/shared/publish_staging/77/clip.mp4")

    def test_admin_client_batch_progress_retries_transient_disconnects(self) -> None:
        client = AdminClient("http://example.test", "admin", "secret", timeout=5)

        class DummyResponse:
            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, object]:
                return {"ok": True}

        with (
            patch.object(client.session, "request", side_effect=[requests.ConnectionError("boom"), DummyResponse()]) as request_mock,
            patch("scripts.publishing.publish_canary.time.sleep") as sleep_mock,
        ):
            payload = client.batch_progress(44)

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(request_mock.call_count, 2)
        sleep_mock.assert_called_once()

    def test_should_upload_existing_video_skips_resume_without_explicit_flag(self) -> None:
        self.assertFalse(
            _should_upload_existing_video(
                launch_mode="existing_video",
                resuming_existing_batch=True,
                upload_existing_video_on_resume=False,
            )
        )
        self.assertTrue(
            _should_upload_existing_video(
                launch_mode="existing_video",
                resuming_existing_batch=True,
                upload_existing_video_on_resume=True,
            )
        )
        self.assertTrue(
            _should_upload_existing_video(
                launch_mode="existing_video",
                resuming_existing_batch=False,
                upload_existing_video_on_resume=False,
            )
        )

    def test_build_parser_defaults_to_generated_launch_mode(self) -> None:
        parser = _build_parser()
        args = parser.parse_args([])
        self.assertEqual(args.launch_mode, "generated")
        self.assertFalse(args.upload_existing_video_on_resume)


if __name__ == "__main__":
    unittest.main()
