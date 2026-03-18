import importlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

from fastapi.testclient import TestClient


class InstagramAppHelperTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        import instagram_app_helper as helper_module

        cls.helper = importlib.reload(helper_module)

    def test_ensure_signed_out_session_skips_clear_when_login_form_visible(self) -> None:
        with (
            patch.object(self.helper, "_login_form_visible", return_value=True),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=False),
            patch.object(self.helper, "_clear_instagram_data") as clear_mock,
        ):
            result = self.helper._ensure_signed_out_instagram_session(object(), "emulator-5554")
        self.assertEqual(result, "already_signed_out")
        clear_mock.assert_not_called()

    def test_ensure_signed_out_session_uses_ui_logout_before_clear(self) -> None:
        with (
            patch.object(self.helper, "_login_form_visible", return_value=False),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=False),
            patch.object(self.helper, "_logout_instagram_ui", return_value=True),
            patch.object(self.helper, "_clear_instagram_data") as clear_mock,
        ):
            result = self.helper._ensure_signed_out_instagram_session(object(), "emulator-5554")
        self.assertEqual(result, "ui_logout")
        clear_mock.assert_not_called()

    def test_ensure_signed_out_session_falls_back_to_clear_if_logout_failed(self) -> None:
        with (
            patch.object(self.helper, "_login_form_visible", return_value=False),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=False),
            patch.object(self.helper, "_logout_instagram_ui", return_value=False),
            patch.object(self.helper, "_open_login_entrypoint", return_value=False),
            patch.object(self.helper, "_clear_instagram_data") as clear_mock,
        ):
            result = self.helper._ensure_signed_out_instagram_session(object(), "emulator-5554")
        self.assertEqual(result, "app_data_cleared")
        clear_mock.assert_called_once_with("emulator-5554")

    def test_ensure_emulator_ready_treats_default_slot_as_no_preference(self) -> None:
        with (
            patch.object(self.helper, "_list_running_emulators", return_value=["emulator-5554"]),
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper, "_wait_for_boot"),
            patch.object(self.helper, "_stabilize_emulator"),
        ):
            result = self.helper._ensure_emulator_ready("default")
        self.assertEqual(result, "emulator-5554")

    def test_open_login_entrypoint_uses_existing_account_link_for_signup_flow(self) -> None:
        with (
            patch.object(self.helper, "_click_first", return_value=False),
            patch.object(self.helper, "_find_first", return_value=object()),
            patch.object(self.helper, "_device_display_size", return_value=(1080, 1920)),
            patch.object(self.helper, "_adb_tap") as tap_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._open_login_entrypoint(object(), "emulator-5554")
        self.assertTrue(result)
        tap_mock.assert_called_once_with("emulator-5554", 540, int(1920 * 0.93))

    def test_detect_post_login_state_success_after_totp(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=True),
            patch.object(self.helper, "_handle_post_login_prompts", return_value=True),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=False),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554", "JBSWY3DPEHPK3PXP")
        self.assertEqual(state, "login_submitted")
        self.assertIn("2FA", detail)

    def test_normalize_twofa_secret_accepts_otpauth_uri(self) -> None:
        value = self.helper._normalize_twofa_secret(
            "otpauth://totp/Instagram:ayugram?secret=JBSWY3DPEHPK3PXP&issuer=Instagram"
        )
        self.assertEqual(value, "JBSWY3DPEHPK3PXP")

    def test_generate_current_twofa_code_zero_pads_short_value(self) -> None:
        totp = MagicMock()
        totp.digits = 6
        totp.now.return_value = "12345"
        pyotp_mock = MagicMock()
        pyotp_mock.TOTP.return_value = totp
        with patch.object(self.helper, "pyotp", pyotp_mock):
            value = self.helper._generate_current_twofa_code("JBSWY3DPEHPK3PXP")
        self.assertEqual(value, "012345")

    def test_maybe_submit_twofa_uses_digit_keyevents_and_keeps_leading_zero(self) -> None:
        field = MagicMock()
        totp = MagicMock()
        totp.digits = 6
        totp.now.return_value = "12345"
        pyotp_mock = MagicMock()
        pyotp_mock.TOTP.return_value = totp
        with (
            patch.object(self.helper, "pyotp", pyotp_mock),
            patch.object(self.helper, "_twofa_secret_is_valid", return_value=True),
            patch.object(self.helper, "_twofa_prompt_visible", return_value=True),
            patch.object(self.helper, "_find_first", return_value=field),
            patch.object(self.helper, "_click_first", return_value=True),
            patch.object(self.helper, "_adb_shell") as adb_shell_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            self.assertTrue(self.helper._maybe_submit_twofa(MagicMock(), "emulator-5554", "JBSWY3DPEHPK3PXP"))
        digit_events = [
            call.args[3]
            for call in adb_shell_mock.call_args_list
            if len(call.args) >= 4 and call.args[1:3] == ("input", "keyevent")
        ]
        self.assertEqual(digit_events[:6], ["7", "8", "9", "10", "11", "12"])

    def test_detect_post_login_state_invalid_password(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_ig_find_first", side_effect=[object()]),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "invalid_password")
        self.assertIn("пароль", detail)

    def test_detect_post_login_state_challenge_required(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=False),
            patch.object(self.helper, "_mail_code_challenge_visible", return_value=False),
            patch.object(self.helper, "_human_check_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=[None, None, object()]),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "challenge_required")
        self.assertIn("Fully-auto", detail)

    def test_detect_post_login_state_mail_code_challenge_prefers_mail_flow(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=False),
            patch.object(self.helper, "_mail_code_challenge_visible", return_value=True),
            patch.object(self.helper, "_ig_find_first", return_value=None),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "challenge_required")
        self.assertIn("код из письма", detail)

    def test_detect_post_login_state_human_check_returns_manual_blocker(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=False),
            patch.object(self.helper, "_mail_code_challenge_visible", return_value=False),
            patch.object(self.helper, "_human_check_visible", return_value=True),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "challenge_required")
        self.assertIn("human", detail.lower())

    def test_classify_login_challenge_screen_detects_channel_choice(self) -> None:
        channel_choice_xml = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node index="0" text="" resource-id="" class="android.widget.FrameLayout" package="com.instagram.android" content-desc="" bounds="[0,0][1080,1920]">
    <node index="1" text="Confirm it's you" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[195,285][886,369]" />
    <node index="2" text="+223 ** ** ** 13" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,540][571,618]" />
    <node index="3" text="Phone Number" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,621][446,691]" />
    <node index="4" text="h*****s@d*****.com" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,736][673,816]" />
    <node index="5" text="Email" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,818][292,889]" />
    <node index="6" text="Continue" resource-id="" class="android.widget.Button" package="com.instagram.android" content-desc="Continue" bounds="[39,1641][1041,1735]" />
  </node>
</hierarchy>
"""

        class FakeDevice:
            def dump_hierarchy(self, compressed: bool = False):
                return channel_choice_xml

        with (
            patch.object(self.helper, "_find_instagram_code_field", return_value=None),
            patch.object(self.helper, "_ig_find_first", return_value=None),
        ):
            kind, detail = self.helper._classify_login_challenge_screen(FakeDevice(), "emulator-5554")
        self.assertEqual(kind, "channel_choice")
        self.assertIn("email", detail.lower())

    def test_classify_login_challenge_screen_detects_phone_only(self) -> None:
        phone_only_xml = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node index="0" text="" resource-id="" class="android.widget.FrameLayout" package="com.instagram.android" content-desc="" bounds="[0,0][1080,1920]">
    <node index="1" text="Confirm it's you" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[195,285][886,369]" />
    <node index="2" text="+223 ** ** ** 13" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,540][571,618]" />
    <node index="3" text="Phone Number" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[134,621][446,691]" />
    <node index="4" text="Continue" resource-id="" class="android.widget.Button" package="com.instagram.android" content-desc="Continue" bounds="[39,1641][1041,1735]" />
    <node index="5" text="I don't have access to my phone or email" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[140,1760][950,1835]" />
  </node>
</hierarchy>
"""

        class FakeDevice:
            def dump_hierarchy(self, compressed: bool = False):
                return phone_only_xml

        with (
            patch.object(self.helper, "_find_instagram_code_field", return_value=None),
            patch.object(self.helper, "_ig_find_first", return_value=None),
        ):
            kind, detail = self.helper._classify_login_challenge_screen(FakeDevice(), "emulator-5554")
        self.assertEqual(kind, "phone_only")
        self.assertIn("phone", detail.lower())

    def test_select_instagram_email_challenge_option_uses_try_another_way(self) -> None:
        with (
            patch.object(self.helper, "_tap_instagram_challenge_email_option", side_effect=[False, True]) as tap_mock,
            patch.object(self.helper, "_ig_click_first", return_value=True) as click_mock,
            patch.object(self.helper, "_press_instagram_challenge_continue", return_value=True) as continue_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            selected, detail = self.helper._select_instagram_email_challenge_option(object(), "emulator-5554")
        self.assertTrue(selected)
        self.assertIn("Try another way", detail)
        self.assertEqual(tap_mock.call_count, 2)
        click_mock.assert_called_once()
        continue_mock.assert_called_once()

    def test_detect_post_login_state_signed_out_loop_requests_retry(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=False),
            patch.object(self.helper, "_mail_code_challenge_visible", return_value=False),
            patch.object(self.helper, "_human_check_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=[None, None, None]),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=True),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "login_failed")
        self.assertIn("Повторю вход", detail)

    def test_detect_post_login_state_foreground_lost_is_blocker(self) -> None:
        with (
            patch.object(self.helper.time, "sleep", return_value=None),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_maybe_submit_twofa", return_value=False),
            patch.object(self.helper, "_mail_code_challenge_visible", return_value=False),
            patch.object(self.helper, "_human_check_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=[None, None, None]),
            patch.object(self.helper, "_signed_out_surface_visible", return_value=False),
            patch.object(self.helper, "_handle_post_login_prompts", return_value=False),
            patch.object(self.helper, "_login_form_visible", return_value=False),
            patch.object(self.helper, "_instagram_is_foreground", return_value=False),
        ):
            state, detail = self.helper._detect_post_login_state(object(), "emulator-5554")
        self.assertEqual(state, "challenge_required")
        self.assertIn("foreground", detail)

    def test_post_publish_share_sheet_visible_detects_direct_private_share_dump(self) -> None:
        share_sheet_xml = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node index="0" text="" resource-id="com.instagram.android:id/direct_private_share_container_view" class="android.view.View" package="com.instagram.android" content-desc="" bounds="[0,741][1080,1857]">
    <node index="1" text="Links you share are unique to you and may be used to improve suggestions and ads you see. Learn more" resource-id="com.instagram.android:id/link_tracking_disclosure_text_view" class="android.widget.TextView" package="com.instagram.android" content-desc="Links you share are unique to you and may be used to improve suggestions and ads you see. Learn more" bounds="[26,741][1054,835]" />
    <node index="2" text="Search" resource-id="" class="android.widget.TextView" package="com.instagram.android" content-desc="" bounds="[168,856][880,972]" />
    <node index="3" text="Add to story" resource-id="com.instagram.android:id/label" class="android.widget.TextView" package="com.instagram.android" content-desc="Add to story" bounds="[31,1736][203,1815]" />
    <node index="4" text="Copy link" resource-id="com.instagram.android:id/label" class="android.widget.TextView" package="com.instagram.android" content-desc="Copy link" bounds="[643,1736][773,1815]" />
  </node>
</hierarchy>
"""

        class FakeDevice:
            def __call__(self, **kwargs):
                raise AssertionError("selector lookup should be mocked in this test")

            def dump_hierarchy(self, compressed: bool = False):
                return share_sheet_xml

        with patch.object(self.helper, "_ig_find_first", return_value=None):
            self.assertTrue(self.helper._post_publish_share_sheet_visible(FakeDevice()))

    def test_published_reel_viewer_visible_detects_insights_and_boost(self) -> None:
        published_reel_xml = """<?xml version='1.0' encoding='UTF-8' standalone='yes' ?>
<hierarchy rotation="0">
  <node index="0" text="" resource-id="" class="android.widget.FrameLayout" package="com.instagram.android" content-desc="" bounds="[0,0][1080,1920]">
    <node index="1" text="Insights" resource-id="" class="android.widget.Button" package="com.instagram.android" content-desc="Insights" bounds="[785,1775][920,1855]" />
    <node index="2" text="Boost" resource-id="" class="android.widget.Button" package="com.instagram.android" content-desc="Boost" bounds="[942,1775][1036,1855]" />
  </node>
</hierarchy>
"""

        class FakeDevice:
            def __call__(self, **kwargs):
                raise AssertionError("selector lookup should be mocked in this test")

            def dump_hierarchy(self, compressed: bool = False):
                return published_reel_xml

        with (
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_bottom_nav_visible", return_value=True),
            patch.object(self.helper, "_reel_viewer_visible", return_value=False),
        ):
            self.assertTrue(self.helper._published_reel_viewer_visible(FakeDevice()))

    def test_run_login_flow_reuses_existing_session_without_relogin(self) -> None:
        payload = {
            "account_id": 5,
            "account_login": "ayugram_sed",
            "account_password": "secret-pass",
            "username": "ayugram_sed",
            "target": "publish_batch_job",
        }
        with (
            patch.object(self.helper, "_ensure_emulator_ready", return_value="emulator-5554"),
            patch.object(self.helper, "_ensure_instagram_installed"),
            patch.object(self.helper, "_launch_instagram_app"),
            patch.object(self.helper, "_connect_ui", return_value=object()),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_wait_for_logged_in_surface", return_value=True),
            patch.object(self.helper, "_ensure_signed_out_instagram_session") as prepare_mock,
            patch.object(self.helper, "_fill_credentials_and_submit") as fill_mock,
            patch.object(self.helper, "_push_account_launch_status") as push_mock,
            patch.object(self.helper, "_set_state"),
        ):
            result = self.helper._run_login_flow(payload)
        self.assertEqual(result["state"], "login_submitted")
        self.assertIn("активной сессии", result["detail"])
        prepare_mock.assert_not_called()
        fill_mock.assert_not_called()
        push_mock.assert_called_once()

    def test_run_login_flow_applies_mail_code_challenge(self) -> None:
        payload = {
            "account_id": 7,
            "account_login": "mail_code_user",
            "account_password": "secret-pass",
            "username": "mail_code_user",
            "target": "instagram_app_login",
            "ticket": "mail-ticket",
            "mail_enabled": True,
            "mail_address": "mail_code_user@example.com",
            "mail_provider": "auto",
        }
        resolved_mail = {
            "status": "resolved",
            "kind": "numeric_code",
            "code": "123456",
            "masked_code": "123***",
            "message_uid": "uid-1",
            "received_at": 1700000000,
            "confidence": 0.92,
            "reason_code": "mail_code_ready",
            "reason_text": "Найден свежий код.",
        }
        with (
            patch.object(self.helper, "_ensure_emulator_ready", return_value="emulator-5554"),
            patch.object(self.helper, "_ensure_instagram_installed"),
            patch.object(self.helper, "_launch_instagram_app"),
            patch.object(self.helper, "_connect_ui", return_value=object()),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_wait_for_logged_in_surface", return_value=False),
            patch.object(self.helper, "_ensure_signed_out_instagram_session", return_value="already_signed_out"),
            patch.object(self.helper, "_fill_credentials_and_submit"),
            patch.object(
                self.helper,
                "_detect_post_login_state",
                side_effect=[
                    ("challenge_required", "Instagram запросил challenge."),
                    ("login_submitted", "Вход выполнен."),
                ],
            ),
            patch.object(self.helper, "_classify_login_challenge_screen", return_value=("numeric_code", "Есть поле ввода кода.")),
            patch.object(self.helper, "_resolve_account_mail_challenge", return_value=resolved_mail) as resolve_mock,
            patch.object(self.helper, "_submit_instagram_email_code", return_value=True) as submit_mock,
            patch.object(self.helper, "_push_account_launch_status") as push_mock,
            patch.object(self.helper, "_set_state"),
        ):
            result = self.helper._run_login_flow(payload)
        self.assertEqual(result["state"], "login_submitted")
        self.assertIn("Код из почты введён автоматически", result["detail"])
        resolve_mock.assert_called_once()
        submit_mock.assert_called_once_with(ANY, "emulator-5554", "123456")
        push_mock.assert_called_once()
        call_kwargs = push_mock.call_args.kwargs
        self.assertEqual(call_kwargs["mail_challenge"]["reason_code"], "mail_code_applied")
        self.assertEqual(call_kwargs["mail_challenge"]["masked_code"], "123***")

    def test_attempt_mail_challenge_login_selects_email_channel_and_applies_code(self) -> None:
        payload = {
            "account_id": 7,
            "ticket": "mail-ticket",
            "mail_enabled": True,
            "twofa": "",
        }
        resolved_mail = {
            "status": "resolved",
            "kind": "numeric_code",
            "code": "123456",
            "masked_code": "123***",
            "message_uid": "uid-1",
            "received_at": 1700000000,
            "confidence": 0.92,
            "reason_code": "mail_code_ready",
            "reason_text": "Найден свежий код.",
        }
        with (
            patch.object(self.helper, "_classify_login_challenge_screen", side_effect=[("channel_choice", "Выбор канала."), ("numeric_code", "Есть поле ввода кода.")]),
            patch.object(
                self.helper,
                "_challenge_delivery_option_nodes",
                return_value={"email_nodes": [], "phone_nodes": [], "continue_nodes": [], "manual_recovery_nodes": []},
            ),
            patch.object(self.helper, "_select_instagram_email_challenge_option", return_value=(True, "На challenge-экране выбран email-канал.")),
            patch.object(self.helper, "_wait_for_logged_in_surface", return_value=False),
            patch.object(self.helper, "_resolve_account_mail_challenge", return_value=resolved_mail) as resolve_mock,
            patch.object(self.helper, "_submit_instagram_email_code", return_value=True) as submit_mock,
            patch.object(self.helper, "_detect_post_login_state", return_value=("login_submitted", "Вход выполнен.")),
        ):
            state, detail, snapshot = self.helper._attempt_mail_challenge_login(
                object(),
                "emulator-5554",
                payload,
                challenge_started_at=1700000000,
                initial_detail="Instagram запросил challenge.",
            )
        self.assertEqual(state, "login_submitted")
        self.assertIn("Код из почты введён автоматически", detail)
        self.assertEqual(snapshot["reason_code"], "mail_code_applied")
        resolve_mock.assert_called_once()
        self.assertEqual(resolve_mock.call_args.kwargs["screen_kind"], "numeric_code")
        submit_mock.assert_called_once_with(ANY, "emulator-5554", "123456")

    def test_attempt_mail_challenge_login_opens_approval_link(self) -> None:
        payload = {
            "account_id": 8,
            "ticket": "approval-ticket",
            "mail_enabled": True,
            "twofa": "",
        }
        resolved_mail = {
            "status": "resolved",
            "kind": "approval_link",
            "link_url": "https://www.instagram.com/_n/mainfeed?approve=1",
            "message_uid": "uid-2",
            "received_at": 1700000000,
            "confidence": 0.84,
            "reason_code": "approval_link_ready",
            "reason_text": "Найдена ссылка подтверждения.",
        }
        with (
            patch.object(self.helper, "_classify_login_challenge_screen", return_value=("approval", "Подтверди вход.")),
            patch.object(
                self.helper,
                "_challenge_delivery_option_nodes",
                return_value={"email_nodes": [], "phone_nodes": [], "continue_nodes": [], "manual_recovery_nodes": []},
            ),
            patch.object(self.helper, "_select_instagram_email_challenge_option", return_value=(False, "")),
            patch.object(self.helper, "_resolve_account_mail_challenge", return_value=resolved_mail) as resolve_mock,
            patch.object(self.helper, "_apply_instagram_approval_link", return_value=("login_submitted", "Вход выполнен по ссылке.", "https://www.instagram.com/_n/mainfeed?approve=1")) as apply_mock,
        ):
            state, detail, snapshot = self.helper._attempt_mail_challenge_login(
                object(),
                "emulator-5554",
                payload,
                challenge_started_at=1700000000,
                initial_detail="Instagram запросил challenge.",
            )
        self.assertEqual(state, "login_submitted")
        self.assertIn("Ссылка из письма открыта автоматически", detail)
        self.assertEqual(snapshot["reason_code"], "approval_link_applied")
        self.assertEqual(resolve_mock.call_args.kwargs["screen_kind"], "approval")
        apply_mock.assert_called_once()

    def test_attempt_mail_challenge_login_marks_phone_only_recovery(self) -> None:
        payload = {
            "account_id": 9,
            "ticket": "phone-only-ticket",
            "mail_enabled": True,
            "twofa": "",
        }
        with (
            patch.object(self.helper, "_classify_login_challenge_screen", return_value=("phone_only", "Instagram предлагает только phone/manual recovery без email-варианта.")),
            patch.object(
                self.helper,
                "_challenge_delivery_option_nodes",
                return_value={"email_nodes": [], "phone_nodes": [{}], "continue_nodes": [{}], "manual_recovery_nodes": [{}]},
            ),
        ):
            state, detail, snapshot = self.helper._attempt_mail_challenge_login(
                object(),
                "emulator-5554",
                payload,
                challenge_started_at=1700000000,
                initial_detail="Instagram запросил challenge.",
            )
        self.assertEqual(state, "challenge_required")
        self.assertIn("phone/manual recovery", detail)
        self.assertEqual(snapshot["reason_code"], "challenge_manual_recovery_only")

    def test_publish_status_from_login_state_marks_numeric_mail_challenge_as_email_code_required(self) -> None:
        status = self.helper._publish_status_from_login_state(
            "challenge_required",
            {"kind": "numeric_code", "reason_code": "mail_not_found"},
        )
        self.assertEqual(status, "email_code_required")

    def test_run_login_flow_requests_new_mail_code_after_not_found(self) -> None:
        payload = {
            "account_id": 5,
            "account_login": "ayugram_sed",
            "account_password": "secret-pass",
            "username": "ayugram_sed",
            "target": "publish_batch_job",
            "mail_enabled": True,
            "ticket": "ticket-123",
        }
        first_mail = {
            "status": "not_found",
            "kind": "unsupported",
            "reason_code": "mail_not_found",
            "reason_text": "Свежих писем не найдено.",
        }
        second_mail = {
            "status": "resolved",
            "kind": "numeric_code",
            "code": "123456",
            "masked_code": "123***",
            "reason_code": "mail_code_ready",
            "reason_text": "Найден свежий код.",
            "message_uid": "mail-uid-2",
            "received_at": 1234567890,
            "confidence": 0.91,
        }
        with (
            patch.object(self.helper, "_ensure_emulator_ready", return_value="emulator-5554"),
            patch.object(self.helper, "_ensure_instagram_installed"),
            patch.object(self.helper, "_launch_instagram_app"),
            patch.object(self.helper, "_connect_ui", return_value=object()),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_wait_for_logged_in_surface", return_value=False),
            patch.object(self.helper, "_ensure_signed_out_instagram_session", return_value="already_signed_out"),
            patch.object(self.helper, "_fill_credentials_and_submit"),
            patch.object(
                self.helper,
                "_detect_post_login_state",
                side_effect=[
                    ("challenge_required", "Instagram запросил challenge."),
                    ("login_submitted", "Вход выполнен."),
                ],
            ),
            patch.object(self.helper, "_classify_login_challenge_screen", return_value=("numeric_code", "Есть поле ввода кода.")),
            patch.object(self.helper, "_resolve_account_mail_challenge", side_effect=[first_mail, second_mail]) as resolve_mock,
            patch.object(self.helper, "_request_instagram_new_email_code", return_value=True) as resend_mock,
            patch.object(self.helper, "_submit_instagram_email_code", return_value=True) as submit_mock,
            patch.object(self.helper, "_push_account_launch_status") as push_mock,
            patch.object(self.helper, "_set_state"),
        ):
            result = self.helper._run_login_flow(payload)
        self.assertEqual(result["state"], "login_submitted")
        self.assertIn("Код из почты введён автоматически", result["detail"])
        self.assertEqual(resolve_mock.call_count, 2)
        resend_mock.assert_called_once()
        submit_mock.assert_called_once_with(ANY, "emulator-5554", "123456")
        push_mock.assert_called_once()
        call_kwargs = push_mock.call_args.kwargs
        self.assertEqual(call_kwargs["mail_challenge"]["reason_code"], "mail_code_applied")

    def test_publish_status_from_login_state_marks_missing_mailbox_credentials_as_email_code_required(self) -> None:
        status = self.helper._publish_status_from_login_state(
            "challenge_required",
            {"kind": "unsupported", "reason_code": "mailbox_missing_credentials"},
        )
        self.assertEqual(status, "email_code_required")

    def test_publish_status_from_login_state_keeps_generic_challenge_for_non_mail_flow(self) -> None:
        status = self.helper._publish_status_from_login_state(
            "challenge_required",
            {"kind": "approval_link", "reason_code": "challenge_requires_link"},
        )
        self.assertEqual(status, "challenge_required")

    def test_run_login_flow_force_clean_login_clears_app_and_skips_session_reuse(self) -> None:
        payload = {
            "account_id": 5,
            "account_login": "ayugram_sed",
            "account_password": "secret-pass",
            "username": "ayugram_sed",
            "target": "publish_batch_job",
            "force_clean_login": True,
        }
        with (
            patch.object(self.helper, "_ensure_emulator_ready", return_value="emulator-5554"),
            patch.object(self.helper, "_ensure_instagram_installed"),
            patch.object(self.helper, "_launch_instagram_app"),
            patch.object(self.helper, "_connect_ui", return_value=object()),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_clear_instagram_data") as clear_mock,
            patch.object(self.helper, "_wait_for_logged_in_surface") as wait_logged_mock,
            patch.object(self.helper, "_ensure_signed_out_instagram_session", return_value="already_signed_out"),
            patch.object(self.helper, "_fill_credentials_and_submit"),
            patch.object(self.helper, "_detect_post_login_state", return_value=("login_submitted", "Вход выполнен.")),
            patch.object(self.helper, "_push_account_launch_status") as push_mock,
            patch.object(self.helper, "_set_state"),
        ):
            result = self.helper._run_login_flow(payload)
        self.assertEqual(result["state"], "login_submitted")
        clear_mock.assert_called_once_with("emulator-5554")
        wait_logged_mock.assert_not_called()
        push_mock.assert_called_once()

    def test_reset_publish_emulator_boundary_clears_instagram_and_kills_processes(self) -> None:
        process = MagicMock()
        process.poll.side_effect = [None, 0]
        self.helper.EMULATOR_PROCESSES.clear()
        self.helper.EMULATOR_PROCESSES["Pixel_8"] = process
        try:
            with (
                patch.object(self.helper, "_list_running_emulators", side_effect=[["emulator-5554"], []]),
                patch.object(self.helper, "_resolve_adb_path", return_value="/usr/bin/adb"),
                patch.object(self.helper, "_clear_instagram_data") as clear_mock,
                patch.object(self.helper, "_set_state") as set_state_mock,
                patch.object(self.helper, "_run") as run_mock,
            ):
                stopped = self.helper._reset_publish_emulator_boundary("emulator-5554", clear_instagram=True)
        finally:
            self.helper.EMULATOR_PROCESSES.clear()
        self.assertEqual(stopped, ["emulator-5554"])
        clear_mock.assert_called_once_with("emulator-5554")
        process.terminate.assert_called_once()
        run_calls = run_mock.call_args_list
        self.assertEqual(len(run_calls), 2)
        self.assertEqual(run_calls[0].args[0], ["/usr/bin/adb", "-s", "emulator-5554", "shell", "am", "force-stop", "com.instagram.android"])
        self.assertEqual(run_calls[1].args[0], ["/usr/bin/adb", "-s", "emulator-5554", "emu", "kill"])
        self.assertEqual(self.helper.EMULATOR_PROCESSES, {})
        set_state_mock.assert_called_once_with(emulator_serial="")

    def test_run_publish_job_enforces_boundary_reset_and_clean_login_before_terminal_push(self) -> None:
        login_result = {
            "state": "manual_2fa_required",
            "detail": "Нужен код 2FA.",
            "serial": "emulator-5554",
            "device": None,
        }
        job = {
            "id": 31,
            "account_id": 48,
            "batch_id": 47,
            "source_path": "/tmp/video.mp4",
            "emulator_serial": "emulator-5554",
            "account_login": "ayugram_king",
            "account_password": "secret-pass",
            "username": "ayugram_king",
            "twofa": "",
        }
        with (
            patch.object(self.helper, "_resolve_publish_job_source", return_value={"path": "/tmp/video.mp4", "name": "video.mp4"}),
            patch.object(self.helper, "_publish_boundary_reset_needed", return_value=True),
            patch.object(self.helper, "_reset_publish_emulator_boundary") as reset_mock,
            patch.object(self.helper, "_run_login_flow", return_value=login_result) as login_mock,
            patch.object(self.helper, "_push_publish_job_status") as push_mock,
            patch.object(self.helper, "_set_state") as set_state_mock,
            patch.object(self.helper, "_capture_publish_diagnostics"),
        ):
            self.helper._run_publish_job(job)
        login_payload = login_mock.call_args.args[0]
        self.assertTrue(login_payload["force_clean_login"])
        self.assertEqual(reset_mock.call_count, 2)
        self.assertEqual(push_mock.call_args_list[-1].args[1], "failed")
        self.assertEqual(push_mock.call_args_list[-1].kwargs["account_publish_state"], "manual_2fa_required")
        self.assertEqual(set_state_mock.call_args_list[-1].kwargs["state"], "idle")
        self.assertEqual(set_state_mock.call_args_list[-1].kwargs["emulator_serial"], "")

    def test_run_publish_job_pushes_mail_challenge_payload_when_login_needs_email_code(self) -> None:
        login_result = {
            "state": "challenge_required",
            "detail": "Свежих писем не найдено.",
            "serial": "emulator-5554",
            "device": None,
            "mail_challenge": {
                "status": "not_found",
                "kind": "numeric_code",
                "reason_code": "mail_not_found",
                "reason_text": "Свежих писем не найдено.",
                "confidence": 0.41,
            },
        }
        job = {
            "id": 33,
            "account_id": 49,
            "batch_id": 48,
            "source_path": "/tmp/video.mp4",
            "emulator_serial": "emulator-5554",
            "account_login": "ayugram_mail",
            "account_password": "secret-pass",
            "username": "ayugram_mail",
            "twofa": "",
            "mail_enabled": True,
            "mail_address": "ayugram_mail@example.com",
            "mail_provider": "auto",
        }
        with (
            patch.object(self.helper, "_resolve_publish_job_source", return_value={"path": "/tmp/video.mp4", "name": "video.mp4"}),
            patch.object(self.helper, "_publish_boundary_reset_needed", return_value=False),
            patch.object(self.helper, "_reset_publish_emulator_boundary"),
            patch.object(self.helper, "_run_login_flow", return_value=login_result) as login_mock,
            patch.object(self.helper, "_push_publish_job_status") as push_mock,
            patch.object(self.helper, "_set_state"),
            patch.object(self.helper, "_capture_publish_diagnostics"),
        ):
            self.helper._run_publish_job(job)
        login_payload = login_mock.call_args.args[0]
        self.assertTrue(login_payload["mail_enabled"])
        self.assertEqual(login_payload["mail_address"], "ayugram_mail@example.com")
        self.assertEqual(login_payload["mail_provider"], "auto")
        final_call = push_mock.call_args_list[-1]
        self.assertEqual(final_call.args[1], "failed")
        self.assertEqual(final_call.kwargs["account_publish_state"], "email_code_required")
        self.assertEqual(final_call.kwargs["payload"]["reason_code"], "email_code_required")
        self.assertEqual(final_call.kwargs["payload"]["mail_challenge"]["status"], "not_found")
        self.assertEqual(final_call.kwargs["payload"]["mail_challenge"]["kind"], "numeric_code")
        self.assertEqual(final_call.kwargs["payload"]["mail_challenge"]["reason_code"], "mail_not_found")

    def test_run_publish_job_success_publishes_only_after_cleanup(self) -> None:
        device = object()
        login_result = {
            "state": "login_submitted",
            "detail": "Вход выполнен.",
            "serial": "emulator-5554",
            "device": device,
        }
        wait_result = self.helper.PublishWaitResult(
            outcome="publish_confirmed",
            publish_phase="waiting_confirmation",
            accepted_by_instagram=True,
            elapsed_seconds=12,
            last_activity="Instagram принял Reel.",
            success=True,
            event_kind="publish_confirmation_wait",
        )
        verify_result = self.helper.ProfileVerificationResult(
            verified=True,
            detail="Найден свежий Reel.",
            publish_phase="verifying_profile",
            matched_slot=0,
            matched_age_seconds=120,
            checked_slots=1,
            event_kind="profile_verified",
            timestamp_readable=True,
        )
        job = {
            "id": 32,
            "account_id": 5,
            "batch_id": 48,
            "source_path": "/tmp/video.mp4",
            "emulator_serial": "emulator-5554",
            "account_login": "ayugram_sed",
            "account_password": "secret-pass",
            "username": "ayugram_sed",
            "twofa": "",
        }
        with (
            patch.object(self.helper, "_resolve_publish_job_source", return_value={"path": "/tmp/video.mp4", "name": "video.mp4"}),
            patch.object(self.helper, "_publish_boundary_reset_needed", side_effect=[False, True]),
            patch.object(self.helper, "_reset_publish_emulator_boundary") as reset_mock,
            patch.object(self.helper, "_run_login_flow", return_value=login_result),
            patch.object(self.helper, "_capture_profile_reels_baseline", return_value={"available": False, "candidates": []}),
            patch.object(self.helper, "_import_video_into_emulator", return_value="/sdcard/Movies/video.mp4"),
            patch.object(self.helper, "_open_reel_creation_flow"),
            patch.object(self.helper, "_select_reel_media"),
            patch.object(self.helper, "_advance_reel_next"),
            patch.object(self.helper, "_share_reel"),
            patch.object(self.helper, "_wait_for_publish_success", return_value=wait_result),
            patch.object(self.helper, "_confirm_publish_via_profile", return_value=verify_result),
            patch.object(self.helper, "_push_publish_job_status") as push_mock,
            patch.object(self.helper, "_set_state") as set_state_mock,
            patch.object(self.helper, "_capture_publish_diagnostics"),
        ):
            self.helper._run_publish_job(job)
        self.assertEqual(reset_mock.call_count, 1)
        self.assertEqual(push_mock.call_args_list[-1].args[1], "published")
        self.assertEqual(set_state_mock.call_args_list[-1].kwargs["state"], "idle")

    def test_wait_for_create_surface_recovers_when_foreground_is_lost(self) -> None:
        device = object()
        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_allow_media_permissions"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_create_surface_visible", side_effect=[False, True]),
            patch.object(self.helper, "_instagram_is_foreground", side_effect=[False, True]),
            patch.object(self.helper, "_instagram_task_has_create_surface", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.google.android.apps.nexuslauncher/.NexusLauncherActivity"),
            patch.object(self.helper, "_current_focus_window", return_value="com.google.android.apps.nexuslauncher/.NexusLauncherActivity"),
            patch.object(self.helper, "_bring_instagram_task_to_front", return_value=True) as bring_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._wait_for_create_surface(device, "emulator-5554", timeout_seconds=2.0)
        self.assertTrue(result)
        bring_mock.assert_called_once_with("emulator-5554")

    def test_recover_reel_creation_flow_brings_hidden_instagram_task_back(self) -> None:
        device = object()
        with (
            patch.object(self.helper, "_instagram_task_has_create_surface", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.google.android.apps.nexuslauncher/.NexusLauncherActivity"),
            patch.object(self.helper, "_current_focus_window", return_value="com.google.android.apps.nexuslauncher/.NexusLauncherActivity"),
            patch.object(self.helper, "_bring_instagram_task_to_front", return_value=True) as bring_mock,
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_allow_media_permissions"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_wait_for_create_surface", return_value=True),
            patch.object(self.helper, "_ensure_create_flow_open") as ensure_mock,
            patch.object(self.helper, "_switch_creation_mode_to_reel") as switch_mock,
            patch.object(self.helper, "_launch_instagram_app") as launch_mock,
        ):
            result = self.helper._recover_reel_creation_flow(device, "emulator-5554", reason="unit-test")
        self.assertTrue(result)
        bring_mock.assert_called_once_with("emulator-5554")
        ensure_mock.assert_not_called()
        switch_mock.assert_not_called()
        launch_mock.assert_not_called()

    def test_open_reel_creation_flow_uses_recovery_before_failing(self) -> None:
        device = object()
        with (
            patch.object(self.helper, "_ensure_create_flow_open"),
            patch.object(self.helper, "_switch_creation_mode_to_reel"),
            patch.object(self.helper, "_wait_for_create_surface", return_value=False),
            patch.object(self.helper, "_recover_reel_creation_flow", return_value=True) as recover_mock,
        ):
            self.helper._open_reel_creation_flow(device, "emulator-5554")
        recover_mock.assert_called_once_with(device, "emulator-5554", reason="reel_surface_missing_after_switch")

    def test_download_publish_job_source_keeps_mac_copy(self) -> None:
        class DummyResponse:
            def raise_for_status(self) -> None:
                return None

            def iter_content(self, chunk_size: int = 0):
                yield b"workflow-video"

        with tempfile.TemporaryDirectory() as cache_dir, tempfile.TemporaryDirectory() as downloads_dir:
            with (
                patch.object(self.helper, "PUBLISH_RUNNER_CACHE_DIR", cache_dir),
                patch.object(self.helper, "PUBLISH_RUNNER_DOWNLOADS_DIR", downloads_dir),
                patch.object(self.helper, "PUBLISH_RUNNER_API_KEY", "runner-key"),
                patch.object(self.helper, "SLEZHKA_ADMIN_BASE_URL", "http://admin.example"),
                patch.object(self.helper.http_utils, "request_with_retry", return_value=DummyResponse()) as get_mock,
            ):
                info = self.helper._download_publish_job_source(42, "workflow-video.mp4")
                cached_path = Path(info["path"])
                saved_path = Path(info["saved_to"])
                self.assertTrue(info["downloaded"])
                self.assertTrue(cached_path.exists())
                self.assertTrue(saved_path.exists())
                self.assertEqual(cached_path.read_bytes(), b"workflow-video")
                self.assertEqual(saved_path.read_bytes(), b"workflow-video")
                self.assertEqual(saved_path.parent, Path(downloads_dir))
                self.assertEqual(saved_path.name, "job-42-workflow-video.mp4")
                get_mock.assert_called_once()
                call_args = get_mock.call_args
                self.assertEqual(call_args.args[0], "GET")
                self.assertEqual(call_args.args[1], "http://admin.example/api/internal/publishing/jobs/42/artifact")
                self.assertEqual(call_args.kwargs["headers"], {"X-Runner-Api-Key": "runner-key"})
                self.assertTrue(call_args.kwargs["stream"])
                self.assertEqual(call_args.kwargs["timeout"], 180)

    def test_wait_for_publish_success_reports_uploading_before_timeout(self) -> None:
        clock = {"value": 0.0}

        def fake_monotonic() -> float:
            current = clock["value"]
            clock["value"] += 0.02
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += min(0.01, max(0.0, float(seconds)))

        def fake_find(_device, selectors, timeout_seconds=0):
            joined = " ".join(
                str(item.get(key, ""))
                for item in selectors
                for key in ("textMatches", "descriptionMatches", "resourceIdMatches", "resourceId")
            ).lower()
            if "sharing to reels" in joined or "sharing to reel" in joined:
                return object()
            return None

        updates = []
        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_handle_publish_confirmation_prompt"),
            patch.object(self.helper, "_instagram_is_foreground", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.instagram.android.activity.MainTabActivity"),
            patch.object(self.helper, "_home_feed_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=fake_find),
            patch.object(self.helper.time, "monotonic", side_effect=fake_monotonic),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
        ):
            result = self.helper._wait_for_publish_success(
                object(),
                "emulator-5554",
                timeout_seconds=0.2,
                heartbeat_seconds=0.05,
                on_update=updates.append,
            )
        self.assertGreaterEqual(len(updates), 2)
        self.assertEqual(updates[0].outcome, "uploading_detected")
        self.assertEqual(updates[0].publish_phase, "uploading")
        self.assertFalse(result.success)
        self.assertEqual(result.outcome, "publish_timeout")
        self.assertEqual(result.reason_code, "publish_confirmation_timeout")
        self.assertTrue(result.accepted_by_instagram)

    def test_wait_for_publish_success_returns_confirmed_on_explicit_success(self) -> None:
        def fake_find(_device, selectors, timeout_seconds=0):
            joined = " ".join(
                str(item.get(key, ""))
                for item in selectors
                for key in ("textMatches", "descriptionMatches", "resourceIdMatches", "resourceId")
            ).lower()
            if "view insights" in joined or "boost reel" in joined:
                return object()
            return None

        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_handle_publish_confirmation_prompt"),
            patch.object(self.helper, "_instagram_is_foreground", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.instagram.android.activity.MainTabActivity"),
            patch.object(self.helper, "_home_feed_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=fake_find),
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._wait_for_publish_success(object(), "emulator-5554", timeout_seconds=0.1)
        self.assertTrue(result.success)
        self.assertEqual(result.outcome, "publish_confirmed")
        self.assertEqual(result.event_kind, "publish_confirmation_wait")

    def test_wait_for_publish_success_treats_post_publish_share_sheet_as_confirmation(self) -> None:
        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_handle_publish_confirmation_prompt"),
            patch.object(self.helper, "_instagram_is_foreground", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.instagram.android.activity.MainTabActivity"),
            patch.object(self.helper, "_home_feed_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_post_publish_share_sheet_visible", return_value=True),
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._wait_for_publish_success(object(), "emulator-5554", timeout_seconds=0.1)
        self.assertTrue(result.success)
        self.assertEqual(result.outcome, "publish_confirmed")
        self.assertEqual(result.event_kind, "post_publish_share_sheet")

    def test_wait_for_publish_success_treats_published_reel_viewer_as_confirmation(self) -> None:
        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_handle_publish_confirmation_prompt"),
            patch.object(self.helper, "_instagram_is_foreground", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.instagram.android.composer.activity.ShareToFeedActivity"),
            patch.object(self.helper, "_home_feed_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_published_reel_viewer_visible", return_value=True),
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._wait_for_publish_success(object(), "emulator-5554", timeout_seconds=0.1)
        self.assertTrue(result.success)
        self.assertEqual(result.outcome, "publish_confirmed")
        self.assertEqual(result.event_kind, "published_reel_viewer")

    def test_wait_for_publish_success_returns_blocked_on_error_surface(self) -> None:
        def fake_find(_device, selectors, timeout_seconds=0):
            joined = " ".join(
                str(item.get(key, ""))
                for item in selectors
                for key in ("textMatches", "descriptionMatches", "resourceIdMatches", "resourceId")
            ).lower()
            if "couldn.t post" in joined or "retry" in joined:
                return object()
            return None

        with (
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_handle_publish_confirmation_prompt"),
            patch.object(self.helper, "_instagram_is_foreground", return_value=True),
            patch.object(self.helper, "_current_top_activity", return_value="com.instagram.android.activity.MainTabActivity"),
            patch.object(self.helper, "_home_feed_visible", return_value=False),
            patch.object(self.helper, "_ig_find_first", side_effect=fake_find),
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._wait_for_publish_success(object(), "emulator-5554", timeout_seconds=0.1)
        self.assertFalse(result.success)
        self.assertEqual(result.outcome, "publish_blocked")
        self.assertEqual(result.reason_code, "publish_blocked")

    def test_parse_relative_age_seconds_supports_ru_and_en(self) -> None:
        self.assertEqual(self.helper._parse_relative_age_seconds("just now"), 0)
        self.assertEqual(self.helper._parse_relative_age_seconds("6 minutes ago"), 360)
        self.assertEqual(self.helper._parse_relative_age_seconds("1 h ago"), 3600)
        self.assertEqual(self.helper._parse_relative_age_seconds("только что"), 0)
        self.assertEqual(self.helper._parse_relative_age_seconds("6 мин назад"), 360)
        self.assertEqual(self.helper._parse_relative_age_seconds("1 ч назад"), 3600)

    def test_confirm_publish_via_profile_verifies_new_fresh_reel(self) -> None:
        device = object()
        baseline = {
            "available": True,
            "candidates": [
                self.helper.ProfileReelCandidate(slot_index=0, age_seconds=4000, fingerprint="old-1", signature_text="old 1", opened=True),
                self.helper.ProfileReelCandidate(slot_index=1, age_seconds=9000, fingerprint="old-2", signature_text="old 2", opened=True),
            ],
        }
        with (
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_refresh_profile_reels_tab"),
            patch.object(
                self.helper,
                "_inspect_profile_reel_slot",
                side_effect=[
                    self.helper.ProfileReelCandidate(slot_index=0, age_seconds=120, age_label="2 min ago", fingerprint="new-1", signature_text="new reel", opened=True),
                    self.helper.ProfileReelCandidate(slot_index=1, age_seconds=4300, fingerprint="old-1", signature_text="old 1", opened=True),
                    self.helper.ProfileReelCandidate(slot_index=2, age_seconds=9100, fingerprint="old-2", signature_text="old 2", opened=True),
                ],
            ) as inspect_slot,
        ):
            result = self.helper._confirm_publish_via_profile(
                device,
                "emulator-5554",
                source_name="video.mp4",
                baseline=baseline,
                expected_handle="phase_user",
                elapsed_since_share_seconds=601,
                start_delay_seconds=600,
                timeout_seconds=1800,
                interval_seconds=0.01,
                freshness_seconds=1800,
            )
        self.assertTrue(result.verified)
        self.assertEqual(result.reason_code, "publish_profile_verified")
        self.assertEqual(result.matched_slot, 0)
        self.assertEqual(result.matched_age_seconds, 120)
        inspect_slot.assert_called_once_with(device, "emulator-5554", 0, expected_handle="phase_user")

    def test_confirm_publish_via_profile_marks_needs_review_for_same_fresh_baseline(self) -> None:
        baseline = {
            "available": True,
            "candidates": [
                self.helper.ProfileReelCandidate(slot_index=0, age_seconds=180, fingerprint="same-reel", signature_text="same reel", opened=True),
            ],
        }
        clock = {"value": 0.0}

        def fake_monotonic() -> float:
            current = clock["value"]
            clock["value"] += 0.2
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        with (
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_refresh_profile_reels_tab"),
            patch.object(
                self.helper,
                "_inspect_profile_reel_slot",
                side_effect=lambda *_args, **kwargs: self.helper.ProfileReelCandidate(
                    slot_index=int(_args[2]),
                    age_seconds=240 if int(_args[2]) == 0 else 7200,
                    fingerprint="same-reel" if int(_args[2]) == 0 else f"old-{int(_args[2])}",
                    signature_text="same reel" if int(_args[2]) == 0 else "older reel",
                    opened=True,
                ),
            ),
            patch.object(self.helper.time, "monotonic", side_effect=fake_monotonic),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
        ):
            result = self.helper._confirm_publish_via_profile(
                object(),
                "emulator-5554",
                source_name="video.mp4",
                baseline=baseline,
                expected_handle="phase_user",
                elapsed_since_share_seconds=601,
                start_delay_seconds=600,
                timeout_seconds=605,
                interval_seconds=0.01,
                freshness_seconds=1800,
            )
        self.assertFalse(result.verified)
        self.assertTrue(result.needs_review)
        self.assertEqual(result.event_kind, "needs_review")
        self.assertIn(result.reason_code, {"publish_profile_inconclusive", "publish_profile_not_fresh", "publish_profile_timestamp_unreadable"})

    def test_confirm_publish_via_profile_waits_before_first_profile_check(self) -> None:
        clock = {"value": 0.0}

        def fake_monotonic() -> float:
            return clock["value"]

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        updates = []
        inspect_calls = []

        def fake_inspect(*args, **kwargs):
            inspect_calls.append(clock["value"])
            return self.helper.ProfileReelCandidate(slot_index=int(args[2]), age_seconds=120, fingerprint=f"fresh-{int(args[2])}", opened=True)

        with (
            patch.object(self.helper.time, "monotonic", side_effect=fake_monotonic),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_refresh_profile_reels_tab"),
            patch.object(self.helper, "_inspect_profile_reel_slot", side_effect=fake_inspect),
        ):
            result = self.helper._confirm_publish_via_profile(
                object(),
                "emulator-5554",
                source_name="video.mp4",
                elapsed_since_share_seconds=0,
                start_delay_seconds=10,
                timeout_seconds=30,
                interval_seconds=1,
                freshness_seconds=1800,
                on_update=updates.append,
            )
        self.assertTrue(updates)
        self.assertEqual(updates[0].publish_phase, "waiting_profile_verification_window")
        self.assertEqual(updates[0].event_kind, "profile_verification_scheduled")
        self.assertTrue(inspect_calls)
        self.assertEqual(len(inspect_calls), 1)
        self.assertGreaterEqual(inspect_calls[0], 10.0)
        self.assertTrue(result.verified)
        self.assertIsNotNone(result.first_profile_check_at)
        self.assertGreaterEqual(int(result.first_profile_check_at or 0), 10)

    def test_confirm_publish_via_profile_verifies_open_published_reel_viewer(self) -> None:
        with (
            patch.object(self.helper, "_published_reel_viewer_visible", return_value=True),
            patch.object(self.helper, "_viewer_text_candidates", return_value=["View insights", "Boost post"]),
            patch.object(self.helper, "_extract_relative_age_from_texts", return_value=(None, "")),
            patch.object(self.helper, "_recover_to_profile_surface") as recover_mock,
            patch.object(self.helper, "_open_profile_reels_tab") as open_mock,
        ):
            result = self.helper._confirm_publish_via_profile(
                object(),
                "emulator-5554",
                source_name="video.mp4",
                elapsed_since_share_seconds=601,
                start_delay_seconds=600,
                timeout_seconds=1800,
                interval_seconds=0.01,
                freshness_seconds=1800,
            )
        self.assertTrue(result.verified)
        self.assertEqual(result.reason_code, "publish_viewer_verified")
        self.assertEqual(result.event_kind, "published_reel_viewer_verified")
        recover_mock.assert_not_called()
        open_mock.assert_not_called()

    def test_confirm_publish_via_profile_does_not_confirm_reel_older_than_30_minutes(self) -> None:
        clock = {"value": 0.0}

        def fake_monotonic() -> float:
            current = clock["value"]
            clock["value"] += 0.2
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        with (
            patch.object(self.helper.time, "monotonic", side_effect=fake_monotonic),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_refresh_profile_reels_tab"),
            patch.object(
                self.helper,
                "_inspect_profile_reel_slot",
                side_effect=lambda *_args, **_kwargs: self.helper.ProfileReelCandidate(
                    slot_index=int(_args[2]),
                    age_seconds=1860 if int(_args[2]) == 0 else 4000,
                    fingerprint=f"candidate-{int(_args[2])}",
                    opened=True,
                ),
            ),
        ):
            result = self.helper._confirm_publish_via_profile(
                object(),
                "emulator-5554",
                source_name="video.mp4",
                elapsed_since_share_seconds=601,
                start_delay_seconds=600,
                timeout_seconds=605,
                interval_seconds=1,
                freshness_seconds=1800,
            )
        self.assertFalse(result.verified)
        self.assertTrue(result.needs_review)
        self.assertIn(result.reason_code, {"publish_profile_not_fresh", "publish_profile_timestamp_unreadable"})

    def test_recover_to_profile_surface_closes_overlay_stack_before_success(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "keyboard",
                "profile_visible": False,
                "keyboard_visible": True,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
            },
            {
                "profile_surface_state": "comment_sheet",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": True,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
            },
            {
                "profile_surface_state": "clips_viewer",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": True,
                "quick_capture_visible": False,
            },
            {
                "profile_surface_state": "quick_capture",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": True,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_close_keyboard") as close_keyboard,
            patch.object(self.helper, "_close_comment_sheet") as close_sheet,
            patch.object(self.helper, "_close_clips_viewer") as close_viewer,
            patch.object(self.helper, "_exit_quick_capture") as exit_capture,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=5.0)
        self.assertTrue(result)
        close_keyboard.assert_called_once()
        close_sheet.assert_called_once()
        close_viewer.assert_called_once()
        exit_capture.assert_called_once()

    def test_recover_to_profile_surface_closes_share_sheet_before_success(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "post_publish_share_sheet",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "share_sheet_visible": True,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "share_sheet_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_close_post_publish_share_sheet") as close_share_sheet,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=5.0)
        self.assertTrue(result)
        close_share_sheet.assert_called_once()

    def test_recover_to_profile_surface_prefers_profile_tab_when_share_sheet_has_bottom_nav(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "post_publish_share_sheet",
                "profile_visible": False,
                "keyboard_visible": True,
                "comment_sheet_visible": True,
                "clips_viewer_visible": True,
                "quick_capture_visible": False,
                "share_sheet_visible": True,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "share_sheet_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_bottom_nav_visible", return_value=True),
            patch.object(self.helper, "_promote_surface_to_profile", return_value=True) as promote,
            patch.object(self.helper, "_close_post_publish_share_sheet") as close_share_sheet,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=5.0)
        self.assertTrue(result)
        promote.assert_called_once()
        close_share_sheet.assert_not_called()

    def test_open_profile_tab_requires_profile_markers(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.2
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_recover_to_profile_surface", return_value=False),
            patch.object(self.helper, "_tap_profile_tab") as tap_profile,
            patch.object(self.helper, "_profile_surface_visible", return_value=False),
        ):
            result = self.helper._open_profile_tab(object(), "emulator-5554", timeout_seconds=1.0)
        self.assertFalse(result)
        self.assertGreaterEqual(tap_profile.call_count, 1)

    def test_open_profile_reels_tab_requires_selected_tab_and_grid(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.2
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_open_profile_tab", return_value=True),
            patch.object(self.helper, "_profile_reels_tab_selected", return_value=True),
            patch.object(self.helper, "_profile_reels_grid_visible", return_value=False),
            patch.object(self.helper, "_comment_sheet_visible", return_value=False),
            patch.object(self.helper, "_reel_viewer_visible", return_value=False),
            patch.object(self.helper, "_quick_capture_visible", return_value=False),
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_device_display_size", return_value=(1080, 1920)),
            patch.object(self.helper, "_ig_find_first", return_value=None),
            patch.object(self.helper, "_ig_click_first", return_value=False),
            patch.object(self.helper, "_adb_tap"),
        ):
            result = self.helper._open_profile_reels_tab(object(), "emulator-5554", timeout_seconds=1.0)
        self.assertFalse(result)

    def test_profile_surface_visible_ignores_plain_unselected_profile_tab(self) -> None:
        fake_profile_tab = MagicMock()
        fake_profile_tab.info = {"selected": False}

        def fake_find_first(device, selectors, timeout_seconds=0.0):
            selector = selectors[0] if selectors else {}
            if selector.get("resourceId") == f"{self.helper.INSTAGRAM_PACKAGE}:id/profile_tab":
                return fake_profile_tab
            if selector.get("descriptionMatches") == "(?i)^profile$":
                return fake_profile_tab
            return None

        with patch.object(self.helper, "_ig_find_first", side_effect=fake_find_first):
            self.assertFalse(self.helper._profile_surface_visible(object()))

    def test_profile_surface_visible_ignores_feed_profile_header(self) -> None:
        def fake_find_first(device, selectors, timeout_seconds=0.0):
            selector = selectors[0] if selectors else {}
            if selector.get("resourceId") == f"{self.helper.INSTAGRAM_PACKAGE}:id/profile_header_actions_top_row":
                return None
            if selector.get("resourceIdMatches") == f"{self.helper.INSTAGRAM_PACKAGE}:id/(profile_header_.*|.*edit_profile.*|.*share_profile.*|row_profile_header_.*)":
                return None
            if selector.get("textMatches") == "(?i)(edit profile|share profile|professional dashboard|view archive|threads)":
                return None
            if selector.get("resourceId") == f"{self.helper.INSTAGRAM_PACKAGE}:id/profile_tab":
                return None
            if selector.get("descriptionMatches") in {"(?i)^profile$", "(?i)(profile tab|open profile)"}:
                return None
            return None

        with patch.object(self.helper, "_ig_find_first", side_effect=fake_find_first):
            self.assertFalse(self.helper._profile_surface_visible(object()))

    def test_feed_contextual_viewer_visible_ignores_main_feed_with_bottom_nav(self) -> None:
        def fake_find_first(device, selectors, timeout_seconds=0.0):
            selector = selectors[0] if selectors else {}
            resource_id = str(selector.get("resourceId") or "")
            if resource_id in {
                f"{self.helper.INSTAGRAM_PACKAGE}:id/clips_tab",
                f"{self.helper.INSTAGRAM_PACKAGE}:id/profile_tab",
                f"{self.helper.INSTAGRAM_PACKAGE}:id/row_feed_profile_header",
                f"{self.helper.INSTAGRAM_PACKAGE}:id/row_feed_photo_profile_name",
                f"{self.helper.INSTAGRAM_PACKAGE}:id/row_feed_photo_profile_imageview",
            }:
                return MagicMock()
            return None

        with patch.object(self.helper, "_ig_find_first", side_effect=fake_find_first):
            self.assertFalse(self.helper._feed_contextual_viewer_visible(object()))

    def test_profile_surface_flags_prefers_instagram_dialog(self) -> None:
        with (
            patch.object(self.helper, "_keyboard_visible", return_value=False),
            patch.object(self.helper, "_comment_sheet_visible", return_value=False),
            patch.object(self.helper, "_clips_viewer_visible", return_value=False),
            patch.object(self.helper, "_quick_capture_visible", return_value=False),
            patch.object(self.helper, "_instagram_app_rate_dialog_visible", return_value=True),
            patch.object(self.helper, "_profile_surface_visible", return_value=False),
            patch.object(self.helper, "_post_publish_feed_visible", return_value=False),
        ):
            flags = self.helper._profile_surface_flags(object(), "emulator-5554")
        self.assertEqual(flags["profile_surface_state"], "instagram_dialog")
        self.assertTrue(flags["instagram_dialog_visible"])

    def test_profile_surface_flags_detects_post_publish_feed(self) -> None:
        with (
            patch.object(self.helper, "_keyboard_visible", return_value=False),
            patch.object(self.helper, "_comment_sheet_visible", return_value=False),
            patch.object(self.helper, "_clips_viewer_visible", return_value=False),
            patch.object(self.helper, "_quick_capture_visible", return_value=False),
            patch.object(self.helper, "_instagram_app_rate_dialog_visible", return_value=False),
            patch.object(self.helper, "_profile_surface_visible", return_value=False),
            patch.object(self.helper, "_post_publish_feed_visible", return_value=True),
        ):
            flags = self.helper._profile_surface_flags(object(), "emulator-5554")
        self.assertEqual(flags["profile_surface_state"], "post_publish_feed")
        self.assertTrue(flags["post_publish_feed_visible"])

    def test_dismiss_instagram_interstitials_closes_app_rate_dialog(self) -> None:
        device = MagicMock()
        with (
            patch.object(self.helper, "_instagram_app_rate_dialog_visible", side_effect=[True, False]),
            patch.object(self.helper, "_ig_click_first", return_value=True) as click_mock,
            patch.object(self.helper, "_find_first", return_value=None),
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._dismiss_instagram_interstitials(device, "emulator-5554", timeout_seconds=0.5)
        self.assertTrue(result)
        click_mock.assert_called_once()

    def test_dismiss_instagram_interstitials_closes_create_sticker_modal(self) -> None:
        device = MagicMock()
        target = object()
        seen_target = False

        def fake_find(_device, selectors, timeout_seconds=0):
            nonlocal seen_target
            for selector in selectors:
                if (
                    not seen_target
                    and selector.get("resourceId") == f"{self.helper.INSTAGRAM_PACKAGE}:id/auxiliary_button"
                ):
                    seen_target = True
                    return target
            return None

        with (
            patch.object(self.helper, "_dismiss_instagram_app_rate_dialog", return_value=False),
            patch.object(self.helper, "_find_first", side_effect=fake_find),
            patch.object(self.helper, "_tap_object", return_value=True) as tap_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._dismiss_instagram_interstitials(device, "emulator-5554", timeout_seconds=0.5)
        self.assertTrue(result)
        tap_mock.assert_called_once_with("emulator-5554", target)

    def test_promote_surface_to_profile_taps_profile_from_post_publish_feed(self) -> None:
        with (
            patch.object(self.helper, "_profile_surface_visible", side_effect=[False, True]),
            patch.object(self.helper, "_post_publish_feed_visible", return_value=True),
            patch.object(self.helper, "_bottom_nav_visible", return_value=False),
            patch.object(self.helper, "_tap_profile_tab") as tap_profile,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._promote_surface_to_profile(object(), "emulator-5554")
        self.assertTrue(result)
        tap_profile.assert_called_once()

    def test_promote_surface_to_profile_taps_profile_from_bottom_nav_viewer(self) -> None:
        with (
            patch.object(self.helper, "_profile_surface_visible", side_effect=[False, True]),
            patch.object(self.helper, "_post_publish_feed_visible", return_value=False),
            patch.object(self.helper, "_bottom_nav_visible", return_value=True),
            patch.object(self.helper, "_tap_profile_tab") as tap_profile,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._promote_surface_to_profile(object(), "emulator-5554")
        self.assertTrue(result)
        tap_profile.assert_called_once()

    def test_inspect_profile_reel_slot_retries_timestamp_after_center_tap(self) -> None:
        with (
            patch.object(self.helper, "_profile_reels_slot_centers", return_value=[(120, 480)]),
            patch.object(self.helper, "_wait_until", return_value=True),
            patch.object(self.helper, "_normalize_open_reel_surface", return_value=True),
            patch.object(self.helper, "_viewer_text_candidates", side_effect=[["Add a comment"], ["6 min ago", "View insights"]]),
            patch.object(self.helper, "_extract_relative_age_from_texts", side_effect=[(None, ""), (360, "6 min ago")]),
            patch.object(self.helper, "_reel_signature_from_texts", return_value=("sig", "sig")),
            patch.object(self.helper, "_reel_viewer_visible", return_value=True),
            patch.object(self.helper, "_recover_to_profile_surface", return_value=True),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(self.helper, "_device_display_size", return_value=(1080, 1920)),
            patch.object(self.helper, "_adb_tap") as tap_mock,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            candidate = self.helper._inspect_profile_reel_slot(object(), "emulator-5554", 0, expected_handle="ayugram_sed")
        self.assertTrue(candidate.opened)
        self.assertEqual(candidate.age_seconds, 360)
        self.assertEqual(candidate.age_label, "6 min ago")
        self.assertGreaterEqual(tap_mock.call_count, 2)

    def test_normalize_open_reel_surface_prefers_open_viewer_over_stale_keyboard(self) -> None:
        with (
            patch.object(self.helper, "_dismiss_system_dialogs", return_value=None),
            patch.object(self.helper, "_dismiss_instagram_interstitials", return_value=False),
            patch.object(self.helper, "_comment_sheet_visible", return_value=False),
            patch.object(self.helper, "_quick_capture_visible", return_value=False),
            patch.object(self.helper, "_reel_viewer_visible", return_value=True),
            patch.object(self.helper, "_keyboard_visible", return_value=True),
            patch.object(self.helper, "_close_keyboard") as close_keyboard,
            patch.object(self.helper.time, "sleep", return_value=None),
        ):
            result = self.helper._normalize_open_reel_surface(object(), "emulator-5554", timeout_seconds=1.0)
        self.assertTrue(result)
        close_keyboard.assert_not_called()

    def test_capture_profile_reels_baseline_uses_baseline_slot_limit(self) -> None:
        device = object()
        with (
            patch.object(self.helper, "PUBLISH_PROFILE_CHECK_SLOTS", 3),
            patch.object(self.helper, "PUBLISH_PROFILE_BASELINE_SLOTS", 1),
            patch.object(self.helper, "_open_profile_reels_tab", return_value=True),
            patch.object(
                self.helper,
                "_inspect_profile_reel_slot",
                side_effect=[
                    self.helper.ProfileReelCandidate(slot_index=0, opened=True, age_seconds=3600, age_label="1 hour ago"),
                ],
            ) as inspect_slot,
        ):
            baseline = self.helper._capture_profile_reels_baseline(device, "emulator-5554", expected_handle="ayugram_sed")
        self.assertTrue(baseline["available"])
        self.assertEqual(len(baseline["candidates"]), 1)
        inspect_slot.assert_called_once_with(device, "emulator-5554", 0, expected_handle="ayugram_sed")

    def test_recover_to_profile_surface_prefers_profile_tab_from_post_publish_feed(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "post_publish_feed",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": True,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_tap_profile_tab") as tap_profile,
            patch.object(self.helper, "_profile_surface_visible", side_effect=[True]),
            patch.object(self.helper, "_close_clips_viewer") as close_viewer,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=3.0)
        self.assertTrue(result)
        tap_profile.assert_called_once()
        close_viewer.assert_not_called()

    def test_recover_to_profile_surface_promotes_after_closing_clips_viewer(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "clips_viewer",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": True,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_close_clips_viewer") as close_viewer,
            patch.object(self.helper, "_promote_surface_to_profile", side_effect=[False, True]) as promote,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=3.0)
        self.assertTrue(result)
        close_viewer.assert_called_once()
        self.assertEqual(promote.call_count, 2)

    def test_recover_to_profile_surface_prefers_profile_tab_from_clips_viewer(self) -> None:
        clock = {"value": 0.0}

        def fake_time() -> float:
            current = clock["value"]
            clock["value"] += 0.1
            return current

        def fake_sleep(seconds: float) -> None:
            clock["value"] += max(0.0, float(seconds))

        states = [
            {
                "profile_surface_state": "clips_viewer",
                "profile_visible": False,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": True,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
            {
                "profile_surface_state": "profile",
                "profile_visible": True,
                "keyboard_visible": False,
                "comment_sheet_visible": False,
                "clips_viewer_visible": False,
                "quick_capture_visible": False,
                "instagram_dialog_visible": False,
                "post_publish_feed_visible": False,
            },
        ]

        with (
            patch.object(self.helper.time, "time", side_effect=fake_time),
            patch.object(self.helper.time, "sleep", side_effect=fake_sleep),
            patch.object(self.helper, "_profile_surface_flags", side_effect=states),
            patch.object(self.helper, "_dismiss_system_dialogs"),
            patch.object(self.helper, "_dismiss_instagram_interstitials"),
            patch.object(self.helper, "_promote_surface_to_profile", return_value=True) as promote,
            patch.object(self.helper, "_close_clips_viewer") as close_viewer,
        ):
            result = self.helper._recover_to_profile_surface(object(), "emulator-5554", timeout_seconds=3.0)
        self.assertTrue(result)
        promote.assert_called_once()
        close_viewer.assert_not_called()

    def test_resolve_publish_job_source_rejects_same_named_local_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as source_dir:
            wrong_local = Path(source_dir) / "video1.mp4"
            wrong_local.write_bytes(b"wrong-video")
            with (
                patch.object(self.helper, "INSTAGRAM_PUBLISH_SOURCE_DIR", source_dir),
                patch.object(self.helper, "_download_publish_job_source", side_effect=RuntimeError("403 forbidden")),
            ):
                with self.assertRaisesRegex(RuntimeError, "Workflow-generated source video is unavailable on this Mac"):
                    self.helper._resolve_publish_job_source(123, "/missing/video1.mp4")

    def test_helper_emulators_returns_default_slot_when_only_default_avd_configured(self) -> None:
        with (
            patch.object(self.helper, "HELPER_API_KEY", "helper-key"),
            patch.object(self.helper, "ANDROID_AVD_NAME", "Pixel_8"),
            patch.object(self.helper, "_list_running_emulators", return_value=[]),
            patch.object(self.helper, "_serial_to_avd_map", return_value={}),
            patch.object(self.helper, "_state_snapshot", return_value={"flow_running": False}),
        ):
            client = TestClient(self.helper.app)
            response = client.get("/api/helper/emulators", headers={"X-Helper-Api-Key": "helper-key"})
            payload = response.json()
            client.close()
        self.assertEqual(response.status_code, 200)
        self.assertEqual(payload["configured_serials"], ["default"])
        self.assertEqual(payload["available_serials"], ["default"])

    def test_run_payload_flow_accepts_instagram_audit_login_target(self) -> None:
        payload = {"target": "instagram_audit_login", "account_id": 1}
        with patch.object(self.helper, "_run_login_flow") as login_mock:
            self.helper._run_payload_flow(payload)
        login_mock.assert_called_once_with(payload, push_status=True)


if __name__ == "__main__":
    unittest.main()
