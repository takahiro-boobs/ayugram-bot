from __future__ import annotations

import os
import unittest
from unittest.mock import patch

import settings


class SettingsTests(unittest.TestCase):
    def test_web_settings_derive_runner_secret_and_embed_flag(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HELPER_API_KEY": "helper-key",
                "SESSION_SECRET": "session-key",
                "EMBED_RUNTIME_WORKER": "0",
            },
            clear=True,
        ):
            cfg = settings.load_web_settings()

        self.assertEqual(cfg.publish_shared_secret, "helper-key")
        self.assertEqual(cfg.publish_runner_api_key, "helper-key")
        self.assertFalse(cfg.embed_runtime_worker)

    def test_helper_settings_respect_runner_toggle(self) -> None:
        with patch.dict(
            os.environ,
            {
                "HELPER_API_KEY": "helper-key",
                "PUBLISH_RUNNER_ENABLED": "1",
                "PUBLISH_RUNNER_API_KEY": "runner-key",
            },
            clear=True,
        ):
            cfg = settings.load_helper_settings()

        self.assertTrue(cfg.publish_runner_enabled)
        self.assertEqual(cfg.publish_runner_api_key, "runner-key")


if __name__ == "__main__":
    unittest.main()
