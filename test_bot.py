from __future__ import annotations

import importlib
import os
import unittest
from unittest.mock import AsyncMock, Mock, patch


class BotPollingRuntimeTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        token = "123456:TESTTOKENabcdefghijklmnopqrstuvwxyzABCDE"
        with patch.dict(os.environ, {"BOT_TOKEN": token}, clear=False):
            import bot as bot_module

            self.bot = importlib.reload(bot_module)

    async def test_main_returns_conflict_exit_code_without_retry_loop(self) -> None:
        conflict = self.bot.TelegramConflictError(Mock(), "conflict")
        with (
            patch.object(self.bot.dp, "include_router"),
            patch.object(self.bot.db, "init_db"),
            patch.object(self.bot.bot, "delete_webhook", new=AsyncMock()),
            patch.object(self.bot.dp, "start_polling", new=AsyncMock(side_effect=conflict)) as start_polling,
            patch.object(self.bot.bot.session, "close", new=AsyncMock()) as close_session,
        ):
            exit_code = await self.bot.main()

        self.assertEqual(exit_code, self.bot.POLLING_CONFLICT_EXIT_CODE)
        self.assertEqual(start_polling.await_count, 1)
        close_session.assert_awaited_once()

    async def test_main_retries_network_error_then_returns_zero_after_success(self) -> None:
        network_error = self.bot.TelegramNetworkError(Mock(), "temporary")
        with (
            patch.object(self.bot.dp, "include_router"),
            patch.object(self.bot.db, "init_db"),
            patch.object(self.bot.bot, "delete_webhook", new=AsyncMock()),
            patch.object(
                self.bot.dp,
                "start_polling",
                new=AsyncMock(side_effect=[network_error, None]),
            ) as start_polling,
            patch.object(self.bot.asyncio, "sleep", new=AsyncMock()) as sleep_mock,
            patch.object(self.bot.bot.session, "close", new=AsyncMock()) as close_session,
        ):
            exit_code = await self.bot.main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(start_polling.await_count, 2)
        sleep_mock.assert_awaited_once()
        close_session.assert_awaited_once()
