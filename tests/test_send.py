#!/usr/bin/env python3
"""
Тест отправки скриншота в бот обучения
"""

import os
import sys

import http_utils
import pytest
TRAINING_BOT_TOKEN = (os.getenv("TRAINING_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
ADMIN_ID = int((os.getenv("TRAINING_ADMIN_ID") or os.getenv("ADMIN_TEST_CHAT_ID") or "481659934").strip())

def test_send():
    """Тестовая отправка сообщения"""
    if not TRAINING_BOT_TOKEN:
        pytest.skip("TRAINING_BOT_TOKEN is not configured")

    url = f"https://api.telegram.org/bot{TRAINING_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": ADMIN_ID,
        "text": "🧪 Тест отправки из основного бота!\n\nЕсли вы видите это сообщение, то интеграция работает! ✅",
    }

    response = http_utils.request_with_retry(
        "POST",
        url,
        data=data,
        timeout=10,
        allow_retry=False,
        log_context="training_test_send",
    )

    assert response.status_code == 200, (
        f"Telegram sendMessage failed: {response.status_code} - {response.text}"
    )

if __name__ == "__main__":
    test_send()
