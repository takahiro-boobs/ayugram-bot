#!/usr/bin/env python3
"""
Тест отправки скриншота в бот обучения
"""

import requests
import sys

TRAINING_BOT_TOKEN = "8263517231:AAEuKr3Kw9KiIQVsNw7FOmAEBxo1bj19Ksw"
ADMIN_ID = 481659934

def test_send():
    """Тестовая отправка сообщения"""
    try:
        url = f"https://api.telegram.org/bot{TRAINING_BOT_TOKEN}/sendMessage"
        
        data = {
            'chat_id': ADMIN_ID,
            'text': '🧪 Тест отправки из основного бота!\n\nЕсли вы видите это сообщение, то интеграция работает! ✅'
        }
        
        response = requests.post(url, data=data, timeout=10)
        
        if response.status_code == 200:
            print("✅ Тестовое сообщение отправлено успешно!")
            return True
        else:
            print(f"❌ Ошибка отправки: {response.status_code} - {response.text}")
            return False
        
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False

if __name__ == "__main__":
    test_send()