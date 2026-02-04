#!/usr/bin/env python3
"""
Файловый бот для обучения - работает через файлы, без конфликтов
"""

import os
import json
import time
import requests
from datetime import datetime

# Токен бота обучения
BOT_TOKEN = "8263517231:AAEuKr3Kw9KiIQVsNw7FOmAEBxo1bj19Ksw"
ADMIN_ID = 481659934

# Папки для работы
QUEUE_DIR = "training_queue"
PROCESSED_DIR = "training_processed"

os.makedirs(QUEUE_DIR, exist_ok=True)
os.makedirs(PROCESSED_DIR, exist_ok=True)

def send_message(text, reply_markup=None):
    """Отправляет сообщение через HTTP API"""
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        data = {
            'chat_id': ADMIN_ID,
            'text': text,
            'parse_mode': 'HTML'
        }
        
        if reply_markup:
            data['reply_markup'] = json.dumps(reply_markup)
        
        response = requests.post(url, data=data, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Ошибка отправки сообщения: {e}")
        return False

def get_decision_keyboard(screenshot_id):
    """Создает клавиатуру с кнопками"""
    return {
        "inline_keyboard": [
            [
                {"text": "✅ ИИ ПРАВ", "callback_data": f"accept_{screenshot_id}"},
                {"text": "❌ ИИ ОШИБСЯ", "callback_data": f"reject_{screenshot_id}"}
            ]
        ]
    }

def process_queue():
    """Обрабатывает очередь скриншотов"""
    try:
        # Ищем новые файлы в очереди
        for filename in os.listdir(QUEUE_DIR):
            if filename.endswith('.json'):
                file_path = os.path.join(QUEUE_DIR, filename)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    screenshot_id = data.get('id', 'unknown')
                    ai_decision = data.get('ai_decision', 'unknown')
                    confidence = data.get('confidence', 'N/A')
                    reason = data.get('reason', 'Не указана')
                    user_id = data.get('user_id', 'Неизвестно')
                    
                    # Отправляем сообщение с кнопками
                    message_text = (
                        f"🤖 <b>Новый скриншот для обучения ИИ</b>\n\n"
                        f"👤 <b>От пользователя:</b> {user_id}\n\n"
                        f"🧠 <b>Решение ИИ:</b> {ai_decision}\n"
                        f"📊 <b>Уверенность:</b> {confidence}%\n"
                        f"💭 <b>Причина:</b> {reason[:150]}...\n\n"
                        f"<b>Согласны ли вы с решением ИИ?</b>"
                    )
                    
                    keyboard = get_decision_keyboard(screenshot_id)
                    
                    if send_message(message_text, keyboard):
                        # Перемещаем файл в обработанные
                        processed_path = os.path.join(PROCESSED_DIR, filename)
                        os.rename(file_path, processed_path)
                        print(f"✅ Обработан скриншот: {screenshot_id}")
                    else:
                        print(f"❌ Ошибка отправки скриншота: {screenshot_id}")
                
                except Exception as e:
                    print(f"❌ Ошибка обработки файла {filename}: {e}")
                    # Перемещаем проблемный файл
                    error_path = os.path.join(PROCESSED_DIR, f"error_{filename}")
                    os.rename(file_path, error_path)
    
    except Exception as e:
        print(f"❌ Ошибка обработки очереди: {e}")

def main():
    """Основной цикл"""
    print("⚠️ Запущен файловый training-бот (legacy). Рекомендуемый сценарий: training_bot.py")
    print("🤖 Файловый бот обучения запущен!")
    print(f"📁 Папка очереди: {QUEUE_DIR}")
    print(f"📁 Папка обработанных: {PROCESSED_DIR}")
    
    while True:
        try:
            process_queue()
            time.sleep(2)  # Проверяем каждые 2 секунды
        except KeyboardInterrupt:
            print("\n🛑 Остановка бота...")
            break
        except Exception as e:
            print(f"❌ Ошибка в основном цикле: {e}")
            time.sleep(5)

if __name__ == "__main__":
    main()
