"""
Интеграция с ботом обучения - простая отправка через requests
"""

import logging
import requests

# Токен бота обучения
TRAINING_BOT_TOKEN = "8263517231:AAEuKr3Kw9KiIQVsNw7FOmAEBxo1bj19Ksw"
ADMIN_ID = 481659934

def send_screenshot_for_training_sync(file_path: str, verification_result: dict, user_id: int = None):
    """
    Создает файл с информацией о скриншоте для файлового бота обучения
    
    Args:
        file_path: путь к файлу скриншота
        verification_result: результат проверки ИИ
        user_id: ID пользователя, который прислал скриншот
    """
    try:
        import os
        import json
        import time
        import shutil
        
        # Создаем папку очереди
        queue_dir = "training_queue"
        os.makedirs(queue_dir, exist_ok=True)
        
        # Формируем данные
        ai_decision = "✅ ПРИНЯТ" if verification_result['valid'] else "❌ ОТКЛОНЕН"
        confidence = verification_result.get('confidence', 'N/A')
        reason = verification_result.get('reason', 'Причина не указана')
        
        screenshot_id = f"screenshot_{int(time.time())}_{user_id}"
        
        # Копируем скриншот в папку обучения
        training_screenshot_path = f"training_queue/{screenshot_id}.jpg"
        shutil.copy2(file_path, training_screenshot_path)
        
        # Создаем файл с метаданными
        metadata = {
            "id": screenshot_id,
            "timestamp": time.time(),
            "ai_decision": ai_decision,
            "confidence": confidence,
            "reason": reason,
            "user_id": user_id,
            "screenshot_path": training_screenshot_path
        }
        
        metadata_file = f"training_queue/{screenshot_id}.json"
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=2)
        
        logging.info(f"✅ Скриншот добавлен в очередь обучения: {ai_decision}, уверенность {confidence}%")
        
        # Дополнительно отправляем через HTTP API
        try:
            import requests
            
            caption = (
                f"🤖 Новый скриншот для обучения ИИ\n\n"
                f"👤 От пользователя: {user_id or 'Неизвестно'}\n\n"
                f"🧠 Решение ИИ: {ai_decision}\n"
                f"📊 Уверенность: {confidence}%\n"
                f"💭 Причина: {reason[:150]}...\n\n"
                f"Ваше решение поможет улучшить точность ИИ!\n"
                f"✅ - ИИ прав, ❌ - ИИ ошибся"
            )
            
            url = f"https://api.telegram.org/bot{TRAINING_BOT_TOKEN}/sendPhoto"
            
            with open(file_path, 'rb') as photo:
                files = {'photo': photo}
                data = {
                    'chat_id': ADMIN_ID,
                    'caption': caption
                }
                
                response = requests.post(url, data=data, files=files, timeout=10)
                
                if response.status_code == 200:
                    logging.info(f"✅ Скриншот также отправлен напрямую в Telegram")
                else:
                    logging.error(f"❌ Ошибка прямой отправки: {response.status_code}")
        
        except Exception as direct_send_error:
            logging.error(f"❌ Ошибка прямой отправки: {direct_send_error}")
        
        return True
        
    except Exception as e:
        logging.error(f"❌ Ошибка создания файла обучения: {e}")
        return False

async def send_screenshot_for_training(file_path: str, verification_result: dict, user_id: int = None):
    """
    Асинхронная обертка для синхронной функции отправки
    """
    import asyncio
    import concurrent.futures
    
    # Запускаем синхронную функцию в отдельном потоке
    loop = asyncio.get_event_loop()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        result = await loop.run_in_executor(
            executor, 
            send_screenshot_for_training_sync, 
            file_path, 
            verification_result, 
            user_id
        )
    return result