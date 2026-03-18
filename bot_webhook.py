"""
Webhook обработчик для Telegram бота
Минимальная версия без импорта bot.py для избежания конфликтов
"""

import logging
import os
from typing import Any, Dict

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BOT_TOKEN = os.getenv("BOT_TOKEN", "")

# Ленивая инициализация - создаем бота и диспетчер только при первом запросе
_bot = None
_dp = None


def _init_bot():
    """Инициализирует бота и диспетчер при первом вызове"""
    global _bot, _dp
    
    if _bot is not None:
        return
    
    from aiogram import Bot, Dispatcher
    from aiogram.client.default import DefaultBotProperties
    import db
    
    # Инициализируем БД
    db.init_db()
    
    # Создаем бота
    _bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
    _dp = Dispatcher()
    
    # Импортируем и регистрируем обработчики
    # Используем exec чтобы избежать импорта bot.py на уровне модуля
    import bot
    _dp.include_router(bot.router)
    
    logger.info("Bot webhook initialized")


async def process_update(update_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Обрабатывает одно обновление от Telegram
    
    Args:
        update_data: JSON данные обновления от Telegram
        
    Returns:
        Словарь с результатом обработки
    """
    try:
        # Инициализируем бота при первом запросе
        _init_bot()
        
        from aiogram.types import Update
        
        # Создаем объект Update из JSON
        update = Update(**update_data)
        
        # Обрабатываем обновление
        await _dp.feed_update(_bot, update)
        
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Ошибка обработки webhook update: {e}", exc_info=True)
        return {"status": "error", "message": str(e)}
