"""
Telegram-бот для обучения ИИ через скриншоты с кнопками "Принять/Отклонить"

Установка зависимостей:
pip install aiogram pillow

Запуск:
python training_bot.py

Автор: Kiro AI Assistant
"""

import asyncio
import logging
import os
import json
import time
from datetime import datetime
from typing import Dict, List

# Основные библиотеки
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# Загрузка переменных окружения
from dotenv import load_dotenv
load_dotenv()

# Состояния для комментариев
class CommentStates(StatesGroup):
    waiting_comment = State()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Токен бота для обучения ИИ
BOT_TOKEN = (os.getenv("TRAINING_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()

# ID администратора (ваш Telegram ID)
ADMIN_ID = int((os.getenv("TRAINING_ADMIN_ID") or os.getenv("ADMIN_TEST_CHAT_ID") or "481659934").strip())

# Папки для сохранения данных
ACCEPTED_DIR = "training_data/accepted"
REJECTED_DIR = "training_data/rejected"
TRAINING_LOG = "training_data/training_log.json"

# Создание папок
os.makedirs(ACCEPTED_DIR, exist_ok=True)
os.makedirs(REJECTED_DIR, exist_ok=True)
os.makedirs("training_data", exist_ok=True)

# Функция для отправки скриншота в бот обучения
async def send_to_training_bot(file_path: str, verification_result: dict):
    """Отправляет скриншот в бот обучения для принятия решения"""
    try:
        if not BOT_TOKEN:
            logger.error("TRAINING_BOT_TOKEN is not configured")
            return
        training_bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
        
        # Отправляем скриншот админу для принятия решения
        with open(file_path, 'rb') as photo:
            caption = (
                f"🤖 <b>Скриншот для обучения</b>\n\n"
                f"📊 <b>Результат ИИ:</b>\n"
                f"• Решение: {'✅ Принят' if verification_result['valid'] else '❌ Отклонен'}\n"
                f"• Уверенность: {verification_result.get('confidence', 'N/A')}%\n"
                f"• Причина: {verification_result.get('reason', 'Не указана')}\n\n"
                f"<b>Ваше решение поможет улучшить ИИ!</b>"
            )
            
            await training_bot.send_photo(
                chat_id=ADMIN_ID,
                photo=photo,
                caption=caption
            )
        
        await training_bot.session.close()
        logger.info("Скриншот отправлен в бот обучения")
        
    except Exception as e:
        logger.error(f"Ошибка отправки в бот обучения: {e}")

# Создание диспетчера
bot: Bot | None = None
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

# Хранилище для ожидающих решения скриншотов
pending_screenshots: Dict[str, dict] = {}

def load_training_log() -> List[dict]:
    """Загружает лог обучения"""
    try:
        if os.path.exists(TRAINING_LOG):
            with open(TRAINING_LOG, 'r', encoding='utf-8') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Ошибка загрузки лога: {e}")
    return []

def save_training_log(log_data: List[dict]):
    """Сохраняет лог обучения"""
    try:
        with open(TRAINING_LOG, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Ошибка сохранения лога: {e}")

def add_training_entry(screenshot_id: str, decision: str, confidence: float = None, comment: str = None):
    """Добавляет запись в лог обучения"""
    log_data = load_training_log()
    
    entry = {
        "id": screenshot_id,
        "timestamp": datetime.now().isoformat(),
        "decision": decision,  # "accepted" или "rejected"
        "confidence": confidence,
        "comment": comment,  # Комментарий администратора
        "admin_id": ADMIN_ID
    }
    
    log_data.append(entry)
    save_training_log(log_data)
    logger.info(f"Добавлена запись в лог: {decision} для {screenshot_id} с комментарием: {comment[:50] if comment else 'нет'}")

def get_decision_keyboard(screenshot_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками принять/отклонить"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ ИИ ПРАВ", callback_data=f"accept_{screenshot_id}"),
                InlineKeyboardButton(text="❌ ИИ ОШИБСЯ", callback_data=f"reject_{screenshot_id}")
            ],
            [
                InlineKeyboardButton(text="💬 Добавить комментарий", callback_data=f"comment_{screenshot_id}")
            ],
            [
                InlineKeyboardButton(text="📊 Статистика", callback_data="stats")
            ]
        ]
    )
    return keyboard

@router.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен. Этот бот только для администратора.")
        return
    
    welcome_text = (
        "🤖 <b>Бот для обучения ИИ запущен!</b>\n\n"
        "📸 <b>Как это работает:</b>\n"
        "• Присылайте скриншоты в чат\n"
        "• Под каждым появятся кнопки ✅ Принять / ❌ Отклонить\n"
        "• Ваши решения сохраняются для обучения ИИ\n"
        "• ИИ учится на ваших предпочтениях\n\n"
        "📊 <b>Команды:</b>\n"
        "/stats - статистика обучения\n"
        "/export - экспорт данных\n"
        "/clear - очистить данные\n\n"
        "Присылайте первый скриншот для начала обучения! 🚀"
    )
    
    await message.answer(welcome_text)

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        # Подсчет файлов
        accepted_count = len([f for f in os.listdir(ACCEPTED_DIR) if f.endswith(('.jpg', '.png', '.jpeg'))])
        rejected_count = len([f for f in os.listdir(REJECTED_DIR) if f.endswith(('.jpg', '.png', '.jpeg'))])
        total_count = accepted_count + rejected_count
        
        # Загрузка лога
        log_data = load_training_log()
        
        # Статистика по времени
        today_decisions = len([entry for entry in log_data if entry['timestamp'].startswith(datetime.now().strftime('%Y-%m-%d'))])
        
        acceptance_rate = (accepted_count / total_count * 100) if total_count > 0 else 0
        
        stats_text = (
            f"📊 <b>Статистика обучения ИИ:</b>\n\n"
            f"✅ Принято: <b>{accepted_count}</b>\n"
            f"❌ Отклонено: <b>{rejected_count}</b>\n"
            f"📈 Всего решений: <b>{total_count}</b>\n"
            f"🎯 Процент принятия: <b>{acceptance_rate:.1f}%</b>\n\n"
            f"📅 Решений сегодня: <b>{today_decisions}</b>\n"
            f"⏰ Последнее обновление: <b>{datetime.now().strftime('%H:%M:%S')}</b>\n\n"
            f"📁 <b>Данные сохранены в:</b>\n"
            f"• training_data/accepted/ ({accepted_count} файлов)\n"
            f"• training_data/rejected/ ({rejected_count} файлов)\n"
            f"• training_data/training_log.json ({len(log_data)} записей)"
        )
        
        await message.answer(stats_text)
        
    except Exception as e:
        await message.answer(f"❌ Ошибка получения статистики: {e}")

@router.message(Command("export"))
async def cmd_export(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    try:
        log_data = load_training_log()
        
        if not log_data:
            await message.answer("📭 Нет данных для экспорта")
            return
        
        # Создаем CSV-подобный текст
        export_text = "ID,Timestamp,Decision,Confidence\n"
        for entry in log_data[-50:]:  # Последние 50 записей
            export_text += f"{entry['id']},{entry['timestamp']},{entry['decision']},{entry.get('confidence', 'N/A')}\n"
        
        # Сохраняем в файл
        export_file = f"training_data/export_{int(time.time())}.csv"
        with open(export_file, 'w', encoding='utf-8') as f:
            f.write(export_text)
        
        await message.answer(
            f"📤 <b>Данные экспортированы!</b>\n\n"
            f"Файл: <code>{export_file}</code>\n"
            f"Записей: {len(log_data)}\n"
            f"Размер: {len(export_text)} символов"
        )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка экспорта: {e}")

@router.message(Command("clear"))
async def cmd_clear(message: Message):
    if message.from_user.id != ADMIN_ID:
        return
    
    # Клавиатура подтверждения
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="⚠️ Да, очистить ВСЕ", callback_data="confirm_clear"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_clear")
            ]
        ]
    )
    
    await message.answer(
        "⚠️ <b>ВНИМАНИЕ!</b>\n\n"
        "Вы собираетесь удалить ВСЕ данные обучения:\n"
        "• Все принятые скриншоты\n"
        "• Все отклоненные скриншоты\n"
        "• Весь лог решений\n\n"
        "Это действие <b>НЕОБРАТИМО</b>!\n\n"
        "Вы уверены?",
        reply_markup=keyboard
    )

@router.callback_query(F.data == "confirm_clear")
async def process_clear_confirm(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    try:
        # Удаляем файлы
        import shutil
        if os.path.exists("training_data"):
            shutil.rmtree("training_data")
        
        # Пересоздаем папки
        os.makedirs(ACCEPTED_DIR, exist_ok=True)
        os.makedirs(REJECTED_DIR, exist_ok=True)
        os.makedirs("training_data", exist_ok=True)
        
        await callback.message.edit_text(
            "✅ <b>Все данные обучения удалены!</b>\n\n"
            "Можете начинать обучение заново.\n"
            "Присылайте новые скриншоты! 🚀"
        )
        
        await callback.answer("Данные очищены!")
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка очистки данных: {e}")
        await callback.answer("Ошибка!")

@router.callback_query(F.data == "cancel_clear")
async def process_clear_cancel(callback: CallbackQuery):
    await callback.message.edit_text(
        "✅ <b>Очистка отменена</b>\n\n"
        "Ваши данные обучения сохранены."
    )
    await callback.answer("Отменено")

@router.message(F.photo)
async def process_screenshot(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен. Этот бот только для администратора.")
        return
    
    try:
        # Генерируем уникальный ID для скриншота
        screenshot_id = f"screenshot_{int(time.time())}_{message.message_id}"
        
        # Скачиваем файл
        file_id = message.photo[-1].file_id
        file = await bot.get_file(file_id)
        file_path = f"temp_{screenshot_id}.jpg"
        
        await bot.download_file(file.file_path, file_path)
        
        # Сохраняем информацию о скриншоте
        pending_screenshots[screenshot_id] = {
            "file_path": file_path,
            "message_id": message.message_id,
            "timestamp": datetime.now().isoformat(),
            "original_file_id": file_id
        }
        
        # Отправляем сообщение с кнопками
        decision_text = (
            f"📸 <b>Новый скриншот для обучения</b>\n\n"
            f"🆔 ID: <code>{screenshot_id}</code>\n"
            f"⏰ Время: {datetime.now().strftime('%H:%M:%S')}\n\n"
            f"<b>Принять этот скриншот как правильный пример?</b>\n\n"
            f"✅ <b>Принять</b> - ИИ будет считать такие скриншоты правильными\n"
            f"❌ <b>Отклонить</b> - ИИ будет считать такие скриншоты неправильными"
        )
        
        await message.answer(
            decision_text,
            reply_markup=get_decision_keyboard(screenshot_id)
        )
        
        logger.info(f"Получен скриншот {screenshot_id} от админа")
        
    except Exception as e:
        logger.error(f"Ошибка обработки скриншота: {e}")
        await message.answer(f"❌ Ошибка обработки скриншота: {e}")

@router.callback_query(F.data.startswith("accept_"))
async def process_accept(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    screenshot_id = callback.data.replace("accept_", "")
    
    if screenshot_id not in pending_screenshots:
        await callback.answer("❌ Скриншот не найден")
        return
    
    try:
        screenshot_data = pending_screenshots[screenshot_id]
        temp_file = screenshot_data["file_path"]
        
        # Перемещаем файл в папку принятых
        final_path = os.path.join(ACCEPTED_DIR, f"{screenshot_id}.jpg")
        os.rename(temp_file, final_path)
        
        # Добавляем в лог с комментарием
        comment = screenshot_data.get("comment", "")
        add_training_entry(screenshot_id, "accepted", comment=comment)
        
        # Удаляем из ожидающих
        del pending_screenshots[screenshot_id]
        
        # Обновляем сообщение
        result_text = (
            f"✅ <b>ИИ БЫЛ ПРАВ!</b>\n\n"
            f"🆔 ID: <code>{screenshot_id}</code>\n"
            f"📁 Сохранен в: training_data/accepted/\n"
            f"🤖 ИИ запомнил: такие решения - <b>ПРАВИЛЬНЫЕ</b>\n"
        )
        
        if comment:
            result_text += f"\n💬 <b>Ваш комментарий:</b>\n{comment}\n"
        
        result_text += f"\nПрисылайте следующий скриншот! 🚀"
        
        await callback.message.edit_text(result_text)
        await callback.answer("✅ ИИ был прав!")
        logger.info(f"Скриншот {screenshot_id} ПРИНЯТ")
        
    except Exception as e:
        logger.error(f"Ошибка принятия скриншота: {e}")
        await callback.answer(f"❌ Ошибка: {e}")

@router.callback_query(F.data.startswith("comment_"))
async def process_comment(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    screenshot_id = callback.data.replace("comment_", "")
    
    if screenshot_id not in pending_screenshots:
        await callback.answer("❌ Скриншот не найден")
        return
    
    await state.update_data(screenshot_id=screenshot_id)
    await state.set_state(CommentStates.waiting_comment)
    
    await callback.message.answer(
        f"💬 <b>Добавьте комментарий для скриншота</b>\n\n"
        f"🆔 ID: <code>{screenshot_id}</code>\n\n"
        f"Напишите, почему ИИ прав или ошибся:\n"
        f"• Что не так с анализом?\n"
        f"• Какие детали ИИ упустил?\n"
        f"• Как улучшить проверку?\n\n"
        f"Ваш комментарий поможет улучшить ИИ! ✍️"
    )
    
    await callback.answer("💬 Ожидаю комментарий")

@router.message(StateFilter(CommentStates.waiting_comment))
async def process_comment_text(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    screenshot_id = data.get('screenshot_id')
    comment = message.text
    
    if screenshot_id not in pending_screenshots:
        await message.answer("❌ Скриншот не найден")
        await state.clear()
        return
    
    # Сохраняем комментарий
    pending_screenshots[screenshot_id]['comment'] = comment
    
    await message.answer(
        f"✅ <b>Комментарий сохранен!</b>\n\n"
        f"💬 <b>Ваш комментарий:</b>\n{comment}\n\n"
        f"Теперь выберите решение:",
        reply_markup=get_decision_keyboard(screenshot_id)
    )
    
    await state.clear()

@router.callback_query(F.data.startswith("reject_"))
async def process_reject(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    screenshot_id = callback.data.replace("reject_", "")
    
    if screenshot_id not in pending_screenshots:
        await callback.answer("❌ Скриншот не найден")
        return
    
    try:
        screenshot_data = pending_screenshots[screenshot_id]
        temp_file = screenshot_data["file_path"]
        
        # Перемещаем файл в папку отклоненных
        final_path = os.path.join(REJECTED_DIR, f"{screenshot_id}.jpg")
        os.rename(temp_file, final_path)
        
        # Добавляем в лог с комментарием
        comment = screenshot_data.get("comment", "")
        add_training_entry(screenshot_id, "rejected", comment=comment)
        
        # Удаляем из ожидающих
        del pending_screenshots[screenshot_id]
        
        # Обновляем сообщение
        result_text = (
            f"❌ <b>ИИ ОШИБСЯ!</b>\n\n"
            f"🆔 ID: <code>{screenshot_id}</code>\n"
            f"📁 Сохранен в: training_data/rejected/\n"
            f"🤖 ИИ запомнил: такие решения - <b>НЕПРАВИЛЬНЫЕ</b>\n"
        )
        
        if comment:
            result_text += f"\n💬 <b>Ваш комментарий:</b>\n{comment}\n"
        
        result_text += f"\nПрисылайте следующий скриншот! 🚀"
        
        await callback.message.edit_text(result_text)
        await callback.answer("❌ ИИ ошибся!")
        logger.info(f"Скриншот {screenshot_id} ОТКЛОНЕН")
        
    except Exception as e:
        logger.error(f"Ошибка отклонения скриншота: {e}")
        await callback.answer(f"❌ Ошибка: {e}")

@router.callback_query(F.data == "stats")
async def process_stats_callback(callback: CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    try:
        accepted_count = len([f for f in os.listdir(ACCEPTED_DIR) if f.endswith(('.jpg', '.png', '.jpeg'))])
        rejected_count = len([f for f in os.listdir(REJECTED_DIR) if f.endswith(('.jpg', '.png', '.jpeg'))])
        total_count = accepted_count + rejected_count
        
        acceptance_rate = (accepted_count / total_count * 100) if total_count > 0 else 0
        
        stats_text = (
            f"📊 <b>Быстрая статистика:</b>\n\n"
            f"✅ Принято: {accepted_count}\n"
            f"❌ Отклонено: {rejected_count}\n"
            f"📈 Всего: {total_count}\n"
            f"🎯 Принятие: {acceptance_rate:.1f}%"
        )
        
        await callback.answer(stats_text, show_alert=True)
        
    except Exception as e:
        await callback.answer(f"❌ Ошибка: {e}")

@router.message()
async def handle_other_messages(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен. Этот бот только для администратора.")
        return
    
    await message.answer(
        "🤖 <b>Бот для обучения ИИ</b>\n\n"
        "📸 Присылайте скриншоты для обучения\n"
        "📊 /stats - статистика\n"
        "📤 /export - экспорт данных\n"
        "🗑 /clear - очистить данные\n\n"
        "Жду ваши скриншоты! 🚀"
    )

# Основная функция
async def main():
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("TRAINING_BOT_TOKEN is not configured")
    bot = Bot(token=BOT_TOKEN, parse_mode="HTML")

    # Подключаем роутер
    dp.include_router(router)
    
    # Запускаем бота
    logger.info("🤖 Бот для обучения ИИ запущен!")
    logger.info(f"👤 Администратор: {ADMIN_ID}")
    logger.info(f"📁 Папка принятых: {ACCEPTED_DIR}")
    logger.info(f"📁 Папка отклоненных: {REJECTED_DIR}")
    
    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])

if __name__ == "__main__":
    # Проверяем токен
    if not BOT_TOKEN:
        print("❌ ОШИБКА: Не установлен токен бота!")
        print("Укажите TRAINING_BOT_TOKEN или BOT_TOKEN в окружении / .env")
        exit(1)
    
    asyncio.run(main())
