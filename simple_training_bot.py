#!/usr/bin/env python3
"""
Простой бот для обучения ИИ - получает скриншоты и показывает кнопки
"""

import logging
import os
import json
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage

# Состояния для комментариев
class CommentStates(StatesGroup):
    waiting_comment = State()

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Токен бота обучения
BOT_TOKEN = (os.getenv("TRAINING_BOT_TOKEN") or os.getenv("BOT_TOKEN") or "").strip()
ADMIN_ID = int((os.getenv("TRAINING_ADMIN_ID") or os.getenv("ADMIN_TEST_CHAT_ID") or "481659934").strip())

# Создание бота с хранилищем состояний
storage = MemoryStorage()
bot: Bot | None = None
dp = Dispatcher(storage=storage)
router = Router()

# Хранилище для решений
decisions_storage = {}

def save_training_data(screenshot_id: str, ai_decision: str, admin_decision: str, comment: str = ""):
    """Сохраняет данные обучения в JSON файл"""
    try:
        os.makedirs("training_data", exist_ok=True)
        
        training_entry = {
            "id": screenshot_id,
            "timestamp": datetime.now().isoformat(),
            "ai_decision": ai_decision,
            "admin_decision": admin_decision,  # "correct" или "wrong"
            "admin_comment": comment,
            "admin_id": ADMIN_ID
        }
        
        # Загружаем существующие данные
        training_file = "training_data/decisions.json"
        training_data = []
        
        if os.path.exists(training_file):
            with open(training_file, 'r', encoding='utf-8') as f:
                training_data = json.load(f)
        
        # Добавляем новую запись
        training_data.append(training_entry)
        
        # Сохраняем обновленные данные
        with open(training_file, 'w', encoding='utf-8') as f:
            json.dump(training_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Сохранены данные обучения: {screenshot_id} - {admin_decision}")
        
    except Exception as e:
        logger.error(f"Ошибка сохранения данных обучения: {e}")

def get_decision_keyboard(screenshot_id: str) -> InlineKeyboardMarkup:
    """Создает клавиатуру с кнопками принять/отклонить"""
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ ИИ ПРАВ", callback_data=f"accept_{screenshot_id}"),
                InlineKeyboardButton(text="❌ ИИ ОШИБСЯ", callback_data=f"reject_{screenshot_id}")
            ]
        ]
    )
    return keyboard

@router.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен")
        return
    
    await message.answer(
        "🤖 <b>Бот для обучения ИИ запущен!</b>\n\n"
        "📸 Скриншоты из основного бота будут приходить сюда автоматически\n"
        "✅ ИИ ПРАВ - если согласны с решением ИИ\n"
        "❌ ИИ ОШИБСЯ - если ИИ принял неправильное решение\n"
        "💬 После каждого решения напишите комментарий\n\n"
        "📊 /stats - статистика обучения\n"
        "📄 /export - экспорт данных\n\n"
        "Ожидаю скриншоты для обучения! 🚀",
        parse_mode="HTML"
    )

@router.message(Command("stats"))
async def cmd_stats(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен")
        return
    
    try:
        training_file = "training_data/decisions.json"
        
        if not os.path.exists(training_file):
            await message.answer("📭 Пока нет данных обучения")
            return
        
        with open(training_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        
        total_decisions = len(training_data)
        correct_decisions = len([d for d in training_data if d['admin_decision'] == 'correct'])
        wrong_decisions = len([d for d in training_data if d['admin_decision'] == 'wrong'])
        
        # Статистика по решениям ИИ
        ai_accepted = len([d for d in training_data if d['ai_decision'] == 'accepted'])
        ai_rejected = len([d for d in training_data if d['ai_decision'] == 'rejected'])
        
        # Точность ИИ
        accuracy = (correct_decisions / total_decisions * 100) if total_decisions > 0 else 0
        
        # Сегодняшние решения
        today = datetime.now().strftime('%Y-%m-%d')
        today_decisions = len([d for d in training_data if d['timestamp'].startswith(today)])
        
        stats_text = (
            f"📊 <b>Статистика обучения ИИ:</b>\n\n"
            f"📈 <b>Общая статистика:</b>\n"
            f"• Всего решений: <b>{total_decisions}</b>\n"
            f"• ИИ был прав: <b>{correct_decisions}</b> ({correct_decisions/total_decisions*100:.1f}%)\n"
            f"• ИИ ошибся: <b>{wrong_decisions}</b> ({wrong_decisions/total_decisions*100:.1f}%)\n\n"
            f"🤖 <b>Решения ИИ:</b>\n"
            f"• Принял: <b>{ai_accepted}</b>\n"
            f"• Отклонил: <b>{ai_rejected}</b>\n\n"
            f"🎯 <b>Точность ИИ: {accuracy:.1f}%</b>\n\n"
            f"📅 Решений сегодня: <b>{today_decisions}</b>\n"
            f"⏰ Последнее обновление: <b>{datetime.now().strftime('%H:%M:%S')}</b>"
        )
        
        await message.answer(stats_text, parse_mode="HTML")
        
    except Exception as e:
        await message.answer(f"❌ Ошибка получения статистики: {e}")

@router.message(Command("export"))
async def cmd_export(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен")
        return
    
    try:
        training_file = "training_data/decisions.json"
        
        if not os.path.exists(training_file):
            await message.answer("📭 Нет данных для экспорта")
            return
        
        with open(training_file, 'r', encoding='utf-8') as f:
            training_data = json.load(f)
        
        # Создаем читаемый экспорт
        export_text = "📄 ЭКСПОРТ ДАННЫХ ОБУЧЕНИЯ\n\n"
        
        for i, entry in enumerate(training_data[-10:], 1):  # Последние 10 записей
            ai_decision_text = "✅ Принял" if entry['ai_decision'] == 'accepted' else "❌ Отклонил"
            admin_decision_text = "✅ Прав" if entry['admin_decision'] == 'correct' else "❌ Ошибся"
            
            export_text += (
                f"{i}. ID: {entry['id']}\n"
                f"   ИИ: {ai_decision_text}\n"
                f"   Админ: {admin_decision_text}\n"
                f"   Комментарий: {entry['admin_comment'][:50]}...\n"
                f"   Время: {entry['timestamp'][:19]}\n\n"
            )
        
        await message.answer(f"<pre>{export_text}</pre>", parse_mode="HTML")
        
        # Отправляем файл
        await message.answer_document(
            document=open(training_file, 'rb'),
            caption=f"📁 Полный файл данных обучения\n📊 Записей: {len(training_data)}"
        )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка экспорта: {e}")

@router.message(F.photo)
async def handle_screenshot(message: Message):
    """Обрабатывает скриншоты, пришедшие из основного бота или напрямую"""
    if message.from_user.id != ADMIN_ID:
        return
    
    # Генерируем ID для скриншота
    screenshot_id = f"screenshot_{int(time.time())}_{message.message_id}"
    
    # Проверяем, есть ли информация об ИИ в подписи
    caption = message.caption or ""
    
    if "Новый скриншот для обучения ИИ" in caption:
        # Это скриншот из основного бота - добавляем кнопки
        await message.reply(
            f"📸 <b>Скриншот получен для обучения!</b>\n\n"
            f"🆔 ID: <code>{screenshot_id}</code>\n\n"
            f"<b>Согласны ли вы с решением ИИ?</b>",
            reply_markup=get_decision_keyboard(screenshot_id),
            parse_mode="HTML"
        )
    else:
        # Это скриншот, присланный напрямую
        await message.answer(
            f"📸 <b>Скриншот получен!</b>\n\n"
            f"🆔 ID: <code>{screenshot_id}</code>\n\n"
            f"Это правильный скриншот настроек iPhone?",
            reply_markup=get_decision_keyboard(screenshot_id),
            parse_mode="HTML"
        )

@router.callback_query(F.data.startswith("accept_"))
async def process_accept(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    screenshot_id = callback.data.replace("accept_", "")
    
    # Сохраняем информацию о решении
    await state.update_data(
        screenshot_id=screenshot_id,
        admin_decision="correct",
        original_message=callback.message
    )
    await state.set_state(CommentStates.waiting_comment)
    
    await callback.message.answer(
        f"✅ <b>Вы согласились с ИИ!</b>\n\n"
        f"🆔 ID: <code>{screenshot_id}</code>\n\n"
        f"💬 <b>Теперь напишите комментарий:</b>\n"
        f"• Почему ИИ принял правильное решение?\n"
        f"• Какие признаки правильно определил?\n"
        f"• Что помогло ИИ сделать верный вывод?\n\n"
        f"Ваш комментарий поможет ИИ лучше понимать правильные решения! ✍️",
        parse_mode="HTML"
    )
    
    await callback.answer("✅ Ожидаю комментарий")

@router.callback_query(F.data.startswith("reject_"))
async def process_reject(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Доступ запрещен")
        return
    
    screenshot_id = callback.data.replace("reject_", "")
    
    # Сохраняем информацию о решении
    await state.update_data(
        screenshot_id=screenshot_id,
        admin_decision="wrong",
        original_message=callback.message
    )
    await state.set_state(CommentStates.waiting_comment)
    
    await callback.message.answer(
        f"❌ <b>Вы не согласились с ИИ!</b>\n\n"
        f"🆔 ID: <code>{screenshot_id}</code>\n\n"
        f"💬 <b>Теперь напишите комментарий:</b>\n"
        f"• Почему ИИ ошибся?\n"
        f"• Что ИИ неправильно определил?\n"
        f"• На что ИИ должен обращать внимание?\n"
        f"• Какие признаки ИИ упустил?\n\n"
        f"Ваш комментарий поможет ИИ избежать таких ошибок! ✍️",
        parse_mode="HTML"
    )
    
    await callback.answer("❌ Ожидаю комментарий")

@router.message(StateFilter(CommentStates.waiting_comment))
async def process_comment(message: Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    
    data = await state.get_data()
    screenshot_id = data.get('screenshot_id')
    admin_decision = data.get('admin_decision')
    original_message = data.get('original_message')
    comment = message.text
    
    # Извлекаем информацию об ИИ из оригинального сообщения
    ai_decision = "unknown"
    if original_message and original_message.caption:
        if "✅ ПРИНЯТ" in original_message.caption:
            ai_decision = "accepted"
        elif "❌ ОТКЛОНЕН" in original_message.caption:
            ai_decision = "rejected"
    
    # Сохраняем данные обучения
    save_training_data(screenshot_id, ai_decision, admin_decision, comment)
    
    # Формируем итоговое сообщение
    decision_emoji = "✅" if admin_decision == "correct" else "❌"
    decision_text = "ИИ БЫЛ ПРАВ" if admin_decision == "correct" else "ИИ ОШИБСЯ"
    
    result_text = (
        f"{decision_emoji} <b>{decision_text}!</b>\n\n"
        f"🆔 ID: <code>{screenshot_id}</code>\n"
        f"🤖 Решение ИИ: {ai_decision}\n"
        f"👤 Ваше решение: {admin_decision}\n\n"
        f"💬 <b>Ваш комментарий:</b>\n{comment}\n\n"
        f"📚 <b>Данные сохранены для обучения ИИ!</b>\n"
        f"Ожидаю следующий скриншот! 🚀"
    )
    
    await message.answer(result_text, parse_mode="HTML")
    
    # Обновляем оригинальное сообщение
    if original_message:
        try:
            await original_message.edit_caption(
                f"{original_message.caption}\n\n"
                f"📝 <b>РЕШЕНИЕ ПРИНЯТО:</b> {decision_text}\n"
                f"💬 <b>Комментарий:</b> {comment[:100]}{'...' if len(comment) > 100 else ''}",
                parse_mode="HTML"
            )
        except:
            pass  # Игнорируем ошибки редактирования
    
    await state.clear()
    logger.info(f"Обучение завершено: {screenshot_id} - {admin_decision} - {comment[:50]}...")

@router.message()
async def handle_other(message: Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещен")
        return
    
    await message.answer(
        "🤖 Бот для обучения ИИ\n\n"
        "📸 Ожидаю скриншоты из основного бота\n"
        "Или пришлите скриншот напрямую для тестирования"
    )

async def main():
    global bot
    if not BOT_TOKEN:
        raise RuntimeError("TRAINING_BOT_TOKEN is not configured")
    bot = Bot(token=BOT_TOKEN)
    dp.include_router(router)
    logger.info("⚠️ Запущен упрощенный training-бот (legacy). Рекомендуемый сценарий: training_bot.py")
    logger.info("🤖 Простой бот обучения запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    import asyncio
    if not BOT_TOKEN:
        raise SystemExit("TRAINING_BOT_TOKEN is not configured")
    asyncio.run(main())
