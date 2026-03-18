#!/usr/bin/env python3
"""
Бот ручной модерации скриншотов.
Принимает решения по кнопкам и пишет результат в review_decisions/.
"""

import json
import logging
import os
import time
from datetime import datetime

from aiogram import Bot, Dispatcher, Router, F
from aiogram.exceptions import TelegramNetworkError, TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, Message
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage
from dotenv import load_dotenv

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REVIEW_BOT_TOKEN = os.getenv("REVIEW_BOT_TOKEN", "<REVIEW_BOT_TOKEN>")
# Единственный модератор (менеджер)
REVIEW_ADMIN_IDS = {8497496702}
REVIEW_QUEUE_DIR = "review_queue"
REVIEW_DECISIONS_DIR = "review_decisions"

os.makedirs(REVIEW_QUEUE_DIR, exist_ok=True)
os.makedirs(REVIEW_DECISIONS_DIR, exist_ok=True)

bot = Bot(token=REVIEW_BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(storage=storage)
router = Router()

def build_result_keyboard(request_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_{request_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_{request_id}")
            ]
        ]
    )

def write_decision(request_id: str, decision: str, admin_id: int) -> bool:
    decision_path = os.path.join(REVIEW_DECISIONS_DIR, f"{request_id}.json")
    if os.path.exists(decision_path):
        return False
    payload = {
        "id": request_id,
        "decision": decision,
        "admin_id": admin_id,
        "timestamp": time.time(),
        "timestamp_iso": datetime.now().isoformat()
    }
    with open(decision_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return True

@router.message(Command("start"))
async def cmd_start(message: Message):
    if message.from_user.id not in REVIEW_ADMIN_IDS:
        await message.answer("❌ Доступ запрещен.")
        return
    await message.answer(
        "🛠 <b>Бот ручной проверки запущен</b>\n\n"
        "Сюда будут приходить скриншоты с кнопками.\n"
        "Нажимай ✅ Принять или ❌ Отклонить."
    )

@router.callback_query(F.data.startswith("accept_"))
async def on_accept(callback: CallbackQuery):
    if callback.from_user.id not in REVIEW_ADMIN_IDS:
        try:
            await callback.answer("❌ Доступ запрещен")
        except TelegramBadRequest:
            pass
        return
    try:
        await callback.answer()
    except TelegramBadRequest:
        pass

    request_id = callback.data.replace("accept_", "")
    if not write_decision(request_id, "accepted", callback.from_user.id):
        try:
            await callback.answer("Решение уже принято")
        except TelegramBadRequest:
            pass
        return
    try:
        await callback.message.edit_caption(
            f"✅ <b>ПРИНЯТО</b>\n\nID: <code>{request_id}</code>"
        )
    except TelegramBadRequest:
        pass
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    try:
        await callback.answer("✅ Принято")
    except TelegramBadRequest:
        pass

@router.callback_query(F.data.startswith("reject_"))
async def on_reject(callback: CallbackQuery):
    if callback.from_user.id not in REVIEW_ADMIN_IDS:
        try:
            await callback.answer("❌ Доступ запрещен")
        except TelegramBadRequest:
            pass
        return
    try:
        await callback.answer()
    except TelegramBadRequest:
        pass

    request_id = callback.data.replace("reject_", "")
    if not write_decision(request_id, "rejected", callback.from_user.id):
        try:
            await callback.answer("Решение уже принято")
        except TelegramBadRequest:
            pass
        return
    try:
        await callback.message.edit_caption(
            f"❌ <b>ОТКЛОНЕНО</b>\n\nID: <code>{request_id}</code>"
        )
    except TelegramBadRequest:
        pass
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except TelegramBadRequest:
        pass
    try:
        await callback.answer("❌ Отклонено")
    except TelegramBadRequest:
        pass

@router.message()
async def handle_other(message: Message):
    if message.from_user.id not in REVIEW_ADMIN_IDS:
        await message.answer("❌ Доступ запрещен.")
        return
    await message.answer("Жду скриншоты с кнопками.")

async def main():
    dp.include_router(router)
    logger.info("🛠 Бот ручной проверки запущен")
    backoff_s = 2
    while True:
        try:
            await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
        except TelegramNetworkError as e:
            logger.error("❌ Telegram недоступен: %s. Повтор через %s сек.", e, backoff_s)
        except Exception:
            logger.exception("❌ Polling упал. Повтор через %s сек.", backoff_s)

        await asyncio.sleep(backoff_s)
        backoff_s = min(backoff_s * 2, 60)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
