"""
Handlers for the marketing funnel bot.
User states are stored in memory (dict). Resets on redeploy — OK for short flows.
"""
import logging
import httpx

from config import MARKETING_BOT_TOKEN, ADMIN_CHAT_ID, GUIDE_KEYWORD, TRIPWIRE_URL, CHANNEL_URL
from quiz import QUESTIONS, RESULTS, calculate_result
import notion_leads

logger = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

# In-memory state per user
# state: None | "quiz" | "awaiting_name"
user_state: dict[int, dict] = {}

# Допустимые источники из deep links
KNOWN_SOURCES = {
    "tiktok": "TikTok",
    "instagram": "Instagram",
    "youtube": "YouTube",
    "telegram": "Telegram",
    "reels": "Instagram",
    "shorts": "YouTube",
}

def _parse_source(start_param: str | None) -> str:
    """Определяет источник по параметру deep link (/start tiktok → TikTok)."""
    if not start_param:
        return "Прямой"
    return KNOWN_SOURCES.get(start_param.lower(), "Прямой")

# Cache the guide file_id after first upload (avoids re-uploading on every send)
_guide_file_id: str | None = None
GUIDE_PDF_PATH = "guide.pdf"


# ─── Telegram helpers ────────────────────────────────────────────────────────

async def _api(method: str, **kwargs) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{BASE}/{method}", **kwargs)
        return r.json()


async def send(chat_id: int, text: str, reply_markup=None) -> dict:
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await _api("sendMessage", json=payload)


async def send_guide(chat_id: int) -> bool:
    """Send the PDF guide. Uses cached file_id after first send."""
    global _guide_file_id
    try:
        if _guide_file_id:
            await _api("sendDocument", json={
                "chat_id": chat_id,
                "document": _guide_file_id,
                "caption": (
                    "📎 <b>Гайд «Анатомия реакций»</b>\n\n"
                    "Диагностика своей кукухи, которая поможет перестать срываться 🧠"
                ),
                "parse_mode": "HTML",
            })
        else:
            with open(GUIDE_PDF_PATH, "rb") as f:
                r = await _api("sendDocument", data={
                    "chat_id": chat_id,
                    "caption": (
                        "📎 <b>Гайд «Анатомия реакций»</b>\n\n"
                        "Диагностика своей кукухи, которая поможет перестать срываться 🧠"
                    ),
                    "parse_mode": "HTML",
                }, files={"document": f})
            if r.get("ok"):
                _guide_file_id = r["result"]["document"]["file_id"]
        return True
    except Exception as e:
        logger.error("send_guide error: %s", e)
        return False


async def notify_admin(text: str) -> None:
    try:
        await send(int(ADMIN_CHAT_ID), text)
    except Exception as e:
        logger.warning("Admin notify failed: %s", e)


# ─── Inline keyboard builders ────────────────────────────────────────────────

def _main_menu():
    return {
        "inline_keyboard": [
            [{"text": "📎 Получить гайд бесплатно", "callback_data": "get_guide"}],
            [{"text": "🧠 Пройти тест на тип привязанности", "callback_data": "start_quiz"}],
            [{"text": "🔔 Хочу в клуб", "callback_data": "join_club"}],
        ]
    }


def _quiz_keyboard(q_index: int):
    q = QUESTIONS[q_index]
    return {
        "inline_keyboard": [
            [{"text": q["a"], "callback_data": f"quiz_a_{q_index}"}],
            [{"text": q["b"], "callback_data": f"quiz_b_{q_index}"}],
        ]
    }


def _after_guide_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🧠 Пройти тест на тип привязанности", "callback_data": "start_quiz"}],
            [{"text": "🔔 Хочу в клуб", "callback_data": "join_club"}],
        ]
    }


def _tripwire_keyboard(attachment_type: str):
    label = "тревожного" if attachment_type == "Тревожный" else "избегающего"
    return {
        "inline_keyboard": [
            [{"text": f"💳 Купить уроки для {label} типа — 990 ₽", "url": TRIPWIRE_URL}],
            [{"text": "🔔 Записаться в клуб", "callback_data": "join_club"}],
        ]
    }


def _after_secure_keyboard():
    return {
        "inline_keyboard": [
            [{"text": "🔔 Хочу в клуб", "callback_data": "join_club"}],
            [{"text": "📺 Читать канал", "url": CHANNEL_URL}],
        ]
    }


# ─── Update router ────────────────────────────────────────────────────────────

async def handle_update(update: dict) -> None:
    if "message" in update:
        await _handle_message(update["message"])
    elif "callback_query" in update:
        await _handle_callback(update["callback_query"])


async def _handle_message(message: dict) -> None:
    chat_id = message["chat"]["id"]
    user = message.get("from", {})
    user_id = user.get("id", chat_id)
    username = user.get("username")
    text = message.get("text", "").strip()

    state = user_state.get(user_id, {}).get("step")

    # Admin broadcast command
    if str(user_id) == str(ADMIN_CHAT_ID) and text.startswith("/broadcast "):
        broadcast_text = text[len("/broadcast "):].strip()
        await _do_broadcast(broadcast_text)
        return

    if str(user_id) == str(ADMIN_CHAT_ID) and text.startswith("/broadcast_waitlist "):
        broadcast_text = text[len("/broadcast_waitlist "):].strip()
        await _do_broadcast(broadcast_text, waitlist_only=True)
        return

    # /start [source] — поддержка deep links
    if text == "/start" or text.startswith("/start "):
        start_param = text[7:].strip() if text.startswith("/start ") else None
        source = _parse_source(start_param)
        # Сохраняем источник в state чтобы использовать при всех дальнейших действиях
        if user_id not in user_state:
            user_state[user_id] = {}
        user_state[user_id]["source"] = source
        await _welcome(chat_id, user_id, username, source=source)
        return

    # Кодовое слово для гайда (регистронезависимо)
    if text.lower() == GUIDE_KEYWORD.lower():
        source = user_state.get(user_id, {}).get("source", "Прямой")
        await _deliver_guide(chat_id, user_id, username, source=source, request="гайд")
        return

    # Awaiting name for club registration
    if state == "awaiting_name":
        await _save_club_registration(chat_id, user_id, username, name=text)
        return

    # Anything else — show menu
    await send(
        chat_id,
        "Напишите <b>гайд</b> — пришлю бесплатный материал.\n\n"
        "Или выберите действие 👇",
        reply_markup=_main_menu(),
    )


async def _handle_callback(cb: dict) -> None:
    chat_id = cb["message"]["chat"]["id"]
    user = cb.get("from", {})
    user_id = user.get("id", chat_id)
    username = user.get("username")
    data = cb.get("data", "")

    # Acknowledge callback
    await _api("answerCallbackQuery", json={"callback_query_id": cb["id"]})

    if data == "get_guide":
        source = user_state.get(user_id, {}).get("source", "Прямой")
        await _deliver_guide(chat_id, user_id, username, source=source, request="кнопка меню")

    elif data == "start_quiz":
        await _start_quiz(chat_id, user_id)

    elif data.startswith("quiz_"):
        parts = data.split("_")
        answer = parts[1]       # 'a' or 'b'
        q_index = int(parts[2]) # 0-5
        await _process_quiz_answer(chat_id, user_id, username, answer, q_index)

    elif data == "join_club":
        await _ask_name_for_club(chat_id, user_id)


# ─── Flow handlers ────────────────────────────────────────────────────────────

async def _welcome(chat_id: int, user_id: int, username: str | None,
                   source: str = "Прямой") -> None:
    await notion_leads.upsert_lead(
        user_id=user_id,
        username=username,
        status="Зашёл",
        source=source,
        request="/start",
    )
    await send(
        chat_id,
        "Привет! Я бот Юлии Гоголевой — психолога и автора канала "
        f"<a href=\"{CHANNEL_URL}\">@cubesoflife</a>.\n\n"
        "Здесь вы можете:\n"
        "▪️ Получить бесплатный гайд по эмоциям\n"
        "▪️ Пройти тест на тип привязанности\n"
        "▪️ Записаться в закрытый клуб «Кубики Жизни»\n\n"
        "Выбирайте 👇",
        reply_markup=_main_menu(),
    )


async def _deliver_guide(chat_id: int, user_id: int, username: str | None,
                         source: str = "Прямой", request: str = "гайд") -> None:
    ok = await send_guide(chat_id)
    if ok:
        await send(
            chat_id,
            "Держите гайд! 🎁\n\n"
            "Пока читаете — предлагаю пройти короткий тест (6 вопросов). "
            "Узнаете свой тип привязанности и получите персональные рекомендации.",
            reply_markup=_after_guide_keyboard(),
        )
        await notion_leads.upsert_lead(
            user_id=user_id,
            username=username,
            status="Получил гайд",
            source=source,
            request=request,
        )
    else:
        await send(chat_id, "Произошла ошибка при отправке файла. Попробуйте позже.")


async def _start_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {
        "step": "quiz",
        "answers": [],
        "q_index": 0,
        "source": prev.get("source", "Прямой"),  # сохраняем источник
    }
    await send(
        chat_id,
        "🧠 <b>Тест на тип привязанности</b>\n\n"
        "6 вопросов, каждый про реальные ситуации в отношениях. "
        "Выбирайте тот ответ, который ближе к вашей <i>первой реакции</i> — не идеальной.\n\n"
        + QUESTIONS[0]["text"],
        reply_markup=_quiz_keyboard(0),
    )


async def _process_quiz_answer(
    chat_id: int, user_id: int, username: str | None, answer: str, q_index: int
) -> None:
    state = user_state.get(user_id, {})

    # Guard: ignore duplicate or out-of-order callbacks
    if state.get("step") != "quiz" or state.get("q_index") != q_index:
        return

    state["answers"].append(answer)
    next_index = q_index + 1

    if next_index < len(QUESTIONS):
        state["q_index"] = next_index
        await send(
            chat_id,
            QUESTIONS[next_index]["text"],
            reply_markup=_quiz_keyboard(next_index),
        )
    else:
        # Quiz done — show result
        attachment_type = calculate_result(state["answers"])
        source = state.get("source", "Прямой")
        user_state[user_id] = {"step": None, "attachment_type": attachment_type, "source": source}

        result = RESULTS[attachment_type]
        await send(
            chat_id,
            f"<b>{result['title']}</b>\n\n{result['text']}",
            reply_markup=(
                _tripwire_keyboard(attachment_type)
                if attachment_type in ("Тревожный", "Избегающий")
                else _after_secure_keyboard()
            ),
        )
        await notion_leads.upsert_lead(
            user_id=user_id,
            username=username,
            attachment_type=attachment_type,
            status="Получил гайд",
            source=source,
            request="тест",
        )


async def _ask_name_for_club(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {"step": "awaiting_name", "source": prev.get("source", "Прямой"),
                           "attachment_type": prev.get("attachment_type")}
    await send(
        chat_id,
        "🔔 <b>Клуб «Кубики Жизни»</b>\n\n"
        "Закрытое сообщество, где каждый месяц разбираем одну сферу жизни — "
        "отношения, эмоции, мышление — по-настоящему глубоко, без воды.\n\n"
        "Как вас зовут? (напишите имя)"
    )


async def _save_club_registration(
    chat_id: int, user_id: int, username: str | None, name: str
) -> None:
    prev = user_state.get(user_id, {})
    attachment_type = prev.get("attachment_type")
    source = prev.get("source", "Прямой")
    user_state[user_id] = {"step": None, "source": source, "attachment_type": attachment_type}

    await notion_leads.upsert_lead(
        user_id=user_id,
        username=username,
        name=name,
        attachment_type=attachment_type,
        status="Предзапись",
        source=source,
        request="клуб",
    )

    await send(
        chat_id,
        f"✅ {name}, записала вас в список!\n\n"
        "Когда откроем набор — напишу вам первой. "
        f"А пока читайте канал <a href=\"{CHANNEL_URL}\">@cubesoflife</a> — "
        "там каждую неделю разбираем психологию без воды 🧠"
    )

    # Notify admin
    tg_link = f"@{username}" if username else f"id{user_id}"
    await notify_admin(
        f"🔔 <b>Новая предзапись в клуб!</b>\n\n"
        f"👤 {name} ({tg_link})\n"
        f"🆔 user_id: <code>{user_id}</code>\n"
        f"📲 Источник: {source}\n"
        f"🧠 Тип привязанности: {attachment_type or 'не проходил тест'}"
    )


# ─── Broadcast ────────────────────────────────────────────────────────────────

async def _do_broadcast(text: str, waitlist_only: bool = False) -> None:
    if waitlist_only:
        leads = await notion_leads.get_waitlist()
        label = "предзаписи"
    else:
        leads = await notion_leads.get_all_leads()
        label = "всей базе"

    await notify_admin(f"📢 Запускаю рассылку по {label}: {len(leads)} человек...")

    sent, failed = 0, 0
    for lead in leads:
        try:
            await send(lead["user_id"], text)
            sent += 1
        except Exception as e:
            logger.warning("Broadcast failed for %s: %s", lead["user_id"], e)
            failed += 1

    await notify_admin(
        f"📢 <b>Рассылка завершена</b>\n"
        f"✅ Отправлено: {sent}\n"
        f"❌ Ошибок: {failed}"
    )
