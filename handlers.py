"""
Handlers for the marketing funnel bot.
Flows: /start → menu → guide / quiz / club / lesson
       quiz → 8 questions → result → deprivation quiz (anxious only)
       deprivation quiz → 10 questions → result → protocol pre-reg
"""
import logging
import httpx

from config import MARKETING_BOT_TOKEN, ADMIN_CHAT_ID, GUIDE_KEYWORD, TRIPWIRE_URL, CHANNEL_URL
from quiz import QUESTIONS as QUIZ_Q, RESULTS as QUIZ_R, calculate_result as quiz_result
from deprivation_quiz import (
    QUESTIONS as DEP_Q, RESULTS as DEP_R, PROTOCOL_DESCRIPTION,
    calculate_result as dep_result,
)
import notion_leads

logger = logging.getLogger(__name__)
BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

# ── In-memory state ──────────────────────────────────────────────────────────
# step values: None | "quiz" | "dep_quiz" | "awaiting_name" | "awaiting_protocol_name"
user_state: dict[int, dict] = {}

# Cached file_ids (avoid re-uploading on every send)
_guide_file_id: str | None = None
_photo_cache: dict[str, str] = {}  # path → file_id

GUIDE_PDF_PATH = "guide.pdf"

KNOWN_SOURCES = {
    "tiktok": "TikTok", "instagram": "Instagram",
    "youtube": "YouTube", "telegram": "Telegram",
    "reels": "Instagram", "shorts": "YouTube",
}


def _parse_source(param: str | None) -> str:
    if not param:
        return "Прямой"
    return KNOWN_SOURCES.get(param.lower(), "Прямой")


# ── Telegram API helpers ─────────────────────────────────────────────────────

async def _api(method: str, **kwargs) -> dict:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(f"{BASE}/{method}", **kwargs)
        return r.json()


async def send(chat_id: int, text: str, reply_markup=None) -> dict:
    payload = {"chat_id": chat_id, "text": text,
                "parse_mode": "HTML", "disable_web_page_preview": True}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    return await _api("sendMessage", json=payload)


async def send_photo(chat_id: int, image_path: str, caption: str = "",
                     reply_markup=None) -> bool:
    """Send photo using cached file_id or upload fresh."""
    global _photo_cache
    try:
        cached = _photo_cache.get(image_path)
        payload = {"chat_id": chat_id, "parse_mode": "HTML",
                   "disable_web_page_preview": True}
        if caption:
            payload["caption"] = caption
        if reply_markup:
            payload["reply_markup"] = reply_markup

        if cached:
            r = await _api("sendPhoto", json={**payload, "photo": cached})
        else:
            with open(image_path, "rb") as f:
                ext = image_path.rsplit(".", 1)[-1].lower()
                r = await _api("sendPhoto", data=payload,
                                files={"photo": (f"img.{ext}", f)})
            if r.get("ok"):
                _photo_cache[image_path] = r["result"]["photo"][-1]["file_id"]
        return r.get("ok", False)
    except Exception as e:
        logger.error("send_photo error %s: %s", image_path, e)
        return False


async def send_guide(chat_id: int, reply_markup=None) -> bool:
    global _guide_file_id
    caption = (
        "📄 <b>Гайд «Как перестать срываться на близких»</b>\n\n"
        "Физиология срывов: почему они случаются и как с этим работать "
        "на уровне тела, а не силы воли.\n\n"
        "Пока читаете — есть ещё 8 вопросов, которые добавят картину. "
        "Тест на тип привязанности напрямую связан с тем, как вы ведёте себя в конфликте."
    )
    payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    if reply_markup:
        payload["reply_markup"] = reply_markup
    try:
        if _guide_file_id:
            r = await _api("sendDocument", json={**payload, "document": _guide_file_id})
        else:
            with open(GUIDE_PDF_PATH, "rb") as f:
                r = await _api("sendDocument", data=payload,
                                files={"document": f})
            if r.get("ok"):
                _guide_file_id = r["result"]["document"]["file_id"]
        return r.get("ok", False)
    except Exception as e:
        logger.error("send_guide error: %s", e)
        return False


async def notify_admin(text: str) -> None:
    try:
        await send(int(ADMIN_CHAT_ID), text)
    except Exception as e:
        logger.warning("Admin notify failed: %s", e)


# ── Keyboards ────────────────────────────────────────────────────────────────

def _main_menu():
    return {"inline_keyboard": [
        [{"text": "📄 Получить гайд бесплатно", "callback_data": "get_guide"}],
        [{"text": "🧠 Пройти тест на тип привязанности", "callback_data": "start_quiz"}],
        [{"text": "🔒 Хочу в клуб «Кубики Жизни»", "callback_data": "join_club"}],
    ]}


def _after_guide_kb():
    return {"inline_keyboard": [
        [{"text": "🧠 Пройти тест на тип привязанности", "callback_data": "start_quiz"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _quiz_kb(q_index: int):
    q = QUIZ_Q[q_index]
    return {"inline_keyboard": [
        [{"text": opt[0], "callback_data": f"q_{q_index}_{i}"}]
        for i, opt in enumerate(q["options"])
    ]}


def _dep_quiz_kb(q_index: int):
    q = DEP_Q[q_index]
    return {"inline_keyboard": [
        [{"text": opt[0], "callback_data": f"dq_{q_index}_{i}"}]
        for i, opt in enumerate(q["options"])
    ]}


def _anxious_result_kb():
    return {"inline_keyboard": [
        [{"text": "📊 Пройти расширенный тест", "callback_data": "start_dep_quiz"}],
        [{"text": "💳 Купить уроки — 990 ₽", "url": TRIPWIRE_URL}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _avoidant_result_kb():
    return {"inline_keyboard": [
        [{"text": "🔒 Записаться в клуб «Кубики Жизни»", "callback_data": "join_club"}],
    ]}


def _fearful_result_kb():
    return {"inline_keyboard": [
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
        [{"text": "📺 Читать канал", "url": CHANNEL_URL}],
    ]}


def _secure_result_kb():
    return {"inline_keyboard": [
        [{"text": "🎬 Смотреть видеоурок — 990 ₽", "url": TRIPWIRE_URL}],
        [{"text": "🔒 Хочу в клуб", "callback_data": "join_club"}],
    ]}


def _dep_result_kb():
    return {"inline_keyboard": [
        [{"text": "🔔 Записаться на практикум", "callback_data": "join_protocol"}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _fallback_kb():
    return {"inline_keyboard": [
        [{"text": "📄 Гайд", "callback_data": "get_guide"},
         {"text": "🧠 Тест", "callback_data": "start_quiz"},
         {"text": "🔒 Клуб", "callback_data": "join_club"}],
    ]}


def _lesson_kb():
    return {"inline_keyboard": [
        [{"text": "🔔 Записаться на практикум", "callback_data": "join_protocol"}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


# ── Update router ────────────────────────────────────────────────────────────

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
    low = text.lower()

    state = user_state.get(user_id, {})

    # ── Admin commands ──
    if str(user_id) == str(ADMIN_CHAT_ID):
        if low.startswith("/broadcast "):
            await _do_broadcast(text[len("/broadcast "):].strip())
            return
        if low.startswith("/broadcast_waitlist "):
            await _do_broadcast(text[len("/broadcast_waitlist "):].strip(), waitlist_only=True)
            return

    # ── /start [source] ──
    if low == "/start" or low.startswith("/start "):
        param = text[7:].strip() if low.startswith("/start ") else None
        source = _parse_source(param)
        user_state[user_id] = {**state, "source": source}
        await _welcome(chat_id, user_id, username, source)
        return

    # ── Keywords ──
    if low == GUIDE_KEYWORD.lower():
        source = state.get("source", "Прямой")
        await _deliver_guide(chat_id, user_id, username, source, "гайд")
        return

    if low == "тест":
        await _start_quiz(chat_id, user_id)
        return

    if low == "клуб":
        await _ask_name_for_club(chat_id, user_id)
        return

    if low == "урок":
        await _show_lesson_info(chat_id)
        return

    # ── Awaiting text input ──
    step = state.get("step")
    if step == "awaiting_name":
        await _save_club_registration(chat_id, user_id, username, text)
        return
    if step == "awaiting_protocol_name":
        await _save_protocol_registration(chat_id, user_id, username, text)
        return

    # ── Fallback ──
    await send(
        chat_id,
        "Я бот — свободный текст не понимаю.\n\n"
        "Напишите одно слово:\n"
        "<b>гайд</b> — пришлю бесплатный материал\n"
        "<b>тест</b> — запустим тест на тип привязанности\n"
        "<b>клуб</b> — расскажу про предзапись\n"
        "<b>урок</b> — про видеопрактикум\n\n"
        "Или нажмите кнопку 👇",
        reply_markup=_fallback_kb(),
    )


async def _handle_callback(cb: dict) -> None:
    chat_id = cb["message"]["chat"]["id"]
    user = cb.get("from", {})
    user_id = user.get("id", chat_id)
    username = user.get("username")
    data = cb.get("data", "")

    await _api("answerCallbackQuery", json={"callback_query_id": cb["id"]})

    state = user_state.get(user_id, {})
    source = state.get("source", "Прямой")

    if data == "get_guide":
        await _deliver_guide(chat_id, user_id, username, source, "кнопка меню")

    elif data == "start_quiz":
        await _start_quiz(chat_id, user_id)

    elif data.startswith("q_"):
        # q_{q_index}_{option_index}
        _, q_idx, opt_idx = data.split("_")
        await _process_quiz_answer(chat_id, user_id, username, int(q_idx), int(opt_idx))

    elif data == "start_dep_quiz":
        await _start_dep_quiz(chat_id, user_id)

    elif data.startswith("dq_"):
        # dq_{q_index}_{option_index}
        _, q_idx, opt_idx = data.split("_")
        await _process_dep_answer(chat_id, user_id, username, int(q_idx), int(opt_idx))

    elif data == "join_club":
        await _ask_name_for_club(chat_id, user_id)

    elif data == "join_protocol":
        await _ask_name_for_protocol(chat_id, user_id)


# ── Flows ────────────────────────────────────────────────────────────────────

async def _welcome(chat_id: int, user_id: int, username: str | None,
                   source: str = "Прямой") -> None:
    await notion_leads.upsert_lead(user_id=user_id, username=username,
                                   status="Зашёл", source=source, request="/start")
    await send_photo(chat_id, "images/julia.jpg")
    await send(
        chat_id,
        "Это бот Юлии Гоголевой — автора канала "
        "<a href=\"https://t.me/gogolevajuls\">Гоголева | ПсихоЛогично 🧪</a>\n\n"
        "Здесь — инструменты для тех, кто хочет разбираться в себе и в отношениях. "
        "С научной базой.\n\n"
        "📄 Гайд «Как перестать срываться на близких» — бесплатно\n"
        "🧠 Тест на тип привязанности — 8 вопросов, результат с разбором\n"
        "🔒 Предзапись в клуб «Кубики Жизни»\n\n"
        "Выбирайте 👇",
        reply_markup=_main_menu(),
    )


async def _deliver_guide(chat_id: int, user_id: int, username: str | None,
                          source: str, request: str) -> None:
    ok = await send_guide(chat_id, reply_markup=_after_guide_kb())
    if ok:
        await notion_leads.upsert_lead(user_id=user_id, username=username,
                                       status="Получил гайд", source=source, request=request)
    else:
        await send(chat_id, "Произошла ошибка при отправке файла. Попробуйте позже.")


async def _start_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "quiz", "q_answers": [], "q_index": 0}
    await send_photo(chat_id, "images/attachment_scheme.png",
                     caption=(
                         "🧠 <b>Тест на тип привязанности</b>\n\n"
                         "Теория привязанности — не популярная психология. Джон Боулби начал "
                         "собирать эту базу в 1960-х, и сегодня она объясняет большинство "
                         "повторяющихся паттернов в отношениях взрослых людей.\n\n"
                         "Тип — не приговор. Паттерны поддаются осознанной корректировке. "
                         "Но сначала нужно знать, с чем именно работать.\n\n"
                         "8 вопросов. Выбирайте первую реакцию — не ту, которой гордитесь."
                     ))
    await send(chat_id, QUIZ_Q[0]["text"], reply_markup=_quiz_kb(0))


async def _process_quiz_answer(chat_id: int, user_id: int, username: str | None,
                                 q_index: int, opt_index: int) -> None:
    state = user_state.get(user_id, {})
    if state.get("step") != "quiz" or state.get("q_index") != q_index:
        return

    opt = QUIZ_Q[q_index]["options"][opt_index]
    atype, score = opt[1], opt[2]
    state["q_answers"].append((atype, score))
    next_idx = q_index + 1

    if next_idx < len(QUIZ_Q):
        state["q_index"] = next_idx
        await send(chat_id, QUIZ_Q[next_idx]["text"], reply_markup=_quiz_kb(next_idx))
    else:
        attachment_type = quiz_result(state["q_answers"])
        source = state.get("source", "Прямой")
        user_state[user_id] = {**state, "step": None,
                                "attachment_type": attachment_type}
        await _show_quiz_result(chat_id, user_id, username, attachment_type, source)


async def _show_quiz_result(chat_id: int, user_id: int, username: str | None,
                              attachment_type: str, source: str) -> None:
    r = QUIZ_R[attachment_type]

    if attachment_type == "Тревожный":
        kb = _anxious_result_kb()
    elif attachment_type == "Избегающий":
        kb = _avoidant_result_kb()
    elif attachment_type == "Тревожно-избегающий":
        kb = _fearful_result_kb()
    else:
        kb = _secure_result_kb()

    # Extra text for avoidant — club pitch
    extra = ""
    if attachment_type == "Избегающий":
        extra = (
            "\n\nОтдельного урока для избегающего типа пока нет — он в разработке.\n\n"
            "Зато в клубе «Кубики Жизни» есть то, что работает именно для вас: "
            "разбор механик избегания, практики постепенного открытия без потери "
            "ощущения себя, и рефлексия в формате, который не давит. "
            "Каждый модуль построен так, чтобы вы двигались в своём темпе."
        )

    await send_photo(
        chat_id, r["image"],
        caption=f"<b>{r['title']}</b>\n\n{r['text']}{extra}",
        reply_markup=kb,
    )
    await notion_leads.upsert_lead(user_id=user_id, username=username,
                                   attachment_type=attachment_type,
                                   status="Получил гайд", source=source, request="тест")


# ── Deprivation quiz ─────────────────────────────────────────────────────────

async def _start_dep_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "dep_quiz",
                            "dep_answers": [], "dep_index": 0}
    await send_photo(
        chat_id, "images/dep_cover.png",
        caption=(
            "📊 <b>Расширенный тест: эмоциональная депривация</b>\n\n"
            "Тревожная привязанность часто идёт вместе с эмоциональной депривацией "
            "разной глубины. 10 вопросов помогут понять, где именно и насколько.\n\n"
            "Результат — конкретная схема, не общее описание. Выбирайте то, что "
            "откликается телесно, а не то, что кажется правильным."
        ),
    )
    await send(chat_id, DEP_Q[0]["text"], reply_markup=_dep_quiz_kb(0))


async def _process_dep_answer(chat_id: int, user_id: int, username: str | None,
                                q_index: int, opt_index: int) -> None:
    state = user_state.get(user_id, {})
    if state.get("step") != "dep_quiz" or state.get("dep_index") != q_index:
        return

    score = DEP_Q[q_index]["options"][opt_index][1]
    state["dep_answers"].append(score)
    next_idx = q_index + 1

    if next_idx < len(DEP_Q):
        state["dep_index"] = next_idx
        await send(chat_id, DEP_Q[next_idx]["text"], reply_markup=_dep_quiz_kb(next_idx))
    else:
        total = sum(state["dep_answers"])
        level = dep_result(total)
        source = state.get("source", "Прямой")
        attachment_type = state.get("attachment_type")
        user_state[user_id] = {**state, "step": None, "dep_level": level}

        r = DEP_R[level]
        await send_photo(
            chat_id, r["image"],
            caption=f"<b>{r['title']}</b>\n\n{r['text']}",
            reply_markup=_dep_result_kb(),
        )
        await notion_leads.upsert_lead(
            user_id=user_id, username=username,
            attachment_type=attachment_type,
            status="Получил гайд", source=source,
            request="тест депривации", deprivation_level=level,
        )


# ── Club registration ────────────────────────────────────────────────────────

async def _ask_name_for_club(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "awaiting_name"}
    await send(
        chat_id,
        "🔒 <b>Клуб «Кубики Жизни»</b>\n\n"
        "Закрытое сообщество, где каждый месяц разбираем одну сферу жизни — "
        "на составляющие, с упражнениями, с реальной базой. "
        "Без мотивации и воздушных шариков.\n\n"
        "Сейчас идёт предзапись: <b>740 ₽/мес</b> вместо 990 ₽ — "
        "цена фиксируется навсегда для тех, кто записывается сейчас.\n\n"
        "Как вас зовут?"
    )


async def _save_club_registration(chat_id: int, user_id: int,
                                   username: str | None, name: str) -> None:
    prev = user_state.get(user_id, {})
    source = prev.get("source", "Прямой")
    attachment_type = prev.get("attachment_type")
    dep_level = prev.get("dep_level")
    user_state[user_id] = {**prev, "step": None}

    await notion_leads.upsert_lead(
        user_id=user_id, username=username, name=name,
        attachment_type=attachment_type, status="Предзапись",
        source=source, request="клуб", deprivation_level=dep_level,
    )
    await send(
        chat_id,
        f"✅ {name}, записала вас.\n\n"
        "Как только откроем набор — напишу первой.\n\n"
        f"Пока читайте канал — <a href=\"{CHANNEL_URL}\">@gogolevajuls</a>. "
        "Там каждую неделю что-нибудь, от чего становится понятнее.",
    )
    tg = f"@{username}" if username else f"id{user_id}"
    await notify_admin(
        f"🔔 <b>Новая предзапись в клуб!</b>\n\n"
        f"👤 {name} ({tg})\n"
        f"🆔 <code>{user_id}</code>\n"
        f"🧠 Тип: {attachment_type or 'тест не проходил(а)'}\n"
        f"📊 Депривация: {dep_level or 'тест не проходил(а)'}\n"
        f"📲 Источник: {source}"
    )


# ── Protocol pre-registration ────────────────────────────────────────────────

async def _show_lesson_info(chat_id: int) -> None:
    await send(chat_id, PROTOCOL_DESCRIPTION, reply_markup=_lesson_kb())


async def _ask_name_for_protocol(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "awaiting_protocol_name"}
    await send(chat_id, "Как вас зовут? Запишу в список на практикум.")


async def _save_protocol_registration(chat_id: int, user_id: int,
                                       username: str | None, name: str) -> None:
    prev = user_state.get(user_id, {})
    source = prev.get("source", "Прямой")
    attachment_type = prev.get("attachment_type")
    dep_level = prev.get("dep_level")
    user_state[user_id] = {**prev, "step": None}

    await notion_leads.upsert_lead(
        user_id=user_id, username=username, name=name,
        attachment_type=attachment_type, status="Предзапись практикум",
        source=source, request="практикум", deprivation_level=dep_level,
    )
    await send(
        chat_id,
        f"✅ {name}, записала.\n\n"
        "Как только практикум будет готов — пришлю ссылку первой.",
    )
    tg = f"@{username}" if username else f"id{user_id}"
    await notify_admin(
        f"📊 <b>Предзапись на практикум!</b>\n\n"
        f"👤 {name} ({tg})\n"
        f"🆔 <code>{user_id}</code>\n"
        f"🧠 Тип: {attachment_type or '—'}\n"
        f"📊 Депривация: {dep_level or '—'}\n"
        f"📲 Источник: {source}"
    )


# ── Broadcast ────────────────────────────────────────────────────────────────

async def _do_broadcast(text: str, waitlist_only: bool = False) -> None:
    leads = await notion_leads.get_waitlist() if waitlist_only else await notion_leads.get_all_leads()
    label = "предзаписи" if waitlist_only else "всей базе"
    await notify_admin(f"📢 Рассылка по {label}: {len(leads)} чел...")
    sent = failed = 0
    for lead in leads:
        try:
            await send(lead["user_id"], text)
            sent += 1
        except Exception as e:
            logger.warning("Broadcast failed %s: %s", lead["user_id"], e)
            failed += 1
    await notify_admin(f"📢 <b>Рассылка завершена</b>\n✅ {sent}\n❌ {failed}")
