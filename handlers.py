"""
Handlers for the marketing funnel bot.
Flows: /start → menu → guide / quiz / club / lesson
       quiz → 8 questions → result → deprivation quiz (anxious only)
       deprivation quiz → 10 questions → result → protocol pre-reg
       talk_quiz → 5 questions → result → video lesson link
"""
import json
import logging
import httpx

from config import MARKETING_BOT_TOKEN, ADMIN_CHAT_ID, GUIDE_KEYWORD, TRIPWIRE_URL, CHANNEL_URL
from quiz import QUESTIONS as QUIZ_Q, RESULTS as QUIZ_R, calculate_result as quiz_result
from deprivation_quiz import (
    QUESTIONS as DEP_Q, RESULTS as DEP_R, PROTOCOL_DESCRIPTION,
    calculate_result as dep_result,
)
from conversation_quiz import (
    QUESTIONS as TALK_Q, RESULTS as TALK_R, TALK_URL,
    calculate_result as talk_result,
)
from texts import (
    WELCOME, GUIDE_CAPTION, CLUB_INVITE, CLUB_CONFIRMED,
    PSYCHOLOGIST_TEXT, PSYCHOLOGIST_URL, PROTOCOL_CONFIRMED,
    FALLBACK, SITE_URL, CHANNEL_INVITE_TEXT, VIDEO_LESSON_TEXT,
)
import notion_leads

LETTERS = ["А", "Б", "В", "Г", "Д"]


def _build_question_text(q: dict) -> str:
    """Форматирует вопрос с вариантами ответов в тексте сообщения."""
    options_text = "\n".join(opt[0] for opt in q["options"])
    return f"{q['text']}\n\n{options_text}"

logger = logging.getLogger(__name__)
BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

# ── In-memory state ──────────────────────────────────────────────────────────
# step values: None | "quiz" | "dep_quiz" | "talk_quiz" | "awaiting_name" | "awaiting_protocol_name"
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
            # multipart upload: reply_markup должен быть JSON-строкой, не словарём
            upload_payload = {**payload}
            if "reply_markup" in upload_payload:
                upload_payload["reply_markup"] = json.dumps(
                    upload_payload["reply_markup"], ensure_ascii=False
                )
            with open(image_path, "rb") as f:
                ext = image_path.rsplit(".", 1)[-1].lower()
                r = await _api("sendPhoto", data=upload_payload,
                                files={"photo": (f"img.{ext}", f)})
            if r.get("ok"):
                _photo_cache[image_path] = r["result"]["photo"][-1]["file_id"]
            else:
                logger.error("sendPhoto failed: %s", r)
        return r.get("ok", False)
    except Exception as e:
        logger.error("send_photo error %s: %s", image_path, e)
        return False


async def send_guide(chat_id: int, reply_markup=None) -> bool:
    global _guide_file_id
    caption = GUIDE_CAPTION
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
        [{"text": "🧪 Тесты", "callback_data": "show_tests"}],
        [{"text": "🔒 Предзапись в клуб «Кубики Жизни»", "callback_data": "join_club"}],
        [{"text": "🩺 Записаться к психологу", "callback_data": "psychologist"}],
        [{"text": "🌐 Сайт", "url": SITE_URL}],
    ]}


def _tests_menu_kb():
    return {"inline_keyboard": [
        [{"text": "🧠 Тип привязанности", "callback_data": "start_quiz"}],
        [{"text": "📊 Эмоциональная депривация", "callback_data": "start_dep_quiz"}],
        [{"text": "💬 Как вы говорите в конфликте", "callback_data": "start_talk_quiz"}],
    ]}


def _after_guide_kb():
    return {"inline_keyboard": [
        [{"text": "🧠 Пройти тест на тип привязанности", "callback_data": "start_quiz"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _quiz_kb(q_index: int):
    q = QUIZ_Q[q_index]
    return {"inline_keyboard": [
        [{"text": LETTERS[i], "callback_data": f"q_{q_index}_{i}"}]
        for i in range(len(q["options"]))
    ]}


def _dep_quiz_kb(q_index: int):
    q = DEP_Q[q_index]
    return {"inline_keyboard": [
        [{"text": LETTERS[i], "callback_data": f"dq_{q_index}_{i}"}]
        for i in range(len(q["options"]))
    ]}


def _talk_quiz_kb(q_index: int):
    q = TALK_Q[q_index]
    return {"inline_keyboard": [
        [{"text": LETTERS[i], "callback_data": f"tq_{q_index}_{i}"}]
        for i in range(len(q["options"]))
    ]}


def _talk_result_kb():
    return {"inline_keyboard": [
        [{"text": "🎬 Урок про разговоры — подробнее", "callback_data": "show_video_lesson"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _anxious_result_kb():
    return {"inline_keyboard": [
        [{"text": "📊 Пройти расширенный тест", "callback_data": "start_dep_quiz"}],
        [{"text": "📋 Хочу узнать про «Практикум»", "callback_data": "join_protocol"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _avoidant_result_kb():
    return {"inline_keyboard": [
        [{"text": "🎬 Урок про разговоры — подробнее", "callback_data": "show_video_lesson"}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _fearful_result_kb():
    return {"inline_keyboard": [
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _secure_result_kb():
    return {"inline_keyboard": [
        [{"text": "🎬 Урок про разговоры — подробнее", "callback_data": "show_video_lesson"}],
        [{"text": "🔒 Хочу в клуб", "callback_data": "join_club"}],
    ]}


def _dep_result_kb():
    return {"inline_keyboard": [
        [{"text": "🔔 Записаться на практикум", "callback_data": "join_protocol"}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _video_lesson_kb():
    """Кнопка покупки — показывается после текста-продажи видео."""
    return {"inline_keyboard": [
        [{"text": "🎬 Купить урок — 990 ₽", "url": TRIPWIRE_URL}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _psychologist_kb():
    return {"inline_keyboard": [
        [{"text": "✍️ Написать ассистенту", "url": PSYCHOLOGIST_URL}],
    ]}


def _fallback_kb():
    return {"inline_keyboard": [
        [{"text": "📄 Гайд", "callback_data": "get_guide"},
         {"text": "🧪 Тесты", "callback_data": "show_tests"},
         {"text": "🔒 Клуб", "callback_data": "join_club"}],
        [{"text": "🩺 К психологу", "callback_data": "psychologist"}],
    ]}


def _lesson_kb():
    return {"inline_keyboard": [
        [{"text": "🔔 Записаться на практикум", "callback_data": "join_protocol"}],
        [{"text": "🔒 Записаться в клуб", "callback_data": "join_club"}],
    ]}


def _persistent_menu_kb():
    """Постоянная клавиатура — всегда видна внизу чата."""
    return {
        "keyboard": [[{"text": "🏠 Меню"}]],
        "resize_keyboard": True,
        "persistent": True,
    }


async def _show_persistent_menu(chat_id: int) -> None:
    """Показывает постоянную кнопку Меню (один раз при старте)."""
    await _api("sendMessage", json={
        "chat_id": chat_id,
        "text": "Кнопка «Меню» закреплена внизу — возвращайтесь сюда в любой момент 👇",
        "reply_markup": _persistent_menu_kb(),
    })


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

    # ── /start [param] ──
    if low == "/start" or low.startswith("/start "):
        param = text[7:].strip() if low.startswith("/start ") else None
        source = _parse_source(param)
        user_state[user_id] = {**state, "source": source}
        # Глубокие ссылки
        if param == "deptest":
            await _show_persistent_menu(chat_id)
            await _start_dep_quiz(chat_id, user_id)
        elif param == "quiz":
            await _show_persistent_menu(chat_id)
            await _start_quiz(chat_id, user_id)
        elif param == "talk":
            await _show_persistent_menu(chat_id)
            await _start_talk_quiz(chat_id, user_id)
        else:
            await _welcome(chat_id, user_id, username, source)
        return

    # ── Постоянная кнопка «Меню» ──
    if low in ("меню", "🏠 меню"):
        await send(chat_id, "Выбирайте 👇", reply_markup=_main_menu())
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
    await send(chat_id, FALLBACK, reply_markup=_fallback_kb())


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

    elif data == "show_tests":
        await send(chat_id, "Выберите тест 👇", reply_markup=_tests_menu_kb())

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

    elif data == "start_talk_quiz":
        await _start_talk_quiz(chat_id, user_id)

    elif data.startswith("tq_"):
        # tq_{q_index}_{option_index}
        _, q_idx, opt_idx = data.split("_")
        await _process_talk_answer(chat_id, user_id, int(q_idx), int(opt_idx))

    elif data == "show_video_lesson":
        await send(chat_id, VIDEO_LESSON_TEXT, reply_markup=_video_lesson_kb())

    elif data == "join_club":
        await _ask_name_for_club(chat_id, user_id)

    elif data == "join_protocol":
        await _ask_name_for_protocol(chat_id, user_id)

    elif data == "psychologist":
        await send(chat_id, PSYCHOLOGIST_TEXT, reply_markup=_psychologist_kb())


# ── Flows ────────────────────────────────────────────────────────────────────

async def _welcome(chat_id: int, user_id: int, username: str | None,
                   source: str = "Прямой") -> None:
    await notion_leads.upsert_lead(user_id=user_id, username=username,
                                   status="Зашёл", source=source, request="/start")
    await _show_persistent_menu(chat_id)
    await send_photo(chat_id, "images/julia.jpg")
    await send(chat_id, WELCOME, reply_markup=_main_menu())


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
    await send(chat_id, _build_question_text(QUIZ_Q[0]), reply_markup=_quiz_kb(0))


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
        await send(chat_id, _build_question_text(QUIZ_Q[next_idx]), reply_markup=_quiz_kb(next_idx))
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

    # Фото отдельно — без ограничения в 1024 символа на подпись
    await send_photo(chat_id, r["image"])
    await send(chat_id, f"<b>{r['title']}</b>\n\n{r['text']}", reply_markup=kb)
    await send(chat_id, CHANNEL_INVITE_TEXT,
               reply_markup={"inline_keyboard": [[{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}]]})
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
    await send(chat_id, _build_question_text(DEP_Q[0]), reply_markup=_dep_quiz_kb(0))


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
        await send(chat_id, _build_question_text(DEP_Q[next_idx]), reply_markup=_dep_quiz_kb(next_idx))
    else:
        total = sum(state["dep_answers"])
        level = dep_result(total)
        source = state.get("source", "Прямой")
        attachment_type = state.get("attachment_type")
        user_state[user_id] = {**state, "step": None, "dep_level": level}

        r = DEP_R[level]
        await send_photo(chat_id, r["image"])
        await send(chat_id, f"<b>{r['title']}</b>\n\n{r['text']}", reply_markup=_dep_result_kb())
        await send(chat_id, CHANNEL_INVITE_TEXT,
                   reply_markup={"inline_keyboard": [[{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}]]})
        await notion_leads.upsert_lead(
            user_id=user_id, username=username,
            attachment_type=attachment_type,
            status="Получил гайд", source=source,
            request="тест депривации", deprivation_level=level,
        )


# ── Conversation quiz (Готтман) ───────────────────────────────────────────────

async def _start_talk_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "talk_quiz",
                            "tq_answers": [], "tq_index": 0}
    await send_photo(
        chat_id, "images/talk_cover.png",
        caption=(
            "💬 <b>Тест «Как вы разговариваете в конфликте»</b>\n\n"
            "Исследователь Джон Готтман 40 лет изучал пары — и выделил четыре "
            "паттерна, которые разрушают диалог. Они называются «четыре всадника».\n\n"
            "Этот тест покажет, какой из них преобладает у вас. "
            "5 вопросов. Выбирайте первую реакцию — не ту, которой гордитесь."
        ),
    )
    await send(chat_id, _build_question_text(TALK_Q[0]), reply_markup=_talk_quiz_kb(0))


async def _process_talk_answer(chat_id: int, user_id: int,
                                 q_index: int, opt_index: int) -> None:
    state = user_state.get(user_id, {})
    if state.get("step") != "talk_quiz" or state.get("tq_index") != q_index:
        return

    opt = TALK_Q[q_index]["options"][opt_index]
    atype, score = opt[1], opt[2]
    state["tq_answers"].append((atype, score))
    next_idx = q_index + 1

    if next_idx < len(TALK_Q):
        state["tq_index"] = next_idx
        await send(chat_id, _build_question_text(TALK_Q[next_idx]),
                   reply_markup=_talk_quiz_kb(next_idx))
    else:
        pattern = talk_result(state["tq_answers"])
        user_state[user_id] = {**state, "step": None, "talk_pattern": pattern}
        r = TALK_R[pattern]
        await send_photo(chat_id, r["image"])
        await send(chat_id, f"<b>{r['title']}</b>\n\n{r['text']}",
                   reply_markup=_talk_result_kb())
        await send(chat_id, CHANNEL_INVITE_TEXT,
                   reply_markup={"inline_keyboard": [[{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}]]})


# ── Club registration ────────────────────────────────────────────────────────

async def _ask_name_for_club(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "awaiting_name"}
    await send(chat_id, CLUB_INVITE)


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
    await send(chat_id, CLUB_CONFIRMED.format(name=name))
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
    await send(chat_id, PROTOCOL_CONFIRMED.format(name=name))
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
