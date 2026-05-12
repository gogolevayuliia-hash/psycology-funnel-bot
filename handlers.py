"""
Handlers for the marketing funnel bot.
Flows: /start → menu → guide / quiz / club / lesson
       quiz → 8 questions → result → «Эмоциональный голод» test (anxious only)
       «Эмоциональный голод» (dep_quiz) → 10 questions → result → protocol pre-reg
       talk_quiz → 5 questions → result → video lesson link
       escape_quiz → 12 questions → result → club / hunger test / protocol
"""
import asyncio
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
    QUESTIONS as TALK_Q, RESULTS as TALK_R,
    calculate_result as talk_result,
)
from escape_quiz import (
    QUESTIONS as ESC_Q, RESULTS as ESC_R,
    calculate_result as escape_result,
)
from texts import (
    WELCOME, GUIDE_CAPTION, CLUB_INVITE, CLUB_CONFIRMED,
    PSYCHOLOGIST_TEXT, PSYCHOLOGIST_URL, PROTOCOL_CONFIRMED,
    FALLBACK, SITE_URL, CHANNEL_INVITE_TEXT, VIDEO_LESSON_TEXT,
    LESSON_DELIVERY_CAPTION,
)
from config import LESSON_PDF_PATH
import notion_leads
import stats as _stats
from articles import ARTICLES, ARTICLES_ALL, format_articles, format_all_articles

LETTERS = ["А", "Б", "В", "Г", "Д"]


def _build_question_text(q: dict) -> str:
    """Форматирует вопрос с вариантами ответов в тексте сообщения."""
    options_text = "\n".join(opt[0] for opt in q["options"])
    return f"{q['text']}\n\n{options_text}"

logger = logging.getLogger(__name__)
BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

# ── In-memory state ──────────────────────────────────────────────────────────
# step values: None | "quiz" | "dep_quiz" | "talk_quiz" | "escape_quiz" | "awaiting_name" | "awaiting_protocol_name"
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
            # При multipart-загрузке reply_markup должен быть JSON-строкой
            upload_payload = {**payload}
            if "reply_markup" in upload_payload:
                upload_payload["reply_markup"] = json.dumps(
                    upload_payload["reply_markup"], ensure_ascii=False
                )
            with open(GUIDE_PDF_PATH, "rb") as f:
                r = await _api("sendDocument", data=upload_payload,
                                files={"document": f})
            if r.get("ok"):
                _guide_file_id = r["result"]["document"]["file_id"]
            else:
                logger.error("sendDocument failed: %s", r)
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
        # Лид-магнит
        [{"text": "🎁 Гайд — как перестать срываться", "callback_data": "get_guide"}],
        # Бесплатный контент
        [{"text": "🧪 Тесты", "callback_data": "show_tests"},
         {"text": "📚 Рубрикатор постов", "callback_data": "show_articles"}],
        # Продукты
        [{"text": "🎬 Обучающие видео", "callback_data": "show_videos"}],
        [{"text": "🔒 Предзапись в клуб «Кубики Жизни»", "callback_data": "join_club"}],
        # Сервисы
        [{"text": "📅 Психолог", "callback_data": "psychologist"},
         {"text": "🌐 Сайт", "url": SITE_URL}],
    ]}


def _videos_menu_kb():
    """Список всех видеоуроков."""
    return {"inline_keyboard": [
        [{"text": "🎬 Нам надо поговорить. Только не так.", "callback_data": "show_video_lesson"}],
        [{"text": "← Главное меню", "callback_data": "back_to_menu"}],
    ]}


def _video_lesson_kb():
    """Кнопка покупки — показывается после текста-продажи видео."""
    return {"inline_keyboard": [
        [{"text": "🎬 Купить урок — 990 ₽", "url": TRIPWIRE_URL}],
        [{"text": "← Все видео", "callback_data": "show_videos"}],
    ]}


def _articles_menu_kb():
    """Рубрикатор — выбор темы, по 2 кнопки в ряд."""
    keys = list(ARTICLES.keys())
    rows = []
    for i in range(0, len(keys), 2):
        row = []
        for k in keys[i:i+2]:
            row.append({"text": ARTICLES[k]["title"], "callback_data": f"art_{k}"})
        rows.append(row)
    return {"inline_keyboard": rows}


async def _show_articles_menu(chat_id: int) -> None:
    """Отправляет картинку-заставку рубрикатора, затем меню тем."""
    await send_photo(chat_id, "images/cat_rubricator.png")
    await send(chat_id, "Выберите тему — пришлю материалы из канала 👇",
               reply_markup=_articles_menu_kb())


def _tests_menu_kb():
    return {"inline_keyboard": [
        [{"text": "🧠 Тип привязанности", "callback_data": "start_quiz"}],
        [{"text": "🚪 Точка побега: как вы уходите от себя", "callback_data": "start_escape_quiz"}],
        [{"text": "💔 Эмоциональный голод", "callback_data": "start_dep_quiz"}],
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


def _escape_quiz_kb(q_index: int):
    q = ESC_Q[q_index]
    return {"inline_keyboard": [
        [{"text": LETTERS[i], "callback_data": f"eq_{q_index}_{i}"}]
        for i in range(len(q["options"]))
    ]}


def _escape_result_kb(route: str):
    """
    Клавиатура после результата «Точки побега» зависит от того, куда ведёт тип:
      club     — П1 → клуб
      hunger   — П2-О → тест «Эмоциональный голод»
      protocol — остальные → практикум
    """
    if route == "club":
        return {"inline_keyboard": [
            [{"text": "🔒 Записаться в клуб «Кубики Жизни»", "callback_data": "join_club"}],
        ]}
    if route == "hunger":
        return {"inline_keyboard": [
            [{"text": "💔 Пройти тест «Эмоциональный голод»", "callback_data": "start_dep_quiz"}],
            [{"text": "📋 Записаться на практикум", "callback_data": "join_protocol"}],
        ]}
    # protocol
    return {"inline_keyboard": [
        [{"text": "📋 Записаться на практикум", "callback_data": "join_protocol"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _talk_result_kb():
    return {"inline_keyboard": [
        [{"text": "🎬 Урок про разговоры — подробнее", "callback_data": "show_video_lesson"}],
        [{"text": "🔒 Предзапись в клуб", "callback_data": "join_club"}],
    ]}


def _anxious_result_kb():
    return {"inline_keyboard": [
        [{"text": "💔 Пройти тест «Эмоциональный голод»", "callback_data": "start_dep_quiz"}],
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
        if low == "/waitlist":
            await _send_waitlist_report(chat_id)
            return

    # ── /start [param] ──
    if low == "/start" or low.startswith("/start "):
        param = text[7:].strip() if low.startswith("/start ") else None
        source = _parse_source(param)
        user_state[user_id] = {**state, "source": source}
        # Строка для аудита: можно сравнить количество /start с числом новых
        # лидов в Notion и поймать тихие потери при рестартах Railway.
        logger.info("/start user_id=%s @%s param=%r source=%s",
                    user_id, username, param, source)
        # Глубокие ссылки — считаем переходы
        _DEEPLINK_KEYS = {"deptest", "quiz", "talk", "articles", "guide", "escape"}
        if param in _DEEPLINK_KEYS:
            _stats.deeplinks[param] += 1

        if param == "deptest":
            await _show_persistent_menu(chat_id)
            await _start_dep_quiz(chat_id, user_id)
        elif param == "quiz":
            await _show_persistent_menu(chat_id)
            await _start_quiz(chat_id, user_id)
        elif param == "talk":
            await _show_persistent_menu(chat_id)
            await _start_talk_quiz(chat_id, user_id)
        elif param == "escape":
            await _show_persistent_menu(chat_id)
            await _start_escape_quiz(chat_id, user_id)
        elif param == "articles":
            await _show_persistent_menu(chat_id)
            await _show_articles_menu(chat_id)
        elif param == "guide":
            # Сразу отдаём бесплатный гайд — для ссылок из соцсетей,
            # чтобы человеку не приходилось искать кнопку.
            await _show_persistent_menu(chat_id)
            await _deliver_guide(chat_id, user_id, username, source, "гайд (deeplink)")
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

    # Аудит активности по кнопкам — каждое касание видно в Deploy Logs.
    # Полезно сопоставить с записями в Notion и dashboard-счётчиками.
    logger.info("callback %s user_id=%s @%s", data, user_id, username)

    state = user_state.get(user_id, {})
    source = state.get("source", "Прямой")

    if data == "get_guide":
        _stats.bot["guide"] += 1
        await _deliver_guide(chat_id, user_id, username, source, "кнопка меню")

    elif data == "show_tests":
        await send(chat_id, "Выберите тест 👇", reply_markup=_tests_menu_kb())

    elif data == "show_videos":
        await send(
            chat_id,
            "🎬 <b>Обучающие видео</b>\n\nВыберите урок — расскажу подробнее и покажу фрагмент 👇",
            reply_markup=_videos_menu_kb(),
        )

    elif data == "back_to_menu":
        await send(chat_id, "Выбирайте 👇", reply_markup=_main_menu())

    elif data == "show_articles":
        await _show_articles_menu(chat_id)

    elif data.startswith("art_all_"):
        category_key = data[8:]
        if category_key in ARTICLES_ALL:
            text = format_all_articles(category_key)
            await send(
                chat_id, text,
                reply_markup={"inline_keyboard": [
                    [{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}],
                    [{"text": "← Топ-3 этой темы", "callback_data": f"art_{category_key}"}],
                    [{"text": "← Все рубрики", "callback_data": "show_articles"}],
                ]},
            )

    elif data.startswith("art_"):
        category_key = data[4:]
        if category_key in ARTICLES:
            text = format_articles(category_key)
            # Показываем кнопку «Все посты» только если они есть
            all_btn = []
            if category_key in ARTICLES_ALL and len(ARTICLES_ALL[category_key]) > 3:
                all_btn = [[{"text": "📋 Все посты по теме", "callback_data": f"art_all_{category_key}"}]]
            await send(
                chat_id, text,
                reply_markup={"inline_keyboard": all_btn + [
                    [{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}],
                    [{"text": "← Другая тема", "callback_data": "show_articles"}],
                ]},
            )
            # Сохраняем рубрику в Notion (fire-and-forget)
            rubric_title = ARTICLES[category_key]["title"]
            asyncio.create_task(notion_leads.log_rubric(user_id, rubric_title))

    elif data == "start_quiz":
        _stats.bot["quiz_attachment"] += 1
        await _start_quiz(chat_id, user_id)

    elif data.startswith("q_"):
        _, q_idx, opt_idx = data.split("_")
        await _process_quiz_answer(chat_id, user_id, username, int(q_idx), int(opt_idx))

    elif data == "start_dep_quiz":
        _stats.bot["quiz_deprivation"] += 1
        await _start_dep_quiz(chat_id, user_id)

    elif data.startswith("dq_"):
        _, q_idx, opt_idx = data.split("_")
        await _process_dep_answer(chat_id, user_id, username, int(q_idx), int(opt_idx))

    elif data == "start_talk_quiz":
        _stats.bot["quiz_talk"] += 1
        await _start_talk_quiz(chat_id, user_id)

    elif data.startswith("tq_"):
        _, q_idx, opt_idx = data.split("_")
        await _process_talk_answer(chat_id, user_id, int(q_idx), int(opt_idx))

    elif data == "start_escape_quiz":
        _stats.bot["quiz_escape"] += 1
        await _start_escape_quiz(chat_id, user_id)

    elif data.startswith("eq_"):
        _, q_idx, opt_idx = data.split("_")
        await _process_escape_answer(chat_id, user_id, username, int(q_idx), int(opt_idx))

    elif data == "show_video_lesson":
        _stats.bot["video_lesson"] += 1
        await _send_preview_video(chat_id)
        await send(chat_id, VIDEO_LESSON_TEXT, reply_markup=_video_lesson_kb())

    elif data == "join_club":
        _stats.bot["club"] += 1
        await _ask_name_for_club(chat_id, user_id)

    elif data == "join_protocol":
        _stats.bot["protocol"] += 1
        await _ask_name_for_protocol(chat_id, user_id)

    elif data == "psychologist":
        _stats.bot["psychologist"] += 1
        await send(chat_id, PSYCHOLOGIST_TEXT, reply_markup=_psychologist_kb())


# ── Flows ────────────────────────────────────────────────────────────────────

async def _welcome(chat_id: int, user_id: int, username: str | None,
                   source: str = "Прямой") -> None:
    # Notion пишем в фоне — не блокируем ответ пользователю
    asyncio.create_task(notion_leads.audit_upsert(
        user_id=user_id, username=username,
        status="Зашёл", source=source, request="/start",
    ))
    await _show_persistent_menu(chat_id)
    await send_photo(chat_id, "images/julia.jpg")
    await send(chat_id, WELCOME, reply_markup=_main_menu())


async def _deliver_guide(chat_id: int, user_id: int, username: str | None,
                          source: str, request: str) -> None:
    ok = await send_guide(chat_id, reply_markup=_after_guide_kb())
    if ok:
        asyncio.create_task(notion_leads.audit_upsert(
            user_id=user_id, username=username,
            status="Получил гайд", source=source, request=request,
        ))
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
    asyncio.create_task(notion_leads.audit_upsert(
        user_id=user_id, username=username,
        attachment_type=attachment_type,
        status="Получил гайд", source=source, request="тест",
    ))


# ── Deprivation quiz ─────────────────────────────────────────────────────────

async def _start_dep_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "dep_quiz",
                            "dep_answers": [], "dep_index": 0}
    await send_photo(
        chat_id, "images/dep_cover.png",
        caption=(
            "💔 <b>Тест «Эмоциональный голод»</b>\n\n"
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
        asyncio.create_task(notion_leads.audit_upsert(
            user_id=user_id, username=username,
            attachment_type=attachment_type,
            status="Получил гайд", source=source,
            request="тест депривации", deprivation_level=level,
        ))


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
        source = state.get("source", "Прямой")
        await send_photo(chat_id, r["image"])
        await send(chat_id, f"<b>{r['title']}</b>\n\n{r['text']}",
                   reply_markup=_talk_result_kb())
        await send(chat_id, CHANNEL_INVITE_TEXT,
                   reply_markup={"inline_keyboard": [[{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}]]})
        asyncio.create_task(notion_leads.audit_upsert(
            user_id=user_id, username=None,
            status="Получил гайд", source=source,
            request="тест разговора", talk_pattern=pattern,
        ))


# ── Escape quiz («Точка побега») ─────────────────────────────────────────────

async def _start_escape_quiz(chat_id: int, user_id: int) -> None:
    prev = user_state.get(user_id, {})
    user_state[user_id] = {**prev, "step": "escape_quiz",
                            "esc_answers": [], "esc_index": 0}
    await send_photo(
        chat_id, "images/escape_cover.png",
        caption=(
            "🚪 <b>Тест «Точка побега: как вы уходите от себя»</b>\n\n"
            "Не про отношения и не про конфликт — а про то, как вы избегаете контакта "
            "с самим собой. Тест определит вашу основную стратегию побега: тревожный "
            "контакт, отключение или гиперпродуктивность. И насколько она захватила "
            "вашу жизнь.\n\n"
            "12 вопросов. Выбирайте то, что больше похоже на реальность — не на ту, "
            "которой хотелось бы."
        ),
    )
    await send(chat_id, _build_question_text(ESC_Q[0]), reply_markup=_escape_quiz_kb(0))


async def _process_escape_answer(chat_id: int, user_id: int, username: str | None,
                                   q_index: int, opt_index: int) -> None:
    state = user_state.get(user_id, {})
    if state.get("step") != "escape_quiz" or state.get("esc_index") != q_index:
        return

    opt = ESC_Q[q_index]["options"][opt_index]
    # (label, total_score, pattern_or_None)
    state["esc_answers"].append((opt[1], opt[2]))
    next_idx = q_index + 1

    if next_idx < len(ESC_Q):
        state["esc_index"] = next_idx
        await send(chat_id, _build_question_text(ESC_Q[next_idx]),
                   reply_markup=_escape_quiz_kb(next_idx))
        return

    code = escape_result(state["esc_answers"])
    # Если каким-то образом получили незнакомый код — фоллбек на ближайший
    # «Смешанный» соответствующего уровня, чтобы не упасть.
    if code not in ESC_R:
        level = code.split("-", 1)[0] if "-" in code else "П1"
        code = f"{level}-Смешанный" if level != "П1" else "П1"
    r = ESC_R[code]
    source = state.get("source", "Прямой")
    user_state[user_id] = {**state, "step": None, "escape_result": code}

    try:
        await send_photo(chat_id, r["image"])
    except Exception:
        # Картинки пока не загружены — пропускаем, не ломаем тест.
        pass
    await send(chat_id, f"<b>{r['title']}</b>\n\n{r['text']}",
               reply_markup=_escape_result_kb(r["route"]))
    await send(chat_id, CHANNEL_INVITE_TEXT,
               reply_markup={"inline_keyboard": [[{"text": "📣 Подписаться на канал", "url": CHANNEL_URL}]]})
    asyncio.create_task(notion_leads.audit_upsert(
        user_id=user_id, username=username,
        status="Получил гайд", source=source,
        request="тест Точка побега", escape_result=code,
    ))


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

    # Критичная регистрация — ждём результат, чтобы алерт админу отразил
    # реальное положение дел в Notion (а не только факт нажатия кнопки).
    page_id = await notion_leads.upsert_lead(
        user_id=user_id, username=username, name=name,
        attachment_type=attachment_type, status="Предзапись",
        source=source, request="клуб", deprivation_level=dep_level,
    )
    tg = f"@{username}" if username else f"id{user_id}"
    try:
        await send(chat_id, CLUB_CONFIRMED.format(name=name))
    except Exception as e:
        logger.error("club confirm send failed: %s", e)
    notion_flag = "✅ сохранено" if page_id else "❌ <b>НЕ сохранилось в Notion</b> — добавьте вручную!"
    await notify_admin(
        f"🔔 <b>Новая предзапись в клуб!</b>\n\n"
        f"👤 {name} ({tg})\n"
        f"🆔 <code>{user_id}</code>\n"
        f"🧠 Тип: {attachment_type or 'тест не проходил(а)'}\n"
        f"📊 Депривация: {dep_level or 'тест не проходил(а)'}\n"
        f"📲 Источник: {source}\n"
        f"📝 Notion: {notion_flag}"
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

    # Критичная регистрация — ждём результат Notion.
    page_id = await notion_leads.upsert_lead(
        user_id=user_id, username=username, name=name,
        attachment_type=attachment_type, status="Предзапись практикум",
        source=source, request="практикум", deprivation_level=dep_level,
    )
    tg = f"@{username}" if username else f"id{user_id}"
    try:
        await send(chat_id, PROTOCOL_CONFIRMED.format(name=name))
    except Exception as e:
        logger.error("protocol confirm send failed: %s", e)
    notion_flag = "✅ сохранено" if page_id else "❌ <b>НЕ сохранилось в Notion</b> — добавьте вручную!"
    await notify_admin(
        f"📊 <b>Предзапись на практикум!</b>\n\n"
        f"👤 {name} ({tg})\n"
        f"🆔 <code>{user_id}</code>\n"
        f"🧠 Тип: {attachment_type or '—'}\n"
        f"📊 Депривация: {dep_level or '—'}\n"
        f"📲 Источник: {source}\n"
        f"📝 Notion: {notion_flag}"
    )


# ── Tribute purchase delivery ────────────────────────────────────────────────

# Кэш file_id PDF шпаргалки и превью-видео (чтобы не перезаливать каждый раз)
_lesson_file_id: str | None = None
_preview_video_file_id: str | None = None

PREVIEW_VIDEO_PATH = "prew.mp4"


async def handle_tribute_purchase(tg_id: int, payload: dict) -> None:
    """
    Вызывается при успешной оплате на Tribute.
    Отправляет покупателю PDF шпаргалку и уведомляет администратора.
    """
    global _lesson_file_id
    logger.info("tribute purchase: tg_id=%s", tg_id)

    sent = await _send_lesson_pdf(tg_id)

    # Уведомление администратора
    # Данные покупки лежат внутри payload["payload"]
    p = payload.get("payload", payload)
    product_title = p.get("product_name") or "видеоурок"
    amount        = p.get("amount", "—")
    username      = p.get("telegram_username") or ""
    tg_info       = f"@{username}" if username else f"id{tg_id}"
    status = "✅ PDF отправлен" if sent else "❌ Не удалось отправить PDF (добавьте lesson.pdf в бот)"

    await notify_admin(
        f"💳 <b>Новая покупка на Tribute!</b>\n\n"
        f"👤 {tg_info} (<code>{tg_id}</code>)\n"
        f"📦 {product_title}\n"
        f"💰 {amount} ₽\n"
        f"{status}"
    )


async def _send_preview_video(chat_id: int) -> bool:
    """Отправляет превью-видео урока. Возвращает True если успешно."""
    global _preview_video_file_id
    import os
    try:
        if _preview_video_file_id:
            r = await _api("sendVideo", json={
                "chat_id": chat_id,
                "video": _preview_video_file_id,
                "supports_streaming": True,
                "width": 1280,
                "height": 720,
            })
        else:
            if not os.path.exists(PREVIEW_VIDEO_PATH):
                logger.warning("Preview video not found: %s", PREVIEW_VIDEO_PATH)
                return False
            with open(PREVIEW_VIDEO_PATH, "rb") as f:
                r = await _api("sendVideo", data={
                    "chat_id": chat_id,
                    "supports_streaming": "true",
                    "width": "1280",
                    "height": "720",
                }, files={"video": (PREVIEW_VIDEO_PATH, f, "video/mp4")})
            if r.get("ok"):
                _preview_video_file_id = r["result"]["video"]["file_id"]
            else:
                logger.error("sendVideo failed: %s", r)
        return r.get("ok", False)
    except Exception as e:
        logger.error("_send_preview_video error: %s", e)
        return False


async def _send_lesson_pdf(chat_id: int) -> bool:
    """Отправляет PDF шпаргалку. Возвращает True если успешно."""
    global _lesson_file_id
    import os
    caption = LESSON_DELIVERY_CAPTION
    payload = {"chat_id": chat_id, "caption": caption, "parse_mode": "HTML"}
    try:
        if _lesson_file_id:
            r = await _api("sendDocument", json={**payload, "document": _lesson_file_id})
        else:
            if not os.path.exists(LESSON_PDF_PATH):
                logger.error("lesson.pdf not found at %s", LESSON_PDF_PATH)
                return False
            with open(LESSON_PDF_PATH, "rb") as f:
                r = await _api("sendDocument", data=payload, files={"document": f})
            if r.get("ok"):
                _lesson_file_id = r["result"]["document"]["file_id"]
        return r.get("ok", False)
    except Exception as e:
        logger.error("_send_lesson_pdf error: %s", e)
        return False


# ── Waitlist report ──────────────────────────────────────────────────────────

async def _send_waitlist_report(chat_id: int) -> None:
    """Отправляет все предзаписи из Notion администратору."""
    await send(chat_id, "⏳ Загружаю список из Notion...")
    try:
        leads = await notion_leads.get_registrations()
    except Exception as e:
        await send(chat_id, f"❌ Ошибка Notion: {e}")
        return

    if not leads:
        await send(chat_id, "📋 Предзаписей в Notion пока нет.")
        return

    club     = [l for l in leads if "клуб" in l["status"] or "Предзапись" == l["status"]]
    protocol = [l for l in leads if "практикум" in l["status"] or "Предзапись практикум" == l["status"]]
    # Остальные — те у кого статус был перезаписан, но Запрос содержит ключевое слово
    others   = [l for l in leads if l not in club and l not in protocol]

    lines = [f"📋 <b>Все предзаписи из Notion</b> — {len(leads)} чел.\n"]

    if club:
        lines.append(f"<b>🔒 Клуб «Кубики Жизни» — {len(club)} чел.</b>")
        for l in club:
            tg = l["username"] if l["username"] != "—" else f"id{l['user_id']}"
            flag = " ⚠️" if "перезаписан" in l["status"] else ""
            lines.append(f"• {l['name']} | {tg} | {l['attachment']} | {l['source']} | {l['created']}{flag}")

    if protocol:
        lines.append(f"\n<b>📊 Практикум — {len(protocol)} чел.</b>")
        for l in protocol:
            tg = l["username"] if l["username"] != "—" else f"id{l['user_id']}"
            flag = " ⚠️" if "перезаписан" in l["status"] else ""
            lines.append(f"• {l['name']} | {tg} | {l['attachment']} | {l['source']} | {l['created']}{flag}")

    if others:
        lines.append(f"\n<b>⚠️ Статус перезаписан, но запрос указывает на регистрацию — {len(others)} чел.</b>")
        for l in others:
            tg = l["username"] if l["username"] != "—" else f"id{l['user_id']}"
            lines.append(f"• {l['name']} | {tg} | запрос: {l['zapros']} | {l['created']}")

    # Telegram ограничивает 4096 символов — режем на части если нужно
    full = "\n".join(lines)
    for i in range(0, len(full), 4000):
        await send(chat_id, full[i:i+4000])


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
