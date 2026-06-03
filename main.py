import asyncio
import hashlib
import hmac
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse

from config import (
    MARKETING_BOT_TOKEN, DASHBOARD_TOKEN, TRIBUTE_API_KEY,
    TRIPWIRE_URL, ESCAPE_LESSON_URL, HUNGER_LESSON_URL,
    LESSON_PDF_PATH,
)
import handlers
import notion_leads
import stats as _stats

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

_processed_updates: set[int] = set()
_MAX_CACHE = 1000


async def set_webhook() -> None:
    webhook_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not webhook_url:
        logger.warning("RAILWAY_PUBLIC_DOMAIN not set — webhook not configured")
        return
    url = f"https://{webhook_url}/webhook"
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{BASE}/setWebhook",
            json={"url": url, "max_connections": 40, "drop_pending_updates": True},
        )
        logger.info("setWebhook → %s", r.json())


async def _autosave_loop() -> None:
    """Сохраняет статистику каждые N секунд."""
    while True:
        await asyncio.sleep(_stats.SAVE_INTERVAL)
        await _stats.save_async()
        logger.info("stats: autosaved")


_HISTORICAL_SALES: list[tuple[str, int]] = [
    # (название продукта, цена ДО вычета комиссии Tribute)
    # Данные из Tribute за период до 3.06.2026 (введены вручную)
    ("Видеоурок «Точка побега»",          990),  # Vasiliy Solyanik, 2 июн, 891 получено
    ("Видеоурок «Точка побега»",          594),  # Lina, 27 мая, 534.60 получено
    ("Видеоурок «Точка побега»",          594),  # Anastasia, 27 мая
    ("Видеоурок «Точка побега»",          594),  # Руслан, 27 мая
    ("Видеоурок «Точка побега»",          594),  # Никита Новеньков, 26 мая
    ("Видеоурок «Точка побега»",          594),  # Natalia Morozova, 24 мая
    ("Видеоурок «Точка побега»",          594),  # Vladislav Bystrov, 23 мая
    ("Видеоурок «Точка побега»",          594),  # Archikatt, 23 мая
    ("Видеоурок «Нам надо поговорить»",   990),  # аноним, 1 июн, 891 получено
]


@asynccontextmanager
async def lifespan(app: FastAPI):
    await _stats.load_async()              # загружаем сохранённые данные (Redis → файл)
    await _stats.seed_historical_sales(_HISTORICAL_SALES)  # стартовые данные (один раз)
    await set_webhook()
    task = asyncio.create_task(_autosave_loop())
    yield
    await _stats.save_async()             # сохраняем при штатном завершении
    task.cancel()


app = FastAPI(lifespan=lifespan)


async def _safe_handle(update: dict) -> None:
    update_id = update.get("update_id")
    if update_id is not None:
        if update_id in _processed_updates:
            return
        _processed_updates.add(update_id)
        if len(_processed_updates) > _MAX_CACHE:
            _processed_updates.discard(min(_processed_updates))
    try:
        await handlers.handle_update(update)
    except Exception as e:
        logger.error("handle_update error (update_id=%s): %s", update_id, e)


@app.post("/webhook")
async def webhook(request: Request):
    try:
        update = await request.json()
        asyncio.create_task(_safe_handle(update))
    except Exception as e:
        logger.error("Webhook parse error: %s", e)
    return JSONResponse({"ok": True})


@app.get("/health")
async def health():
    return {"status": "ok", "bot": "funnel"}


# ── Tribute webhook ───────────────────────────────────────────────────────────

@app.post("/tribute_webhook")
async def tribute_webhook(request: Request):
    """
    Tribute отправляет POST при успешной оплате.
    Верификация через заголовок trbt-signature (HMAC-SHA256 тела запроса).
    """
    try:
        body = await request.body()

        # Верификация подписи Tribute (HMAC-SHA256 тела запроса)
        if TRIBUTE_API_KEY:
            signature = request.headers.get("trbt-signature", "")
            expected = hmac.new(
                TRIBUTE_API_KEY.encode(), body, hashlib.sha256
            ).hexdigest()
            if signature and not hmac.compare_digest(signature, expected):
                # Предупреждение вместо отказа — алгоритм подписи уточним после первой покупки
                logger.warning("tribute_webhook: signature mismatch (sig=%s expected=%s)", signature, expected)

        data = await request.json()
        logger.info("tribute_webhook payload: %s", data)

        # Тестовый запрос из Tribute — просто подтверждаем получение
        if data.get("test_event") or data.get("name") not in ("new_digital_product", None):
            if data.get("name") not in ("new_digital_product", None):
                logger.info("tribute_webhook: skipping event type=%s", data.get("name"))
            else:
                logger.info("tribute_webhook: test event received, OK")
            return JSONResponse({"ok": True})

        # Структура Tribute: данные покупки внутри data["payload"]
        tribute_payload = data.get("payload", data)

        tg_id = tribute_payload.get("telegram_user_id")

        if not tg_id:
            logger.warning("tribute_webhook: no telegram_id found. payload=%s", data)
            asyncio.create_task(handlers.notify_admin(
                f"⚠️ <b>Tribute: покупка без telegram_id</b>\n\nPayload:\n<code>{data}</code>"
            ))
            return JSONResponse({"ok": True})

        tg_id = int(tg_id)
        asyncio.create_task(handlers.handle_tribute_purchase(tg_id, data))

    except Exception as e:
        logger.error("tribute_webhook error: %s", e)

    return JSONResponse({"ok": True})


def _deep_get(d: dict, *keys):
    """Безопасное получение вложенного значения из словаря."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)
    return d


# ── Site event tracking ───────────────────────────────────────────────────────

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
}

LINK_LABELS = {
    "linkProduct":  "🎬 Видеоурок",
    "linkEscape":   "🚪 Точка побега",
    "linkTalk":     "💬 Тест на разговор",
    "linkQuiz":     "🧠 Тест привязанности",
    "linkFree":     "📄 Гайд",
    "linkClub":     "🔒 Клуб",
    "linkTelegram": "📣 Telegram канал",
}


@app.options("/track")
async def track_options():
    return JSONResponse({}, headers=CORS_HEADERS)


@app.post("/track")
async def track(request: Request):
    try:
        data = await request.json()
        event  = data.get("event", "")
        label  = data.get("label", "")
        source = (data.get("source") or "direct").lower()
        if event == "pageview":
            asyncio.create_task(_stats.incr_async("pv"))
            if source and source != "direct":
                asyncio.create_task(_stats.incr_async(f"src:{source}"))
        elif event == "click" and label:
            friendly = LINK_LABELS.get(label, label)
            asyncio.create_task(_stats.incr_async(f"click:{friendly}"))
    except Exception as e:
        logger.warning("track event error: %s", e)
    return JSONResponse({"ok": True}, headers=CORS_HEADERS)


# ── Dashboard rendering ───────────────────────────────────────────────────────

def _pct(part: int, total: int) -> str:
    return f"{round(part / total * 100)}%" if total else "0%"


def _bar(part: int, mx: int, color: str) -> str:
    w = round(part / mx * 100) if mx else 0
    return (
        f'<div style="background:#f0f0f0;border-radius:6px;height:8px;margin-top:4px">'
        f'<div style="background:{color};width:{w}%;height:8px;border-radius:6px;'
        f'min-width:{min(w,3)}px"></div></div>'
    )


def _rows(d: dict, total: int, color: str) -> str:
    if not d:
        return "<p style='color:#aaa;font-size:13px'>Нет данных</p>"
    mx = max(d.values())
    out = ""
    for k, v in d.items():
        out += (
            f'<div style="margin-bottom:10px">'
            f'<div style="display:flex;justify-content:space-between;font-size:13px">'
            f'<span>{k}</span>'
            f'<span style="font-weight:600;color:{color}">{v}'
            f'<span style="color:#bbb;font-weight:400"> ({_pct(v,total)})</span></span>'
            f'</div>{_bar(v, mx, color)}</div>'
        )
    return out


def _card(title: str, content: str) -> str:
    return (
        f'<div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">'
        f'<h3 style="font-size:11px;font-weight:600;letter-spacing:1.4px;'
        f'text-transform:uppercase;color:#999;margin:0 0 14px">{title}</h3>'
        f'{content}</div>'
    )


def _big(n, label: str, color: str, sub: str = "") -> str:
    sub_html = f'<div style="font-size:11px;margin-top:2px;color:{color}">{sub}</div>' if sub else ""
    return (
        f'<div style="flex:1;background:#fff;border-radius:14px;padding:20px 16px;'
        f'text-align:center;border:1.5px solid #eee">'
        f'<div style="font-size:36px;font-weight:800;color:{color}">{n}</div>'
        f'<div style="font-size:12px;color:#888;margin-top:3px">{label}</div>'
        f'{sub_html}'
        f'</div>'
    )


def _bot_tab(s: dict) -> str:
    total = s.get("total", 0)
    period = s.get("period", "all")
    period_label = PERIOD_LABELS.get(period, PERIOD_LABELS["all"])
    if total == 0:
        return (
            f"<p style='color:#888;padding:20px 0'>За период «{period_label}» "
            f"данных нет.</p>"
        )

    engaged   = s["engaged"]
    prereg    = s["preregistered"]
    pr_club   = s.get("prereg_club", 0)
    pr_proto  = s.get("prereg_protocol", 0)
    prereg_sub = f"клуб {pr_club} · практикум {pr_proto}" if (pr_club or pr_proto) else ""

    # in-memory кнопочная статистика
    b = _stats.bot
    bot_rows = {
        "🧠 Тест привязанности":     b["quiz_attachment"],
        "🚪 Тест «Точка побега»":    b["quiz_escape"],
        "💔 Тест «Эмоциональный голод»": b["quiz_deprivation"],
        "💬 Тест на разговор":       b["quiz_talk"],
        "📄 Гайд":                   b["guide"],
        "🎬 Видеоурок (детали)":     b["video_lesson"],
        "🩺 Психолог":               b["psychologist"],
        "🔒 Клуб":                   b["club"],
        "📋 Практикум":              b["protocol"],
    }
    bot_rows = {k: v for k, v in bot_rows.items() if v > 0}
    bot_total = sum(bot_rows.values()) or 1

    dl = _stats.deeplinks
    deeplink_rows = {
        "🧠 Тест привязанности":     dl["quiz"],
        "🚪 Точка побега":           dl["escape"],
        "💔 Эмоциональный голод":    dl["deptest"],
        "💬 Тест разговора":         dl["talk"],
        "📚 Рубрикатор постов":      dl["articles"],
        "📄 Гайд":                   dl["guide"],
        "🩺 Психолог":               dl["psy"],
        "🔒 Клуб «Кубики Жизни»":    dl["club"],
    }
    deeplink_rows = {k: v for k, v in deeplink_rows.items() if v > 0}
    dl_total = sum(deeplink_rows.values()) or 1

    funnel_title = (
        "Воронка (всего в Notion)" if period == "all"
        else f"Воронка · {period_label}"
    )

    return f"""
<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
    color:#888;margin:0 0 12px">{funnel_title}</h2>
<div style="display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap">
  {_big(total, "Зашли в бот", "#1a1a1a")}
  {_big(engaged, "Взаимодействие", "#4a64f5", _pct(engaged, total))}
  {_big(prereg, "Предзапись", "#ee7258", prereg_sub or _pct(prereg, total))}
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("📲 Источники трафика", _rows(s.get("sources",{}), total, "#62d6c3"))}
  {_card("📊 Статусы", _rows(s.get("statuses",{}), total, "#4a64f5"))}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🧠 Тест привязанности", _rows(s.get("attachment",{}), total, "#4a64f5"))}
  {_card("💔 Эмоциональный голод", _rows(s.get("deprivation",{}), total, "#ee7258"))}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🚪 Точка побега", _rows(s.get("escape",{}), total, "#cc4f35"))}
  {_card("💬 Тест на разговор", _rows(s.get("talk",{}), total, "#62d6c3"))}
</div>
<div style="margin-bottom:12px">
  {_card("📚 Популярные рубрики", _rows(s.get("rubrics",{}), total, "#f4956b"))}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🖱 Нажатия кнопок (сессия)", _rows(bot_rows, bot_total, "#4a64f5"))}
  {_card("🔗 Переходы по ссылкам (сессия)", _rows(deeplink_rows, dl_total, "#f4956b"))}
</div>
<p style="font-size:11px;color:#bbb">* Нажатия кнопок и переходы по ссылкам бота — статистика сессии, обнуляется при деплое</p>
"""


def _site_tab(site_stats: dict) -> str:
    period       = site_stats.get("period", "all")
    period_label = PERIOD_LABELS.get(period, PERIOD_LABELS["all"])
    views        = site_stats.get("pageviews", 0)
    clicks       = site_stats.get("clicks", {})
    sources      = site_stats.get("sources", {})
    since        = site_stats.get("since", "—")
    cl_total = max(sum(clicks.values()), 1)
    sr_total = max(sum(sources.values()), 1)

    title = "Посещения сайта" if period == "all" else f"Посещения сайта · {period_label}"
    if views == 0 and not clicks:
        return (
            f'<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;'
            f'text-transform:uppercase;color:#888;margin:0 0 12px">{title}</h2>'
            f'<p style="color:#888;padding:20px 0">За период «{period_label}» данных нет.</p>'
            f'<p style="font-size:11px;color:#bbb">* Разбивка по дням ведётся с подключения '
            f'Upstash Redis — на «Всё время» цифры могут быть больше, т.к. учитывают '
            f'историю до этого момента.</p>'
        )

    footer = (
        f'* Данные с {since} · сохраняются в Redis на каждое событие'
        if period == "all"
        else f'* Период «{period_label}» агрегируется из дневных счётчиков '
             f'(хранятся 90 дней). До подключения Redis события считались только в total — '
             f'на «Всё время» цифры могут быть больше.'
    )

    return f"""
<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
    color:#888;margin:0 0 12px">{title}</h2>
<div style="display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap">
  {_big(views, "Визитов на сайт", "#4a64f5")}
  {_big(sum(clicks.values()), "Кликов по ссылкам", "#62d6c3")}
  {_big(len(sources), "Источников трафика", "#ee7258")}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🖱 Клики по ссылкам", _rows(clicks, cl_total, "#4a64f5"))}
  {_card("📲 Откуда пришли", _rows(sources, sr_total, "#62d6c3"))}
</div>
<p style="font-size:11px;color:#bbb">{footer}</p>
"""


# ── Tribute sales ─────────────────────────────────────────────────────────────

async def _fetch_tribute_sales() -> dict:
    """Запрашивает список заказов через Tribute API.
    Правильные эндпоинты: /digital/orders и /physical/orders (не /shop/orders).
    Auth: заголовок Api-Key. Генерируется в Tribute → Настройки → ⋯ → API Keys.
    """
    if not TRIBUTE_API_KEY:
        return {
            "error": (
                "TRIBUTE_API_KEY не задан. "
                "Сгенерируйте в Tribute: Настройки → ⋯ → API Keys → Generate"
            ),
            "items": [],
        }
    headers = {"Api-Key": TRIBUTE_API_KEY}
    base = "https://tribute.tg/api/v1"
    all_items: list = []
    last_error: str | None = None

    async with httpx.AsyncClient(timeout=15) as client:
        for endpoint in ("/digital/orders", "/physical/orders"):
            try:
                r = await client.get(
                    f"{base}{endpoint}",
                    headers=headers,
                    params={"size": 200},
                )
                logger.info("tribute %s → %s", endpoint, r.status_code)
                if r.status_code == 401:
                    return {
                        "error": (
                            "401 — неверный ключ. "
                            "Сгенерируйте в Tribute: Настройки → ⋯ → API Keys → Generate"
                        ),
                        "items": [],
                    }
                if r.status_code == 404:
                    continue  # этот тип не поддерживается — пробуем следующий
                if r.status_code != 200:
                    last_error = f"HTTP {r.status_code} ({endpoint})"
                    continue
                data = r.json()
                raw_snippet = json.dumps(data, ensure_ascii=False)[:500]
                logger.info("tribute %s raw: %s", endpoint, raw_snippet)
                # Tribute может вернуть список или объект с items/orders/data
                if isinstance(data, list):
                    items = data
                    raw_saved = raw_snippet
                else:
                    items = (
                        data.get("items")
                        or data.get("orders")
                        or data.get("data")
                        or []
                    )
                    raw_saved = raw_snippet
                if isinstance(items, list):
                    all_items.extend(items)
            except Exception as e:
                logger.error("tribute sales fetch error (%s): %s", endpoint, e)
                last_error = str(e)[:120]

    if not all_items and last_error:
        return {"error": last_error, "items": []}
    return {"items": all_items, "total": len(all_items), "_raw": locals().get("raw_saved", "")}


# ── Products tab ───────────────────────────────────────────────────────────────

_BOT_NAME = "gogolevajuls_bot"

_PRODUCTS = [
    {
        "emoji": "📄",
        "name": "Гайд «Как перестать срываться на близких»",
        "type": "Lead magnet",
        "price": "Бесплатно",
        "link": None,
        "file": LESSON_PDF_PATH.replace("lesson.pdf", "guide.pdf"),
        "file_label": "guide.pdf",
        "keyword": "гайд",
        "status": "✅ Активен",
        "color": "#62d6c3",
        "deeplink": "guide",
    },
    {
        "emoji": "🎬",
        "name": "Видеоурок «Нам надо поговорить. Только не так.»",
        "type": "Tripwire",
        "price": "990 ₽",
        "link": TRIPWIRE_URL,
        "file": LESSON_PDF_PATH,
        "file_label": "lesson.pdf (шпаргалка)",
        "keyword": "урок",
        "status": "✅ Активен",
        "color": "#4a64f5",
        "deeplink": None,
    },
    {
        "emoji": "🧠",
        "name": "Практикум «Точка побега»",
        "type": "Практикум",
        "price": "742 ₽ (скидка 25%)",
        "link": ESCAPE_LESSON_URL,
        "file": None,
        "file_label": None,
        "keyword": None,
        "status": "✅ Активен",
        "color": "#f4956b",
        "deeplink": "escape",
    },
    {
        "emoji": "💔",
        "name": "Практикум «Эмоциональный голод»",
        "type": "Практикум",
        "price": "Скидка 40% до выхода",
        "link": HUNGER_LESSON_URL,
        "file": None,
        "file_label": None,
        "keyword": None,
        "status": "🔜 Продаётся до выхода",
        "color": "#ee7258",
        "deeplink": "deptest",
    },
    {
        "emoji": "🔒",
        "name": "Клуб «Кубики Жизни»",
        "type": "Подписка",
        "price": "740 ₽/мес (предзапись)",
        "link": None,
        "file": None,
        "file_label": None,
        "keyword": "клуб",
        "status": "🔜 Предзапись",
        "color": "#9b6cf5",
        "deeplink": "club",
    },
]

_BOT_KEYWORDS = [
    ("гайд",    "Бесплатный гайд «Как перестать срываться»"),
    ("тест",    "Тест на тип привязанности"),
    ("клуб",    "Предзапись в клуб «Кубики Жизни»"),
    ("урок",    "Видеоурок «Нам надо поговорить»"),
    ("психолог","Записаться к психологу"),
]

_BOT_DEEPLINKS = [
    ("quiz",     "🧠 Тест привязанности"),
    ("escape",   "🚪 Тест «Точка побега»"),
    ("deptest",  "💔 Тест «Эмоциональный голод»"),
    ("talk",     "💬 Тест на разговор"),
    ("articles", "📚 Рубрикатор постов"),
    ("guide",    "📄 Гайд"),
    ("psy",      "🩺 Психолог"),
    ("club",     "🔒 Клуб «Кубики Жизни»"),
]


def _file_updated(path: str | None) -> str:
    if not path:
        return "—"
    try:
        ts = os.path.getmtime(path)
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%d.%m.%Y")
    except Exception:
        return "—"


def _products_tab(sales_data: dict) -> str:
    # ── Продажи из локальной статистики ──────────────────────────────────
    product_counts:  dict[str, int] = sales_data.get("_counts", {})
    product_revenue: dict[str, int] = sales_data.get("_revenue", {})
    total_sales   = sum(product_counts.values())
    total_revenue = sum(product_revenue.values())

    # ── Products catalog ───────────────────────────────────────────────────
    catalog_html = ""
    for p in _PRODUCTS:
        link_html = (
            f'<a href="{p["link"]}" target="_blank" style="color:{p["color"]};'
            f'font-size:12px;font-weight:600;text-decoration:none">'
            f'🛒 Открыть на Tribute</a>'
            if p["link"] else
            '<span style="font-size:12px;color:#bbb">нет ссылки (через бота)</span>'
        )
        deeplink_html = (
            f'<a href="https://t.me/{_BOT_NAME}?start={p["deeplink"]}" target="_blank" '
            f'style="color:#888;font-size:11px;text-decoration:none">'
            f't.me/{_BOT_NAME}?start={p["deeplink"]}</a>'
            if p["deeplink"] else ""
        )
        keyword_html = (
            f'<span style="background:#f5f5f3;border-radius:4px;padding:2px 7px;'
            f'font-size:11px;font-family:monospace;color:#555">{p["keyword"]}</span>'
            if p["keyword"] else ""
        )
        file_html = ""
        if p["file_label"]:
            updated_date = _file_updated(p["file"])
            file_html = (
                f'<span style="font-size:11px;color:#888">📁 {p["file_label"]} '
                f'<span style="color:#bbb">· обновлён {updated_date}</span></span>'
            )

        catalog_html += f"""
<div style="background:#fff;border-radius:14px;padding:16px;border:1.5px solid #eee;
    border-left:4px solid {p['color']};margin-bottom:10px">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:8px">
    <div style="flex:1">
      <div style="font-size:14px;font-weight:700;margin-bottom:4px">{p['emoji']} {p['name']}</div>
      <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin-bottom:8px">
        <span style="background:{p['color']}22;color:{p['color']};border-radius:5px;
            padding:2px 8px;font-size:11px;font-weight:600">{p['type']}</span>
        <span style="font-size:13px;font-weight:600;color:#1a1a1a">{p['price']}</span>
        <span style="font-size:11px;color:#888">{p['status']}</span>
      </div>
      <div style="display:flex;flex-direction:column;gap:4px">
        {link_html}
        {f'<div>{deeplink_html}</div>' if deeplink_html else ''}
        {f'<div>{keyword_html}</div>' if keyword_html else ''}
        {f'<div>{file_html}</div>' if file_html else ''}
      </div>
    </div>
  </div>
</div>"""

    # ── Bot deeplinks ──────────────────────────────────────────────────────
    deeplinks_html = ""
    for slug, label in _BOT_DEEPLINKS:
        url = f"https://t.me/{_BOT_NAME}?start={slug}"
        deeplinks_html += (
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:8px 0;border-bottom:1px solid #f5f5f3">'
            f'<span style="font-size:13px">{label}</span>'
            f'<a href="{url}" target="_blank" style="font-size:11px;color:#4a64f5;'
            f'font-family:monospace;text-decoration:none">?start={slug}</a>'
            f'</div>'
        )

    # ── Bot keywords ───────────────────────────────────────────────────────
    keywords_html = ""
    for kw, desc in _BOT_KEYWORDS:
        keywords_html += (
            f'<div style="display:flex;justify-content:space-between;align-items:center;'
            f'padding:8px 0;border-bottom:1px solid #f5f5f3">'
            f'<span style="background:#f5f5f3;border-radius:5px;padding:3px 10px;'
            f'font-size:12px;font-family:monospace;color:#1a1a1a;font-weight:600">{kw}</span>'
            f'<span style="font-size:12px;color:#666">{desc}</span>'
            f'</div>'
        )

    # ── Sales section ──────────────────────────────────────────────────────
    if total_sales == 0:
        sales_html = (
            '<p style="color:#aaa;font-size:13px">Продаж пока нет.</p>'
            '<p style="font-size:11px;color:#bbb;margin-top:4px">'
            'Счётчик работает с момента этого деплоя — каждая новая покупка '
            'через Tribute будет здесь автоматически.</p>'
        )
    else:
        received = round(total_revenue * 0.9)
        sales_html = f"""
<div style="display:flex;gap:10px;margin-bottom:16px;flex-wrap:wrap">
  {_big(total_sales, "Продаж всего", "#4a64f5")}
  {_big(f"{total_revenue:,}".replace(",", " ") + " ₽", "Выручка (до комиссии)", "#62d6c3")}
  {_big(f"{received:,}".replace(",", " ") + " ₽", "Получено (−10% Tribute)", "#f4956b")}
</div>"""
        if product_counts:
            mx = max(product_counts.values())
            for pname, cnt in sorted(product_counts.items(), key=lambda x: -x[1]):
                rev = product_revenue.get(pname, 0)
                rev_str = f"{rev:,}".replace(",", " ") + " ₽" if rev else ""
                w = round(cnt / mx * 100) if mx else 0
                sales_html += f"""
<div style="margin-bottom:10px">
  <div style="display:flex;justify-content:space-between;font-size:13px">
    <span style="max-width:70%;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{pname}</span>
    <span style="font-weight:600;color:#4a64f5">{cnt}
      <span style="color:#bbb;font-weight:400"> · {rev_str}</span></span>
  </div>
  <div style="background:#f0f0f0;border-radius:6px;height:8px;margin-top:4px">
    <div style="background:#4a64f5;width:{w}%;height:8px;border-radius:6px;
        min-width:{min(w,3)}px"></div></div>
</div>"""

    return f"""
<div style="background:#fff;border-radius:14px;padding:18px 16px;
    border:1.5px solid #eee;border-left:4px solid #4a64f5;margin-bottom:20px">
  <h3 style="font-size:11px;font-weight:600;letter-spacing:1.4px;text-transform:uppercase;
      color:#999;margin:0 0 14px">💳 Продажи Tribute</h3>
  {sales_html}
</div>

<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
    color:#888;margin:0 0 16px">Каталог продуктов</h2>
{catalog_html}

<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:20px;margin-bottom:12px">
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:11px;font-weight:600;letter-spacing:1.4px;text-transform:uppercase;
        color:#999;margin:0 0 10px">🔗 Диплинки бота</h3>
    {deeplinks_html}
  </div>
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:11px;font-weight:600;letter-spacing:1.4px;text-transform:uppercase;
        color:#999;margin:0 0 10px">⌨️ Ключевые слова</h3>
    {keywords_html}
  </div>
</div>
<p style="font-size:11px;color:#bbb">* Суммы до вычета 10% комиссии Tribute · история с 23.05.2026 · новые продажи пишутся автоматически через вебхук</p>
"""


PERIOD_LABELS = {
    "all":   "Всё время",
    "today": "Сегодня",
    "7d":    "7 дней",
    "30d":   "30 дней",
}


def _render(bot_html: str, site_html: str, products_html: str, updated: str,
            token: str = "", tab: str = "bot", period: str = "all") -> str:
    active_bot      = " active" if tab == "bot" else ""
    active_site     = " active" if tab == "site" else ""
    active_products = " active" if tab == "products" else ""

    def _period_btn(p: str) -> str:
        cls = "period active" if p == period else "period"
        return (f'<a class="{cls}" href="?token={token}&tab={tab}&period={p}">'
                f'{PERIOD_LABELS[p]}</a>')

    # Фильтр периода применяется к бот и сайт вкладкам;
    # вкладка «Продукты» скрывает бар периода.
    period_bar = (
        '<div class="period-bar" id="periodBar">' +
        "".join(_period_btn(p) for p in ("all", "today", "7d", "30d")) +
        '</div>'
    )

    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Аналитика · PSYcology</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Inter',sans-serif;
         background:#f5f5f3; min-height:100vh; }}
  .topbar {{ background:#1a1a1a; color:#fff; padding:14px 20px;
             display:flex; align-items:center; justify-content:space-between; gap:12px; }}
  .topbar-left {{ display:flex; align-items:center; gap:14px; flex-wrap:wrap; }}
  .topbar h1 {{ font-size:15px; font-weight:700; white-space:nowrap; }}
  .tabs {{ display:flex; gap:4px; }}
  .tab {{ background:rgba(255,255,255,0.12); color:#ccc; border:none; border-radius:7px;
          padding:6px 14px; font-size:13px; font-weight:500; cursor:pointer; }}
  .tab.active {{ background:#4a64f5; color:#fff; }}
  .refresh-btn {{ background:#333; color:#fff; border:none; border-radius:8px;
    padding:7px 14px; font-size:13px; font-weight:600; cursor:pointer;
    text-decoration:none; display:inline-block; white-space:nowrap; }}
  .refresh-btn:hover {{ background:#444; }}
  .period-bar {{ max-width:820px; margin:18px auto 0; padding:0 16px;
    display:flex; gap:6px; flex-wrap:wrap; }}
  .period {{ background:#fff; color:#555; border:1.5px solid #e8e6e2;
    border-radius:8px; padding:6px 12px; font-size:12px; font-weight:500;
    text-decoration:none; cursor:pointer; }}
  .period:hover {{ border-color:#bbb; color:#1a1a1a; }}
  .period.active {{ background:#1a1a1a; color:#fff; border-color:#1a1a1a; }}
  .content {{ max-width:820px; margin:16px auto 0; padding:0 16px 40px; }}
  .tab-pane {{ display:none; }}
  .tab-pane.active {{ display:block; }}
  small {{ font-size:11px; color:#666; }}
</style>
</head>
<body>
<div class="topbar">
  <div class="topbar-left">
    <h1>📊 Аналитика</h1>
    <div class="tabs">
      <button class="tab{active_bot}" onclick="switchTab('bot',this)">🤖 Бот</button>
      <button class="tab{active_site}" onclick="switchTab('site',this)">🌐 Сайт</button>
      <button class="tab{active_products}" onclick="switchTab('products',this)">📦 Продукты</button>
    </div>
    <small>Notion · {updated}</small>
  </div>
  <a id="refreshBtn" class="refresh-btn" href="?token={token}&tab={tab}&period={period}">🔄 Обновить</a>
</div>
{period_bar}
<div class="content">
  <div id="pane-bot" class="tab-pane{active_bot}">{bot_html}</div>
  <div id="pane-site" class="tab-pane{active_site}">{site_html}</div>
  <div id="pane-products" class="tab-pane{active_products}">{products_html}</div>
</div>
<script>
const TOKEN = {json.dumps(token)};
const PERIOD = {json.dumps(period)};
// Скрываем период-бар при первоначальной загрузке на вкладке «Продукты»
(function() {{
  const active = document.querySelector('.tab-pane.active');
  if (active && active.id === 'pane-products') {{
    const pb = document.getElementById('periodBar');
    if (pb) pb.style.display = 'none';
  }}
}})();
function switchTab(name, btn) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-' + name).classList.add('active');
  btn.classList.add('active');
  // Скрываем фильтр периода на вкладке «Продукты»
  const pb = document.getElementById('periodBar');
  if (pb) pb.style.display = (name === 'products') ? 'none' : '';
  // Запоминаем выбранную вкладку, период и в URL, и в кнопке «Обновить»,
  // чтобы refresh ничего не сбрасывал.
  const url = new URL(window.location.href);
  url.searchParams.set('token', TOKEN);
  url.searchParams.set('tab', name);
  url.searchParams.set('period', PERIOD);
  history.replaceState(null, '', url.toString());
  const btnRefresh = document.getElementById('refreshBtn');
  if (btnRefresh) btnRefresh.href = '?token=' + encodeURIComponent(TOKEN)
                                    + '&tab=' + name + '&period=' + encodeURIComponent(PERIOD);
  // Кнопки периода тоже должны помнить активную вкладку.
  document.querySelectorAll('.period').forEach(a => {{
    const u = new URL(a.href, window.location.origin);
    u.searchParams.set('tab', name);
    a.href = u.pathname + u.search;
  }});
}}
</script>
</body>
</html>"""


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, token: str = "", tab: str = "bot", period: str = "all"):
    if token != DASHBOARD_TOKEN:
        return HTMLResponse("<h2 style='padding:40px;font-family:sans-serif'>403 — доступ запрещён</h2>", status_code=403)
    active_period = period if period in ("all", "today", "7d", "30d") else "all"
    active_tab = tab if tab in ("bot", "site", "products") else "bot"

    notion_error: str | None = None
    try:
        notion_stats = await notion_leads.get_stats(period=active_period)
    except Exception as e:
        logger.error("dashboard notion error: %s", e)
        notion_error = str(e).split(":", 1)[0][:60]
        notion_stats = {"total": 0, "period": active_period}

    if notion_error:
        updated = f"⚠️ Notion временно недоступен ({notion_error}) · обновите через минуту"
    else:
        updated = notion_stats.get("updated_at", "—")

    site_stats = await _stats.get_site_stats(active_period)

    # Продажи берём из локальной статистики (пишется при каждом вебхуке Tribute).
    # Tribute API не отдаёт историю цифровых продуктов — только физических товаров.
    sales_data = {
        "items":   [],  # не используется — берём из _stats.sales напрямую
        "total":   sum(_stats.sales.values()),
        "_counts": dict(_stats.sales),
        "_revenue": dict(_stats.sales_revenue),
    }

    return HTMLResponse(_render(
        _bot_tab(notion_stats),
        _site_tab(site_stats),
        _products_tab(sales_data),
        updated, token, active_tab, active_period,
    ))
