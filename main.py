import asyncio
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse

from config import MARKETING_BOT_TOKEN, DASHBOARD_TOKEN, TRIBUTE_API_KEY
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


@asynccontextmanager
async def lifespan(app: FastAPI):
    await set_webhook()
    yield


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
    Документация: https://tribute.tg/api
    """
    try:
        # Верификация по API ключу (Tribute передаёт его в заголовке)
        if TRIBUTE_API_KEY:
            auth = request.headers.get("Authorization", "")
            api_key = auth.replace("Bearer ", "").strip()
            if api_key != TRIBUTE_API_KEY:
                logger.warning("tribute_webhook: invalid API key")
                return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)

        data = await request.json()
        logger.info("tribute_webhook payload: %s", data)

        # Извлекаем Telegram user_id покупателя
        # Tribute может отдавать разные структуры — пробуем все варианты
        tg_id = (
            _deep_get(data, "user", "id") or
            _deep_get(data, "buyer", "telegram_id") or
            _deep_get(data, "telegram_id") or
            _deep_get(data, "user_id")
        )

        if not tg_id:
            logger.warning("tribute_webhook: no telegram_id found. payload=%s", data)
            return JSONResponse({"ok": True, "note": "no user id"})

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
            _stats.site_pageviews[0] += 1
            if source and source != "direct":
                _stats.site_sources[source] += 1
        elif event == "click" and label:
            friendly = LINK_LABELS.get(label, label)
            _stats.site_clicks[friendly] += 1
    except Exception:
        pass
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
    return (
        f'<div style="flex:1;background:#fff;border-radius:14px;padding:20px 16px;'
        f'text-align:center;border:1.5px solid #eee">'
        f'<div style="font-size:36px;font-weight:800;color:{color}">{n}</div>'
        f'<div style="font-size:12px;color:#888;margin-top:3px">{label}</div>'
        f'{"<div style=\\"font-size:11px;margin-top:2px;color:"+color+"\\">"+sub+"</div>" if sub else ""}'
        f'</div>'
    )


def _bot_tab(s: dict) -> str:
    total = s.get("total", 0)
    if total == 0:
        return "<p style='color:#888;padding:20px 0'>Данных пока нет.</p>"

    engaged = s["engaged"]
    prereg  = s["preregistered"]

    # in-memory кнопочная статистика
    b = _stats.bot
    bot_rows = {
        "🧠 Тест привязанности":  b["quiz_attachment"],
        "📊 Тест депривации":     b["quiz_deprivation"],
        "💬 Тест на разговор":    b["quiz_talk"],
        "📄 Гайд":                b["guide"],
        "🎬 Видеоурок (детали)":  b["video_lesson"],
        "🩺 Психолог":            b["psychologist"],
        "🔒 Клуб":                b["club"],
        "📋 Практикум":           b["protocol"],
    }
    bot_rows = {k: v for k, v in bot_rows.items() if v > 0}
    bot_total = sum(bot_rows.values()) or 1

    return f"""
<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
    color:#888;margin:0 0 12px">Воронка (всего в Notion)</h2>
<div style="display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap">
  {_big(total, "Зашли в бот", "#1a1a1a")}
  {_big(engaged, "Взаимодействие", "#4a64f5", _pct(engaged, total))}
  {_big(prereg, "Предзапись", "#ee7258", _pct(prereg, total))}
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("📲 Источники трафика", _rows(s.get("sources",{}), total, "#62d6c3"))}
  {_card("📊 Статусы", _rows(s.get("statuses",{}), total, "#4a64f5"))}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🧠 Тест привязанности", _rows(s.get("attachment",{}), total, "#4a64f5"))}
  {_card("📊 Депривация", _rows(s.get("deprivation",{}), total, "#ee7258"))}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("💬 Тест на разговор", _rows(s.get("talk",{}), total, "#62d6c3"))}
  {_card("📚 Популярные рубрики", _rows(s.get("rubrics",{}), total, "#f4956b"))}
</div>
<div style="margin-bottom:12px">
  {_card("🖱 Нажатия кнопок (текущая сессия)", _rows(bot_rows, bot_total, "#4a64f5"))}
</div>
<p style="font-size:11px;color:#bbb">* Нажатия кнопок сбрасываются при каждом деплое</p>
"""


def _site_tab() -> str:
    views    = _stats.site_pageviews[0]
    clicks   = dict(_stats.site_clicks.most_common())
    sources  = dict(_stats.site_sources.most_common())
    since    = _stats.since[0]
    cl_total = max(sum(clicks.values()), 1)
    sr_total = max(sum(sources.values()), 1)

    return f"""
<h2 style="font-size:13px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;
    color:#888;margin:0 0 12px">Посещения сайта (текущая сессия)</h2>
<div style="display:flex;gap:10px;margin-bottom:24px;flex-wrap:wrap">
  {_big(views, "Визитов на сайт", "#4a64f5")}
  {_big(sum(clicks.values()), "Кликов по ссылкам", "#62d6c3")}
  {_big(len(sources), "Источников трафика", "#ee7258")}
</div>
<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:12px">
  {_card("🖱 Клики по ссылкам", _rows(clicks, cl_total, "#4a64f5"))}
  {_card("📲 Откуда пришли", _rows(sources, sr_total, "#62d6c3"))}
</div>
<p style="font-size:11px;color:#bbb">* Данные сайта сбрасываются при деплое · Сессия с {since}</p>
<p style="font-size:11px;color:#bbb;margin-top:4px">* Трекинг работает только при добавлении скрипта на сайт</p>
"""


def _render(bot_html: str, site_html: str, updated: str) -> str:
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
  .content {{ max-width:820px; margin:24px auto; padding:0 16px 40px; }}
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
      <button class="tab active" onclick="switchTab('bot',this)">🤖 Бот</button>
      <button class="tab" onclick="switchTab('site',this)">🌐 Сайт</button>
    </div>
    <small>Notion · {updated}</small>
  </div>
  <a class="refresh-btn" href="">🔄 Обновить</a>
</div>
<div class="content">
  <div id="pane-bot" class="tab-pane active">{bot_html}</div>
  <div id="pane-site" class="tab-pane">{site_html}</div>
</div>
<script>
function switchTab(name, btn) {{
  document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
  document.getElementById('pane-' + name).classList.add('active');
  btn.classList.add('active');
}}
</script>
</body>
</html>"""


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, token: str = ""):
    if token != DASHBOARD_TOKEN:
        return HTMLResponse("<h2 style='padding:40px;font-family:sans-serif'>403 — доступ запрещён</h2>", status_code=403)
    try:
        notion_stats = await notion_leads.get_stats()
    except Exception as e:
        logger.error("dashboard notion error: %s", e)
        notion_stats = {"total": 0, "updated_at": f"ошибка Notion: {e}"}

    updated = notion_stats.get("updated_at", "—")
    return HTMLResponse(_render(_bot_tab(notion_stats), _site_tab(), updated))
