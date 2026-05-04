import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, HTMLResponse

from config import MARKETING_BOT_TOKEN, DASHBOARD_TOKEN
import handlers
import notion_leads

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"

# Дедупликация: храним последние 1000 обработанных update_id
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
    """Обрабатывает update, пропуская дубликаты."""
    update_id = update.get("update_id")

    if update_id is not None:
        if update_id in _processed_updates:
            logger.info("Дубликат update %s — пропускаем", update_id)
            return
        _processed_updates.add(update_id)
        # Не даём кэшу расти бесконечно
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
        # Запускаем обработку в фоне — Telegram получает 200 OK мгновенно
        # и не делает повторных попыток
        asyncio.create_task(_safe_handle(update))
    except Exception as e:
        logger.error("Webhook parse error: %s", e)
    return JSONResponse({"ok": True})


@app.get("/health")
async def health():
    return {"status": "ok", "bot": "funnel"}


# ── Dashboard ─────────────────────────────────────────────────────────────────

def _pct(part: int, total: int) -> str:
    if not total:
        return "0%"
    return f"{round(part / total * 100)}%"


def _bar(part: int, total: int, color: str = "#4a64f5") -> str:
    w = round(part / total * 100) if total else 0
    return (
        f'<div style="background:#f0f0f0;border-radius:6px;height:8px;margin-top:4px">'
        f'<div style="background:{color};width:{w}%;height:8px;border-radius:6px;'
        f'min-width:3px;transition:width .4s"></div></div>'
    )


def _render_dashboard(s: dict) -> str:
    total = s.get("total", 0)
    if total == 0:
        body = "<p style='color:#888'>Данных пока нет.</p>"
    else:
        engaged      = s["engaged"]
        prereg       = s["preregistered"]
        sources      = s.get("sources", {})
        attachment   = s.get("attachment", {})
        deprivation  = s.get("deprivation", {})
        talk         = s.get("talk", {})
        rubrics      = s.get("rubrics", {})

        def rows_table(d: dict, color: str = "#4a64f5") -> str:
            if not d:
                return "<p style='color:#aaa;font-size:13px'>Нет данных</p>"
            out = ""
            mx = max(d.values()) if d else 1
            for k, v in d.items():
                out += (
                    f'<div style="margin-bottom:10px">'
                    f'<div style="display:flex;justify-content:space-between;font-size:13px">'
                    f'<span>{k}</span>'
                    f'<span style="font-weight:600;color:{color}">{v} <span style="color:#aaa;font-weight:400">({_pct(v,total)})</span></span>'
                    f'</div>{_bar(v, mx, color)}</div>'
                )
            return out

        funnel_style = "flex:1;background:#fff;border-radius:14px;padding:20px 16px;text-align:center;border:1.5px solid #eee"
        body = f"""
<h2 style="font-size:14px;font-weight:600;letter-spacing:1.5px;text-transform:uppercase;color:#888;margin:0 0 12px">Воронка</h2>
<div style="display:flex;gap:10px;margin-bottom:28px">
  <div style="{funnel_style}">
    <div style="font-size:32px;font-weight:800;color:#1a1a1a">{total}</div>
    <div style="font-size:12px;color:#888;margin-top:2px">Всего зашли</div>
  </div>
  <div style="{funnel_style}">
    <div style="font-size:32px;font-weight:800;color:#4a64f5">{engaged}</div>
    <div style="font-size:12px;color:#888;margin-top:2px">Взаимодействие</div>
    <div style="font-size:11px;color:#4a64f5;margin-top:2px">{_pct(engaged,total)}</div>
  </div>
  <div style="{funnel_style}">
    <div style="font-size:32px;font-weight:800;color:#ee7258">{prereg}</div>
    <div style="font-size:12px;color:#888;margin-top:2px">Предзапись</div>
    <div style="font-size:11px;color:#ee7258;margin-top:2px">{_pct(prereg,total)}</div>
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">📲 Источники</h3>
    {rows_table(sources, "#62d6c3")}
  </div>
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">📊 Статусы</h3>
    {rows_table(s.get("statuses",{}), "#4a64f5")}
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">🧠 Тест привязанности</h3>
    {rows_table(attachment, "#4a64f5")}
  </div>
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">📊 Депривация</h3>
    {rows_table(deprivation, "#ee7258")}
  </div>
</div>

<div style="display:grid;grid-template-columns:1fr 1fr;gap:14px;margin-bottom:14px">
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">💬 Тест на разговор</h3>
    {rows_table(talk, "#62d6c3")}
  </div>
  <div style="background:#fff;border-radius:14px;padding:18px 16px;border:1.5px solid #eee">
    <h3 style="font-size:12px;font-weight:600;letter-spacing:1.2px;text-transform:uppercase;color:#888;margin:0 0 14px">📚 Популярные рубрики</h3>
    {rows_table(rubrics, "#f4956b")}
  </div>
</div>
"""

    updated = s.get("updated_at", "—")
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Аналитика · PSYcology Bot</title>
<style>
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ font-family:-apple-system,BlinkMacSystemFont,'Inter',sans-serif;
         background:#f5f5f3; min-height:100vh; }}
  .topbar {{ background:#1a1a1a; color:#fff; padding:16px 20px;
             display:flex; align-items:center; justify-content:space-between; }}
  .topbar h1 {{ font-size:16px; font-weight:700; letter-spacing:0.3px; }}
  .topbar small {{ font-size:11px; color:#888; margin-left:10px; }}
  .refresh-btn {{
    background:#4a64f5; color:#fff; border:none; border-radius:8px;
    padding:8px 16px; font-size:13px; font-weight:600; cursor:pointer;
    text-decoration:none; display:inline-block;
  }}
  .refresh-btn:hover {{ background:#3a54e5; }}
  .content {{ max-width:800px; margin:24px auto; padding:0 16px 40px; }}
  .ts {{ font-size:11px; color:#aaa; margin-bottom:20px; }}
</style>
</head>
<body>
<div class="topbar">
  <div>
    <h1>📊 Аналитика бота</h1>
    <small>Данные из Notion · {updated}</small>
  </div>
  <a class="refresh-btn" href="">🔄 Обновить</a>
</div>
<div class="content">
{body}
</div>
</body>
</html>"""


@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request, token: str = ""):
    if token != DASHBOARD_TOKEN:
        return HTMLResponse("<h2>403 — доступ запрещён</h2>", status_code=403)
    try:
        stats = await notion_leads.get_stats()
    except Exception as e:
        logger.error("dashboard stats error: %s", e)
        stats = {"total": 0, "updated_at": "ошибка загрузки"}
    return HTMLResponse(_render_dashboard(stats))
