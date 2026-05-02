import asyncio
import logging
import os
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from config import MARKETING_BOT_TOKEN
import handlers

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
