"""Отказы, каждый из которых раньше делал бота тихо немым.

Все шесть проверок написаны по находкам критика результата (круг 1, 21.08.2026):
провал setWebhook проходил как штатный старт, недоступность Telegram роняла lifespan
в рестарт-луп, мусорный update_id ломал кеш навсегда, а припаркованная копия затирала
статистику боевой.
"""
import asyncio
import logging

import pytest
from fastapi.testclient import TestClient

import main


class _Resp:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


@pytest.fixture
def telegram(monkeypatch):
    """Подменяет Bot API. Кладите в `.reply` тело ответа или исключение."""
    box = {"reply": _Resp({"ok": True, "result": True}), "calls": []}

    async def fake_post(self, url, **kw):
        box["calls"].append(kw.get("json") or {})
        if isinstance(box["reply"], Exception):
            raise box["reply"]
        return box["reply"]

    import httpx
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post, raising=True)
    return box


# ── setWebhook: провал больше не проходит молча ───────────────────────────────

def test_setwebhook_ответил_ok_false__поднимаем_ошибку(telegram, monkeypatch):
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    telegram["reply"] = _Resp({"ok": False, "error_code": 400,
                               "description": "Bad Request: bad webhook"})

    with pytest.raises(main.WebhookNotSet):
        asyncio.run(main.set_webhook())


def test_провал_вебхука_не_роняет_приложение_и_зовёт_админа(telegram, monkeypatch):
    """Иначе restart:unless-stopped даёт бесконечный рестарт-луп и /health не встаёт."""
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    telegram["reply"] = ConnectionError("Telegram недоступен")
    alerts: list[str] = []

    async def fake_notify(text):
        alerts.append(text)

    monkeypatch.setattr(main.handlers, "notify_admin", fake_notify)

    async def run_lifespan():
        async with main.lifespan(main.app):
            pass

    asyncio.run(run_lifespan())          # не должно бросить наружу

    assert alerts, "админа не предупредили о непоставленном вебхуке"


# ── Мусор во входящем апдейте ─────────────────────────────────────────────────

@pytest.mark.parametrize("bad_id", ["строка", {"a": 1}, [1, 2], 3.5, None])
def test_мусорный_update_id__не_роняет_кеш(bad_id, monkeypatch):
    """`/webhook` смотрит в интернет. Раньше один такой запрос ломал приём навсегда."""
    seen = []

    async def fake_handle(update):
        seen.append(update)

    monkeypatch.setattr(main.handlers, "handle_update", fake_handle)
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
    main._processed_updates.clear()

    asyncio.run(main._safe_handle({"update_id": bad_id}))

    assert len(seen) == 1, "апдейт с мусорным id должен обрабатываться, а не падать"


def test_переполнение_кеша__выбрасывает_самый_старый(monkeypatch):
    async def fake_handle(update):
        pass

    monkeypatch.setattr(main.handlers, "handle_update", fake_handle)
    main._processed_updates.clear()

    async def run():
        for i in range(main._MAX_CACHE + 5):
            await main._safe_handle({"update_id": i})

    asyncio.run(run())

    assert len(main._processed_updates) <= main._MAX_CACHE
    assert 0 not in main._processed_updates, "самый старый должен был вытесниться"
    assert main._MAX_CACHE + 4 in main._processed_updates, "свежий должен остаться"


# ── Припаркованная копия не портит статистику боевой ──────────────────────────

def test_без_public_url__автосохранение_выключено(monkeypatch, caplog):
    """Копия на Railway всю неделю отката иначе затирала бы снимок в общем Redis."""
    monkeypatch.delenv("PUBLIC_URL", raising=False)
    saved = []

    async def fake_save():
        saved.append(1)

    monkeypatch.setattr(main._stats, "save_async", fake_save)
    monkeypatch.setattr(main._stats, "SAVE_INTERVAL", 0)

    async def run():
        await asyncio.wait_for(main._autosave_loop(), timeout=1)

    with caplog.at_level(logging.WARNING):
        asyncio.run(run())          # должен просто выйти, а не крутиться вечно

    assert saved == [], "припаркованная копия записала статистику"
    assert "автосохранение статистики выключено" in caplog.text
