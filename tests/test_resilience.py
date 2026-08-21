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

def test_припаркованная_копия_не_пишет_в_redis_на_старте(monkeypatch, telegram):
    """seed_historical_sales сохраняет безусловно — и при сорвавшемся чтении снимка
    затирал бы всю историю продаж нулями. Копия без PUBLIC_URL не должна писать вообще."""
    monkeypatch.delenv("PUBLIC_URL", raising=False)
    writes: list[str] = []

    async def fake_load():
        pass

    async def fake_seed(_sales):
        writes.append("seed")

    async def fake_save():
        writes.append("save")

    monkeypatch.setattr(main._stats, "load_async", fake_load)
    monkeypatch.setattr(main._stats, "seed_historical_sales", fake_seed)
    monkeypatch.setattr(main._stats, "save_async", fake_save)

    async def run():
        async with main.lifespan(main.app):
            pass

    asyncio.run(run())

    assert writes == [], f"припаркованная копия писала в общий Redis: {writes}"


def test_фоновая_обработка_дожидается_остановки(monkeypatch, telegram):
    """Апдейт уже подтверждён 200-м, Telegram его не переспросит —
    оборвать обработку на середине значит потерять сообщение пользователя."""
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    finished: list[int] = []

    async def slow_handle(update):
        await asyncio.sleep(0.3)
        finished.append(update["update_id"])

    async def noop(*a, **kw):
        pass

    monkeypatch.setattr(main.handlers, "handle_update", slow_handle)
    monkeypatch.setattr(main._stats, "load_async", noop)
    monkeypatch.setattr(main._stats, "seed_historical_sales", noop)
    monkeypatch.setattr(main._stats, "save_async", noop)
    main._processed_updates.clear()

    async def run():
        async with main.lifespan(main.app):
            task = asyncio.create_task(main._safe_handle({"update_id": 777}))
            main._background.add(task)
            task.add_done_callback(main._background.discard)

    asyncio.run(run())

    assert finished == [777], "обработку оборвали на выходе — апдейт потерян"


def test_health_показывает_немого_бота(monkeypatch):
    """«Живой, но нем» раньше выглядел как полный порядок — и дважды стоил переезда."""
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    monkeypatch.setattr(main, "_webhook_ok", False)

    r = TestClient(main.app).get("/health")

    assert r.status_code == 503
    assert r.json()["webhook"] == "not_set"


def test_health_ок_у_припаркованной_копии(monkeypatch):
    """Она вебхук и не должна была ставить — красным гореть не за что."""
    monkeypatch.delenv("PUBLIC_URL", raising=False)
    monkeypatch.setattr(main, "_webhook_ok", False)

    r = TestClient(main.app).get("/health")

    assert r.status_code == 200
    assert r.json()["status"] == "ok"


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
