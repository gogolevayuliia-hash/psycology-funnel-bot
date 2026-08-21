"""Что именно уходит в Telegram при регистрации вебхука.

Главное здесь — отсутствие фолбэка на RAILWAY_PUBLIC_DOMAIN: именно этот фолбэк дважды
срывал переезд на CT 110, потому что копия на Railway при каждом рестарте забирала вебхук себе.
"""
import asyncio
import logging

import pytest

import main
from main import webhook_url_from


class _FakeResponse:
    def json(self):
        return {"ok": True, "result": True}


@pytest.fixture
def sent(monkeypatch):
    """Перехватывает вызов setWebhook. Возвращает список отправленных тел запроса."""
    calls: list[dict] = []

    async def fake_post(self, url, **kw):
        calls.append(kw.get("json") or {})
        return _FakeResponse()

    import httpx
    monkeypatch.setattr(httpx.AsyncClient, "post", fake_post, raising=True)
    return calls


@pytest.mark.parametrize("public_url", [
    "funnel.gogolevajuls.org",
    "https://funnel.gogolevajuls.org",
    "https://funnel.gogolevajuls.org/",
    "https://funnel.gogolevajuls.org/webhook",
    "  funnel.gogolevajuls.org  ",
    # Telegram принимает только https — схему принудительно выправляем,
    # иначе setWebhook отвечает «HTTPS url must be provided» и бот остаётся нем
    "http://funnel.gogolevajuls.org",
    "HTTPS://funnel.gogolevajuls.org/WEBHOOK",
    "https://funnel.gogolevajuls.org/webhook?x=1",
    "https://funnel.gogolevajuls.org//",
])
def test_нормализация_адреса(public_url):
    assert webhook_url_from(public_url) == "https://funnel.gogolevajuls.org/webhook"


@pytest.mark.parametrize("bad", [
    "", "   ", "https://", "///",
    ":",                              # давало 'https://:/webhook'
    "https:/funnel.gogolevajuls.org",  # опечатка в схеме: один слеш
])
def test_негодный_public_url__явная_ошибка(bad):
    """Раньше такие значения давали адрес, который Telegram молча отвергал."""
    with pytest.raises(ValueError):
        webhook_url_from(bad)


def test_путь_из_public_url_отбрасывается():
    """Маршрут в приложении один. Путь из переменной дал бы 404 на каждый апдейт."""
    assert webhook_url_from("https://funnel.gogolevajuls.org/sub/path") == \
        "https://funnel.gogolevajuls.org/webhook"


def test_сеть_моргнула__три_попытки_и_явная_ошибка(monkeypatch):
    """Без ретраев транзиентный сбой оставлял вебхук у прежнего владельца навсегда."""
    attempts = []

    async def flaky_post(self, url, **kw):
        attempts.append(1)
        raise ConnectionError("сеть моргнула")

    import httpx
    monkeypatch.setattr(httpx.AsyncClient, "post", flaky_post, raising=True)
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    # Не ждём бэкофф по-настоящему. Оригинал забираем до подмены, иначе рекурсия.
    real_sleep = asyncio.sleep

    async def no_wait(_seconds):
        await real_sleep(0)

    monkeypatch.setattr(asyncio, "sleep", no_wait)

    with pytest.raises(main.WebhookNotSet):
        asyncio.run(main.set_webhook())

    assert len(attempts) == 3, f"ждали 3 попытки, было {len(attempts)}"


def test_ошибка_от_telegram__повторять_бессмысленно(sent, monkeypatch):
    """ok:false — осмысленный ответ, ретраи только тянут старт."""
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    sent_holder = sent

    async def bad_post(self, url, **kw):
        sent_holder.append(kw.get("json") or {})
        return _FakeResponse2({"ok": False, "error_code": 400, "description": "bad webhook"})

    import httpx
    monkeypatch.setattr(httpx.AsyncClient, "post", bad_post, raising=True)

    with pytest.raises(main.WebhookNotSet):
        asyncio.run(main.set_webhook())

    assert len(sent_holder) == 1, "на осмысленную ошибку Telegram не должно быть повторов"


class _FakeResponse2:
    def __init__(self, body):
        self._body = body

    def json(self):
        return self._body


def test_ставит_вебхук_по_public_url(sent, monkeypatch):
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)

    asyncio.run(main.set_webhook())

    assert len(sent) == 1
    assert sent[0]["url"] == "https://funnel.gogolevajuls.org/webhook"
    assert sent[0]["drop_pending_updates"] is False, "очередь апдейтов терять нельзя"
    assert "secret_token" not in sent[0]


def test_секрет_уходит_в_setwebhook(sent, monkeypatch):
    monkeypatch.setenv("PUBLIC_URL", "funnel.gogolevajuls.org")
    monkeypatch.setenv("WEBHOOK_SECRET", "abc123")

    asyncio.run(main.set_webhook())

    assert sent[0]["secret_token"] == "abc123"


def test_без_public_url__ничего_не_ставим(sent, monkeypatch, caplog):
    monkeypatch.delenv("PUBLIC_URL", raising=False)

    with caplog.at_level(logging.WARNING):
        asyncio.run(main.set_webhook())

    assert sent == []
    assert "PUBLIC_URL not set" in caplog.text


def test_railway_public_domain_больше_НЕ_фолбэк(sent, monkeypatch):
    """Защита от перетягивания каната: копия на Railway не должна забирать вебхук."""
    monkeypatch.delenv("PUBLIC_URL", raising=False)
    monkeypatch.setenv("RAILWAY_PUBLIC_DOMAIN", "psycology-funnel-bot-production.up.railway.app")

    asyncio.run(main.set_webhook())

    assert sent == [], "копия с RAILWAY_PUBLIC_DOMAIN отобрала вебхук — фолбэк вернулся"


def test_логгер_httpx_приглушён():
    """Иначе токен бота из URL уходит открытым текстом в docker logs."""
    assert logging.getLogger("httpx").level >= logging.WARNING
