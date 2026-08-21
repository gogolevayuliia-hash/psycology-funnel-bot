"""Проверки вебхука через настоящий ASGI-стек FastAPI.

Запросы идут `client.post("/webhook")`, а не прямым вызовом обработчика: именно в слое
посредников и живут самые дорогие ошибки. Факт обработки ловим через `threading.Event`,
а не «проверим, что вызвался» — обработка уходит в `asyncio.create_task`, и наивная
проверка была бы зелёной даже в сломанном коде просто потому, что задача не успела стартовать.
"""
import asyncio
import threading

import pytest
from fastapi.testclient import TestClient

import main


SECRET = "s3cret-for-tests"
HEADER = "X-Telegram-Bot-Api-Secret-Token"


@pytest.fixture
def handled(monkeypatch):
    """Подменяет обработчик апдейтов. Возвращает (событие, список пойманных апдейтов)."""
    event = threading.Event()
    seen: list[dict] = []

    async def fake_handle(update: dict) -> None:
        seen.append(update)
        event.set()

    monkeypatch.setattr(main.handlers, "handle_update", fake_handle)
    main._processed_updates.clear()
    return event, seen


@pytest.fixture
def client():
    # Без контекстного менеджера — lifespan не поднимаем: он ставит боевой вебхук.
    return TestClient(main.app)


def test_секрет_задан_и_верный__апдейт_обработан(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)

    r = client.post("/webhook", json={"update_id": 1}, headers={HEADER: SECRET})

    assert r.status_code == 200
    assert event.wait(timeout=2), "апдейт не дошёл до обработчика"
    assert len(seen) == 1


def test_секрет_задан_заголовок_неверный__403_и_без_обработки(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)

    r = client.post("/webhook", json={"update_id": 2}, headers={HEADER: "wrong"})

    assert r.status_code == 403
    assert not event.wait(timeout=0.3), "апдейт с чужим секретом ушёл в обработку"
    assert seen == []


def test_секрет_задан_заголовка_нет__403_и_без_обработки(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)

    r = client.post("/webhook", json={"update_id": 3})

    assert r.status_code == 403
    assert not event.wait(timeout=0.3)
    assert seen == []


def test_секрет_не_задан__работает_как_раньше(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)

    r = client.post("/webhook", json={"update_id": 4})

    assert r.status_code == 200
    assert event.wait(timeout=2)
    assert len(seen) == 1


def test_тело_не_json__не_падает_и_не_обрабатывает(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)

    r = client.post("/webhook", content=b"not-a-json", headers={HEADER: SECRET})

    assert r.status_code == 200          # Telegram не должен ретраить мусор
    assert not event.wait(timeout=0.3)
    assert seen == []


def test_повторный_update_id__обработан_один_раз(client, handled, monkeypatch):
    event, seen = handled
    monkeypatch.delenv("WEBHOOK_SECRET", raising=False)

    assert client.post("/webhook", json={"update_id": 42}).status_code == 200
    assert event.wait(timeout=2)
    event.clear()
    assert client.post("/webhook", json={"update_id": 42}).status_code == 200
    assert not event.wait(timeout=0.3), "тот же update_id обработан повторно"
    assert len(seen) == 1


@pytest.mark.parametrize("bad_header", [
    "мусор-кириллицей".encode("utf-8"),
    b"\xe9\xff",
    "ключ-с-ёлкой-🎄".encode("utf-8"),
])
def test_не_ascii_в_заголовке__403_а_не_500(client, handled, monkeypatch, bad_header):
    """compare_digest на строках требует ASCII с обеих сторон, а заголовок приходит
    снаружи: на кириллице публичный эндпоинт отдавал 500 вместо 403.

    Значение передаём БАЙТАМИ — именно так его получает uvicorn, который декодирует
    заголовки как latin-1. Строкой httpx такое просто не отправит.
    """
    event, seen = handled
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)

    r = client.post("/webhook", json={"update_id": 5}, headers={HEADER: bad_header})

    assert r.status_code == 403, f"ждали 403, получили {r.status_code}"
    assert not event.wait(timeout=0.3)
    assert seen == []
