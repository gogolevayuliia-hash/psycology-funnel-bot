"""Изоляция тестов от прода.

Важно: `config.py` читает четыре переменные через `os.environ[...]` — без них модуль
не импортируется. А `lifespan` в `main.py` дёргает реальный `setWebhook` и пишет в общий
Upstash Redis. Поэтому окружение подменяем ДО импорта `main`, а любой исходящий HTTP
заворачиваем в ошибку: тест, который полез в сеть, должен падать, а не тихо трогать прод.
"""
import os
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# ── Подмена окружения до импорта приложения ───────────────────────────────────
os.environ.update({
    "MARKETING_BOT_TOKEN": "test:token-not-real",
    "ADMIN_CHAT_ID": "0",
    "NOTION_TOKEN": "test-notion",
    "NOTION_LEADS_DB_ID": "test-db",
    "DASHBOARD_TOKEN": "test-dashboard-token",
    # Пустые Upstash — stats.py уходит в режим «без Redis» и никуда не ходит
    "UPSTASH_REDIS_URL": "",
    "UPSTASH_REDIS_TOKEN": "",
    "STATS_FILE": os.path.join(tempfile.mkdtemp(prefix="funnel-stats-"), "stats.json"),
})
# Эти две в тестах выставляются точечно — здесь их быть не должно,
# иначе тест мог бы переставить боевой вебхук.
os.environ.pop("PUBLIC_URL", None)
os.environ.pop("WEBHOOK_SECRET", None)
os.environ.pop("RAILWAY_PUBLIC_DOMAIN", None)


class NetworkAccessAttempted(AssertionError):
    """Тест попытался сходить в сеть — это ошибка теста, а не приложения."""


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    """Любой реальный исходящий запрос роняет тест.

    Тесты, которым нужен httpx, подменяют его сами — поверх этой заглушки.
    """
    import httpx

    async def _boom(self, *a, **kw):
        raise NetworkAccessAttempted(
            "тест попытался сделать реальный HTTP-запрос — замокай httpx явно"
        )

    monkeypatch.setattr(httpx.AsyncClient, "post", _boom, raising=True)
    monkeypatch.setattr(httpx.AsyncClient, "get", _boom, raising=True)
