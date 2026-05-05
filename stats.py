"""
Счётчики событий с персистентным хранением на диске.

При наличии Railway Volume (путь /data) данные сохраняются каждые 5 минут
и переживают деплои/перезапуски. Без volume работает как раньше (in-memory).

Как подключить Railway Volume:
  Railway → проект → Storage → Add Volume → Mount Path: /data
"""
import json
import logging
import os
from collections import Counter
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# Путь к файлу — Railway Volume монтируется в /data
STATS_FILE = os.environ.get("STATS_FILE", "/data/stats.json")
SAVE_INTERVAL = 300  # секунд между автосохранениями (5 минут)


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


# ── Бот ──────────────────────────────────────────────────────────────────────
bot: Counter = Counter()
# Ключи: quiz_attachment | quiz_deprivation | quiz_talk |
#         guide | psychologist | video_lesson | articles | club | start

# ── Глубокие ссылки (переходы извне) ─────────────────────────────────────────
deeplinks: Counter = Counter()
# Ключи: quiz | deptest | talk | articles

# ── Сайт ─────────────────────────────────────────────────────────────────────
site_clicks: Counter = Counter()
# Ключи: linkProduct | linkTalk | linkQuiz | linkFree | linkClub | linkTelegram

site_sources: Counter = Counter()
# Ключи: tiktok | instagram | youtube | direct | ...

site_pageviews: list = [0]   # list чтобы мутировать из любого модуля

since: list = [_now()]       # время старта текущей сессии


# ── Persist ───────────────────────────────────────────────────────────────────

def load() -> None:
    """Загружает счётчики из файла при старте. Молча игнорирует если файла нет."""
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        bot.update(data.get("bot", {}))
        deeplinks.update(data.get("deeplinks", {}))
        site_clicks.update(data.get("site_clicks", {}))
        site_sources.update(data.get("site_sources", {}))
        site_pageviews[0] = data.get("site_pageviews", 0)
        saved_since = data.get("since")
        if saved_since:
            since[0] = saved_since
        logger.info("stats: loaded from %s (pageviews=%s)", STATS_FILE, site_pageviews[0])
    except FileNotFoundError:
        logger.info("stats: no saved file at %s, starting fresh", STATS_FILE)
    except Exception as e:
        logger.warning("stats: failed to load from %s: %s", STATS_FILE, e)


def save() -> None:
    """Сохраняет счётчики на диск. Молча игнорирует если нет прав/места."""
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump({
                "bot":           dict(bot),
                "deeplinks":     dict(deeplinks),
                "site_clicks":   dict(site_clicks),
                "site_sources":  dict(site_sources),
                "site_pageviews": site_pageviews[0],
                "since":         since[0],
                "saved_at":      _now(),
            }, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATS_FILE)  # атомарная замена
    except Exception as e:
        logger.warning("stats: failed to save to %s: %s", STATS_FILE, e)
