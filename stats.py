"""
In-memory счётчики событий (сбрасываются при перезапуске/деплое).
Хранят данные текущей сессии — для исторической аналитики смотрите Notion.
"""
from collections import Counter
from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


# ── Бот ──────────────────────────────────────────────────────────────────────
bot: Counter = Counter()
# Ключи: quiz_attachment | quiz_deprivation | quiz_talk |
#         guide | psychologist | video_lesson | articles | club | start

# ── Сайт ─────────────────────────────────────────────────────────────────────
site_clicks: Counter = Counter()
# Ключи: linkProduct | linkTalk | linkQuiz | linkFree | linkClub | linkTelegram

site_sources: Counter = Counter()
# Ключи: tiktok | instagram | youtube | direct | ...

site_pageviews: list = [0]   # list чтобы мутировать из любого модуля

since: list = [_now()]       # время старта текущей сессии
