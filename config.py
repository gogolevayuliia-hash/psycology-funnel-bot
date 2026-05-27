import os

MARKETING_BOT_TOKEN = os.environ["MARKETING_BOT_TOKEN"]
ADMIN_CHAT_ID = os.environ["ADMIN_CHAT_ID"]

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_LEADS_DB_ID = os.environ["NOTION_LEADS_DB_ID"]

# Кодовое слово для получения гайда
GUIDE_KEYWORD = "гайд"

# Ссылка на видеоурок «Нам надо поговорить» на Tribute
TRIPWIRE_URL = os.environ.get("TRIPWIRE_URL", "https://web.tribute.tg/p/vnW")

# Ссылка на практикум «Точка побега» на Tribute
ESCAPE_LESSON_URL = os.environ.get("ESCAPE_LESSON_URL", "https://t.me/tribute/app?startapp=sVVg_pc_1WKB73E")

# Ссылка на практикум «Эмоциональный голод» (со скидкой до выхода)
HUNGER_LESSON_URL = os.environ.get("HUNGER_LESSON_URL", "https://t.me/tribute/app?startapp=sVVl_pc_U0ZQUFOY")

# Ссылка на открытый канал
CHANNEL_URL = "https://t.me/gogolevajuls"

# Токен для доступа к дашборду аналитики: /dashboard?token=...
DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "change_me_in_railway")

# Tribute API ключ — для верификации входящих вебхуков
TRIBUTE_API_KEY = os.environ.get("TRIBUTE_API_KEY", "")

# Путь к PDF шпаргалке видеоурока (отправляется покупателям)
LESSON_PDF_PATH = "lesson.pdf"
