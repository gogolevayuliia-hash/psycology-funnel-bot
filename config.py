import os

MARKETING_BOT_TOKEN = os.environ["MARKETING_BOT_TOKEN"]
ADMIN_CHAT_ID = os.environ["ADMIN_CHAT_ID"]

NOTION_TOKEN = os.environ["NOTION_TOKEN"]
NOTION_LEADS_DB_ID = os.environ["NOTION_LEADS_DB_ID"]

# Кодовое слово для получения гайда
GUIDE_KEYWORD = "гайд"

# Ссылка на трипваер (Tribute или другой сервис оплаты)
TRIPWIRE_URL = os.environ.get("TRIPWIRE_URL", "https://tribute.tg/")

# Ссылка на открытый канал
CHANNEL_URL = "https://t.me/gogolevajuls"

# Токен для доступа к дашборду аналитики: /dashboard?token=...
DASHBOARD_TOKEN = os.environ.get("DASHBOARD_TOKEN", "change_me_in_railway")

# Tribute API ключ — для верификации входящих вебхуков
TRIBUTE_API_KEY = os.environ.get("TRIBUTE_API_KEY", "")

# Путь к PDF шпаргалке видеоурока (отправляется покупателям)
LESSON_PDF_PATH = "lesson.pdf"
