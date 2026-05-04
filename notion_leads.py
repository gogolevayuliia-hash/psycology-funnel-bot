"""
Notion database for leads / waitlist.
Schema (existing):
  Name (title)         — имя введённое пользователем
  Telegram ID (number) — user_id
  Username (rich_text) — @username
  Тип (select)         — Тревожный / Избегающий / Надёжный / Тревожно-избегающий
  Статус (select)      — Зашёл / Получил гайд / Предзапись / Предзапись практикум
  Источник (select)    — TikTok / Instagram / YouTube / Telegram / Прямой
  Запрос (rich_text)   — последнее действие
  Депривация (select)  — Д1 / Д2 / Д3 / Д4
  Дата (date)          — ISO timestamp

New fields (добавить в Notion вручную):
  Тест разговора (select) — Критик / Презрение / Оборона / Стена / Надёжный
  Рубрика (rich_text)     — последняя выбранная рубрика статей
"""
import logging
import httpx
from collections import Counter
from datetime import datetime, timezone
from config import NOTION_TOKEN, NOTION_LEADS_DB_ID

logger = logging.getLogger(__name__)

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# Маппинг внутренних ключей → человекочитаемые названия для дашборда
TALK_LABELS = {
    "criticism": "🔴 Критик",
    "contempt":  "⚫ Презрение",
    "defense":   "🔵 Оборона",
    "wall":      "🟤 Каменная стена",
    "secure":    "🟢 Надёжный",
}


async def upsert_lead(
    user_id: int,
    username: str | None,
    name: str | None = None,
    attachment_type: str | None = None,
    status: str = "Получил гайд",
    source: str = "Прямой",
    request: str = "/start",
    deprivation_level: str | None = None,
    talk_pattern: str | None = None,   # результат теста на разговор
) -> str:
    """Create or update a lead. Returns page_id."""
    existing = await _find_lead(user_id)
    if existing:
        return await _update_lead(
            existing, name=name, attachment_type=attachment_type,
            status=status, request=request,
            deprivation_level=deprivation_level, talk_pattern=talk_pattern,
        )
    return await _create_lead(
        user_id, username, name, attachment_type, status, source, request,
        deprivation_level, talk_pattern,
    )


async def log_rubric(user_id: int, rubric_title: str) -> None:
    """Записывает последнюю выбранную рубрику статей."""
    existing = await _find_lead(user_id)
    if not existing:
        return
    props = {"Рубрика": {"rich_text": [{"text": {"content": rubric_title}}]}}
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.patch(
                f"https://api.notion.com/v1/pages/{existing}",
                headers=HEADERS,
                json={"properties": props},
            )
    except Exception as e:
        logger.warning("log_rubric error: %s", e)


async def _find_lead(user_id: int) -> str | None:
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            headers=HEADERS,
            json={"filter": {"property": "Telegram ID", "number": {"equals": user_id}}},
        )
        results = r.json().get("results", [])
        return results[0]["id"] if results else None


async def _create_lead(user_id, username, name, attachment_type, status, source, request,
                       deprivation_level=None, talk_pattern=None) -> str:
    now = datetime.now(timezone.utc).isoformat()
    props = {
        "Name":        {"title": [{"text": {"content": name or username or str(user_id)}}]},
        "Telegram ID": {"number": user_id},
        "Username":    {"rich_text": [{"text": {"content": f"@{username}" if username else ""}}]},
        "Статус":      {"select": {"name": status}},
        "Источник":    {"select": {"name": source}},
        "Запрос":      {"rich_text": [{"text": {"content": request}}]},
        "Дата":        {"date": {"start": now}},
    }
    if attachment_type:
        props["Тип"] = {"select": {"name": attachment_type}}
    if deprivation_level:
        props["Депривация"] = {"select": {"name": deprivation_level}}
    if talk_pattern:
        label = TALK_LABELS.get(talk_pattern, talk_pattern)
        props["Тест разговора"] = {"select": {"name": label}}

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent": {"database_id": NOTION_LEADS_DB_ID}, "properties": props},
        )
        return r.json().get("id", "")


async def _update_lead(page_id: str, name=None, attachment_type=None, status=None,
                       request=None, deprivation_level=None, talk_pattern=None) -> str:
    props = {}
    if name:
        props["Name"] = {"title": [{"text": {"content": name}}]}
    if attachment_type:
        props["Тип"] = {"select": {"name": attachment_type}}
    if status:
        props["Статус"] = {"select": {"name": status}}
    if request:
        props["Запрос"] = {"rich_text": [{"text": {"content": request}}]}
    if deprivation_level:
        props["Депривация"] = {"select": {"name": deprivation_level}}
    if talk_pattern:
        label = TALK_LABELS.get(talk_pattern, talk_pattern)
        props["Тест разговора"] = {"select": {"name": label}}

    if not props:
        return page_id

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.patch(
                f"https://api.notion.com/v1/pages/{page_id}",
                headers=HEADERS,
                json={"properties": props},
            )
    except Exception as e:
        logger.warning("_update_lead error: %s", e)
    return page_id


# ── Stats ─────────────────────────────────────────────────────────────────────

def _sel(props: dict, key: str) -> str:
    # Notion возвращает {"select": null} для пустых полей — нужен доп. or {}
    val = (props.get(key) or {})
    sel = val.get("select") or {}
    return sel.get("name") or "—"

def _txt(props: dict, key: str) -> str:
    parts = ((props.get(key) or {}).get("rich_text")) or []
    return parts[0]["text"]["content"] if parts else "—"


async def get_stats() -> dict:
    """Выгружает все лиды из Notion и возвращает агрегированную статистику."""
    rows = []
    cursor = None
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            body: dict = {"page_size": 100}
            if cursor:
                body["start_cursor"] = cursor
            r = await client.post(
                f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
                headers=HEADERS, json=body,
            )
            data = r.json()
            for page in data.get("results", []):
                p = page["properties"]
                rows.append({
                    "status":      _sel(p, "Статус"),
                    "source":      _sel(p, "Источник"),
                    "attachment":  _sel(p, "Тип"),
                    "deprivation": _sel(p, "Депривация"),
                    "talk":        _sel(p, "Тест разговора"),
                    "rubric":      _txt(p, "Рубрика"),
                    "created":     page.get("created_time", "")[:10],
                })
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")

    total = len(rows)
    if total == 0:
        return {"total": 0}

    statuses   = Counter(r["status"]      for r in rows)
    sources    = Counter(r["source"]      for r in rows)
    attachment = Counter(r["attachment"]  for r in rows if r["attachment"] != "—")
    deprivation= Counter(r["deprivation"] for r in rows if r["deprivation"] != "—")
    talk       = Counter(r["talk"]        for r in rows if r["talk"] != "—")
    rubrics    = Counter(r["rubric"]      for r in rows if r["rubric"] != "—")

    # Воронка
    engaged     = total - statuses.get("Зашёл", 0)
    preregistered = statuses.get("Предзапись", 0) + statuses.get("Предзапись практикум", 0)

    return {
        "total":          total,
        "engaged":        engaged,
        "preregistered":  preregistered,
        "statuses":       dict(statuses.most_common()),
        "sources":        dict(sources.most_common()),
        "attachment":     dict(attachment.most_common()),
        "deprivation":    dict(deprivation.most_common()),
        "talk":           dict(talk.most_common()),
        "rubrics":        dict(rubrics.most_common(10)),
        "updated_at":     datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC"),
    }


async def get_waitlist() -> list[dict]:
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            headers=HEADERS,
            json={"filter": {"property": "Статус", "select": {"equals": "Предзапись"}}},
        )
        results = r.json().get("results", [])
    leads = []
    for page in results:
        uid = page["properties"].get("Telegram ID", {}).get("number")
        if uid:
            leads.append({"user_id": int(uid)})
    return leads


async def get_all_leads() -> list[dict]:
    leads = []
    cursor = None
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body: dict = {}
            if cursor:
                body["start_cursor"] = cursor
            r = await client.post(
                f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
                headers=HEADERS, json=body,
            )
            data = r.json()
            for page in data.get("results", []):
                uid = page["properties"].get("Telegram ID", {}).get("number")
                if uid:
                    leads.append({"user_id": int(uid)})
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
    return leads
