"""
Notion database for leads / waitlist.
Schema:
  Name (title)         — имя введённое пользователем
  Telegram ID (number) — user_id
  Username (rich_text) — @username
  Тип (select)         — Тревожный / Избегающий / Надёжный / Не проходил тест
  Статус (select)      — Предзапись / Получил гайд / Купил трипваер
  Источник (rich_text) — /start / гайд / кнопка
  Дата (date)          — ISO timestamp
"""
import httpx
from datetime import datetime, timezone
from config import NOTION_TOKEN, NOTION_LEADS_DB_ID

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}


async def upsert_lead(
    user_id: int,
    username: str | None,
    name: str | None = None,
    attachment_type: str | None = None,
    status: str = "Получил гайд",
    source: str = "/start",
) -> str:
    """Create or update a lead. Returns page_id."""
    # Check if lead already exists
    existing = await _find_lead(user_id)
    if existing:
        return await _update_lead(existing, name=name, attachment_type=attachment_type,
                                  status=status)
    return await _create_lead(user_id, username, name, attachment_type, status, source)


async def _find_lead(user_id: int) -> str | None:
    """Return page_id if user already in DB, else None."""
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            headers=HEADERS,
            json={"filter": {"property": "Telegram ID", "number": {"equals": user_id}}},
        )
        results = r.json().get("results", [])
        return results[0]["id"] if results else None


async def _create_lead(user_id, username, name, attachment_type, status, source) -> str:
    now = datetime.now(timezone.utc).isoformat()
    props = {
        "Name": {"title": [{"text": {"content": name or username or str(user_id)}}]},
        "Telegram ID": {"number": user_id},
        "Username": {"rich_text": [{"text": {"content": f"@{username}" if username else ""}}]},
        "Статус": {"select": {"name": status}},
        "Источник": {"rich_text": [{"text": {"content": source}}]},
        "Дата": {"date": {"start": now}},
    }
    if attachment_type:
        props["Тип"] = {"select": {"name": attachment_type}}

    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://api.notion.com/v1/pages",
            headers=HEADERS,
            json={"parent": {"database_id": NOTION_LEADS_DB_ID}, "properties": props},
        )
        return r.json().get("id", "")


async def _update_lead(page_id: str, name=None, attachment_type=None, status=None) -> str:
    props = {}
    if name:
        props["Name"] = {"title": [{"text": {"content": name}}]}
    if attachment_type:
        props["Тип"] = {"select": {"name": attachment_type}}
    if status:
        props["Статус"] = {"select": {"name": status}}

    async with httpx.AsyncClient(timeout=10) as client:
        await client.patch(
            f"https://api.notion.com/v1/pages/{page_id}",
            headers=HEADERS,
            json={"properties": props},
        )
    return page_id


async def get_waitlist() -> list[dict]:
    """Return all users with status Предзапись for broadcast."""
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            headers=HEADERS,
            json={"filter": {"property": "Статус", "select": {"equals": "Предзапись"}}},
        )
        results = r.json().get("results", [])

    leads = []
    for page in results:
        props = page["properties"]
        user_id = props.get("Telegram ID", {}).get("number")
        if user_id:
            leads.append({"user_id": int(user_id)})
    return leads


async def get_all_leads() -> list[dict]:
    """Return all leads for broadcast."""
    leads = []
    cursor = None
    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            body = {}
            if cursor:
                body["start_cursor"] = cursor
            r = await client.post(
                f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
                headers=HEADERS,
                json=body,
            )
            data = r.json()
            for page in data.get("results", []):
                props = page["properties"]
                user_id = props.get("Telegram ID", {}).get("number")
                if user_id:
                    leads.append({"user_id": int(user_id)})
            if not data.get("has_more"):
                break
            cursor = data.get("next_cursor")
    return leads
