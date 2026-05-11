"""
Notion database for leads / waitlist.
Schema (existing):
  Name (title)         — технический идентификатор: @username или Telegram ID
  Имя (rich_text)      — имя, которое человек ввёл при записи в клуб/практикум
  Telegram ID (number) — user_id
  Username (rich_text) — @username
  Тип привязанности (select) — Тревожный / Избегающий / Надёжный / Тревожно-избегающий
  Статус (select)      — Зашёл / Получил гайд / Предзапись / Предзапись практикум
  Источник (select)    — TikTok / Instagram / YouTube / Telegram / Прямой
  Запрос (rich_text)   — последнее действие
  Депривация (select)  — Д1 / Д2 / Д3 / Д4
  Дата (date)          — ISO timestamp

New fields (добавить в Notion вручную):
  Тест разговора (select) — Критик / Презрение / Оборона / Стена / Надёжный
  Рубрика (rich_text)     — последняя выбранная рубрика статей
  Точка побега (select)   — П1 / П2-Т / П2-О / П2-Н / П2-Смешанный /
                            П3-Т / П3-О / П3-Н / П3-Смешанный /
                            П4-Т / П4-О / П4-Н / П4-Смешанный
"""
import asyncio
import logging
import httpx
from collections import Counter
from datetime import datetime, timezone
from config import NOTION_TOKEN, NOTION_LEADS_DB_ID

logger = logging.getLogger(__name__)


# ── HTTP helper with status check + 429 retry ────────────────────────────────

class NotionError(Exception):
    """Поднимается когда Notion возвращает не-2xx после ретраев."""


async def _notion_request(method: str, url: str, *, json_body: dict | None = None,
                          timeout: float = 15.0, max_retries: int = 2) -> dict:
    """
    Единая обёртка над httpx с проверкой статуса и ретраем на 429.
    Бросает NotionError при неуспехе — вызывающий код решает что делать.
    """
    last_exc: Exception | None = None
    for attempt in range(max_retries + 1):
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.request(method, url, headers=HEADERS, json=json_body)
            if r.status_code == 429:
                # Rate limit. Retry-After из заголовка, иначе 1s бэкофф.
                wait = float(r.headers.get("Retry-After", 1.0))
                logger.warning("notion: 429 rate limit, retry in %ss (attempt %s/%s)",
                               wait, attempt + 1, max_retries + 1)
                await asyncio.sleep(wait)
                continue
            if r.status_code >= 400:
                # Тело ответа — критично для диагностики (object_not_found,
                # validation_error, имя несуществующей select-опции и т.п.).
                body = r.text[:800]
                logger.error("notion: %s %s → %s: %s", method, url.split("/")[-1],
                             r.status_code, body)
                raise NotionError(f"Notion {r.status_code}: {body}")
            return r.json()
        except (httpx.RequestError, httpx.HTTPError) as e:
            last_exc = e
            logger.warning("notion: network error (attempt %s/%s): %s",
                           attempt + 1, max_retries + 1, e)
            if attempt < max_retries:
                await asyncio.sleep(0.5 * (attempt + 1))
                continue
            raise NotionError(f"Notion network error: {e}") from e
    # Все попытки на 429 исчерпаны.
    raise NotionError(f"Notion 429 after {max_retries + 1} attempts") from last_exc

# Приоритет статусов — статус не может быть понижен
STATUS_PRIORITY = {
    "Предзапись практикум": 4,
    "Предзапись":           3,
    "Получил гайд":         2,
    "Зашёл":                1,
}

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
    escape_result: str | None = None,  # результат теста «Точка побега» (П1…П4-Смешанный)
) -> str | None:
    """
    Create or update a lead. Returns page_id on success, None on Notion failure
    (полное тело ошибки уже в логах). Вызывающий код решает, поднимать ли
    тревогу админу.
    """
    try:
        existing, current_status = await _find_lead(user_id)
    except NotionError:
        return None

    try:
        if existing:
            # Не понижаем статус: «Предзапись» не перезаписывается «Получил гайд»
            cur_pri = STATUS_PRIORITY.get(current_status or "", 0)
            new_pri = STATUS_PRIORITY.get(status, 0)
            effective_status = status if new_pri >= cur_pri else None
            return await _update_lead(
                existing, username=username, name=name, attachment_type=attachment_type,
                status=effective_status, request=request,
                deprivation_level=deprivation_level, talk_pattern=talk_pattern,
                escape_result=escape_result,
                registration=status if new_pri >= STATUS_PRIORITY["Предзапись"] else None,
            )
        return await _create_lead(
            user_id, username, name, attachment_type, status, source, request,
            deprivation_level, talk_pattern, escape_result,
        )
    except NotionError:
        return None
    except Exception:
        # Любая непредвиденная ошибка не должна утекать в asyncio как
        # «Task exception was never retrieved» — для fire-and-forget вызовов.
        logger.exception("upsert_lead unexpected error (user_id=%s)", user_id)
        return None


async def safe_upsert_lead(**kwargs) -> None:
    """
    Fire-and-forget обёртка для некритичных мест (тесты, /start) — гарантирует,
    что исключение не утечёт в asyncio как «Task exception was never retrieved».
    Ошибка уже залогирована в _notion_request.
    """
    await upsert_lead(**kwargs)


async def log_rubric(user_id: int, rubric_title: str) -> None:
    """Записывает последнюю выбранную рубрику статей."""
    try:
        existing, _ = await _find_lead(user_id)
    except NotionError:
        return
    if not existing:
        return
    props = {"Рубрика": {"rich_text": [{"text": {"content": rubric_title}}]}}
    try:
        await _notion_request(
            "PATCH", f"https://api.notion.com/v1/pages/{existing}",
            json_body={"properties": props},
        )
    except NotionError:
        pass  # уже залогировано в _notion_request


async def _find_lead(user_id: int) -> tuple[str | None, str | None]:
    """Returns (page_id, current_status) or (None, None). Бросает NotionError при сбое."""
    data = await _notion_request(
        "POST", f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
        json_body={"filter": {"property": "Telegram ID", "number": {"equals": user_id}}},
    )
    results = data.get("results", [])
    if not results:
        return None, None
    page = results[0]
    current_status = _sel(page["properties"], "Статус")
    return page["id"], current_status


async def _create_lead(user_id, username, name, attachment_type, status, source, request,
                       deprivation_level=None, talk_pattern=None, escape_result=None) -> str:
    """
    Создаёт лид. Если запись с дополнительным полем (Тип/Депривация/Тест разговора)
    падает по validation_error (например, опции select нет в схеме), повторяем
    создание без этого поля и добавляем его отдельным PATCH-ом. Так базовый лид
    гарантированно попадает в Notion, а реальная причина остаётся в логах.
    """
    now = datetime.now(timezone.utc).isoformat()
    # Name (title) — технический идентификатор: @username или TG ID.
    # Введённое при регистрации имя живёт отдельно в колонке «Имя».
    base_props = {
        "Name":        {"title": [{"text": {"content": (f"@{username}" if username else str(user_id))}}]},
        "Telegram ID": {"number": user_id},
        "Username":    {"rich_text": [{"text": {"content": f"@{username}" if username else ""}}]},
        "Статус":      {"select": {"name": status}},
        "Источник":    {"select": {"name": source}},
        "Запрос":      {"rich_text": [{"text": {"content": request}}]},
        "Дата":        {"date": {"start": now}},
    }
    extras: dict = {}
    if name:
        extras["Имя"] = {"rich_text": [{"text": {"content": name}}]}
    if attachment_type:
        extras["Тип привязанности"] = {"select": {"name": attachment_type}}
    if deprivation_level:
        extras["Депривация"] = {"select": {"name": deprivation_level}}
    if talk_pattern:
        label = TALK_LABELS.get(talk_pattern, talk_pattern)
        extras["Тест разговора"] = {"select": {"name": label}}
    if escape_result:
        extras["Точка побега"] = {"select": {"name": escape_result}}

    try:
        data = await _notion_request(
            "POST", "https://api.notion.com/v1/pages",
            json_body={"parent": {"database_id": NOTION_LEADS_DB_ID},
                       "properties": {**base_props, **extras}},
        )
        return data.get("id", "")
    except NotionError as e:
        if not extras:
            raise
        logger.warning("notion: create with extras failed, retry base-only: %s", e)
        # Базовый лид — без проблемных полей.
        data = await _notion_request(
            "POST", "https://api.notion.com/v1/pages",
            json_body={"parent": {"database_id": NOTION_LEADS_DB_ID},
                       "properties": base_props},
        )
        page_id = data.get("id", "")
        # Пытаемся добавить extras по одному — какое-то поле, скорее всего,
        # сломано (несуществующая select-опция или поле). Остальные сохраним.
        for prop_name, prop_value in extras.items():
            try:
                await _notion_request(
                    "PATCH", f"https://api.notion.com/v1/pages/{page_id}",
                    json_body={"properties": {prop_name: prop_value}},
                )
            except NotionError as inner:
                logger.error("notion: дроп поля %s для user=%s: %s",
                             prop_name, user_id, inner)
        return page_id


async def _update_lead(page_id: str, username=None, name=None, attachment_type=None,
                       status=None, request=None, deprivation_level=None,
                       talk_pattern=None, escape_result=None, registration=None) -> str:
    """
    registration — если передан, добавляем запись в накопительное поле «Записи»
    (клуб / практикум могут быть оба у одного человека).
    Возвращает page_id. Если общий PATCH падает (например, кривая select-опция
    в одном из полей), фоллбек — патчим каждое поле по отдельности, чтобы
    отдельные поля прошли, а сломанное осталось в логах. NotionError
    поднимается только если упал ВЕСЬ фоллбек.
    """
    props: dict = {}
    if username is not None:
        props["Username"] = {"rich_text": [{"text": {"content": f"@{username}" if username else ""}}]}
    if name:
        # Имя из формы записи на клуб/практикум — пишем в отдельную колонку,
        # чтобы не затирать Name (title), который служит идентификатором.
        props["Имя"] = {"rich_text": [{"text": {"content": name}}]}
    if attachment_type:
        props["Тип привязанности"] = {"select": {"name": attachment_type}}
    if status:
        props["Статус"] = {"select": {"name": status}}
    if request:
        props["Запрос"] = {"rich_text": [{"text": {"content": request}}]}
    if deprivation_level:
        props["Депривация"] = {"select": {"name": deprivation_level}}
    if talk_pattern:
        label = TALK_LABELS.get(talk_pattern, talk_pattern)
        props["Тест разговора"] = {"select": {"name": label}}
    if escape_result:
        props["Точка побега"] = {"select": {"name": escape_result}}

    # Накапливаем все регистрации в текстовом поле «Записи»
    if registration:
        try:
            data = await _notion_request(
                "GET", f"https://api.notion.com/v1/pages/{page_id}",
            )
            page_props = data.get("properties", {})
            existing_reg = _txt(page_props, "Записи")
            existing_reg = "" if existing_reg == "—" else existing_reg
            if registration not in existing_reg:
                new_reg = f"{existing_reg}, {registration}".lstrip(", ") if existing_reg else registration
                props["Записи"] = {"rich_text": [{"text": {"content": new_reg}}]}
        except NotionError as e:
            logger.warning("_update_lead read «Записи» failed: %s", e)

    if not props:
        return page_id

    url = f"https://api.notion.com/v1/pages/{page_id}"
    try:
        await _notion_request("PATCH", url, json_body={"properties": props})
        return page_id
    except NotionError as e:
        if len(props) == 1:
            raise
        logger.warning("notion: bulk PATCH failed, retry per-property to save what we can: %s", e)

    # Фоллбек: один PATCH на каждое поле. Если сломан Тип — Депривация всё
    # равно сохранится. Сломанное поле останется в логах.
    saved_any = False
    for prop_name, prop_value in props.items():
        try:
            await _notion_request("PATCH", url, json_body={"properties": {prop_name: prop_value}})
            saved_any = True
        except NotionError as inner:
            logger.error("notion: дроп поля %s для page=%s: %s",
                         prop_name, page_id, inner)
    if not saved_any:
        raise NotionError(f"all properties failed for page {page_id}")
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
    while True:
        body: dict = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        data = await _notion_request(
            "POST", f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            json_body=body, timeout=60.0,
        )
        for page in data.get("results", []):
            p = page["properties"]
            rows.append({
                "status":      _sel(p, "Статус"),
                "source":      _sel(p, "Источник"),
                "attachment":  _sel(p, "Тип привязанности"),
                "deprivation": _sel(p, "Депривация"),
                "talk":        _sel(p, "Тест разговора"),
                "escape":      _sel(p, "Точка побега"),
                "rubric":      _txt(p, "Рубрика"),
                # Источники правды для подсчёта предзаписей (Статус могла
                # перезаписать гонка фоновых задач — см. handlers.py).
                "zapisi":      _txt(p, "Записи"),
                "zapros":      _txt(p, "Запрос"),
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
    escape     = Counter(r["escape"]      for r in rows if r["escape"] != "—")
    rubrics    = Counter(r["rubric"]      for r in rows if r["rubric"] != "—")

    # Воронка
    engaged = total - statuses.get("Зашёл", 0)

    # Уникальные предзаписи — человек считается записавшимся, если выполняется
    # хотя бы одно: статус ∈ {Предзапись, Предзапись практикум},
    # поле «Записи» содержит «клуб» / «практикум»,
    # поле «Запрос» содержит «клуб» / «практикум».
    # Дедуп по строке (одна строка Notion = один человек).
    PREREG_STATUSES = {"Предзапись", "Предзапись практикум"}
    preregistered = 0
    prereg_club = 0
    prereg_protocol = 0
    for r in rows:
        zapisi_lc = (r["zapisi"] or "").lower()
        zapros_lc = (r["zapros"] or "").lower()
        is_club  = "клуб" in zapisi_lc or "клуб" in zapros_lc or r["status"] == "Предзапись"
        is_proto = "практикум" in zapisi_lc or "практикум" in zapros_lc or r["status"] == "Предзапись практикум"
        if is_club:
            prereg_club += 1
        if is_proto:
            prereg_protocol += 1
        if is_club or is_proto or r["status"] in PREREG_STATUSES:
            preregistered += 1

    return {
        "total":            total,
        "engaged":          engaged,
        "preregistered":    preregistered,
        "prereg_club":      prereg_club,
        "prereg_protocol":  prereg_protocol,
        "statuses":         dict(statuses.most_common()),
        "sources":          dict(sources.most_common()),
        "attachment":       dict(attachment.most_common()),
        "deprivation":      dict(deprivation.most_common()),
        "talk":             dict(talk.most_common()),
        "escape":           dict(escape.most_common()),
        "rubrics":          dict(rubrics.most_common(10)),
        "updated_at":       datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC"),
    }


async def get_registrations() -> list[dict]:
    """
    Ищет все предзаписи максимально широко:
    1. По статусу Предзапись / Предзапись практикум
    2. По полю Запрос содержащему 'клуб' или 'практикум'
       (на случай если статус был перезаписан гонкой фоновых задач)
    Дедуплицирует по page_id.
    """
    seen_ids: set[str] = set()
    leads: list[dict] = []

    def _extract(page: dict, override_status: str | None = None) -> dict | None:
        if page["id"] in seen_ids:
            return None
        seen_ids.add(page["id"])
        p = page["properties"]
        uid  = p.get("Telegram ID", {}).get("number")
        # Предпочитаем введённое при регистрации имя из колонки «Имя»,
        # фоллбек — Name (title), чтобы записи до миграции тоже показывались.
        imya = _txt(p, "Имя")
        if imya != "—":
            name = imya
        else:
            name_parts = (p.get("Name") or {}).get("title") or []
            name = name_parts[0]["text"]["content"] if name_parts else "—"
        uname_parts = (p.get("Username") or {}).get("rich_text") or []
        username = uname_parts[0]["text"]["content"] if uname_parts else "—"
        zapros = _txt(p, "Запрос")
        status = override_status or _sel(p, "Статус")
        return {
            "user_id":    uid,
            "name":       name,
            "username":   username,
            "status":     status,
            "zapros":     zapros,
            "source":     _sel(p, "Источник"),
            "attachment": _sel(p, "Тип привязанности"),
            "created":    page.get("created_time", "")[:10],
        }

    url = f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query"

    # 1. По статусу
    for status_val in ("Предзапись", "Предзапись практикум"):
        try:
            data = await _notion_request(
                "POST", url, timeout=30.0,
                json_body={"filter": {"property": "Статус", "select": {"equals": status_val}}},
            )
        except NotionError:
            continue
        for page in data.get("results", []):
            row = _extract(page)
            if row:
                leads.append(row)

    # 2. По полю Запрос (ловим тех, у кого статус был перезаписан)
    for keyword in ("клуб", "практикум"):
        try:
            data = await _notion_request(
                "POST", url, timeout=30.0,
                json_body={"filter": {"property": "Запрос", "rich_text": {"contains": keyword}}},
            )
        except NotionError:
            continue
        for page in data.get("results", []):
            row = _extract(page, override_status=f"⚠️ статус перезаписан (был: {keyword})")
            if row:
                leads.append(row)

    # 3. По полю Записи (накопительное поле — самый надёжный источник)
    for keyword in ("клуб", "практикум"):
        try:
            data = await _notion_request(
                "POST", url, timeout=30.0,
                json_body={"filter": {"property": "Записи", "rich_text": {"contains": keyword}}},
            )
        except NotionError:
            continue
        for page in data.get("results", []):
            row = _extract(page, override_status=f"⚠️ найден по полю Записи ({keyword})")
            if row:
                leads.append(row)

    leads.sort(key=lambda x: x["created"])
    return leads


async def get_waitlist() -> list[dict]:
    try:
        data = await _notion_request(
            "POST", f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
            timeout=20.0,
            json_body={"filter": {"property": "Статус", "select": {"equals": "Предзапись"}}},
        )
    except NotionError:
        return []
    leads = []
    for page in data.get("results", []):
        uid = page["properties"].get("Telegram ID", {}).get("number")
        if uid:
            leads.append({"user_id": int(uid)})
    return leads


async def get_all_leads() -> list[dict]:
    leads = []
    cursor = None
    while True:
        body: dict = {}
        if cursor:
            body["start_cursor"] = cursor
        try:
            data = await _notion_request(
                "POST", f"https://api.notion.com/v1/databases/{NOTION_LEADS_DB_ID}/query",
                json_body=body, timeout=30.0,
            )
        except NotionError:
            break
        for page in data.get("results", []):
            uid = page["properties"].get("Telegram ID", {}).get("number")
            if uid:
                leads.append({"user_id": int(uid)})
        if not data.get("has_more"):
            break
        cursor = data.get("next_cursor")
    return leads
