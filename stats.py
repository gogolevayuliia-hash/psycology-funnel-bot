"""
Счётчики событий с надёжным хранением через Upstash Redis.

Upstash — бесплатный облачный Redis (10к команд/день).
Данные переживают любые деплои Railway.

Как подключить (один раз):
  1. Зайти на upstash.com → Create Database → выбрать регион EU West
  2. Скопировать UPSTASH_REDIS_URL и UPSTASH_REDIS_TOKEN
  3. Добавить их в Railway → Variables

Без Upstash работает как раньше (in-memory, сбрасывается при деплое).
"""
import json
import logging
import os
import httpx
from collections import Counter
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

# ── Upstash Redis (основное хранилище) ────────────────────────────────────────
UPSTASH_URL    = os.environ.get("UPSTASH_REDIS_URL", "")
UPSTASH_TOKEN  = os.environ.get("UPSTASH_REDIS_TOKEN", "")
REDIS_KEY      = "psycology_bot_stats"           # JSON-snapshot (метаданные + полное состояние)
REDIS_HASH_KEY = "psycology_bot_stats_counters"  # хеш атомарных счётчиков (HINCRBY)
REDIS_FILE_IDS_KEY = "psycology_bot_file_ids"    # хеш { локальный_путь → telegram file_id }

# ── Fallback: файл (работает только если есть Railway Volume /data) ───────────
STATS_FILE    = os.environ.get("STATS_FILE", "/data/stats.json")
SAVE_INTERVAL = 300   # секунд между автосохранениями


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%d.%m.%Y %H:%M UTC")


# ── Счётчики (in-memory, всегда актуальны) ────────────────────────────────────
bot: Counter = Counter()
# quiz_attachment | quiz_deprivation | quiz_talk | guide | psychologist |
# video_lesson | articles | club | protocol | start

deeplinks: Counter = Counter()
# quiz | deptest | talk | articles

site_clicks: Counter    = Counter()
site_sources: Counter   = Counter()
site_pageviews: list    = [0]
since: list             = [_now()]


# ── Redis helpers ─────────────────────────────────────────────────────────────

async def _redis_get() -> dict | None:
    """Читает JSON-blob из Redis. None если недоступно."""
    if not UPSTASH_URL:
        return None
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["GET", REDIS_KEY],
            )
            result = r.json().get("result")
            return json.loads(result) if result else None
    except Exception as e:
        logger.warning("stats: redis GET error: %s", e)
        return None


async def _redis_set(data: dict) -> bool:
    """Записывает JSON-blob в Redis. Возвращает True если успешно."""
    if not UPSTASH_URL:
        return False
    try:
        value = json.dumps(data, ensure_ascii=False)
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["SET", REDIS_KEY, value],
            )
            return r.json().get("result") == "OK"
    except Exception as e:
        logger.warning("stats: redis SET error: %s", e)
        return False


async def _redis_hgetall() -> dict[str, int]:
    """Читает все поля хеша счётчиков. Пустой dict если недоступно."""
    if not UPSTASH_URL:
        return {}
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["HGETALL", REDIS_HASH_KEY],
            )
            result = r.json().get("result") or []
        # Upstash возвращает плоский массив [k1, v1, k2, v2, ...]
        out: dict[str, int] = {}
        for i in range(0, len(result), 2):
            try:
                out[result[i]] = int(result[i + 1])
            except (ValueError, IndexError):
                continue
        return out
    except Exception as e:
        logger.warning("stats: redis HGETALL error: %s", e)
        return {}


# ── Telegram file_id cache (HSET/HGET) ────────────────────────────────────────
# Цель: первый аплоад файла в Telegram занимает время; полученный file_id
# можно использовать повторно вместо повторной загрузки. Кэш в Redis, чтобы
# переживал деплои.

async def file_id_get(path: str) -> str | None:
    if not UPSTASH_URL:
        return None
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            r = await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["HGET", REDIS_FILE_IDS_KEY, path],
            )
            return r.json().get("result")
    except Exception as e:
        logger.warning("stats: file_id HGET %s error: %s", path, e)
        return None


async def file_id_set(path: str, file_id: str) -> None:
    if not UPSTASH_URL or not file_id:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["HSET", REDIS_FILE_IDS_KEY, path, file_id],
            )
    except Exception as e:
        logger.warning("stats: file_id HSET %s error: %s", path, e)


async def _redis_hincrby(field: str, by: int = 1) -> None:
    """Атомарный инкремент поля в хеше счётчиков. Тихо игнорирует сбой."""
    if not UPSTASH_URL:
        return
    try:
        async with httpx.AsyncClient(timeout=8) as client:
            await client.post(
                UPSTASH_URL,
                headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
                json=["HINCRBY", REDIS_HASH_KEY, field, str(by)],
            )
    except Exception as e:
        logger.warning("stats: redis HINCRBY %s error: %s", field, e)


# ── Serialization ─────────────────────────────────────────────────────────────

def _to_dict() -> dict:
    return {
        "bot":            dict(bot),
        "deeplinks":      dict(deeplinks),
        "site_clicks":    dict(site_clicks),
        "site_sources":   dict(site_sources),
        "site_pageviews": site_pageviews[0],
        "since":          since[0],
        "saved_at":       _now(),
    }


def _from_dict(data: dict) -> None:
    bot.update(data.get("bot", {}))
    deeplinks.update(data.get("deeplinks", {}))
    site_clicks.update(data.get("site_clicks", {}))
    site_sources.update(data.get("site_sources", {}))
    site_pageviews[0] = data.get("site_pageviews", 0)
    saved_since = data.get("since")
    if saved_since:
        since[0] = saved_since


# ── Public API ────────────────────────────────────────────────────────────────

async def load_async() -> None:
    """Загружает данные при старте: сначала JSON-снимок, потом поверх — хеш счётчиков."""
    # 1. JSON-снимок (метаданные since/saved_at + дореформенные значения)
    data = await _redis_get()
    if data:
        _from_dict(data)
        logger.info("stats: loaded JSON snapshot (pageviews=%s)", site_pageviews[0])
    else:
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                _from_dict(json.load(f))
            logger.info("stats: loaded from file %s (pageviews=%s)", STATS_FILE, site_pageviews[0])
        except FileNotFoundError:
            logger.info("stats: no saved snapshot, starting fresh")
        except Exception as e:
            logger.warning("stats: file load error: %s", e)

    # 2. Хеш атомарных счётчиков — источник правды, перекрывает значения снимка
    counters = await _redis_hgetall()
    if counters:
        site_clicks.clear()
        site_sources.clear()
        pv = 0
        for k, v in counters.items():
            if k == "pv":
                pv = v
            elif k.startswith("click:"):
                site_clicks[k[6:]] = v
            elif k.startswith("src:"):
                site_sources[k[4:]] = v
        site_pageviews[0] = pv
        logger.info("stats: loaded counters from Redis hash (pageviews=%s, clicks=%s)",
                    pv, sum(site_clicks.values()))


async def incr_async(field: str, by: int = 1) -> None:
    """
    Атомарно инкрементит счётчик (в Redis + in-memory).
    Поддерживает поля: 'pv', 'click:<label>', 'src:<source>'.
    """
    # In-memory — мгновенно, для текущего процесса
    if field == "pv":
        site_pageviews[0] += by
    elif field.startswith("click:"):
        site_clicks[field[6:]] += by
    elif field.startswith("src:"):
        site_sources[field[4:]] += by
    # Redis — переживает рестарты Railway, агрегирует между процессами
    await _redis_hincrby(field, by)


async def save_async() -> None:
    """Сохраняет данные: Redis + файл (двойное резервирование)."""
    data = _to_dict()

    # 1. Redis (основное)
    if UPSTASH_URL:
        ok = await _redis_set(data)
        if ok:
            logger.info("stats: saved to Redis (pageviews=%s)", site_pageviews[0])
        else:
            logger.warning("stats: Redis save failed")

    # 2. Файл (запасной, работает только при наличии Railway Volume)
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATS_FILE)
    except Exception:
        pass  # нет прав/volume — молчим


# Синхронные обёртки для обратной совместимости
def load() -> None:
    """Синхронная загрузка из файла (fallback если async не доступен)."""
    try:
        with open(STATS_FILE, "r", encoding="utf-8") as f:
            _from_dict(json.load(f))
    except Exception:
        pass


def save() -> None:
    """Синхронное сохранение в файл."""
    try:
        os.makedirs(os.path.dirname(STATS_FILE), exist_ok=True)
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(_to_dict(), f, ensure_ascii=False, indent=2)
        os.replace(tmp, STATS_FILE)
    except Exception:
        pass
