# Откат — вернуть бота на Railway

> Составлено 21.08.2026. **Порядок шагов обязателен.** Наивный «просто вернуть вебхук
> одной командой» больше не работает: после этой правки CT 110 сам ставит вебхук на себя
> при каждом старте, а `restart: unless-stopped` поднимает контейнер после ребута, OOM
> и обслуживания. Вебхук вернулся бы на CT 110 сам, тихо, в течение часа.

## Шаг 1 — сначала разоружить CT 110 (иначе он отберёт вебхук обратно)

```bash
ssh funnel 'sed -i "s/^PUBLIC_URL=/#PUBLIC_URL=/" /opt/funnel/.env && docker restart funnel-app'
```

Либо жёстче, если бот на CT 110 сломан:

```bash
ssh funnel 'cd /opt/funnel && docker compose stop funnel-app'
```

## Шаг 2 — вернуть вебхук на Railway

Без `secret_token`: на Railway `WEBHOOK_SECRET` не задан, и если секрет оставить,
Telegram будет слать заголовок, а Railway его не проверит — работать будет,
но лучше вернуть конфигурацию в исходный вид.

```bash
ssh funnel 'set -a; . /opt/funnel/.env.bak-2026-08-21; set +a; \
  curl -s -X POST "https://api.telegram.org/bot$MARKETING_BOT_TOKEN/setWebhook" \
  -H "Content-Type: application/json" \
  -d "{\"url\":\"https://psycology-funnel-bot-production.up.railway.app/webhook\",\"max_connections\":40,\"drop_pending_updates\":false}"'
```

## Шаг 3 — проверить

```bash
ssh funnel 'set -a; . /opt/funnel/.env.bak-2026-08-21; set +a; \
  curl -s "https://api.telegram.org/bot$MARKETING_BOT_TOKEN/getWebhookInfo"' | python3 -m json.tool
curl -s https://psycology-funnel-bot-production.up.railway.app/health
```
Ждём: `url` = railway-адрес, `last_error_message` пуст, `/health` → 200.
Затем `/start` тест-юзером `@stickers_support` — бот отвечает.

## Если сам сервис на Railway сломался (например, пересборка после merge)

Точка отката зафиксирована **до** merge:

| Что | Значение |
|---|---|
| Проект | `courageous-wisdom` (`66ab65cd-f098-4b6c-b2f3-aea8d4170dae`) |
| Сервис | `psycology-funnel-bot` (`1c5f4405-0360-496c-8cba-07e86e447718`) |
| Рабочий деплой | **`56464614-837a-4fcd-83d2-e73a18bda207`**, SUCCESS, 2026-08-17 13:31, `canRedeploy: true` |

Откатить через интерфейс Railway (Deployments → этот деплой → Redeploy).

⚠️ **Этот откат поднимет СТАРЫЙ код**, который читает `RAILWAY_PUBLIC_DOMAIN` и заберёт
вебхук себе автоматически. Для аварийного возврата на Railway это удобно, но помни:
после него шаг 1 (разоружение CT 110) обязателен, иначе снова начнётся перетягивание каната.

## Бэкапы состояния (сняты 21.08.2026 до переключения)

`~/backups/funnel-2026-08-21/` — вне артефактов сессии и вне git: внутри `telegram_id`
пользователей и результаты психологических тестов.

| Файл | Что |
|---|---|
| `redis-psycology_bot_stats.json` | **источник правды** — общий Upstash обеих копий, 10 ключей |
| `ct110-stats.json` | запасной файл на CT 110, 1297 Б |
| `railway-volume-stats.json` | тот же файл с тома Railway, 1297 Б — совпадает по размеру, подтверждает общий Redis |

Файл на CT 110 и файл с тома Railway идентичны по размеру — том Railway отдельной
ценности не несёт и при сносе сервиса ничего уникального не теряется.
