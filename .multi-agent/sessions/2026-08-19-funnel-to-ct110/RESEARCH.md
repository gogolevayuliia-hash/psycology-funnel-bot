# RESEARCH — funnel-to-ct110

## 1. Корень проблемы: вебхук ставит само приложение при каждом старте

`main.py:32-43`, вызывается из `lifespan` (`main.py:73`) на каждом запуске:

```python
async def set_webhook() -> None:
    webhook_url = os.environ.get("RAILWAY_PUBLIC_DOMAIN")
    if not webhook_url:
        logger.warning("RAILWAY_PUBLIC_DOMAIN not set — webhook not configured")
        return
    url = f"https://{webhook_url}/webhook"
    ... json={"url": url, "max_connections": 40, "drop_pending_updates": True}
```

Обе копии гоняют один и тот же код и обе ставят вебхук **на себя**:

| Копия | `RAILWAY_PUBLIC_DOMAIN` | Последний старт |
|---|---|---|
| Railway | `psycology-funnel-bot-production.up.railway.app` | деплой 17.08.2026 13:31 |
| CT 110 | `funnel.gogolevajuls.org` | контейнер up 12 дней (~07.08.2026) |

**Владеет вебхуком тот, кто перезапустился последним.** Railway передеплоился 17.08 и забрал
вебхук у CT 110. Это не «переезд не сделали» — это перетягивание каната, которое повторится
при любом следующем рестарте любой из копий.

Следствия для задачи:
- Ручной `setWebhook` **не является решением**: он проживёт до первого рестарта.
- Пока копия на Railway жива (а Юля просила подержать неделю), она при любом рестарте
  платформы заберёт вебхук обратно — и заберёт **тихо**, бот продолжит работать, никто не
  заметит, что CT 110 снова простаивает.

## 2. Состояние переносить не надо — оно общее

Сверил переменные Railway ↔ `/opt/funnel/.env` по sha256 (значения не печатались):

| Переменная | Вердикт |
|---|---|
| `MARKETING_BOT_TOKEN` | совпадает |
| `UPSTASH_REDIS_URL` | **совпадает** |
| `UPSTASH_REDIS_TOKEN` | **совпадает** |
| `NOTION_TOKEN`, `NOTION_LEADS_DB_ID` | совпадают |
| `TRIBUTE_API_KEY`, `ADMIN_CHAT_ID` | совпадают |
| `DASHBOARD_TOKEN` | ⚠️ **РАЗНЫЕ** |

`stats.py:1-34`: Upstash Redis — основное хранилище, `STATS_FILE` — запасной путь. Redis у копий
один и тот же, значит счётчики, лиды и `file_id` уже общие. **Мигрировать нечего**, том Railway
(`psycology-funnel-bot-volume`, 191 МБ) — только запасная копия того, что и так в Redis.

## 3. Расхождение `DASHBOARD_TOKEN` — единственная реальная потеря при переключении

`main.py:928-930` — `GET /dashboard?token=...`, сверяется с `DASHBOARD_TOKEN` (`config.py:25`).
`Dashboard/sync_secrets.py:33` тянет `PSY_DASHBOARD_TOKEN` **из Railway**
(`rproject: courageous-wisdom, service: psycology-funnel-bot, key: DASHBOARD_TOKEN`),
значение лежит в `~/.config/jul-dashboard/secrets.yaml`, имя объявлено в
`PSYcology/wiki/dashboard.yaml:39`.

То есть в дашборде Юли лежит **railway-овский** токен. После переключения дашборд воронки
переедет на `funnel.gogolevajuls.org/dashboard`, где действует **другой** токен → ссылка
перестанет открываться. Чинится выравниванием значения плюс правкой источника в `sync_secrets.py`.

## 4. Утечка токена бота в логи (найдено попутно, чинится одной строкой)

`main.py:23` — `logging.basicConfig(level=logging.INFO)`, логгер `httpx` не приглушён.
`main.py:26` — `BASE = f"https://api.telegram.org/bot{MARKETING_BOT_TOKEN}"`.

httpx на INFO печатает полный URL каждого запроса, а токен — часть пути. Проверено на живых
логах CT 110: строка `POST https://api.telegram.org/bot<ТОКЕН>/setWebhook "HTTP/1.1 200 OK"`
присутствует в открытом виде. Это касается **каждого** вызова Bot API, а не только setWebhook,
и было так же на Railway.

## 5. Что говорит документация Telegram (WEB/q1–q3)

- `setWebhook` на новый URL вызывается напрямую, `deleteWebhook` перед этим **не нужен**;
  очередь сохраняется и уходит новому адресу. `q1.md`
- `drop_pending_updates=true` **безвозвратно отбрасывает** очередь. В нашем коде стоит `True` —
  значит каждый рестарт бота молча теряет всё, что накопилось. Для переключения нужно `false`. `q1.md`
- Старый endpoint разумно подержать: гарантии по запросам «в полёте» в документации нет.
  Это ровно то, что Юля и выбрала (Railway живёт неделю). `q1.md`
- `secret_token` уходит в заголовке `X-Telegram-Bot-Api-Secret-Token`; **приложение обязано
  проверять его само**, иначе `/webhook` принимает любой POST от кого угодно. Сейчас проверки
  нет (`main.py:97-104`). `q3.md`
- Cloudflare Tunnel как фронт подходит: Telegram ходит на Cloudflare Edge по 443 с доверенным
  сертификатом. Условие — на хостнейме не должно быть Cloudflare Access и bot challenge.
  У нас на `funnel.gogolevajuls.org` Access нет: `/health` открывается снаружи без авторизации. `q2.md`
- Обработчик должен быть идемпотентен по `update_id`. В коде есть `_processed_updates`
  (`main.py:28`, `83-94`) — множество в памяти процесса. Для одной работающей копии достаточно. `q1.md`

## 6. Как устроен выкат на CT 110

`/opt/funnel/scripts/deploy.sh`: `git -C repo pull --ff-only` → `docker compose build funnel-app`
→ `docker compose up -d funnel-app`. Репозиторий публичный, клон настоящий, тянет `origin/main`.

**Значит правку кода надо сначала влить в `main`, и только потом выкатывать на CT 110.**
Бэкапы `/opt/funnel/backups` ежедневно в 02:00, есть `restore-test.sh`.

## 7. Затронутые файлы

| Файл | Что и почему |
|---|---|
| `main.py:32-43` | `set_webhook()` — источник URL, `drop_pending_updates`, secret_token |
| `main.py:97-104` | обработчик `/webhook` — проверка secret_token |
| `main.py:23` | приглушить логгер httpx (утечка токена) |
| `/opt/funnel/.env` (CT 110) | переименовать переменную, выровнять `DASHBOARD_TOKEN`, добавить secret |
| `Dashboard/sync_secrets.py:33` | источник `PSY_DASHBOARD_TOKEN`: railway → CT 110 |

## 8. Риски

1. **Тихий откат.** Рестарт копии на Railway вернёт вебхук себе, и это никак не проявится —
   бот продолжит отвечать. Нужен способ либо запретить ей это, либо заметить.
2. **Потеря очереди при переключении.** `drop_pending_updates: True` в текущем коде. На момент
   проверки `pending_update_count` = 0, но между проверкой и переключением может прийти апдейт.
3. **Дашборд воронки отвалится** у Юли из-за разных `DASHBOARD_TOKEN` (п. 3).
4. **Токен бота скомпрометирован логами** (п. 4) — вопрос ротации за Юлей.
5. Правка едет через `main` → на CT 110 приедет и всё остальное, что сейчас в `main`.
   На момент старта ветки `main` = `5e8273e`, расхождений с задеплоенным на CT 110 нет
   (проверить перед выкатом).

## 9. Открытые вопросы к плану

- Чем нейтрализовать копию на Railway на неделю, не ломая откат: снять публичный домен
  (тогда `RAILWAY_PUBLIC_DOMAIN` пуст → приложение само откажется ставить вебхук, в логи уйдёт
  предупреждение) — откат = вернуть домен. Альтернатива — остановить сервис, но тогда откат
  требует передеплоя.
- Выравнивать `DASHBOARD_TOKEN` в сторону Railway (у Юли в дашборде лежит именно он) или
  сгенерировать новый на сервере и обновить хранилище.
- Вводить ли `secret_token` в этой же задаче или отдельной (эндпоинт публичный и сейчас
  принимает любой POST).
