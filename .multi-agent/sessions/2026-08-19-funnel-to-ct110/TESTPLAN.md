# TESTPLAN — funnel-to-ct110

> Написан независимым субагентом `testplan-critic` **до кода** (Шаг 4 конвейера).
> Сценарии не подгонять под реализацию. Секреты нигде не печатать — брать на сервере
> из `/opt/funnel/.env`.

Стенд: `https://funnel.gogolevajuls.org` (Cloudflare Tunnel → 192.168.1.210:8000, CT 110).
Старый адрес: `https://psycology-funnel-bot-production.up.railway.app`.
Бот: `@gogolevajuls_bot` (ПРОД). Тест-юзер: `@stickers_support`.

## Сценарии для браузера

### Б1. Здоровье нового адреса
`GET https://funnel.gogolevajuls.org/health` → 200, тело ровно `{"status":"ok","bot":"funnel"}`,
без Cloudflare Access, челленджа и редиректов.
Провал: 1033/502/530, форма Access, «Checking your browser», иной код/тело.

### Б2. Старый адрес Railway жив (готовность отката)
`GET https://psycology-funnel-bot-production.up.railway.app/health` → 200 и то же тело.
Провал: 502/404/«Application failed to respond» — **дальше переключаться НЕЛЬЗЯ**.

### Б3. Дашборд на новом адресе со старым токеном
`GET /dashboard?token=<PSY_DASHBOARD_TOKEN из ~/.config/jul-dashboard/secrets.yaml>` (в чат не печатать)
→ HTML-дашборд, вкладка «Бот» активна, есть переключатели `all/today/7d/30d`, вкладки «Сайт», «Продукты».
Провал: 403 (токен не выровнен), 502, пустая страница, трейсбек.

### Б4. Дашборд с неверным токеном
`GET /dashboard?token=wrong-token-123` → 403 «доступ запрещён». Провал: 200 с данными или 500.

### Б5. Дашборд без токена
`GET /dashboard` → 403. Провал: 200 с данными (значит токен пустой/дефолтный).

### Б6. Вкладки не теряют токен
Открыть дашборд → вкладка «Сайт» → «Обновить». В адресной строке остаются `token=` и `tab=site`,
после «Обновить» снова данные. Провал: 403 или белый экран.

### Б7. Smoke бота через `bot-test` — сквозной прогон по реальной проводке
Идёт через Telegram → Cloudflare Edge → тоннель CT 103 → uvicorn CT 110 → `/webhook`
с заголовком `X-Telegram-Bot-Api-Secret-Token`. Никаких прямых вызовов `handlers.handle_update`.
Аккаунт — только `@stickers_support`.
1. Зафиксировать «до»: `ssh funnel 'docker logs --since 1m funnel-app | tail -5'` + снимок счётчиков (Б3).
2. Отправить `/start`, дождаться ответа (≤15 с).
3. Нажать первую кнопку главного меню, дождаться ответа.
4. Нажать следующую кнопку из ответа.
5. Транскрипт → `EVIDENCE/bot-smoke.md`.
Ожидается: приветствие с кнопками, ответы ≤15 с, в логах CT 110 видны эти `update_id`.
Провал: бот молчит; отвечает, но в логах CT 110 пусто (**значит апдейты обрабатывает Railway**);
ответ приходит дважды; в логах 403.

### Б8. Повторный `/start` — нет задвоения
Ровно одно приветственное сообщение.

### Б9. Негатив: неизвестный ввод
Отправить `ъъъ-несуществующая-команда` → бот не падает; в логах нет `Traceback` и
`handle_update error`; контейнер не ушёл в рестарт.

### Б10. Переезд пережил рестарт (ключевой)
1. `ssh funnel 'docker compose -f /opt/funnel/docker-compose.yml restart funnel-app'`
2. Подождать 30 с. 3. `/health` → 200. 4. `/start` тест-юзером.
Ожидается: ответ приходит, `getWebhookInfo` по-прежнему `funnel.gogolevajuls.org/webhook`,
`last_error_message` пуст.
Провал: молчит; вебхук на railway-адресе; `last_error_message = "Wrong response from the
webhook: 403 Forbidden"` (секрет в вебхуке и в контейнере разошлись).

## Проверки в коде

**К1. Тесты не ходят в сеть и не трогают прод.** `tests/conftest.py` до импорта `main`
подставляет фиктивные `MARKETING_BOT_TOKEN/ADMIN_CHAT_ID/NOTION_TOKEN/NOTION_LEADS_DB_ID`,
`UPSTASH_REDIS_URL=""`, `UPSTASH_REDIS_TOKEN=""`, `STATS_FILE=<tmp>`; `httpx.AsyncClient.post`
замокан и **фейлит тест** при попытке реального запроса.

**К2. Вебхук через настоящий ASGI-стек** (`tests/test_webhook.py`, `fastapi.testclient`):
- секрет задан, заголовок верный → 200 и `handle_update` вызван ровно один раз;
- секрет задан, заголовок неверный → **403**, обработчик НЕ вызван;
- секрет задан, заголовка нет → **403**, обработчик не вызван;
- секрет не задан → 200, обработчик вызван (обратная совместимость);
- тело не JSON при верном секрете → без исключения наружу, обработчик не вызван;
- повторный тот же `update_id` → `handle_update` вызван один раз.

**К3. `set_webhook()` — параметры запроса** (мок `httpx.AsyncClient.post`):
- `PUBLIC_URL=funnel.gogolevajuls.org` → `https://funnel.gogolevajuls.org/webhook`;
- со схемой `https://…` → тот же результат;
- со слешем на конце → без `//webhook`;
- уже с `/webhook` → без `/webhook/webhook`;
- `PUBLIC_URL` не задан → `post` НЕ вызван, в логах warning;
- `RAILWAY_PUBLIC_DOMAIN` задан, `PUBLIC_URL` нет → `post` НЕ вызван (фолбэка нет);
- `drop_pending_updates` = `False`;
- `WEBHOOK_SECRET` задан → в теле `secret_token`; не задан → ключа нет.

**К4. Токен не течёт в логи.** `logging.getLogger("httpx").level >= WARNING`; на сервере
`docker logs --since 30m funnel-app | grep -c "api.telegram.org/bot[0-9]"` → `0`
(проверять ПОСЛЕ выката и после Б7).

**К5. Состояние вебхука** — `getWebhookInfo` до и после, оба снимка в `EVIDENCE/`.
После: `https://funnel.gogolevajuls.org/webhook`, `last_error_message` пуст.
**Два замера с интервалом 5 минут**: `pending_update_count` во втором не больше первого.

**К6. Реальный HTTP до контейнера мимо Telegram** (тело без `message`, чтобы никому не написать):
без заголовка → 403; с неверным → 403; с верным (с самого сервера) → 200.

**К7. Переменные доехали внутрь контейнера.**
`docker exec funnel-app printenv` содержит `PUBLIC_URL`, `WEBHOOK_SECRET`, `DASHBOARD_TOKEN`
и НЕ содержит `RAILWAY_PUBLIC_DOMAIN`.

**К8. Выкат тем кодом, что в `main`.** `git -C /opt/funnel/repo rev-parse HEAD` = `origin/main`,
локальных модификаций нет.

**К9. Railway после автодеплоя жив и разоружён.** В логах предупреждение об отсутствии
`PUBLIC_URL`, строки `setWebhook` НЕТ, деплой SUCCESS, `/health` → 200.

**К10. `DASHBOARD_TOKEN` — сверка sha256**, значения в чат не выводятся.

**К11. `sync_secrets.py`** отработал; `PSY_DASHBOARD_TOKEN` больше не тянется из Railway.

**К12. Инвентаризация ВСЕХ входящих адресов**, не только `/webhook`
(`grep -rn "psycology-funnel-bot-production"`).

**К13. Прод-зависимости.** `requirements.txt` изменён осознанно (пины), `pytest` — только в
`requirements-dev.txt`.

**К14. Секретов нет в артефактах** — grep по маске токена бота в `EVIDENCE/` и сессии.

## Критерии приёмки

1. `getWebhookInfo` → `funnel.gogolevajuls.org/webhook`, `last_error_message` пуст,
   `pending_update_count` во втором замере через 5 мин не вырос; оба снимка в `EVIDENCE/`.
2. Апдейты тест-юзера видны в `docker logs funnel-app` с совпадением по `update_id`.
3. Smoke `bot-test` зелёный: `/start` + минимум две кнопки, ответы ≤15 с, транскрипт в `EVIDENCE/`.
4. Живой HTTP: `/webhook` без секрета → 403, с верным → 200 (не «функция вернула 403»).
5. `pytest tests/test_webhook.py` зелёный, все шесть кейсов К2, проверка вызова —
   детерминированная, без `sleep`-угадайки.
6. Прогон `pytest` не сделал ни одного исходящего запроса и не писал в прод-Upstash.
7. Контейнер перезапущен вручную — бот отвечает, вебхук остался на CT 110 (Б10).
8. `printenv` в контейнере: три переменные есть, `RAILWAY_PUBLIC_DOMAIN` нет.
9. Railway: деплой SUCCESS, `/health` 200, предупреждение в логах, `setWebhook` нет.
   Сервис и том **НЕ удалены**.
10. Дашборд открывается по старому токену; неверный и пустой → 403; хеши совпадают.
11. В логах CT 110 за 30 минут ноль вхождений `api.telegram.org/bot<цифры>`.
12. `EVIDENCE/rollback.md` содержит откат из трёх шагов, начиная с разоружения CT 110.
13. Для `/tribute_webhook` и `/track` зафиксировано, куда они смотрят и когда переключаются.
14. Бэкап состояния снят и лежит вне артефактов сессии.
15. Доки обновлены, ссылка на дашборд воронки добавлена в `PSYcology/wiki/dashboard.yaml`.
16. Секретов нет ни в чате, ни в логах, ни в `EVIDENCE/`.
