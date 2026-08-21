# Доказательства ПОСЛЕ выката — 21.08.2026

## Гейт по Railway (К9): копия разоружилась сама

Merge в `main` передеплоил Railway (деплой `ce5487a8`, SUCCESS). Лог старта:

```
INFO:stats:stats: loaded JSON snapshot (pageviews=1148)
INFO:stats:stats: loaded counters from Redis hash (pageviews=1148, clicks=732)
WARNING:main:PUBLIC_URL not set — webhook not configured
WARNING:main:PUBLIC_URL not set — автосохранение статистики выключено, чтобы не затирать снимок боевой копии
INFO:     Application startup complete.
```
Строки `setWebhook` нет — вебхук не тронут. `/health` → 200. Сервис жив и годится для отката.

## Момент переключения (К5)

```
до:    https://psycology-funnel-bot-production.up.railway.app/webhook | pending 0 | err None
лог CT 110: INFO:main:setWebhook OK (url=https://funnel.gogolevajuls.org/webhook, secret=yes)
после: https://funnel.gogolevajuls.org/webhook | pending 0 | err None | ip 172.67.182.18
```

## Б7 smoke бота через `bot-test` (тест-юзер @stickers_support)

```
🧪 Я: /start
🤖 Бот: Кнопка «Меню» закреплена внизу — возвращайтесь сюда в любой момент 👇  ⌨️ [🏠 Меню]
🤖 Бот: [photo]
🤖 Бот: Это бот Юлии Гоголевой — автора канала Гоголева | ПсихоЛогично 🧪 …
        ⌨️ [📖 Гайд «Переводчик с мужского» | 🎁 Гайд — как перестать срываться | 🧪 Тесты |
            📚 Рубрикатор постов | 🎬 Обучающие видео | 🔒 Предзапись в клуб | 📅 Психолог |
            🌐 Сайт | 🚀 Микрошаг]
🧪 Я: 🏠 Меню
🤖 Бот: Выбирайте 👇  ⌨️ […тот же набор кнопок…]
```

Апдейты пришли **именно в контейнер на CT 110** (192.168.1.203 — это тоннель CT 103):
```
INFO:     192.168.1.203:49946 - "POST /webhook HTTP/1.1" 200 OK
INFO:     192.168.1.203:50228 - "POST /webhook HTTP/1.1" 200 OK
```

## Б10 переезд пережил пересоздание контейнера

Контейнер пересоздавался трижды по ходу выката (правка `.env`, ротация токена дашборда).
После каждого — `getWebhookInfo` показывает `funnel.gogolevajuls.org/webhook`, `pending 0`,
ошибок нет. Вебхук держится, а не отыгрывает назад.

## К4 токена бота в логах больше нет

```
docker logs --since 30m funnel-app | grep -cE "api\.telegram\.org/bot[0-9]"  →  0
```

## К7 переменные внутри контейнера

`PUBLIC_URL`, `WEBHOOK_SECRET`, `DASHBOARD_TOKEN` — на месте; `RAILWAY_PUBLIC_DOMAIN` — нет.

## Б3/Б4/Б5 дашборд

```
с рабочим токеном:   HTTP 200
с неверным токеном:  HTTP 403
без токена:          HTTP 403
```

⚠️ **Токен дашборда пришлось пересоздать.** Прежний (из Railway) содержал `$`, и Docker Compose
трактовал его в `env_file` как подстановку переменной: в `.env` лежало одно, а в контейнер
приезжало обрезанное значение. Поймано сверкой отпечатков `docker exec printenv` — глазами
такое не видно. Новый токен hex, отпечатки `.env` ↔ контейнер ↔ `secrets.yaml` совпадают.

## `/track` — аналитика сайта

`site/index.html` переключён на `funnel.gogolevajuls.org/track`, выкачено на GitHub Pages,
проверено на живом домене. Preflight `OPTIONS` отдаёт 200 (проверялся ДО правки, чтобы
аналитика не умерла молча). Контрольное событие дошло:
```
INFO:     192.168.1.203:34728 - "POST /track HTTP/1.1" 200 OK
```
*(это же событие добавило +1 к счётчику посещений — учитывать при чтении цифр за 21.08)*

## Монитор захвата вебхука

`/opt/funnel/scripts/webhook-watch.sh`, cron `7 * * * *`. Проверен обоими путями:
в норме молчит (код 0), при подменённом ожидаемом адресе — шлёт тревогу в админ-чат.
Троттлинг: об одной и той же беде не чаще раза в сутки.

## Что осталось непроверенным

- `/tribute_webhook` — **не переключён**, смотрит на Railway. Кабинет tribute.tg — руки Юли.
  До этого сервис на Railway сносить нельзя.
