# Прогон тестов (venv Python 3.12, пины из requirements-dev.txt)

```
$ .venv/bin/python -m pytest -q
................                                                         [100%]
16 passed, 1 warning in 1.47s
```

## Мутация 1 — вернули фолбэк на RAILWAY_PUBLIC_DOMAIN
```
FAILED tests/test_set_webhook.py::test_railway_public_domain_больше_НЕ_фолбэк
1 failed, 15 passed
```

## Мутация 2 — убрали проверку secret_token
```
FAILED tests/test_webhook.py::test_секрет_задан_заголовок_неверный__403_и_без_обработки
FAILED tests/test_webhook.py::test_секрет_задан_заголовка_нет__403_и_без_обработки
2 failed, 14 passed
```

## Контроль после восстановления кода
```
16 passed, 1 warning in 1.42s
```

---

# Круг 2: после исправлений по критике результата

```
$ .venv/bin/python -m pytest -q
.................................                                        [100%]
33 passed, 1 warning in 1.44s
```

Тестов стало 33 (было 16): добавлен `tests/test_resilience.py` на отказы, которые критик
поймал как непокрытые, и краевые случаи нормализации адреса.

## Тест поймал настоящий дефект в моём же коде
`PUBLIC_URL=HTTPS://…/WEBHOOK` давал адрес `…/WEBHOOK` в верхнем регистре. Маршрут FastAPI
регистрозависимый → Telegram получал бы 404 на каждый апдейт. Починен код, не тест.

## Мутации круга 2

| Мутация | Что сломали | Результат |
|---|---|---|
| 3 | провал `setWebhook` снова логируется вместо исключения | ❌ упал `test_setwebhook_ответил_ok_false__поднимаем_ошибку` |
| 4 | убрали защиту автосохранения у припаркованной копии | ❌ упал `test_без_public_url__автосохранение_выключено` |
| 5 | убрали проверку типа `update_id` | ❌ упали 2 случая `test_мусорный_update_id__не_роняет_кеш` |
| контроль | код восстановлен | ✅ 33 passed |

## Фаза 0 переделана на живом сервере

Критик доказал, что правка `.env` не влияет на работающий контейнер — окружение снимается
при создании, а `docker restart` поднимает тот же контейнер:

```
ДО:     docker exec funnel-app printenv | grep -c RAILWAY_PUBLIC_DOMAIN  → 1   (при пустом .env!)
$ docker compose up -d funnel-app     # пересоздание
ПОСЛЕ:  docker exec funnel-app printenv | grep -c RAILWAY_PUBLIC_DOMAIN  → 0
лог:    WARNING:main:RAILWAY_PUBLIC_DOMAIN not set — webhook not configured
статус: Up 12 seconds (healthy)
вебхук: https://psycology-funnel-bot-production.up.railway.app/webhook  (не тронут, pending 0)
```

Старый код при старте вебхук не забрал — окно, которое критик описал в баге 6, закрыто.
