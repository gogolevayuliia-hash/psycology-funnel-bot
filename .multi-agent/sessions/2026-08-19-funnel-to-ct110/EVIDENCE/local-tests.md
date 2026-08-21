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
