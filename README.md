# Сервис обработки платежей

API принимает платёж. Обработчик (`consumer`) читает события RabbitMQ, обрабатывает платежи и отправляет клиенту HTTP-запросы (webhook).

Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL, RabbitMQ, FastStream, Alembic, Docker Compose.

## Запуск

Установите Docker с Compose v2. Из корня проекта выполните:

```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose ps
```

Compose запускает `postgres`, `rabbitmq`, `api` и `consumer`. API применяет миграции при запуске.
Обработчик ждёт готовности API и RabbitMQ. Он также публикует события из таблицы `outbox`.

| Компонент | Адрес |
| --- | --- |
| API | `http://127.0.0.1:8001` |
| RabbitMQ Management | `http://127.0.0.1:15672` |
| PostgreSQL | `127.0.0.1:55433` |

Логин и пароль RabbitMQ: `payments`. Пользователь, пароль и база PostgreSQL: `payments`.
Compose берёт `API_KEY` из `.env`. Файл `.env` исключён из Git.

Просмотрите логи командой `docker compose logs -f api consumer`.
Остановите сервисы командой `docker compose down`.
Тома Docker сохраняют данные PostgreSQL и RabbitMQ после остановки.

## Проверка webhook

Установите Python 3.12 или новее. В отдельном терминале из корня проекта выполните:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/uvicorn scripts.webhook_receiver:app --host 0.0.0.0 --port 9000
```

Получатель выводит тело webhook в терминал и возвращает `204`.
На macOS и Windows с Docker Desktop используйте `http://host.docker.internal:9000/webhook`.
На Linux укажите адрес, доступный из контейнера. Для обработчика вне Docker используйте `http://localhost:9000/webhook`.

Webhook содержит `payment_id`, `status` и `processed_at` в UTC.
Обработчик считает ответ `2xx` успешной доставкой.

## API

Передавайте `X-API-Key` в каждом запросе. Отсутствующий или неверный ключ вызывает ответ `401`.
Если изменили `API_KEY`, замените ключ в примерах.

### Создать платёж

```bash
curl -i http://127.0.0.1:8001/api/v1/payments \
  -H 'Content-Type: application/json' \
  -H 'X-API-Key: local-development-key' \
  -H 'Idempotency-Key: example-payment-1' \
  -d '{
    "amount": 1500.25,
    "currency": "RUB",
    "description": "Тестовый платёж",
    "metadata": {"order_id": "123"},
    "webhook_url": "http://host.docker.internal:9000/webhook"
  }'
```

Все пять полей обязательны:

| Поле | Требование |
| --- | --- |
| `amount` | Положительная сумма. До 16 цифр до десятичной точки и до двух после неё. |
| `currency` | `RUB`, `USD` или `EUR` |
| `description` | Строка |
| `metadata` | JSON-объект |
| `webhook_url` | Адрес HTTP или HTTPS |

Передавайте `Idempotency-Key` длиной от 1 до 255 символов.
API возвращает `202 Accepted` с полями `payment_id`, `status` и `created_at`. Ошибки входных данных вызывают ответ `422`.
Повторный запрос с тем же ключом возвращает первый платёж. Это правило действует при изменённом теле и одновременных запросах.

### Получить платёж

Замените `UUID_ИЗ_ОТВЕТА` значением `payment_id` из ответа. Выполните запрос:

```bash
curl -i http://127.0.0.1:8001/api/v1/payments/UUID_ИЗ_ОТВЕТА \
  -H 'X-API-Key: local-development-key'
```

API возвращает `200` с данными платежа, `404` для неизвестного UUID или `422` для некорректного UUID.
API передаёт сумму строкой, например `"1500.25"`. Статусы платежа: `pending`, `succeeded`, `failed`.
Swagger, ReDoc и `/openapi.json` отключены.

## Обработка и доставка

1. API сохраняет платёж и событие в `outbox` одной транзакцией.
2. Обработчик публикует сообщение в очередь `payments.new`. RabbitMQ сохраняет очереди и сообщения на диске.
   После подтверждения RabbitMQ обработчик отмечает событие как опубликованное. При ошибке он повторяет публикацию с тем же `message_id`.
3. Один обработчик эмулирует шлюз: задержка 2–5 секунд, вероятность `succeeded` — 90%, `failed` — 10%.
   Он сохраняет результат до отправки webhook. Дубликат не запускает шлюз повторно. Ошибка webhook не меняет результат платежа.
4. Обработчик отправляет webhook с таймаутом 5 секунд. Лимит: три попытки с паузами 2 и 4 секунды после ошибок.
   PostgreSQL хранит счётчик, время повтора и отметку доставки. Перезапуск не обнуляет лимит.
   Обработчик записывает попытку до HTTP-запроса. Остановка между записью и отправкой расходует попытку.
5. После исчерпания лимита попыток обработчик публикует сообщение в очередь `payments.dlq`.
   Некорректные события и неизвестные платежи также проходят три проверки перед переносом.
   Обработчик подтверждает исходное сообщение после доставки webhook или подтверждения публикации в `payments.dlq`.
   Если публикация не удалась, обработчик повторяет перенос без четвёртой попытки webhook.

Обрыв после публикации в RabbitMQ или доставки webhook, но до отметки в PostgreSQL, может вызвать повторную отправку.
Получатель должен исключать повторную обработку webhook по `payment_id`.

## Миграции и тесты

Миграция `0001` создаёт таблицы `payments` и `outbox`. Миграция `0002` добавляет поля доставки webhook.
Для локального запуска используйте настройки `.env` и выполните `.venv/bin/alembic upgrade head`.

После установки зависимостей `dev` выполните проверки без PostgreSQL:

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m pytest tests -m 'not integration' -q
```

Запустите PostgreSQL и выполните все тесты:

```bash
docker compose up -d --wait postgres
.venv/bin/python -m pytest tests -q
```

Интеграционные тесты используют PostgreSQL. Тесты подменяют HTTP-запросы и подтверждения RabbitMQ.
Каждый интеграционный тест создаёт базу `test_payments_<uuid>` и удаляет её после завершения.
По умолчанию тесты используют PostgreSQL из Compose на порту `55433`.
Для другого сервера задайте `TEST_DATABASE_URL`. Пользователь подключения должен иметь право создавать базы.

## Официальная документация

[FastAPI](https://fastapi.tiangolo.com/), [Pydantic](https://docs.pydantic.dev/latest/), [SQLAlchemy async](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html), [RabbitMQ confirms](https://www.rabbitmq.com/docs/confirms), [FastStream](https://faststream.ag2.ai/latest/), [Alembic async](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic), [Docker Compose](https://docs.docker.com/compose/how-tos/startup-order/).
