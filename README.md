# Сервис обработки платежей

API принимает платёж, consumer обрабатывает его через RabbitMQ и отправляет результат
на webhook клиента. Стек: FastAPI, Pydantic v2, SQLAlchemy 2.0 async, PostgreSQL,
RabbitMQ, FastStream, Alembic и Docker Compose.

## Запуск

Нужен Docker с Compose v2. Из корня проекта:

```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose ps
```

Запускаются четыре сервиса: `postgres`, `rabbitmq`, `api`, `consumer`.
API автоматически применяет миграции; consumer запускается после готовности API и RabbitMQ.
Публикация Outbox работает в процессе consumer, отдельный сервис для неё не нужен.

API доступно на `http://127.0.0.1:8001`, RabbitMQ Management — на
`http://127.0.0.1:15672`. Демонстрационные логин и пароль RabbitMQ: `payments`.
PostgreSQL доступен на порту `55433`; пользователь, пароль и база — `payments`.
В Compose адреса подключения заданы именами сервисов, а `API_KEY` берётся из `.env`.
Локальные настройки `.env` исключены из Git.

Логи и остановка:

```bash
docker compose logs -f api consumer
docker compose down
```

Данные PostgreSQL и RabbitMQ сохраняются в volumes после остановки.

## Получатель webhook для проверки

Для локального получателя и тестов нужен Python 3.12 или новее.
В отдельном терминале из корня проекта:

```bash
python3.12 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
.venv/bin/uvicorn scripts.webhook_receiver:app --host 0.0.0.0 --port 9000
```

Получатель выводит уведомление в терминал и отвечает `204`.
В Docker Desktop для macOS/Windows адрес получателя из контейнера:
`http://host.docker.internal:9000/webhook`. На Linux укажите адрес получателя,
доступный из контейнера. Если consumer запущен на компьютере, используйте `localhost:9000`.

ТЗ не задаёт тело webhook. В реализации выбраны `payment_id`, итоговый `status`
и `processed_at` в UTC. Успешным считается ответ получателя с кодом `2xx`.

## API

Все запросы требуют `X-API-Key`. Ниже используется значение из `.env.example`;
если изменили `API_KEY`, подставьте своё. Отсутствующий или неверный ключ даёт `401`.

Создание платежа:

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

Все пять полей обязательны. Сумма положительная, до 16 цифр до запятой и двух после;
валюты — `RUB`, `USD`, `EUR`; `metadata` — JSON-объект; `webhook_url` — HTTP или HTTPS.
`Idempotency-Key` обязателен, длина от 1 до 255 символов. Ошибки входных данных дают `422`.
Ответ `202 Accepted` содержит `payment_id`, `status` и `created_at`.

Подставьте `payment_id` из ответа, чтобы получить состояние платежа:

```bash
curl -i http://127.0.0.1:8001/api/v1/payments/UUID_ИЗ_ОТВЕТА \
  -H 'X-API-Key: local-development-key'
```

GET возвращает `200` и все данные платежа, `404` для неизвестного UUID,
`422` для некорректного UUID. Сумма в JSON-ответе передаётся строкой, например `"1500.25"`.
Статусы: `pending`, `succeeded`, `failed`. Страницы Swagger/ReDoc и `/openapi.json` отключены.

## Обработка и доставка

- Платёж и событие Outbox сохраняются одной транзакцией. Повторный ключ возвращает
  первый платёж, сохраняя его данные, даже при изменённом теле или одновременных запросах.
- Relay отправляет постоянные сообщения в durable-очередь `payments.new` через стандартный
  exchange. Отметка публикации ставится после подтверждения RabbitMQ; при ошибке событие
  остаётся для повтора. Повторная отправка использует тот же `message_id`.
- Один consumer с `prefetch_count=1` эмулирует шлюз: задержка 2–5 секунд, 90% `succeeded`
  и 10% `failed`. Результат сохраняется до отправки webhook. Дубликат события не запускает
  шлюз повторно; ошибка уведомления не меняет результат платежа.
- Webhook имеет максимум три попытки с таймаутом 5 секунд; после ошибок — паузы 2 и 4 секунды.
  Счётчик, время повтора и отметка успеха хранятся в PostgreSQL, поэтому перезапуск не обнуляет
  лимит. Попытка фиксируется до HTTP: остановка между записью и отправкой расходует её.
- После третьей ошибки сообщение публикуется в durable-очередь `payments.dlq` с заголовками
  `attempts` и `error_type`. Исходное сообщение подтверждается после доставки webhook или
  подтверждённой публикации в DLQ. Отказ DLQ вызывает повтор переноса без четвёртого webhook.
  Некорректные события и неизвестные платежи также проходят три проверки перед DLQ.

Доставка допускает дубликаты: обрыв после отправки в RabbitMQ или принятия webhook,
но до сохранения отметки в БД, может привести к повтору. Получателю следует обрабатывать
уведомления идемпотентно по `payment_id`.

## Миграции и тесты

В Docker миграции выполняются при запуске API. Для запуска приложения локально настройки
берутся из `.env`, а схема обновляется командой `.venv/bin/alembic upgrade head`.
`0001` создаёт `payments` и `outbox`, `0002` добавляет состояние доставки webhook.

После установки группы `dev`:

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/python -m pytest tests -m 'not integration' -q
docker compose up -d --wait postgres
.venv/bin/python -m pytest tests -q
```

Интеграционные тесты проверяют API, транзакции, идемпотентность, Outbox и повторы webhook
на настоящем PostgreSQL; HTTP и подтверждения RabbitMQ в этих тестах подменяются.
Каждый тест создаёт отдельную базу `test_payments_<uuid>` и удаляет её после завершения.
По умолчанию используется сервер Compose на порту `55433`. Другой сервер можно задать
переменной окружения `TEST_DATABASE_URL`; роль подключения должна иметь право создавать БД.

## Официальная документация

[FastAPI](https://fastapi.tiangolo.com/),
[Pydantic](https://docs.pydantic.dev/latest/),
[SQLAlchemy async](https://docs.sqlalchemy.org/en/20/orm/extensions/asyncio.html),
[RabbitMQ confirms](https://www.rabbitmq.com/docs/confirms),
[FastStream](https://faststream.ag2.ai/latest/),
[Alembic async](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic),
[Docker Compose](https://docs.docker.com/compose/how-tos/startup-order/).
