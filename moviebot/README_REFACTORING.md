# Инструкция по завершению рефакторинга

## Что уже сделано

✅ Создана новая структура директорий `moviebot/`
✅ Созданы базовые файлы:
- `moviebot/config.py` - конфигурация
- `moviebot/states.py` - состояния
- `moviebot/scheduler.py` - задачи планировщика
- `moviebot/main.py` - точка входа
- `moviebot/bot/commands.py` - регистрация handlers
- `moviebot/bot/bot_init.py` - инициализация бота
- `moviebot/bot/handlers/start.py` - обработчик /start
- `moviebot/utils/helpers.py` - вспомогательные функции

✅ Обновлены импорты в:
- `moviebot/database/db_operations.py`
- `moviebot/database/db_connection.py`
- `moviebot/api/kinopoisk_api.py`
- `moviebot/utils/parsing.py`
- `moviebot/web/web_app.py`
- `moviebot/scheduler.py`

✅ Скопированы существующие модули в новую структуру

## Что осталось сделать

### 1. Разбить moviebot.py на handlers

Файл `moviebot.py` содержит 24451 строку кода. Необходимо извлечь обработчики команд в соответствующие handlers:

#### `bot/handlers/seasons.py`
- Команда `/seasons`
- Callbacks: `seasons_kp:`, `seasons_locked:`, `watched_series_list`

#### `bot/handlers/plan.py`
- Команда `/plan`
- Команда `/schedule`
- Callbacks: `plan_type:`, `plan_from_list`, `plan_from_added:`, `remove_from_calendar:`, `plan_detail:`, `schedule_back:`

#### `bot/handlers/payment.py`
- Команда `/payment`
- Callbacks: `payment:*` (все callback'и связанные с оплатой)

#### `bot/handlers/series.py`
- Команда `/search`
- Команда `/random`
- Команда `/premieres`
- Команда `/ticket`
- Команда `/settings`
- Команда `/help`
- Callbacks связанные с сериалами, поиском, рандомом, премьерами

#### `bot/handlers/list.py`
- Команда `/list`
- Callbacks: `list_page:`

#### `bot/handlers/rate.py`
- Команда `/rate`
- Callbacks: `confirm_rating:`, `cancel_rating:`, `rate_film:`

#### `bot/handlers/stats.py`
- Команда `/stats`
- Команда `/total`
- Команда `/admin_stats`

#### `bot/handlers/other.py`
- Команда `/join`
- Команда `/clean`
- Команда `/edit`
- Команда `/refundstars`
- Команда `/unsubscribe`
- Обработчики реакций
- Обработчики текстовых сообщений
- Обработчики файлов

### 2. Создать callbacks модули

#### `bot/callbacks/series_callbacks.py`
- Все callback'и связанные с сериалами
- `series_subscribe:`, `series_unsubscribe:`
- Callbacks для сезонов и эпизодов

#### `bot/callbacks/payment_callbacks.py`
- Все callback'и связанные с оплатой
- `payment:*` (все варианты)

#### `bot/callbacks/search_callbacks.py`
- Callbacks для поиска
- `search_type:`, `search_back:`, `add_film_*`, `confirm_add_film_*`

#### `bot/callbacks/random_callbacks.py`
- Callbacks для рандома
- `rand_mode:`, `rand_period:`, `rand_year:`, `rand_genre:`, `rand_dir:`, `rand_actor:`, `rand_final:`

#### `bot/callbacks/premieres_callbacks.py`
- Callbacks для премьер
- `premiere_detail:`, `premiere_notify:`, `premiere_cancel:`, `premiere_add:`, `premieres_back:`, `premieres_period:`, `premieres_page:`

### 3. Создать api/yookassa_api.py

Вынести всю логику работы с ЮKassa из handlers в отдельный модуль API.

### 4. Обновить все импорты

После создания всех handlers необходимо обновить импорты:
- В `bot/commands.py` зарегистрировать все handlers
- В handlers обновить импорты на новую структуру
- В callbacks обновить импорты

### 5. Протестировать и запушить

- Протестировать работоспособность
- Запушить на GitHub

## Важные замечания

1. **Старая версия сохранена** - файл `moviebot.py` остался на месте, можно использовать как справочник
2. **Импорты** - все импорты должны использовать префикс `moviebot.`
3. **Состояния** - все состояния находятся в `moviebot/states.py`
4. **Конфигурация** - все настройки в `moviebot/config.py`
5. **Планировщик** - задачи в `moviebot/scheduler.py`

## Структура после рефакторинга

```
moviebot/
├── main.py                  # Точка входа
├── config.py               # Конфигурация
├── states.py               # Состояния
├── scheduler.py            # Задачи планировщика
├── bot/
│   ├── __init__.py
│   ├── bot_init.py         # Инициализация бота
│   ├── commands.py         # Регистрация handlers
│   ├── handlers/          # Все handlers
│   │   ├── start.py
│   │   ├── seasons.py
│   │   ├── plan.py
│   │   ├── payment.py
│   │   ├── series.py
│   │   ├── list.py
│   │   ├── rate.py
│   │   ├── stats.py
│   │   └── other.py
│   └── callbacks/         # Callback handlers
│       ├── series_callbacks.py
│       ├── payment_callbacks.py
│       ├── search_callbacks.py
│       ├── random_callbacks.py
│       └── premieres_callbacks.py
├── database/
│   ├── __init__.py
│   ├── db_connection.py
│   └── db_operations.py
├── api/
│   ├── __init__.py
│   ├── kinopoisk_api.py
│   └── yookassa_api.py
├── utils/
│   ├── __init__.py
│   ├── parsing.py
│   ├── payments.py
│   └── helpers.py
├── web/
│   ├── __init__.py
│   ├── web_app.py
│   └── static/
└── services/
    └── nalog_service.py
```

## Следующие шаги

1. Продолжить разбиение `moviebot.py` на handlers
2. Создать все callbacks модули
3. Обновить импорты
4. Протестировать
5. Запушить на GitHub

