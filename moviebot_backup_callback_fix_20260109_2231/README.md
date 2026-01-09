# Movie Planner Bot

Telegram бот для планирования просмотра фильмов и сериалов.

## Структура проекта

```
moviebot/
├── main.py                  # Точка входа: создаёт bot, запускает webhook/polling
├── config.py                # TOKEN, настройки
├── states.py                 # Все состояния (user_plan_state и т.д.)
├── scheduler.py             # APScheduler задачи
├── bot/
│   ├── __init__.py
│   ├── bot_init.py          # Инициализация бота, установка команд
│   ├── commands.py          # Регистрация всех command handlers
│   ├── handlers/            # Все хэндлеры по папкам
│   │   ├── start.py
│   │   ├── list.py
│   │   ├── seasons.py
│   │   ├── plan.py
│   │   ├── payment.py
│   │   ├── series.py
│   │   ├── rate.py
│   │   └── stats.py
│   └── callbacks/           # Callback-хэндлеры
│       ├── series_callbacks.py
│       └── payment_callbacks.py
├── database/
│   ├── __init__.py
│   ├── db_connection.py     # Подключение к БД
│   └── db_operations.py     # Операции с БД
├── api/
│   ├── __init__.py
│   ├── kinopoisk_api.py     # API Кинопоиска
│   └── yookassa_api.py      # API ЮKassa (TODO)
├── utils/
│   ├── __init__.py
│   ├── parsing.py           # Парсинг текста, дат, ссылок
│   ├── payments.py          # Утилиты для платежей (TODO)
│   └── helpers.py           # Вспомогательные функции
├── web/
│   ├── __init__.py
│   ├── web_app.py           # Flask webhook
│   └── static/              # Для Mini App, если нужно
└── services/
    └── nalog_service.py     # Сервис для работы с nalog.ru
```

## Установка

1. Установите зависимости:
```bash
pip install -r requirements.txt
```

2. Создайте файл `.env` с переменными окружения:
```
BOT_TOKEN=your_bot_token
KP_TOKEN=your_kinopoisk_token
DATABASE_URL=your_database_url
YOOKASSA_SHOP_ID=your_shop_id
YOOKASSA_SECRET_KEY=your_secret_key
NALOG_INN=your_inn
NALOG_PASSWORD=your_password
```

3. Запустите бота:
```bash
python moviebot/main.py
```

## Рефакторинг

Проект находится в процессе рефакторинга. Старая версия сохранена в `moviebot.py`.

См. `COMPLETION_GUIDE.md` для инструкций по завершению рефакторинга.

