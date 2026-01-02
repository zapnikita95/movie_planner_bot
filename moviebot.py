from dotenv import load_dotenv
load_dotenv()  # загружает .env (для локальной разработки)

# В Railway переменные окружения должны быть доступны напрямую через os.getenv()
# Но иногда Railway не подставляет значения из ${{Service.VAR}}
# Поэтому проверяем и логируем все доступные переменные

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
import os
import random
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta, date
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
import pytz
import dateutil.parser
import logging
import json
import sys
from flask import Flask, request, abort, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Детальное логирование для отладки
logger.info("[DEBUG] Проверка переменных окружения...")
all_env_keys = list(os.environ.keys())
logger.info(f"[DEBUG] Всего переменных окружения: {len(all_env_keys)}")
logger.info(f"[DEBUG] Первые 20 ключей: {all_env_keys[:20]}")
logger.info(f"[DEBUG] BOT_TOKEN присутствует: {'BOT_TOKEN' in os.environ}")
logger.info(f"[DEBUG] DATABASE_URL присутствует: {'DATABASE_URL' in os.environ}")

TOKEN = os.getenv('BOT_TOKEN')
if TOKEN:
    logger.info(f"[DEBUG] BOT_TOKEN получен, длина: {len(TOKEN)} символов")
else:
    logger.error("BOT_TOKEN не задан! Бот не может работать.")
    logger.error(f"[DEBUG] Все переменные окружения: {all_env_keys}")
    raise ValueError("Добавьте BOT_TOKEN в environment variables")

bot = telebot.TeleBot(TOKEN)
# Получаем ID бота для исключения из подсчета участников
try:
    bot_info = bot.get_me()
    BOT_ID = bot_info.id
    logger.info(f"ID бота: {BOT_ID}")
except Exception as e:
    logger.warning(f"Не удалось получить ID бота: {e}")
    BOT_ID = None

# Очищаем старые webhook, если были (с обработкой ошибок)
try:
    bot.remove_webhook()
    logger.info("Старые webhook очищены")
except Exception as e:
    logger.warning(f"Не удалось очистить webhook (возможно, токен неверный или еще не установлен): {e}")

# Токен Kinopoisk API
KP_TOKEN = os.getenv('KP_TOKEN')

# Планировщик для уведомлений
scheduler = BackgroundScheduler()
scheduler.start()

# Состояния планирования
user_plan_state = {}  # user_id: {'step': int, 'link': str, 'type': str, 'day_or_date': str}
bot_messages = {}  # message_id: link (храним карточки бота)
plan_notification_messages = {}  # message_id: {'link': str} (храним сообщения о планах для обработки реакций)
list_messages = {}  # message_id: chat_id (храним сообщения /list для обработки ответов)
plan_error_messages = {}  # message_id: {'user_id': int, 'chat_id': int, 'link': str, 'plan_type': str or None, 'day_or_date': str or None, 'missing': str}
# Состояния настроек
user_settings_state = {}  # user_id: {'waiting_emoji': bool}
settings_messages = {}  # message_id: {'user_id': int, 'action': str, 'chat_id': int} - для отслеживания сообщений settings
user_import_state = {}  # user_id: {'step': str, 'kp_user_id': str, 'count': int} - для импорта базы из Кинопоиска
# Состояния очистки
user_clean_state = {}  # user_id: {'action': str, 'target': str}
clean_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}
# Состояния очистки
user_clean_state = {}  # user_id: {'action': str, 'target': str}
clean_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}
# Состояния редактирования
user_edit_state = {}  # user_id: {'action': str, 'plan_id': int, 'step': str, ...}
# Состояния работы с билетами
user_ticket_state = {}  # user_id: {'step': str, 'plan_id': int, 'file_id': str, ...}
plans_tz = pytz.timezone('Europe/Moscow')
months_map = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12,
    # Сокращенные названия месяцев
    'янв': 1, 'фев': 2, 'мар': 3, 'апр': 4, 'май': 5, 'июн': 6,
    'июл': 7, 'авг': 8, 'сен': 9, 'сент': 9, 'окт': 10, 'ноя': 11, 'дек': 12,
    # Названия месяцев в именительном падеже (для "в марте")
    'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4, 'май': 5, 'июнь': 6,
    'июль': 7, 'август': 8, 'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
}
# Расширенный маппинг дней недели
days_full = {
    'понедельник': 0, 'пн': 0, 'в понедельник': 0, 'на понедельник': 0,
    'вторник': 1, 'вт': 1, 'во вторник': 1, 'на вторник': 1,
    'среда': 2, 'ср': 2, 'в среду': 2, 'на среду': 2,
    'четверг': 3, 'чт': 3, 'в четверг': 3, 'на четверг': 3,
    'пятница': 4, 'пт': 4, 'в пятницу': 4, 'на пятницу': 4,
    'суббота': 5, 'сб': 5, 'в субботу': 5, 'на субботу': 5,
    'воскресенье': 6, 'вс': 6, 'в воскресенье': 6, 'на воскресенье': 6,
    'в пн': 0, 'в вт': 1, 'в ср': 2, 'в чт': 3, 'в пт': 4, 'в сб': 5, 'в вс': 6,
    'на пн': 0, 'на вт': 1, 'на ср': 2, 'на чт': 3, 'на пт': 4, 'на сб': 5, 'на вс': 6
}
days_map = days_full  # Для обратной совместимости

# Команды
commands = [
    BotCommand("start", "Приветствие и инструкция по использованию"),
    BotCommand("list", "Список непросмотренных фильмов"),
    BotCommand("random", "Рандомный фильм с фильтрами"),
    BotCommand("search", "Поиск фильмов через Kinopoisk API"),
    BotCommand("plan", "Запланировать просмотр дома или в кино"),
    BotCommand("schedule", "Список запланированных просмотров"),
    BotCommand("total", "Статистика: фильмы, жанры, режиссёры, актёры и оценки"),
    BotCommand("stats", "Детальная статистика группы и участников"),
    BotCommand("rate", "Оценить просмотренные фильмы"),
    BotCommand("settings", "Настройки: эмодзи, часовой пояс, загрузка голосов"),
    BotCommand("clean", "Очистить базу данных (чат или данные о просмотрах)"),
    BotCommand("edit", "Редактировать расписание и оценки"),
    BotCommand("ticket", "Работа с билетами в кино"),
    BotCommand("seasons", "Просмотр сезонов сериалов"),
    BotCommand("premieres", "Список премьер месяца"),
    BotCommand("help", "Помощь по командам")
]
bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllGroupChats())
bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeDefault())

# БД
DATABASE_URL = os.getenv('DATABASE_URL')

if DATABASE_URL:
    # Проверяем, не является ли это ссылкой на другую переменную (Railway синтаксис)
    if DATABASE_URL.startswith('${{') and DATABASE_URL.endswith('}}'):
        logger.error(f"[DEBUG] DATABASE_URL содержит ссылку на другую переменную: {DATABASE_URL}")
        logger.error("[DEBUG] Railway не подставил значение автоматически. Попробуйте использовать прямой connection string.")
    else:
        logger.info(f"[DEBUG] DATABASE_URL получен, длина: {len(DATABASE_URL)} символов")
        logger.info(f"[DEBUG] DATABASE_URL начинается с: {DATABASE_URL[:20]}...")
else:
    logger.error("DATABASE_URL не задан! Бот не может подключиться к БД.")
    logger.error(f"[DEBUG] Все переменные окружения: {all_env_keys}")
    logger.error("Проверьте, что переменная окружения DATABASE_URL установлена в настройках вашей платформы (Railway/Render/etc.)")
    raise ValueError("DATABASE_URL не задан! Добавьте DATABASE_URL в environment variables вашей платформы (Railway/Render/etc.)")

try:
    conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    cursor = conn.cursor()
    logger.info("Подключение к PostgreSQL успешно!")
except Exception as e:
    logger.error(f"Не удалось подключиться к БД: {e}")
    raise
# Блокировка для синхронизации доступа к БД из разных потоков
db_lock = threading.Lock()
# Создаём таблицы с BIGINT для chat_id (Telegram группы могут иметь очень большие отрицательные ID)
cursor.execute('''
    CREATE TABLE IF NOT EXISTS movies (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        link TEXT,
        kp_id TEXT,
        title TEXT,
        year INTEGER,
        genres TEXT,
        description TEXT,
        director TEXT,
        actors TEXT,
        watched INTEGER DEFAULT 0,
        rating REAL DEFAULT NULL,
        UNIQUE(chat_id, kp_id)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS settings (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        key TEXT,
        value TEXT,
        UNIQUE(chat_id, key)
    )
''')
# Базовый набор эмодзи: ✅, все варианты лайков (👍 👍🏻 👍🏼 👍🏽 👍🏾 👍🏿), все варианты сердечек (❤️ ❤️‍🔥 ❤️‍🩹 💛 🧡 💚 💙 💜 🖤 🤍 🤎)
default_watched_emojis = "✅👍👍🏻👍🏼👍🏽👍🏾👍🏿❤️❤️‍🔥❤️‍🩹💛🧡💚💙💜🖤🤍🤎"
cursor.execute('INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', (-1, "watched_emoji", default_watched_emojis))
cursor.execute('''
    CREATE TABLE IF NOT EXISTS plans (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        plan_type TEXT,
        plan_datetime TIMESTAMP WITH TIME ZONE,
        user_id BIGINT,
        ticket_file_id TEXT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS stats (
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        username TEXT,
        command_or_action TEXT,
        timestamp TEXT,
        chat_id BIGINT
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS series_tracking (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        kp_id TEXT,
        user_id BIGINT,
        season_number INTEGER,
        episode_number INTEGER,
        watched BOOLEAN DEFAULT FALSE,
        watched_date TIMESTAMP WITH TIME ZONE,
        UNIQUE(chat_id, film_id, user_id, season_number, episode_number)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS series_subscriptions (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        kp_id TEXT,
        user_id BIGINT,
        subscribed BOOLEAN DEFAULT TRUE,
        UNIQUE(chat_id, film_id, user_id)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS ratings (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        user_id BIGINT,
        rating INTEGER CHECK(rating BETWEEN 1 AND 10),
        is_imported BOOLEAN DEFAULT FALSE,
        UNIQUE(chat_id, film_id, user_id)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS watched_movies (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        user_id BIGINT,
        watched_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(chat_id, film_id, user_id)
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS cinema_votes (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        deadline TEXT,
        message_id BIGINT,
        yes_users TEXT DEFAULT '[]',
        no_users TEXT DEFAULT '[]'
    )
''')
cursor.execute('''
    CREATE TABLE IF NOT EXISTS tickets (
        id SERIAL PRIMARY KEY,
        plan_id INTEGER REFERENCES plans(id) ON DELETE CASCADE,
        chat_id BIGINT,
        file_id TEXT,
        file_path TEXT,
        session_datetime TIMESTAMP WITH TIME ZONE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    );
    
    CREATE TABLE IF NOT EXISTS premiere_reminders (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT NOT NULL,
        user_id BIGINT NOT NULL,
        kp_id TEXT NOT NULL,
        film_title TEXT,
        premiere_date DATE,
        reminder_sent BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        UNIQUE(chat_id, user_id, kp_id)
    )
''')

# Миграция: изменяем тип данных для существующих таблиц (если они уже созданы с INTEGER)
# Это безопасно - если колонка уже BIGINT, команда не изменит ничего
try:
    cursor.execute('ALTER TABLE movies ALTER COLUMN chat_id TYPE BIGINT')
    logger.info("Миграция: movies.chat_id изменён на BIGINT")
except Exception as e:
    logger.debug(f"Миграция movies.chat_id: {e}")

try:
    cursor.execute('ALTER TABLE settings ALTER COLUMN chat_id TYPE BIGINT')
    logger.info("Миграция: settings.chat_id изменён на BIGINT")
except Exception as e:
    logger.debug(f"Миграция settings.chat_id: {e}")

try:
    cursor.execute('ALTER TABLE plans ALTER COLUMN chat_id TYPE BIGINT')
    cursor.execute('ALTER TABLE plans ALTER COLUMN user_id TYPE BIGINT')
    logger.info("Миграция: plans.chat_id и plans.user_id изменены на BIGINT")
except Exception as e:
    logger.debug(f"Миграция plans: {e}")

try:
    cursor.execute('ALTER TABLE stats ALTER COLUMN chat_id TYPE BIGINT')
    cursor.execute('ALTER TABLE stats ALTER COLUMN user_id TYPE BIGINT')
    logger.info("Миграция: stats.chat_id и stats.user_id изменены на BIGINT")
except Exception as e:
    logger.debug(f"Миграция stats: {e}")

try:
    cursor.execute('ALTER TABLE ratings ALTER COLUMN chat_id TYPE BIGINT')
    cursor.execute('ALTER TABLE ratings ALTER COLUMN user_id TYPE BIGINT')
    logger.info("Миграция: ratings.chat_id и ratings.user_id изменены на BIGINT")
except Exception as e:
    logger.debug(f"Миграция ratings: {e}")

try:
    cursor.execute('ALTER TABLE ratings ADD COLUMN IF NOT EXISTS is_imported BOOLEAN DEFAULT FALSE')
    logger.info("Миграция: поле is_imported добавлено в ratings")
except Exception as e:
    logger.debug(f"Миграция ratings.is_imported: {e}")

try:
    cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN chat_id TYPE BIGINT')
    cursor.execute('ALTER TABLE cinema_votes ALTER COLUMN message_id TYPE BIGINT')
    logger.info("Миграция: cinema_votes.chat_id и cinema_votes.message_id изменены на BIGINT")
except Exception as e:
    logger.debug(f"Миграция cinema_votes: {e}")

# Миграция: изменяем тип plan_datetime с TEXT на TIMESTAMP WITH TIME ZONE
try:
    cursor.execute("ALTER TABLE plans ALTER COLUMN plan_datetime TYPE TIMESTAMP WITH TIME ZONE USING plan_datetime::TIMESTAMP WITH TIME ZONE")
    logger.info("Миграция: plan_datetime в plans изменён на TIMESTAMP WITH TIME ZONE")
    conn.commit()
except Exception as e:
    logger.debug(f"Миграция plan_datetime: {e}")
    try:
        conn.rollback()
    except:
        pass

# Добавляем поле ticket_file_id в таблицу plans, если его нет
try:
    cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS ticket_file_id TEXT")
    conn.commit()
    logger.info("Поле ticket_file_id добавлено в таблицу plans (или уже существует)")
except Exception as e:
    logger.warning(f"Ошибка при добавлении поля ticket_file_id: {e}")
    conn.rollback()
    try:
        conn.rollback()
    except:
        pass

# Добавляем поле notification_sent в таблицу plans, если его нет
try:
    cursor.execute("ALTER TABLE plans ADD COLUMN IF NOT EXISTS notification_sent BOOLEAN DEFAULT FALSE")
    conn.commit()
    logger.info("Поле notification_sent добавлено в таблицу plans (или уже существует)")
except Exception as e:
    logger.warning(f"Ошибка при добавлении поля notification_sent: {e}")
    conn.rollback()
    try:
        conn.rollback()
    except:
        pass

# Ключевой блок: очистка дубликатов и создание уникального индекса
try:
    # Удаляем старые индексы и constraints, если они существуют
    # Добавляем поле is_series, если его нет
    try:
        cursor.execute('ALTER TABLE movies ADD COLUMN IF NOT EXISTS is_series INTEGER DEFAULT 0')
        conn.commit()
        logger.info("Поле is_series добавлено в таблицу movies")
    except Exception as e:
        logger.debug(f"Поле is_series уже существует или ошибка: {e}")
        conn.rollback()
    
    try:
        cursor.execute('DROP INDEX IF EXISTS movies_chat_id_kp_id_key')
        cursor.execute('DROP INDEX IF EXISTS movies_chat_id_kp_id_idx')
        cursor.execute('DROP INDEX IF EXISTS movies_chat_id_kp_id_unique')
    except Exception as idx_error:
        logger.debug(f"Ошибка при удалении индексов (может не существовать): {idx_error}")
        conn.rollback()
    
    try:
        cursor.execute('ALTER TABLE movies DROP CONSTRAINT IF EXISTS movies_chat_id_kp_id_unique')
    except Exception as const_error:
        logger.debug(f"Ошибка при удалении constraint (может не существовать): {const_error}")
        conn.rollback()  # КРИТИЧНО: откатываем транзакцию после ошибки
    
    # Удаляем дубликаты, оставляя только одну запись (с наименьшим id)
    try:
        cursor.execute("""
            DELETE FROM movies a USING (
                SELECT MIN(id) as keep_id, chat_id, kp_id
                FROM movies 
                GROUP BY chat_id, kp_id 
                HAVING COUNT(*) > 1
            ) b
            WHERE a.chat_id = b.chat_id AND a.kp_id = b.kp_id AND a.id != b.keep_id
        """)
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            logger.info(f"Удалено дубликатов фильмов: {deleted_count}")
        conn.commit()
    except Exception as del_error:
        logger.warning(f"Ошибка при удалении дубликатов: {del_error}")
        conn.rollback()
        raise del_error
    
    # Теперь безопасно создаём уникальный индекс
    try:
        # Используем обычный CREATE UNIQUE INDEX (CONCURRENTLY не работает в транзакции)
        cursor.execute('CREATE UNIQUE INDEX IF NOT EXISTS movies_chat_id_kp_id_unique ON movies (chat_id, kp_id)')
        logger.info("Уникальный индекс на movies(chat_id, kp_id) успешно создан")
        conn.commit()
    except Exception as idx_create_error:
        logger.warning(f"Ошибка при создании уникального индекса: {idx_create_error}")
        conn.rollback()
        # Пробуем создать constraint как fallback
        try:
            cursor.execute('ALTER TABLE movies ADD CONSTRAINT movies_chat_id_kp_id_unique UNIQUE (chat_id, kp_id)')
            conn.commit()
            logger.info("Уникальный constraint movies(chat_id, kp_id) создан как fallback")
        except Exception as e2:
            logger.debug(f"Constraint уже существует или ошибка: {e2}")
            conn.rollback()
except Exception as e:
    logger.warning(f"Критическая ошибка при очистке дубликатов или создании уникального индекса: {e}", exc_info=True)
    try:
        conn.rollback()
    except:
        pass

# Индексы для скорости
logger.info("[DEBUG] Перед созданием индексов")
try:
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_chat_id ON movies (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_movies_link ON movies (link)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_chat_id ON ratings (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ratings_film_id ON ratings (film_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_chat_id ON plans (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_film_id ON plans (film_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_plans_datetime ON plans (plan_datetime)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_settings_chat_id ON settings (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_stats_chat_id ON stats (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cinema_votes_chat_id ON cinema_votes (chat_id)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cinema_votes_film_id ON cinema_votes (film_id)')
    logger.info("[DEBUG] Индексы созданы")
except Exception as idx_error:
    logger.error(f"[DEBUG] Ошибка при создании индексов: {idx_error}", exc_info=True)
    conn.rollback()

conn.commit()

logger.info("[DEBUG] После conn.commit(), перед определением функций")
logger.info("[DEBUG] Все таблицы созданы, миграции выполнены")

def get_watched_emoji(chat_id):
    """Возвращает строку с эмодзи для отметки просмотренных (может быть несколько) для конкретного чата"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                return value
        # Дефолт, если не настроено: ✅, все варианты лайков и сердечек
        return "✅👍👍🏻👍🏼👍🏽👍🏾👍🏿❤️❤️‍🔥❤️‍🩹💛🧡💚💙💜🖤🤍🤎"

def get_watched_emojis(chat_id):
    """Возвращает эмодзи для отметки просмотренных для конкретного чата как список"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                # Убираем кастомные эмодзи вида custom:ID из строки
                import re
                value_clean = re.sub(r'custom:\d+,?', '', str(value))
                
                # Список известных эмодзи для правильного извлечения
                known_emojis = ['✅', '👍', '👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿', '❤️', '❤️‍🔥', '❤️‍🩹', '💛', '🧡', '💚', '💙', '💜', '🖤', '🤍', '🤎']
                
                # Извлекаем эмодзи из строки, проверяя по известным эмодзи (в порядке длины, чтобы сначала проверять составные)
                found_emojis = []
                value_remaining = value_clean
                
                # Сортируем по длине (от длинных к коротким), чтобы сначала находить составные эмодзи
                sorted_emojis = sorted(known_emojis, key=len, reverse=True)
                
                for emoji in sorted_emojis:
                    while emoji in value_remaining:
                        idx = value_remaining.index(emoji)
                        found_emojis.append(emoji)
                        # Удаляем найденный эмодзи из строки
                        value_remaining = value_remaining[:idx] + value_remaining[idx+len(emoji):]
                
                # Если нашли эмодзи, возвращаем их
                if found_emojis:
                    return found_emojis
                
                # Если не нашли известные эмодзи, пробуем извлечь все эмодзи из строки
                # Используем библиотеку для правильного разбора эмодзи
                try:
                    import emoji
                    emojis_list = emoji.distinct_emoji_list(value_clean)
                    if emojis_list:
                        return emojis_list
                except:
                    # Если библиотека emoji недоступна, возвращаем дефолт
                    pass
                
                # Если ничего не нашли, возвращаем дефолт
                return ['✅']
        # Дефолт, если не настроено: ✅, все варианты лайков и сердечек
        return ['✅', '👍', '👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿', '❤️', '❤️‍🔥', '❤️‍🩹', '💛', '🧡', '💚', '💙', '💜', '🖤', '🤍', '🤎']

def get_watched_custom_emoji_ids(chat_id):
    """Возвращает список ID кастомных эмодзи для отметки просмотренных для конкретного чата"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                # Ищем кастомные эмодзи в формате custom:ID
                import re
                custom_ids = re.findall(r'custom:(\d+)', str(value))
                return [str(cid) for cid in custom_ids]
        return []

def is_watched_emoji(reaction_emoji, chat_id):
    """Проверяет, является ли реакция одним из сохранённых эмодзи для просмотра"""
    watched_emojis = get_watched_emoji(chat_id)
    # Если сохранено несколько эмодзи, проверяем каждый
    return reaction_emoji in watched_emojis

def get_user_timezone(user_id):
    """Получает часовой пояс пользователя. Возвращает pytz.timezone объект или None"""
    try:
        with db_lock:
            cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = %s", (user_id, 'user_timezone'))
            row = cursor.fetchone()
            if row:
                tz_name = row.get('value') if isinstance(row, dict) else row[0]
                if tz_name == 'Moscow':
                    return pytz.timezone('Europe/Moscow')
                elif tz_name == 'Serbia':
                    return pytz.timezone('Europe/Belgrade')
        return None
    except Exception as e:
        logger.error(f"Ошибка получения часового пояса для user_id={user_id}: {e}", exc_info=True)
        return None

def parse_session_time(text, user_tz):
    """Парсит время сеанса из текста в форматах:
    - 15 января 10:30
    - 17.01 15:20
    - 10.05.2025 21:40
    - 17 января 12 12 (без двоеточия)
    Возвращает datetime в user_tz или None
    """
    text = text.strip()
    now = datetime.now(user_tz)
    
    # Формат: "15 января 10:30" или "15 января 10 30" или "17 января 15:30"
    match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2})[: ](\d{1,2})', text)
    if match:
        day = int(match.group(1))
        month_str = match.group(2)
        hour = int(match.group(3))
        minute = int(match.group(4))
        
        month = months_map.get(month_str.lower())
        if month:
            year = now.year
            try:
                dt = datetime(year, month, day, hour, minute)
                dt = user_tz.localize(dt)
                if dt < now:
                    # Если дата в прошлом, берем следующий год
                    dt = datetime(year + 1, month, day, hour, minute)
                    dt = user_tz.localize(dt)
                return dt
            except ValueError:
                return None
    
    # Формат: "17.01 15:20" или "17.01.2025 15:20"
    match = re.search(r'(\d{1,2})\.(\d{1,2})(?:\.(\d{2,4}))?\s+(\d{1,2})[: ](\d{1,2})', text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_str = match.group(3)
        hour = int(match.group(4))
        minute = int(match.group(5))
        
        if year_str:
            year = int(year_str)
            if year < 100:
                year += 2000
        else:
            year = now.year
        
        try:
            dt = datetime(year, month, day, hour, minute)
            dt = user_tz.localize(dt)
            if dt < now:
                # Если дата в прошлом, берем следующий год
                dt = datetime(year + 1, month, day, hour, minute)
                dt = user_tz.localize(dt)
            return dt
        except ValueError:
            return None
    
    return None

def get_user_timezone_or_default(user_id):
    """Получает часовой пояс пользователя или возвращает часовой пояс по умолчанию (Москва)"""
    tz = get_user_timezone(user_id)
    if tz:
        return tz
    return pytz.timezone('Europe/Moscow')

def set_user_timezone(user_id, timezone_name):
    """Устанавливает часовой пояс пользователя. timezone_name: 'Moscow' или 'Serbia'"""
    try:
        with db_lock:
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (user_id, 'user_timezone', timezone_name))
            conn.commit()
            logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
            return True
    except Exception as e:
        logger.error(f"Ошибка установки часового пояса для user_id={user_id}: {e}", exc_info=True)
        conn.rollback()
        return False

def detect_timezone_from_message(message_date_utc):
    """Пытается определить часовой пояс по времени сообщения (UTC).
    Возвращает предполагаемый часовой пояс или None если неясно.
    message_date_utc - datetime объект в UTC"""
    try:
        # Получаем текущее время в UTC
        utc_now = datetime.now(pytz.utc)
        if message_date_utc.tzinfo is None:
            # Если нет таймзоны, предполагаем UTC
            msg_utc = pytz.utc.localize(message_date_utc)
        else:
            msg_utc = message_date_utc.astimezone(pytz.utc)
        
        # Вычисляем разницу между текущим временем и временем сообщения
        # Это не очень надежно, но может дать подсказку
        # Более надежный способ - анализировать время активности пользователя
        
        # Получаем час в UTC
        utc_hour = msg_utc.hour
        
        # Предполагаем, что пользователь активен в разумное время (8-23 часа местного времени)
        # Если сообщение отправлено в 16:00 UTC, и это разумное время для активности:
        # - Москва (UTC+3): 16:00 UTC = 19:00 MSK - разумно
        # - Сербия (UTC+1): 16:00 UTC = 17:00 CET - разумно
        
        # Но это неточно, поэтому просто возвращаем None
        # Лучше спросить у пользователя
        return None
    except Exception as e:
        logger.error(f"Ошибка определения часового пояса: {e}", exc_info=True)
        return None

def check_timezone_change(user_id, message_date_utc):
    """Проверяет, изменился ли часовой пояс пользователя.
    Возвращает True если нужно уточнить часовой пояс, False если все ок"""
    try:
        current_tz = get_user_timezone(user_id)
        if not current_tz:
            # Часовой пояс не установлен - нужно уточнить
            return True
        
        # Сохраняем время последнего сообщения для анализа
        with db_lock:
            # Получаем предыдущее время сообщения
            cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = %s", (user_id, 'prev_message_utc'))
            prev_row = cursor.fetchone()
            
            if prev_row:
                prev_utc_str = prev_row.get('value') if isinstance(prev_row, dict) else prev_row[0]
                try:
                    prev_utc = datetime.fromisoformat(prev_utc_str)
                    if prev_utc.tzinfo is None:
                        prev_utc = pytz.utc.localize(prev_utc)
                    
                    # Вычисляем разницу во времени между сообщениями
                    time_diff = message_date_utc - prev_utc
                    
                    # Если разница больше 2 часов, возможно пользователь переехал
                    # Но это не надежно, поэтому просто проверяем паттерн активности
                    # Для простоты: если часовой пояс установлен, считаем что все ок
                except:
                    pass
            
            # Обновляем предыдущее время
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (%s, %s, %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (user_id, 'prev_message_utc', message_date_utc.isoformat()))
            conn.commit()
        
        return False
    except Exception as e:
        logger.error(f"Ошибка проверки изменения часового пояса: {e}", exc_info=True)
        return True  # В случае ошибки лучше уточнить

def show_timezone_selection(chat_id, user_id, prompt_text="Выберите часовой пояс:"):
    """Показывает выбор часового пояса пользователю"""
    current_tz = get_user_timezone(user_id)
    current_tz_name = "Москва" if not current_tz or current_tz.zone == 'Europe/Moscow' else "Сербия"
    current_tz_display = current_tz_name if current_tz else "не установлен"
    
    # Получаем текущее время в обоих часовых поясах для отображения
    moscow_tz = pytz.timezone('Europe/Moscow')
    serbia_tz = pytz.timezone('Europe/Belgrade')
    now_utc = datetime.now(pytz.utc)
    moscow_time = now_utc.astimezone(moscow_tz).strftime('%H:%M')
    serbia_time = now_utc.astimezone(serbia_tz).strftime('%H:%M')
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton(f"🇷🇺 Москва (MSK) — сейчас {moscow_time}", callback_data="timezone:Moscow"))
    markup.add(InlineKeyboardButton(f"🇷🇸 Сербия (CET) — сейчас {serbia_time}", callback_data="timezone:Serbia"))
    
    bot.send_message(
        chat_id,
        f"🕐 {prompt_text}\n\n"
        f"Текущий: <b>{current_tz_display}</b>\n\n"
        f"Часовой пояс будет автоматически обновляться при путешествиях.",
        reply_markup=markup,
        parse_mode='HTML'
    )

def get_watched_reactions(chat_id):
    """Возвращает словарь с обычными и кастомными эмодзи для реакций"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'watched_reactions'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                try:
                    reactions = json.loads(value)
                    emojis = [r for r in reactions if not r.startswith('custom:')]
                    custom_ids = [r.split('custom:')[1] for r in reactions if r.startswith('custom:')]
                    return {'emoji': emojis, 'custom': custom_ids}
                except:
                    pass
    # Дефолт: ✅, все варианты лайков и сердечек
    return {'emoji': ['✅', '👍', '👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿', '❤️', '❤️‍🔥', '❤️‍🩹', '💛', '🧡', '💚', '💙', '💜', '🖤', '🤍', '🤎'], 'custom': []}

# Статистика
def log_request(user_id, username, command_or_action, chat_id=None):
    """Логирует запрос пользователя в БД"""
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"[LOG_REQUEST] Попытка логирования: user_id={user_id}, username={username}, command={command_or_action}, chat_id={chat_id}, timestamp={timestamp}")
        with db_lock:
            try:
                # Проверяем, не в состоянии ли ошибки транзакция
                try:
                    cursor.execute('SELECT 1')
                    cursor.fetchone()
                except:
                    # Если транзакция в состоянии ошибки, откатываем
                    conn.rollback()
                
                cursor.execute('''
                    INSERT INTO stats (user_id, username, command_or_action, timestamp, chat_id)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (user_id, username, command_or_action, timestamp, chat_id))
                conn.commit()
                logger.debug(f"[LOG_REQUEST] Успешно залогировано: user_id={user_id}, command={command_or_action}, chat_id={chat_id}")
            except Exception as db_error:
                # КРИТИЧНО: откатываем транзакцию при ошибке
                conn.rollback()
                logger.error(f"[LOG_REQUEST] Ошибка БД при логировании: {db_error}", exc_info=True)
                raise db_error
    except Exception as e:
        logger.error(f"Ошибка логирования запроса: {e}", exc_info=True)
        # Убеждаемся, что транзакция откачена
        try:
            with db_lock:
                conn.rollback()
        except:
            pass

def print_daily_stats():
    """Выводит статистику за текущий день в консоль"""
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        with db_lock:
            cursor.execute('''
                SELECT COUNT(*) as total_requests,
                       COUNT(DISTINCT user_id) as unique_users
                FROM stats
                WHERE DATE(timestamp) = DATE(%s)
            ''', (today,))
            row = cursor.fetchone()
            if row:
                total_requests = row.get('total_requests') if isinstance(row, dict) else (row[0] if len(row) > 0 else 0)
                unique_users = row.get('unique_users') if isinstance(row, dict) else (row[1] if len(row) > 1 else 0)
            else:
                total_requests = 0
                unique_users = 0
            
            # Статистика по командам
            cursor.execute('''
                SELECT command_or_action, COUNT(*) as count
                FROM stats
                WHERE DATE(timestamp) = DATE(%s)
                GROUP BY command_or_action
                ORDER BY count DESC
            ''', (today,))
            commands_stats = cursor.fetchall()
        
        print("\n" + "=" * 60)
        print(f"📊 СТАТИСТИКА БОТА ЗА {today}")
        print("=" * 60)
        print(f"📈 Всего запросов за день: {total_requests}")
        print(f"👥 Уникальных пользователей: {unique_users}")
        print("\n📋 Топ команд/действий:")
        if commands_stats:
            for cmd, count in commands_stats:
                print(f"   • {cmd}: {count}")
        else:
            print("   (нет данных)")
        print("=" * 60 + "\n")
    except Exception as e:
        logger.error(f"Ошибка вывода статистики: {e}")

# Периодический вывод статистики (каждый час)
def hourly_stats():
    """Вызывается каждый час для вывода статистики"""
    print_daily_stats()

# Функции для уведомлений о планах (определяем до использования в scheduler)
def send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=None):
    """Отправляет уведомление о запланированном просмотре"""
    try:
        plan_type_text = "дома" if plan_type == 'home' else "в кино"
        text = f"🔔 Напоминание: сегодня запланирован просмотр {plan_type_text}!\n\n"
        text += f"<b>{title}</b>\n{link}"
        msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
        # Сохраняем message_id для обработки реакций
        plan_notification_messages[msg.message_id] = {'link': link}
        logger.info(f"[PLAN NOTIFICATION] Уведомление отправлено для фильма {title} в чат {chat_id}")
        
        # Отмечаем как отправленное в базе данных, если plan_id передан
        if plan_id:
            try:
                with db_lock:
                    cursor.execute('''
                        UPDATE plans 
                        SET notification_sent = TRUE 
                        WHERE id = %s
                    ''', (plan_id,))
                    conn.commit()
                logger.info(f"[PLAN NOTIFICATION] План {plan_id} отмечен как уведомление отправлено")
            except Exception as e:
                logger.warning(f"[PLAN NOTIFICATION] Не удалось отметить план {plan_id} как отправленный: {e}")
    except Exception as e:
        logger.error(f"[PLAN NOTIFICATION] Ошибка отправки уведомления: {e}")

def check_and_send_plan_notifications():
    """Периодическая проверка планов и отправка пропущенных уведомлений"""
    try:
        now_utc = datetime.now(pytz.utc)
        # Проверяем планы, которые должны были быть отправлены в последние 30 минут
        # (чтобы не пропустить уведомления после перезапуска бота)
        check_start = now_utc - timedelta(minutes=30)
        check_end = now_utc + timedelta(minutes=5)  # Небольшой запас на будущее
        
        with db_lock:
            cursor.execute('''
                SELECT p.id, p.chat_id, p.film_id, p.plan_type, p.plan_datetime, p.user_id,
                       m.title, m.link, p.notification_sent
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.plan_datetime >= %s 
                  AND p.plan_datetime <= %s
                  AND (p.notification_sent IS NULL OR p.notification_sent = FALSE)
            ''', (check_start, check_end))
            plans = cursor.fetchall()
        
        if plans:
            logger.info(f"[PLAN CHECK] Найдено {len(plans)} планов для проверки уведомлений")
        
        for plan in plans:
            if isinstance(plan, dict):
                plan_id = plan.get('id')
                chat_id = plan.get('chat_id')
                film_id = plan.get('film_id')
                plan_type = plan.get('plan_type')
                plan_datetime = plan.get('plan_datetime')
                user_id = plan.get('user_id')
                title = plan.get('title')
                link = plan.get('link')
            else:
                plan_id = plan[0]
                chat_id = plan[1]
                film_id = plan[2]
                plan_type = plan[3]
                plan_datetime = plan[4]
                user_id = plan[5]
                title = plan[6]
                link = plan[7]
            
            # Проверяем, что время уже наступило (или прошло не более 30 минут назад)
            if plan_datetime <= now_utc:
                try:
                    # Отправляем уведомление (plan_id передается для отметки в БД)
                    send_plan_notification(chat_id, film_id, title, link, plan_type, plan_id=plan_id)
                    logger.info(f"[PLAN CHECK] Уведомление отправлено для плана {plan_id} (фильм {title})")
                except Exception as e:
                    logger.error(f"[PLAN CHECK] Ошибка отправки уведомления для плана {plan_id}: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[PLAN CHECK] Ошибка при проверке планов: {e}", exc_info=True)

# Настройка периодического вывода статистики
scheduler.add_job(hourly_stats, 'interval', hours=1, id='hourly_stats')

# Периодическая проверка планов и отправка пропущенных уведомлений (каждые 5 минут)
scheduler.add_job(check_and_send_plan_notifications, 'interval', minutes=5, id='check_plan_notifications')

# Очистка планов
def clean_home_plans():
    """Ежедневно удаляет планы дома на вчерашний день, если по фильму нет оценок"""
    yesterday = (datetime.now(plans_tz) - timedelta(days=1)).date()
    
    with db_lock:
        # Находим планы дома на вчера (используем AT TIME ZONE для корректной работы с TIMESTAMP WITH TIME ZONE)
        cursor.execute('''
            SELECT p.id, p.film_id, p.chat_id
            FROM plans p
            WHERE p.plan_type = 'home' AND DATE(p.plan_datetime AT TIME ZONE 'Europe/Moscow') = %s
        ''', (yesterday,))
        rows = cursor.fetchall()
        
        deleted_count = 0
        for row in rows:
            # RealDictCursor возвращает словари, но поддерживает доступ по индексу
            plan_id = row.get('id') if isinstance(row, dict) else row[0]
            film_id = row.get('film_id') if isinstance(row, dict) else row[1]
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
            # Проверяем, есть ли оценки по фильму
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            count_row = cursor.fetchone()
            count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
            if count == 0:
                cursor.execute('DELETE FROM plans WHERE id = %s', (plan_id,))
                deleted_count += 1
                try:
                    bot.send_message(chat_id, f"📅 План на фильм удалён (нет оценок за вчера).")
                except:
                    pass
        
        conn.commit()
    
    logger.info(f"Очищены планы дома без оценок: {deleted_count} планов")

def clean_cinema_plans():
    """Каждый понедельник удаляет все планы кино"""
    with db_lock:
        cursor.execute("DELETE FROM plans WHERE plan_type = 'cinema'")
        deleted_count = cursor.rowcount
        conn.commit()
    
    logger.info(f"Очищены все планы кино (понедельник): {deleted_count} планов")

# Голосование для фильмов "в кино"
def start_cinema_votes():
    """Каждый понедельник в 9:00 запускает голосование для фильмов в кино"""
    now = datetime.now(plans_tz)
    if now.weekday() != 0:  # только понедельник
        return
    
    with db_lock:
        cursor.execute('''
            SELECT p.id, p.film_id, p.chat_id, m.title, m.link
            FROM plans p
            JOIN movies m ON p.film_id = m.id AND m.chat_id = p.chat_id
            WHERE p.plan_type = 'cinema' AND p.plan_datetime < NOW()
        ''')
        rows = cursor.fetchall()
        
        for row in rows:
            # RealDictCursor возвращает словари, но поддерживает доступ по индексу
            plan_id = row.get('id') if isinstance(row, dict) else row[0]
            film_id = row.get('film_id') if isinstance(row, dict) else row[1]
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[2]
            title = row.get('title') if isinstance(row, dict) else row[3]
            link = row.get('link') if isinstance(row, dict) else row[4]
            # Проверяем, есть ли оценки
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            count_row = cursor.fetchone()
            count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
            if count > 0:
                continue  # оценки есть — не запускаем голосование
            
            # Запускаем голосование
            deadline = (now.replace(hour=23, minute=59, second=59) + timedelta(days=1)).isoformat()  # конец понедельника
            
            try:
                text = f"📊 Голосование: Оставить в расписании фильм <b>{title}</b> ещё на неделю%s\n{link}\n\nОтветьте \"да\" или \"нет\" (в ответ на это сообщение)."
                msg = bot.send_message(chat_id, text, parse_mode='HTML')
                
                cursor.execute('''
                    INSERT INTO cinema_votes (chat_id, film_id, message_id, deadline)
                    VALUES (%s, %s, %s, %s)
                ''', (chat_id, film_id, msg.message_id, deadline))
                conn.commit()
            except Exception as e:
                logger.error(f"Ошибка при отправке сообщения голосования для фильма {film_id}: {e}")
    
    logger.info(f"Запущены голосования для фильмов в кино")

def resolve_cinema_votes():
    """Во вторник в 9:00 подводит итоги голосований"""
    with db_lock:
        cursor.execute('''
            SELECT chat_id, film_id, yes_users, no_users, m.title
            FROM cinema_votes v
            JOIN movies m ON v.film_id = m.id AND m.chat_id = v.chat_id
            WHERE deadline < NOW()
        ''')
        rows = cursor.fetchall()
        
        for row in rows:
            # RealDictCursor возвращает словари, но поддерживает доступ по индексу
            chat_id = row.get('chat_id') if isinstance(row, dict) else row[0]
            film_id = row.get('film_id') if isinstance(row, dict) else row[1]
            yes_json = row.get('yes_votes') if isinstance(row, dict) else row[2]
            no_json = row.get('no_votes') if isinstance(row, dict) else row[3]
            title = row.get('title') if isinstance(row, dict) else row[4]
            yes_count = len(json.loads(yes_json or '[]'))
            no_count = len(json.loads(no_json or '[]'))
            
            if no_count > yes_count or (yes_count == no_count and no_count > 0):
                cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                try:
                    bot.send_message(chat_id, f"📅 Фильм <b>{title}</b> удалён из расписания по результатам голосования.", parse_mode='HTML')
                except:
                    pass
            else:
                try:
                    bot.send_message(chat_id, f"📅 Фильм <b>{title}</b> остался в расписании на следующую неделю.", parse_mode='HTML')
                except:
                    pass
            
            cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        conn.commit()
    
    logger.info(f"Подведены итоги для {len(rows)} голосований")

# Добавляем задачи очистки и голосования в scheduler
scheduler.add_job(clean_home_plans, 'cron', hour=2, minute=0, timezone=plans_tz, id='clean_home_plans')  # каждый день в 2:00 МСК
scheduler.add_job(start_cinema_votes, 'cron', day_of_week='mon', hour=9, minute=0, timezone=plans_tz, id='start_cinema_votes')  # каждый понедельник в 9:00 МСК
scheduler.add_job(resolve_cinema_votes, 'cron', day_of_week='tue', hour=9, minute=0, timezone=plans_tz, id='resolve_cinema_votes')  # каждый вторник в 9:00 МСК

def send_rating_reminder(chat_id, film_id, film_title, user_id):
    """Отправляет напоминание пользователю об оценке фильма на следующий день после просмотра"""
    try:
        # Проверяем, не оценил ли уже пользователь
        with db_lock:
            cursor.execute("""
                SELECT id FROM ratings 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            """, (chat_id, film_id, user_id))
            has_rating = cursor.fetchone()
            
            if has_rating:
                logger.info(f"[RATING REMINDER] Пользователь {user_id} уже оценил фильм {film_id}, пропускаем")
                return
            
            # Получаем ссылку на фильм
            cursor.execute("SELECT link FROM movies WHERE id = %s", (film_id,))
            film_row = cursor.fetchone()
            link = film_row.get('link') if isinstance(film_row, dict) else (film_row[0] if film_row else None)
            
            # Отправляем напоминание
            message_text = (
                f"📅 Напоминание: вы просмотрели фильм <b>{film_title}</b> вчера.\n\n"
                f"💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку."
            )
            
            if link:
                message_text += f"\n\n{link}"
            
            msg = bot.send_message(chat_id, message_text, parse_mode='HTML')
            
            # Сохраняем связь для обработки оценки
            rating_messages[msg.message_id] = film_id
            logger.info(f"[RATING REMINDER] Напоминание отправлено user_id={user_id}, film_id={film_id}, message_id={msg.message_id}")
    except Exception as e:
        logger.error(f"[RATING REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)

def send_rating_reminder(chat_id, film_id, film_title, user_id):
    """Отправляет напоминание пользователю об оценке фильма на следующий день после просмотра"""
    try:
        # Проверяем, не оценил ли уже пользователь
        with db_lock:
            cursor.execute("""
                SELECT id FROM ratings 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            """, (chat_id, film_id, user_id))
            has_rating = cursor.fetchone()
            
            if has_rating:
                logger.info(f"[RATING REMINDER] Пользователь {user_id} уже оценил фильм {film_id}, пропускаем")
                return
            
            # Получаем ссылку на фильм
            cursor.execute("SELECT link FROM movies WHERE id = %s", (film_id,))
            film_row = cursor.fetchone()
            link = film_row.get('link') if isinstance(film_row, dict) else (film_row[0] if film_row else None)
            
            # Отправляем напоминание
            message_text = (
                f"📅 Напоминание: вы просмотрели фильм <b>{film_title}</b> вчера.\n\n"
                f"💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку."
            )
            
            if link:
                message_text += f"\n\n{link}"
            
            msg = bot.send_message(chat_id, message_text, parse_mode='HTML')
            
            # Сохраняем связь для обработки оценки
            rating_messages[msg.message_id] = film_id
            logger.info(f"[RATING REMINDER] Напоминание отправлено user_id={user_id}, film_id={film_id}, message_id={msg.message_id}")
    except Exception as e:
        logger.error(f"[RATING REMINDER] Ошибка при отправке напоминания: {e}", exc_info=True)

# Получение информации о фильме через прямой запрос к API
def extract_kp_id_from_text(text):
    """Извлекает kp_id из текста (URL или просто число)"""
    if not text:
        return None
    
    # Пытаемся найти kp_id в URL
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', text)
    if match:
        return match.group(2)
    
    # Если это просто число, возвращаем его
    match = re.search(r'^(\d+)$', text.strip())
    if match:
        return match.group(1)
    
    return None

def extract_kp_user_id(text):
    """Извлекает ID пользователя Кинопоиска из текста (ID или ссылка)"""
    import re
    # Пробуем извлечь из ссылки
    match = re.search(r'kinopoisk\.ru/user/(\d+)', text)
    if match:
        return match.group(1)
    # Пробуем извлечь просто число
    match = re.search(r'^(\d+)$', text.strip())
    if match:
        return match.group(1)
    return None

def import_kp_ratings(kp_user_id, chat_id, user_id, max_count=100):
    """Импортирует оценки из Кинопоиска"""
    headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
    base_url = f"https://kinopoiskapiunofficial.tech/api/v1/kp_users/{kp_user_id}/votes"
    
    imported_count = 0
    page = 1
    max_pages = min(75, (max_count + 19) // 20)  # Максимум 75 страниц, по 20 фильмов на странице
    
    try:
        while imported_count < max_count and page <= max_pages:
            url = f"{base_url}?page={page}"
            logger.info(f"[IMPORT] Запрос страницы {page}: {url}")
            
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                logger.error(f"[IMPORT] Ошибка {response.status_code}: {response.text[:200]}")
                break
            
            data = response.json()
            items = data.get('items', [])
            
            if not items or len(items) == 0:
                logger.info(f"[IMPORT] Нет больше фильмов на странице {page}")
                break
            
            # Обрабатываем фильмы на странице
            for item in items:
                if imported_count >= max_count:
                    break
                
                kp_id = str(item.get('kinopoiskId'))
                if not kp_id:
                    continue
                
                # Проверяем тип - только FILM
                if item.get('type') != 'FILM':
                    continue
                
                user_rating = item.get('userRating')
                if not user_rating or user_rating < 1 or user_rating > 10:
                    continue
                
                link = f"https://kinopoisk.ru/film/{kp_id}/"
                
                # Добавляем фильм в базу (если еще нет)
                try:
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        film_row = cursor.fetchone()
                        
                        if film_row:
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            logger.debug(f"[IMPORT] Фильм {kp_id} уже существует в базе, film_id={film_id}")
                        else:
                            # Фильма нет в базе - получаем полную информацию через API v2.2
                            logger.debug(f"[IMPORT] Получаем информацию о новом фильме {kp_id} через API")
                            info = None
                            
                            # Используем API v2.2 для получения полной информации
                            headers = {'X-API-KEY': KP_TOKEN}
                            api_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                            
                            try:
                                api_response = requests.get(api_url, headers=headers, timeout=10)
                                if api_response.status_code == 200:
                                    api_data = api_response.json()
                                    
                                    # Извлекаем информацию из API ответа
                                    title = api_data.get('nameRu') or api_data.get('nameOriginal') or item.get('nameRu') or item.get('nameEn') or 'Без названия'
                                    year = api_data.get('year') or item.get('year') or None
                                    
                                    # Жанры
                                    genres_list = api_data.get('genres', [])
                                    genres = ', '.join([g.get('genre', '') for g in genres_list]) if genres_list else ''
                                    
                                    # Описание
                                    description = api_data.get('description') or api_data.get('shortDescription') or ''
                                    
                                    # Режиссёр
                                    directors_list = api_data.get('directors', [])
                                    director = directors_list[0].get('nameRu') or directors_list[0].get('nameEn', '') if directors_list else 'Не указан'
                                    
                                    # Актёры
                                    actors_list = api_data.get('actors', [])[:10]  # Берем первых 10
                                    actors = ', '.join([a.get('nameRu') or a.get('nameEn', '') for a in actors_list]) if actors_list else ''
                                    
                                    # Сериал или фильм
                                    is_series = api_data.get('type') == 'TV_SERIES' or api_data.get('serial', False)
                                    
                                    info = {
                                        'title': title,
                                        'year': year or '—',
                                        'genres': genres or '—',
                                        'description': description or '—',
                                        'director': director or 'Не указан',
                                        'actors': actors or '—',
                                        'is_series': is_series
                                    }
                                    
                                    logger.info(f"[IMPORT] Получена информация о фильме {kp_id}: {title}")
                                else:
                                    logger.warning(f"[IMPORT] API v2.2 вернул {api_response.status_code} для {kp_id}")
                            except Exception as api_error:
                                logger.warning(f"[IMPORT] Ошибка при запросе API v2.2 для {kp_id}: {api_error}")
                            
                            # Если не удалось получить через API, используем базовые данные из votes
                            if not info:
                                title = item.get('nameRu') or item.get('nameEn') or 'Без названия'
                                year = item.get('year') or '—'
                                info = {
                                    'title': title,
                                    'year': year,
                                    'genres': '—',
                                    'description': '—',
                                    'director': 'Не указан',
                                    'actors': '—',
                                    'is_series': False
                                }
                                logger.info(f"[IMPORT] Используем базовые данные из votes для {kp_id}: {title}")
                            
                            # Добавляем фильм в базу
                            logger.debug(f"[IMPORT] Добавляем новый фильм {kp_id}: {info['title']}")
                            cursor.execute('''
                                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                                RETURNING id
                            ''', (chat_id, link, kp_id, info['title'], info['year'], info['genres'], 
                                  info['description'], info['director'], info['actors'], 1 if info.get('is_series') else 0))
                            film_row = cursor.fetchone()
                            if not film_row:
                                # Если RETURNING не вернул результат (может быть при конфликте), делаем SELECT
                                logger.warning(f"[IMPORT] RETURNING не вернул результат для kp_id={kp_id}, делаем SELECT")
                                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                                film_row = cursor.fetchone()
                                if not film_row:
                                    logger.error(f"[IMPORT] Не удалось получить film_id для kp_id={kp_id}")
                                    continue
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            logger.debug(f"[IMPORT] Фильм добавлен, film_id={film_id}")
                        
                        # Проверяем, есть ли уже оценка у этого пользователя для этого фильма
                        cursor.execute('''
                            SELECT rating FROM ratings 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s
                        ''', (chat_id, film_id, user_id))
                        existing_rating = cursor.fetchone()
                        
                        if existing_rating:
                            # Оценка уже есть, пропускаем
                            # Получаем название для лога
                            cursor.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                            title_row = cursor.fetchone()
                            title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                            logger.debug(f"[IMPORT] Фильм {title} уже имеет оценку, пропускаем")
                            continue
                        
                        # Добавляем оценку с пометкой is_imported = TRUE
                        cursor.execute('''
                            INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                            VALUES (%s, %s, %s, %s, TRUE)
                            ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = TRUE
                        ''', (chat_id, film_id, user_id, user_rating))
                        conn.commit()
                        
                        imported_count += 1
                        logger.info(f"[IMPORT] Импортирован фильм {info['title']} с оценкой {user_rating}")
                except Exception as db_error:
                    logger.error(f"[IMPORT] Ошибка при работе с БД для фильма {kp_id}: {db_error}", exc_info=True)
                    continue
            
            # Если получили меньше 20 фильмов, значит страницы закончились
            if len(items) < 20:
                logger.info(f"[IMPORT] Получено меньше 20 фильмов, заканчиваем")
                break
            
            page += 1
        
        return imported_count
    except Exception as e:
        logger.error(f"[IMPORT] Ошибка при импорте: {e}", exc_info=True)
        return imported_count

def handle_import_user_id_internal(message, state):
    """Обрабатывает ввод user_id для импорта"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    kp_user_id = extract_kp_user_id(text)
    
    if not kp_user_id:
        bot.reply_to(message, "❌ Не удалось извлечь ID пользователя. Отправьте ID или ссылку на профиль Кинопоиска.")
        return
    
    state['kp_user_id'] = kp_user_id
    state['step'] = 'waiting_count'
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("50", callback_data=f"import_count:50"))
    markup.add(InlineKeyboardButton("100", callback_data=f"import_count:100"))
    markup.add(InlineKeyboardButton("300", callback_data=f"import_count:300"))
    markup.add(InlineKeyboardButton("500", callback_data=f"import_count:500"))
    markup.add(InlineKeyboardButton("1000", callback_data=f"import_count:1000"))
    markup.add(InlineKeyboardButton("1500", callback_data=f"import_count:1500"))
    
    bot.reply_to(message, 
        f"✅ ID пользователя: <code>{kp_user_id}</code>\n\n"
        f"Сколько фильмов загрузить?",
        reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("import_count:"))
def handle_import_count_callback(call):
    """Обработчик выбора количества фильмов для импорта"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        count = int(call.data.split(":")[1])
        
        if user_id not in user_import_state:
            bot.answer_callback_query(call.id, "❌ Состояние импорта потеряно", show_alert=True)
            return
        
        state = user_import_state[user_id]
        kp_user_id = state.get('kp_user_id')
        
        if not kp_user_id:
            bot.answer_callback_query(call.id, "❌ ID пользователя не найден", show_alert=True)
            return
        
        bot.answer_callback_query(call.id, f"⏳ Начинаю импорт {count} фильмов...")
        status_msg = bot.edit_message_text(
            f"📥 <b>Импорт базы из Кинопоиска</b>\n\n"
            f"ID пользователя: <code>{kp_user_id}</code>\n"
            f"Количество: {count}\n\n"
            f"⏳ Импорт начат в фоновом режиме, это может занять некоторое время...\n"
            f"Вы получите уведомление по завершении.",
            chat_id, call.message.message_id, parse_mode='HTML'
        )
        
        # Удаляем состояние
        del user_import_state[user_id]
        
        # Запускаем импорт в фоновом потоке
        def background_import():
            try:
                imported = import_kp_ratings(kp_user_id, chat_id, user_id, count)
                
                # Отправляем результат
                bot.edit_message_text(
                    f"✅ <b>Импорт завершён!</b>\n\n"
                    f"ID пользователя: <code>{kp_user_id}</code>\n"
                    f"Загружено новых оценок: <b>{imported}</b>\n\n"
                    f"Оценки загружены в базу! 🎉",
                    chat_id, status_msg.message_id, parse_mode='HTML'
                )
                
                logger.info(f"[IMPORT] Импорт завершён для user_id={user_id}, kp_user_id={kp_user_id}, imported={imported}")
            except Exception as e:
                logger.error(f"[IMPORT] Ошибка в фоновом импорте: {e}", exc_info=True)
                try:
                    bot.edit_message_text(
                        f"❌ <b>Ошибка при импорте</b>\n\n"
                        f"Произошла ошибка: {str(e)[:200]}",
                        chat_id, status_msg.message_id, parse_mode='HTML'
                    )
                except:
                    pass
        
        # Запускаем в отдельном потоке
        import_thread = threading.Thread(target=background_import, daemon=True)
        import_thread.start()
    except Exception as e:
        logger.error(f"[IMPORT] Ошибка в handle_import_count_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при импорте", show_alert=True)
        except:
            pass

def extract_movie_info(link):
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
    if not match:
        logger.warning(f"Не распознана ссылка: {link}")
        return None
    kp_id = match.group(2)
    is_series = match.group(1) == 'series'  # Определяем, сериал это или фильм

    headers = {
        'X-API-KEY': KP_TOKEN,
        'Content-Type': 'application/json'
    }

    try:
        # Основные данные (название, год, жанры, описание)
        url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
        response_main = requests.get(url_main, headers=headers, timeout=15)
        if response_main.status_code != 200:
            logger.error(f"Основной запрос ошибка {response_main.status_code}")
            return None
        data_main = response_main.json()

        title = data_main.get('nameRu') or data_main.get('nameOriginal') or "Unknown"
        year = data_main.get('year') or "—"
        genres = ', '.join([g['genre'] for g in data_main.get('genres', [])]) or "—"
        description = data_main.get('description') or data_main.get('shortDescription') or "Нет описания"

        # Отдельный запрос на staff (режиссёр и актёры)
        # Используем v1 endpoint как основной, так как v2.2 не работает
        url_staff = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kp_id}"
        logger.debug(f"Staff запрос URL: {url_staff}")
        response_staff = requests.get(url_staff, headers=headers, timeout=15)
        staff = []
        if response_staff.status_code == 200:
            staff = response_staff.json()
            logger.debug(f"Staff ответ получен, количество записей: {len(staff) if isinstance(staff, list) else 'не список'}")
        else:
            logger.warning(f"Staff запрос ошибка {response_staff.status_code} — режиссёр/актёры не загружены")
            logger.warning(f"Staff ответ: {response_staff.text[:200] if response_staff.text else 'нет текста'}")

        # Режиссёр
        director = "Не указан"
        if staff and len(staff) > 0:
            # Логируем структуру первого элемента для отладки
            logger.debug(f"Пример структуры staff элемента: {list(staff[0].keys()) if isinstance(staff[0], dict) else 'не словарь'}")
        
        for person in staff:
            if not isinstance(person, dict):
                continue
            # Проверяем разные варианты полей для профессии
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('DIRECTOR' in str(profession).upper() or 'РЕЖИССЕР' in str(profession).upper() or profession == 'DIRECTOR'):
                # Проверяем разные варианты полей для имени
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    director = name
                    break

        # Актёры (top 6)
        actors_list = []
        for person in staff:
            if not isinstance(person, dict):
                continue
            # Проверяем разные варианты полей для профессии
            profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
            if profession and ('ACTOR' in str(profession).upper() or 'АКТЕР' in str(profession).upper() or profession == 'ACTOR') and len(actors_list) < 6:
                # Проверяем разные варианты полей для имени
                name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                if name:
                    actors_list.append(name)
        actors = ', '.join(actors_list) if actors_list else "—"

        logger.info(f"Успешно: {title} ({year}), режиссёр: {director}, актёры: {actors}")

        return {
            'kp_id': kp_id,
            'title': title,
            'year': year,
            'genres': genres,
            'director': director,
            'actors': actors,
            'description': description,
            'is_series': is_series
        }
    except Exception as e:
        logger.error(f"Ошибка получения данных для {kp_id}: {e}")
        return None

# ==================== ФУНКЦИИ API KINOPOISK ====================

def get_facts(kp_id):
    """Получает интересные факты о фильме"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/facts"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            facts = data.get('items', [])
            if facts:
                # Разделяем факты на Факты и Ошибки
                facts_list = []
                bloopers_list = []
                
                for fact in facts:
                    fact_text = fact.get('text', '').strip()
                    fact_type = fact.get('type', '')
                    if fact_text:
                        # Исправляем HTML-сущности
                        fact_text = fact_text.replace('&laquo;', '«').replace('&raquo;', '»').replace('&quot;', '"').replace('&amp;', '&')
                        if fact_type == 'FACT':
                            facts_list.append((fact_type, fact_text))
                        elif fact_type == 'BLOOPER':
                            bloopers_list.append((fact_type, fact_text))
                
                text = "🤔 <b>Интересные факты о фильме:</b>\n\n"
                
                # Сначала Факты
                if facts_list:
                    for fact_type, fact_text in facts_list[:3]:  # Максимум 3 факта
                        text += f"• <b>Факты:</b> {fact_text}\n\n"
                
                # Потом Ошибки
                if bloopers_list:
                    for fact_type, fact_text in bloopers_list[:3]:  # Максимум 3 блупера
                        text += f"• <b>Ошибки:</b> {fact_text}\n\n"
                
                return text if (facts_list or bloopers_list) else None
            else:
                return None
        else:
            logger.error(f"Ошибка get_facts: {response.status_code}")
            return None
    except Exception as e:
        logger.error(f"Ошибка get_facts: {e}", exc_info=True)
        return None

def get_seasons(kp_id, chat_id=None, user_id=None):
    """Получает информацию о сезонах сериала с отметками просмотренных"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    # Пробуем сначала v2.2, если не работает - v2.1
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/seasons"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            seasons = data.get('items', [])
            if seasons:
                # Получаем информацию о просмотренных сериях
                watched_episodes = set()
                if chat_id and user_id:
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                            cursor.execute('''
                                SELECT season_number, episode_number 
                                FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id))
                            watched_rows = cursor.fetchall()
                            for w_row in watched_rows:
                                if isinstance(w_row, dict):
                                    watched_episodes.add((w_row.get('season_number'), w_row.get('episode_number')))
                                else:
                                    watched_episodes.add((w_row[0], w_row[1]))
                
                from datetime import datetime as dt
                now = dt.now()
                
                # Получаем информацию о выходе серий
                next_episode = None
                next_episode_date = None
                is_airing = False
                
                for season in seasons:
                    episodes = season.get('episodes', [])
                    for ep in episodes:
                        release_str = ep.get('releaseDate', '')
                        if release_str and release_str != '—':
                            try:
                                # Пробуем разные форматы даты
                                release_date = None
                                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                    try:
                                        release_date = dt.strptime(release_str.split('T')[0], fmt)
                                        break
                                    except:
                                        continue
                                
                                if release_date and release_date > now:
                                    if not next_episode_date or release_date < next_episode_date:
                                        next_episode_date = release_date
                                        next_episode = {
                                            'season': season.get('number', ''),
                                            'episode': ep.get('episodeNumber', ''),
                                            'date': release_date
                                        }
                                        is_airing = True
                            except:
                                pass
                
                # Подсчитываем просмотренные сезоны
                season_stats = {}
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    watched_in_season = sum(1 for ep in episodes if (number, str(ep.get('episodeNumber', ''))) in watched_episodes)
                    total_in_season = len(episodes)
                    season_stats[number] = {'watched': watched_in_season, 'total': total_in_season}
                
                text = "📺 <b>Сезоны сериала:</b>\n\n"
                for season in seasons:
                    number = season.get('number', '')
                    episodes = season.get('episodes', [])
                    stats = season_stats.get(number, {'watched': 0, 'total': len(episodes)})
                    
                    # Определяем статус сезона
                    if stats['watched'] == stats['total'] and stats['total'] > 0:
                        status = "✅ Просмотрен полностью"
                    elif stats['watched'] > 0:
                        status = f"⏳ Просмотрено {stats['watched']}/{stats['total']}"
                    else:
                        status = "⬜ Не просмотрен"
                    
                    text += f"<b>Сезон {number}</b> ({stats['total']} серий) — {status}\n"
                
                text += "\n"
                
                # Информация о выходе серий
                if is_airing and next_episode:
                    text += f"🟢 <b>Сериал выходит сейчас</b>\n"
                    text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n\n"
                else:
                    text += f"🔴 <b>Сериал не выходит</b>\n\n"
                
                return text
            else:
                return None
        elif response.status_code == 400:
            # Пробуем v2.1 если v2.2 не работает
            logger.warning(f"Ошибка 400 для v2.2, пробуем v2.1 для kp_id={kp_id}")
            url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{kp_id}/seasons"
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                seasons = data.get('items', [])
                if seasons:
                    # Получаем информацию о просмотренных сериях
                    watched_episodes = set()
                    if chat_id and user_id:
                        with db_lock:
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                            row = cursor.fetchone()
                            if row:
                                film_id = row.get('id') if isinstance(row, dict) else row[0]
                                cursor.execute('''
                                    SELECT season_number, episode_number 
                                    FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                                ''', (chat_id, film_id, user_id))
                                watched_rows = cursor.fetchall()
                                for w_row in watched_rows:
                                    if isinstance(w_row, dict):
                                        watched_episodes.add((w_row.get('season_number'), w_row.get('episode_number')))
                                    else:
                                        watched_episodes.add((w_row[0], w_row[1]))
                    
                    from datetime import datetime as dt
                    now = dt.now()
                    
                    # Получаем информацию о выходе серий
                    next_episode = None
                    next_episode_date = None
                    is_airing = False
                    
                    for season in seasons:
                        episodes = season.get('episodes', [])
                        for ep in episodes:
                            release_str = ep.get('releaseDate', '')
                            if release_str and release_str != '—':
                                try:
                                    # Пробуем разные форматы даты
                                    release_date = None
                                    for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                        try:
                                            release_date = dt.strptime(release_str.split('T')[0], fmt)
                                            break
                                        except:
                                            continue
                                    
                                    if release_date and release_date > now:
                                        if not next_episode_date or release_date < next_episode_date:
                                            next_episode_date = release_date
                                            next_episode = {
                                                'season': season.get('number', ''),
                                                'episode': ep.get('episodeNumber', ''),
                                                'date': release_date
                                            }
                                            is_airing = True
                                except:
                                    pass
                    
                    # Подсчитываем просмотренные сезоны
                    season_stats = {}
                    for season in seasons:
                        number = season.get('number', '')
                        episodes = season.get('episodes', [])
                        watched_in_season = sum(1 for ep in episodes if (number, str(ep.get('episodeNumber', ''))) in watched_episodes)
                        total_in_season = len(episodes)
                        season_stats[number] = {'watched': watched_in_season, 'total': total_in_season}
                    
                    text = "📺 <b>Сезоны сериала:</b>\n\n"
                    for season in seasons:
                        number = season.get('number', '')
                        episodes = season.get('episodes', [])
                        stats = season_stats.get(number, {'watched': 0, 'total': len(episodes)})
                        
                        # Определяем статус сезона
                        if stats['watched'] == stats['total'] and stats['total'] > 0:
                            status = "✅ Просмотрен полностью"
                        elif stats['watched'] > 0:
                            status = f"⏳ Просмотрено {stats['watched']}/{stats['total']}"
                        else:
                            status = "⬜ Не просмотрен"
                        
                        text += f"<b>Сезон {number}</b> ({stats['total']} серий) — {status}\n"
                    
                    text += "\n"
                    
                    # Информация о выходе серий
                    if is_airing and next_episode:
                        text += f"🟢 <b>Сериал выходит сейчас</b>\n"
                        text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n\n"
                    else:
                        text += f"🔴 <b>Сериал не выходит</b>\n\n"
                    
                    return text
                else:
                    return None
            else:
                logger.error(f"Ошибка get_seasons (v2.1): {response.status_code}, response: {response.text[:200]}")
                return None
        else:
            logger.error(f"Ошибка get_seasons: {response.status_code}, response: {response.text[:200]}")
            return None
    except Exception as e:
        logger.error(f"Ошибка get_seasons: {e}", exc_info=True)
        return None

def get_seasons_data(kp_id):
    """Получает данные о сезонах сериала (возвращает список сезонов)"""
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    # Пробуем сначала v2.2, если не работает - v2.1
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/seasons"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            return data.get('items', [])
        elif response.status_code == 400:
            # Пробуем v2.1 если v2.2 не работает
            url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/{kp_id}/seasons"
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                return data.get('items', [])
            else:
                logger.error(f"Ошибка get_seasons_data (v2.1): {response.status_code}, response: {response.text[:200]}")
                return []
        else:
            logger.error(f"Ошибка get_seasons_data: {response.status_code}, response: {response.text[:200]}")
            return []
    except Exception as e:
        logger.error(f"Ошибка get_seasons_data: {e}", exc_info=True)
        return []

def get_similars(kp_id):
    """Получает похожие фильмы"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/similars"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            similars = data.get('items', [])
            return [(s.get('filmId'), s.get('nameRu') or s.get('nameEn', 'Без названия')) for s in similars[:5]]
        return []
    except Exception as e:
        logger.error(f"Ошибка get_similars: {e}", exc_info=True)
        return []

def get_sequels(kp_id):
    """Получает продолжения и приквелы"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/sequels_and_prequels"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            sequels = data.get('items', [])
            return [(s.get('filmId'), s.get('nameRu') or s.get('nameEn', 'Без названия')) for s in sequels[:5]]
        return []
    except Exception as e:
        logger.error(f"Ошибка get_sequels: {e}", exc_info=True)
        return []

def get_external_sources(kp_id):
    """Получает внешние источники для просмотра фильма"""
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/external_sources"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            sources = data.get('items', [])
            links = []
            for s in sources:
                if s.get('url'):
                    platform = s.get('platform', 'Смотреть')
                    links.append((platform, s['url']))
            return links
        return []
    except Exception as e:
        logger.error(f"Ошибка get_external_sources: {e}", exc_info=True)
        return []

def get_premieres_for_period(period_type='current_month'):
    """Получает список премьер для указанного периода"""
    now = datetime.now()
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    
    all_premieres = []
    
    if period_type == 'current_month':
        # Текущий месяц
        months = [(now.year, now.month)]
    elif period_type == 'next_month':
        # Следующий месяц
        next_month = now.month + 1
        next_year = now.year
        if next_month > 12:
            next_month = 1
            next_year += 1
        months = [(next_year, next_month)]
    elif period_type == '3_months':
        # 3 месяца
        months = []
        for i in range(3):
            month = now.month + i
            year = now.year
            while month > 12:
                month -= 12
                year += 1
            months.append((year, month))
    elif period_type == '6_months':
        # 6 месяцев
        months = []
        for i in range(6):
            month = now.month + i
            year = now.year
            while month > 12:
                month -= 12
                year += 1
            months.append((year, month))
    elif period_type == 'current_year':
        # Текущий год (до 31 декабря)
        months = [(now.year, m) for m in range(now.month, 13)]
    elif period_type == 'next_year':
        # Ближайший год (следующий год полностью)
        months = [(now.year + 1, m) for m in range(1, 13)]
    else:
        months = [(now.year, now.month)]
    
    # Получаем премьеры для каждого месяца
    # API требует месяц в формате JANUARY, FEBRUARY и т.д. для v2.2
    month_names = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
                   'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
    
    for year, month in months:
        month_name = month_names[month - 1] if 1 <= month <= 12 else 'JANUARY'
        urls_to_try = [
            # v2.2 требует название месяца
            f"https://kinopoiskapiunofficial.tech/api/v2.2/films/premieres?year={year}&month={month_name}",
            # v2.1 может принимать число
            f"https://kinopoiskapiunofficial.tech/api/v2.1/films/premieres?year={year}&month={month}",
        ]
        
        for url in urls_to_try:
            try:
                logger.info(f"[PREMIERES] Запрос к API: {url}")
                response = requests.get(url, headers=headers, timeout=15)
                logger.info(f"[PREMIERES] Статус ответа: {response.status_code}")
                
                if response.status_code == 200:
                    data = response.json()
                    premieres = data.get('releases', []) or data.get('items', []) or data.get('premieres', [])
                    if premieres:
                        logger.info(f"[PREMIERES] Получено премьер для {year}-{month:02d}: {len(premieres)}")
                        all_premieres.extend(premieres)
                        break  # Успешно получили, переходим к следующему месяцу
                elif response.status_code != 400:
                    logger.warning(f"[PREMIERES] Ошибка {response.status_code} для {url}: {response.text[:200]}")
                    continue
                else:
                    logger.warning(f"[PREMIERES] Ошибка 400 для {url}: {response.text[:200]}")
                    continue
            except Exception as e:
                logger.warning(f"[PREMIERES] Ошибка при запросе {url}: {e}")
                continue
    
    # Убираем дубликаты по kinopoiskId
    seen_ids = set()
    unique_premieres = []
    for p in all_premieres:
        kp_id = p.get('kinopoiskId') or p.get('filmId')
        if kp_id and kp_id not in seen_ids:
            seen_ids.add(kp_id)
            unique_premieres.append(p)
    
    logger.info(f"[PREMIERES] Всего уникальных премьер: {len(unique_premieres)}")
    return unique_premieres

def get_premieres(year=None, month=None):
    """Получает список премьер на указанный месяц (старая функция для обратной совместимости)"""
    if not year:
        year = datetime.now().year
    if not month:
        month = datetime.now().month
    
    headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/premieres?year={year}&month={month}"
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            premieres = data.get('releases', []) or data.get('items', []) or data.get('premieres', [])
            return premieres
    except Exception as e:
        logger.error(f"[PREMIERES] Ошибка: {e}")
    
        return []

# Новая функция для поиска фильмов через API
def search_films(query, page=1):
    """Поиск фильмов через Kinopoisk API"""
    if not KP_TOKEN:
        logger.error("[SEARCH] KP_TOKEN не установлен")
        return [], 0
    
    # Используем правильный endpoint для поиска
    url = f"https://kinopoiskapiunofficial.tech/api/v2.1/films/search-by-keyword"
    params = {"keyword": query, "page": page}
    headers = {
        "X-API-KEY": KP_TOKEN,
        "accept": "application/json"
    }
    
    logger.info(f"[SEARCH] Запрос: query='{query}', page={page}, url={url}")
    
    try:
        response = requests.get(url, params=params, headers=headers, timeout=15)
        logger.info(f"[SEARCH] Статус ответа: {response.status_code}")
        logger.info(f"[SEARCH] URL запроса: {response.url}")
        
        if response.status_code != 200:
            logger.error(f"[SEARCH] Ошибка API: статус {response.status_code}, ответ: {response.text[:500]}")
            return [], 0
        
        data = response.json()
        items = data.get("films", []) or data.get("items", [])
        total_pages = data.get("totalPages", 1) or data.get("pagesCount", 1)
        logger.info(f"[SEARCH] Найдено результатов: {len(items)}, всего страниц: {total_pages}")
        
        # Логируем структуру первого элемента для отладки
        if items and len(items) > 0:
            first_item = items[0]
            logger.info(f"[SEARCH] Структура первого элемента: {list(first_item.keys()) if isinstance(first_item, dict) else 'не словарь'}")
            logger.info(f"[SEARCH] Пример данных: nameRu={first_item.get('nameRu')}, nameEn={first_item.get('nameEn')}, kinopoiskId={first_item.get('kinopoiskId')}, filmId={first_item.get('filmId')}")
        
        return items, total_pages
    except requests.exceptions.RequestException as e:
        logger.error(f"[SEARCH] Ошибка запроса: {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"[SEARCH] Ответ сервера: {e.response.text[:500]}")
        return [], 0
    except Exception as e:
        logger.error(f"[SEARCH] Неожиданная ошибка: {e}", exc_info=True)
        return [], 0

# Добавление и анонс
def add_and_announce(link, chat_id):
    info = extract_movie_info(link)
    if not info:
        logger.warning(f"Не удалось извлечь информацию о фильме: {link}")
        return False
    
    duplicate_data = None  # Для хранения данных о дубликате, найденном во второй проверке

    # Проверяем, существует ли уже фильм в этом чате по kp_id (не по ссылке, так как ссылки могут отличаться)
    kp_id = info.get('kp_id')
    logger.info(f"[DUPLICATE CHECK] Проверяем фильм kp_id={kp_id}, title={info.get('title')}, chat_id={chat_id}")
    with db_lock:
        cursor.execute('SELECT id, title, watched, rating FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        existing = cursor.fetchone()
    
    if existing:
        # RealDictCursor возвращает словари, но поддерживает доступ по индексу
        film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
        existing_title = existing.get('title') if isinstance(existing, dict) else existing[1]
        watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
        
        logger.info(f"[DUPLICATE FOUND] Фильм уже в базе: id={film_id}, title={existing_title}, watched={watched}")
        
        # Фильм уже есть в базе
        text = f"🎞️ <b>Уже добавлено ранее в базу!</b>\n\n"
        text += f"<b>{existing_title}</b>\n"
        
        # Если фильм просмотрен, рассчитываем среднее из ratings (внутри db_lock)
        if watched:
            with db_lock:
                cursor.execute('SELECT AVG(rating) as avg FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                avg_result = cursor.fetchone()
                if avg_result:
                    avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                    # Проверяем, что avg не None
                    avg = float(avg) if avg is not None else None
                else:
                    avg = None
            
            text += f"\n✅ <b>Просмотрено</b>\n"
            if avg:
                text += f"⭐ <b>Средняя оценка: {avg:.1f}/10</b>\n"
            else:
                text += f"⭐ <b>Оценка не указана</b>\n"
        else:
            text += f"\n⏳ <b>Ещё не просмотрено</b>\n"
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        try:
            logger.info(f"[DUPLICATE] Отправляем сообщение о дубликате в чат {chat_id}")
            msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
            # Сохраняем ссылку в bot_messages для обработки реакций
            if msg and msg.message_id:
                bot_messages[msg.message_id] = link
                logger.info(f"[DUPLICATE] Ссылка сохранена в bot_messages для message_id={msg.message_id}: {link}")
            logger.info(f"✅ Сообщение отправлено: фильм уже в базе - {existing_title}")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения (фильм уже в базе): {e}", exc_info=True)
        return False
    
    # Новый фильм - добавляем
    inserted = False
    try:
        with db_lock:
            # Проверяем, не в состоянии ли ошибки транзакция
            try:
                cursor.execute('SELECT 1')
                cursor.fetchone()
            except:
                # Если транзакция в состоянии ошибки, откатываем
                conn.rollback()
                logger.debug("Транзакция была в состоянии ошибки, выполнен rollback")
            
            # Проверяем, существует ли фильм до вставки
            cursor.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
            existing_row = cursor.fetchone()
            exists_before = existing_row is not None
            
            if exists_before:
                logger.info(f"[DUPLICATE CHECK 2] Фильм с kp_id={info['kp_id']} уже существует в базе, отправляем сообщение о дубликате")
                # Получаем данные о существующем фильме
                film_id = existing_row.get('id') if isinstance(existing_row, dict) else existing_row[0]
                existing_title = existing_row.get('title') if isinstance(existing_row, dict) else existing_row[1]
                watched = existing_row.get('watched') if isinstance(existing_row, dict) else existing_row[2]
                
                # Получаем среднюю оценку, если фильм просмотрен
                avg = None
                if watched:
                    cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                    avg_result = cursor.fetchone()
                    avg = avg_result[0] if avg_result and avg_result[0] else None
                
                # Сохраняем данные для отправки сообщения после выхода из db_lock
                duplicate_data = {
                    'title': existing_title,
                    'watched': watched,
                    'avg': avg,
                    'link': link
                }
                inserted = False
            else:
                duplicate_data = None
                try:
                    cursor.execute('''
                        INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                    ''', (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors'], 1 if info.get('is_series') else 0))
                    conn.commit()
                    inserted = True
                    logger.info(f"Фильм успешно добавлен в БД: kp_id={info['kp_id']}, title={info['title']}")
                except Exception as db_error:
                    conn.rollback()
                    logger.error(f"Ошибка при добавлении фильма в БД: {db_error}", exc_info=True)
                    inserted = False
    except Exception as e:
        logger.error(f"Критическая ошибка при работе с БД: {e}", exc_info=True)
        try:
            with db_lock:
                conn.rollback()
        except:
            pass
        inserted = False
        duplicate_data = None
    
    logger.info(f"Результат вставки: inserted={inserted}, title={info['title']}")
    
    # Если фильм был найден как дубликат во второй проверке, отправляем сообщение
    if not inserted and duplicate_data:
        text = f"🎞️ <b>Уже добавлено ранее в базу!</b>\n\n"
        text += f"<b>{duplicate_data['title']}</b>\n"
        
        if duplicate_data['watched']:
            text += f"\n✅ <b>Просмотрено</b>\n"
            if duplicate_data['avg']:
                text += f"⭐ <b>Средняя оценка: {duplicate_data['avg']:.1f}/10</b>\n"
            else:
                text += f"⭐ <b>Оценка не указана</b>\n"
        else:
            text += f"\n⏳ <b>Ещё не просмотрено</b>\n"
        
        text += f"\n<a href='{duplicate_data['link']}'>Кинопоиск</a>"
        
        try:
            logger.info(f"[DUPLICATE] Отправляем сообщение о дубликате (вторая проверка) в чат {chat_id}")
            msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
            # Сохраняем ссылку в bot_messages для обработки реакций
            if msg and msg.message_id:
                bot_messages[msg.message_id] = duplicate_data['link']
                logger.info(f"[DUPLICATE] Ссылка сохранена в bot_messages для message_id={msg.message_id}: {duplicate_data['link']}")
            logger.info(f"✅ Сообщение отправлено: фильм уже в базе - {duplicate_data['title']}")
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения о дубликате: {e}", exc_info=True)
        return False
    
    if inserted:
        # Только если реально добавили в БД — отправляем сообщение и сохраняем message_id
        text = f"🎬 <b>Добавлено в базу!</b>\n\n"
        text += f"<b>{info['title']}</b> ({info['year'] or '—'})\n"
        text += f"<i>Режиссёр:</i> {info['director']}\n"
        text += f"<i>Жанры:</i> {info['genres']}\n"
        text += f"<i>В ролях:</i> {info['actors']}\n\n"
        text += f"<i>Кратко:</i> {info['description']}\n\n"
        text += f"<a href='{link}'>Кинопоиск</a>"
        
        # Создаем кнопку "Запланировать просмотр"
        markup = InlineKeyboardMarkup()
        kp_id = info.get('kp_id')
        if kp_id:
            # Используем kp_id для callback_data (короче, чем полная ссылка)
            markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
        
        try:
            logger.info(f"Отправляем сообщение в чат {chat_id}")
            msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
            # Только если сообщение отправлено успешно и фильм добавлен в БД — сохраняем для реакций
            bot_messages[msg.message_id] = link
            logger.info(f"✅ Сообщение успешно отправлено! Новый фильм добавлен: {info['title']}, message_id={msg.message_id}")
            
            # Если это сериал, показываем сезоны и предлагаем отметить просмотренные
            if info.get('is_series'):
                seasons_text = get_seasons(info['kp_id'], chat_id, None)
                if seasons_text:
                    bot.send_message(chat_id, seasons_text, parse_mode='HTML')
                    
                    # Предлагаем отметить сезоны/серии как просмотренные
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("✅ Отметить сезоны/серии", callback_data=f"series_track:{info['kp_id']}"))
                    markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{info['kp_id']}"))
                    bot.send_message(chat_id, "📺 Что хотите сделать с сериалом?", reply_markup=markup)
            
            return True
        except Exception as e:
            logger.error(f"❌ Ошибка при отправке сообщения: {e}", exc_info=True)
            return False
    else:
        # Фильм не был добавлен в БД — отправляем предупреждение
        try:
            bot.send_message(chat_id, "⚠️ Карточка не отправлена, фильм НЕ сохранён в базу из-за ошибки. Проверь логи.")
            logger.warning(f"Фильм не был вставлен в БД, отправлено предупреждение пользователю")
        except Exception as e:
            logger.error(f"Ошибка при отправке предупреждения: {e}", exc_info=True)
    return False

# /start — приветственное сообщение
# Логируем регистрацию обработчика
logger.info("[WEB APP] Регистрируем обработчик web_app_data")

@bot.message_handler(content_types=['web_app_data'])
def handle_web_app_data(message):
    logger.info(f"[WEB APP] Получены данные от Web App: {message.web_app_data.data}")
    
    try:
        data = json.loads(message.web_app_data.data)
        command = data.get('command')
        
        if not command:
            logger.warning("[WEB APP] Нет команды в данных")
            return
        
        logger.info(f"[WEB APP] Выполняем команду: /{command}")
        
        # Создаём фейковое сообщение для команды
        fake_message = telebot.types.Message()
        fake_message.text = f'/{command}'
        fake_message.from_user = message.from_user
        fake_message.chat = message.chat
        fake_message.message_id = message.message_id  # Для реплаев
        fake_message.date = message.date
        
        # Вызываем хэндлер команды
        if command == 'random':
            random_start(fake_message)
        elif command == 'premieres':
            premieres_command(fake_message)
        elif command == 'list':
            list_movies(fake_message)
        elif command == 'schedule':
            show_schedule(fake_message)
        elif command == 'plan':
            plan_handler(fake_message)
        elif command == 'ticket':
            ticket_command(fake_message)
        elif command == 'seasons':
            seasons_command(fake_message)
        elif command == 'total':
            total_stats(fake_message)
        elif command == 'stats':
            stats_command(fake_message)
        elif command == 'rate':
            rate_movie(fake_message)
        elif command == 'settings':
            settings_command(fake_message)
        elif command == 'start':
            send_welcome(fake_message)
        elif command == 'help':
            help_command(fake_message)
        elif command == 'clean':
            clean_command(fake_message)
        elif command == 'search':
            handle_search(fake_message)
        else:
            bot.send_message(chat_id=message.chat.id, text=f"Неизвестная команда: {command}")
    except json.JSONDecodeError:
        logger.error("[WEB APP] Не удалось распарсить JSON")
    except Exception as e:
        logger.error(f"[WEB APP] Ошибка: {e}", exc_info=True)
        bot.send_message(chat_id=message.chat.id, text="Произошла ошибка в Web App. Попробуйте заново.")

@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"[HANDLER] /start вызван от {message.from_user.id}, chat_type={message.chat.type}, text='{message.text}'")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/start', message.chat.id)
    logger.info(f"Команда /start от пользователя {message.from_user.id}")
    
    emoji = get_watched_emoji(message.chat.id)  # Берёт актуальный эмодзи из настроек

    # Разные приветствия для личных сообщений и групп
    if message.chat.type == 'private':
        welcome_text = f"""
🎬 <b>Добро пожаловать в MovieBot!</b>

Этот бот помогает собирать фильмы, отмечать просмотренные, планировать просмотр и выбирать, что посмотреть следующим.

<b>Как это работает:</b>
• Отправляйте ссылки на фильмы/сериалы с Кинопоиска
• Бот сразу добавит фильм в базу и покажет карточку с информацией

• Когда посмотрели — поставьте на сообщение со ссылкой эмодзи {emoji}  
  Бот поздравит и попросит оценку от 1 до 10

<b>Основные команды:</b>
/list — список непросмотренных фильмов
/random — рандомный непросмотренный фильм с фильтрами
/plan — запланировать просмотр дома или в кино
/schedule — список запланированных просмотров
/total — статистика: фильмы, жанры, режиссёры, актёры и оценки
/stats — детальная статистика
/rate — оценить просмотренные фильмы
/settings — настройки: эмодзи, часовой пояс, загрузка голосов
/join — присоединиться к группе (для статистики)

<b>Сериалы:</b>
/seasons — просмотр сезонов и серий, отметка просмотренных эпизодов

<b>Премьеры:</b>
/premieres — список премьер с выбором периода, постеры и трейлеры, напоминания о выходе

<b>Билеты в кино:</b>
/ticket — прикрепить билет к запланированному просмотру в кино

Просто отправляйте ссылки и пользуйтесь командами! 🍿

Если нужно больше деталей — /help
        """.strip()
    else:
        welcome_text = f"""
🎬 <b>Добро пожаловать в MovieBot — ваш групповой планировщик кино!</b>

Этот бот помогает друзьям собирать фильмы, отмечать просмотренные, планировать просмотр и выбирать, что посмотреть следующим.

<b>Как это работает:</b>
• Кидайте в чат ссылки на фильмы/сериалы с Кинопоиска
• Бот сразу добавит фильм в базу и покажет красивую карточку с названием, годом, жанрами, режиссёром, актёрами и описанием

• Когда посмотрели — поставьте на сообщение со ссылкой эмодзи {emoji}  
  Бот поздравит и попросит оценку от 1 до 10

<b>Основные команды:</b>
/list — список непросмотренных фильмов
/random — рандомный непросмотренный фильм с фильтрами (год, жанр, режиссёр — можно пропустить)
/plan — запланировать просмотр дома или в кино (с напоминанием)
/schedule — список запланированных просмотров
/total — статистика группы: сколько посмотрели, любимые жанры, режиссёры, актёры и оценки
/stats — детальная статистика группы и участников
/rate — дооценить просмотренные фильмы
/settings — настройки: эмодзи, часовой пояс, загрузка голосов

<b>Сериалы:</b>
/seasons — просмотр сезонов и серий, отметка просмотренных эпизодов

<b>Премьеры:</b>
/premieres — список премьер с выбором периода, постеры и трейлеры, напоминания о выходе

<b>Билеты в кино:</b>
/ticket — прикрепить билет к запланированному просмотру в кино

Просто кидайте ссылки и пользуйтесь командами — бот всё запомнит и сделает кино-вечера идеальными! 🍿

Если нужно больше деталей — /help
        """.strip()

    try:
        bot.reply_to(message, welcome_text, parse_mode='HTML')
        logger.info(f"✅ Ответ на /start отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке ответа на /start: {e}", exc_info=True)

# Реакции + сбор оценок
rating_messages = {}  # message_id: film_id (связь сообщения о просмотренном фильме с film_id)
rate_list_messages = {}  # chat_id: message_id (сообщение со списком фильмов для /rate)

@bot.message_reaction_handler(func=lambda r: True)
def handle_reaction(reaction):
    logger.info(f"[REACTION] Получена реакция в чате {reaction.chat.id} на сообщение {reaction.message_id}")
    
    chat_id = reaction.chat.id
    message_id = reaction.message_id
    
    # Проверяем, не это ли реакция на сообщение settings
    if message_id in settings_messages:
        settings_info = settings_messages[message_id]
        if reaction.new_reaction:
            # Собираем все новые эмодзи сначала
            new_emojis = []
            new_custom_ids = []
            
            for r in reaction.new_reaction:
                if r.type == 'emoji' and hasattr(r, 'emoji'):
                    new_emojis.append(r.emoji)
                elif r.type == 'custom_emoji' and hasattr(r, 'custom_emoji_id'):
                    new_custom_ids.append(str(r.custom_emoji_id))
            
            if new_emojis or new_custom_ids:
                # Получаем текущие эмодзи для этого чата
                current_emojis = get_watched_emojis(chat_id)
                current_custom_ids = get_watched_custom_emoji_ids(chat_id)
                
                action = settings_info.get('action', 'add')
                
                # Фильтруем только новые эмодзи (которых еще нет)
                actually_new_emojis = [e for e in new_emojis if e not in current_emojis]
                actually_new_custom_ids = [cid for cid in new_custom_ids if cid not in current_custom_ids]
                
                if actually_new_emojis or actually_new_custom_ids:
                    if action == "add":
                        # Добавляем к текущим
                        current_emojis.extend(actually_new_emojis)
                        current_custom_ids.extend(actually_new_custom_ids)
                    else:
                        # Заменяем полностью (но добавляем новое)
                        current_emojis = actually_new_emojis if actually_new_emojis else current_emojis
                        current_custom_ids = actually_new_custom_ids if actually_new_custom_ids else current_custom_ids
                    
                    # Формируем строку для сохранения
                    emojis_str = ''.join(current_emojis)
                    if current_custom_ids:
                        custom_str = ','.join([f"custom:{cid}" for cid in current_custom_ids])
                        emojis_str = emojis_str + (',' + custom_str if emojis_str else custom_str)
                    
                    # Сохраняем в БД для этого чата
                    try:
                        with db_lock:
                            cursor.execute("""
                                INSERT INTO settings (chat_id, key, value) 
                                VALUES (%s, 'watched_emoji', %s) 
                                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                            """, (chat_id, emojis_str))
                            conn.commit()
                            logger.info(f"[SETTINGS REACTION] Эмодзи сохранено: {emojis_str}")
                            
                            # Отправляем одно подтверждение со всеми новыми эмодзи
                            emoji_displays = []
                            if actually_new_emojis:
                                emoji_displays.extend(actually_new_emojis)
                            if actually_new_custom_ids:
                                emoji_displays.extend([f"custom:{cid}" for cid in actually_new_custom_ids])
                            
                            if emoji_displays:
                                emojis_text = ', '.join(emoji_displays)
                                if len(emoji_displays) == 1:
                                    bot.send_message(chat_id, f"✅ Эмодзи {emojis_text} добавлен! Теперь он отмечает фильмы как просмотренные.")
                                else:
                                    bot.send_message(chat_id, f"✅ Эмодзи добавлены: {emojis_text}\nТеперь они отмечают фильмы как просмотренные.")
                    except Exception as e:
                        conn.rollback()
                        logger.error(f"[SETTINGS REACTION] Ошибка сохранения: {e}", exc_info=True)
        return
    
    # Сначала проверяем, не это ли голосование по обнулению базы
    if message_id in clean_votes:
        vote_data = clean_votes[message_id]
        is_like = False
        user_id = reaction.user.id if reaction.user else None
        if reaction.new_reaction:
            for r in reaction.new_reaction:
                if hasattr(r, 'type'):
                    if r.type == 'emoji' and hasattr(r, 'emoji') and r.emoji == '👍':
                        is_like = True
                        break
                elif hasattr(r, 'emoji') and r.emoji == '👍':
                    is_like = True
                    break
        
        if is_like and user_id:
            # Любой участник чата может проголосовать, не только те, кто в active_members
            vote_data['voted'].add(user_id)
            
            # Проверяем, все ли проголосовали (используем members_count, а не len(active_members))
            if len(vote_data['voted']) >= vote_data['members_count']:
                # Все проголосовали - удаляем базу
                with db_lock:
                    # Удаляем билеты (связаны с plans через plan_id)
                    cursor.execute('DELETE FROM tickets WHERE chat_id = %s', (chat_id,))
                    # Удаляем планы (расписание)
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
                    # Удаляем фильмы
                    cursor.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
                    # Удаляем оценки
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
                    # Удаляем настройки
                    cursor.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
                    # Удаляем статистику
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
                    # Удаляем голосования
                    cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s', (chat_id,))
                    conn.commit()
                
                bot.send_message(chat_id, "✅ Все участники проголосовали. База данных чата полностью обнулена.")
                logger.info(f"База данных чата {chat_id} обнулена после голосования всех участников")
                
                # Удаляем из clean_votes
                del clean_votes[message_id]
            else:
                # Обновляем сообщение с прогрессом
                voted_count = len(vote_data['voted'])
                total_count = vote_data['members_count']
                try:
                    bot.edit_message_text(
                        f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                        f"Участников в чате: {total_count}\n"
                        f"Проголосовало: {voted_count}/{total_count}\n\n"
                        f"Для подтверждения все участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                        f"Если не все проголосуют, база не будет удалена.",
                        chat_id, message_id, parse_mode='HTML')
                except:
                    pass
        return
    
    # Получаем обычные эмодзи (как список символов) для этого чата
    ordinary_emojis = list(get_watched_emojis(chat_id))  # ['✅', '💋', '❤️' и т.д.]
    
    # Получаем кастомные эмодзи ID для этого чата
    custom_emoji_ids = get_watched_custom_emoji_ids(chat_id)
    
    logger.info(f"[REACTION] Проверка watched эмодзи для чата {chat_id}")
    logger.info(f"[REACTION] Доступные watched эмодзи: {ordinary_emojis}")
    logger.info(f"[REACTION] Доступные кастомные ID: {custom_emoji_ids}")
    
    is_watched = False
    
    if not reaction.new_reaction:
        logger.info("[REACTION] Нет новых реакций")
        return
    
    logger.info(f"[REACTION] Количество новых реакций: {len(reaction.new_reaction)}")
    
    # Нормализуем эмодзи для сравнения (убираем variation selector)
    def normalize_emoji(emoji_str):
        """Убирает variation selector (FE0F) из эмодзи для нормализации"""
        if not emoji_str:
            return emoji_str
        # Убираем variation selector (U+FE0F)
        return emoji_str.replace('\ufe0f', '')
    
    # Нормализуем список watched эмодзи
    normalized_watched = [normalize_emoji(e) for e in ordinary_emojis]
    
    for r in reaction.new_reaction:
        logger.info(f"[REACTION DEBUG] Реакция: type={getattr(r, 'type', 'unknown')}, emoji={getattr(r, 'emoji', None)}, custom_emoji_id={getattr(r, 'custom_emoji_id', None)}")
        
        if hasattr(r, 'type') and r.type == 'emoji' and hasattr(r, 'emoji'):
            normalized_reaction = normalize_emoji(r.emoji)
            if normalized_reaction in normalized_watched:
                logger.info(f"[REACTION DEBUG] ✅ Найден watched эмодзи: {r.emoji} (нормализован: {normalized_reaction})")
                is_watched = True
                break
            else:
                logger.info(f"[REACTION DEBUG] ❌ Эмодзи {r.emoji} (нормализован: {normalized_reaction}) не в списке watched: {normalized_watched}")
        elif hasattr(r, 'type') and r.type == 'custom_emoji' and hasattr(r, 'custom_emoji_id'):
            if str(r.custom_emoji_id) in custom_emoji_ids:
                logger.info(f"[REACTION DEBUG] ✅ Найден watched кастомный эмодзи ID: {r.custom_emoji_id}")
                is_watched = True
                break
            else:
                logger.info(f"[REACTION DEBUG] ❌ Кастомный ID {r.custom_emoji_id} не в списке watched: {custom_emoji_ids}")
        else:
            # Старый формат реакции (без type)
            if hasattr(r, 'emoji'):
                if r.emoji in ordinary_emojis:
                    logger.info(f"[REACTION DEBUG] ✅ Найден watched эмодзи (старый формат): {r.emoji}")
                    is_watched = True
                    break
                else:
                    logger.info(f"[REACTION DEBUG] ❌ Эмодзи {r.emoji} не в списке watched (старый формат): {ordinary_emojis}")
    
    if not is_watched:
        logger.info("[REACTION] Не watched эмодзи — игнорируем")
        return
    
    link = None
    if is_watched:
        link = bot_messages.get(message_id)
        if not link:
            # Проверяем также plan_notification_messages
            plan_data = plan_notification_messages.get(message_id)
            if plan_data:
                link = plan_data.get('link')
                logger.info(f"[REACTION] Найдена ссылка в plan_notification_messages: {link}")
        
        # Если не найдено, пытаемся найти в БД по message_id или другим способом
        if not link:
            logger.info(f"[REACTION] Не найдено в bot_messages и plan_notification_messages для message_id={message_id}")
            # Пробуем найти фильм в БД по последним добавленным фильмам в этом чате
            # Это не идеально, но лучше чем пересылать сообщение
            try:
                with db_lock:
                    # Ищем последние фильмы в этом чате (за последний час)
                    cursor.execute("""
                        SELECT link FROM movies 
                        WHERE chat_id = %s 
                        ORDER BY id DESC 
                        LIMIT 10
                    """, (chat_id,))
                    recent_links = cursor.fetchall()
                    # Если в чате недавно был добавлен только один фильм, используем его
                    if len(recent_links) == 1:
                        link = recent_links[0].get('link') if isinstance(recent_links[0], dict) else recent_links[0][0]
                        logger.info(f"[REACTION] Использована последняя ссылка из БД: {link}")
                        bot_messages[message_id] = link
            except Exception as e:
                logger.warning(f"[REACTION] Ошибка при поиске в БД: {e}")
    
    if not link:
        logger.info(f"[REACTION] Нет link для message_id={message_id}, chat_id={chat_id}. Реакция не обработана.")
        return
    
    user_id = reaction.user.id if reaction.user else None
    if not user_id:
        logger.warning("[REACTION] Не удалось получить user_id")
        return
    
    with db_lock:
        cursor.execute("SELECT id, title FROM movies WHERE link = %s AND chat_id = %s", (link, chat_id))
        film = cursor.fetchone()
        if not film:
            logger.info("[REACTION] Фильм не найден")
            return
        
        film_id = film['id']
        film_title = film['title']
        
        # Проверяем, не просмотрел ли уже этот пользователь
        cursor.execute("SELECT id FROM watched_movies WHERE chat_id = %s AND film_id = %s AND user_id = %s", 
                      (chat_id, film_id, user_id))
        already_watched = cursor.fetchone()
        
        if already_watched:
            logger.info(f"[REACTION] Пользователь {user_id} уже отметил фильм {film_title} как просмотренный")
            return
        
        # Сохраняем просмотр для конкретного пользователя
        cursor.execute("""
            INSERT INTO watched_movies (chat_id, film_id, user_id, watched_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (chat_id, film_id, user_id) DO NOTHING
        """, (chat_id, film_id, user_id))
        
        # Обновляем watched для фильма (если хотя бы один просмотрел)
        cursor.execute("""
            UPDATE movies 
            SET watched = 1 
            WHERE id = %s AND (
                SELECT COUNT(*) FROM watched_movies WHERE film_id = %s AND chat_id = %s
            ) > 0
        """, (film_id, film_id, chat_id))
        
        conn.commit()
        logger.info(f"[REACTION] Фильм {film_title} отмечен просмотренным пользователем {user_id}")
        
        # Получаем kp_id для получения фактов
        cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        kp_row = cursor.fetchone()
        kp_id = kp_row.get('kp_id') if isinstance(kp_row, dict) else (kp_row[0] if kp_row else None)
    
    # Получаем и отправляем факты о фильме ПЕРЕД сообщением об оценке
    if kp_id:
        facts = get_facts(kp_id)
        if facts:
            bot.send_message(chat_id, facts, parse_mode='HTML')
    
    # Отправляем персональное сообщение пользователю с упоминанием
    user_name = reaction.user.first_name if reaction.user else "Вы"
    user_mention = f"@{reaction.user.username}" if reaction.user and reaction.user.username else user_name
    msg = bot.send_message(chat_id, 
        f"🎬 {user_mention}, фильм <b>{film_title}</b> отмечен как просмотренный!\n\n"
        f"💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку.",
        parse_mode='HTML')
    
    # Сохраняем связь message_id -> film_id для обработки оценки
    rating_messages[msg.message_id] = film_id
    logger.info(f"[REACTION] Сообщение об оценке отправлено для {user_name}, message_id={msg.message_id}, film_id={film_id}")
    
    # Планируем напоминание на следующий день после просмотра (только для планов "дома")
    try:
        with db_lock:
            # Проверяем, есть ли план "дома" для этого фильма
            cursor.execute("""
                SELECT plan_type 
                FROM plans 
                WHERE chat_id = %s AND film_id = %s AND plan_type = 'home'
                LIMIT 1
            """, (chat_id, film_id))
            plan_row = cursor.fetchone()
            
            if plan_row:
                # Получаем дату просмотра для этого пользователя
                cursor.execute("""
                    SELECT watched_at 
                    FROM watched_movies 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                """, (chat_id, film_id, user_id))
                watched_row = cursor.fetchone()
                
                if watched_row:
                    watched_at = watched_row.get('watched_at') if isinstance(watched_row, dict) else watched_row[0]
                    if isinstance(watched_at, str):
                        from datetime import datetime
                        watched_at = datetime.fromisoformat(watched_at.replace('Z', '+00:00'))
                    
                    # Проверяем, не оценил ли уже пользователь
                    cursor.execute("""
                        SELECT id FROM ratings 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s
                    """, (chat_id, film_id, user_id))
                    has_rating = cursor.fetchone()
                    
                    if not has_rating:
                        # Напоминание на следующий день после просмотра
                        from datetime import timedelta
                        reminder_datetime = watched_at + timedelta(days=1)
                        
                        # Планируем напоминание
                        scheduler.add_job(
                            send_rating_reminder,
                            'date',
                            run_date=reminder_datetime.astimezone(pytz.utc),
                            args=[chat_id, film_id, film_title, user_id],
                            id=f'rating_reminder_{chat_id}_{film_id}_{user_id}'
                        )
                        logger.info(f"[REACTION] Запланировано напоминание об оценке для user_id={user_id}, film_id={film_id}, datetime={reminder_datetime}")
    except Exception as e:
        logger.error(f"[REACTION] Ошибка при планировании напоминания: {e}", exc_info=True)

# ==================== ВНУТРЕННИЕ ФУНКЦИИ ДЛЯ ГЛАВНОГО ХЭНДЛЕРА ====================

def handle_new_session_input_internal(message, state):
    """Внутренняя функция для обработки ввода нового сеанса"""
    # Используем существующую логику из handle_new_session_input
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    file_id_from_state = state.get('file_id')
    
    # Парсим ссылку или kp_id
    link = None
    kp_id = None
    
    link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/(\d+))', text)
    if link_match:
        link = link_match.group(1)
        kp_id = link_match.group(3)
    
    if not kp_id:
        id_match = re.search(r'^(\d+)', text)
        if id_match:
            kp_id = id_match.group(1)
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
    
    if not kp_id:
        bot.reply_to(message, "⚠️ Не найдена ссылка или ID фильма. Формат: ссылка или ID + дата + время")
        return
    
    # Парсим дату и время
    user_tz = get_user_timezone_or_default(user_id)
    session_dt = parse_session_time(text, user_tz)
    if not session_dt:
        bot.reply_to(message, "⚠️ Не удалось распознать дату и время. Формат: 10.01 15:20 или 10 января 20:30")
        return
    
    movie_info = extract_movie_info(link)
    if not movie_info:
        bot.reply_to(message, "Не удалось получить данные о фильме.")
        return
    
    with db_lock:
        cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        row = cursor.fetchone()
        if row:
            film_id = row['id'] if isinstance(row, dict) else row[0]
            title = row['title'] if isinstance(row, dict) else row[1]
        else:
            is_series_val = 1 if movie_info.get('is_series') else 0
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                RETURNING id, title
            ''', (chat_id, link, kp_id, movie_info.get('title'), movie_info.get('year'), 
                  movie_info.get('genres'), movie_info.get('description'), 
                  movie_info.get('director'), movie_info.get('actors'), is_series_val))
            conn.commit()
            row = cursor.fetchone()
            film_id = row['id'] if isinstance(row, dict) else row[0]
            title = row['title'] if isinstance(row, dict) else row[1]
        
        plan_utc = session_dt.astimezone(pytz.utc)
        cursor.execute('''
            INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id, ticket_file_id)
            VALUES (%s, %s, 'cinema', %s, %s, %s)
            RETURNING id
        ''', (chat_id, film_id, plan_utc, user_id, file_id_from_state))
        conn.commit()
        plan_row = cursor.fetchone()
        plan_id = plan_row['id'] if isinstance(plan_row, dict) else plan_row[0]
    
    # Планируем напоминания
    morning_dt = session_dt.replace(hour=9, minute=0)
    if morning_dt < datetime.now(user_tz):
        morning_dt = morning_dt + timedelta(days=1)
    morning_utc = morning_dt.astimezone(pytz.utc)
    
    scheduler.add_job(
        send_plan_notification,
        'date',
        run_date=morning_utc,
        args=[chat_id, film_id, title, link, 'cinema'],
        id=f'plan_morning_{chat_id}_{plan_id}_{int(morning_utc.timestamp())}'
    )
    
    if file_id_from_state:
        ticket_dt = session_dt - timedelta(minutes=10)
        if ticket_dt > datetime.now(user_tz):
            ticket_utc = ticket_dt.astimezone(pytz.utc)
            scheduler.add_job(
                send_ticket_notification,
                'date',
                run_date=ticket_utc,
                args=[chat_id, plan_id],
                id=f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'
            )
    
    # Отправляем ответ пользователю
    bot.reply_to(message, f"✅ Сеанс запланирован!\n\n<b>{title}</b>\n{session_dt.strftime('%d.%m.%Y %H:%M')}\n\nПрикрепите фото билетов (можно несколько).", parse_mode='HTML')
    
    # Переход к загрузке билетов
    user_ticket_state[user_id] = {
        'step': 'upload_ticket',
        'plan_id': plan_id,
        'film_title': title,
        'plan_dt': session_dt.strftime('%d.%m %H:%M'),
        'chat_id': chat_id
    }

def ticket_done_internal(message, state):
    """Внутренняя функция для обработки команды 'готово'"""
    user_id = message.from_user.id
    title = state.get('film_title', 'фильм')
    dt = state.get('plan_dt', '')
    
    bot.reply_to(message, f"✅ Все билеты прикреплены к сеансу:\n\n<b>{title}</b> — {dt}\n\nПриятного просмотра! 🎬", parse_mode='HTML')
    
    if user_id in user_ticket_state:
        del user_ticket_state[user_id]

def handle_edit_ticket_text_internal(message, state):
    """Внутренняя функция для обработки времени сеанса"""
    # Используем существующую логику из handle_edit_ticket_text
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    if state.get('step') == 'waiting_session_time':
        plan_id = state.get('plan_id')
        user_tz = get_user_timezone_or_default(user_id)
        
        session_dt = parse_session_time(text, user_tz)
        if not session_dt:
            bot.reply_to(message, "❌ Не удалось распознать время. Попробуйте в формате:\n• 15 января 10:30\n• 17.01 15:20")
            return
        
        with db_lock:
            cursor.execute('''
                SELECT m.title, m.link, p.plan_type
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s
            ''', (plan_id,))
            plan_info = cursor.fetchone()
            
            session_utc = session_dt.astimezone(pytz.utc)
            cursor.execute('UPDATE plans SET plan_datetime = %s WHERE id = %s', (session_utc, plan_id))
            cursor.execute('UPDATE tickets SET session_datetime = %s WHERE plan_id = %s', (session_utc, plan_id))
            conn.commit()
        
        if plan_info:
            title = plan_info.get('title') if isinstance(plan_info, dict) else plan_info[0]
            link = plan_info.get('link') if isinstance(plan_info, dict) else plan_info[1]
            plan_type = plan_info.get('plan_type') if isinstance(plan_info, dict) else plan_info[2]
            
            morning_dt = session_dt.replace(hour=9, minute=0)
            if morning_dt < datetime.now(user_tz):
                morning_dt = morning_dt + timedelta(days=1)
            morning_utc = morning_dt.astimezone(pytz.utc)
            
            scheduler.add_job(
                send_plan_notification,
                'date',
                run_date=morning_utc,
                args=[chat_id, plan_info.get('film_id') if isinstance(plan_info, dict) else None, title, link, plan_type],
                id=f'plan_morning_{chat_id}_{plan_id}_{int(morning_utc.timestamp())}'
            )
            
            tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
            formatted_time = session_dt.strftime('%d.%m %H:%M')
            bot.reply_to(message, f"✅ <b>Время принято!</b>\n\n🕐 Сеанс: {formatted_time} {tz_name}", parse_mode='HTML')
            del user_ticket_state[user_id]

def handle_edit_rating_internal(message, state):
    """Внутренняя функция для обработки редактирования оценки"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    try:
        rating = int(text)
        if 1 <= rating <= 10:
            film_id = state.get('film_id')
            with db_lock:
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                conn.commit()
            
            bot.reply_to(message, f"✅ Оценка изменена на {rating}/10")
            del user_edit_state[user_id]
        else:
            bot.reply_to(message, "❌ Оценка должна быть от 1 до 10")
    except ValueError:
        bot.reply_to(message, "❌ Введите число от 1 до 10")

def handle_edit_plan_datetime_internal(message, state):
    """Внутренняя функция для обработки редактирования времени плана"""
    # Используем существующую логику из handle_edit_plan_datetime
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    plan_id = state.get('plan_id')
    
    user_tz = get_user_timezone_or_default(user_id)
    session_dt = parse_session_time(text, user_tz)
    if not session_dt:
        bot.reply_to(message, "❌ Не удалось распознать время. Попробуйте в формате:\n• 15 января 10:30\n• 17.01 15:20")
        return
    
    with db_lock:
        cursor.execute('UPDATE plans SET plan_datetime = %s WHERE id = %s', (session_dt.astimezone(pytz.utc), plan_id))
        conn.commit()
    
    tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
    formatted_time = session_dt.strftime('%d.%m %H:%M')
    bot.reply_to(message, f"✅ <b>Время принято!</b>\n\n🕐 Сеанс: {formatted_time} {tz_name}", parse_mode='HTML')
    del user_edit_state[user_id]

def handle_delete_movie_internal(message, state):
    """Внутренняя функция для обработки удаления фильма"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    logger.info(f"[DELETE MOVIE] Обработка удаления фильма: text='{text}', user_id={user_id}, chat_id={chat_id}")
    
    # Извлекаем kp_id из ссылки или используем как ID
    kp_id = extract_kp_id_from_text(text)
    if not kp_id:
        logger.warning(f"[DELETE MOVIE] Не удалось извлечь kp_id из текста: '{text}'")
        bot.reply_to(message, "❌ Не удалось распознать ссылку или ID. Введите ссылку на фильм (kinopoisk.ru/film/...) или ID фильма.")
        return
    
    logger.info(f"[DELETE MOVIE] Извлечен kp_id: {kp_id}")
    
    # Ищем фильм в БД
    with db_lock:
        cursor.execute("SELECT id, title FROM movies WHERE (kp_id = %s OR id = %s) AND chat_id = %s", (kp_id, kp_id, chat_id))
        film = cursor.fetchone()
        
        logger.info(f"[DELETE MOVIE] Результат поиска фильма: {film}")
        
        if not film:
            logger.warning(f"[DELETE MOVIE] Фильм с kp_id={kp_id} или id={kp_id} не найден в чате {chat_id}")
            bot.reply_to(message, f"❌ Фильм с ID {kp_id} не найден в базе этого чата.")
            if user_id in user_edit_state:
                del user_edit_state[user_id]
            return
        
        film_id = film.get('id') if isinstance(film, dict) else film[0]
        title = film.get('title') if isinstance(film, dict) else film[1]
        
        logger.info(f"[DELETE MOVIE] Найден фильм: id={film_id}, title={title}")
        
        # Удаляем связанные данные
        cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        ratings_deleted = cursor.rowcount
        logger.info(f"[DELETE MOVIE] Удалено оценок: {ratings_deleted}")
        
        cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        plans_deleted = cursor.rowcount
        logger.info(f"[DELETE MOVIE] Удалено планов: {plans_deleted}")
        
        cursor.execute('DELETE FROM watched_movies WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        watched_deleted = cursor.rowcount
        logger.info(f"[DELETE MOVIE] Удалено отметок просмотра: {watched_deleted}")
        
        cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        movie_deleted = cursor.rowcount
        logger.info(f"[DELETE MOVIE] Удалено фильмов: {movie_deleted}")
        
        conn.commit()
        logger.info(f"[DELETE MOVIE] Транзакция закоммичена")
        
        # Отправляем сообщение о результате
        if movie_deleted > 0:
            bot.reply_to(message, f"✅ Фильм <b>{title}</b> удалён из базы.\n\nТакже удалено:\n• Оценок: {ratings_deleted}\n• Планов: {plans_deleted}\n• Отметок просмотра: {watched_deleted}", parse_mode='HTML')
            logger.info(f"[DELETE MOVIE] Фильм {title} (id={film_id}) удалён пользователем {user_id} из чата {chat_id}")
        else:
            logger.error(f"[DELETE MOVIE] Фильм не был удален! movie_deleted={movie_deleted}")
            bot.reply_to(message, f"❌ Ошибка при удалении фильма. Попробуйте снова.")
    
    if user_id in user_edit_state:
        del user_edit_state[user_id]

def handle_settings_emojis_internal(message, state):
    """Внутренняя функция для обработки ответа с эмодзи на /settings"""
    # Используем существующую логику из handle_settings_emojis
    user_id = message.from_user.id
    chat_id = state.get('chat_id') or message.chat.id
    
    import re
    emoji_pattern = re.compile(
        r'[\U0001F300-\U0001F9FF]|[\U0001F600-\U0001F64F]|[\U0001F680-\U0001F6FF]|[\U00002600-\U000026FF]|[\U00002700-\U000027BF]|[\U0001F900-\U0001F9FF]|[\U0001FA00-\U0001FAFF]|[\U0001F1E0-\U0001F1FF]'
    )
    
    emojis = emoji_pattern.findall(message.text or "")
    
    if not emojis:
        bot.reply_to(message, "⚠️ Не найдено эмодзи в сообщении. Отправьте только эмодзи (можно несколько).")
        return
    
    emojis_str = ''.join(set(emojis))
    
    action = state.get('action', 'replace')
    if action == "add":
        current_emojis = get_watched_emojis(chat_id)
        emojis_str = ''.join(current_emojis) + emojis_str
        seen = set()
        emojis_str = ''.join(c for c in emojis_str if c not in seen and not seen.add(c))
        action_text = "добавлены к текущим"
    else:
        action_text = "заменены"
    
    with db_lock:
        try:
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (%s, 'watched_emoji', %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (chat_id, emojis_str))
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"[SETTINGS] Ошибка сохранения эмодзи: {e}", exc_info=True)
            bot.reply_to(message, "❌ Ошибка сохранения. Попробуй позже.")
            return
    
    bot.reply_to(message, f"✅ Реакции {action_text}:\n{emojis_str}")
    
    if user_id in user_settings_state:
        del user_settings_state[user_id]

def get_plan_link_internal(message, state):
    """Внутренняя функция для получения ссылки на фильм в /plan"""
    # Используем существующую логику из get_plan_link
    user_id = message.from_user.id
    chat_id = message.chat.id
    link = None
    
    if message.reply_to_message:
        link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', message.reply_to_message.text or '')
        if link_match:
            link = link_match.group(0)
    
    if not link:
        link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', message.text)
        if link_match:
            link = link_match.group(0)
    
    if not link:
        id_match = re.search(r'^(\d+)', message.text.strip())
        if id_match:
            kp_id = id_match.group(1)
            with db_lock:
                cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    link = row.get('link') if isinstance(row, dict) else row[0]
                else:
                    link = f"https://kinopoisk.ru/film/{kp_id}"
    
    if not link:
        bot.reply_to(message, "Не нашёл ссылку или ID фильма. Попробуйте снова.")
        return
    
    user_plan_state[user_id]['link'] = link
    user_plan_state[user_id]['step'] = 2
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
    markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
    bot.send_message(message.chat.id, "Где планируете смотреть?", reply_markup=markup)

def get_plan_day_or_date_internal(message, state):
    """Внутренняя функция для получения дня/даты в /plan"""
    # Используем существующую логику из get_plan_day_or_date
    user_id = message.from_user.id
    text = message.text.lower().strip()
    plan_type = state.get('type')
    link = state.get('link')
    
    logger.info(f"[PLAN DAY/DATE INTERNAL] Обработка: text='{text}', plan_type={plan_type}, link={link}")
    
    if not plan_type or not link:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Отсутствует plan_type или link: plan_type={plan_type}, link={link}")
        bot.reply_to(message, "❌ Ошибка: не указан тип просмотра или ссылка. Начните заново.")
        if user_id in user_plan_state:
            del user_plan_state[user_id]
        return
    
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)
    plan_dt = None
    
    target_weekday = None
    for phrase, wd in days_full.items():
        if phrase in text:
            target_weekday = wd
            logger.info(f"[PLAN DAY/DATE INTERNAL] Найден день недели: {phrase} -> {wd}")
            break
    
    if target_weekday is not None:
        current_wd = now.weekday()
        delta = (target_weekday - current_wd + 7) % 7
        if delta == 0:
            delta = 7
        plan_date = now.date() + timedelta(days=delta)
        
        if plan_type == 'home':
            hour = 19 if target_weekday < 5 else 10
        else:
            hour = 9
        
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)
        logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата по дню недели: {plan_dt}")
    else:
        # Обработка специальных форматов: "завтра", "следующая неделя"
        if 'завтра' in text:
            plan_date = (now.date() + timedelta(days=1))
            if plan_type == 'home':
                hour = 19 if plan_date.weekday() < 5 else 10
            else:
                hour = 9
            plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
            plan_dt = user_tz.localize(plan_dt)
            logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата 'завтра': {plan_dt}")
        elif 'следующая неделя' in text or 'след неделя' in text or 'след. неделя' in text or 'на следующей неделе' in text:
            if plan_type == 'home':
                # Для дома - суббота следующей недели в 10:00
                current_wd = now.weekday()
                days_until_next_saturday = (5 - current_wd + 7) % 7
                if days_until_next_saturday == 0:
                    days_until_next_saturday = 7
                else:
                    days_until_next_saturday += 7
                plan_date = now.date() + timedelta(days=days_until_next_saturday)
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=10))
                plan_dt = user_tz.localize(plan_dt)
                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата 'на следующей неделе' (дом): {plan_dt}")
            else:
                # Для кино - четверг следующей недели
                current_wd = now.weekday()
                days_until_thursday = (3 - current_wd + 7) % 7
                if days_until_thursday == 0:
                    days_until_thursday = 7
                else:
                    days_until_thursday += 7
                plan_date = now.date() + timedelta(days=days_until_thursday)
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=9))
                plan_dt = user_tz.localize(plan_dt)
                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата 'на следующей неделе' (кино): {plan_dt}")
        else:
            # Парсинг дат: "15 января", "15 января 17:00", "10.01", "14 апреля"
            # Сначала пробуем формат с временем: "15 января 17:00"
            date_time_match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2})[.:](\d{2})', text)
            if date_time_match:
                day_num = int(date_time_match.group(1))
                month_str = date_time_match.group(2)
                hour = int(date_time_match.group(3))
                minute = int(date_time_match.group(4))
                month = months_map.get(month_str.lower())
                if month:
                    try:
                        year = now.year
                        candidate = user_tz.localize(datetime(year, month, day_num, hour, minute))
                        if candidate < now:
                            year += 1
                        plan_dt = user_tz.localize(datetime(year, month, day_num, hour, minute))
                        logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата с временем: {plan_dt}")
                    except ValueError as e:
                        logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга даты с временем: {e}")
            else:
                # Парсинг "15 января" или "14 апреля"
                date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
                if date_match:
                    day = int(date_match.group(1))
                    month_str = date_match.group(2).lower()
                    month = months_map.get(month_str)
                    if month:
                        year = now.year
                        try:
                            candidate = user_tz.localize(datetime(year, month, day))
                            if candidate < now:
                                year += 1
                            if plan_type == 'home':
                                hour = 19 if datetime(year, month, day).weekday() < 5 else 10
                            else:
                                hour = 9
                            plan_dt = user_tz.localize(datetime(year, month, day, hour))
                            logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата текстовым форматом: {plan_dt}")
                        except ValueError as e:
                            logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга текстовой даты: {e}")
                else:
                    # Парсинг "10.01" или "06.01"
                    date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
                    if date_match:
                        day_num = int(date_match.group(1))
                        month_num = int(date_match.group(2))
                        if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                            try:
                                year = now.year
                                if date_match.group(3):
                                    year_part = int(date_match.group(3))
                                    if year_part < 100:
                                        year = 2000 + year_part
                                    else:
                                        year = year_part
                                candidate = user_tz.localize(datetime(year, month_num, day_num))
                                if candidate < now:
                                    year += 1
                                if plan_type == 'home':
                                    hour = 19 if datetime(year, month_num, day_num).weekday() < 5 else 10
                                else:
                                    hour = 9
                                plan_dt = user_tz.localize(datetime(year, month_num, day_num, hour))
                                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата числовым форматом: {plan_dt}")
                            except ValueError as e:
                                logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга числовой даты: {e}")
    
    if not plan_dt:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Не удалось распознать дату из текста: '{text}'")
        bot.reply_to(message, "Не удалось распознать день/дату. Попробуйте снова.")
        return
    
    # Вызываем process_plan
    message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
    result = process_plan(user_id, message.chat.id, link, plan_type, None, message_date_utc, plan_dt)
    if result == 'NEEDS_TIMEZONE':
        show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
    elif result:
        del user_plan_state[user_id]

def handle_clean_confirm_internal(message):
    """Внутренняя функция для обработки подтверждения удаления"""
    # Используем существующую логику из handle_clean_confirm
    user_id = message.from_user.id
    state = user_clean_state.get(user_id)
    if not state:
        return
    
    film_id = state.get('film_id')
    chat_id = message.chat.id
    
    with db_lock:
        cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        ratings_deleted = cursor.rowcount
        cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        plans_deleted = cursor.rowcount
        cursor.execute('DELETE FROM watched_movies WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
        watched_deleted = cursor.rowcount
        cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        conn.commit()
    
    bot.reply_to(message, f"✅ Фильм удалён из базы (удалено {ratings_deleted} оценок, {plans_deleted} планов, {watched_deleted} отметок просмотра)")
    del user_clean_state[user_id]

def handle_rate_list_reply_internal(message):
    """Внутренняя функция для обработки реплая на список фильмов с оценками"""
    # Используем существующую логику из handle_rate_list_reply
    chat_id = message.chat.id
    user_id = message.from_user.id
    text = message.text.strip()
    
    ratings_pattern = r'(\d+)\s*[,;:\t]?\s*(\d+)'
    matches = re.findall(ratings_pattern, text)
    
    if not matches:
        bot.reply_to(message, "❌ Не удалось распознать оценки. Используйте формат: <code>kp_id оценка</code>", parse_mode='HTML')
        return
    
    results = []
    errors = []
    
    with db_lock:
        for kp_id_str, rating_str in matches:
            try:
                kp_id = kp_id_str.strip()
                rating = int(rating_str.strip())
                
                if not (1 <= rating <= 10):
                    errors.append(f"{kp_id}: оценка должна быть от 1 до 10")
                    continue
                
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, kp_id))
                film_row = cursor.fetchone()
                
                if not film_row:
                    errors.append(f"{kp_id}: фильм не найден или не просмотрен")
                    continue
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                cursor.execute('''
                    SELECT rating FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                existing = cursor.fetchone()
                
                if existing:
                    errors.append(f"{kp_id}: вы уже оценили этот фильм")
                    continue
                
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                
                results.append((kp_id, title, rating))
                
            except ValueError:
                errors.append(f"{kp_id_str}: неверный формат оценки")
            except Exception as e:
                logger.error(f"Ошибка при сохранении оценки {kp_id_str}: {e}")
                errors.append(f"{kp_id_str}: ошибка обработки")
        
        conn.commit()
    
    response_text = ""
    if results:
        response_text += f"✅ Сохранено оценок: {len(results)}\n"
        for kp_id, title, rating in results[:5]:
            response_text += f"{kp_id}: {title[:30]}... — {rating}/10\n"
        if len(results) > 5:
            response_text += f"... и ещё {len(results) - 5}\n"
    
    if errors:
        response_text += f"\n❌ Ошибки ({len(errors)}):\n"
        for error in errors[:5]:
            response_text += f"{error}\n"
        if len(errors) > 5:
            response_text += f"... и ещё {len(errors) - 5}\n"
    
    bot.reply_to(message, response_text or "Не удалось обработать оценки.")

def handle_random_plan_reply_internal(message, link):
    """Внутренняя функция для обработки реплая на сообщение с фильмом из /random"""
    # Используем существующую логику из handle_random_plan_reply
    original_text = message.text or ''
    text = original_text.lower().strip()
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    plan_type = 'home' if 'дома' in text else 'cinema' if ('в кино' in text or 'кино' in text) else None
    
    if not plan_type:
        error_msg = bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
        if error_msg:
            plan_error_messages[error_msg.message_id] = {
                'user_id': user_id,
                'chat_id': chat_id,
                'link': link,
                'plan_type': None,
                'day_or_date': None,
                'missing': 'plan_type'
            }
        return
    
    day_or_date = None
    
    sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
    for phrase in sorted_phrases:
        if phrase in text:
            day_or_date = phrase
            break
    
    if not day_or_date:
        date_match = re.search(r'(?:с|на|до)?\s*(\d{1,2})\s+([а-яё]+)', text)
        if date_match:
            day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
        else:
            date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
            if date_match:
                day_num = int(date_match.group(1))
                month_num = int(date_match.group(2))
                if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                    month_names = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                 'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                    day_or_date = f"{day_num} {month_names[month_num - 1]}"
    
    if not day_or_date:
        error_msg = bot.reply_to(message, "Не указан день/дата. Для дома укажите день недели (пн, вт, ср, чт, пт, сб, вс или 'в сб'), для кино - день недели или дату (15 января).")
        if error_msg:
            plan_error_messages[error_msg.message_id] = {
                'user_id': user_id,
                'chat_id': chat_id,
                'link': link,
                'plan_type': plan_type,
                'day_or_date': None,
                'missing': 'day_or_date'
            }
        return
    
    message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
    result = process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
    if result == 'NEEDS_TIMEZONE':
        show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")

def handle_plan_error_reply_internal(message):
    """Внутренняя функция для обработки реплая на сообщение с ошибкой планирования"""
    # Используем существующую логику из handle_plan_error_reply
    error_data = plan_error_messages.get(message.reply_to_message.message_id)
    if not error_data:
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    link = error_data['link']
    plan_type = error_data.get('plan_type')
    day_or_date = error_data.get('day_or_date')
    missing = error_data.get('missing')
    
    text = message.text.lower().strip()
    
    if missing == 'plan_type':
        plan_type = 'home' if 'дома' in text else 'cinema' if ('в кино' in text or 'кино' in text) else None
        if not plan_type:
            bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
            return
        error_data['plan_type'] = plan_type
    
    if missing == 'day_or_date' or not day_or_date:
        sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
        for phrase in sorted_phrases:
            if phrase in text:
                day_or_date = phrase
                break
        
        if not day_or_date:
            date_match = re.search(r'(?:с|на|до)?\s*(\d{1,2})\s+([а-яё]+)', text)
            if date_match:
                day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
        
        if not day_or_date:
            bot.reply_to(message, "Не указан день/дата.")
            return
        error_data['day_or_date'] = day_or_date
    
    message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
    result = process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
    if result == 'NEEDS_TIMEZONE':
        show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
    elif result:
        plan_error_messages.pop(message.reply_to_message.message_id, None)

def handle_rating_internal(message, rating):
    """Внутренняя функция для обработки оценки"""
    # Используем существующую логику из handle_rating
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    film_id = None
    
    if message.reply_to_message:
        reply_msg_id = message.reply_to_message.message_id
        film_id = rating_messages.get(reply_msg_id)
        
        if not film_id and message.reply_to_message.reply_to_message:
            parent_reply_msg_id = message.reply_to_message.reply_to_message.message_id
            film_id = rating_messages.get(parent_reply_msg_id)
            if not film_id:
                reply_link = bot_messages.get(parent_reply_msg_id)
                if reply_link:
                    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                    if match:
                        kp_id = match.group(2)
                        with db_lock:
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                            row = cursor.fetchone()
                            if row:
                                film_id = row.get('id') if isinstance(row, dict) else row[0]
        
        if not film_id:
            reply_link = bot_messages.get(reply_msg_id)
            if reply_link:
                match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                if match:
                    kp_id = match.group(2)
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
    
    if film_id:
        try:
            with db_lock:
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                conn.commit()
                
                cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                avg_row = cursor.fetchone()
                avg = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row and len(avg_row) > 0 else None)
                
                # Получаем kp_id для похожих фильмов
                cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                kp_row = cursor.fetchone()
                kp_id = kp_row.get('kp_id') if isinstance(kp_row, dict) else (kp_row[0] if kp_row else None)
                
                avg_str = f"{avg:.1f}" if avg else "—"
                bot.reply_to(message, f"Спасибо! Ваша оценка {rating}/10 сохранена.\nСредняя: {avg_str}/10")
                
                # Если средняя оценка > 9, показываем похожие фильмы и продолжения
                if avg and avg > 9 and kp_id:
                    similars = get_similars(kp_id)
                    sequels = get_sequels(kp_id)
                    
                    if similars or sequels:
                        markup = InlineKeyboardMarkup(row_width=1)
                        if similars:
                            for fid, name in similars:
                                if len(name) > 50:
                                    name = name[:47] + "..."
                                markup.add(InlineKeyboardButton(f"🎬 {name}", callback_data=f"add_similar:{fid}"))
                        
                        if sequels:
                            for fid, name in sequels:
                                if len(name) > 50:
                                    name = name[:47] + "..."
                                markup.add(InlineKeyboardButton(f"▶️ {name}", callback_data=f"add_similar:{fid}"))
                        
                        if markup.keyboard:
                            bot.send_message(chat_id, "🎥 Фильм высоко оценён! Посмотреть похожие или продолжения?", reply_markup=markup)
        except Exception as e:
            logger.error(f"Ошибка при сохранении оценки: {e}", exc_info=True)
            bot.reply_to(message, "Произошла ошибка при сохранении оценки. Попробуйте позже.")
        
        if message.reply_to_message:
            rating_messages.pop(message.reply_to_message.message_id, None)
    else:
        bot.reply_to(message, "❌ Оценка не привязана к фильму. Ответьте на сообщение о просмотренном фильме или на сообщение с фильмом.")

def handle_cinema_vote_internal(message, vote):
    """Внутренняя функция для обработки голосования 'в кино'"""
    # Используем существующую логику из handle_cinema_vote
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    with db_lock:
        cursor.execute('''
            SELECT film_id, deadline, yes_users, no_users
            FROM cinema_votes
            WHERE chat_id = %s AND deadline > NOW()
            ORDER BY deadline ASC
            LIMIT 1
        ''', (chat_id,))
        vote_row = cursor.fetchone()
        
        if not vote_row:
            return
        
        film_id = vote_row.get('film_id') if isinstance(vote_row, dict) else vote_row[0]
        yes_users = vote_row.get('yes_users') or [] if isinstance(vote_row, dict) else (vote_row[2] or [])
        no_users = vote_row.get('no_users') or [] if isinstance(vote_row, dict) else (vote_row[3] or [])
        
        if vote == 'да':
            if user_id not in yes_users:
                yes_users = list(set(yes_users + [user_id]))
                if user_id in no_users:
                    no_users = [u for u in no_users if u != user_id]
        else:
            if user_id not in no_users:
                no_users = list(set(no_users + [user_id]))
                if user_id in yes_users:
                    yes_users = [u for u in yes_users if u != user_id]
        
        cursor.execute('''
            UPDATE cinema_votes
            SET yes_users = %s, no_users = %s
            WHERE chat_id = %s AND film_id = %s
        ''', (yes_users, no_users, chat_id, film_id))
        conn.commit()
    
    bot.reply_to(message, f"✅ Ваш голос '{vote}' учтён!")

def handle_list_reply_internal(message):
    """Внутренняя функция для обработки реплая на список фильмов"""
    # Используем существующую логику из handle_list_reply
    reply_msg_id = message.reply_to_message.message_id
    link = bot_messages.get(reply_msg_id)
    
    if not link:
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    text = message.text.lower().strip()
    
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
    if not match:
        return
    
    kp_id = match.group(2)
    
    with db_lock:
        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        row = cursor.fetchone()
        if not row:
            return
        
        film_id = row.get('id') if isinstance(row, dict) else row[0]
        
        if 'дома' in text:
            plan_type = 'home'
        elif 'в кино' in text or 'кино' in text:
            plan_type = 'cinema'
        else:
            return
        
        # Парсим день/дату
        day_or_date = None
        for phrase in sorted(days_full.keys(), key=len, reverse=True):
            if phrase in text:
                day_or_date = phrase
                break
        
        if not day_or_date:
            date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
            if date_match:
                day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
        
        if not day_or_date:
            return
        
        message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
        result = process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
        if result == 'NEEDS_TIMEZONE':
            show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")

# ==================== ГЛАВНЫЙ ХЭНДЛЕР ДЛЯ ВСЕХ ТЕКСТОВЫХ СООБЩЕНИЙ ====================
@bot.message_handler(content_types=['text'], func=lambda m: not (m.text and m.text.strip().startswith('/')))
def main_text_handler(message):
    """Единый главный хэндлер для всех текстовых сообщений (исключая команды)"""
    logger.info(f"[MAIN TEXT HANDLER] Получено текстовое сообщение от {message.from_user.id}: '{message.text[:100] if message.text else ''}'")
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    # 1. Проверяем состояния (ticket, settings, plan, edit)
    
    # === user_ticket_state ===
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_ticket_state, step={step}")
        
        if step == 'waiting_new_session':
            # Обработка ввода нового сеанса (фильм + дата)
            handle_new_session_input_internal(message, state)
            return
        
        if step == 'upload_ticket':
            # Если ждём билеты, но пришёл текст (например "готово")
            if text.lower().strip() == 'готово':
                ticket_done_internal(message, state)
                return
            # Иначе игнорируем текст (билеты обрабатываются отдельным хэндлером для фото/документов)
            logger.info(f"[MAIN TEXT HANDLER] Игнорируем текст в режиме upload_ticket (ожидаются фото/документы)")
            return
        
        if step == 'waiting_session_time':
            # Обработка времени сеанса
            handle_edit_ticket_text_internal(message, state)
            return
    
    # === user_import_state ===
    if user_id in user_import_state:
        state = user_import_state[user_id]
        step = state.get('step')
        
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_import_state, step={step}")
        
        if step == 'waiting_user_id':
            # Обработка ввода user_id или ссылки
            handle_import_user_id_internal(message, state)
            return
        
        return
    
    # === user_edit_state ===
    if user_id in user_edit_state:
        state = user_edit_state[user_id]
        action = state.get('action')
        
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_edit_state, action={action}")
        
        if action == 'edit_rating':
            # Обработка редактирования оценки
            handle_edit_rating_internal(message, state)
            return
        
        if action == 'edit_plan_datetime':
            # Обработка редактирования времени плана
            handle_edit_plan_datetime_internal(message, state)
            return
        
        if action == 'delete_movie':
            # Обработка удаления фильма
            handle_delete_movie_internal(message, state)
            return
    
    # === user_settings_state ===
    if user_id in user_settings_state:
        state = user_settings_state.get(user_id)
        
        # Проверяем, что это ответ на правильное сообщение
        if message.reply_to_message:
            settings_msg_id = state.get('settings_msg_id')
            if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                if state.get('adding_reactions'):
                    # Обработка ответа с эмодзи на /settings
                    handle_settings_emojis_internal(message, state)
                    return
    
    # === user_plan_state ===
    if user_id in user_plan_state:
        state = user_plan_state[user_id]
        step = state.get('step')
        
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_plan_state, step={step}")
        
        if step == 1:
            # Получение ссылки на фильм
            get_plan_link_internal(message, state)
            return
        
        if step == 3:
            # Получение дня/даты
            get_plan_day_or_date_internal(message, state)
            return
    
    # === user_clean_state ===
    if user_id in user_clean_state:
        if text.upper().strip() == 'ДА, УДАЛИТЬ':
            # Обработка подтверждения удаления
            handle_clean_confirm_internal(message)
            return
    
    # 2. Обработка реплаев
    
    # Реплай на сообщение бота с оценками
    if message.reply_to_message and message.reply_to_message.from_user.id == bot.get_me().id:
        reply_text = message.reply_to_message.text or ""
        
        # Реплай на список фильмов с оценками
        if "Список просмотренных фильмов для оценки" in reply_text:
            handle_rate_list_reply_internal(message)
            return
        
        # Реплай на сообщение с фильмом из /random для планирования
        reply_msg_id = message.reply_to_message.message_id
        if reply_msg_id in bot_messages:
            link = bot_messages.get(reply_msg_id)
            if link:
                handle_random_plan_reply_internal(message, link)
                return
    
    # Реплай на сообщение с ошибкой планирования
    if message.reply_to_message and message.reply_to_message.message_id in plan_error_messages:
        handle_plan_error_reply_internal(message)
        return
    
    # Реплай на сообщение с оценкой (для сохранения оценки)
    if message.reply_to_message and message.text and message.text.isdigit():
        rating = int(message.text)
        if 1 <= rating <= 10:
            handle_rating_internal(message, rating)
            return
    
    # Реплай на голосование "в кино"
    if message.reply_to_message and text.lower() in ['да', 'нет']:
        handle_cinema_vote_internal(message, text.lower())
        return
    
    # Реплай на список фильмов
    if message.reply_to_message and message.reply_to_message.message_id in list_messages:
        handle_list_reply_internal(message)
        return
    
    # 3. Обычные сообщения с фильмами (если нет состояния)
    
    # Сообщения с ссылками на Кинопоиск
    if 'kinopoisk.ru' in text or 'kinopoisk.com' in text:
        link_match = re.search(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', text)
        if link_match:
            link = link_match.group(1)
            # Сохраняем ссылку в bot_messages для обработки реакций
            bot_messages[message.message_id] = link
            logger.info(f"[MAIN TEXT HANDLER] Ссылка сохранена в bot_messages для message_id={message.message_id}: {link}")
            
            # Добавляем фильм в базу
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, 'add_movie', chat_id)
            
            added_count = 0
            links = re.findall(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', text)
            for link_item in links:
                if add_and_announce(link_item, chat_id):
                    added_count += 1
                    logger.info(f"[MAIN TEXT HANDLER] Фильм обработан: {link_item}")
            
            if added_count > 1:
                bot.send_message(chat_id, f"🎉 Добавлено {added_count} новых фильмов в базу!")
            return
    
    # Сообщения с entities (URL в тексте)
    if message.entities:
        links = []
        for entity in message.entities:
            if entity.type == 'url':
                link = text[entity.offset:entity.offset + entity.length]
                if 'kinopoisk.ru' in link or 'kinopoisk.com' in link:
                    links.append(link)
        
        if links:
            for link in links:
                bot_messages[message.message_id] = link
                if add_and_announce(link, chat_id):
                    logger.info(f"[MAIN TEXT HANDLER] Фильм обработан через entities: {link}")
            return
    
    # Если ничего не подошло - игнорируем
    logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано: '{text[:50]}'")

# ==================== ЕДИНЫЙ ХЭНДЛЕР ДЛЯ ФОТО/ДОКУМЕНТОВ ====================
@bot.message_handler(content_types=['photo', 'document'])
def main_file_handler(message):
    """Единый хэндлер для всех фото и документов"""
    logger.info(f"[MAIN FILE HANDLER] Получено фото/документ от {message.from_user.id}")
    
    user_id = message.from_user.id
    
    # Обработка билетов
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        
        if step == 'upload_ticket':
            # Обработка загрузки билетов
            handle_ticket_upload_internal(message, state)
            return
        
        if step == 'waiting_ticket_file':
            # Пользователь выбрал сеанс и загружает билет
            plan_id = state.get('plan_id')
            if plan_id:
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                # Сохраняем билет в БД
                with db_lock:
                    cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (file_id, plan_id))
                    conn.commit()
                logger.info(f"[TICKET FILE] Билет сохранен в БД для plan_id={plan_id}, file_id={file_id}")
                bot.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿")
                # Очищаем состояние пользователя, завершаем цикл работы с билетами
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                logger.info(f"[TICKET FILE] Состояние пользователя {user_id} очищено после сохранения билета")
                return
        
        if step != 'upload_ticket':
            # Сохраняем file_id для последующей обработки
            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            state['file_id'] = file_id
            bot.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿")
            # Очищаем состояние пользователя, завершаем цикл работы с билетами
            if user_id in user_ticket_state:
                del user_ticket_state[user_id]
            logger.info(f"[TICKET FILE] Состояние пользователя {user_id} очищено после получения файла")
            return
    
    # Если не в состоянии - игнорируем
    logger.info(f"[MAIN FILE HANDLER] Фото/документ не обработан (пользователь не в user_ticket_state)")

def handle_ticket_upload_internal(message, state):
    """Внутренняя функция для обработки загрузки билетов"""
    user_id = message.from_user.id
    plan_id = state.get('plan_id')
    
    if not plan_id:
        bot.reply_to(message, "❌ Ошибка: план не найден.")
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        return
    
    file_id = message.photo[-1].file_id if message.photo else message.document.file_id
    
    with db_lock:
        cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (file_id, plan_id))
        conn.commit()
    
    title = state.get('film_title', 'фильм')
    dt = state.get('plan_dt', '')
    
    bot.reply_to(message, f"✅ Билет прикреплён!\n\n<b>{title}</b> — {dt}\n\nМожете отправить ещё билеты или написать 'готово'.", parse_mode='HTML')

# Обработчик для сохранения сообщений пользователей с ссылками на фильмы (ОСТАВЛЕН ДЛЯ ОБРАТНОЙ СОВМЕСТИМОСТИ, НО НЕ ИСПОЛЬЗУЕТСЯ)
@bot.message_handler(func=lambda m: (
    m.text and 
    ('kinopoisk.ru' in m.text or 'kinopoisk.com' in m.text) and
    not m.text.strip().startswith('/plan')  # Не обрабатываем команду /plan
))
def save_movie_message(message):
    """Обрабатывает сообщения пользователей со ссылками на фильмы: добавляет в базу и отправляет карточку"""
    logger.info(f"[SAVE MOVIE] save_movie_message вызван для пользователя {message.from_user.id}, текст: '{message.text[:100]}'")
    
    # Сохраняем ссылку в bot_messages для обработки реакций, даже если пропускаем обработку
    links = []
    try:
        links = re.findall(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', message.text)
        if links:
            # Сохраняем первую ссылку для обработки реакций
            bot_messages[message.message_id] = links[0]
            logger.info(f"[SAVE MOVIE] Ссылка сохранена в bot_messages для message_id={message.message_id}: {links[0]}")
    except Exception as e:
        logger.warning(f"[SAVE MOVIE] Ошибка при сохранении ссылки в bot_messages: {e}")
    
    # Пропускаем, если пользователь работает с билетами или планированием
    if message.from_user.id in user_ticket_state:
        state = user_ticket_state.get(message.from_user.id, {})
        step = state.get('step')
        logger.info(f"[SAVE MOVIE] Пропущено - пользователь в user_ticket_state, step={step}")
        return
    
    if message.from_user.id in user_plan_state:
        logger.info(f"[SAVE MOVIE] Пропущено - пользователь в user_plan_state")
        return
    
    try:
        # Ищем все ссылки на Кинопоиск в сообщении (уже найдены выше)
        if links:
            chat_id = message.chat.id
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, 'add_movie', chat_id)
            logger.info(f"[SAVE MESSAGE] Найдено ссылок на фильмы: {len(links)}, chat_id={chat_id}")
            
            added_count = 0
            for link in links:
                
                # Добавляем фильм в базу и отправляем карточку
                if add_and_announce(link, chat_id):
                    added_count += 1
                    logger.info(f"[SAVE MESSAGE] Фильм обработан: {link}")
            
            if added_count > 1:
                bot.send_message(chat_id, f"🎉 Добавлено {added_count} новых фильмов в базу!")
    except Exception as e:
        logger.warning(f"[SAVE MESSAGE] Ошибка при обработке сообщения с фильмом: {e}", exc_info=True)

# Обработка реплаев для планирования фильмов из /random
@bot.message_handler(func=lambda m: (
    m.text and 
    m.reply_to_message and 
    m.reply_to_message.message_id in bot_messages and
    not m.text.strip().startswith('/') and
    m.from_user.id not in user_plan_state
))
def handle_random_plan_reply(message):
    """Обрабатывает реплаи на сообщения фильмов из /random для планирования"""
    try:
        reply_msg_id = message.reply_to_message.message_id
        link = bot_messages.get(reply_msg_id)
        
        logger.info(f"[RANDOM PLAN] Reply received: reply_msg_id={reply_msg_id}, link={link}, text={message.text}")
        
        if not link:
            logger.warning(f"[RANDOM PLAN] Link not found for message_id={reply_msg_id}, bot_messages keys: {list(bot_messages.keys())[:10]}")
            return
        
        original_text = message.text or ''
        text = original_text.lower().strip()
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        logger.info(f"[RANDOM PLAN] Processing: text='{text}', link={link}, user_id={user_id}")
        
        # Определяем тип планирования
        plan_type = 'home' if 'дома' in text else 'cinema' if ('в кино' in text or 'кино' in text) else None
        logger.info(f"[RANDOM PLAN] plan_type={plan_type}")
        
        if not plan_type:
            error_msg = bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
            # Сохраняем состояние для обработки ответа
            if error_msg:
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': None,
                    'day_or_date': None,
                    'missing': 'plan_type'
                }
            return
        
        # Парсим дату/день недели используя ту же логику, что и в plan_handler
        day_or_date = None
        
        # Сначала ищем день недели (для обоих режимов)
        sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
        for phrase in sorted_phrases:
            if phrase in text:
                day_or_date = phrase
                break
        
        # Если день недели не найден, ищем дату (для обоих режимов)
        if not day_or_date:
            # Пробуем разные форматы даты: "15 января", "с 20 февраля", "15.01", "15/01", "15.01.25", "15.01.2025"
            # Убираем предлоги "с", "на" и т.д. перед датой
            date_match = re.search(r'(?:с|на|до)?\s*(\d{1,2})\s+([а-яё]+)', text)
            if date_match:
                day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
                logger.info(f"[RANDOM PLAN] Найдена дата (текстовый формат): {day_or_date}")
            else:
                # Формат "15.01", "15/01", "15.01.25", "15.01.2025", "15/01/25", "15/01/2025"
                date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
                if date_match:
                    day_num = int(date_match.group(1))
                    month_num = int(date_match.group(2))
                    if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                        month_names = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                     'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                        day_or_date = f"{day_num} {month_names[month_num - 1]}"
                        logger.info(f"[RANDOM PLAN] Найдена дата (числовой формат): {day_or_date}")
        
        if not day_or_date:
            error_msg = bot.reply_to(message, "Не указан день/дата. Для дома укажите день недели (пн, вт, ср, чт, пт, сб, вс или 'в сб'), для кино - день недели или дату (15 января).")
            logger.warning(f"[RANDOM PLAN] Day/date not found in text: '{text}'")
            # Сохраняем состояние для обработки ответа
            if error_msg:
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': plan_type,
                    'day_or_date': None,
                    'missing': 'day_or_date'
                }
            return
        
        logger.info(f"[RANDOM PLAN] Parsed: plan_type={plan_type}, day_or_date={day_or_date}")
        
        # Получаем время сообщения в UTC
        message_date_utc = None
        if message.date:
            message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc)
        
        # Вызываем process_plan
        result = process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
        if result == 'NEEDS_TIMEZONE':
            # Нужно уточнить часовой пояс
            show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
            return
        elif result:
            logger.info(f"[RANDOM PLAN] Plan created successfully for link={link}")
        else:
            logger.warning(f"[RANDOM PLAN] process_plan returned False for link={link}")
    except Exception as e:
        logger.error(f"[RANDOM PLAN] Error processing plan reply: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Ошибка при планировании. Используйте формат: <code>дома в субботу</code> или <code>в кино 15 февраля</code>", parse_mode='HTML')
        except:
            pass

# Обработка оценок текстом
@bot.message_handler(func=lambda m: m.text and m.text.isdigit() and 1 <= int(m.text) <= 10 and m.reply_to_message)
def handle_rating(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    rating = int(message.text)
    
    film_id = None
    
    # Проверяем реплай на сообщение о просмотренном фильме
    if message.reply_to_message:
        reply_msg_id = message.reply_to_message.message_id
        film_id = rating_messages.get(reply_msg_id)
        
        # Если не найдено, проверяем цепочку реплаев - может быть реплай на сообщение, которое само является реплаем
        if not film_id and message.reply_to_message.reply_to_message:
            parent_reply_msg_id = message.reply_to_message.reply_to_message.message_id
            film_id = rating_messages.get(parent_reply_msg_id)
            if not film_id:
                reply_link = bot_messages.get(parent_reply_msg_id)
                if reply_link:
                    # Извлекаем kp_id из ссылки для поиска
                    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                    if match:
                        kp_id = match.group(2)
                        with db_lock:
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                            row = cursor.fetchone()
                            if row:
                                film_id = row.get('id') if isinstance(row, dict) else row[0]
        
        # Если не найдено, проверяем реплай на исходное сообщение с фильмом
        if not film_id:
            reply_link = bot_messages.get(reply_msg_id)
            if reply_link:
                # Извлекаем kp_id из ссылки для поиска
                match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                if match:
                    kp_id = match.group(2)
                    with db_lock:
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
    
    if film_id:
        try:
            with db_lock:
                try:
                    # Проверяем, не в состоянии ли ошибки транзакция
                    try:
                        cursor.execute('SELECT 1')
                        cursor.fetchone()
                    except:
                        conn.rollback()
                    
                    cursor.execute('''
                        INSERT INTO ratings (chat_id, film_id, user_id, rating)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating
                    ''', (chat_id, film_id, user_id, rating))
                    conn.commit()
                    
                    cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                    avg_row = cursor.fetchone()
                    avg = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row and len(avg_row) > 0 else None)
                    
                    avg_str = f"{avg:.1f}" if avg else "—"
                    bot.reply_to(message, f"Спасибо! Ваша оценка {rating}/10 сохранена.\nСредняя: {avg_str}/10")
                except Exception as db_error:
                    conn.rollback()
                    logger.error(f"Ошибка при сохранении оценки: {db_error}", exc_info=True)
                    bot.reply_to(message, "Произошла ошибка при сохранении оценки. Попробуйте позже.")
        except Exception as e:
            logger.error(f"Критическая ошибка в handle_rating: {e}", exc_info=True)
        
        # Удаляем из rating_messages после сохранения
        if message.reply_to_message:
            rating_messages.pop(message.reply_to_message.message_id, None)
    else:
        bot.reply_to(message, "❌ Оценка не привязана к фильму. Ответьте на сообщение о просмотренном фильме или на сообщение с фильмом.")

# Обработка голосований "в кино"
@bot.message_handler(func=lambda m: m.text and m.text.lower() in ['да', 'нет'] and m.reply_to_message)
def handle_cinema_vote(message):
    chat_id = message.chat.id
    user_id = message.from_user.id
    vote = message.text.lower()
    
    with db_lock:
        cursor.execute('''
            SELECT film_id, deadline, yes_users, no_users
            FROM cinema_votes
            WHERE chat_id = %s AND message_id = %s
        ''', (chat_id, message.reply_to_message.message_id))
        row = cursor.fetchone()
        if not row:
            return
        film_id, deadline, yes_json, no_json = row
        
        if datetime.now(plans_tz).isoformat() > deadline:
            bot.reply_to(message, "Голосование завершено.")
            return
        
        yes_users = json.loads(yes_json or '[]')
        no_users = json.loads(no_json or '[]')
        
        if user_id in yes_users or user_id in no_users:
            bot.reply_to(message, "Вы уже голосовали.")
            return
        
        if vote == 'да':
            yes_users.append(user_id)
        else:
            no_users.append(user_id)
        
        cursor.execute('''
            UPDATE cinema_votes
            SET yes_users = %s, no_users = %s
            WHERE chat_id = %s AND film_id = %s
        ''', (json.dumps(yes_users), json.dumps(no_users), chat_id, film_id))
        conn.commit()
        
        bot.reply_to(message, "Ответ принят!")
        logger.info(f"Голос '{vote}' сохранён для фильма {film_id} от пользователя {user_id}")

# Состояние пагинации для /list
user_list_state = {}  # user_id: {'page': int, 'total_pages': int, 'chat_id': int}

def show_list_page(chat_id, user_id, page=1, message_id=None):
    """Показывает страницу списка фильмов"""
    try:
        MOVIES_PER_PAGE = 15
        
        with db_lock:
            # Получаем все непросмотренные фильмы, отсортированные по алфавиту
            cursor.execute('SELECT id, kp_id, title, year, genres, link FROM movies WHERE chat_id = %s AND watched = 0 ORDER BY title', (chat_id,))
            rows = cursor.fetchall()
        
        if not rows:
            text = "⏳ Нет непросмотренных фильмов!"
            markup = None
        else:
            total_movies = len(rows)
            total_pages = (total_movies + MOVIES_PER_PAGE - 1) // MOVIES_PER_PAGE  # Округление вверх
            page = max(1, min(page, total_pages))  # Ограничиваем страницу
            
            # Вычисляем диапазон фильмов для текущей страницы
            start_idx = (page - 1) * MOVIES_PER_PAGE
            end_idx = min(start_idx + MOVIES_PER_PAGE, total_movies)
            page_movies = rows[start_idx:end_idx]
            
            # Формируем текст страницы
            text = f"⏳ Непросмотренные фильмы (страница {page}/{total_pages}):\n\n"
            for row in page_movies:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                kp_id = row.get('kp_id') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                title = row.get('title') if isinstance(row, dict) else row[2]
                year = row.get('year') if isinstance(row, dict) else (row[3] if len(row) > 3 else '—')
                genres = row.get('genres') if isinstance(row, dict) else (row[4] if len(row) > 4 else None)
                link = row.get('link') if isinstance(row, dict) else (row[5] if len(row) > 5 else '')
                
                # Извлекаем первый жанр
                first_genre = None
                if genres and genres != '—' and genres.strip():
                    genres_list = [g.strip() for g in genres.split(',')]
                    if genres_list:
                        first_genre = genres_list[0]
                
                # Используем kp_id если есть, иначе film_id
                movie_id = kp_id or film_id
                genre_str = f" • {first_genre}" if first_genre else ""
                text += f"• <b>{title}</b> ({year}){genre_str} [ID: {movie_id}]\n<a href='{link}'>{link}</a>\n\n"
            
            text += "\n<i>В ответном сообщении пришлите ID фильмов, и они будут отмечены как просмотренные</i>"
            
            # Создаем кнопки пагинации
            markup = InlineKeyboardMarkup(row_width=10)
            buttons = []
            for p in range(1, total_pages + 1):
                label = f"•{p}" if p == page else str(p)
                buttons.append(InlineKeyboardButton(label, callback_data=f"list_page:{p}"))
            # Разбиваем кнопки на строки по 10 штук
            for i in range(0, len(buttons), 10):
                markup.row(*buttons[i:i+10])
            
            # Сохраняем состояние
            user_list_state[user_id] = {
                'page': page,
                'total_pages': total_pages,
                'chat_id': chat_id
            }
        
        if message_id:
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
                # Обновляем message_id в list_messages для обработки ответов
                list_messages[message_id] = chat_id
            except Exception as e:
                logger.error(f"[LIST] Ошибка редактирования сообщения: {e}", exc_info=True)
                msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
                list_messages[msg.message_id] = chat_id
        else:
            msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
            # Сохраняем message_id для обработки ответов
            list_messages[msg.message_id] = chat_id
            return msg.message_id
    except Exception as e:
        logger.error(f"[LIST] Ошибка в show_list_page: {e}", exc_info=True)
        return None

# /list — только непросмотренные
@bot.message_handler(commands=['list'])
def list_movies(message):
    logger.info(f"[HANDLER] /list вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/list', message.chat.id)
        logger.info(f"Команда /list от пользователя {message.from_user.id}")
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        show_list_page(chat_id, user_id, page=1)
        logger.info(f"✅ Ответ на /list отправлен пользователю {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /list: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /list")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("list_page:"))
def handle_list_page(call):
    """Обработчик переключения страниц в /list"""
    try:
        user_id = call.from_user.id
        page = int(call.data.split(":")[1])
        
        state = user_list_state.get(user_id)
        if not state:
            bot.answer_callback_query(call.id, "Сессия устарела. Используйте /list заново")
            return
        
        chat_id = state['chat_id']
        show_list_page(chat_id, user_id, page, call.message.message_id)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[LIST] Ошибка в handle_list_page: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка переключения страницы")
        except:
            pass

# Обработчик ответов на /list для отметки фильмов как просмотренных
@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in list_messages)
def handle_list_reply(message):
    """Обрабатывает ответ на сообщение /list с ID фильмов для отметки как просмотренных"""
    try:
        logger.info(f"[LIST REPLY] Получен ответ на /list от пользователя {message.from_user.id}, reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}")
        chat_id = list_messages.get(message.reply_to_message.message_id)
        if not chat_id:
            logger.warning(f"[LIST REPLY] Не найден chat_id для message_id={message.reply_to_message.message_id if message.reply_to_message else None}, list_messages keys: {list(list_messages.keys())}")
            return
        logger.info(f"[LIST REPLY] Обработка ответа для chat_id={chat_id}, текст: {message.text}")
        
        # Парсим ID фильмов из сообщения (используем kp_id, как в /rate)
        text = message.text.strip()
        # Извлекаем все числа из текста (это будут kp_id)
        kp_ids = re.findall(r'\d+', text)
        
        if not kp_ids:
            bot.reply_to(message, "Не найдены ID фильмов. Отправьте список ID кинопоиска через запятую или пробел.")
            return
        
        marked_count = 0
        with db_lock:
            for kp_id_str in kp_ids:
                try:
                    kp_id = kp_id_str.strip()
                    # Проверяем, что фильм существует и не просмотрен по kp_id
                    cursor.execute('SELECT id, title, watched FROM movies WHERE kp_id = %s AND chat_id = %s', (kp_id, chat_id))
                    row = cursor.fetchone()
                    if row:
                        film_id_db = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        watched = row.get('watched') if isinstance(row, dict) else row[2]
                        
                        if not watched:
                            cursor.execute('UPDATE movies SET watched = 1 WHERE kp_id = %s AND chat_id = %s', (kp_id, chat_id))
                            marked_count += 1
                            logger.info(f"Фильм {film_id_db} ({title}, kp_id: {kp_id}) отмечен как просмотренный в чате {chat_id}")
                except ValueError:
                    continue
                except Exception as e:
                    logger.error(f"Ошибка при отметке фильма {film_id_str}: {e}", exc_info=True)
                    continue
            
            if marked_count > 0:
                conn.commit()
                bot.reply_to(message, f"✅ Отмечено как просмотрено: {marked_count} фильм(ов).\n\nТеперь вы можете оценить их командой /rate")
            else:
                bot.reply_to(message, "Не удалось отметить фильмы. Проверьте, что ID корректны и фильмы не были просмотрены ранее.")
    except Exception as e:
        logger.error(f"❌ Ошибка в handle_list_reply: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке списка фильмов.")
        except:
            pass

# /total — расширенная статистика
@bot.message_handler(commands=['stats'])
def stats_command(message):
    logger.info(f"[HANDLER] /stats вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/stats', message.chat.id)
        logger.info(f"Команда /stats от пользователя {message.from_user.id}, chat_id={message.chat.id}")
        chat_id = message.chat.id
        
        with db_lock:
            # Получаем всех участников из разных источников: stats, ratings, watched_movies, plans
            all_users = {}
            
            # Из stats (команды)
            cursor.execute('''
                SELECT 
                    user_id,
                    username,
                    COUNT(*) as command_count,
                    MAX(timestamp) as last_activity
                FROM stats
                WHERE chat_id = %s AND user_id IS NOT NULL
                GROUP BY user_id, username
            ''', (chat_id,))
            for row in cursor.fetchall():
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                username = row.get('username') if isinstance(row, dict) else row[1]
                command_count = row.get('command_count') if isinstance(row, dict) else row[2]
                last_activity = row.get('last_activity') if isinstance(row, dict) else row[3]
                all_users[user_id] = {
                    'username': username,
                    'command_count': command_count,
                    'last_activity': last_activity
                }
            
            # Из ratings (оценки)
            cursor.execute('''
                SELECT DISTINCT user_id
                FROM ratings
                WHERE chat_id = %s AND user_id IS NOT NULL
            ''', (chat_id,))
            for row in cursor.fetchall():
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                if user_id not in all_users:
                    all_users[user_id] = {
                        'username': None,
                        'command_count': 0,
                        'last_activity': None
                    }
            
            # Из watched_movies (просмотренные фильмы)
            cursor.execute('''
                SELECT DISTINCT user_id
                FROM watched_movies
                WHERE chat_id = %s AND user_id IS NOT NULL
            ''', (chat_id,))
            for row in cursor.fetchall():
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                if user_id not in all_users:
                    all_users[user_id] = {
                        'username': None,
                        'command_count': 0,
                        'last_activity': None
                    }
            
            # Из plans (планы)
            cursor.execute('''
                SELECT DISTINCT user_id
                FROM plans
                WHERE chat_id = %s AND user_id IS NOT NULL
            ''', (chat_id,))
            for row in cursor.fetchall():
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                if user_id not in all_users:
                    all_users[user_id] = {
                        'username': None,
                        'command_count': 0,
                        'last_activity': None
                    }
            
            # Преобразуем в список и сортируем
            users_stats = []
            for user_id, data in all_users.items():
                users_stats.append({
                    'user_id': user_id,
                    'username': data['username'],
                    'command_count': data['command_count'],
                    'last_activity': data['last_activity']
                })
            
            # Сортируем по количеству команд и последней активности
            users_stats.sort(key=lambda x: (x['command_count'], x['last_activity'] or ''), reverse=True)
            
            # Получаем общую статистику чата (исключаем фильмы, добавленные только через импорт)
            # Фильм считается импортированным, если у него есть только импортированные оценки
            cursor.execute('''
                SELECT COUNT(*) FROM movies m
                WHERE m.chat_id = %s
                AND NOT EXISTS (
                    SELECT 1 FROM ratings r 
                    WHERE r.chat_id = m.chat_id 
                    AND r.film_id = m.id 
                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                )
                AND EXISTS (
                    SELECT 1 FROM ratings r 
                    WHERE r.chat_id = m.chat_id 
                    AND r.film_id = m.id 
                    AND r.is_imported = TRUE
                )
            ''', (chat_id,))
            imported_movies_row = cursor.fetchone()
            imported_movies_count = imported_movies_row.get('count') if isinstance(imported_movies_row, dict) else (imported_movies_row[0] if imported_movies_row else 0)
            
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
            total_movies_row = cursor.fetchone()
            total_movies_all = total_movies_row.get('count') if isinstance(total_movies_row, dict) else (total_movies_row[0] if total_movies_row else 0)
            total_movies = total_movies_all - imported_movies_count
            
            cursor.execute('''
                SELECT COUNT(*) FROM movies m
                WHERE m.chat_id = %s AND m.watched = 1
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND r.is_imported = TRUE
                    )
                )
            ''', (chat_id,))
            watched_movies_row = cursor.fetchone()
            watched_movies = watched_movies_row.get('count') if isinstance(watched_movies_row, dict) else (watched_movies_row[0] if watched_movies_row else 0)
            
            # Исключаем импортированные оценки
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
            total_ratings_row = cursor.fetchone()
            total_ratings = total_ratings_row.get('count') if isinstance(total_ratings_row, dict) else (total_ratings_row[0] if total_ratings_row else 0)
            
            cursor.execute('SELECT COUNT(*) FROM plans WHERE chat_id = %s', (chat_id,))
            total_plans_row = cursor.fetchone()
            total_plans = total_plans_row.get('count') if isinstance(total_plans_row, dict) else (total_plans_row[0] if total_plans_row else 0)
            
            # Получаем статистику по оценкам участников (исключаем импортированные)
            cursor.execute('''
                SELECT 
                    r.user_id,
                    COUNT(*) as ratings_count,
                    AVG(r.rating) as avg_rating
                FROM ratings r
                WHERE r.chat_id = %s AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                GROUP BY r.user_id
                ORDER BY ratings_count DESC
            ''', (chat_id,))
            ratings_stats = cursor.fetchall()
            ratings_by_user = {}
            for row in ratings_stats:
                user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                count = row.get('ratings_count') if isinstance(row, dict) else row[1]
                avg = row.get('avg_rating') if isinstance(row, dict) else row[2]
                ratings_by_user[user_id] = {'count': count, 'avg': avg}
        
        # Формируем сообщение
        text = "📊 <b>Детальная статистика группы</b>\n\n"
        
        # Общая статистика
        text += "📈 <b>Общая статистика:</b>\n"
        text += f"• Всего фильмов: <b>{total_movies}</b>\n"
        text += f"• Просмотрено: <b>{watched_movies}</b>\n"
        text += f"• Всего оценок: <b>{total_ratings}</b>\n"
        text += f"• Запланировано: <b>{total_plans}</b>\n\n"
        
        # Статистика по участникам
        if users_stats:
            text += "👥 <b>Участники группы:</b>\n"
            for idx, user_row in enumerate(users_stats[:10], 1):  # Показываем топ-10
                # users_stats теперь список словарей
                user_id = user_row.get('user_id')
                username = user_row.get('username')
                command_count = user_row.get('command_count', 0)
                
                user_display = username or f"user_{user_id}"
                rating_info = ratings_by_user.get(user_id, {})
                if rating_info:
                    text += f"{idx}. <b>{user_display}</b>\n"
                    text += f"   • Команд: {command_count}\n"
                    text += f"   • Оценок: {rating_info.get('count', 0)}\n"
                    if rating_info.get('avg'):
                        text += f"   • Средняя оценка: {rating_info['avg']:.1f}/10\n"
                else:
                    text += f"{idx}. <b>{user_display}</b>\n"
                    text += f"   • Команд: {command_count}\n"
                text += "\n"
            
            if len(users_stats) > 10:
                text += f"<i>... и ещё {len(users_stats) - 10} участников</i>\n"
        else:
            text += "👥 <i>Нет данных об участниках</i>\n"
        
        bot.reply_to(message, text, parse_mode='HTML')
        logger.info(f"✅ Ответ на /stats отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /stats: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /stats")
        except Exception as reply_error:
            logger.error(f"❌ Ошибка при отправке сообщения об ошибке: {reply_error}", exc_info=True)

# /join — регистрация участника группы
@bot.message_handler(commands=['join'])
def join_command(message):
    logger.info(f"[HANDLER] /join вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/join', message.chat.id)
        logger.info(f"Команда /join от пользователя {message.from_user.id}, chat_id={message.chat.id}")
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Регистрируем текущего пользователя
        registered_users = [{'user_id': user_id, 'username': username}]
        
        # Парсим текст сообщения для поиска упоминаний
        text = message.text or ""
        logger.info(f"[JOIN] Текст сообщения: {text}")
        
        # Извлекаем упоминания из entities (если есть реальные упоминания пользователей)
        mentioned_user_ids = set()
        if message.entities:
            for entity in message.entities:
                if entity.type == 'mention' and hasattr(entity, 'user') and entity.user:
                    mentioned_user = entity.user
                    mentioned_user_ids.add(mentioned_user.id)
                    mentioned_username = mentioned_user.username or f"user_{mentioned_user.id}"
                    registered_users.append({
                        'user_id': mentioned_user.id,
                        'username': mentioned_username
                    })
                    logger.info(f"[JOIN] Найдено упоминание через entity: user_id={mentioned_user.id}, username={mentioned_username}")
        
        # Также парсим текст для поиска @username (на случай, если entities не сработали)
        # Разбиваем по пробелам и знакам препинания
        import re
        # Ищем все @username в тексте
        text_mentions = re.findall(r'@(\w+)', text)
        logger.info(f"[JOIN] Найдено упоминаний в тексте: {text_mentions}")
        
        # Если есть упоминания в тексте, но их нет в entities, пытаемся найти через get_chat_member
        for mention_username in text_mentions:
            # Пропускаем, если уже зарегистрировали через entities
            found_in_entities = False
            for reg_user in registered_users:
                if reg_user['username'].lower() == mention_username.lower():
                    found_in_entities = True
                    break
            
            if not found_in_entities:
                # Пытаемся найти пользователя в группе по username
                try:
                    # В группах можно попробовать найти через поиск, но это ограничено
                    # Пока просто сохраняем username для будущего сопоставления
                    logger.info(f"[JOIN] Упоминание @{mention_username} найдено в тексте, но user_id неизвестен")
                except Exception as e:
                    logger.warning(f"[JOIN] Не удалось обработать упоминание @{mention_username}: {e}")
        
        # Регистрируем всех найденных пользователей
        response_text = "✅ Зарегистрированы участники:\n"
        for reg_user in registered_users:
            log_request(reg_user['user_id'], reg_user['username'], '/join', chat_id)
            response_text += f"• @{reg_user['username']}\n"
        
        if len(registered_users) == 1:
            response_text = f"✅ Вы зарегистрированы как участник группы!\n\nТеперь вы будете учитываться в статистике /stats."
        else:
            response_text += "\nТеперь вы будете учитываться в статистике /stats."
        
        bot.reply_to(message, response_text)
        logger.info(f"✅ Зарегистрировано пользователей через /join: {len(registered_users)}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /join: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /join")
        except:
            pass

@bot.message_handler(commands=['total'])
def total_stats(message):
    logger.info(f"[HANDLER] /total вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/total', message.chat.id)
        logger.info(f"Команда /total от пользователя {message.from_user.id}")
        chat_id = message.chat.id
        with db_lock:
            # Исключаем фильмы, добавленные только через импорт
            cursor.execute('''
                SELECT COUNT(*) as count FROM movies m
                WHERE m.chat_id = %s
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND r.is_imported = TRUE
                    )
                )
            ''', (chat_id,))
            total_row = cursor.fetchone()
            total = total_row.get('count') if isinstance(total_row, dict) else (total_row[0] if total_row and len(total_row) > 0 else 0)
            
            cursor.execute('''
                SELECT COUNT(*) as count FROM movies m
                WHERE m.chat_id = %s AND m.watched = 1
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND r.is_imported = TRUE
                    )
                )
            ''', (chat_id,))
            watched_row = cursor.fetchone()
            watched = watched_row.get('count') if isinstance(watched_row, dict) else (watched_row[0] if watched_row and len(watched_row) > 0 else 0)
            unwatched = total - watched
            
            # Если нет данных, отправляем сообщение
            if total == 0:
                bot.reply_to(message, "📊 Нет данных о вашей статистике.\n\nОцените первый фильм, чтобы статистика начала собираться.")
                return

            # Жанры (исключаем импортированные фильмы)
            cursor.execute('''
                SELECT m.genres FROM movies m
                WHERE m.chat_id = %s AND m.watched = 1
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND r.is_imported = TRUE
                    )
                )
            ''', (chat_id,))
            genre_counts = {}
            for row in cursor.fetchall():
                genres = row.get('genres') if isinstance(row, dict) else row[0]
                if genres:
                    for g in str(genres).split(', '):
                        if g.strip():
                            genre_counts[g.strip()] = genre_counts.get(g.strip(), 0) + 1
            fav_genre = max(genre_counts, key=genre_counts.get) if genre_counts else "—"

            # Режиссёры - используем оценки из таблицы ratings (исключаем импортированные)
            cursor.execute('''
                SELECT m.director, AVG(r.rating) as avg_rating, COUNT(DISTINCT m.id) as film_count
                FROM movies m
                LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                WHERE m.chat_id = %s AND m.watched = 1 AND m.director IS NOT NULL AND m.director != %s
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r2 
                        WHERE r2.chat_id = m.chat_id 
                        AND r2.film_id = m.id 
                        AND (r2.is_imported = FALSE OR r2.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r3 
                        WHERE r3.chat_id = m.chat_id 
                        AND r3.film_id = m.id 
                        AND r3.is_imported = TRUE
                    )
                )
                GROUP BY m.director
            ''', (chat_id, 'Не указан'))
            director_stats = {}
            for row in cursor.fetchall():
                d = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                avg_r = row.get('avg_rating') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                film_count = row.get('film_count') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0)
                if d and avg_r:  # Только если есть неимпортированные оценки
                    director_stats[d] = {
                        'count': film_count,
                        'sum_rating': (avg_r * film_count) if avg_r else 0,
                        'avg_rating': avg_r if avg_r else 0
                    }
            top_directors = sorted(director_stats.items(), key=lambda x: (-x[1]['count'], -x[1]['avg_rating']))[:3]

            # Актёры - используем оценки из таблицы ratings (исключаем импортированные)
            cursor.execute('''
                SELECT m.actors, AVG(r.rating) as avg_rating, COUNT(DISTINCT m.id) as film_count
                FROM movies m
                LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                WHERE m.chat_id = %s AND m.watched = 1
                AND NOT (
                    NOT EXISTS (
                        SELECT 1 FROM ratings r2 
                        WHERE r2.chat_id = m.chat_id 
                        AND r2.film_id = m.id 
                        AND (r2.is_imported = FALSE OR r2.is_imported IS NULL)
                    )
                    AND EXISTS (
                        SELECT 1 FROM ratings r3 
                        WHERE r3.chat_id = m.chat_id 
                        AND r3.film_id = m.id 
                        AND r3.is_imported = TRUE
                    )
                )
                GROUP BY m.actors
            ''', (chat_id,))
            actor_stats = {}
            for row in cursor.fetchall():
                actors_str = row.get('actors') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                avg_r = row.get('avg_rating') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                film_count = row.get('film_count') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0)
                if actors_str and avg_r:  # Только если есть неимпортированные оценки
                    for a in actors_str.split(', '):
                        a = a.strip()
                        if a and a != "—":
                            if a not in actor_stats:
                                actor_stats[a] = {'count': 0, 'sum_rating': 0, 'total_ratings': 0}
                            # Для актеров считаем количество фильмов, где они участвовали
                            actor_stats[a]['count'] += film_count
                            # Суммируем средние оценки, умноженные на количество фильмов
                            if avg_r:
                                actor_stats[a]['sum_rating'] += avg_r * film_count
                                actor_stats[a]['total_ratings'] += film_count
            
            # Пересчитываем средние для актеров
            for actor in actor_stats:
                if actor_stats[actor]['total_ratings'] > 0:
                    actor_stats[actor]['avg_rating'] = actor_stats[actor]['sum_rating'] / actor_stats[actor]['total_ratings']
                else:
                    actor_stats[actor]['avg_rating'] = 0
            
            top_actors = sorted(actor_stats.items(), key=lambda x: (-x[1]['count'], -x[1].get('avg_rating', 0)))[:3]

            # Рассчитываем среднее из ratings (исключаем импортированные)
            cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
            avg_row = cursor.fetchone()
            avg_rating = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row and len(avg_row) > 0 else None)
            avg_str = f"{avg_rating:.1f}/10" if avg_rating else "—"

        text = f"📊 <b>Статистика кино-группы</b>\n\n"
        text += f"🎬 Всего фильмов: <b>{total}</b>\n"
        text += f"✅ Просмотрено: <b>{watched}</b>\n"
        text += f"⏳ Ждёт просмотра: <b>{unwatched}</b>\n"
        text += f"🌟 Средняя оценка: <b>{avg_str}</b>\n"
        text += f"❤️ Любимый жанр: <b>{fav_genre}</b>\n\n"
        text += "<b>Топ режиссёров:</b>\n"
        for d, stats in top_directors:
            avg_d = stats.get('avg_rating', 0) if stats.get('avg_rating') else 0
            text += f"• {d} — {stats['count']} фильм(ов), средняя {avg_d:.1f}/10\n"
        text += "\n<b>Топ актёров:</b>\n"
        for a, stats in top_actors:
            avg_a = stats.get('avg_rating', 0) if stats.get('avg_rating') else 0
            text += f"• {a} — {stats['count']} фильм(ов), средняя {avg_a:.1f}/10\n"

        bot.reply_to(message, text, parse_mode='HTML')
        logger.info(f"✅ Ответ на /total отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /total: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /total")
        except:
            pass

# /search — поиск фильмов
@bot.message_handler(commands=['search'])
def handle_search(message):
    logger.info(f"[HANDLER] /search вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/search', message.chat.id)
        
        query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None
        if not query:
            bot.reply_to(message, "❌ Укажите запрос, например: /search джон уик")
            return
        
        logger.info(f"Команда /search от пользователя {message.from_user.id}, запрос: {query}")
        
        films, total_pages = search_films(query, page=1)
        if not films:
            bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for film in films[:10]:  # Показываем максимум 10 результатов на странице
            # Пробуем разные варианты полей для совместимости с разными версиями API
            title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
            year = film.get('year') or film.get('releaseYear') or 'N/A'
            rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            logger.info(f"[SEARCH] Фильм: title={title}, year={year}, kp_id={kp_id}")
            
            if kp_id:
                # Ограничиваем длину текста кнопки
                button_text = f"{title} ({year})"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                results_text += f"• <b>{title}</b> ({year})"
                if rating != 'N/A':
                    results_text += f" ⭐ {rating}"
                results_text += "\n"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}"))
            else:
                logger.warning(f"[SEARCH] Фильм без ID: {film}")
        
        # Добавляем пагинацию, если нужно
        if total_pages > 1:
            pagination_row = []
            # Кодируем запрос для callback_data (заменяем пробелы на подчеркивания)
            query_encoded = query.replace(' ', '_')
            pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
            if total_pages > 1:
                pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
            markup.row(*pagination_row)
        
        bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"✅ Ответ на /search отправлен пользователю {message.from_user.id}, найдено {len(films)} результатов")
    except Exception as e:
        logger.error(f"❌ Ошибка в /search: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /search")
        except:
            pass

# /rate
@bot.message_handler(commands=['rate'])
def rate_movie(message):
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/rate', message.chat.id)
    logger.info(f"Команда /rate от пользователя {message.from_user.id}")
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, есть ли аргументы в команде
    text = message.text or ""
    parts = text.split(None, 2)  # Разбиваем на максимум 3 части: /rate, kp_id/url, rating
    
    if len(parts) >= 3:
        # Есть аргументы - пытаемся поставить оценку напрямую
        kp_id_or_url = parts[1]
        rating_str = parts[2]
        
        # Извлекаем kp_id
        kp_id = extract_kp_id_from_text(kp_id_or_url)
        if not kp_id:
            bot.reply_to(message, "❌ Не удалось распознать kp_id. Используйте формат:\n<code>/rate 81682 10</code>\nили\n<code>/rate https://www.kinopoisk.ru/film/81682/ 10</code>", parse_mode='HTML')
            return
        
        # Парсим оценку
        try:
            rating = int(rating_str.strip())
            if not (1 <= rating <= 10):
                bot.reply_to(message, "❌ Оценка должна быть от 1 до 10")
                return
        except ValueError:
            bot.reply_to(message, "❌ Неверный формат оценки. Используйте число от 1 до 10")
            return
        
        # Ищем фильм в базе
        with db_lock:
            cursor.execute('''
                SELECT id, title FROM movies
                WHERE chat_id = %s AND kp_id = %s AND watched = 1
            ''', (chat_id, kp_id))
            film_row = cursor.fetchone()
            
            if not film_row:
                bot.reply_to(message, f"❌ Фильм с kp_id={kp_id} не найден в базе или не помечен как просмотренный")
                return
            
            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
            title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
            
            # Проверяем, не оценил ли уже пользователь этот фильм
            cursor.execute('''
                SELECT rating FROM ratings
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            ''', (chat_id, film_id, user_id))
            existing = cursor.fetchone()
            
            if existing:
                old_rating = existing.get('rating') if isinstance(existing, dict) else existing[0]
                # Обновляем оценку
                cursor.execute('''
                    UPDATE ratings SET rating = %s, is_imported = FALSE
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (rating, chat_id, film_id, user_id))
                conn.commit()
                bot.reply_to(message, f"✅ Оценка обновлена!\n\n<b>{title}</b>\nСтарая оценка: {old_rating}/10\nНовая оценка: {rating}/10", parse_mode='HTML')
                logger.info(f"[RATE] Пользователь {user_id} обновил оценку для фильма {kp_id} с {old_rating} на {rating}")
            else:
                # Сохраняем новую оценку
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating)
                    VALUES (%s, %s, %s, %s)
                ''', (chat_id, film_id, user_id, rating))
                conn.commit()
                bot.reply_to(message, f"✅ Оценка сохранена!\n\n<b>{title}</b>\nОценка: {rating}/10", parse_mode='HTML')
                logger.info(f"[RATE] Пользователь {user_id} поставил оценку {rating} для фильма {kp_id}")
        
        return
    
    # Если аргументов нет - показываем список как раньше
    # Получаем все просмотренные фильмы (максимум 10)
    with db_lock:
        cursor.execute('''
            SELECT m.id, m.kp_id, m.title, m.year
            FROM movies m
            WHERE m.chat_id = %s AND m.watched = 1
            ORDER BY m.title
            LIMIT 10
        ''', (chat_id,))
        movies = cursor.fetchall()
    
    if not movies:
        bot.reply_to(message, "Нет просмотренных фильмов.")
        return
    
    # Получаем всех пользователей чата из stats (внутри db_lock)
    with db_lock:
        cursor.execute('''
            SELECT DISTINCT user_id, username
            FROM stats
            WHERE chat_id = %s AND user_id IS NOT NULL
        ''', (chat_id,))
        chat_users = {}
        for row in cursor.fetchall():
            user_id = row.get('user_id') if isinstance(row, dict) else row[0]
            username = row.get('username') if isinstance(row, dict) else row[1]
            chat_users[user_id] = username or f"user_{user_id}"
    
    # Для каждого фильма находим, кто не оценил
    text = "📊 <b>Список просмотренных фильмов для оценки:</b>\n\n"
    text += "💬 <i>Ответьте на это сообщение списком оценок в формате:</i>\n"
    text += "<code>kp_id оценка</code>\n\n"
    text += "<i>Пример:</i>\n"
    text += "<code>123 10\n31341 8\n123123 4</code>\n\n"
    text += "=" * 40 + "\n\n"
    
    for movie in movies:
        # RealDictCursor возвращает словари, но поддерживает доступ по индексу
        film_id = movie.get('id') if isinstance(movie, dict) else movie[0]
        kp_id = movie.get('kp_id') if isinstance(movie, dict) else movie[1]
        title = movie.get('title') if isinstance(movie, dict) else movie[2]
        year = (movie.get('year') if isinstance(movie, dict) else movie[3]) or '—'
        
        # Получаем всех, кто оценил этот фильм
        cursor.execute('''
            SELECT user_id FROM ratings
            WHERE chat_id = %s AND film_id = %s
        ''', (chat_id, film_id))
        rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
        
        # Находим, кто не оценил
        not_rated = []
        for user_id, username in chat_users.items():
            if user_id not in rated_users:
                not_rated.append(username)
        
        not_rated_text = ", ".join(not_rated[:5])
        if len(not_rated) > 5:
            not_rated_text += f" и ещё {len(not_rated) - 5}"
        
        # Формируем ссылку на кинопоиск
        kp_link = f"https://kinopoisk.ru/film/{kp_id}"
        text += f"<b>{kp_id}</b> — <a href=\"{kp_link}\">{title}</a> ({year})\n"
        if not_rated:
            text += f"   ⚠️ Не оценили: {not_rated_text}\n"
        else:
            text += f"   ✅ Все оценили\n"
        text += "\n"
    
    # Отправляем сообщение и сохраняем его message_id для обработки реплая
    sent_msg = bot.reply_to(message, text, parse_mode='HTML')
    rate_list_messages[message.chat.id] = sent_msg.message_id

# Обработка реплая на список фильмов с оценками

@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == bot.get_me().id and m.text, priority=2)
def handle_rate_list_reply(message):
    # Пропускаем команды - они обрабатываются отдельными обработчиками
    if message.text and message.text.startswith('/'):
        logger.info(f"[RATE LIST REPLY] Пропущена команда: {message.text[:50]}")
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, что это реплай на список фильмов
    reply_text = message.reply_to_message.text or ""
    if "Список просмотренных фильмов для оценки" not in reply_text:
        return
    
    # Дополнительная проверка по message_id
    expected_msg_id = rate_list_messages.get(chat_id)
    if expected_msg_id and message.reply_to_message.message_id != expected_msg_id:
        return
    
    text = message.text.strip()
    if not text:
        return
    
    # Парсим оценки: kp_id оценка (разделители: пробел, запятая, точка с запятой, таб)
    import re
    ratings_pattern = r'(\d+)\s*[,;:\t]?\s*(\d+)'
    matches = re.findall(ratings_pattern, text)
    
    if not matches:
        bot.reply_to(message, "❌ Не удалось распознать оценки. Используйте формат: <code>kp_id оценка</code>", parse_mode='HTML')
        return
    
    results = []
    errors = []
    
    with db_lock:
        for kp_id_str, rating_str in matches:
            try:
                kp_id = kp_id_str.strip()
                rating = int(rating_str.strip())
                
                if not (1 <= rating <= 10):
                    errors.append(f"{kp_id}: оценка должна быть от 1 до 10")
                    continue
                
                # Находим фильм по kp_id
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, kp_id))
                film_row = cursor.fetchone()
                
                if not film_row:
                    errors.append(f"{kp_id}: фильм не найден или не просмотрен")
                    continue
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                # Проверяем, не оценил ли уже пользователь этот фильм
                cursor.execute('''
                    SELECT rating FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                existing = cursor.fetchone()
                
                if existing:
                    errors.append(f"{kp_id}: вы уже оценили этот фильм")
                    continue
                
                # Сохраняем оценку
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                
                results.append((kp_id, title, rating))
                
            except ValueError:
                errors.append(f"{kp_id_str}: неверный формат оценки")
            except Exception as e:
                logger.error(f"Ошибка при сохранении оценки {kp_id_str}: {e}")
                errors.append(f"{kp_id_str}: ошибка обработки")
        
        conn.commit()
    
    # Формируем ответ
    response_text = ""
    
    if results:
        user_name = message.from_user.first_name or f"user_{user_id}"
        response_text += f"✅ <b>{user_name}</b> поставил(а) оценки:\n\n"
        for kp_id, title, rating in results:
            response_text += f"• <b>{kp_id}</b> — {title}: {rating}/10\n"
        response_text += "\n"
    
    if errors:
        response_text += "⚠️ <b>Ошибки:</b>\n"
        for error in errors:
            response_text += f"• {error}\n"
    
    if not results and not errors:
        response_text = "❌ Не удалось обработать оценки. Проверьте формат."
    
    bot.reply_to(message, response_text, parse_mode='HTML')

# /settings
@bot.message_handler(commands=['settings'])
def settings_command(message):
    logger.info(f"[HANDLER] /settings вызван от {message.from_user.id}")
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/settings', chat_id)
        logger.info(f"Команда /settings от пользователя {user_id}")
        
        # Проверяем на reset
        if message.text and 'reset' in message.text.lower():
            with db_lock:
                cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
                conn.commit()
            bot.reply_to(message, "✅ Реакции сброшены к значению по умолчанию (✅)")
            logger.info(f"Реакции сброшены для чата {chat_id}")
            return
        
        # Сначала показываем меню выбора действия
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("😀 Настроить эмодзи просмотра", callback_data="settings:emoji"))
        markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
        markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
        
        sent = bot.send_message(chat_id,
            f"⚙️ <b>Настройки</b>\n\n"
            f"Выберите, что хотите настроить:",
            reply_markup=markup,
            parse_mode='HTML')
        
        logger.info(f"Настройки открыты для {user_id}, msg_id: {sent.message_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /settings: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /settings")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("timezone:"))
def handle_timezone_callback(call):
    """Обработчик выбора часового пояса"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        timezone_name = call.data.split(":", 1)[1]  # "Moscow" или "Serbia"
        
        if set_user_timezone(user_id, timezone_name):
            tz_display = "Москва" if timezone_name == "Moscow" else "Сербия"
            tz_obj = pytz.timezone('Europe/Moscow' if timezone_name == "Moscow" else 'Europe/Belgrade')
            current_time = datetime.now(tz_obj).strftime('%H:%M')
            
            bot.edit_message_text(
                f"✅ Часовой пояс установлен: <b>{tz_display}</b>\n\n"
                f"Текущее время: <b>{current_time}</b>\n\n"
                f"Все время будет отображаться и планироваться в часовом поясе {tz_display}.\n"
                f"Часовой пояс будет автоматически обновляться при путешествиях.",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML'
            )
            logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
        else:
            bot.answer_callback_query(call.id, "Ошибка сохранения часового пояса", show_alert=True)
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка в handle_timezone_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("settings:"))
def handle_settings_callback(call):
    """Обработчик callback для кнопок настроек"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        action = call.data.split(":", 1)[1]  # "emoji", "timezone", "import", "add", "replace", "reset" или "back"
        
        if action == "emoji":
            # Показываем настройки эмодзи
            current = get_watched_emojis(chat_id)
            current_emojis_str = ''.join(current) if isinstance(current, list) else str(current)
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("➕ Добавить к текущим", callback_data="settings:add"))
            markup.add(InlineKeyboardButton("🔄 Заменить полностью", callback_data="settings:replace"))
            markup.add(InlineKeyboardButton("🗑️ Сбросить", callback_data="settings:reset"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
            
            bot.edit_message_text(
                f"😀 <b>Настройка эмодзи просмотра</b>\n\n"
                f"<b>Текущие реакции:</b> {current_emojis_str}\n\n"
                f"Выберите действие или поставьте реакцию на это сообщение — она автоматически добавится к текущим.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            
            # Сохраняем состояние для обработки реакций
            user_settings_state[user_id] = {
                'settings_msg_id': call.message.message_id,
                'chat_id': chat_id,
                'adding_reactions': False
            }
            settings_messages[call.message.message_id] = {
                'user_id': user_id,
                'action': 'add',
                'chat_id': chat_id
            }
            return
        
        if action == "import":
            # Импорт базы из Кинопоиска
            user_import_state[user_id] = {
                'step': 'waiting_user_id',
                'kp_user_id': None,
                'count': None
            }
            bot.edit_message_text(
                f"📥 <b>Импорт базы из Кинопоиска</b>\n\n"
                f"Отправьте ID пользователя Кинопоиска или ссылку на профиль.\n\n"
                f"Примеры:\n"
                f"• <code>1931396</code>\n"
                f"• <code>https://www.kinopoisk.ru/user/1931396</code>",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML'
            )
            return
        
        if action == "back":
            # Возврат к главному меню settings
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("😀 Настроить эмодзи просмотра", callback_data="settings:emoji"))
            markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
            markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
            
            bot.edit_message_text(
                f"⚙️ <b>Настройки</b>\n\n"
                f"Выберите, что хотите настроить:",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "timezone":
            # Показываем выбор часового пояса
            current_tz = get_user_timezone(user_id)
            current_tz_name = "Москва" if not current_tz or current_tz.zone == 'Europe/Moscow' else "Сербия"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🇷🇺 Москва (Europe/Moscow)", callback_data="timezone:Moscow"))
            markup.add(InlineKeyboardButton("🇷🇸 Сербия (Europe/Belgrade)", callback_data="timezone:Serbia"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
            
            bot.edit_message_text(
                f"🕐 <b>Выбор часового пояса</b>\n\n"
                f"Текущий: <b>{current_tz_name}</b>\n\n"
                f"Выберите часовой пояс. Все время будет отображаться и планироваться в выбранном часовом поясе.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "back":
            # Возврат к главному меню settings
            current = get_watched_emojis(chat_id)
            # Преобразуем список эмодзи в строку для корректного отображения
            current_emojis_str = ''.join(current) if isinstance(current, list) else str(current)
            user_tz = get_user_timezone(user_id)
            current_tz = "Москва" if not user_tz or user_tz.zone == 'Europe/Moscow' else "Сербия"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("➕ Добавить к текущим", callback_data="settings:add"))
            markup.add(InlineKeyboardButton("🔄 Заменить полностью", callback_data="settings:replace"))
            markup.add(InlineKeyboardButton("🗑️ Сбросить", callback_data="settings:reset"))
            markup.add(InlineKeyboardButton(f"🕐 Часовой пояс: {current_tz}", callback_data="settings:timezone"))
            
            bot.edit_message_text(
                f"⚙️ <b>Настройки</b>\n\n"
                f"<b>Реакции:</b> {current_emojis_str}\n"
                f"<b>Часовой пояс:</b> {current_tz}\n\n"
                f"Выберите действие или поставьте реакцию на это сообщение — она автоматически добавится к текущим.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            return
        
        if action == "reset":
            # Сброс к значению по умолчанию для этого чата
            with db_lock:
                cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
                conn.commit()
            bot.edit_message_text(
                "✅ Реакции сброшены к значению по умолчанию (✅)",
                call.message.chat.id,
                call.message.message_id,
                parse_mode='HTML'
            )
            logger.info(f"Реакции сброшены для чата {chat_id} пользователем {user_id}")
            if user_id in user_settings_state:
                del user_settings_state[user_id]
            return
        
        # Для add и replace - сохраняем режим и просим отправить эмодзи
        user_settings_state[user_id] = {
            'adding_reactions': True,
            'settings_msg_id': call.message.message_id,
            'action': action,  # "add" или "replace"
            'chat_id': chat_id
        }
        
        mode_text = "добавлены к текущим" if action == "add" else "заменят текущие"
        bot.edit_message_text(
            f"⚙️ <b>Настройки реакций</b>\n\n"
            f"📝 Поставьте выбранный эмодзи в ответ на это сообщение.\n\n"
            f"Новые реакции будут {mode_text}.",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML'
        )
        # Обновляем информацию о сообщении settings
        if call.message.message_id in settings_messages:
            settings_messages[call.message.message_id]['action'] = action
        else:
            settings_messages[call.message.message_id] = {
                'user_id': user_id,
                'action': action,
                'chat_id': call.message.chat.id
            }
        logger.info(f"[SETTINGS] Пользователь {user_id} выбрал режим: {action}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка в handle_settings_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
        except:
            pass

# Обработка ответа с эмодзи на сообщение /settings
# Этот обработчик обрабатывает ответы на settings с учетом режимов add/replace
@bot.message_handler(func=lambda m: m.reply_to_message and m.from_user.id in user_settings_state, priority=10)
def handle_settings_emojis(message):
    # Пропускаем команды - они обрабатываются отдельными обработчиками
    if message.text and message.text.startswith('/'):
        return
    
    user_id = message.from_user.id
    state = user_settings_state.get(user_id)
    
    if not state:
        return  # нет состояния
    
    # Проверяем, что это ответ на правильное сообщение
    if not message.reply_to_message:
        return
    
    settings_msg_id = state.get('settings_msg_id')
    if not settings_msg_id or message.reply_to_message.message_id != settings_msg_id:
        return  # не наш реплай
    
    # Проверяем, что состояние ожидает эмодзи
    if not state.get('adding_reactions'):
        return  # не в режиме добавления реакций
    
    logger.info(f"[SETTINGS] Получен ответ с эмодзи от {user_id}, state={state}, text={message.text}")
    
    # Извлекаем все эмодзи из текста
    import re
    # Более широкий паттерн для эмодзи
    emoji_pattern = re.compile(
        r'[\U0001F300-\U0001F9FF]'  # Различные символы и пиктограммы
        r'|[\U0001F600-\U0001F64F]'  # Эмодзи лиц
        r'|[\U0001F680-\U0001F6FF]'  # Транспорт и карты
        r'|[\U00002600-\U000026FF]'  # Разные символы
        r'|[\U00002700-\U000027BF]'  # Dingbats
        r'|[\U0001F900-\U0001F9FF]'  # Дополнительные символы
        r'|[\U0001FA00-\U0001FAFF]'  # Шахматы и другие
        r'|[\U00002700-\U000027BF]'  # Дополнительные символы
        r'|[\U0001F1E0-\U0001F1FF]'  # Флаги
        r'|[\U0001F300-\U0001F5FF]'  # Символы и пиктограммы
        r'|[\U0001F600-\U0001F64F]'  # Эмодзи лиц
        r'|[\U0001F680-\U0001F6FF]'  # Транспорт и карты
        r'|[\U0001F700-\U0001F77F]'  # Алхимические символы
        r'|[\U0001F780-\U0001F7FF]'  # Геометрические фигуры
        r'|[\U0001F800-\U0001F8FF]'  # Дополнительные стрелки
        r'|[\U0001F900-\U0001F9FF]'  # Дополнительные символы
        r'|[\U0001FA00-\U0001FA6F]'  # Шахматы
        r'|[\U0001FA70-\U0001FAFF]'  # Символы и пиктограммы
        r'|[\U00002600-\U000026FF]'  # Разные символы
        r'|[\U00002700-\U000027BF]'  # Dingbats
    )
    
    emojis = emoji_pattern.findall(message.text or "")
    
    if not emojis:
        bot.reply_to(message, "⚠️ Не найдено эмодзи в сообщении. Отправьте только эмодзи (можно несколько).")
        logger.warning(f"[SETTINGS] Не найдено эмодзи в сообщении от {user_id}: {message.text}")
        return
    
    emojis_str = ''.join(set(emojis))  # убираем дубли
    
    logger.info(f"[SETTINGS] Извлечено эмодзи: {emojis_str}")
    
    # Проверяем режим (add или replace)
    action = state.get('action', 'replace')
    
    # Получаем chat_id из состояния или сообщения
    chat_id = state.get('chat_id') or message.chat.id
    
    if action == "add":
        # Добавляем к текущим
        current_emojis = get_watched_emojis(chat_id)
        emojis_str = ''.join(current_emojis) + emojis_str
        # Убираем дубликаты, сохраняя порядок
        seen = set()
        emojis_str = ''.join(c for c in emojis_str if c not in seen and not seen.add(c))
        action_text = "добавлены к текущим"
    else:
        # Заменяем полностью
        action_text = "заменены"
    
    # Сохраняем в БД для этого чата
    with db_lock:
        try:
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (%s, 'watched_emoji', %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (chat_id, emojis_str))
            conn.commit()
            logger.info(f"[SETTINGS] Эмодзи сохранены (режим: {action}): {emojis_str}")
        except Exception as e:
            conn.rollback()
            logger.error(f"Ошибка сохранения эмодзи: {e}", exc_info=True)
            bot.reply_to(message, "❌ Ошибка сохранения. Попробуй позже.")
            return
    
    bot.reply_to(message, f"✅ Реакции {action_text}:\n{emojis_str}")
    
    # Очищаем состояние
    if user_id in user_settings_state:
        del user_settings_state[user_id]

# Старый обработчик (оставляем для обратной совместимости, но он не должен срабатывать)
@bot.message_handler(func=lambda m: (
    m.reply_to_message and 
    m.from_user.id in user_settings_state and 
    not user_settings_state[m.from_user.id].get('adding_reactions')  # Только если НЕ выбран режим add/replace
), priority=9)  # Немного ниже приоритет
def handle_settings_reply(message):
    user_id = message.from_user.id
    state = user_settings_state.get(user_id)
    
    logger.info(f"[SETTINGS REPLY] Получено сообщение от {user_id}, reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}, state={state}")
    
    if not state:
        logger.warning(f"[SETTINGS REPLY] Нет состояния для user_id={user_id}")
        return
    
    if not message.reply_to_message:
        logger.warning(f"[SETTINGS REPLY] Нет reply_to_message для user_id={user_id}")
        return
    
    expected_msg_id = state.get('settings_msg_id')
    if expected_msg_id and message.reply_to_message.message_id != expected_msg_id:
        logger.warning(f"[SETTINGS REPLY] Несоответствие message_id: reply_to={message.reply_to_message.message_id}, expected={expected_msg_id}")
        return
    
    logger.info(f"[SETTINGS REPLY] Проверка пройдена, обрабатываем эмодзи для user_id={user_id}")
    
    # Извлекаем эмодзи (упрощенная версия)
    if not message.text:
        bot.reply_to(message, "⚠️ Не найдено эмодзи. Отправь только эмодзи.")
        return
    
    # Расширенная проверка эмодзи
    emojis = ''.join(c for c in message.text if (
        '\U0001F300' <= c <= '\U0001F9FF' or  # Различные символы и пиктограммы
        '\U0001F600' <= c <= '\U0001F64F' or  # Эмодзи лиц
        '\U0001F680' <= c <= '\U0001F6FF' or  # Транспорт и карты
        '\U00002600' <= c <= '\U000026FF' or  # Разные символы
        '\U00002700' <= c <= '\U000027BF' or  # Dingbats
        c in '✅💋🙏❤️😍😘☺️👍😁☑️😊😂🥰🎉⭐🔥'
    ))
    
    if not emojis:
        bot.reply_to(message, "⚠️ Не найдено эмодзи. Отправь только эмодзи.")
        return
    
    # Получаем chat_id из сообщения
    chat_id = message.chat.id
    
    # Сохраняем в БД для этого чата - режим replace по умолчанию
    try:
        with db_lock:
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (%s, 'watched_emoji', %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (chat_id, emojis))
            conn.commit()
        
        bot.reply_to(message, f"✅ Реакции обновлены:\n{emojis}")
        logger.info(f"[SETTINGS] Реакции обновлены для чата {chat_id}, user_id={user_id}: {emojis}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка при сохранении реакций: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при сохранении реакций.")
    
    # Очищаем состояние
    if user_id in user_settings_state:
        del user_settings_state[user_id]

# Обработка ответа с эмодзи на сообщение /settings (расширенная версия с режимами)
# Этот обработчик должен иметь высокий приоритет, чтобы сработать раньше handle_message
def add_reactions_check(message):
    """Проверка для обработчика add_reactions"""
    # Пропускаем команды (кроме тех, которые должны обрабатываться другими обработчиками)
    if message.text and message.text.startswith('/'):
        # Команды, которые должны обрабатываться отдельными обработчиками
        allowed_commands = ['/seasons', '/premieres', '/settings', '/plan', '/list', '/random', '/search', '/schedule', '/total', '/stats', '/rate', '/clean', '/edit', '/ticket', '/help', '/start']
        command = message.text.split('@')[0].split()[0] if message.text else ''
        if command in allowed_commands:
            logger.info(f"[SETTINGS CHECK] add_reactions_check: пропущена команда {message.text[:50]}")
            return False
        # Для других команд тоже пропускаем
        logger.info(f"[SETTINGS CHECK] add_reactions_check: пропущена команда {message.text[:50]}")
        return False
    if not message.reply_to_message:
        return False
    if message.from_user.id not in user_settings_state:
        return False
    state = user_settings_state.get(message.from_user.id, {})
    if not state.get('adding_reactions'):
        return False
    if message.reply_to_message.message_id != state.get('settings_msg_id'):
        return False
    logger.info(f"[SETTINGS CHECK] add_reactions_check: True для user_id={message.from_user.id}")
    return True

@bot.message_handler(func=add_reactions_check, priority=10)  # Высокий приоритет
def add_reactions(message):
    # Пропускаем команды - они обрабатываются отдельными обработчиками
    if message.text and message.text.startswith('/'):
        logger.info(f"[SETTINGS] add_reactions пропущена команда: {message.text[:50]}")
        return
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Получаем состояние
    state = user_settings_state.get(user_id, {})
    settings_msg_id = state.get('settings_msg_id')
    action = state.get('action', 'replace')  # По умолчанию replace
    
    logger.info(f"[SETTINGS] add_reactions вызван для user_id={user_id}, reply_to_message={message.reply_to_message is not None}, settings_msg_id={settings_msg_id}, action={action}")
    logger.info(f"[SETTINGS] add_reactions: state={state}, message.text={message.text[:50] if message.text else None}")
    
    if not message.reply_to_message:
        logger.warning(f"[SETTINGS] Нет reply_to_message для user_id={user_id}")
        bot.reply_to(message, "⚠️ Пожалуйста, отправьте эмодзи в ответ на сообщение бота о настройках.")
        return
    
    if settings_msg_id and message.reply_to_message.message_id != settings_msg_id:
        logger.warning(f"[SETTINGS] Несоответствие message_id: reply_to={message.reply_to_message.message_id}, expected={settings_msg_id}")
        bot.reply_to(message, "⚠️ Пожалуйста, отправьте эмодзи в ответ на сообщение бота о настройках.")
        return
    
    logger.info(f"[SETTINGS] Получен ответ на settings от user_id={user_id}, action={action}, reply_to_message_id={message.reply_to_message.message_id}, settings_msg_id={settings_msg_id}")
    
    # Собираем обычные эмодзи и custom_id из сообщения
    emojis = []
    custom_ids = []
    
    # Обычные эмодзи из текста - используем regex для более точного извлечения
    if message.text:
        # Используем regex для извлечения эмодзи (поддерживает все основные диапазоны Unicode)
        import re
        emoji_pattern = re.compile(
            r'[\U0001F300-\U0001F9FF]'  # Различные символы и пиктограммы
            r'|[\U0001F600-\U0001F64F]'  # Эмодзи лиц
            r'|[\U0001F680-\U0001F6FF]'  # Транспорт и карты
            r'|[\U00002600-\U000026FF]'  # Разные символы
            r'|[\U00002700-\U000027BF]'  # Dingbats
            r'|[\U0001F900-\U0001F9FF]'  # Дополнительные символы
            r'|[\U0001FA00-\U0001FAFF]'  # Шахматы и другие
            r'|[\U00002B50-\U00002B55]'  # Звезды
            r'|👍|✅|❤️|🔥|🎉|😂|🤣|😍|😢|😡|👎|⭐|🌟|💯|🎬|🍿'  # Популярные эмодзи
        )
        emojis = emoji_pattern.findall(message.text)
    
    # Кастомные эмодзи из entities
    if message.entities:
        for entity in message.entities:
            if entity.type == 'custom_emoji' and hasattr(entity, 'custom_emoji_id'):
                custom_id = str(entity.custom_emoji_id)
                custom_ids.append(custom_id)
    
    new_reactions = emojis + [f"custom:{cid}" for cid in custom_ids]
    
    if not new_reactions:
        bot.reply_to(message, "❌ Не нашёл эмодзи в вашем сообщении. Попробуйте отправить эмодзи снова.")
        logger.warning(f"[SETTINGS] Не найдено эмодзи в сообщении от user_id={user_id}, text={message.text}")
        return
    
    # Сохраняем в БД
    try:
        with db_lock:
            try:
                # Проверяем состояние транзакции
                try:
                    cursor.execute('SELECT 1')
                    cursor.fetchone()
                except:
                    conn.rollback()
                
                # Получаем chat_id из сообщения
                chat_id = message.chat.id
                
                # Получаем текущие реакции для этого чата
                current_emojis_local = get_watched_emojis(chat_id)  # Получаем список эмодзи
                
                if action == "add":
                    # Добавляем к текущим
                    # Объединяем текущие эмодзи с новыми
                    all_emojis = ''.join(current_emojis_local) + ''.join(emojis)
                    # Убираем дубликаты, сохраняя порядок
                    seen = set()
                    unique_emojis = ''.join(c for c in all_emojis if c not in seen and not seen.add(c))
                else:
                    # Заменяем полностью
                    unique_emojis = ''.join(emojis)
                
                # Сохраняем в БД для этого чата
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, 'watched_emoji', %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (chat_id, unique_emojis))
                conn.commit()
                
                action_text = "добавлены к текущим" if action == "add" else "заменены"
                bot.reply_to(message, f"✅ Готово! Реакции {action_text}:\n{unique_emojis}")
                logger.info(f"[SETTINGS] Реакции сохранены для чата {chat_id} (режим: {action}): {unique_emojis}")
            except Exception as db_error:
                conn.rollback()
                logger.error(f"[SETTINGS] Ошибка БД при сохранении реакций: {db_error}", exc_info=True)
                bot.reply_to(message, "❌ Произошла ошибка при сохранении реакций. Попробуйте снова.")
    except Exception as e:
        logger.error(f"[SETTINGS] Критическая ошибка при сохранении реакций: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при сохранении реакций. Попробуйте снова.")
    
    # Удаляем состояние
    if user_id in user_settings_state:
        del user_settings_state[user_id]

@bot.message_handler(func=lambda m: user_settings_state.get(m.from_user.id, {}).get('waiting_emoji', False) and m.text and not m.text.startswith('/'))
def handle_emoji_input(message):
    """Обработчик получения эмодзи после команды /settings"""
    user_id = message.from_user.id
    emoji_text = message.text.strip()
    
    logger.info(f"Получен эмодзи от пользователя {user_id}: {emoji_text}")
    
    if not emoji_text:
        bot.reply_to(message, "Пожалуйста, отправьте эмодзи.")
        return
    
    # Сохраняем эмодзи в БД
    with db_lock:
        cursor.execute('INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, %s) ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value', (message.chat.id, "watched_emoji", emoji_text))
        conn.commit()
    
    # Убираем состояние ожидания
    if user_id in user_settings_state:
        del user_settings_state[user_id]
    
    bot.reply_to(message, f"Готово, эмодзи просмотра изменен на: {emoji_text}")
    logger.info(f"Эмодзи просмотра изменён пользователем {user_id} на: {emoji_text}")

# /plan — планирование просмотра
def process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc=None):
    """Планирует просмотр фильма. Возвращает True при успехе, False при ошибке, 'NEEDS_TIMEZONE' если нужно уточнить часовой пояс.
    message_date_utc - время сообщения в UTC для определения часового пояса"""
    plan_dt = None
    
    # Проверяем, нужно ли уточнить часовой пояс
    if message_date_utc:
        needs_tz_check = check_timezone_change(user_id, message_date_utc)
        if needs_tz_check:
            # Возвращаем специальный код для запроса часового пояса
            return 'NEEDS_TIMEZONE'
    
    # Используем часовой пояс пользователя или по умолчанию Москва
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)
    
    # Обработка специальных случаев
    day_lower = day_or_date.lower().strip()
    
    # Обработка "сегодня"
    if 'сегодня' in day_lower:
        plan_date = now.date()
        if plan_type == 'home':
            # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
            hour = 19 if now.weekday() < 5 else 10
        else:
            hour = 9
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)
        if plan_dt < now:
            # Если время уже прошло, переносим на завтра
            plan_dt = plan_dt + timedelta(days=1)
    
    # Обработка "завтра" (для обоих режимов)
    elif 'завтра' in day_lower:
        plan_date = (now.date() + timedelta(days=1))
        if plan_type == 'cinema':
            hour = 9
        else:  # home
            # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
            hour = 19 if plan_date.weekday() < 5 else 10
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)
    
    # Обработка "следующая неделя" (для обоих режимов)
    elif 'следующая неделя' in day_lower or 'след неделя' in day_lower or 'след. неделя' in day_lower or 'на следующей неделе' in day_lower:
        if plan_type == 'cinema':
            # Для кино - напоминание в четверг, день премьер
            current_wd = now.weekday()
            days_until_thursday = (3 - current_wd + 7) % 7
            if days_until_thursday == 0:
                # Если сегодня четверг, берем следующий четверг
                days_until_thursday = 7
            else:
                # Добавляем еще неделю, чтобы получить четверг следующей недели
                days_until_thursday += 7
            plan_date = now.date() + timedelta(days=days_until_thursday)
            hour = 9
        else:  # home
            # Для дома - суббота следующей недели в 10:00
            current_wd = now.weekday()
            days_until_next_saturday = (5 - current_wd + 7) % 7
            if days_until_next_saturday == 0:
                # Если сегодня суббота, берем следующую
                days_until_next_saturday = 7
            else:
                # Иначе добавляем еще неделю, чтобы получить субботу следующей недели
                days_until_next_saturday += 7
            plan_date = now.date() + timedelta(days=days_until_next_saturday)
            hour = 10
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)
    
    # Обработка "в марте", "в апреле" и т.д. (для обоих режимов - напоминание 1 числа месяца)
    elif re.search(r'в\s+([а-яё]+)', day_lower):
        month_match = re.search(r'в\s+([а-яё]+)', day_lower)
        if month_match:
            month_str = month_match.group(1)
            month = months_map.get(month_str)
            if month:
                year = now.year
                # Проверяем, не прошел ли уже этот месяц
                candidate_date = datetime(year, month, 1).date()
                if candidate_date < now.date():
                    # Месяц уже прошел, берем следующий год
                    year += 1
                plan_date = datetime(year, month, 1)
                if plan_type == 'cinema':
                    hour = 9
                else:  # home
                    # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
                    hour = 19 if plan_date.weekday() < 5 else 10
                plan_dt = user_tz.localize(plan_date.replace(hour=hour, minute=0))
    
    # Ищем день недели в расширенном словаре (для обоих режимов)
    if not plan_dt:
        target_weekday = None
        # Сортируем фразы по длине (от длинных к коротким), чтобы сначала находить более специфичные варианты
        sorted_phrases = sorted(days_full.items(), key=lambda x: len(x[0]), reverse=True)
        for phrase, wd in sorted_phrases:
            if phrase in day_lower:
                target_weekday = wd
                break
    
    if target_weekday is not None:
        # Вычисляем ближайший указанный день (вперёд)
        current_wd = now.weekday()
        delta = (target_weekday - current_wd + 7) % 7
        
        # Если сегодня указанный день недели
        if delta == 0:
            # Проверяем время: если до 20:00, можно планировать на сегодня
            if now.hour < 20:
                # Планируем на сегодня
                plan_date = now.date()
            else:
                # Уже 20:00 или позже - переносим на следующую неделю
                delta = 7
                plan_date = now.date() + timedelta(days=delta)
        else:
            plan_date = now.date() + timedelta(days=delta)
        
        if plan_type == 'home':
            # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
            hour = 19 if target_weekday < 5 else 10
        else:  # cinema
            hour = 9
        
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)
    
    else:
        # Если день недели не найден — пытаемся распарсить дату (для обоих режимов)
        # Формат "15 января", "15 янв", "15 января 2025"
        date_match = re.search(r'(\d{1,2})\s+([а-яё]+)(?:\s+(\d{4}))?', day_lower)
        if date_match:
            day_num = int(date_match.group(1))
            month_str = date_match.group(2)
            year_str = date_match.group(3) if date_match.group(3) else None
            month = months_map.get(month_str)
            if month:
                try:
                    # Используем указанный год или текущий/следующий
                    if year_str:
                        year = int(year_str)
                    else:
                        year = now.year
                    candidate_date = datetime(year, month, day_num).date()
                    candidate_dt = user_tz.localize(datetime(year, month, day_num))
                    
                    # Проверяем, не является ли дата сегодняшней
                    if candidate_date == now.date():
                        # Если сегодня, проверяем время: если до 20:00, можно планировать на сегодня
                        if now.hour < 20:
                            plan_date = datetime(year, month, day_num)
                        else:
                            # Уже 20:00 или позже - переносим на следующий год (или следующий месяц, если это возможно)
                            year += 1
                            plan_date = datetime(year, month, day_num)
                    elif candidate_dt < now:
                        # Дата в прошлом - переносим на следующий год
                        year += 1
                        plan_date = datetime(year, month, day_num)
                    else:
                        plan_date = datetime(year, month, day_num)
                    
                    if plan_type == 'cinema':
                        hour = 9
                    else:  # home
                        # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
                        hour = 19 if plan_date.weekday() < 5 else 10
                    plan_dt = user_tz.localize(plan_date.replace(hour=hour, minute=0))
                except ValueError:
                    logger.error(f"[PLAN] Некорректная дата: {day_num} {month_str}")
                    return False
            else:
                logger.warning(f"[PLAN] Не распознан месяц: {month_str}")
                return False
        else:
            # Формат "15.01", "15/01", "15.01.25", "15.01.2025"
            date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', day_lower)
            if date_match:
                day_num = int(date_match.group(1))
                month_num = int(date_match.group(2))
                year_str = date_match.group(3) if date_match.group(3) else None
                
                if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                    try:
                        if year_str:
                            # Если указан год
                            if len(year_str) == 2:
                                # Двузначный год: 25 -> 2025, 24 -> 2024
                                year = 2000 + int(year_str)
                            else:
                                year = int(year_str)
                        else:
                            # Год не указан, используем текущий или следующий
                            year = now.year
                        
                        candidate_date = datetime(year, month_num, day_num).date()
                        candidate_dt = user_tz.localize(datetime(year, month_num, day_num))
                        
                        # Проверяем, не является ли дата сегодняшней
                        if candidate_date == now.date():
                            # Если сегодня, проверяем время: если до 20:00, можно планировать на сегодня
                            if now.hour < 20:
                                plan_date = datetime(year, month_num, day_num)
                            else:
                                # Уже 20:00 или позже - переносим на следующий год (или следующий месяц, если это возможно)
                                if month_num == 12:
                                    year += 1
                                    month_num = 1
                                else:
                                    month_num += 1
                                plan_date = datetime(year, month_num, day_num)
                        elif candidate_dt < now:
                            # Дата в прошлом - переносим на следующий год
                            year += 1
                            plan_date = datetime(year, month_num, day_num)
                        else:
                            plan_date = datetime(year, month_num, day_num)
                        
                        if plan_type == 'cinema':
                            hour = 9
                        else:  # home
                            # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
                            hour = 19 if plan_date.weekday() < 5 else 10
                        plan_dt = user_tz.localize(plan_date.replace(hour=hour, minute=0))
                        logger.info(f"[PLAN] Найдена дата (числовой формат): {day_num}.{month_num}.{year}")
                    except ValueError as e:
                        logger.error(f"[PLAN] Некорректная дата: {day_num}.{month_num}.{year_str if year_str else 'N/A'}: {e}")
                        return False
                else:
                    return False
            else:
                return False
    
    if plan_dt:
        # Извлекаем kp_id из ссылки для поиска
        match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
        kp_id = match.group(2) if match else None
        
        with db_lock:
            if kp_id:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            else:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND link = %s', (chat_id, link))
            row = cursor.fetchone()
            if not row:
                info = extract_movie_info(link)
                if info:
                    is_series_val = 1 if info.get('is_series') else 0
                    cursor.execute('INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series', 
                                 (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors'], is_series_val))
                    conn.commit()
                    cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                    else:
                        bot.send_message(chat_id, "Не удалось добавить фильм в базу.")
                        return False
                else:
                    bot.send_message(chat_id, "Не удалось извлечь информацию о фильме.")
                    return False
            else:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
                is_series = row.get('is_series') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0)
            
            # Для сериалов: находим следующую непросмотренную серию
            episode_info = None
            if is_series:
                # Получаем информацию о просмотренных сериях
                cursor.execute('''
                    SELECT season_number, episode_number 
                    FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                    ORDER BY season_number DESC, episode_number DESC
                    LIMIT 1
                ''', (chat_id, film_id, user_id))
                last_watched = cursor.fetchone()
                
                if last_watched:
                    last_season = last_watched.get('season_number') if isinstance(last_watched, dict) else last_watched[0]
                    last_episode = last_watched.get('episode_number') if isinstance(last_watched, dict) else last_watched[1]
                    
                    # Получаем сезоны из API
                    seasons_data = get_seasons_data(kp_id)
                    if seasons_data:
                        # Ищем следующую непросмотренную серию
                        found_next = False
                        for season in seasons_data:
                            season_num = season.get('number', '')
                            episodes = season.get('episodes', [])
                            
                            for ep in episodes:
                                ep_num = ep.get('episodeNumber', '')
                                
                                # Сравниваем сезоны и эпизоды
                                if (int(season_num) > int(last_season)) or (int(season_num) == int(last_season) and int(ep_num) > int(last_episode)):
                                    # Проверяем, не просмотрен ли уже этот эпизод
                                    cursor.execute('''
                                        SELECT watched FROM series_tracking 
                                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                        AND season_number = %s AND episode_number = %s AND watched = TRUE
                                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                                    already_watched = cursor.fetchone()
                                    
                                    if not already_watched:
                                        episode_info = {
                                            'season': season_num,
                                            'episode': ep_num,
                                            'release_date': ep.get('releaseDate', '—')
                                        }
                                        found_next = True
                                        break
                            
                            if found_next:
                                break
                
                # Если не нашли следующую серию, берем первую непросмотренную
                if not episode_info:
                    seasons_data = get_seasons_data(kp_id)
                    if seasons_data:
                        for season in seasons_data:
                            season_num = season.get('number', '')
                            episodes = season.get('episodes', [])
                            
                            for ep in episodes:
                                ep_num = ep.get('episodeNumber', '')
                                cursor.execute('''
                                    SELECT watched FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                    AND season_number = %s AND episode_number = %s AND watched = TRUE
                                ''', (chat_id, film_id, user_id, season_num, ep_num))
                                already_watched = cursor.fetchone()
                                
                                if not already_watched:
                                    episode_info = {
                                        'season': season_num,
                                        'episode': ep_num,
                                        'release_date': ep.get('releaseDate', '—')
                                    }
                                    break
                            
                            if episode_info:
                                break
            
            plan_utc = plan_dt.astimezone(pytz.utc)
            cursor.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s)',
                          (chat_id, film_id, plan_type, plan_utc, user_id))
            conn.commit()
        
        plan_type_text = "в кино" if plan_type == 'cinema' else "дома"
        # Определяем название часового пояса для отображения
        tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
        
        # Для кино добавляем кнопку "Добавить билеты"
        markup = None
        if plan_type == 'cinema':
            # Получаем plan_id только что созданного плана
            with db_lock:
                cursor.execute('SELECT id FROM plans WHERE chat_id = %s AND film_id = %s AND plan_type = %s AND plan_datetime = %s ORDER BY id DESC LIMIT 1',
                             (chat_id, film_id, plan_type, plan_utc))
                plan_row = cursor.fetchone()
                if plan_row:
                    plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("🎟️ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
        
        plan_message = f"✅ Запланирован фильм {plan_type_text}: <b>{title}</b>"
        if episode_info:
            plan_message += f" — Сезон {episode_info['season']}, Эпизод {episode_info['episode']}"
        plan_message += f" на {plan_dt.strftime('%d.%m.%Y %H:%M')} {tz_name}"
        
        bot.send_message(chat_id, plan_message, parse_mode='HTML', reply_markup=markup)
        
        # Если планируем дома, показываем где посмотреть
        if plan_type == 'home' and kp_id:
            sources = get_external_sources(kp_id)
            if sources:
                sources_markup = InlineKeyboardMarkup(row_width=2)
                for platform, url in sources[:6]:  # Максимум 6 кнопок
                    sources_markup.add(InlineKeyboardButton(platform, url=url))
                bot.send_message(chat_id, f"📺 Где посмотреть <b>{title}</b>?", reply_markup=sources_markup, parse_mode='HTML')
        
        # Планируем уведомление на время плана
        scheduler.add_job(
            send_plan_notification,
            'date',
            run_date=plan_utc,  # plan_utc — это уже в UTC
            args=[chat_id, film_id, title, link, plan_type],
            id=f'plan_notify_{chat_id}_{film_id}_{int(plan_utc.timestamp())}'  # уникальный ID
        )
        
        logger.info(f"[PLAN] Уведомление запланировано на {plan_dt} МСК для фильма {title}")
        return True

@bot.message_handler(commands=['plan'])
def plan_handler(message):
    logger.info(f"[HANDLER] /plan вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/plan', message.chat.id)
        logger.info(f"Команда /plan от пользователя {message.from_user.id}")
        user_id = message.from_user.id
        chat_id = message.chat.id
        original_text = message.text or ''
        # Убираем /plan и возможный @botname из текста
        text = original_text.lower()
        # Удаляем команду /plan и возможный @botname
        text = re.sub(r'/plan(@\w+)?\s*', '', text, flags=re.IGNORECASE).strip()
        
        logger.info(f"[PLAN] ===== НАЧАЛО ОБРАБОТКИ /plan =====")
        logger.info(f"[PLAN] user_id={user_id}, chat_id={chat_id}")
        logger.info(f"[PLAN] original_text='{original_text}'")
        
        # Проверяем реплай на сообщение со ссылкой
        link = None
        if message.reply_to_message:
            reply_msg = message.reply_to_message
            reply_msg_id = reply_msg.message_id
            
            logger.info(f"[PLAN] Обработка реплая: reply_msg_id={reply_msg_id}, chat_id={chat_id}")
            logger.info(f"[PLAN] bot_messages keys (первые 10): {list(bot_messages.keys())[:10]}")
            logger.info(f"[PLAN] plan_notification_messages keys (первые 10): {list(plan_notification_messages.keys())[:10]}")
            
            # 1. Проверяем bot_messages и plan_notification_messages
            link = bot_messages.get(reply_msg_id)
            if link:
                logger.info(f"[PLAN] ✅ Найдена ссылка в bot_messages: {link}")
            else:
                plan_data = plan_notification_messages.get(reply_msg_id)
                if plan_data:
                    link = plan_data.get('link')
                    logger.info(f"[PLAN] ✅ Найдена ссылка в plan_notification_messages: {link}")
            
            # 2. Ищем ссылку в тексте сообщения (обычная ссылка)
            if not link:
                reply_text = reply_msg.text or ''
                logger.info(f"[PLAN] Текст реплая (первые 200 символов): {reply_text[:200]}")
                link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', reply_text)
                if link_match:
                    link = link_match.group(1)
                    logger.info(f"[PLAN] ✅ Найдена ссылка в тексте реплая: {link}")
            
            # 3. Ищем HTML-ссылку "Кинопоиск" в тексте сообщения
            # Telegram может не возвращать HTML в text, но entities должны содержать ссылку
            if not link:
                reply_text = reply_msg.text or ''
                # Пробуем найти HTML-тег (хотя Telegram обычно не возвращает HTML в text)
                html_link_match = re.search(r"<a\s+href=['\"](https?://[\w\./-]*kinopoisk\.ru/(?:film|series)/\d+)['\"]", reply_text)
                if html_link_match:
                    link = html_link_match.group(1)
                    logger.info(f"[PLAN] ✅ Найдена HTML-ссылка в тексте реплая: {link}")
            
            # 4. Проверяем entities сообщения (URL entities) - это основной способ для HTML-ссылок
            # В Telegram HTML-ссылки доступны через entities типа 'text_link' с полем 'url'
            if not link and reply_msg.entities:
                logger.info(f"[PLAN] Проверяем entities реплая: {len(reply_msg.entities)} entities")
                for idx, entity in enumerate(reply_msg.entities):
                    logger.info(f"[PLAN] Entity {idx}: type={entity.type}, offset={entity.offset}, length={entity.length}")
                    if entity.type == 'text_link':
                        # text_link - это HTML-ссылка, URL хранится в entity.url
                        if hasattr(entity, 'url') and entity.url:
                            url = entity.url
                            logger.info(f"[PLAN] Entity text_link URL: {url}")
                            if 'kinopoisk.ru' in url and ('/film/' in url or '/series/' in url):
                                link = url
                                logger.info(f"[PLAN] ✅ Найдена ссылка в text_link entity: {link}")
                                break
                    elif entity.type == 'url':
                        # url - это обычная ссылка в тексте, извлекаем из текста
                        if reply_msg.text:
                            url = reply_msg.text[entity.offset:entity.offset + entity.length]
                            logger.info(f"[PLAN] Entity url из текста: {url}")
                            if 'kinopoisk.ru' in url and ('/film/' in url or '/series/' in url):
                                link = url
                                logger.info(f"[PLAN] ✅ Найдена ссылка в url entity: {link}")
                                break
            else:
                logger.info(f"[PLAN] Нет entities в реплае или ссылка уже найдена")
            
            if not link:
                logger.warning(f"[PLAN] ❌ Не удалось найти ссылку в реплае message_id={reply_msg_id}")
        else:
            logger.info(f"[PLAN] Нет реплая в сообщении")
        
        # Ищем ссылку в тексте команды (используем оригинальный текст для правильного извлечения)
        if not link:
            link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', original_text)
            link = link_match.group(1) if link_match else None
            if link:
                logger.info(f"[PLAN] Найдена ссылка в тексте команды: {link}")
        
        # Если ссылка найдена в тексте команды, извлекаем оставшийся текст для plan_type и day_or_date
        if link and original_text:
            # Удаляем /plan и ссылку из текста
            remaining_text = original_text.replace('/plan', '').replace(link, '').strip().lower()
            if remaining_text:
                text = remaining_text
                logger.info(f"[PLAN] Оставшийся текст после извлечения ссылки: {text}")
        
        # Ищем ID кинопоиска (например, "/plan 484791 дома в воскресенье")
        kp_id = None
        if not link:
            id_match = re.search(r'^(\d+)', text.strip())
            if id_match:
                kp_id = id_match.group(1)
                # Проверяем, есть ли фильм с таким ID в базе
                with db_lock:
                    cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    row = cursor.fetchone()
                    if row:
                        link = row.get('link') if isinstance(row, dict) else row[0]
                        logger.info(f"[PLAN] Найден фильм по ID {kp_id}: {link}")
                    else:
                        # Если фильма нет в базе, создаем ссылку из ID
                        link = f"https://kinopoisk.ru/film/{kp_id}"
                        logger.info(f"[PLAN] Фильм с ID {kp_id} не найден в базе, создана ссылка: {link}")
        
        plan_type = 'home' if 'дома' in text else 'cinema' if 'кино' in text else None
        logger.info(f"[PLAN] plan_type={plan_type}, text={text}")
        
        day_or_date = None
        
        # Сначала ищем день недели (для обоих режимов)
        sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
        for phrase in sorted_phrases:
            if phrase in text:
                day_or_date = phrase
                break
        
        # Если день недели не найден, ищем дату (для обоих режимов)
        if not day_or_date:
            # Сначала проверяем специальные форматы: "завтра", "следующая неделя"
            if 'завтра' in text:
                day_or_date = 'завтра'
                logger.info(f"[PLAN] Найден формат 'завтра'")
            elif 'следующая неделя' in text or 'след неделя' in text or 'след. неделя' in text:
                day_or_date = 'следующая неделя'
                logger.info(f"[PLAN] Найден формат 'следующая неделя'")
            # Затем проверяем формат "в апреле", "в марте" и т.д. (без числа)
            else:
                month_match = re.search(r'в\s+([а-яё]+)', text)
                if month_match:
                    month_str = month_match.group(1)
                    # Проверяем, что это действительно месяц
                    months_map = {
                        'январь': 1, 'янв': 1, 'февраль': 2, 'фев': 2, 'март': 3, 'мар': 3,
                        'апрель': 4, 'апр': 4, 'май': 5, 'июнь': 6, 'июн': 6,
                        'июль': 7, 'июл': 7, 'август': 8, 'авг': 8, 'сентябрь': 9, 'сент': 9, 'сен': 9,
                        'октябрь': 10, 'окт': 10, 'ноябрь': 11, 'ноя': 11, 'декабрь': 12, 'дек': 12
                    }
                    if month_str.lower() in months_map:
                        day_or_date = f"в {month_str}"
                        logger.info(f"[PLAN] Найден месяц (формат 'в [месяц]'): {day_or_date}")
        
        # Если специальные форматы не найдены, пробуем другие форматы
        if not day_or_date:
            # Пробуем разные форматы даты: "15 января", "с 20 февраля", "15.01", "15/01", "15.01.25", "15.01.2025"
            # Убираем предлоги "с", "на" и т.д. перед датой
            date_match = re.search(r'(?:с|на|до)?\s*(\d{1,2})\s+([а-яё]+)', text)
            if date_match:
                day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
                logger.info(f"[PLAN] Найдена дата (текстовый формат): {day_or_date}")
            else:
                # Формат "15.01", "15/01", "15.01.25", "15.01.2025", "15/01/25", "15/01/2025"
                date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
                if date_match:
                    day_num = int(date_match.group(1))
                    month_num = int(date_match.group(2))
                    if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                        month_names = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                     'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                        day_or_date = f"{day_num} {month_names[month_num - 1]}"
                        logger.info(f"[PLAN] Найдена дата (числовой формат): {day_or_date}")
        
        logger.info(f"[PLAN] link={link}, plan_type={plan_type}, day_or_date={day_or_date}")
        
        if link and plan_type and day_or_date:
            try:
                process_plan(user_id, chat_id, link, plan_type, day_or_date)
            except Exception as e:
                bot.reply_to(message, f"Ошибка при планировании: {e}")
                logger.error(f"Ошибка process_plan: {e}", exc_info=True)
            return
        
        # Если нет ссылки, отправляем новое сообщение с возможностью отправки по частям
        if not link:
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Выйти из режима", callback_data="plan:cancel"))
            reply_msg = bot.reply_to(message, "Пришлите ссылку на фильм в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!", reply_markup=markup)
            # Устанавливаем состояние для получения данных по частям
            user_plan_state[user_id] = {'step': 1, 'chat_id': chat_id}
            return
        
        if not plan_type:
            error_msg = bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
            # Сохраняем состояние для обработки ответа
            if error_msg:
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': None,
                    'day_or_date': day_or_date,
                    'missing': 'plan_type'
                }
            return
        
        if not day_or_date:
            error_msg = bot.reply_to(message, "Не указан день/дата. Для дома укажите день недели (пн, вт, ср, чт, пт, сб, вс или 'в сб'), для кино - день недели или дату (15 января).")
            # Сохраняем состояние для обработки ответа
            if error_msg:
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': plan_type,
                    'day_or_date': None,
                    'missing': 'day_or_date'
                }
            return
    except Exception as e:
        logger.error(f"❌ Ошибка в /plan: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /plan")
        except:
            pass

@bot.message_handler(func=lambda m: user_plan_state.get(m.from_user.id, {}).get('step') == 1)
def get_plan_link(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Игнорируем команды (начинающиеся с /)
    if message.text and message.text.startswith('/'):
        logger.info(f"[PLAN] Игнорируем команду {message.text} в режиме планирования")
        return
    
    link = None
    
    if message.reply_to_message:
        link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', message.reply_to_message.text or '')
        if link_match:
            link = link_match.group(0)
    
    if not link:
        link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', message.text)
        if link_match:
            link = link_match.group(0)
    
    # Проверяем ID кинопоиска
    if not link:
        id_match = re.search(r'^(\d+)', message.text.strip())
        if id_match:
            kp_id = id_match.group(1)
            # Проверяем, есть ли фильм с таким ID в базе
            with db_lock:
                cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    link = row.get('link') if isinstance(row, dict) else row[0]
                    logger.info(f"[PLAN] Найден фильм по ID {kp_id}: {link}")
                else:
                    # Если фильма нет в базе, создаем ссылку из ID
                    link = f"https://kinopoisk.ru/film/{kp_id}"
                    logger.info(f"[PLAN] Фильм с ID {kp_id} не найден в базе, создана ссылка: {link}")
    
    if not link:
        bot.reply_to(message, "Не нашёл ссылку или ID фильма. Попробуйте снова.")
        return
    
    user_plan_state[user_id]['link'] = link
    user_plan_state[user_id]['step'] = 2
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
    markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
    bot.send_message(message.chat.id, "Где планируете смотреть?", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("plan_from_added:"))
def plan_from_added_callback(call):
    """Обработчик кнопки 'Запланировать просмотр' из сообщения о добавлении фильма"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        kp_id = call.data.split(":")[1]
        
        logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать фильм kp_id={kp_id}")
        
        # Получаем link из базы или формируем его
        link = None
        with db_lock:
            cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if row:
                link = row.get('link') if isinstance(row, dict) else row[0]
        
        if not link:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
        
        # Устанавливаем состояние для планирования
        user_plan_state[user_id] = {
            'step': 2,
            'link': link,
            'chat_id': chat_id
        }
        
        # Показываем кнопки выбора типа просмотра
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
        markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
        
        bot.answer_callback_query(call.id, "Выберите тип просмотра")
        bot.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
        logger.info(f"[PLAN FROM ADDED] Состояние установлено для пользователя {user_id}, link={link}")
    except Exception as e:
        logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "plan:cancel")
def plan_cancel_callback(call):
    """Обработчик кнопки выхода из режима планирования"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    # Удаляем состояние планирования
    if user_id in user_plan_state:
        del user_plan_state[user_id]
        logger.info(f"[PLAN] Пользователь {user_id} вышел из режима планирования")
    
    bot.answer_callback_query(call.id, "Режим планирования отменён")
    bot.edit_message_text("✅ Режим планирования отменён. Можете использовать другие команды.", 
                         chat_id, call.message.message_id)

@bot.callback_query_handler(func=lambda call: call.data.startswith("plan_type:"))
def plan_type_choice(call):
    user_id = call.from_user.id
    plan_type = call.data.split(":")[1]
    
    # Проверяем, что пользователь в состоянии планирования
    if user_id not in user_plan_state:
        bot.answer_callback_query(call.id, "❌ Состояние планирования не найдено", show_alert=True)
        return
    
    user_plan_state[user_id]['type'] = plan_type
    user_plan_state[user_id]['step'] = 3

    bot.answer_callback_query(call.id)
    bot.edit_message_text("Укажите день/дату:", call.message.chat.id, call.message.message_id)
    if plan_type == 'home':
        bot.send_message(call.message.chat.id, 
            "📅 <b>Укажите дату, это может быть:</b>\n\n"
            "• <b>15 января 17:00</b>\n"
            "• <b>10.01</b>\n"
            "• <b>14 апреля</b>\n"
            "• <b>пятница</b>\n"
            "• <b>сб</b>\n"
            "• <b>завтра</b>\n"
            "• <b>на следующей неделе</b>", 
            parse_mode='HTML')
    else:
        bot.send_message(call.message.chat.id, "Для кино: '15 января' или 'с четверга'.")

@bot.message_handler(func=lambda m: user_plan_state.get(m.from_user.id, {}).get('step') == 3)
def get_plan_day_or_date(message):
    user_id = message.from_user.id
    
    # Игнорируем команды (начинающиеся с /)
    if message.text and message.text.startswith('/'):
        logger.info(f"[PLAN] Игнорируем команду {message.text} в режиме планирования (step 3)")
        return
    
    text = message.text.lower().strip()
    plan_type = user_plan_state[user_id]['type']
    link = user_plan_state[user_id]['link']
    
    # Используем часовой пояс пользователя
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)
    plan_dt = None

    # Поиск дня недели
    target_weekday = None
    for phrase, wd in days_full.items():
        if phrase in text:
            target_weekday = wd
            break

    if target_weekday is not None:
        # Вычисляем ближайший указанный день (вперёд)
        current_wd = now.weekday()
        delta = (target_weekday - current_wd + 7) % 7
        if delta == 0:  # если сегодня — переносим на следующую неделю
            delta = 7
        plan_date = now.date() + timedelta(days=delta)

        if plan_type == 'home':
            # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
            hour = 19 if target_weekday < 5 else 10
        else:  # cinema
            hour = 9

        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = user_tz.localize(plan_dt)

    else:
        # Обработка специальных форматов: "завтра", "следующая неделя"
        if 'завтра' in text:
            plan_date = (now.date() + timedelta(days=1))
            if plan_type == 'home':
                # Будние дни (понедельник-пятница, 0-4) — 19:00, выходные (суббота-воскресенье, 5-6) — 10:00
                hour = 19 if plan_date.weekday() < 5 else 10
            else:
                hour = 9
            plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
            plan_dt = user_tz.localize(plan_dt)
        elif 'следующая неделя' in text or 'след неделя' in text or 'след. неделя' in text or 'на следующей неделе' in text:
            if plan_type == 'home':
                # Для дома - суббота следующей недели в 10:00
                current_wd = now.weekday()
                days_until_next_saturday = (5 - current_wd + 7) % 7
                if days_until_next_saturday == 0:
                    # Если сегодня суббота, берем следующую
                    days_until_next_saturday = 7
                else:
                    # Иначе добавляем еще неделю, чтобы получить субботу следующей недели
                    days_until_next_saturday += 7
                plan_date = now.date() + timedelta(days=days_until_next_saturday)
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=10))
                plan_dt = user_tz.localize(plan_dt)
            else:
                # Для кино - четверг следующей недели
                current_wd = now.weekday()
                days_until_thursday = (3 - current_wd + 7) % 7
                if days_until_thursday == 0:
                    days_until_thursday = 7
                else:
                    days_until_thursday += 7
                plan_date = now.date() + timedelta(days=days_until_thursday)
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=9))
                plan_dt = user_tz.localize(plan_dt)
        else:
            # Парсинг дат: "15 января", "15 января 17:00", "10.01", "14 апреля"
            # Сначала пробуем формат с временем: "15 января 17:00"
            date_time_match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2})[.:](\d{2})', text)
            if date_time_match:
                day_num = int(date_time_match.group(1))
                month_str = date_time_match.group(2)
                hour = int(date_time_match.group(3))
                minute = int(date_time_match.group(4))
                month = months_map.get(month_str.lower())
                if month:
                    try:
                        year = now.year
                        candidate = user_tz.localize(datetime(year, month, day_num, hour, minute))
                        if candidate < now:
                            year += 1
                        plan_dt = user_tz.localize(datetime(year, month, day_num, hour, minute))
                    except ValueError:
                        bot.reply_to(message, "Некорректная дата или время. Попробуйте снова.")
                        return
                else:
                    bot.reply_to(message, "Не распознал месяц.")
                    return
            else:
                # Парсинг "15 января" или "14 апреля"
                date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
                if date_match:
                    day_num = int(date_match.group(1))
                    month_str = date_match.group(2)
                    month = months_map.get(month_str.lower())
                    if month:
                        try:
                            year = now.year
                            candidate = user_tz.localize(datetime(year, month, day_num))
                            if candidate < now:
                                year += 1
                            plan_date = datetime(year, month, day_num)
                            if plan_type == 'home':
                                # Будние дни — 19:00, выходные — 10:00
                                hour = 19 if plan_date.weekday() < 5 else 10
                            else:
                                hour = 9
                            plan_dt = user_tz.localize(plan_date.replace(hour=hour, minute=0))
                        except ValueError:
                            bot.reply_to(message, "Некорректная дата. Попробуйте снова.")
                            return
                    else:
                        bot.reply_to(message, "Не распознал месяц.")
                        return
                else:
                    # Парсинг "10.01" или "06.01"
                    date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
                    if date_match:
                        day_num = int(date_match.group(1))
                        month_num = int(date_match.group(2))
                        if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                            try:
                                year = now.year
                                if date_match.group(3):
                                    year_part = int(date_match.group(3))
                                    if year_part < 100:
                                        year = 2000 + year_part
                                    else:
                                        year = year_part
                                candidate = user_tz.localize(datetime(year, month_num, day_num))
                                if candidate < now:
                                    year += 1
                                plan_date = datetime(year, month_num, day_num)
                                if plan_type == 'home':
                                    # Будние дни — 19:00, выходные — 10:00
                                    hour = 19 if plan_date.weekday() < 5 else 10
                                else:
                                    hour = 9
                                plan_dt = user_tz.localize(plan_date.replace(hour=hour, minute=0))
                            except ValueError:
                                bot.reply_to(message, "Некорректная дата. Попробуйте снова.")
                                return
                        else:
                            bot.reply_to(message, "Некорректная дата. Попробуйте снова.")
                            return
                    else:
                        # Если ничего не распознано
                        if plan_type == 'cinema':
                            bot.reply_to(message, "Укажите день недели или дату в формате '15 января' или '10.01'.")
                        else:
                            bot.reply_to(message, "Укажите день недели, дату (15 января, 10.01) или 'завтра', 'на следующей неделе'.")
                        return

    if plan_dt:
        # Получаем/создаём фильм
        chat_id = message.chat.id
        # Извлекаем kp_id из ссылки для поиска
        match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
        kp_id = match.group(2) if match else None
        
        with db_lock:
            if kp_id:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            else:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND link = %s', (chat_id, link))
            row = cursor.fetchone()
            if not row:
                info = extract_movie_info(link)
                if not info:
                    bot.reply_to(message, "Не удалось получить информацию о фильме.")
                    return
                is_series_val = 1 if info.get('is_series') else 0
                cursor.execute('''
                    INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                ''', (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors'], is_series_val))
                conn.commit()
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title = row.get('title') if isinstance(row, dict) else row[1]
                else:
                    bot.reply_to(message, "Не удалось добавить фильм в базу.")
                    return
            else:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]

            # Сохраняем план и получаем plan_id
            plan_utc = plan_dt.astimezone(pytz.utc)
            cursor.execute('''
                INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
        ''', (chat_id, film_id, plan_type, plan_utc, user_id))
            result = cursor.fetchone()
            plan_id = result[0] if result else None
        conn.commit()

        day_name = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье'][plan_dt.weekday()]
        plan_type_text = "в кино" if plan_type == 'cinema' else "дома"
        # Определяем название часового пояса для отображения
        tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
        bot.reply_to(message, f"✅ Запланирован фильм {plan_type_text}: <b>{title}</b> на <b>{day_name} {plan_dt.strftime('%d.%m.%Y в %H:%M')}</b> {tz_name}", parse_mode='HTML')

        # Планируем уведомление через scheduler
        try:
            scheduler.add_job(
                send_plan_notification,
                'date',
                run_date=plan_utc,
                args=[chat_id, film_id, title, link, plan_type, plan_id],
                id=f'plan_notify_{chat_id}_{film_id}_{int(plan_utc.timestamp())}'
            )
            logger.info(f"[PLAN] Уведомление запланировано через scheduler на {plan_utc}, plan_id={plan_id}")
        except Exception as e:
            logger.warning(f"[PLAN] Не удалось запланировать уведомление через scheduler: {e}. Будет отправлено через периодическую проверку.")
        
        # Периодическая проверка также отправит уведомление, если scheduler не сработает

        del user_plan_state[user_id]

# Обработка ответов на сообщения об ошибках планирования
@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in plan_error_messages)
def handle_plan_error_reply(message):
    """Обрабатывает ответы на сообщения об ошибках планирования для дополнения недостающих данных"""
    # Пропускаем команды - они обрабатываются отдельными обработчиками
    if message.text and message.text.startswith('/'):
        return
    
    try:
        reply_msg_id = message.reply_to_message.message_id
        error_data = plan_error_messages.get(reply_msg_id)
        
        if not error_data:
            return
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверяем, что это тот же пользователь
        if error_data['user_id'] != user_id:
            return
        
        text = (message.text or '').strip().lower()
        logger.info(f"[PLAN ERROR REPLY] Reply received: text='{text}', missing={error_data['missing']}")
        
        link = error_data['link']
        plan_type = error_data['plan_type']
        day_or_date = error_data['day_or_date']
        missing = error_data['missing']
        
        # Дополняем недостающие данные
        if missing == 'plan_type':
            # Определяем тип из ответа
            if 'дома' in text:
                plan_type = 'home'
            elif 'в кино' in text or 'кино' in text:
                plan_type = 'cinema'
            else:
                # Пробуем определить по контексту
                if 'кино' in text:
                    plan_type = 'cinema'
                else:
                    plan_type = 'home'  # По умолчанию
            
            if not plan_type:
                bot.reply_to(message, "Не удалось определить тип просмотра. Укажите 'дома' или 'в кино'.")
                return
            
            logger.info(f"[PLAN ERROR REPLY] plan_type determined: {plan_type}")
        
        elif missing == 'day_or_date':
            # Парсим дату/день недели из ответа
            # Сначала ищем день недели
            sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
            for phrase in sorted_phrases:
                if phrase in text:
                    day_or_date = phrase
                    break
            
            # Если день недели не найден, ищем дату
            if not day_or_date:
                # Пробуем разные форматы даты
                date_match = re.search(r'(?:с|на|до)?\s*(\d{1,2})\s+([а-яё]+)', text)
                if date_match:
                    day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
                else:
                    # Формат "15.01", "15/01", "15.01.25", "15.01.2025"
                    date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
                    if date_match:
                        day_num = int(date_match.group(1))
                        month_num = int(date_match.group(2))
                        if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                            month_names = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                         'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                            day_or_date = f"{day_num} {month_names[month_num - 1]}"
            
            if not day_or_date:
                bot.reply_to(message, "Не удалось определить день/дату. Укажите день недели или дату.")
                return
            
            logger.info(f"[PLAN ERROR REPLY] day_or_date determined: {day_or_date}")
        
        # Теперь у нас есть все данные, пытаемся планировать
        if link and plan_type and day_or_date:
            # Получаем время сообщения в UTC
            message_date_utc = None
            if message.date:
                message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc)
            
            # Удаляем из plan_error_messages
            del plan_error_messages[reply_msg_id]
            
            # Вызываем process_plan
            result = process_plan(user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
            if result == 'NEEDS_TIMEZONE':
                show_timezone_selection(chat_id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
            elif not result:
                bot.reply_to(message, "❌ Ошибка при планировании. Проверьте формат даты.")
        else:
            logger.warning(f"[PLAN ERROR REPLY] Still missing data: link={bool(link)}, plan_type={plan_type}, day_or_date={day_or_date}")
    
    except Exception as e:
        logger.error(f"[PLAN ERROR REPLY] Error processing error reply: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Ошибка при обработке ответа.")
        except:
            pass

# /schedule — список запланированных просмотров
@bot.message_handler(commands=['schedule'])
def show_schedule(message):
    logger.info(f"[SCHEDULE COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[SCHEDULE COMMAND] /schedule вызван от {message.from_user.id}")
    logger.info(f"[SCHEDULE COMMAND] message.text={message.text}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/schedule', message.chat.id)
        logger.info(f"Команда /schedule от пользователя {message.from_user.id}")
        
        chat_id = message.chat.id
        user_id = message.from_user.id
        # Используем часовой пояс пользователя для отображения
        user_tz = get_user_timezone_or_default(user_id)
        
        with db_lock:
            cursor.execute('''
                SELECT m.title, p.plan_datetime, p.plan_type
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND m.chat_id = p.chat_id
                WHERE p.chat_id = %s
                ORDER BY p.plan_type DESC, p.plan_datetime ASC
            ''', (chat_id,))
            rows = cursor.fetchall()
        
        if not rows:
            bot.reply_to(message, "📅 Нет запланированных просмотров.")
            return
        
        # Разделяем на секции: сначала кино, потом дома
        cinema_plans = []
        home_plans = []
        
        for row in rows:
            # RealDictCursor возвращает словари, но поддерживает доступ по индексу
            title = row.get('title') if isinstance(row, dict) else row[0]
            plan_dt_value = row.get('plan_datetime') if isinstance(row, dict) else row[1]
            plan_type = row.get('plan_type') if isinstance(row, dict) else row[2]
            # Преобразуем TIMESTAMP в дату в часовом поясе пользователя
            try:
                # psycopg2 возвращает объект datetime для TIMESTAMP WITH TIME ZONE
                if isinstance(plan_dt_value, datetime):
                    # Если уже объект datetime, конвертируем в нужную таймзону
                    if plan_dt_value.tzinfo is None:
                        # Если нет таймзоны, предполагаем UTC
                        plan_dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                    else:
                        plan_dt = plan_dt_value.astimezone(user_tz)
                elif isinstance(plan_dt_value, str):
                    # Fallback для старых данных (если миграция еще не применена)
                    plan_dt_iso = plan_dt_value
                    if plan_dt_iso.endswith('Z'):
                        plan_dt = datetime.fromisoformat(plan_dt_iso.replace('Z', '+00:00')).astimezone(user_tz)
                    elif '+' in plan_dt_iso or plan_dt_iso.count('-') > 2:
                        plan_dt = datetime.fromisoformat(plan_dt_iso).astimezone(user_tz)
                    else:
                        plan_dt = datetime.fromisoformat(plan_dt_iso + '+00:00').astimezone(user_tz)
                else:
                    # Неожиданный тип
                    logger.warning(f"Неожиданный тип plan_datetime: {type(plan_dt_value)}")
                    continue
                
                date_str = plan_dt.strftime('%d.%m.%Y %H:%M')
                plan_info = (title, date_str)
                
                if plan_type == 'cinema':
                    cinema_plans.append(plan_info)
                else:  # home
                    home_plans.append(plan_info)
            except Exception as e:
                logger.error(f"Ошибка при обработке даты {plan_dt_value}: {e}")
                # Fallback: пытаемся извлечь дату из строки или использовать текущую дату
                if isinstance(plan_dt_value, str):
                    date_str = plan_dt_value[:10] if len(plan_dt_value) >= 10 else plan_dt_value
                else:
                    date_str = datetime.now(user_tz).strftime('%d.%m.%Y')
                plan_info = (title, date_str)
                
                if plan_type == 'cinema':
                    cinema_plans.append(plan_info)
                else:  # home
                    home_plans.append(plan_info)
        
        # Формируем текст с секциями
        text = "📅 Запланированные просмотры:\n\n"
        
        # Секция: Премьеры в кино
        if cinema_plans:
            text += "🎦 Премьеры в кино:\n"
            for title, date_str in cinema_plans:
                text += f"• <b>{title}</b> — {date_str}\n"
            text += "\n"
        
        # Секция: Просмотры дома
        if home_plans:
            text += "🏠 Просмотры дома:\n"
            for title, date_str in home_plans:
                text += f"• <b>{title}</b> — {date_str}\n"
            text += "\n"
        
        text += "Приятного просмотра! 🍿"
        bot.reply_to(message, text, parse_mode='HTML')
        
        # Отдельным сообщением показываем раздел "Ожидаю" (фильмы, которые выйдут через 2+ месяца)
        now = datetime.now(user_tz).date()
        two_months_later = now + timedelta(days=60)  # Примерно 2 месяца
        
        with db_lock:
            cursor.execute('''
                SELECT kp_id, film_title, premiere_date
                FROM premiere_reminders
                WHERE chat_id = %s AND user_id = %s AND reminder_sent = FALSE
                AND premiere_date > %s
                ORDER BY premiere_date ASC
            ''', (chat_id, user_id, two_months_later))
            waiting_rows = cursor.fetchall()
        
        if waiting_rows:
            waiting_text = "⏳ <b>Ожидаю:</b>\n\n"
            for row in waiting_rows:
                kp_id = row.get('kp_id') if isinstance(row, dict) else row[0]
                title = row.get('film_title') if isinstance(row, dict) else row[1]
                premiere_date = row.get('premiere_date') if isinstance(row, dict) else row[2]
                
                if isinstance(premiere_date, date):
                    date_str = premiere_date.strftime('%d.%m.%Y')
                else:
                    date_str = str(premiere_date)
                
                waiting_text += f"• <b>{title}</b> — {date_str}\n"
            
            bot.send_message(chat_id, waiting_text, parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Ошибка в /schedule: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /schedule")
        except:
            pass

# /help
@bot.message_handler(commands=['help'])
def help_command(message):
    logger.info(f"[HANDLER] /help вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/help', message.chat.id)
    logger.info(f"Команда /help от пользователя {message.from_user.id}")
    text = """*🎬 Помощь по командам бота:*

*/list* — Показать список непросмотренных фильмов
*/random* — Выбрать случайный непросмотренный фильм с фильтрами (год, жанр, режиссёр)
*/search* — Поиск фильмов через Kinopoisk API
*/total* — Статистика: фильмы, жанры, режиссёры, актёры, оценки
*/stats* — Детальная статистика группы и участников
*/rate* — Оценить просмотренные фильмы
*/plan* — Запланировать просмотр фильма (дома/в кино)
*/schedule* — Показать список запланированных просмотров
*/settings* — Настроить эмодзи для отметки просмотренных фильмов
*/clean* — Удалить оценку, просмотр, план или обнулить базу
*/help* — Эта справка

*Как использовать:*
1. Отправьте ссылку на фильм с Кинопоиска — бот автоматически добавит его
2. Поставьте реакцию ✅ (или настроенное эмодзи) на сообщение со ссылкой — фильм будет отмечен как просмотренный
3. После отметки напишите оценку от 1 до 10

*Приятного просмотра!* 🍿"""
    
    bot.reply_to(message, text, parse_mode='Markdown')

# /seasons - команда для просмотра сезонов сериалов
@bot.message_handler(commands=['seasons'])
def seasons_command(message):
    """Команда /seasons - просмотр сезонов сериалов"""
    logger.info(f"[HANDLER] /seasons вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/seasons', message.chat.id)
    
    chat_id = message.chat.id
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        series = cursor.fetchall()
    
    if not series:
        bot.reply_to(message, "📺 Нет сериалов в базе.")
        return
    
    markup = InlineKeyboardMarkup(row_width=1)
    for row in series:
        if isinstance(row, dict):
            title = row.get('title')
            kp_id = row.get('kp_id')
            film_id = row.get('id')
        else:
            film_id = row[0]
            title = row[1]
            kp_id = row[2]
        
        button_text = title
        if len(button_text) > 50:
            button_text = button_text[:47] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{kp_id}"))
    
    bot.reply_to(message, "📺 <b>Выберите сериал:</b>", reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
def show_seasons_callback(call):
    """Показывает сезоны выбранного сериала"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем актуальные данные о сезонах (с обновленными статусами просмотра)
        seasons_text = get_seasons(kp_id, chat_id, user_id)
        
        if seasons_text:
            # Получаем film_id для подписки
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                film_id = row.get('id') if isinstance(row, dict) else (row[0] if row else None)
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("✅ Отметить сезоны/серии", callback_data=f"series_track:{kp_id}"))
            
            # Проверяем, подписан ли пользователь
            if film_id:
                cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                sub_row = cursor.fetchone()
                is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
                
                if is_subscribed:
                    markup.add(InlineKeyboardButton("🔕 Отписаться от уведомлений", callback_data=f"series_unsubscribe:{kp_id}"))
                else:
                    markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
            
            # Обновляем сообщение с актуальными данными о сезонах
            bot.edit_message_text(seasons_text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            logger.debug(f"[SEASONS] Обновлен список сезонов для kp_id={kp_id}, user_id={user_id}")
        else:
            bot.edit_message_text("❌ Не удалось получить информацию о сезонах.", chat_id, call.message.message_id)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[SEASONS] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

# /premieres - команда для просмотра премьер
@bot.message_handler(commands=['premieres'])
def premieres_command(message):
    """Команда /premieres - выбор периода для просмотра премьер"""
    logger.info(f"[HANDLER] /premieres вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/premieres', message.chat.id)
    
    # Показываем выбор периода
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📅 Текущий месяц", callback_data="premieres_period:current_month"))
    markup.add(InlineKeyboardButton("📅 Следующий месяц", callback_data="premieres_period:next_month"))
    markup.add(InlineKeyboardButton("📅 3 месяца", callback_data="premieres_period:3_months"))
    markup.add(InlineKeyboardButton("📅 6 месяцев", callback_data="premieres_period:6_months"))
    markup.add(InlineKeyboardButton("📅 Текущий год", callback_data="premieres_period:current_year"))
    markup.add(InlineKeyboardButton("📅 Ближайший год", callback_data="premieres_period:next_year"))
    
    bot.reply_to(message, "📅 <b>Выберите период для просмотра премьер:</b>", reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_period:"))
def premieres_period_callback(call):
    """Обработчик выбора периода для премьер"""
    try:
        period = call.data.split(":")[1]
        chat_id = call.message.chat.id
        
        # Получаем премьеры для выбранного периода
        premieres = get_premieres_for_period(period)
        
        if not premieres:
            bot.edit_message_text("❌ Не удалось получить список премьер для выбранного периода.", chat_id, call.message.message_id)
            bot.answer_callback_query(call.id)
            return
        
        # Сохраняем премьеры для пагинации (можно использовать временное хранилище или передавать через callback_data)
        # Для простоты будем показывать первую страницу
        show_premieres_page(call, premieres, period, page=0)
        
    except Exception as e:
        logger.error(f"[PREMIERES PERIOD] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

def show_premieres_page(call, premieres, period, page=0):
    """Показывает страницу премьер с пагинацией"""
    try:
        chat_id = call.message.chat.id
        items_per_page = 10
        total_pages = (len(premieres) + items_per_page - 1) // items_per_page
        start_idx = page * items_per_page
        end_idx = min(start_idx + items_per_page, len(premieres))
        
        period_names = {
            'current_month': 'текущего месяца',
            'next_month': 'следующего месяца',
            '3_months': '3 месяцев',
            '6_months': '6 месяцев',
            'current_year': 'текущего года',
            'next_year': 'ближайшего года'
        }
        period_name = period_names.get(period, 'периода')
        
        text = f"📅 <b>Премьеры {period_name}:</b>\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Сортируем премьеры по дате выхода
        def get_premiere_date(p):
            """Извлекает дату премьеры из данных"""
            # Пробуем разные форматы дат
            if p.get('premiereRuDate'):
                try:
                    return datetime.strptime(p.get('premiereRuDate'), '%Y-%m-%d').date()
                except:
                    pass
            if p.get('year') and p.get('month'):
                try:
                    day = p.get('day', 1)
                    return datetime(int(p.get('year')), int(p.get('month')), int(day)).date()
                except:
                    pass
            return datetime(2099, 12, 31).date()  # Для сортировки - в конец
        
        premieres_sorted = sorted(premieres, key=get_premiere_date)
        
        for p in premieres_sorted[start_idx:end_idx]:
            kp_id = p.get('kinopoiskId') or p.get('filmId')
            title_ru = p.get('nameRu') or p.get('nameEn') or "Без названия"
            
            # Получаем дату выхода
            premiere_date = get_premiere_date(p)
            date_str = ""
            if premiere_date and premiere_date.year < 2099:
                date_str = f" ({premiere_date.strftime('%d.%m.%Y')})"
            elif p.get('year') and p.get('month'):
                year = p.get('year')
                month = p.get('month')
                day = p.get('day')
                if day:
                    date_str = f" ({day:02d}.{month:02d}.{year})"
                else:
                    date_str = f" ({month:02d}.{year})"
            
            text += f"• <b>{title_ru}</b>{date_str}\n"
            
            button_text = title_ru
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"premiere_detail:{kp_id}"))
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"premieres_page:{period}:{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"premieres_page:{period}:{page+1}"))
        
        if nav_buttons:
            markup.add(*nav_buttons)
        
        text += f"\nСтраница {page + 1} из {total_pages}"
        text += "\n\nВыберите фильм для подробностей:"
        
        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_page:"))
def premieres_page_callback(call):
    """Обработчик пагинации премьер"""
    try:
        parts = call.data.split(":")
        period = parts[1]
        page = int(parts[2])
        
        # Получаем премьеры заново (можно оптимизировать, сохраняя в кэш)
        premieres = get_premieres_for_period(period)
        show_premieres_page(call, premieres, period, page)
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_detail:"))
def premiere_detail_handler(call):
    """Показывает детали премьеры с постером и трейлером"""
    logger.info(f"[PREMIERES] Детали премьеры: {call.data}")
    kp_id = call.data.split(":")[1]
    chat_id = call.message.chat.id
    
    # Получаем полную информацию о фильме
    headers = {'X-API-KEY': KP_TOKEN}
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
    
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            bot.answer_callback_query(call.id, "Не удалось загрузить данные фильма", show_alert=True)
            return
        
        data = response.json()
        
        title = data.get('nameRu') or data.get('nameOriginal') or "Без названия"
        year = data.get('year') or '—'
        poster_url = data.get('posterUrlPreview') or data.get('posterUrl')
        trailer_url = None
        
        # Ищем трейлер
        videos = data.get('videos', {}).get('trailers', [])
        if videos:
            trailer_url = videos[0].get('url')  # Первый трейлер
        
        description = data.get('description') or data.get('shortDescription') or "Нет описания"
        genres = ', '.join([g['genre'] for g in data.get('genres', [])]) or '—'
        countries = ', '.join([c['country'] for c in data.get('countries', [])]) or '—'
        
        # Получаем дату премьеры из данных о премьерах
        premiere_date = None
        premiere_date_str = ""
        # Пробуем найти дату в данных фильма
        # Проверяем разные поля с датами
        for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
            date_value = data.get(date_field)
            if date_value:
                try:
                    # Пробуем разные форматы
                    for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%d.%m.%Y']:
                        try:
                            if 'T' in str(date_value):
                                premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                            else:
                                premiere_date = datetime.strptime(str(date_value), fmt).date()
                            premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                            break
                        except:
                            continue
                    if premiere_date:
                        break
                except:
                    continue
        
        text = f"<b>{title}</b> ({year})\n\n"
        if premiere_date_str:
            text += f"📅 Премьера: {premiere_date_str}\n\n"
        text += f"{description}\n\n"
        text += f"🌍 {countries}\n"
        text += f"🎭 {genres}\n"
        
        if trailer_url:
            text += f"\n<a href='{trailer_url}'>📺 Смотреть трейлер</a>"
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"premiere_add:{kp_id}"))
        
        # Кнопка напоминания о премьере (только если есть дата)
        if premiere_date:
            # Проверяем, не установлено ли уже напоминание
            with db_lock:
                cursor.execute('''
                    SELECT id FROM premiere_reminders 
                    WHERE chat_id = %s AND user_id = %s AND kp_id = %s
                ''', (chat_id, call.from_user.id, kp_id))
                existing = cursor.fetchone()
            
            if not existing:
                # Используем безопасный формат даты для callback_data (без двоеточий)
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Напомнить о выходе премьеры", callback_data=f"premiere_remind:{kp_id}:{date_for_callback}"))
        
        # Отправляем с постером
        if poster_url:
            try:
                bot.send_photo(
                    chat_id,
                    poster_url,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=markup
                )
                bot.delete_message(chat_id, call.message.message_id)  # Удаляем старое сообщение
            except Exception as e:
                logger.error(f"[PREMIERES DETAIL] Ошибка отправки фото: {e}")
                # Если не удалось отправить фото, отправляем текст
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML',
                    reply_markup=markup,
                    disable_web_page_preview=False
                )
        else:
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=markup,
                disable_web_page_preview=False
            )
        
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"[PREMIERES DETAIL] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка загрузки фильма", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_add:"))
def premiere_add_to_db(call):
    """Добавляет премьеру в базу"""
    try:
        kp_id = call.data.split(":")[1]
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        chat_id = call.message.chat.id
        
        # Проверяем, есть ли фильм уже в базе
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            existing = cursor.fetchone()
        
        if existing:
            # Фильм уже есть - просто показываем маленькое уведомление
            bot.answer_callback_query(call.id, "ℹ️ Фильм уже есть в базе")
            return
        
        # Добавляем в базу через существующую функцию
        if add_and_announce(link, chat_id):
            bot.answer_callback_query(call.id, "✅ Фильм добавлен в базу!")
        else:
            bot.answer_callback_query(call.id, "❌ Не удалось добавить фильм")
    except Exception as e:
        logger.error(f"[PREMIERE ADD] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка обработки")

@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_remind:"))
def premiere_remind_handler(call):
    """Устанавливает напоминание о выходе премьеры"""
    try:
        parts = call.data.split(":")
        kp_id = parts[1]
        # Дата может содержать дефисы вместо точек, если была заменена
        premiere_date_str = parts[2] if len(parts) > 2 else None
        if premiere_date_str:
            premiere_date_str = premiere_date_str.replace('-', '.')  # Возвращаем обратно точки
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем информацию о фильме
        headers = {'X-API-KEY': KP_TOKEN}
        url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
        response = requests.get(url, headers=headers, timeout=15)
        
        if response.status_code != 200:
            bot.answer_callback_query(call.id, "Не удалось получить данные фильма", show_alert=True)
            return
        
        data = response.json()
        title = data.get('nameRu') or data.get('nameOriginal') or "Без названия"
        
        # Парсим дату премьеры (используем те же методы, что и в premiere_detail_handler)
        premiere_date = None
        if premiere_date_str:
            try:
                premiere_date = datetime.strptime(premiere_date_str, '%d.%m.%Y').date()
            except:
                pass
        
        # Если не получилось из строки, пробуем найти в данных фильма
        if not premiere_date:
            for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                date_value = data.get(date_field)
                if date_value:
                    try:
                        for fmt in ['%Y-%m-%d', '%Y-%m-%dT%H:%M:%S', '%d.%m.%Y']:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), fmt).date()
                                break
                            except:
                                continue
                        if premiere_date:
                            break
                    except:
                        continue
        
        if not premiere_date:
            bot.answer_callback_query(call.id, "Не удалось определить дату премьеры", show_alert=True)
            return
        
        # Сохраняем напоминание в базу
        with db_lock:
            cursor.execute('''
                INSERT INTO premiere_reminders (chat_id, user_id, kp_id, film_title, premiere_date)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, user_id, kp_id) DO UPDATE 
                SET premiere_date = EXCLUDED.premiere_date, reminder_sent = FALSE
            ''', (chat_id, user_id, kp_id, title, premiere_date))
            conn.commit()
        
        # Планируем уведомление на дату премьеры
        user_tz = get_user_timezone_or_default(user_id)
        reminder_dt = user_tz.localize(datetime.combine(premiere_date, datetime.min.time().replace(hour=9, minute=0)))
        reminder_utc = reminder_dt.astimezone(pytz.utc)
        
        scheduler.add_job(
            send_premiere_reminder,
            'date',
            run_date=reminder_utc,
            args=[chat_id, user_id, kp_id, title],
            id=f'premiere_remind_{chat_id}_{user_id}_{kp_id}_{int(reminder_utc.timestamp())}'
        )
        
        bot.answer_callback_query(call.id, f"✅ Напоминание установлено на {premiere_date_str}")
        
    except Exception as e:
        logger.error(f"[PREMIERE REMIND] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка установки напоминания", show_alert=True)

def send_premiere_reminder(chat_id, user_id, kp_id, title):
    """Отправляет напоминание о выходе премьеры"""
    try:
        message = f"🎬 <b>{title}</b> выходит в прокат сегодня! 🎉"
        bot.send_message(chat_id, message, parse_mode='HTML')
        
        # Отмечаем напоминание как отправленное
        with db_lock:
            cursor.execute('''
                UPDATE premiere_reminders 
                SET reminder_sent = TRUE 
                WHERE chat_id = %s AND user_id = %s AND kp_id = %s
            ''', (chat_id, user_id, kp_id))
            conn.commit()
    except Exception as e:
        logger.error(f"[PREMIERE REMINDER] Ошибка отправки: {e}", exc_info=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_track:"))
def series_track_callback(call):
    """Обработчик для отметки сезонов/серий как просмотренных"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем film_id
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
        
        # Получаем сезоны из API
        seasons_data = get_seasons_data(kp_id)
        if not seasons_data:
            bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сезонах", show_alert=True)
            return
        
        # Показываем меню выбора сезона с отметками статуса
        markup = InlineKeyboardMarkup(row_width=1)
        for season in seasons_data:
            season_num = season.get('number', '')
            episodes = season.get('episodes', [])
            episodes_count = len(episodes)
            
            # Проверяем статус сезона
            watched_count = 0
            for ep in episodes:
                ep_num = ep.get('episodeNumber', '')
                cursor.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s AND watched = TRUE
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor.fetchone()
                if watched_row:
                    watched_count += 1
            
            # Определяем статус
            if watched_count == episodes_count and episodes_count > 0:
                status_emoji = "✅"
            elif watched_count > 0:
                status_emoji = "⏳"
            else:
                status_emoji = "⬜"
            
            button_text = f"{status_emoji} Сезон {season_num} ({episodes_count} эп.)"
            if watched_count > 0 and watched_count < episodes_count:
                button_text += f" [{watched_count}/{episodes_count}]"
            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_season:{kp_id}:{season_num}"))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"seasons_kp:{kp_id}"))
        
        bot.edit_message_text(
            f"📺 <b>{title}</b>\n\nВыберите сезон для отметки просмотренных эпизодов:",
            chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[SERIES TRACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_season:"))
def series_season_callback(call):
    """Обработчик для выбора сезона и отметки эпизодов"""
    try:
        parts = call.data.split(":")
        kp_id = parts[1]
        season_num = parts[2]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем film_id
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
        
        # Получаем эпизоды сезона
        seasons_data = get_seasons_data(kp_id)
        season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
        if not season:
            bot.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
            return
        
        episodes = season.get('episodes', [])
        
        # Показываем эпизоды с возможностью отметить
        text = f"📺 <b>{title}</b> - Сезон {season_num}\n\n"
        markup = InlineKeyboardMarkup(row_width=2)
        
        for ep in episodes[:20]:  # Показываем первые 20 эпизодов
            ep_num = ep.get('episodeNumber', '')
            release = ep.get('releaseDate', '—')
            
            # Проверяем, просмотрен ли эпизод
            cursor.execute('''
                SELECT watched FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                AND season_number = %s AND episode_number = %s
            ''', (chat_id, film_id, user_id, season_num, ep_num))
            watched_row = cursor.fetchone()
            is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            
            mark = "✅" if is_watched else "⬜"
            button_text = f"{mark} {ep_num}"
            if len(button_text) > 20:
                button_text = button_text[:17] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_episode:{kp_id}:{season_num}:{ep_num}"))
        
        if len(episodes) > 20:
            text += f"... и ещё {len(episodes) - 20} эпизодов\n\n"
        text += "Нажмите на эпизод, чтобы отметить как просмотренный"
        
        # Добавляем кнопку "Все просмотрены"
        # Проверяем, все ли эпизоды просмотрены
        all_watched = True
        for ep in episodes:
            ep_num = ep.get('episodeNumber', '')
            cursor.execute('''
                SELECT watched FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                AND season_number = %s AND episode_number = %s
            ''', (chat_id, film_id, user_id, season_num, ep_num))
            watched_row = cursor.fetchone()
            is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            if not is_watched:
                all_watched = False
                break
        
        if not all_watched:
            markup.add(InlineKeyboardButton("✅ Все просмотрены", callback_data=f"series_season_all:{kp_id}:{season_num}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад к сезонам", callback_data=f"series_track:{kp_id}"))
        
        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[SERIES SEASON] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_episode:"))
def series_episode_callback(call):
    """Обработчик для отметки эпизода как просмотренного"""
    try:
        parts = call.data.split(":")
        kp_id = parts[1]
        season_num = parts[2]
        ep_num = parts[3]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем film_id
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            
            # Переключаем статус просмотра
            cursor.execute('''
                INSERT INTO series_tracking (chat_id, film_id, kp_id, user_id, season_number, episode_number, watched, watched_date)
                VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number)
                DO UPDATE SET watched = NOT series_tracking.watched, watched_date = CASE WHEN NOT series_tracking.watched THEN NOW() ELSE series_tracking.watched_date END
            ''', (chat_id, film_id, kp_id, user_id, season_num, ep_num))
            conn.commit()
            
            # Получаем новый статус
            cursor.execute('''
                SELECT watched FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                AND season_number = %s AND episode_number = %s
            ''', (chat_id, film_id, user_id, season_num, ep_num))
            watched_row = cursor.fetchone()
            is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            
            status = "✅ отмечен как просмотренный" if is_watched else "⬜ снята отметка о просмотре"
            bot.answer_callback_query(call.id, status)
            
            # Обновляем список эпизодов (визуально обновляем чекбоксы)
            call.data = f"series_season:{kp_id}:{season_num}"
            series_season_callback(call)
    except Exception as e:
        logger.error(f"[SERIES EPISODE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_season_all:"))
def series_season_all_callback(call):
    """Обработчик для отметки всех эпизодов сезона как просмотренных"""
    try:
        parts = call.data.split(":")
        kp_id = parts[1]
        season_num = parts[2]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем film_id
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
        
        # Получаем эпизоды сезона
        seasons_data = get_seasons_data(kp_id)
        season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
        if not season:
            bot.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
            return
        
        episodes = season.get('episodes', [])
        
        # Отмечаем все эпизоды как просмотренные
        marked_count = 0
        with db_lock:
            for ep in episodes:
                ep_num = ep.get('episodeNumber', '')
                # Проверяем, не просмотрен ли уже
                cursor.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s AND watched = TRUE
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                already_watched = cursor.fetchone()
                
                if not already_watched:
                    cursor.execute('''
                        INSERT INTO series_tracking (chat_id, film_id, kp_id, user_id, season_number, episode_number, watched, watched_date)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE, NOW())
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number)
                        DO UPDATE SET watched = TRUE, watched_date = NOW()
                    ''', (chat_id, film_id, kp_id, user_id, season_num, ep_num))
                    marked_count += 1
            conn.commit()
        
        bot.answer_callback_query(call.id, f"✅ Отмечено {marked_count} эпизодов как просмотренные")
        
        # Обновляем список эпизодов
        call.data = f"series_season:{kp_id}:{season_num}"
        series_season_callback(call)
    except Exception as e:
        logger.error(f"[SERIES SEASON ALL] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_subscribe:"))
def series_subscribe_callback(call):
    """Обработчик для подписки на новые серии"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            
            cursor.execute('''
                INSERT INTO series_subscriptions (chat_id, film_id, kp_id, user_id, subscribed)
                VALUES (%s, %s, %s, %s, TRUE)
                ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET subscribed = TRUE
            ''', (chat_id, film_id, kp_id, user_id))
            conn.commit()
        
        bot.answer_callback_query(call.id, "✅ Подписка активирована! Вы будете получать уведомления о новых сериях в 9:00 утра.")
        
        # Обновляем кнопки
        call.data = f"seasons_kp:{kp_id}"
        show_seasons_callback(call)
    except Exception as e:
        logger.error(f"[SERIES SUBSCRIBE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_unsubscribe:"))
def series_unsubscribe_callback(call):
    """Обработчик для отписки от уведомлений о новых сериях"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            
            cursor.execute('''
                UPDATE series_subscriptions 
                SET subscribed = FALSE 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            ''', (chat_id, film_id, user_id))
            conn.commit()
        
        bot.answer_callback_query(call.id, "🔕 Вы отписаны от уведомлений")
        
        # Обновляем кнопки
        call.data = f"seasons_kp:{kp_id}"
        show_seasons_callback(call)
    except Exception as e:
        logger.error(f"[SERIES UNSUBSCRIBE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

# /clean
@bot.message_handler(commands=['dbcheck'])
def dbcheck_command(message):
    """Диагностическая команда для проверки данных в БД"""
    logger.info(f"[HANDLER] /dbcheck вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/dbcheck', message.chat.id)
        chat_id = message.chat.id
        
        text = "🔍 <b>Диагностика базы данных</b>\n\n"
        
        with db_lock:
            # Проверяем таблицу movies
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
            movies_count = cursor.fetchone()
            movies_total = movies_count.get('count') if isinstance(movies_count, dict) else (movies_count[0] if movies_count else 0)
            
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s AND watched = 0', (chat_id,))
            movies_unwatched = cursor.fetchone()
            unwatched = movies_unwatched.get('count') if isinstance(movies_unwatched, dict) else (movies_unwatched[0] if movies_unwatched else 0)
            
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s AND watched = 1', (chat_id,))
            movies_watched = cursor.fetchone()
            watched = movies_watched.get('count') if isinstance(movies_watched, dict) else (movies_watched[0] if movies_watched else 0)
            
            text += f"🎬 <b>Фильмы:</b>\n"
            text += f"• Всего: {movies_total}\n"
            text += f"• Непросмотренных: {unwatched}\n"
            text += f"• Просмотренных: {watched}\n\n"
            
            # Проверяем таблицу stats
            cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s', (chat_id,))
            stats_count = cursor.fetchone()
            stats_total = stats_count.get('count') if isinstance(stats_count, dict) else (stats_count[0] if stats_count else 0)
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM stats WHERE chat_id = %s', (chat_id,))
            stats_users = cursor.fetchone()
            unique_users = stats_users.get('count') if isinstance(stats_users, dict) else (stats_users[0] if stats_users else 0)
            
            # Проверяем записи за последние 30 дней
            thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
            cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s AND timestamp > %s', (chat_id, thirty_days_ago))
            stats_recent = cursor.fetchone()
            recent_stats = stats_recent.get('count') if isinstance(stats_recent, dict) else (stats_recent[0] if stats_recent else 0)
            
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM stats WHERE chat_id = %s AND timestamp > %s', (chat_id, thirty_days_ago))
            stats_recent_users = cursor.fetchone()
            recent_users = stats_recent_users.get('count') if isinstance(stats_recent_users, dict) else (stats_recent_users[0] if stats_recent_users else 0)
            
            text += f"📊 <b>Статистика (stats):</b>\n"
            text += f"• Всего записей: {stats_total}\n"
            text += f"• Уникальных пользователей: {unique_users}\n"
            text += f"• Записей за 30 дней: {recent_stats}\n"
            text += f"• Активных пользователей за 30 дней: {recent_users}\n\n"
            
            # Последние 5 записей из stats
            cursor.execute('''
                SELECT user_id, username, command_or_action, timestamp
                FROM stats
                WHERE chat_id = %s
                ORDER BY timestamp DESC
                LIMIT 5
            ''', (chat_id,))
            recent_actions = cursor.fetchall()
            
            if recent_actions:
                text += f"📝 <b>Последние действия:</b>\n"
                for row in recent_actions:
                    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    username = row.get('username') if isinstance(row, dict) else row[1]
                    command = row.get('command_or_action') if isinstance(row, dict) else row[2]
                    timestamp = row.get('timestamp') if isinstance(row, dict) else row[3]
                    text += f"• {username} ({user_id}): {command} [{timestamp}]\n"
            else:
                text += f"⚠️ <b>Нет записей в stats для этого чата!</b>\n"
                text += f"Это означает, что команды не логируются в БД.\n"
                text += f"Проверьте логи на наличие ошибок в log_request().\n"
            
            # Проверяем таблицу ratings
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
            ratings_count = cursor.fetchone()
            ratings_total = ratings_count.get('count') if isinstance(ratings_count, dict) else (ratings_count[0] if ratings_count else 0)
            
            text += f"\n⭐ <b>Оценки:</b> {ratings_total}\n"
            
            # Проверяем таблицу plans
            cursor.execute('SELECT COUNT(*) FROM plans WHERE chat_id = %s', (chat_id,))
            plans_count = cursor.fetchone()
            plans_total = plans_count.get('count') if isinstance(plans_count, dict) else (plans_count[0] if plans_count else 0)
            
            text += f"📅 <b>Планы:</b> {plans_total}\n"
        
        bot.reply_to(message, text, parse_mode='HTML')
        logger.info(f"✅ Ответ на /dbcheck отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /dbcheck: {e}", exc_info=True)
        try:
            bot.reply_to(message, f"Произошла ошибка при проверке БД: {e}")
        except:
            pass

@bot.message_handler(commands=['clean'])
def clean_command(message):
    logger.info(f"[HANDLER] /clean вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/clean', message.chat.id)
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Показываем меню только с опциями массового удаления
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("💥 Обнулить базу чата", callback_data="clean:chat_db"))
    markup.add(InlineKeyboardButton("👤 Обнулить базу пользователя", callback_data="clean:user_db"))
    
    help_text = (
        "🧹 <b>Массовое удаление данных</b>\n\n"
        "<b>💥 Обнулить базу чата</b> — удаляет <b>ВСЕ данные чата</b>:\n"
        "• Все фильмы\n"
        "• Все оценки всех пользователей\n"
        "• Все планы и расписание всех пользователей\n"
        "• Все билеты\n"
        "• Все настройки\n\n"
        "<b>👤 Обнулить базу пользователя</b> — удаляет <b>только ваши данные в этом чате</b>:\n"
        "• Ваши оценки\n"
        "• Ваши планы и расписание\n"
        "• Ваши билеты\n"
        "• Ваша статистика\n"
        "• Ваши настройки (включая часовой пояс)\n\n"
        "<i>Фильмы и данные других пользователей останутся без изменений.</i>\n\n"
        "Выберите действие:"
    )
    bot.reply_to(message, help_text, reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("clean:"))
def clean_action_choice(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    action = call.data.split(":")[1]
    
    user_clean_state[user_id] = {'action': action}
    
    if action == 'chat_db':
        # Обнуление базы чата - требует голосования в группах
        if call.message.chat.type in ['group', 'supergroup']:
            try:
                members_count = bot.get_chat_members_count(chat_id)
                # Получаем список активных участников
                # Сначала пробуем получить реальное количество участников через Telegram API
                try:
                    chat_member_count = bot.get_chat_member_count(chat_id)
                    logger.info(f"[CLEAN] Количество участников чата через API: {chat_member_count}")
                except Exception as api_error:
                    logger.warning(f"[CLEAN] Не удалось получить количество участников через API: {api_error}")
                    chat_member_count = None
                
                # Получаем список активных участников из stats (за последние 30 дней)
                with db_lock:
                    # Используем тот же формат, что и в log_request: '%Y-%m-%d %H:%M:%S'
                    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                    logger.info(f"[CLEAN] Поиск активных участников для chat_id={chat_id}, thirty_days_ago={thirty_days_ago}")
                    
                    # Сначала проверим, есть ли вообще записи в stats для этого чата
                    cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s', (chat_id,))
                    total_stats = cursor.fetchone()
                    total_count = total_stats.get('count') if isinstance(total_stats, dict) else (total_stats[0] if total_stats else 0)
                    logger.info(f"[CLEAN] Всего записей в stats для chat_id={chat_id}: {total_count}")
                    
                    # Проверим записи за последние 30 дней
                    cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s AND timestamp > %s', (chat_id, thirty_days_ago))
                    recent_stats = cursor.fetchone()
                    recent_count = recent_stats.get('count') if isinstance(recent_stats, dict) else (recent_stats[0] if recent_stats else 0)
                    logger.info(f"[CLEAN] Записей в stats за последние 30 дней для chat_id={chat_id}: {recent_count}")
                    
                    cursor.execute('''
                        SELECT DISTINCT user_id
                        FROM stats
                        WHERE chat_id = %s AND timestamp > %s
                    ''', (chat_id, thirty_days_ago))
                    rows = cursor.fetchall()
                    active_members_from_stats = set()
                    for row in rows:
                        user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                        active_members_from_stats.add(user_id)
                    logger.info(f"[CLEAN] Найдено активных участников в stats: {len(active_members_from_stats)}, user_ids: {list(active_members_from_stats)}")
                
                # Исключаем бота из списка активных участников
                if BOT_ID and BOT_ID in active_members_from_stats:
                    active_members_from_stats.discard(BOT_ID)
                    logger.info(f"[CLEAN] Бот (ID: {BOT_ID}) исключен из списка активных участников")
                
                # Определяем количество участников для голосования
                # Если получили через API и оно больше, используем его
                # Иначе используем количество из stats, но минимум 2 (чтобы учесть хотя бы двух участников)
                if chat_member_count:
                    # Вычитаем бота из общего количества участников
                    if chat_member_count > 0:
                        chat_member_count = max(1, chat_member_count - 1)  # Вычитаем бота, минимум 1
                        logger.info(f"[CLEAN] Количество участников после исключения бота: {chat_member_count}")
                    
                    if chat_member_count > len(active_members_from_stats):
                        active_members_count = chat_member_count
                        logger.info(f"[CLEAN] Используем количество участников из API (без бота): {active_members_count}")
                        # Для голосования используем всех участников чата (не только активных в stats)
                        active_members = active_members_from_stats  # Это будут те, кто может проголосовать
                    else:
                        # Используем количество из stats, но минимум 2
                        active_members_count = max(len(active_members_from_stats), 2)
                        active_members = active_members_from_stats
                        logger.info(f"[CLEAN] Используем количество участников из stats (минимум 2): {active_members_count}")
                else:
                    # Используем количество из stats, но минимум 2
                    active_members_count = max(len(active_members_from_stats), 2)
                    active_members = active_members_from_stats
                    logger.info(f"[CLEAN] Используем количество участников из stats (минимум 2): {active_members_count}")
                
                logger.info(f"[CLEAN] Итоговое количество участников для голосования: {active_members_count}, активных в stats: {len(active_members)}")
                
                # Проверяем, есть ли хотя бы минимальное количество участников
                if active_members_count < 2:
                    # Показываем более подробное сообщение с диагностикой
                    with db_lock:
                        cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s', (chat_id,))
                        total_stats = cursor.fetchone()
                        total_count = total_stats.get('count') if isinstance(total_stats, dict) else (total_stats[0] if total_stats else 0)
                    
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата за последние 30 дней.\n\n"
                        f"📊 Диагностика:\n"
                        f"• Всего записей в stats для этого чата: {total_count}\n"
                        f"• Участников в чате (через API): {chat_member_count if chat_member_count else 'неизвестно'}\n"
                        f"• Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                    f"Участников в чате: {active_members_count}\n"
                    f"Для подтверждения все участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                    f"Если не все проголосуют, база не будет удалена.",
                    parse_mode='HTML')
                
                clean_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': active_members_count,
                    'voted': set(),
                    'active_members': active_members  # Те, кто активен в stats (для логирования)
                }
                
                bot.edit_message_text("✅ Запрос на обнуление базы отправлен. Ожидаю голосования всех участников.", call.message.chat.id, call.message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при инициировании голосования: {e}", exc_info=True)
                bot.edit_message_text("Ошибка при инициировании голосования.", call.message.chat.id, call.message.message_id)
        else:
            # В личном чате можно сразу удалить
            bot.edit_message_text(
                "⚠️ <b>Обнуление базы данных чата</b>\n\n"
                "Это удалит <b>ВСЕ данные чата</b>:\n"
                "• Все фильмы\n"
                "• Все оценки\n"
                "• Все планы и расписание\n"
                "• Все билеты\n"
                "• Все настройки\n\n"
                "Это действие необратимо!\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'chat'
    
    elif action == 'user_db':
        # Обнуление базы пользователя - удаляет только данные конкретного пользователя в этом чате
        bot.edit_message_text(
            "⚠️ <b>Обнуление базы данных пользователя</b>\n\n"
            "Это удалит <b>только ваши данные в этом чате</b>:\n"
            "• Все ваши оценки\n"
            "• Все ваши планы и расписание\n"
            "• Все ваши билеты\n"
            "• Вашу статистику\n"
            "• Ваши настройки (включая часовой пояс)\n\n"
            "<i>Фильмы и данные других пользователей останутся без изменений.</i>\n\n"
            "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
            call.message.chat.id, call.message.message_id, parse_mode='HTML'
        )
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'user'
    
    elif action == 'cancel':
        bot.edit_message_text("❌ Операция отменена.", call.message.chat.id, call.message.message_id)
        if user_id in user_clean_state:
            del user_clean_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("clean_rating:"))
def clean_rating_execute(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    film_id = int(call.data.split(":")[1])
    
    with db_lock:
        cursor.execute('SELECT title FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        row = cursor.fetchone()
        if row:
            title = row[0]
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            conn.commit()
            bot.edit_message_text(f"✅ Оценки для фильма <b>{title}</b> удалены.", call.message.chat.id, call.message.message_id, parse_mode='HTML')
        else:
            bot.edit_message_text("Фильм не найден.", call.message.chat.id, call.message.message_id)
    
    if user_id in user_clean_state:
        del user_clean_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("clean_watched:"))
def clean_watched_execute(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    film_id = int(call.data.split(":")[1])
    
    with db_lock:
        cursor.execute('SELECT title FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        row = cursor.fetchone()
        if row:
            title = row.get('title') if isinstance(row, dict) else row[0]
            cursor.execute('UPDATE movies SET watched = 0 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
            bot.edit_message_text(f"✅ Отметка просмотра для фильма <b>{title}</b> удалена.", call.message.chat.id, call.message.message_id, parse_mode='HTML')
        else:
            bot.edit_message_text("Фильм не найден.", call.message.chat.id, call.message.message_id)
    
    if user_id in user_clean_state:
        del user_clean_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("clean_plan:"))
def clean_plan_execute(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    with db_lock:
        cursor.execute('''
            SELECT m.title
            FROM plans p
            JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
            WHERE p.id = %s AND p.chat_id = %s
        ''', (plan_id, chat_id))
        row = cursor.fetchone()
        if row:
            title = row.get('title') if isinstance(row, dict) else row[0]
            cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            conn.commit()
            bot.edit_message_text(f"✅ План для фильма <b>{title}</b> удалён.", call.message.chat.id, call.message.message_id, parse_mode='HTML')
        else:
            bot.edit_message_text("План не найден.", call.message.chat.id, call.message.message_id)
    
    if user_id in user_clean_state:
        del user_clean_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("clean_movie:"))
def clean_movie_execute(call):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    film_id = int(call.data.split(":")[1])
    
    logger.info(f"[CLEAN] Удаление фильма film_id={film_id} от пользователя {user_id}")
    
    with db_lock:
        # Получаем информацию о фильме перед удалением
        cursor.execute('SELECT title, kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        movie_row = cursor.fetchone()
        
        if not movie_row:
            bot.answer_callback_query(call.id, "Фильм не найден", show_alert=True)
            return
        
        title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[0]
        
        # Удаляем связанные записи
        # 1. Удаляем оценки
        cursor.execute('DELETE FROM ratings WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
        ratings_deleted = cursor.rowcount
        
        # 2. Удаляем планы
        cursor.execute('DELETE FROM plans WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
        plans_deleted = cursor.rowcount
        
        # 3. Удаляем отметки просмотра (watched_movies)
        cursor.execute('DELETE FROM watched_movies WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
        watched_deleted = cursor.rowcount
        
        # 4. Удаляем сам фильм
        cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        movie_deleted = cursor.rowcount
        
        conn.commit()
    
    if movie_deleted > 0:
        bot.edit_message_text(
            f"✅ <b>Фильм удален из базы</b>\n\n"
            f"<b>{title}</b>\n\n"
            f"Также удалено:\n"
            f"• Оценок: {ratings_deleted}\n"
            f"• Планов: {plans_deleted}\n"
            f"• Отметок просмотра: {watched_deleted}",
            chat_id, call.message.message_id, parse_mode='HTML'
        )
        bot.answer_callback_query(call.id, "Фильм удален")
        logger.info(f"[CLEAN] Фильм {title} (id={film_id}) успешно удален вместе с {ratings_deleted} оценками, {plans_deleted} планами и {watched_deleted} отметками просмотра")
    else:
        bot.answer_callback_query(call.id, "Ошибка удаления фильма", show_alert=True)
        logger.error(f"[CLEAN] Не удалось удалить фильм id={film_id}")

# Обработка подтверждения удаления базы
# Работает независимо от того, реплай это или нет
@bot.message_handler(func=lambda m: m.text and m.text.upper().strip() == 'ДА, УДАЛИТЬ' and m.from_user.id in user_clean_state)
def clean_confirm_execute(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    state = user_clean_state.get(user_id, {})
    action = state.get('action')
    confirm_needed = state.get('confirm_needed', False)
    
    # Проверяем, что подтверждение действительно требуется
    if not confirm_needed:
        logger.warning(f"Попытка подтверждения без установленного confirm_needed для пользователя {user_id}")
        return
    
    try:
        if action == 'chat_db':
            # Удаляем все данные чата
            with db_lock:
                try:
                    # Удаляем билеты (связаны с plans через plan_id)
                    cursor.execute('DELETE FROM tickets WHERE chat_id = %s', (chat_id,))
                    # Удаляем планы (расписание)
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
                    # Удаляем фильмы
                    cursor.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
                    # Удаляем оценки
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
                    # Удаляем настройки
                    cursor.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
                    # Удаляем статистику
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
                    # Удаляем голосования
                    cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s', (chat_id,))
                    conn.commit()
                    bot.reply_to(message, "✅ База данных чата полностью обнулена.\n\nВсе фильмы, оценки, планы, расписание, билеты и настройки удалены.")
                    logger.info(f"База данных чата {chat_id} обнулена пользователем {user_id}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Ошибка при удалении данных чата: {e}", exc_info=True)
                    bot.reply_to(message, "❌ Произошла ошибка при удалении данных. Попробуйте позже.")
                    raise
        
        elif action == 'user_db':
            # Удаляем все данные пользователя ТОЛЬКО в этом конкретном чате
            with db_lock:
                try:
                    # Удаляем билеты пользователя в этом чате (через plans)
                    cursor.execute('''
                        DELETE FROM tickets 
                        WHERE chat_id = %s AND plan_id IN (
                            SELECT id FROM plans WHERE chat_id = %s AND user_id = %s
                        )
                    ''', (chat_id, chat_id, user_id))
                    # Удаляем планы пользователя в этом чате
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    # Удаляем оценки пользователя в этом чате
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    # Удаляем статистику пользователя в этом чате
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    # Удаляем настройки пользователя (часовой пояс) - используется user_id как chat_id в settings
                    cursor.execute('DELETE FROM settings WHERE chat_id = %s AND key = %s', (user_id, 'user_timezone'))
                    conn.commit()
                    bot.reply_to(message, "✅ Все ваши данные удалены из этого чата.\n\nВаши оценки, планы, расписание, билеты, статистика и настройки (включая часовой пояс) удалены только в этом чате. Фильмы и данные других пользователей остались без изменений.")
                    logger.info(f"Данные пользователя {user_id} удалены из чата {chat_id}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Ошибка при удалении данных пользователя: {e}", exc_info=True)
                    bot.reply_to(message, "❌ Произошла ошибка при удалении данных. Попробуйте позже.")
                    raise
    except Exception as e:
        logger.error(f"Критическая ошибка в clean_confirm_execute: {e}", exc_info=True)
    
    if user_id in user_clean_state:
        del user_clean_state[user_id]


# ==================== КОМАНДА /EDIT ====================
@bot.message_handler(commands=['edit'])
def edit_command(message):
    """Команда /edit - редактирование расписания и оценок"""
    logger.info(f"[EDIT COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[EDIT COMMAND] /edit вызван от {message.from_user.id}")
    logger.info(f"[EDIT COMMAND] message.text={message.text}")
    logger.info(f"[EDIT COMMAND] message.chat.id={message.chat.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/edit', message.chat.id)
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    logger.info(f"[EDIT COMMAND] Создаем меню редактирования для user_id={user_id}, chat_id={chat_id}")
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📅 Изменить фильм в расписании", callback_data="edit:plan"))
    markup.add(InlineKeyboardButton("⭐ Изменить оценку", callback_data="edit:rating"))
    markup.add(InlineKeyboardButton("🗑️ Удалить оценку", callback_data="edit:delete_rating"))
    markup.add(InlineKeyboardButton("👁️ Удалить просмотр", callback_data="edit:delete_watched"))
    markup.add(InlineKeyboardButton("📅 Удалить задачу из планов", callback_data="edit:delete_plan"))
    markup.add(InlineKeyboardButton("🎬 Удалить фильм из базы", callback_data="edit:delete_movie"))
    
    help_text = (
        "✏️ <b>Что вы хотите изменить?</b>\n\n"
        "<b>📅 Изменить фильм в расписании</b> — изменить дату/время или переключить между 'дома' и 'в кино'\n"
        "<b>⭐ Изменить оценку</b> — изменить вашу оценку фильма\n\n"
        "<b>Остальные опции:</b> удаление оценок, просмотров, планов и фильмов"
    )
    
    logger.info(f"[EDIT COMMAND] Отправляем меню редактирования")
    try:
        bot.reply_to(message, help_text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[EDIT COMMAND] ✅ Меню редактирования отправлено успешно")
    except Exception as e:
        logger.error(f"[EDIT COMMAND] ❌ Ошибка отправки меню: {e}", exc_info=True)


# ==================== КОМАНДА /TICKET ====================
logger.info("[TICKET REGISTRATION] Регистрируем обработчик команды /ticket")
@bot.message_handler(commands=['ticket'])
def ticket_command(message):
    """Команда /ticket - работа с билетами в кино"""
    logger.info(f"[TICKET COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        username = message.from_user.username or f"user_{user_id}"
        
        logger.info(f"[TICKET COMMAND] ===== НАЧАЛО ОБРАБОТКИ /ticket =====")
        logger.info(f"[TICKET COMMAND] Пользователь {user_id} ({username}) вызвал /ticket в чате {chat_id}")
        logger.info(f"[TICKET COMMAND] message.text={message.text}")
        logger.info(f"[TICKET COMMAND] message.photo={message.photo}")
        logger.info(f"[TICKET COMMAND] message.document={message.document}")
        log_request(user_id, username, '/ticket', chat_id)
        
        # Проверяем, есть ли файл в сообщении
        has_photo = message.photo is not None and len(message.photo) > 0
        has_document = message.document is not None
        
        logger.info(f"[TICKET COMMAND] Проверка файла: has_photo={has_photo}, has_document={has_document}")
        
        if has_photo or has_document:
            # Сохраняем file_id для последующей обработки
            if has_photo:
                file_id = message.photo[-1].file_id  # Берем самое большое фото
                logger.info(f"[TICKET COMMAND] Получено фото, file_id={file_id}")
            else:
                file_id = message.document.file_id
                logger.info(f"[TICKET COMMAND] Получен документ, file_id={file_id}")
            
            user_ticket_state[user_id] = {
                'step': 'select_session',
                'file_id': file_id,
                'chat_id': chat_id
            }
            logger.info(f"[TICKET COMMAND] Сохранено состояние для пользователя {user_id}: step=select_session, file_id={file_id}")
            
            # Показываем список сеансов в кино
            show_cinema_sessions(chat_id, user_id, file_id)
        else:
            # Нет файла - показываем список сеансов для выбора
            logger.info(f"[TICKET COMMAND] Файл не найден, показываем список сеансов без file_id")
            show_cinema_sessions(chat_id, user_id, None)
    
    except Exception as e:
        logger.error(f"[TICKET COMMAND] Ошибка при обработке /ticket: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке команды /ticket")
        except:
            pass


def show_cinema_sessions(chat_id, user_id, file_id=None):
    """Показывает список запланированных сеансов в кино"""
    logger.info(f"[SHOW SESSIONS] Показываем сеансы для пользователя {user_id}, chat_id={chat_id}, file_id={file_id}")
    with db_lock:
        cursor.execute('''
            SELECT p.id, m.title, p.plan_datetime, 
                   CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as ticket_count
            FROM plans p
            JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
            WHERE p.chat_id = %s AND p.plan_type = 'cinema'
            ORDER BY p.plan_datetime
            LIMIT 20
        ''', (chat_id,))
        sessions = cursor.fetchall()
    
    logger.info(f"[SHOW SESSIONS] Найдено сеансов: {len(sessions) if sessions else 0}")
    
    if not sessions:
        logger.info(f"[SHOW SESSIONS] Нет сеансов, отправляем сообщение пользователю {user_id}")
        bot.send_message(chat_id, "❌ Нет запланированных сеансов в кино.")
        return
    
    user_tz = get_user_timezone_or_default(user_id)
    markup = InlineKeyboardMarkup(row_width=1)
    
    for row in sessions:
        if isinstance(row, dict):
            plan_id = row.get('id')
            title = row.get('title')
            plan_dt_value = row.get('plan_datetime')
            ticket_count = row.get('ticket_count', 0)
        else:
            plan_id = row[0]
            title = row[1]
            plan_dt_value = row[2]
            ticket_count = row[3] if len(row) > 3 else 0
        
        if plan_dt_value:
            if isinstance(plan_dt_value, datetime):
                if plan_dt_value.tzinfo is None:
                    dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                else:
                    dt = plan_dt_value.astimezone(user_tz)
            else:
                dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
            
            date_str = dt.strftime('%d.%m %H:%M')
            ticket_emoji = "🎟️ " if ticket_count > 0 else ""
            button_text = f"{ticket_emoji}{title} | {date_str}"
            
            if len(button_text) > 60:
                short_title = title[:50] + "..."
                button_text = f"{ticket_emoji}{short_title} | {date_str}"
            
            callback_data = f"ticket_session:{plan_id}"
            if file_id:
                callback_data += f":{file_id}"
            logger.info(f"[SHOW SESSIONS] Добавляем кнопку для plan_id={plan_id}, callback_data={callback_data}, ticket_count={ticket_count}")
            markup.add(InlineKeyboardButton(button_text, callback_data=callback_data))
    
    if file_id:
        markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data=f"ticket_new:{file_id}"))
        logger.info(f"[SHOW SESSIONS] Добавлена кнопка 'Добавить новый сеанс' с file_id={file_id}")
    else:
        markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data="ticket_new"))
        logger.info(f"[SHOW SESSIONS] Добавлена кнопка 'Добавить новый сеанс' без file_id")
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    text = "🎟️ <b>Выберите сеанс:</b>\n\n"
    if file_id:
        text += "📎 Файл готов к добавлению. Выберите сеанс или создайте новый."
    else:
        text += "Выберите сеанс для просмотра билетов или добавления новых."
    
    logger.info(f"[SHOW SESSIONS] Отправляем сообщение с выбором сеансов пользователю {user_id}, file_id={file_id}")
    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')


# ==================== ОБРАБОТКА ИЗОБРАЖЕНИЙ И ФАЙЛОВ ДЛЯ БИЛЕТОВ ====================
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_file_handler
# @bot.message_handler(content_types=['photo', 'document'], func=lambda message: message.from_user.id in user_ticket_state and user_ticket_state.get(message.from_user.id, {}).get('step') != 'upload_ticket', priority=10)
def handle_ticket_file_OLD(message):
    """Обработчик загрузки билетов (фото или файл)"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    state = user_ticket_state.get(user_id, {})
    step = state.get('step')
    
    logger.info(f"[TICKET FILE] Пользователь {user_id} отправил файл, step={step}, state={state}")
    
    # Получаем file_id
    if message.photo:
        file_id = message.photo[-1].file_id  # Берем самое большое фото
        logger.info(f"[TICKET FILE] Получено фото, file_id={file_id}")
    elif message.document:
        file_id = message.document.file_id
        logger.info(f"[TICKET FILE] Получен документ, file_id={file_id}")
    else:
        logger.warning(f"[TICKET FILE] Не удалось получить file_id из сообщения")
        bot.reply_to(message, "❌ Не удалось получить файл. Попробуйте еще раз.")
        return
    
    if step == 'waiting_ticket_file':
        # Добавляем билеты к существующему плану
        plan_id = state.get('plan_id')
        logger.info(f"[TICKET FILE] Добавляем билеты к плану plan_id={plan_id}")
        if not plan_id:
            logger.error(f"[TICKET FILE] Ошибка: plan_id не найден в состоянии")
            bot.reply_to(message, "❌ Ошибка: план не найден.")
            if user_id in user_ticket_state:
                del user_ticket_state[user_id]
            return
        
        # Проверяем, есть ли уже билеты для этого плана
        with db_lock:
            cursor.execute('SELECT COUNT(*) FROM tickets WHERE plan_id = %s', (plan_id,))
            existing_count = cursor.fetchone()[0] if cursor.rowcount > 0 else 0
            logger.info(f"[TICKET FILE] Текущее количество билетов для plan_id={plan_id}: {existing_count}")
            
            # Добавляем новые билеты (не удаляем старые, если добавляем еще)
            cursor.execute('INSERT INTO tickets (plan_id, chat_id, file_id) VALUES (%s, %s, %s)',
                         (plan_id, chat_id, file_id))
            conn.commit()
        logger.info(f"[TICKET FILE] Билеты сохранены в БД для plan_id={plan_id}")
        
        # Очищаем состояние, так как билеты добавлены
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        
        if existing_count > 0:
            # Если билеты уже были, просто подтверждаем добавление
            bot.reply_to(message, "✅ <b>Дополнительные билеты успешно добавлены!</b>", parse_mode='HTML')
        else:
            # Если это первые билеты, предлагаем указать время
            user_ticket_state[user_id] = {
                'step': 'waiting_session_time',
                'plan_id': plan_id,
                'chat_id': chat_id
            }
        
            # Проверяем, есть ли время у сеанса
            with db_lock:
                cursor.execute('SELECT plan_datetime FROM plans WHERE id = %s', (plan_id,))
                plan_row = cursor.fetchone()
            
            has_time = False
            if plan_row:
                plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[0]
                if plan_dt:
                    has_time = True
            
            markup = InlineKeyboardMarkup()
            if not has_time:
                # Если нет времени, добавляем обе кнопки
                markup.add(InlineKeyboardButton("⏰ Указать точное время сеанса", callback_data=f"ticket_time:{plan_id}"))
                markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"ticket_add_more:{plan_id}"))
            else:
                # Если время есть, только кнопка указания времени
                markup.add(InlineKeyboardButton("⏰ Указать точное время сеанса", callback_data=f"ticket_time:{plan_id}"))
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
            if not has_time:
                bot.reply_to(message, 
                            "✅ <b>Билеты успешно добавлены!</b>\n\n"
                            "Что хотите сделать дальше?",
                            reply_markup=markup, parse_mode='HTML')
            else:
                bot.reply_to(message, 
                            "✅ <b>Билеты успешно добавлены!</b>\n\n"
                            "Если нужно, укажите точное время сеанса:",
                            reply_markup=markup, parse_mode='HTML')
        logger.info(f"[TICKET FILE] Сообщение об успешном добавлении отправлено пользователю {user_id}")
    else:
        # Сохраняем file_id для последующей обработки
        logger.info(f"[TICKET FILE] Сохраняем file_id в состояние, step={step}")
        user_ticket_state[user_id]['file_id'] = file_id
        bot.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿")
        # Очищаем состояние пользователя, завершаем цикл работы с билетами
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        logger.info(f"[TICKET FILE] file_id сохранен, состояние пользователя {user_id} очищено")


# ==================== ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ ДЛЯ РЕДАКТИРОВАНИЯ И БИЛЕТОВ ====================
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_text_handler
# @bot.message_handler(content_types=['text'], func=lambda message: message.from_user.id in user_edit_state or message.from_user.id in user_ticket_state, priority=15)
def handle_edit_ticket_text_OLD(message):
    """Обработчик текстовых сообщений для редактирования и билетов"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    logger.info(f"[EDIT/TICKET TEXT] ===== ОБРАБОТЧИК ВЫЗВАН =====")
    logger.info(f"[EDIT/TICKET TEXT] Получено сообщение от {user_id}: '{text}'")
    logger.info(f"[EDIT/TICKET TEXT] user_id в user_edit_state: {user_id in user_edit_state}")
    logger.info(f"[EDIT/TICKET TEXT] user_id в user_ticket_state: {user_id in user_ticket_state}")
    if user_id in user_ticket_state:
        logger.info(f"[EDIT/TICKET TEXT] Состояние пользователя: {user_ticket_state.get(user_id)}")
    
    # Обработка редактирования оценки
    if user_id in user_edit_state:
        state = user_edit_state[user_id]
        action = state.get('action')
        
        if action == 'edit_rating':
            try:
                rating = int(text)
                if 1 <= rating <= 10:
                    film_id = state.get('film_id')
                    with db_lock:
                        cursor.execute('''
                            INSERT INTO ratings (chat_id, film_id, user_id, rating)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating
                        ''', (chat_id, film_id, user_id, rating))
                        conn.commit()
                    
                    bot.reply_to(message, f"✅ Оценка изменена на {rating}/10")
                    del user_edit_state[user_id]
                else:
                    bot.reply_to(message, "❌ Оценка должна быть от 1 до 10")
            except ValueError:
                bot.reply_to(message, "❌ Введите число от 1 до 10")
            return
    
    # Обработка времени сеанса для билетов
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        
        logger.info(f"[TICKET TIME] Пользователь {user_id} отправил текст '{text}', step={step}, state={state}")
        
        if step == 'waiting_session_time':
            plan_id = state.get('plan_id')
            user_tz = get_user_timezone_or_default(user_id)
            
            logger.info(f"[TICKET TIME] Обрабатываем время сеанса для plan_id={plan_id}, текст='{text}'")
            
            # Парсим время сеанса
            session_dt = parse_session_time(text, user_tz)
            if not session_dt:
                logger.warning(f"[TICKET TIME] Не удалось распарсить время из текста '{text}'")
                bot.reply_to(message, "❌ Не удалось распознать время. Попробуйте в формате:\n• 15 января 10:30\n• 17.01 15:20")
                return
            
            logger.info(f"[TICKET TIME] Время успешно распарсено: {session_dt}")
            
            # Получаем информацию о плане и обновляем время сеанса
            with db_lock:
                # Получаем информацию о фильме для планирования напоминаний
                cursor.execute('''
                    SELECT m.title, m.link, p.plan_type
                    FROM plans p
                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.id = %s
                ''', (plan_id,))
                plan_info = cursor.fetchone()
                
                # Обновляем время сеанса в плане и билетах
                session_utc = session_dt.astimezone(pytz.utc)
                # Обновляем план
                cursor.execute('UPDATE plans SET plan_datetime = %s WHERE id = %s', (session_utc, plan_id))
                # Обновляем время сеанса в билетах (если есть)
                cursor.execute('UPDATE tickets SET session_datetime = %s WHERE plan_id = %s', (session_utc, plan_id))
                conn.commit()
            
            if plan_info:
                if isinstance(plan_info, dict):
                    title = plan_info.get('title')
                    link = plan_info.get('link')
                    plan_type = plan_info.get('plan_type')
                else:
                    title = plan_info[0]
                    link = plan_info[1]
                    plan_type = plan_info[2]
                
                # Планируем напоминания
                # 1. Утреннее напоминание (без билетов) - в 9:00 в день сеанса
                morning_dt = session_dt.replace(hour=9, minute=0)
                if morning_dt < datetime.now(user_tz):
                    morning_dt = morning_dt + timedelta(days=1)
                morning_utc = morning_dt.astimezone(pytz.utc)
                
                scheduler.add_job(
                    send_plan_notification,
                    'date',
                    run_date=morning_utc,
                    args=[chat_id, None, title, link, plan_type],
                    id=f'plan_morning_{chat_id}_{plan_id}_{int(morning_utc.timestamp())}'
                )
                
                # 2. Напоминание за 10 минут до сеанса (с билетами)
                ticket_dt = session_dt - timedelta(minutes=10)
                if ticket_dt > datetime.now(user_tz):
                    ticket_utc = ticket_dt.astimezone(pytz.utc)
                    scheduler.add_job(
                        send_ticket_notification,
                        'date',
                        run_date=ticket_utc,
                        args=[chat_id, plan_id],
                        id=f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'
                    )
            
            tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
            formatted_time = session_dt.strftime('%d.%m %H:%M')
            logger.info(f"[TICKET TIME] Время обновлено для plan_id={plan_id}: {formatted_time} {tz_name}")
            logger.info(f"[TICKET TIME] Обновляем план в БД: plan_id={plan_id}, session_utc={session_utc}")
            bot.reply_to(message, f"✅ <b>Время принято!</b>\n\n🕐 Сеанс: {formatted_time} {tz_name}", parse_mode='HTML')
            del user_ticket_state[user_id]
            logger.info(f"[TICKET TIME] Состояние пользователя {user_id} очищено")
        elif step == 'waiting_new_session':
            logger.info(f"[TICKET NEW SESSION] ===== НАЧАЛО ОБРАБОТКИ waiting_new_session =====")
            logger.info(f"[TICKET NEW SESSION] Пользователь {user_id} отправил: '{text}'")
            # Обрабатываем создание нового сеанса с билетами
            file_id = state.get('file_id')
            logger.info(f"[TICKET NEW SESSION] file_id из состояния: {file_id}")
            
            # Парсим ссылку и дату из текста
            link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/(\d+))', text)
            logger.info(f"[TICKET NEW SESSION] Результат поиска ссылки: {link_match is not None}")
            if link_match:
                link = link_match.group(1)
                kp_id = link_match.group(3)
                logger.info(f"[TICKET NEW SESSION] Найдена ссылка: {link}, kp_id={kp_id}")
            else:
                # Пробуем найти просто ID
                id_match = re.search(r'^(\d+)', text.strip())
                if id_match:
                    kp_id = id_match.group(1)
                    link = f"https://kinopoisk.ru/film/{kp_id}/"
                    logger.info(f"[TICKET NEW SESSION] Найден ID: {kp_id}, создана ссылка: {link}")
                else:
                    logger.warning(f"[TICKET NEW SESSION] Не найдена ссылка или ID в тексте: '{text}'")
                    bot.reply_to(message, "❌ Не найдена ссылка на фильм. Укажите ссылку или ID фильма.")
                    return
            
            # Парсим дату и время
            user_tz = get_user_timezone_or_default(user_id)
            logger.info(f"[TICKET NEW SESSION] Парсим время из текста: '{text}', tz={user_tz}")
            session_dt = parse_session_time(text, user_tz)
            if not session_dt:
                logger.warning(f"[TICKET NEW SESSION] Не удалось распарсить время из текста: '{text}'")
                bot.reply_to(message, "❌ Не удалось распознать время. Попробуйте в формате:\n• 15 января 10:30\n• 17.01 15:20")
                return
            logger.info(f"[TICKET NEW SESSION] Время успешно распарсено: {session_dt}")
            
            # Создаем план и добавляем билеты
            # Получаем информацию о фильме
            movie_info = extract_movie_info(link)
            if not movie_info:
                bot.reply_to(message, "❌ Не удалось получить информацию о фильме.")
                return
            
            # Добавляем фильм в базу, если его нет
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                movie_row = cursor.fetchone()
                if movie_row:
                    film_id = movie_row.get('id') if isinstance(movie_row, dict) else movie_row[0]
                else:
                    # Добавляем фильм
                    is_series_val = 1 if movie_info.get('is_series') else 0
                    cursor.execute('''
                        INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    ''', (chat_id, link, kp_id, movie_info.get('title'), movie_info.get('year'),
                          movie_info.get('genres'), movie_info.get('description'),
                          movie_info.get('director'), movie_info.get('actors'), is_series_val))
                    film_id = cursor.fetchone()[0]
                    conn.commit()
            
            # Создаем план
            session_utc = session_dt.astimezone(pytz.utc)
            with db_lock:
                cursor.execute('''
                    INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
                    VALUES (%s, %s, %s, %s, %s)
                    RETURNING id
                ''', (chat_id, film_id, 'cinema', session_utc, user_id))
                plan_id = cursor.fetchone()[0]
                
                # Добавляем билеты
                if file_id:
                    cursor.execute('''
                        INSERT INTO tickets (plan_id, chat_id, file_id, session_datetime)
                        VALUES (%s, %s, %s, %s)
                    ''', (plan_id, chat_id, file_id, session_utc))
                
                conn.commit()
            
            # Планируем напоминания (аналогично выше)
            title = movie_info.get('title')
            morning_dt = session_dt.replace(hour=9, minute=0)
            if morning_dt < datetime.now(user_tz):
                morning_dt = morning_dt + timedelta(days=1)
            morning_utc = morning_dt.astimezone(pytz.utc)
            
            scheduler.add_job(
                send_plan_notification,
                'date',
                run_date=morning_utc,
                args=[chat_id, film_id, title, link, 'cinema'],
                id=f'plan_morning_{chat_id}_{plan_id}_{int(morning_utc.timestamp())}'
            )
            
            ticket_dt = session_dt - timedelta(minutes=10)
            if ticket_dt > datetime.now(user_tz):
                ticket_utc = ticket_dt.astimezone(pytz.utc)
                scheduler.add_job(
                    send_ticket_notification,
                    'date',
                    run_date=ticket_utc,
                    args=[chat_id, plan_id],
                    id=f'ticket_notify_{chat_id}_{plan_id}_{int(ticket_utc.timestamp())}'
                )
            
            tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
            formatted_time = session_dt.strftime('%d.%m %H:%M')
            bot.reply_to(message, f"✅ <b>Время принято!</b>\n\n🎬 Сеанс создан: {title}\n🕐 Время: {formatted_time} {tz_name}", parse_mode='HTML')
            del user_ticket_state[user_id]


def send_ticket_notification(chat_id, plan_id):
    """Отправляет напоминание с билетами за 10 минут до сеанса"""
    try:
        with db_lock:
            cursor.execute('''
                SELECT t.file_id, m.title, p.plan_datetime
                FROM tickets t
                JOIN plans p ON t.plan_id = p.id
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE t.plan_id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            ticket_row = cursor.fetchone()
        
        if not ticket_row:
            logger.warning(f"[TICKET NOTIFICATION] Билеты не найдены для plan_id={plan_id}")
            return
        
        if isinstance(ticket_row, dict):
            file_id = ticket_row.get('file_id')
            title = ticket_row.get('title')
            plan_dt_value = ticket_row.get('plan_datetime')
        else:
            file_id = ticket_row[0]
            title = ticket_row[1]
            plan_dt_value = ticket_row[2]
        
        text = f"🎟️ <b>Напоминание: через 10 минут сеанс!</b>\n\n<b>{title}</b>\n\nВаши билеты:"
        
        try:
            bot.send_photo(chat_id, file_id, caption=text, parse_mode='HTML')
        except:
            try:
                bot.send_document(chat_id, file_id, caption=text, parse_mode='HTML')
            except Exception as e:
                logger.error(f"[TICKET NOTIFICATION] Ошибка отправки билетов: {e}")
                bot.send_message(chat_id, f"🎟️ <b>Напоминание: через 10 минут сеанс!</b>\n\n<b>{title}</b>", parse_mode='HTML')
        
        logger.info(f"[TICKET NOTIFICATION] Напоминание с билетами отправлено для {title} в чат {chat_id}")
    except Exception as e:
        logger.error(f"[TICKET NOTIFICATION] Ошибка отправки напоминания: {e}")


# Обработка реплаев на сообщения бота (для settings и других случаев)
@bot.message_handler(content_types=['text'], func=lambda message: message.reply_to_message and message.reply_to_message.from_user.is_bot and not (message.text and message.text.strip().startswith('/')), priority=10)
def handle_reply_to_bot(message):
    # Пропускаем команды - они обрабатываются отдельными обработчиками
    if message.text and message.text.strip().startswith('/'):
        logger.info(f"[REPLY TO BOT] Пропущена команда: {message.text[:50]}")
        return
    
    # Пропускаем сообщения, которые должны обрабатываться обработчиками edit/ticket
    if message.from_user.id in user_edit_state or message.from_user.id in user_ticket_state:
        logger.info(f"[REPLY TO BOT] Пропущено сообщение для обработки edit/ticket: {message.text[:50] if message.text else 'None'}")
        return
    
    logger.info(f"[REPLY TO BOT] Получен реплай на сообщение бота от {message.from_user.id}, text: '{message.text}'")
    
    # Обработка для /settings
    if message.from_user.id in user_settings_state:
        state = user_settings_state.get(message.from_user.id)
        if state and message.reply_to_message.message_id == state.get('settings_msg_id'):
            logger.info(f"[REPLY TO BOT] Это ответ на settings, state={state}")
            
            # Извлекаем эмодзи
            import re
            emojis = re.findall(r'[\U0001F300-\U0001F9FF\U0001F600-\U0001F64F\U0001F680-\U0001F6FF\U00002600-\U000027BF\U0001F900-\U0001F9FF]+', message.text or "")
            
            if not emojis:
                bot.reply_to(message, "⚠️ Не найдено эмодзи в сообщении. Отправьте только эмодзи (можно несколько).")
                return
            
            emojis_str = ''.join(set(''.join(emojis)))  # убираем дубли
            
            # Проверяем режим (add или replace)
            action = state.get('action', 'replace')
            
            # Получаем chat_id из сообщения
            chat_id = message.chat.id
            
            if action == "add":
                # Добавляем к текущим
                current_emojis = get_watched_emojis(chat_id)
                emojis_str = ''.join(current_emojis) + emojis_str
                # Убираем дубликаты, сохраняя порядок
                seen = set()
                emojis_str = ''.join(c for c in emojis_str if c not in seen and not seen.add(c))
                action_text = "добавлены к текущим"
            else:
                # Заменяем полностью
                action_text = "заменены"
            
            # Сохраняем в БД для этого чата
            with db_lock:
                try:
                    cursor.execute("""
                        INSERT INTO settings (chat_id, key, value) 
                        VALUES (%s, 'watched_emoji', %s) 
                        ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    """, (chat_id, emojis_str))
                    conn.commit()
                    logger.info(f"[REPLY TO BOT] Эмодзи сохранены (режим: {action}): {emojis_str}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"[REPLY TO BOT] Ошибка сохранения эмодзи: {e}", exc_info=True)
                    bot.reply_to(message, "❌ Ошибка сохранения. Попробуй позже.")
                    return
            
            bot.reply_to(message, f"✅ Реакции {action_text}:\n{emojis_str}")
            
            # Очищаем состояние
            if message.from_user.id in user_settings_state:
                del user_settings_state[message.from_user.id]
            return  # Важно: возвращаемся, чтобы не обрабатывать дальше

# ==================== ОБРАБОТКА СООБЩЕНИЙ С ФИЛЬМОМ + ДАТОЙ В РЕЖИМЕ ДОБАВЛЕНИЯ НОВОГО СЕАНСА ====================
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_text_handler
# @bot.message_handler(func=lambda m: m.text and m.from_user.id in user_ticket_state, priority=20)
def handle_new_session_input_OLD(message):
    # Проверяем состояние внутри обработчика
    user_id = message.from_user.id
    state = user_ticket_state.get(user_id, {})
    step = state.get('step')
    
    logger.info(f"[TICKET NEW SESSION HANDLER] ===== ОБРАБОТЧИК ВЫЗВАН =====")
    logger.info(f"[TICKET NEW SESSION HANDLER] Пользователь {user_id}, step={step}, state={state}")
    
    if step != 'waiting_new_session':
        logger.info(f"[TICKET NEW SESSION HANDLER] Пропущено - step={step}, ожидался 'waiting_new_session'")
        return
    
    logger.info(f"[TICKET NEW SESSION HANDLER] Обработка ввода нового сеанса от {user_id}: {message.text}")
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    # Парсим ссылку или kp_id
    link = None
    kp_id = None
    
    link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/(\d+))', text)
    if link_match:
        link = link_match.group(1)
        kp_id = link_match.group(3)
    
    if not kp_id:
        id_match = re.search(r'^(\d+)', text)
        if id_match:
            kp_id = id_match.group(1)
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
    
    if not kp_id:
        bot.reply_to(message, "⚠️ Не найдена ссылка или ID фильма. Формат: ссылка или ID + дата + время")
        return
    
    # Парсим дату и время
    # Форматы: 10.01 15:20, 10 января 20:30, 10.01 15 20
    time_match = re.search(r'(\d{1,2})[\.:](\d{2})', text)
    if not time_match:
        time_match = re.search(r'(\d{1,2})\s+(\d{2})', text)
    
    if not time_match:
        bot.reply_to(message, "⚠️ Не найдено время. Формат: 15:20 или 15 20")
        return
    
    hour = int(time_match.group(1))
    minute = int(time_match.group(2))
    
    # Парсим дату
    date_match = re.search(r'(\d{1,2})[\./](\d{1,2})', text)
    if date_match:
        day = int(date_match.group(1))
        month = int(date_match.group(2))
    else:
        date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
        if date_match:
            day = int(date_match.group(1))
            month_str = date_match.group(2).lower()
            month = months_map.get(month_str)
            if not month:
                bot.reply_to(message, "⚠️ Не распознан месяц.")
                return
        else:
            bot.reply_to(message, "⚠️ Не найдена дата. Формат: 10.01 или 10 января")
            return
    
    now = datetime.now(plans_tz)
    year = now.year
    try:
        candidate = plans_tz.localize(datetime(year, month, day, hour, minute))
        if candidate < now:
            year += 1
            candidate = plans_tz.localize(datetime(year, month, day, hour, minute))
    except ValueError:
        bot.reply_to(message, "⚠️ Некорректная дата.")
        return
    
    plan_dt = candidate
    
    # Создаём фильм и план
    with db_lock:
        cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        row = cursor.fetchone()
        if row:
            if isinstance(row, dict):
                film_id = row.get('id')
                title = row.get('title')
            else:
                film_id = row[0]
                title = row[1]
        else:
            info = extract_movie_info(link)
            if not info:
                bot.reply_to(message, "Не удалось получить данные о фильме.")
                return
            is_series_val = 1 if info.get('is_series') else 0
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
            ''', (chat_id, link, kp_id, info.get('title'), info.get('year'), info.get('genres'), info.get('description'), info.get('director'), info.get('actors'), is_series_val))
            conn.commit()
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if isinstance(row, dict):
                film_id = row.get('id')
                title = row.get('title')
            else:
                film_id = row[0]
                title = row[1]
        
        # Создаём план "в кино"
        plan_utc = plan_dt.astimezone(pytz.utc)
        cursor.execute('''
            INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
            VALUES (%s, %s, 'cinema', %s, %s)
            RETURNING id
        ''', (chat_id, film_id, plan_utc, user_id))
        plan_row = cursor.fetchone()
        if isinstance(plan_row, dict):
            plan_id = plan_row.get('id')
        else:
            plan_id = plan_row[0]
        conn.commit()
    
    # Переходим к загрузке билетов
    user_ticket_state[user_id] = {
        'step': 'upload_ticket',
        'plan_id': plan_id,
        'film_title': title,
        'plan_dt': plan_dt.strftime('%d.%m %H:%M')
    }
    
    bot.reply_to(message, f"✅ Сеанс запланирован!\n\n<b>{title}</b>\n{plan_dt.strftime('%d.%m.%Y %H:%M')}\n\nПрикрепите фото билетов (можно несколько).", parse_mode='HTML')
    logger.info(f"[TICKET NEW SESSION HANDLER] Сеанс создан, ожидаем загрузку билетов, plan_id={plan_id}")


# ==================== ЗАГРУЗКА БИЛЕТОВ ====================
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_file_handler
# @bot.message_handler(content_types=['photo', 'document'], func=lambda m: m.from_user.id in user_ticket_state, priority=20)
def handle_ticket_upload_OLD(message):
    # Проверяем состояние внутри обработчика
    user_id = message.from_user.id
    state = user_ticket_state.get(user_id, {})
    step = state.get('step')
    plan_id = state.get('plan_id')
    
    logger.info(f"[TICKET UPLOAD HANDLER] ===== ОБРАБОТЧИК ВЫЗВАН =====")
    logger.info(f"[TICKET UPLOAD HANDLER] Пользователь {user_id}, step={step}, plan_id={plan_id}, state={state}")
    
    if step != 'upload_ticket':
        logger.info(f"[TICKET UPLOAD HANDLER] Пропущено - step={step}, ожидался 'upload_ticket'")
        return
    
    if not plan_id:
        logger.error(f"[TICKET UPLOAD HANDLER] plan_id не найден в состоянии")
        bot.reply_to(message, "❌ Ошибка: план не найден.")
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        return
    
    file_id = message.photo[-1].file_id if message.photo else message.document.file_id
    
    # Сохраняем file_id в plans
    with db_lock:
        cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (file_id, plan_id))
        conn.commit()
        logger.info(f"[TICKET UPLOAD HANDLER] Билет сохранен в БД для plan_id={plan_id}")
    
    title = state.get('film_title', 'фильм')
    dt = state.get('plan_dt', '')
    
    bot.reply_to(message, f"✅ Билет прикреплён!\n\n<b>{title}</b> — {dt}\n\nМожете отправить ещё билеты или написать 'готово'.", parse_mode='HTML')
    
    # Не удаляем состояние — пусть отправляет сколько угодно билетов
    # del user_ticket_state[user_id]  # Удаляй только по команде "готово" или кнопке


# ==================== ОБРАБОТКА КОМАНДЫ "ГОТОВО" ДЛЯ ЗАВЕРШЕНИЯ ЗАГРУЗКИ БИЛЕТОВ ====================
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_text_handler
# @bot.message_handler(func=lambda m: m.text and m.text.lower().strip() == 'готово' and m.from_user.id in user_ticket_state and user_ticket_state.get(m.from_user.id, {}).get('step') == 'upload_ticket', priority=20)
def ticket_done_OLD(message):
    """Обработка команды 'готово' для завершения загрузки билетов"""
    user_id = message.from_user.id
    state = user_ticket_state.get(user_id, {})
    title = state.get('film_title', 'фильм')
    dt = state.get('plan_dt', '')
    
    logger.info(f"[TICKET DONE] Пользователь {user_id} завершил загрузку билетов для сеанса: {title} — {dt}")
    
    bot.reply_to(message, f"✅ Все билеты прикреплены к сеансу:\n\n<b>{title}</b> — {dt}\n\nПриятного просмотра! 🎬", parse_mode='HTML')
    
    if user_id in user_ticket_state:
        del user_ticket_state[user_id]
        logger.info(f"[TICKET DONE] Состояние пользователя {user_id} очищено")


# Обработка новых ссылок (должен быть последним, чтобы не перехватывать команды)
# ВЫКЛЮЧЕНО: Теперь обрабатывается через main_text_handler
# @bot.message_handler(func=lambda m: m.text and not m.text.startswith('/') and m.entities, priority=1)
def handle_message_OLD(message):
    logger.info(f"[HANDLER] handle_message вызван для сообщения от {message.from_user.id}")
    
    # Пропускаем сообщения, если пользователь работает с билетами или планированием
    if message.from_user.id in user_ticket_state:
        state = user_ticket_state.get(message.from_user.id, {})
        step = state.get('step')
        logger.info(f"[HANDLER] Пропущено сообщение - пользователь в user_ticket_state, step={step}")
        return
    
    if message.from_user.id in user_plan_state:
        logger.info(f"[HANDLER] Пропущено сообщение - пользователь в user_plan_state")
        return
    
    if message.from_user.id in user_plan_state:
        logger.info(f"[HANDLER] Пропущено сообщение - пользователь в user_plan_state")
        return
    
    # НЕ пропускаем сообщения - пусть обработчики settings сами решают, обрабатывать ли их
    # Это позволяет handle_settings_emojis корректно обработать ответы на settings
    
    if not message.entities:
        return
    added_count = 0
    links = []
    for entity in message.entities:
        if entity.type == 'url':
            link = message.text[entity.offset:entity.offset + entity.length]
            if 'kinopoisk.ru' in link and ('/film/' in link or '/series/' in link):
                links.append(link)
    
    if links:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, 'add_movie', message.chat.id)
        logger.info(f"Найдено ссылок на Кинопоиск: {len(links)}")
    
    for link in links:
        if add_and_announce(link, message.chat.id):
            added_count += 1
    
    if added_count > 1:
        bot.send_message(message.chat.id, f"🎉 Добавлено {added_count} новых фильма в базу!")

logger.info("=" * 50)
logger.info("Финальная версия бота запущена! Всё готово 🎉")
logger.info(f"Токен: {TOKEN[:10] if TOKEN else 'не установлен'}...")
logger.info("=" * 50)

# --- /random — рандомный фильм с фильтрами ---
user_random_state = {}  # user_id: {'step': str, 'periods': [], 'genre': str, 'director': str, 'actor': str}

@bot.message_handler(commands=['random'])
def random_start(message):
    try:
        logger.info(f"[RANDOM] ===== START: user_id={message.from_user.id}, chat_id={message.chat.id}")
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Инициализируем состояние
        user_random_state[user_id] = {
            'step': 'mode',
            'mode': None,  # 'my_votes', 'group_votes', или None (обычный режим)
            'periods': [],
            'genres': [],
            'directors': [],
            'actors': []
        }
        
        # Шаг 0: Выбор режима
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎲 Обычный режим", callback_data="rand_mode:normal"))
        
        # Проверяем, есть ли у пользователя больше 50 оценок (включая импортированные из КП)
        with db_lock:
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
            user_ratings_count = cursor.fetchone()
            user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
            
            if user_ratings >= 50:
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                # Заблокированная кнопка
                markup.add(InlineKeyboardButton("🔒 Откроется от 50 оценок с КП", callback_data="rand_mode_locked:my_votes"))
            
            # Проверяем условие для group_votes: больше 20 групповых оценок, где хотя бы 20 фильмов оценили все участники группы
            # Сначала получаем общее количество уникальных пользователей в группе (исключаем импортированные оценки)
            cursor.execute('SELECT COUNT(DISTINCT user_id) FROM ratings WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id,))
            total_users_row = cursor.fetchone()
            total_users = total_users_row.get('count') if isinstance(total_users_row, dict) else (total_users_row[0] if total_users_row else 0)
            
            group_votes_available = False
            if total_users > 0:
                # Находим фильмы, которые оценили все участники группы (исключаем импортированные оценки)
                cursor.execute('''
                    SELECT film_id, COUNT(DISTINCT user_id) as user_count
                    FROM ratings 
                    WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                    GROUP BY film_id 
                    HAVING COUNT(DISTINCT user_id) = %s
                ''', (chat_id, total_users))
                group_rated_films = cursor.fetchall()
                group_rated_count = len(group_rated_films)
                
                # Также проверяем, что общее количество групповых оценок больше 20 (исключаем импортированные)
                cursor.execute('''
                    SELECT COUNT(*) 
                    FROM (
                        SELECT film_id 
                        FROM ratings 
                        WHERE chat_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                        GROUP BY film_id 
                        HAVING COUNT(DISTINCT user_id) > 1
                    ) as group_rated
                ''', (chat_id,))
                total_group_ratings_row = cursor.fetchone()
                total_group_ratings = total_group_ratings_row.get('count') if isinstance(total_group_ratings_row, dict) else (total_group_ratings_row[0] if total_group_ratings_row else 0)
                
                if group_rated_count >= 20 and total_group_ratings > 20:
                    markup.add(InlineKeyboardButton("👥 По оценкам группы (8+)", callback_data="rand_mode:group_votes"))
                    group_votes_available = True
            
            # Если режим group_votes недоступен, добавляем заблокированную кнопку
            if not group_votes_available:
                markup.add(InlineKeyboardButton("🔒 Откроется от 20 групповых оценок", callback_data="rand_mode_locked:group_votes"))
        
        bot.send_message(chat_id, "🎲 <b>Выберите режим рандомайзера:</b>", reply_markup=markup, parse_mode='HTML')
        logger.info(f"[RANDOM] Step 0 sent: mode selection, user_id={user_id}")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_start: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Ошибка при запуске рандомайзера. Попробуйте позже.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode_locked:"))
def random_mode_locked_handler(call):
    """Обработчик заблокированных режимов рандомайзера"""
    try:
        mode = call.data.split(":")[1]
        
        if mode == 'my_votes':
            bot.answer_callback_query(call.id, "🔒 Этот режим откроется при наличии 50 оценок с Кинопоиска", show_alert=False)
        elif mode == 'group_votes':
            bot.answer_callback_query(call.id, "🔒 Этот режим откроется при наличии 20 групповых оценок", show_alert=False)
        else:
            bot.answer_callback_query(call.id, "🔒 Этот режим пока недоступен", show_alert=False)
    except Exception as e:
        logger.error(f"[RANDOM LOCKED] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=False)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode:"))
def random_mode_handler(call):
    """Обработчик выбора режима рандомайзера"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        mode = call.data.split(":")[1]
        
        if user_id not in user_random_state:
            bot.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
            return
        
        user_random_state[user_id]['mode'] = mode
        user_random_state[user_id]['step'] = 'period'
        
        # Шаг 1: Выбор периода - показываем только те периоды, где есть фильмы
        all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
        available_periods = []
        
        with db_lock:
            # Формируем базовый запрос в зависимости от режима
            base_query = "SELECT COUNT(*) FROM movies m WHERE m.chat_id = %s AND m.watched = 0"
            params = [chat_id]
            
            if mode == 'my_votes':
                # Фильмы с оценкой пользователя >= 8
                base_query += " AND EXISTS (SELECT 1 FROM ratings r WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND r.user_id = %s AND r.rating >= 8)"
                params.append(user_id)
            elif mode == 'group_votes':
                # Фильмы со средней оценкой группы >= 8 (исключаем импортированные оценки)
                base_query += " AND EXISTS (SELECT 1 FROM ratings r WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) GROUP BY r.film_id, r.chat_id HAVING AVG(r.rating) >= 8)"
            
            for period in all_periods:
                if period == "До 1980":
                    condition = "m.year < 1980"
                elif period == "1980–1990":
                    condition = "(m.year >= 1980 AND m.year <= 1990)"
                elif period == "1990–2000":
                    condition = "(m.year >= 1990 AND m.year <= 2000)"
                elif period == "2000–2010":
                    condition = "(m.year >= 2000 AND m.year <= 2010)"
                elif period == "2010–2020":
                    condition = "(m.year >= 2010 AND m.year <= 2020)"
                elif period == "2020–сейчас":
                    condition = "m.year >= 2020"
                
                query = f"{base_query} AND {condition}"
                cursor.execute(query, tuple(params))
                count_row = cursor.fetchone()
                count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                
                if count > 0:
                    available_periods.append(period)
        
        user_random_state[user_id]['available_periods'] = available_periods
        
        markup = InlineKeyboardMarkup(row_width=2)
        if available_periods:
            for i in range(0, len(available_periods), 2):
                row = []
                row.append(InlineKeyboardButton(available_periods[i], callback_data=f"rand_period:{available_periods[i]}"))
                if i+1 < len(available_periods):
                    row.append(InlineKeyboardButton(available_periods[i+1], callback_data=f"rand_period:{available_periods[i+1]}"))
                markup.row(*row)
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
        
        bot.answer_callback_query(call.id)
        bot.edit_message_text("🎲 <b>Шаг 1/4: Выберите период</b>\n\n(можно выбрать несколько или пропустить)", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[RANDOM] Mode selected: {mode}, moving to period selection, user_id={user_id}")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_mode_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_period:"))
def random_period_handler(call):
    try:
        logger.info(f"[RANDOM] ===== PERIOD HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1]
        
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM] State not found for user {user_id}, reinitializing")
            user_random_state[user_id] = {'step': 'period', 'periods': [], 'genre': None, 'director': None, 'actor': None}
        
        if data == "skip":
            logger.info(f"[RANDOM] Period skipped, moving to genre")
            user_random_state[user_id]['periods'] = []
            user_random_state[user_id]['step'] = 'genre'
            _show_genre_step(call, chat_id, user_id)
        elif data == "done":
            # Переход к следующему шагу
            logger.info(f"[RANDOM] Periods confirmed, moving to genre")
            user_random_state[user_id]['step'] = 'genre'
            _show_genre_step(call, chat_id, user_id)
        else:
            # Toggle периода
            periods = user_random_state[user_id].get('periods', [])
            if data in periods:
                periods.remove(data)
                logger.info(f"[RANDOM] Period removed: {data}")
            else:
                periods.append(data)
                logger.info(f"[RANDOM] Period added: {data}")
            
            user_random_state[user_id]['periods'] = periods
            
            # Получаем доступные периоды из состояния
            available_periods = user_random_state[user_id].get('available_periods', [])
            if not available_periods:
                # Если нет в состоянии, получаем заново
                all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                with db_lock:
                    for period in all_periods:
                        if period == "До 1980":
                            condition = "year < 1980"
                        elif period == "1980–1990":
                            condition = "(year >= 1980 AND year <= 1990)"
                        elif period == "1990–2000":
                            condition = "(year >= 1990 AND year <= 2000)"
                        elif period == "2000–2010":
                            condition = "(year >= 2000 AND year <= 2010)"
                        elif period == "2010–2020":
                            condition = "(year >= 2010 AND year <= 2020)"
                        elif period == "2020–сейчас":
                            condition = "year >= 2020"
                        
                        cursor.execute(f"""
                            SELECT COUNT(*) FROM movies 
                            WHERE chat_id = %s AND watched = 0 AND {condition}
                        """, (chat_id,))
                        count_row = cursor.fetchone()
                        count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                        
                        if count > 0:
                            available_periods.append(period)
                user_random_state[user_id]['available_periods'] = available_periods
            
            # Обновляем кнопки - используем только доступные периоды
            markup = InlineKeyboardMarkup(row_width=2)
            if available_periods:
                for i in range(0, len(available_periods), 2):
                    row = []
                    for j in range(2):
                        if i + j < len(available_periods):
                            p = available_periods[i + j]
                            label = f"✓ {p}" if p in periods else p
                            row.append(InlineKeyboardButton(label, callback_data=f"rand_period:{p}"))
                    markup.row(*row)
            
            # Кнопка "Продолжить" появляется только если выбран хотя бы один период
            # "Пропустить" убирается, если выбран хотя бы один период
            if periods:
                markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
            else:
                markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            
            selected = ', '.join(periods) if periods else 'ничего'
            try:
                bot.edit_message_text(f"🎲 <b>Шаг 1/4: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько)", 
                                    chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                bot.answer_callback_query(call.id)
                logger.info(f"[RANDOM] Period keyboard updated, selected={selected}")
            except Exception as e:
                logger.error(f"[RANDOM] Error updating period keyboard: {e}", exc_info=True)
                bot.answer_callback_query(call.id, "Ошибка обновления")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_period_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

def _show_genre_step(call, chat_id, user_id):
    """Показывает шаг выбора жанра с учетом выбранных периодов"""
    try:
        logger.info(f"[RANDOM] Showing genre step for user {user_id}")
        
        # Получаем состояние пользователя
        state = user_random_state.get(user_id, {})
        selected_genres = state.get('genres', [])
        periods = state.get('periods', [])
        
        # Формируем WHERE условие с учетом периодов
        base_query = """
            SELECT DISTINCT TRIM(UNNEST(string_to_array(genres, ', '))) as genre
            FROM movies
            WHERE chat_id = %s AND watched = 0 
            AND genres IS NOT NULL AND genres != '' AND genres != '—'
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
        if periods:
            period_conditions = []
            for p in periods:
                if p == "До 1980":
                    period_conditions.append("year < 1980")
                elif p == "1980–1990":
                    period_conditions.append("(year >= 1980 AND year <= 1990)")
                elif p == "1990–2000":
                    period_conditions.append("(year >= 1990 AND year <= 2000)")
                elif p == "2000–2010":
                    period_conditions.append("(year >= 2000 AND year <= 2010)")
                elif p == "2010–2020":
                    period_conditions.append("(year >= 2010 AND year <= 2020)")
                elif p == "2020–сейчас":
                    period_conditions.append("year >= 2020")
            if period_conditions:
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        with db_lock:
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            genres = []
            for row in rows:
                genre = row.get('genre') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                if genre and genre.strip():
                    genres.append(genre.strip())
            logger.info(f"[RANDOM] Genres found: {len(genres)}")
        
        markup = InlineKeyboardMarkup(row_width=2)
        if genres:
            for genre in sorted(set(genres))[:20]:  # Ограничиваем до 20 жанров
                label = f"✓ {genre}" if genre in selected_genres else genre
                markup.add(InlineKeyboardButton(label, callback_data=f"rand_genre:{genre}"))
        
        # Кнопки навигации: "Назад" и "Пропустить"/"Продолжить" в одной строке
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rand_genre:back"))
        if selected_genres:
            nav_buttons.append(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_genre:done"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_genre:skip"))
        markup.row(*nav_buttons)
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_genres)}" if selected_genres else ""
        try:
            bot.edit_message_text(f"🎬 <b>Шаг 2/4: Выберите жанр</b>\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
            logger.info(f"[RANDOM] Genre step shown, user_id={user_id}, selected={len(selected_genres)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing genre step: {e}", exc_info=True)
            # Пробуем отправить новое сообщение
            bot.send_message(chat_id, f"🎬 <b>Шаг 2/4: Выберите жанр</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_genre_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки жанров")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_genre:"))
def random_genre_handler(call):
    try:
        logger.info(f"[RANDOM] ===== GENRE HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1]
        
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM] State not found for user {user_id}, reinitializing")
            user_random_state[user_id] = {'step': 'genre', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
        
        mode = user_random_state[user_id].get('mode')
        
        # Обрабатываем выбор жанра (toggle)
        if data not in ["skip", "done", "back"]:
            # Toggle жанра
            genres = user_random_state[user_id].get('genres', [])
            if data in genres:
                genres.remove(data)
                logger.info(f"[RANDOM] Genre removed: {data}")
            else:
                genres.append(data)
                logger.info(f"[RANDOM] Genre added: {data}")
            
            user_random_state[user_id]['genres'] = genres
            user_random_state[user_id]['step'] = 'genre'
            
            # Для режимов my_votes и group_votes после выбора жанра сразу переходим к финалу
            if mode in ['my_votes', 'group_votes']:
                # Переходим сразу к финалу (жанр уже сохранен)
                logger.info(f"[RANDOM] Mode {mode}: genre '{data}' selected, moving to final")
                user_random_state[user_id]['step'] = 'final'
                _random_final(call, chat_id, user_id)
                return
            else:
                # Для обычного режима обновляем клавиатуру
                _show_genre_step(call, chat_id, user_id)
                return
        
        # Для режимов my_votes и group_votes после подтверждения жанров сразу переходим к финалу
        if mode in ['my_votes', 'group_votes']:
            if data == "skip":
                user_random_state[user_id]['genres'] = []
            elif data == "done":
                pass  # Жанры уже сохранены
            
            # Переходим сразу к финалу
            logger.info(f"[RANDOM] Mode {mode}: genres selected, moving to final")
            user_random_state[user_id]['step'] = 'final'
            _random_final(call, chat_id, user_id)
            return
        
        # Для обычного режима переходим к режиссёру
        if data == "skip":
            user_random_state[user_id]['genres'] = []
            user_random_state[user_id]['step'] = 'director'
            logger.info(f"[RANDOM] Genre skipped, moving to director")
            _show_director_step(call, chat_id, user_id)
        elif data == "done":
            # Переход к следующему шагу
            logger.info(f"[RANDOM] Genres confirmed, moving to director")
            user_random_state[user_id]['step'] = 'director'
            _show_director_step(call, chat_id, user_id)
        elif data == "back":
            # Возврат к предыдущему шагу (периоды)
            logger.info(f"[RANDOM] Genre back, moving to period")
            user_random_state[user_id]['step'] = 'period'
            # Показываем шаг периодов
            periods = user_random_state[user_id].get('periods', [])
            available_periods = user_random_state[user_id].get('available_periods', [])
            if not available_periods:
                available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            
            markup = InlineKeyboardMarkup(row_width=2)
            if available_periods:
                for i in range(0, len(available_periods), 2):
                    row = []
                    for j in range(2):
                        if i + j < len(available_periods):
                            p = available_periods[i + j]
                            label = f"✓ {p}" if p in periods else p
                            row.append(InlineKeyboardButton(label, callback_data=f"rand_period:{p}"))
                    markup.row(*row)
            
            if periods:
                markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
            else:
                markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            
            selected = ', '.join(periods) if periods else 'ничего'
            try:
                bot.edit_message_text(f"🎲 <b>Шаг 1/4: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько)", 
                                    chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                bot.answer_callback_query(call.id)
            except Exception as e:
                logger.error(f"[RANDOM] Error going back to period: {e}", exc_info=True)
                bot.answer_callback_query(call.id, "Ошибка")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_genre_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

def _show_director_step(call, chat_id, user_id):
    """Показывает шаг выбора режиссёра с учетом выбранных периодов и жанров"""
    try:
        logger.info(f"[RANDOM] Showing director step for user {user_id}")
        
        # Получаем состояние пользователя
        state = user_random_state.get(user_id, {})
        selected_directors = state.get('directors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        
        # Формируем WHERE условие с учетом периодов и жанров
        base_query = """
            SELECT director, COUNT(*) as cnt
            FROM movies
            WHERE chat_id = %s AND watched = 0 
            AND director IS NOT NULL AND director != 'Не указан' AND director != ''
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
        if periods:
            period_conditions = []
            for p in periods:
                if p == "До 1980":
                    period_conditions.append("year < 1980")
                elif p == "1980–1990":
                    period_conditions.append("(year >= 1980 AND year <= 1990)")
                elif p == "1990–2000":
                    period_conditions.append("(year >= 1990 AND year <= 2000)")
                elif p == "2000–2010":
                    period_conditions.append("(year >= 2000 AND year <= 2010)")
                elif p == "2010–2020":
                    period_conditions.append("(year >= 2010 AND year <= 2020)")
                elif p == "2020–сейчас":
                    period_conditions.append("year >= 2020")
            if period_conditions:
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        base_query += " GROUP BY director ORDER BY cnt DESC LIMIT 10"
        
        with db_lock:
            cursor.execute(base_query, params)
            rows = cursor.fetchall()
            directors = []
            for row in rows:
                director = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                if director:
                    directors.append(director)
            logger.info(f"[RANDOM] Directors found: {len(directors)}")
        
        markup = InlineKeyboardMarkup(row_width=2)
        if directors:
            for d in directors:
                label = f"✓ {d}" if d in selected_directors else d
                markup.add(InlineKeyboardButton(label, callback_data=f"rand_dir:{d}"))
        
        # Кнопки навигации: "Назад" и "Пропустить"/"Продолжить" в одной строке
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rand_dir:back"))
        if selected_directors:
            nav_buttons.append(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_dir:done"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_dir:skip"))
        markup.row(*nav_buttons)
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_directors)}" if selected_directors else ""
        try:
            bot.edit_message_text(f"🎥 <b>Шаг 3/4: Выберите режиссёра</b>\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
            logger.info(f"[RANDOM] Director step shown, user_id={user_id}, selected={len(selected_directors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing director step: {e}", exc_info=True)
            bot.send_message(chat_id, f"🎥 <b>Шаг 3/4: Выберите режиссёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_director_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки режиссёров")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_dir:"))
def random_director_handler(call):
    try:
        logger.info(f"[RANDOM] ===== DIRECTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1]
        
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM] State not found for user {user_id}, reinitializing")
            user_random_state[user_id] = {'step': 'director', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
        
        if data == "skip":
            user_random_state[user_id]['directors'] = []
            user_random_state[user_id]['step'] = 'actor'
            logger.info(f"[RANDOM] Director skipped, moving to actor")
            # Инициализируем список актёров, если его нет
            if 'actors' not in user_random_state[user_id]:
                user_random_state[user_id]['actors'] = []
            _show_actor_step(call, chat_id, user_id)
        elif data == "done":
            # Переход к следующему шагу
            logger.info(f"[RANDOM] Directors confirmed, moving to actor")
            user_random_state[user_id]['step'] = 'actor'
            # Инициализируем список актёров, если его нет
            if 'actors' not in user_random_state[user_id]:
                user_random_state[user_id]['actors'] = []
            _show_actor_step(call, chat_id, user_id)
        elif data == "back":
            # Возврат к предыдущему шагу (жанры)
            logger.info(f"[RANDOM] Director back, moving to genre")
            user_random_state[user_id]['step'] = 'genre'
            _show_genre_step(call, chat_id, user_id)
        else:
            # Toggle режиссера
            directors = user_random_state[user_id].get('directors', [])
            if data in directors:
                directors.remove(data)
                logger.info(f"[RANDOM] Director removed: {data}")
            else:
                directors.append(data)
                logger.info(f"[RANDOM] Director added: {data}")
            
            user_random_state[user_id]['directors'] = directors
            user_random_state[user_id]['step'] = 'director'
            
            # Обновляем клавиатуру
            _show_director_step(call, chat_id, user_id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_director_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

def _show_actor_step(call, chat_id, user_id):
    """Показывает шаг выбора актёра с учетом всех выбранных фильтров"""
    try:
        logger.info(f"[RANDOM] Showing actor step for user {user_id}")
        
        # Получаем состояние пользователя
        if user_id not in user_random_state:
            user_random_state[user_id] = {'actors': []}
        state = user_random_state[user_id]
        selected_actors = state.get('actors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        directors = state.get('directors', [])
        
        # Формируем WHERE условие с учетом всех фильтров
        base_query = """
            SELECT actors FROM movies
            WHERE chat_id = %s AND watched = 0 
            AND actors IS NOT NULL AND actors != '' AND actors != '—'
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
        if periods:
            period_conditions = []
            for p in periods:
                if p == "До 1980":
                    period_conditions.append("year < 1980")
                elif p == "1980–1990":
                    period_conditions.append("(year >= 1980 AND year <= 1990)")
                elif p == "1990–2000":
                    period_conditions.append("(year >= 1990 AND year <= 2000)")
                elif p == "2000–2010":
                    period_conditions.append("(year >= 2000 AND year <= 2010)")
                elif p == "2010–2020":
                    period_conditions.append("(year >= 2010 AND year <= 2020)")
                elif p == "2020–сейчас":
                    period_conditions.append("year >= 2020")
            if period_conditions:
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Добавляем фильтр по режиссерам, если они выбраны
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("director = %s")
                params.append(director)
            if director_conditions:
                base_query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Берем топ актёров по частоте
        actor_counts = {}
        with db_lock:
            cursor.execute(base_query, params)
            for row in cursor.fetchall():
                actors_str = row.get('actors') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                if actors_str:
                    for actor in actors_str.split(', '):
                        actor = actor.strip()
                        if actor:
                            actor_counts[actor] = actor_counts.get(actor, 0) + 1
            logger.info(f"[RANDOM] Unique actors found: {len(actor_counts)}")
        
        markup = InlineKeyboardMarkup(row_width=2)
        if actor_counts:
            top_actors = sorted(actor_counts.items(), key=lambda x: x[1], reverse=True)[:10]
            for actor, _ in top_actors:
                # Показываем галочку, если актёр выбран
                label = f"✓ {actor}" if actor in selected_actors else actor
                markup.add(InlineKeyboardButton(label, callback_data=f"rand_actor:{actor}"))
        
        # Кнопки навигации: "Назад" и "Пропустить"/"Найти фильм" в одной строке
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rand_actor:back"))
        if selected_actors:
            nav_buttons.append(InlineKeyboardButton("🎲 Найти фильм", callback_data="rand_final:go"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_actor:skip"))
        markup.row(*nav_buttons)
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_actors)}" if selected_actors else ""
        try:
            bot.edit_message_text(f"🎭 <b>Шаг 4/4: Выберите актёра</b>\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
            logger.info(f"[RANDOM] Actor step shown, user_id={user_id}, selected={len(selected_actors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing actor step: {e}", exc_info=True)
            bot.send_message(chat_id, f"🎭 <b>Шаг 4/4: Выберите актёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_actor_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки актёров")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_actor:"))
def random_actor_handler(call):
    try:
        logger.info(f"[RANDOM] ===== ACTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        data = call.data.split(":", 1)[1]
        
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM] State not found for user {user_id}, reinitializing")
            user_random_state[user_id] = {'step': 'actor', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
        
        if data == "skip":
            # Пропускаем выбор актёров - переходим к финалу
            user_random_state[user_id]['actors'] = []
            user_random_state[user_id]['step'] = 'final'
            logger.info(f"[RANDOM] Actors skipped, moving to final")
            _random_final(call, chat_id, user_id)
        elif data == "back":
            # Возврат к предыдущему шагу (режиссеры)
            logger.info(f"[RANDOM] Actor back, moving to director")
            user_random_state[user_id]['step'] = 'director'
            _show_director_step(call, chat_id, user_id)
        else:
            # Toggle актёра
            actors = user_random_state[user_id].get('actors', [])
            if data in actors:
                actors.remove(data)
                logger.info(f"[RANDOM] Actor removed: {data}")
            else:
                actors.append(data)
                logger.info(f"[RANDOM] Actor added: {data}")
            
            user_random_state[user_id]['actors'] = actors
            user_random_state[user_id]['step'] = 'actor'
            
            # Обновляем клавиатуру
            _show_actor_step(call, chat_id, user_id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_actor_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_final:"))
def random_final_handler(call):
    try:
        logger.info(f"[RANDOM] ===== FINAL HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM] State not found for user {user_id}")
            bot.answer_callback_query(call.id, "Ошибка: сессия устарела. Начните заново /random")
            return
        
        _random_final(call, chat_id, user_id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in random_final_handler: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

def _random_final(call, chat_id, user_id):
    """Финальный шаг - поиск и показ фильма"""
    try:
        logger.info(f"[RANDOM] ===== FINAL: user_id={user_id}, chat_id={chat_id}")
        state = user_random_state.get(user_id, {})
        logger.info(f"[RANDOM] State: {state}")
        
        # Формируем запрос - исключаем фильмы, которые уже запланированы
        query = """SELECT m.id, m.title, m.year, m.genres, m.director, m.actors, m.description, m.link, m.kp_id 
                   FROM movies m 
                   WHERE m.chat_id = %s AND m.watched = 0 
                   AND m.id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s)"""
        params = [chat_id, chat_id]
        
        # Фильтр по режиму (my_votes или group_votes)
        mode = state.get('mode')
        if mode == 'my_votes':
            # Фильмы с импортированной оценкой пользователя 9 или 10 из Кинопоиска
            query += """ AND m.id IN (
                SELECT DISTINCT r2.film_id 
                FROM ratings r2 
                WHERE r2.chat_id = %s AND r2.user_id = %s AND r2.rating IN (9, 10) AND r2.is_imported = TRUE
            )"""
            params.append(chat_id)
            params.append(user_id)
        elif mode == 'group_votes':
            # Фильмы со средней оценкой группы >= 8 (исключаем импортированные оценки)
            query += " AND EXISTS (SELECT 1 FROM ratings r WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) GROUP BY r.film_id, r.chat_id HAVING AVG(r.rating) >= 8)"
        
        # Фильтр по периодам
        periods = state.get('periods', [])
        if periods:
            period_conditions = []
            for p in periods:
                if p == "До 1980":
                    period_conditions.append("year < 1980")
                elif p == "1980–1990":
                    period_conditions.append("(year >= 1980 AND year <= 1990)")
                elif p == "1990–2000":
                    period_conditions.append("(year >= 1990 AND year <= 2000)")
                elif p == "2000–2010":
                    period_conditions.append("(year >= 2000 AND year <= 2010)")
                elif p == "2010–2020":
                    period_conditions.append("(year >= 2010 AND year <= 2020)")
                elif p == "2020–сейчас":
                    period_conditions.append("year >= 2020")
            if period_conditions:
                query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Фильтр по жанрам (можно несколько, OR условие)
        genres = state.get('genres', [])
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Фильтр по режиссёрам (можно несколько, OR условие)
        directors = state.get('directors', [])
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("director = %s")
                params.append(director)
            if director_conditions:
                query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Фильтр по актёрам (можно несколько, OR условие)
        actors = state.get('actors', [])
        if actors:
            actor_conditions = []
            for actor in actors:
                actor_conditions.append("actors ILIKE %s")
                params.append(f"%{actor}%")
            if actor_conditions:
                query += " AND (" + " OR ".join(actor_conditions) + ")"
        
        logger.info(f"[RANDOM] Query: {query}")
        logger.info(f"[RANDOM] Params: {params}")
        
        with db_lock:
            cursor.execute(query, params)
            candidates = cursor.fetchall()
            logger.info(f"[RANDOM] Candidates found: {len(candidates)}")
        
        if not candidates:
            # Ищем похожие фильмы из запланированных
            similar_query = """SELECT m.title, m.year, m.link 
                               FROM movies m 
                               JOIN plans p ON m.id = p.film_id 
                               WHERE m.chat_id = %s AND m.watched = 0"""
            similar_params = [chat_id]
            
            # Применяем те же фильтры для поиска похожих
            if periods:
                period_conditions = []
                for p in periods:
                    if p == "До 1980":
                        period_conditions.append("m.year < 1980")
                    elif p == "1980–1990":
                        period_conditions.append("(m.year >= 1980 AND m.year <= 1990)")
                    elif p == "1990–2000":
                        period_conditions.append("(m.year >= 1990 AND m.year <= 2000)")
                    elif p == "2000–2010":
                        period_conditions.append("(m.year >= 2000 AND m.year <= 2010)")
                    elif p == "2010–2020":
                        period_conditions.append("(m.year >= 2010 AND m.year <= 2020)")
                    elif p == "2020–сейчас":
                        period_conditions.append("m.year >= 2020")
                if period_conditions:
                    similar_query += " AND (" + " OR ".join(period_conditions) + ")"
            
            # Фильтр по жанрам (можно несколько, OR условие)
            genres = state.get('genres', [])
            if genres:
                genre_conditions = []
                for genre in genres:
                    genre_conditions.append("m.genres ILIKE %s")
                    similar_params.append(f"%{genre}%")
                if genre_conditions:
                    similar_query += " AND (" + " OR ".join(genre_conditions) + ")"
            
            # Фильтр по режиссёрам (можно несколько, OR условие)
            directors = state.get('directors', [])
            if directors:
                director_conditions = []
                for director in directors:
                    director_conditions.append("m.director = %s")
                    similar_params.append(director)
                if director_conditions:
                    similar_query += " AND (" + " OR ".join(director_conditions) + ")"
            
            if actors:
                actor_conditions = []
                for actor in actors:
                    actor_conditions.append("m.actors ILIKE %s")
                    similar_params.append(f"%{actor}%")
                if actor_conditions:
                    similar_query += " AND (" + " OR ".join(actor_conditions) + ")"
            
            similar_query += " LIMIT 10"
            
            with db_lock:
                cursor.execute(similar_query, similar_params)
                similar_movies = cursor.fetchall()
            
            if similar_movies:
                # Формируем список похожих фильмов
                similar_list = []
                for movie in similar_movies:
                    if isinstance(movie, dict):
                        title = movie.get('title')
                        year = movie.get('year') or '—'
                        link = movie.get('link')
                    else:
                        title = movie[0] if len(movie) > 0 else None
                        year = movie[1] if len(movie) > 1 else '—'
                        link = movie[2] if len(movie) > 2 else None
                    
                    if title and link:
                        similar_list.append(f"• <a href='{link}'>{title}</a> ({year})")
                
                if similar_list:
                    similar_text = "\n".join(similar_list)
                    message_text = f"😔 Таких фильмов в базе не найдено! Но есть похожие из запланированных:\n\n{similar_text}"
                else:
                    message_text = "😔 Таких фильмов в базе не найдено!"
            else:
                message_text = "😔 Таких фильмов в базе не найдено!"
            
            try:
                bot.edit_message_text(message_text, 
                                    chat_id, call.message.message_id, parse_mode='HTML', disable_web_page_preview=False)
                bot.answer_callback_query(call.id)
            except:
                bot.send_message(chat_id, message_text, parse_mode='HTML', disable_web_page_preview=False)
            del user_random_state[user_id]
            return
        
        movie = random.choice(candidates)
        if isinstance(movie, dict):
            film_id = movie.get('id')
            title = movie.get('title')
            year = movie.get('year') or '—'
            link = movie.get('link')
            kp_id = movie.get('kp_id') if 'kp_id' in movie else None
        else:
            # Кортеж
            film_id = movie[0] if len(movie) > 0 else None
            title = movie[1] if len(movie) > 1 else None
            year = movie[2] if len(movie) > 2 else '—'
            link = movie[7] if len(movie) > 7 else None
            kp_id = movie[8] if len(movie) > 8 else None
        
        # Для режимов my_votes и group_votes ищем похожие фильмы
        if mode in ['my_votes', 'group_votes']:
            # Получаем kp_id из базы, если не получили
            if not kp_id and link:
                try:
                    kp_match = re.search(r'/film/(\d+)/', link)
                    if kp_match:
                        kp_id = kp_match.group(1)
                except:
                    pass
            
            if kp_id:
                # Получаем похожие фильмы
                similars = get_similars(kp_id)
                logger.info(f"[RANDOM] Found {len(similars)} similar films for kp_id={kp_id}")
                
                if similars:
                    # Получаем выбранные периоды и жанры для фильтрации
                    periods = state.get('periods', [])
                    genres = state.get('genres', [])
                    
                    # Функция для проверки года
                    def check_year(film_year, periods_list):
                        if not periods_list:
                            return True
                        for p in periods_list:
                            if p == "До 1980" and film_year < 1980:
                                return True
                            elif p == "1980–1990" and 1980 <= film_year <= 1990:
                                return True
                            elif p == "1990–2000" and 1990 <= film_year <= 2000:
                                return True
                            elif p == "2000–2010" and 2000 <= film_year <= 2010:
                                return True
                            elif p == "2010–2020" and 2010 <= film_year <= 2020:
                                return True
                            elif p == "2020–сейчас" and film_year >= 2020:
                                return True
                        return False
                    
                    # Функция для проверки жанра
                    def check_genre(film_genres, genres_list):
                        if not genres_list:
                            return True
                        film_genres_lower = str(film_genres).lower() if film_genres else ""
                        for g in genres_list:
                            if g.lower() in film_genres_lower:
                                return True
                        return False
                    
                    # Получаем информацию о похожих фильмах через API и фильтруем
                    filtered_similars = []
                    headers = {'X-API-KEY': KP_TOKEN}
                    
                    for similar_kp_id, similar_title in similars:
                        try:
                            # Получаем информацию о фильме через API
                            url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{similar_kp_id}"
                            response = requests.get(url, headers=headers, timeout=10)
                            if response.status_code == 200:
                                data = response.json()
                                similar_year = data.get('year')
                                similar_genres = ', '.join([g.get('genre', '') for g in data.get('genres', [])])
                                
                                # Проверяем год и жанр
                                if similar_year and check_year(similar_year, periods):
                                    if check_genre(similar_genres, genres):
                                        filtered_similars.append({
                                            'kp_id': similar_kp_id,
                                            'title': similar_title,
                                            'year': similar_year,
                                            'genres': similar_genres,
                                            'link': f"https://www.kinopoisk.ru/film/{similar_kp_id}/"
                                        })
                        except Exception as e:
                            logger.warning(f"[RANDOM] Error getting info for similar film {similar_kp_id}: {e}")
                            continue
                    
                    if filtered_similars:
                        # Выбираем случайный из отфильтрованных похожих
                        selected_similar = random.choice(filtered_similars)
                        title = selected_similar['title']
                        year = selected_similar['year']
                        link = selected_similar['link']
                        logger.info(f"[RANDOM] Selected similar film: {title} ({year})")
                    else:
                        logger.info(f"[RANDOM] No similar films match filters, using original")
        
        text = f"🍿 <b>Случайный фильм:</b>\n\n<b>{title}</b> ({year})\n\n<a href='{link}'>Кинопоиск</a>"
        
        film_message_id = None
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, parse_mode='HTML', disable_web_page_preview=False)
            film_message_id = call.message.message_id
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"[RANDOM] Error editing message: {e}", exc_info=True)
            sent_msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
            film_message_id = sent_msg.message_id
            bot.answer_callback_query(call.id)
        
        # Сохраняем message_id фильма для обработки реакций и реплаев
        if film_message_id:
            bot_messages[film_message_id] = link
            logger.info(f"[RANDOM] Saved film message_id={film_message_id} with link={link}")
        
        # Отправляем инструкцию
        try:
            instruction_text = (
                "💬 <b>Что дальше?</b>\n\n"
                "• Ответьте на это сообщение в формате <code>дома/в кино + дата</code>, "
                "чтобы запланировать фильм\n"
                "• Поставьте реакцию просмотра на это сообщение или сообщение фильма, "
                "чтобы отметить фильм как просмотренный"
            )
            sent = bot.send_message(chat_id, instruction_text, parse_mode='HTML')
            # Также сохраняем для обработки реплаев
            bot_messages[sent.message_id] = link
        except Exception as e:
            logger.error(f"[RANDOM] Error sending instruction message: {e}", exc_info=True)
        
        del user_random_state[user_id]
        logger.info(f"[RANDOM] ===== COMPLETED: Film shown - {title}")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _random_final: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка поиска фильма")
            if user_id in user_random_state:
                del user_random_state[user_id]
        except:
            pass

# Callback handlers для /search
@bot.callback_query_handler(func=lambda call: call.data.startswith("add_film_"))
def handle_add_film_callback(call):
    """Обработчик показа описания фильма из результатов поиска"""
    try:
        kp_id = call.data.split("_")[-1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[SEARCH] Показ описания фильма kp_id={kp_id} от пользователя {user_id}")
        
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        # Проверяем, добавлен ли уже
        with db_lock:
            cursor.execute("SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
            existing = cursor.fetchone()
            if existing:
                title = existing.get('title') if isinstance(existing, dict) else existing[1]
                bot.answer_callback_query(call.id, f"Фильм '{title}' уже добавлен!", show_alert=False)
                return
        
        # Получаем информацию о фильме
        info = extract_movie_info(link)
        if not info:
            bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            return
        
        # Формируем описание фильма
        title = info.get('title', 'Без названия')
        year = info.get('year', '—')
        genres = info.get('genres', '—')
        director = info.get('director', '—')
        actors = info.get('actors', '—')
        description = info.get('description', 'Нет описания')
        
        # Ограничиваем длину описания
        if len(description) > 500:
            description = description[:497] + "..."
        
        text = f"🎬 <b>{title}</b> ({year})\n\n"
        if genres != '—':
            text += f"📂 <b>Жанры:</b> {genres}\n"
        if director != '—':
            text += f"🎥 <b>Режиссёр:</b> {director}\n"
        if actors != '—':
            text += f"👥 <b>Актёры:</b> {actors}\n"
        text += f"\n📝 <b>Описание:</b>\n{description}\n\n"
        text += f"<a href='{link}'>Кинопоиск</a>"
        
        # Создаем кнопку для добавления
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"confirm_add_film_{kp_id}"))
        
        # Отправляем описание
        try:
            msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
            # НЕ сохраняем ссылку в bot_messages, так как фильм еще не добавлен в базу
            bot.answer_callback_query(call.id, "Описание показано")
            logger.info(f"[SEARCH] Описание фильма {title} показано пользователю {user_id}")
        except Exception as e:
            logger.error(f"[SEARCH] Ошибка отправки описания: {e}", exc_info=True)
            bot.answer_callback_query(call.id, "❌ Ошибка отправки описания", show_alert=True)
    except Exception as e:
        logger.error(f"[SEARCH] Ошибка в handle_add_film_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_similar:"))
def handle_add_similar_callback(call):
    """Обработчик добавления похожего фильма"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        
        link = f"https://kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        if info:
            text = f"<b>{info['title']}</b> ({info['year']})\n"
            text += f"<b>Режиссёр:</b> {info['director']}\n"
            text += f"<b>Жанры:</b> {info['genres']}\n"
            if info.get('actors'):
                text += f"<b>В ролях:</b> {info['actors']}\n"
            text += f"\n{info['description'][:300]}..." if len(info['description']) > 300 else f"\n{info['description']}"
            text += f"\n\n<a href='{link}'>Кинопоиск</a>"
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_db:{kp_id}"))
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[ADD SIMILAR] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_to_db:"))
def handle_add_to_db_callback(call):
    """Обработчик добавления фильма в базу из похожих/продолжений"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        
        link = f"https://kinopoisk.ru/film/{kp_id}/"
        if add_and_announce(link, chat_id):
            bot.answer_callback_query(call.id, "✅ Фильм добавлен!")
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка добавления", show_alert=True)
    except Exception as e:
        logger.error(f"[ADD TO DB] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_add_film_"))
def handle_confirm_add_film_callback(call):
    """Обработчик подтверждения добавления фильма в базу"""
    try:
        kp_id = call.data.split("_")[-1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[SEARCH] Подтверждение добавления фильма kp_id={kp_id} от пользователя {user_id}")
        
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        # Проверяем, добавлен ли уже
        with db_lock:
            cursor.execute("SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
            existing = cursor.fetchone()
            if existing:
                title = existing.get('title') if isinstance(existing, dict) else existing[1]
                bot.answer_callback_query(call.id, f"Фильм '{title}' уже добавлен!", show_alert=False)
                return
        
        # Добавляем фильм
        if add_and_announce(link, chat_id):
            bot.answer_callback_query(call.id, "✅ Фильм добавлен!", show_alert=False)
            # Обновляем сообщение с описанием, убирая кнопку
            try:
                # Получаем текст сообщения
                message_text = call.message.text
                # Убираем кнопку
                bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
            except Exception as e:
                logger.warning(f"[SEARCH] Не удалось обновить сообщение: {e}")
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка добавления фильма", show_alert=True)
    except Exception as e:
        logger.error(f"[SEARCH] Ошибка в handle_confirm_add_film_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("search_"))
def handle_search_pagination_callback(call):
    """Обработчик пагинации результатов поиска"""
    try:
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Парсим callback_data: search_<query>_<page>
        parts = call.data.split("_", 2)  # Разделяем максимум на 3 части
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Ошибка формата")
            return
        
        query_encoded = parts[1]
        try:
            page = int(parts[2])
        except ValueError:
            bot.answer_callback_query(call.id, "Ошибка номера страницы")
            return
        
        # Декодируем запрос (заменяем подчеркивания обратно на пробелы)
        query = query_encoded.replace('_', ' ')
        
        logger.info(f"[SEARCH] Пагинация: запрос='{query}', страница={page}, пользователь={user_id}")
        
        films, total_pages = search_films(query, page)
        if not films:
            bot.answer_callback_query(call.id, "Нет результатов на этой странице")
            return
        
        # Формируем сообщение с новыми результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for film in films[:10]:  # Показываем максимум 10 результатов на странице
            # Пробуем разные варианты полей для совместимости с разными версиями API
            title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
            year = film.get('year') or film.get('releaseYear') or 'N/A'
            rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            if kp_id:
                # Ограничиваем длину текста кнопки
                button_text = f"{title} ({year})"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                results_text += f"• <b>{title}</b> ({year})"
                if rating != 'N/A':
                    results_text += f" ⭐ {rating}"
                results_text += "\n"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}"))
            else:
                logger.warning(f"[SEARCH PAGINATION] Фильм без ID: {film}")
        
        # Пагинация
        if total_pages > 1:
            pagination_row = []
            if page > 1:
                pagination_row.append(InlineKeyboardButton("◀️ Назад", callback_data=f"search_{query_encoded}_{page-1}"))
            pagination_row.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
            if page < total_pages:
                pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_{page+1}"))
            markup.row(*pagination_row)
        
        try:
            bot.edit_message_text(results_text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"[SEARCH] Ошибка при редактировании сообщения: {e}")
            bot.answer_callback_query(call.id, "Ошибка обновления")
    except Exception as e:
        logger.error(f"[SEARCH] Ошибка в handle_search_pagination_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "noop")
def handle_noop_callback(call):
    """Обработчик для плейсхолдера (кнопка с информацией о странице)"""
    bot.answer_callback_query(call.id)


# ==================== CALLBACK ОБРАБОТЧИКИ ДЛЯ /EDIT ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit:"))
def edit_callback_handler(call):
    """Обработчик callback для команды /edit"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    action = call.data.split(":")[1]
    
    if action == "plan":
        # Показываем список планов для редактирования
        with db_lock:
            cursor.execute('''
                SELECT p.id, m.title, p.plan_type, p.plan_datetime
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s
                ORDER BY p.plan_datetime
                LIMIT 20
            ''', (chat_id,))
            plans = cursor.fetchall()
        
        if not plans:
            bot.edit_message_text("Нет запланированных фильмов для редактирования.", chat_id, call.message.message_id)
            return
        
        user_tz = get_user_timezone_or_default(user_id)
        markup = InlineKeyboardMarkup(row_width=1)
        
        for row in plans:
            if isinstance(row, dict):
                plan_id = row.get('id')
                title = row.get('title')
                plan_type = row.get('plan_type')
                plan_dt_value = row.get('plan_datetime')
            else:
                plan_id = row[0]
                title = row[1]
                plan_type = row[2]
                plan_dt_value = row[3] if len(row) > 3 else None
            
            if plan_dt_value:
                if isinstance(plan_dt_value, datetime):
                    if plan_dt_value.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                    else:
                        dt = plan_dt_value.astimezone(user_tz)
                else:
                    dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
                
                date_str = dt.strftime('%d.%m %H:%M')
                type_text = "🎦" if plan_type == 'cinema' else "🏠"
                button_text = f"{title} | {date_str} {type_text}"
                
                if len(button_text) > 60:
                    short_title = title[:50] + "..."
                    button_text = f"{short_title} | {date_str} {type_text}"
                
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_plan:{plan_id}"))
        
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
        bot.edit_message_text("📅 <b>Выберите план для редактирования:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    
    elif action == "rating":
        # Показываем список фильмов с оценками для изменения
        with db_lock:
            cursor.execute('''
                SELECT m.id, m.title, m.year, r.rating
                FROM movies m
                JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                WHERE m.chat_id = %s AND r.user_id = %s
                ORDER BY m.title
                LIMIT 20
            ''', (chat_id, user_id))
            movies = cursor.fetchall()
        
        if not movies:
            bot.edit_message_text("Нет фильмов с вашими оценками для изменения.", chat_id, call.message.message_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        for row in movies:
            if isinstance(row, dict):
                film_id = row.get('id')
                title = row.get('title')
                year = row.get('year')
                rating = row.get('rating')
            else:
                film_id = row[0]
                title = row[1]
                year = row[2]
                rating = row[3] if len(row) > 3 else None
            
            button_text = f"{title} ({year or '—'}) ⭐ {rating}/10"
            if len(button_text) > 60:
                short_title = title[:45] + "..."
                button_text = f"{short_title} ({year or '—'}) ⭐ {rating}/10"
            
            markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_rating:{film_id}"))
        
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
        bot.edit_message_text("⭐ <b>Выберите фильм для изменения оценки:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    
    elif action == "delete_movie":
        # Удаление фильма из базы - запрашиваем ссылку или id
        user_edit_state[user_id] = {
            'action': 'delete_movie',
            'chat_id': chat_id
        }
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
        bot.edit_message_text(
            "🎬 <b>Удаление фильма из базы</b>\n\n"
            "Введите ссылку на фильм (kinopoisk.ru/film/...) или ID фильма для удаления.",
            chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
        bot.answer_callback_query(call.id, "Введите ссылку или ID")
    
    elif action.startswith("delete_"):
        # Перенаправляем на соответствующие обработчики clean
        clean_action = action.replace("delete_", "")
        bot.answer_callback_query(call.id, "Перенаправление...")
        # Вызываем соответствующий обработчик clean
        call.data = f"clean:{clean_action}"
        clean_action_choice(call)
    
    elif action == "cancel":
        bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
        bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_plan:"))
def edit_plan_callback(call):
    """Обработчик выбора плана для редактирования"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    # Получаем информацию о плане
    with db_lock:
        cursor.execute('''
            SELECT p.plan_type, p.plan_datetime, m.title
            FROM plans p
            JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
            WHERE p.id = %s AND p.chat_id = %s
        ''', (plan_id, chat_id))
        plan_row = cursor.fetchone()
    
    if not plan_row:
        bot.answer_callback_query(call.id, "План не найден")
        return
    
    if isinstance(plan_row, dict):
        plan_type = plan_row.get('plan_type')
        plan_dt_value = plan_row.get('plan_datetime')
        title = plan_row.get('title')
    else:
        plan_type = plan_row[0]
        plan_dt_value = plan_row[1]
        title = plan_row[2]
    
    user_tz = get_user_timezone_or_default(user_id)
    if plan_dt_value:
        if isinstance(plan_dt_value, datetime):
            if plan_dt_value.tzinfo is None:
                dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
            else:
                dt = plan_dt_value.astimezone(user_tz)
        else:
            dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
        date_str = dt.strftime('%d.%m.%Y %H:%M')
    else:
        date_str = "не указана"
    
    user_edit_state[user_id] = {
        'action': 'edit_plan',
        'plan_id': plan_id,
        'plan_type': plan_type
    }
    
    markup = InlineKeyboardMarkup(row_width=1)
    if plan_type == 'cinema':
        markup.add(InlineKeyboardButton("📅 Изменить дату/время", callback_data=f"edit_plan_datetime:{plan_id}"))
        markup.add(InlineKeyboardButton("🎟️ Загрузить билеты", callback_data=f"edit_plan_ticket:{plan_id}"))
    else:
        markup.add(InlineKeyboardButton("📅 Изменить дату/время", callback_data=f"edit_plan_datetime:{plan_id}"))
        markup.add(InlineKeyboardButton("🎦 Переключить в 'в кино'", callback_data=f"edit_plan_switch:{plan_id}"))
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
    
    text = f"✏️ <b>Редактирование плана:</b>\n\n"
    text += f"<b>{title}</b>\n"
    text += f"Тип: {'🎦 в кино' if plan_type == 'cinema' else '🏠 дома'}\n"
    text += f"Дата/время: {date_str}\n\n"
    text += f"Что вы хотите изменить?"
    
    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_rating:"))
def edit_rating_callback(call):
    """Обработчик изменения оценки"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    film_id = int(call.data.split(":")[1])
    
    user_edit_state[user_id] = {
        'action': 'edit_rating',
        'film_id': film_id
    }
    
    bot.edit_message_text(
        "⭐ <b>Введите новую оценку (1-10):</b>\n\n"
        "Ответьте на это сообщение числом от 1 до 10.",
        chat_id, call.message.message_id, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id)


# ==================== CALLBACK ОБРАБОТЧИКИ ДЛЯ /TICKET ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket:"))
def ticket_callback_handler(call):
    """Обработчик callback для команды /ticket"""
    user_id = call.from_user.id
    action = call.data.split(":")[1]
    
    if action == "cancel":
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        bot.edit_message_text("❌ Операция отменена.", call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_session:"))
def ticket_session_callback(call):
    """Обработчик выбора сеанса для работы с билетами"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    plan_id = int(parts[1])
    file_id_from_callback = parts[2] if len(parts) > 2 else None
    
    logger.info(f"[TICKET SESSION] Пользователь {user_id} выбрал сеанс plan_id={plan_id}, file_id из callback={file_id_from_callback}")
    
    # Проверяем, есть ли file_id в состоянии пользователя (если был отправлен файл с /ticket)
    state = user_ticket_state.get(user_id, {})
    file_id_from_state = state.get('file_id')
    
    # Используем file_id из callback, если есть, иначе из состояния
    file_id = file_id_from_callback or file_id_from_state
    
    logger.info(f"[TICKET SESSION] file_id из состояния={file_id_from_state}, итоговый file_id={file_id}")
    
    # Проверяем, есть ли уже билеты для этого сеанса и есть ли время сеанса
    with db_lock:
        cursor.execute('SELECT ticket_file_id, plan_datetime FROM plans WHERE id = %s', (plan_id,))
        plan_row = cursor.fetchone()
    
    ticket_file_id = None
    if plan_row:
        if isinstance(plan_row, dict):
            ticket_file_id = plan_row.get('ticket_file_id')
            plan_dt = plan_row.get('plan_datetime')
        else:
            ticket_file_id = plan_row[0] if len(plan_row) > 0 else None
            plan_dt = plan_row[1] if len(plan_row) > 1 else None
    
    logger.info(f"[TICKET SESSION] Билеты в БД: {ticket_file_id is not None}")
    
    # Проверяем, есть ли время у сеанса
    has_time = False
    plan_dt = None
    if plan_row:
        plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else (plan_row[1] if len(plan_row) > 1 else None)
        if plan_dt:
            has_time = True
    
    logger.info(f"[TICKET SESSION] У сеанса есть время: {has_time}")
    
    if ticket_file_id and not file_id:
        # Показываем существующие билеты
        existing_file_id = ticket_file_id
        logger.info(f"[TICKET SESSION] Отправляем существующие билеты, file_id={existing_file_id}")
        if existing_file_id:
            try:
                bot.send_photo(chat_id, existing_file_id, caption="🎟️ Ваши билеты на этот сеанс")
                bot.answer_callback_query(call.id, "Билеты отправлены")
                logger.info(f"[TICKET SESSION] Билеты успешно отправлены как фото")
            except Exception as e:
                logger.warning(f"[TICKET SESSION] Ошибка отправки фото, пробуем как документ: {e}")
                # Если фото не найдено, пытаемся отправить как документ
                try:
                    bot.send_document(chat_id, existing_file_id, caption="🎟️ Ваши билеты на этот сеанс")
                    bot.answer_callback_query(call.id, "Билеты отправлены")
                    logger.info(f"[TICKET SESSION] Билеты успешно отправлены как документ")
                except Exception as e2:
                    logger.error(f"[TICKET SESSION] Ошибка отправки билетов: {e2}")
                    bot.answer_callback_query(call.id, "Ошибка отправки билетов", show_alert=True)
                    bot.send_message(chat_id, "❌ Не удалось отправить билеты. Возможно, файл был удален.")
            
            # Создаем кнопки в зависимости от наличия времени
            markup = InlineKeyboardMarkup()
            if not has_time:
                # Если нет времени, добавляем обе кнопки
                markup.add(InlineKeyboardButton("⏰ Указать точное время сеанса", callback_data=f"ticket_time:{plan_id}"))
                markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"ticket_add_more:{plan_id}"))
                bot.send_message(chat_id, "💡 Что хотите сделать?", reply_markup=markup)
            else:
                # Если время есть, только кнопка добавления билетов
                markup.add(InlineKeyboardButton("➕ Добавить еще билет", callback_data=f"ticket_add_more:{plan_id}"))
                bot.send_message(chat_id, "💡 Хотите добавить еще билеты к этому сеансу?", reply_markup=markup)
        else:
            logger.warning(f"[TICKET SESSION] file_id в БД пустой")
            bot.answer_callback_query(call.id, "Билеты не найдены", show_alert=True)
        return
    
    if file_id:
        # Добавляем билеты к существующему сеансу
        logger.info(f"[TICKET SESSION] Добавляем билеты к сеансу plan_id={plan_id}, file_id={file_id}")
        user_ticket_state[user_id] = {
            'step': 'add_ticket',
            'plan_id': plan_id,
            'file_id': file_id,
            'chat_id': chat_id
        }
        
        # Сохраняем билет в БД
        with db_lock:
            # Удаляем старые билеты для этого плана
            cursor.execute('DELETE FROM tickets WHERE plan_id = %s', (plan_id,))
            # Добавляем новые
            cursor.execute('INSERT INTO tickets (plan_id, chat_id, file_id) VALUES (%s, %s, %s)',
                         (plan_id, chat_id, file_id))
            conn.commit()
        logger.info(f"[TICKET SESSION] Билеты сохранены в БД для plan_id={plan_id}")
        
        # Проверяем, есть ли время у сеанса
        with db_lock:
            cursor.execute('SELECT plan_datetime FROM plans WHERE id = %s', (plan_id,))
            plan_row = cursor.fetchone()
        
        has_time = False
        if plan_row:
            plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[0]
            if plan_dt:
                has_time = True
        
        markup = InlineKeyboardMarkup()
        if not has_time:
            # Если нет времени, добавляем обе кнопки
            markup.add(InlineKeyboardButton("⏰ Указать точное время сеанса", callback_data=f"ticket_time:{plan_id}"))
            markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"ticket_add_more:{plan_id}"))
        else:
            # Если время есть, только кнопка указания времени (на случай изменения)
            markup.add(InlineKeyboardButton("⏰ Указать точное время сеанса", callback_data=f"ticket_time:{plan_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        if not has_time:
            bot.edit_message_text(
                "✅ <b>Билеты успешно добавлены!</b>\n\n"
                "Что хотите сделать дальше?",
                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
            )
        else:
            bot.edit_message_text(
                "✅ <b>Билеты успешно добавлены!</b>\n\n"
                "Если нужно, укажите точное время сеанса:",
                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
            )
        bot.answer_callback_query(call.id, "Билеты добавлены")
        logger.info(f"[TICKET SESSION] Сообщение об успешном добавлении отправлено пользователю {user_id}")
    else:
        # Если file_id не передан и билетов нет в БД, предлагаем загрузить билеты
        logger.info(f"[TICKET SESSION] file_id не найден, билетов нет в БД, предлагаем загрузить билеты")
        user_ticket_state[user_id] = {
            'step': 'waiting_ticket_file',
            'plan_id': plan_id,
            'chat_id': chat_id
        }
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        bot.edit_message_text(
            "🎟️ <b>Билеты не найдены</b>\n\n"
            "Загрузите билеты для этого сеанса:\n"
            "Отправьте фото или файл с билетами в следующем сообщении.",
            chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
        bot.answer_callback_query(call.id, "Загрузите билеты")


@bot.callback_query_handler(func=lambda call: call.data.startswith("add_ticket:"))
def add_ticket_from_plan_callback(call):
    """Обработчик кнопки 'Добавить билеты' из подтверждения /plan"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    user_ticket_state[user_id] = {
        'step': 'waiting_ticket_file',
        'plan_id': plan_id,
        'chat_id': chat_id
    }
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    bot.answer_callback_query(call.id, "Загрузите билеты в чат")
    bot.send_message(
        chat_id,
        "🎟️ <b>Загрузите билеты в чат</b>\n\n"
        "Отправьте фото или файл с билетами в следующем сообщении.",
        reply_markup=markup, parse_mode='HTML'
    )


@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_time:"))
def ticket_time_callback(call):
    """Обработчик запроса времени сеанса"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    user_ticket_state[user_id] = {
        'step': 'waiting_session_time',
        'plan_id': plan_id,
        'chat_id': chat_id
    }
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    bot.edit_message_text(
        "⏰ <b>Уточните дату и время сеанса:</b>\n\n"
        "Ответьте на это сообщение в формате:\n"
        "• 15 января 10:30\n"
        "• 17.01 15:20\n"
        "• 10.05.2025 21:40",
        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_new"))
def ticket_new_session_callback(call):
    """Обработчик создания нового сеанса с билетами"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    parts = call.data.split(":")
    file_id = parts[1] if len(parts) > 1 else None
    
    logger.info(f"[TICKET NEW CALLBACK] Пользователь {user_id} нажал 'Добавить новый сеанс', file_id={file_id}")
    
    user_ticket_state[user_id] = {
        'step': 'waiting_new_session',
        'file_id': file_id,
        'chat_id': chat_id
    }
    
    logger.info(f"[TICKET NEW CALLBACK] Установлено состояние: step=waiting_new_session, file_id={file_id}, chat_id={chat_id}")
    logger.info(f"[TICKET NEW CALLBACK] Текущее состояние пользователя {user_id}: {user_ticket_state.get(user_id)}")
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    bot.edit_message_text(
        "➕ <b>Пришлите фильм и дату сеанса</b>\n\n"
        "Формат:\n"
        "• https://www.kinopoisk.ru/film/81682/ 17 января 20:30\n"
        "• https://www.kinopoisk.ru/film/81682/ 17.01 15:15\n"
        "• 81682 17 января 12 12",
        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id)
    logger.info(f"[TICKET NEW CALLBACK] Сообщение отправлено пользователю {user_id}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_add_more:"))
def ticket_add_more_callback(call):
    """Обработчик кнопки 'Добавить еще билет'"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    logger.info(f"[TICKET ADD MORE] Пользователь {user_id} хочет добавить еще билеты к plan_id={plan_id}")
    
    user_ticket_state[user_id] = {
        'step': 'waiting_ticket_file',
        'plan_id': plan_id,
        'chat_id': chat_id
    }
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    bot.edit_message_text(
        "🎟️ <b>Загрузите дополнительные билеты</b>\n\n"
        "Отправьте фото или файл с билетами в следующем сообщении.",
        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id, "Загрузите билеты")


# ==================== ДОПОЛНИТЕЛЬНЫЕ ОБРАБОТЧИКИ ДЛЯ РЕДАКТИРОВАНИЯ ПЛАНОВ ====================
@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_plan_datetime:"))
def edit_plan_datetime_callback(call):
    """Обработчик изменения даты/времени плана"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    user_edit_state[user_id] = {
        'action': 'edit_plan_datetime',
        'plan_id': plan_id
    }
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
    
    bot.edit_message_text(
        "📅 <b>Введите новую дату и время:</b>\n\n"
        "Формат:\n"
        "• 15 января 10:30\n"
        "• 17.01 15:20\n"
        "• 10.05.2025 21:40\n"
        "• завтра\n"
        "• в субботу 15:00",
        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_plan_ticket:"))
def edit_plan_ticket_callback(call):
    """Обработчик загрузки билетов через /edit"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    user_ticket_state[user_id] = {
        'step': 'waiting_ticket_file',
        'plan_id': plan_id,
        'chat_id': chat_id
    }
    
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
    
    bot.edit_message_text(
        "🎟️ <b>Пришлите билеты скриншотом или вложением</b>\n\n"
        "Отправьте фото или файл с билетами в следующем сообщении.",
        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id)


@bot.callback_query_handler(func=lambda call: call.data.startswith("edit_plan_switch:"))
def edit_plan_switch_callback(call):
    """Обработчик переключения типа плана (дома <-> в кино)"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    plan_id = int(call.data.split(":")[1])
    
    # Получаем текущий тип плана
    with db_lock:
        cursor.execute('SELECT plan_type FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
        plan_row = cursor.fetchone()
        
        if not plan_row:
            bot.answer_callback_query(call.id, "План не найден", show_alert=True)
            return
        
        current_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[0]
        new_type = 'cinema' if current_type == 'home' else 'home'
        
        # Обновляем тип плана
        cursor.execute('UPDATE plans SET plan_type = %s WHERE id = %s', (new_type, plan_id))
        conn.commit()
    
    type_text = "в кино" if new_type == 'cinema' else "дома"
    bot.edit_message_text(
        f"✅ Тип плана изменен на: <b>{type_text}</b>",
        chat_id, call.message.message_id, parse_mode='HTML'
    )
    bot.answer_callback_query(call.id, f"Изменено на {type_text}")


# Обработка текстовых сообщений для редактирования даты/времени плана
@bot.message_handler(content_types=['text'], func=lambda message: message.from_user.id in user_edit_state and user_edit_state.get(message.from_user.id, {}).get('action') == 'edit_plan_datetime')
def handle_edit_plan_datetime_text(message):
    """Обработчик текстового сообщения для изменения даты/времени плана"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    state = user_edit_state.get(user_id, {})
    plan_id = state.get('plan_id')
    
    if not plan_id:
        bot.reply_to(message, "❌ Ошибка: план не найден.")
        if user_id in user_edit_state:
            del user_edit_state[user_id]
        return
    
    user_tz = get_user_timezone_or_default(user_id)
    
    # Используем функцию process_plan для парсинга даты
    # Но сначала нужно получить информацию о фильме
    with db_lock:
        cursor.execute('''
            SELECT m.link, p.plan_type
            FROM plans p
            JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
            WHERE p.id = %s AND p.chat_id = %s
        ''', (plan_id, chat_id))
        plan_row = cursor.fetchone()
    
    if not plan_row:
        bot.reply_to(message, "❌ План не найден.")
        if user_id in user_edit_state:
            del user_edit_state[user_id]
        return
    
    if isinstance(plan_row, dict):
        link = plan_row.get('link')
        plan_type = plan_row.get('plan_type')
    else:
        link = plan_row[0]
        plan_type = plan_row[1]
    
    # Парсим новую дату/время используя process_plan
    # Но нам нужно только получить datetime, не создавать новый план
    # Используем parse_session_time для простых форматов, или process_plan логику
    from moviebot import process_plan
    # Временно создаем новый план для парсинга, затем удалим его
    # Лучше использовать прямую логику парсинга из process_plan
    
    # Используем логику парсинга из process_plan, но без создания нового плана
    # Сначала пробуем parse_session_time для форматов с временем
    session_dt = parse_session_time(text, user_tz)
    
    if not session_dt:
        # Если parse_session_time не сработал, используем process_plan для парсинга
        # Создаем временный план для парсинга
        temp_result = process_plan(user_id, chat_id, link, plan_type, text)
        if temp_result == True:
            # Получаем новый plan_datetime из последнего созданного плана
            with db_lock:
                cursor.execute('SELECT plan_datetime FROM plans WHERE chat_id = %s AND user_id = %s ORDER BY id DESC LIMIT 1', (chat_id, user_id))
                new_plan_row = cursor.fetchone()
                if new_plan_row:
                    session_dt = new_plan_row.get('plan_datetime') if isinstance(new_plan_row, dict) else new_plan_row[0]
                    if isinstance(session_dt, datetime):
                        if session_dt.tzinfo is None:
                            session_dt = pytz.utc.localize(session_dt).astimezone(user_tz)
                        else:
                            session_dt = session_dt.astimezone(user_tz)
                    # Удаляем временный план
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s AND user_id = %s ORDER BY id DESC LIMIT 1', (chat_id, user_id))
                    conn.commit()
    
    if session_dt:
        # Обновляем план
        if isinstance(session_dt, datetime):
            session_utc = session_dt.astimezone(pytz.utc) if session_dt.tzinfo else pytz.utc.localize(session_dt)
        else:
            session_utc = session_dt
        
        with db_lock:
            cursor.execute('UPDATE plans SET plan_datetime = %s WHERE id = %s', (session_utc, plan_id))
            conn.commit()
        
        tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
        if isinstance(session_dt, datetime):
            date_str = session_dt.strftime('%d.%m.%Y %H:%M')
        else:
            date_str = str(session_dt)
        bot.reply_to(message, f"✅ Дата и время плана обновлены: {date_str} {tz_name}")
        if user_id in user_edit_state:
            del user_edit_state[user_id]
    else:
        bot.reply_to(message, "❌ Не удалось распознать дату/время. Попробуйте еще раз.")

logger.info("[DEBUG] Перед созданием Flask app")
logger.info(f"[DEBUG] sys.argv={sys.argv}, sys.executable={sys.executable}")

# Flask app для webhook
app = Flask(__name__)

logger.info("[DEBUG] Flask app создан")

@app.route('/webhook', methods=['POST'])
def webhook():
    logger.info("=" * 80)
    logger.info("[WEBHOOK] ===== ПОЛУЧЕН ЗАПРОС =====")
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        logger.info(f"[WEBHOOK] Размер JSON: {len(json_string)} байт")
        # Проверяем, есть ли web_app_data в сыром JSON
        if 'web_app_data' in json_string.lower():
            logger.info("🔍 [WEBHOOK] ⚠️⚠️⚠️ В JSON ЕСТЬ 'web_app_data'! ⚠️⚠️⚠️")
        # Логируем первые 1000 символов JSON для отладки
        logger.info(f"[WEBHOOK] JSON (первые 1000 символов): {json_string[:1000]}")
        update = telebot.types.Update.de_json(json_string)
        logger.info(f"[WEBHOOK] Тип update: {type(update)}")
        logger.info(f"[WEBHOOK] Update имеет message: {hasattr(update, 'message') and update.message is not None}")
        
        # Логируем информацию о реплае для отладки
        if update.message:
            logger.info(f"[WEBHOOK] Update.message.content_type={update.message.content_type if hasattr(update.message, 'content_type') else 'НЕТ'}")
            logger.info(f"[WEBHOOK] Update.message.text='{update.message.text[:200] if update.message.text else None}'")
            logger.info(f"[WEBHOOK] Update.message.from_user.id={update.message.from_user.id if update.message.from_user else None}")
            
            # ПРОВЕРКА WEB_APP_DATA (приоритетная проверка!)
            logger.info(f"[WEBHOOK] Проверка web_app_data: hasattr={hasattr(update.message, 'web_app_data')}")
            if hasattr(update.message, 'web_app_data') and update.message.web_app_data:
                logger.info(f"[WEBHOOK] ✅✅✅ WEB_APP_DATA НАЙДЕН! Данные: {update.message.web_app_data.data}")
                # ВАЖНО: Вызываем обработчик web_app_data напрямую ПЕРВЫМ, до обработки обычных команд
                logger.info(f"[WEBHOOK] Вызываем handle_web_app_data напрямую")
                try:
                    handle_web_app_data(update.message)
                    logger.info(f"[WEBHOOK] handle_web_app_data завершен успешно")
                    return ''  # Не обрабатываем дальше, так как уже обработали
                except Exception as web_app_error:
                    logger.error(f"[WEBHOOK] Ошибка в handle_web_app_data: {web_app_error}", exc_info=True)
                    # Продолжаем обычную обработку в случае ошибки
            elif hasattr(update.message, 'web_app_data'):
                logger.info(f"[WEBHOOK] ⚠️ web_app_data существует, но равен None (это обычное сообщение)")
            else:
                logger.info(f"[WEBHOOK] web_app_data отсутствует (это обычное сообщение)")
            
            logger.info(f"[WEBHOOK] Update.message.entities={update.message.entities if update.message.entities else None}")
            if update.message.entities:
                for entity in update.message.entities:
                    logger.info(f"[WEBHOOK] Entity type={entity.type}, offset={entity.offset}, length={entity.length}")
            if update.message.reply_to_message:
                logger.info(f"[WEBHOOK] Update содержит reply_to_message: message_id={update.message.reply_to_message.message_id}")
            else:
                logger.info(f"[WEBHOOK] Update.message есть, но reply_to_message отсутствует")
        
            # Проверяем, является ли это командой
            if update.message.text and update.message.text.startswith('/'):
                logger.info(f"[WEBHOOK] Обнаружена команда: {update.message.text}")
                # Проверяем, есть ли обработчик для этой команды
                command = update.message.text.split()[0] if update.message.text else None
                logger.info(f"[WEBHOOK] Команда для обработки: {command}")
                
                # Если команда содержит @botname, убираем его для правильной обработки
                if '@' in command:
                    command_base = command.split('@')[0]
                    logger.info(f"[WEBHOOK] Команда с @botname, базовая команда: {command_base}")
                    # Обновляем текст сообщения, убирая @botname
                    update.message.text = update.message.text.replace(command, command_base, 1)
                    logger.info(f"[WEBHOOK] Обновленный текст сообщения: {update.message.text}")
        
        logger.info(f"[WEBHOOK] Вызываем bot.process_new_updates")
        try:
            bot.process_new_updates([update])
            logger.info(f"[WEBHOOK] bot.process_new_updates завершен успешно")
        except Exception as e:
            logger.error(f"[WEBHOOK] Ошибка в bot.process_new_updates: {e}", exc_info=True)
        return ''
    else:
        abort(403)

@app.route('/', methods=['GET'])
def root():
    logger.info("[ROOT] Root запрос получен")
    return jsonify({'status': 'ok', 'service': 'moviebot'}), 200

@app.route('/health', methods=['GET'])
def health():
    logger.info("[HEALTH] Health check запрос получен")
    return jsonify({'status': 'ok', 'bot': 'running'}), 200

# Логируем зарегистрированные маршруты после их определения
logger.info(f"[DEBUG] Flask маршруты зарегистрированы: {[str(rule) for rule in app.url_map.iter_rules()]}")

logger.info("[DEBUG] Перед определением IS_RENDER")

try:
    # Определяем, где запускается бот: на Render, Railway или локально
    # Проверяем несколько признаков облачных окружений
    RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL')
    RENDER_SERVICE_ID = os.getenv('RENDER_SERVICE_ID')
    RENDER = os.getenv('RENDER')
    RAILWAY_ENVIRONMENT = os.getenv('RAILWAY_ENVIRONMENT')
    RAILWAY_PUBLIC_DOMAIN = os.getenv('RAILWAY_PUBLIC_DOMAIN')
    RAILWAY_PRIVATE_DOMAIN = os.getenv('RAILWAY_PRIVATE_DOMAIN')
    PORT = os.getenv('PORT')  # На Render и Railway всегда есть PORT

    logger.info(f"[DEBUG] Переменные окружения: PORT={PORT}, RENDER_EXTERNAL_URL={RENDER_EXTERNAL_URL}, RAILWAY_ENVIRONMENT={RAILWAY_ENVIRONMENT}, RAILWAY_PUBLIC_DOMAIN={RAILWAY_PUBLIC_DOMAIN}, RAILWAY_PRIVATE_DOMAIN={RAILWAY_PRIVATE_DOMAIN}")
except Exception as e:
    logger.error(f"[DEBUG] Ошибка при получении переменных окружения: {e}", exc_info=True)
    raise

# Дополнительная проверка: путь выполнения (Render использует /opt/render/)
IS_RENDER_PATH = '/opt/render' in sys.executable or '/opt/render' in str(sys.path)
logger.info(f"[DEBUG] IS_RENDER_PATH={IS_RENDER_PATH}, sys.executable={sys.executable}")

# Явная переменная для отключения polling (можно установить в env vars)
USE_POLLING = os.getenv('USE_POLLING', '').lower() in ('true', '1', 'yes')

# ВАЖНО: Если есть PORT или признаки облачного окружения, это production
# Polling НИКОГДА не должен запускаться в production, если не установлена явно USE_POLLING=True
IS_PRODUCTION = bool(PORT or RENDER_EXTERNAL_URL or RENDER_SERVICE_ID or RENDER or IS_RENDER_PATH or RAILWAY_ENVIRONMENT or RAILWAY_PUBLIC_DOMAIN)
IS_RENDER = IS_PRODUCTION  # Для обратной совместимости

logger.info(f"[DEBUG] IS_PRODUCTION={IS_PRODUCTION}, IS_RENDER={IS_RENDER}, USE_POLLING={USE_POLLING}")

# Если это production, принудительно отключаем polling (если не установлена явно USE_POLLING)
if IS_PRODUCTION and not USE_POLLING:
    IS_PRODUCTION = True  # Гарантируем, что это production
    IS_RENDER = True  # Для обратной совместимости
    logger.info(f"[DEBUG] Определение окружения: PORT={PORT}, RENDER_EXTERNAL_URL={bool(RENDER_EXTERNAL_URL)}, RAILWAY_ENVIRONMENT={bool(RAILWAY_ENVIRONMENT)}, IS_PRODUCTION={IS_PRODUCTION}")
else:
    logger.info(f"[DEBUG] Определение окружения: PORT={PORT}, RENDER_EXTERNAL_URL={bool(RENDER_EXTERNAL_URL)}, RAILWAY_ENVIRONMENT={bool(RAILWAY_ENVIRONMENT)}, IS_PRODUCTION={IS_PRODUCTION}, USE_POLLING={USE_POLLING}")

if IS_PRODUCTION:
    # Определяем платформу и URL для webhook
    # Railway может использовать RAILWAY_PUBLIC_DOMAIN или RAILWAY_PRIVATE_DOMAIN
    RAILWAY_PRIVATE_DOMAIN = os.getenv('RAILWAY_PRIVATE_DOMAIN')
    
    logger.info(f"[DEBUG] Railway домены: PUBLIC={RAILWAY_PUBLIC_DOMAIN}, PRIVATE={RAILWAY_PRIVATE_DOMAIN}")
    
    if RAILWAY_PUBLIC_DOMAIN:
        # Railway с публичным доменом
        webhook_base_url = f"https://{RAILWAY_PUBLIC_DOMAIN}"
        logger.info("=== RAILWAY MODE: WEBHOOK + FLASK SERVER ===")
    elif RAILWAY_PRIVATE_DOMAIN:
        # Railway с приватным доменом (можно использовать для тестирования)
        webhook_base_url = f"https://{RAILWAY_PRIVATE_DOMAIN}"
        logger.info("=== RAILWAY MODE (PRIVATE DOMAIN): WEBHOOK + FLASK SERVER ===")
        logger.warning("[DEBUG] Используется приватный домен Railway. Для production лучше использовать публичный домен.")
    elif RENDER_EXTERNAL_URL:
        # Render
        webhook_base_url = RENDER_EXTERNAL_URL
        logger.info("=== RENDER MODE: WEBHOOK + FLASK SERVER ===")
    else:
        # Другая облачная платформа с PORT
        webhook_base_url = None
        logger.info("=== PRODUCTION MODE: WEBHOOK + FLASK SERVER ===")
        logger.warning("[DEBUG] Webhook URL не определён. Убедитесь, что RAILWAY_PUBLIC_DOMAIN установлен или включите публичный домен в Railway.")
    
    # Очистка и установка webhook
    try:
        bot.remove_webhook()
        time.sleep(2)  # пауза, чтобы Telegram обработал
        logger.info("Старый webhook удалён")
    except Exception as e:
        logger.warning(f"Ошибка при remove_webhook: {e}")
    
    if webhook_base_url:
        webhook_url = webhook_base_url + '/webhook'
        allowed_updates = [
            "message",  # web_app_data приходит внутри message
            "edited_message",
            "callback_query",
            "message_reaction",
            "message_reaction_count",
            "chat_member",
            "my_chat_member"
        ]
        logger.info(f"Устанавливаем webhook с allowed_updates: {allowed_updates}")
        try:
            bot.set_webhook(url=webhook_url, allowed_updates=allowed_updates)
            logger.info(f"Webhook успешно установлен: {webhook_url}")
            logger.info(f"allowed_updates включает: {', '.join(allowed_updates)}")
        except Exception as e:
            logger.error(f"ОШИБКА при set_webhook: {e}")
    else:
        logger.warning("Webhook URL не определён! Установите RENDER_EXTERNAL_URL или RAILWAY_PUBLIC_DOMAIN")

    # КЛЮЧЕВОЕ: запускаем Flask сервер
    port = int(os.getenv('PORT', 10000))
    logger.info(f"Запускаем Flask сервер на 0.0.0.0:{port}")
    
    # Это важно — чтобы Render сразу увидел порт
    import socket
    logger.info(f"Текущий хост: {socket.gethostname()}")
    
    app.run(host='0.0.0.0', port=port, debug=False, use_reloader=False)
else:
    # Локальный запуск - используем polling (только если IS_PRODUCTION=False)
    logger.info("Локальное окружение - будет использован polling")
    try:
        bot.remove_webhook()
        logger.info("Старые webhook очищены")
    except Exception as e:
        logger.warning(f"Не удалось очистить webhook: {e}")
    
    # Запускаем polling независимо от того, как выполняется код
    # (это важно для случаев, когда скрипт импортируется, но нужно запустить бота)
    logger.info("Локальный запуск: используется polling")
    try:
        bot.infinity_polling()
    except KeyboardInterrupt:
        logger.info("Получен сигнал прерывания, останавливаем бота...")
    except Exception as e:
        logger.error(f"Ошибка при запуске polling: {e}", exc_info=True)
        raise
