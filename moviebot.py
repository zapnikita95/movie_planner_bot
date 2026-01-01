from dotenv import load_dotenv
load_dotenv()  # загружает .env

import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton, BotCommand
import os
import random
import re
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
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

TOKEN = os.getenv('BOT_TOKEN')
if not TOKEN:
    logger.error("BOT_TOKEN не задан! Бот не может работать.")
    raise ValueError("Добавьте BOT_TOKEN в environment variables")

bot = telebot.TeleBot(TOKEN)
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
list_messages = {}  # message_id: chat_id (храним сообщения /list для обработки ответов)
# Состояния настроек
user_settings_state = {}  # user_id: {'waiting_emoji': bool}
# Состояния очистки
user_clean_state = {}  # user_id: {'action': str, 'target': str}
clean_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}
# Состояния очистки
user_clean_state = {}  # user_id: {'action': str, 'target': str}
clean_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set}
plans_tz = pytz.timezone('Europe/Moscow')
months_map = {
    'января': 1, 'февраля': 2, 'марта': 3, 'апреля': 4, 'мая': 5, 'июня': 6,
    'июля': 7, 'августа': 8, 'сентября': 9, 'октября': 10, 'ноября': 11, 'декабря': 12
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
    BotCommand("plan", "Запланировать просмотр дома или в кино"),
    BotCommand("schedule", "Список запланированных просмотров"),
    BotCommand("total", "Статистика: фильмы, жанры, режиссёры, актёры и оценки"),
    BotCommand("rate", "Оценить просмотренные фильмы"),
    BotCommand("settings", "Настроить эмодзи просмотра"),
    BotCommand("clean", "Очистить базу данных (чат или данные о просмотрах)"),
    BotCommand("help", "Помощь по командам")
]
bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeAllGroupChats())
bot.set_my_commands(commands, scope=telebot.types.BotCommandScopeDefault())

# БД
DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    logger.error("DATABASE_URL не задан! Бот не может подключиться к БД.")
    raise ValueError("Добавьте DATABASE_URL в Render environment variables")

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
cursor.execute('INSERT INTO settings (chat_id, key, value) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING', (-1, "watched_emoji", "✅"))
cursor.execute('''
    CREATE TABLE IF NOT EXISTS plans (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        plan_type TEXT,
        plan_datetime TIMESTAMP WITH TIME ZONE,
        user_id BIGINT
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
    CREATE TABLE IF NOT EXISTS ratings (
        id SERIAL PRIMARY KEY,
        chat_id BIGINT,
        film_id INTEGER,
        user_id BIGINT,
        rating INTEGER CHECK(rating BETWEEN 1 AND 10),
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

# Ключевой блок: очистка дубликатов и создание уникального индекса
try:
    # Удаляем старые индексы и constraints, если они существуют
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

conn.commit()

def get_watched_emoji(chat_id):
    """Возвращает строку с эмодзи для отметки просмотренных (может быть несколько) для конкретного чата"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                return value
        # Дефолт, если не настроено
        return "✅"

def get_watched_emojis():
    """Возвращает глобальные эмодзи для отметки просмотренных (chat_id=-1)"""
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE chat_id = -1 AND key = 'watched_emoji'")
        row = cursor.fetchone()
        if row:
            value = row.get('value') if isinstance(row, dict) else row[0]
            if value:
                return value
        # Дефолт, если не настроено
        return "✅"

def is_watched_emoji(reaction_emoji, chat_id):
    """Проверяет, является ли реакция одним из сохранённых эмодзи для просмотра"""
    watched_emojis = get_watched_emoji(chat_id)
    # Если сохранено несколько эмодзи, проверяем каждый
    return reaction_emoji in watched_emojis

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
    # Дефолт
    return {'emoji': ['✅'], 'custom': []}

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

# Настройка периодического вывода статистики
scheduler.add_job(hourly_stats, 'interval', hours=1, id='hourly_stats')

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

def send_plan_notification(chat_id, title, link, plan_type):
    if plan_type == 'home':
        text = f"Привет! Вы планировали посмотреть дома фильм <b>{title}</b>: {link}"
    else:
        text = f"Привет! Вы планировали сходить в кино на <b>{title}</b>: {link}"
    bot.send_message(chat_id, text, parse_mode='HTML')

# Получение информации о фильме через прямой запрос к API
def extract_movie_info(link):
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
    if not match:
        logger.warning(f"Не распознана ссылка: {link}")
        return None
    kp_id = match.group(2)

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
            'description': description
        }
    except Exception as e:
        logger.error(f"Ошибка получения данных для {kp_id}: {e}")
        return None

# Добавление и анонс
def add_and_announce(link, chat_id):
    info = extract_movie_info(link)
    if not info:
        logger.warning(f"Не удалось извлечь информацию о фильме: {link}")
        return False

    # Проверяем, существует ли уже фильм в этом чате по kp_id (не по ссылке, так как ссылки могут отличаться)
    kp_id = info.get('kp_id')
    with db_lock:
        cursor.execute('SELECT id, title, watched, rating FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        existing = cursor.fetchone()
    
    if existing:
        # RealDictCursor возвращает словари, но поддерживает доступ по индексу
        film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
        existing_title = existing.get('title') if isinstance(existing, dict) else existing[1]
        watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
        
        # Фильм уже есть в базе
        text = f"🎞️ <b>Уже добавлено ранее в базу!</b>\n\n"
        text += f"<b>{existing_title}</b>\n"
        
        # Если фильм просмотрен, рассчитываем среднее из ratings (внутри db_lock)
        if watched:
            with db_lock:
                cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                avg_result = cursor.fetchone()
                avg = avg_result[0] if avg_result and avg_result[0] else None
            
            text += f"\n✅ <b>Просмотрено</b>\n"
            if avg:
                text += f"⭐ <b>Средняя оценка: {avg:.1f}/10</b>\n"
            else:
                text += f"⭐ <b>Оценка не указана</b>\n"
        else:
            text += f"\n⏳ <b>Ещё не просмотрено</b>\n"
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        try:
            bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
            logger.info(f"Сообщение отправлено: фильм уже в базе - {existing_title}")
        except Exception as e:
            logger.error(f"Ошибка при отправке сообщения (фильм уже в базе): {e}", exc_info=True)
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
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
            exists_before = cursor.fetchone() is not None
            
            if exists_before:
                logger.info(f"Фильм с kp_id={info['kp_id']} уже существует в базе, пропускаем вставку")
                inserted = False
            else:
                try:
                    cursor.execute('''
                        INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                    ''', (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors']))
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
    
    logger.info(f"Результат вставки: inserted={inserted}, title={info['title']}")
    
    if inserted:
        # Только если реально добавили в БД — отправляем сообщение и сохраняем message_id
        text = f"🎬 <b>Добавлено в базу!</b>\n\n"
        text += f"<b>{info['title']}</b> ({info['year'] or '—'})\n"
        text += f"<i>Режиссёр:</i> {info['director']}\n"
        text += f"<i>Жанры:</i> {info['genres']}\n"
        text += f"<i>В ролях:</i> {info['actors']}\n\n"
        text += f"<i>Кратко:</i> {info['description']}\n\n"
        text += f"<a href='{link}'>Кинопоиск</a>"
        
        try:
            logger.info(f"Отправляем сообщение в чат {chat_id}")
            msg = bot.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False)
            # Только если сообщение отправлено успешно и фильм добавлен в БД — сохраняем для реакций
            bot_messages[msg.message_id] = link
            logger.info(f"✅ Сообщение успешно отправлено! Новый фильм добавлен: {info['title']}, message_id={msg.message_id}")
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
@bot.message_handler(commands=['start'])
def send_welcome(message):
    logger.info(f"[HANDLER] /start вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/start', message.chat.id)
    logger.info(f"Команда /start от пользователя {message.from_user.id}")
    
    # Проверяем, не является ли это автоматическим /start из лички (когда пользователь пишет боту впервые)
    # Если это личный чат и нет текста после /start - это автоматический /start, пропускаем
    if message.chat.type == 'private' and (not message.text or message.text.strip() == '/start'):
        logger.info(f"Автоматический /start в личке - пропускаем приветствие")
        return
    
    emoji = get_watched_emoji(message.chat.id)  # Берёт актуальный эмодзи из настроек

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
/total — статистика группы: сколько посмотрели, любимые жанры, режиссёры, актёры и оценки
/rate — дооценить просмотренные фильмы
/settings — сменить эмодзи для отметки просмотренных

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
def handle_reaction(update):
    if not update.new_reaction:
        return
    
    chat_id = update.chat.id
    user_id = update.user.id if update.user else None
    message_id = update.message_id
    
    logger.info(f"[REACTION] Получена реакция в чате {chat_id} на сообщение {message_id} от пользователя {user_id}")
    
    # Сначала проверяем, не это ли голосование по обнулению базы
    if message_id in clean_votes:
        vote_data = clean_votes[message_id]
        is_like = False
        for reaction in update.new_reaction:
            if hasattr(reaction, 'type'):
                if reaction.type == 'emoji' and hasattr(reaction, 'emoji') and reaction.emoji == '👍':
                    is_like = True
                    break
            elif hasattr(reaction, 'emoji') and reaction.emoji == '👍':
                is_like = True
                break
        
        if is_like and user_id and user_id in vote_data['active_members']:
            vote_data['voted'].add(user_id)
            
            # Проверяем, все ли проголосовали
            if len(vote_data['voted']) >= len(vote_data['active_members']):
                # Все проголосовали - удаляем базу
                with db_lock:
                    cursor.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s', (chat_id,))
                    conn.commit()
                
                bot.send_message(chat_id, "✅ Все участники проголосовали. База данных чата полностью обнулена.")
                logger.info(f"База данных чата {chat_id} обнулена после голосования всех участников")
                
                # Удаляем из clean_votes
                del clean_votes[message_id]
            else:
                # Обновляем сообщение с прогрессом
                voted_count = len(vote_data['voted'])
                total_count = len(vote_data['active_members'])
                try:
                    bot.edit_message_text(
                        f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                        f"Активных участников: {total_count}\n"
                        f"Проголосовало: {voted_count}/{total_count}\n\n"
                        f"Для подтверждения все активные участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                        f"Если не все проголосуют, база не будет удалена.",
                        chat_id, message_id, parse_mode='HTML')
                except:
                    pass
        return
    
    # Обычная обработка реакций для отметки просмотренных
    # Используем глобальные настройки (get_watched_emojis) и локальные (get_watched_reactions) для обратной совместимости
    watched_emojis_global = get_watched_emojis()
    watched = get_watched_reactions(chat_id)
    logger.info(f"[REACTION DEBUG] Chat: {chat_id}, Msg: {message_id}, User: {user_id}, New reactions: {len(update.new_reaction) if update.new_reaction else 0}, Watched emojis global: {watched_emojis_global}, Watched reactions: {watched}")
    
    for reaction in update.new_reaction:
        is_watched = False
        reaction_emoji = None
        
        if hasattr(reaction, 'type'):
            if reaction.type == 'emoji' and hasattr(reaction, 'emoji'):
                reaction_emoji = reaction.emoji
                # Проверяем и в глобальных настройках, и в локальных
                is_watched = reaction.emoji in watched_emojis_global or reaction.emoji in watched['emoji']
            elif reaction.type == 'custom_emoji' and hasattr(reaction, 'custom_emoji_id'):
                custom_id = str(reaction.custom_emoji_id)
                is_watched = custom_id in watched['custom']
        elif hasattr(reaction, 'emoji'):
            # Старый формат для обратной совместимости
            reaction_emoji = reaction.emoji
            is_watched = reaction.emoji in watched_emojis_global or reaction.emoji in watched['emoji']
        
        logger.info(f"[REACTION DEBUG] Reaction emoji: {reaction_emoji}, is_watched: {is_watched}")
        
        if is_watched:
            link = bot_messages.get(message_id)
            if not link:
                # Проверяем также plan_notification_messages
                plan_data = plan_notification_messages.get(message_id)
                if plan_data:
                    link = plan_data.get('link')
                    logger.info(f"[REACTION] Найдена ссылка в plan_notification_messages: {link}")
            
            if not link:
                logger.warning(f"[REACTION] Нет ссылки в bot_messages для message_id {message_id}. Доступные message_ids: {list(bot_messages.keys())[:10]}")
                return
            
            try:
                logger.info(f"[REACTION] Обрабатываем реакцию для фильма с ссылкой {link}")
                # Извлекаем kp_id из ссылки для поиска фильма
                match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
                if not match:
                    logger.warning(f"[REACTION] Не удалось извлечь kp_id из ссылки: {link}")
                    return
                
                kp_id = match.group(2)
                film_id = None
                title = None
                watched_status = None
                
                try:
                    with db_lock:
                        # Проверяем состояние транзакции
                        try:
                            cursor.execute('SELECT 1')
                            cursor.fetchone()
                        except:
                            conn.rollback()
                        
                        # Сначала проверяем, есть ли фильм и его текущий статус
                        cursor.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        row = cursor.fetchone()
                        
                        if not row:
                            logger.warning(f"[REACTION] Фильм с kp_id={kp_id} не найден в БД для chat_id={chat_id}")
                            return
                        
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        watched_status = row.get('watched') if isinstance(row, dict) else row[2]
                        
                        if watched_status == 1:
                            logger.info(f"[REACTION] Фильм {title} уже отмечен как просмотренный")
                            return
                        
                        # Обновляем watched
                        cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                        updated_count = cursor.rowcount
                        conn.commit()
                        logger.info(f"[REACTION] Обновлено записей: {updated_count} для film_id={film_id}, kp_id={kp_id}, chat_id={chat_id}")
                        
                except Exception as db_error:
                    logger.error(f"[REACTION] Ошибка БД при обработке реакции: {db_error}", exc_info=True)
                    try:
                        with db_lock:
                            conn.rollback()
                    except:
                        pass
                    return
                
                # Отправляем сообщение только если фильм найден и обновлён
                if film_id and title:
                    user_name = update.user.first_name if update.user else "Кто-то"
                    try:
                        msg = bot.send_message(chat_id, f"🎉 {user_name} отметил фильм <b>{title}</b> просмотренным!\n\n💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку.", parse_mode='HTML')
                        # Сохраняем связь message_id -> film_id для обработки оценки
                        rating_messages[msg.message_id] = film_id
                        logger.info(f"[REACTION] Сообщение об отметке фильма отправлено для {title}, message_id={msg.message_id}, film_id={film_id}")
                    except Exception as send_error:
                        logger.error(f"[REACTION] Ошибка при отправке сообщения: {send_error}", exc_info=True)
                else:
                    logger.warning(f"[REACTION] Не удалось отправить сообщение: film_id={film_id}, title={title}")
            except Exception as e:
                logger.error(f"[REACTION] Ошибка реакции: {e}", exc_info=True)

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
                    
                    cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
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

# /list — только непросмотренные
@bot.message_handler(commands=['list'])
def list_movies(message):
    logger.info(f"[HANDLER] /list вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/list', message.chat.id)
        logger.info(f"Команда /list от пользователя {message.from_user.id}")
        chat_id = message.chat.id
        with db_lock:
            # Сначала проверяем общее количество фильмов в базе
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
            total_count = cursor.fetchone()
            total = total_count.get('count') if isinstance(total_count, dict) else (total_count[0] if total_count else 0)
            logger.info(f"[LIST] Всего фильмов в базе для chat_id={chat_id}: {total}")
            
            # Проверяем количество просмотренных
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s AND watched = 1', (chat_id,))
            watched_count = cursor.fetchone()
            watched = watched_count.get('count') if isinstance(watched_count, dict) else (watched_count[0] if watched_count else 0)
            logger.info(f"[LIST] Просмотренных фильмов для chat_id={chat_id}: {watched}")
            
            # Получаем непросмотренные
            cursor.execute('SELECT id, kp_id, title, year, link FROM movies WHERE chat_id = %s AND watched = 0 ORDER BY title', (chat_id,))
            rows = cursor.fetchall()
            logger.info(f"[LIST] Непросмотренных фильмов для chat_id={chat_id}: {len(rows) if rows else 0}")
        
        if not rows:
            bot.reply_to(message, "⏳ Нет непросмотренных фильмов!")
            return
        
        text = "*⏳ Непросмотренные фильмы:*\n\n"
        # Все запросы к БД должны быть внутри db_lock
        with db_lock:
            for row in rows:
                # RealDictCursor возвращает словари, но поддерживает доступ по индексу
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
                year = row.get('year') if isinstance(row, dict) else row[2]
                link = row.get('link') if isinstance(row, dict) else row[3]
                kp_id = row.get('kp_id') if isinstance(row, dict) else (row[4] if len(row) > 4 else None)
                
                # Рассчитываем среднее из ratings
                cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
                avg_result = cursor.fetchone()
                # RealDictCursor возвращает словари, но поддерживает доступ по индексу
                if avg_result:
                    avg = avg_result.get('avg') if isinstance(avg_result, dict) else (avg_result[0] if len(avg_result) > 0 else None)
                else:
                    avg = None
                rate_str = f" 🌟 {avg:.1f}/10" if avg else ""
                # Используем kp_id вместо film_id для единообразия с /rate
                text += f"• <b>{title}</b> ({year}){rate_str} [ID: {kp_id or film_id}]\n{link}\n\n"
        
        text += "\n<i>В ответном сообщении пришлите ID фильмов, и они будут отмечены как просмотренные</i>"
        msg = bot.reply_to(message, text, parse_mode='HTML', disable_web_page_preview=True)
        # Сохраняем message_id для обработки ответов
        list_messages[msg.message_id] = chat_id
        logger.info(f"✅ Ответ на /list отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /list: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /list")
        except:
            pass

# Обработчик ответов на /list для отметки фильмов как просмотренных
@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in list_messages)
def handle_list_reply(message):
    """Обрабатывает ответ на сообщение /list с ID фильмов для отметки как просмотренных"""
    try:
        chat_id = list_messages.get(message.reply_to_message.message_id)
        if not chat_id:
            return
        
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
@bot.message_handler(commands=['total'])
def total_stats(message):
    logger.info(f"[HANDLER] /total вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/total', message.chat.id)
        logger.info(f"Команда /total от пользователя {message.from_user.id}")
        chat_id = message.chat.id
        with db_lock:
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
            total_row = cursor.fetchone()
            total = total_row[0] if total_row and total_row[0] else 0
            
            cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s AND watched = 1', (chat_id,))
            watched_row = cursor.fetchone()
            watched = watched_row[0] if watched_row and watched_row[0] else 0
            unwatched = total - watched
            
            # Если нет данных, отправляем сообщение
            if total == 0:
                bot.reply_to(message, "📊 Нет данных о вашей статистике.\n\nОцените первый фильм, чтобы статистика начала собираться.")
                return

            # Жанры
            cursor.execute('SELECT genres FROM movies WHERE chat_id = %s AND watched = 1', (chat_id,))
            genre_counts = {}
            for row in cursor.fetchall():
                genres = row.get('genres') if isinstance(row, dict) else row[0]
                if genres:
                    for g in str(genres).split(', '):
                        if g.strip():
                            genre_counts[g.strip()] = genre_counts.get(g.strip(), 0) + 1
            fav_genre = max(genre_counts, key=genre_counts.get) if genre_counts else "—"

            # Режиссёры
            cursor.execute('SELECT director, rating FROM movies WHERE chat_id = %s AND watched = 1 AND director IS NOT NULL AND director != "Не указан"', (chat_id,))
            director_stats = {}
            for row in cursor.fetchall():
                d = row.get('director') if isinstance(row, dict) else row[0]
                r = row.get('rating') if isinstance(row, dict) else row[1]
                if d not in director_stats:
                    director_stats[d] = {'count': 0, 'sum_rating': 0}
                director_stats[d]['count'] += 1
                if r:
                    director_stats[d]['sum_rating'] += r
            top_directors = sorted(director_stats.items(), key=lambda x: (-x[1]['count'], -(x[1]['sum_rating']/x[1]['count'] if x[1]['count'] > 0 else 0)))[:3]

            # Актёры
            cursor.execute('SELECT actors, rating FROM movies WHERE chat_id = %s AND watched = 1', (chat_id,))
            actor_stats = {}
            for row in cursor.fetchall():
                actors_str = row.get('actors') if isinstance(row, dict) else row[0]
                r = row.get('rating') if isinstance(row, dict) else row[1]
                if actors_str:
                    for a in actors_str.split(', '):
                        a = a.strip()
                        if a and a != "—":
                            if a not in actor_stats:
                                actor_stats[a] = {'count': 0, 'sum_rating': 0}
                            actor_stats[a]['count'] += 1
                            if r:
                                actor_stats[a]['sum_rating'] += r
            top_actors = sorted(actor_stats.items(), key=lambda x: (-x[1]['count'], -(x[1]['sum_rating']/x[1]['count'] if x[1]['count'] > 0 else 0)))[:3]

            # Рассчитываем среднее из ratings (не из movies.rating)
            cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s', (chat_id,))
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
            avg_d = stats['sum_rating']/stats['count'] if stats['count'] > 0 else 0
            text += f"• {d} — {stats['count']} фильм(ов), средняя {avg_d:.1f}/10\n"
        text += "\n<b>Топ актёров:</b>\n"
        for a, stats in top_actors:
            avg_a = stats['sum_rating']/stats['count'] if stats['count'] > 0 else 0
            text += f"• {a} — {stats['count']} фильм(ов), средняя {avg_a:.1f}/10\n"

        bot.reply_to(message, text, parse_mode='HTML')
        logger.info(f"✅ Ответ на /total отправлен пользователю {message.from_user.id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /total: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /total")
        except:
            pass

# /random с пропуском шагов
user_random_state = {}  # user_id: {'periods': [...], 'genre': ..., 'director': ...}

@bot.message_handler(commands=['random'])
def random_start(message):
    logger.info(f"[HANDLER] /random вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/random', message.chat.id)
        logger.info(f"Команда /random от пользователя {message.from_user.id}")
        user_id = message.from_user.id
        user_random_state[user_id] = {}

        markup = InlineKeyboardMarkup(row_width=2)
        periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
        for i in range(0, len(periods), 2):
            row = []
            row.append(InlineKeyboardButton(periods[i], callback_data=f"rand_period:{periods[i]}"))
            if i+1 < len(periods):
                row.append(InlineKeyboardButton(periods[i+1], callback_data=f"rand_period:{periods[i+1]}"))
            markup.row(*row)
        markup.add(InlineKeyboardButton("✅ Готово", callback_data="rand_period:done"))
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
        bot.send_message(message.chat.id, "🎲 Выберите периоды (можно несколько). Нажмите 'Готово' для продолжения:", reply_markup=markup)
    except Exception as e:
        logger.error(f"❌ Ошибка в /random: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /random")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_period:"))
def random_genre(call):
    try:
        user_id = call.from_user.id
        try:
            period_data = call.data.split(":", 1)[1]
        except (IndexError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка парсинга period_data: {e}, call.data={call.data}", exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при обработке выбора периода", show_alert=True)
            return
        
        logger.info(f"[RANDOM] Обработка выбора периода: user_id={user_id}, period_data={period_data}")
        
        if period_data == "skip":
            # Пропустить выбор периодов
            if user_id not in user_random_state:
                user_random_state[user_id] = {}
            user_random_state[user_id]['periods'] = []
            logger.info(f"[RANDOM] Периоды пропущены для user_id={user_id}")
        elif period_data == "done":
            # Готово - переходим к выбору жанра
            if user_id not in user_random_state or 'periods' not in user_random_state[user_id]:
                user_random_state[user_id] = {'periods': []}
            logger.info(f"[RANDOM] Периоды выбраны, переходим к жанру для user_id={user_id}, periods={user_random_state[user_id]['periods']}")
        else:
            # Переключение периода (toggle)
            if user_id not in user_random_state:
                user_random_state[user_id] = {'periods': []}
            if 'periods' not in user_random_state[user_id]:
                user_random_state[user_id]['periods'] = []
            
            periods_list = user_random_state[user_id]['periods']
            if period_data in periods_list:
                # Убираем период, если он уже выбран
                periods_list.remove(period_data)
                logger.info(f"[RANDOM] Период {period_data} убран для user_id={user_id}")
            else:
                # Добавляем период
                periods_list.append(period_data)
                logger.info(f"[RANDOM] Период {period_data} добавлен для user_id={user_id}")
            
            # Обновляем кнопки с отметками выбранных периодов
            markup = InlineKeyboardMarkup(row_width=2)
            all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            for i in range(0, len(all_periods), 2):
                row = []
                for j in range(2):
                    if i + j < len(all_periods):
                        period = all_periods[i + j]
                        label = period
                        if period in periods_list:
                            label = f"✓ {period}"
                        row.append(InlineKeyboardButton(label, callback_data=f"rand_period:{period}"))
                markup.row(*row)
            markup.add(InlineKeyboardButton("✅ Готово", callback_data="rand_period:done"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            
            selected_text = f"Выбрано: {', '.join(periods_list)}" if periods_list else "Периоды не выбраны"
            try:
                bot.edit_message_text(
                    f"🎲 Выберите периоды (можно несколько). Нажмите 'Готово' для продолжения:\n\n{selected_text}",
                    call.message.chat.id, call.message.message_id, reply_markup=markup)
                bot.answer_callback_query(call.id)  # Подтверждаем нажатие кнопки
            except Exception as e:
                logger.error(f"[RANDOM] Ошибка при обновлении сообщения с периодами: {e}", exc_info=True)
                try:
                    bot.answer_callback_query(call.id, "Ошибка при обновлении", show_alert=True)
                except:
                    pass
            return
        
        # Переходим к выбору жанра
        chat_id = call.message.chat.id
        try:
            with db_lock:
                cursor.execute("""
                    SELECT genres FROM movies 
                    WHERE chat_id = %s AND watched = 0 
                    AND id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s AND plan_datetime > NOW())
                """, (chat_id, chat_id))
                all_genres = set()
                for row in cursor.fetchall():
                    genres = row.get('genres') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if genres:
                        for g in str(genres).split(', '):
                            if g.strip():
                                all_genres.add(g.strip())
            
            if not all_genres:
                bot.edit_message_text("😔 Нет доступных жанров в непросмотренных фильмах.", call.message.chat.id, call.message.message_id)
                if user_id in user_random_state:
                    del user_random_state[user_id]
                bot.answer_callback_query(call.id, "Нет доступных жанров", show_alert=True)
                return
            
            markup = InlineKeyboardMarkup(row_width=2)
            for genre in sorted(all_genres):
                markup.add(InlineKeyboardButton(genre, callback_data=f"rand_genre:{genre}"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_genre:skip"))
            
            try:
                bot.edit_message_text("🎬 Выберите жанр:", call.message.chat.id, call.message.message_id, reply_markup=markup)
                bot.answer_callback_query(call.id)  # Подтверждаем нажатие кнопки
                logger.info(f"[RANDOM] Переход к выбору жанра для user_id={user_id}")
            except Exception as e:
                logger.error(f"[RANDOM] Ошибка при переходе к выбору жанра: {e}", exc_info=True)
                try:
                    bot.answer_callback_query(call.id, "Ошибка при переходе к выбору жанра", show_alert=True)
                except:
                    pass
        except Exception as e:
            logger.error(f"[RANDOM] Критическая ошибка в random_genre: {e}", exc_info=True)
            try:
                bot.edit_message_text("Произошла ошибка при обработке выбора периода.", call.message.chat.id, call.message.message_id)
                bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
            except:
                pass
    except Exception as e:
        logger.error(f"[RANDOM] Критическая ошибка в random_genre (внешний блок): {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка при обработке выбора периода", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("rand_year:"))
def handle_random_year(call):
    """Обработчик выбора года для рандомайзера с поддержкой форматов 2020-2025 и 2020_2025"""
    try:
        bot.answer_callback_query(call.id)  # убираем "часики" на кнопке
        
        try:
            data = call.data.split(":", 1)[1].strip()  # всё после "rand_year:"
        except (IndexError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка парсинга rand_year data: {e}, call.data={call.data}", exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при обработке выбора года", show_alert=True)
            return
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Инициализируем состояние пользователя, если нет
        if user_id not in user_random_state:
            user_random_state[user_id] = {}
        
        try:
            if data == "any":
                year_range = None
                logger.info(f"[RANDOM] Пользователь {user_id} пропустил выбор года")
            elif data == "old":  # если есть кнопка "До 2000"
                year_range = (1900, 1999)
                logger.info(f"[RANDOM] Пользователь {user_id} выбрал период до 2000")
            else:
                # Поддерживаем оба формата: 2020-2025 и 2020_2025
                separator = "-" if "-" in data else "_"
                try:
                    start_str, end_str = data.split(separator)
                    start = int(start_str.strip())
                    end = int(end_str.strip())
                    year_range = (start, end)
                    logger.info(f"[RANDOM] Пользователь {user_id} выбрал период {start}-{end}")
                except ValueError as ve:
                    logger.error(f"[RANDOM] Ошибка парсинга периода для {user_id}: {data} — {ve}", exc_info=True)
                    bot.answer_callback_query(call.id, "Неверный формат периода", show_alert=True)
                    return
            
            # Сохраняем в состояние
            user_random_state[user_id]["year_range"] = year_range
            
            # Переходим к следующему шагу — выбор жанра (вызываем random_genre логику)
            # Используем существующую логику перехода к выбору жанра из random_genre
            try:
                with db_lock:
                    cursor.execute("""
                        SELECT genres FROM movies 
                        WHERE chat_id = %s AND watched = 0 
                        AND id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s AND plan_datetime > NOW())
                    """, (chat_id, chat_id))
                    all_genres = set()
                    for row in cursor.fetchall():
                        genres = row.get('genres') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                        if genres:
                            for g in str(genres).split(', '):
                                if g.strip():
                                    all_genres.add(g.strip())
                
                if not all_genres:
                    bot.edit_message_text("😔 Нет доступных жанров в непросмотренных фильмах.", chat_id, call.message.message_id)
                    if user_id in user_random_state:
                        del user_random_state[user_id]
                    return
                
                markup = InlineKeyboardMarkup(row_width=2)
                for genre in sorted(all_genres):
                    markup.add(InlineKeyboardButton(genre, callback_data=f"rand_genre:{genre}"))
                markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_genre:skip"))
                
                bot.edit_message_text("🎬 Выберите жанр:", chat_id, call.message.message_id, reply_markup=markup)
                logger.info(f"[RANDOM] Переход к выбору жанра для user_id={user_id} после выбора года")
            except Exception as db_error:
                logger.error(f"[RANDOM] Ошибка БД при получении жанров: {db_error}", exc_info=True)
                bot.answer_callback_query(call.id, "Ошибка при получении списка жанров", show_alert=True)
                
        except ValueError as ve:
            logger.error(f"[RANDOM] Ошибка парсинга периода для {user_id}: {data} — {ve}", exc_info=True)
            try:
                bot.send_message(chat_id, "⚠️ Неверный формат периода. Начните заново: /random")
            except:
                pass
        except Exception as e:
            logger.error(f"[RANDOM] Критическая ошибка в handle_random_year: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, "Произошла ошибка при обработке выбора периода.")
            except:
                pass
    except Exception as e:
        logger.error(f"[RANDOM] Критическая ошибка в handle_random_year (внешний блок): {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_genre:"))
def random_director(call):
    try:
        user_id = call.from_user.id
        try:
            genre = call.data.split(":", 1)[1]
        except (IndexError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка парсинга genre: {e}, call.data={call.data}", exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при обработке выбора жанра", show_alert=True)
            return
        
        if genre == "skip":
            genre = None
        
        if user_id not in user_random_state:
            user_random_state[user_id] = {}
        user_random_state[user_id]['genre'] = genre

        # Топ-3 режиссёра
        chat_id = call.message.chat.id
        try:
            with db_lock:
                cursor.execute("""
                    SELECT director FROM movies 
                    WHERE chat_id = %s AND watched = 0 
                    AND director IS NOT NULL AND director != "Не указан"
                    AND id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s AND plan_datetime > NOW())
                """, (chat_id, chat_id))
                directors = []
                for row in cursor.fetchall():
                    director = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if director:
                        directors.append(director)
                top_directors = [d for d in sorted(set(directors), key=directors.count, reverse=True)[:3]]
        except Exception as db_error:
            logger.error(f"[RANDOM] Ошибка БД при получении режиссёров: {db_error}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при получении списка режиссёров", show_alert=True)
            except:
                pass
            return

        markup = InlineKeyboardMarkup(row_width=2)
        for d in top_directors:
            markup.add(InlineKeyboardButton(d, callback_data=f"rand_dir:{d}"))
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_dir:skip"))
        
        try:
            bot.edit_message_text("🎥 Выберите режиссёра из любимых группы:", call.message.chat.id, call.message.message_id, reply_markup=markup)
            bot.answer_callback_query(call.id)
            logger.info(f"[RANDOM] Переход к выбору режиссёра для user_id={user_id}")
        except Exception as e:
            logger.error(f"[RANDOM] Ошибка при переходе к выбору режиссёра: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при переходе к выбору режиссёра", show_alert=True)
            except:
                pass
    except Exception as e:
        logger.error(f"[RANDOM] Критическая ошибка в random_director: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка при обработке выбора жанра", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_dir:"))
def random_final(call):
    try:
        user_id = call.from_user.id
        try:
            director = call.data.split(":", 1)[1]
        except (IndexError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка парсинга director: {e}, call.data={call.data}", exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при обработке выбора режиссёра", show_alert=True)
            return
        
        if director == "skip":
            director = None
        
        if user_id not in user_random_state:
            user_random_state[user_id] = {}
        user_random_state[user_id]['director'] = director

        state = user_random_state[user_id]
        chat_id = call.message.chat.id
        
        try:
            with db_lock:
                query = "SELECT id, kp_id, title, year, genres, description, director, actors, link FROM movies WHERE chat_id = %s AND watched = 0 AND id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s AND plan_datetime > NOW())"
                params = [chat_id, chat_id]

                # Обработка year_range (числовой диапазон от rand_year)
                if state.get('year_range') is not None:
                    year_range = state['year_range']
                    if year_range:
                        start, end = year_range
                        query += " AND (year >= %s AND year <= %s)"
                        params.append(start)
                        params.append(end)

                # Обработка множественного выбора периодов (текстовые периоды от rand_period)
                if state.get('periods') and len(state['periods']) > 0:
                    period_conditions = []
                    for p in state['periods']:
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

                if state.get('genre'):
                    query += " AND genres LIKE %s"
                    params.append(f"%{state['genre']}%")

                if state.get('director'):
                    query += " AND director = %s"
                    params.append(state['director'])

                cursor.execute(query, params)
                candidates = cursor.fetchall()
        except Exception as db_error:
            logger.error(f"[RANDOM] Ошибка БД при поиске фильмов: {db_error}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при поиске фильмов", show_alert=True)
            except:
                pass
            return
        
        if not candidates:
            try:
                bot.edit_message_text("😔 Нет подходящих непросмотренных фильмов по вашим фильтрам.", call.message.chat.id, call.message.message_id)
                bot.answer_callback_query(call.id, "Нет подходящих фильмов", show_alert=True)
            except:
                pass
            if user_id in user_random_state:
                del user_random_state[user_id]
            return

        # Выбираем случайный фильм и сохраняем его данные в state
        try:
            movie = random.choice(candidates)
        except (IndexError, ValueError) as e:
            logger.error(f"[RANDOM] Ошибка при выборе случайного фильма: {e}, candidates={len(candidates) if candidates else 0}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при выборе фильма", show_alert=True)
            except:
                pass
            return
        
        # RealDictCursor возвращает словари, но поддерживает доступ по индексу
        try:
            user_random_state[user_id]['movie'] = {
                'id': movie.get('id') if isinstance(movie, dict) else movie[0],
                'kp_id': movie.get('kp_id') if isinstance(movie, dict) else movie[1],
                'title': movie.get('title') if isinstance(movie, dict) else movie[2],
                'year': movie.get('year') if isinstance(movie, dict) else movie[3],
                'genres': movie.get('genres') if isinstance(movie, dict) else movie[4],
                'description': movie.get('description') if isinstance(movie, dict) else movie[5],
                'director': movie.get('director') if isinstance(movie, dict) else movie[6],
                'actors': movie.get('actors') if isinstance(movie, dict) else movie[7],
                'link': movie.get('link') if isinstance(movie, dict) else movie[8]
            }
        except (IndexError, KeyError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка при извлечении данных фильма: {e}, movie={movie}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при обработке данных фильма", show_alert=True)
            except:
                pass
            return
        
        # Показываем выбор дня
        now = datetime.now(plans_tz)
        days = []
        # Русские названия дней недели
        days_ru = ['Понедельник', 'Вторник', 'Среда', 'Четверг', 'Пятница', 'Суббота', 'Воскресенье']
        
        # Находим ближайшее воскресенье
        current_weekday = now.weekday()  # 0 = понедельник, 6 = воскресенье
        days_until_sunday = (6 - current_weekday) % 7
        if days_until_sunday == 0:
            # Сегодня воскресенье, берем следующее
            days_until_sunday = 7
        
        nearest_sunday = now + timedelta(days=days_until_sunday)
        # Следующее за ближайшим воскресенье
        next_sunday = nearest_sunday + timedelta(days=7)
        
        # Включаем все даты до следующего воскресенья включительно
        end_date = next_sunday
        current_date = now
        
        day_count = 0
        while current_date <= end_date and day_count < 20:  # Ограничение на 20 дней
            day_date = current_date.strftime('%d.%m')
            weekday = current_date.weekday()  # 0 = понедельник, 6 = воскресенье
            
            if day_count == 0:
                label = f"Сегодня ({day_date})"
            elif day_count == 1:
                label = f"Завтра ({day_date})"
            else:
                day_name_ru = days_ru[weekday]
                label = f"{day_name_ru} ({day_date})"
            days.append((label, current_date.isoformat()))
            
            current_date = current_date + timedelta(days=1)
            day_count += 1
        
        markup = InlineKeyboardMarkup(row_width=1)
        for label, iso_date in days:
            markup.add(InlineKeyboardButton(label, callback_data=f"rand_day:{iso_date}"))
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_day:skip"))
        
        try:
            bot.edit_message_text("📅 Выберите день для просмотра:", call.message.chat.id, call.message.message_id, reply_markup=markup)
            bot.answer_callback_query(call.id)
            logger.info(f"[RANDOM] Переход к выбору дня для user_id={user_id}")
        except Exception as e:
            logger.error(f"[RANDOM] Ошибка при переходе к выбору дня: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при переходе к выбору дня", show_alert=True)
            except:
                pass
    except Exception as e:
        logger.error(f"[RANDOM] Критическая ошибка в random_final: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка при обработке выбора режиссёра", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_day:"))
def random_show_movie(call):
    try:
        user_id = call.from_user.id
        try:
            day_data = call.data.split(":", 1)[1]
        except (IndexError, AttributeError) as e:
            logger.error(f"[RANDOM] Ошибка парсинга day_data: {e}, call.data={call.data}", exc_info=True)
            bot.answer_callback_query(call.id, "Ошибка при обработке выбора дня", show_alert=True)
            return
        
        if user_id not in user_random_state or 'movie' not in user_random_state[user_id]:
            try:
                bot.edit_message_text("Ошибка: данные о фильме не найдены. Начните заново с /random", call.message.chat.id, call.message.message_id)
                bot.answer_callback_query(call.id, "Ошибка: данные о фильме не найдены", show_alert=True)
            except:
                pass
            if user_id in user_random_state:
                del user_random_state[user_id]
            return
        
        movie = user_random_state[user_id]['movie']
        
        # Формируем текст с днем
        if day_data == "skip":
            day_text = "на вечер"
            plan_dt = None
        else:
            try:
                day_dt = datetime.fromisoformat(day_data.replace('Z', '+00:00')).astimezone(plans_tz)
                day_text = day_dt.strftime('%d.%m.%Y')
                # Планируем на этот день в 19:00 для дома
                plan_dt = day_dt.replace(hour=19, minute=0)
            except Exception as date_error:
                logger.warning(f"[RANDOM] Ошибка парсинга даты: {date_error}, day_data={day_data}")
                day_text = "на вечер"
                plan_dt = None
        
        # Формируем полное описание фильма
        try:
            text = f"🍿 <b>Фильм {day_text}:</b>\n\n"
            text += f"<b>{movie.get('title', 'Неизвестно')}</b> ({movie.get('year', '—')})\n\n"
            
            if movie.get('director') and movie['director'] != "Не указан":
                text += f"🎬 <b>Режиссёр:</b> {movie['director']}\n"
            
            if movie.get('genres') and movie['genres'] != "—":
                text += f"🎭 <b>Жанры:</b> {movie['genres']}\n"
            
            if movie.get('actors') and movie['actors'] != "—":
                text += f"👥 <b>В ролях:</b> {movie['actors']}\n"
            
            text += f"\n📝 <b>Описание:</b>\n{movie.get('description', 'Нет описания')}\n\n"
            text += f"🔗 {movie.get('link', '')}"
            
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode='HTML', disable_web_page_preview=False)
            bot.answer_callback_query(call.id)
        except Exception as edit_error:
            logger.error(f"[RANDOM] Ошибка при редактировании сообщения: {edit_error}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка при отображении фильма", show_alert=True)
            except:
                pass
        
        # Автоматически планируем фильм на выбранную дату
        if plan_dt:
            try:
                chat_id = call.message.chat.id
                film_id = movie.get('id')
                kp_id = movie.get('kp_id')
                
                # Если нет kp_id, пытаемся извлечь из ссылки
                if not kp_id and movie.get('link'):
                    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', movie['link'])
                    if match:
                        kp_id = match.group(2)
                
                if not kp_id:
                    logger.error(f"Не удалось определить kp_id для фильма {movie.get('title')}")
                    bot.answer_callback_query(call.id, "Ошибка: не удалось определить ID фильма", show_alert=True)
                    return
                
                try:
                    with db_lock:
                        # Проверяем, есть ли фильм в базе по kp_id
                        if kp_id:
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        else:
                            # Fallback на id, если kp_id нет
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND id = %s', (chat_id, film_id))
                        row = cursor.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                        else:
                            # Добавляем фильм в базу, если его нет
                            if not kp_id:
                                logger.error(f"Не удалось добавить фильм в базу: нет kp_id")
                                bot.answer_callback_query(call.id, "Ошибка: не удалось добавить фильм в базу", show_alert=True)
                                return
                            
                            cursor.execute('''
                                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                            ''', (chat_id, movie.get('link'), kp_id, movie.get('title'), movie.get('year'), movie.get('genres'), movie.get('description'), movie.get('director'), movie.get('actors')))
                            conn.commit()
                            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                            row = cursor.fetchone()
                            if row:
                                film_id = row.get('id') if isinstance(row, dict) else row[0]
                            else:
                                logger.error(f"Не удалось добавить фильм в базу для планирования")
                                bot.answer_callback_query(call.id, "Ошибка: не удалось добавить фильм в базу", show_alert=True)
                                return
                        
                        # Добавляем план (передаем объект datetime, psycopg2 сам конвертирует в TIMESTAMP)
                        plan_utc = plan_dt.astimezone(pytz.utc)
                        cursor.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s)', 
                                      (chat_id, film_id, 'home', plan_utc, user_id))
                        conn.commit()
                    
                    bot.answer_callback_query(call.id, f"Фильм запланирован на {plan_dt.strftime('%d.%m.%Y')}")
                    logger.info(f"Фильм {movie.get('title')} автоматически запланирован на {plan_dt.strftime('%d.%m.%Y %H:%M')}")
                except Exception as db_error:
                    logger.error(f"Ошибка БД при автоматическом планировании фильма: {db_error}", exc_info=True)
                    try:
                        with db_lock:
                            conn.rollback()
                    except:
                        pass
                    try:
                        bot.answer_callback_query(call.id, "Ошибка при планировании фильма", show_alert=True)
                    except:
                        pass
            except Exception as e:
                logger.error(f"Ошибка при автоматическом планировании фильма: {e}", exc_info=True)
                try:
                    bot.answer_callback_query(call.id, "Ошибка при планировании фильма", show_alert=True)
                except:
                    pass
        
        if user_id in user_random_state:
            del user_random_state[user_id]
    except Exception as e:
        logger.error(f"[RANDOM] Критическая ошибка в random_show_movie: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка при обработке выбора дня", show_alert=True)
        except:
            pass
        if 'user_id' in locals() and user_id in user_random_state:
            del user_random_state[user_id]

# /rate
@bot.message_handler(commands=['rate'])
def rate_movie(message):
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/rate', message.chat.id)
    logger.info(f"Команда /rate от пользователя {message.from_user.id}")
    chat_id = message.chat.id
    
    # Получаем все просмотренные фильмы
    with db_lock:
        cursor.execute('''
            SELECT m.id, m.kp_id, m.title, m.year
            FROM movies m
            WHERE m.chat_id = %s AND m.watched = 1
            ORDER BY m.title
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
        
        text += f"<b>{kp_id}</b> — {title} ({year})\n"
        if not_rated:
            text += f"   ⚠️ Не оценили: {not_rated_text}\n"
        else:
            text += f"   ✅ Все оценили\n"
        text += "\n"
    
    # Отправляем сообщение и сохраняем его message_id для обработки реплая
    sent_msg = bot.reply_to(message, text, parse_mode='HTML')
    rate_list_messages[message.chat.id] = sent_msg.message_id

# Обработка реплая на список фильмов с оценками

@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == bot.get_me().id and m.text)
def handle_rate_list_reply(message):
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
                    INSERT INTO ratings (chat_id, film_id, user_id, rating)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating
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
                cursor.execute("DELETE FROM settings WHERE chat_id = -1 AND key = 'watched_emoji'", ())
                conn.commit()
            bot.reply_to(message, "✅ Реакции сброшены к значению по умолчанию (✅)")
            logger.info(f"Реакции сброшены (глобально)")
            return
        
        current = get_watched_emojis()
        
        sent = bot.send_message(chat_id,
            f"⚙️ Текущие реакции для просмотренных: {current}\n\n"
            "Отправьте эмодзи в ответ на это сообщение (можно несколько). "
            "Для сброса — /settings reset",
            reply_markup=None)
        
        # Сохраняем состояние
        user_settings_state[user_id] = {
            'settings_msg_id': sent.message_id,
            'chat_id': chat_id
        }
        logger.info(f"Настройки открыты для {user_id}, msg_id: {sent.message_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /settings: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /settings")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("settings:"))
def handle_settings_callback(call):
    """Обработчик callback для кнопок настроек"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        action = call.data.split(":", 1)[1]  # "add", "replace" или "reset"
        
        if action == "reset":
            # Сброс к значению по умолчанию
            with db_lock:
                cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_reactions'", (chat_id,))
                conn.commit()
            bot.edit_message_text("✅ Реакции сброшены к значению по умолчанию (✅)", call.message.chat.id, call.message.message_id)
            logger.info(f"Реакции сброшены для чата {chat_id}")
            if user_id in user_settings_state:
                del user_settings_state[user_id]
            return
        
        # Для add и replace - сохраняем режим и просим отправить эмодзи
        user_settings_state[user_id] = {
            'adding_reactions': True,
            'settings_msg_id': call.message.message_id,
            'action': action  # "add" или "replace"
        }
        
        mode_text = "добавлены к текущим" if action == "add" else "заменят текущие"
        bot.edit_message_text(
            f"⚙️ <b>Настройки реакций</b>\n\n📝 Отправьте эмодзи в ответ на это сообщение.\n\nНовые реакции будут {mode_text}.",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='HTML'
        )
        logger.info(f"[SETTINGS] Пользователь {user_id} выбрал режим: {action}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка в handle_settings_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
        except:
            pass

# Обработка ответа с эмодзи на сообщение /settings (упрощенная версия)
@bot.message_handler(func=lambda m: m.reply_to_message and m.from_user.id in user_settings_state)
def handle_settings_reply(message):
    user_id = message.from_user.id
    state = user_settings_state.get(user_id)
    
    if not state or message.reply_to_message.message_id != state.get('settings_msg_id'):
        return
    
    # Извлекаем эмодзи (упрощенная версия)
    if not message.text:
        bot.reply_to(message, "⚠️ Не найдено эмодзи. Отправь только эмодзи.")
        return
    
    # Простая проверка эмодзи
    emojis = ''.join(c for c in message.text if '\U0001F300' <= c <= '\U0001F9FF' or c in '✅💋🙏❤️😍😘☺️👍😁☑️😊😂🥰🎉⭐🔥')
    
    if not emojis:
        bot.reply_to(message, "⚠️ Не найдено эмодзи. Отправь только эмодзи.")
        return
    
    # Сохраняем в БД (глобально, chat_id=-1)
    try:
        with db_lock:
            cursor.execute("""
                INSERT INTO settings (chat_id, key, value) 
                VALUES (-1, 'watched_emoji', %s) 
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            """, (emojis,))
            conn.commit()
        
        bot.reply_to(message, f"✅ Реакции обновлены:\n{emojis}")
        logger.info(f"[SETTINGS] Реакции обновлены для user_id={user_id}: {emojis}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка при сохранении реакций: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при сохранении реакций.")
    
    # Очищаем состояние
    if user_id in user_settings_state:
        del user_settings_state[user_id]

# Обработка ответа с эмодзи на сообщение /settings (расширенная версия с режимами)
@bot.message_handler(func=lambda message: (
    message.reply_to_message and 
    message.from_user.id in user_settings_state and 
    user_settings_state[message.from_user.id].get('adding_reactions') and
    message.reply_to_message.message_id == user_settings_state[message.from_user.id].get('settings_msg_id')
))
def add_reactions(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Получаем состояние
    state = user_settings_state.get(user_id, {})
    settings_msg_id = state.get('settings_msg_id')
    action = state.get('action', 'replace')  # По умолчанию replace
    
    logger.info(f"[SETTINGS] add_reactions вызван для user_id={user_id}, reply_to_message={message.reply_to_message is not None}, settings_msg_id={settings_msg_id}, action={action}")
    
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
                
                if action == "add":
                    # Добавляем к текущим
                    current_reactions = get_watched_reactions(chat_id)
                    current_emoji = current_reactions['emoji']
                    current_custom = [f"custom:{cid}" for cid in current_reactions['custom']]
                    all_reactions = list(set(current_emoji + current_custom + new_reactions))  # Убираем дубликаты
                else:
                    # Заменяем полностью
                    all_reactions = new_reactions
                
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (chat_id, "watched_reactions", json.dumps(all_reactions)))
                conn.commit()
                
                reactions_str = ', '.join(all_reactions)
                action_text = "добавлены к текущим" if action == "add" else "заменены"
                bot.reply_to(message, f"✅ Готово! Реакции {action_text}:\n{reactions_str}")
                logger.info(f"[SETTINGS] Реакции сохранены для chat_id={chat_id} (режим: {action}): {reactions_str}")
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
def process_plan(user_id, chat_id, link, plan_type, day_or_date):
    plan_dt = None
    now = datetime.now(plans_tz)
    
    # Ищем день недели в расширенном словаре
    target_weekday = None
    day_lower = day_or_date.lower()
    for phrase, wd in days_full.items():
        if phrase in day_lower:
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
            # Пятница — 19:00, остальные — 10:00
            hour = 19 if target_weekday == 4 else 10
        else:  # cinema
            hour = 9
        
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = plans_tz.localize(plan_dt)
    
    elif plan_type == 'cinema':
        # Если день недели не найден — пытаемся распарсить дату (только для "в кино")
        date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', day_lower)
        if date_match:
            day_num = int(date_match.group(1))
            month_str = date_match.group(2)
            month = months_map.get(month_str)
            if month:
                try:
                    year = now.year
                    candidate = plans_tz.localize(datetime(year, month, day_num))
                    if candidate < now:
                        year += 1
                    plan_date = datetime(year, month, day_num)
                    plan_dt = plans_tz.localize(plan_date.replace(hour=9, minute=0))
                except ValueError:
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
                    cursor.execute('INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link', 
                                 (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors']))
                    conn.commit()
                    cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                    else:
                        bot.send_message(chat_id, "Не удалось добавить фильм в базу.")
                        return
                else:
                    bot.send_message(chat_id, "Не удалось извлечь информацию о фильме.")
                    return
            else:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
            
            plan_utc = plan_dt.astimezone(pytz.utc)
            cursor.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s)', 
                          (chat_id, film_id, plan_type, plan_utc, user_id))
            conn.commit()
        
        plan_type_text = "в кино" if plan_type == 'cinema' else "дома"
        bot.send_message(chat_id, f"✅ Запланирован фильм {plan_type_text}: <b>{title}</b> на {plan_dt.strftime('%d.%m.%Y %H:%M')} MSK", parse_mode='HTML')
        
        scheduler.add_job(send_plan_notification, 'date', run_date=plan_dt.astimezone(pytz.utc), 
                         args=[chat_id, title, link, plan_type])

# /plan — планирование просмотра
def process_plan(user_id, chat_id, link, plan_type, day_or_date):
    plan_dt = None
    now = datetime.now(plans_tz)
    
    # Ищем день недели в расширенном словаре
    target_weekday = None
    day_lower = day_or_date.lower()
    for phrase, wd in days_full.items():
        if phrase in day_lower:
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
            # Пятница — 19:00, остальные — 10:00
            hour = 19 if target_weekday == 4 else 10
        else:  # cinema
            hour = 9
        
        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = plans_tz.localize(plan_dt)
    
    elif plan_type == 'cinema':
        # Если день недели не найден — пытаемся распарсить дату (только для "в кино")
        date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', day_lower)
        if date_match:
            day_num = int(date_match.group(1))
            month_str = date_match.group(2)
            month = months_map.get(month_str)
            if month:
                try:
                    year = now.year
                    candidate = plans_tz.localize(datetime(year, month, day_num))
                    if candidate < now:
                        year += 1
                    plan_date = datetime(year, month, day_num)
                    plan_dt = plans_tz.localize(plan_date.replace(hour=9, minute=0))
                except ValueError:
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
                    cursor.execute('INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link', 
                                 (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors']))
                    conn.commit()
                    cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                    else:
                        bot.send_message(chat_id, "Не удалось добавить фильм в базу.")
                        return
                else:
                    bot.send_message(chat_id, "Не удалось извлечь информацию о фильме.")
                    return
            else:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
            
            plan_utc_iso = plan_dt.astimezone(pytz.utc).isoformat()
            cursor.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s)',
                          (chat_id, film_id, plan_type, plan_utc_iso, user_id))
            conn.commit()
        
        plan_type_text = "в кино" if plan_type == 'cinema' else "дома"
        bot.send_message(chat_id, f"✅ Запланирован фильм {plan_type_text}: <b>{title}</b> на {plan_dt.strftime('%d.%m.%Y %H:%M')} MSK", parse_mode='HTML')
        
        scheduler.add_job(send_plan_notification, 'date', run_date=plan_dt.astimezone(pytz.utc), 
                         args=[chat_id, title, link, plan_type])

@bot.message_handler(commands=['plan'])
def plan_handler(message):
    logger.info(f"[HANDLER] /plan вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/plan', message.chat.id)
        logger.info(f"Команда /plan от пользователя {message.from_user.id}")
        user_id = message.from_user.id
        text = message.text.lower().replace('/plan', '').strip()
        
        link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', text)
        link = link_match.group(1) if link_match else None
        
        plan_type = 'home' if 'дома' in text else 'cinema' if 'кино' in text else None
        
        day_or_date = None
        if plan_type == 'home':
            # Ищем любой день недели из расширенного словаря (сначала длинные фразы)
            sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
            for phrase in sorted_phrases:
                if phrase in text:
                    day_or_date = phrase
                    break
        elif plan_type == 'cinema':
            # Ищем день недели или дату
            sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
            for phrase in sorted_phrases:
                if phrase in text:
                    day_or_date = phrase
                    break
            if not day_or_date:
                # Пробуем разные форматы даты: "15 января", "15.01", "15/01"
                date_match = re.search(r'(\d+)\s*([а-яё]+)', text)
                if date_match:
                    day_or_date = f"{date_match.group(1)} {date_match.group(2)}"
                else:
                    # Формат "15.01" или "15/01"
                    date_match = re.search(r'(\d{1,2})[./](\d{1,2})', text)
                    if date_match:
                        day_num = int(date_match.group(1))
                        month_num = int(date_match.group(2))
                        if 1 <= month_num <= 12 and 1 <= day_num <= 31:
                            month_names = ['января', 'февраля', 'марта', 'апреля', 'мая', 'июня', 
                                         'июля', 'августа', 'сентября', 'октября', 'ноября', 'декабря']
                            day_or_date = f"{day_num} {month_names[month_num - 1]}"
        
        if link and plan_type and day_or_date:
            try:
                process_plan(user_id, message.chat.id, link, plan_type, day_or_date)
            except Exception as e:
                bot.reply_to(message, f"Ошибка при планировании: {e}")
                logger.error(f"Ошибка process_plan: {e}", exc_info=True)
            return
        
        if not link:
            bot.reply_to(message, "Не найдена ссылка на фильм. Отправьте ссылку на фильм (или реплай на сообщение с ней).")
            return
        
        if not plan_type:
            bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
            return
        
        if not day_or_date:
            bot.reply_to(message, "Не указан день/дата. Для дома укажите день недели (пн, вт, ср, чт, пт, сб, вс или 'в сб'), для кино - день недели или дату (15 января).")
            return
        
        user_plan_state[user_id] = {'step': 1}
        bot.reply_to(message, "Отправьте ссылку на фильм (или реплай на сообщение с ней).")
    except Exception as e:
        logger.error(f"❌ Ошибка в /plan: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /plan")
        except:
            pass

@bot.message_handler(func=lambda m: user_plan_state.get(m.from_user.id, {}).get('step') == 1)
def get_plan_link(message):
    user_id = message.from_user.id
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
        bot.reply_to(message, "Не нашёл ссылку. Попробуйте снова.")
        return
    
    user_plan_state[user_id]['link'] = link
    user_plan_state[user_id]['step'] = 2
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
    markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
    bot.send_message(message.chat.id, "Где планируете смотреть%s", reply_markup=markup)

@bot.callback_query_handler(func=lambda call: call.data.startswith("plan_type:"))
def plan_type_choice(call):
    user_id = call.from_user.id
    plan_type = call.data.split(":")[1]
    user_plan_state[user_id]['type'] = plan_type
    user_plan_state[user_id]['step'] = 3

    bot.edit_message_text("Укажите день/дату:", call.message.chat.id, call.message.message_id)
    if plan_type == 'home':
        bot.send_message(call.message.chat.id, "Для дома: пт, сб или вс.")
    else:
        bot.send_message(call.message.chat.id, "Для кино: '15 января' или 'с четверга'.")

@bot.message_handler(func=lambda m: user_plan_state.get(m.from_user.id, {}).get('step') == 3)
def get_plan_day_or_date(message):
    user_id = message.from_user.id
    text = message.text.lower().strip()
    plan_type = user_plan_state[user_id]['type']
    link = user_plan_state[user_id]['link']
    
    now_msk = datetime.now(plans_tz)
    plan_dt = None

    # Поиск дня недели
    target_weekday = None
    for phrase, wd in days_full.items():
        if phrase in text:
            target_weekday = wd
            break

    if target_weekday is not None:
        # Вычисляем ближайший указанный день (вперёд)
        current_wd = now_msk.weekday()
        delta = (target_weekday - current_wd + 7) % 7
        if delta == 0:  # если сегодня — переносим на следующую неделю
            delta = 7
        plan_date = now_msk.date() + timedelta(days=delta)

        if plan_type == 'home':
            # Пятница — 19:00, остальные — 10:00
            hour = 19 if target_weekday == 4 else 10
        else:  # cinema
            hour = 9

        plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour))
        plan_dt = plans_tz.localize(plan_dt)

    else:
        # Если день недели не найден — пытаемся распарсить дату (только для "в кино")
        if plan_type == 'cinema':
            if 'четверг' in text or any(p in text for p in ['чт', 'в четверг']):
                target_weekday = 3
                current_wd = now_msk.weekday()
                delta = (3 - current_wd + 7) % 7
                if delta == 0:
                    delta = 7
                plan_date = now_msk.date() + timedelta(days=delta)
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=9))
                plan_dt = plans_tz.localize(plan_dt)
            else:
                # Парсинг "15 января"
                date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
                if date_match:
                    day_num = int(date_match.group(1))
                    month_str = date_match.group(2)
                    month = months_map.get(month_str)
                    if month:
                        try:
                            year = now_msk.year
                            candidate = plans_tz.localize(datetime(year, month, day_num))
                            if candidate < now_msk:
                                year += 1
                            plan_date = datetime(year, month, day_num)
                            plan_dt = plans_tz.localize(plan_date.replace(hour=9, minute=0))
                        except ValueError:
                            bot.reply_to(message, "Некорректная дата. Попробуйте снова.")
                            return
                    else:
                        bot.reply_to(message, "Не распознал месяц.")
                        return
                else:
                    bot.reply_to(message, "Укажите день недели или дату в формате '15 января'.")
                    return
        else:
            bot.reply_to(message, "Укажите день недели (пн, вт, ср, чт, пт, сб, вс или полное название).")
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
                cursor.execute('''
                    INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                ''', (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors']))
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

            # Сохраняем план
            plan_utc = plan_dt.astimezone(pytz.utc)
            cursor.execute('''
                INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
            VALUES (%s, %s, %s, %s, %s)
        ''', (chat_id, film_id, plan_type, plan_utc, user_id))
        conn.commit()

        day_name = ['понедельник', 'вторник', 'среда', 'четверг', 'пятница', 'суббота', 'воскресенье'][plan_dt.weekday()]
        plan_type_text = "в кино" if plan_type == 'cinema' else "дома"
        bot.reply_to(message, f"✅ Запланирован фильм {plan_type_text}: <b>{title}</b> на <b>{day_name} {plan_dt.strftime('%d.%m.%Y в %H:%M')}</b> МСК", parse_mode='HTML')

        # Планируем уведомление
        scheduler.add_job(
            send_plan_notification,
            'date',
            run_date=plan_dt.astimezone(pytz.utc),
            args=[message.chat.id, title, link, plan_type]
        )

        del user_plan_state[user_id]

# /schedule — список запланированных просмотров
@bot.message_handler(commands=['schedule'])
def show_schedule(message):
    logger.info(f"[HANDLER] /schedule вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/schedule', message.chat.id)
        logger.info(f"Команда /schedule от пользователя {message.from_user.id}")
        
        chat_id = message.chat.id
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
            # Преобразуем TIMESTAMP в дату МСК
            try:
                # psycopg2 возвращает объект datetime для TIMESTAMP WITH TIME ZONE
                if isinstance(plan_dt_value, datetime):
                    # Если уже объект datetime, конвертируем в нужную таймзону
                    if plan_dt_value.tzinfo is None:
                        # Если нет таймзоны, предполагаем UTC
                        plan_dt = pytz.utc.localize(plan_dt_value).astimezone(plans_tz)
                    else:
                        plan_dt = plan_dt_value.astimezone(plans_tz)
                elif isinstance(plan_dt_value, str):
                    # Fallback для старых данных (если миграция еще не применена)
                    plan_dt_iso = plan_dt_value
                    if plan_dt_iso.endswith('Z'):
                        plan_dt = datetime.fromisoformat(plan_dt_iso.replace('Z', '+00:00')).astimezone(plans_tz)
                    elif '+' in plan_dt_iso or plan_dt_iso.count('-') > 2:
                        plan_dt = datetime.fromisoformat(plan_dt_iso).astimezone(plans_tz)
                    else:
                        plan_dt = datetime.fromisoformat(plan_dt_iso + '+00:00').astimezone(plans_tz)
                else:
                    # Неожиданный тип
                    logger.warning(f"Неожиданный тип plan_datetime: {type(plan_dt_value)}")
                    continue
                
                date_str = plan_dt.strftime('%d.%m.%Y')
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
                    date_str = datetime.now(plans_tz).strftime('%d.%m.%Y')
                plan_info = (title, date_str)
                
                if plan_type == 'cinema':
                    cinema_plans.append(plan_info)
                else:  # home
                    home_plans.append(plan_info)
        
        # Формируем текст с секциями
        text = "*📅 Запланированные просмотры:*\n\n"
        
        # Секция: Премьеры в кино
        if cinema_plans:
            text += "*🎦 Премьеры в кино:*\n"
            for title, date_str in cinema_plans:
                text += f"• <b>{title}</b> — {date_str}\n"
            text += "\n"
        
        # Секция: Просмотры дома
        if home_plans:
            text += "*🏠 Просмотры дома:*\n"
            for title, date_str in home_plans:
                text += f"• <b>{title}</b> — {date_str}\n"
            text += "\n"
        
        text += "Приятного просмотра! 🍿"
        bot.reply_to(message, text, parse_mode='HTML')
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
*/total* — Статистика: фильмы, жанры, режиссёры, актёры, оценки
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
            cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s', (chat_id,))
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
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("🗑️ Удалить оценку", callback_data="clean:rating"))
    markup.add(InlineKeyboardButton("👁️ Удалить просмотр", callback_data="clean:watched"))
    markup.add(InlineKeyboardButton("📅 Удалить задачу из планов", callback_data="clean:plan"))
    markup.add(InlineKeyboardButton("💥 Обнулить базу чата", callback_data="clean:chat_db"))
    markup.add(InlineKeyboardButton("👤 Обнулить базу пользователя", callback_data="clean:user_db"))
    
    help_text = (
        "🧹 <b>Что вы хотите удалить?</b>\n\n"
        "<b>💥 Обнулить базу чата</b> — удаляет <b>ВСЕ данные чата</b>:\n"
        "• Все фильмы\n"
        "• Все оценки всех пользователей\n"
        "• Все планы всех пользователей\n"
        "• Все настройки\n\n"
        "<b>👤 Обнулить базу пользователя</b> — удаляет <b>только ваши данные</b>:\n"
        "• Ваши оценки\n"
        "• Ваши планы\n"
        "• Ваша статистика\n\n"
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
    
    if action == 'rating':
        # Показываем список фильмов с оценками
        with db_lock:
            cursor.execute('''
                SELECT DISTINCT m.id, m.title, m.year
                FROM movies m
                JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                WHERE m.chat_id = %s
                ORDER BY m.title
                LIMIT 20
            ''', (chat_id,))
            movies = cursor.fetchall()
        
        if not movies:
            bot.edit_message_text("Нет фильмов с оценками для удаления.", call.message.chat.id, call.message.message_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        for film_id, title, year in movies:
            markup.add(InlineKeyboardButton(f"{title} ({year or '—'})", callback_data=f"clean_rating:{film_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="clean:cancel"))
        
        bot.edit_message_text("🗑️ <b>Выберите фильм для удаления оценки:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    
    elif action == 'watched':
        # Показываем список просмотренных фильмов
        with db_lock:
            cursor.execute('''
                SELECT id, title, year
                FROM movies
                WHERE chat_id = %s AND watched = 1
                ORDER BY title
                LIMIT 20
            ''', (chat_id,))
            movies = cursor.fetchall()
        
        if not movies:
            bot.edit_message_text("Нет просмотренных фильмов для удаления отметки.", call.message.chat.id, call.message.message_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        for film_id, title, year in movies:
            markup.add(InlineKeyboardButton(f"{title} ({year or '—'})", callback_data=f"clean_watched:{film_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="clean:cancel"))
        
        bot.edit_message_text("👁️ <b>Выберите фильм для удаления отметки просмотра:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    
    elif action == 'plan':
        # Показываем список планов
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
            bot.edit_message_text("Нет запланированных фильмов для удаления.", call.message.chat.id, call.message.message_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        for plan_id, title, plan_type, plan_dt_value in plans:
            try:
                # psycopg2 возвращает объект datetime для TIMESTAMP WITH TIME ZONE
                if isinstance(plan_dt_value, datetime):
                    if plan_dt_value.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt_value).astimezone(plans_tz)
                    else:
                        dt = plan_dt_value.astimezone(plans_tz)
                elif isinstance(plan_dt_value, str):
                    # Fallback для старых данных
                    dt = datetime.fromisoformat(plan_dt_value.replace('Z', '+00:00')).astimezone(plans_tz)
                else:
                    logger.warning(f"Неожиданный тип plan_datetime: {type(plan_dt_value)}")
                    continue
                date_str = dt.strftime('%d.%m.%Y %H:%M')
                type_text = "🎦 кино" if plan_type == 'cinema' else "🏠 дома"
                markup.add(InlineKeyboardButton(f"{title} — {date_str} ({type_text})", callback_data=f"clean_plan:{plan_id}"))
            except:
                markup.add(InlineKeyboardButton(f"{title} ({plan_type})", callback_data=f"clean_plan:{plan_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="clean:cancel"))
        
        bot.edit_message_text("📅 <b>Выберите план для удаления:</b>", call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    
    elif action == 'chat_db':
        # Обнуление базы чата - требует голосования в группах
        if call.message.chat.type in ['group', 'supergroup']:
            try:
                members_count = bot.get_chat_members_count(chat_id)
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
                    active_members = set()
                    for row in rows:
                        user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                        active_members.add(user_id)
                    logger.info(f"[CLEAN] Найдено активных участников: {len(active_members)}, user_ids: {list(active_members)}")
                
                if not active_members:
                    # Показываем более подробное сообщение с диагностикой
                    with db_lock:
                        cursor.execute('SELECT COUNT(*) FROM stats WHERE chat_id = %s', (chat_id,))
                        total_stats = cursor.fetchone()
                        total_count = total_stats.get('count') if isinstance(total_stats, dict) else (total_stats[0] if total_stats else 0)
                    
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата за последние 30 дней.\n\n"
                        f"📊 Диагностика:\n"
                        f"• Всего записей в stats для этого чата: {total_count}\n"
                        f"• Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                    f"Активных участников: {len(active_members)}\n"
                    f"Для подтверждения все активные участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                    f"Если не все проголосуют, база не будет удалена.",
                    parse_mode='HTML')
                
                clean_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': len(active_members),
                    'voted': set(),
                    'active_members': active_members
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
                "• Все планы\n"
                "• Все настройки\n\n"
                "Это действие необратимо!\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'chat'
    
    elif action == 'user_db':
        # Обнуление базы пользователя - удаляет только данные конкретного пользователя
        bot.edit_message_text(
            "⚠️ <b>Обнуление базы данных пользователя</b>\n\n"
            "Это удалит <b>только ваши данные</b>:\n"
            "• Все ваши оценки\n"
            "• Все ваши планы\n"
            "• Вашу статистику\n\n"
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

# Обработка подтверждения удаления базы
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
                    cursor.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
                    cursor.execute('DELETE FROM cinema_votes WHERE chat_id = %s', (chat_id,))
                    conn.commit()
                    bot.reply_to(message, "✅ База данных чата полностью обнулена.\n\nВсе фильмы, оценки, планы и настройки удалены.")
                    logger.info(f"База данных чата {chat_id} обнулена пользователем {user_id}")
                except Exception as e:
                    conn.rollback()
                    logger.error(f"Ошибка при удалении данных чата: {e}", exc_info=True)
                    bot.reply_to(message, "❌ Произошла ошибка при удалении данных. Попробуйте позже.")
                    raise
        
        elif action == 'user_db':
            # Удаляем все данные пользователя
            with db_lock:
                try:
                    cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    cursor.execute('DELETE FROM plans WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    cursor.execute('DELETE FROM stats WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    conn.commit()
                    bot.reply_to(message, "✅ Все ваши данные удалены из базы.\n\nВаши оценки, планы и статистика удалены. Фильмы и данные других пользователей остались без изменений.")
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


# Обработка новых ссылок (должен быть последним, чтобы не перехватывать команды)
@bot.message_handler(func=lambda m: m.text and not m.text.startswith('/') and m.entities)
def handle_message(message):
    logger.info(f"[HANDLER] handle_message вызван для сообщения от {message.from_user.id}")
    
    # Пропускаем сообщения, которые являются ответами на настройки
    if message.reply_to_message and message.from_user.id in user_settings_state:
        state = user_settings_state.get(message.from_user.id, {})
        if state.get('adding_reactions') and message.reply_to_message.message_id == state.get('settings_msg_id'):
            logger.info(f"[HANDLER] Пропускаем сообщение - это ответ на settings")
            return
    
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

# Flask app для webhook
app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():
    if request.headers.get('content-type') == 'application/json':
        json_string = request.get_data().decode('utf-8')
        update = telebot.types.Update.de_json(json_string)
        bot.process_new_updates([update])
        return ''
    else:
        abort(403)

@app.route('/')
def root():
    return jsonify({'status': 'ok', 'service': 'moviebot'}), 200

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200

# Логируем зарегистрированные маршруты после их определения
logger.info(f"Flask маршруты зарегистрированы: {[str(rule) for rule in app.url_map.iter_rules()]}")

# Определяем, где запускается бот: на Render или локально
# Проверяем несколько признаков Render окружения
RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL')
RENDER_SERVICE_ID = os.getenv('RENDER_SERVICE_ID')
RENDER = os.getenv('RENDER')
PORT = os.getenv('PORT')  # На Render всегда есть PORT

# Дополнительная проверка: путь выполнения (Render использует /opt/render/)
IS_RENDER_PATH = '/opt/render' in sys.executable or '/opt/render' in str(sys.path)

# Явная переменная для отключения polling (можно установить в Render env vars)
USE_POLLING = os.getenv('USE_POLLING', '').lower() in ('true', '1', 'yes')

# ВАЖНО: Если есть PORT или путь Render, это точно Render
# Polling НИКОГДА не должен запускаться на Render, если не установлена явно USE_POLLING=True
IS_RENDER = bool(PORT or RENDER_EXTERNAL_URL or RENDER_SERVICE_ID or RENDER or IS_RENDER_PATH)

# Если это Render, принудительно отключаем polling (если не установлена явно USE_POLLING)
if IS_RENDER and not USE_POLLING:
    IS_RENDER = True  # Гарантируем, что это Render
    logger.info(f"Определение окружения: PORT={PORT}, RENDER_EXTERNAL_URL={bool(RENDER_EXTERNAL_URL)}, IS_RENDER_PATH={IS_RENDER_PATH}, IS_RENDER={IS_RENDER}")
else:
    logger.info(f"Определение окружения: PORT={PORT}, RENDER_EXTERNAL_URL={bool(RENDER_EXTERNAL_URL)}, IS_RENDER_PATH={IS_RENDER_PATH}, IS_RENDER={IS_RENDER}, USE_POLLING={USE_POLLING}")

if IS_RENDER:
    logger.info("=== RENDER MODE: WEBHOOK + FLASK SERVER ===")
    
    # Очистка и установка webhook
    try:
        bot.remove_webhook()
        time.sleep(1)  # небольшая пауза
    except:
        pass
    
    if RENDER_EXTERNAL_URL:
        webhook_url = RENDER_EXTERNAL_URL + '/webhook'
        allowed_updates = [
            "message", "edited_message", "callback_query",
            "message_reaction", "message_reaction_count",
            "chat_member", "my_chat_member"
        ]
        bot.set_webhook(url=webhook_url, allowed_updates=allowed_updates)
        logger.info(f"Webhook установлен: {webhook_url}")
    else:
        logger.error("RENDER_EXTERNAL_URL не задан!")

    # На Render используется gunicorn для запуска Flask приложения
    # Gunicorn сам запускает app через: gunicorn moviebot:app
    # Поэтому app.run() НЕ вызываем - gunicorn сам запустит приложение
    logger.info("Flask приложение готово к запуску через gunicorn")
    logger.info(f"Зарегистрированные маршруты: {[str(rule) for rule in app.url_map.iter_rules()]}")
    logger.info("Gunicorn запустит приложение автоматически")
else:
    # Локальный запуск - используем polling (только если IS_RENDER=False)
    if IS_RENDER:
        # Дополнительная защита: если по какой-то причине IS_RENDER=True, но мы в блоке else
        logger.error("ОШИБКА: IS_RENDER=True, но код попал в блок else! Polling НЕ будет запущен!")
    elif __name__ == '__main__':
        logger.info("Локальное окружение - будет использован polling")
        try:
            bot.remove_webhook()
            logger.info("Старые webhook очищены")
        except Exception as e:
            logger.warning(f"Не удалось очистить webhook: {e}")
        logger.info("Локальный запуск: используется polling")
        bot.infinity_polling()
    else:
        logger.warning("Код выполняется не как main, но IS_RENDER=False. Polling не будет запущен автоматически.")
