"""
Модуль для работы с базой данных
"""
import logging
import pytz
from datetime import datetime
from database.db_connection import get_db_connection, get_db_cursor, db_lock
from config.settings import DEFAULT_WATCHED_EMOJIS

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

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

                

                # Используем библиотеку emoji для правильного извлечения всех эмодзи из строки

                try:

                    import emoji

                    emojis_list = emoji.distinct_emoji_list(value_clean)

                    if emojis_list:

                        return emojis_list

                except ImportError:

                    # Если библиотека emoji недоступна, используем fallback метод

                    # Список известных эмодзи для правильного извлечения

                    known_emojis = ['✅', '👍', '👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿', '❤️', '❤️‍🔥', '❤️‍🩹', '💛', '🧡', '💚', '💙', '💜', '🖤', '🤍', '🤎', '🔥']

                    

                    # Извлекаем эмодзи из строки, проверяя по известным эмодзи (в порядке длины, чтобы сначала проверять составные)

                    found_emojis = []

                    value_remaining = value_clean

                    

                    # Сортируем по длине (от длинных к коротким), чтобы сначала находить составные эмодзи

                    sorted_emojis = sorted(known_emojis, key=len, reverse=True)

                    

                    for emoji_char in sorted_emojis:

                        while emoji_char in value_remaining:

                            idx = value_remaining.index(emoji_char)

                            found_emojis.append(emoji_char)

                            # Удаляем найденный эмодзи из строки

                            value_remaining = value_remaining[:idx] + value_remaining[idx+len(emoji_char):]

                    

                    # Если нашли эмодзи, возвращаем их

                    if found_emojis:

                        return found_emojis

                except Exception as e:

                    logger.warning(f"[GET WATCHED EMOJIS] Ошибка при извлечении эмодзи: {e}")

                    pass

                

                # Если ничего не нашли, возвращаем дефолт

                return ['✅']

        # Дефолт, если не настроено: ✅, все варианты лайков и сердечек

        return ['✅', '👍', '👍🏻', '👍🏼', '👍🏽', '👍🏾', '👍🏿', '❤️', '❤️‍🔥', '❤️‍🩹', '💛', '🧡', '💚', '💙', '💜', '🖤', '🤍', '🤎', '🔥']



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


def get_ratings_info(chat_id, film_id, user_id):
    """Получает информацию об оценках для фильма и пользователя"""
    with db_lock:
        cursor.execute("""
            SELECT rating 
            FROM ratings 
            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
        """, (chat_id, film_id, user_id))
        row = cursor.fetchone()
        return {
            'current_user_rated': row is not None,
            'current_user_rating': row.get('rating') if row and isinstance(row, dict) else (row[0] if row else None)
        }


def get_notification_settings(chat_id):
    """Получает настройки времени напоминаний для чата"""
    defaults = {
        'separate_weekdays': 'true',  # По умолчанию разделяем будни и выходные
        'home_weekday_hour': 19,  # Будни: 19:00
        'home_weekday_minute': 0,
        'home_weekend_hour': 9,  # Выходные: 9:00
        'home_weekend_minute': 0,
        'cinema_weekday_hour': 9,  # Кино будни: 9:00
        'cinema_weekday_minute': 0,
        'cinema_weekend_hour': 9,  # Кино выходные: 9:00
        'cinema_weekend_minute': 0,
        'ticket_before_minutes': 10  # За 10 минут по умолчанию
    }
    
    with db_lock:
        cursor.execute("""
            SELECT key, value FROM settings 
            WHERE chat_id = %s AND key IN (
                'notify_separate_weekdays', 'notify_home_weekday_hour', 'notify_home_weekday_minute',
                'notify_home_weekend_hour', 'notify_home_weekend_minute',
                'notify_cinema_weekday_hour', 'notify_cinema_weekday_minute',
                'notify_cinema_weekend_hour', 'notify_cinema_weekend_minute',
                'ticket_before_minutes'
            )
        """, (chat_id,))
        rows = cursor.fetchall()
        
        for row in rows:
            key = row.get('key') if isinstance(row, dict) else row[0]
            value = row.get('value') if isinstance(row, dict) else row[1]
            
            if key == 'notify_separate_weekdays':
                defaults['separate_weekdays'] = value
            elif key == 'notify_home_weekday_hour':
                defaults['home_weekday_hour'] = int(value) if value else defaults['home_weekday_hour']
            elif key == 'notify_home_weekday_minute':
                defaults['home_weekday_minute'] = int(value) if value else defaults['home_weekday_minute']
            elif key == 'notify_home_weekend_hour':
                defaults['home_weekend_hour'] = int(value) if value else defaults['home_weekend_hour']
            elif key == 'notify_home_weekend_minute':
                defaults['home_weekend_minute'] = int(value) if value else defaults['home_weekend_minute']
            elif key == 'notify_cinema_weekday_hour':
                defaults['cinema_weekday_hour'] = int(value) if value else defaults['cinema_weekday_hour']
            elif key == 'notify_cinema_weekday_minute':
                defaults['cinema_weekday_minute'] = int(value) if value else defaults['cinema_weekday_minute']
            elif key == 'notify_cinema_weekend_hour':
                defaults['cinema_weekend_hour'] = int(value) if value else defaults['cinema_weekend_hour']
            elif key == 'notify_cinema_weekend_minute':
                defaults['cinema_weekend_minute'] = int(value) if value else defaults['cinema_weekend_minute']
            elif key == 'ticket_before_minutes':
                defaults['ticket_before_minutes'] = int(value) if value else defaults['ticket_before_minutes']
    
    return defaults


def set_notification_setting(chat_id, key, value):
    """Сохраняет настройку времени напоминаний для чата"""
    with db_lock:
        cursor.execute("""
            INSERT INTO settings (chat_id, key, value)
            VALUES (%s, %s, %s)
            ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
        """, (chat_id, key, str(value)))
        conn.commit()

