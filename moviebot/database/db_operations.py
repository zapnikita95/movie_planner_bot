"""
Модуль для работы с базой данных
"""
import logging
import json
import pytz
import requests
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.config import DEFAULT_WATCHED_EMOJIS, KP_TOKEN

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

        # Используем локальное подключение, чтобы не зависеть от глобального курсора
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()

        try:
            with db_lock:
                cursor_local.execute(
                    "SELECT value FROM settings WHERE chat_id = %s AND key = %s",
                    (user_id, 'user_timezone')
                )
                row = cursor_local.fetchone()

            if row:
                tz_name = row.get('value') if isinstance(row, dict) else row[0]

                # Карта поддерживаемых идентификаторов часовых поясов
                tz_map = {
                    'Moscow': 'Europe/Moscow',
                    'Serbia': 'Europe/Belgrade',
                    'Samara': 'Europe/Samara',                # +1 МСК
                    'Yekaterinburg': 'Asia/Yekaterinburg',    # +2 МСК
                    'Novosibirsk': 'Asia/Novosibirsk',        # +4 МСК
                }

                if tz_name in tz_map:
                    return pytz.timezone(tz_map[tz_name])

            return None
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass

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
    """Устанавливает часовой пояс пользователя. timezone_name: 'Moscow', 'Serbia', 'Samara', 'Yekaterinburg', 'Novosibirsk'"""

    try:

        conn_local = get_db_connection()
        cursor_local = get_db_cursor()

        try:
            with db_lock:
                cursor_local.execute(
                    """
                    INSERT INTO settings (chat_id, key, value) 
                    VALUES (%s, %s, %s) 
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    """,
                    (user_id, 'user_timezone', timezone_name),
                )
                conn_local.commit()

            logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
            return True
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass

    except Exception as e:

        logger.error(f"Ошибка установки часового пояса для user_id={user_id}: {e}", exc_info=True)

        conn.rollback()

        return False



def get_user_films_count(user_id):
    """Возвращает количество фильмов в базе пользователя (для личного чата, где chat_id = user_id)"""
    with db_lock:
        cursor.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (user_id,))
        row = cursor.fetchone()
        if row:
            count = row.get('count') if isinstance(row, dict) else row[0]
            return count if count else 0
        return 0


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
    # ВАЖНО: Используем локальные соединения вместо глобальных
    conn_local = None
    cursor_local = None
    
    try:
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logger.debug(f"[LOG_REQUEST] Попытка логирования: user_id={user_id}, username={username}, command={command_or_action}, chat_id={chat_id}, timestamp={timestamp}")

        conn_local = get_db_connection()
        cursor_local = get_db_cursor()

        with db_lock:
            try:
                # Проверяем, не в состоянии ли ошибки транзакция
                try:
                    cursor_local.execute('SELECT 1')
                    cursor_local.fetchone()
                except:
                    # Если транзакция в состоянии ошибки, откатываем
                    conn_local.rollback()
                
                cursor_local.execute('''
                    INSERT INTO stats (user_id, username, command_or_action, timestamp, chat_id)
                    VALUES (%s, %s, %s, %s, %s)
                ''', (user_id, username, command_or_action, timestamp, chat_id))

                conn_local.commit()
                logger.debug(f"[LOG_REQUEST] Успешно залогировано: user_id={user_id}, command={command_or_action}, chat_id={chat_id}")

            except Exception as db_error:
                # КРИТИЧНО: откатываем транзакцию при ошибке
                try:
                    conn_local.rollback()
                except:
                    pass
                logger.error(f"[LOG_REQUEST] Ошибка БД при логировании: {db_error}", exc_info=True)
                # Не делаем raise, чтобы не прерывать выполнение основной логики

    except Exception as e:
        logger.error(f"Ошибка логирования запроса: {e}", exc_info=True)
        # Убеждаемся, что транзакция откачена
        if conn_local:
            try:
                with db_lock:
                    conn_local.rollback()
            except:
                pass
    finally:
        # Закрываем локальные соединения
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        if conn_local:
            try:
                conn_local.close()
            except:
                pass

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


# Функции для работы с подписками
def get_active_subscription(chat_id, user_id, subscription_type=None):
    """Получает активную подписку для чата/пользователя"""
    # Специальный доступ для создателя бота (@zap_nikita, user_id=301810276)
    # Возвращаем виртуальную подписку "all" с lifetime периодом ТОЛЬКО для личных подписок
    # Для групповых подписок создатель должен покупать подписку как и все остальные
    if user_id == 301810276 and subscription_type == 'personal':
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.UTC)
        # Возвращаем словарь с данными полной подписки только для личных подписок
        virtual_sub = {
            'id': None,
            'chat_id': chat_id,
            'user_id': user_id,
            'subscription_type': 'personal',
            'plan_type': 'all',
            'period_type': 'lifetime',
            'price': 0,
            'activated_at': now,
            'next_payment_date': None,
            'expires_at': None,
            'is_active': True,
            'cancelled_at': None,
            'telegram_username': 'zap_nikita',
            'group_username': None,
            'created_at': now
        }
        return virtual_sub
    
    with db_lock:
        # Сначала проверяем, есть ли реальная подписка в БД
        query = """
            SELECT * FROM subscriptions 
            WHERE chat_id = %s AND user_id = %s AND is_active = TRUE 
            AND (expires_at IS NULL OR expires_at > NOW())
        """
        params = [chat_id, user_id]
        if subscription_type:
            query += " AND subscription_type = %s"
            params.append(subscription_type)
        query += " ORDER BY activated_at DESC LIMIT 1"
        cursor.execute(query, params)
        row = cursor.fetchone()
        
        # Если есть реальная подписка, возвращаем её
        if row:
            return row
        
        # Если подписки нет, возвращаем None
        # Не создаем виртуальную подписку автоматически - пользователь должен купить подписку
        return None


def get_active_subscription_by_username(telegram_username, subscription_type='personal'):
    """Получает активную персональную подписку по username"""
    # Специальный доступ для создателя бота (@zap_nikita)
    username_clean = telegram_username.lstrip('@')
    if username_clean == 'zap_nikita':
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.UTC)
        # Возвращаем словарь с данными полной подписки
        virtual_sub = {
            'id': None,
            'chat_id': None,
            'user_id': 301810276,
            'subscription_type': subscription_type,
            'plan_type': 'all',
            'period_type': 'lifetime',
            'price': 0,
            'activated_at': now,
            'next_payment_date': None,
            'expires_at': None,
            'is_active': True,
            'cancelled_at': None,
            'telegram_username': 'zap_nikita',
            'group_username': None,
            'created_at': now
        }
        return virtual_sub
    
    with db_lock:
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE telegram_username = %s AND subscription_type = %s 
            AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY activated_at DESC LIMIT 1
        """, (telegram_username, subscription_type))
        return cursor.fetchone()


def get_active_group_subscription(group_username):
    """Получает активную групповую подписку по username группы"""
    with db_lock:
        # Сначала проверяем реальную подписку
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE group_username = %s AND subscription_type = 'group' 
            AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY activated_at DESC LIMIT 1
        """, (group_username,))
        row = cursor.fetchone()
        
        # Если есть реальная подписка, возвращаем её
        if row:
            return row
        
        # Если подписки нет, проверяем наличие активности (бот присутствует в группе)
        # Для этого нужно найти chat_id по username, но это сложно без bot объекта
        # Поэтому возвращаем None - проверка будет в обработчике через bot.get_chat
        return None


def get_active_group_subscription_by_chat_id(chat_id):
    """Получает активную групповую подписку по chat_id группы"""
    with db_lock:
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE chat_id = %s AND subscription_type = 'group' 
            AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY activated_at DESC LIMIT 1
        """, (chat_id,))
        row = cursor.fetchone()
        
        # Если есть реальная подписка, возвращаем её
        if row:
            return row
        
        return None


def get_user_personal_subscriptions(user_id):
    """Получает все активные персональные подписки пользователя"""
    # Специальный доступ для создателя бота (@zap_nikita, user_id=301810276)
    if user_id == 301810276:
        from datetime import datetime
        import pytz
        now = datetime.now(pytz.UTC)
        virtual_sub = {
            'id': None,
            'chat_id': None,
            'user_id': user_id,
            'subscription_type': 'personal',
            'plan_type': 'all',
            'period_type': 'lifetime',
            'price': 0,
            'activated_at': now,
            'next_payment_date': None,
            'expires_at': None,
            'is_active': True,
            'cancelled_at': None,
            'telegram_username': 'zap_nikita',
            'group_username': None,
            'created_at': now
        }
        return [virtual_sub]
    
    with db_lock:
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE user_id = %s AND subscription_type = 'personal' 
            AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
        """, (user_id,))
        return cursor.fetchall()


def get_user_group_subscriptions(user_id):
    """Получает все активные групповые подписки пользователя"""
    # Специальный доступ для создателя бота (@zap_nikita, user_id=301810276)
    # Для групповых подписок возвращаем пустой список, так как доступ проверяется через get_active_subscription
    if user_id == 301810276:
        return []
    
    with db_lock:
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE user_id = %s AND subscription_type = 'group' 
            AND is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
        """, (user_id,))
        return cursor.fetchall()


def renew_subscription(subscription_id, period_type):
    """Продлевает существующую подписку на указанный период
    
    ВАЖНО: Эта функция НЕ изменяет цену подписки (price).
    Цена остается той же, что была сохранена при создании подписки.
    Это гарантирует, что изменение тарифов в SUBSCRIPTION_PRICES
    не повлияет на уже существующие подписки пользователей.
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import pytz
    
    now = datetime.now(pytz.UTC)
    
    # Вычисляем новую дату следующего платежа и окончания
    expires_at = None
    next_payment_date = None
    
    if period_type == 'month':
        next_payment_date = now + relativedelta(months=1)
        expires_at = now + relativedelta(months=1)
    elif period_type == '3months':
        next_payment_date = now + relativedelta(months=3)
        expires_at = now + relativedelta(months=3)
    elif period_type == 'year':
        next_payment_date = now + relativedelta(years=1)
        expires_at = now + relativedelta(years=1)
    elif period_type == 'test':
        # Тестовый тариф - списание раз в 10 минут
        from datetime import timedelta
        next_payment_date = now + timedelta(minutes=10)
        expires_at = now + timedelta(minutes=10)
    elif period_type == 'lifetime':
        expires_at = None
        next_payment_date = None
    
    # ВАЖНО: НЕ обновляем поле price - цена остается той же, что была при создании подписки
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET next_payment_date = %s, expires_at = %s, activated_at = %s
            WHERE id = %s
        """, (next_payment_date, expires_at, now, subscription_id))
        conn.commit()
        return True


def create_subscription(chat_id, user_id, subscription_type, plan_type, period_type, price, 
                       telegram_username=None, group_username=None, group_size=None, payment_method_id=None, next_payment_date=None):
    """Создает новую подписку"""
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    import pytz
    
    now = datetime.now(pytz.UTC)
    
    # Вычисляем дату окончания и следующего платежа
    expires_at = None
    if next_payment_date is None:
        # Если next_payment_date не указан, вычисляем автоматически
        if period_type == 'month':
            # Ежемесячная подписка - списание каждый месяц
            expires_at = now + relativedelta(months=1)
            next_payment_date = now + relativedelta(months=1)
        elif period_type == '3months':
            # Ежеквартальная подписка - списание каждые 3 месяца
            expires_at = now + relativedelta(months=3)
            next_payment_date = now + relativedelta(months=3)
        elif period_type == 'year':
            # Годовая подписка - списание раз в год
            expires_at = now + relativedelta(years=1)
            next_payment_date = now + relativedelta(years=1)
        elif period_type == 'test':
            # Тестовый тариф - списание раз в 10 минут
            expires_at = now + timedelta(minutes=10)
            next_payment_date = now + timedelta(minutes=10)
        elif period_type == 'lifetime':
            expires_at = None
            next_payment_date = None
    else:
        # Если next_payment_date указан, вычисляем expires_at на основе period_type
        if period_type == 'month':
            expires_at = next_payment_date
        elif period_type == '3months':
            expires_at = next_payment_date
        elif period_type == 'year':
            expires_at = next_payment_date
        elif period_type == 'test':
            expires_at = next_payment_date
        elif period_type == 'lifetime':
            expires_at = None
    
    with db_lock:
        cursor.execute("""
            INSERT INTO subscriptions 
            (chat_id, user_id, subscription_type, plan_type, period_type, price, 
             activated_at, next_payment_date, expires_at, telegram_username, group_username, group_size, payment_method_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """, (chat_id, user_id, subscription_type, plan_type, period_type, price,
              now, next_payment_date, expires_at, telegram_username, group_username, group_size, payment_method_id))
        result = cursor.fetchone()
        if result:
            subscription_id = result.get('id') if isinstance(result, dict) else result[0]
        else:
            subscription_id = None
        
        # Добавляем features в зависимости от plan_type
        if plan_type == 'all':
            features = ['notifications', 'recommendations', 'tickets']
        elif plan_type == 'notifications':
            features = ['notifications']
        elif plan_type == 'recommendations':
            features = ['recommendations']
        elif plan_type == 'tickets':
            features = ['tickets']
        else:
            features = []
        
        for feature in features:
            cursor.execute("""
                INSERT INTO subscription_features (subscription_id, feature_type)
                VALUES (%s, %s)
            """, (subscription_id, feature))
        
        conn.commit()
        return subscription_id


def cancel_subscription(subscription_id, user_id):
    """Отменяет подписку (включая отмену подписки Telegram Stars, если применимо)"""
    from datetime import datetime
    import pytz
    import os
    import logging
    
    logger = logging.getLogger(__name__)
    
    # Получаем информацию о подписке перед отменой
    with db_lock:
        cursor.execute("""
            SELECT payment_method_id, subscription_type, period_type
            FROM subscriptions 
            WHERE id = %s AND user_id = %s
        """, (subscription_id, user_id))
        sub_info = cursor.fetchone()
    
    # Отменяем подписку Telegram Stars, если она была оплачена через Stars
    # Проверяем, есть ли платежи через Stars для этой подписки
    if sub_info:
        try:
            # Ищем платежи через Stars для этой подписки
            # Проверяем наличие поля payment_method в таблице payments
            cursor.execute("""
                SELECT p.yookassa_payment_id, p.status
                FROM payments p
                WHERE p.subscription_id = %s 
                AND p.status = 'succeeded'
                AND p.yookassa_payment_id IS NULL
                ORDER BY p.created_at DESC
                LIMIT 1
            """, (subscription_id,))
            stars_payment = cursor.fetchone()
            
            if stars_payment:
                # Если есть платеж через Stars (yookassa_payment_id = NULL), пытаемся отменить подписку
                # Согласно документации Telegram, отмена подписки Stars происходит автоматически
                # при деактивации подписки в БД, но можно также вызвать API для явной отмены
                logger.info(f"[CANCEL SUBSCRIPTION] Найден платеж через Stars для подписки {subscription_id}")
                # Примечание: Отмена подписки Telegram Stars происходит автоматически при деактивации
                # в нашей БД, так как Telegram отслеживает активные подписки через payments.getStarsSubscriptions
        except Exception as e:
            logger.error(f"[CANCEL SUBSCRIPTION] Ошибка проверки платежей Stars: {e}", exc_info=True)
    
    # Отменяем подписку в БД
    # Также обнуляем payment_method_id, чтобы прекратить автоплатежи через YooKassa
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET is_active = FALSE, cancelled_at = %s, payment_method_id = NULL
            WHERE id = %s AND user_id = %s
        """, (datetime.now(pytz.UTC), subscription_id, user_id))
        conn.commit()
        return cursor.rowcount > 0


def has_subscription_feature(chat_id, user_id, feature_type):
    """Проверяет, есть ли у пользователя/чата доступ к функции"""
    # Специальный доступ для создателя бота (@zap_nikita)
    if user_id == 301810276:
        return True
    
    with db_lock:
        # Проверяем персональную подписку
        cursor.execute("""
            SELECT 1 FROM subscriptions s
            JOIN subscription_features sf ON s.id = sf.subscription_id
            WHERE s.chat_id = %s AND s.user_id = %s 
            AND s.subscription_type = 'personal' AND s.is_active = TRUE
            AND (s.expires_at IS NULL OR s.expires_at > NOW())
            AND sf.feature_type = %s
            LIMIT 1
        """, (chat_id, user_id, feature_type))
        if cursor.fetchone():
            return True
        
        # Проверяем групповую подписку
        cursor.execute("""
            SELECT s.id, s.group_size 
            FROM subscriptions s
            JOIN subscription_features sf ON s.id = sf.subscription_id
            WHERE s.chat_id = %s 
            AND s.subscription_type = 'group' 
            AND s.is_active = TRUE 
            AND (s.expires_at IS NULL OR s.expires_at > NOW())
            AND sf.feature_type = %s
            LIMIT 1
        """, (chat_id, feature_type))
        sub_row = cursor.fetchone()
        
        if not sub_row:
            return False
        
        # Безопасно извлекаем значения
        if isinstance(sub_row, dict):
            subscription_id = sub_row['id']
            group_size = sub_row.get('group_size')  # .get() — безопасно, если нет ключа
        else:
            subscription_id = sub_row.get("id") if isinstance(sub_row, dict) else (sub_row[0] if sub_row else None)
            group_size = sub_row[1] if len(sub_row) > 1 else None  # если только id вернулся
        
        # Если есть ограничение по участникам — проверяем membership
        if group_size is not None:
            cursor.execute("""
                SELECT 1 FROM subscription_members
                WHERE subscription_id = %s AND user_id = %s
                LIMIT 1
            """, (subscription_id, user_id))
            if not cursor.fetchone():
                return False
        
        return True


def check_user_in_group(bot, user_id, group_username):
    """Проверяет, состоит ли пользователь и бот в группе"""
    try:
        # Получаем информацию о чате по username
        chat = bot.get_chat(f"@{group_username}")
        if chat.type not in ['group', 'supergroup']:
            return False
        
        # Проверяем, является ли пользователь участником
        try:
            member = bot.get_chat_member(chat.id, user_id)
            return member.status in ['member', 'administrator', 'creator']
        except:
            return False
    except:
        return False


def get_active_group_users(chat_id, bot_id=None):
    """Получает список активных пользователей группы (кто отправлял запросы или присоединился)"""
    with db_lock:
        # Получаем пользователей из stats (кто отправлял запросы)
        if bot_id:
            cursor.execute("""
                SELECT DISTINCT user_id, username 
                FROM stats 
                WHERE chat_id = %s AND user_id IS NOT NULL AND user_id != %s
            """, (chat_id, bot_id))
        else:
            cursor.execute("""
                SELECT DISTINCT user_id, username 
                FROM stats 
                WHERE chat_id = %s AND user_id IS NOT NULL
            """, (chat_id,))
        users = {}
        for row in cursor.fetchall():
            if isinstance(row, dict):
                user_id = row.get('user_id')
                username = row.get('username')
            else:
                user_id = row.get("user_id") if isinstance(row, dict) else (row[0] if row and len(row) > 0 else None)
                username = row[1] if len(row) > 1 else None
            if user_id:
                users[user_id] = username or f"user_{user_id}"
        
        return users


    """Получает список групп, где есть и пользователь, и бот"""
    groups = []
    with db_lock:
        # Получаем группы из stats, где пользователь был активен
        cursor.execute("""
            SELECT DISTINCT chat_id, username
            FROM stats 
            WHERE user_id = %s AND chat_id < 0
            ORDER BY chat_id
        """, (user_id,))
        
        for row in cursor.fetchall():
            if isinstance(row, dict):
                chat_id = row.get('chat_id')
                username = row.get('username')
            else:
                chat_id = row.get("chat_id") if isinstance(row, dict) else (row[0] if row and len(row) > 0 else None)
                username = row[1] if len(row) > 1 else None
            
def get_user_groups(user_id, bot=None):
    """Получает список групп, где есть и пользователь, и бот (если bot передан)"""
    groups = []
    with db_lock:
        # Получаем группы из stats, где пользователь был активен
        cursor.execute("""
            SELECT DISTINCT chat_id, username
            FROM stats 
            WHERE user_id = %s AND chat_id < 0
            ORDER BY chat_id
        """, (user_id,))
        
        for row in cursor.fetchall():
            if isinstance(row, dict):
                chat_id = row.get('chat_id')
                username = row.get('username')
            else:
                chat_id = row.get("chat_id") if isinstance(row, dict) else (row[0] if row and len(row) > 0 else None)
                username = row[1] if len(row) > 1 else None
            
            if chat_id and chat_id < 0:  # Только группы (отрицательные ID)
                if bot:
                    # Проверяем, что бот состоит в группе
                    try:
                        chat = bot.get_chat(chat_id)
                        if chat.type in ['group', 'supergroup']:
                            groups.append({
                                'chat_id': chat_id,
                                'title': chat.title,
                                'username': chat.username or username
                            })
                    except Exception as e:
                        logger.warning(f"Не удалось получить информацию о группе {chat_id}: {e}")
                        continue
                else:
                    # Если бот не передан — возвращаем без проверки (для совместимости)
                    groups.append({
                        'chat_id': chat_id,
                        'title': None,
                        'username': username
                    })
    
    return groups


def get_subscription_by_id(subscription_id):
    """Получает подписку по ID"""
    with db_lock:
        cursor.execute("""
            SELECT * FROM subscriptions 
            WHERE id = %s
        """, (subscription_id,))
        return cursor.fetchone()


def get_subscription_members(subscription_id):
    """Получает список участников подписки"""
    with db_lock:
        cursor.execute("""
            SELECT user_id, username FROM subscription_members
            WHERE subscription_id = %s
        """, (subscription_id,))
        members = {}
        for row in cursor.fetchall():
            # RealDictCursor возвращает словари с ключами-именами колонок
            user_id = row.get('user_id') if isinstance(row, dict) else row[0]
            username = row.get('username') if isinstance(row, dict) else row[1]
            members[user_id] = username or f"user_{user_id}"
        return members


def add_subscription_member(subscription_id, user_id, username=None):
    """Добавляет участника в подписку"""
    with db_lock:
        cursor.execute("""
            INSERT INTO subscription_members (subscription_id, user_id, username)
            VALUES (%s, %s, %s)
            ON CONFLICT (subscription_id, user_id) DO NOTHING
        """, (subscription_id, user_id, username))
        conn.commit()
        return cursor.rowcount > 0


def update_subscription_group_size(subscription_id, new_group_size, additional_price):
    """Обновляет размер группы подписки и добавляет доплату"""
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET group_size = %s, price = price + %s
            WHERE id = %s
        """, (new_group_size, additional_price, subscription_id))
        conn.commit()
        return cursor.rowcount > 0


def update_subscription_price(subscription_id, new_price):
    """Обновляет цену подписки"""
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET price = %s
            WHERE id = %s
        """, (new_price, subscription_id))
        conn.commit()
        return cursor.rowcount > 0


def update_subscription_plan_type(subscription_id, new_plan_type, new_price):
    """Обновляет тип плана и цену подписки (для изменения со следующего платежа)"""
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET plan_type = %s, price = %s
            WHERE id = %s
        """, (new_plan_type, new_price, subscription_id))
        conn.commit()
        return True


def update_subscription_next_payment(subscription_id, next_payment_date):
    """Обновляет дату следующего платежа подписки"""
    with db_lock:
        cursor.execute("""
            UPDATE subscriptions 
            SET next_payment_date = %s
            WHERE id = %s
        """, (next_payment_date, subscription_id))
        conn.commit()
        return cursor.rowcount > 0


def remove_subscription_member(subscription_id, user_id):
    """Удаляет участника из подписки"""
    with db_lock:
        cursor.execute("""
            DELETE FROM subscription_members
            WHERE subscription_id = %s AND user_id = %s
        """, (subscription_id, user_id))
        conn.commit()
        return cursor.rowcount > 0


def save_payment(payment_id, yookassa_payment_id, user_id, chat_id, subscription_type, plan_type, period_type, group_size, amount, status='pending'):
    """Сохраняет информацию о платеже"""
    with db_lock:
        cursor.execute("""
            INSERT INTO payments (payment_id, yookassa_payment_id, user_id, chat_id, subscription_type, plan_type, period_type, group_size, amount, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (payment_id) DO UPDATE SET
                yookassa_payment_id = EXCLUDED.yookassa_payment_id,
                status = EXCLUDED.status,
                updated_at = NOW()
        """, (payment_id, yookassa_payment_id, user_id, chat_id, subscription_type, plan_type, period_type, group_size, amount, status))
        conn.commit()
        return cursor.rowcount > 0


def update_payment_status(payment_id, status, subscription_id=None):
    """Обновляет статус платежа"""
    with db_lock:
        if subscription_id:
            cursor.execute("""
                UPDATE payments 
                SET status = %s, subscription_id = %s, updated_at = NOW()
                WHERE payment_id = %s
            """, (status, subscription_id, payment_id))
        else:
            cursor.execute("""
                UPDATE payments 
                SET status = %s, updated_at = NOW()
                WHERE payment_id = %s
            """, (status, payment_id))
        conn.commit()
        return cursor.rowcount > 0


def get_payment_by_yookassa_id(yookassa_payment_id):
    """Получает платеж по ID из ЮKassa"""
    with db_lock:
        cursor.execute("""
            SELECT * FROM payments 
            WHERE yookassa_payment_id = %s
        """, (yookassa_payment_id,))
        row = cursor.fetchone()
        if row:
            if isinstance(row, dict):
                return dict(row)
            else:
                return {
                    'id': row.get('id') if isinstance(row, dict) else row[0],
                    'payment_id': row[1],
                    'yookassa_payment_id': row[2],
                    'user_id': row[3],
                    'chat_id': row[4],
                    'subscription_type': row[5],
                    'plan_type': row[6],
                    'period_type': row[7],
                    'group_size': row[8],
                    'amount': row[9],
                    'status': row[10],
                    'subscription_id': row[11],
                    'created_at': row[12],
                    'updated_at': row[13]
                }
        return None


def get_admin_statistics():
    """Получает статистику для администратора бота"""
    from datetime import datetime, timedelta
    import pytz
    
    stats = {}
    
    try:
        with db_lock:
            # Активные пользователи (кто отправлял запросы за последние 30 дней)
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) as count
                FROM stats
                WHERE user_id > 0 AND timestamp >= NOW() - INTERVAL '30 days'
            ''')
            row = cursor.fetchone()
            stats['active_users_30d'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Активные группы (группы, где были запросы за последние 30 дней)
            cursor.execute('''
                SELECT COUNT(DISTINCT chat_id) as count
                FROM stats
                WHERE chat_id < 0 AND timestamp >= NOW() - INTERVAL '30 days'
            ''')
            row = cursor.fetchone()
            stats['active_groups_30d'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Всего пользователей (кто когда-либо отправлял запросы)
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) as count
                FROM stats
                WHERE user_id > 0
            ''')
            row = cursor.fetchone()
            stats['total_users'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Всего групп
            cursor.execute('''
                SELECT COUNT(DISTINCT chat_id) as count
                FROM stats
                WHERE chat_id < 0
            ''')
            row = cursor.fetchone()
            stats['total_groups'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Запросы к API Кинопоиска за день
            try:
                cursor.execute('''
                    SELECT COUNT(*) as count
                    FROM kinopoisk_api_logs
                    WHERE timestamp >= CURRENT_DATE
                ''')
                row = cursor.fetchone()
                stats['kp_api_requests_day'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            except Exception as e:
                logger.warning(f"Ошибка получения статистики API за день: {e}")
                stats['kp_api_requests_day'] = 0
            
            # Запросы к API Кинопоиска за неделю
            try:
                cursor.execute('''
                    SELECT COUNT(*) as count
                    FROM kinopoisk_api_logs
                    WHERE timestamp >= NOW() - INTERVAL '7 days'
                ''')
                row = cursor.fetchone()
                stats['kp_api_requests_week'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            except Exception as e:
                logger.warning(f"Ошибка получения статистики API за неделю: {e}")
                stats['kp_api_requests_week'] = 0
            
            # Запросы к API Кинопоиска за месяц
            try:
                cursor.execute('''
                    SELECT COUNT(*) as count
                    FROM kinopoisk_api_logs
                    WHERE timestamp >= NOW() - INTERVAL '30 days'
                ''')
                row = cursor.fetchone()
                stats['kp_api_requests_month'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            except Exception as e:
                logger.warning(f"Ошибка получения статистики API за месяц: {e}")
                stats['kp_api_requests_month'] = 0
            
            # Всего запросов к API Кинопоиска
            try:
                cursor.execute('SELECT COUNT(*) as count FROM kinopoisk_api_logs')
                row = cursor.fetchone()
                stats['kp_api_requests_total'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            except Exception as e:
                logger.warning(f"Ошибка получения общей статистики API: {e}")
                stats['kp_api_requests_total'] = 0
            
            # Платные пользователи (активные подписки personal)
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) as count
                FROM subscriptions
                WHERE subscription_type = 'personal' AND is_active = TRUE 
                AND (expires_at IS NULL OR expires_at > NOW())
            ''')
            row = cursor.fetchone()
            stats['paid_users'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Платные группы (активные подписки group)
            cursor.execute('''
                SELECT COUNT(DISTINCT chat_id) as count
                FROM subscriptions
                WHERE subscription_type = 'group' AND is_active = TRUE 
                AND (expires_at IS NULL OR expires_at > NOW())
            ''')
            row = cursor.fetchone()
            stats['paid_groups'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Всего фильмов в базе
            cursor.execute('SELECT COUNT(*) as count FROM movies')
            row = cursor.fetchone()
            stats['total_movies'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Всего планов
            cursor.execute('SELECT COUNT(*) as count FROM plans')
            row = cursor.fetchone()
            stats['total_plans'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Всего оценок
            cursor.execute('SELECT COUNT(*) as count FROM ratings')
            row = cursor.fetchone()
            stats['total_ratings'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Запросы пользователей за день
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM stats
                WHERE timestamp >= CURRENT_DATE
            ''')
            row = cursor.fetchone()
            stats['user_requests_day'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Запросы пользователей за неделю
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM stats
                WHERE timestamp >= NOW() - INTERVAL '7 days'
            ''')
            row = cursor.fetchone()
            stats['user_requests_week'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Запросы пользователей за месяц
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM stats
                WHERE timestamp >= NOW() - INTERVAL '30 days'
            ''')
            row = cursor.fetchone()
            stats['user_requests_month'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Топ команд за день
            cursor.execute('''
                SELECT command_or_action, COUNT(*) as count
                FROM stats
                WHERE timestamp >= CURRENT_DATE
                GROUP BY command_or_action
                ORDER BY count DESC
                LIMIT 5
            ''')
            stats['top_commands_day'] = cursor.fetchall()
            
            # Топ команд за неделю
            cursor.execute('''
                SELECT command_or_action, COUNT(*) as count
                FROM stats
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                GROUP BY command_or_action
                ORDER BY count DESC
                LIMIT 5
            ''')
            stats['top_commands_week'] = cursor.fetchall()
            
            # Новые пользователи за день (кто впервые появился в stats за день)
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) as count
                FROM stats s1
                WHERE s1.user_id > 0 
                AND s1.timestamp >= CURRENT_DATE
                AND NOT EXISTS (
                    SELECT 1 FROM stats s2 
                    WHERE s2.user_id = s1.user_id 
                    AND s2.timestamp < CURRENT_DATE
                )
            ''')
            row = cursor.fetchone()
            stats['new_users_day'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Новые пользователи за неделю (кто впервые появился в stats за неделю)
            cursor.execute('''
                SELECT COUNT(DISTINCT user_id) as count
                FROM stats s1
                WHERE s1.user_id > 0 
                AND s1.timestamp >= NOW() - INTERVAL '7 days'
                AND NOT EXISTS (
                    SELECT 1 FROM stats s2 
                    WHERE s2.user_id = s1.user_id 
                    AND s2.timestamp < NOW() - INTERVAL '7 days'
                )
            ''')
            row = cursor.fetchone()
            stats['new_users_week'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Новые платные подписки за день
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM subscriptions
                WHERE activated_at >= CURRENT_DATE
                AND is_active = TRUE
            ''')
            row = cursor.fetchone()
            stats['new_subscriptions_day'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Новые платные подписки за неделю
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM subscriptions
                WHERE activated_at >= NOW() - INTERVAL '7 days'
                AND is_active = TRUE
            ''')
            row = cursor.fetchone()
            stats['new_subscriptions_week'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
            # Отписавшиеся за неделю
            cursor.execute('''
                SELECT COUNT(*) as count
                FROM subscriptions
                WHERE cancelled_at >= NOW() - INTERVAL '7 days'
                AND cancelled_at IS NOT NULL
            ''')
            row = cursor.fetchone()
            stats['cancelled_subscriptions_week'] = row.get('count') if isinstance(row, dict) else (row[0] if row else 0)
            
    except Exception as e:
        logger.error(f"Ошибка получения статистики: {e}", exc_info=True)
        stats['error'] = str(e)
    
    return stats


def is_bot_participant(chat_id, user_id):
    """Проверяет, является ли пользователь участником бота (есть ли запись в stats)"""
    try:
        with db_lock:
            cursor.execute('''
                SELECT COUNT(*) FROM stats 
                WHERE chat_id = %s AND user_id = %s
            ''', (chat_id, user_id))
            count = cursor.fetchone()
            return (count.get('count') if isinstance(count, dict) else count[0]) > 0
    except Exception as e:
        logger.error(f"[IS_BOT_PARTICIPANT] Ошибка: {e}")
        return False


def add_and_announce(link, chat_id, user_id=None, source='unknown'):
    """Обрабатывает присланную ссылку на фильм/сериал.
    Показывает соответствующую карточку в зависимости от наличия фильма в базе.
    НЕ добавляет фильм автоматически в базу при обработке ссылки."""
    
    from moviebot.api.kinopoisk_api import extract_movie_info
    from moviebot.bot.bot_init import bot
    from moviebot.bot.handlers.series import show_film_info_with_buttons

    info = extract_movie_info(link)
    if not info:
        logger.warning(f"[ADD_AND_ANNOUNCE] Не удалось получить данные о фильме: {link}")
        try:
            bot.send_message(chat_id, "❌ Не удалось загрузить информацию о фильме. Проверьте ссылку.")
        except:
            pass
        return False

    kp_id = info.get('kp_id')
    if not kp_id:
        logger.warning(f"[ADD_AND_ANNOUNCE] kp_id не найден")
        return False

    logger.info(f"[ADD_AND_ANNOUNCE] Обработка kp_id={kp_id}, chat_id={chat_id}")

    # Проверяем, есть ли фильм в базе
    # ВАЖНО: Используем локальные соединения вместо глобальных
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    existing = None
    
    try:
        with db_lock:
            cursor_local.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
            row = cursor_local.fetchone()
            if row:
                # Конвертируем DictRow в кортеж для совместимости с show_film_info_with_buttons
                if isinstance(row, dict):
                    existing = (row.get('id'), row.get('title'), row.get('watched', 0))
                else:
                    existing = (row[0], row[1], row[2] if len(row) > 2 else 0)
    except Exception as db_e:
        logger.error(f"[ADD_AND_ANNOUNCE] Ошибка БД при проверке существования: {db_e}", exc_info=True)
        try:
            conn_local.rollback()
        except:
            pass
    finally:
        # Закрываем локальные соединения
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

    try:
        # Всегда используем одну функцию — она умеет показывать и новые, и существующие
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=existing  # передаём None или кортеж — функция внутри разберётся
        )
        
        logger.info(f"[ADD_AND_ANNOUNCE] Карточка показана для kp_id={kp_id}")
        return True

    except Exception as e:
        logger.error(f"[ADD_AND_ANNOUNCE] Ошибка при показе карточки: {e}", exc_info=True)
        # Фолбек — простое сообщение с названием и ссылкой
        try:
            title = info.get('title', 'Фильм')
            bot.send_message(
                chat_id,
                f"🎬 <b>{title}</b>\n\n<a href='{link}'>Кинопоиск</a>",
                parse_mode='HTML',
                disable_web_page_preview=False
            )
        except Exception as send_e:
            logger.error(f"[ADD_AND_ANNOUNCE] Не удалось отправить фолбек: {send_e}")

    return True