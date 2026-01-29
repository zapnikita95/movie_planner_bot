from moviebot.bot.bot_init import bot, BOT_ID
"""
Обработчики команд связанных с сериалами, поиском, рандомом, премьерами, билетами, настройками и помощью
"""
import logging
import re
import random
import threading
import requests
import pytz
import time
from datetime import datetime, date
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton  
from telebot.apihelper import ApiTelegramException 
from moviebot.bot.handlers.text_messages import is_expected_text_in_private
from moviebot.database.db_operations import (

    log_request, get_user_timezone_or_default, set_user_timezone,
    get_watched_emojis, get_user_timezone, get_notification_settings, set_notification_setting
)
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.database.db_operations import get_user_timezone_or_default, get_user_films_count
from moviebot.utils.helpers import extract_film_info_from_existing
from moviebot.api.kinopoisk_api import search_films, extract_movie_info, get_premieres_for_period, get_seasons_data, search_films_by_filters, get_film_distribution, search_persons, get_staff
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access, has_notifications_access, has_pro_access
from moviebot.utils.parsing import parse_plan_date_text
from moviebot.bot.handlers.seasons import get_series_airing_status, count_episodes_for_watch_check

from moviebot.config import KP_TOKEN, PLANS_TZ, TOKEN

from moviebot.states import (

    user_search_state, user_random_state, user_ticket_state,
    user_settings_state, settings_messages, bot_messages, added_movie_messages,
    dice_game_state, user_import_state
)
from moviebot.bot.handlers.text_messages import expect_text_from_user

from moviebot.utils.parsing import extract_kp_id_from_text, show_timezone_selection, extract_kp_user_id

logger = logging.getLogger(__name__)

# Жанры, которые нужно исключать из режимов рандома
EXCLUDED_GENRES = ['музыка', 'короткометражка', 'реальное тв', 'церемония', 'концерт', 'ток-шоу']
random_plan_data = {}  # user_id → данные для планирования рандомного фильма

# Обработчик выбора типа поиска (фильм/сериал) - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
# КРИТИЧЕСКИ ВАЖНО: Этот обработчик регистрируется при импорте модуля
logger.info("=" * 80)
logger.info(f"[SEARCH TYPE HANDLER] Регистрация обработчика search_type_callback")
logger.info(f"[SEARCH TYPE HANDLER] id(bot)={id(bot)}")
logger.info("=" * 80)

def get_film_current_state(chat_id, kp_id, user_id=None):
    """
    Получает актуальное состояние фильма/сериала из базы данных.
    
    Returns:
        dict с ключами:
        - film_id: int или None
        - existing: tuple (film_id, title, watched) или None
        - plan_info: dict с ключами 'id', 'type', 'date' или None
        - has_tickets: bool (True если у плана в кино есть билеты)
        - is_subscribed: bool (для сериалов, True если пользователь подписан)
    """
    logger.info(f"[GET FILM STATE] ===== START: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}")
    kp_id_str = str(kp_id)
    film_id = None
    existing = None
    plan_info = None
    has_tickets = False
    is_subscribed = False
    
    # ВАЖНО: Используем локальные соединения вместо глобальных
    from moviebot.database.db_connection import get_db_connection, db_lock
    from psycopg2.extras import RealDictCursor
    conn_local = None
    cursor_local = None
    
    # Инициализируем plan_data в начале функции, чтобы избежать UnboundLocalError
    plan_data = None
    
    try:
        logger.info(f"[GET FILM STATE] Получение локального соединения...")
        conn_local = get_db_connection()
        # Создаем локальный курсор из локального соединения, а не используем глобальный
        # Это предотвращает ошибку "cursor already closed" при параллельных вызовах
        cursor_local = conn_local.cursor(cursor_factory=RealDictCursor)
        logger.info(f"[GET FILM STATE] Локальное соединение и курсор получены")
        
        logger.info(f"[GET FILM STATE] Попытка получить db_lock...")
        with db_lock:
            logger.info(f"[GET FILM STATE] db_lock получен, выполняем запросы")
            # Получаем информацию о фильме
            cursor_local.execute("""
                SELECT id, title, watched, is_series
                FROM movies 
                WHERE chat_id = %s AND kp_id = %s
            """, (chat_id, kp_id_str))
            film_row = cursor_local.fetchone()
            logger.info(f"[GET FILM STATE] Запрос к movies выполнен, film_row={film_row is not None}")
            
            if film_row:
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                watched = bool(film_row.get('watched') if isinstance(film_row, dict) else film_row[2])
                existing = (film_id, title, watched)
                logger.info(f"[GET FILM STATE] Фильм найден: film_id={film_id}, title={title}, watched={watched}")
                
                # Проверяем план для этого фильма
                logger.info(f"[GET FILM STATE] Проверка плана для film_id={film_id}")
                cursor_local.execute("""
                    SELECT id, plan_type, plan_datetime, ticket_file_id
                    FROM plans 
                    WHERE film_id = %s AND chat_id = %s 
                    LIMIT 1
                """, (film_id, chat_id))
                plan_row = cursor_local.fetchone()
                logger.info(f"[GET FILM STATE] Запрос к plans выполнен, plan_row={plan_row is not None}")
                
                # Сохраняем данные плана для обработки ВНЕ db_lock
                plan_data = None
                if plan_row:
                    plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                    plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[1]
                    plan_dt_value = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[2]
                    ticket_file_id = plan_row.get('ticket_file_id') if isinstance(plan_row, dict) else (plan_row[3] if len(plan_row) > 3 else None)
                    plan_data = {
                        'id': plan_id,
                        'type': plan_type,
                        'datetime': plan_dt_value,
                        'ticket_file_id': ticket_file_id
                    }
                    logger.info(f"[GET FILM STATE] Данные плана сохранены: plan_id={plan_id}, plan_type={plan_type}")
                else:
                    logger.info(f"[GET FILM STATE] План не найден для film_id={film_id}")
            
            # Для сериалов проверяем подписку (внутри db_lock, но это безопасно)
            if film_row:
                is_series_db = bool(film_row.get('is_series') if isinstance(film_row, dict) else (film_row[3] if len(film_row) > 3 else 0))
                logger.info(f"[GET FILM STATE] is_series_db={is_series_db}, user_id={user_id}")
                if is_series_db and user_id and film_id:
                    query_user = user_id if user_id is not None else None
                    logger.info(f"[GET FILM STATE] Проверка подписки для сериала: film_id={film_id}, user_id={query_user}")
                    cursor_local.execute("""
                        SELECT subscribed 
                        FROM series_subscriptions 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        LIMIT 1
                    """, (chat_id, film_id, query_user))
                    sub_row = cursor_local.fetchone()
                    if sub_row:
                        is_subscribed = bool(sub_row[0] if isinstance(sub_row, tuple) else sub_row.get('subscribed'))
                        logger.info(f"[GET FILM STATE] Подписка найдена: is_subscribed={is_subscribed}")
                    else:
                        logger.info(f"[GET FILM STATE] Подписка не найдена")
        
        # ВАЖНО: Обрабатываем данные плана ВНЕ db_lock, чтобы избежать дедлока при вызове get_user_timezone_or_default
        if plan_data:
            logger.info(f"[GET FILM STATE] Обработка данных плана ВНЕ db_lock...")
            plan_id = plan_data['id']
            plan_type = plan_data['type']
            plan_dt_value = plan_data['datetime']
            ticket_file_id = plan_data['ticket_file_id']
            
            # Форматируем дату (ВНЕ db_lock, чтобы избежать дедлока)
            date_str = "не указана"
            if plan_dt_value and user_id:
                try:
                    # ВАЖНО: Вызываем get_user_timezone_or_default ВНЕ db_lock, чтобы избежать дедлока
                    logger.info(f"[GET FILM STATE] Вызов get_user_timezone_or_default для user_id={user_id}")
                    user_tz = get_user_timezone_or_default(user_id)
                    logger.info(f"[GET FILM STATE] Часовой пояс получен: {user_tz}")
                    if isinstance(plan_dt_value, datetime):
                        if plan_dt_value.tzinfo is None:
                            dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                        else:
                            dt = plan_dt_value.astimezone(user_tz)
                    else:
                        dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
                    date_str = dt.strftime('%d.%m.%Y %H:%M')
                    logger.info(f"[GET FILM STATE] Дата отформатирована: {date_str}")
                except Exception as e:
                    logger.warning(f"[GET FILM STATE] Ошибка парсинга plan_datetime: {e}", exc_info=True)
                    date_str = str(plan_dt_value)[:16] if plan_dt_value else "не указана"
            
            plan_info = {
                'id': plan_id,
                'type': plan_type,
                'date': date_str
            }
            logger.info(f"[GET FILM STATE] plan_info создан: {plan_info}")
            
            # Проверяем наличие билетов для планов в кино
            if plan_type == 'cinema' and ticket_file_id:
                import json
                try:
                    # ticket_file_id может быть JSON массивом или строкой
                    tickets_data = json.loads(ticket_file_id) if isinstance(ticket_file_id, str) else ticket_file_id
                    if isinstance(tickets_data, list) and len(tickets_data) > 0:
                        has_tickets = True
                    elif tickets_data and isinstance(tickets_data, str) and tickets_data.strip():
                        has_tickets = True
                except:
                    # Если не JSON, проверяем как строку
                    if ticket_file_id and str(ticket_file_id).strip():
                        has_tickets = True
            logger.info(f"[GET FILM STATE] has_tickets={has_tickets}")
        else:
            logger.info(f"[GET FILM STATE] Нет данных плана для обработки")
            
        if not film_row:
            logger.info(f"[GET FILM STATE] Фильм не найден в базе")
    
    except Exception as e:
        logger.error(f"[GET FILM STATE] ❌ Ошибка получения состояния: {e}", exc_info=True)
    finally:
        # Закрываем локальные соединения, если они были созданы
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
    
    result = {
        'film_id': film_id,
        'existing': existing,
        'plan_info': plan_info,
        'has_tickets': has_tickets,
        'is_subscribed': is_subscribed
    }
    logger.info(f"[GET FILM STATE] ===== END: existing={existing is not None}, plan_info={plan_info is not None}, has_tickets={has_tickets}, is_subscribed={is_subscribed}")
    return result

def show_film_info_with_buttons(
    chat_id, user_id, info, link, kp_id,
    existing=None, message_id=None, message_thread_id=None,
    override_is_subscribed=None   # ← ДОБАВЛЕН ПАРАМЕТР
):
    """Показывает описание фильма с кнопками действий"""
    import inspect
    import traceback
    
    kp_id = int(kp_id)
    
    # САМОЕ ВАЖНОЕ: фиксируем is_series САМЫМ ПЕРВЫМ ДЕЙСТВИЕМ и больше никогда не меняем!
    is_series = bool(info.get('is_series', False))
    logger.info(f"[SHOW FILM INFO] >>> ФИКСИРУЕМ is_series = {is_series} (из входного info)")

    # Лог с caller'ом (оставляем для дебага)
    logger.info(
        "[SHOW FILM INFO] >>> ВХОД | caller = %s() | file = %s:%d | kp_id=%s | is_series=%s | existing=%s | msg_id=%s | user_id=%s",
        inspect.stack()[1].function,
        inspect.stack()[1].filename.split('/')[-1],
        inspect.stack()[1].lineno,
        kp_id,
        is_series,
        existing,
        message_id,
        user_id
    )

    if message_id:
        try:
            bot.edit_message_text("⏳ Загружаю...", chat_id, message_id)
        except:
            message_id = None

    try:
        # ВАЖНО: Всегда проверяем актуальное состояние из БД для правильного chat_id
        logger.info(f"[SHOW FILM INFO] Проверка состояния фильма: chat_id={chat_id}, kp_id={kp_id}, existing передан={existing is not None}")
        
        # Всегда получаем актуальное состояние из БД для текущего chat_id
        current_state = get_film_current_state(chat_id, kp_id, user_id)
        actual_existing = current_state['existing']
        plan_info = current_state['plan_info']
        has_tickets = current_state['has_tickets']
        
        # Используем override, если передан (важно после подписки/отписки)
        is_subscribed = override_is_subscribed if override_is_subscribed is not None else current_state['is_subscribed']
        logger.info(f"[SHOW FILM INFO] is_subscribed = {is_subscribed} (override={override_is_subscribed is not None})")
        
        # Используем актуальный existing из БД (для правильного chat_id)
        if actual_existing:
            existing = actual_existing
            logger.info(f"[SHOW FILM INFO] Фильм найден в БД для chat_id={chat_id}: existing={existing}")
        else:
            # Фильм не найден в БД для этого chat_id
            existing = None
            logger.info(f"[SHOW FILM INFO] Фильм НЕ найден в БД для chat_id={chat_id}, existing=None")
        
        # Если existing был передан, но не найден в БД для текущего chat_id - это нормально
        # Просто используем None, чтобы показать кнопку "Добавить в базу"
        
        type_emoji = "📺" if is_series else "🎬"
        film_type_text = "Сериал" if is_series else "Фильм"
        logger.info(f"[SHOW FILM INFO] is_series={is_series}, type_emoji={type_emoji}, plan_info={plan_info}, has_tickets={has_tickets}")
        logger.info(f"[SHOW FILM INFO] ===== ФОРМИРОВАНИЕ ТЕКСТА И КНОПОК =====")
        
        # Инициализируем markup заранее, чтобы избежать UnboundLocalError
        markup = InlineKeyboardMarkup()
        logger.info(f"[SHOW FILM INFO] Markup инициализирован")
        
        # Формируем текст описания
        text = ""
        logger.info(f"[SHOW FILM INFO] Начало формирования текста")

        if existing:
            # Защитная распаковка existing
            if len(existing) == 3:
                film_id, title_from_db, watched = existing
            elif len(existing) == 2:
                film_id, title_from_db = existing
                watched = 0
            else:
                logger.error(f"[SHOW FILM INFO] Некорректный existing: {existing}")
                film_id = existing[0] if existing else None
                title_from_db = "Без названия"
                watched = 0

            # Получаем данные из БД, но НЕ перезаписываем is_series!
            db_row = None
            try:
                conn_db = get_db_connection()
                cursor_db = get_db_cursor()
                try:
                    with db_lock:
                        cursor_db.execute("""
                            SELECT title, year, genres, description, director, actors, is_series, online_link
                            FROM movies 
                            WHERE id = %s AND chat_id = %s
                        """, (film_id, chat_id))
                        db_row = cursor_db.fetchone()
                finally:
                    try:
                        cursor_db.close()
                    except:
                        pass
                    try:
                        conn_db.close()
                    except:
                        pass
            except Exception as db_err:
                logger.warning(f"[DB_FETCH] Не удалось получить полные данные: {db_err}")

            if db_row:
                # Обрабатываем как dict или tuple
                if isinstance(db_row, dict):
                    db_is_series = bool(db_row.get('is_series', 0))
                    info = {
                        'title': db_row.get('title') or title_from_db,
                        'year': db_row.get('year'),
                        'genres': db_row.get('genres'),
                        'description': db_row.get('description'),
                        'director': db_row.get('director'),
                        'actors': db_row.get('actors'),
                        'is_series': is_series  # ← важно! используем фиксированное значение
                    }
                else:
                    # tuple/list
                    db_is_series = bool(db_row[6] if len(db_row) > 6 else 0)
                    info = {
                        'title': db_row[0] if len(db_row) > 0 else title_from_db,
                        'year': db_row[1] if len(db_row) > 1 else None,
                        'genres': db_row[2] if len(db_row) > 2 else None,
                        'description': db_row[3] if len(db_row) > 3 else None,
                        'director': db_row[4] if len(db_row) > 4 else None,
                        'actors': db_row[5] if len(db_row) > 5 else None,
                        'is_series': is_series  # ← важно! используем фиксированное значение
                    }
                
                if db_is_series != is_series:
                    logger.warning(f"[SHOW FILM INFO] Конфликт is_series! API/info = {is_series}, БД = {db_is_series}. Оставляем значение из info: {is_series}")
            else:
                info = info or {}
                info['title'] = title_from_db
                info['is_series'] = is_series  # защита

            text += f"✅ <b>{film_type_text} уже в базе</b>\n\n"

        # Основной текст
        type_emoji = "📺" if is_series else "🎬"  # ещё раз, на всякий
        year = info.get('year')
        year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' else ""
        text += f"{type_emoji} <b>{info.get('title', 'Без названия')}</b>{year_str}\n"

        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"

        # Статус выхода серий — только если сериал И у пользователя есть отмеченные серии
        # ОПТИМИЗАЦИЯ: Загружаем статус асинхронно, не блокируя показ описания
        if is_series:
            logger.info(f"[SHOW_FILM] Сериал! kp_id={kp_id}")
            
            # Проверяем, есть ли у пользователя отмеченные серии по этому сериалу
            has_watched_episodes = False
            logger.info(f"[SHOW FILM INFO] Проверка отмеченных серий: is_series={is_series}, existing={existing}, user_id={user_id}, chat_id={chat_id}")
            if existing and user_id:
                # existing - это кортеж (film_id, title, watched) или (film_id, _, watched)
                film_id_for_check = None
                if isinstance(existing, (list, tuple)) and len(existing) > 0:
                    film_id_for_check = existing[0]  # Первый элемент - film_id
                elif isinstance(existing, dict) and 'film_id' in existing:
                    film_id_for_check = existing['film_id']
                elif isinstance(existing, (int, str)):
                    film_id_for_check = existing
                
                logger.info(f"[SHOW FILM INFO] Извлечен film_id_for_check={film_id_for_check} из existing={existing} (тип: {type(existing)})")
                if film_id_for_check:
                    try:
                        # Используем импорт из начала файла, не создаем локальный
                        conn_check = get_db_connection()
                        cursor_check = get_db_cursor()
                        try:
                            with db_lock:
                                cursor_check.execute("""
                                    SELECT COUNT(*) as count
                                    FROM series_tracking 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                                    LIMIT 1
                                """, (chat_id, film_id_for_check, user_id))
                                row = cursor_check.fetchone()
                                if row:
                                    count = row.get('count') if isinstance(row, dict) else row[0]
                                    has_watched_episodes = (count or 0) > 0
                                    logger.info(f"[SHOW FILM INFO] Проверка отмеченных серий: film_id={film_id_for_check}, has_watched_episodes={has_watched_episodes}")
                        finally:
                            try:
                                cursor_check.close()
                            except:
                                pass
                            try:
                                conn_check.close()
                            except:
                                pass
                    except Exception as check_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка проверки отмеченных серий: {check_e}", exc_info=True)
            
            # Показываем заглушку только если есть отмеченные серии
            if has_watched_episodes:
                text += "\n\n"
                text += f"⏳ <b>Загрузка статуса серий...</b>\n"
                # Функция load_series_status_async будет создана после формирования всех кнопок
                should_load_status_async = True
                logger.info(f"[SHOW FILM INFO] ✅ Есть отмеченные серии, будет загружен статус (should_load_status_async=True)")
            else:
                should_load_status_async = False
                logger.info(f"[SHOW FILM INFO] ❌ У пользователя нет отмеченных серий по сериалу kp_id={kp_id}, статус не показываем")
            
            # Статус подписки для сериалов
            if user_id:
                if is_subscribed:
                    text += f"\n🔔 <b>Статус подписки: ✅ Подписан</b>"
                else:
                    text += f"\n🔔 <b>Статус подписки: ❌ Не подписан</b>"

        text += f"\n<a href='{link}'>Кинопоиск</a>"
        logger.info(f"[SHOW FILM INFO] Основной текст сформирован, длина={len(text)}")

        # Просмотрено / не просмотрено + оценки
        logger.info(f"[SHOW FILM INFO] Проверка existing: {existing}")
        if existing:
            if watched:
                text += "\n\n✅ <b>Просмотрено</b>"
                try:
                    conn_local = get_db_connection()
                    cursor_local = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_local.execute("""
                                SELECT AVG(rating) as avg 
                                FROM ratings 
                                WHERE chat_id = %s AND film_id = %s 
                                AND (is_imported = FALSE OR is_imported IS NULL)
                            """, (chat_id, film_id))
                            avg_result = cursor_local.fetchone()
                            avg = avg_result[0] if avg_result else None
                            if avg:
                                text += f"\n⭐ <b>Средняя оценка: {avg:.1f}/10</b>"
                    finally:
                        try:
                            cursor_local.close()
                        except:
                            pass
                        try:
                            conn_local.close()
                        except:
                            pass

                    if user_id:
                        conn_local = get_db_connection()
                        cursor_local = get_db_cursor()
                        try:
                            with db_lock:
                                cursor_local.execute("""
                                    SELECT rating 
                                    FROM ratings 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                                    AND (is_imported = FALSE OR is_imported IS NULL)
                                """, (chat_id, film_id, user_id))
                                user_rating_row = cursor_local.fetchone()
                                user_rating = user_rating_row[0] if user_rating_row else None
                                text += f"\n⭐ <b>Ваша оценка: {user_rating if user_rating else '—'}/10</b>"
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
                    logger.warning(f"[SHOW FILM INFO] Ошибка оценок: {e}")
            else:
                text += "\n\n⏳ <b>Ещё не просмотрено</b>"
                if user_id:
                    try:
                        conn_local = get_db_connection()
                        cursor_local = get_db_cursor()
                        try:
                            with db_lock:
                                cursor_local.execute("""
                                    SELECT rating 
                                    FROM ratings 
                                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                                    AND (is_imported = FALSE OR is_imported IS NULL)
                                """, (chat_id, film_id, user_id))
                                user_rating_row = cursor_local.fetchone()
                                user_rating = user_rating_row[0] if user_rating_row else None
                                text += f"\n⭐ <b>Ваша оценка: {user_rating if user_rating else '—'}/10</b>"
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
                        logger.warning(f"[SHOW FILM INFO] Ошибка личной оценки: {e}")
            
            # Добавляем информацию о планировании, если фильм/сериал запланирован
            if plan_info:
                plan_type_text = "🎦 в кино" if plan_info['type'] == 'cinema' else "🏠 дома"
                text += f"\n\n📅 <b>Запланирован {plan_type_text}</b> на {plan_info['date']}"
                
                # Для запланированных фильмов показываем среднюю оценку, если фильм просмотрен
                if watched and film_id:
                    try:
                        lock_acquired = db_lock.acquire(timeout=3.0)
                        if lock_acquired:
                            try:
                                # Получаем среднюю оценку всех участников
                                conn_local = get_db_connection()
                                cursor_local = get_db_cursor()
                                try:
                                    cursor_local.execute('''
                                        SELECT AVG(rating) as avg FROM ratings 
                                        WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                                    ''', (chat_id, film_id))
                                    avg_result = cursor_local.fetchone()
                                    if avg_result:
                                        avg_rating = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                                        avg_rating = float(avg_rating) if avg_rating is not None else None
                                        if avg_rating:
                                            text += f"\n⭐ <b>Средняя оценка: {avg_rating:.1f}/10</b>"
                                finally:
                                    try:
                                        cursor_local.close()
                                    except:
                                        pass
                                    try:
                                        conn_local.close()
                                    except:
                                        pass
                            finally:
                                db_lock.release()
                    except Exception as avg_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при запросе средней оценки для запланированного фильма: {avg_e}")
            logger.info(f"[SHOW FILM INFO] Обработка existing завершена")
        
        logger.info(f"[SHOW FILM INFO] ===== ЗАГРУЗКА ИСТОЧНИКОВ =====")
        # ОПТИМИЗАЦИЯ: Загружаем источники с коротким таймаутом (500ms)
        # Если загрузились быстро - показываем кнопку, если нет - показываем без нее
        # Это экономит 1-3 секунды на запросе к API
        # threading уже импортирован в начале файла, не нужно импортировать снова
        from moviebot.api.kinopoisk_api import get_external_sources
        import time
        
        sources = None
        has_sources = False
        logger.info(f"[SHOW FILM INFO] Запуск загрузки источников для kp_id={kp_id}")
        
        def load_sources_async():
            """Загружает источники в фоне"""
            nonlocal sources, has_sources
            try:
                sources = get_external_sources(kp_id)
                has_sources = bool(sources)
                logger.info(f"[SHOW FILM INFO] Источники загружены: {len(sources) if sources else 0}")
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка загрузки источников: {e}", exc_info=True)
        
        # Запускаем загрузку источников в фоне
        sources_thread = threading.Thread(target=load_sources_async, daemon=True)
        sources_thread.start()
        
        # Даем немного времени (500ms) для загрузки, но не блокируем показ
        # Если источники не загрузились за это время - показываем без них
        sources_thread.join(timeout=0.5)
        if sources is not None:
            has_sources = bool(sources)
            logger.info(f"[SHOW FILM INFO] Источники загружены быстро: {len(sources) if sources else 0}")
        else:
            logger.info("[SHOW FILM INFO] Источники еще загружаются, показываем описание без кнопки источников")

        # Создаем кнопки
        logger.info(f"[SHOW FILM INFO] ===== СОЗДАНИЕ КНОПОК =====")
        markup = InlineKeyboardMarkup(row_width=2)
        # Флаг для отслеживания, добавлены ли уже кнопки "Факты" и "Оценить"
        facts_and_rate_added = False
        logger.info(f"[SHOW FILM INFO] Markup создан, facts_and_rate_added={facts_and_rate_added}")
        
        # Премьера: дата выхода и кнопка "Уведомить о премьере"
        # Кнопка ТОЛЬКО у фильмов (не сериалов), НЕ в базе, без плана, премьера в будущем.
        logger.info(f"[SHOW FILM INFO] Проверка премьеры...")
        premiere_date = None
        premiere_date_str = ""
        russia_release = info.get('russia_release')

        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        elif not is_series and existing is None:
            dist = get_film_distribution(kp_id)
            if dist:
                premiere_date = dist['date']
                premiere_date_str = dist['date_str']
        if premiere_date is None:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                        date_value = data_main.get(date_field)
                        if date_value:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                                premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                                break
                            except Exception:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения даты премьеры: {e}")

        today = date.today()
        show_premiere_button = (
            not is_series
            and existing is None
            and plan_info is None
            and premiere_date is not None
            and premiere_date > today
        )
        if show_premiere_button:
            logger.info(f"[SHOW FILM INFO] Показываем кнопку «Уведомить о премьере»: kp_id={kp_id}, дата={premiere_date_str}")
        elif premiere_date and premiere_date <= today:
            logger.info(f"[SHOW FILM INFO] Премьера {kp_id} уже прошла ({premiere_date}), кнопку не показываем")

        if show_premiere_button:
            conn_prem = get_db_connection()
            cursor_prem = None
            has_premiere_reminder = False
            try:
                if user_id is not None:
                    with db_lock:
                        cursor_prem = conn_prem.cursor()
                        cursor_prem.execute("""
                            SELECT 1 FROM premiere_reminders
                            WHERE chat_id = %s AND user_id = %s AND kp_id = %s
                        """, (chat_id, user_id, str(kp_id)))
                        has_premiere_reminder = cursor_prem.fetchone() is not None
            finally:
                if cursor_prem:
                    try:
                        cursor_prem.close()
                    except Exception:
                        pass
                try:
                    conn_prem.close()
                except Exception:
                    pass

            callback_date = premiere_date.strftime('%d.%m.%Y')
            if has_premiere_reminder:
                markup.add(InlineKeyboardButton("🔕 Отменить уведомление", callback_data=f"premiere_cancel:{int(kp_id)}"))
            else:
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{callback_date}"))

        # Получаем film_id и watched из existing (уже получено через get_film_current_state)
        logger.info(f"[SHOW FILM INFO] Получение film_id из existing...")
        film_id = None
        watched = False
        if existing:
            film_id, _, watched = existing
            logger.info(f"[SHOW FILM INFO] film_id из existing: {film_id}, watched: {watched}")
        else:
            # existing не передан и не получен из БД - используем данные из current_state
            # Но current_state может быть не определен, если existing был передан
            if 'current_state' in locals():
                film_id = current_state.get('film_id')
                if film_id and 'actual_existing' in locals() and actual_existing:
                    watched = actual_existing[2] if len(actual_existing) > 2 else False
                logger.info(f"[SHOW FILM INFO] film_id из current_state: {film_id}, watched: {watched}")
            else:
                logger.info(f"[SHOW FILM INFO] current_state не определен, existing был передан")
        
        has_plan = plan_info is not None
        logger.info(f"[SHOW FILM INFO] Проверка планов завершена, has_plan={has_plan}, plan_info={plan_info}")
        
        # Добавляем кнопку "Просмотрено" для всех фильмов (даже не добавленных в базу)
        # Кнопка должна работать для всех фильмов, даже если film_id отсутствует
        if not is_series:
            if film_id:
                # Фильм в базе - проверяем статус просмотра
                if watched:
                    markup.add(InlineKeyboardButton("✅ Просмотрено", callback_data=f"toggle_watched_from_description:{film_id}"))
                else:
                    markup.add(InlineKeyboardButton("👁️ Просмотрено", callback_data=f"mark_watched_from_description:{film_id}"))
            else:
                # Фильм не в базе - всегда показываем кнопку "Просмотрено"
                markup.add(InlineKeyboardButton("👁️ Просмотрено", callback_data=f"mark_watched_from_description_kp:{int(kp_id)}"))
        
        logger.info(f"[BUTTONS] film_id={film_id}, has_plan={has_plan}, watched={watched}, has_sources={has_sources}")
        
        # Инициализируем online_link ДО использования (ВАЖНО: до всех проверок кнопок!)
        online_link = None
        if film_id:
            try:
                conn_online = get_db_connection()
                cursor_online = get_db_cursor()
                try:
                    with db_lock:
                        cursor_online.execute("SELECT online_link FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                        online_row = cursor_online.fetchone()
                        if online_row:
                            online_link = online_row.get('online_link') if isinstance(online_row, dict) else (online_row[0] if len(online_row) > 0 else None)
                finally:
                    try:
                        cursor_online.close()
                    except:
                        pass
                    try:
                        conn_online.close()
                    except:
                        pass
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения online_link: {e}", exc_info=True)

        # Если уже запланирован — не добавляем кнопку планирования
        if has_plan:
            # Уже запланирован → показываем кнопку "Изменить план" + онлайн если home
            if plan_info and 'id' in plan_info:
                markup.add(InlineKeyboardButton("✏️ Изменить в расписании", callback_data=f"edit_plan:{plan_info['id']}"))
            else:
                markup.add(InlineKeyboardButton("✏️ Изменить в расписании", callback_data="edit:plan"))  # фоллбек на общее меню

            # Кнопка билетов для планов в кино
            if plan_info and plan_info.get('type') == 'cinema':
                from moviebot.utils.helpers import has_tickets_access
                if has_tickets_access(chat_id, user_id):
                    if has_tickets:
                        markup.add(InlineKeyboardButton("🎟️ Билеты", callback_data=f"show_ticket:{plan_info['id']}"))
                    else:
                        markup.add(InlineKeyboardButton("🎟️ Добавить билет", callback_data=f"add_ticket:{plan_info['id']}"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Добавить билеты", callback_data=f"ticket_locked:{plan_info['id']}"))

            # Онлайн-кинотеатр для планов дома
            # Если есть online_link, показываем прямую ссылку, иначе - выбор
            if plan_info and plan_info.get('type') == 'home' and not watched:
                if online_link:
                    markup.add(InlineKeyboardButton("🎬 Онлайн-кинотеатр", url=online_link))
                elif has_sources:
                    markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{int(kp_id)}"))
        else:
            # Нет плана → всегда показываем кнопку "Запланировать просмотр"
            logger.info(f"[BUTTONS] Нет плана → добавляем 'Запланировать просмотр'")
            
            if film_id is None:
                markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{int(kp_id)}"))
                markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{int(kp_id)}"))
            else:
                # Фильм в базе, но без плана — только "Запланировать"
                markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{int(kp_id)}"))

            # === КНОПКИ ОНЛАЙН-КИНОТЕАТРОВ ===
            # Только для не просмотренных: прямая ссылка или выбор
            if not watched and online_link:
                logger.info(f"[SHOW FILM INFO] Добавляем кнопку 'Онлайн-кинотеатр' с прямой ссылкой: {online_link[:50]}...")
                markup.add(InlineKeyboardButton("🎬 Онлайн-кинотеатр", url=online_link))
            elif not watched and has_sources:
                logger.info(f"[SHOW FILM INFO] Добавляем кнопку 'Выбрать онлайн-кинотеатр' для kp_id={kp_id}")
                
                # Глобальный кэш источников (в памяти, живёт пока бот работает)
                if 'streaming_sources_cache' not in globals():
                    streaming_sources_cache = {}
                
                # Сохраняем источники по kp_id
                streaming_sources_cache[str(kp_id)] = sources
                
                # Кнопка, которая откроет выбор
                markup.add(InlineKeyboardButton(
                    "🎬 Выбрать онлайн-кинотеатр",
                    callback_data=f"stream_sel:{int(kp_id)}"  # короткий: stream_sel:767379
                ))

        # Кнопка удаления — если фильм в базе
        if film_id:
            markup.add(InlineKeyboardButton("🗑️ Удалить из базы", callback_data=f"remove_from_database:{int(kp_id)}"))
            
        # Добавляем кнопки "Факты" и "Оценить" всегда (для фильмов в базе и не в базе)
        logger.info(f"[SHOW FILM INFO] Добавление кнопок оценок для film_id={film_id}...")
        if film_id:
            # Получаем информацию об оценках — каждый раз новый курсор
            logger.info(f"[SHOW FILM INFO] Запрос оценок из БД...")
            avg_rating = None
            rating_text = "💬 Оценить"

            try:
                # Используем свежий курсор через get_db_connection
                conn_ratings = get_db_connection()
                cursor_ratings = get_db_cursor()
                try:
                    with db_lock:
                        cursor_ratings.execute('''
                            SELECT AVG(rating) as avg FROM ratings 
                            WHERE chat_id = %s AND film_id = %s 
                            AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        avg_result = cursor_ratings.fetchone()
                        if avg_result:
                            avg_rating = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                            avg_rating = float(avg_rating) if avg_rating is not None else None
                finally:
                    try:
                        cursor_ratings.close()
                    except:
                        pass
                    try:
                        conn_ratings.close()
                    except:
                        pass
                
                # Получаем активных пользователей и тех, кто оценил
                conn_ratings2 = get_db_connection()
                cursor_ratings2 = get_db_cursor()
                try:
                    with db_lock:
                        cursor_ratings2.execute('''
                            SELECT DISTINCT user_id
                            FROM stats
                            WHERE chat_id = %s AND user_id IS NOT NULL
                        ''', (chat_id,))
                        active_users_rows = cursor_ratings2.fetchall()
                        active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in active_users_rows if row}
                        
                        cursor_ratings2.execute('''
                            SELECT DISTINCT user_id FROM ratings
                            WHERE chat_id = %s AND film_id = %s 
                            AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        rated_users_rows = cursor_ratings2.fetchall()
                        rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in rated_users_rows if row}
                finally:
                    try:
                        cursor_ratings2.close()
                    except:
                        pass
                    try:
                        conn_ratings2.close()
                    except:
                        pass
                
                # Формируем текст кнопки
                if avg_rating is not None:
                    rating_int = int(round(avg_rating))
                    emoji = "💩" if rating_int <= 4 else "💬" if rating_int <= 7 else "🏆"
                    rating_text = f"{emoji} {avg_rating:.0f}/10"
                
                logger.info(f"[SHOW FILM INFO] Запрос оценок выполнен, avg_rating={avg_rating}, rating_text={rating_text}")
                
            except Exception as e:
                logger.error(f"[SHOW FILM INFO] ❌ Ошибка при запросе оценок: {e}", exc_info=True)
                rating_text = "💬 Оценить"

            logger.info(f"[SHOW FILM INFO] Оценки получены, rating_text={rating_text}")
            
            if not facts_and_rate_added:
                markup.row(
                    InlineKeyboardButton("🤔 Факты", callback_data=f"show_facts:{int(kp_id)}"),
                    InlineKeyboardButton(rating_text, callback_data=f"rate_film:{int(kp_id)}")
                )
                facts_and_rate_added = True
        else:
            # Фильм не в базе - добавляем кнопки "Факты" и "Оценить"
            if not facts_and_rate_added:
                markup.row(
                    InlineKeyboardButton("🤔 Факты", callback_data=f"show_facts:{int(kp_id)}"),
                    InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{int(kp_id)}")
                )
                facts_and_rate_added = True
        logger.info(f"[SHOW FILM INFO] Кнопки оценок добавлены, facts_and_rate_added={facts_and_rate_added}")
        
        # === КНОПКИ ДЛЯ СЕРИАЛОВ ===
        logger.info(f"[SHOW FILM INFO] Обработка кнопок сериала: is_series={is_series}, user_id={user_id}, film_id={film_id}")

        if is_series:
            # КРИТИЧЕСКАЯ ПРОВЕРКА: user_id должен быть указан для проверки доступа
            if user_id is None:
                logger.warning(f"[SHOW FILM INFO] user_id is None для сериала kp_id={kp_id}, показываем заблокированные кнопки")
                has_access = False
            else:
                # Проверяем доступ — функция требует user_id
                has_access = has_notifications_access(chat_id, user_id)
                logger.info(f"[SHOW FILM INFO] Проверка подписки для сериала: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}, has_notifications_access={has_access}")
                if has_access:
                    logger.info(f"[SHOW FILM INFO] ✅ Кнопки сериала РАЗБЛОКИРОВАНЫ (есть подписка Уведомления или пакетная)")
                else:
                    logger.info(f"[SHOW FILM INFO] 🔒 Кнопки сериала ЗАБЛОКИРОВАНЫ (нет подписки Уведомления или пакетной)")
            
            # Отметка серий
            if has_access:
                markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{int(kp_id)}"))
            else:
                markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{int(kp_id)}"))

            # Подписка/отписка — используем данные из current_state
            if has_access:
                if is_subscribed:
                    markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{int(kp_id)}"))
                else:
                    markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{int(kp_id)}"))
            else:
                markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{int(kp_id)}"))

        logger.info(f"[SHOW FILM INFO] Обработка сериала завершена")
        
        # online_link уже инициализирован выше (до использования в кнопках)
        
        logger.info(f"[SHOW FILM INFO] ===== ФИНАЛЬНАЯ ПОДГОТОВКА =====")
        # Проверяем длину текста перед отправкой
        logger.info(f"[SHOW FILM INFO] Текст сформирован, длина={len(text)}, message_id={message_id}")
        logger.info(f"[SHOW FILM INFO] Количество кнопок в markup: {len(markup.keyboard) if markup and markup.keyboard else 0}")
        if len(text) > 4096:
            logger.warning(f"[SHOW FILM INFO] Текст слишком длинный ({len(text)} символов), обрезаю до 4096")
            text = text[:4093] + "..."
        
        # Проверяем валидность markup перед отправкой
        markup_valid = True
        markup_json = None
        try:
            if markup:
                import json
                markup_dict = markup.to_dict()
                markup_json = json.dumps(markup_dict)
                logger.info(f"[SHOW FILM INFO] Markup валиден, количество кнопок: {len(markup_dict.get('inline_keyboard', []))}")
            else:
                logger.info(f"[SHOW FILM INFO] Markup отсутствует (None)")
        except Exception as markup_e:
            logger.error(f"[SHOW FILM INFO] ❌ Ошибка при проверке markup: {markup_e}", exc_info=True)
            markup_valid = False
            markup = None  # Отправляем без клавиатуры
        
        # Проверяем, что text не пустой
        if not text or not text.strip():
            logger.error(f"[SHOW FILM INFO] ❌ Текст пустой или None!")
            text = f"🎬 <b>{info.get('title', 'Фильм')}</b>\n\n❌ Произошла ошибка при формировании описания."
        
        logger.info(f"[SHOW FILM INFO] Финальные проверки: text_length={len(text)}, markup_valid={markup_valid}, markup={markup is not None}")
        
        # Детальное логирование перед отправкой
        if markup:
            try:
                markup_dict = markup.to_dict()
                keyboard = markup_dict.get('inline_keyboard', [])
                total_buttons = sum(len(row) for row in keyboard)
                logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup кнопок: {total_buttons} (строк: {len(keyboard)})")
            except Exception as markup_log_e:
                logger.warning(f"[SHOW FILM INFO] Не удалось получить информацию о markup для логирования: {markup_log_e}")
                logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup присутствует")
        else:
            logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup отсутствует")

        # === ОБНОВЛЕНИЕ ИЛИ ОТПРАВКА СООБЩЕНИЯ (единственный блок) ===
        logger.info(f"[SHOW FILM INFO] ===== ОТПРАВКА СООБЩЕНИЯ =====")
        logger.info(f"[SHOW FILM INFO] message_id={message_id}, message_thread_id={message_thread_id}, chat_id={chat_id}")

        send_kwargs = {
            'chat_id': chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False,
            'reply_markup': markup if markup else None
        }

        # message_thread_id только для send_message, НЕ для edit
        if message_thread_id is not None:
            send_kwargs_for_send = send_kwargs.copy()
            send_kwargs_for_send['message_thread_id'] = message_thread_id
            logger.info(f"[SHOW FILM INFO] message_thread_id добавлен: {message_thread_id}")
        else:
            send_kwargs_for_send = send_kwargs

        sent_new = False
        if message_id:
            logger.info(f"[SHOW FILM INFO] Пытаемся редактировать сообщение message_id={message_id}")
            edit_kwargs = {
                'chat_id': chat_id,
                'message_id': message_id,
                'text': text,
                'parse_mode': 'HTML',
                'disable_web_page_preview': False,
                'reply_markup': markup if markup else None
            }
            try:
                bot.edit_message_text(**edit_kwargs)
                logger.info(f"[SHOW FILM INFO] ✅ Обновлено успешно, message_id={message_id}")
            except Exception as e:  # ловим все ошибки, т.к. ApiTelegramException может быть не импортирован
                logger.warning(f"[SHOW FILM INFO] Ошибка редактирования: {e}")
                if "message is not modified" in str(e).lower():
                    if "exactly the same" in str(e):
                        logger.info("[SHOW FILM INFO] Ничего не изменилось — пропускаем")
                    else:
                        # Пробуем обновить только markup
                        try:
                            bot.edit_message_reply_markup(
                                chat_id=chat_id,
                                message_id=message_id,
                                reply_markup=markup
                            )
                            logger.info("[SHOW FILM INFO] Только markup обновлён")
                        except Exception as e2:
                            if "message is not modified" in str(e2):
                                logger.info("[SHOW FILM INFO] Markup одинаковый — пропускаем")
                            else:
                                logger.error(f"[SHOW FILM INFO] Ошибка markup: {e2}")
                                sent_new = True
                else:
                    logger.error(f"[SHOW FILM INFO] Ошибка edit: {e}")
                    sent_new = True
        else:
            logger.info(f"[SHOW FILM INFO] message_id=None, отправляем новое сообщение")
            sent_new = True

        if sent_new:
            logger.info(f"[SHOW FILM INFO] ===== ОТПРАВКА НОВОГО СООБЩЕНИЯ =====")
            logger.info(f"[SHOW FILM INFO] send_kwargs_for_send: chat_id={send_kwargs_for_send.get('chat_id')}, text_length={len(send_kwargs_for_send.get('text', ''))}, has_markup={send_kwargs_for_send.get('reply_markup') is not None}")
            try:
                sent = bot.send_message(**send_kwargs_for_send)
                logger.info(f"[SHOW FILM INFO] ✅ Отправлено новое сообщение, message_id={sent.message_id}, title={info.get('title')}")
            except Exception as e:
                logger.error(f"[SHOW FILM INFO] ❌ Не отправилось даже новое: {e}", exc_info=True)
                # Fallback: минимальное сообщение
                bot.send_message(chat_id, f"🎬 {info.get('title','Фильм')}\n\n<a href='{link}'>Кинопоиск</a>", parse_mode='HTML')

        logger.info(f"[SHOW FILM INFO] ===== END (успешно) ===== kp_id={kp_id}, title={info.get('title')}")
        
        
    except Exception as e:
        import traceback
        logger.critical(
            f"[SHOW_FILM_CRASH] kp_id={kp_id} | chat_id={chat_id} | user_id={user_id} | message_id={message_id} | "
            f"ОШИБКА: {type(e).__name__}: {str(e)}\n"
            f"Полный traceback:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}\n"
            f"info на момент краша: {info}",
            exc_info=True
        )

        # Берём название из info или existing или хотя бы ID
        safe_title = info.get('title') if info else None
        if not safe_title and existing:
            try:
                _, title_from_db, _ = existing
                safe_title = title_from_db
            except:
                pass
        safe_title = safe_title or f"ID {kp_id}"

        error_text = f"🎬 <b>{safe_title}</b>\n"
        if link:
            error_text += f"<a href='{link}'>Кинопоиск</a>\n\n"
        error_text += "❌ Не удалось полностью загрузить информацию.\n"
        error_text += "Но вы всё равно можете добавить/запланировать 👇"

        # === ОТПРАВКА ОСНОВНОГО СООБЩЕНИЯ ===
        logger.info("[SHOW FILM INFO] Попытка отправки/обновления сообщения")

        if message_id:
            try:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=text,
                    parse_mode='HTML',
                    reply_markup=markup,
                    disable_web_page_preview=False
                )
                logger.info(f"[SHOW FILM INFO] Успешно отредактировано, message_id={message_id}")
                # Сохраняем актуальный message_id для обновления статуса
                actual_message_id = message_id
            except Exception as edit_e:
                logger.warning(f"[EDIT FAIL] {edit_e}")
                # Если edit упал — пробуем отправить новое
                try:
                    sent_msg = bot.send_message(
                        chat_id=chat_id,
                        text=text,
                        parse_mode='HTML',
                        reply_markup=markup,
                        disable_web_page_preview=False,
                        message_thread_id=message_thread_id
                    )
                    actual_message_id = sent_msg.message_id if sent_msg else None
                except Exception as send_e:
                    logger.error(f"[SEND FAIL] {send_e}", exc_info=True)
                    fallback_text = f"🎬 {info.get('title', 'Фильм/Сериал')}\n<a href='{link}'>Кинопоиск</a>"
                    sent_msg = bot.send_message(chat_id, fallback_text, parse_mode='HTML', message_thread_id=message_thread_id)
                    actual_message_id = sent_msg.message_id if sent_msg else None
        else:
            try:
                sent_msg = bot.send_message(
                    chat_id=chat_id,
                    text=text,
                    parse_mode='HTML',
                    reply_markup=markup,
                    disable_web_page_preview=False,
                    message_thread_id=message_thread_id
                )
                actual_message_id = sent_msg.message_id if sent_msg else None
            except Exception as send_e:
                logger.error(f"[SEND FAIL] {send_e}", exc_info=True)
                fallback_text = f"🎬 {info.get('title', 'Фильм/Сериал')}\n<a href='{link}'>Кинопоиск</a>"
                sent_msg = bot.send_message(chat_id, fallback_text, parse_mode='HTML', message_thread_id=message_thread_id)
                actual_message_id = sent_msg.message_id if sent_msg else None

        # === СОЗДАНИЕ ФУНКЦИИ ОБНОВЛЕНИЯ СТАТУСА СЕРИЙ (после отправки сообщения) ===
        # Создаем функцию обновления статуса только если нужно и если есть отмеченные серии
        logger.info(f"[SHOW FILM INFO] Проверка создания функции обновления статуса: is_series={is_series}, should_load_status_async={'should_load_status_async' in locals() and should_load_status_async if 'should_load_status_async' in locals() else 'NOT_IN_LOCALS'}, actual_message_id={actual_message_id if 'actual_message_id' in locals() else 'NOT_DEFINED'}")
        if is_series and 'should_load_status_async' in locals() and should_load_status_async and 'actual_message_id' in locals() and actual_message_id:
            logger.info("[SHOW FILM INFO] ✅ Создаем функцию обновления статуса серий")
            # Сохраняем финальный markup и text в замыкании
            final_markup = markup
            final_text = text
            final_message_id = actual_message_id
            final_chat_id = chat_id
            final_message_thread_id = message_thread_id
            final_kp_id = kp_id
            
            def load_series_status_async():
                """Загружает статус серий в фоне и обновляет сообщение (только текст, кнопки сохраняются)"""
                try:
                    logger.info(f"[SERIES_STATUS_ASYNC] Начало загрузки статуса для kp_id={final_kp_id}")
                    is_airing, next_episode = get_series_airing_status(final_kp_id)
                    status_text = ""
                    if is_airing and next_episode:
                        status_text = f"🟢 <b>Сериал выходит</b>\n📅 След. серия: S{next_episode['season']} E{next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
                    else:
                        status_text = f"🔴 <b>Новых серий нет</b>\n"
                    
                    logger.info(f"[SERIES_STATUS_ASYNC] Статус получен: is_airing={is_airing}, status_text={status_text[:50]}...")
                    
                    # Обновляем сообщение с актуальным статусом
                    if final_message_id:
                        try:
                            # Заменяем заглушку на актуальный статус в тексте
                            updated_text = final_text.replace("⏳ <b>Загрузка статуса серий...</b>\n", status_text)
                            
                            # Обновляем только текст, сохраняя кнопки из финального markup
                            # Используем final_markup, чтобы сохранить кнопки такими, какими они были при отправке
                            bot.edit_message_text(
                                updated_text,
                                final_chat_id,
                                final_message_id,
                                parse_mode='HTML',
                                reply_markup=final_markup,  # Сохраняем кнопки
                                message_thread_id=final_message_thread_id
                            )
                            logger.info("[SHOW FILM INFO] ✅ Серии загружены! Статус серий обновлен в сообщении (текст обновлен, кнопки сохранены)")
                        except Exception as update_e:
                            logger.warning(f"[SHOW FILM INFO] Не удалось обновить статус серий: {update_e}", exc_info=True)
                except Exception as e:
                    logger.error(f"[SERIES_STATUS_CRASH] {e}", exc_info=True)
                    # Обновляем сообщение с ошибкой
                    if final_message_id:
                        try:
                            error_text = final_text.replace("⏳ <b>Загрузка статуса серий...</b>\n", "ℹ️ Не удалось загрузить статус новых серий\n")
                            # Обновляем только текст, сохраняя кнопки из финального markup
                            bot.edit_message_text(
                                error_text,
                                final_chat_id,
                                final_message_id,
                                parse_mode='HTML',
                                reply_markup=final_markup,  # Сохраняем кнопки
                                message_thread_id=final_message_thread_id
                            )
                        except:
                            pass
            
            # Запускаем загрузку статуса в фоне
            status_thread = threading.Thread(target=load_series_status_async, daemon=True)
            status_thread.start()
            logger.info("[SHOW FILM INFO] Загрузка статуса серий запущена в фоне (после отправки сообщения)")
        elif is_series and 'should_load_status_async' in locals() and should_load_status_async:
            logger.warning(f"[SHOW FILM INFO] ⚠️ Не удалось создать функцию обновления статуса: actual_message_id не определен")

        logger.info("[SHOW FILM INFO] ===== END (успешно) =====")

    except Exception as e:
        import traceback
        logger.critical(
            f"[SHOW_FILM_CRASH] kp_id={kp_id} | chat_id={chat_id} | user_id={user_id} | "
            f"ОШИБКА: {type(e).__name__}: {str(e)}\n"
            f"Полный traceback:\n{''.join(traceback.format_exception(type(e), e, e.__traceback__))}\n"
            f"info на момент краша: {info}",
            exc_info=True
        )

        # ЕДИНСТВЕННЫЙ НАДЁЖНЫЙ FALLBACK
        safe_title = info.get('title') or "Фильм/Сериал"
        error_text = f"🎬 <b>{safe_title}</b>\n<a href='{link}'>Кинопоиск</a>\n\n❌ Не удалось загрузить полную информацию.\nНо вы можете:"

        fallback_markup = InlineKeyboardMarkup(row_width=2)
        fallback_markup.add(
            InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{kp_id}")
        )
        fallback_markup.add(
            InlineKeyboardButton("📅 Запланировать", callback_data=f"plan_from_added:{kp_id}")
        )
        fallback_markup.row(
            InlineKeyboardButton("🤔 Факты", callback_data=f"show_facts:{kp_id}"),
            InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_id}")
        )

        try:
            if message_id:
                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=message_id,
                    text=error_text,
                    parse_mode='HTML',
                    reply_markup=fallback_markup,
                    disable_web_page_preview=False
                )
            else:
                bot.send_message(
                    chat_id=chat_id,
                    text=error_text,
                    parse_mode='HTML',
                    reply_markup=fallback_markup,
                    disable_web_page_preview=False,
                    message_thread_id=message_thread_id
                )
        except Exception as final_err:
            logger.error(f"[FALLBACK FAIL] {final_err}")
            bot.send_message(
                chat_id,
                f"🎬 {safe_title}\n{link}",
                parse_mode='HTML',
                message_thread_id=message_thread_id
            )

# ===== TICKET CALLBACK HANDLERS (на верхнем уровне для ранней регистрации) =====
@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_session:"))
def ticket_session_callback(call):
    """Обработчик выбора сеанса - показывает информацию о сеансе и билеты"""
    logger.info(f"[TICKET SESSION] ===== START: callback_id={call.id}, data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[TICKET SESSION] Обработчик вызван! call.data={call.data}")
    try:
        from moviebot.utils.helpers import has_tickets_access
        
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Парсим plan_id и file_id (если есть)
        parts = call.data.split(":")
        plan_id = int(parts[1])
        file_id = parts[2] if len(parts) > 2 else None
        logger.info(f"[TICKET SESSION] Парсинг: plan_id={plan_id}, file_id={file_id}")
        
        # Проверяем доступ к функциям билетов
        if not has_tickets_access(chat_id, user_id):
            bot.edit_message_text(
                "🎫 <b>Билеты в кино</b>\n\n"
                "Вы можете загружать билеты и получать их в боте прямо перед мероприятием с подпиской <b>\"Билеты\"</b>.\n\n"
                "Используйте /payment для оформления подписки.",
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
            return
        
        # Получаем информацию о сеансе (включая мероприятия без film_id)
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        plan_row = None
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT p.id, p.plan_datetime, p.ticket_file_id, p.film_id,
                           COALESCE(m.title, 'Мероприятие') as title, 
                           m.kp_id
                    FROM plans p
                    LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.id = %s AND p.chat_id = %s AND p.plan_type = 'cinema'
                ''', (plan_id, chat_id))
                plan_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if not plan_row:
            logger.error(f"[TICKET SESSION] Сеанс не найден: plan_id={plan_id}, chat_id={chat_id}")
            bot.answer_callback_query(call.id, "❌ Сеанс не найден", show_alert=True)
            return
        
        if isinstance(plan_row, dict):
            plan_dt = plan_row.get('plan_datetime')
            ticket_file_id = plan_row.get('ticket_file_id')
            film_id = plan_row.get('film_id')
            title = plan_row.get('title')
            kp_id = plan_row.get('kp_id')
        else:
            plan_dt = plan_row[1]
            ticket_file_id = plan_row[2]
            film_id = plan_row[3]
            title = plan_row[4]
            kp_id = plan_row[5] if len(plan_row) > 5 else None
        
        logger.info(f"[TICKET SESSION] Данные сеанса получены: ticket_file_id={ticket_file_id}, film_id={film_id}, kp_id={kp_id}, title={title}")
        
        # Если нет билетов и есть film_id и kp_id, открываем описание фильма напрямую
        if not ticket_file_id and film_id and kp_id and str(kp_id).strip():
            logger.info(f"[TICKET SESSION] Нет билетов, но есть film_id и kp_id - открываем описание фильма")
            conn_film = get_db_connection()
            cursor_film = get_db_cursor()
            film_row = None
            try:
                with db_lock:
                    cursor_film.execute('''
                        SELECT id, title, link, watched
                        FROM movies
                        WHERE chat_id = %s AND kp_id = %s
                    ''', (chat_id, str(kp_id)))
                    film_row = cursor_film.fetchone()
            finally:
                if cursor_film:
                    try:
                        cursor_film.close()
                    except:
                        pass
                try:
                    conn_film.close()
                except:
                    pass
            
            if film_row:
                if isinstance(film_row, dict):
                    film_id_val = film_row.get('id')
                    film_title = film_row.get('title')
                    link = film_row.get('link')
                    watched = film_row.get('watched', 0)
                else:
                    film_id_val = film_row[0]
                    film_title = film_row[1]
                    link = film_row[2]
                    watched = film_row[3] if len(film_row) > 3 else 0
                
                logger.info(f"[TICKET SESSION] Фильм найден в БД: film_id={film_id_val}, title={film_title}, link={link}")
                
                from moviebot.api.kinopoisk_api import extract_movie_info
                info = extract_movie_info(link)
                
                if info:
                    logger.info(f"[TICKET SESSION] Информация о фильме получена, открываем описание")
                    existing = (film_id_val, film_title, watched)
                    show_film_info_with_buttons(
                        chat_id=chat_id,
                        user_id=user_id,
                        info=info,
                        link=link,
                        kp_id=str(kp_id),
                        existing=existing
                    )
                    logger.info(f"[TICKET SESSION] ===== END: открыто описание фильма =====")
                    return
                else:
                    logger.warning(f"[TICKET SESSION] Не удалось получить информацию о фильме через API")
            else:
                logger.warning(f"[TICKET SESSION] Фильм не найден в БД по kp_id={kp_id}")
        
        # Если есть билеты или это мероприятие без фильма, показываем информацию о сеансе
        user_tz = get_user_timezone_or_default(user_id)
        if plan_dt:
            if isinstance(plan_dt, datetime):
                if plan_dt.tzinfo is None:
                    dt = pytz.utc.localize(plan_dt).astimezone(user_tz)
                else:
                    dt = plan_dt.astimezone(user_tz)
            else:
                dt = datetime.fromisoformat(str(plan_dt).replace('Z', '+00:00')).astimezone(user_tz)
            date_str = dt.strftime('%d.%m.%Y %H:%M')
        else:
            date_str = "Не указано"
        
        text = f"🎬 <b>{title}</b>\n\n"
        text += f"📅 <b>Дата и время:</b> {date_str}\n\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        if ticket_file_id:
            text += "🎟️ <b>Билеты загружены</b>\n\n"
            text += "Билеты будут отправлены вам перед событием."
            markup.add(InlineKeyboardButton("📎 Показать билеты", callback_data=f"show_ticket:{plan_id}"))
            add_more_btn = "🔒 Добавить ещё билеты" if not has_pro_access(chat_id, user_id) else "➕ Добавить ещё билеты"
            markup.add(InlineKeyboardButton(add_more_btn, callback_data=f"add_more_tickets:{plan_id}"))
            markup.add(InlineKeyboardButton("🔄 Заменить билеты", callback_data=f"add_ticket:{plan_id}"))
        else:
            text += "🎟️ <b>Билеты не загружены</b>\n\n"
            text += "Загрузите билеты, чтобы получать их перед событием."
            markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
        
        markup.add(InlineKeyboardButton("✏️ Изменить", callback_data=f"ticket_edit_time:{plan_id}"))
        
        if not film_id:
            markup.add(InlineKeyboardButton("🗑️ Удалить из расписания", callback_data=f"remove_from_calendar:{plan_id}"))
        elif kp_id:
            markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{int(kp_id)}"))
        
        if file_id:
            from moviebot.states import user_ticket_state
            user_ticket_state[user_id] = {
                'step': 'upload_ticket',
                'plan_id': plan_id,
                'chat_id': chat_id,
                'file_id': file_id
            }
            text += "\n\n📎 Файл готов к добавлению. Нажмите '➕ Добавить билеты' для продолжения."
        
        markup.add(InlineKeyboardButton("⬅️ Назад к событиям", callback_data="ticket_back_to_list"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        logger.info(f"[TICKET SESSION] Показываем информацию о сеансе: plan_id={plan_id}, has_tickets={bool(ticket_file_id)}")
        try:
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            logger.info(f"[TICKET SESSION] ===== END: успешно показана информация о сеансе =====")
        except ApiTelegramException as e:
            error_str = str(e).lower()
            if "message is not modified" in error_str:
                logger.debug(f"[TICKET SESSION] Сообщение не изменилось (это нормально)")
                try:
                    bot.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        reply_markup=markup
                    )
                except:
                    pass
            else:
                raise
    except Exception as e:
        logger.error(f"[TICKET SESSION] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("show_ticket:"))
def show_ticket_callback(call):
    """Обработчик кнопки 'Показать билеты' - отправляет билеты пользователю"""
    logger.info(f"[SHOW TICKET] ===== START: callback_id={call.id}, data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[SHOW TICKET] Обработчик вызван! call.data={call.data}")
    try:
        from moviebot.utils.helpers import has_tickets_access
        
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        if not has_tickets_access(chat_id, user_id):
            bot.answer_callback_query(
                call.id,
                "🎫 Билеты доступны с подпиской 💎 Movie Planner PRO. Подключите через /payment",
                show_alert=True
            )
            return
        
        import json
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        ticket_row = None
        try:
            with db_lock:
                cursor_local.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                ticket_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if not ticket_row:
            bot.answer_callback_query(call.id, "❌ Билеты не найдены", show_alert=True)
            return
        
        if isinstance(ticket_row, dict):
            ticket_data = ticket_row.get('ticket_file_id')
        else:
            ticket_data = ticket_row.get("ticket_file_id") if isinstance(ticket_row, dict) else (ticket_row[0] if ticket_row else None)
        
        if not ticket_data:
            bot.answer_callback_query(call.id, "❌ Билеты не загружены", show_alert=True)
            return
        
        ticket_files = []
        try:
            ticket_files = json.loads(ticket_data)
            if not isinstance(ticket_files, list):
                ticket_files = [ticket_data]
        except:
            ticket_files = [ticket_data]
        
        sent_count = 0
        for i, ticket_file_id in enumerate(ticket_files):
            try:
                if i == 0:
                    caption = f"🎟️ Ваши билеты ({len(ticket_files)} шт.)"
                else:
                    caption = f"🎟️ Билет {i+1}/{len(ticket_files)}"
                
                bot.send_photo(chat_id, ticket_file_id, caption=caption)
                sent_count += 1
            except:
                try:
                    bot.send_document(chat_id, ticket_file_id, caption=caption)
                    sent_count += 1
                except Exception as e:
                    logger.error(f"[SHOW TICKET] Ошибка отправки билета {i+1}: {e}", exc_info=True)
        
        if sent_count > 0:
            bot.answer_callback_query(call.id, f"✅ Отправлено билетов: {sent_count}/{len(ticket_files)}")
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка отправки билетов", show_alert=True)
    except Exception as e:
        logger.error(f"[SHOW TICKET] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("search_type:"))
def search_type_callback(call):
    """Обработчик выбора типа поиска (фильм или сериал)"""
    logger.info("=" * 80)
    logger.info(f"[SEARCH TYPE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[SEARCH TYPE] call.data={call.data}, call.message.message_id={call.message.message_id if call.message else 'N/A'}")
    try:
        # Отвечаем на callback сразу
        bot.answer_callback_query(call.id)
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        search_type = call.data.split(":")[1]  # 'film' или 'series'

        logger.info(f"[SEARCH TYPE] Пользователь {user_id} выбрал тип поиска: {search_type}, chat_id={chat_id}")

        # Обновляем состояние (mixed будет default, если не выбрано)
        if user_id in user_search_state:
            user_search_state[user_id]['search_type'] = search_type
            user_search_state[user_id]['message_id'] = call.message.message_id
        else:
            user_search_state[user_id] = {
                'chat_id': chat_id,
                'message_id': call.message.message_id,
                'search_type': search_type
            }
        logger.info(f"[SEARCH TYPE] ✅ Состояние обновлено: {user_search_state[user_id]}")

        current_type = user_search_state[user_id].get('search_type', 'mixed')

        markup = InlineKeyboardMarkup(row_width=3)
        film_btn = "🎬 Фильмы" + (" ✅" if current_type == "film" else "")
        series_btn = "📺 Сериалы" + (" ✅" if current_type == "series" else "")
        people_btn = "👥 Люди" + (" ✅" if current_type == "people" else "")
        markup.add(
            InlineKeyboardButton(film_btn, callback_data="search_type:film"),
            InlineKeyboardButton(series_btn, callback_data="search_type:series"),
            InlineKeyboardButton(people_btn, callback_data="search_type:people")
        )
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))

        prompt_text = "🔍 Выберите направление поиска — по фильмам, по сериалам, по людям, или укажите запрос для поиска фильмов и сериалов в ответном сообщении. Примеры запросов: Джон Уик, Миллиарды, Брэд Питт"

        try:
            sent_msg = bot.edit_message_text(
                prompt_text,
                chat_id,
                call.message.message_id,
                reply_markup=markup
            )
            logger.info(f"[SEARCH TYPE] ✅ Сообщение обновлено успешно")
        except ApiTelegramException as edit_e:
            error_str = str(edit_e).lower()
            if "message is not modified" in error_str:
                logger.debug(f"[SEARCH TYPE] Сообщение не изменилось (это нормально)")
            else:
                logger.error(f"[SEARCH TYPE] Ошибка редактирования сообщения: {edit_e}", exc_info=True)
                try:
                    sent_msg = bot.send_message(
                        chat_id,
                        prompt_text,
                        reply_markup=markup
                    )
                    logger.info(f"[SEARCH TYPE] ✅ Новое сообщение отправлено")
                except Exception as send_e:
                    logger.error(f"[SEARCH TYPE] ❌ Ошибка отправки нового сообщения: {send_e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                    return
        except Exception as edit_e:
            # Проверяем, не является ли это ошибкой "message is not modified" в другом формате
            error_str = str(edit_e).lower()
            if "message is not modified" in error_str:
                logger.debug(f"[SEARCH TYPE] Сообщение не изменилось (это нормально)")
            else:
                logger.error(f"[SEARCH TYPE] Ошибка редактирования сообщения: {edit_e}", exc_info=True)
                try:
                    sent_msg = bot.send_message(
                        chat_id,
                        prompt_text,
                        reply_markup=markup
                    )
                    logger.info(f"[SEARCH TYPE] ✅ Новое сообщение отправлено")
                except Exception as send_e:
                    logger.error(f"[SEARCH TYPE] ❌ Ошибка отправки нового сообщения: {send_e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                    return

        # Для лички ставим ожидание текста
        if call.message.chat.type == 'private':
            expect_text_from_user(user_id, chat_id, expected_for='search', message_id=call.message.message_id)

    except Exception as e:
        logger.error(f"[SEARCH TYPE] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[SEARCH TYPE] ===== END: callback_id={call.id}")


@bot.callback_query_handler(func=lambda call: call.data == "search:retry")
def search_retry_callback(call):
    """Обработчик кнопки 'Повторить запрос' - возвращает промпт поиска"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        is_private = call.message.chat.type == 'private'
        
        search_type = user_search_state.get(user_id, {}).get('search_type', 'mixed')

        markup = InlineKeyboardMarkup(row_width=3)
        film_btn = "🎬 Фильмы" + (" ✅" if search_type == "film" else "")
        series_btn = "📺 Сериалы" + (" ✅" if search_type == "series" else "")
        people_btn = "👥 Люди" + (" ✅" if search_type == "people" else "")
        markup.add(
            InlineKeyboardButton(film_btn, callback_data="search_type:film"),
            InlineKeyboardButton(series_btn, callback_data="search_type:series"),
            InlineKeyboardButton(people_btn, callback_data="search_type:people")
        )
        markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="search:cancel"))

        prompt_text = "🔍 Выберите направление поиска — по фильмам, по сериалам, по людям, или укажите запрос для поиска фильмов и сериалов в ответном сообщении. Примеры запросов: Джон Уик, Миллиарды, Брэд Питт"
        if is_private:
            prompt_text += "\n\n📝 В личке можно отправить запрос следующим сообщением или в ответ на это сообщение."
        else:
            prompt_text += "\n\n📝 В группе отправьте запрос в ответ на это сообщение."
        
        prompt_msg = bot.send_message(chat_id, prompt_text, reply_markup=markup)
        
        # Устанавливаем состояние для ожидания запроса
        user_search_state[user_id] = {
            'chat_id': chat_id,
            'message_id': prompt_msg.message_id,
            'search_type': search_type
        }
        logger.info(f"[SEARCH RETRY] Состояние поиска установлено для user_id={user_id}: {user_search_state[user_id]}")
        
        # Для ЛС устанавливаем ожидание текста
        if is_private and prompt_msg:
            expect_text_from_user(user_id, chat_id, expected_for='search', message_id=prompt_msg.message_id)
    except Exception as e:
        logger.error(f"[SEARCH RETRY] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


# Вспомогательная функция для поиска с фильтрацией по типу
def search_films_with_type(query, page=1, search_type='mixed'):
    """
    Поиск фильмов с фильтрацией по типу
    Использует фильтрацию на стороне клиента, так как API не поддерживает фильтрацию по типу
    """
    films, total_pages = search_films(query, page)
    
    SERIES_TYPES = ('TV_SERIES', 'MINI_SERIES')  # мини-сериалы тоже идут по /series/
    if search_type == 'film':
        # Фильтруем только фильмы (исключаем сериалы и мини-сериалы)
        films = [f for f in films if f.get('type', '').upper() not in SERIES_TYPES]
    elif search_type == 'series':
        # Фильтруем только сериалы и мини-сериалы (оба по ссылке /series/)
        films = [f for f in films if f.get('type', '').upper() in SERIES_TYPES]
    # Если search_type == 'mixed', возвращаем все
    
    return films, total_pages


PERSON_PROFESSION_KEYS = ('ACTOR', 'PRODUCER', 'DIRECTOR', 'OPERATOR', 'WRITER')
PERSON_PROFESSION_LABELS = {
    'ACTOR': 'Актер', 'PRODUCER': 'Продюсер', 'DIRECTOR': 'Режиссер',
    'OPERATOR': 'Оператор', 'WRITER': 'Сценарист',
}
PERSON_FILMS_PER_PAGE = 8


def _person_films_by_role(staff_data, role_key):
    films = staff_data.get('films') or []
    filtered = [f for f in films if (f.get('professionKey') or '').upper() == role_key.upper()]
    seen = set()
    out = []
    for f in filtered:
        fid = f.get('filmId')
        if fid is not None and fid not in seen:
            seen.add(fid)
            out.append(f)
    return out


def _person_roles_from_staff(staff_data):
    films = staff_data.get('films') or []
    seen = set()
    out = []
    for f in films:
        k = (f.get('professionKey') or '').upper()
        if k in PERSON_PROFESSION_KEYS and k not in seen:
            seen.add(k)
            out.append(k)
    return sorted(out, key=lambda x: list(PERSON_PROFESSION_KEYS).index(x) if x in PERSON_PROFESSION_KEYS else 99)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_select:"))
def person_select_callback(call):
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        person_id = call.data.split(":")[1].strip()
        if not person_id or not person_id.isdigit():
            return
        staff = get_staff(int(person_id))
        if not staff:
            try:
                bot.edit_message_text("❌ Не удалось загрузить данные персоны.", chat_id, call.message.message_id)
            except Exception:
                pass
            return
        name = staff.get('nameRu') or staff.get('nameEn') or 'Без имени'
        roles = _person_roles_from_staff(staff)
        if not roles:
            try:
                bot.edit_message_text(f"У {name} нет подходящих ролей (актёр, режиссёр и т.д.).", chat_id, call.message.message_id)
            except Exception:
                pass
            return
        text = f"Выберите роль, в которой выступал(а) <b>{name}</b>"
        markup = InlineKeyboardMarkup(row_width=1)
        for r in roles:
            label = PERSON_PROFESSION_LABELS.get(r, r)
            markup.add(InlineKeyboardButton(label, callback_data=f"person_role:{person_id}:{r}"))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="person_back_to_results"))
        state = dict(user_search_state.get(user_id) or {})
        state.update({'person_id': person_id, 'person_name': name, 'staff_data': staff, 'chat_id': chat_id, 'search_type': 'people'})
        user_search_state[user_id] = state
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[PERSON SELECT] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_back_to_results"))
def person_back_to_results_callback(call):
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        state = user_search_state.get(user_id) or {}
        results = state.get('people_results') or []
        if not results:
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.edit_message_text("❌ Результаты поиска людей не сохранены. Повторите поиск.", chat_id, call.message.message_id, reply_markup=markup)
            except Exception:
                bot.send_message(chat_id, "❌ Результаты поиска людей не сохранены. Повторите поиск.", reply_markup=markup)
            if user_id in user_search_state:
                del user_search_state[user_id]
            return
        text = "👥 Вот люди из киносферы, найденные по вашему запросу:\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        for p in results[:20]:
            pid = p.get('kinopoiskId')
            name = p.get('nameRu') or p.get('nameEn') or 'Без имени'
            if pid:
                btn = (name[:60] + "…") if len(name) > 60 else name
                markup.add(InlineKeyboardButton(btn, callback_data=f"person_select:{pid}"))
        markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        keep = {'chat_id', 'message_id', 'search_type', 'people_query', 'people_results'}
        user_search_state[user_id] = {k: v for k, v in state.items() if k in keep}
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[PERSON BACK RESULTS] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_back_to_roles"))
def person_back_to_roles_callback(call):
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        state = user_search_state.get(user_id) or {}
        person_id = state.get('person_id')
        name = state.get('person_name') or 'Без имени'
        staff = state.get('staff_data')
        if not staff or not person_id:
            bot.answer_callback_query(call.id, "❌ Сессия поиска устарела. Повторите поиск.", show_alert=True)
            return
        roles = _person_roles_from_staff(staff)
        if not roles:
            return
        text = f"Выберите роль, в которой выступал(а) <b>{name}</b>"
        markup = InlineKeyboardMarkup(row_width=1)
        for r in roles:
            label = PERSON_PROFESSION_LABELS.get(r, r)
            markup.add(InlineKeyboardButton(label, callback_data=f"person_role:{person_id}:{r}"))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="person_back_to_results"))
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[PERSON BACK ROLES] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


def _show_person_films_page(call, person_id, role_key, page=0):
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    state = user_search_state.get(user_id) or {}
    staff = state.get('staff_data')
    name = state.get('person_name') or 'Без имени'
    if not staff:
        return
    films = _person_films_by_role(staff, role_key)
    total = len(films)
    total_pages = max(1, (total + PERSON_FILMS_PER_PAGE - 1) // PERSON_FILMS_PER_PAGE)
    page = max(0, min(page, total_pages - 1))
    start = page * PERSON_FILMS_PER_PAGE
    chunk = films[start:start + PERSON_FILMS_PER_PAGE]
    label = PERSON_PROFESSION_LABELS.get(role_key, role_key)
    text = f"🎬 Фильмы и сериалы: <b>{name}</b> — {label}\n\n"
    markup = InlineKeyboardMarkup(row_width=1)
    for f in chunk:
        film_id = f.get('filmId')
        title = f.get('nameRu') or f.get('nameEn') or 'Без названия'
        year = f.get('year')
        rating = f.get('rating')
        year_str = f" ({year})" if year else ""
        r_str = f" ⭐ {rating}" if rating else ""
        btn_text = title + year_str + r_str
        btn = btn_text[:60] if len(btn_text) <= 60 else btn_text[:57] + "..."
        markup.add(InlineKeyboardButton(btn, callback_data=f"add_film_{film_id}:FILM"))
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"person_films_page:{person_id}:{role_key}:{page - 1}"))
    nav.append(InlineKeyboardButton(f"Стр. {page + 1}/{total_pages}", callback_data="noop"))
    if page < total_pages - 1:
        nav.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"person_films_page:{person_id}:{role_key}:{page + 1}"))
    if nav:
        markup.row(*nav)
    markup.add(InlineKeyboardButton("📥 Добавить все фильмы в базу", callback_data=f"person_add_all:{person_id}:{role_key}"))
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="person_back_to_roles"))
    try:
        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    except Exception:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_role:"))
def person_role_callback(call):
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        if len(parts) < 3:
            return
        person_id, role_key = parts[1], parts[2]
        state = user_search_state.get(call.from_user.id) or {}
        state['person_id'] = person_id
        state['person_role'] = role_key
        user_search_state[call.from_user.id] = state
        _show_person_films_page(call, person_id, role_key, page=0)
    except Exception as e:
        logger.error(f"[PERSON ROLE] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_films_page:"))
def person_films_page_callback(call):
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        if len(parts) < 4:
            return
        person_id, role_key = parts[1], parts[2]
        page = int(parts[3]) if parts[3].isdigit() else 0
        _show_person_films_page(call, person_id, role_key, page=page)
    except Exception as e:
        logger.error(f"[PERSON FILMS PAGE] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("person_add_all:"))
def person_add_all_callback(call):
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        if len(parts) < 3:
            return
        person_id, role_key = parts[1], parts[2]
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        msg_id = call.message.message_id
        state = user_search_state.get(user_id) or {}
        staff = state.get('staff_data') or get_staff(int(person_id))
        name = state.get('person_name') or 'Без имени'
        if not staff:
            try:
                bot.edit_message_text("❌ Не удалось загрузить данные персоны.", chat_id, msg_id)
            except Exception:
                pass
            return
        films = _person_films_by_role(staff, role_key)
        if not films:
            try:
                bot.edit_message_text(f"Нет фильмов по выбранной роли у {name}.", chat_id, msg_id)
            except Exception:
                pass
            return
        loading = bot.send_message(chat_id, "⏳ Добавляю фильмы в базу... 0%")
        loading_id = loading.message_id if loading else None
        added = skipped = 0
        total = len(films)
        for i, f in enumerate(films):
            film_id = f.get('filmId')
            if not film_id:
                continue
            link = f"https://www.kinopoisk.ru/film/{film_id}/"
            info = extract_movie_info(link)
            if not info:
                skipped += 1
                continue
            is_series = info.get('is_series', False)
            link = f"https://www.kinopoisk.ru/series/{film_id}/" if is_series else f"https://www.kinopoisk.ru/film/{film_id}/"
            fid, inserted = ensure_movie_in_database(chat_id, film_id, link, info, user_id)
            if inserted:
                added += 1
            else:
                skipped += 1
            if loading_id and (i + 1) % 3 == 0:
                try:
                    pct = int((i + 1) / total * 100)
                    bot.edit_message_text(f"⏳ Добавляю фильмы в базу... {pct}% ({i + 1}/{total})", chat_id, loading_id)
                except Exception:
                    pass
        if loading_id:
            try:
                bot.delete_message(chat_id, loading_id)
            except Exception:
                pass
        line = f"Добавлено в базу <b>{added}</b> фильмов <b>{name}</b>."
        if skipped:
            line += f" Пропущено (уже в базе/план/просмотр): {skipped}."
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("✅ Готово", callback_data="back_to_start_menu"))
        markup.add(InlineKeyboardButton("◀️ В главное меню", callback_data="back_to_start_menu"))
        try:
            bot.edit_message_text(line, chat_id, msg_id, reply_markup=markup, parse_mode='HTML')
        except Exception:
            bot.send_message(chat_id, line, reply_markup=markup, parse_mode='HTML')
        if user_id in user_search_state:
            del user_search_state[user_id]
    except Exception as e:
        logger.error(f"[PERSON ADD ALL] {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


# Обработчик поиска
@bot.callback_query_handler(func=lambda call: call.data.startswith("view_film_from_ticket:"))
def view_film_from_ticket_callback(call):
    """Обработчик кнопки 'Описание фильма' из билетов - показывает описание запланированного фильма"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Парсим kp_id из callback_data
        kp_id = call.data.split(":")[1]
        
        # Получаем информацию о фильме из базы
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        film_row = None
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, link, watched
                    FROM movies
                    WHERE chat_id = %s AND kp_id = %s
                ''', (chat_id, kp_id))
                film_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if not film_row:
            bot.answer_callback_query(call.id, "❌ Фильм не найден в базе", show_alert=True)
            return
        
        if isinstance(film_row, dict):
            film_id = film_row.get('id')
            title = film_row.get('title')
            link = film_row.get('link')
            watched = film_row.get('watched', 0)
        else:
            film_id = film_row[0]
            title = film_row[1]
            link = film_row[2]
            watched = film_row[3] if len(film_row) > 3 else 0
        
        # Получаем информацию о фильме через API
        from moviebot.api.kinopoisk_api import extract_movie_info
        info = extract_movie_info(link)
        
        if not info:
            bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            return
        
        # Формируем existing для передачи в show_film_info_with_buttons
        existing = (film_id, title, watched)
        
        # Показываем описание фильма
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=existing
        )
    except Exception as e:
        logger.error(f"[VIEW FILM FROM TICKET] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_film_"))
def search_film_callback(call):
    try:
        bot.answer_callback_query(call.id)
        data = call.data[len("add_film_"):]
        parts = data.split(":")
        kp_id = parts[0]
        film_type = parts[1] if len(parts) > 1 else "FILM"

        link = f"https://www.kinopoisk.ru/series/{kp_id}/" if film_type in ("TV_SERIES", "MINI_SERIES") else f"https://www.kinopoisk.ru/film/{kp_id}/"

        info = extract_movie_info(link)
        if not info:
            bot.edit_message_text("Не смог загрузить карточку :(", call.message.chat.id, call.message.message_id)
            return

        show_film_info_with_buttons(
            chat_id=call.message.chat.id,
            user_id=call.from_user.id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=None,
            message_id=call.message.message_id
        )
    except Exception as e:
        logger.error(f"[SEARCH FILM CALLBACK] Ошибка: {e}")
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

def handle_search(message):
    """Команда /search - поиск фильмов и сериалов"""
    logger.info(f"[HANDLER] /search вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/search', message.chat.id)
        
        query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None

        if not query:
            markup = InlineKeyboardMarkup(row_width=3)
            markup.add(
                InlineKeyboardButton("🎬 Фильмы", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Сериалы", callback_data="search_type:series"),
                InlineKeyboardButton("👥 Люди", callback_data="search_type:people")
            )
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))

            prompt_text = "🔍 Выберите направление поиска — по фильмам, по сериалам, по людям, или укажите запрос для поиска фильмов и сериалов в ответном сообщении. Примеры запросов: Джон Уик, Миллиарды, Брэд Питт"

            reply_msg = bot.reply_to(message, prompt_text, reply_markup=markup)

            # Сохраняем состояние (mixed по умолчанию)
            user_id = message.from_user.id
            chat_id = message.chat.id
            user_search_state[user_id] = {
                'chat_id': chat_id, 
                'message_id': reply_msg.message_id, 
                'search_type': 'mixed'
            }
            logger.info(f"[SEARCH] Состояние поиска установлено для user_id={user_id}: {user_search_state[user_id]}")
            
            # Для ЛС устанавливаем ожидание текста
            if message.chat.type == 'private':
                expect_text_from_user(user_id, chat_id, expected_for='search', message_id=reply_msg.message_id)
            return
        
        logger.info(f"Команда /search от пользователя {message.from_user.id}, запрос: {query}")
        
        search_type = user_search_state.get(message.from_user.id, {}).get('search_type', 'mixed')

        if search_type == 'people':
            persons, _ = search_persons(query, page=1)
            if not persons:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                bot.reply_to(message, f"❌ По запросу «{query}» людей не найдено.", reply_markup=markup)
                return
            results_text = "👥 Вот люди из киносферы, найденные по вашему запросу:\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            for p in persons[:20]:
                pid = p.get('kinopoiskId')
                name = p.get('nameRu') or p.get('nameEn') or 'Без имени'
                if pid:
                    btn = (name[:60] + "…") if len(name) > 60 else name
                    markup.add(InlineKeyboardButton(btn, callback_data=f"person_select:{pid}"))
            markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            results_msg = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
            if results_msg:
                user_search_state[message.from_user.id] = {
                    'chat_id': message.chat.id, 'message_id': results_msg.message_id,
                    'search_type': 'people', 'people_query': query, 'people_results': persons[:20],
                }
            logger.info(f"[SEARCH] Люди: отправлено {len(persons)} результатов")
            return

        films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
        if not films:
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🔄 Повторить запрос", callback_data="search:retry"))
            markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
            bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'", reply_markup=markup)
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for film in films[:10]:  # Показываем максимум 10 результатов на странице
            # Пробуем разные варианты полей для совместимости с разными версиями API
            title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
            year = film.get('year') or film.get('releaseYear') or 'N/A'
            _r = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb')
            rating = None
            if _r is not None and str(_r).strip().lower() not in ('', 'null', 'none', 'n/a'):
                rating = _r
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            # Определяем тип (сериал или фильм) по полю type из API
            film_type = film.get('type', '').upper()  # FILM, TV_SERIES, MINI_SERIES
            is_series = film_type in ('TV_SERIES', 'MINI_SERIES')
            
            logger.info(f"[SEARCH] Фильм: title={title}, year={year}, kp_id={kp_id}, type={film_type}, is_series={is_series}")
            
            if kp_id:
                # Ограничиваем длину текста кнопки
                type_indicator = "📺" if is_series else "🎬"
                year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' and year != 'N/A' else ""
                button_text = f"{type_indicator} {title}{year_str}"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                results_text += f"• {type_indicator} <b>{title}</b>{year_str}"
                if rating:
                    results_text += f" ⭐ {rating}"
                results_text += "\n"
                # Сохраняем тип в callback_data для правильного формирования ссылки
                markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
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
        
        # Добавляем кнопку "Назад в меню"
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # Добавляем пояснение про эмодзи
        results_text += "\n\n🎬 - фильм\n📺 - сериал"
        
        results_msg = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
        # Сохраняем message_id результатов поиска для кнопки "Назад"
        if results_msg:
            user_search_state[message.from_user.id] = {
                'chat_id': message.chat.id,
                'message_id': results_msg.message_id,
                'search_type': search_type,
                'query': query,
                'results_text': results_text,
                'films': films[:10],  # Сохраняем первые 10 фильмов для восстановления
                'total_pages': total_pages
            }
        logger.info(f"✅ Ответ на /search отправлен пользователю {message.from_user.id}, найдено {len(films)} результатов")
    except Exception as e:
        logger.error(f"❌ Ошибка в /search: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /search")
        except:
            pass


def random_start(message):
        """Команда /random - рандомный выбор фильма"""
        # TODO: Извлечь из moviebot.py строки 10210-10296
        try:
            logger.info(f"[RANDOM] ===== START: user_id={message.from_user.id}, chat_id={message.chat.id}")
            user_id = message.from_user.id
            chat_id = message.chat.id
            
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(user_id, username, '/random', chat_id)
            
            # Инициализируем состояние
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,  # 'my_votes', 'group_votes', или None (обычный режим)
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }
            
            # Шаг 0: Выбор режима — 1) база, 2) по оценкам в базе (всегда), далее режимы PRO
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            has_rec_access = has_recommendations_access(chat_id, user_id)
            if has_rec_access:
                markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
                markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
            
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            try:
                bot.reply_to(message, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
            except Exception as reply_error:
                # Если не удалось отправить как reply (например, сообщение удалено), отправляем обычное сообщение
                logger.warning(f"[RANDOM] Reply failed, sending new message: {reply_error}")
                bot.send_message(chat_id, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
            logger.info(f"✅ Ответ на /random отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /random: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /random")
            except:
                pass


def premieres_command(message):
        """Команда /premieres - премьеры фильмов. Сначала выбор сортировки."""
        logger.info(f"[HANDLER] /premieres вызван от {message.from_user.id}")
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/premieres', message.chat.id)
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📆 По датам", callback_data="premieres_mode:date"))
        markup.add(InlineKeyboardButton("🎭 По жанрам", callback_data="premieres_mode:genre"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        bot.reply_to(message, "Выберите вариант сортировки:", reply_markup=markup)


def ticket_command(message):
    """Команда /ticket - работа с билетами"""
    # TODO: Извлечь из moviebot.py строки 17031-17333
    logger.info(f"[TICKET COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[TICKET COMMAND] message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        username = message.from_user.username or f"user_{user_id}"
        logger.info(f"[TICKET COMMAND] Вызов log_request")
        log_request(user_id, username, '/ticket', chat_id)
        logger.info(f"[TICKET COMMAND] log_request выполнен")
        
        # Проверяем доступ к функциям билетов
        logger.info(f"[TICKET COMMAND] Проверка доступа к билетам")
        if not has_tickets_access(chat_id, user_id):
            logger.info(f"[TICKET COMMAND] Нет доступа, отправка сообщения о подписке")
            text = "🎫 <b>Билеты в кино</b>\n\n"
            text += "В групповых чатах загрузка билетов доступна с подпиской <b>💎 Movie Planner PRO</b>.\n\n"
            text += "Используйте /payment для оформления подписки."
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("💎 Movie Planner PRO", callback_data="payment:tariffs:personal"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            logger.info(f"[TICKET COMMAND] Вызов reply_to для сообщения о подписке")
            bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[TICKET COMMAND] Сообщение о подписке отправлено")
            return
        
        # Проверяем, есть ли файл в сообщении
        logger.info(f"[TICKET COMMAND] Проверка наличия файла")
        has_photo = message.photo is not None and len(message.photo) > 0
        has_document = message.document is not None
        logger.info(f"[TICKET COMMAND] has_photo={has_photo}, has_document={has_document}")
        
        if has_photo or has_document:
            # Сохраняем file_id для последующей обработки
            if has_photo:
                file_id = message.photo[-1].file_id  # Берем самое большое фото
            else:
                file_id = message.document.file_id
            
            logger.info(f"[TICKET COMMAND] Файл найден, file_id={file_id}")
            user_ticket_state[user_id] = {
                'step': 'select_session',
                'file_id': file_id,
                'chat_id': chat_id
            }
            
            # Показываем список сеансов в кино
            logger.info(f"[TICKET COMMAND] Вызов show_cinema_sessions с file_id")
            show_cinema_sessions(chat_id, user_id, file_id)
            logger.info(f"[TICKET COMMAND] show_cinema_sessions завершен")
        else:
            # Нет файла - показываем список сеансов для выбора или сообщение об отсутствии билетов
            logger.info(f"[TICKET COMMAND] Файла нет, вызов show_cinema_sessions без file_id")
            show_cinema_sessions(chat_id, user_id, None)
            logger.info(f"[TICKET COMMAND] show_cinema_sessions завершен")
        
        logger.info(f"[TICKET COMMAND] ===== КОНЕЦ (успешно) =====")
    except Exception as e:
        logger.error(f"[TICKET COMMAND] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        logger.error(f"[TICKET COMMAND] Тип ошибки: {type(e).__name__}, args: {e.args}")
        try:
            logger.info(f"[TICKET COMMAND] Попытка отправить сообщение об ошибке")
            bot.reply_to(message, "Произошла ошибка при обработке команды /ticket")
            logger.info(f"[TICKET COMMAND] Сообщение об ошибке отправлено")
        except Exception as send_error:
            logger.error(f"[TICKET COMMAND] ❌ Не удалось отправить сообщение об ошибке: {send_error}", exc_info=True)


def help_command(message):
    """Команда /help - помощь"""
    logger.info(f"[HANDLER] /help вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/help', message.chat.id)
    logger.info(f"Команда /help от пользователя {message.from_user.id}")

    text = """🎬 <b>Помощь по использованию бота</b>

Чтобы открыть главное меню, отправьте команду <code>/start</code> или нажмите кнопку "◀️ Назад в меню" ниже.

<b>Разделы меню:</b>

<b>📺 Сериалы</b> — ваши сериалы и отметки просмотренных серий

<b>📅 Премьеры</b> — премьеры по дате выхода или по жанру

<b>🔍 Поиск</b> — поиск фильмов, сериалов и людей через Kinopoisk API

<b>🗄️ База</b> — управление базой фильмов и сериалов: подборки, статистика

<b>🤔 Что посмотреть?</b> — рандом по базе, по кинопоиску, по оценкам; Шазам. Часть режимов — с подпиской 💎 Movie Planner PRO

<b>🗓️ Расписание</b> — запланированные просмотры

<b>🎫 Билеты</b> — билеты и напоминания (в личке — для всех; в группах — с подпиской 💎 Movie Planner PRO)

<b>💰</b> — подписки и оплата

<b>💻</b> — браузерное расширение

<b>⚙️</b> — настройки

<b>❓</b> — эта справка

Подробнее: <a href="https://t.me/movie_planner_channel?hashtag=guide">#guide@movie_planner_channel</a>"""

    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📖 Сценарии взаимодействия с сервисом", callback_data="help:scenarios"))
    markup.add(InlineKeyboardButton("💻 Работа с расширением", callback_data="help:extension"))
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    
    bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')


@bot.callback_query_handler(func=lambda call: call.data == "help:scenarios")
def help_scenarios_callback(call):
    """Обработчик кнопки 'Сценарии взаимодействия с сервисом'"""
    try:
        bot.answer_callback_query(call.id)
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        text = """<b>📖 Сценарии взаимодействия с сервисом</b>

<b>1) Добавление фильмов</b>
1. Отправьте ссылку на фильм с Кинопоиска — бот автоматически добавит его
2. Запланируйте просмотр фильма — дома или в кино. При домашнем просмотре, будут предложены онлайн-кинотеатры, где можно посмотреть фильм.
3. В день просмотра вам придет напоминание о просмотре со ссылкой на кинотеатр, если смотрите дома, или с билетами, если вы подгрузили билет в кино.
4. После просмотра, поставьте реакцию на сообщение с фильмом — фильм будет отмечен как просмотренный
5. После отметки напишите оценку от 1 до 10

При групповом участии, учитываются оценки всех участников. К высоко оцененным фильмам предлагаются похожие, а также оцененные фильмы участвуют в рекомендательных функциях

<b>2) Сериалы</b>
Можно добавлять сериалы, трекать просмотренные серии и подписаться на уведомления

<b>3) Планирование премьер</b>
Если фильм ещё не вышел, вы можете подписаться на его дату выхода

<b>4) Поиск</b>
Вы можете искать фильмы и сериалы с командой /search, а также искать премьеры по /premiere, там будет актуальный список премьер

<b>5) Планирование походов в кино</b>
Вы можете запланировать, хотите вы посмотреть тот или иной фильм дома или в кино. При просмотре фильма дома, вам будут предложны онлайн-кинотеатры, а при просмотре в кино — предложена возможность загрузить билет и указать время сеанса. В день просмотра фильма придет уведомление и напоминание с билетами заранее (функционал платный). Время уведомлений можно настроить.

Подробнее: <a href="https://t.me/movie_planner_channel?hashtag=guide">#guide@movie_planner_channel</a>"""
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="help:back"))
        
        try:
            bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"[HELP SCENARIOS] Не удалось отредактировать сообщение: {e}")
            bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"[HELP SCENARIOS] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "help:extension")
def help_extension_callback(call):
    """Обработчик кнопки 'Работа с расширением'"""
    try:
        bot.answer_callback_query(call.id)
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        text = """<b>💻 Работа с расширением</b>

Браузерное расширение Movie Planner Bot позволяет удобно сохранять фильмы и планировать просмотры прямо из браузера.

<b>🔗 Установка расширения:</b>
<a href="https://chromewebstore.google.com/detail/movie-planner-bot/fldeclcfcngcjphhklommcebkpfipdol">https://chromewebstore.google.com/detail/movie-planner-bot/fldeclcfcngcjphhklommcebkpfipdol</a>

<b>📋 Как использовать:</b>
1. Установите расширение из Chrome Web Store
2. Откройте расширение и введите код, который можно получить в боте (кнопка 💻 в главном меню)
3. Код действителен 10 минут
4. После подключения вы сможете сохранять фильмы и планировать просмотры прямо из браузера

<b>🎬 Что можно делать:</b>
• Сохранять фильмы и сериалы с Кинопоиска, IMDb, Letterboxd
• Планировать просмотры
• На топ стриминговых сервисов (Амедиатека, Okko, ivi, hd.kinopoisk, tvoe, Start, Premier, Wink и др.) расширение распознает фильм или сериал, который вы смотрите
• У сериалов можно отмечать просмотренные серии (платный функционал)
• Бот запомнит ресурс, с которого вы сохранили в базу фильм или сериал, и к ресурсу можно будет вернуться из бота

Подробнее: <a href="https://t.me/movie_planner_channel?hashtag=guide">#guide@movie_planner_channel</a>"""
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="help:back"))
        
        try:
            bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML',
                disable_web_page_preview=False
            )
        except Exception as e:
            logger.warning(f"[HELP EXTENSION] Не удалось отредактировать сообщение: {e}")
            bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML',
                disable_web_page_preview=False
            )
    except Exception as e:
        logger.error(f"[HELP EXTENSION] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "help:back")
def help_back_callback(call):
    """Обработчик кнопки 'Назад' в разделах помощи"""
    try:
        bot.answer_callback_query(call.id)
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        user_id = call.from_user.id
        
        # Используем тот же текст, что и в help_command
        text = """🎬 <b>Помощь по использованию бота</b>

Чтобы открыть главное меню, отправьте команду <code>/start</code> или нажмите кнопку "◀️ Назад в меню" ниже.

<b>Разделы меню:</b>

<b>📺 Сериалы</b> — ваши сериалы и отметки просмотренных серий

<b>📅 Премьеры</b> — премьеры по дате выхода или по жанру

<b>🔍 Поиск</b> — поиск фильмов, сериалов и людей через Kinopoisk API

<b>🗄️ База</b> — управление базой фильмов и сериалов: подборки, статистика

<b>🗓️ Расписание</b> — запланированные просмотры

<b>🤔 Что посмотреть?</b> — рандом по базе, по кинопоиску, по оценкам; Шазам. Часть режимов — с подпиской 💎 Movie Planner PRO

<b>🎫 Билеты</b> — билеты и напоминания (в личке — для всех; в группах — с подпиской 💎 Movie Planner PRO)

<b>💰</b> — подписки и оплата

<b>💻</b> — браузерное расширение

<b>⚙️</b> — настройки

<b>❓</b> — эта справка

Подробнее: <a href="https://t.me/movie_planner_channel?hashtag=guide">#guide@movie_planner_channel</a>"""
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📖 Сценарии взаимодействия с сервисом", callback_data="help:scenarios"))
        markup.add(InlineKeyboardButton("💻 Работа с расширением", callback_data="help:extension"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        try:
            bot.edit_message_text(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.warning(f"[HELP BACK] Не удалось отредактировать сообщение: {e}")
            bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=markup,
                parse_mode='HTML'
            )
    except Exception as e:
        logger.error(f"[HELP BACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass


def show_cinema_sessions(chat_id, user_id, file_id=None):
    """Показывает список запланированных сеансов в кино"""
    logger.info(f"[SHOW SESSIONS] Показываем сеансы для пользователя {user_id}, chat_id={chat_id}, file_id={file_id}")
    try:
        from datetime import datetime as dt_class
        import pytz
        now_utc = dt_class.now(pytz.utc)
        user_tz = get_user_timezone_or_default(user_id)
        now_local = now_utc.astimezone(user_tz)
        # Получаем начало текущего дня в часовом поясе пользователя
        today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
        today_start_utc = today_start.astimezone(pytz.utc)
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        with db_lock:
            cursor_local.execute('''
                SELECT p.id, 
                       COALESCE(p.custom_title, m.title, 'Мероприятие') as title, 
                       p.plan_datetime, 
                       CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as ticket_count,
                       p.film_id,
                       p.custom_title
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND p.plan_type = 'cinema'
                  AND p.plan_datetime >= %s
                ORDER BY p.plan_datetime
                LIMIT 20
            ''', (chat_id, today_start_utc))
            sessions = cursor_local.fetchall()
        
        logger.info(f"[SHOW SESSIONS] Найдено сеансов: {len(sessions) if sessions else 0}")
        
        if not sessions:
            logger.info(f"[SHOW SESSIONS] Нет сеансов, отправляем сообщение пользователю {user_id}")
            if file_id:
                # Если есть файл, но нет сеансов, предлагаем создать новый
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новое событие", callback_data=f"ticket_new:{file_id}"))
                markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
                bot.send_message(chat_id, "❌ Нет запланированных событий.\n\n📎 Файл готов к добавлению. Создайте новое событие.", reply_markup=markup, parse_mode='HTML')
            else:
                # Нет файла и нет сеансов
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новое событие", callback_data="ticket_new"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                bot.send_message(chat_id, "❌ Нет запланированных событий.", reply_markup=markup, parse_mode='HTML')
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
                plan_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None)
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
                
                if len(button_text) > 30:
                    short_title = title[:20] + "..."
                    button_text = f"{ticket_emoji}{short_title} | {date_str}"
                    if len(button_text) > 30:
                        button_text = button_text[:27] + "..."
                
                callback_data = f"ticket_session:{plan_id}"
                if file_id:
                    callback_data += f":{file_id}"
                markup.add(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        if file_id:
            markup.add(InlineKeyboardButton("➕ Добавить новое событие", callback_data=f"ticket_new:{file_id}"))
        else:
            markup.add(InlineKeyboardButton("➕ Добавить новое событие", callback_data="ticket_new"))
        markup.add(InlineKeyboardButton("◀️ Назад в меню", callback_data="back_to_start_menu"))
        
        text = "🎟️ <b>Выберите событие:</b>\n\n"
        if file_id:
            text += "📎 Файл готов к добавлению. Выберите событие или создайте новое."
        else:
            text += "Выберите событие для просмотра билетов или добавления новых."
        
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[SHOW SESSIONS] Сообщение с сеансами отправлено пользователю {user_id}")
    except Exception as e:
        logger.error(f"[SHOW SESSIONS] Ошибка (внешний блок): {e}", exc_info=True)
        try:
            bot.send_message(chat_id, "❌ Произошла ошибка при загрузке событий.")
        except:
            pass


def register_series_handlers(bot_param):
    """Регистрирует обработчики команд связанных с сериалами"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER SERIES HANDLERS] ===== START: регистрация обработчиков сериалов =====")
    logger.info(f"[REGISTER SERIES HANDLERS] bot_param: {bot_param}")
    logger.info(f"[REGISTER SERIES HANDLERS] id(bot_param): {id(bot_param)}, id(bot): {id(bot)}")
    logger.info(f"[REGISTER SERIES HANDLERS] ✅ Используем переданный bot_param для всех хэндлеров")

    @bot_param.message_handler(commands=['search'])
    def _handle_search_handler(message):
        """Обертка для регистрации команды /search"""
        handle_search(message)
    
    @bot_param.message_handler(commands=['random'])
    def _random_start_handler(message):
        """Обертка для регистрации команды /random"""
        random_start(message)
    
    @bot_param.message_handler(commands=['premieres'])
    def _premieres_command_handler(message):
        """Обертка для регистрации команды /premieres"""
        premieres_command(message)
    
    @bot_param.message_handler(commands=['ticket'])
    def _ticket_command_handler(message):
        """Обертка для регистрации команды /ticket"""
        ticket_command(message)
    
    @bot_param.message_handler(commands=['help'])
    def _help_command_handler(message):
        """Обертка для регистрации команды /help"""
        help_command(message)
    
    # Регистрируем handler для dice внутри register_series_handlers
    @bot_param.message_handler(content_types=['dice'])
    def _handle_dice_result(message):
        """Обертка для регистрации handler'а dice"""
        logger.info(f"[REGISTER SERIES HANDLERS] Регистрация handler для dice")
        handle_dice_result(message)
    
    logger.info(f"[REGISTER SERIES HANDLERS] ✅ Handler для dice зарегистрирован")



def _show_period_step(call, chat_id, user_id):
    """Показывает шаг выбора периода для рандома с учетом типа контента (films/series/mixed)"""
    try:
        logger.info(f"[RANDOM] Showing period step for user {user_id}")
        
        state = user_random_state.get(user_id, {})
        mode = state.get('mode')
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Period step: mode={mode}, content_type={content_type}")
        
        all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
        available_periods = []
        
        logger.info(f"[RANDOM CALLBACK] Checking available periods for mode={mode}")
        
        if mode == 'my_votes':
            years = []
            
            # 1. Годы из фильмов в базе (с оценками 9-10, импортированные)
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                is_series_param = None
                if content_type == 'films':
                    is_series_param = 0
                elif content_type == 'series':
                    is_series_param = 1

                if is_series_param is not None:
                    with db_lock:  # здесь lock оставляем — JOIN + фильтр по пользователю
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                            WHERE m.chat_id = %s 
                              AND r.user_id = %s 
                              AND r.rating IN (9, 10) 
                              AND r.is_imported = TRUE
                              AND m.year IS NOT NULL 
                              AND m.is_series = %s
                            ORDER BY m.year
                        """, (chat_id, user_id, is_series_param))
                else:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                            WHERE m.chat_id = %s 
                              AND r.user_id = %s 
                              AND r.rating IN (9, 10) 
                              AND r.is_imported = TRUE
                              AND m.year IS NOT NULL
                            ORDER BY m.year
                        """, (chat_id, user_id))
                    
                years_rows = cursor_local.fetchall()
                years_from_movies = [row['year'] for row in years_rows if row['year'] is not None]
                years.extend(years_from_movies)
            finally:
                try: cursor_local.close()
                except: pass
                try: conn_local.close()
                except: pass
            
            # 2. Годы из импортированных оценок (film_id IS NULL)
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                if content_type == 'films':
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT r.year
                            FROM ratings r
                            LEFT JOIN movies m ON r.kp_id = m.kp_id AND r.chat_id = m.chat_id
                            WHERE r.chat_id = %s 
                              AND r.user_id = %s 
                              AND r.rating IN (9, 10) 
                              AND r.is_imported = TRUE
                              AND r.film_id IS NULL 
                              AND r.year IS NOT NULL
                              AND (r.type = 'FILM' OR (r.type IS NULL AND (m.id IS NULL OR m.is_series = 0)))
                            ORDER BY r.year
                        """, (chat_id, user_id))
                elif content_type == 'series':
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT r.year
                            FROM ratings r
                            LEFT JOIN movies m ON r.kp_id = m.kp_id AND r.chat_id = m.chat_id
                            WHERE r.chat_id = %s 
                              AND r.user_id = %s 
                              AND r.rating IN (9, 10) 
                              AND r.is_imported = TRUE
                              AND r.film_id IS NULL 
                              AND r.year IS NOT NULL
                              AND (r.type = 'TV_SERIES' OR (r.type IS NULL AND m.id IS NOT NULL AND m.is_series = 1))
                            ORDER BY r.year
                        """, (chat_id, user_id))
                else:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT r.year
                            FROM ratings r
                            WHERE r.chat_id = %s 
                              AND r.user_id = %s 
                              AND r.rating IN (9, 10) 
                              AND r.is_imported = TRUE
                              AND r.film_id IS NULL 
                              AND r.year IS NOT NULL
                            ORDER BY r.year
                        """, (chat_id, user_id))
                    
                years_rows = cursor_local.fetchall()
                years_from_ratings = [row['year'] for row in years_rows if row['year'] is not None]
                years.extend(years_from_ratings)
            finally:
                try: cursor_local.close()
                except: pass
                try: conn_local.close()
                except: pass
            
            years = sorted(set(y for y in years if y is not None))
            logger.info(f"[RANDOM] Found {len(years)} years for my_votes mode")
            
            for period in all_periods:
                if period == "До 1980" and any(y < 1980 for y in years):
                    available_periods.append(period)
                elif period == "1980–1990" and any(1980 <= y <= 1990 for y in years):
                    available_periods.append(period)
                elif period == "1990–2000" and any(1990 <= y <= 2000 for y in years):
                    available_periods.append(period)
                elif period == "2000–2010" and any(2000 <= y <= 2010 for y in years):
                    available_periods.append(period)
                elif period == "2010–2020" and any(2010 <= y <= 2020 for y in years):
                    available_periods.append(period)
                elif period == "2020–сейчас" and any(y >= 2020 for y in years):
                    available_periods.append(period)

        elif mode == 'group_votes':
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            years = []
            try:
                is_series_param = None
                if content_type == 'films':
                    is_series_param = 0
                elif content_type == 'series':
                    is_series_param = 1

                if is_series_param is not None:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            WHERE m.chat_id = %s 
                              AND m.year IS NOT NULL 
                              AND m.is_series = %s
                              AND EXISTS (
                                  SELECT 1 FROM ratings r 
                                  WHERE r.film_id = m.id 
                                    AND r.chat_id = m.chat_id 
                                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                                  GROUP BY r.film_id, r.chat_id 
                                  HAVING AVG(r.rating) >= 7.5
                              )
                            ORDER BY m.year
                        """, (chat_id, is_series_param))
                else:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            WHERE m.chat_id = %s 
                              AND m.year IS NOT NULL
                              AND EXISTS (
                                  SELECT 1 FROM ratings r 
                                  WHERE r.film_id = m.id 
                                    AND r.chat_id = m.chat_id 
                                    AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                                  GROUP BY r.film_id, r.chat_id 
                                  HAVING AVG(r.rating) >= 7.5
                              )
                            ORDER BY m.year
                        """, (chat_id,))
                    
                years_rows = cursor_local.fetchall()
                years = [row['year'] for row in years_rows if row['year'] is not None]
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            logger.info(f"[RANDOM] Found {len(years)} years for group_votes mode")
            
            # Определяем доступные периоды (аналогично my_votes)
            for period in all_periods:
                if period == "До 1980" and any(y < 1980 for y in years):
                    available_periods.append(period)
                elif period == "1980–1990" and any(1980 <= y <= 1990 for y in years):
                    available_periods.append(period)
                elif period == "1990–2000" and any(1990 <= y <= 2000 for y in years):
                    available_periods.append(period)
                elif period == "2000–2010" and any(2000 <= y <= 2010 for y in years):
                    available_periods.append(period)
                elif period == "2010–2020" and any(2010 <= y <= 2020 for y in years):
                    available_periods.append(period)
                elif period == "2020–сейчас" and any(y >= 2020 for y in years):
                    available_periods.append(period)

        elif mode == 'kinopoisk':
            # Для режима kinopoisk показываем ВСЕ периоды, так как ищем на Кинопоиске
            available_periods = all_periods.copy()
            logger.info(f"[RANDOM] Kinopoisk mode: showing all periods")

        else:
            # database mode — САМЫЙ ЧАСТЫЙ СЛУЧАЙ → убираем lock полностью
            base_query = """
                SELECT COUNT(DISTINCT m.id) AS count
                FROM movies m
                LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
            """
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            
            base_query += f" {is_series_filter}"
            params = [chat_id]
            
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
                else:
                    continue
                
                query = f"{base_query} AND {condition}"
                
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    cursor_local.execute(query, tuple(params))   # ← lock убрали
                    count_row = cursor_local.fetchone()
                    count = count_row['count'] if count_row else 0
                    
                    if count > 0:
                        available_periods.append(period)
                finally:
                    try: cursor_local.close()
                    except: pass
                    try: conn_local.close()
                    except: pass
        
        logger.info(f"[RANDOM CALLBACK] Available periods: {available_periods}")
        
        user_random_state[user_id]['available_periods'] = available_periods
        
        # ── остальной код без изменений ──
        markup = InlineKeyboardMarkup(row_width=1)
        if available_periods:
            for period in available_periods:
                markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))

        if mode in ['my_votes', 'group_votes']:
            step_text = "🎲 <b>Шаг 2/3: Выберите период</b>"
        elif mode == 'kinopoisk':
            step_text = "🎲 <b>Шаг 2/4: Выберите период</b>"
        else:
            step_text = "🎲 <b>Шаг 2/5: Выберите период</b>"
        
        content_type_text = ""
        if content_type == 'films':
            content_type_text = "\n🎬 Выбрано: Фильмы"
        elif content_type == 'series':
            content_type_text = "\n📺 Выбрано: Сериалы"
        else:
            content_type_text = "\n🎬📺 Выбрано: Смешанный режим"
        
        try:
            bot.answer_callback_query(call.id)
        except Exception as e:
            if "query is too old" not in str(e) and "query ID is invalid" not in str(e) and "timeout expired" not in str(e):
                logger.warning(f"[RANDOM PERIOD] Не удалось ответить на callback query: {e}")
        
        mode_descriptions = {
            'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
            'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм по вашим фильтрам.',
            'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
            'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nПолучите рекомендацию, основанную на оценках в вашей локальной базе.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
        }
        mode_description = mode_descriptions.get(mode, '')
        
        text = f"{mode_description}{content_type_text}\n\n{step_text}\n\n(можно выбрать несколько или пропустить)"
        
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[RANDOM PERIOD] Ошибка редактирования сообщения: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            except:
                pass
        
        logger.info(f"[RANDOM CALLBACK] ✅ Period step shown: mode={mode}, content_type={content_type}, user_id={user_id}")
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in _show_period_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
        
        logger.info(f"[RANDOM CALLBACK] Available periods: {available_periods}")
        
        user_random_state[user_id]['available_periods'] = available_periods
        
        # ────────────────────────────────────────────────────────────────
        # Дальше формирование клавиатуры и отправка сообщения — без изменений
        # ────────────────────────────────────────────────────────────────
        markup = InlineKeyboardMarkup(row_width=1)
        if available_periods:
            for period in available_periods:
                markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))

        if mode in ['my_votes', 'group_votes']:
            step_text = "🎲 <b>Шаг 2/3: Выберите период</b>"
        elif mode == 'kinopoisk':
            step_text = "🎲 <b>Шаг 2/4: Выберите период</b>"
        else:
            step_text = "🎲 <b>Шаг 2/5: Выберите период</b>"
        
        content_type_text = ""
        if content_type == 'films':
            content_type_text = "\n🎬 Выбрано: Фильмы"
        elif content_type == 'series':
            content_type_text = "\n📺 Выбрано: Сериалы"
        else:
            content_type_text = "\n🎬📺 Выбрано: Смешанный режим"
        
        try:
            bot.answer_callback_query(call.id)
        except Exception as e:
            if "query is too old" not in str(e) and "query ID is invalid" not in str(e) and "timeout expired" not in str(e):
                logger.warning(f"[RANDOM PERIOD] Не удалось ответить на callback query: {e}")
        
        mode_descriptions = {
            'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
            'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм по вашим фильтрам.',
            'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
            'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nПолучите рекомендацию, основанную на оценках в вашей локальной базе.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
        }
        mode_description = mode_descriptions.get(mode, '')
        
        text = f"{mode_description}{content_type_text}\n\n{step_text}\n\n(можно выбрать несколько или пропустить)"
        
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[RANDOM PERIOD] Ошибка редактирования сообщения: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            except:
                pass
        
        logger.info(f"[RANDOM CALLBACK] ✅ Period step shown: mode={mode}, content_type={content_type}, user_id={user_id}")
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in _show_period_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_content_type:"))
def handle_rand_content_type(call):
    """Обработчик выбора типа контента (фильмы/сериалы/пропустить) для рандома"""
    try:
        logger.info(f"[RANDOM CONTENT TYPE] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Парсим callback_data: rand_content_type:{mode}:{content_type}
        data_parts = call.data.split(":", 2)
        if len(data_parts) < 3:
            logger.error(f"[RANDOM CONTENT TYPE] Некорректный callback_data: {call.data}")
            bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
            return
        
        mode = data_parts[1]          # database, kinopoisk, my_votes, group_votes
        content_type = data_parts[2]  # films, series, mixed
        
        # Инициализируем состояние, если его нет
        if user_id not in user_random_state:
            logger.warning(f"[RANDOM CONTENT TYPE] Состояние не найдено для user_id={user_id}, инициализируем новое")
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,
                'content_type': None,
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }
        
        state = user_random_state[user_id]
        state['mode'] = mode
        state['content_type'] = content_type
        
        logger.info(f"[RANDOM CONTENT TYPE] Mode={mode}, content_type={content_type}, user_id={user_id}")
        
        bot.answer_callback_query(call.id)
        
        # Для ВСЕХ режимов теперь идём к шагу выбора периода
        state['step'] = 'period'
        logger.info(f"[RANDOM CONTENT TYPE] Переход к периоду, user_id={user_id}")
        _show_period_step(call, chat_id, user_id)
        
    except Exception as e:
        logger.error(f"[RANDOM CONTENT TYPE] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАНДОМА ==========

    
    def check_film_matches_criteria(film_info, periods, genres, directors, actors):
        """Проверяет, соответствует ли фильм критериям"""
        # Проверка на исключаемые жанры
        film_genres_str = film_info.get('genres', '')
        film_genres_lower = str(film_genres_str).lower() if film_genres_str else ""
        for excluded_genre in EXCLUDED_GENRES:
            if excluded_genre.lower() in film_genres_lower:
                return False
        
        # Проверка периода (года)
        if periods:
            film_year = film_info.get('year')
            if not film_year:
                return False
            year_matches = False
            for p in periods:
                if p == "До 1980" and film_year < 1980:
                    year_matches = True
                    break
                elif p == "1980–1990" and 1980 <= film_year <= 1990:
                    year_matches = True
                    break
                elif p == "1990–2000" and 1990 <= film_year <= 2000:
                    year_matches = True
                    break
                elif p == "2000–2010" and 2000 <= film_year <= 2010:
                    year_matches = True
                    break
                elif p == "2010–2020" and 2010 <= film_year <= 2020:
                    year_matches = True
                    break
                elif p == "2020–сейчас" and film_year >= 2020:
                    year_matches = True
                    break
            if not year_matches:
                return False
        
        # Проверка жанров (хотя бы один должен совпадать)
        if genres:
            film_genres_str = film_info.get('genres', '')
            film_genres_lower = str(film_genres_str).lower() if film_genres_str else ""
            genre_matches = False
            for g in genres:
                if g.lower() in film_genres_lower:
                    genre_matches = True
                    break
            if not genre_matches:
                return False
        
        # Проверка режиссеров (если выбраны, хотя бы один должен совпадать)
        if directors:
            film_director = film_info.get('director', '')
            if not film_director or film_director == 'Не указан':
                return False
            director_matches = False
            for d in directors:
                if d.lower() in film_director.lower() or film_director.lower() in d.lower():
                    director_matches = True
                    break
            if not director_matches:
                return False
        
        # Проверка актеров (если выбраны, хотя бы один должен совпадать)
        if actors:
            film_actors_str = film_info.get('actors', '')
            if not film_actors_str or film_actors_str == '—':
                return False
            film_actors_lower = str(film_actors_str).lower()
            actor_matches = False
            for a in actors:
                if a.lower() in film_actors_lower:
                    actor_matches = True
                    break
            if not actor_matches:
                return False
        
        return True
    
    def show_similar_films_page(films, chat_id, user_id, message_id, mode, page=0):
        """Показывает страницу похожих фильмов с пагинацией"""
        try:
            items_per_page = 5
            total_pages = (len(films) + items_per_page - 1) // items_per_page
            start_idx = page * items_per_page
            end_idx = min(start_idx + items_per_page, len(films))
            
            mode_descriptions = {
                'my_votes': '⭐ <b>По моим оценкам (9-10)</b>',
                'group_votes': '👥 <b>По оценкам в базе (7.5+)</b>'
            }
            mode_description = mode_descriptions.get(mode, '🎲 <b>Рекомендации</b>')
            
            text = f"{mode_description}\n\n"
            text += "Вот несколько фильмов, которые могут вам понравиться, основываясь на ваших предпочтениях:\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[start_idx:end_idx]:
                kp_id = film.get('kp_id')
                title = film.get('title', 'Без названия')
                is_series = film.get('is_series', False)
                year = film.get('year', '—')
                
                emoji = "📺" if is_series else "🎬"
                year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' else ""
                text += f"{emoji} <b>{title}</b>{year_str}\n"
                
                button_text = f"{emoji} {title}"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"back_to_film:{kp_id}"))
            
            # Кнопки пагинации
            nav_buttons = []
            if page > 0:
                nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"rand_similar_page:{mode}:{page-1}"))
            if page < total_pages - 1:
                nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"rand_similar_page:{mode}:{page+1}"))
            
            if nav_buttons:
                markup.row(*nav_buttons)
            
            markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
            
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                logger.error(f"[SIMILAR FILMS PAGE] Error editing message: {e}", exc_info=True)
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[SIMILAR FILMS PAGE] ERROR: {e}", exc_info=True)
    
            
    @bot.callback_query_handler(func=lambda call: call.data == "random_back_to_menu")
    def handle_random_back_to_menu(call):
        """Обработчик кнопки 'Вернуться к меню' в рандоме - возвращает к выбору режима"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Очищаем состояние рандома
            if user_id in user_random_state:
                del user_random_state[user_id]
            
            # Создаем фиктивное сообщение для вызова random_start
            class FakeMessage:
                def __init__(self, call):
                    self.from_user = call.from_user
                    self.chat = call.message.chat
                    self.text = '/random'
                    self.message_id = call.message.message_id
                
                def reply_to(self, text, **kwargs):
                    # Используем edit_message_text для обновления сообщения
                    reply_markup = kwargs.get('reply_markup')
                    parse_mode = kwargs.get('parse_mode', 'HTML')
                    try:
                        return bot.edit_message_text(
                            text,
                            self.chat.id,
                            self.message_id,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode
                        )
                    except:
                        # Если не удалось отредактировать, отправляем новое
                        return bot.send_message(
                            self.chat.id,
                            text,
                            reply_markup=reply_markup,
                            parse_mode=parse_mode
                        )
            
            fake_message = FakeMessage(call)
            random_start(fake_message)
            
        except Exception as e:
            logger.error(f"[RANDOM BACK TO MENU] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_locked:"))
    def handle_ticket_locked(call):
        """Обработчик заблокированных кнопок билетов"""
        try:
            bot.answer_callback_query(
                call.id,
                "🎫 Билеты доступны с подпиской 💎 Movie Planner PRO. Подключите через /payment",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[TICKET LOCKED] Ошибка: {e}", exc_info=True)

    # Обработчики ticket_session и show_ticket перемещены на верхний уровень модуля для ранней регистрации

    @bot.callback_query_handler(func=lambda call: call.data == "ticket:add_event")
    def ticket_add_event_callback(call):
        """Обработчик кнопки 'Добавить билет' - начинает флоу добавления билета на мероприятие"""
        try:
            from moviebot.states import user_ticket_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Начинаем флоу добавления билета на мероприятие
            user_ticket_state[user_id] = {
                'step': 'event_add_name',  # ← измени на это
                'chat_id': chat_id,
                'type': 'event'
            }
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            sent_msg = bot.edit_message_text(
                "🎤 <b>Добавление билета на мероприятие</b>\n\n"
                "Напишите название мероприятия в ответ на это сообщение:",
                chat_id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=markup
            )
            # Сохраняем message_id для проверки реплая в групповом чате
            user_ticket_state[user_id]['prompt_message_id'] = call.message.message_id
        except Exception as e:
            logger.error(f"[TICKET ADD EVENT] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_new_film"))
    def ticket_new_film_callback(call):
        """Обработчик кнопки 'Добавить фильм' - начинает флоу планирования фильма в кино"""
        try:
            from moviebot.states import user_plan_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Парсим file_id из callback_data, если есть
            parts = call.data.split(":")
            file_id = parts[1] if len(parts) > 1 else None
            
            # Начинаем флоу планирования фильма в кино
            # Устанавливаем состояние планирования с автоматическим plan_type='cinema'
            user_plan_state[user_id] = {
                'step': 1,  # Шаг 1: ожидание ссылки или ID фильма
                'chat_id': chat_id,
                'plan_type': 'cinema',  # Автоматически ставим "В кино"
                'file_id': file_id  # Сохраняем file_id для последующего добавления билета
            }
            
            # Отправляем сообщение с запросом ссылки
            text = "Пришлите в ответном сообщении ссылку или ID фильма, которое хотели бы запланировать к просмотру"
            
            try:
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
            except Exception as edit_e:
                logger.error(f"[TICKET NEW FILM] Ошибка при редактировании сообщения: {edit_e}", exc_info=True)
                try:
                    bot.send_message(
                        chat_id,
                        text,
                        parse_mode='HTML'
                    )
                except Exception as send_e:
                    logger.error(f"[TICKET NEW FILM] Ошибка при отправке сообщения: {send_e}", exc_info=True)
        except Exception as e:
            logger.error(f"[TICKET NEW FILM] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    # Обработчик show_ticket перемещен на верхний уровень модуля для ранней регистрации

    @bot.callback_query_handler(func=lambda call: call.data.startswith("add_more_tickets:"))
    def add_more_tickets_callback(call):
        """Обработчик кнопки 'Добавить ещё билеты' - начинает загрузку дополнительных билетов"""
        try:
            from moviebot.states import user_ticket_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Устанавливаем состояние для загрузки дополнительных билетов
            user_ticket_state[user_id] = {
                'step': 'add_more_tickets',
                'plan_id': plan_id,
                'chat_id': chat_id
            }
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            bot.edit_message_text(
                "📎 <b>Загрузка дополнительных билетов</b>\n\n"
                "Отправьте файлы билетов. После загрузки всех билетов напишите 'готово'.",
                chat_id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=markup
            )
        except Exception as e:
            logger.error(f"[ADD MORE TICKETS] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_edit_time:"))
    def ticket_edit_time_callback(call):
        """Обработчик кнопки 'Изменить время' - позволяет изменить время сеанса"""
        try:
            from moviebot.states import user_ticket_state
            from moviebot.utils.parsing import parse_session_time
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Получаем текущее время сеанса
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            plan_row = None
            try:
                with db_lock:
                    cursor_local.execute('SELECT plan_datetime FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                    plan_row = cursor_local.fetchone()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if not plan_row:
                bot.answer_callback_query(call.id, "❌ Сеанс не найден", show_alert=True)
                return
            
            plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[0]
            
            # Устанавливаем состояние для изменения времени
            user_ticket_state[user_id] = {
                'step': 'edit_time',
                'plan_id': plan_id,
                'chat_id': chat_id
            }
            
            # Формируем сообщение с примером
            current_time_str = ""
            if plan_dt:
                user_tz = get_user_timezone_or_default(user_id)
                if isinstance(plan_dt, datetime):
                    if plan_dt.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt).astimezone(user_tz)
                    else:
                        dt = plan_dt.astimezone(user_tz)
                else:
                    dt = datetime.fromisoformat(str(plan_dt).replace('Z', '+00:00')).astimezone(user_tz)
                current_time_str = f"\n\nТекущее время: {dt.strftime('%d.%m.%Y %H:%M')}"
            
            text = (
                "✏️ <b>Изменение времени сеанса</b>\n\n"
                "Напишите новую дату и время сеанса в ответ на это сообщение.\n"
                "Формат: дата + время\n"
                "Например: 18 января 19:30 или 18.01 19:30" + current_time_str
            )
            
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET EDIT TIME] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "ticket:cancel")
    def ticket_cancel_callback(call):
        """Обработчик кнопки 'Отмена' для билетов"""
        try:
            from moviebot.states import user_ticket_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if user_id in user_ticket_state:
                del user_ticket_state[user_id]
            
            bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
        except Exception as e:
            logger.error(f"[TICKET CANCEL] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "search:cancel")
    def search_cancel_callback(call):
        """Обработчик кнопки 'Отмена' для поиска"""
        try:
            from moviebot.states import user_search_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if user_id in user_search_state:
                del user_search_state[user_id]
            
            bot.edit_message_text("❌ Поиск отменен.", chat_id, call.message.message_id)
        except Exception as e:
            logger.error(f"[SEARCH CANCEL] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "random_event:close")
    def handle_random_event_close(call):
        """Обработчик кнопки 'Закрыть' для случайных уведомлений"""
        try:
            bot.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            
            # Удаляем состояние игры кубика, если оно есть
            if chat_id in dice_game_state:
                game_state = dice_game_state[chat_id]
                # Удаляем все сообщения с кубиками
                dice_messages = game_state.get('dice_messages', {})
                for dice_msg_id in dice_messages.keys():
                    try:
                        bot.delete_message(chat_id, dice_msg_id)
                        logger.info(f"[RANDOM EVENTS] Удалено сообщение с кубиком {dice_msg_id}")
                    except Exception as e:
                        logger.warning(f"[RANDOM EVENTS] Не удалось удалить сообщение с кубиком {dice_msg_id}: {e}")
                
                # Удаляем состояние игры
                del dice_game_state[chat_id]
                logger.info(f"[RANDOM EVENTS] Состояние игры кубика удалено для чата {chat_id}")
            
            # Удаляем сообщение
            try:
                bot.delete_message(chat_id, message_id)
                logger.info(f"[RANDOM EVENTS] Сообщение {message_id} закрыто пользователем {call.from_user.id}")
            except Exception as e:
                logger.warning(f"[RANDOM EVENTS] Не удалось удалить сообщение {message_id}: {e}")
                # Если не удалось удалить, просто отвечаем на callback
                bot.answer_callback_query(call.id, "Сообщение закрыто")
        except Exception as e:
            logger.error(f"[RANDOM EVENTS] Ошибка при закрытии случайного события: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "dice_game:start")
    def handle_dice_game_start(call):
        """Обработчик кнопки 'Бросить кубик' для игры в кубик"""
        try:
            from moviebot.bot.bot_init import BOT_ID
            from moviebot.utils.random_events import update_dice_game_message
            from datetime import datetime, timedelta
            
            bot.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            
            # Проверяем, что это групповой чат
            try:
                chat_info = bot.get_chat(chat_id)
                if chat_info.type == 'private':
                    bot.answer_callback_query(call.id, "Игра в кубик работает только в групповых чатах", show_alert=True)
                    return
            except Exception as e:
                logger.warning(f"[DICE GAME] Не удалось получить информацию о чате {chat_id}: {e}")
            
            # Если состояние игры не инициализировано, инициализируем его
            if chat_id not in dice_game_state:
                logger.info(f"[DICE GAME] Инициализация состояния игры для чата {chat_id}")
                dice_game_state[chat_id] = {
                    'participants': {},
                    'message_id': message_id,
                    'start_time': datetime.now(PLANS_TZ),
                    'dice_messages': {}
                }
            
            game_state = dice_game_state[chat_id]
            
            # Проверяем, не истекло ли время игры (24 часа)
            if (datetime.now(PLANS_TZ) - game_state['start_time']).total_seconds() > 86400:
                del dice_game_state[chat_id]
                bot.answer_callback_query(call.id, "Время игры истекло", show_alert=True)
                return
            
            # Проверяем, не бросил ли уже пользователь кубик
            if user_id in game_state.get('participants', {}) and 'dice_message_id' in game_state['participants'][user_id]:
                # Проверяем, все ли участники уже бросили
                participants_with_results = {uid: p for uid, p in game_state.get('participants', {}).items() if 'value' in p and p.get('value') is not None}
                all_participants = len(game_state.get('participants', {}))
                all_have_results = len(participants_with_results) == all_participants and all_participants >= 2
                
                if all_have_results:
                    bot.answer_callback_query(call.id, "🎲 Кости уже брошены", show_alert=True)
                else:
                    bot.answer_callback_query(call.id, "Вы уже бросили кубик!", show_alert=True)
                return
            
            # Отправляем стикер игральной кости
            try:
                logger.info(f"[DICE GAME] Попытка отправить кубик для chat_id={chat_id}, user_id={user_id}")
                try:
                    dice_msg = bot.send_dice(chat_id, emoji='🎲')
                    logger.info(f"[DICE GAME] Кубик отправлен с emoji, message_id={dice_msg.message_id if dice_msg else None}")
                except TypeError as e:
                    # Если emoji не поддерживается, используем стандартный кубик
                    logger.warning(f"[DICE GAME] emoji не поддерживается, используем стандартный кубик: {e}")
                    dice_msg = bot.send_dice(chat_id)
                    logger.info(f"[DICE GAME] Стандартный кубик отправлен, message_id={dice_msg.message_id if dice_msg else None}")
                except Exception as e:
                    logger.error(f"[DICE GAME] Ошибка при отправке кубика: {e}", exc_info=True)
                    raise
                
                if dice_msg:
                    # Сохраняем message_id для получения значения позже
                    game_state['dice_messages'] = game_state.get('dice_messages', {})
                    game_state['dice_messages'][dice_msg.message_id] = user_id
                    
                    # Сохраняем информацию об участнике
                    username = call.from_user.username or call.from_user.first_name or f"user_{user_id}"
                    game_state['participants'][user_id] = {
                        'username': username,
                        'dice_message_id': dice_msg.message_id,
                        'user_id': user_id
                    }
                    
                    # Фиксируем в БД, кто бросил кубик
                    conn_local = get_db_connection()
                    cursor_local = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_local.execute('''
                                INSERT INTO stats (user_id, username, command_or_action, timestamp, chat_id)
                                VALUES (%s, %s, %s, %s, %s)
                            ''', (
                                user_id,
                                username,
                                'dice_game:thrown',
                                datetime.now(PLANS_TZ).isoformat(),
                                chat_id
                            ))
                            conn_local.commit()
                    finally:
                        try:
                            cursor_local.close()
                        except:
                            pass
                        try:
                            conn_local.close()
                        except:
                            pass
                    
                    logger.info(f"[DICE GAME] Пользователь {user_id} ({username}) бросил кубик в чате {chat_id}, message_id={dice_msg.message_id}")
                    logger.info(f"[DICE GAME] Текущее состояние dice_game_state[{chat_id}]: participants={list(game_state.get('participants', {}).keys())}, dice_messages={list(game_state.get('dice_messages', {}).keys())}")
                    
                    # Детальное логирование состояния для отладки
                    for pid, pinfo in game_state.get('participants', {}).items():
                        logger.info(f"[DICE GAME] participant {pid}: username={pinfo.get('username')}, dice_message_id={pinfo.get('dice_message_id')}, has_value={'value' in pinfo}")
                    for dmid, duid in game_state.get('dice_messages', {}).items():
                        logger.info(f"[DICE GAME] dice_message {dmid} -> user_id {duid}")
                    
                    # КРИТИЧЕСКИ ВАЖНО: Боты не получают edited_message для своих собственных сообщений
                    # Поэтому нужно периодически проверять результат через прямой API вызов getUpdates
                    def check_dice_result_after_delay():
                        """Проверяет результат dice через 2-3 секунды после отправки через getUpdates"""
                        time.sleep(2.5)  # Ждем, пока кубик остановится (обычно 1-2 секунды)
                        
                        try:
                            # Проверяем, что состояние игры все еще существует
                            if chat_id not in dice_game_state:
                                logger.warning(f"[DICE GAME POLL] Чат {chat_id} больше не в dice_game_state")
                                return
                            
                            current_game_state = dice_game_state[chat_id]
                            if user_id not in current_game_state.get('participants', {}):
                                logger.warning(f"[DICE GAME POLL] Пользователь {user_id} больше не в participants")
                                return
                            
                            # Получаем результат через прямой API вызов getUpdates
                            from moviebot.bot.bot_init import BOT_ID
                            from moviebot.utils.random_events import update_dice_game_message
                            
                            url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
                            params = {'offset': -100, 'limit': 100, 'timeout': 1}  # Получаем последние 100 обновлений
                            
                            try:
                                response = requests.get(url, params=params, timeout=5)
                                if response.status_code == 200:
                                    data = response.json()
                                    if data.get('ok') and data.get('result'):
                                        # Ищем обновление с нашим message_id
                                        for update in data['result']:
                                            if 'message' in update and update['message'].get('message_id') == dice_msg.message_id:
                                                if 'dice' in update['message']:
                                                    dice_value = update['message']['dice'].get('value')
                                                    if dice_value is not None and 1 <= dice_value <= 6:
                                                        logger.info(f"[DICE GAME POLL] ✅ Найден результат dice: {dice_value} для message_id={dice_msg.message_id}")
                                                        
                                                        # Сохраняем значение
                                                        if user_id in current_game_state.get('participants', {}):
                                                            current_game_state['participants'][user_id]['value'] = dice_value
                                                            logger.info(f"[DICE GAME POLL] ✅ Сохранено значение {dice_value} для user_id={user_id}")
                                                            
                                                            # Обновляем сообщение с результатами
                                                            message_id_to_update = current_game_state.get('message_id')
                                                            if message_id_to_update:
                                                                update_dice_game_message(chat_id, current_game_state, message_id_to_update, BOT_ID)
                                                                logger.info(f"[DICE GAME POLL] ✅ Сообщение с результатами обновлено")
                                                        return
                                    logger.warning(f"[DICE GAME POLL] ⚠️ Результат dice не найден в getUpdates для message_id={dice_msg.message_id}")
                            except Exception as api_e:
                                logger.error(f"[DICE GAME POLL] ❌ Ошибка при вызове getUpdates API: {api_e}", exc_info=True)
                            
                        except Exception as poll_e:
                            logger.error(f"[DICE GAME POLL] ❌ Ошибка при проверке результата dice: {poll_e}", exc_info=True)
                    
                    # Запускаем проверку в отдельном потоке
                    poll_thread = threading.Thread(target=check_dice_result_after_delay, daemon=True)
                    poll_thread.start()
                    
                    # Обновляем сообщение с результатами
                    message_id_to_update = game_state.get('message_id', message_id)
                    try:
                        update_dice_game_message(chat_id, game_state, message_id_to_update, BOT_ID)
                        logger.info(f"[DICE GAME] ✅ Сообщение с результатами обновлено успешно")
                    except Exception as update_e:
                        logger.error(f"[DICE GAME] ❌ Ошибка при обновлении сообщения с результатами: {update_e}", exc_info=True)
                else:
                    raise Exception("Не удалось отправить кубик")
            except Exception as e:
                logger.error(f"[DICE GAME] Ошибка при отправке кубика: {e}", exc_info=True)
                bot.answer_callback_query(call.id, "Ошибка при отправке кубика", show_alert=True)
        except Exception as e:
            logger.error(f"[DICE GAME] Ошибка в handle_dice_game_start: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass


def handle_dice_result(message):
    """
    Обрабатывает ВСЕ сообщения с dice — как начальные (value=None), так и с финальным значением.
    Основная логика теперь здесь.
    """
    try:
        from moviebot.bot.bot_init import BOT_ID
        from moviebot.config import PLANS_TZ
        from moviebot.utils.random_events import update_dice_game_message
        from datetime import datetime

        logger.info(f"[DICE GAME] ===== START: msg_id={message.message_id}, "
                    f"chat_id={message.chat.id}, user_id={message.from_user.id if message.from_user else None}")

        if not message.dice:
            logger.warning("[DICE GAME] Сообщение без dice — странно, пропуск")
            return

        dice = message.dice
        logger.info(f"[DICE GAME] dice.emoji={dice.emoji}, value={dice.value}, type(value)={type(dice.value)}")

        # Пропускаем, если не наш кубик
        if dice.emoji != '🎲':
            logger.info(f"[DICE GAME] Пропуск — не 🎲 (было {dice.emoji})")
            return

        # Если value ещё не пришло — ждём следующего апдейта
        if dice.value is None:
            logger.info("[DICE GAME] Кубик крутится (value=None) — ждём финального значения")
            return

        # Проверяем валидность значения
        if not (1 <= dice.value <= 6):
            logger.warning(f"[DICE GAME] Неверное значение кубика: {dice.value}")
            return

        chat_id = message.chat.id
        user_id = message.from_user.id if message.from_user else None

        if not user_id:
            logger.warning("[DICE GAME] Нет user_id — пропуск")
            return

        # Проверяем состояние игры - если не найдено, логируем все доступные состояния для отладки
        if chat_id not in dice_game_state:
            logger.warning(f"[DICE GAME] Чат {chat_id} не найден в dice_game_state")
            logger.info(f"[DICE GAME] Доступные чаты в dice_game_state: {list(dice_game_state.keys())}")
            # Пытаемся найти состояние по другим возможным chat_id (например, после миграции)
            # Проверяем, может быть это супергруппа
            try:
                chat_info = bot.get_chat(chat_id)
                if hasattr(chat_info, 'migrated_from_chat_id') and chat_info.migrated_from_chat_id:
                    old_chat_id = chat_info.migrated_from_chat_id
                    if old_chat_id in dice_game_state:
                        logger.info(f"[DICE GAME] Найдено состояние по старому chat_id {old_chat_id}, переносим на новый {chat_id}")
                        dice_game_state[chat_id] = dice_game_state.pop(old_chat_id)
                    else:
                        logger.warning(f"[DICE GAME] Старый chat_id {old_chat_id} тоже не найден в dice_game_state")
                        return
                else:
                    return
            except Exception as e:
                logger.error(f"[DICE GAME] Ошибка при проверке миграции чата: {e}")
                return

        game_state = dice_game_state[chat_id]

        # Защита от повторного броска одним пользователем
        if user_id in game_state.get('participants', {}):
            prev_value = game_state['participants'][user_id].get('value')
            logger.info(f"[DICE GAME] Пользователь {user_id} уже бросал (было {prev_value}) — повтор игнорируем")
            return

        # Добавляем участника и сразу сохраняем значение
        username = message.from_user.username or message.from_user.first_name or f"user_{user_id}"
        game_state.setdefault('participants', {})[user_id] = {
            'username': username,
            'dice_message_id': message.message_id,
            'user_id': user_id,
            'value': dice.value
        }
        game_state.setdefault('dice_messages', {})[message.message_id] = user_id

        logger.info(f"[DICE GAME] ✅ Добавлен участник {username} ({user_id}) со значением {dice.value}")

        # Обновляем главное сообщение
        if 'message_id' in game_state:
            logger.info(f"[DICE GAME] Обновляем главное сообщение, msg_id={game_state['message_id']}")
            try:
                update_dice_game_message(chat_id, game_state, game_state['message_id'], BOT_ID)
                logger.info("[DICE GAME] Главное сообщение успешно обновлено")
            except Exception as e:
                logger.error(f"[DICE GAME] Ошибка при обновлении главного сообщения: {e}", exc_info=True)

        logger.info("[DICE GAME] ===== END =====")

    except Exception as e:
        logger.error(f"[DICE GAME] Критическая ошибка в handle_dice_result: {e}", exc_info=True)

    # Обработчик settings: перенесен в handlers/settings_main.py

    # Обработчик текстовых сообщений для поиска (ответы на сообщения поиска)
    @bot.message_handler(content_types=['text'], func=lambda m: (
        m.text and 
        not m.text.strip().startswith('/') and 
        m.from_user.id in user_search_state and
        not (m.from_user.id in __import__('moviebot.bot.handlers.tags', fromlist=['user_add_tag_state']).user_add_tag_state and
             __import__('moviebot.bot.handlers.tags', fromlist=['user_add_tag_state']).user_add_tag_state[m.from_user.id].get('step') == 'waiting_for_tag_data' and
             m.reply_to_message and
             m.reply_to_message.message_id == __import__('moviebot.bot.handlers.tags', fromlist=['user_add_tag_state']).user_add_tag_state[m.from_user.id].get('prompt_message_id'))
    ))
    def handle_search_reply(message):
        """Обработчик ответных сообщений для поиска"""
        logger.info(f"[SEARCH REPLY] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[SEARCH REPLY] Получено сообщение: user_id={message.from_user.id}, text={message.text[:50] if message.text else 'None'}, has_reply={message.reply_to_message is not None}")
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            query = message.text.strip()
            
            logger.info(f"[SEARCH REPLY] Проверка состояния: user_id={user_id}, user_search_state keys={list(user_search_state.keys())}")
            
            # Проверяем, находится ли пользователь в состоянии поиска
            if user_id not in user_search_state:
                logger.info(f"[SEARCH REPLY] Пользователь {user_id} не в состоянии поиска, пропускаем")
                return  # Не обрабатываем, если пользователь не в состоянии поиска
            
            state = user_search_state[user_id]
            reply_to_message = message.reply_to_message
            
            logger.info(f"[SEARCH REPLY] Состояние найдено: state={state}, reply_to_message_id={reply_to_message.message_id if reply_to_message else 'None'}, state_message_id={state.get('message_id')}")
            
            # Если пользователь в состоянии поиска, обрабатываем его сообщение
            # Не требуем точного совпадения message_id, так как состояние может быть обновлено
            logger.info(f"[SEARCH REPLY] Пользователь {user_id} в состоянии поиска, обрабатываем запрос: {query}")
            
            search_type = state.get('search_type', 'mixed')
            logger.info(f"[SEARCH REPLY] Тип поиска: {search_type}")

            if search_type == 'people':
                persons, _ = search_persons(query, page=1)
                if not persons:
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                    markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                    bot.reply_to(message, f"❌ По запросу «{query}» людей не найдено.", reply_markup=markup)
                    if user_id in user_search_state:
                        del user_search_state[user_id]
                    return
                results_text = "👥 Вот люди из киносферы, найденные по вашему запросу:\n\n"
                markup = InlineKeyboardMarkup(row_width=1)
                for p in persons[:20]:
                    pid = p.get('kinopoiskId')
                    name = p.get('nameRu') or p.get('nameEn') or 'Без имени'
                    if pid:
                        btn = (name[:60] + "…") if len(name) > 60 else name
                        markup.add(InlineKeyboardButton(btn, callback_data=f"person_select:{pid}"))
                markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                sent = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                user_search_state[user_id] = {
                    'chat_id': chat_id, 'message_id': sent.message_id if sent else None,
                    'search_type': 'people', 'people_query': query, 'people_results': persons[:20],
                }
                logger.info(f"[SEARCH REPLY] Люди: отправлено {len(persons)} результатов")
                return

            logger.info(f"[SEARCH REPLY] Вызов search_films_with_type для query={query}, search_type={search_type}")
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY] Поиск завершен: найдено {len(films)} результатов, страниц: {total_pages}")

            if not films:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔄 Повторить запрос", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'", reply_markup=markup)
                if user_id in user_search_state:
                    del user_search_state[user_id]
                return

            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[:10]:  # Показываем максимум 10 результатов на странице
                try:
                    # Пробуем разные варианты полей для совместимости с разными версиями API
                    title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                    year = film.get('year') or film.get('releaseYear') or 'N/A'
                    _r = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb')
                    rating = None
                    if _r is not None and str(_r).strip().lower() not in ('', 'null', 'none', 'n/a'):
                        rating = _r
                    # Пробуем разные варианты ID
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    # Определяем тип (сериал или фильм) по полю type из API
                    film_type = film.get('type', '').upper() if film.get('type') else 'FILM'
                    is_series = film_type in ('TV_SERIES', 'MINI_SERIES')
                    
                    if kp_id:
                        # Ограничиваем длину текста кнопки
                        type_indicator = "📺" if is_series else "🎬"
                        year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' and year != 'N/A' else ""
                        button_text = f"{type_indicator} {title}{year_str}"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        results_text += f"• {type_indicator} <b>{title}</b>{year_str}"
                        if rating:
                            results_text += f" ⭐ {rating}"
                        results_text += "\n"
                        # Сохраняем тип в callback_data для правильного формирования ссылки
                        markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
                except Exception as film_e:
                    logger.error(f"[SEARCH REPLY] Ошибка обработки фильма: {film_e}", exc_info=True)
                    continue
            
            # Добавляем пагинацию, если нужно
            if total_pages > 1:
                pagination_row = []
                query_encoded = query.replace(' ', '_')
                pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
                if total_pages > 1:
                    pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
                markup.row(*pagination_row)
            
            # Добавляем кнопку "Назад в меню"
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            # Добавляем пояснение про эмодзи
            results_text += "\n\n🎬 - фильм\n📺 - сериал"
            
            # Проверяем длину сообщения (лимит Telegram - 4096 символов)
            if len(results_text) > 4096:
                logger.warning(f"[SEARCH REPLY] Сообщение слишком длинное ({len(results_text)} символов), обрезаем")
                max_length = 4000
                results_text = results_text[:max_length] + "\n\n... (показаны не все результаты)"
            
            # Отправляем результаты
            try:
                sent_message = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[SEARCH REPLY] ✅ Ответ отправлен пользователю {user_id}, найдено {len(films)} результатов, message_id={sent_message.message_id if sent_message else 'None'}")
                # Состояние уже удалено выше, не нужно удалять снова
            except Exception as e:
                logger.error(f"[SEARCH REPLY] ❌ Ошибка отправки результатов поиска: {e}", exc_info=True)
                try:
                    bot.reply_to(message, f"❌ Ошибка при отправке результатов поиска. Попробуйте еще раз.")
                except Exception:
                    pass
                # Состояние уже удалено выше, не нужно удалять снова
        except Exception as e:
            logger.error(f"[SEARCH REPLY] ❌ Критическая ошибка: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Произошла ошибка при обработке запроса. Попробуйте еще раз.")
            except:
                pass

# Обработчик ссылок на Кинопоиск - вынесен на уровень модуля для правильной регистрации
@bot.message_handler(
    content_types=['text'],
    func=lambda m: m.text and not m.text.strip().startswith('/') and ('kinopoisk.ru' in m.text.lower() or 'kinopoisk.com' in m.text.lower())
)
def handle_kinopoisk_link(message):
    """Обработчик текстовых сообщений со ссылками на Кинопоиск"""
    logger.info(f"[KINOPOISK LINK] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}")
    try:
        from moviebot.bot.bot_init import BOT_ID
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip()
        
        logger.info(f"[KINOPOISK LINK] Текст сообщения: '{text[:100]}'")
        
        # КРИТИЧЕСКАЯ ПРОВЕРКА: пропускаем, если пользователь в состоянии /add_tags
        from moviebot.bot.handlers.tags import user_add_tag_state
        if user_id in user_add_tag_state:
            state = user_add_tag_state.get(user_id, {})
            if state.get('step') == 'waiting_for_tag_data' and message.reply_to_message:
                prompt_message_id = state.get('prompt_message_id')
                if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
                    logger.info(f"[KINOPOISK LINK] ⚠️ Пользователь в состоянии /add_tags, ПРОПУСКАЕМ - пусть handle_add_tag_reply обработает")
                    return
        
        # Пропускаем, если это ответ на промпт бота (обрабатывается отдельно)
        if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.id == BOT_ID:
            reply_text = message.reply_to_message.text or ""
            if any(prompt in reply_text for prompt in [
                "Пришлите ссылку или ID фильма в ответном сообщении",
                "Пришлите в ответном сообщении ссылку или ID фильма",
                "В ответном сообщении пришлите ID фильмов"
            ]):
                logger.info(f"[KINOPOISK LINK] Реплай на промпт бота — пропускаем")
                return
        
        # Проверяем состояние планирования/просмотра
        from moviebot.states import user_plan_state, user_view_film_state
        if user_id in user_plan_state:
            logger.info(f"[KINOPOISK LINK] Пользователь в планировании — прерываем и обрабатываем ссылку")
            bot.reply_to(message, "⚠️ Планирование прервано. Обрабатываю ссылку...")
            del user_plan_state[user_id]
        elif user_id in user_view_film_state:
            logger.info(f"[KINOPOISK LINK] Пользователь в состоянии просмотра — пропускаем ссылку")
            return
        
        # Извлекаем kp_id
        kp_id = extract_kp_id_from_text(text)
        if not kp_id:
            logger.warning(f"[KINOPOISK LINK] Не удалось извлечь kp_id из: {text[:200]}")
            bot.reply_to(message, f"❌ Не удалось извлечь ID из ссылки.")
            return
        
        # Нормализуем ссылку
        if text.strip().startswith('http'):
            link = text.strip()
            link = re.sub(r'https?://www\.', 'https://', link)
            link = link.rstrip('/')
        else:
            link = f"https://kinopoisk.ru/film/{kp_id}"
        
        logger.info(f"[KINOPOISK LINK] Обрабатываем kp_id={kp_id}, link={link}")
        
        # Получаем свежие данные из API
        try:
            info = extract_movie_info(link)
            if not info:
                bot.reply_to(message, "❌ Не удалось получить данные о фильме/сериале.")
                return
        except Exception as api_e:
            logger.error(f"[KINOPOISK LINK] Ошибка API: {api_e}", exc_info=True)
            bot.reply_to(message, "❌ Ошибка при загрузке данных с Кинопоиска.")
            return
        
        logger.info(f"[KINOPOISK LINK] Данные получены: {info.get('title')} (сериал: {info.get('is_series')})")
        
        # Проверяем наличие в базе (таблица movies — как у тебя везде)
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        row = None
        try:
            with db_lock:
                cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if row:
            # Уже в базе — обновляем актуальными данными
            film_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None) if not isinstance(row, dict) else row.get('id')
            logger.info(f"[KINOPOISK LINK] Фильм в базе (id={film_id}) — обновляем данные")
            
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('''
                        UPDATE movies 
                        SET title = %s, year = %s, genres = %s, description = %s, 
                            director = %s, actors = %s, is_series = %s, link = %s
                        WHERE id = %s
                    ''', (
                        info.get('title'),
                        info.get('year'),
                        info.get('genres', '—'),
                        info.get('description', 'Нет описания'),
                        info.get('director', 'Не указан'),
                        info.get('actors', '—'),
                        1 if info.get('is_series') else 0,
                        link,
                        film_id
                    ))
                    conn_local.commit()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Получаем watched для existing
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            movie_row = None
            try:
                with db_lock:
                    cursor_local.execute("SELECT title, watched FROM movies WHERE id = %s", (film_id,))
                    movie_row = cursor_local.fetchone()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            title_db = movie_row[0] if not isinstance(movie_row, dict) else movie_row.get('title')
            watched = movie_row[1] if not isinstance(movie_row, dict) else movie_row.get('watched')
            
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id,
                existing=(film_id, title_db, watched),
                message_id=None
            )
        else:
            # НЕ в базе — показываем обычную карточку как для нового фильма
            logger.info(f"[KINOPOISK LINK] Фильм НЕ в базе — показываем with_buttons с existing=None")
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id,
                existing=None,  # важно — чтобы показало "Добавить в базу"
                message_id=None
            )
        
    except Exception as e:
        logger.error(f"[KINOPOISK LINK] Критическая ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Ошибка при обработке ссылки.")
        except:
            pass
    finally:
        logger.info(f"[KINOPOISK LINK] ===== END =====")
        
def ensure_movie_in_database(kp_id, title=None):
    """Убеждается, что фильм есть в базе данных. Если нет - добавляет его."""
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    existing = None
    try:
        with db_lock:
            cursor_local.execute("SELECT id FROM films WHERE kp_id = %s", (str(kp_id),))
            existing = cursor_local.fetchone()
            
            if not existing:
                # Фильма нет в базе, добавляем его
                link = f"https://kinopoisk.ru/film/{kp_id}"
                info = extract_movie_info(link)
                
                if info:
                    cursor_local.execute("""
                        INSERT INTO films (kp_id, title, year, genres, director, actors, description, is_series)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        kp_id,
                        info.get('title') or title,
                        info.get('year'),
                        info.get('genres'),
                        info.get('director'),
                        info.get('actors'),
                        info.get('description'),
                        info.get('is_series', False)
                    ))
                    conn_local.commit()
                    logger.info(f"[ENSURE MOVIE] Фильм {kp_id} добавлен в базу")
                else:
                    logger.warning(f"[ENSURE MOVIE] Не удалось получить информацию о фильме {kp_id}")
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
    
    return existing or (cursor_local.lastrowid if 'cursor_local' in locals() else None)

# Обработчик текстовых сообщений для поиска (ответы на сообщения поиска)
def should_skip_for_add_tags(message):
    """Проверяет, нужно ли пропустить обработку (если пользователь в состоянии /add_tags)"""
    from moviebot.bot.handlers.tags import user_add_tag_state
    user_id = message.from_user.id
    if user_id in user_add_tag_state:
        state = user_add_tag_state.get(user_id, {})
        if state.get('step') == 'waiting_for_tag_data' and message.reply_to_message:
            prompt_message_id = state.get('prompt_message_id')
            if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
                return True
    return False

@bot.message_handler(content_types=['text'], func=lambda m: (
    m.text and 
    not m.text.strip().startswith('/') and 
    m.from_user.id in user_search_state and
    not should_skip_for_add_tags(m)
))
def handle_search_reply(message):
        """Обработчик ответных сообщений для поиска"""
        logger.info(f"[SEARCH REPLY] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[SEARCH REPLY] Получено сообщение: user_id={message.from_user.id}, text={message.text[:50] if message.text else 'None'}, has_reply={message.reply_to_message is not None}")
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            query = message.text.strip()
            
            logger.info(f"[SEARCH REPLY] Проверка состояния: user_id={user_id}, user_search_state keys={list(user_search_state.keys())}")
            
            # Проверяем, находится ли пользователь в состоянии поиска
            if user_id not in user_search_state:
                logger.info(f"[SEARCH REPLY] Пользователь {user_id} не в состоянии поиска, пропускаем")
                return  # Не обрабатываем, если пользователь не в состоянии поиска
            
            state = user_search_state[user_id]
            reply_to_message = message.reply_to_message
            
            logger.info(f"[SEARCH REPLY] Состояние найдено: state={state}, reply_to_message_id={reply_to_message.message_id if reply_to_message else 'None'}, state_message_id={state.get('message_id')}")
            
            # Если пользователь в состоянии поиска, обрабатываем его сообщение
            # Не требуем точного совпадения message_id, так как состояние может быть обновлено
            logger.info(f"[SEARCH REPLY] Пользователь {user_id} в состоянии поиска, обрабатываем запрос: {query}")
            
            search_type = state.get('search_type', 'mixed')
            logger.info(f"[SEARCH REPLY] Тип поиска: {search_type}")

            if search_type == 'people':
                persons, _ = search_persons(query, page=1)
                if not persons:
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                    markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                    bot.reply_to(message, f"❌ По запросу «{query}» людей не найдено.", reply_markup=markup)
                    if user_id in user_search_state:
                        del user_search_state[user_id]
                    return
                results_text = "👥 Вот люди из киносферы, найденные по вашему запросу:\n\n"
                markup = InlineKeyboardMarkup(row_width=1)
                for p in persons[:20]:
                    pid = p.get('kinopoiskId')
                    name = p.get('nameRu') or p.get('nameEn') or 'Без имени'
                    if pid:
                        btn = (name[:60] + "…") if len(name) > 60 else name
                        markup.add(InlineKeyboardButton(btn, callback_data=f"person_select:{pid}"))
                markup.add(InlineKeyboardButton("🔄 Повторить поиск", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                sent = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                user_search_state[user_id] = {
                    'chat_id': chat_id, 'message_id': sent.message_id if sent else None,
                    'search_type': 'people', 'people_query': query, 'people_results': persons[:20],
                }
                logger.info(f"[SEARCH REPLY] Люди: отправлено {len(persons)} результатов")
                return

            logger.info(f"[SEARCH REPLY] Вызов search_films_with_type для query={query}, search_type={search_type}")
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY] Поиск завершен: найдено {len(films)} результатов, страниц: {total_pages}")

            if not films:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔄 Повторить запрос", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'", reply_markup=markup)
                if user_id in user_search_state:
                    del user_search_state[user_id]
                return

            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[:10]:  # Показываем максимум 10 результатов на странице
                title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                year = film.get('year') or film.get('releaseYear') or 'N/A'
                _r = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb')
                rating = None
                if _r is not None and str(_r).strip().lower() not in ('', 'null', 'none', 'n/a'):
                    rating = _r
                kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                
                # Определяем тип (сериал или фильм)
                film_type = film.get('type', '').upper()
                is_series = film_type in ('TV_SERIES', 'MINI_SERIES')
                
                if kp_id:
                    type_indicator = "📺" if is_series else "🎬"
                    year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' and year != 'N/A' else ""
                    button_text = f"{type_indicator} {title}{year_str}"
                    if len(button_text) > 50:
                        button_text = button_text[:47] + "..."
                    results_text += f"• {type_indicator} <b>{title}</b>{year_str}"
                    if rating:
                        results_text += f" ⭐ {rating}"
                    results_text += "\n"
                    markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
            
            # Добавляем пагинацию, если нужно
            if total_pages > 1:
                pagination_row = []
                query_encoded = query.replace(' ', '_')
                pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
                if total_pages > 1:
                    pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
                markup.row(*pagination_row)
            
            # Добавляем кнопку "Назад в меню"
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            # Добавляем пояснение про эмодзи
            results_text += "\n\n🎬 - фильм\n📺 - сериал"
            
            logger.info(f"[SEARCH REPLY] Отправка результатов поиска пользователю {user_id}")
            results_msg = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
            
            # Обновляем состояние
            if results_msg:
                user_search_state[user_id] = {
                    'chat_id': chat_id,
                    'message_id': results_msg.message_id,
                    'search_type': search_type,
                    'query': query,
                    'results_text': results_text,
                    'films': films[:10],
                    'total_pages': total_pages
                }
            
            logger.info(f"[SEARCH REPLY] Результаты поиска отправлены пользователю {user_id}, найдено {len(films)} результатов")
        except Exception as e:
            logger.error(f"[SEARCH REPLY] Ошибка: {e}", exc_info=True)
            try:
                bot.reply_to(message, "❌ Произошла ошибка при обработке запроса поиска")
            except:
                pass

# Обработчик кнопки результата поиска "add_film_{kp_id}:{film_type}" - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
@bot.callback_query_handler(func=lambda call: call.data.startswith("add_film_"))
def add_film_from_search_callback(call):
        """Обработчик кнопки результата поиска - показывает информацию о фильме"""
        logger.info("=" * 80)
        logger.info(f"[ADD FILM FROM SEARCH] ===== START: callback_id={call.id}, callback_data={call.data}")
        try:
            # Проверяем, не устарел ли callback, но продолжаем выполнение даже если устарел
            callback_is_old = False
            try:
                bot.answer_callback_query(call.id, text="⏳ Загружаю информацию...")
                logger.info(f"[ADD FILM FROM SEARCH] answer_callback_query вызван, callback_id={call.id}")
            except Exception as answer_error:
                error_str = str(answer_error)
                if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                    callback_is_old = True
                    logger.warning(f"[ADD FILM FROM SEARCH] Callback query устарел, но продолжаем выполнение: {answer_error}")
                else:
                    logger.error(f"[ADD FILM FROM SEARCH] Ошибка answer_callback_query: {answer_error}", exc_info=True)
            
            # Парсим callback_data: add_film_{kp_id}:{film_type}
            parts = call.data.split(":")
            if len(parts) < 2:
                logger.error(f"[ADD FILM FROM SEARCH] Неверный формат callback_data: {call.data}")
                if not callback_is_old:
                    try:
                        bot.answer_callback_query(call.id, "❌ Ошибка: неверный формат", show_alert=True)
                    except:
                        pass
                return
            
            kp_id = parts[0].replace("add_film_", "")
            film_type = parts[1] if len(parts) > 1 else "FILM"
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            message_id = call.message.message_id if not callback_is_old else None
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            
            logger.info(f"[ADD FILM FROM SEARCH] kp_id={kp_id}, film_type={film_type}, user_id={user_id}, chat_id={chat_id}")
            
            # Формируем ссылку на Кинопоиск
            if film_type == "TV_SERIES" or film_type == "MINI_SERIES":
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            else:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            # Получаем информацию о фильме через API
            info = extract_movie_info(link)
            
            if not info:
                logger.error(f"[ADD FILM FROM SEARCH] Не удалось получить информацию о фильме: kp_id={kp_id}")
                if not callback_is_old:
                    try:
                        bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                    except:
                        pass
                else:
                    # Если callback устарел, отправляем новое сообщение об ошибке
                    try:
                        send_kwargs = {
                            'text': "❌ Не удалось получить информацию о фильме",
                            'chat_id': chat_id
                        }
                        if message_thread_id is not None:
                            send_kwargs['message_thread_id'] = message_thread_id
                        bot.send_message(**send_kwargs)
                    except:
                        pass
                return
            
            # Проверяем, есть ли фильм уже в базе
            existing = None
            # Приводим kp_id к строке для корректного поиска в БД
            kp_id_str = str(kp_id)
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute("SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id_str))
                    row = cursor_local.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        watched = row.get('watched') if isinstance(row, dict) else row[2]
                        existing = (film_id, title, watched)
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Показываем карточку фильма с кнопками (всегда, даже если просмотрен)
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id_str, existing, message_id=message_id, message_thread_id=message_thread_id)
            
            logger.info(f"[ADD FILM FROM SEARCH] ===== END: успешно показана информация о фильме {kp_id}")
        except Exception as e:
            logger.error(f"[ADD FILM FROM SEARCH] Ошибка: {e}", exc_info=True)
            if not callback_is_old:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except:
                    pass
        finally:
            logger.info(f"[ADD FILM FROM SEARCH] ===== END: callback_id={call.id}")



def ensure_movie_in_database(chat_id, kp_id, link, info, user_id=None):
    """
    Добавляет фильм/сериал в базу, если его еще нет.
    Возвращает (film_id, was_inserted), где was_inserted = True если фильм был добавлен.
    """
    logger.info(f"[ENSURE MOVIE] ===== START: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}, link={link}")
    try:
        logger.info(f"[ENSURE MOVIE] Входим в db_lock")
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                logger.info(f"[ENSURE MOVIE] db_lock получен, проверяю существование фильма")
                # Проверяем, существует ли фильм
                cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                row = cursor_local.fetchone()
                
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    logger.info(f"[ENSURE MOVIE] Фильм уже в базе: film_id={film_id}, kp_id={kp_id}")
                    logger.info(f"[ENSURE MOVIE] ===== END (уже в базе) =====")
                    return film_id, False
                
                # Добавляем фильм в базу
                logger.info(f"[ENSURE MOVIE] Фильм не найден, добавляю в БД")
                logger.info(f"[ENSURE MOVIE] Данные: title={info.get('title', 'N/A')}, year={info.get('year', 'N/A')}, is_series={info.get('is_series', False)}")
                
                # Обрабатываем year: если это "—" или не число, ставим None
                year_value = info.get('year')
                if year_value and year_value != '—':
                    try:
                        year_value = int(year_value)
                    except (ValueError, TypeError):
                        year_value = None
                else:
                    year_value = None
                
                cursor_local.execute('''
                    INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                    RETURNING id
                ''', (chat_id, link, str(kp_id), info['title'], year_value, info['genres'], info['description'], 
                      info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
                
                result = cursor_local.fetchone()
                logger.info(f"[ENSURE MOVIE] INSERT выполнен, result={result}")
                film_id = result.get('id') if isinstance(result, dict) else result[0]
                logger.info(f"[ENSURE MOVIE] film_id извлечен: {film_id}")
                conn_local.commit()
                logger.info(f"[ENSURE MOVIE] commit выполнен")
                
                logger.info(f"[ENSURE MOVIE] Фильм добавлен в базу: film_id={film_id}, kp_id={kp_id}, title={info['title']}")
                logger.info(f"[ENSURE MOVIE] ===== END (добавлен) =====")
                return film_id, True
        except Exception as e:
            logger.error(f"[ENSURE MOVIE] КРИТИЧЕСКАЯ ОШИБКА при добавлении фильма в базу: {e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            raise
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
        logger.error(f"[ENSURE MOVIE] КРИТИЧЕСКАЯ ОШИБКА при добавлении фильма в базу: {e}", exc_info=True)
        logger.info(f"[ENSURE MOVIE] ===== END (ошибка) =====")
        return None, False

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
                
                # Получаем тип (FILM или TV_SERIES) - теперь сохраняем все типы
                film_type = item.get('type', 'FILM')  # FILM или TV_SERIES
                
                user_rating = item.get('userRating')
                if not user_rating or user_rating < 1 or user_rating > 10:
                    continue
                
                # Получаем год и жанры из данных импорта
                film_year = item.get('year')
                film_genres_list = item.get('genres', [])
                film_genres_str = ', '.join([g.get('genre', '') for g in film_genres_list if g.get('genre')]) if film_genres_list else None
                
                # Формируем ссылку в зависимости от типа
                if film_type == 'TV_SERIES':
                    link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                else:
                    link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                
                # Импортированные оценки НЕ добавляют фильмы в базу группы
                # Они существуют только как оценки в таблице ratings с is_imported = TRUE
                # Для импортированных оценок используем film_id = NULL или создаем виртуальный film_id
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        # Проверяем, есть ли фильм в базе группы (добавлен через бота)
                        cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                        film_row = cursor_local.fetchone()
                        
                        if film_row:
                            # Фильм уже есть в базе группы - можем добавить импортированную оценку
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            logger.debug(f"[IMPORT] Фильм {kp_id} уже существует в базе группы, film_id={film_id}")
                            
                            # Проверяем, есть ли уже оценка у этого пользователя для этого фильма
                            cursor_local.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s
                            ''', (chat_id, film_id, user_id))
                            existing_rating = cursor_local.fetchone()
                            
                            if existing_rating:
                                # Оценка уже есть, пропускаем
                                cursor_local.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                                title_row = cursor_local.fetchone()
                                title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                                logger.debug(f"[IMPORT] Фильм {title} уже имеет оценку, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку для существующего фильма
                            cursor_local.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id, year, genres, type)
                                VALUES (%s, %s, %s, %s, TRUE, %s, %s, %s, %s)
                                ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = TRUE, kp_id = EXCLUDED.kp_id, year = EXCLUDED.year, genres = EXCLUDED.genres, type = EXCLUDED.type
                            ''', (chat_id, film_id, user_id, user_rating, kp_id, film_year, film_genres_str, film_type))
                            conn_local.commit()
                            
                            imported_count += 1
                            cursor_local.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                            title_row = cursor_local.fetchone()
                            title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                            logger.info(f"[IMPORT] Импортирован фильм {title} с оценкой {user_rating}")
                        else:
                            # Фильма нет в базе группы - создаем импортированную оценку БЕЗ добавления фильма в movies
                            # Используем film_id = NULL и kp_id для хранения импортированных оценок
                            title = item.get('nameRu') or item.get('nameEn') or 'Без названия'
                            
                            # Проверяем, есть ли уже импортированная оценка для этого kp_id и пользователя
                            cursor_local.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND kp_id = %s AND user_id = %s AND film_id IS NULL
                            ''', (chat_id, kp_id, user_id))
                            existing_imported_rating = cursor_local.fetchone()
                            
                            if existing_imported_rating:
                                logger.debug(f"[IMPORT] Импортированная оценка для фильма {kp_id} ({title}) уже существует, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку БЕЗ film_id (film_id = NULL)
                            cursor_local.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id, year, genres, type)
                                VALUES (%s, NULL, %s, %s, TRUE, %s, %s, %s, %s)
                            ''', (chat_id, user_id, user_rating, kp_id, film_year, film_genres_str, film_type))
                            conn_local.commit()
                            
                            imported_count += 1
                            logger.info(f"[IMPORT] Импортирован фильм {title} (kp_id={kp_id}) с оценкой {user_rating} (без добавления в базу группы)")
                except Exception as db_error:
                    logger.error(f"[IMPORT] Ошибка при работе с БД для фильма {kp_id}: {db_error}", exc_info=True)
                    try:
                        conn_local.rollback()
                    except:
                        pass
                    continue
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
            
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


# Обработчик выбора количества фильмов для импорта - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("import_count:"))
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


def handle_clean_confirm_internal(message):
    """Внутренняя функция для обработки подтверждения удаления"""
    from moviebot.states import user_clean_state
    from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
    
    user_id = message.from_user.id
    state = user_clean_state.get(user_id)
    if not state:
        logger.warning(f"[CLEAN CONFIRM] Пользователь {user_id} не в состоянии user_clean_state")
        return
    
    target = state.get('target')
    chat_id = message.chat.id
    
    logger.info(f"[CLEAN CONFIRM] ===== START: user_id={user_id}, target={target}, chat_id={chat_id}")
    
    if target == 'user':
        # Удаление всех данных пользователя
        logger.info(f"[CLEAN CONFIRM] Начало удаления данных пользователя: user_id={user_id}, chat_id={chat_id}")
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        ratings_deleted = 0
        plans_deleted = 0
        watched_deleted = 0
        stats_deleted = 0
        settings_deleted = 0
        tags_deleted = 0
        try:
            with db_lock:
                # Удаляем оценки пользователя (но не импортированные - они удаляются отдельной командой)
                cursor_local.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, user_id))
                ratings_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено оценок: {ratings_deleted}")
                
                # Удаляем планы пользователя
                cursor_local.execute('DELETE FROM plans WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                plans_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено планов: {plans_deleted}")
                
                # Удаляем отметки просмотра пользователя
                cursor_local.execute('DELETE FROM watched_movies WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                watched_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено отметок просмотра: {watched_deleted}")
                
                # Удаляем статистику пользователя
                cursor_local.execute('DELETE FROM stats WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                stats_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено статистики: {stats_deleted}")
                
                # Удаляем настройки пользователя
                cursor_local.execute('DELETE FROM settings WHERE chat_id = %s AND key LIKE %s', (user_id, 'user_%'))
                settings_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено настроек: {settings_deleted}")
                
                # Удаляем все подборки пользователя (user_tag_movies)
                cursor_local.execute('DELETE FROM user_tag_movies WHERE user_id = %s AND chat_id = %s', (user_id, chat_id))
                tags_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено подборок: {tags_deleted}")
                
                conn_local.commit()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        action_text = "✅ ДЕЙСТВИЕ ВЫПОЛНЕНО: Обнуление базы данных пользователя"
        result_text = f"{action_text}\n\nУдалено:\n"
        result_text += f"• Оценок: {ratings_deleted}\n"
        result_text += f"• Планов: {plans_deleted}\n"
        result_text += f"• Отметок просмотра: {watched_deleted}\n"
        result_text += f"• Статистики: {stats_deleted}\n"
        result_text += f"• Настроек: {settings_deleted}\n"
        result_text += f"• Подборок: {tags_deleted}"
        
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        if is_private:
            bot.send_message(message.chat.id, result_text)
        else:
            bot.reply_to(message, result_text)
        
        logger.info(f"[CLEAN CONFIRM] ✅ Завершено удаление данных пользователя: user_id={user_id}, chat_id={chat_id}")
        del user_clean_state[user_id]
    
    elif target == 'imported_ratings':
        # Удаление импортированных оценок пользователя
        logger.info(f"[CLEAN CONFIRM] Начало удаления импортированных оценок: user_id={user_id}, chat_id={chat_id}")
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        imported_deleted = 0
        try:
            with db_lock:
                cursor_local.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s AND is_imported = TRUE', (chat_id, user_id))
                imported_deleted = cursor_local.rowcount
                conn_local.commit()
                logger.info(f"[CLEAN CONFIRM] Удалено импортированных оценок: {imported_deleted}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        action_text = "✅ ДЕЙСТВИЕ ВЫПОЛНЕНО: Удаление импортированных оценок с Кинопоиска"
        result_text = f"{action_text}\n\nУдалено импортированных оценок: {imported_deleted}"
        
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        if is_private:
            bot.send_message(message.chat.id, result_text)
        else:
            bot.reply_to(message, result_text)
        
        logger.info(f"[CLEAN CONFIRM] ✅ Завершено удаление импортированных оценок: user_id={user_id}, chat_id={chat_id}")
        del user_clean_state[user_id]
    
    elif target == 'chat':
        # Удаление всех данных чата (требует голосования в группах)
        logger.info(f"[CLEAN CONFIRM] Начало обнуления базы данных чата: chat_id={chat_id}")
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        ratings_deleted = 0
        plans_deleted = 0
        watched_deleted = 0
        movies_deleted = 0
        stats_deleted = 0
        settings_deleted = 0
        tags_deleted = 0
        try:
            with db_lock:
                cursor_local.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
                ratings_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено оценок: {ratings_deleted}")
                
                cursor_local.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
                plans_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено планов: {plans_deleted}")
                
                cursor_local.execute('DELETE FROM watched_movies WHERE chat_id = %s', (chat_id,))
                watched_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено отметок просмотра: {watched_deleted}")
                
                cursor_local.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
                movies_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено фильмов: {movies_deleted}")
                
                cursor_local.execute('DELETE FROM user_tag_movies WHERE chat_id = %s', (chat_id,))
                tags_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено записей подборок: {tags_deleted}")
                
                cursor_local.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
                stats_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено статистики: {stats_deleted}")
                
                cursor_local.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
                settings_deleted = cursor_local.rowcount
                logger.info(f"[CLEAN CONFIRM] Удалено настроек: {settings_deleted}")
                
                conn_local.commit()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        action_text = "✅ ДЕЙСТВИЕ ВЫПОЛНЕНО: Обнуление базы данных чата"
        result_text = f"{action_text}\n\nУдалено:\n"
        result_text += f"• Фильмов: {movies_deleted}\n"
        result_text += f"• Оценок: {ratings_deleted}\n"
        result_text += f"• Планов: {plans_deleted}\n"
        result_text += f"• Отметок просмотра: {watched_deleted}\n"
        result_text += f"• Записей подборок: {tags_deleted}\n"
        result_text += f"• Статистики: {stats_deleted}\n"
        result_text += f"• Настроек: {settings_deleted}"
        
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        if is_private:
            bot.send_message(message.chat.id, result_text)
        else:
            bot.reply_to(message, result_text)
        
        logger.info(f"[CLEAN CONFIRM] ✅ Завершено обнуление базы данных чата: chat_id={chat_id}")
        del user_clean_state[user_id]
    
    elif target == 'unwatched_movies':
        # Удаление непросмотренных фильмов
        logger.info(f"[CLEAN CONFIRM] Начало удаления непросмотренных фильмов: user_id={user_id}, chat_id={chat_id}")
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        movies_deleted = 0
        try:
            with db_lock:
                cursor_local.execute('''
                    DELETE FROM movies 
                    WHERE chat_id = %s 
                      AND watched = 0
                      AND id NOT IN (SELECT DISTINCT film_id FROM plans WHERE chat_id = %s AND film_id IS NOT NULL)
                      AND id NOT IN (SELECT DISTINCT film_id FROM watched_movies WHERE chat_id = %s AND film_id IS NOT NULL)
                ''', (chat_id, chat_id, chat_id))
                movies_deleted = cursor_local.rowcount
                conn_local.commit()
                logger.info(f"[CLEAN CONFIRM] Удалено непросмотренных фильмов: {movies_deleted}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        action_text = "✅ ДЕЙСТВИЕ ВЫПОЛНЕНО: Удаление непросмотренных фильмов"
        result_text = f"{action_text}\n\nУдалено непросмотренных фильмов: {movies_deleted}"
        
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        if is_private:
            bot.send_message(message.chat.id, result_text)
        else:
            bot.reply_to(message, result_text)
        
        logger.info(f"[CLEAN CONFIRM] ✅ Завершено удаление непросмотренных фильмов: user_id={user_id}, chat_id={chat_id}")
        del user_clean_state[user_id]
    
    else:
        logger.warning(f"[CLEAN CONFIRM] Неизвестный target: {target}")
        bot.reply_to(message, "❌ Неизвестный тип удаления")
        if user_id in user_clean_state:
            del user_clean_state[user_id]

def process_random_plan(message, text: str):
    user_id = message.from_user.id
    chat_id = message.chat.id

    plan_data = random_plan_data.get(user_id)
    if not plan_data:
        bot.send_message(chat_id, "❌ Сессия истекла. Запустите /random заново.")
        return

    title = plan_data['title']
    link = plan_data['link']
    kp_id = plan_data['kp_id']

    place, date_raw = parse_plan_input(text)  # твой парсер места

    if not place:
        bot.send_message(chat_id, "❌ Укажите место: «дома» или «в кино»")
        return
    if not date_raw:
        bot.send_message(chat_id, "❌ Укажите дату после места")
        return

    planned_dt = parse_plan_date_text(date_raw, user_id)

    if not planned_dt:
        bot.send_message(chat_id, "❌ Не понял дату. Примеры: завтра, 20.01, 15 января, в пятницу 20:00")
        return

    # Здесь вызов твоей функции добавления в планы
    success = add_film_to_plans(
        chat_id=chat_id,
        user_id=user_id,
        title=title,
        link=link,
        kp_id=kp_id,
        place=place,
        planned_at=planned_dt
    )

    if success:
        formatted = planned_dt.astimezone(PLANS_TZ).strftime("%d.%m %H:%M")
        bot.send_message(chat_id, f"✅ «{title}» запланирован!\n\n{place.capitalize()} — {formatted}")
    else:
        bot.send_message(chat_id, "❌ Не удалось сохранить план")

    # Очистка
    random_plan_data.pop(user_id, None)
    user_expected_text.pop(user_id, None)

# Личка: следующее сообщение после рандома
@bot.message_handler(content_types=['text'], func=is_expected_text_in_private)
def handle_expected_text_in_private(message):
    user_id = message.from_user.id
    state = user_expected_text.get(user_id)
    if not state:
        return

    query = message.text.strip()
    expected_for = state['expected_for']

    del user_expected_text[user_id]  # всегда очищаем

    if expected_for == 'search':
        process_search_query(message, query, reply_to_message=None)
    elif expected_for == 'random_plan':
        process_random_plan(message, query)



# Группа: reply на сообщение фильма или инструкцию
@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and
                                      m.reply_to_message and
                                      m.reply_to_message.from_user.id == bot.get_me().id and
                                      m.reply_to_message.message_id in bot_messages)
def handle_group_random_plan_reply(message):
    query = message.text.strip()
    if not query:
        return
    process_random_plan(message, query)

# === Вспомогательная функция для промпта (личка/группа) ===
def send_event_prompt(bot, message_or_call, state, text, markup=None):
    chat_id = state['chat_id']
    if message_or_call.chat.type == 'private':
        sent = bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup)
    else:
        reply_to = getattr(message_or_call, 'reply_to_message', None) or message_or_call.message
        sent = bot.reply_to(reply_to, text, parse_mode='HTML', reply_markup=markup)
    state['prompt_message_id'] = sent.message_id
    return sent

# === Текст: название и дата ===
def is_event_text(message):
    # Пропускаем, если пользователь в состоянии /add_tags
    from moviebot.bot.handlers.tags import user_add_tag_state
    user_id = message.from_user.id
    if user_id in user_add_tag_state:
        state_tag = user_add_tag_state.get(user_id, {})
        if state_tag.get('step') == 'waiting_for_tag_data' and message.reply_to_message:
            prompt_message_id = state_tag.get('prompt_message_id')
            if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
                return False  # Пропускаем - обработает handle_add_tag_reply
    
    user_id = message.from_user.id
    state = user_ticket_state.get(user_id, {})
    return (state.get('type') == 'event' and state.get('step') in ['event_add_name', 'event_add_date'])

@bot.message_handler(content_types=['text'], func=is_event_text)
def handle_event_text(message):
    user_id = message.from_user.id
    state = user_ticket_state[user_id]
    step = state['step']

    if step == 'event_add_name':
        custom_title = message.text.strip()
        if not custom_title:
            send_event_prompt(bot, message, state, "❌ Название не может быть пустым. Попробуйте ещё раз.")
            return

        state['custom_title'] = custom_title
        state['step'] = 'event_add_date'

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        send_event_prompt(bot, message, state,
                          "Отлично! Теперь укажите дату и время мероприятия.\n"
                          "Примеры:\n• 15 января 19:30\n• завтра 20:00\n• послезавтра",
                          markup)

    elif step == 'event_add_date':
        plan_dt = parse_plan_date_text(message.text, user_id)
        if not plan_dt:
            send_event_prompt(bot, message, state,
                              "❌ Не понял дату/время. Попробуйте ещё раз.\n"
                              "Примеры: 15 января 19:30 или «завтра 20:00»")
            return

        state['plan_datetime_utc'] = plan_dt.astimezone(pytz.UTC).replace(tzinfo=None)
        state['step'] = 'event_add_ticket'  # один билет

        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))

        send_event_prompt(bot, message, state,
                          "Теперь отправьте фото/файл для добавления билета.\n\n",
                          markup)

# === Фото/файл: один билет ===
def is_event_file(message):
    user_id = message.from_user.id
    state = user_ticket_state.get(user_id, {})
    if state.get('type') != 'event' or state.get('step') != 'event_add_ticket':
        return False
    
    # В группе — только реплай на промпт
    if message.chat.type != 'private':
        prompt_id = state.get('prompt_message_id')
        return (prompt_id and message.reply_to_message and
                message.reply_to_message.message_id == prompt_id and
                message.reply_to_message.from_user.id == bot.get_me().id)
    return True

@bot.message_handler(content_types=['photo', 'document'], func=is_event_file)
def handle_event_file(message):
    user_id = message.from_user.id
    state = user_ticket_state[user_id]

    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document:
        file_id = message.document.file_id

    if not file_id:
        return

    # Сохраняем как массив с одним элементом (для совместимости)
    tickets_json = json.dumps([file_id])

    conn = get_db_connection()
    cursor = get_db_cursor()
    try:
        with db_lock:
            cursor.execute("""
                INSERT INTO plans 
                (chat_id, user_id, film_id, custom_title, plan_type, plan_datetime, ticket_file_id)
                VALUES (%s, %s, NULL, %s, 'cinema', %s, %s)
                RETURNING id
            """, (state['chat_id'], user_id, state['custom_title'],
                  state['plan_datetime_utc'], tickets_json))
            plan_id = cursor.fetchone()[0]
            conn.commit()

        # Успех
        bot.edit_message_text("💾 Билет сохранён!", state['chat_id'], state['prompt_message_id'], parse_mode='HTML')

        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎟️ Билеты", callback_data=f"show_ticket:{plan_id}"))
        add_more_btn = "🔒 Добавить ещё билеты" if not has_pro_access(state['chat_id'], user_id) else "➕ Добавить ещё билеты"
        markup.add(InlineKeyboardButton(add_more_btn, callback_data=f"add_more_tickets:{plan_id}"))
        markup.add(InlineKeyboardButton("🔄 Заменить билеты", callback_data=f"add_ticket:{plan_id}"))

        bot.send_message(state['chat_id'], "Управление планом:", reply_markup=markup)

    except Exception as e:
        logger.error(f"[EVENT TICKET SAVE] Ошибка: {e}", exc_info=True)
        conn.rollback()
        send_event_prompt(bot, message, state, "❌ Ошибка сохранения билета. Попробуйте ещё раз.")
    finally:
        cursor.close()
        conn.close()
        del user_ticket_state[user_id]

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode:"))
def handle_rand_mode(call):
    """Обработчик выбора режима рандомайзера"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        data_parts = call.data.split(":", 1)
        if len(data_parts) < 2:
            bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
            return
        mode_or_action = data_parts[1]
        
        # Обработка кнопки "Назад к режимам"
        if mode_or_action == "back":
            logger.info(f"[RANDOM CALLBACK] Back to mode selection")
            bot.answer_callback_query(call.id)
            
            # Показываем выбор режима — 1) база, 2) по оценкам в базе (всегда), далее режимы PRO
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            has_rec_access = has_recommendations_access(chat_id, user_id)
            if has_rec_access:
                markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
                markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            bot.edit_message_text(
                "🎲 <b>Выберите режим рандома:</b>",
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
            # Очищаем состояние
            if user_id in user_random_state:
                user_random_state[user_id] = {
                    'step': 'mode',
                    'mode': None,
                    'periods': [],
                    'genres': [],
                    'directors': [],
                    'actors': []
                }
            return
        
        mode = mode_or_action
        
        # Инициализируем состояние, если его нет (может быть утеряно при перезапуске бота или долгом ожидании)
        if user_id not in user_random_state:
            logger.info(f"[RANDOM CALLBACK] Состояние не найдено для user_id={user_id}, инициализируем новое")
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }

        state = user_random_state[user_id]
        
        logger.info(f"[RANDOM CALLBACK] Mode: {mode}, user_id={user_id}, chat_id={chat_id}")
        
        # Проверяем доступ к рекомендациям только для режимов PRO (по оценкам в базе — всегда доступен)
        if mode in ['kinopoisk', 'my_votes']:
            has_rec_access = has_recommendations_access(chat_id, user_id)
            logger.info(f"[RANDOM CALLBACK] Mode {mode} requires recommendations access: {has_rec_access}")
            if not has_rec_access:
                bot.answer_callback_query(
                    call.id, 
                    "❌ Этот режим доступен с подпиской 💎 Movie Planner PRO. Используйте /payment.", 
                    show_alert=True
                )
                logger.warning(f"[RANDOM CALLBACK] Access denied for mode {mode}, user_id={user_id}")
                return
            
        if mode == 'database':
            # Проверяем количество фильмов в базе для данного chat_id (работает и для личных, и для групповых чатов)
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            count = 0
            try:
                with db_lock:
                    cursor_local.execute('SELECT COUNT(*) FROM movies WHERE chat_id = %s', (chat_id,))
                    count_row = cursor_local.fetchone()
                    count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if count == 0:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(
                    InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search")
                )
                markup.add(
                    InlineKeyboardButton("⬅️ Назад к режимам", callback_data="start_menu:random")
                )

                bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=call.message.message_id,
                    text=(
                        "😔 <b>В вашей базе пока нет фильмов</b>\n\n"
                        "Рандом по своей базе работает только когда в базе есть хотя бы один фильм или сериал.\n\n"
                        "Что делаем дальше?"
                    ),
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                bot.answer_callback_query(call.id)
                logger.info(f"[RANDOM] Пустая база chat_id={chat_id}, user_id={user_id} — показываем кнопки в главное меню")
                return
        
        logger.info(f"[RANDOM CALLBACK] State found: {user_random_state[user_id]}")
        
        user_random_state[user_id]['mode'] = mode
        # Первый этап - выбор типа контента (фильмы/сериалы/пропустить)
        user_random_state[user_id]['step'] = 'content_type'
        user_random_state[user_id]['content_type'] = None  # 'films', 'series', или 'mixed' (если пропустить)
        
        logger.info(f"[RANDOM CALLBACK] State updated: mode={mode}, step=content_type")
        
        # Добавляем справку о режиме
        mode_descriptions = {
            'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
            'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм по вашим фильтрам.',
            'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
            'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nПолучите рекомендацию, основанную на оценках в вашей локальной базе.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
        }
        mode_description = mode_descriptions.get(mode, '')
        
        # Показываем выбор типа контента
        bot.answer_callback_query(call.id)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎬 Фильмы", callback_data=f"rand_content_type:{mode}:films"))
        markup.add(InlineKeyboardButton("📺 Сериалы", callback_data=f"rand_content_type:{mode}:series"))
        markup.add(InlineKeyboardButton("▶️ Пропустить", callback_data=f"rand_content_type:{mode}:mixed"))
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        text = f"{mode_description}\n\nВыберите, будем искать сериалы или фильмы:"
        
        try:
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] Ошибка редактирования сообщения: {e}", exc_info=True)
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        return
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] Ошибка в handle_rand_mode: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass
        

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode_locked:"))
def handle_rand_mode_locked(call):
    """Обработчик заблокированных режимов рандомайзера"""
    try:
        logger.info(f"[RANDOM CALLBACK] Locked mode handler: data={call.data}, user_id={call.from_user.id}")
        mode = call.data.split(":")[1]  # kinopoisk, my_votes, group_votes
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # По оценкам в базе — всегда доступен; при нажатии на старую кнопку с замком открываем режим
        if mode == "group_votes":
            call.data = "rand_mode:group_votes"
            handle_rand_mode(call)
            return
        # Проверяем доступ для остальных заблокированных режимов
        if mode in ['kinopoisk', 'my_votes']:
            has_rec_access = has_recommendations_access(chat_id, user_id)
            logger.info(f"[RANDOM CALLBACK] Locked mode {mode} - проверка доступа: {has_rec_access}")
            if has_rec_access:
                call.data = f"rand_mode:{mode}"
                handle_rand_mode(call)
                return
        
        if mode == "kinopoisk":
            message_text = "🎬 Рандом по Кинопоиску доступен с подпиской 💎 Movie Planner PRO. Подключите через /payment"
        elif mode == "shazam":
            message_text = "🔮 Шазам доступен с подпиской 💎 Movie Planner PRO. Подключите через /payment"
        elif mode == "my_votes":
            # Проверяем количество оценок
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            user_ratings = 0
            try:
                with db_lock:
                    cursor_local.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    user_ratings_count = cursor_local.fetchone()
                    user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if user_ratings < 50:
                message_text = "⭐ Режим «По моим оценкам» откроется после добавления 50 оценок в базу. Оцените больше фильмов!"
            else:
                message_text = "⭐ Режим «По моим оценкам» доступен с подпиской 💎 Movie Planner PRO. Подключите через /payment"
        else:
            message_text = "🔒 Этот режим недоступен. Подписка 💎 Movie Planner PRO — через /payment"
        
        bot.answer_callback_query(
            call.id,
            message_text,
            show_alert=True
        )
    except Exception as e:
        logger.error(f"[RAND MODE LOCKED] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(
                call.id,
                "🔒 Функционал можно подключить через /payment",
                show_alert=True
            )
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
        mode = state.get('mode')
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Genre step: mode={mode}, content_type={content_type}")
        
        # --------------------- Формируем запрос ---------------------
        params = []
        
        if mode == 'my_votes':
            # Жанры из импортированных оценок пользователя с оценкой 9-10
            # Учитываем content_type: films - только FILM, series - только TV_SERIES, mixed - оба
            # Используем UNION для объединения жанров из фильмов в базе группы и импортированных оценок
            
            # Добавляем фильтр по is_series для фильмов из базы группы
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            # mixed - фильтр не добавляем
            
            base_query = """
                SELECT DISTINCT genre FROM (
                    SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                    FROM movies m
                    JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                    AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—' """ + is_series_filter + """
            """
            params = [chat_id, user_id]
            
            # Добавляем фильтр по периодам для фильмов из базы группы
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
                    base_query += " AND (" + " OR ".join(period_conditions) + ")"
            
            # Добавляем фильтр по type для импортированных оценок (film_id = NULL)
            type_filter = ""
            if content_type == 'films':
                type_filter = "AND (r.type = 'FILM' OR (r.type IS NULL AND NOT EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 1)))"
            elif content_type == 'series':
                type_filter = "AND (r.type = 'TV_SERIES' OR (r.type IS NULL AND EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 1)))"
            # Если mixed - фильтр не добавляем
            
            base_query += """
                    UNION ALL
                    SELECT DISTINCT TRIM(UNNEST(string_to_array(r.genres, ', '))) as genre
                    FROM ratings r
                    WHERE r.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                    AND r.film_id IS NULL AND r.genres IS NOT NULL AND r.genres != '' AND r.genres != '—' """ + type_filter + """
            """
            params.append(chat_id)
            params.append(user_id)
            
            # Добавляем фильтр по периодам для импортированных оценок (используем сохраненный year)
            if periods:
                period_conditions = []
                for p in periods:
                    if p == "До 1980":
                        period_conditions.append("r.year < 1980")
                    elif p == "1980–1990":
                        period_conditions.append("(r.year >= 1980 AND r.year <= 1990)")
                    elif p == "1990–2000":
                        period_conditions.append("(r.year >= 1990 AND r.year <= 2000)")
                    elif p == "2000–2010":
                        period_conditions.append("(r.year >= 2000 AND r.year <= 2010)")
                    elif p == "2010–2020":
                        period_conditions.append("(r.year >= 2010 AND r.year <= 2020)")
                    elif p == "2020–сейчас":
                        period_conditions.append("r.year >= 2020")
                if period_conditions:
                    base_query += " AND (" + " OR ".join(period_conditions) + ")"
            
            base_query += """
                ) AS all_genres
                WHERE genre IS NOT NULL AND genre != ''
            """
            
        elif mode == 'group_votes':
            # Жанры из фильмов со средней оценкой группы >= 7.5
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            # mixed - фильтр не добавляем
            
            base_query = """
                SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                FROM movies m
                WHERE m.chat_id = %s """ + is_series_filter + """
                AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—'
                AND EXISTS (
                    SELECT 1 FROM ratings r 
                    WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                    GROUP BY r.film_id, r.chat_id 
                    HAVING AVG(r.rating) >= 7.5
                )
            """
            params = [chat_id]
            
        else:
            # Обычный режим (database) – жанры из непросмотренных фильмов/сериалов чата
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            # mixed - фильтр не добавляем
            
            base_query = """
                SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                FROM movies m
                LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL """ + is_series_filter + """
                AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—'
            """
            params = [chat_id]
        
        # --------------------- Фильтр по периодам ---------------------
        # Для my_votes фильтр по периодам уже применен в запросе выше
        # Для остальных режимов применяем фильтр здесь
        if periods and mode != 'my_votes':
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # --------------------- Выполняем запрос ---------------------
        genres = []  # всегда инициализируем, даже если запрос вернёт пусто
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                rows = cursor_local.fetchall()
                
                for row in rows:
                    genre = row.get('genre') if isinstance(row, dict) else (row[0] if row else None)
                    if genre and genre.strip():
                        genres.append(genre.strip())
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Исключаем нежелательные жанры
        genres = [g for g in genres if g.lower() not in [eg.lower() for eg in EXCLUDED_GENRES]]
        
        logger.info(f"[RANDOM] Genres found: {len(genres)}")
        
        # --------------------- Формируем клавиатуру ---------------------
        markup = InlineKeyboardMarkup(row_width=1)
        
        if genres:
            for genre in sorted(set(genres))[:20]:  # ограничиваем до 20 самых популярных
                label = f"✓ {genre}" if genre in selected_genres else genre
                markup.add(InlineKeyboardButton(label, callback_data=f"rand_genre:{genre}"))
        
        # Навигация
        nav_buttons = [
            InlineKeyboardButton("⬅️ Назад", callback_data="rand_genre:back")
        ]
        if selected_genres:
            nav_buttons.append(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_genre:done"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_genre:skip"))
        markup.row(*nav_buttons)
        # Добавляем кнопку "Назад к режимам" для my_votes и group_votes
        if mode in ['my_votes', 'group_votes']:
            markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        # Текст с выбранными жанрами
        selected_text = f"\n\nВыбрано: {', '.join(selected_genres)}" if selected_genres else ""
        
        # Определяем номер шага в зависимости от режима
        if mode in ['my_votes', 'group_votes']:
            step_text = "🎬 <b>Шаг 2/2: Выберите жанр</b>"
        else:
            step_text = "🎬 <b>Шаг 2/4: Выберите жанр</b>"
        
        text = f"{step_text}\n\n(можно выбрать несколько){selected_text}"
        
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id,
                                    reply_markup=markup, parse_mode='HTML')
            logger.info(f"[RANDOM] Genre step shown, user_id={user_id}, selected={len(selected_genres)}")
        except Exception as e:
            logger.warning(f"[RANDOM] Edit failed, sending new message: {e}")
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        bot.answer_callback_query(call.id)
        
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_genre_step: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки жанров")
        except:
            pass

def _show_genre_step_group_votes(call, chat_id, user_id):
    """Показывает шаг выбора жанра для режима group_votes - использует _show_genre_step"""
    _show_genre_step(call, chat_id, user_id)

def _show_director_step_group_votes(call, chat_id, user_id):
    """Показывает шаг выбора режиссёра для режима group_votes с учетом выбранных периодов и жанров"""
    try:
        logger.info(f"[RANDOM] Showing director step for group_votes mode, user {user_id}")
        
        # Получаем состояние пользователя
        state = user_random_state.get(user_id, {})
        selected_directors = state.get('directors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Director step group_votes: content_type={content_type}")
        
        # Формируем WHERE условие с учетом периодов, жанров и средней оценки >= 7.5
        # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
        is_series_filter = ""
        if content_type == 'films':
            is_series_filter = "AND m.is_series = 0"
        elif content_type == 'series':
            is_series_filter = "AND m.is_series = 1"
        # mixed - фильтр не добавляем
        
        base_query = """
            SELECT m.director, COUNT(*) as cnt
            FROM movies m
            WHERE m.chat_id = %s """ + is_series_filter + """
            AND m.director IS NOT NULL AND m.director != 'Не указан' AND m.director != ''
            AND EXISTS (
                SELECT 1 FROM ratings r 
                WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                GROUP BY r.film_id, r.chat_id 
                HAVING AVG(r.rating) >= 7.5
            )
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        base_query += " GROUP BY m.director"
        base_query += " ORDER BY cnt DESC LIMIT 10"
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        directors = []
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                rows = cursor_local.fetchall()
                for row in rows:
                    director = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if director:
                        directors.append(director)
            logger.info(f"[RANDOM] Directors found for group_votes: {len(directors)}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Если режиссеров нет, пропускаем шаг и переходим к актерам
        if not directors:
            logger.info(f"[RANDOM] No directors found for group_votes, skipping to actor step")
            _show_actor_step_group_votes(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
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
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_directors)}" if selected_directors else ""
        mode_description = '👥 <b>По оценкам в базе (7.5+)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
        try:
            # Для режима group_votes это шаг 3/3 (период, жанр, режиссёр)
            step_text = "🎥 <b>Шаг 3/3: Выберите режиссёра</b>"
            
            bot.edit_message_text(f"{mode_description}\n\n{step_text}\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                error_str = str(e)
                if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                    logger.warning(f"[RANDOM DIRECTOR GROUP_VOTES] Не удалось ответить на callback query: {e}")
            logger.info(f"[RANDOM] Director step shown for group_votes, user_id={user_id}, selected={len(selected_directors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing director step for group_votes: {e}", exc_info=True)
            bot.send_message(chat_id, f"{mode_description}\n\n🎥 <b>Шаг 3/4: Выберите режиссёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_director_step_group_votes: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки режиссёров")
        except:
            pass

def _show_actor_step_group_votes(call, chat_id, user_id):
    """Показывает шаг выбора актёра для режима group_votes с учетом всех выбранных фильтров"""
    try:
        logger.info(f"[RANDOM] Showing actor step for group_votes mode, user {user_id}")
        
        # Получаем состояние пользователя
        if user_id not in user_random_state:
            user_random_state[user_id] = {'actors': []}
        state = user_random_state[user_id]
        selected_actors = state.get('actors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        directors = state.get('directors', [])
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Actor step group_votes: content_type={content_type}")
        
        # Добавляем фильтр по is_series в зависимости от content_type
        is_series_filter = ""
        if content_type == 'films':
            is_series_filter = "AND m.is_series = 0"
        elif content_type == 'series':
            is_series_filter = "AND m.is_series = 1"
        else:
            is_series_filter = ""
        
        base_query += f" {is_series_filter}"
        params = [chat_id]

        base_query = """
            SELECT m.actors 
            FROM movies m
            WHERE m.chat_id = %s """ + is_series_filter + """
            AND m.actors IS NOT NULL AND m.actors != '' AND m.actors != '—'
            AND EXISTS (
                SELECT 1 FROM ratings r 
                WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                GROUP BY r.film_id, r.chat_id 
                HAVING AVG(r.rating) >= 7.5
            )
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Добавляем фильтр по режиссерам, если они выбраны
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("m.director = %s")
                params.append(director)
            if director_conditions:
                base_query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Берем топ актёров по частоте
        actor_counts = {}
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                for row in cursor_local.fetchall():
                    actors_str = row.get('actors') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if actors_str:
                        for actor in actors_str.split(', '):
                            actor = actor.strip()
                            if actor:
                                actor_counts[actor] = actor_counts.get(actor, 0) + 1
            logger.info(f"[RANDOM] Unique actors found for group_votes: {len(actor_counts)}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Если актеров нет, пропускаем шаг и переходим к финалу
        if not actor_counts:
            logger.info(f"[RANDOM] No actors found for group_votes, skipping to final step")
            _random_final(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
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
            nav_buttons.append(InlineKeyboardButton("🎲 Найти фильм", callback_data=f"rand_final:go:{user_id}"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_actor:skip"))
        markup.row(*nav_buttons)
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_actors)}" if selected_actors else ""
        mode_description = '👥 <b>По оценкам в базе (7.5+)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
        try:
            # Для режима group_votes это шаг 4/4 (период, жанр, режиссёр, актёр)
            # Но если режиссёр пропущен, то это шаг 3/3
            directors = state.get('directors', [])
            if directors:
                step_text = "🎭 <b>Шаг 4/4: Выберите актёра</b>"
            else:
                step_text = "🎭 <b>Шаг 3/3: Выберите актёра</b>"
            
            bot.edit_message_text(f"{mode_description}\n\n{step_text}\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                error_str = str(e)
                if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                    logger.warning(f"[RANDOM ACTOR GROUP_VOTES] Не удалось ответить на callback query: {e}")
            logger.info(f"[RANDOM] Actor step shown for group_votes, user_id={user_id}, selected={len(selected_actors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing actor step for group_votes: {e}", exc_info=True)
            bot.send_message(chat_id, f"{mode_description}\n\n🎭 <b>Шаг 4/4: Выберите актёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_actor_step_group_votes: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки актёров")
        except:
            pass

def _show_director_step_my_votes(call, chat_id, user_id):
    """Показывает шаг выбора режиссёра для режима my_votes - получает режиссеров из API"""
    try:
        logger.info(f"[RANDOM] Showing director step for my_votes mode, user {user_id}")
        
        # Получаем состояние пользователя
        state = user_random_state.get(user_id, {})
        selected_directors = state.get('directors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Director step my_votes: content_type={content_type}")
        
        # Получаем список kp_id фильмов с оценками 9-10, которые соответствуют периодам и жанрам
        # Учитываем content_type: films - только FILM, series - только TV_SERIES, mixed - оба
        # Используем UNION для объединения фильмов из базы группы и импортированных оценок
        
        # Добавляем фильтр по is_series для фильмов из базы группы
        is_series_filter = ""
        if content_type == 'films':
            is_series_filter = "AND m.is_series = 0"
        elif content_type == 'series':
            is_series_filter = "AND m.is_series = 1"
        # mixed - фильтр не добавляем
        
        base_query = """
            SELECT DISTINCT kp_id FROM (
                SELECT m.kp_id
                FROM movies m
                JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                AND m.kp_id IS NOT NULL """ + is_series_filter + """
        """
        params = [chat_id, user_id]
        
        # Добавляем фильтр по периодам для фильмов из базы группы
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам для фильмов из базы группы
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Добавляем фильтр по type для импортированных оценок (film_id = NULL)
        type_filter = ""
        if content_type == 'films':
            type_filter = "AND (r.type = 'FILM' OR (r.type IS NULL AND NOT EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 1)))"
        elif content_type == 'series':
            type_filter = "AND (r.type = 'TV_SERIES' OR (r.type IS NULL AND EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 1)))"
        # Если mixed - фильтр не добавляем
        
        base_query += """
                UNION ALL
                SELECT r.kp_id
                FROM ratings r
                WHERE r.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                AND r.film_id IS NULL AND r.kp_id IS NOT NULL """ + type_filter + """
        """
        params.append(chat_id)
        params.append(user_id)
        
        # Добавляем фильтр по периодам для импортированных оценок (используем сохраненный year)
        if periods:
            period_conditions = []
            for p in periods:
                if p == "До 1980":
                    period_conditions.append("r.year < 1980")
                elif p == "1980–1990":
                    period_conditions.append("(r.year >= 1980 AND r.year <= 1990)")
                elif p == "1990–2000":
                    period_conditions.append("(r.year >= 1990 AND r.year <= 2000)")
                elif p == "2000–2010":
                    period_conditions.append("(r.year >= 2000 AND r.year <= 2010)")
                elif p == "2010–2020":
                    period_conditions.append("(r.year >= 2010 AND r.year <= 2020)")
                elif p == "2020–сейчас":
                    period_conditions.append("r.year >= 2020")
            if period_conditions:
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        base_query += ") AS all_films"
        
        # Ограничиваем количество фильмов для производительности
        base_query += " LIMIT 50"
        
        kp_ids = []
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                rows = cursor_local.fetchall()
                for row in rows:
                    kp_id = row.get('kp_id') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if kp_id:
                        kp_ids.append(str(kp_id))
            
            logger.info(f"[RANDOM] Found {len(kp_ids)} films for my_votes director step")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if not kp_ids:
            logger.info(f"[RANDOM] No films found for my_votes, skipping to actor step")
            _show_actor_step_my_votes(call, chat_id, user_id)
            return
        
        # Получаем режиссеров через API
        from moviebot.api.kinopoisk_api import extract_movie_info
        directors_set = set()
        
        for kp_id in kp_ids[:30]:  # Ограничиваем до 30 для производительности
            try:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                if info and info.get('director') and info['director'] != 'Не указан':
                    directors_set.add(info['director'])
            except Exception as e:
                logger.warning(f"[RANDOM] Error getting info for kp_id={kp_id}: {e}")
                continue
        
        directors = sorted(list(directors_set))
        logger.info(f"[RANDOM] Directors found for my_votes: {len(directors)}")
        
        # Если режиссеров нет, пропускаем шаг и переходим к актерам
        if not directors:
            logger.info(f"[RANDOM] No directors found for my_votes, skipping to actor step")
            _show_actor_step_my_votes(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        # Ограничиваем до 10 режиссеров для отображения
        for d in directors[:10]:
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
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_directors)}" if selected_directors else ""
        mode_description = '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.'
        try:
            # Для режима my_votes это шаг 3/3 (период, жанр, режиссёр)
            step_text = "🎥 <b>Шаг 3/3: Выберите режиссёра</b>"
            
            bot.edit_message_text(f"{mode_description}\n\n{step_text}\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                error_str = str(e)
                if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                    logger.warning(f"[RANDOM DIRECTOR MY_VOTES] Не удалось ответить на callback query: {e}")
            logger.info(f"[RANDOM] Director step shown for my_votes, user_id={user_id}, selected={len(selected_directors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing director step for my_votes: {e}", exc_info=True)
            bot.send_message(chat_id, f"{mode_description}\n\n🎥 <b>Шаг 3/4: Выберите режиссёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_director_step_my_votes: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки режиссёров")
        except:
            pass

def _show_actor_step_my_votes(call, chat_id, user_id):
    """Показывает шаг выбора актёра для режима my_votes - получает актеров из API"""
    try:
        logger.info(f"[RANDOM] Showing actor step for my_votes mode, user {user_id}")
        
        # Получаем состояние пользователя
        if user_id not in user_random_state:
            user_random_state[user_id] = {'actors': []}
        state = user_random_state[user_id]
        selected_actors = state.get('actors', [])
        periods = state.get('periods', [])
        genres = state.get('genres', [])
        directors = state.get('directors', [])
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        logger.info(f"[RANDOM] Actor step my_votes: content_type={content_type}")
        
        # Получаем список kp_id фильмов с оценками 9-10, которые соответствуют фильтрам
        # Учитываем content_type: films - только FILM, series - только TV_SERIES, mixed - оба
        
        # Добавляем фильтр по is_series для фильмов из базы группы
        is_series_filter = ""
        if content_type == 'films':
            is_series_filter = "AND m.is_series = 0"
        elif content_type == 'series':
            is_series_filter = "AND m.is_series = 1"
        # mixed - фильтр не добавляем
        
        base_query = """
            SELECT DISTINCT m.kp_id
            FROM movies m
            JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
            WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
            AND m.kp_id IS NOT NULL """ + is_series_filter + """
        """
        params = [chat_id, user_id]
        
        # Добавляем фильтр по периодам, если они выбраны
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Добавляем фильтр по режиссерам, если они выбраны (проверяем по БД, если есть в базе)
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("m.director = %s")
                params.append(director)
            if director_conditions:
                base_query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Ограничиваем количество фильмов для производительности
        base_query += " LIMIT 50"
        
        kp_ids = []
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                rows = cursor_local.fetchall()
                for row in rows:
                    kp_id = row.get('kp_id') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if kp_id:
                        kp_ids.append(str(kp_id))
            
            logger.info(f"[RANDOM] Found {len(kp_ids)} films for my_votes actor step")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if not kp_ids:
            logger.info(f"[RANDOM] No films found for my_votes, skipping to final step")
            _random_final(call, chat_id, user_id)
            return
        
        # Получаем актеров через API
        from moviebot.api.kinopoisk_api import extract_movie_info
        import requests
        from moviebot.config import KP_TOKEN
        
        actors_counts = {}
        
        for kp_id in kp_ids[:30]:  # Ограничиваем до 30 для производительности
            try:
                # Получаем информацию о фильме через API (используем staff endpoint для получения всех актеров)
                headers = {'X-API-KEY': KP_TOKEN}
                url_staff = f"https://kinopoiskapiunofficial.tech/api/v1/staff?filmId={kp_id}"
                response_staff = requests.get(url_staff, headers=headers, timeout=10)
                
                if response_staff.status_code == 200:
                    staff = response_staff.json()
                    for person in staff:
                        if not isinstance(person, dict):
                            continue
                        profession = person.get('professionKey') or person.get('professionText') or person.get('profession')
                        if profession and ('ACTOR' in str(profession).upper() or 'АКТЕР' in str(profession).upper()):
                            name = person.get('nameRu') or person.get('nameEn') or person.get('name') or person.get('staffName')
                            if name:
                                actors_counts[name] = actors_counts.get(name, 0) + 1
                
                # Дополнительно проверяем фильтр по режиссерам через API (если фильтр был по режиссерам)
                if directors:
                    link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                    info = extract_movie_info(link)
                    if info and info.get('director'):
                        if info['director'] not in directors:
                            # Если режиссер не совпадает, исключаем актеров этого фильма
                            continue
            except Exception as e:
                logger.warning(f"[RANDOM] Error getting actors for kp_id={kp_id}: {e}")
                continue
        
        logger.info(f"[RANDOM] Unique actors found for my_votes: {len(actors_counts)}")
        
        # Если актеров нет, пропускаем шаг и переходим к финалу
        if not actors_counts:
            logger.info(f"[RANDOM] No actors found for my_votes, skipping to final step")
            _random_final(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
        # Берем топ 10 актеров по частоте
        top_actors = sorted(actors_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for actor, _ in top_actors:
            label = f"✓ {actor}" if actor in selected_actors else actor
            markup.add(InlineKeyboardButton(label, callback_data=f"rand_actor:{actor}"))
        
        # Кнопки навигации: "Назад" и "Пропустить"/"Найти фильм" в одной строке
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rand_actor:back"))
        if selected_actors:
            nav_buttons.append(InlineKeyboardButton("🎲 Найти фильм", callback_data=f"rand_final:go:{user_id}"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_actor:skip"))
        markup.row(*nav_buttons)
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
        
        selected_text = f"\n\nВыбрано: {', '.join(selected_actors)}" if selected_actors else ""
        mode_description = '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.'
        try:
            # Для режима my_votes это шаг 4/4 (период, жанр, режиссёр, актёр)
            # Но если режиссёр пропущен, то это шаг 3/3
            directors = state.get('directors', [])
            if directors:
                step_text = "🎭 <b>Шаг 4/4: Выберите актёра</b>"
            else:
                step_text = "🎭 <b>Шаг 3/3: Выберите актёра</b>"
            
            bot.edit_message_text(f"{mode_description}\n\n{step_text}\n\n(можно выбрать несколько){selected_text}", 
                                chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                error_str = str(e)
                if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                    logger.warning(f"[RANDOM ACTOR MY_VOTES] Не удалось ответить на callback query: {e}")
            logger.info(f"[RANDOM] Actor step shown for my_votes, user_id={user_id}, selected={len(selected_actors)}")
        except Exception as e:
            logger.error(f"[RANDOM] Error showing actor step for my_votes: {e}", exc_info=True)
            bot.send_message(chat_id, f"{mode_description}\n\n🎭 <b>Шаг 4/4: Выберите актёра</b>\n\n(можно выбрать несколько){selected_text}", 
                            reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_actor_step_my_votes: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки актёров")
        except:
            pass
        

def _show_genre_step_kinopoisk(call, chat_id, user_id):
    """Показывает шаг выбора жанра для режима kinopoisk - жанры из API Кинопоиска"""
    try:
        logger.info(f"[RANDOM] Showing genre step for kinopoisk mode, user {user_id}")
        
        state = user_random_state.get(user_id, {})
        selected_genres = state.get('genres', [])
        selected_periods = state.get('periods', [])
        content_type = state.get('content_type', 'ALL')
        
        # Получаем жанры из API Кинопоиска
        from moviebot.api.kinopoisk_api import get_film_filters
        api_genres = get_film_filters()
        
        if not api_genres:
            # Если не удалось получить жанры из API, показываем стандартный список
            all_genres = [
                {'id': 1, 'genre': 'триллер'}, {'id': 2, 'genre': 'драма'}, {'id': 3, 'genre': 'криминал'},
                {'id': 4, 'genre': 'мелодрама'}, {'id': 5, 'genre': 'детектив'}, {'id': 6, 'genre': 'фантастика'},
                {'id': 7, 'genre': 'приключения'}, {'id': 11, 'genre': 'боевик'}, {'id': 12, 'genre': 'фэнтези'},
                {'id': 13, 'genre': 'комедия'}, {'id': 17, 'genre': 'ужасы'}, {'id': 18, 'genre': 'мультфильм'},
                {'id': 19, 'genre': 'семейный'}, {'id': 14, 'genre': 'военный'}, {'id': 15, 'genre': 'история'}
            ]
        else:
            all_genres = api_genres
        
        # Исключаем нежелательные жанры
        all_genres = [g for g in all_genres if g.get('genre', '').lower() not in [eg.lower() for eg in EXCLUDED_GENRES]]
        
        # Ограничиваем до 3 выбранных жанров
        max_selected = 3
        if len(selected_genres) >= max_selected:
            # Показываем только выбранные жанры
            display_genres = [g for g in all_genres if str(g.get('id', '')) in selected_genres or g.get('genre', '').lower() in selected_genres]
        else:
            display_genres = all_genres
        
        markup = InlineKeyboardMarkup(row_width=2)
        for genre_item in display_genres:
            genre_id = str(genre_item.get('id', ''))
            genre_name = genre_item.get('genre', '')
            
            # Проверяем, выбран ли жанр (по id или по названию)
            is_selected = genre_id in selected_genres or genre_name.lower() in [g.lower() for g in selected_genres]
            
            if is_selected:
                label = f"✓ {genre_name}"
            else:
                label = genre_name
            
            # Если уже выбрано 3 жанра, неактивные жанры не добавляем
            if len(selected_genres) >= max_selected and not is_selected:
                continue
            
            markup.add(InlineKeyboardButton(label, callback_data=f"rand_genre:{genre_id}"))
        
        nav_buttons = []
        nav_buttons.append(InlineKeyboardButton("⬅️ Назад", callback_data="rand_genre:back"))
        if selected_genres:
            nav_buttons.append(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_genre:done"))
        else:
            nav_buttons.append(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_genre:skip"))
        markup.row(*nav_buttons)
        
        # Формируем текст с выбранными фильтрами
        filter_parts = []
        content_type_text = {
            'FILM': '🎬 Фильм',
            'TV_SERIES': '📺 Сериал',
            'ALL': '🎬 Фильм и Сериал'
        }.get(content_type, '')
        if content_type_text:
            filter_parts.append(f"Тип: {content_type_text}")
        if selected_periods:
            filter_parts.append(f"Период: {', '.join(selected_periods)}")
        if selected_genres:
            # Получаем названия жанров по id
            selected_genre_names = []
            for g_id in selected_genres:
                for g_item in all_genres:
                    if str(g_item.get('id', '')) == g_id or g_item.get('genre', '').lower() == g_id.lower():
                        selected_genre_names.append(g_item.get('genre', g_id))
                        break
            if selected_genre_names:
                filter_parts.append(f"Жанр: {', '.join(selected_genre_names)}")
        
        selected_text = f"\n\nВыбрано: {'; '.join(filter_parts)}" if filter_parts else ""
        mode_description = '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.'
        
        genre_limit_text = f"\n\n(можно выбрать до {max_selected} жанров или пропустить)" if len(selected_genres) < max_selected else f"\n\n(выбрано {len(selected_genres)}/{max_selected} жанров)"
        text = f"{mode_description}\n\n🎬 <b>Шаг 3/3: Выберите жанр</b>{genre_limit_text}{selected_text}"
        
        try:
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        bot.answer_callback_query(call.id)
        logger.info(f"[RANDOM] Genre step shown for kinopoisk, user_id={user_id}")
    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _show_genre_step_kinopoisk: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки жанров")
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
            SELECT m.director, COUNT(*) as cnt
            FROM movies m
            LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
            WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
            AND m.director IS NOT NULL AND m.director != 'Не указан' AND m.director != ''
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        base_query += " GROUP BY m.director"
        base_query += " ORDER BY cnt DESC LIMIT 10"
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        directors = []
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                rows = cursor_local.fetchall()
                for row in rows:
                    director = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if director:
                        directors.append(director)
            logger.info(f"[RANDOM] Directors found: {len(directors)}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Если режиссеров нет, пропускаем шаг и переходим к актерам
        if not directors:
            logger.info(f"[RANDOM] No directors found, skipping to actor step")
            _show_actor_step(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
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
            SELECT m.actors 
            FROM movies m
            LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
            WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
            AND m.actors IS NOT NULL AND m.actors != '' AND m.actors != '—'
        """
        params = [chat_id]
        
        # Добавляем фильтр по периодам, если они выбраны
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
                base_query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Добавляем фильтр по жанрам, если они выбраны
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                base_query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Добавляем фильтр по режиссерам, если они выбраны
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("m.director = %s")
                params.append(director)
            if director_conditions:
                base_query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Берем топ актёров по частоте
        actor_counts = {}
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(base_query, params)
                for row in cursor_local.fetchall():
                    actors_str = row.get('actors') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if actors_str:
                        for actor in actors_str.split(', '):
                            actor = actor.strip()
                            if actor:
                                actor_counts[actor] = actor_counts.get(actor, 0) + 1
            logger.info(f"[RANDOM] Unique actors found: {len(actor_counts)}")
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Если актеров нет, пропускаем шаг и переходим к финалу
        if not actor_counts:
            logger.info(f"[RANDOM] No actors found, skipping to final step")
            _random_final(call, chat_id, user_id)
            return
        
        markup = InlineKeyboardMarkup(row_width=1)
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

# ========== ОБРАБОТЧИКИ CALLBACK ДЛЯ РАНДОМА ==========

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_genre:"))
def handle_rand_genre(call):
    """Обработчик выбора жанра для рандома"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== GENRE HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        # Инициализируем состояние, если его нет
        if user_id not in user_random_state:
            logger.info(f"[RANDOM CALLBACK] Состояние не найдено для user_id={user_id}, инициализируем новое")
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }

        state = user_random_state[user_id]
        data = call.data.split(":", 1)[1]
        
        mode = user_random_state[user_id].get('mode')
        
        # Обрабатываем выбор жанра (toggle)
        if data not in ["skip", "done", "back"]:
            # Toggle жанра
            genres = user_random_state[user_id].get('genres', [])
            if data in genres:
                genres.remove(data)
                logger.info(f"[RANDOM CALLBACK] Genre removed: {data}")
            else:
                # Для kinopoisk ограничиваем до 3 жанров
                if mode == 'kinopoisk' and len(genres) >= 3:
                    bot.answer_callback_query(call.id, "Можно выбрать максимум 3 жанра", show_alert=True)
                    return
                genres.append(data)
                logger.info(f"[RANDOM CALLBACK] Genre added: {data}")
            
            user_random_state[user_id]['genres'] = genres
            user_random_state[user_id]['step'] = 'genre'
            
            # Для режимов my_votes, group_votes и kinopoisk после выбора жанра обновляем клавиатуру
            if mode == 'kinopoisk':
                _show_genre_step_kinopoisk(call, chat_id, user_id)
                return
            elif mode == 'group_votes':
                _show_genre_step_group_votes(call, chat_id, user_id)
                return
            elif mode == 'my_votes':
                # Для my_votes обновляем клавиатуру жанров
                _show_genre_step(call, chat_id, user_id)
                return
            else:
                # Для обычного режима обновляем клавиатуру
                _show_genre_step(call, chat_id, user_id)
                return
        
        # Для режима kinopoisk после подтверждения жанров сразу переходим к финалу
        if mode == 'kinopoisk':
            if data == "skip":
                user_random_state[user_id]['genres'] = []
            elif data == "done":
                pass  # Жанры уже сохранены
            elif data == "back":
                # Возврат к выбору периода
                logger.info(f"[RANDOM CALLBACK] Genre back, moving to period")
                user_random_state[user_id]['step'] = 'period'
                # Показываем шаг периодов
                periods = user_random_state[user_id].get('periods', [])
                available_periods = user_random_state[user_id].get('available_periods', [])
                if not available_periods:
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                
                markup = InlineKeyboardMarkup(row_width=1)
                if available_periods:
                    for period in available_periods:
                        label = f"✓ {period}" if period in periods else period
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{period}"))
                
                if periods:
                    markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
                else:
                    markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
                markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="rand_content_type:back"))
                
                selected = ', '.join(periods) if periods else 'ничего'
                content_type = user_random_state[user_id].get('content_type', 'ALL')
                content_type_text = {
                    'FILM': '🎬 Фильм',
                    'TV_SERIES': '📺 Сериал',
                    'ALL': '🎬 Фильм и Сериал'
                }.get(content_type, '')
                mode_description = '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.'
                text = f"{mode_description}\n\nВыбрано: {content_type_text}\n\n🎲 <b>Шаг 2/3: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    bot.answer_callback_query(call.id)
                except Exception as e:
                    logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                    bot.answer_callback_query(call.id, "Ошибка обновления")
                return
            
            # Переходим к финалу
            logger.info(f"[RANDOM CALLBACK] Mode kinopoisk: moving to final")
            user_random_state[user_id]['step'] = 'final'
            _random_final(call, chat_id, user_id)
            return
        
        # Для режимов my_votes и group_votes после подтверждения жанров переходим к режиссерам
        if mode == 'group_votes':
            if data == "skip":
                user_random_state[user_id]['genres'] = []
            elif data == "done":
                pass  # Жанры уже сохранены
            elif data == "back":
                # Возврат к выбору периода
                logger.info(f"[RANDOM CALLBACK] Genre back, moving to period")
                user_random_state[user_id]['step'] = 'period'
                # Показываем шаг периодов
                periods = user_random_state[user_id].get('periods', [])
                available_periods = user_random_state[user_id].get('available_periods', [])
                if not available_periods:
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                
                markup = InlineKeyboardMarkup(row_width=1)
                if available_periods:
                    for period in available_periods:
                        label = f"✓ {period}" if period in periods else period
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{period}"))
                
                if periods:
                    markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
                else:
                    markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
                markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
                
                selected = ', '.join(periods) if periods else 'ничего'
                mode_description = '👥 <b>По оценкам в базе (7.5+)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
                text = f"{mode_description}\n\n🎲 <b>Шаг 1/2: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    bot.answer_callback_query(call.id)
                except Exception as e:
                    logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                    bot.answer_callback_query(call.id, "Ошибка обновления")
                return
            
            # Переходим к режиссерам
            logger.info(f"[RANDOM CALLBACK] Mode {mode}: genres selected, moving to director")
            user_random_state[user_id]['step'] = 'director'
            _show_director_step_group_votes(call, chat_id, user_id)
            return
        elif mode == 'my_votes':
            if data == "skip":
                user_random_state[user_id]['genres'] = []
            elif data == "done":
                pass  # Жанры уже сохранены
            elif data == "back":
                # Возврат к выбору периода
                logger.info(f"[RANDOM CALLBACK] Genre back, moving to period")
                user_random_state[user_id]['step'] = 'period'
                # Показываем шаг периодов
                periods = user_random_state[user_id].get('periods', [])
                available_periods = user_random_state[user_id].get('available_periods', [])
                if not available_periods:
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                
                markup = InlineKeyboardMarkup(row_width=1)
                if available_periods:
                    for period in available_periods:
                        label = f"✓ {period}" if period in periods else period
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{period}"))
                
                if periods:
                    markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
                else:
                    markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
                markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
                
                selected = ', '.join(periods) if periods else 'ничего'
                mode_description = '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.'
                text = f"{mode_description}\n\n🎲 <b>Шаг 1/2: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    bot.answer_callback_query(call.id)
                except Exception as e:
                    logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                    bot.answer_callback_query(call.id, "Ошибка обновления")
                return
            
            # Переходим к режиссерам
            logger.info(f"[RANDOM CALLBACK] Mode {mode}: genres selected, moving to director")
            user_random_state[user_id]['step'] = 'director'
            _show_director_step_my_votes(call, chat_id, user_id)
            return
        
        # Для обычного режима переходим к режиссёру
        if data == "skip":
            user_random_state[user_id]['genres'] = []
            user_random_state[user_id]['step'] = 'director'
            logger.info(f"[RANDOM CALLBACK] Genre skipped, moving to director")
            _show_director_step(call, chat_id, user_id)
        elif data == "done":
            logger.info(f"[RANDOM CALLBACK] Genres confirmed, moving to director")
            user_random_state[user_id]['step'] = 'director'
            _show_director_step(call, chat_id, user_id)
        elif data == "back":
            # Возврат к предыдущему шагу (периоды)
            logger.info(f"[RANDOM CALLBACK] Genre back, moving to period")
            user_random_state[user_id]['step'] = 'period'
            # Показываем шаг периодов
            periods = user_random_state[user_id].get('periods', [])
            available_periods = user_random_state[user_id].get('available_periods', [])
            if not available_periods:
                available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            
            markup = InlineKeyboardMarkup(row_width=1)
            if available_periods:
                for period in available_periods:
                    label = f"✓ {period}" if period in periods else period
                    markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{period}"))
            
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
                logger.error(f"[RANDOM CALLBACK] Error going back to period: {e}", exc_info=True)
                bot.answer_callback_query(call.id, "Ошибка")
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_genre: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_dir:"))
def handle_rand_dir(call):
    """Обработчик выбора режиссёра для рандома"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== DIRECTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        # Инициализируем состояние, если его нет
        if user_id not in user_random_state:
            logger.info(f"[RANDOM CALLBACK] Состояние не найдено для user_id={user_id}, инициализируем новое")
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }

        state = user_random_state[user_id]
        data = call.data.split(":", 1)[1]
        mode = state.get('mode')
        
        if data == "skip":
            user_random_state[user_id]['directors'] = []
            user_random_state[user_id]['step'] = 'actor'
            logger.info(f"[RANDOM CALLBACK] Director skipped, moving to actor")
            if 'actors' not in user_random_state[user_id]:
                user_random_state[user_id]['actors'] = []
            if mode == 'my_votes':
                _show_actor_step_my_votes(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_actor_step_group_votes(call, chat_id, user_id)
            else:
                _show_actor_step(call, chat_id, user_id)
        elif data == "done":
            logger.info(f"[RANDOM CALLBACK] Directors confirmed, moving to actor")
            user_random_state[user_id]['step'] = 'actor'
            if 'actors' not in user_random_state[user_id]:
                user_random_state[user_id]['actors'] = []
            if mode == 'my_votes':
                _show_actor_step_my_votes(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_actor_step_group_votes(call, chat_id, user_id)
            else:
                _show_actor_step(call, chat_id, user_id)
        elif data == "back":
            logger.info(f"[RANDOM CALLBACK] Director back, moving to genre")
            user_random_state[user_id]['step'] = 'genre'
            if mode == 'my_votes':
                _show_genre_step(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_genre_step_group_votes(call, chat_id, user_id)
            else:
                _show_genre_step(call, chat_id, user_id)
        else:
            # Toggle режиссера
            directors = user_random_state[user_id].get('directors', [])
            if data in directors:
                directors.remove(data)
                logger.info(f"[RANDOM CALLBACK] Director removed: {data}")
            else:
                directors.append(data)
                logger.info(f"[RANDOM CALLBACK] Director added: {data}")
            
            user_random_state[user_id]['directors'] = directors
            user_random_state[user_id]['step'] = 'director'
            
            # Обновляем клавиатуру в зависимости от режима
            if mode == 'my_votes':
                _show_director_step_my_votes(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_director_step_group_votes(call, chat_id, user_id)
            else:
                _show_director_step(call, chat_id, user_id)
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_dir: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_actor:"))
def handle_rand_actor(call):
    """Обработчик выбора актёра для рандома"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== ACTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        # Инициализируем состояние, если его нет
        if user_id not in user_random_state:
            logger.info(f"[RANDOM CALLBACK] Состояние не найдено для user_id={user_id}, инициализируем новое")
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }

        state = user_random_state[user_id]
        data = call.data.split(":", 1)[1]
        mode = state.get('mode')
        
        if data == "skip":
            user_random_state[user_id]['actors'] = []
            user_random_state[user_id]['step'] = 'final'
            logger.info(f"[RANDOM CALLBACK] Actors skipped, moving to final")
            _random_final(call, chat_id, user_id)
        elif data == "back":
            logger.info(f"[RANDOM CALLBACK] Actor back, moving to director")
            user_random_state[user_id]['step'] = 'director'
            if mode == 'my_votes':
                _show_director_step_my_votes(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_director_step_group_votes(call, chat_id, user_id)
            else:
                _show_director_step(call, chat_id, user_id)
        else:
            # Toggle актёра
            actors = user_random_state[user_id].get('actors', [])
            if data in actors:
                actors.remove(data)
                logger.info(f"[RANDOM CALLBACK] Actor removed: {data}")
            else:
                actors.append(data)
                logger.info(f"[RANDOM CALLBACK] Actor added: {data}")
            
            user_random_state[user_id]['actors'] = actors
            user_random_state[user_id]['step'] = 'actor'
            
            # Обновляем клавиатуру в зависимости от режима
            if mode == 'my_votes':
                _show_actor_step_my_votes(call, chat_id, user_id)
            elif mode == 'group_votes':
                _show_actor_step_group_votes(call, chat_id, user_id)
            else:
                _show_actor_step(call, chat_id, user_id)
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_actor: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_final:"))
def handle_rand_final(call):
    """Обработчик финального шага рандома"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== FINAL HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id

        # === СПЕЦИАЛЬНЫЙ СЛУЧАЙ: кнопка "Найти фильм" из случайных событий (без состояния) ===
        if call.data.startswith("rand_final:go"):
            # Парсим callback_data: rand_final:go или rand_final:go:participant_id
            parts = call.data.split(":")
            expected_participant_id = None
            if len(parts) > 2:
                try:
                    expected_participant_id = int(parts[2])
                except (ValueError, IndexError):
                    pass
            
            # Проверяем, что кнопка доступна только для выбранного участника
            if expected_participant_id is not None and user_id != expected_participant_id:
                try:
                    bot.answer_callback_query(call.id, "Эта кнопка доступна только для выбранного участника случайного события", show_alert=True)
                    logger.info(f"[RANDOM CALLBACK] Показана ошибка пользователю {user_id} (кнопка для {expected_participant_id})")
                except Exception as e:
                    logger.warning(f"[RANDOM CALLBACK] Не удалось показать ошибку: {e}")
                logger.info(f"[RANDOM CALLBACK] Пользователь {user_id} пытается использовать кнопку, предназначенную для {expected_participant_id}")
                return
            
            logger.info(f"[RANDOM CALLBACK] Кнопка 'Найти фильм' из случайных событий, запускаем рандом по своей базе")
            bot.answer_callback_query(call.id)
            
            # Инициализируем состояние для рандома по своей базе
            user_random_state[user_id] = {
                'step': 'final',
                'mode': 'database',
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }
            
            # Переходим к финальному шагу (без фильтров)
            _random_final(call, chat_id, user_id)
            return

        # === ЗАЩИТА ОТ УСТАРЕВШИХ CALLBACK (для всех остальных случаев рандома) ===
        if user_id not in user_random_state:
            bot.answer_callback_query(call.id)
            return

        state = user_random_state[user_id]

        # Основная логика — просто запускаем финальный поиск
        _random_final(call, chat_id, user_id)

    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_final: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rand_similar_page:"))
def handle_rand_similar_page(call):
    """Обработчик пагинации похожих фильмов"""
    try:
        logger.info(f"[RANDOM CALLBACK] ===== SIMILAR PAGE HANDLER: data={call.data}, user_id={call.from_user.id}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        parts = call.data.split(":")
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "Ошибка")
            return
        
        mode = parts[1]
        page = int(parts[2])
        
        # Получаем список фильмов из состояния
        if user_id not in user_random_state:
            bot.answer_callback_query(call.id, "Состояние потеряно", show_alert=True)
            return
        
        state = user_random_state[user_id]
        similar_films = state.get('similar_films', [])
        
        if not similar_films:
            bot.answer_callback_query(call.id, "Фильмы не найдены", show_alert=True)
            return
        
        # Показываем страницу
        show_similar_films_page(similar_films, chat_id, user_id, call.message.message_id, mode, page)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_similar_page: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка обработки")
        except:
            pass

# ========== ФУНКЦИЯ _random_final ==========

def _random_final(call, chat_id, user_id):
    global show_film_info_with_buttons  # ← ДОБАВЬ ЭТУ СТРОКУ СРАЗУ ПОСЛЕ def

    from moviebot.api.kinopoisk_api import extract_movie_info
    """Финальный шаг - поиск и показ фильма"""
    try:
        logger.info(f"[RANDOM] ===== FINAL: user_id={user_id}, chat_id={chat_id}")
        state = user_random_state.get(user_id, {})
        logger.info(f"[RANDOM] State: {state}")
        
        mode = state.get('mode')
        
        # Для режима "kinopoisk" используем новый API endpoint для поиска фильмов
        if mode == 'kinopoisk':
            # Получаем фильтры из состояния
            periods = state.get('periods', [])
            genres = state.get('genres', [])  # Это список id жанров
            content_type = state.get('content_type', 'mixed')  # films, series, mixed
            
            # Преобразуем content_type в формат API: FILM, TV_SERIES или None (для mixed не передаем type)
            film_type_api = None
            if content_type == 'films':
                film_type_api = 'FILM'
            elif content_type == 'series':
                film_type_api = 'TV_SERIES'
            # Если mixed - не передаем type (получим оба типа)
            
            logger.info(f"[RANDOM KINOPOISK] content_type={content_type}, film_type_api={film_type_api}")
            
            # Получаем любимый жанр из /total
            fav_genre = None
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('''
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
                    for row in cursor_local.fetchall():
                        genres_str = row.get('genres') if isinstance(row, dict) else row[0]
                        if genres_str:
                            for g in str(genres_str).split(', '):
                                if g.strip():
                                    genre_counts[g.strip()] = genre_counts.get(g.strip(), 0) + 1
                    if genre_counts:
                        fav_genre = max(genre_counts, key=genre_counts.get)
                        logger.info(f"[RANDOM KINOPOISK] Любимый жанр: {fav_genre}")
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Исключаем фильмы, которые уже в базе
            exclude_kp_ids = set()
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND kp_id IS NOT NULL', (chat_id,))
                    existing_movies = cursor_local.fetchall()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Получаем список жанров из API для преобразования названий в ID
            from moviebot.api.kinopoisk_api import get_film_filters
            api_genres = get_film_filters()
            genre_id_map = {}  # словарь: название -> ID
            if api_genres:
                for g in api_genres:
                    genre_name = g.get('genre', '').lower()
                    genre_id = g.get('id')
                    if genre_name and genre_id:
                        genre_id_map[genre_name] = genre_id
            
            # Преобразуем названия жанров в ID, если нужно
            genre_ids = []
            for genre in genres:
                if genre is None:
                    genre_ids.append(None)
                else:
                    # Проверяем, является ли это числом (ID)
                    try:
                        genre_id_int = int(genre)
                        genre_ids.append(genre_id_int)
                    except ValueError:
                        # Это название, ищем ID
                        genre_lower = genre.lower()
                        if genre_lower in genre_id_map:
                            genre_ids.append(genre_id_map[genre_lower])
                        else:
                            logger.warning(f"[RANDOM KINOPOISK] Жанр '{genre}' не найден в API, пропускаем")
            
            # Формируем список запросов: для каждого периода и каждого жанра (если выбрано несколько)
            search_queries = []
            
            # Если периоды не выбраны, используем один запрос без фильтра по годам
            if not periods:
                periods = [None]  # Один запрос без фильтра по годам
            
            # Если жанры не выбраны, используем один запрос без фильтра по жанрам
            if not genre_ids:
                genre_ids = [None]  # Один запрос без фильтра по жанрам
            
            # Формируем все комбинации периодов и жанров
            for period in periods:
                for genre_id in genre_ids:
                    year_from = None
                    year_to = None
                    
                    if period:
                        # Определяем год для периода
                        if period == "До 1980":
                            year_from = 1000
                            year_to = 1979
                        elif period == "1980–1990":
                            year_from = 1980
                            year_to = 1990
                        elif period == "1990–2000":
                            year_from = 1990
                            year_to = 2000
                        elif period == "2000–2010":
                            year_from = 2000
                            year_to = 2010
                        elif period == "2010–2020":
                            year_from = 2010
                            year_to = 2020
                        elif period == "2020–сейчас":
                            year_from = 2020
                            year_to = 3000
                    else:
                        # Если период не выбран, используем широкий диапазон
                        year_from = 1000
                        year_to = 3000
                    
                    search_queries.append({
                        'genre_id': genre_id,
                        'year_from': year_from,
                        'year_to': year_to,
                        'film_type_api': film_type_api  # FILM, TV_SERIES или None
                    })
            
            # Выполняем поиск по всем запросам
            all_films = []
            
            for query in search_queries:
                try:
                    # genre_id уже число или None
                    genre_param = query['genre_id']
                    films = search_films_by_filters(
                        genres=genre_param,
                        film_type=query['film_type_api'],  # FILM, TV_SERIES или None (для mixed)
                        year_from=query['year_from'],
                        year_to=query['year_to'],
                        page=1
                    )
                    all_films.extend(films)
                    logger.info(f"[RANDOM KINOPOISK] Найдено {len(films)} фильмов для запроса: genre={query['genre_id']}, year={query['year_from']}-{query['year_to']}, type={query['film_type_api']}")
                except Exception as e:
                    logger.error(f"[RANDOM KINOPOISK] Ошибка поиска для запроса {query}: {e}", exc_info=True)
                    continue
            
            if not all_films:
                bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям на Кинопоиске.", chat_id, call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Фильтруем фильмы: исключаем те, что уже в базе и с нежелательными жанрами
            filtered_films = []
            for film in all_films:
                kp_id_film = str(film.get('kinopoiskId', ''))
                if kp_id_film and kp_id_film not in exclude_kp_ids:
                    # Проверяем жанры фильма
                    film_genres = [g.get('genre', '').lower() for g in film.get('genres', [])]
                    has_excluded_genre = any(eg.lower() in [fg.lower() for fg in film_genres] for eg in EXCLUDED_GENRES)
                    if not has_excluded_genre:
                        filtered_films.append(film)
            
            if not filtered_films:
                bot.edit_message_text("😔 Все найденные фильмы уже есть в вашей базе.", chat_id, call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Разделяем фильмы на приоритетные (с любимым жанром) и обычные
            priority_films = []
            regular_films = []
            
            for film in filtered_films:
                film_genres = [g.get('genre', '').lower() for g in film.get('genres', [])]
                # Проверяем, есть ли любимый жанр в жанрах фильма
                if fav_genre and fav_genre.lower() in film_genres:
                    priority_films.append(film)
                else:
                    regular_films.append(film)
            
            # Выбираем фильм: сначала из приоритетных, если есть, иначе из обычных
            if priority_films:
                selected_film = random.choice(priority_films)
                logger.info(f"[RANDOM KINOPOISK] Выбран приоритетный фильм (с любимым жанром)")
            else:
                selected_film = random.choice(regular_films)
                logger.info(f"[RANDOM KINOPOISK] Выбран обычный фильм")
            
            kp_id_result = str(selected_film.get('kinopoiskId', ''))
            
            if kp_id_result:
                # Пытаемся найти фильм без исключаемых жанров (максимум 10 попыток)
                max_attempts = 10
                attempt = 0
                found_valid_film = False
                
                while attempt < max_attempts and not found_valid_film:
                    # Получаем полную информацию о фильме
                    # Формируем ссылку в зависимости от типа (для сериалов /series/, для фильмов /film/)
                    # Определяем тип из selected_film, если есть поле type
                    film_type_from_result = selected_film.get('type', '').upper() if selected_film.get('type') else None
                    if film_type_from_result == 'TV_SERIES':
                        link = f"https://www.kinopoisk.ru/series/{kp_id_result}/"
                    else:
                        link = f"https://www.kinopoisk.ru/film/{kp_id_result}/"
                    
                    movie_info = extract_movie_info(link)
                    
                    if movie_info:
                        # Проверяем, что фильм не содержит исключаемые жанры
                        film_genres_str = movie_info.get('genres', '')
                        film_genres_lower = str(film_genres_str).lower() if film_genres_str else ""
                        has_excluded_genre = any(eg.lower() in film_genres_lower for eg in EXCLUDED_GENRES)
                        
                        if not has_excluded_genre:
                            # Фильм подходит, используем его
                            found_valid_film = True
                            from moviebot.bot.handlers.series import show_film_info_with_buttons
                            show_film_info_with_buttons(
                                chat_id, user_id, movie_info, link, kp_id_result,
                                existing=None, message_id=call.message.message_id
                            )
                            bot.answer_callback_query(call.id)
                            del user_random_state[user_id]
                            return
                        else:
                            # Фильм содержит исключаемый жанр, выбираем другой
                            logger.info(f"[RANDOM KINOPOISK] Фильм {kp_id_result} содержит исключаемый жанр, пробуем другой")
                            filtered_films.remove(selected_film)
                            if priority_films and selected_film in priority_films:
                                priority_films.remove(selected_film)
                            if selected_film in regular_films:
                                regular_films.remove(selected_film)
                            
                            if not filtered_films:
                                break
                            
                            # Выбираем следующий фильм
                            if priority_films:
                                selected_film = random.choice(priority_films)
                            elif regular_films:
                                selected_film = random.choice(regular_films)
                            else:
                                break
                            
                            kp_id_result = str(selected_film.get('kinopoiskId', ''))
                            attempt += 1
                    else:
                        # Не удалось получить информацию, пробуем другой фильм
                        filtered_films.remove(selected_film)
                        if priority_films and selected_film in priority_films:
                            priority_films.remove(selected_film)
                        if selected_film in regular_films:
                            regular_films.remove(selected_film)
                        
                        if not filtered_films:
                            break
                        
                        if priority_films:
                            selected_film = random.choice(priority_films)
                        elif regular_films:
                            selected_film = random.choice(regular_films)
                        else:
                            break
                        
                        kp_id_result = str(selected_film.get('kinopoiskId', ''))
                        attempt += 1
                
                if not found_valid_film:
                    bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям на Кинопоиске.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                else:
                    # Если не удалось получить полную информацию, показываем базовую с кнопками
                    title = selected_film.get('nameRu') or selected_film.get('nameEn', 'Без названия')
                    year = selected_film.get('year', '—')
                    film_genres = selected_film.get('genres', [])
                    genres_str = ', '.join([g.get('genre', '') for g in film_genres]) if film_genres else '—'
                    
                    year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' else ""
                    text = f"🎬 <b>{title}</b>{year_str}\n\n"
                    if genres_str and genres_str != '—':
                        text += f"🎭 <b>Жанры:</b> {genres_str}\n"
                    text += f"\n<a href='{link}'>Кинопоиск</a>"
                    
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id_result}"))
                    markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{kp_id_result}"))
                    markup.add(InlineKeyboardButton("🔗 Перейти к карточке", callback_data=f"add_to_database:{kp_id_result}"))
                    
                    try:
                        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)
                    except:
                        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
            else:
                bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям на Кинопоиске.", chat_id, call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
        
        # Для остальных режимов используем поиск в базе
        # Получаем content_type из состояния для фильтрации
        content_type = state.get('content_type', 'mixed')  # films, series, mixed
        
        # Формируем запрос - исключаем фильмы, которые уже запланированы и фильмы с импортированными оценками
        # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
        is_series_filter = ""
        if content_type == 'films':
            is_series_filter = "AND m.is_series = 0"
        elif content_type == 'series':
            is_series_filter = "AND m.is_series = 1"
        # Если mixed - фильтр не добавляем
        
        query = """SELECT m.id, m.title, m.year, m.genres, m.director, m.actors, m.description, m.link, m.kp_id 
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                    WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL """ + is_series_filter + """
                    AND m.id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s)"""
        params = [chat_id, chat_id]
        
        # Фильтр по режиму
        mode = state.get('mode')
        if mode == 'my_votes':
            # Для режима "по моим оценкам" - выбираем до 5 случайных фильмов с оценкой 9-10,
            # находим похожие к ним, фильтруем по критериям и показываем список с пагинацией
            
            # Показываем индикатор загрузки
            message_id = call.message.message_id
            try:
                bot.edit_message_text("⏳ Загружаю...", chat_id, message_id)
            except:
                message_id = None
            
            # Получаем до 5 случайных фильмов с импортированной оценкой 9-10
            # Учитываем content_type: films - только FILM, series - только TV_SERIES, mixed - оба
            # Используем UNION для объединения фильмов из базы группы и импортированных оценок
            
            # Добавляем фильтр по is_series для фильмов из базы группы
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            # mixed - фильтр не добавляем
            
            # Добавляем фильтр по type для импортированных оценок (film_id = NULL)
            type_filter = ""
            if content_type == 'films':
                type_filter = "AND (r.type = 'FILM' OR (r.type IS NULL AND NOT EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 0)))"
            elif content_type == 'series':
                type_filter = "AND (r.type = 'TV_SERIES' OR (r.type IS NULL AND EXISTS (SELECT 1 FROM movies m2 WHERE m2.kp_id = r.kp_id AND m2.chat_id = r.chat_id AND m2.is_series = 1)))"
            # Если mixed - фильтр не добавляем
            
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            favorite_films = []
            try:
                with db_lock:
                    cursor_local.execute(f"""
                        (SELECT r.kp_id, NULL::integer as id
                        FROM ratings r
                        WHERE r.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                        AND r.film_id IS NULL AND r.kp_id IS NOT NULL {type_filter}
                        ORDER BY RANDOM()
                        LIMIT 5)
                        UNION ALL
                        (SELECT m.kp_id, m.id
                        FROM movies m
                        JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                        AND m.kp_id IS NOT NULL {is_series_filter}
                        ORDER BY RANDOM()
                        LIMIT 5)
                    """, (chat_id, user_id, chat_id, user_id))
                    favorite_films = cursor_local.fetchall()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if not favorite_films:
                bot.edit_message_text("😔 Не найдено фильмов с оценкой 9-10, импортированных с Кинопоиска.", chat_id, message_id or call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Собираем все похожие фильмы к выбранным любимым
            all_similars_kp_ids = set()
            from moviebot.api.kinopoisk_api import get_similars
            
            for film_row in favorite_films:
                kp_id = film_row.get('kp_id') if isinstance(film_row, dict) else film_row[0]
                if kp_id:
                    similars = get_similars(str(str(kp_id)))
                    logger.info(f"[RANDOM MY_VOTES] Found {len(similars)} similar films for kp_id={kp_id}")
                    for item in similars:
                        if len(item) >= 2:
                            similar_kp_id = item[0]
                            all_similars_kp_ids.add(similar_kp_id)
            
            if not all_similars_kp_ids:
                bot.edit_message_text("😔 Не найдено похожих фильмов к вашим любимым.", chat_id, message_id or call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Получаем выбранные фильтры
            periods = state.get('periods', [])
            genres = state.get('genres', [])
            directors = state.get('directors', [])
            actors = state.get('actors', [])
            
            # Исключаем фильмы, которые уже в базе
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            exclude_kp_ids = set()
            try:
                with db_lock:
                    cursor_local.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND kp_id IS NOT NULL', (chat_id,))
                    existing_movies = cursor_local.fetchall()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Получаем информацию о похожих фильмах через extract_movie_info и фильтруем
            filtered_films = []
            request_count = 0
            max_requests_per_second = 5
            last_request_time = time.time()
            
            for similar_kp_id in all_similars_kp_ids:
                if str(similar_kp_id) in exclude_kp_ids:
                    continue
                
                # Ограничиваем скорость запросов (не более 5 в секунду)
                current_time = time.time()
                if request_count >= max_requests_per_second:
                    elapsed = current_time - last_request_time
                    if elapsed < 1.0:
                        time.sleep(1.0 - elapsed)
                    request_count = 0
                    last_request_time = time.time()
                
                try:
                    link = f"https://www.kinopoisk.ru/film/{similar_kp_id}/"
                    film_info = extract_movie_info(link)
                    request_count += 1
                    
                    if film_info and check_film_matches_criteria(film_info, periods, genres, directors, actors):
                        filtered_films.append({
                            'kp_id': str(similar_kp_id),
                            'title': film_info.get('title', 'Без названия'),
                            'year': film_info.get('year', '—'),
                            'is_series': film_info.get('is_series', False)
                        })
                        
                        # Ограничиваем количество фильмов для производительности
                        if len(filtered_films) >= 25:
                            break
                except Exception as e:
                    logger.warning(f"[RANDOM MY_VOTES] Error getting info for similar film {similar_kp_id}: {e}")
                    continue
            
            if filtered_films:
                # Сохраняем список фильмов в состоянии для пагинации
                user_random_state[user_id]['similar_films'] = filtered_films
                
                # Показываем первую страницу списка
                show_similar_films_page(filtered_films, chat_id, user_id, message_id or call.message.message_id, mode, page=0)
                try:
                    bot.answer_callback_query(call.id)
                except Exception as answer_error:
                    error_str = str(answer_error)
                    if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                        logger.error(f"[RANDOM MY_VOTES] Ошибка answer_callback_query: {answer_error}", exc_info=True)
                # Не удаляем состояние, чтобы пагинация работала
            else:
                bot.edit_message_text("😔 Не найдено похожих фильмов по заданным фильтрам.", chat_id, message_id or call.message.message_id)
                try:
                    bot.answer_callback_query(call.id)
                except Exception as answer_error:
                    error_str = str(answer_error)
                    if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                        logger.error(f"[RANDOM MY_VOTES] Ошибка answer_callback_query: {answer_error}", exc_info=True)
                del user_random_state[user_id]
                return
        elif mode == 'group_votes':
            # Для режима "По оценкам в базе" - выбираем до 5 фильмов из базы со средней оценкой >= 7.5,
            # находим похожие к ним, фильтруем по критериям и показываем список с пагинацией
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            
            # Показываем индикатор загрузки
            message_id = call.message.message_id
            try:
                bot.edit_message_text("⏳ Загружаю...", chat_id, message_id)
            except:
                message_id = None
            
            # Получаем фильтры из состояния
            periods = state.get('periods', [])
            genres = state.get('genres', [])
            directors = state.get('directors', [])
            actors = state.get('actors', [])
            content_type = state.get('content_type', 'mixed')  # films, series, mixed
            
            logger.info(f"[RANDOM GROUP_VOTES] content_type={content_type}")
            
            # Получаем список kp_id фильмов, которые уже в базе (исключаем их)
            exclude_kp_ids = set()
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND kp_id IS NOT NULL', (chat_id,))
                    existing_movies = cursor_local.fetchall()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Выбираем до 5 фильмов из базы со средней оценкой >= 7.5, которые соответствуют выбранным критериям
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            is_series_filter = ""
            if content_type == 'films':
                is_series_filter = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter = "AND m.is_series = 1"
            # mixed - фильтр не добавляем
            
            base_query = """
                SELECT m.kp_id, m.title, m.year, m.genres
                FROM movies m
                WHERE m.chat_id = %s AND m.kp_id IS NOT NULL """ + is_series_filter + """
                AND EXISTS (
                    SELECT 1 FROM ratings r 
                    WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                    GROUP BY r.film_id, r.chat_id 
                    HAVING AVG(r.rating) >= 7.5
                )
            """
            params = [chat_id]
            
            # Добавляем фильтр по периодам, если они выбраны
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
                    base_query += " AND (" + " OR ".join(period_conditions) + ")"
            
            # Добавляем фильтр по жанрам, если они выбраны
            if genres:
                genre_conditions = []
                for genre in genres:
                    genre_conditions.append(f"LOWER(m.genres) LIKE LOWER('%{genre}%')")
                if genre_conditions:
                    base_query += " AND (" + " OR ".join(genre_conditions) + ")"
            
            # Добавляем фильтр по режиссерам, если они выбраны
            if directors:
                director_conditions = []
                for director in directors:
                    director_conditions.append("m.director = %s")
                    params.append(director)
                if director_conditions:
                    base_query += " AND (" + " OR ".join(director_conditions) + ")"
            
            # Добавляем фильтр по актерам, если они выбраны
            if actors:
                actor_conditions = []
                for actor in actors:
                    actor_conditions.append("m.actors ILIKE %s")
                    params.append(f"%{actor}%")
                if actor_conditions:
                    base_query += " AND (" + " OR ".join(actor_conditions) + ")"
            
            base_query += " ORDER BY RANDOM() LIMIT 5"
            
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            base_films = []
            try:
                with db_lock:
                    cursor_local.execute(base_query, tuple(params))
                    base_films = cursor_local.fetchall()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if not base_films:
                bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям в вашей базе.", chat_id, message_id or call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Собираем все похожие фильмы к выбранным фильмам из базы
            all_similars_kp_ids = set()
            from moviebot.api.kinopoisk_api import get_similars
            
            for base_film in base_films:
                base_kp_id = str(base_film.get('kp_id') if isinstance(base_film, dict) else base_film[0])
                if not base_kp_id:
                    continue
                
                logger.info(f"[RANDOM GROUP_VOTES] Ищем похожие для фильма {base_kp_id}")
                similars = get_similars(base_kp_id)
                for item in similars:
                    if len(item) >= 2:
                        similar_kp_id = item[0]
                        all_similars_kp_ids.add(similar_kp_id)
            
            if not all_similars_kp_ids:
                bot.edit_message_text("😔 Не найдено похожих фильмов к выбранным фильмам из базы.", chat_id, message_id or call.message.message_id)
                bot.answer_callback_query(call.id)
                del user_random_state[user_id]
                return
            
            # Получаем информацию о похожих фильмах через extract_movie_info и фильтруем
            filtered_films = []
            request_count = 0
            max_requests_per_second = 5
            last_request_time = time.time()
            
            for similar_kp_id in all_similars_kp_ids:
                if str(similar_kp_id) in exclude_kp_ids:
                    continue
                
                # Ограничиваем скорость запросов (не более 5 в секунду)
                current_time = time.time()
                if request_count >= max_requests_per_second:
                    elapsed = current_time - last_request_time
                    if elapsed < 1.0:
                        time.sleep(1.0 - elapsed)
                    request_count = 0
                    last_request_time = time.time()
                
                try:
                    # Формируем ссылку в зависимости от типа
                    # Сначала получаем информацию через extract_movie_info по kp_id напрямую
                    film_info = extract_movie_info(similar_kp_id)
                    request_count += 1
                    
                    if film_info and check_film_matches_criteria(film_info, periods, genres, directors, actors):
                        # Проверяем, что фильм соответствует content_type
                        is_series = film_info.get('is_series', False)
                        if content_type == 'films' and is_series:
                            continue  # Пропускаем сериалы, если выбраны только фильмы
                        elif content_type == 'series' and not is_series:
                            continue  # Пропускаем фильмы, если выбраны только сериалы
                        
                        # Проверяем, что фильм не в базе
                        if str(similar_kp_id) not in exclude_kp_ids:
                            filtered_films.append({
                                'kp_id': str(similar_kp_id),
                                'title': film_info.get('title', 'Без названия'),
                                'year': film_info.get('year', '—'),
                                'is_series': is_series
                            })
                            
                            # Ограничиваем количество фильмов для производительности
                            if len(filtered_films) >= 25:
                                break
                except Exception as e:
                    logger.warning(f"[RANDOM GROUP_VOTES] Error getting info for similar film {similar_kp_id}: {e}")
                    continue
            
            if filtered_films:
                # Сохраняем список фильмов в состоянии для пагинации
                user_random_state[user_id]['similar_films'] = filtered_films
                
                # Показываем первую страницу списка
                show_similar_films_page(filtered_films, chat_id, user_id, message_id or call.message.message_id, mode, page=0)
                try:
                    bot.answer_callback_query(call.id)
                except Exception as answer_error:
                    error_str = str(answer_error)
                    if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                        logger.error(f"[RANDOM GROUP_VOTES] Ошибка answer_callback_query: {answer_error}", exc_info=True)
                # Не удаляем состояние, чтобы пагинация работала
            else:
                bot.edit_message_text("😔 Не найдено похожих фильмов по заданным фильтрам.", chat_id, message_id or call.message.message_id)
                try:
                    bot.answer_callback_query(call.id)
                except Exception as answer_error:
                    error_str = str(answer_error)
                    if "query is too old" not in error_str and "query ID is invalid" not in error_str and "timeout expired" not in error_str:
                        logger.error(f"[RANDOM GROUP_VOTES] Ошибка answer_callback_query: {answer_error}", exc_info=True)
                del user_random_state[user_id]
                return
        elif mode == 'database':
            # Режим "Рандом по своей базе" - только фильмы/сериалы из базы
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            # Никаких дополнительных фильтров, только базовые (watched = 0, не в планах)
            pass
        
        # Фильтр по периодам
        periods = state.get('periods', [])
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
                query += " AND (" + " OR ".join(period_conditions) + ")"
        
        # Фильтр по жанрам (можно несколько, OR условие)
        genres = state.get('genres', [])
        if genres:
            genre_conditions = []
            for genre in genres:
                genre_conditions.append("m.genres ILIKE %s")
                params.append(f"%{genre}%")
            if genre_conditions:
                query += " AND (" + " OR ".join(genre_conditions) + ")"
        
        # Фильтр по режиссёрам (можно несколько, OR условие)
        directors = state.get('directors', [])
        if directors:
            director_conditions = []
            for director in directors:
                director_conditions.append("m.director = %s")
                params.append(director)
            if director_conditions:
                query += " AND (" + " OR ".join(director_conditions) + ")"
        
        # Фильтр по актёрам (можно несколько, OR условие)
        actors = state.get('actors', [])
        if actors:
            actor_conditions = []
            for actor in actors:
                actor_conditions.append("m.actors ILIKE %s")
                params.append(f"%{actor}%")
            if actor_conditions:
                query += " AND (" + " OR ".join(actor_conditions) + ")"
        
        logger.info(f"[RANDOM] Query: {query}")
        logger.info(f"[RANDOM] Params: {params}")
        
        # Используем get_db_cursor() для получения свежего курсора, чтобы избежать проблем с закрытыми курсорами
        cursor_local = get_db_cursor()
        with db_lock:
            try:
                cursor_local.execute(query, params)
                candidates = cursor_local.fetchall()
                logger.info(f"[RANDOM] Candidates found: {len(candidates)}")
            except Exception as db_e:
                logger.error(f"[RANDOM] Ошибка при запросе фильмов: {db_e}", exc_info=True)
                # Пересоздаем курсор при ошибке
                cursor_local = get_db_cursor()
                try:
                    cursor_local.execute(query, params)
                    candidates = cursor_local.fetchall()
                    logger.info(f"[RANDOM] Candidates found: {len(candidates)} (после пересоздания курсора)")
                except Exception as db_e2:
                    logger.error(f"[RANDOM] Критическая ошибка при запросе фильмов: {db_e2}", exc_info=True)
                    candidates = []
        
        if not candidates:
            # Ищем похожие фильмы из запланированных
            # Учитываем content_type: films - только фильмы, series - только сериалы, mixed - оба
            is_series_filter_similar = ""
            if content_type == 'films':
                is_series_filter_similar = "AND m.is_series = 0"
            elif content_type == 'series':
                is_series_filter_similar = "AND m.is_series = 1"
            
            similar_query = """SELECT m.title, m.year, m.link, m.kp_id
                                FROM movies m 
                                JOIN plans p ON m.id = p.film_id 
                                WHERE m.chat_id = %s AND m.watched = 0 """ + is_series_filter_similar + """
            """
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
            
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            similar_movies = []
            try:
                with db_lock:
                    cursor_local.execute(similar_query, similar_params)
                    similar_movies = cursor_local.fetchall()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            if similar_movies:
                # Формируем список похожих фильмов
                similar_list = []
                first_movie_kp_id = None
                for movie in similar_movies:
                    if isinstance(movie, dict):
                        title = movie.get('title')
                        year = movie.get('year') or '—'
                        link = movie.get('link')
                        kp_id = movie.get("kp_id")
                    else:
                        title = movie[0] if len(movie) > 0 else None
                        year = movie[1] if len(movie) > 1 else '—'
                        link = movie[2] if len(movie) > 2 else None
                        kp_id = movie[3] if len(movie) > 3 else None
                    
                    if title and link:
                        year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' else ""
                        similar_list.append(f"• <a href='{link}'>{title}</a>{year_str}")
                        if not first_movie_kp_id and kp_id:
                            first_movie_kp_id = kp_id
                
                if similar_list:
                    # Берем первый фильм для кнопки "Перейти к описанию"
                    message_text = f"🕵 Найден подходящий фильм в вашей базе!\n\n{similar_list[0].replace('• ', '')}"
                    
                    # Создаем кнопку "Вернуться к описанию" для первого фильма
                    markup = InlineKeyboardMarkup()
                    if first_movie_kp_id:
                        markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{first_movie_kp_id}"))
                    markup.add(InlineKeyboardButton("⬅️ Вернуться к меню", callback_data="random_back_to_menu"))
                else:
                    message_text = (
                        "😔 <b>Таких фильмов в базе не найдено!</b>\n\n"
                        "Что делаем дальше?"
                    )
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(
                        InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search")
                    )
                    markup.add(
                        InlineKeyboardButton("⬅️ Назад к режимам", callback_data="start_menu:random")
                    )
            else:
                message_text = (
                    "😔 <b>Таких фильмов в базе не найдено!</b>\n\n"
                    "Что делаем дальше?"
                )
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(
                    InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search")
                )
                markup.add(
                    InlineKeyboardButton("⬅️ Назад к режимам", callback_data="start_menu:random")
                )
            
            try:
                bot.edit_message_text(message_text, 
                                    chat_id, call.message.message_id, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                bot.answer_callback_query(call.id)
            except:
                bot.send_message(chat_id, message_text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
            del user_random_state[user_id]
            return
        
        movie = random.choice(candidates)
        if isinstance(movie, dict):
            title = movie.get('title')
            year = movie.get('year') or '—'
            link = movie.get('link')
            kp_id = movie.get('kp_id')
        else:
            title = movie[1] if len(movie) > 1 else 'Без названия'
            year = movie[2] if len(movie) > 2 else '—'
            link = movie[7] if len(movie) > 7 else None
            kp_id = movie[3] if len(movie) > 3 else None

        if not link or not kp_id:
            year_str = f" ({year})" if year and str(year).lower() != 'none' and year != '—' else ""
            text = f"🍿 <b>Случайный фильм:</b>\n\n<b>{title}</b>{year_str}"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Вернуться к меню", callback_data="random_back_to_menu"))
            try:
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            bot.answer_callback_query(call.id)
            del user_random_state[user_id]
            return

        link = f"https://www.kinopoisk.ru/film/{kp_id}/"

        movie_info = extract_movie_info(link)

        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        row = None
        try:
            with db_lock:
                cursor_local.execute("SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(kp_id)))
                row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass

        existing = None
        if row:
            # row может быть dict или tuple, обрабатываем оба случая
            film_id = row.get('id') if isinstance(row, dict) else (row[0] if row else None)
            title = row.get('title') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
            watched = row.get('watched') if isinstance(row, dict) else (row[2] if len(row) > 2 else False)
            existing = (film_id, title, watched)

        fallback_info = {
            'title': title,
            'year': year,
            'description': '',
            'director': '',
            'actors': '',
            'genres': '',
            'is_series': False
        }

        try:
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=movie_info or fallback_info,
                link=link,
                kp_id=kp_id,
                existing=existing,
                message_id=call.message.message_id
            )
            film_message_id = call.message.message_id
        except Exception as e:
            logger.error(f"[RANDOM] Ошибка edit в show_film_info: {e}")
            sent = show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=movie_info or fallback_info,
                link=link,
                kp_id=kp_id,
                existing=existing
            )
            film_message_id = sent.message_id if hasattr(sent, 'message_id') else None

        bot.answer_callback_query(call.id)

        if film_message_id:
            bot_messages[film_message_id] = link
            logger.info(f"[RANDOM] Saved film message_id={film_message_id} with link={link}")

        # === УЛУЧШЕННЫЙ ПАРСЕР МЕСТА И ДАТЫ ===
        def parse_plan_input(text: str):
            """Парсит ввод: 'дома 20.01', 'в кино, завтра в 20:00', 'Дома — 15 января' и т.д.
            Возвращает (place, date_raw_str) — где date_raw_str это оригинальная часть с датой без места"""
            original = text.strip()
            lower = text.lower().strip()

            place = None
            place_match = None

            # Ищем место просмотра
            if re.search(r'\bдома\b', lower):
                place = 'дома'
                place_match = 'дома'
            elif re.search(r'\bв\s+кино\b|\bкинотеатр\b|\bкино\b', lower):
                place = 'в кино'
                place_match = 'в кино' if 'в кино' in lower else 'кино' if 'кино' in lower else 'кинотеатр'

            if not place:
                return None, None

            # Удаляем все вхождения слов места + возможные разделители вокруг них
            cleaned = re.sub(rf'\b{re.escape(place_match)}\b', '', lower, flags=re.IGNORECASE)
            # Убираем разделители в начале и конце
            cleaned = re.sub(r'^[.,:;—\s\-]+|[.,:;—\s\-]+$', '', cleaned).strip()

            # Если после удаления места ничего не осталось — дата не указана
            if not cleaned:
                return place, None

            # Возвращаем место и оригинальный текст даты (не lower, чтобы сохранить "Завтра", "Января" и т.д.)
            # Выделяем часть после места из оригинального текста
            date_raw = re.sub(rf'\b{re.escape(place_match)}\b.*?(?=\b.{{\b|$)', '', original, flags=re.IGNORECASE).strip()
            date_raw = re.sub(r'^[.,:;—\s\-]+|[.,:;—\s\-]+$', '', date_raw).strip()

            return place, date_raw or cleaned  # fallback на cleaned, если не удалось точно выделить

        # === Сохранение данных для планирования ===
        random_plan_data[user_id] = {
            'link': link,
            'kp_id': kp_id,
            'title': title,
            'film_message_id': film_message_id,
            'instruction_message_id': None,  # Не используется, оставлено для совместимости
            'chat_id': chat_id,
            'place_and_date_raw': None  # будет заполнено в process_random_plan после парсинга
        }

        # Сохраняем функцию парсинга отдельно (или можно просто вызвать здесь, но лучше в process)
        # Вместо сохранения функции — просто сохраним оригинальный текст позже

        # === Активация ожидания ===
        if call.message.chat.type == 'private':
            user_expected_text[user_id] = {'expected_for': 'random_plan'}
            logger.info(f"[RANDOM] Ожидание планирования в ЛС включено для user_id={user_id}")

        del user_random_state[user_id]
        logger.info(f"[RANDOM] ===== COMPLETED: Film shown - {title}")

    except Exception as e:
        logger.error(f"[RANDOM] ERROR in _random_final: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка поиска фильма")
            if user_id in user_random_state:
                del user_random_state[user_id]
            if user_id in random_plan_data:
                del random_plan_data[user_id]
        except:
            pass

def send_episode_marked_message(bot, chat_id, user_id, kp_id, film_id, season, episode, mark_all_previous):
    """Отправляет сообщение в бота об отметке серии"""
    try:
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        from moviebot.api.kinopoisk_api import extract_movie_info
        
        conn = get_db_connection()
        cursor = get_db_cursor()
        
        try:
            # Получаем информацию о сериале
            cursor.execute("SELECT title, link, online_link FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
            row = cursor.fetchone()
            if not row:
                return
            
            title = row.get('title') if isinstance(row, dict) else row[0]
            link = row.get('link') if isinstance(row, dict) else row[1]
            online_link = row.get('online_link') if isinstance(row, dict) else (row[2] if len(row) > 2 else None)
            
            # Формируем сообщение
            if mark_all_previous:
                text = f"✅ <b>{title}</b>\n\nОтмечены все серии до {season}×{episode} как просмотренные"
            else:
                text = f"✅ <b>{title}</b>\n\nОтмечена серия {season}×{episode} как просмотренная"
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"seasons_kp:{kp_id}"))
            
            if online_link:
                markup.add(InlineKeyboardButton("🎬 Онлайн-кинотеатр", url=online_link))
            
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        finally:
            try:
                cursor.close()
            except:
                pass
            try:
                conn.close()
            except:
                pass
    except Exception as e:
        logger.error(f"[SERIES] Ошибка отправки сообщения об отметке серии: {e}", exc_info=True)