from moviebot.bot.bot_init import bot
"""
Обработчики команд связанных с сериалами, поиском, рандомом, премьерами, билетами, настройками и помощью
"""
import logging
import re
import random
import threading
import requests
import pytz
from datetime import datetime
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton  
from telebot.apihelper import ApiTelegramException 
from moviebot.bot.handlers.text_messages import is_expected_text_in_private
from moviebot.database.db_operations import (

    log_request, get_user_timezone_or_default, set_user_timezone,
    get_watched_emojis, get_user_timezone, get_notification_settings, set_notification_setting
)
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.database.db_operations import get_user_timezone_or_default
from moviebot.api.kinopoisk_api import search_films, extract_movie_info, get_premieres_for_period, get_seasons_data, search_films_by_filters
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access, has_notifications_access
from moviebot.utils.parsing import parse_plan_date_text
from moviebot.bot.handlers.seasons import get_series_airing_status, count_episodes_for_watch_check

from moviebot.config import KP_TOKEN, PLANS_TZ

from moviebot.states import (

    user_search_state, user_random_state, user_ticket_state,
    user_settings_state, settings_messages, bot_messages, added_movie_messages,
    dice_game_state, user_import_state
)
from moviebot.bot.handlers.text_messages import expect_text_from_user

from moviebot.utils.parsing import extract_kp_id_from_text, show_timezone_selection, extract_kp_user_id

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()
random_plan_data = {}  # user_id → данные для планирования рандомного фильма

# Обработчик выбора типа поиска (фильм/сериал) - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
# КРИТИЧЕСКИ ВАЖНО: Этот обработчик регистрируется при импорте модуля
logger.info("=" * 80)
logger.info(f"[SEARCH TYPE HANDLER] Регистрация обработчика search_type_callback")
logger.info(f"[SEARCH TYPE HANDLER] id(bot)={id(bot)}")
logger.info("=" * 80)

def show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=None, message_id=None, message_thread_id=None):
    """Показывает описание фильма с кнопками действий"""
    import inspect
    
    # Сначала обработаем message_id
    if message_id:
        try:
            bot.edit_message_text("⏳ Загружаю...", chat_id, message_id)
        except:
            message_id = None

    # Лог с caller'ом (оставляем для дебага)
    logger.info(
        "[SHOW FILM INFO] >>> ВХОД | caller = %s() | file = %s:%d | kp_id=%s | existing=%s | msg_id=%s | user_id=%s",
        inspect.stack()[1].function,
        inspect.stack()[1].filename.split('/')[-1],
        inspect.stack()[1].lineno,
        kp_id,
        existing,
        message_id,
        user_id
    )

    logger.info(f"[SHOW FILM INFO] ===== START: chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, message_id={message_id}, existing={existing}")

    try:
        logger.info(f"[SHOW FILM INFO] info keys: {list(info.keys()) if info else 'None'}")
        if not info:
            logger.error(f"[SHOW FILM INFO] info is None или пустой!")
            bot.send_message(chat_id, "❌ Произошла ошибка: информация о фильме не получена.")
            return
        
        # Инициализируем plan_info как None, чтобы она была доступна во всех путях выполнения
        plan_info = None
        
        is_series = info.get('is_series', False)
        type_emoji = "📺" if is_series else "🎬"
        logger.info(f"[SHOW FILM INFO] is_series={is_series}, type_emoji={type_emoji}")
        
        # Формируем текст описания
        # Если фильм уже в базе, добавляем сообщение об этом в начало
        text = ""
        if existing:
            # Определяем, сериал это или фильм
            film_type_text = "Сериал" if is_series else "Фильм"
            text += f"✅ <b>{film_type_text} уже в базе</b>\n\n"
        text += f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
        logger.info(f"[SHOW FILM INFO] Текст начала формироваться, title={info.get('title')}")
        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"
        logger.info(f"[SHOW FILM INFO] Базовый текст сформирован, is_series={is_series}")
        
        # Если это сериал, добавляем информацию о статусе выхода серий
        if is_series:
            logger.info(f"[SHOW FILM INFO] Получение статуса выхода серий для kp_id={kp_id}")
            try:
                is_airing, next_episode = get_series_airing_status(kp_id)
                logger.info(f"[SHOW FILM INFO] is_airing={is_airing}, next_episode={next_episode}")
                if is_airing and next_episode:
                    text += f"\n🟢 <b>Сериал выходит сейчас</b>\n"
                    text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
                else:
                    text += f"\n🔴 <b>Сериал не выходит</b>\n"
            except Exception as airing_e:
                logger.error(f"[SHOW FILM INFO] Ошибка get_series_airing_status: {airing_e}", exc_info=True)
                # Продолжаем без информации о статусе выхода
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        logger.info(f"[SHOW FILM INFO] Ссылка добавлена, existing={existing}")
        
        # Если фильм уже в базе, показываем дополнительную информацию
        if existing:
            logger.info(f"[SHOW FILM INFO] Фильм в базе, обрабатываем existing={existing}")
            logger.info(f"[SHOW FILM INFO] Тип existing: {type(existing)}, isinstance dict: {isinstance(existing, dict)}, isinstance tuple: {isinstance(existing, tuple)}")
            try:
                if isinstance(existing, dict):
                    logger.info(f"[SHOW FILM INFO] existing - словарь, извлекаю через .get()")
                    film_id = existing.get('id')
                    watched = existing.get('watched')
                else:
                    logger.info(f"[SHOW FILM INFO] existing - не словарь, извлекаю через индексы, len={len(existing) if hasattr(existing, '__len__') else 'N/A'}")
                    film_id = existing[0] if len(existing) > 0 else None
                    watched = existing[2] if len(existing) > 2 else None
                logger.info(f"[SHOW FILM INFO] Извлечены film_id={film_id}, watched={watched}")
            except Exception as extract_e:
                logger.error(f"[SHOW FILM INFO] ❌ ОШИБКА при извлечении film_id и watched: {extract_e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] existing type: {type(existing)}, value: {existing}")
                # Пытаемся продолжить с дефолтными значениями
                film_id = None
                watched = False
            
            if watched:
                logger.info(f"[SHOW FILM INFO] Фильм просмотрен, запрашиваем оценки...")
                avg = None
                user_rating = None
                try:
                    # Чтение безопасно без блокировки, используем короткий таймаут только для защиты от deadlock
                    lock_acquired = False
                    try:
                        # Короткий таймаут 1 секунда - если lock занят, просто пропускаем запрос
                        lock_acquired = db_lock.acquire(timeout=3.0)
                        if lock_acquired:
                            logger.info(f"[SHOW FILM INFO] db_lock получен, выполняю запрос AVG...")
                            try:
                                cursor.execute('SELECT AVG(rating) as avg FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                                avg_result = cursor.fetchone()
                                logger.info(f"[SHOW FILM INFO] AVG запрос выполнен, результат: {avg_result}")
                                if avg_result:
                                    avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                                    avg = float(avg) if avg is not None else None
                                else:
                                    avg = None
                                
                                # Получаем личную оценку пользователя (если есть)
                                if user_id:
                                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки пользователя user_id={user_id}...")
                                    cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                                    user_rating_row = cursor.fetchone()
                                    logger.info(f"[SHOW FILM INFO] Личная оценка получена: {user_rating_row}")
                                    if user_rating_row:
                                        user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                                    else:
                                        user_rating = None
                            finally:
                                db_lock.release()
                                logger.info(f"[SHOW FILM INFO] db_lock освобожден")
                        else:
                            logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос оценок (не критично)")
                            avg = None
                            user_rating = None
                    except Exception as lock_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при получении lock для оценок: {lock_e}")
                        if lock_acquired:
                            try:
                                db_lock.release()
                            except:
                                pass
                        avg = None
                        user_rating = None
                except Exception as db_e:
                    logger.warning(f"[SHOW FILM INFO] Ошибка при запросе оценок (не критично): {db_e}")
                    avg = None
                    user_rating = None
                
                text += f"\n\n✅ <b>Просмотрено</b>"
                if avg:
                    text += f"\n⭐ <b>Средняя оценка: {avg:.1f}/10</b>"
                # Добавляем строку о личной оценке пользователя (чтобы текст всегда менялся при обновлении)
                if user_rating is not None:
                    text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                else:
                    text += f"\n⭐ <b>Ваша оценка: —</b>"
            else:
                logger.info(f"[SHOW FILM INFO] Фильм не просмотрен (watched=False), проверяем личную оценку...")
                text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
                # Добавляем строку о личной оценке пользователя даже если фильм не просмотрен (чтобы текст всегда менялся)
                if user_id and film_id:
                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки (без блокировки, чтение безопасно)...")
                    user_rating = None
                    try:
                        # Чтение безопасно без блокировки, используем короткий таймаут только для защиты от deadlock
                        lock_acquired = False
                        try:
                            # Короткий таймаут 1 секунда - если lock занят, просто пропускаем запрос
                            lock_acquired = db_lock.acquire(timeout=3.0)
                            if lock_acquired:
                                try:
                                    cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                                    user_rating_row = cursor.fetchone()
                                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки выполнен, результат: {user_rating_row}")
                                    if user_rating_row:
                                        user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                                finally:
                                    db_lock.release()
                                    logger.info(f"[SHOW FILM INFO] db_lock освобожден")
                            else:
                                logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос оценки (не критично)")
                        except Exception as lock_e:
                            logger.warning(f"[SHOW FILM INFO] Ошибка при получении lock для оценки: {lock_e}")
                            if lock_acquired:
                                try:
                                    db_lock.release()
                                except:
                                    pass
                        
                        # Добавляем оценку в текст
                        if user_rating is not None:
                            text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                        else:
                            text += f"\n⭐ <b>Ваша оценка: —</b>"
                    except Exception as db_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при запросе оценки (не критично): {db_e}")
                else:
                    logger.info(f"[SHOW FILM INFO] user_id или film_id отсутствуют, пропускаем запрос оценки")
            
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
                                cursor.execute('''
                                    SELECT AVG(rating) as avg FROM ratings 
                                    WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                                ''', (chat_id, film_id))
                                avg_result = cursor.fetchone()
                                if avg_result:
                                    avg_rating = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                                    avg_rating = float(avg_rating) if avg_rating is not None else None
                                    if avg_rating:
                                        text += f"\n⭐ <b>Средняя оценка: {avg_rating:.1f}/10</b>"
                            finally:
                                db_lock.release()
                    except Exception as avg_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при запросе средней оценки для запланированного фильма: {avg_e}")
            logger.info(f"[SHOW FILM INFO] Обработка existing завершена")
        
        # Создаем кнопки
        logger.info(f"[SHOW FILM INFO] Создание кнопок...")
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Флаг для отслеживания, добавлены ли уже кнопки "Интересные факты" и "Оценить"
        facts_and_rate_added = False
        
        # Проверяем премьеру
        logger.info(f"[SHOW FILM INFO] Проверка премьеры...")
        russia_release = info.get('russia_release')
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    from datetime import date as date_class
                    today = date_class.today()
                    
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
                            except:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения информации о премьере: {e}")
        
        # Если премьера еще не состоялась, добавляем кнопку
        if premiere_date:
            from datetime import date as date_class
            today = date_class.today()
            if premiere_date > today:
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:current_month"))
        
        # Получаем film_id для проверки оценок и планов
        logger.info(f"[SHOW FILM INFO] Получение film_id...")
        film_id = None
        watched = False  # Инициализируем watched по умолчанию
        if existing:
            film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
            watched = existing.get('watched') if isinstance(existing, dict) else (existing[2] if len(existing) > 2 else False)
            logger.info(f"[SHOW FILM INFO] film_id из existing: {film_id}, watched: {watched}")
        else:
            logger.info(f"[SHOW FILM INFO] Запрос film_id из БД...")
            try:
                lock_acquired = db_lock.acquire(timeout=3.0)
                if lock_acquired:
                    try:
                        # Приводим kp_id к строке, так как в БД это text
                        cursor.execute("SELECT id, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, str(str(kp_id))))
                        film_row = cursor.fetchone()
                        if film_row:
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            watched = film_row.get('watched') if isinstance(film_row, dict) else (film_row[1] if len(film_row) > 1 else False)
                        logger.info(f"[SHOW FILM INFO] Запрос film_id выполнен, film_id={film_id}, watched={watched}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после запроса film_id")
                else:
                    logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос film_id (не критично)")
                    film_id = None
                    watched = False
            except Exception as film_id_e:
                logger.warning(f"[SHOW FILM INFO] Ошибка при запросе film_id (не критично): {film_id_e}")
                film_id = None
                watched = False
            logger.info(f"[SHOW FILM INFO] film_id из БД: {film_id}, watched: {watched}")
        
        # Проверяем, есть ли уже план для этого фильма (чтение безопасно без lock)
        logger.info(f"[SHOW FILM INFO] Проверка планов для film_id={film_id}...")
        has_plan = False
        plan_info = None
        if film_id:
            try:
                # КРИТИЧЕСКИЙ ФИКС: Обернуто в try-except с таймаутом для предотвращения зависания
                
                # Пробуем получить lock с таймаутом
                lock_acquired = db_lock.acquire(timeout=3.0)
                if lock_acquired:
                    try:
                        cursor.execute('''
                            SELECT id, plan_type, plan_datetime 
                            FROM plans 
                            WHERE film_id = %s AND chat_id = %s 
                            LIMIT 1
                        ''', (film_id, chat_id))
                        plan_row = cursor.fetchone()
                        has_plan = plan_row is not None
                        if has_plan:
                            if isinstance(plan_row, dict):
                                plan_id = plan_row.get('id')
                                plan_type = plan_row.get('plan_type')
                                plan_dt_value = plan_row.get('plan_datetime')
                            else:
                                plan_id = plan_row.get("id") if isinstance(plan_row, dict) else (plan_row[0] if plan_row else None)
                                plan_type = plan_row[1]
                                plan_dt_value = plan_row[2] if len(plan_row) > 2 else None
                            
                            # Форматируем дату
                            if plan_dt_value and user_id:
                                user_tz = get_user_timezone_or_default(user_id)
                                try:
                                    if isinstance(plan_dt_value, datetime):
                                        if plan_dt_value.tzinfo is None:
                                            dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                                        else:
                                            dt = plan_dt_value.astimezone(user_tz)
                                    else:
                                        dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
                                    date_str = dt.strftime('%d.%m.%Y %H:%M')
                                except Exception as e:
                                    logger.warning(f"[SHOW FILM INFO] Ошибка парсинга plan_datetime: {e}")
                                    date_str = str(plan_dt_value)[:16]
                            else:
                                date_str = "не указана"
                            
                            plan_info = {
                                'id': plan_id,
                                'type': plan_type,
                                'date': date_str
                            }
                        logger.info(f"[SHOW FILM INFO] Запрос планов выполнен (с lock), has_plan={has_plan}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после проверки планов")
                else:
                    logger.warning(f"[SHOW FILM INFO] db_lock timeout (5 сек) - пропускаем проверку планов (не критично)")
                    has_plan = False
            except Exception as plan_e:
                logger.error(f"[SHOW FILM INFO] ❌ Ошибка при проверке планов (пропускаем): {plan_e}", exc_info=True)
                has_plan = False
                plan_info = None
        logger.info(f"[SHOW FILM INFO] Проверка планов завершена, has_plan={has_plan}")
        
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
                markup.add(InlineKeyboardButton("👁️ Просмотрено", callback_data=f"mark_watched_from_description_kp:{kp_id}"))
        
        # Если фильм запланирован, показываем специальную логику кнопок
        if has_plan:
            # Если фильм запланирован, не показываем кнопки "добавить в базу" и "запланировать просмотр"
            
            # Добавляем кнопку "Выбрать онлайн-кинотеатр" только для планов типа 'home' (дома) и непросмотренных фильмов
            if plan_info and plan_info.get('type') == 'home' and not watched:
                markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{kp_id}"))
        else:
            # Фильм НЕ запланирован
            if film_id is None:
                # Фильм НЕ в базе — добавляем "Добавить в базу" + "Запланировать" (добавит автоматически)
                markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{kp_id}"))
                markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
                if not watched:
                    markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{kp_id}"))
            else:
                # Фильм в базе, но не запланирован — добавляем "Запланировать" и "Выбрать онлайн-кинотеатр"
                markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
                if not watched:
                    markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{kp_id}"))
        
        # Кнопка "Удалить из базы" — только если фильм в базе (film_id есть)
        if film_id:
            markup.add(InlineKeyboardButton("🗑️ Удалить из базы", callback_data=f"remove_from_database:{kp_id}"))
            
        # Добавляем кнопки "Интересные факты" и "Оценить" всегда (для фильмов в базе и не в базе)
        logger.info(f"[SHOW FILM INFO] Добавление кнопок оценок для film_id={film_id}...")
        if film_id:
            # Получаем информацию об оценках
            logger.info(f"[SHOW FILM INFO] Запрос оценок из БД...")
            avg_rating = None
            rating_text = "💬 Оценить"
            try:
                # КРИТИЧЕСКИЙ ФИКС: Увеличен таймаут до 5 секунд и добавлена обработка ошибок
                lock_acquired = db_lock.acquire(timeout=3.0)
                if lock_acquired:
                    try:
                        # Получаем среднюю оценку
                        cursor.execute('''
                            SELECT AVG(rating) as avg FROM ratings 
                            WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        avg_result = cursor.fetchone()
                        if avg_result:
                            avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                            avg_rating = float(avg) if avg is not None else None
                        
                        # Получаем активных пользователей
                        cursor.execute('''
                            SELECT DISTINCT user_id
                            FROM stats
                            WHERE chat_id = %s AND user_id IS NOT NULL
                        ''', (chat_id,))
                        active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                        
                        # Получаем всех, кто оценил этот фильм
                        cursor.execute('''
                            SELECT DISTINCT user_id FROM ratings
                            WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                        
                        # Определяем текст и эмодзи кнопки
                        # Показываем среднюю оценку, если есть хотя бы одна оценка
                        if avg_rating is not None:
                            rating_int = int(round(avg_rating))
                            if 1 <= rating_int <= 4:
                                emoji = "💩"
                            elif 5 <= rating_int <= 7:
                                emoji = "💬"
                            else:  # 8-10
                                emoji = "🏆"
                            rating_text = f"{emoji} {avg_rating:.0f}/10"
                        logger.info(f"[SHOW FILM INFO] Запрос оценок выполнен, avg_rating={avg_rating}, rating_text={rating_text}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после запроса оценок")
                else:
                    logger.warning(f"[SHOW FILM INFO] db_lock timeout (5 сек) - пропускаем запрос оценок (не критично)")
                    rating_text = "💬 Оценить"
            except Exception as rating_e:
                logger.error(f"[SHOW FILM INFO] ❌ Ошибка при запросе оценок (пропускаем): {rating_e}", exc_info=True)
                rating_text = "💬 Оценить"
            logger.info(f"[SHOW FILM INFO] Оценки получены, rating_text={rating_text}")
            
            if not facts_and_rate_added:
                markup.row(
                    InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
                    InlineKeyboardButton(rating_text, callback_data=f"rate_film:{kp_id}")
                )
                facts_and_rate_added = True
        else:
            # Фильм не в базе - добавляем кнопки "Интересные факты" и "Оценить"
            if not facts_and_rate_added:
                markup.row(
                    InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
                    InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_id}")
                )
                facts_and_rate_added = True
        logger.info(f"[SHOW FILM INFO] Кнопки оценок добавлены, facts_and_rate_added={facts_and_rate_added}")
        
        # === КНОПКИ ДЛЯ СЕРИАЛОВ ===
        logger.info(f"[SHOW FILM INFO] Обработка кнопок сериала: is_series={is_series}, user_id={user_id}, film_id={film_id}")

        if is_series:
            if user_id is None:
                # Группа + новая ссылка → показываем locked кнопки
                logger.info("[SHOW FILM INFO] Группа + новая ссылка: user_id=None → показываем locked кнопки")
                markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
            else:
                # Личка или есть user_id → нормальная проверка
                has_access = has_notifications_access(chat_id, user_id)
                logger.info(f"[SHOW FILM INFO] Доступ к уведомлениям: has_access={has_access}")

                # Отметка серий
                if has_access:
                    markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))

                # Подписка
                is_subscribed = False
                if film_id:
                    try:
                        lock_acquired = db_lock.acquire(timeout=3.0)
                        if lock_acquired:
                            try:
                                cursor.execute(
                                    'SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s LIMIT 1',
                                    (chat_id, film_id, user_id)
                                )
                                sub_row = cursor.fetchone()
                                if sub_row:
                                    is_subscribed = bool(sub_row[0] if isinstance(sub_row, tuple) else sub_row.get('subscribed'))
                            finally:
                                db_lock.release()
                    except Exception as e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка проверки подписки: {e}")

                if has_access:
                    if is_subscribed:
                        markup.add(InlineKeyboardButton("🔕 Отписаться от новых серий", callback_data=f"series_unsubscribe:{kp_id}"))
                    else:
                        markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))

        logger.info(f"[SHOW FILM INFO] Обработка сериала завершена")
        
        # Проверяем длину текста перед отправкой
        logger.info(f"[SHOW FILM INFO] Текст сформирован, длина={len(text)}, message_id={message_id}")
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
        logger.info("[SHOW FILM INFO] Попытка обновления или отправки")

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
        else:
            send_kwargs_for_send = send_kwargs

        sent_new = False
        if message_id:
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
                logger.info(f"[SHOW FILM INFO] Обновлено успешно, message_id={message_id}")
            except Exception as e:  # ловим все ошибки, т.к. ApiTelegramException может быть не импортирован
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
            sent_new = True

        if sent_new:
            try:
                sent = bot.send_message(**send_kwargs_for_send)
                logger.info(f"[SHOW FILM INFO] Отправлено новое, message_id={sent.message_id}, title={info.get('title')}")
            except Exception as e:
                logger.error(f"[SHOW FILM INFO] Не отправилось даже новое: {e}")
                # Fallback: минимальное сообщение
                bot.send_message(chat_id, f"🎬 {info.get('title','Фильм')}\n\n<a href='{link}'>Кинопоиск</a>", parse_mode='HTML')

        logger.info("[SHOW FILM INFO] ===== END (успешно) =====")
        
        
    except Exception as e:
        error_type = type(e).__name__
        error_str = str(e)
        import sys
        import traceback
        print(f"[SHOW FILM INFO] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", file=sys.stdout, flush=True)
        print(f"[SHOW FILM INFO] Traceback: {traceback.format_exc()}", file=sys.stdout, flush=True)
        logger.error(f"[SHOW FILM INFO] ❌ КРИТИЧЕСКАЯ ОШИБКА в show_film_info_with_buttons: {e}", exc_info=True)
        logger.error(f"[SHOW FILM INFO] Тип ошибки: {error_type}, args: {e.args}")
        logger.error(f"[SHOW FILM INFO] chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, existing={existing}")
        
        # Пытаемся отправить сообщение об ошибке
        try:
            error_text = f"🎬 <b>{info.get('title', 'Фильм') if info else 'Фильм'}</b>\n\n"
            if link:
                error_text += f"<a href='{link}'>Кинопоиск</a>\n\n"
            error_text += "❌ Произошла ошибка при формировании описания."
            bot.send_message(chat_id, error_text, parse_mode='HTML', disable_web_page_preview=False)
            logger.info(f"[SHOW FILM INFO] ✅ Сообщение об ошибке отправлено")
        except Exception as send_error_e:
            logger.error(f"[SHOW FILM INFO] ❌ Не удалось отправить даже сообщение об ошибке: {send_error_e}", exc_info=True)
        # НЕ пробрасываем ошибку дальше - бот должен продолжать работать
        logger.info(f"[SHOW FILM INFO] ===== END (с ошибкой) =====")
        print(f"[SHOW FILM INFO] ===== END (с ошибкой) =====", file=sys.stdout, flush=True)
    else:
        logger.info(f"[SHOW FILM INFO] ===== END (успешно) =====")
        import sys
        print(f"[SHOW FILM INFO] ===== END (успешно) =====", file=sys.stdout, flush=True)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("search_type:"))
def search_type_callback(call):
    """Обработчик выбора типа поиска (фильм или сериал)"""
    logger.info("=" * 80)
    logger.info(f"[SEARCH TYPE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[SEARCH TYPE] call.data={call.data}, call.message.message_id={call.message.message_id if call.message else 'N/A'}")
    try:
        # Отвечаем на callback сразу, чтобы убрать "крутилку"
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        search_type = call.data.split(":")[1]  # 'film' или 'series'
        
        logger.info(f"[SEARCH TYPE] Пользователь {user_id} выбрал тип поиска: {search_type}, chat_id={chat_id}")
        
        # Обновляем состояние
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
        
        # Обновляем сообщение с указанием выбранного типа (как в старом файле)
        type_text = "🎬 фильмы" if search_type == 'film' else "📺 сериалы" if search_type == 'series' else "🎬📺 фильмы и сериалы"
        
        # Обновляем кнопки, чтобы показать выбранный тип
        markup = InlineKeyboardMarkup(row_width=2)
        if search_type == 'film':
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм ✅", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
        else:  # series
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал ✅", callback_data="search_type:series")
            )
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # answer_callback_query уже вызван выше (строка 50)
        logger.info(f"[SEARCH TYPE] Тип поиска выбран: {type_text}")
        
        is_private = call.message.chat.type == 'private'
        prompt_text = f"🔍 Укажите запрос для поиска {type_text} в ответном сообщении, например: джон уик"
        
        try:
            sent_msg = bot.edit_message_text(
                prompt_text,
                chat_id,
                call.message.message_id,
                reply_markup=markup
            )
            message_id = call.message.message_id if sent_msg else None
            logger.info(f"[SEARCH TYPE] ✅ Сообщение обновлено успешно")
        except Exception as edit_e:
            logger.error(f"[SEARCH TYPE] Ошибка редактирования сообщения: {edit_e}", exc_info=True)
            # Пробуем отправить новое сообщение
            try:
                sent_msg = bot.send_message(
                    chat_id,
                    prompt_text,
                    reply_markup=markup
                )
                message_id = sent_msg.message_id if sent_msg else None
                logger.info(f"[SEARCH TYPE] ✅ Новое сообщение отправлено")
            except Exception as send_e:
                logger.error(f"[SEARCH TYPE] ❌ Ошибка отправки нового сообщения: {send_e}", exc_info=True)
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                return
        
        # Для ЛС устанавливаем ожидание текста
        if is_private and message_id:
            expect_text_from_user(user_id, chat_id, expected_for='search', message_id=message_id)
    except Exception as e:
        logger.error(f"[SEARCH TYPE] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except Exception as answer_e:
            logger.error(f"[SEARCH TYPE] Не удалось вызвать answer_callback_query: {answer_e}")
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
        
        # Получаем тип поиска из состояния, если есть
        search_type = user_search_state.get(user_id, {}).get('search_type', 'mixed')
        type_text = "🎬 фильмы" if search_type == 'film' else "📺 сериалы" if search_type == 'series' else "🎬📺 фильмы и сериалы"
        
        # Создаем кнопки для выбора типа поиска
        markup = InlineKeyboardMarkup(row_width=2)
        if search_type == 'film':
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм ✅", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
        elif search_type == 'series':
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал ✅", callback_data="search_type:series")
            )
        else:
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
        markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
        
        # Отправляем новое сообщение с промптом
        prompt_text = f"🔍 Укажите запрос для поиска {type_text} в ответном сообщении, например: джон уик"
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
    
    if search_type == 'film':
        # Фильтруем только фильмы
        films = [f for f in films if f.get('type', '').upper() != 'TV_SERIES']
    elif search_type == 'series':
        # Фильтруем только сериалы
        films = [f for f in films if f.get('type', '').upper() == 'TV_SERIES']
    # Если search_type == 'mixed', возвращаем все
    
    return films, total_pages

# Обработчик поиска
@bot.callback_query_handler(func=lambda call: call.data.startswith("add_film_"))
def search_film_callback(call):
    try:
        bot.answer_callback_query(call.id)
        data = call.data[len("add_film_"):]
        parts = data.split(":")
        kp_id = parts[0]
        film_type = parts[1] if len(parts) > 1 else "FILM"

        link = f"https://www.kinopoisk.ru/series/{kp_id}/" if film_type == "TV_SERIES" else f"https://www.kinopoisk.ru/film/{kp_id}/"

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
            # Создаем кнопки для выбора типа поиска
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            reply_msg = bot.reply_to(message, "🔍 Укажите запрос для поиска в ответном сообщении, например: джон уик", reply_markup=markup)
            # Сохраняем состояние для получения запроса (по умолчанию смешанный поиск)
            user_id = message.from_user.id
            chat_id = message.chat.id
            is_private = message.chat.type == 'private'
            user_search_state[user_id] = {
                'chat_id': chat_id, 
                'message_id': reply_msg.message_id, 
                'search_type': 'mixed'
            }
            logger.info(f"[SEARCH] Состояние поиска установлено для user_id={user_id}: {user_search_state[user_id]}")
            
            # Для ЛС устанавливаем ожидание текста
            if is_private and reply_msg:
                expect_text_from_user(user_id, chat_id, expected_for='search', message_id=reply_msg.message_id)
            return
        
        logger.info(f"Команда /search от пользователя {message.from_user.id}, запрос: {query}")
        
        # Получаем тип поиска из состояния, если есть
        search_type = user_search_state.get(message.from_user.id, {}).get('search_type', 'mixed')
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
            rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            # Определяем тип (сериал или фильм) по полю type из API
            film_type = film.get('type', '').upper()  # "FILM" или "TV_SERIES"
            is_series = film_type == 'TV_SERIES'
            
            logger.info(f"[SEARCH] Фильм: title={title}, year={year}, kp_id={kp_id}, type={film_type}, is_series={is_series}")
            
            if kp_id:
                # Ограничиваем длину текста кнопки
                type_indicator = "📺" if is_series else "🎬"
                button_text = f"{type_indicator} {title} ({year})"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                if rating != 'N/A':
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
            
            # Шаг 0: Выбор режима
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            
            # Проверяем доступ к рекомендациям
            has_rec_access = has_recommendations_access(chat_id, user_id)
            
            if has_rec_access:
                markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
                markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
                markup.add(InlineKeyboardButton("🔒 По оценкам в базе", callback_data="rand_mode_locked:group_votes"))
            
            # Для режима "По моим оценкам" - если есть подписка, показываем без замочка
            # Проверка импортированных оценок будет при нажатии
            if has_rec_access:
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
            
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            bot.reply_to(message, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
            logger.info(f"✅ Ответ на /random отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /random: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /random")
            except:
                pass


def premieres_command(message):
        """Команда /premieres - премьеры фильмов"""
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
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        bot.reply_to(message, "📅 <b>Выберите период для просмотра премьер:</b>", reply_markup=markup, parse_mode='HTML')


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
            text += "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
            text += "Используйте /payment для оформления подписки."
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
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
    text = """🎬 Помощь по командам бота:

/list — Показать список непросмотренных фильмов
/random — Выбрать случайный непросмотренный фильм с фильтрами (год, жанр, режиссёр)
/search — Поиск фильмов через Kinopoisk API
/total — Статистика: фильмы, жанры, режиссёры, актёры, оценки
/stats — Детальная статистика группы и участников
/rate — Оценить просмотренные фильмы
/plan — Запланировать просмотр фильма (дома/в кино)
/schedule — Показать список запланированных просмотров
/settings — Настроить эмодзи для отметки просмотренных фильмов
/clean — Удалить оценку, просмотр, план или обнулить базу
/help — Эта справка

Как использовать бота:

Есть два варианта, использование лично или использование в группе. Чтобы бот работал в группе, нужно добавить бота и сделать админом группы. В боте могут участвовать не все члены группы: для того, чтобы начать участие, нужно отправить любую команду боту. Вы можете добавить других членов группы к участию в боте по команде /join.

Сценарии работы с ботом:

1) Добавление фильмов
1. Отправьте ссылку на фильм с Кинопоиска — бот автоматически добавит его
2. Запланируйте просмотр фильма — дома или в кино. При домашнем просмотре, будут предложены онлайн-кинотеатры, где можно посмотреть фильм.
3. В день просмотра вам придет напоминание о просмотре со ссылкой на кинотеатр, если смотрите дома, или с билетами, если вы подгрузили билет в кино.
4. После просмотра, поставьте реакцию на сообщение с фильмом — фильм будет отмечен как просмотренный
5. После отметки напишите оценку от 1 до 10

При групповом участии, учитываются оценки всех участников. К высоко оцененным фильмам предлагаются похожие, а также оцененные фильмы участвуют в рекомендательных функциях

2) Сериалы
Можно добавлять сериалы, трекать просмотренные серии и подписаться на уведомления

3) Планирование премьер
Если фильм ещё не вышел, вы можете подписаться на его дату выхода

4) Поиск
Вы можете искать фильмы и сериалы с командой /search, а также искать премьеры по /premiere, там будет актуальный список премьер

5) Планирование походов в кино
Вы можете запланировать, хотите вы посмотреть тот или иной фильм дома или в кино. При просмотре фильма дома, вам будут предложны онлайн-кинотеатры, а при просмотре в кино — предложена возможность загрузить билет и указать время сеанса. В день просмотра фильма придет уведомление и напоминание с билетами заранее (функционал платный). Время уведомлений можно настроить.

Приятного просмотра! 🍿

Если у вас возникли сложности с ботом или оплатой, напишите нам:
@zap_nikita
movie-planner-bot@yandex.com"""
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    # Переключаемся на HTML, так как Markdown может вызывать ошибки парсинга
    text_html = text.replace('*', '').replace('_', '')
    # Добавляем базовое форматирование
    text_html = text_html.replace('🎬 Помощь по командам бота:', '<b>🎬 Помощь по командам бота:</b>')
    text_html = text_html.replace('Как использовать бота:', '<b>Как использовать бота:</b>')
    text_html = text_html.replace('Сценарии работы с ботом:', '<b>Сценарии работы с ботом:</b>')
    text_html = text_html.replace('1) Добавление фильмов', '<b>1) Добавление фильмов</b>')
    text_html = text_html.replace('2) Сериалы', '<b>2) Сериалы</b>')
    text_html = text_html.replace('3) Планирование премьер', '<b>3) Планирование премьер</b>')
    text_html = text_html.replace('4) Поиск', '<b>4) Поиск</b>')
    text_html = text_html.replace('5) Планирование походов в кино', '<b>5) Планирование походов в кино</b>')
    text_html = text_html.replace('Приятного просмотра!', '<b>Приятного просмотра!</b>')
    text_html = text_html.replace('Если у вас возникли сложности с ботом или оплатой, напишите нам:', '<b>Если у вас возникли сложности с ботом или оплатой, напишите нам:</b>')
    bot.reply_to(message, text_html, reply_markup=markup, parse_mode='HTML')


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
        
        with db_lock:
            cursor.execute('''
                SELECT p.id, 
                       COALESCE(m.title, 'Мероприятие') as title, 
                       p.plan_datetime, 
                       CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as ticket_count,
                       p.film_id
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND p.plan_type = 'cinema'
                  AND p.plan_datetime >= %s
                ORDER BY p.plan_datetime
                LIMIT 20
            ''', (chat_id, today_start_utc))
            sessions = cursor.fetchall()
        
        logger.info(f"[SHOW SESSIONS] Найдено сеансов: {len(sessions) if sessions else 0}")
        
        if not sessions:
            logger.info(f"[SHOW SESSIONS] Нет сеансов, отправляем сообщение пользователю {user_id}")
            if file_id:
                # Если есть файл, но нет сеансов, предлагаем создать новый
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data=f"ticket_new:{file_id}"))
                markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
                bot.send_message(chat_id, "❌ Нет запланированных сеансов в кино.\n\n📎 Файл готов к добавлению. Создайте новый сеанс.", reply_markup=markup, parse_mode='HTML')
            else:
                # Нет файла и нет сеансов
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data="ticket_new"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                bot.send_message(chat_id, "❌ Нет запланированных сеансов в кино.", reply_markup=markup, parse_mode='HTML')
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
            markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data=f"ticket_new:{file_id}"))
        else:
            markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data="ticket_new"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        text = "🎟️ <b>Выберите сеанс:</b>\n\n"
        if file_id:
            text += "📎 Файл готов к добавлению. Выберите сеанс или создайте новый."
        else:
            text += "Выберите сеанс для просмотра билетов или добавления новых."
        
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[SHOW SESSIONS] Сообщение с сеансами отправлено пользователю {user_id}")
    except Exception as e:
        logger.error(f"[SHOW SESSIONS] Ошибка: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, "❌ Произошла ошибка при загрузке сеансов.")
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

    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_mode:"))
    def handle_rand_mode(call):
        """Обработчик выбора режима рандомайзера"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            mode = call.data.split(":")[1]
            
            logger.info(f"[RANDOM CALLBACK] Mode: {mode}, user_id={user_id}, chat_id={chat_id}")
            
            # Проверяем доступ к рекомендациям для режимов, требующих подписку
            if mode in ['kinopoisk', 'my_votes', 'group_votes']:
                has_rec_access = has_recommendations_access(chat_id, user_id)
                logger.info(f"[RANDOM CALLBACK] Mode {mode} requires recommendations access: {has_rec_access}")
                if not has_rec_access:
                    bot.answer_callback_query(
                        call.id, 
                        "❌ Этот режим доступен только с подпиской на рекомендации. Используйте /payment для оформления подписки.", 
                        show_alert=True
                    )
                    logger.warning(f"[RANDOM CALLBACK] Access denied for mode {mode}, user_id={user_id}")
                    return
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user_id={user_id}, state keys: {list(user_random_state.keys())}, initializing new state")
                # Инициализируем состояние заново, если оно не найдено
                user_random_state[user_id] = {
                    'step': 'mode',
                    'mode': None,
                    'periods': [],
                    'genres': [],
                    'directors': [],
                    'actors': []
                }
            
            logger.info(f"[RANDOM CALLBACK] State found: {user_random_state[user_id]}")
            
            user_random_state[user_id]['mode'] = mode
            user_random_state[user_id]['step'] = 'period'
            
            logger.info(f"[RANDOM CALLBACK] State updated: mode={mode}, step=period")
            
            # Добавляем справку о режиме
            mode_descriptions = {
                'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
                'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм по вашим фильтрам.',
                'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
                'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nПолучите рекомендацию, основанную на оценках в вашей локальной базе.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
            }
            mode_description = mode_descriptions.get(mode, '')
            
            # Для режима kinopoisk пропускаем периоды и сразу переходим к выбору года и жанра
            if mode == 'kinopoisk':
                user_random_state[user_id]['step'] = 'year'
                bot.answer_callback_query(call.id)
                logger.info(f"[RANDOM CALLBACK] Mode kinopoisk selected, moving to year selection")
                _show_year_step(call, chat_id, user_id)
                return
            
            # Шаг 1: Выбор периода - показываем только те периоды, где есть фильмы
            all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            available_periods = []
            
            logger.info(f"[RANDOM CALLBACK] Checking available periods for mode={mode}")
            
            with db_lock:
                if mode == 'my_votes':
                    # Для режима "по моим оценкам" - получаем годы из импортированных фильмов с оценкой 9-10
                    cursor.execute("""
                        SELECT DISTINCT m.year
                        FROM movies m
                        JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                        AND m.year IS NOT NULL
                        ORDER BY m.year
                    """, (chat_id, user_id))
                    years_rows = cursor.fetchall()
                    years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row]
                    
                    logger.info(f"[RANDOM CALLBACK] Found {len(years)} years for my_votes mode")
                    
                    # Определяем доступные периоды на основе найденных годов
                    for period in all_periods:
                        if period == "До 1980":
                            if any(y < 1980 for y in years):
                                available_periods.append(period)
                        elif period == "1980–1990":
                            if any(1980 <= y <= 1990 for y in years):
                                available_periods.append(period)
                        elif period == "1990–2000":
                            if any(1990 <= y <= 2000 for y in years):
                                available_periods.append(period)
                        elif period == "2000–2010":
                            if any(2000 <= y <= 2010 for y in years):
                                available_periods.append(period)
                        elif period == "2010–2020":
                            if any(2010 <= y <= 2020 for y in years):
                                available_periods.append(period)
                        elif period == "2020–сейчас":
                            if any(y >= 2020 for y in years):
                                available_periods.append(period)
                elif mode == 'group_votes':
                    # Для режима "По оценкам в базе" - получаем годы из фильмов со средней оценкой группы >= 9
                    cursor.execute("""
                        SELECT DISTINCT m.year
                        FROM movies m
                        WHERE m.chat_id = %s AND m.year IS NOT NULL
                        AND EXISTS (
                            SELECT 1 FROM ratings r 
                            WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                            GROUP BY r.film_id, r.chat_id 
                            HAVING AVG(r.rating) >= 9
                        )
                        ORDER BY m.year
                    """, (chat_id,))
                    years_rows = cursor.fetchall()
                    years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row]
                    
                    logger.info(f"[RANDOM CALLBACK] Found {len(years)} years for group_votes mode")
                    
                    # Определяем доступные периоды на основе найденных годов
                    for period in all_periods:
                        if period == "До 1980":
                            if any(y < 1980 for y in years):
                                available_periods.append(period)
                        elif period == "1980–1990":
                            if any(1980 <= y <= 1990 for y in years):
                                available_periods.append(period)
                        elif period == "1990–2000":
                            if any(1990 <= y <= 2000 for y in years):
                                available_periods.append(period)
                        elif period == "2000–2010":
                            if any(2000 <= y <= 2010 for y in years):
                                available_periods.append(period)
                        elif period == "2010–2020":
                            if any(2010 <= y <= 2020 for y in years):
                                available_periods.append(period)
                        elif period == "2020–сейчас":
                            if any(y >= 2020 for y in years):
                                available_periods.append(period)
                else:
                    # Для режима database - используем старую логику
                    base_query = """
                        SELECT COUNT(DISTINCT m.id) 
                        FROM movies m
                        LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                        WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
                    """
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
                        
                        query = f"{base_query} AND {condition}"
                        cursor.execute(query, tuple(params))
                        count_row = cursor.fetchone()
                        count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                        
                        if count > 0:
                            available_periods.append(period)
            
            logger.info(f"[RANDOM CALLBACK] Available periods: {available_periods}")
            
            user_random_state[user_id]['available_periods'] = available_periods
            
            markup = InlineKeyboardMarkup(row_width=1)
            if available_periods:
                for period in available_periods:
                    markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            
            bot.answer_callback_query(call.id)
            text = f"{mode_description}\n\n🎲 <b>Шаг 1/4: Выберите период</b>\n\n(можно выбрать несколько или пропустить)"
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[RANDOM CALLBACK] ✅ Mode selected: {mode}, moving to period selection, user_id={user_id}")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_mode: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_mode_locked:"))
    def handle_rand_mode_locked(call):
        """Обработчик заблокированных режимов рандомайзера"""
        try:
            logger.info(f"[RANDOM CALLBACK] Locked mode handler: data={call.data}, user_id={call.from_user.id}")
            mode = call.data.split(":")[1]  # kinopoisk, my_votes, group_votes
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if mode == "kinopoisk":
                message_text = "🎬 Рандом по Кинопоиску доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            elif mode == "group_votes":
                message_text = "⭐ Режим \"По оценкам в базе\" доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            elif mode == "my_votes":
                # Проверяем количество оценок
                with db_lock:
                    cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    user_ratings_count = cursor.fetchone()
                    user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
                
                if user_ratings < 50:
                    message_text = "⭐ Режим \"По моим оценкам\" откроется после добавления 50 оценок в базу. Оцените больше фильмов!"
                else:
                    message_text = "⭐ Режим \"По моим оценкам\" доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            else:
                message_text = "🔒 Этот режим недоступен. Подключите подписку через /payment"
            
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
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_content_type:"))
    def handle_rand_content_type(call):
        """Обработчик выбора типа контента для режима kinopoisk"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== CONTENT TYPE HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}")
                bot.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
                return
            
            mode = user_random_state[user_id].get('mode')
            if mode != 'kinopoisk':
                logger.warning(f"[RANDOM CALLBACK] Content type handler called for non-kinopoisk mode: {mode}")
                bot.answer_callback_query(call.id, "❌ Неверный режим", show_alert=True)
                return
            
            if data == "back":
                # Возврат к выбору режима
                logger.info(f"[RANDOM CALLBACK] Content type back, returning to mode selection")
                bot.answer_callback_query(call.id)
                # Вызываем random_start для возврата к выбору режима
                from moviebot.bot.handlers.series import random_start
                class FakeMessage:
                    def __init__(self, call):
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.text = '/random'
                    def reply_to(self, text, **kwargs):
                        return bot.send_message(self.chat.id, text, **kwargs)
                fake_message = FakeMessage(call)
                random_start(fake_message)
                return
            
            # Сохраняем выбранный тип контента
            user_random_state[user_id]['content_type'] = data
            user_random_state[user_id]['step'] = 'period'
            
            logger.info(f"[RANDOM CALLBACK] Content type selected: {data}, moving to period selection")
            
            # Показываем выбор периодов
            available_periods = user_random_state[user_id].get('available_periods', [])
            if not available_periods:
                available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            
            markup = InlineKeyboardMarkup(row_width=1)
            if available_periods:
                for period in available_periods:
                    markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="rand_content_type:back"))
            
            mode_description = '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.'
            content_type_text = {
                'FILM': '🎬 Фильм',
                'TV_SERIES': '📺 Сериал',
                'ALL': '🎬 Фильм и Сериал'
            }.get(data, '')
            
            bot.answer_callback_query(call.id)
            text = f"{mode_description}\n\nВыбрано: {content_type_text}\n\n🎲 <b>Шаг 2/3: Выберите период</b>\n\n(можно выбрать несколько или пропустить)"
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_content_type: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_period:"))
    def handle_rand_period(call):
        """Обработчик выбора периода для рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== PERIOD HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, reinitializing")
                user_random_state[user_id] = {'step': 'period', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
            
            mode = user_random_state[user_id].get('mode')
            
            if data == "skip":
                logger.info(f"[RANDOM CALLBACK] Period skipped, moving to genre")
                user_random_state[user_id]['periods'] = []
                user_random_state[user_id]['step'] = 'genre'
                if mode == 'kinopoisk':
                    _show_genre_step_kinopoisk(call, chat_id, user_id)
                elif mode == 'group_votes':
                    _show_genre_step_group_votes(call, chat_id, user_id)
                else:
                    _show_genre_step(call, chat_id, user_id)
                return
            elif data == "done":
                logger.info(f"[RANDOM CALLBACK] Periods confirmed, moving to genre")
                user_random_state[user_id]['step'] = 'genre'
                if mode == 'kinopoisk':
                    _show_genre_step_kinopoisk(call, chat_id, user_id)
                elif mode == 'group_votes':
                    _show_genre_step_group_votes(call, chat_id, user_id)
                else:
                    _show_genre_step(call, chat_id, user_id)
                return
            else:
                # Toggle периода
                periods = user_random_state[user_id].get('periods', [])
                if data in periods:
                    periods.remove(data)
                    logger.info(f"[RANDOM CALLBACK] Period removed: {data}")
                else:
                    periods.append(data)
                    logger.info(f"[RANDOM CALLBACK] Period added: {data}")
                
                user_random_state[user_id]['periods'] = periods
                
                # Получаем доступные периоды из состояния
                available_periods = user_random_state[user_id].get('available_periods', [])
                if not available_periods:
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                
                # Обновляем кнопки
                markup = InlineKeyboardMarkup(row_width=1)
                if available_periods:
                    for p in available_periods:
                        label = f"✓ {p}" if p in periods else p
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{p}"))
                
                if periods:
                    markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
                else:
                    markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
                
                selected = ', '.join(periods) if periods else 'ничего'
                
                # Определяем текст шага в зависимости от режима
                mode = user_random_state[user_id].get('mode')
                if mode == 'kinopoisk':
                    step_text = "🎲 <b>Шаг 1/2: Выберите период</b>"
                    mode_description = '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.'
                    text = f"{mode_description}\n\n{step_text}\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                elif mode == 'group_votes':
                    step_text = "🎲 <b>Шаг 1/2: Выберите период</b>"
                    mode_description = '👥 <b>По оценкам в базе (9-10)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
                    text = f"{mode_description}\n\n{step_text}\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                else:
                    step_text = "🎲 <b>Шаг 1/4: Выберите период</b>"
                    text = f"{step_text}\n\nВыбрано: {selected}\n\n(можно выбрать несколько)"
                
                try:
                    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    bot.answer_callback_query(call.id)
                    logger.info(f"[RANDOM CALLBACK] Period keyboard updated, selected={selected}")
                except Exception as e:
                    logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                    bot.answer_callback_query(call.id, "Ошибка обновления")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_period: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка обработки")
            except:
                pass
    
    # ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ РАНДОМА ==========
    
    def _show_year_step(call, chat_id, user_id):
        """Показывает шаг выбора года для режима kinopoisk"""
        try:
            logger.info(f"[RANDOM] Showing year step for user {user_id}")
            
            state = user_random_state.get(user_id, {})
            selected_periods = state.get('periods', [])
            mode_description = {
                'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
            }.get(state.get('mode'), '')
            
            # Используем те же промежутки, что и в режиме "Рандом по своей базе"
            available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            
            markup = InlineKeyboardMarkup(row_width=1)
            for period in available_periods:
                label = f"✓ {period}" if period in selected_periods else period
                markup.add(InlineKeyboardButton(label, callback_data=f"rand_year:{period}"))
            
            if selected_periods:
                markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_year:done"))
            else:
                markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_year:skip"))
            
            selected = ', '.join(selected_periods) if selected_periods else 'ничего'
            text = f"{mode_description}\n\n🎲 <b>Шаг 1/2: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
            
            try:
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            
            logger.info(f"[RANDOM] Year step shown for user {user_id}")
        except Exception as e:
            logger.error(f"[RANDOM] ERROR in _show_year_step: {e}", exc_info=True)
    
    def _show_genre_step(call, chat_id, user_id):
        """Показывает шаг выбора жанра с учетом выбранных периодов"""
        try:
            logger.info(f"[RANDOM] Showing genre step for user {user_id}")
            
            # Получаем состояние пользователя
            state = user_random_state.get(user_id, {})
            selected_genres = state.get('genres', [])
            periods = state.get('periods', [])
            mode = state.get('mode')
            
            # --------------------- Формируем запрос ---------------------
            params = []
            
            if mode == 'my_votes':
                # Жанры из импортированных фильмов пользователя с оценкой 9-10
                base_query = """
                    SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                    FROM movies m
                    JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                    AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—'
                """
                params = [chat_id, user_id]
                
            elif mode == 'group_votes':
                # Жанры из фильмов со средней оценкой группы >= 9
                base_query = """
                    SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                    FROM movies m
                    WHERE m.chat_id = %s
                    AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—'
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                        GROUP BY r.film_id, r.chat_id 
                        HAVING AVG(r.rating) >= 9
                    )
                """
                params = [chat_id]
                
            else:
                # Обычный режим – жанры из непросмотренных фильмов чата
                base_query = """
                    SELECT DISTINCT TRIM(UNNEST(string_to_array(m.genres, ', '))) as genre
                    FROM movies m
                    LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                    WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
                    AND m.genres IS NOT NULL AND m.genres != '' AND m.genres != '—'
                """
                params = [chat_id]
            
            # --------------------- Фильтр по периодам ---------------------
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
            
            # --------------------- Выполняем запрос ---------------------
            genres = []  # всегда инициализируем, даже если запрос вернёт пусто
            with db_lock:
                cursor.execute(base_query, params)
                rows = cursor.fetchall()
                
                for row in rows:
                    genre = row.get('genre') if isinstance(row, dict) else (row[0] if row else None)
                    if genre and genre.strip():
                        genres.append(genre.strip())
            
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
            
            # Текст с выбранными жанрами
            selected_text = f"\n\nВыбрано: {', '.join(selected_genres)}" if selected_genres else ""
            
            text = f"🎬 <b>Шаг 2/4: Выберите жанр</b>\n\n(можно выбрать несколько){selected_text}"
            
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
            
            with db_lock:
                cursor.execute(base_query, params)
                rows = cursor.fetchall()
                directors = []
                for row in rows:
                    director = row.get('director') if isinstance(row, dict) else (row[0] if len(row) > 0 else None)
                    if director:
                        directors.append(director)
                logger.info(f"[RANDOM] Directors found: {len(directors)}")
            
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
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_year:"))
    def handle_rand_year(call):
        """Обработчик выбора года для режима kinopoisk"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== YEAR HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}")
                bot.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
                return
            
            mode = user_random_state[user_id].get('mode')
            
            # Для режима kinopoisk используем промежутки, как в режиме "Рандом по своей базе"
            if mode == 'kinopoisk':
                if data == "skip":
                    logger.info(f"[RANDOM CALLBACK] Periods skipped, moving to genre")
                    user_random_state[user_id]['periods'] = []
                    user_random_state[user_id]['step'] = 'genre'
                    _show_genre_step_kinopoisk(call, chat_id, user_id)
                elif data == "done":
                    logger.info(f"[RANDOM CALLBACK] Periods confirmed, moving to genre")
                    user_random_state[user_id]['step'] = 'genre'
                    _show_genre_step_kinopoisk(call, chat_id, user_id)
                else:
                    # Toggle промежутка
                    periods = user_random_state[user_id].get('periods', [])
                    if data in periods:
                        periods.remove(data)
                        logger.info(f"[RANDOM CALLBACK] Period removed: {data}")
                    else:
                        periods.append(data)
                        logger.info(f"[RANDOM CALLBACK] Period added: {data}")
                    
                    user_random_state[user_id]['periods'] = periods
                    
                    # Обновляем клавиатуру
                    state = user_random_state.get(user_id, {})
                    selected_periods = state.get('periods', [])
                    mode_description = {
                        'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.',
                        'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
                    }.get(state.get('mode'), '')
                    
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    for period in available_periods:
                        label = f"✓ {period}" if period in selected_periods else period
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_year:{period}"))
                    
                    if selected_periods:
                        markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_year:done"))
                    else:
                        markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_year:skip"))
                    
                    selected = ', '.join(selected_periods) if selected_periods else 'ничего'
                    text = f"{mode_description}\n\n🎲 <b>Шаг 1/2: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                    
                    try:
                        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        bot.answer_callback_query(call.id)
                    except Exception as e:
                        logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                        bot.answer_callback_query(call.id, "Ошибка обновления")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_year: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_genre:"))
    def handle_rand_genre(call):
        """Обработчик выбора жанра для рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== GENRE HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, reinitializing")
                user_random_state[user_id] = {'step': 'genre', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
            
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
                    # Для my_votes переходим сразу к финалу (жанр уже сохранен)
                    logger.info(f"[RANDOM CALLBACK] Mode {mode}: genre '{data}' selected, moving to final")
                    user_random_state[user_id]['step'] = 'final'
                    _random_final(call, chat_id, user_id)
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
            
            # Для режимов my_votes и group_votes после подтверждения жанров сразу переходим к финалу
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
                    
                    selected = ', '.join(periods) if periods else 'ничего'
                    mode_description = '👥 <b>По оценкам в базе (9-10)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.'
                    text = f"{mode_description}\n\n🎲 <b>Шаг 1/2: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько или пропустить)"
                    
                    try:
                        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        bot.answer_callback_query(call.id)
                    except Exception as e:
                        logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                        bot.answer_callback_query(call.id, "Ошибка обновления")
                    return
                
                # Переходим к финалу
                logger.info(f"[RANDOM CALLBACK] Mode {mode}: genres selected, moving to final")
                user_random_state[user_id]['step'] = 'final'
                _random_final(call, chat_id, user_id)
                return
            elif mode == 'my_votes':
                if data == "skip":
                    user_random_state[user_id]['genres'] = []
                elif data == "done":
                    pass  # Жанры уже сохранены
                
                # Переходим сразу к финалу
                logger.info(f"[RANDOM CALLBACK] Mode {mode}: genres selected, moving to final")
                user_random_state[user_id]['step'] = 'final'
                _random_final(call, chat_id, user_id)
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
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_dir:"))
    def handle_rand_dir(call):
        """Обработчик выбора режиссёра для рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== DIRECTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, reinitializing")
                user_random_state[user_id] = {'step': 'director', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
            
            if data == "skip":
                user_random_state[user_id]['directors'] = []
                user_random_state[user_id]['step'] = 'actor'
                logger.info(f"[RANDOM CALLBACK] Director skipped, moving to actor")
                if 'actors' not in user_random_state[user_id]:
                    user_random_state[user_id]['actors'] = []
                _show_actor_step(call, chat_id, user_id)
            elif data == "done":
                logger.info(f"[RANDOM CALLBACK] Directors confirmed, moving to actor")
                user_random_state[user_id]['step'] = 'actor'
                if 'actors' not in user_random_state[user_id]:
                    user_random_state[user_id]['actors'] = []
                _show_actor_step(call, chat_id, user_id)
            elif data == "back":
                logger.info(f"[RANDOM CALLBACK] Director back, moving to genre")
                user_random_state[user_id]['step'] = 'genre'
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
                
                # Обновляем клавиатуру
                _show_director_step(call, chat_id, user_id)
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_dir: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка обработки")
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_actor:"))
    def handle_rand_actor(call):
        """Обработчик выбора актёра для рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== ACTOR HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, reinitializing")
                user_random_state[user_id] = {'step': 'actor', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
            
            if data == "skip":
                user_random_state[user_id]['actors'] = []
                user_random_state[user_id]['step'] = 'final'
                logger.info(f"[RANDOM CALLBACK] Actors skipped, moving to final")
                _random_final(call, chat_id, user_id)
            elif data == "back":
                logger.info(f"[RANDOM CALLBACK] Actor back, moving to director")
                user_random_state[user_id]['step'] = 'director'
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
                
                # Обновляем клавиатуру
                _show_actor_step(call, chat_id, user_id)
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_actor: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка обработки")
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_final:"))
    def handle_rand_final(call):
        """Обработчик финального шага рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== FINAL HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Если это кнопка "Найти фильм" из случайных событий и нет состояния
            if call.data == "rand_final:go" and user_id not in user_random_state:
                logger.info(f"[RANDOM CALLBACK] Кнопка 'Найти фильм' из случайных событий, запускаем рандом по своей базе")
                bot.answer_callback_query(call.id)
                
                # Создаем фиктивное сообщение для вызова random_start
                class FakeMessage:
                    def __init__(self, call):
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.text = '/random'
                
                    def reply_to(self, text, **kwargs):
                        return bot.send_message(self.chat.id, text, **kwargs)
                
                fake_message = FakeMessage(call)
                random_start(fake_message)
                
                # Инициализируем состояние для рандома по своей базе
                user_random_state[user_id] = {
                    'step': 'mode',
                    'mode': 'database',
                    'periods': [],
                    'genres': [],
                    'directors': [],
                    'actors': []
                }
                
                # Автоматически переходим к финальному шагу (без фильтров)
                user_random_state[user_id]['step'] = 'final'
                _random_final(call, chat_id, user_id)
                return
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, initializing default state")
                user_random_state[user_id] = {
                    'step': 'final',
                    'mode': 'database',
                    'periods': [],
                    'genres': [],
                    'directors': [],
                    'actors': []
                }
                logger.info(f"[RANDOM CALLBACK] Default state initialized for user {user_id}")
            
            _random_final(call, chat_id, user_id)
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_final: {e}", exc_info=True)
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
                content_type = state.get('content_type', 'ALL')
                
                # Получаем любимый жанр из /total
                fav_genre = None
                with db_lock:
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
                        genres_str = row.get('genres') if isinstance(row, dict) else row[0]
                        if genres_str:
                            for g in str(genres_str).split(', '):
                                if g.strip():
                                    genre_counts[g.strip()] = genre_counts.get(g.strip(), 0) + 1
                    if genre_counts:
                        fav_genre = max(genre_counts, key=genre_counts.get)
                        logger.info(f"[RANDOM KINOPOISK] Любимый жанр: {fav_genre}")
                
                # Исключаем фильмы, которые уже в базе
                exclude_kp_ids = set()
                with db_lock:
                    cursor.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND kp_id IS NOT NULL', (chat_id,))
                    existing_movies = cursor.fetchall()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
                
                # Формируем список запросов: для каждого периода и каждого жанра (если выбрано несколько)
                search_queries = []
                
                # Если периоды не выбраны, используем один запрос без фильтра по годам
                if not periods:
                    periods = [None]  # Один запрос без фильтра по годам
                
                # Если жанры не выбраны, используем один запрос без фильтра по жанрам
                if not genres:
                    genres = [None]  # Один запрос без фильтра по жанрам
                
                # Формируем все комбинации периодов и жанров
                for period in periods:
                    for genre_id in genres:
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
                            'content_type': content_type
                        })
                
                # Выполняем поиск по всем запросам
                all_films = []
                
                for query in search_queries:
                    try:
                        # Передаем genre_id напрямую (число), если он есть
                        genre_param = int(query['genre_id']) if query['genre_id'] else None
                        films = search_films_by_filters(
                            genres=genre_param,
                            film_type=query['content_type'],
                            year_from=query['year_from'],
                            year_to=query['year_to'],
                            page=1
                        )
                        all_films.extend(films)
                        logger.info(f"[RANDOM KINOPOISK] Найдено {len(films)} фильмов для запроса: genre={query['genre_id']}, year={query['year_from']}-{query['year_to']}, type={query['content_type']}")
                    except Exception as e:
                        logger.error(f"[RANDOM KINOPOISK] Ошибка поиска для запроса {query}: {e}", exc_info=True)
                        continue
                
                if not all_films:
                    bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям на Кинопоиске.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                
                # Фильтруем фильмы: исключаем те, что уже в базе
                filtered_films = []
                for film in all_films:
                    kp_id_film = str(film.get('kinopoiskId', ''))
                    if kp_id_film and kp_id_film not in exclude_kp_ids:
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
                    # Получаем полную информацию о фильме
                    link = f"https://www.kinopoisk.ru/film/{kp_id_result}/"
                    movie_info = extract_movie_info(link)
                    
                    if movie_info:
                        # Используем show_film_info_with_buttons для отображения (там уже есть все нужные кнопки, включая "Выбрать онлайн-кинотеатр")
                        from moviebot.bot.handlers.series import show_film_info_with_buttons
                        show_film_info_with_buttons(
                            chat_id, user_id, movie_info, link, kp_id_result,
                            existing=None, message_id=call.message.message_id
                        )
                        bot.answer_callback_query(call.id)
                        del user_random_state[user_id]
                        return
                    else:
                        # Если не удалось получить полную информацию, показываем базовую с кнопками
                        title = selected_film.get('nameRu') or selected_film.get('nameEn', 'Без названия')
                        year = selected_film.get('year', '—')
                        film_genres = selected_film.get('genres', [])
                        genres_str = ', '.join([g.get('genre', '') for g in film_genres]) if film_genres else '—'
                        
                        text = f"🎬 <b>{title}</b> ({year})\n\n"
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
            # Формируем запрос - исключаем фильмы, которые уже запланированы и фильмы с импортированными оценками
            query = """SELECT m.id, m.title, m.year, m.genres, m.director, m.actors, m.description, m.link, m.kp_id 
                       FROM movies m
                       LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                       WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
                       AND m.id NOT IN (SELECT film_id FROM plans WHERE chat_id = %s)"""
            params = [chat_id, chat_id]
            
            # Фильтр по режиму
            mode = state.get('mode')
            if mode == 'my_votes':
                # Для режима "по моим оценкам" - выбираем 3 случайных фильма с оценкой 9-10,
                # находим похожие к ним, и выбираем случайный из похожих
                # Сначала получаем 3 случайных фильма с импортированной оценкой 9-10
                with db_lock:
                    cursor.execute("""
                        SELECT DISTINCT m.kp_id, m.id
                        FROM movies m
                        JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                        AND m.kp_id IS NOT NULL
                        ORDER BY RANDOM()
                        LIMIT 3
                    """, (chat_id, user_id))
                    favorite_films = cursor.fetchall()
                
                if not favorite_films:
                    bot.edit_message_text("😔 Не найдено фильмов с оценкой 9-10, импортированных с Кинопоиска.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                
                # Собираем все похожие фильмы к выбранным любимым
                all_similars = []
                from moviebot.api.kinopoisk_api import get_similars
                
                for film_row in favorite_films:
                    kp_id = film_row.get('kp_id') if isinstance(film_row, dict) else film_row[0]
                    if kp_id:
                        similars = get_similars(str(str(kp_id)))
                        logger.info(f"[RANDOM] Found {len(similars)} similar films for kp_id={kp_id}")
                        all_similars.extend(similars)
                
                # Убираем дубликаты по kp_id
                seen_kp_ids = set()
                unique_similars = []
                for item in all_similars:
                    # Поддерживаем как старый формат (kp_id, title), так и новый (kp_id, title, is_series)
                    if len(item) >= 2:
                        similar_kp_id = item[0]
                        similar_title = item[1]
                        if similar_kp_id not in seen_kp_ids:
                            seen_kp_ids.add(similar_kp_id)
                            unique_similars.append((similar_kp_id, similar_title))
                
                if not unique_similars:
                    bot.edit_message_text("😔 Не найдено похожих фильмов к вашим любимым.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                
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
                
                # Исключаем фильмы, которые уже в базе или просмотрены
                with db_lock:
                    cursor.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND (watched = 1 OR kp_id IS NOT NULL)', (chat_id,))
                    existing_movies = cursor.fetchall()
                    exclude_kp_ids = set()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
                
                for similar_kp_id, similar_title in unique_similars:
                    if str(similar_kp_id) in exclude_kp_ids:
                        continue
                        
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
                    selected_similar = random.choice(filtered_similars)
                    kp_id_result = str(selected_similar['kp_id'])
                    link = f"https://www.kinopoisk.ru/film/{kp_id_result}/"

                    movie_info = extract_movie_info(link)

                    if movie_info:
                        # Полная карточка — как при ссылке
                        from moviebot.bot.handlers.series import show_film_info_with_buttons
                        show_film_info_with_buttons(
                            chat_id=chat_id,
                            user_id=user_id,
                            info=movie_info,
                            link=link,
                            kp_id=kp_id_result,
                            existing=None,
                            message_id=call.message.message_id
                        )
                    else:
                        # Фолбэк на простой текст, если API упал
                        title = selected_similar['title']
                        year = selected_similar.get('year', '—')
                        text = f"🍿 <b>Случайный фильм:</b>\n\n<b>{title}</b> ({year})\n\n<a href='{link}'>Кинопоиск</a>"
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id_result}"))
                        markup.add(InlineKeyboardButton("🎬 Выбрать онлайн-кинотеатр", callback_data=f"streaming_select:{kp_id_result}"))
                        markup.add(InlineKeyboardButton("🔗 Добавить в базу", callback_data=f"add_to_database:{kp_id_result}"))
                        try:
                            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        except:
                            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')

                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                
                else:
                    bot.edit_message_text("😔 Не найдено похожих фильмов по заданным фильтрам.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
            elif mode == 'group_votes':
                # Для режима "По оценкам в базе" - выбираем случайный фильм из базы со средней оценкой >= 9,
                # который соответствует выбранным годам и жанрам, затем ищем похожие/сиквелы на Кинопоиске
                # Получаем фильтры из состояния
                periods = state.get('periods', [])
                genres = state.get('genres', [])
                
                # Получаем список kp_id фильмов, которые уже в базе (исключаем их)
                exclude_kp_ids = set()
                with db_lock:
                    cursor.execute('SELECT DISTINCT kp_id FROM movies WHERE chat_id = %s AND kp_id IS NOT NULL', (chat_id,))
                    existing_movies = cursor.fetchall()
                    for movie in existing_movies:
                        kp_id_val = movie.get('kp_id') if isinstance(movie, dict) else (movie[0] if len(movie) > 0 else None)
                        if kp_id_val:
                            exclude_kp_ids.add(str(kp_id_val))
                
                # Выбираем случайный фильм из базы со средней оценкой >= 9, который соответствует выбранным годам и жанрам
                base_query = """
                    SELECT m.kp_id, m.title, m.year, m.genres
                    FROM movies m
                    WHERE m.chat_id = %s AND m.kp_id IS NOT NULL
                    AND EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                        GROUP BY r.film_id, r.chat_id 
                        HAVING AVG(r.rating) >= 9
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
                
                base_query += " ORDER BY RANDOM() LIMIT 5"  # Берем 5 случайных фильмов для поиска похожих
                
                with db_lock:
                    cursor.execute(base_query, tuple(params))
                    base_films = cursor.fetchall()
                
                if not base_films:
                    bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям в вашей базе.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                
                # Функция для проверки, соответствует ли фильм критериям
                def film_matches_criteria(film_info, periods, genres, exclude_kp_ids):
                    """Проверяет, соответствует ли фильм критериям"""
                    kp_id = str(film_info.get('kp_id', ''))
                    if not kp_id or kp_id in exclude_kp_ids:
                        return False
                    
                    # Проверяем год
                    film_year = film_info.get('year')
                    if periods and film_year:
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
                    
                    # Проверяем жанры
                    if genres:
                        film_genres_str = film_info.get('genres', '')
                        film_genres = [g.strip().lower() for g in film_genres_str.split(',') if g.strip()]
                        if not any(g.lower() in film_genres for g in genres):
                            return False
                    
                    return True
                
                # Ищем похожие фильмы для каждого фильма из базы
                from moviebot.api.kinopoisk_api import get_similars, get_sequels, extract_movie_info
                found_film = None
                
                for base_film in base_films:
                    base_kp_id = str(base_film.get('kp_id') if isinstance(base_film, dict) else base_film[0])
                    if not base_kp_id:
                        continue
                    
                    logger.info(f"[RANDOM GROUP_VOTES] Ищем похожие для фильма {base_kp_id}")
                    
                    # 1. Ищем в similars
                    similars = get_similars(base_kp_id)
                    for similar in similars:
                        similar_kp_id = str(similar[0])
                        similar_info = extract_movie_info(f"https://kinopoisk.ru/film/{similar_kp_id}")
                        if similar_info and film_matches_criteria(similar_info, periods, genres, exclude_kp_ids):
                            found_film = similar_info
                            found_film['kp_id'] = similar_kp_id
                            logger.info(f"[RANDOM GROUP_VOTES] Найден похожий фильм: {similar_kp_id}")
                            break
                    
                    if found_film:
                        break
                    
                    # 2. Если не нашли в similars, ищем в sequels_and_prequels
                    sequels_data = get_sequels(base_kp_id)
                    for sequel_kp_id, sequel_name in sequels_data.get('sequels', []):
                        sequel_info = extract_movie_info(f"https://kinopoisk.ru/film/{sequel_kp_id}")
                        if sequel_info and film_matches_criteria(sequel_info, periods, genres, exclude_kp_ids):
                            found_film = sequel_info
                            found_film['kp_id'] = str(sequel_kp_id)
                            logger.info(f"[RANDOM GROUP_VOTES] Найден сиквел/приквел: {sequel_kp_id}")
                            break
                    
                    if found_film:
                        break
                
                if found_film:
                    # Показываем найденный фильм
                    kp_id_result = str(found_film['kp_id'])
                    title = found_film.get('title', 'Без названия')
                    year = found_film.get('year', '—')
                    genres_str = found_film.get('genres', '—')
                    description = found_film.get('description', '—')
                    director = found_film.get('director', 'Не указан')
                    actors = found_film.get('actors', '—')
                    link = f"https://www.kinopoisk.ru/film/{kp_id_result}/"
                    
                    text = f"🎬 <b>{title}</b> ({year})\n\n"
                    if description and description != '—':
                        text += f"{description[:300]}...\n\n"
                    text += f"🎭 <b>Жанры:</b> {genres_str}\n"
                    text += f"🎬 <b>Режиссёр:</b> {director}\n"
                    if actors and actors != '—':
                        text += f"👥 <b>Актёры:</b> {actors[:100]}...\n"
                    text += f"\n<a href='{link}'>Кинопоиск</a>"
                    
                    # Используем show_film_info_with_buttons для отображения
                    from moviebot.bot.handlers.series import show_film_info_with_buttons
                    show_film_info_with_buttons(
                        chat_id, user_id, found_film, link, kp_id_result,
                        existing=None, message_id=call.message.message_id
                    )
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
                else:
                    # Если не удалось найти фильм
                    bot.edit_message_text("😔 Не удалось найти фильм по заданным критериям на Кинопоиске.", chat_id, call.message.message_id)
                    bot.answer_callback_query(call.id)
                    del user_random_state[user_id]
                    return
            elif mode == 'database':
                # Режим "Рандом по своей базе" - только фильмы из базы
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
            
            with db_lock:
                cursor.execute(query, params)
                candidates = cursor.fetchall()
                logger.info(f"[RANDOM] Candidates found: {len(candidates)}")
            
            if not candidates:
                # Ищем похожие фильмы из запланированных
                similar_query = """SELECT m.title, m.year, m.link, m.kp_id
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
                            similar_list.append(f"• <a href='{link}'>{title}</a> ({year})")
                            if not first_movie_kp_id and kp_id:
                                first_movie_kp_id = kp_id
                    
                    if similar_list:
                        # Берем первый фильм для кнопки "Перейти к описанию"
                        message_text = f"🕵 Найден подходящий фильм в вашей базе!\n\n{similar_list[0].replace('• ', '')}"
                        
                        # Создаем кнопку "Перейти к описанию" для первого фильма
                        markup = InlineKeyboardMarkup()
                        if first_movie_kp_id:
                            markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"show_film_description:{first_movie_kp_id}"))
                        markup.add(InlineKeyboardButton("⬅️ Вернуться к меню", callback_data="random_back_to_menu"))
                    else:
                        message_text = "😔 Таких фильмов в базе не найдено!"
                        markup = InlineKeyboardMarkup()
                        markup.add(InlineKeyboardButton("⬅️ Вернуться к меню", callback_data="random_back_to_menu"))
                else:
                    message_text = "😔 Таких фильмов в базе не найдено!"
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("⬅️ Вернуться к меню", callback_data="random_back_to_menu"))
                
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
                text = f"🍿 <b>Случайный фильм:</b>\n\n<b>{title}</b> ({year})"
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

            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
            row = cur.fetchone()
            cur.close()
            conn.close()

            existing = None
            if row:
                existing = (row[0], row[1], row[2] if len(row) > 2 else False)

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
                'instruction_message_id': instruction_message_id,
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
                "🎫 Билеты в кино доступны с подпиской 🎫 Билеты или 📦 Все режимы. Подключите подписку через /payment",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[TICKET LOCKED] Ошибка: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_session:"))
    def ticket_session_callback(call):
        """Обработчик выбора сеанса - показывает информацию о сеансе и билеты"""
        try:
            from moviebot.utils.helpers import has_tickets_access
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Парсим plan_id и file_id (если есть)
            parts = call.data.split(":")
            plan_id = int(parts[1])
            file_id = parts[2] if len(parts) > 2 else None
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot.edit_message_text(
                    "🎫 <b>Билеты в кино</b>\n\n"
                    "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                    "Используйте /payment для оформления подписки.",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                return
            
            # Получаем информацию о сеансе (включая мероприятия без film_id)
            with db_lock:
                cursor.execute('''
                    SELECT p.id, p.plan_datetime, p.ticket_file_id, p.film_id,
                           COALESCE(m.title, 'Мероприятие') as title, 
                           m.kp_id
                    FROM plans p
                    LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.id = %s AND p.chat_id = %s AND p.plan_type = 'cinema'
                ''', (plan_id, chat_id))
                plan_row = cursor.fetchone()
            
            if not plan_row:
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
            
            # Форматируем дату и время
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
            
            # Формируем текст и кнопки
            text = f"🎬 <b>{title}</b>\n\n"
            text += f"📅 <b>Дата и время:</b> {date_str}\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            if ticket_file_id:
                text += "🎟️ <b>Билеты загружены</b>\n\n"
                text += "Билеты будут отправлены вам перед сеансом."
                markup.add(InlineKeyboardButton("📎 Показать билеты", callback_data=f"show_ticket:{plan_id}"))
                markup.add(InlineKeyboardButton("➕ Добавить ещё билеты", callback_data=f"add_more_tickets:{plan_id}"))
                markup.add(InlineKeyboardButton("🔄 Заменить билеты", callback_data=f"add_ticket:{plan_id}"))
            else:
                text += "🎟️ <b>Билеты не загружены</b>\n\n"
                text += "Загрузите билеты, чтобы получать их перед сеансом."
                markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
            
            # Добавляем кнопку "✏️ Изменить" для изменения времени сеанса
            markup.add(InlineKeyboardButton("✏️ Изменить", callback_data=f"ticket_edit_time:{plan_id}"))
            
            # Если это мероприятие без film_id, добавляем кнопку "🗑️ Удалить"
            if not film_id:
                markup.add(InlineKeyboardButton("🗑️ Удалить из расписания", callback_data=f"remove_from_calendar:{plan_id}"))
            elif kp_id:
                # Если это фильм, добавляем кнопку "📖 Перейти к описанию"
                markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"show_film_description:{kp_id}"))
            
            if file_id:
                # Если есть file_id, значит пользователь хочет добавить билеты к этому сеансу
                user_ticket_state[user_id] = {
                    'step': 'upload_ticket',
                    'plan_id': plan_id,
                    'chat_id': chat_id,
                    'file_id': file_id
                }
                text += "\n\n📎 Файл готов к добавлению. Нажмите '➕ Добавить билеты' для продолжения."
            
            markup.add(InlineKeyboardButton("⬅️ Назад к сеансам", callback_data="ticket_new"))
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            # Показываем информацию о сеансе
            try:
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except telebot.apihelper.ApiTelegramException as e:
                error_str = str(e).lower()
                if "message is not modified" in error_str:
                    # Если сообщение не изменилось, просто обновляем клавиатуру
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

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_new"))
    def ticket_new_callback(call):
        """Обработчик кнопки 'Добавить новый сеанс' - показывает выбор типа билета"""
        try:
            from moviebot.states import user_ticket_state
            from moviebot.utils.helpers import has_tickets_access
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if not has_tickets_access(chat_id, user_id):
                bot.edit_message_text(
                    "🎫 <b>Билеты в кино</b>\n\n"
                    "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                    "Используйте /payment для оформления подписки.",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                return
            
            parts = call.data.split(":")
            file_id = parts[1] if len(parts) > 1 else None
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("➕ Добавить фильм", callback_data=f"ticket_new_film:{file_id}" if file_id else "ticket_new_film"))
            markup.add(InlineKeyboardButton("🎤 Добавить билет", callback_data="ticket:add_event"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))  # ← НОВАЯ КНОПКА
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            bot.edit_message_text(
                "🎫 <b>Добавление билета</b>\n\n"
                "Выберите тип билета:",
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET NEW] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
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
                'step': 'event_name',
                'chat_id': chat_id,
                'type': 'event'
            }
            
            bot.edit_message_text(
                "🎤 <b>Добавление билета на мероприятие</b>\n\n"
                "Напишите название мероприятия в ответ на это сообщение:",
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET ADD EVENT] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_new_film"))
    def ticket_new_film_callback(call):
        """Обработчик кнопки 'Добавить фильм' - начинает флоу добавления билета на фильм"""
        try:
            from moviebot.states import user_ticket_state
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Парсим file_id из callback_data, если есть
            parts = call.data.split(":")
            file_id = parts[1] if len(parts) > 1 else None
            
            # Начинаем флоу добавления билета на фильм
            user_ticket_state[user_id] = {
                'step': 'waiting_new_session',
                'chat_id': chat_id,
                'type': 'film',
                'file_id': file_id
            }
            
            # Проверяем, не совпадает ли текст с текущим сообщением
            current_text = call.message.text or ""
            new_text = (
                "🎬 <b>Добавление билета на фильм</b>\n\n"
                "Отправьте ссылку на фильм или его ID с Кинопоиска и укажите дату/время сеанса.\n"
                "Формат: ссылка или ID + дата + время\n"
                "Например: https://kinopoisk.ru/film/123456/ 15 января 19:30"
            )
            
            # Если текст совпадает, просто обновляем клавиатуру или отправляем новое сообщение
            if current_text.strip() == new_text.strip():
                # Текст не изменился, отправляем новое сообщение
                bot.send_message(
                    chat_id,
                    new_text,
                    parse_mode='HTML'
                )
            else:
                # Текст изменился, обновляем сообщение
                try:
                    bot.edit_message_text(
                        new_text,
                        chat_id,
                        call.message.message_id,
                        parse_mode='HTML'
                    )
                except telebot.apihelper.ApiTelegramException as e:
                    error_str = str(e).lower()
                    if "message is not modified" in error_str:
                        # Если сообщение не изменилось, отправляем новое
                        bot.send_message(
                            chat_id,
                            new_text,
                            parse_mode='HTML'
                        )
                    else:
                        raise
        except Exception as e:
            logger.error(f"[TICKET NEW FILM] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("show_ticket:"))
    def show_ticket_callback(call):
        """Обработчик кнопки 'Показать билеты' - отправляет билеты пользователю"""
        try:
            from moviebot.utils.helpers import has_tickets_access
            
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot.answer_callback_query(
                    call.id,
                    "🎫 Билеты в кино доступны с подпиской 🎫 Билеты или 📦 Все режимы. Подключите подписку через /payment",
                    show_alert=True
                )
                return
            
            # Получаем ticket_file_id (может быть JSON массив или один file_id)
            import json
            with db_lock:
                cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                ticket_row = cursor.fetchone()
            
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
            
            # Парсим билеты (может быть JSON массив или один file_id)
            ticket_files = []
            try:
                ticket_files = json.loads(ticket_data)
                if not isinstance(ticket_files, list):
                    ticket_files = [ticket_data]
            except:
                # Старый формат - один file_id
                ticket_files = [ticket_data]
            
            # Отправляем все билеты
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
            
            bot.edit_message_text(
                "📎 <b>Загрузка дополнительных билетов</b>\n\n"
                "Отправьте файлы билетов. После загрузки всех билетов напишите 'готово'.",
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
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
            with db_lock:
                cursor.execute('SELECT plan_datetime FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                plan_row = cursor.fetchone()
            
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
                    with db_lock:
                        cursor.execute('''
                            INSERT INTO stats (user_id, username, command_or_action, timestamp, chat_id)
                            VALUES (%s, %s, %s, %s, %s)
                        ''', (
                            user_id,
                            username,
                            'dice_game:thrown',
                            datetime.now(PLANS_TZ).isoformat(),
                            chat_id
                        ))
                        conn.commit()
                    
                    logger.info(f"[DICE GAME] Пользователь {user_id} ({username}) бросил кубик в чате {chat_id}, message_id={dice_msg.message_id}")
                    
                    # Обновляем сообщение с результатами
                    message_id_to_update = game_state.get('message_id', message_id)
                    update_dice_game_message(chat_id, game_state, message_id_to_update, BOT_ID)
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

    @bot.message_handler(content_types=['dice'])
    def handle_dice_result(message):
        """Обработчик получения значения кубика"""
        try:
            from moviebot.bot.bot_init import BOT_ID
            from moviebot.utils.random_events import update_dice_game_message
            from datetime import datetime, timedelta
            
            logger.info(f"[DICE GAME RESULT] ===== START: message_id={message.message_id}, chat_id={message.chat.id}, user_id={message.from_user.id if message.from_user else None}")
            
            # Проверяем наличие dice и эмодзи
            if not message.dice:
                logger.warning(f"[DICE GAME RESULT] Сообщение {message.message_id} не содержит dice")
                return
            
            logger.info(f"[DICE GAME RESULT] dice.emoji={message.dice.emoji}, dice.value={message.dice.value}")
            
            if message.dice.emoji != '🎲':
                logger.info(f"[DICE GAME RESULT] Пропуск: эмодзи {message.dice.emoji} не является 🎲")
                return
            
            chat_id = message.chat.id
            if chat_id not in dice_game_state:
                logger.warning(f"[DICE GAME RESULT] Чат {chat_id} не найден в dice_game_state")
                return
            
            game_state = dice_game_state[chat_id]
            dice_message_id = message.message_id
            dice_value = message.dice.value
            
            logger.info(f"[DICE GAME RESULT] Получено значение кубика: {dice_value} для message_id={dice_message_id}")
            
            # Находим пользователя по message_id кубика
            # Сначала проверяем прямое соответствие
            user_id = game_state.get('dice_messages', {}).get(dice_message_id)
            
            # Если не найдено, ищем по участникам (на случай, если message_id изменился)
            if not user_id:
                logger.info(f"[DICE GAME RESULT] Поиск пользователя по dice_message_id в participants...")
                for uid, p in game_state.get('participants', {}).items():
                    stored_dice_id = p.get('dice_message_id')
                    if stored_dice_id == dice_message_id:
                        user_id = uid
                        logger.info(f"[DICE GAME RESULT] Пользователь найден в participants: user_id={user_id}, stored_dice_id={stored_dice_id}")
                        break
                
                # Если все еще не найдено, пробуем найти по from_user.id (если есть)
                if not user_id and message.from_user:
                    potential_user_id = message.from_user.id
                    if potential_user_id in game_state.get('participants', {}):
                        # Проверяем, есть ли у этого пользователя уже значение кубика
                        if 'value' not in game_state['participants'][potential_user_id] or game_state['participants'][potential_user_id].get('value') is None:
                            user_id = potential_user_id
                            # Обновляем dice_message_id для этого пользователя
                            game_state['participants'][user_id]['dice_message_id'] = dice_message_id
                            game_state['dice_messages'][dice_message_id] = user_id
                            logger.info(f"[DICE GAME RESULT] Пользователь найден по from_user.id: user_id={user_id}, обновлен dice_message_id")
            
            if not user_id:
                logger.warning(f"[DICE GAME RESULT] Пользователь не найден для dice_message_id={dice_message_id}")
                logger.info(f"[DICE GAME RESULT] dice_messages keys: {list(game_state.get('dice_messages', {}).keys())}")
                logger.info(f"[DICE GAME RESULT] participants: {list(game_state.get('participants', {}).keys())}")
                # Выводим детальную информацию для отладки
                for uid, p in game_state.get('participants', {}).items():
                    logger.info(f"[DICE GAME RESULT] participant {uid}: dice_message_id={p.get('dice_message_id')}, value={p.get('value')}")
                return
            
            logger.info(f"[DICE GAME RESULT] Найден пользователь: user_id={user_id}, значение кубика={dice_value}")
            
            # Сохраняем значение кубика
            if user_id in game_state['participants']:
                old_value = game_state['participants'][user_id].get('value')
                game_state['participants'][user_id]['value'] = dice_value
                game_state['last_dice_time'] = datetime.now(PLANS_TZ)  # Обновляем время последнего броска
                
                username = game_state['participants'][user_id].get('username', f'user_{user_id}')
                logger.info(f"[DICE GAME RESULT] ✅ Сохранено значение кубика для {username} (user_id={user_id}): {dice_value} (было: {old_value})")
                
                # Обновляем сообщение с результатами
                if 'message_id' in game_state:
                    logger.info(f"[DICE GAME RESULT] Обновление сообщения с результатами, message_id={game_state['message_id']}")
                    update_dice_game_message(chat_id, game_state, game_state['message_id'], BOT_ID)
                else:
                    logger.warning(f"[DICE GAME RESULT] message_id не найден в game_state")
            else:
                logger.warning(f"[DICE GAME RESULT] user_id={user_id} не найден в participants")
                
            logger.info(f"[DICE GAME RESULT] ===== END =====")
        except Exception as e:
            logger.error(f"[DICE GAME RESULT] ❌ Ошибка в handle_dice_result: {e}", exc_info=True)

    @bot.edited_message_handler(content_types=['dice'])
    def handle_dice_result_edited(message):
        """Обработчик обновления сообщения с кубиком (когда кубик останавливается)"""
        try:
            from moviebot.bot.bot_init import BOT_ID
            from moviebot.utils.random_events import update_dice_game_message
            from datetime import datetime, timedelta
            
            logger.info(f"[DICE GAME RESULT EDITED] ===== START: message_id={message.message_id}, chat_id={message.chat.id}, user_id={message.from_user.id if message.from_user else None}")
            
            # Проверяем наличие dice и эмодзи
            if not message.dice:
                logger.warning(f"[DICE GAME RESULT EDITED] Сообщение {message.message_id} не содержит dice")
                return
            
            logger.info(f"[DICE GAME RESULT EDITED] dice.emoji={message.dice.emoji}, dice.value={message.dice.value}")
            
            if message.dice.emoji != '🎲':
                logger.info(f"[DICE GAME RESULT EDITED] Пропуск: эмодзи {message.dice.emoji} не является 🎲")
                return
            
            chat_id = message.chat.id
            if chat_id not in dice_game_state:
                logger.warning(f"[DICE GAME RESULT EDITED] Чат {chat_id} не найден в dice_game_state")
                return
            
            game_state = dice_game_state[chat_id]
            dice_message_id = message.message_id
            dice_value = message.dice.value
            
            logger.info(f"[DICE GAME RESULT EDITED] Получено значение кубика: {dice_value} для message_id={dice_message_id}")
            
            # Находим пользователя по message_id кубика
            user_id = game_state.get('dice_messages', {}).get(dice_message_id)
            
            # Если не найдено, ищем по участникам
            if not user_id:
                logger.info(f"[DICE GAME RESULT EDITED] Поиск пользователя по dice_message_id в participants...")
                for uid, p in game_state.get('participants', {}).items():
                    stored_dice_id = p.get('dice_message_id')
                    if stored_dice_id == dice_message_id:
                        user_id = uid
                        logger.info(f"[DICE GAME RESULT EDITED] Пользователь найден в participants: user_id={user_id}, stored_dice_id={stored_dice_id}")
                        break
                
                # Если все еще не найдено, пробуем найти по from_user.id (если есть)
                if not user_id and message.from_user:
                    potential_user_id = message.from_user.id
                    if potential_user_id in game_state.get('participants', {}):
                        # Проверяем, есть ли у этого пользователя уже значение кубика
                        if 'value' not in game_state['participants'][potential_user_id] or game_state['participants'][potential_user_id].get('value') is None:
                            user_id = potential_user_id
                            # Обновляем dice_message_id для этого пользователя
                            game_state['participants'][user_id]['dice_message_id'] = dice_message_id
                            game_state['dice_messages'][dice_message_id] = user_id
                            logger.info(f"[DICE GAME RESULT EDITED] Пользователь найден по from_user.id: user_id={user_id}, обновлен dice_message_id")
            
            if not user_id:
                logger.warning(f"[DICE GAME RESULT EDITED] Пользователь не найден для dice_message_id={dice_message_id}")
                logger.info(f"[DICE GAME RESULT EDITED] dice_messages keys: {list(game_state.get('dice_messages', {}).keys())}")
                logger.info(f"[DICE GAME RESULT EDITED] participants: {list(game_state.get('participants', {}).keys())}")
                return
            
            logger.info(f"[DICE GAME RESULT EDITED] Найден пользователь: user_id={user_id}, значение кубика={dice_value}")
            
            # Сохраняем значение кубика
            if user_id in game_state['participants']:
                old_value = game_state['participants'][user_id].get('value')
                game_state['participants'][user_id]['value'] = dice_value
                game_state['last_dice_time'] = datetime.now(PLANS_TZ)  # Обновляем время последнего броска
                
                username = game_state['participants'][user_id].get('username', f'user_{user_id}')
                logger.info(f"[DICE GAME RESULT EDITED] ✅ Сохранено значение кубика для {username} (user_id={user_id}): {dice_value} (было: {old_value})")
                
                # Обновляем сообщение с результатами
                if 'message_id' in game_state:
                    logger.info(f"[DICE GAME RESULT EDITED] Обновление сообщения с результатами, message_id={game_state['message_id']}")
                    update_dice_game_message(chat_id, game_state, game_state['message_id'], BOT_ID)
                else:
                    logger.warning(f"[DICE GAME RESULT EDITED] message_id не найден в game_state")
            else:
                logger.warning(f"[DICE GAME RESULT EDITED] user_id={user_id} не найден в participants")
                
            logger.info(f"[DICE GAME RESULT EDITED] ===== END =====")
        except Exception as e:
            logger.error(f"[DICE GAME RESULT EDITED] ❌ Ошибка в handle_dice_result_edited: {e}", exc_info=True)

    # Обработчик ссылок на Кинопоиск вынесен на уровень модуля для правильной регистрации
    pass


    # Обработчик settings: перенесен в handlers/settings.py

    # Обработчик текстовых сообщений для поиска (ответы на сообщения поиска)
    @bot.message_handler(content_types=['text'], func=lambda m: m.text and not m.text.strip().startswith('/') and m.from_user.id in user_search_state)
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
            
            # Получаем тип поиска из состояния
            search_type = state.get('search_type', 'mixed')
            logger.info(f"[SEARCH REPLY] Тип поиска: {search_type}")
            
            # Выполняем поиск
            logger.info(f"[SEARCH REPLY] Вызов search_films_with_type для query={query}, search_type={search_type}")
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY] Поиск завершен: найдено {len(films)} результатов, страниц: {total_pages}")
            
            if not films:
                logger.warning(f"[SEARCH REPLY] Ничего не найдено по запросу '{query}'")
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔄 Повторить запрос", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'", reply_markup=markup)
                # Очищаем состояние
                del user_search_state[user_id]
                return
            
            # Формируем сообщение с результатами
            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[:10]:  # Показываем максимум 10 результатов на странице
                try:
                    # Пробуем разные варианты полей для совместимости с разными версиями API
                    title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                    year = film.get('year') or film.get('releaseYear') or 'N/A'
                    rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                    # Пробуем разные варианты ID
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    # Определяем тип (сериал или фильм) по полю type из API
                    film_type = film.get('type', '').upper() if film.get('type') else 'FILM'  # "FILM" или "TV_SERIES"
                    is_series = film_type == 'TV_SERIES'
                    
                    if kp_id:
                        # Ограничиваем длину текста кнопки
                        type_indicator = "📺" if is_series else "🎬"
                        button_text = f"{type_indicator} {title} ({year})"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                        if rating != 'N/A':
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
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
            row = cursor.fetchone()
        
        if row:
            # Уже в базе — обновляем актуальными данными
            film_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None) if not isinstance(row, dict) else row.get('id')
            logger.info(f"[KINOPOISK LINK] Фильм в базе (id={film_id}) — обновляем данные")
            
            with db_lock:
                cursor.execute('''
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
                conn.commit()
            
            # Получаем watched для existing
            cursor.execute("SELECT title, watched FROM movies WHERE id = %s", (film_id,))
            movie_row = cursor.fetchone()
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
    with db_lock:
        cursor.execute("SELECT id FROM films WHERE kp_id = %s", (str(kp_id),))
        existing = cursor.fetchone()
        
        if not existing:
            # Фильма нет в базе, добавляем его
            link = f"https://kinopoisk.ru/film/{kp_id}"
            info = extract_movie_info(link)
            
            if info:
                cursor.execute("""
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
                conn.commit()
                logger.info(f"[ENSURE MOVIE] Фильм {kp_id} добавлен в базу")
            else:
                logger.warning(f"[ENSURE MOVIE] Не удалось получить информацию о фильме {kp_id}")
        
        return existing or cursor.lastrowid

# Обработчик текстовых сообщений для поиска (ответы на сообщения поиска)
@bot.message_handler(content_types=['text'], func=lambda m: m.text and not m.text.strip().startswith('/') and m.from_user.id in user_search_state)
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
            
            # Получаем тип поиска из состояния
            search_type = state.get('search_type', 'mixed')
            logger.info(f"[SEARCH REPLY] Тип поиска: {search_type}")
            
            # Выполняем поиск
            logger.info(f"[SEARCH REPLY] Вызов search_films_with_type для query={query}, search_type={search_type}")
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY] Поиск завершен: найдено {len(films)} результатов, страниц: {total_pages}")
            
            if not films:
                logger.warning(f"[SEARCH REPLY] Ничего не найдено по запросу '{query}'")
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🔄 Повторить запрос", callback_data="search:retry"))
                markup.add(InlineKeyboardButton("⬅️ Вернуться в меню", callback_data="back_to_start_menu"))
                bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'", reply_markup=markup)
                # Очищаем состояние
                del user_search_state[user_id]
                return
            
            # Формируем сообщение с результатами
            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[:10]:  # Показываем максимум 10 результатов на странице
                title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                year = film.get('year') or film.get('releaseYear') or 'N/A'
                rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                
                # Определяем тип (сериал или фильм)
                film_type = film.get('type', '').upper()
                is_series = film_type == 'TV_SERIES'
                
                if kp_id:
                    type_indicator = "📺" if is_series else "🎬"
                    button_text = f"{type_indicator} {title} ({year})"
                    if len(button_text) > 50:
                        button_text = button_text[:47] + "..."
                    results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                    if rating != 'N/A':
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
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id, text="⏳ Загружаю информацию...")
            logger.info(f"[ADD FILM FROM SEARCH] answer_callback_query вызван, callback_id={call.id}")
            
            # Парсим callback_data: add_film_{kp_id}:{film_type}
            parts = call.data.split(":")
            if len(parts) < 2:
                logger.error(f"[ADD FILM FROM SEARCH] Неверный формат callback_data: {call.data}")
                bot.answer_callback_query(call.id, "❌ Ошибка: неверный формат", show_alert=True)
                return
            
            kp_id = parts[0].replace("add_film_", "")
            film_type = parts[1] if len(parts) > 1 else "FILM"
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
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
                from moviebot.bot.bot_init import safe_answer_callback_query
                safe_answer_callback_query(bot, call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Проверяем, есть ли фильм уже в базе
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            conn = get_db_connection()
            cursor = get_db_cursor()
            
            existing = None
            # Приводим kp_id к строке для корректного поиска в БД
            kp_id_str = str(kp_id)
            with db_lock:
                cursor.execute("SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id_str))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title = row.get('title') if isinstance(row, dict) else row[1]
                    watched = row.get('watched') if isinstance(row, dict) else row[2]
                    existing = (film_id, title, watched)
            
            # Показываем карточку фильма с кнопками (всегда, даже если просмотрен)
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id_str, existing)
            
            logger.info(f"[ADD FILM FROM SEARCH] ===== END: успешно показана информация о фильме {kp_id}")
        except Exception as e:
            logger.error(f"[ADD FILM FROM SEARCH] Ошибка: {e}", exc_info=True)
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id, "❌ Ошибка обработки", show_alert=True)
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
        with db_lock:
            logger.info(f"[ENSURE MOVIE] db_lock получен, проверяю существование фильма")
            # Проверяем, существует ли фильм
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
            row = cursor.fetchone()
            
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                logger.info(f"[ENSURE MOVIE] Фильм уже в базе: film_id={film_id}, kp_id={kp_id}")
                logger.info(f"[ENSURE MOVIE] ===== END (уже в базе) =====")
                return film_id, False
            
            # Добавляем фильм в базу
            logger.info(f"[ENSURE MOVIE] Фильм не найден, добавляю в БД")
            logger.info(f"[ENSURE MOVIE] Данные: title={info.get('title', 'N/A')}, year={info.get('year', 'N/A')}, is_series={info.get('is_series', False)}")
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                RETURNING id
            ''', (chat_id, link, str(kp_id), info['title'], info['year'], info['genres'], info['description'], 
                  info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
            
            result = cursor.fetchone()
            logger.info(f"[ENSURE MOVIE] INSERT выполнен, result={result}")
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            logger.info(f"[ENSURE MOVIE] film_id извлечен: {film_id}")
            conn.commit()
            logger.info(f"[ENSURE MOVIE] commit выполнен")
            
            logger.info(f"[ENSURE MOVIE] Фильм добавлен в базу: film_id={film_id}, kp_id={kp_id}, title={info['title']}")
            logger.info(f"[ENSURE MOVIE] ===== END (добавлен) =====")
            return film_id, True
            
    except Exception as e:
        logger.error(f"[ENSURE MOVIE] КРИТИЧЕСКАЯ ОШИБКА при добавлении фильма в базу: {e}", exc_info=True)
        try:
            conn.rollback()
            logger.info(f"[ENSURE MOVIE] rollback выполнен")
        except Exception as rollback_e:
            logger.error(f"[ENSURE MOVIE] Ошибка при rollback: {rollback_e}")
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
                
                # Проверяем тип - только FILM
                if item.get('type') != 'FILM':
                    continue
                
                user_rating = item.get('userRating')
                if not user_rating or user_rating < 1 or user_rating > 10:
                    continue
                
                link = f"https://kinopoisk.ru/film/{kp_id}/"
                
                # Импортированные оценки НЕ добавляют фильмы в базу группы
                # Они существуют только как оценки в таблице ratings с is_imported = TRUE
                # Для импортированных оценок используем film_id = NULL или создаем виртуальный film_id
                try:
                    with db_lock:
                        # Проверяем, есть ли фильм в базе группы (добавлен через бота)
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                        film_row = cursor.fetchone()
                        
                        if film_row:
                            # Фильм уже есть в базе группы - можем добавить импортированную оценку
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            logger.debug(f"[IMPORT] Фильм {kp_id} уже существует в базе группы, film_id={film_id}")
                            
                            # Проверяем, есть ли уже оценка у этого пользователя для этого фильма
                            cursor.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s
                            ''', (chat_id, film_id, user_id))
                            existing_rating = cursor.fetchone()
                            
                            if existing_rating:
                                # Оценка уже есть, пропускаем
                                cursor.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                                title_row = cursor.fetchone()
                                title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                                logger.debug(f"[IMPORT] Фильм {title} уже имеет оценку, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку для существующего фильма
                            cursor.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id)
                                VALUES (%s, %s, %s, %s, TRUE, %s)
                                ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = TRUE, kp_id = EXCLUDED.kp_id
                            ''', (chat_id, film_id, user_id, user_rating, kp_id))
                            conn.commit()
                            
                            imported_count += 1
                            cursor.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                            title_row = cursor.fetchone()
                            title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                            logger.info(f"[IMPORT] Импортирован фильм {title} с оценкой {user_rating}")
                        else:
                            # Фильма нет в базе группы - создаем импортированную оценку БЕЗ добавления фильма в movies
                            # Используем film_id = NULL и kp_id для хранения импортированных оценок
                            title = item.get('nameRu') or item.get('nameEn') or 'Без названия'
                            
                            # Проверяем, есть ли уже импортированная оценка для этого kp_id и пользователя
                            cursor.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND kp_id = %s AND user_id = %s AND film_id IS NULL
                            ''', (chat_id, kp_id, user_id))
                            existing_imported_rating = cursor.fetchone()
                            
                            if existing_imported_rating:
                                logger.debug(f"[IMPORT] Импортированная оценка для фильма {kp_id} ({title}) уже существует, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку БЕЗ film_id (film_id = NULL)
                            cursor.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id)
                                VALUES (%s, NULL, %s, %s, TRUE, %s)
                            ''', (chat_id, user_id, user_rating, kp_id))
                            conn.commit()
                            
                            imported_count += 1
                            logger.info(f"[IMPORT] Импортирован фильм {title} (kp_id={kp_id}) с оценкой {user_rating} (без добавления в базу группы)")
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
    conn = get_db_connection()
    cursor = get_db_cursor()
    
    if target == 'user':
        # Удаление всех данных пользователя
        with db_lock:
            # Удаляем оценки пользователя (но не импортированные - они удаляются отдельной командой)
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, user_id))
            ratings_deleted = cursor.rowcount
            
            # Удаляем планы пользователя
            cursor.execute('DELETE FROM plans WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
            plans_deleted = cursor.rowcount
            
            # Удаляем отметки просмотра пользователя
            cursor.execute('DELETE FROM watched_movies WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
            watched_deleted = cursor.rowcount
            
            # Удаляем статистику пользователя
            cursor.execute('DELETE FROM stats WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
            stats_deleted = cursor.rowcount
            
            # Удаляем настройки пользователя
            cursor.execute('DELETE FROM settings WHERE chat_id = %s AND key LIKE %s', (user_id, 'user_%'))
            settings_deleted = cursor.rowcount
            
            conn.commit()
        
        bot.reply_to(message, 
            f"✅ Ваши данные удалены:\n"
            f"• Оценок: {ratings_deleted}\n"
            f"• Планов: {plans_deleted}\n"
            f"• Отметок просмотра: {watched_deleted}\n"
            f"• Статистики: {stats_deleted}\n"
            f"• Настроек: {settings_deleted}")
        del user_clean_state[user_id]
    
    elif target == 'imported_ratings':
        # Удаление импортированных оценок пользователя
        with db_lock:
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND user_id = %s AND is_imported = TRUE', (chat_id, user_id))
            imported_deleted = cursor.rowcount
            conn.commit()
        
        bot.reply_to(message, f"✅ Удалено импортированных оценок: {imported_deleted}")
        del user_clean_state[user_id]
    
    elif target == 'clean_imported_movies':
        # Удаление фильмов, которые были добавлены только из-за импорта
        # Удаляем фильмы, у которых есть только импортированные оценки и нет обычных
        with db_lock:
            # Находим фильмы, которые имеют только импортированные оценки
            cursor.execute('''
                SELECT DISTINCT m.id, m.title
                FROM movies m
                WHERE m.chat_id = %s
                  AND m.watched = 0
                  AND m.id NOT IN (
                      SELECT DISTINCT film_id 
                      FROM plans 
                      WHERE chat_id = %s AND film_id IS NOT NULL
                  )
                  AND EXISTS (
                      SELECT 1 
                      FROM ratings r 
                      WHERE r.chat_id = %s 
                        AND r.film_id = m.id 
                        AND r.is_imported = TRUE
                  )
                  AND NOT EXISTS (
                      SELECT 1 
                      FROM ratings r 
                      WHERE r.chat_id = %s 
                        AND r.film_id = m.id 
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                  )
            ''', (chat_id, chat_id, chat_id, chat_id))
            movies_to_delete = cursor.fetchall()
            
            if not movies_to_delete:
                bot.reply_to(message, "✅ Нет фильмов для удаления. Все фильмы либо имеют обычные оценки, либо находятся в планах.")
                del user_clean_state[user_id]
                return
            
            movie_ids = [row.get('id') if isinstance(row, dict) else row[0] for row in movies_to_delete]
            movies_count = len(movie_ids)
            
            # Удаляем связанные данные
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = ANY(%s)', (chat_id, movie_ids))
            ratings_deleted = cursor.rowcount
            
            cursor.execute('DELETE FROM watched_movies WHERE chat_id = %s AND film_id = ANY(%s)', (chat_id, movie_ids))
            watched_deleted = cursor.rowcount
            
            # Удаляем сами фильмы
            cursor.execute('DELETE FROM movies WHERE chat_id = %s AND id = ANY(%s)', (chat_id, movie_ids))
            movies_deleted = cursor.rowcount
            
            conn.commit()
        
        bot.reply_to(message, 
            f"✅ Удалено фильмов, добавленных при импорте: {movies_deleted}\n"
            f"• Удалено оценок: {ratings_deleted}\n"
            f"• Удалено отметок просмотра: {watched_deleted}")
        del user_clean_state[user_id]
    
    elif target == 'chat':
        # Удаление всех данных чата (требует голосования в группах)
        with db_lock:
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s', (chat_id,))
            ratings_deleted = cursor.rowcount
            cursor.execute('DELETE FROM plans WHERE chat_id = %s', (chat_id,))
            plans_deleted = cursor.rowcount
            cursor.execute('DELETE FROM watched_movies WHERE chat_id = %s', (chat_id,))
            watched_deleted = cursor.rowcount
            cursor.execute('DELETE FROM movies WHERE chat_id = %s', (chat_id,))
            movies_deleted = cursor.rowcount
            cursor.execute('DELETE FROM stats WHERE chat_id = %s', (chat_id,))
            stats_deleted = cursor.rowcount
            cursor.execute('DELETE FROM settings WHERE chat_id = %s', (chat_id,))
            settings_deleted = cursor.rowcount
            conn.commit()
        
        bot.reply_to(message, 
            f"✅ База данных чата обнулена:\n"
            f"• Фильмов: {movies_deleted}\n"
            f"• Оценок: {ratings_deleted}\n"
            f"• Планов: {plans_deleted}\n"
            f"• Отметок просмотра: {watched_deleted}\n"
            f"• Статистики: {stats_deleted}\n"
            f"• Настроек: {settings_deleted}")
        del user_clean_state[user_id]
    
    elif target == 'unwatched_movies':
        # Удаление непросмотренных фильмов
        with db_lock:
            cursor.execute('''
                DELETE FROM movies 
                WHERE chat_id = %s 
                  AND watched = 0
                  AND id NOT IN (SELECT DISTINCT film_id FROM plans WHERE chat_id = %s AND film_id IS NOT NULL)
                  AND id NOT IN (SELECT DISTINCT film_id FROM watched_movies WHERE chat_id = %s AND film_id IS NOT NULL)
            ''', (chat_id, chat_id, chat_id))
            movies_deleted = cursor.rowcount
            conn.commit()
        
        bot.reply_to(message, f"✅ Удалено непросмотренных фильмов: {movies_deleted}")
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
    # ... другие elif если есть


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