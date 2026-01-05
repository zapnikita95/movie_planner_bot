"""
Callback handlers для работы с сериалами
"""
import logging
import json
from datetime import datetime as dt, timedelta
import pytz
import telebot

from moviebot.bot.bot_init import bot as bot_instance, scheduler
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.database.db_operations import get_watched_emojis, get_watched_custom_emoji_ids
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info, get_series_airing_status
from moviebot.utils.helpers import has_notifications_access
from moviebot.scheduler import send_series_notification, check_series_for_new_episodes
from moviebot.states import user_episodes_state
import sys
import os

# Импортируем show_film_info_with_buttons из старого файла (временно, пока не перенесена в новую структуру)
# Создаем обертку, которая использует правильные зависимости
def show_film_info_with_buttons_wrapper(chat_id, user_id, info, link, kp_id, existing=None, message_id=None, message_thread_id=None):
    """Обертка для show_film_info_with_buttons, которая использует правильные зависимости"""
    try:
        # Пытаемся импортировать функцию из старого файла
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        old_moviebot_path = os.path.join(project_root, 'moviebot.py')
        if os.path.exists(old_moviebot_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location("moviebot_module", old_moviebot_path)
            moviebot_module = importlib.util.module_from_spec(spec)
            
            # Устанавливаем правильные зависимости в модуль перед выполнением
            moviebot_module.bot = bot_instance
            moviebot_module.cursor = cursor
            moviebot_module.conn = conn
            moviebot_module.db_lock = db_lock
            moviebot_module.logger = logger
            
            # Импортируем необходимые функции
            from moviebot.api.kinopoisk_api import get_series_airing_status, get_seasons_data
            from moviebot.utils.helpers import has_notifications_access
            moviebot_module.get_series_airing_status = get_series_airing_status
            moviebot_module.get_seasons_data = get_seasons_data
            moviebot_module.has_notifications_access = has_notifications_access
            
            spec.loader.exec_module(moviebot_module)
            original_function = moviebot_module.show_film_info_with_buttons
            
            # Вызываем функцию с правильными зависимостями
            return original_function(chat_id, user_id, info, link, kp_id, existing, message_id, message_thread_id)
        else:
            raise ImportError("Файл moviebot.py не найден")
    except Exception as import_e:
        logger.error(f"[SERIES CALLBACKS] Ошибка импорта show_film_info_with_buttons: {import_e}", exc_info=True)
        # Fallback: обновляем только клавиатуру
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        
        # Проверяем статус подписки из БД
        is_subscribed = False
        if existing:
            film_id = existing[0] if isinstance(existing, tuple) else existing.get('id')
            if film_id:
                with db_lock:
                    cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                    sub_row = cursor.fetchone()
                    is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        
        markup = InlineKeyboardMarkup(row_width=1)
        if is_subscribed:
            markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
        else:
            markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
        
        if message_id:
            try:
                if message_thread_id:
                    bot_instance.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        reply_markup=markup
                    )
                else:
                    bot_instance.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=markup
                    )
            except Exception as e:
                logger.error(f"[SERIES CALLBACKS] Ошибка обновления сообщения: {e}")

# Создаем алиас для удобства
show_film_info_with_buttons = show_film_info_with_buttons_wrapper

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

# Импортируем show_film_info_with_buttons из старого файла (временно, пока не перенесена в новую структуру)
# Создаем обертку, которая использует правильные зависимости
def show_film_info_with_buttons_wrapper(chat_id, user_id, info, link, kp_id, existing=None, message_id=None, message_thread_id=None):
    """Обертка для show_film_info_with_buttons, которая использует правильные зависимости"""
    try:
        # Пытаемся импортировать функцию из старого файла
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
        old_moviebot_path = os.path.join(project_root, 'moviebot.py')
        if os.path.exists(old_moviebot_path):
            import importlib.util
            spec = importlib.util.spec_from_file_location("moviebot_module", old_moviebot_path)
            moviebot_module = importlib.util.module_from_spec(spec)
            
            # Устанавливаем правильные зависимости в модуль перед выполнением
            moviebot_module.bot = bot_instance
            moviebot_module.cursor = cursor
            moviebot_module.conn = conn
            moviebot_module.db_lock = db_lock
            moviebot_module.logger = logger
            
            # Импортируем необходимые функции
            from moviebot.api.kinopoisk_api import get_series_airing_status, get_seasons_data
            from moviebot.utils.helpers import has_notifications_access
            moviebot_module.get_series_airing_status = get_series_airing_status
            moviebot_module.get_seasons_data = get_seasons_data
            moviebot_module.has_notifications_access = has_notifications_access
            
            spec.loader.exec_module(moviebot_module)
            original_function = moviebot_module.show_film_info_with_buttons
            
            # Вызываем функцию с правильными зависимостями
            return original_function(chat_id, user_id, info, link, kp_id, existing, message_id, message_thread_id)
        else:
            raise ImportError("Файл moviebot.py не найден")
    except Exception as import_e:
        logger.error(f"[SERIES CALLBACKS] Ошибка импорта show_film_info_with_buttons: {import_e}", exc_info=True)
        # Fallback: обновляем только клавиатуру
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        
        # Проверяем статус подписки из БД
        is_subscribed = False
        if existing:
            film_id = existing[0] if isinstance(existing, tuple) else existing.get('id')
            if film_id:
                with db_lock:
                    cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                    sub_row = cursor.fetchone()
                    is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        
        markup = InlineKeyboardMarkup(row_width=1)
        if is_subscribed:
            markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
        else:
            markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
        
        if message_id:
            try:
                if message_thread_id:
                    bot_instance.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        reply_markup=markup
                    )
                else:
                    bot_instance.edit_message_reply_markup(
                        chat_id=chat_id,
                        message_id=message_id,
                        reply_markup=markup
                    )
            except Exception as e:
                logger.error(f"[SERIES CALLBACKS] Ошибка обновления сообщения: {e}")

# Создаем алиас для удобства
show_film_info_with_buttons = show_film_info_with_buttons_wrapper


def register_series_callbacks(bot_instance):
    """Регистрирует callback handlers для сериалов"""
    
    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_subscribe:"))
    def series_subscribe_callback(call):
        """Обработчик подписки на новые серии сериала"""
        logger.info(f"[SERIES SUBSCRIBE] ===== START: callback_id={call.id}, user_id={call.from_user.id}, chat_id={call.message.chat.id if call.message else None}")
        try:
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SERIES SUBSCRIBE] Парсинг данных: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")
            
            # Проверяем доступ к функциям уведомлений
            if not has_notifications_access(chat_id, user_id):
                logger.warning(f"[SERIES SUBSCRIBE] Нет доступа к уведомлениям для user_id={user_id}, chat_id={chat_id}")
                bot_instance.answer_callback_query(
                    call.id, 
                    "🔒 Функционал можно подключить через /payment", 
                    show_alert=True
                )
                return
            
            logger.info(f"[SERIES SUBSCRIBE] Получение film_id из БД для kp_id={kp_id}")
            with db_lock:
                # Получаем film_id
                cursor.execute("SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    logger.error(f"[SERIES SUBSCRIBE] Сериал не найден в БД: kp_id={kp_id}, chat_id={chat_id}")
                    bot_instance.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
                logger.info(f"[SERIES SUBSCRIBE] Найден сериал: film_id={film_id}, title={title}")
                
                # Проверяем, подписан ли уже
                cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                sub_row = cursor.fetchone()
                is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
                
                if is_subscribed:
                    logger.info(f"[SERIES SUBSCRIBE] Пользователь уже подписан: user_id={user_id}, film_id={film_id}")
                    bot_instance.answer_callback_query(call.id, "Вы уже подписан на этот сериал", show_alert=True)
                    return
                
                logger.info(f"[SERIES SUBSCRIBE] Добавление подписки в БД: user_id={user_id}, film_id={film_id}, kp_id={kp_id}")
                # Добавляем/обновляем подписку
                cursor.execute('''
                    INSERT INTO series_subscriptions (chat_id, film_id, kp_id, user_id, subscribed)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET subscribed = TRUE
                ''', (chat_id, film_id, kp_id, user_id))
                conn.commit()
                logger.info(f"[SERIES SUBSCRIBE] Подписка добавлена в БД успешно")
            
            # Получаем информацию о следующей серии и ставим уведомление
            logger.info(f"[SERIES SUBSCRIBE] Получение данных о сезонах для kp_id={kp_id}")
            seasons = None
            try:
                seasons = get_seasons_data(kp_id)
                logger.info(f"[SERIES SUBSCRIBE] Получено сезонов: {len(seasons) if seasons else 0}")
            except Exception as seasons_e:
                logger.error(f"[SERIES SUBSCRIBE] Ошибка при получении данных о сезонах: {seasons_e}", exc_info=True)
                seasons = None
            
            next_episode_date = None
            next_episode = None
            if seasons:
                now = dt.now()
                
                for season in seasons:
                    episodes = season.get('episodes', [])
                    for ep in episodes:
                        release_str = ep.get('releaseDate', '')
                        if release_str and release_str != '—':
                            try:
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
                            except:
                                pass
            
            if next_episode_date and next_episode:
                # Ставим уведомление на дату выхода следующей серии
                # Получаем часовой пояс пользователя
                user_tz = pytz.timezone('Europe/Moscow')  # По умолчанию
                try:
                    with db_lock:
                        cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                        tz_row = cursor.fetchone()
                        if tz_row:
                            tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                            user_tz = pytz.timezone(tz_str)
                except:
                    pass
                
                # Уведомление за день до выхода
                notification_time = next_episode_date - timedelta(days=1)
                notification_time = user_tz.localize(notification_time.replace(hour=10, minute=0))
                
                logger.info(f"[SERIES SUBSCRIBE] Постановка уведомления на {notification_time}")
                try:
                    if scheduler:
                        scheduler.add_job(
                            send_series_notification,
                            'date',
                            run_date=notification_time.astimezone(pytz.utc),
                            args=[chat_id, film_id, kp_id, title, next_episode['season'], next_episode['episode']],
                            id=f'series_notification_{chat_id}_{film_id}_{user_id}_{next_episode_date.strftime("%Y%m%d")}'
                        )
                        logger.info(f"[SERIES SUBSCRIBE] Уведомление поставлено успешно")
                except Exception as scheduler_e:
                    logger.error(f"[SERIES SUBSCRIBE] Ошибка при постановке уведомления: {scheduler_e}", exc_info=True)
            else:
                # Нет ближайшей даты - ставим периодическую проверку (через 3 недели)
                logger.info(f"[SERIES SUBSCRIBE] Нет ближайшей даты выхода, ставим проверку через 3 недели")
                check_time = dt.now(pytz.utc) + timedelta(weeks=3)
                logger.info(f"[SERIES SUBSCRIBE] Постановка задачи проверки на {check_time}")
                try:
                    if scheduler:
                        scheduler.add_job(
                            check_series_for_new_episodes,
                            'date',
                            run_date=check_time,
                            args=[chat_id, film_id, kp_id, user_id],
                            id=f'series_check_{chat_id}_{film_id}_{user_id}_{int(check_time.timestamp())}'
                        )
                        logger.info(f"[SERIES SUBSCRIBE] Задача проверки поставлена успешно")
                except Exception as scheduler_e:
                    logger.error(f"[SERIES SUBSCRIBE] Ошибка при постановке задачи проверки: {scheduler_e}", exc_info=True)
            
            logger.info(f"[SERIES SUBSCRIBE] Пользователь {user_id} подписался на сериал {title} (kp_id={kp_id})")
            
            # Обновляем сообщение с обновленной кнопкой
            logger.info(f"[SERIES SUBSCRIBE] Обновление сообщения с описанием сериала")
            try:
                # Получаем информацию о сериале из базы
                with db_lock:
                    cursor.execute("SELECT id, title, link, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                    row = cursor.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        link = row.get('link') if isinstance(row, dict) else row[2]
                        watched = row.get('watched') if isinstance(row, dict) else row[3]
                        
                        logger.info(f"[SERIES SUBSCRIBE] Получение информации о сериале через API: link={link}")
                        # Получаем информацию о сериале через API с таймаутом
                        info = None
                        try:
                            import threading
                            
                            result = [None]
                            exception = [None]
                            
                            def call_extract():
                                try:
                                    result[0] = extract_movie_info(link)
                                except Exception as e:
                                    exception[0] = e
                            
                            thread = threading.Thread(target=call_extract)
                            thread.daemon = True
                            thread.start()
                            thread.join(timeout=10)  # Таймаут 10 секунд
                            
                            if thread.is_alive():
                                logger.error(f"[SERIES SUBSCRIBE] Таймаут при получении информации о сериале через API (превышен лимит 10 секунд)")
                                info = None
                            elif exception[0]:
                                raise exception[0]
                            else:
                                info = result[0]
                                if info:
                                    logger.info(f"[SERIES SUBSCRIBE] Информация о сериале получена успешно")
                                else:
                                    logger.warning(f"[SERIES SUBSCRIBE] extract_movie_info вернул None")
                        except Exception as api_e:
                            logger.error(f"[SERIES SUBSCRIBE] Ошибка API при получении информации о сериале: {api_e}", exc_info=True)
                            info = None
                        
                        if info:
                            existing = (film_id, title, watched)
                            # Получаем message_thread_id из сообщения, если оно есть
                            message_thread_id = None
                            message_id = None
                            if call.message:
                                message_id = call.message.message_id
                                if hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                                    message_thread_id = call.message.message_thread_id
                            
                            logger.info(f"[SERIES SUBSCRIBE] Вызываю show_film_info_with_buttons: message_id={message_id}, message_thread_id={message_thread_id}")
                            # Обновляем существующее сообщение с обновленной кнопкой
                            try:
                                # Пытаемся использовать функцию из старого файла
                                if 'show_film_info_with_buttons' in globals() and callable(show_film_info_with_buttons):
                                    show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing, message_id=message_id, message_thread_id=message_thread_id)
                                    logger.info(f"[SERIES SUBSCRIBE] Сообщение обновлено успешно через show_film_info_with_buttons")
                                else:
                                    # Если функция не доступна, обновляем только клавиатуру
                                    logger.warning(f"[SERIES SUBSCRIBE] show_film_info_with_buttons не доступна, обновляю только клавиатуру")
                                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                                    new_markup = InlineKeyboardMarkup(row_width=1)
                                    new_markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
                                    
                                    if message_thread_id:
                                        bot_instance.edit_message_reply_markup(
                                            chat_id=chat_id,
                                            message_id=message_id,
                                            message_thread_id=message_thread_id,
                                            reply_markup=new_markup
                                        )
                                    else:
                                        bot_instance.edit_message_reply_markup(
                                            chat_id=chat_id,
                                            message_id=message_id,
                                            reply_markup=new_markup
                                        )
                                    logger.info(f"[SERIES SUBSCRIBE] Клавиатура обновлена успешно")
                            except telebot.apihelper.ApiTelegramException as api_e:
                                error_str = str(api_e).lower()
                                logger.error(f"[SERIES SUBSCRIBE] Telegram API ошибка при обновлении сообщения: {api_e}", exc_info=True)
                                
                                # Если ошибка "message is not modified", пробуем обновить только клавиатуру
                                if "message is not modified" in error_str or "message_not_modified" in error_str:
                                    logger.info(f"[SERIES SUBSCRIBE] Telegram: 'message is not modified' — пробую только markup")
                                    try:
                                        # Получаем текущий текст и обновляем только клавиатуру
                                        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                                        new_markup = InlineKeyboardMarkup(row_width=1)
                                        new_markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
                                        
                                        if message_thread_id:
                                            bot_instance.edit_message_reply_markup(
                                                chat_id=chat_id,
                                                message_id=message_id,
                                                message_thread_id=message_thread_id,
                                                reply_markup=new_markup
                                            )
                                        else:
                                            bot_instance.edit_message_reply_markup(
                                                chat_id=chat_id,
                                                message_id=message_id,
                                                reply_markup=new_markup
                                            )
                                        logger.info(f"[SERIES SUBSCRIBE] Клавиатура обновлена успешно")
                                    except Exception as markup_e:
                                        logger.error(f"[SERIES SUBSCRIBE] Ошибка обновления клавиатуры: {markup_e}", exc_info=True)
                                        # Отправляем новое сообщение как fallback
                                        bot_instance.send_message(chat_id, f"✅ Вы подписались на уведомления о новых сериях для {title}")
                                else:
                                    # Другая ошибка - отправляем новое сообщение
                                    logger.warning(f"[SERIES SUBSCRIBE] Отправляю новое сообщение из-за ошибки API")
                                    bot_instance.send_message(chat_id, f"✅ Вы подписались на уведомления о новых сериях для {title}")
                            except Exception as update_e:
                                logger.error(f"[SERIES SUBSCRIBE] Ошибка при обновлении сообщения через show_film_info_with_buttons: {update_e}", exc_info=True)
                                # Отправляем новое сообщение как fallback
                                bot_instance.send_message(chat_id, f"✅ Вы подписались на уведомления о новых сериях для {title}")
                        else:
                            logger.warning(f"[SERIES SUBSCRIBE] Не удалось получить информацию о сериале через API для kp_id={kp_id}")
                            # Даже если не удалось получить info, обновляем клавиатуру
                            if call.message:
                                message_id = call.message.message_id
                                message_thread_id = None
                                if hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                                    message_thread_id = call.message.message_thread_id
                                
                                try:
                                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                                    new_markup = InlineKeyboardMarkup(row_width=1)
                                    new_markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
                                    
                                    if message_thread_id:
                                        bot_instance.edit_message_reply_markup(
                                            chat_id=chat_id,
                                            message_id=message_id,
                                            message_thread_id=message_thread_id,
                                            reply_markup=new_markup
                                        )
                                    else:
                                        bot_instance.edit_message_reply_markup(
                                            chat_id=chat_id,
                                            message_id=message_id,
                                            reply_markup=new_markup
                                        )
                                    logger.info(f"[SERIES SUBSCRIBE] Клавиатура обновлена успешно (без info)")
                                except Exception as markup_e:
                                    logger.error(f"[SERIES SUBSCRIBE] Ошибка обновления клавиатуры без info: {markup_e}", exc_info=True)
            except Exception as e:
                logger.error(f"[SERIES SUBSCRIBE] Ошибка при обновлении сообщения: {e}", exc_info=True)
                try:
                    bot_instance.send_message(chat_id, "✅ Вы подписались на уведомления о новых сериях.\n(Не удалось обновить карточку — попробуйте открыть заново)")
                    logger.info(f"[SERIES SUBSCRIBE] Отправлено fallback сообщение")
                except Exception as send_e:
                    logger.error(f"[SERIES SUBSCRIBE] Ошибка отправки fallback сообщения: {send_e}", exc_info=True)
        except Exception as e:
            logger.error(f"[SERIES SUBSCRIBE] КРИТИЧЕСКАЯ ОШИБКА в хэндлере: {e}", exc_info=True)
            try:
                bot_instance.send_message(chat_id, "✅ Подписка добавлена, но произошла ошибка при обновлении карточки.")
            except Exception as send_e:
                logger.error(f"[SERIES SUBSCRIBE] Не удалось отправить сообщение об ошибке: {send_e}", exc_info=True)
        finally:
            # ВСЕГДА отвечаем на callback!
            try:
                bot_instance.answer_callback_query(call.id, text="✅ Подписка оформлена!")
            except Exception as answer_e:
                logger.error(f"[SERIES SUBSCRIBE] Не удалось ответить на callback: {answer_e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_unsubscribe:"))
    def series_unsubscribe_callback(call):
        """Обработчик отписки от новых серий сериала"""
        logger.info(f"[SERIES UNSUBSCRIBE] ===== START: callback_id={call.id}, user_id={call.from_user.id}, chat_id={call.message.chat.id if call.message else None}")
        try:
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SERIES UNSUBSCRIBE] Парсинг данных: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")
            
            # Проверяем доступ к функциям уведомлений
            if not has_notifications_access(chat_id, user_id):
                logger.warning(f"[SERIES UNSUBSCRIBE] Нет доступа к уведомлениям для user_id={user_id}, chat_id={chat_id}")
                bot_instance.answer_callback_query(
                    call.id, 
                    "🔒 Функционал можно подключить через /payment", 
                    show_alert=True
                )
                return
            
            logger.info(f"[SERIES UNSUBSCRIBE] Получение film_id из БД для kp_id={kp_id}")
            with db_lock:
                # Получаем film_id
                cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    logger.error(f"[SERIES UNSUBSCRIBE] Сериал не найден в БД: kp_id={kp_id}, chat_id={chat_id}")
                    bot_instance.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                logger.info(f"[SERIES UNSUBSCRIBE] Найден сериал: film_id={film_id}")
                
                # Отписываемся
                logger.info(f"[SERIES UNSUBSCRIBE] Отписка от сериала: user_id={user_id}, film_id={film_id}")
                cursor.execute('''
                    UPDATE series_subscriptions 
                    SET subscribed = FALSE 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                conn.commit()
                logger.info(f"[SERIES UNSUBSCRIBE] Отписка выполнена в БД")
            
            logger.info(f"[SERIES UNSUBSCRIBE] Пользователь {user_id} отписался от сериала (kp_id={kp_id})")
            # TODO: Обновить сообщение с описанием сериала (как в series_subscribe)
            bot_instance.send_message(chat_id, "🔕 Вы отписались от уведомлений о новых сериях")
        except Exception as e:
            logger.error(f"[SERIES UNSUBSCRIBE] КРИТИЧЕСКАЯ ОШИБКА в хэндлере: {e}", exc_info=True)
            try:
                bot_instance.send_message(chat_id, "🔕 Отписка выполнена, но произошла ошибка при обновлении карточки.")
            except Exception as send_e:
                logger.error(f"[SERIES UNSUBSCRIBE] Не удалось отправить сообщение об ошибке: {send_e}", exc_info=True)
        finally:
            # ВСЕГДА отвечаем на callback!
            try:
                bot_instance.answer_callback_query(call.id, text="✅ Отписка выполнена")
            except Exception as answer_e:
                logger.error(f"[SERIES UNSUBSCRIBE] Не удалось ответить на callback: {answer_e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_locked:"))
    def series_locked_callback(call):
        """Обработчик для заблокированных функций сериалов (нет доступа)"""
        try:
            bot_instance.answer_callback_query(
                call.id,
                "🔒 Функционал можно подключить через /payment",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[SERIES LOCKED] Ошибка: {e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_episode_toggle:") or call.data.startswith("series_episode:"))
    def handle_episode_toggle(call):
        """Обработчик переключения статуса просмотра эпизода"""
        # TODO: Извлечь из moviebot.py строки 7800-7900
        # Или из handlers/seasons.py если уже реализовано
        try:
            bot_instance.answer_callback_query(call.id)
            # Формат: series_episode:{kp_id}:{season_num}:{ep_num}
            parts = call.data.split(":")
            if len(parts) < 4:
                logger.error(f"[EPISODE TOGGLE] Неверный формат callback_data: {call.data}")
                return
            
            kp_id = parts[1]
            season_num = parts[2]
            ep_num = parts[3]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[EPISODE TOGGLE] Переключение эпизода: kp_id={kp_id}, season={season_num}, episode={ep_num}, user_id={user_id}")
            
            # Получаем film_id
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    bot_instance.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                
                # Проверяем текущий статус
                cursor.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor.fetchone()
                is_watched = False
                if watched_row:
                    is_watched = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                
                # Переключаем статус
                if is_watched:
                    # Убираем отметку
                    cursor.execute('''
                        DELETE FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND season_number = %s AND episode_number = %s
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                else:
                    # Добавляем отметку
                    cursor.execute('''
                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                        VALUES (%s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                        DO UPDATE SET watched = TRUE
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                
                conn.commit()
            
            # Обновляем страницу эпизодов
            from moviebot.bot.handlers.seasons import show_episodes_page
            message_id = call.message.message_id if call.message else None
            message_thread_id = None
            if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                message_thread_id = call.message.message_thread_id
            
            # Получаем текущую страницу из состояния
            current_page = 1
            if user_id in user_episodes_state:
                state = user_episodes_state[user_id]
                if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                    current_page = state.get('page', 1)
            
            show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
        except Exception as e:
            logger.error(f"[EPISODE TOGGLE] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_season_all:"))
    def handle_season_all_toggle(call):
        """Обработчик отметки всех эпизодов сезона как просмотренных"""
        # TODO: Извлечь из moviebot.py строки 7900-8000
        try:
            bot_instance.answer_callback_query(call.id)
            parts = call.data.split(":")
            if len(parts) < 3:
                return
            
            kp_id = parts[1]
            season_num = parts[2]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SEASON ALL] Отметка всех эпизодов сезона: kp_id={kp_id}, season={season_num}, user_id={user_id}")
            
            # Получаем film_id и эпизоды сезона
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    bot_instance.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                
                # Получаем эпизоды сезона
                seasons_data = get_seasons_data(kp_id)
                if not seasons_data:
                    bot_instance.answer_callback_query(call.id, "❌ Не удалось получить данные о сезонах", show_alert=True)
                    return
                
                season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
                if not season:
                    bot_instance.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
                    return
                
                episodes = season.get('episodes', [])
                
                # Отмечаем все эпизоды как просмотренные
                for ep in episodes:
                    ep_num = str(ep.get('episodeNumber', ''))
                    cursor.execute('''
                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                        VALUES (%s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                        DO UPDATE SET watched = TRUE
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                
                conn.commit()
            
            # Обновляем страницу эпизодов
            from moviebot.bot.handlers.seasons import show_episodes_page
            message_id = call.message.message_id if call.message else None
            message_thread_id = None
            if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                message_thread_id = call.message.message_thread_id
            
            current_page = 1
            if user_id in user_episodes_state:
                state = user_episodes_state[user_id]
                if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                    current_page = state.get('page', 1)
            
            show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
        except Exception as e:
            logger.error(f"[SEASON ALL] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("episodes_page:"))
    def handle_episodes_page_navigation(call):
        """Обработчик навигации по страницам эпизодов"""
        # TODO: Извлечь из moviebot.py или handlers/seasons.py
        try:
            bot_instance.answer_callback_query(call.id)
            parts = call.data.split(":")
            if len(parts) < 4:
                return
            
            kp_id = parts[1]
            season_num = parts[2]
            page = int(parts[3])
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            from moviebot.bot.handlers.seasons import show_episodes_page
            message_id = call.message.message_id if call.message else None
            message_thread_id = None
            if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                message_thread_id = call.message.message_thread_id
            
            show_episodes_page(kp_id, season_num, chat_id, user_id, page=page, message_id=message_id, message_thread_id=message_thread_id)
        except Exception as e:
            logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("episodes_back_to_seasons:"))
    def handle_episodes_back_to_seasons(call):
        """Обработчик возврата к списку сезонов из эпизодов"""
        # TODO: Извлечь из moviebot.py или handlers/seasons.py
        try:
            bot_instance.answer_callback_query(call.id)
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            # TODO: Вызвать функцию показа сезонов из handlers/seasons.py
            logger.info(f"[EPISODES BACK] Возврат к сезонам для kp_id={kp_id}")
        except Exception as e:
            logger.error(f"[EPISODES BACK] Ошибка: {e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("episodes_back_to_watched_list:") or call.data.startswith("episodes_back_to_series_list:"))
    def handle_episodes_back_to_list(call):
        """Обработчик возврата к списку сериалов из эпизодов"""
        # TODO: Извлечь из moviebot.py или handlers/seasons.py
        try:
            bot_instance.answer_callback_query(call.id)
            # TODO: Вызвать функцию показа списка сериалов
            logger.info(f"[EPISODES BACK] Возврат к списку сериалов")
        except Exception as e:
            logger.error(f"[EPISODES BACK] Ошибка: {e}", exc_info=True)

