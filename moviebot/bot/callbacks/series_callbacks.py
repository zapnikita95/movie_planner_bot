from moviebot.bot.bot_init import bot
"""
Callback handlers для работы с сериалами
"""
import logging
import json
import re
import math
from datetime import datetime, timedelta

import pytz
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.scheduler import scheduler
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.handlers.series import ensure_movie_in_database
from moviebot.database.db_operations import get_watched_emojis, get_watched_custom_emoji_ids

from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info

from moviebot.utils.helpers import has_notifications_access

from moviebot.scheduler import send_series_notification, check_series_for_new_episodes

from moviebot.states import user_episodes_state, rating_messages, user_plan_state, user_episode_auto_mark_state

from moviebot.api.kinopoisk_api import get_facts

# show_film_info_with_buttons больше не используется - обновляем только кнопку подписки без API запросов

logger = logging.getLogger(__name__)

# Константа для пагинации сезонов
SEASONS_PER_PAGE = 10

def register_series_callbacks(bot):
    """Регистрирует callback handlers для сериалов"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER SERIES CALLBACKS] ===== START: регистрация обработчиков сериалов =====")
    logger.info(f"[REGISTER SERIES CALLBACKS] bot: {bot}, id(bot): {id(bot)}")
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_track:"))
    def series_track_callback(call):
        """Обработчик для отметки сезонов/серий как просмотренных"""
        try:
            # Пытаемся сразу ответить на callback (убираем "часики")
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                logger.warning(f"[SERIES TRACK] Не удалось ответить на callback query (возможно, истек): {e}")

            # ── Безопасный парсинг kp_id ─────────────────────────────────────────────
            parts = call.data.split(":")
            if len(parts) < 2:
                logger.error(f"[SERIES TRACK] Некорректный callback_data (нет kp_id): {call.data}")
                bot.answer_callback_query(call.id, "Ошибка формата кнопки", show_alert=True)
                return

            kp_id_raw = parts[1].strip()
            try:
                kp_id = str(int(kp_id_raw))  # приводим к чистой строке-числу
            except ValueError:
                logger.error(f"[SERIES TRACK] kp_id не является числом: '{kp_id_raw}' в {call.data}")
                bot.answer_callback_query(call.id, "Неверный ID сериала", show_alert=True)
                return

            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id

            logger.info(f"[SERIES TRACK] Начало: user_id={user_id}, chat_id={chat_id}, kp_id={kp_id}")
            
            # Очищаем состояние автоотметки при переходе к списку сезонов
            # чтобы не мешать двойному клику при возврате к другому эпизоду
            if user_id in user_episode_auto_mark_state:
                auto_state = user_episode_auto_mark_state[user_id]
                if str(auto_state.get('kp_id')) == str(kp_id):
                    logger.info(f"[SERIES TRACK] Очищаем состояние автоотметки при переходе к списку сезонов: user_id={user_id}, kp_id={kp_id}")
                    del user_episode_auto_mark_state[user_id]

            # Проверяем доступ к функциям уведомлений
            if not has_notifications_access(chat_id, user_id):
                logger.warning(f"[SERIES TRACK] Нет доступа: user_id={user_id}, chat_id={chat_id}")
                bot.answer_callback_query(
                    call.id,
                    "🔒 Функционал можно подключить через /payment",
                    show_alert=True
                )
                return

            # Получаем film_id (добавляем в базу, если нет)
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            info = extract_movie_info(link)
            if not info:
                logger.error(f"[SERIES TRACK] Не удалось получить информацию о сериале для kp_id={kp_id}")
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                return

            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            if not film_id:
                logger.error(f"[SERIES TRACK] Не удалось добавить сериал в базу для kp_id={kp_id}")
                bot.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                return

            title = info.get('title', 'Сериал')

            if was_inserted:
                bot.send_message(chat_id, f"✅ Сериал добавлен в базу!")
                logger.info(f"[SERIES TRACK] Сериал добавлен в базу: film_id={film_id}, title={title}")

            # Проверяем, отмечен ли сериал как просмотренный
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT watched FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    watched_row = cursor_local.fetchone()
                    is_series_watched = False
                    if watched_row:
                        is_series_watched = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                
                # Если сериал отмечен как просмотренный, но не все серии отмечены - отмечаем все серии
                if is_series_watched:
                    # Получаем сезоны из API
                    seasons_data = get_seasons_data(kp_id)
                    if seasons_data:
                        # Получаем все сезоны и эпизоды
                        all_seasons_sorted = sorted(seasons_data, key=lambda s: int(s.get('number', 0)) if str(s.get('number', '')).isdigit() else 0)
                        
                        # Отмечаем все серии как просмотренные
                        with db_lock:
                            for season in all_seasons_sorted:
                                season_num = season.get('number', '')
                                episodes = season.get('episodes', [])
                                for ep in episodes:
                                    # ВАЖНО: Всегда приводим к строке для единообразия
                                    ep_num = str(ep.get('episodeNumber', ''))
                                    # Проверяем, не отмечена ли уже эта серия
                                    cursor_local.execute('''
                                        SELECT watched FROM series_tracking 
                                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                        AND season_number = %s AND episode_number = %s
                                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                                    existing = cursor_local.fetchone()
                                    if not existing or not (existing.get('watched') if isinstance(existing, dict) else existing[0]):
                                        # Отмечаем серию как просмотренную
                                        cursor_local.execute('''
                                            INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                                            VALUES (%s, %s, %s, %s, %s, TRUE)
                                            ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                                            DO UPDATE SET watched = TRUE
                                        ''', (chat_id, film_id, user_id, season_num, ep_num))
                            conn_local.commit()
                            logger.info(f"[SERIES TRACK] Все серии отмечены как просмотренные для film_id={film_id}, user_id={user_id}")
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass

            # Получаем сезоны из API
            seasons_data = get_seasons_data(kp_id)
            if not seasons_data:
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сезонах", show_alert=True)
                return

            # Показываем первую страницу сезонов через вспомогательную функцию
            show_seasons_page(chat_id, user_id, kp_id, film_id, title, seasons_data, page=1, message_id=message_id, call=call)
        except Exception as e:
            logger.error(f"[SERIES TRACK] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    def show_seasons_page(chat_id, user_id, kp_id, film_id, title, seasons_data, page=1, message_id=None, call=None):
        """Показывает страницу сезонов с пагинацией"""
        try:
            message_thread_id = getattr(call.message, 'message_thread_id', None) if call else None
            
            # Фильтруем только вышедшие сезоны
            now = datetime.now()
            released_seasons = []
            for season in seasons_data:
                season_num = season.get('number', '')
                episodes = season.get('episodes', [])
                
                season_released = True
                if episodes:
                    for ep in episodes:
                        release_str = ep.get('releaseDate', '')
                        if release_str and release_str != '—':
                            try:
                                release_date = None
                                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                                    try:
                                        release_date = datetime.strptime(release_str.split('T')[0], fmt)
                                        break
                                    except:
                                        continue
                                if release_date and release_date > now:
                                    season_released = False
                                    break
                            except:
                                pass
                
                if season_released:
                    released_seasons.append(season)
            
            # Пагинация
            total_seasons = len(released_seasons)
            total_pages = (total_seasons + SEASONS_PER_PAGE - 1) // SEASONS_PER_PAGE if total_seasons > 0 else 1
            page = max(1, min(page, total_pages))
            
            start_idx = (page - 1) * SEASONS_PER_PAGE
            end_idx = start_idx + SEASONS_PER_PAGE
            seasons_page = released_seasons[start_idx:end_idx]
            
            # Используем локальные соединение и курсор
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            try:
                markup = InlineKeyboardMarkup(row_width=1)
                
                # Добавляем кнопки сезонов для текущей страницы
                for season in seasons_page:
                    season_num = season.get('number', '')
                    episodes = season.get('episodes', [])
                    episodes_count = len(episodes)
                    
                    watched_count = 0
                    with db_lock:
                        for ep in episodes:
                            # ВАЖНО: Всегда приводим к строке для единообразия
                            ep_num = str(ep.get('episodeNumber', ''))
                            cursor_local.execute('''
                                SELECT watched FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                AND season_number = %s AND episode_number = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id, season_num, ep_num))
                            watched_row = cursor_local.fetchone()
                            if watched_row:
                                watched_count += 1
                    
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
                
                # Проверяем, все ли сезоны просмотрены (один раз после цикла)
                all_seasons_watched = True
                for season in released_seasons:
                    season_num = season.get('number', '')
                    episodes = season.get('episodes', [])
                    episodes_count = len(episodes)
                    
                    watched_count = 0
                    with db_lock:
                        for ep in episodes:
                            # ВАЖНО: Всегда приводим к строке для единообразия
                            ep_num = str(ep.get('episodeNumber', ''))
                            cursor_local.execute('''
                                SELECT watched FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                                AND season_number = %s AND episode_number = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id, season_num, ep_num))
                            watched_row = cursor_local.fetchone()
                            if watched_row:
                                watched_count += 1
                    
                    if watched_count < episodes_count or episodes_count == 0:
                        all_seasons_watched = False
                        break
                
                # Если все сезоны просмотрены, отмечаем сериал как просмотренный в БД
                if all_seasons_watched:
                    with db_lock:
                        try:
                            cursor_local.execute("UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                            conn_local.commit()
                        except Exception as update_e:
                            logger.error(f"[SERIES TRACK] Ошибка обновления watched: {update_e}", exc_info=True)
                            try:
                                conn_local.rollback()
                            except:
                                pass
                
                # Добавляем кнопку "Оценить" если все сезоны просмотрены
                if all_seasons_watched:
                    with db_lock:
                        try:
                            # Получаем среднюю оценку
                            cursor_local.execute('''
                                SELECT AVG(rating) as avg FROM ratings 
                                WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                            ''', (chat_id, film_id))
                            avg_result = cursor_local.fetchone()
                            avg_rating = None
                            if avg_result:
                                avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                                avg_rating = float(avg) if avg is not None else None
                            
                            # Получаем активных пользователей
                            cursor_local.execute('''
                                SELECT DISTINCT user_id
                                FROM stats
                                WHERE chat_id = %s AND user_id IS NOT NULL
                            ''', (chat_id,))
                            active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor_local.fetchall()}
                            
                            # Получаем всех, кто оценил этот фильм
                            cursor_local.execute('''
                                SELECT DISTINCT user_id FROM ratings
                                WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                            ''', (chat_id, film_id))
                            rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor_local.fetchall()}
                        except Exception as rating_e:
                            logger.error(f"[SERIES TRACK] Ошибка получения информации об оценках: {rating_e}", exc_info=True)
                            active_users = set()
                            rated_users = set()
                            avg_rating = None
                        
                        # Определяем текст и эмодзи кнопки
                        if active_users and active_users.issubset(rated_users) and avg_rating is not None:
                            rating_int = int(round(avg_rating))
                            if 1 <= rating_int <= 4:
                                emoji = "💩"
                            elif 5 <= rating_int <= 7:
                                emoji = "💬"
                            else:
                                emoji = "🏆"
                            rating_text = f"{emoji} {avg_rating:.0f}/10"
                        else:
                            rating_text = "💬 Оценить"
                    
                    markup.add(InlineKeyboardButton(rating_text, callback_data=f"rate_film:{int(kp_id)}"))
                
                # Пагинация
                if total_pages > 1:
                    nav_buttons = []
                    if page > 1:
                        nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"series_track_seasons_page:{kp_id}:{page-1}"))
                    if page < total_pages:
                        nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"series_track_seasons_page:{kp_id}:{page+1}"))
                    if nav_buttons:
                        markup.row(*nav_buttons)
                
                # Кнопка "К сериалам" - возвращает к списку всех сериалов
                markup.add(InlineKeyboardButton("◀️ К сериалам", callback_data="back_to_seasons_list"))
                
                # Формируем текст сообщения
                text_msg = f"📺 <b>{title}</b>\n\n<b>Выберите сезон:</b>"
                if total_pages > 1:
                    text_msg += f"\n<i>Страница {page}/{total_pages}</i>"
                
                # Отправляем/обновляем сообщение (только один раз!)
                send_kwargs = {
                    'chat_id': chat_id,
                    'text': text_msg,
                    'reply_markup': markup,
                    'parse_mode': 'HTML'
                }
                if message_thread_id is not None:
                    send_kwargs['message_thread_id'] = message_thread_id
                
                logger.info(f"[SERIES TRACK] Обновление сообщения: message_id={message_id}, message_thread_id={message_thread_id}, page={page}/{total_pages}")
                try:
                    if message_id:
                        edit_kwargs = {
                            'chat_id': chat_id,
                            'message_id': message_id,
                            'text': text_msg,
                            'reply_markup': markup,
                            'parse_mode': 'HTML'
                        }
                        bot.edit_message_text(**edit_kwargs)
                        logger.info(f"[SERIES TRACK] Сообщение обновлено успешно")
                    else:
                        bot.send_message(**send_kwargs)
                        logger.info(f"[SERIES TRACK] Сообщение отправлено успешно")
                except Exception as e:
                    logger.error(f"[SERIES TRACK] Ошибка обновления: {e}")
                    # Fallback - новое сообщение
                    try:
                        bot.send_message(**send_kwargs)
                        logger.info(f"[SERIES TRACK] Отправлено новое сообщение как fallback")
                    except Exception as send_e:
                        logger.error(f"[SERIES TRACK] Фейл отправки: {send_e}", exc_info=True)
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
            logger.error(f"[SHOW SEASONS PAGE] Ошибка: {e}", exc_info=True)
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_track_seasons_page:"))
    def series_track_seasons_page_callback(call):
        """Обработчик пагинации сезонов"""
        try:
            bot.answer_callback_query(call.id)
            
            parts = call.data.split(":")
            kp_id = parts[1]
            page = int(parts[2])
            
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            
            # Получаем film_id и title
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    row = cursor_local.fetchone()
                    if not row:
                        bot.answer_callback_query(call.id, "❌ Сериал не найден", show_alert=True)
                        return
                    
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title = row.get('title') if isinstance(row, dict) else row[1]
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            # Получаем сезоны из API
            seasons_data = get_seasons_data(kp_id)
            if not seasons_data:
                bot.answer_callback_query(call.id, "❌ Не удалось получить сезоны", show_alert=True)
                return
            
            # Показываем нужную страницу
            show_seasons_page(chat_id, user_id, kp_id, film_id, title, seasons_data, page=page, message_id=message_id, call=call)
        except Exception as e:
            logger.error(f"[SERIES TRACK SEASONS PAGE] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_season:"))
    def series_season_callback(call):
        """Обработчик для выбора сезона и отметки эпизодов"""
        try:
            bot.answer_callback_query(call.id)
            
            parts = call.data.split(":")
            kp_id = parts[1]
            season_num = parts[2]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SERIES SEASON] Выбор сезона: user_id={user_id}, chat_id={chat_id}, kp_id={kp_id}, season={season_num}")
            message_id = call.message.message_id
            
            # Получаем thread_id из сообщения, если оно есть
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            
            # Используем функцию show_episodes_page для отображения эпизодов
            from moviebot.bot.handlers.seasons import show_episodes_page
            if show_episodes_page(kp_id, season_num, chat_id, user_id, page=1, message_id=message_id, message_thread_id=message_thread_id):
                bot.answer_callback_query(call.id)
            else:
                bot.answer_callback_query(call.id, "❌ Ошибка загрузки эпизодов", show_alert=True)
        except Exception as e:
            logger.error(f"[SERIES SEASON] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
@bot.callback_query_handler(func=lambda call: call.data.startswith("series_subscribe:"))
def series_subscribe_callback(call):
    """Обработчик подписки на новые серии сериала"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    # Сразу отвечаем на callback, чтобы убрать "крутилку"
    try:
        bot.answer_callback_query(call.id, text="⏳ Обрабатываю...")
        logger.info(f"[SERIES SUBSCRIBE] answer_callback_query вызван сразу, callback_id={call.id}")
    except Exception as e:
        logger.warning(f"[SERIES SUBSCRIBE] Не удалось вызвать answer_callback_query сразу: {e}")
    
    try:
        logger.info(f"[SERIES SUBSCRIBE] ===== START: callback_id={call.id}, user_id={user_id}, chat_id={chat_id}")
        
        data = call.data.split(':')
        kp_id = data[1]
        logger.info(f"[SERIES SUBSCRIBE] Парсинг данных: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")
        
        # Проверяем доступ к функциям уведомлений
        if not has_notifications_access(chat_id, user_id):
            logger.warning(f"[SERIES SUBSCRIBE] Нет доступа к уведомлениям для user_id={user_id}, chat_id={chat_id}")
            bot.answer_callback_query(
                call.id, 
                "🔒 Функционал можно подключить через /payment", 
                show_alert=True
            )
            return
        
        # Используем локальные соединение и курсор
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        # Получение film_id и title из БД (добавляем в базу, если нет)
        with db_lock:
            try:
                cursor_local.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                row = cursor_local.fetchone()
            except Exception as db_e:
                logger.error(f"[SERIES SUBSCRIBE] Ошибка запроса film_id: {db_e}", exc_info=True)
                row = None
                
            if row:
                film_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None) if isinstance(row, tuple) else row.get('id')
                title = row[1] if isinstance(row, tuple) else row.get('title')
                logger.info(f"[SERIES SUBSCRIBE] Найден сериал: film_id={film_id}, title={title}")
            else:
                # Сериал не в базе - добавляем через API
                logger.info(f"[SERIES SUBSCRIBE] Сериал не найден в БД, добавляем через API")
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                
                logger.info(f"[SERIES SUBSCRIBE] Вызываю extract_movie_info для kp_id={kp_id}, link={link}")
                
                try:
                    movie_data = extract_movie_info(link)
                    logger.info(f"[SERIES SUBSCRIBE] extract_movie_info завершен, title={movie_data.get('title', 'N/A')}")
                except Exception as api_e:
                    logger.error(f"[SERIES SUBSCRIBE] Ошибка в extract_movie_info: {api_e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка при получении информации о сериале", show_alert=True)
                    return
                
                if not movie_data or not movie_data.get('title'):
                    logger.error(f"[SERIES SUBSCRIBE] extract_movie_info вернул пустой/невалидный результат для kp_id={kp_id}")
                    bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                    return
                
                logger.info(f"[SERIES SUBSCRIBE] Информация получена: title={movie_data.get('title')}, is_series={movie_data.get('is_series', False)}")
                
                logger.info(f"[SERIES SUBSCRIBE] Вызываю ensure_movie_in_database: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}")
                try:
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, movie_data, user_id)
                    logger.info(f"[SERIES SUBSCRIBE] ensure_movie_in_database завершен: film_id={film_id}, was_inserted={was_inserted}")
                except Exception as db_e:
                    logger.error(f"[SERIES SUBSCRIBE] Ошибка в ensure_movie_in_database: {db_e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                    return
                
                if not film_id:
                    logger.error(f"[SERIES SUBSCRIBE] Не удалось добавить сериал в базу для kp_id={kp_id}")
                    bot.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                    return
                
                title = movie_data.get('title', 'Сериал')
                logger.info(f"[SERIES SUBSCRIBE] Сериал добавлен/найден в БД: film_id={film_id}, title={title}, was_inserted={was_inserted}")
                
                # Если сериал был добавлен, отправляем уведомление
                if was_inserted:
                    bot.send_message(chat_id, f"✅ Сериал добавлен в базу!")
                    logger.info(f"[SERIES SUBSCRIBE] Уведомление об добавлении отправлено")
        
        # Добавление подписки
        logger.info(f"[SERIES SUBSCRIBE] Добавляю подписку в БД: chat_id={chat_id}, film_id={film_id}, kp_id={kp_id}, user_id={user_id}")
        with db_lock:
            try:
                cursor_local.execute('''
                    INSERT INTO series_subscriptions (chat_id, film_id, kp_id, user_id, subscribed)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET subscribed = TRUE
                ''', (chat_id, film_id, kp_id, user_id))
                conn_local.commit()
                logger.info(f"[SERIES SUBSCRIBE] Подписка добавлена в БД успешно")
                
                # Проверяем, что подписка действительно установлена
                cursor_local.execute('''
                    SELECT subscribed FROM series_subscriptions 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                check_row = cursor_local.fetchone()
            except Exception as db_e:
                logger.error(f"[SERIES SUBSCRIBE] Ошибка работы с БД: {db_e}", exc_info=True)
                try:
                    conn_local.rollback()
                except:
                    pass
                check_row = None
            if check_row:
                subscribed_status = bool(check_row.get('subscribed') if isinstance(check_row, dict) else check_row[0])
                logger.info(f"[SERIES SUBSCRIBE] ✅ ПОДТВЕРЖДЕНО: Пользователь {user_id} успешно подписан на сериал {title} (kp_id={kp_id}, film_id={film_id}, subscribed={subscribed_status})")
            else:
                logger.warning(f"[SERIES SUBSCRIBE] ⚠️ Предупреждение: Подписка не найдена после вставки для user_id={user_id}, film_id={film_id}")
        
        # Получение данных о сезонах (с try)
        logger.info(f"[SERIES SUBSCRIBE] Получение данных о сезонах для kp_id={kp_id}")
        try:
            seasons_data = get_seasons_data(kp_id)
            logger.info(f"[SERIES SUBSCRIBE] Получено сезонов: {len(seasons_data)}")
        except Exception as e:
            logger.error(f"[SERIES SUBSCRIBE] Ошибка get_seasons_data: {e}", exc_info=True)
            seasons_data = []  # Fallback
        
        # Постановка задачи проверки
        next_check_date = None
        nearest_release_date = None
        for season in seasons_data:
            episodes = season.get('episodes', [])
            for ep in episodes:
                release_str = ep.get('releaseDate', '')
                if release_str and release_str != '—':
                    try:
                        release_date = datetime.strptime(release_str, '%Y-%m-%d').replace(tzinfo=pytz.utc)
                        if release_date > datetime.now(pytz.utc):
                            if nearest_release_date is None or release_date < nearest_release_date:
                                nearest_release_date = release_date
                    except:
                        pass
        
        if nearest_release_date:
            next_check_date = nearest_release_date - timedelta(days=1)  # Проверяем за день до выхода
        else:
            next_check_date = datetime.now(pytz.utc) + timedelta(weeks=3)  # Если нет дат, проверка через 3 недели
        
        logger.info(f"[SERIES SUBSCRIBE] Постановка задачи проверки на {next_check_date}")
        scheduler.add_job(
            check_series_for_new_episodes,
            'date',
            run_date=next_check_date,
            args=[kp_id, film_id, chat_id, user_id]
        )
        logger.info(f"[SERIES SUBSCRIBE] Задача проверки поставлена успешно")
        
        logger.info(f"[SERIES SUBSCRIBE] Пользователь {user_id} подписался на сериал {title} (kp_id={kp_id})")
        
        # Обновление сообщения - используем show_film_info_with_buttons для обновления описания
        logger.info("[SERIES SUBSCRIBE] Обновление описания через show_film_info_with_buttons")
        try:
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            
            # Получаем link
            link = None
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                    link_row = cursor_local.fetchone()
                    if link_row:
                        link = link_row.get('link') if isinstance(link_row, dict) else link_row[0]
            finally:
                try: cursor_local.close()
                except: pass
                try: conn_local.close()
                except: pass
            
            if not link:
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            
            info = extract_movie_info(link)
            if not info:
                # fallback из БД (оставь как есть)
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        cursor_local.execute('SELECT title, year, genres, description, director, actors, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                        db_row = cursor_local.fetchone()
                        if db_row:
                            info = {
                                'title': db_row.get('title') if isinstance(db_row, dict) else db_row[0],
                                'year': db_row.get('year') if isinstance(db_row, dict) else (db_row[1] if len(db_row) > 1 else None),
                                'genres': db_row.get('genres') if isinstance(db_row, dict) else (db_row[2] if len(db_row) > 2 else None),
                                'description': db_row.get('description') if isinstance(db_row, dict) else (db_row[3] if len(db_row) > 3 else None),
                                'director': db_row.get('director') if isinstance(db_row, dict) else (db_row[4] if len(db_row) > 4 else None),
                                'actors': db_row.get('actors') if isinstance(db_row, dict) else (db_row[5] if len(db_row) > 5 else None),
                                'is_series': bool(db_row.get('is_series') if isinstance(db_row, dict) else (db_row[6] if len(db_row) > 6 else 0))
                            }
                finally:
                    try: cursor_local.close()
                    except: pass
                    try: conn_local.close()
                    except: pass
            
            if info:
                message_id = call.message.message_id if call.message else None
                message_thread_id = getattr(call.message, 'message_thread_id', None)
                
                # ПЕРЕЧИТЫВАЕМ is_subscribed ПОСЛЕ COMMIT
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                is_subscribed_now = False
                try:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT subscribed 
                            FROM series_subscriptions 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s
                        """, (chat_id, film_id, user_id))
                        row = cursor_local.fetchone()
                        if row:
                            is_subscribed_now = bool(row[0] if isinstance(row, tuple) else row.get('subscribed'))
                finally:
                    try: cursor_local.close()
                    except: pass
                    try: conn_local.close()
                    except: pass
                
                logger.info(f"[SERIES SUBSCRIBE] Перечитано is_subscribed = {is_subscribed_now}")
                
                show_film_info_with_buttons(
                    chat_id=chat_id,
                    user_id=user_id,
                    info=info,
                    link=link,
                    kp_id=int(kp_id),
                    existing=None,
                    message_id=message_id,
                    message_thread_id=message_thread_id,
                    override_is_subscribed=is_subscribed_now   # ← ПЕРЕДАЁМ ЗДЕСЬ
                )
                logger.info("[SERIES SUBSCRIBE] Описание обновлено")
            else:
                logger.warning("[SERIES SUBSCRIBE] Не удалось получить информацию для обновления описания")
        
        except Exception as e:
            logger.error(f"[SERIES SUBSCRIBE] Ошибка обновления описания: {e}", exc_info=True)
    
    except Exception as e:
        logger.error(f"[SERIES SUBSCRIBE] КРИТИЧЕСКАЯ ошибка в хэндлере: {e}", exc_info=True)
        try:
            bot.send_message(chat_id, "🔔 Подписка добавлена с ошибкой. Попробуйте позже.")
        except:
            pass
    
    finally:
        # answer_callback_query уже вызван в начале, но вызываем еще раз для финального уведомления
        try:
            bot.answer_callback_query(call.id, text="🔔 Подписка добавлена", show_alert=False)
            logger.info(f"[SERIES SUBSCRIBE] Финальный answer_callback_query вызван с id={call.id}")
        except Exception as e:
            logger.warning(f"[SERIES SUBSCRIBE] Не удалось вызвать финальный answer_callback_query: {e}")
        logger.info(f"[SERIES SUBSCRIBE] ===== END: callback_id={call.id}")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_unsubscribe:"))
    def series_unsubscribe_callback(call):
        """Обработчик отписки от новых серий сериала"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Сразу отвечаем на callback, чтобы убрать "крутилку"
        try:
            bot.answer_callback_query(call.id, text="⏳ Обрабатываю...")
            logger.info(f"[SERIES UNSUBSCRIBE] answer_callback_query вызван сразу, callback_id={call.id}")
        except Exception as e:
            logger.warning(f"[SERIES UNSUBSCRIBE] Не удалось вызвать answer_callback_query сразу: {e}")
        
        try:
            logger.info(f"[SERIES UNSUBSCRIBE] ===== START: callback_id={call.id}, user_id={user_id}, chat_id={chat_id}")
            
            data = call.data.split(':')
            kp_id = data[1]
            logger.info(f"[SERIES UNSUBSCRIBE] Парсинг данных: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")
            
            # Проверяем доступ к функциям уведомлений
            if not has_notifications_access(chat_id, user_id):
                logger.warning(f"[SERIES UNSUBSCRIBE] Нет доступа к уведомлениям для user_id={user_id}, chat_id={chat_id}")
                bot.answer_callback_query(
                    call.id, 
                    "🔒 Функционал можно подключить через /payment", 
                    show_alert=True
                )
                return
            
            # Получение film_id
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                    row = cursor_local.fetchone()
                    if not row:
                        logger.error(f"[SERIES UNSUBSCRIBE] Сериал не найден для kp_id={kp_id}")
                        raise ValueError("Сериал не найден в БД")
                    
                    if isinstance(row, dict):
                        film_id = row.get('id')
                        title = row.get('title')
                    else:
                        film_id = row[0]
                        title = row[1]
                    
                    logger.info(f"[SERIES UNSUBSCRIBE] Найден сериал: film_id={film_id}, title={title}")
                    
                    # Отписываемся
                    logger.info(f"[SERIES UNSUBSCRIBE] Отписка от сериала: user_id={user_id}, film_id={film_id}")
                    cursor_local.execute('''
                        UPDATE series_subscriptions 
                        SET subscribed = FALSE 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s
                    ''', (chat_id, film_id, user_id))
                    conn_local.commit()
                    logger.info(f"[SERIES UNSUBSCRIBE] Отписка выполнена в БД")
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            logger.info(f"[SERIES UNSUBSCRIBE] Пользователь {user_id} отписался от сериала (kp_id={kp_id})")
            
            # Обновление сообщения - используем show_film_info_with_buttons для обновления описания
            logger.info("[SERIES UNSUBSCRIBE] Обновление описания через show_film_info_with_buttons")
            try:
                from moviebot.bot.handlers.series import show_film_info_with_buttons
                
                # Получаем link из БД
                link = None
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        cursor_local.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                        link_row = cursor_local.fetchone()
                        if link_row:
                            link = link_row.get('link') if isinstance(link_row, dict) else link_row[0]
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
                
                if not link:
                    link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                
                # Получаем информацию из API
                info = extract_movie_info(link)
                if not info:
                    # Если API не сработал, получаем из БД
                    conn_local = get_db_connection()
                    cursor_local = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_local.execute('SELECT title, year, genres, description, director, actors, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                            db_row = cursor_local.fetchone()
                            if db_row:
                                if isinstance(db_row, dict):
                                    info = {
                                        'title': db_row.get('title'),
                                        'year': db_row.get('year'),
                                        'genres': db_row.get('genres'),
                                        'description': db_row.get('description'),
                                        'director': db_row.get('director'),
                                        'actors': db_row.get('actors'),
                                        'is_series': bool(db_row.get('is_series', 0))
                                    }
                                else:
                                    info = {
                                        'title': db_row[0],
                                        'year': db_row[1] if len(db_row) > 1 else None,
                                        'genres': db_row[2] if len(db_row) > 2 else None,
                                        'description': db_row[3] if len(db_row) > 3 else None,
                                        'director': db_row[4] if len(db_row) > 4 else None,
                                        'actors': db_row[5] if len(db_row) > 5 else None,
                                        'is_series': bool(db_row[6] if len(db_row) > 6 else 0)
                                    }
                    finally:
                        try:
                            cursor_local.close()
                        except:
                            pass
                        try:
                            conn_local.close()
                        except:
                            pass
                
                if info:
                    message_id = call.message.message_id if call.message else None
                    message_thread_id = getattr(call.message, 'message_thread_id', None)
                    
                    show_film_info_with_buttons(
                        chat_id=chat_id,
                        user_id=user_id,
                        info=info,
                        link=link,
                        kp_id=int(kp_id),
                        existing=None,  # Будет получено внутри функции через get_film_current_state
                        message_id=message_id,
                        message_thread_id=message_thread_id
                    )
                    logger.info("[SERIES UNSUBSCRIBE] Описание обновлено через show_film_info_with_buttons")
                else:
                    logger.warning("[SERIES UNSUBSCRIBE] Не удалось получить информацию для обновления описания")
            
            except Exception as e:
                logger.error(f"[SERIES UNSUBSCRIBE] Ошибка обновления описания: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"[SERIES UNSUBSCRIBE] КРИТИЧЕСКАЯ ошибка в хэндлере: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, "🔕 Отписка выполнена с ошибкой. Попробуйте позже.")
            except:
                pass
        
        finally:
            # answer_callback_query уже вызван в начале, но вызываем еще раз для финального уведомления
            try:
                bot.answer_callback_query(call.id, text="🔕 Отписка выполнена", show_alert=False)
                logger.info(f"[SERIES UNSUBSCRIBE] Финальный answer_callback_query вызван с id={call.id}")
            except Exception as e:
                logger.warning(f"[SERIES UNSUBSCRIBE] Не удалось вызвать финальный answer_callback_query: {e}")
            logger.info(f"[SERIES UNSUBSCRIBE] ===== END: callback_id={call.id}")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_locked:"))
    def series_locked_callback(call):
        """Обработчик для заблокированных функций сериалов (нет доступа)"""
        try:
            bot.answer_callback_query(
                call.id,
                "🔒 Функционал можно подключить через /payment",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[SERIES LOCKED] Ошибка: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("series_episode_cancel_auto:"))
    def handle_episode_cancel_auto(call):
        """Обработчик отмены автоотметки эпизодов"""
        try:
            bot.answer_callback_query(call.id)
            parts = call.data.split(":")
            if len(parts) < 3:
                logger.error(f"[EPISODE CANCEL AUTO] Неверный формат callback_data: {call.data}")
                return
            
            kp_id = parts[1]
            season_num = parts[2]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[EPISODE CANCEL AUTO] Отмена автоотметки: kp_id={kp_id}, season={season_num}, user_id={user_id}")
            
            # Получаем список автоматически отмеченных эпизодов
            if user_id not in user_episode_auto_mark_state:
                bot.answer_callback_query(call.id, "❌ Нет автоотметки для отмены", show_alert=True)
                return
            
            auto_state = user_episode_auto_mark_state[user_id]
            if auto_state.get('kp_id') != kp_id or auto_state.get('season_num') != season_num:
                bot.answer_callback_query(call.id, "❌ Нет автоотметки для этого сезона", show_alert=True)
                return
            
            auto_marked = auto_state.get('episodes', [])
            if not auto_marked:
                bot.answer_callback_query(call.id, "❌ Нет эпизодов для отмены", show_alert=True)
                return
            
            # Получаем film_id
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            try:
                with db_lock:
                    cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                    row = cursor_local.fetchone()
                    if not row:
                        bot.answer_callback_query(call.id, "❌ Сериал не найден", show_alert=True)
                        return
                    
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    
                    # Удаляем все автоматически отмеченные эпизоды
                    for season_num_mark, ep_num_mark in auto_marked:
                        cursor_local.execute('''
                            DELETE FROM series_tracking 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                            AND season_number = %s AND episode_number = %s
                        ''', (chat_id, film_id, user_id, season_num_mark, ep_num_mark))
                    
                    conn_local.commit()
                    logger.info(f"[EPISODE CANCEL AUTO] Отменено {len(auto_marked)} эпизодов")
                
                # Удаляем состояние автоотметки
                del user_episode_auto_mark_state[user_id]
                
                # Обновляем страницу эпизодов
                from moviebot.bot.handlers.seasons import show_episodes_page
                message_id = call.message.message_id if call.message else None
                message_thread_id = getattr(call.message, 'message_thread_id', None)
                
                # Получаем текущую страницу из состояния
                current_page = 1
                if user_id in user_episodes_state:
                    state = user_episodes_state[user_id]
                    if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                        current_page = state.get('page', 1)
                
                show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
                
                bot.answer_callback_query(call.id, f"✅ Отменено {len(auto_marked)} эпизодов")
                
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
            logger.error(f"[EPISODE CANCEL AUTO] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

        """Обработчик отметки всех эпизодов сезона как просмотренных"""
        logger.info(f"[SEASON ALL] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
        try:
            bot.answer_callback_query(call.id)
            parts = call.data.split(":")
            if len(parts) < 3:
                logger.error(f"[SEASON ALL] Неверный формат callback_data: {call.data}")
                return
            
            kp_id = parts[1]
            season_num = parts[2]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SEASON ALL] Отметка всех эпизодов сезона: kp_id={kp_id}, season={season_num}, user_id={user_id}")
            
            # Используем локальные соединение и курсор
            from moviebot.database.db_connection import get_db_connection, get_db_cursor
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            # Получаем film_id и эпизоды сезона
            with db_lock:
                try:
                    cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                    row = cursor_local.fetchone()
                except Exception as db_e:
                    logger.error(f"[SEASON ALL] Ошибка запроса film_id: {db_e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка доступа к базе данных", show_alert=True)
                    return
                    
                if not row:
                    bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                
                # Получаем эпизоды сезона
                seasons_data = get_seasons_data(kp_id)
                if not seasons_data:
                    bot.answer_callback_query(call.id, "❌ Не удалось получить данные о сезонах", show_alert=True)
                    return
                
                season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
                if not season:
                    bot.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
                    return
                
                episodes = season.get('episodes', [])
                
                # Отмечаем все эпизоды как просмотренные
                try:
                    for ep in episodes:
                        ep_num = str(ep.get('episodeNumber', ''))
                        cursor_local.execute('''
                            INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                            VALUES (%s, %s, %s, %s, %s, TRUE)
                            ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                            DO UPDATE SET watched = TRUE
                        ''', (chat_id, film_id, user_id, season_num, ep_num))
                    
                    conn_local.commit()
                except Exception as db_e:
                    logger.error(f"[SEASON ALL] Ошибка обновления эпизодов: {db_e}", exc_info=True)
                    try:
                        conn_local.rollback()
                    except:
                        pass
                    bot.answer_callback_query(call.id, "❌ Ошибка при обновлении эпизодов", show_alert=True)
                    return
            
            # Обновляем страницу эпизодов
            from moviebot.bot.handlers.seasons import show_episodes_page
            message_id = call.message.message_id if call.message else None
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            
            current_page = 1
            if user_id in user_episodes_state:
                state = user_episodes_state[user_id]
                if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                    current_page = state.get('page', 1)
            
            show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
            logger.info(f"[SEASON ALL] ===== END: успешно обновлено")
        except Exception as e:
            logger.error(f"[SEASON ALL] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("episodes_page:"))
def handle_episodes_page_navigation(call):
    """Обработчик навигации по страницам эпизодов"""
    logger.info(f"[EPISODES PAGE] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        if len(parts) < 4:
            logger.error(f"[EPISODES PAGE] Неверный формат callback_data: {call.data}")
            return
        
        kp_id = parts[1]
        season_num = parts[2]
        page = int(parts[3])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        from moviebot.bot.handlers.seasons import show_episodes_page
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        show_episodes_page(kp_id, season_num, chat_id, user_id, page=page, message_id=message_id, message_thread_id=message_thread_id)
        logger.info(f"[EPISODES PAGE] ===== END: успешно обновлено, page={page}")
    except Exception as e:
        logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("episodes_back_to_seasons:"))
def handle_episodes_back_to_seasons(call):
    """Обработчик возврата к списку сезонов из эпизодов"""
    try:
        # Сразу отвечаем на callback, чтобы убрать "часики"
        try:
            bot.answer_callback_query(call.id)
        except Exception as ans_e:
            logger.warning(f"[EPISODES BACK] Не удалось ответить на callback (возможно истёк): {ans_e}")

        # ── Безопасный парсинг kp_id ─────────────────────────────────────────────
        parts = call.data.split(":")
        if len(parts) < 2:
            logger.error(f"[EPISODES BACK] Нет kp_id в callback_data: {call.data}")
            bot.answer_callback_query(call.id, "Ошибка кнопки, попробуй заново", show_alert=True)
            return

        kp_id_raw = parts[1].strip()
        try:
            kp_id = str(int(kp_id_raw))  # делаем чистую строку-число
        except ValueError:
            logger.error(f"[EPISODES BACK] kp_id не число: '{kp_id_raw}' → {call.data}")
            bot.answer_callback_query(call.id, "Неверный ID сериала", show_alert=True)
            return

        chat_id = call.message.chat.id
        user_id = call.from_user.id

        logger.info(f"[EPISODES BACK] Возврат к сезонам: kp_id={kp_id}, user_id={user_id}, chat_id={chat_id}")

        # ── Здесь должен быть вызов функции показа сезонов ───────────────────────
        # Самый простой и надёжный вариант сейчас — вызвать уже существующую функцию
        from moviebot.bot.callbacks.series_callbacks import series_track_callback

        # Формируем фейковый call с нужным callback_data
        fake_call = types.CallbackQuery(
            id=call.id,
            from_user=call.from_user,
            message=call.message,
            chat_instance=call.chat_instance,
            data=f"series_track:{kp_id}"
        )

        # Вызываем обработчик списка сезонов
        series_track_callback(fake_call)

        logger.info(f"[EPISODES BACK] Успешно вызван series_track для kp_id={kp_id}")

    except Exception as e:
        logger.error(f"[EPISODES BACK] Критическая ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Не удалось вернуться к сезонам", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data and (call.data.startswith("series_episode_toggle:") or call.data.startswith("series_episode:")))
def handle_episode_toggle(call):
    """Обработчик переключения статуса просмотра эпизода с поддержкой двойного клика для автоотметки"""
    logger.info(f"[EPISODE TOGGLE] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
    logger.info(f"[EPISODE TOGGLE] Обработчик вызван! bot={bot}, id(bot)={id(bot)}")
    
    if not call.data:
        logger.error(f"[EPISODE TOGGLE] call.data is None!")
        return
    
    # Сразу отвечаем на callback, чтобы убрать индикатор загрузки
    try:
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.warning(f"[EPISODE TOGGLE] Не удалось ответить на callback сразу: {e}")
    
    try:
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
        
        # Используем локальные соединение и курсор
        from moviebot.database.db_connection import get_db_connection
        conn_local = get_db_connection()
        
        # Получаем film_id (добавляем сериал в базу, если его еще нет)
        film_id = None
        with db_lock:
            try:
                cursor_local = conn_local.cursor()
                cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                row = cursor_local.fetchone()
                cursor_local.close()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
            except Exception as db_e:
                logger.error(f"[EPISODE TOGGLE] Ошибка запроса film_id: {db_e}", exc_info=True)
            
        # Если сериала нет в базе, добавляем его
        if not film_id:
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            info = extract_movie_info(link)
            if info:
                film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                if was_inserted:
                    logger.info(f"[EPISODE TOGGLE] Сериал добавлен в базу при отметке эпизода: kp_id={kp_id}, film_id={film_id}")
                if not film_id:
                    bot.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                    return
            else:
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                return
        
        # Получаем данные о сезоне для автоотметки
        seasons_data = get_seasons_data(kp_id)
        season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
        if not season:
            logger.warning(f"[EPISODE TOGGLE] Сезон не найден: season={season_num}, kp_id={kp_id}")
            bot.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
            try:
                conn_local.close()
            except:
                pass
            return
        
        episodes = season.get('episodes', [])
        # Сортируем эпизоды по номеру
        episodes_sorted = sorted(episodes, key=lambda e: int(e.get('episodeNumber', 0)))
        
        cursor_local = None
        try:
            # Все операции с БД внутри одного блока db_lock
            with db_lock:
                cursor_local = conn_local.cursor()
                
                # Проверяем текущий статус
                cursor_local.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor_local.fetchone()
                is_watched = False
                if watched_row:
                    is_watched = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                
                logger.info(f"[EPISODE TOGGLE] Текущий статус: is_watched={is_watched}, user_episode_auto_mark_state для user_id={user_id}: {user_episode_auto_mark_state.get(user_id)}")
                
                # Логика обработки
                auto_marked_episodes = []
                
                # Проверяем, был ли это двойной клик на уже просмотренную серию
                # Двойной клик = клик на просмотренную серию, для которой уже сохранено состояние (после первого клика)
                is_double_click = False
                if is_watched and user_id in user_episode_auto_mark_state:
                    auto_state = user_episode_auto_mark_state[user_id]
                    if str(auto_state.get('kp_id')) == str(kp_id) and str(auto_state.get('season_num')) == str(season_num):
                        last_clicked = auto_state.get('last_clicked_ep')
                        # Если last_clicked совпадает с текущим эпизодом - это двойной клик!
                        if last_clicked and str(last_clicked[0]) == str(season_num) and str(last_clicked[1]) == str(ep_num):
                            is_double_click = True
                            logger.info(f"[EPISODE TOGGLE] ✅✅✅ ДВОЙНОЙ КЛИК на просмотренную серию! Запускаем автоотметку! ✅✅✅")
                
                if is_watched and is_double_click:
                    # ДВОЙНОЙ КЛИК: эпизод уже просмотрен И уже был отмечен в автоотметке - запускаем автоотметку
                    ep_num_int = int(ep_num) if ep_num.isdigit() else 0
                    season_num_int = int(season_num) if str(season_num).isdigit() else 0
                    
                    # ВАЖНО: Получаем все просмотренные эпизоды ДО начала отметки новых (во всех сезонах)
                    cursor_local.execute('''
                        SELECT season_number, episode_number FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND watched = TRUE
                    ''', (chat_id, film_id, user_id))
                    watched_episodes_set_before = set()
                    for w_row in cursor_local.fetchall():
                        watched_season = w_row.get('season_number') if isinstance(w_row, dict) else w_row[0]
                        watched_ep_num = w_row.get('episode_number') if isinstance(w_row, dict) else w_row[1]
                        watched_season_int = int(watched_season) if str(watched_season).isdigit() else 0
                        watched_ep_num_int = int(watched_ep_num) if str(watched_ep_num).isdigit() else 0
                        watched_episodes_set_before.add((watched_season_int, watched_ep_num_int))
                    
                    # Получаем ВСЕ сезоны сериала и сортируем их
                    all_seasons_sorted = sorted(seasons_data, key=lambda s: int(s.get('number', 0)) if str(s.get('number', '')).isdigit() else 0)
                    
                    # Проходим по всем сезонам до текущего (включительно)
                    # Это работает для любого сезона, включая первый
                    for current_season in all_seasons_sorted:
                        current_season_num = current_season.get('number', '')
                        current_season_num_int = int(current_season_num) if str(current_season_num).isdigit() else 0
                        
                        # Если это сезон раньше текущего - отмечаем все непросмотренные эпизоды этого сезона
                        if current_season_num_int < season_num_int:
                            current_episodes = current_season.get('episodes', [])
                            current_episodes_sorted = sorted(current_episodes, key=lambda e: int(e.get('episodeNumber', 0)) if str(e.get('episodeNumber', '')).isdigit() else 0)
                            
                            for ep in current_episodes_sorted:
                                ep_current_num = int(ep.get('episodeNumber', 0)) if str(ep.get('episodeNumber', '')).isdigit() else 0
                                if (current_season_num_int, ep_current_num) not in watched_episodes_set_before:
                                    # Отмечаем эпизод
                                    cursor_local.execute('''
                                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                                        VALUES (%s, %s, %s, %s, %s, TRUE)
                                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                                        DO UPDATE SET watched = TRUE
                                    ''', (chat_id, film_id, user_id, str(current_season_num), str(ep_current_num)))
                                    auto_marked_episodes.append((str(current_season_num), str(ep_current_num)))
                        
                        # Если это текущий сезон (может быть любой, включая первый) - отмечаем все непросмотренные до выбранной серии
                        elif current_season_num_int == season_num_int:
                            current_episodes = current_season.get('episodes', [])
                            current_episodes_sorted = sorted(current_episodes, key=lambda e: int(e.get('episodeNumber', 0)) if str(e.get('episodeNumber', '')).isdigit() else 0)
                            
                            # Отмечаем все непросмотренные эпизоды до выбранной серии включительно
                            # Это работает даже если в сезоне 500+ серий
                            for ep in current_episodes_sorted:
                                ep_current_num = int(ep.get('episodeNumber', 0)) if str(ep.get('episodeNumber', '')).isdigit() else 0
                                # Условие: номер эпизода <= выбранного И эпизод не был просмотрен до начала автоотметки
                                if ep_current_num <= ep_num_int and (current_season_num_int, ep_current_num) not in watched_episodes_set_before:
                                    # Отмечаем эпизод
                                    cursor_local.execute('''
                                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                                        VALUES (%s, %s, %s, %s, %s, TRUE)
                                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                                        DO UPDATE SET watched = TRUE
                                    ''', (chat_id, film_id, user_id, str(current_season_num), str(ep_current_num)))
                                    auto_marked_episodes.append((str(current_season_num), str(ep_current_num)))
                        
                        # Если сезон после текущего - не обрабатываем (выходим из цикла)
                        else:
                            break
                    
                    # ВАЖНО: Добавляем изначально просмотренную серию в список для отмены
                    # чтобы при отмене она тоже была удалена
                    auto_marked_episodes.append((season_num, ep_num))
                    
                    # Сохраняем список автоматически отмеченных эпизодов для возможной отмены
                    user_episode_auto_mark_state[user_id] = {
                        'kp_id': kp_id,
                        'season_num': season_num,
                        'episodes': auto_marked_episodes
                    }
                    
                    logger.info(f"[EPISODE TOGGLE] Автоотметка: отмечено {len(auto_marked_episodes)} эпизодов (включая изначальную {ep_num})")
                    
                elif is_watched and not is_double_click:
                    # Просмотренная серия без сохраненного состояния - просто снимаем отметку
                    cursor_local.execute('''
                        UPDATE series_tracking 
                        SET watched = FALSE
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND season_number = %s AND episode_number = %s
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                    
                    # Очищаем состояние, если оно было для другого эпизода
                    if user_id in user_episode_auto_mark_state:
                        auto_state = user_episode_auto_mark_state[user_id]
                        if str(auto_state.get('kp_id')) != str(kp_id) or str(auto_state.get('season_num')) != str(season_num):
                            del user_episode_auto_mark_state[user_id]
                    
                    logger.info(f"[EPISODE TOGGLE] Отметка снята с серии {season_num}:{ep_num}")
                    
                    # Проверяем, все ли серии просмотрены после снятия отметки
                    # Если хотя бы одна серия не просмотрена - убираем отметку с сериала
                    cursor_local.execute('''
                        SELECT COUNT(*) FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND watched = FALSE
                    ''', (chat_id, film_id, user_id))
                    unwatched_count_row = cursor_local.fetchone()
                    unwatched_count = unwatched_count_row.get('count') if isinstance(unwatched_count_row, dict) else (unwatched_count_row[0] if unwatched_count_row else 0)
                    
                    if unwatched_count > 0:
                        # Есть непросмотренные серии - убираем отметку с сериала
                        cursor_local.execute('UPDATE movies SET watched = 0 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                        logger.info(f"[EPISODE TOGGLE] Убрана отметка с сериала (есть непросмотренные серии)")
                else:
                    # Эпизод НЕ просмотрен - это ПЕРВЫЙ клик: отмечаем как просмотренную и сохраняем состояние
                    logger.info(f"[EPISODE TOGGLE] ===== ПЕРВЫЙ КЛИК: эпизод НЕ просмотрен, отмечаем и сохраняем состояние =====")
                    
                    # Отмечаем эпизод как просмотренный
                    cursor_local.execute('''
                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                        VALUES (%s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                        DO UPDATE SET watched = TRUE
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                    
                    # Сохраняем состояние для возможного двойного клика (второй клик на эту же просмотренную серию)
                    user_episode_auto_mark_state[user_id] = {
                        'kp_id': str(kp_id),  # Приводим к строке для надежности
                        'season_num': str(season_num),  # Приводим к строке для надежности
                        'episodes': [(str(season_num), str(ep_num))],  # Сохраняем только этот эпизод для проверки двойного клика
                        'last_clicked_ep': (str(season_num), str(ep_num))  # Приводим к строкам для надежности
                    }
                    logger.info(f"[EPISODE TOGGLE] ✅ Серия {season_num}:{ep_num} отмечена как просмотренная, состояние сохранено для двойного клика: {user_episode_auto_mark_state[user_id]}")
                    
                    # Проверяем, все ли серии просмотрены после отметки
                    cursor_local.execute('''
                        SELECT COUNT(*) FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND watched = FALSE
                    ''', (chat_id, film_id, user_id))
                    unwatched_count_row = cursor_local.fetchone()
                    unwatched_count = unwatched_count_row.get('count') if isinstance(unwatched_count_row, dict) else (unwatched_count_row[0] if unwatched_count_row else 0)
                    
                    if unwatched_count == 0:
                        # Все серии просмотрены - отмечаем сериал
                        cursor_local.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                        logger.info(f"[EPISODE TOGGLE] Все серии просмотрены - сериал отмечен как просмотренный")
                
                conn_local.commit()
                if cursor_local:
                    cursor_local.close()
                
                # Проверяем состояние после коммита
                if user_id in user_episode_auto_mark_state:
                    logger.info(f"[EPISODE TOGGLE] Состояние после коммита: user_id={user_id}, state={user_episode_auto_mark_state[user_id]}")
                else:
                    logger.info(f"[EPISODE TOGGLE] Состояние после коммита: user_id={user_id}, state=None (очищено)")
        except Exception as db_e:
            logger.error(f"[EPISODE TOGGLE] Ошибка работы с БД: {db_e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            try:
                if cursor_local:
                    cursor_local.close()
            except:
                pass
            raise
        finally:
            try:
                conn_local.close()
            except:
                pass
        
        # Обновляем страницу эпизодов
        from moviebot.bot.handlers.seasons import show_episodes_page
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        # Получаем текущую страницу из состояния
        current_page = 1
        if user_id in user_episodes_state:
            state = user_episodes_state[user_id]
            if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                current_page = state.get('page', 1)
        
        result = show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
        
        # answer_callback_query уже вызван в начале функции
        if not result:
            logger.warning(f"[EPISODE TOGGLE] show_episodes_page вернула False, возможно ошибка обновления сообщения")
            try:
                bot.answer_callback_query(call.id, "⚠️ Не удалось обновить сообщение", show_alert=False)
            except:
                pass
        
        logger.info(f"[EPISODE TOGGLE] ===== END: успешно обновлено")
    except Exception as e:
        logger.error(f"[EPISODE TOGGLE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("series_season_all:"))
def handle_season_all_toggle(call):
    """Обработчик отметки всех эпизодов сезона как просмотренных"""
    logger.info(f"[SEASON ALL] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        if len(parts) < 3:
            logger.error(f"[SEASON ALL] Неверный формат callback_data: {call.data}")
            return
        
        kp_id = parts[1]
        season_num = parts[2]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[SEASON ALL] Отметка всех эпизодов сезона: kp_id={kp_id}, season={season_num}, user_id={user_id}")
        
        # Используем локальные соединение и курсор
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        # Получаем film_id и эпизоды сезона
        with db_lock:
            try:
                cursor_local.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                row = cursor_local.fetchone()
            except Exception as db_e:
                logger.error(f"[SEASON ALL] Ошибка запроса film_id: {db_e}", exc_info=True)
                bot.answer_callback_query(call.id, "❌ Ошибка доступа к базе данных", show_alert=True)
                return
                
            if not row:
                bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            
            # Получаем эпизоды сезона
            seasons_data = get_seasons_data(kp_id)
            if not seasons_data:
                bot.answer_callback_query(call.id, "❌ Не удалось получить данные о сезонах", show_alert=True)
                return
            
            season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
            if not season:
                bot.answer_callback_query(call.id, "❌ Сезон не найден", show_alert=True)
                return
            
            episodes = season.get('episodes', [])
            
            # Отмечаем все эпизоды как просмотренные
            try:
                for ep in episodes:
                    ep_num = str(ep.get('episodeNumber', ''))
                    cursor_local.execute('''
                        INSERT INTO series_tracking (chat_id, film_id, user_id, season_number, episode_number, watched)
                        VALUES (%s, %s, %s, %s, %s, TRUE)
                        ON CONFLICT (chat_id, film_id, user_id, season_number, episode_number) 
                        DO UPDATE SET watched = TRUE
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                
                conn_local.commit()
            except Exception as db_e:
                logger.error(f"[SEASON ALL] Ошибка обновления эпизодов: {db_e}", exc_info=True)
                try:
                    conn_local.rollback()
                except:
                    pass
                bot.answer_callback_query(call.id, "❌ Ошибка при обновлении эпизодов", show_alert=True)
                return
        
        # Обновляем страницу эпизодов
        from moviebot.bot.handlers.seasons import show_episodes_page
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        current_page = 1
        if user_id in user_episodes_state:
            state = user_episodes_state[user_id]
            if state.get('kp_id') == kp_id and state.get('season_num') == season_num:
                current_page = state.get('page', 1)
        
        show_episodes_page(kp_id, season_num, chat_id, user_id, page=current_page, message_id=message_id, message_thread_id=message_thread_id)
        logger.info(f"[SEASON ALL] ===== END: успешно обновлено")
    except Exception as e:
        logger.error(f"[SEASON ALL] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("rate_film:"))
def rate_film_callback(call):
    """Обработчик кнопки 'Оценить'"""
    try:
        try:
            bot.answer_callback_query(call.id)
        except Exception as ans_e:
            logger.warning(f"[RATE FILM] Не удалось ответить на callback сразу: {ans_e}")

        parts = call.data.split(":")
        if len(parts) < 2:
            logger.error(f"[RATE FILM] Нет kp_id в callback_data: {call.data}")
            bot.answer_callback_query(call.id, "Ошибка кнопки", show_alert=True)
            return

        kp_id_raw = parts[1].strip()
        try:
            kp_id = str(int(kp_id_raw))
        except ValueError:
            logger.error(f"[RATE FILM] kp_id не число: '{kp_id_raw}' в {call.data}")
            bot.answer_callback_query(call.id, "Неверный ID фильма", show_alert=True)
            return

        user_id = call.from_user.id
        chat_id = call.message.chat.id

        logger.info(f"[RATE FILM] Пользователь {user_id} хочет оценить kp_id={kp_id}")

        film_id = None
        title = 'Фильм'
        is_series = False

        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        row = None
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, is_series 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                ''', (chat_id, kp_id))
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
            film_id = row[0] if isinstance(row, tuple) else row.get('id')
            title = row[1] if isinstance(row, tuple) else row.get('title', 'Фильм')
            is_series_db = row[2] if isinstance(row, tuple) else row.get('is_series', 0)
            is_series = bool(is_series_db)

        if is_series:
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
        else:
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"

        # Экранирование для MarkdownV2
        def escape_md_v2(text):
            chars = r'_[]()~`>#+-=|{}.!'
            for char in chars:
                text = text.replace(char, f'\\{char}')
            return text

        escaped_title = escape_md_v2(title)

        if not film_id:
            # Фильм не в базе
            info = extract_movie_info(link)
            title = info.get('title', f'Фильм {kp_id}') if info else f'Фильм {kp_id}'
            escaped_title = escape_md_v2(title)

            text_new = (
                f"💬 Чтобы оценить *{escaped_title}*, ответьте на это сообщение числом от 1 до 10.\n\n"
                f"Фильм/сериал будет добавлен в базу при оценке."
            )

            try:
                msg = bot.reply_to(
                    call.message,
                    text_new,
                    parse_mode='MarkdownV2'
                )
                rating_messages[msg.message_id] = f"kp_id:{kp_id}"
                logger.info(f"[RATE FILM] Добавлено в rating_messages: msg_id={msg.message_id} → kp_id:{kp_id}")
            except telebot.apihelper.ApiTelegramException as api_err:
                if "can't parse entities" in str(api_err):
                    logger.warning(f"[RATE FILM] MarkdownV2 сломался, fallback на plain текст")
                    msg = bot.reply_to(
                        call.message,
                        text_new.replace('\\', ''),  # убираем экранирование
                        parse_mode=None
                    )
                    rating_messages[msg.message_id] = f"kp_id:{kp_id}"
                    logger.info(f"[RATE FILM] Добавлено (fallback): msg_id={msg.message_id}")
                else:
                    raise
            return

        # Фильм в базе — проверяем оценку
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        existing = None
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT rating FROM ratings 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id, user_id))
                existing = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass

        if existing:
            rating = existing[0] if isinstance(existing, tuple) else existing.get('rating')
            text_already = (
                f"✅ Вы уже оценили *{escaped_title}*: {rating}/10\n\n"
                f"Чтобы изменить — ответьте на это сообщение числом от 1 до 10."
            )

            try:
                bot.reply_to(
                    call.message,
                    text_already,
                    parse_mode='MarkdownV2'
                )
            except telebot.apihelper.ApiTelegramException as api_err:
                if "can't parse entities" in str(api_err):
                    logger.warning(f"[RATE FILM] MarkdownV2 сломался (уже оценено), fallback на plain")
                    bot.reply_to(
                        call.message,
                        text_already.replace('\\', ''),
                        parse_mode=None
                    )
                else:
                    raise
        else:
            text_new = (
                f"💬 Чтобы оценить *{escaped_title}*, ответьте на это сообщение числом от 1 до 10.\n\n"
                f"Фильм/сериал будет добавлен в базу при оценке."
            )

            try:
                msg = bot.reply_to(
                    call.message,
                    text_new,
                    parse_mode='MarkdownV2'
                )
                rating_messages[msg.message_id] = film_id
                logger.info(f"[RATE FILM] rating_messages обновлено: msg_id={msg.message_id} → film_id={film_id}")
            except telebot.apihelper.ApiTelegramException as api_err:
                if "can't parse entities" in str(api_err):
                    logger.warning(f"[RATE FILM] MarkdownV2 сломался, fallback на plain")
                    msg = bot.reply_to(
                        call.message,
                        text_new.replace('\\', ''),
                        parse_mode=None
                    )
                    rating_messages[msg.message_id] = film_id
                    logger.info(f"[RATE FILM] rating_messages обновлено (fallback): msg_id={msg.message_id}")
                else:
                    raise

    except Exception as e:
        logger.error(f"[RATE FILM] Критическая ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при обработке оценки", show_alert=True)
        except:
            pass
        
    @bot.callback_query_handler(func=lambda call: call.data.startswith("show_facts:") or call.data.startswith("facts:"))
    def facts_callback(call):
        """Обработчик кнопки 'Интересные факты'"""
        try:
            # Сразу отвечаем на callback, чтобы убрать "часики"
            try:
                bot.answer_callback_query(call.id)
            except Exception as ans_e:
                logger.warning(f"[FACTS] Не удалось сразу ответить на callback: {ans_e}")

            # ── Безопасный парсинг kp_id ─────────────────────────────────────────────
            parts = call.data.split(":")
            if len(parts) < 2:
                logger.error(f"[FACTS] Нет kp_id в callback_data: {call.data}")
                bot.answer_callback_query(call.id, "Ошибка кнопки", show_alert=True)
                return

            kp_id_raw = parts[1].strip()
            try:
                kp_id = str(int(kp_id_raw))  # чистая строка-число
            except ValueError:
                logger.error(f"[FACTS] kp_id не является числом: '{kp_id_raw}' в {call.data}")
                bot.answer_callback_query(call.id, "Неверный ID фильма/сериала", show_alert=True)
                return

            chat_id = call.message.chat.id
            user_id = call.from_user.id

            logger.info(f"[FACTS] Пользователь {user_id} запросил факты для kp_id={kp_id}")

            # Получаем факты
            facts = get_facts(kp_id)
            if facts:
                bot.send_message(chat_id, facts, parse_mode='HTML')
                bot.answer_callback_query(call.id, "Факты отправлены")
            else:
                bot.answer_callback_query(call.id, "Факты не найдены", show_alert=True)

        except Exception as e:
            logger.error(f"[FACTS] Критическая ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка при загрузке фактов", show_alert=True)
            except:
                pass
    
    # Проверяем, что обработчик действительно зарегистрирован
    logger.info(f"[REGISTER SERIES CALLBACKS] Проверка регистрации handle_episode_toggle...")
    logger.info(f"[REGISTER SERIES CALLBACKS] handle_episode_toggle функция: {handle_episode_toggle}")
    logger.info(f"[REGISTER SERIES CALLBACKS] handle_episode_toggle.__name__: {handle_episode_toggle.__name__}")
    
    # Проверяем, есть ли обработчик в списке зарегистрированных
    try:
        handlers_count = len(bot.callback_query_handlers)
        logger.info(f"[REGISTER SERIES CALLBACKS] bot callback handlers count: {handlers_count}")
        
        # Ищем наш обработчик в списке
        found_handler = False
        for handler in bot.callback_query_handlers:
            if hasattr(handler, 'callback') and handler.callback == handle_episode_toggle:
                found_handler = True
                logger.info(f"[REGISTER SERIES CALLBACKS] ✅ handle_episode_toggle найден в списке обработчиков!")
                break
        
        if not found_handler:
            logger.warning(f"[REGISTER SERIES CALLBACKS] ⚠️ handle_episode_toggle НЕ найден в списке обработчиков!")
    except Exception as e:
        logger.error(f"[REGISTER SERIES CALLBACKS] Ошибка при проверке обработчиков: {e}", exc_info=True)
    
    logger.info(f"[REGISTER SERIES CALLBACKS] ✅ Все обработчики сериалов зарегистрированы (включая handle_episode_toggle)")
    logger.info(f"[REGISTER SERIES CALLBACKS] ===== END =====")
    logger.info("=" * 80)

    # Обработчик plan_from_added перенесен в moviebot/bot/handlers/plan.py
    # чтобы избежать конфликтов с дублирующим обработчиком
