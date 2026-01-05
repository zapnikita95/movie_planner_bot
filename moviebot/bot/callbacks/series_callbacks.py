"""
Callback handlers для работы с сериалами
"""
import logging
import json
import re
from datetime import datetime as dt, timedelta
import pytz
import telebot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance, scheduler
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.database.db_operations import get_watched_emojis, get_watched_custom_emoji_ids
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info
from moviebot.utils.helpers import has_notifications_access
from moviebot.scheduler import send_series_notification, check_series_for_new_episodes
from moviebot.states import user_episodes_state, rating_messages, user_plan_state
from moviebot.api.kinopoisk_api import get_facts
# show_film_info_with_buttons больше не используется - обновляем только кнопку подписки без API запросов

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_series_callbacks(bot_instance):
    """Регистрирует callback handlers для сериалов"""
    
    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_track:"))
    def series_track_callback(call):
        """Обработчик для отметки сезонов/серий как просмотренных"""
        try:
            bot_instance.answer_callback_query(call.id)
            
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            
            logger.info(f"[SERIES TRACK] Начало: user_id={user_id}, chat_id={chat_id}, kp_id={kp_id}")
            
            # Проверяем доступ к функциям уведомлений
            if not has_notifications_access(chat_id, user_id):
                logger.warning(f"[SERIES TRACK] Нет доступа: user_id={user_id}, chat_id={chat_id}")
                bot_instance.answer_callback_query(
                    call.id, 
                    "🔒 Функционал можно подключить через /payment", 
                    show_alert=True
                )
                return
            
            # Получаем film_id (добавляем в базу, если нет)
            from moviebot.bot.handlers.series import ensure_movie_in_database
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            info = extract_movie_info(link)
            if not info:
                logger.error(f"[SERIES TRACK] Не удалось получить информацию о сериале для kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                return
            
            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            if not film_id:
                logger.error(f"[SERIES TRACK] Не удалось добавить сериал в базу для kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                return
            
            title = info.get('title', 'Сериал')
            
            # Если сериал был добавлен, отправляем уведомление
            if was_inserted:
                bot_instance.send_message(chat_id, f"✅ Сериал добавлен в базу!")
                logger.info(f"[SERIES TRACK] Сериал добавлен в базу: film_id={film_id}, title={title}")
            
            # Получаем сезоны из API
            seasons_data = get_seasons_data(kp_id)
            if not seasons_data:
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о сезонах", show_alert=True)
                return
            
            # Показываем меню выбора сезона с отметками статуса
            from datetime import datetime as dt
            now = dt.now()
            
            markup = InlineKeyboardMarkup(row_width=1)
            for season in seasons_data:
                season_num = season.get('number', '')
                episodes = season.get('episodes', [])
                episodes_count = len(episodes)
                
                # Проверяем, вышел ли сезон (все эпизоды должны иметь дату выхода <= текущей дате)
                season_released = True
                if episodes:
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
                                    season_released = False
                                    break
                            except:
                                pass
                
                # Показываем только сезоны, которые уже вышли
                if not season_released:
                    continue
                
                # Проверяем статус сезона
                watched_count = 0
                with db_lock:
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
            
            # Проверяем, все ли сезоны просмотрены
            all_seasons_watched = True
            for season in seasons_data:
                season_num = season.get('number', '')
                episodes = season.get('episodes', [])
                episodes_count = len(episodes)
                
                # Проверяем, вышел ли сезон
                season_released = True
                if episodes:
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
                                    season_released = False
                                    break
                            except:
                                pass
                
                # Если сезон не вышел, пропускаем
                if not season_released:
                    continue
                
                # Проверяем, все ли эпизоды сезона просмотрены
                watched_count = 0
                with db_lock:
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
                
                if watched_count < episodes_count or episodes_count == 0:
                    all_seasons_watched = False
                    break
            
            # Если все сезоны просмотрены, отмечаем сериал как просмотренный в БД
            if all_seasons_watched:
                with db_lock:
                    cursor.execute("UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                    conn.commit()
            
            # Добавляем кнопку "Оценить" если все сезоны просмотрены
            if all_seasons_watched:
                # Получаем информацию об оценках
                with db_lock:
                    # Получаем среднюю оценку
                    cursor.execute('''
                        SELECT AVG(rating) as avg FROM ratings 
                        WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                    ''', (chat_id, film_id))
                    avg_result = cursor.fetchone()
                    avg_rating = None
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
                    if active_users and active_users.issubset(rated_users) and avg_rating is not None:
                        # Все активные пользователи оценили - показываем среднюю оценку
                        rating_int = int(round(avg_rating))
                        if 1 <= rating_int <= 4:
                            emoji = "💩"
                        elif 5 <= rating_int <= 7:
                            emoji = "💬"
                        else:  # 8-10
                            emoji = "🏆"
                        rating_text = f"{emoji} {avg_rating:.0f}/10"
                    else:
                        rating_text = "💬 Оценить"
                
                markup.add(InlineKeyboardButton(rating_text, callback_data=f"rate_film:{kp_id}"))
            
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"seasons_kp:{kp_id}"))
            
            # Получаем message_thread_id из сообщения, если оно есть
            message_thread_id = None
            if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                message_thread_id = call.message.message_thread_id
            
            logger.info(f"[SERIES TRACK] Обновление сообщения: message_id={message_id}, message_thread_id={message_thread_id}")
            try:
                text_msg = f"📺 <b>{title}</b>\n\nВыберите сезон для отметки просмотренных эпизодов:"
                if all_seasons_watched:
                    text_msg += f"\n\n✅ Отлично, все сезоны просмотрены! Оцените сериал"
                if message_thread_id:
                    # Используем API напрямую для поддержки тредов
                    reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                    params = {
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'text': text_msg,
                        'parse_mode': 'HTML',
                        'message_thread_id': message_thread_id
                    }
                    if reply_markup_json:
                        params['reply_markup'] = reply_markup_json
                    bot_instance.api_call('editMessageText', params)
                else:
                    bot_instance.edit_message_text(
                        text_msg,
                        chat_id, message_id, reply_markup=markup, parse_mode='HTML'
                    )
                logger.info(f"[SERIES TRACK] Сообщение обновлено успешно")
            except Exception as e:
                logger.error(f"[SERIES TRACK] Ошибка обновления сообщения: {e}", exc_info=True)
                # При ошибке отправляем новое сообщение
                if message_thread_id:
                    bot_instance.send_message(chat_id, text_msg, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                else:
                    bot_instance.send_message(chat_id, text_msg, reply_markup=markup, parse_mode='HTML')
            bot_instance.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"[SERIES TRACK] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_season:"))
    def series_season_callback(call):
        """Обработчик для выбора сезона и отметки эпизодов"""
        try:
            bot_instance.answer_callback_query(call.id)
            
            parts = call.data.split(":")
            kp_id = parts[1]
            season_num = parts[2]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SERIES SEASON] Выбор сезона: user_id={user_id}, chat_id={chat_id}, kp_id={kp_id}, season={season_num}")
            message_id = call.message.message_id
            
            # Получаем message_thread_id из сообщения, если оно есть
            message_thread_id = None
            if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                message_thread_id = call.message.message_thread_id
            
            # Используем функцию show_episodes_page для отображения эпизодов
            from moviebot.bot.handlers.seasons import show_episodes_page
            if show_episodes_page(kp_id, season_num, chat_id, user_id, page=1, message_id=message_id, message_thread_id=message_thread_id):
                bot_instance.answer_callback_query(call.id)
            else:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка загрузки эпизодов", show_alert=True)
        except Exception as e:
            logger.error(f"[SERIES SEASON] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_subscribe:"))
    def series_subscribe_callback(call):
        """Обработчик подписки на новые серии сериала"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Сразу отвечаем на callback, чтобы убрать "крутилку"
        try:
            bot_instance.answer_callback_query(call.id, text="⏳ Обрабатываю...")
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
                bot_instance.answer_callback_query(
                    call.id, 
                    "🔒 Функционал можно подключить через /payment", 
                    show_alert=True
                )
                return
            
            # Получение film_id и title из БД (добавляем в базу, если нет)
            with db_lock:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    film_id = row[0] if isinstance(row, tuple) else row.get('id')
                    title = row[1] if isinstance(row, tuple) else row.get('title')
                    logger.info(f"[SERIES SUBSCRIBE] Найден сериал: film_id={film_id}, title={title}")
                else:
                    # Сериал не в базе - добавляем через API
                    logger.info(f"[SERIES SUBSCRIBE] Сериал не найден в БД, добавляем через API")
                    from moviebot.bot.handlers.series import ensure_movie_in_database
                    link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                    
                    logger.info(f"[SERIES SUBSCRIBE] Вызываю extract_movie_info для kp_id={kp_id}, link={link}")
                    try:
                        info = extract_movie_info(link)
                        logger.info(f"[SERIES SUBSCRIBE] extract_movie_info завершен, info={'получен' if info else 'None'}")
                    except Exception as api_e:
                        logger.error(f"[SERIES SUBSCRIBE] Ошибка в extract_movie_info: {api_e}", exc_info=True)
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при получении информации о сериале", show_alert=True)
                        return
                    
                    if not info:
                        logger.error(f"[SERIES SUBSCRIBE] Не удалось получить информацию о сериале для kp_id={kp_id}")
                        bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                        return
                    
                    logger.info(f"[SERIES SUBSCRIBE] Информация получена, title={info.get('title', 'N/A')}, is_series={info.get('is_series', False)}")
                    logger.info(f"[SERIES SUBSCRIBE] Вызываю ensure_movie_in_database: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}")
                    try:
                        film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                        logger.info(f"[SERIES SUBSCRIBE] ensure_movie_in_database завершен: film_id={film_id}, was_inserted={was_inserted}")
                    except Exception as db_e:
                        logger.error(f"[SERIES SUBSCRIBE] Ошибка в ensure_movie_in_database: {db_e}", exc_info=True)
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                        return
                    if not film_id:
                        logger.error(f"[SERIES SUBSCRIBE] Не удалось добавить сериал в базу для kp_id={kp_id}")
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                        return
                    
                    title = info.get('title', 'Сериал')
                    logger.info(f"[SERIES SUBSCRIBE] Сериал добавлен/найден в БД: film_id={film_id}, title={title}, was_inserted={was_inserted}")
                    
                    # Если сериал был добавлен, отправляем уведомление
                    if was_inserted:
                        bot_instance.send_message(chat_id, f"✅ Сериал добавлен в базу!")
                        logger.info(f"[SERIES SUBSCRIBE] Уведомление об добавлении отправлено")
            
            # Добавление подписки
            logger.info(f"[SERIES SUBSCRIBE] Добавляю подписку в БД: chat_id={chat_id}, film_id={film_id}, kp_id={kp_id}, user_id={user_id}")
            with db_lock:
                cursor.execute('''
                    INSERT INTO series_subscriptions (chat_id, film_id, kp_id, user_id, subscribed)
                    VALUES (%s, %s, %s, %s, TRUE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET subscribed = TRUE
                ''', (chat_id, film_id, kp_id, user_id))
                conn.commit()
                logger.info(f"[SERIES SUBSCRIBE] Подписка добавлена в БД успешно")
            
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
                            release_date = dt.strptime(release_str, '%Y-%m-%d').replace(tzinfo=pytz.utc)
                            if release_date > dt.now(pytz.utc):
                                if nearest_release_date is None or release_date < nearest_release_date:
                                    nearest_release_date = release_date
                        except:
                            pass
            
            if nearest_release_date:
                next_check_date = nearest_release_date - timedelta(days=1)  # Проверяем за день до выхода
            else:
                next_check_date = dt.now(pytz.utc) + timedelta(weeks=3)  # Если нет дат, проверка через 3 недели
            
            logger.info(f"[SERIES SUBSCRIBE] Постановка задачи проверки на {next_check_date}")
            scheduler.add_job(
                check_series_for_new_episodes,
                'date',
                run_date=next_check_date,
                args=[kp_id, film_id, chat_id, user_id]
            )
            logger.info(f"[SERIES SUBSCRIBE] Задача проверки поставлена успешно")
            
            logger.info(f"[SERIES SUBSCRIBE] Пользователь {user_id} подписался на сериал {title} (kp_id={kp_id})")
            
            # Обновление сообщения - обновляем текст и кнопку подписки (без API запросов)
            logger.info("[SERIES SUBSCRIBE] Прямое обновление текста и кнопки подписки (без API)")
            try:
                # Получаем существующую клавиатуру и текст из сообщения
                old_markup = call.message.reply_markup
                old_text = call.message.text or call.message.caption or ""
                new_markup = InlineKeyboardMarkup()
                
                # Получаем ссылку на Кинопоиск из базы данных
                link = None
                with db_lock:
                    cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    link_row = cursor.fetchone()
                    if link_row:
                        link = link_row[0] if isinstance(link_row, tuple) else link_row.get('link')
                
                # Обновляем текст: заменяем строку со статусом подписки
                new_text = old_text
                # Заменяем статус подписки на "Подписан"
                import re
                new_text = re.sub(
                    r'🔔 <b>Статус подписки: ❌ Не подписан</b>',
                    '🔔 <b>Статус подписки: ✅ Подписан</b>',
                    new_text
                )
                # Если строки со статусом не было, добавляем её в конец
                if 'Статус подписки' not in new_text:
                    new_text += "\n\n🔔 <b>Статус подписки: ✅ Подписан</b>"
                
                # Убеждаемся, что ссылка "Кинопоиск" присутствует в тексте
                if link:
                    # Проверяем, есть ли ссылка в тексте (как HTML или как plain text)
                    if '<a href' not in new_text and 'Кинопоиск' not in new_text:
                        # Если ссылки нет вообще, добавляем её перед статусом подписки
                        new_text = new_text.replace('🔔 <b>Статус подписки:', f'\n<a href="{link}">Кинопоиск</a>\n\n🔔 <b>Статус подписки:')
                    elif 'Кинопоиск' in new_text and '<a href' not in new_text:
                        # Если есть текст "Кинопоиск", но нет HTML-ссылки, заменяем его на ссылку
                        new_text = re.sub(
                            r'Кинопоиск',
                            f'<a href="{link}">Кинопоиск</a>',
                            new_text,
                            count=1
                        )
                    elif '<a href' not in new_text:
                        # Если ссылки нет, добавляем её перед статусом подписки
                        new_text = new_text.replace('🔔 <b>Статус подписки:', f'\n<a href="{link}">Кинопоиск</a>\n\n🔔 <b>Статус подписки:')
                
                # Копируем все кнопки из старой клавиатуры, заменяя только кнопку подписки
                if old_markup and old_markup.keyboard:
                    for row in old_markup.keyboard:
                        new_row = []
                        for button in row:
                            # Проверяем, является ли это кнопкой подписки
                            if button.callback_data and ('series_subscribe:' in button.callback_data or 'series_unsubscribe:' in button.callback_data):
                                # Заменяем на кнопку отписки
                                new_row.append(InlineKeyboardButton(
                                    "🔕 Убрать подписку на новые серии",
                                    callback_data=f"series_unsubscribe:{kp_id}"
                                ))
                            else:
                                # Копируем остальные кнопки как есть
                                new_row.append(button)
                        if new_row:
                            new_markup.row(*new_row)
                else:
                    # Если клавиатуры нет, создаем только кнопку подписки
                    new_markup.add(InlineKeyboardButton(
                        "🔕 Убрать подписку на новые серии",
                        callback_data=f"series_unsubscribe:{kp_id}"
                    ))
                
                # Обновляем текст и клавиатуру
                message_id = call.message.message_id if call.message else None
                message_thread_id = None
                if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                    message_thread_id = call.message.message_thread_id
                
                if message_thread_id:
                    bot_instance.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        text=new_text,
                        reply_markup=new_markup,
                        parse_mode='HTML'
                    )
                else:
                    bot_instance.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=new_text,
                        reply_markup=new_markup,
                        parse_mode='HTML'
                    )
                logger.info("[SERIES SUBSCRIBE] Текст и клавиатура обновлены напрямую (без API)")
            
            except telebot.apihelper.ApiTelegramException as tele_e:
                logger.error(f"[SERIES SUBSCRIBE] Telegram ошибка: {tele_e}", exc_info=True)
                bot_instance.send_message(chat_id, f"🔔 Подписка добавлена на {title}, но карточка не обновилась. Переоткройте.")
            
            except Exception as e:
                logger.error(f"[SERIES SUBSCRIBE] Ошибка обновления: {e}", exc_info=True)
                bot_instance.send_message(chat_id, f"🔔 Подписка добавлена на {title}, но карточка не обновилась. Переоткройте.")
        
        except Exception as e:
            logger.error(f"[SERIES SUBSCRIBE] КРИТИЧЕСКАЯ ошибка в хэндлере: {e}", exc_info=True)
            try:
                bot_instance.send_message(chat_id, "🔔 Подписка добавлена с ошибкой. Попробуйте позже.")
            except:
                pass
        
        finally:
            # answer_callback_query уже вызван в начале, но вызываем еще раз для финального уведомления
            try:
                bot_instance.answer_callback_query(call.id, text="🔔 Подписка добавлена", show_alert=False)
                logger.info(f"[SERIES SUBSCRIBE] Финальный answer_callback_query вызван с id={call.id}")
            except Exception as e:
                logger.warning(f"[SERIES SUBSCRIBE] Не удалось вызвать финальный answer_callback_query: {e}")
            logger.info(f"[SERIES SUBSCRIBE] ===== END: callback_id={call.id}")

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_unsubscribe:"))
    def series_unsubscribe_callback(call):
        """Обработчик отписки от новых серий сериала"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        try:
            logger.info(f"[SERIES UNSUBSCRIBE] ===== START: callback_id={call.id}, user_id={user_id}, chat_id={chat_id}")
            
            data = call.data.split(':')
            kp_id = data[1]
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
            
            # Получение film_id
            with db_lock:
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    logger.error(f"[SERIES UNSUBSCRIBE] Сериал не найден для kp_id={kp_id}")
                    raise ValueError("Сериал не найден в БД")
                
                film_id = row[0] if isinstance(row, tuple) else row.get('id')
                title = row[1] if isinstance(row, tuple) else row.get('title')
                logger.info(f"[SERIES UNSUBSCRIBE] Найден сериал: film_id={film_id}, title={title}")
                
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
            
            # Обновление сообщения - обновляем текст и кнопку подписки (без API запросов)
            logger.info("[SERIES UNSUBSCRIBE] Прямое обновление текста и кнопки подписки (без API)")
            try:
                # Получаем существующую клавиатуру и текст из сообщения
                old_markup = call.message.reply_markup
                old_text = call.message.text or call.message.caption or ""
                new_markup = InlineKeyboardMarkup()
                
                # Получаем ссылку на Кинопоиск из базы данных
                link = None
                with db_lock:
                    cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    link_row = cursor.fetchone()
                    if link_row:
                        link = link_row[0] if isinstance(link_row, tuple) else link_row.get('link')
                
                # Обновляем текст: заменяем строку со статусом подписки
                new_text = old_text
                # Заменяем статус подписки на "Не подписан"
                import re
                new_text = re.sub(
                    r'🔔 <b>Статус подписки: ✅ Подписан</b>',
                    '🔔 <b>Статус подписки: ❌ Не подписан</b>',
                    new_text
                )
                # Если строки со статусом не было, добавляем её в конец
                if 'Статус подписки' not in new_text:
                    new_text += "\n\n🔔 <b>Статус подписки: ❌ Не подписан</b>"
                
                # Убеждаемся, что ссылка "Кинопоиск" присутствует в тексте
                if link:
                    # Проверяем, есть ли ссылка в тексте (как HTML или как plain text)
                    if '<a href' not in new_text and 'Кинопоиск' not in new_text:
                        # Если ссылки нет вообще, добавляем её перед статусом подписки
                        new_text = new_text.replace('🔔 <b>Статус подписки:', f'\n<a href="{link}">Кинопоиск</a>\n\n🔔 <b>Статус подписки:')
                    elif 'Кинопоиск' in new_text and '<a href' not in new_text:
                        # Если есть текст "Кинопоиск", но нет HTML-ссылки, заменяем его на ссылку
                        new_text = re.sub(
                            r'Кинопоиск',
                            f'<a href="{link}">Кинопоиск</a>',
                            new_text,
                            count=1
                        )
                    elif '<a href' not in new_text:
                        # Если ссылки нет, добавляем её перед статусом подписки
                        new_text = new_text.replace('🔔 <b>Статус подписки:', f'\n<a href="{link}">Кинопоиск</a>\n\n🔔 <b>Статус подписки:')
                
                # Копируем все кнопки из старой клавиатуры, заменяя только кнопку подписки
                if old_markup and old_markup.keyboard:
                    for row in old_markup.keyboard:
                        new_row = []
                        for button in row:
                            # Проверяем, является ли это кнопкой подписки
                            if button.callback_data and ('series_subscribe:' in button.callback_data or 'series_unsubscribe:' in button.callback_data):
                                # Заменяем на кнопку подписки
                                new_row.append(InlineKeyboardButton(
                                    "🔔 Подписаться на новые серии",
                                    callback_data=f"series_subscribe:{kp_id}"
                                ))
                            else:
                                # Копируем остальные кнопки как есть
                                new_row.append(button)
                        if new_row:
                            new_markup.row(*new_row)
                else:
                    # Если клавиатуры нет, создаем только кнопку подписки
                    new_markup.add(InlineKeyboardButton(
                        "🔔 Подписаться на новые серии",
                        callback_data=f"series_subscribe:{kp_id}"
                    ))
                
                # Обновляем текст и клавиатуру
                message_id = call.message.message_id if call.message else None
                message_thread_id = None
                if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
                    message_thread_id = call.message.message_thread_id
                
                if message_thread_id:
                    bot_instance.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        message_thread_id=message_thread_id,
                        text=new_text,
                        reply_markup=new_markup,
                        parse_mode='HTML'
                    )
                else:
                    bot_instance.edit_message_text(
                        chat_id=chat_id,
                        message_id=message_id,
                        text=new_text,
                        reply_markup=new_markup,
                        parse_mode='HTML'
                    )
                logger.info("[SERIES UNSUBSCRIBE] Текст и клавиатура обновлены напрямую (без API)")
            
            except telebot.apihelper.ApiTelegramException as tele_e:
                logger.error(f"[SERIES UNSUBSCRIBE] Telegram ошибка: {tele_e}", exc_info=True)
                bot_instance.send_message(chat_id, f"🔕 Отписка выполнена от {title}, но карточка не обновилась. Переоткройте.")
            
            except Exception as e:
                logger.error(f"[SERIES UNSUBSCRIBE] Ошибка обновления: {e}", exc_info=True)
                bot_instance.send_message(chat_id, f"🔕 Отписка выполнена от {title}, но карточка не обновилась. Переоткройте.")
        
        except Exception as e:
            logger.error(f"[SERIES UNSUBSCRIBE] КРИТИЧЕСКАЯ ошибка в хэндлере: {e}", exc_info=True)
            try:
                bot_instance.send_message(chat_id, "🔕 Отписка выполнена с ошибкой. Попробуйте позже.")
            except:
                pass
        
        finally:
            try:
                bot_instance.answer_callback_query(call.id, text="🔕 Отписка выполнена")
                logger.info(f"[SERIES UNSUBSCRIBE] answer_callback_query вызван с id={call.id}")
            except Exception as e:
                logger.error(f"[ANSWER CALLBACK] Ошибка: {e}")

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
            
            # Получаем film_id (добавляем сериал в базу, если его еще нет)
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                
            film_id = None
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                
            # Если сериала нет в базе, добавляем его
            if not film_id:
                from moviebot.bot.handlers.series import ensure_movie_in_database
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
                info = extract_movie_info(link)
                if info:
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                    if was_inserted:
                        logger.info(f"[EPISODE TOGGLE] Сериал добавлен в базу при отметке эпизода: kp_id={kp_id}, film_id={film_id}")
                    if not film_id:
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                        return
                else:
                    bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                    return
            
            # Проверяем текущий статус и переключаем
            with db_lock:
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
                    # Добавляем отметку эпизода
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
        try:
            bot_instance.answer_callback_query(call.id)
            # TODO: Вызвать функцию показа списка сериалов
            logger.info(f"[EPISODES BACK] Возврат к списку сериалов")
        except Exception as e:
            logger.error(f"[EPISODES BACK] Ошибка: {e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("rate_film:"))
    def rate_film_callback(call):
        """Обработчик кнопки 'Оценить'"""
        try:
            kp_id = call.data.split(":")[1]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[RATE FILM] Пользователь {user_id} хочет оценить фильм kp_id={kp_id}")
            
            # Проверяем, есть ли фильм в базе
            with db_lock:
                cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
            
            film_id = None
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
            
            # Если фильма нет в базе, просто отправляем сообщение с просьбой оценить
            # Фильм будет добавлен в базу только при успешной оценке (через handle_rating_internal)
            if not film_id:
                # Получаем информацию о фильме для отображения названия
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                title = info.get('title', 'Фильм') if info else 'Фильм'
                
                # Отправляем сообщение с просьбой оценить и добавляем его в rating_messages с kp_id
                # Используем специальный формат для хранения kp_id вместо film_id
                msg = bot_instance.reply_to(call.message, f"💬 Чтобы оценить фильм *{title}*, ответьте на это сообщение числом от 1 до 10.\n\n<i>Фильм будет добавлен в базу при успешной оценке.</i>", parse_mode='Markdown')
                # Сохраняем kp_id в rating_messages с префиксом "kp_id:" для идентификации
                rating_messages[msg.message_id] = f"kp_id:{kp_id}"
                logger.info(f"[RATE FILM] Сообщение {msg.message_id} добавлено в rating_messages для kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id)
                return
            
            # Проверяем, есть ли уже оценка
            with db_lock:
                cursor.execute('''
                    SELECT rating FROM ratings 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id, user_id))
                existing_rating = cursor.fetchone()
                
                if existing_rating:
                    rating = existing_rating.get('rating') if isinstance(existing_rating, dict) else existing_rating[0]
                    bot_instance.reply_to(call.message, f"✅ Вы уже оценили этот фильм: {rating}/10\n\nЧтобы изменить оценку, ответьте на сообщение с фильмом числом от 1 до 10.")
                else:
                    # Отправляем сообщение с просьбой оценить и добавляем его в rating_messages
                    msg = bot_instance.reply_to(call.message, f"💬 Чтобы оценить фильм *{title}*, ответьте на это сообщение числом от 1 до 10.", parse_mode='Markdown')
                    # Добавляем сообщение в rating_messages, чтобы при ответе можно было найти film_id
                    rating_messages[msg.message_id] = film_id
                    logger.info(f"[RATE FILM] Сообщение {msg.message_id} добавлено в rating_messages для film_id={film_id}")
        except Exception as e:
            logger.error(f"[RATE FILM] Ошибка: {e}", exc_info=True)
        finally:
            # ВСЕГДА отвечаем на callback!
            try:
                bot_instance.answer_callback_query(call.id)
            except Exception as answer_e:
                logger.error(f"[RATE FILM] Не удалось ответить на callback: {answer_e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("show_facts:") or call.data.startswith("facts:"))
    def facts_callback(call):
        """Обработчик кнопки 'Интересные факты'"""
        try:
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[FACTS] Пользователь {user_id} запросил факты для kp_id={kp_id}")
            
            # Получаем факты
            facts = get_facts(kp_id)
            if facts:
                bot_instance.send_message(chat_id, facts, parse_mode='HTML')
                bot_instance.answer_callback_query(call.id, "Факты отправлены")
            else:
                bot_instance.answer_callback_query(call.id, "Факты не найдены", show_alert=True)
        except Exception as e:
            logger.error(f"[FACTS] Ошибка: {e}", exc_info=True)
        finally:
            # ВСЕГДА отвечаем на callback!
            try:
                bot_instance.answer_callback_query(call.id)
            except Exception as answer_e:
                logger.error(f"[FACTS] Не удалось ответить на callback: {answer_e}", exc_info=True)

    # Обработчик plan_from_added перенесен в moviebot/bot/handlers/plan.py
    # чтобы избежать конфликтов с дублирующим обработчиком
