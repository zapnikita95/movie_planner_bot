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
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        try:
            logger.info(f"[SERIES TRACK] ===== START: callback_id={call.id}, user_id={user_id}, chat_id={chat_id}")
            
            data = call.data.split(':')
            kp_id = data[1]
            logger.info(f"[SERIES TRACK] Парсинг данных: kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")
            
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
            
            # TODO: Перенести остальную логику из moviebot.py (строки 16401-16600)
            # Пока просто отвечаем на callback
            bot_instance.answer_callback_query(call.id, "✅ Функция в разработке")
            
        except Exception as e:
            logger.error(f"[SERIES TRACK] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("series_subscribe:"))
    def series_subscribe_callback(call):
        """Обработчик подписки на новые серии сериала"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
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
                    info = extract_movie_info(link)
                    if not info:
                        logger.error(f"[SERIES SUBSCRIBE] Не удалось получить информацию о сериале для kp_id={kp_id}")
                        bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                        return
                    
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                    if not film_id:
                        logger.error(f"[SERIES SUBSCRIBE] Не удалось добавить сериал в базу для kp_id={kp_id}")
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении сериала в базу", show_alert=True)
                        return
                    
                    title = info.get('title', 'Сериал')
                    
                    # Если сериал был добавлен, отправляем уведомление
                    if was_inserted:
                        bot_instance.send_message(chat_id, f"✅ Сериал добавлен в базу!")
                        logger.info(f"[SERIES SUBSCRIBE] Сериал добавлен в базу: film_id={film_id}, title={title}")
            
            # Добавление подписки
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
            try:
                bot_instance.answer_callback_query(call.id, text="🔔 Подписка добавлена")
                logger.info(f"[SERIES SUBSCRIBE] answer_callback_query вызван с id={call.id}")
            except Exception as e:
                logger.error(f"[ANSWER CALLBACK] Ошибка: {e}")

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
            
            # Получаем film_id по kp_id (добавляем в базу, если нет)
            from moviebot.bot.handlers.series import ensure_movie_in_database
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            info = extract_movie_info(link)
            if not info:
                logger.error(f"[RATE FILM] Не удалось получить информацию о фильме для kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            if not film_id:
                logger.error(f"[RATE FILM] Не удалось добавить фильм в базу для kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                return
            
            title = info.get('title', 'Фильм')
            
            # Если фильм был добавлен, отправляем уведомление
            if was_inserted:
                bot_instance.send_message(chat_id, f"✅ Фильм добавлен в базу!")
                logger.info(f"[RATE FILM] Фильм добавлен в базу: film_id={film_id}, title={title}")
            
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

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("plan_from_added:") or call.data.startswith("plan_film:"))
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
            
            bot_instance.answer_callback_query(call.id, "Выберите тип просмотра")
            bot_instance.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
        except Exception as e:
            logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
        finally:
            # ВСЕГДА отвечаем на callback!
            try:
                bot_instance.answer_callback_query(call.id)
            except Exception as answer_e:
                logger.error(f"[PLAN FROM ADDED] Не удалось ответить на callback: {answer_e}", exc_info=True)
