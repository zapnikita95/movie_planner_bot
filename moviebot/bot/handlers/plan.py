from moviebot.bot.bot_init import bot
"""
Обработчики команд /plan и /schedule
"""
import logging
import re
import pytz
from datetime import datetime, timedelta

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.database.db_operations import log_request, get_user_timezone_or_default

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.api.kinopoisk_api import extract_movie_info, get_seasons_data

from moviebot.utils.parsing import parse_session_time, check_timezone_change, extract_kp_id_from_text, show_timezone_selection

from moviebot.states import (

    user_plan_state, plan_notification_messages, plan_error_messages,
    bot_messages
)
from moviebot.config import MONTHS_MAP, DAYS_FULL


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

# Используем DAYS_FULL из config
days_full = DAYS_FULL
months_map = MONTHS_MAP


def process_plan(bot, user_id, chat_id, link, plan_type, day_or_date, message_date_utc=None):
    """
    Планирует просмотр фильма. Возвращает True при успехе, False при ошибке, 
    'NEEDS_TIMEZONE' если нужно уточнить часовой пояс.
    message_date_utc - время сообщения в UTC для определения часового пояса
    """
    # TODO: Извлечь полную реализацию из moviebot.py строки 22844-23279
    # Это большая функция, нужно скопировать весь код
    plan_dt = None
    needs_tz_check = False
    
    # Проверяем, нужно ли уточнить часовой пояс (НО не прерываем текущее планирование)
    if message_date_utc:
        needs_tz_check = check_timezone_change(user_id, message_date_utc)
    
    # Используем часовой пояс пользователя или по умолчанию Москва
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)
    
    # Сначала пробуем использовать parse_session_time
    parsed_dt = parse_session_time(day_or_date, user_tz)
    if parsed_dt:
        plan_dt = parsed_dt
        logger.info(f"[PROCESS_PLAN] Использован parse_session_time: {plan_dt}")
    else:
        # TODO: Добавить полную логику парсинга дат из moviebot.py
        # Это очень большая функция, нужно скопировать весь код
        logger.warning(f"[PROCESS_PLAN] parse_session_time не сработал, нужна полная реализация")
        return False
    
    if not plan_dt:
        return False
    
    # Извлекаем kp_id из ссылки
    match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', link)
    kp_id = match.group(2) if match else None
    is_series_from_link = match.group(1) == 'series' if match else False
    
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        with db_lock:
            if kp_id:
                cursor_local.execute('SELECT id, title, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
            else:
                cursor_local.execute('SELECT id, title, is_series FROM movies WHERE chat_id = %s AND link = %s', (chat_id, link))
            row = cursor_local.fetchone()
            if not row:
                # При планировании фильма автоматически добавляем его в базу
                info = extract_movie_info(link)
                if info:
                    is_series_val = 1 if info.get('is_series') else 0
                    kp_id_from_info = str(info.get('kp_id', kp_id))
                    # Обновляем kp_id из info, если он есть
                    if kp_id_from_info:
                        kp_id = kp_id_from_info
                    cursor_local.execute('INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series', 
                                 (chat_id, link, kp_id, info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors'], is_series_val))
                    conn_local.commit()
                    cursor_local.execute('SELECT id, title, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                    row = cursor_local.fetchone()
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        is_series_db = bool(row.get('is_series') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0))
                        logger.info(f"[PROCESS_PLAN] Фильм автоматически добавлен в базу при планировании: kp_id={kp_id}, film_id={film_id}, is_series={is_series_db}")
                    else:
                        bot.send_message(chat_id, "Не удалось добавить фильм в базу.")
                        return False
                else:
                    bot.send_message(chat_id, "Не удалось извлечь информацию о фильме.")
                    return False
            else:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
                is_series_db = bool(row.get('is_series') if isinstance(row, dict) else (row[2] if len(row) > 2 else 0))
                # Получаем kp_id из базы, если он еще не был извлечен из ссылки
                if not kp_id:
                    cursor_local.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    kp_row = cursor_local.fetchone()
                    if kp_row:
                        kp_id = str(kp_row.get('kp_id') if isinstance(kp_row, dict) else kp_row[0])
            
            # TODO: Добавить обработку сериалов (episode_info) из moviebot.py строки 23196-23274
            
            plan_utc = plan_dt.astimezone(pytz.utc)
            cursor_local.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
                          (chat_id, film_id, plan_type, plan_utc, user_id))
            plan_id_row = cursor_local.fetchone()
            plan_id = plan_id_row.get('id') if isinstance(plan_id_row, dict) else plan_id_row[0] if plan_id_row else None
            
            # ВАЖНО: Получаем kp_id из базы, чтобы быть уверенными, что он правильный
            if film_id:
                cursor_local.execute('SELECT kp_id, is_series FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                movie_row = cursor_local.fetchone()
                if movie_row:
                    kp_id_from_db = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
                    is_series_db = bool(movie_row.get('is_series') if isinstance(movie_row, dict) else (movie_row[1] if len(movie_row) > 1 else 0))
                    if kp_id_from_db:
                        kp_id = str(kp_id_from_db)
                        logger.info(f"[PROCESS PLAN] kp_id получен из базы: {kp_id}, is_series={is_series_db}")
                    elif not kp_id:
                        logger.warning(f"[PROCESS PLAN] kp_id не найден в базе для film_id={film_id}")
                else:
                    logger.warning(f"[PROCESS PLAN] Фильм не найден в базе для film_id={film_id}")
            
            conn_local.commit()
        
        # Успешное планирование - фильм уже в базе (film_id получен выше)
        logger.info(f"[PLAN] Успешное планирование: plan_id={plan_id}, film_id={film_id}, kp_id={kp_id}, plan_type={plan_type}, plan_datetime={plan_utc}")
    except Exception as e:
        logger.error(f"[PROCESS_PLAN] Ошибка при планировании: {e}", exc_info=True)
        return False
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass
    
    # Если нужно уточнить часовой пояс, показываем выбор, но текущее планирование уже завершено
    if needs_tz_check:
        try:
            show_timezone_selection(chat_id, user_id, "Для будущих планов выберите часовой пояс:")
        except Exception as tz_e:
            logger.warning(f"[PROCESS_PLAN] Не удалось показать выбор часового пояса: {tz_e}")

    # Формируем сообщение об успехе
    date_str = plan_dt.strftime('%d.%m %H:%M')
    type_text = "дома" if plan_type == 'home' else "в кино"
    
    # Проверяем доступ к билетам для кнопки "Добавить билеты" (только для планов в кино)
    from moviebot.utils.helpers import has_tickets_access
    markup = InlineKeyboardMarkup()
    sources = None  # Инициализируем переменную для источников
    
    if plan_type == 'cinema' and plan_id:
        if has_tickets_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("🎟️ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
        else:
            markup.add(InlineKeyboardButton("🔒 Добавить билеты", callback_data=f"ticket_locked:{plan_id}"))
    elif plan_type == 'home' and plan_id and kp_id:
        # ОПТИМИЗАЦИЯ: Загружаем источники асинхронно, не блокируя отправку сообщения
        # Это экономит 1-3 секунды на запросе к API
        import threading
        import json
        from moviebot.api.kinopoisk_api import get_external_sources
        
        sources = None
        sources_loaded = False
        
        def load_sources_async():
            """Загружает источники в фоне и обновляет план в БД"""
            nonlocal sources, sources_loaded
            try:
                sources = get_external_sources(kp_id)
                sources_loaded = True
                if sources:
                    # Сохраняем источники в базу для будущего использования
                    sources_dict = {platform: url for platform, url in sources[:6]}
                    sources_json = json.dumps(sources_dict, ensure_ascii=False)
                    # Используем отдельное соединение для фонового потока
                    conn_sources = get_db_connection()
                    cursor_sources = get_db_cursor()
                    try:
                        with db_lock:
                            cursor_sources.execute('''
                                UPDATE plans 
                                SET ticket_file_id = %s 
                                WHERE id = %s
                            ''', (sources_json, plan_id))
                            conn_sources.commit()
                    finally:
                        try:
                            cursor_sources.close()
                        except:
                            pass
                        try:
                            conn_sources.close()
                        except:
                            pass
                    logger.info(f"[PROCESS PLAN] Найдено {len(sources)} источников для kp_id={kp_id} (загружено в фоне)")
                else:
                    logger.info(f"[PROCESS PLAN] Источники не найдены для kp_id={kp_id}")
            except Exception as e:
                logger.warning(f"[PROCESS PLAN] Ошибка загрузки источников в фоне: {e}", exc_info=True)
        
        # Запускаем загрузку источников в фоне
        sources_thread = threading.Thread(target=load_sources_async, daemon=True)
        sources_thread.start()
        logger.info(f"[PROCESS PLAN] Загрузка источников запущена в фоне для kp_id={kp_id}")
        
        # Создаем пустую разметку - источники добавятся позже, если загрузятся
        markup = InlineKeyboardMarkup()
    
    # Добавляем кнопку "Вернуться к описанию" для обоих типов планов (если есть kp_id)
    # Для планов "в кино" добавляем обе кнопки: "Добавить билеты" и "Вернуться к описанию"
    if kp_id:
        try:
            kp_id_int = int(kp_id)
            if not markup.keyboard:
                markup = InlineKeyboardMarkup(row_width=1)
            # Для планов "в кино" кнопка "Добавить билеты" уже добавлена выше, добавляем "Вернуться к описанию"
            markup.add(
                InlineKeyboardButton(
                    "◀️ Вернуться к описанию",
                    callback_data=f"back_to_film:{kp_id_int}"
                )
            )
            logger.info(f"[PROCESS PLAN] Добавлена кнопка 'Вернуться к описанию' с kp_id={kp_id_int}")
        except (ValueError, TypeError) as e:
            logger.warning(f"[PROCESS PLAN] Не удалось преобразовать kp_id в int: {kp_id}, ошибка: {e}")
    
    text = f"✅ <b>{title}</b> запланирован на {date_str} {type_text}"
    if plan_type == 'home' and sources:
        text += f"\n\n📺 <b>Онлайн-кинотеатры для просмотра:</b>"
    
    bot.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup if markup.keyboard else None)
    
    # Очищаем состояние планирования после успешного завершения
    if user_id in user_plan_state:
        del user_plan_state[user_id]
        logger.info(f"[PROCESS PLAN] Состояние планирования очищено для user_id={user_id}")
    
    return True


def register_plan_handlers(bot):
    """Регистрирует обработчики команд /plan и /schedule"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER PLAN HANDLERS] ===== START: регистрация обработчиков планирования =====")
    
    @bot.message_handler(commands=['plan'], func=lambda m: not m.reply_to_message)
    def plan_handler(message):
        """Команда /plan - планирование просмотра (только чистая команда без реплая)"""
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
            text = re.sub(r'/plan(@\w+)?\s*', '', text, flags=re.IGNORECASE).strip()
            
            logger.info(f"[PLAN] ===== НАЧАЛО ОБРАБОТКИ /plan =====")
            logger.info(f"[PLAN] user_id={user_id}, chat_id={chat_id}")
            logger.info(f"[PLAN] original_text='{original_text}'")
            
            # Проверяем реплай на сообщение со ссылкой
            link = None
            if message.reply_to_message:
                reply_msg = message.reply_to_message
                reply_msg_id = reply_msg.message_id
                
                # 1. Проверяем bot_messages и plan_notification_messages
                link = bot_messages.get(reply_msg_id)
                if not link:
                    plan_data = plan_notification_messages.get(reply_msg_id)
                    if plan_data:
                        link = plan_data.get('link')
                
                # 2. Ищем ссылку в тексте сообщения
                if not link:
                    reply_text = reply_msg.text or ''
                    link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', reply_text)
                    if link_match:
                        link = link_match.group(1)
                
                # 3. Проверяем entities сообщения (URL entities)
                if not link and reply_msg.entities:
                    for entity in reply_msg.entities:
                        if entity.type == 'text_link':
                            if hasattr(entity, 'url') and entity.url:
                                url = entity.url
                                if 'kinopoisk.ru' in url and ('/film/' in url or '/series/' in url):
                                    link = url
                                    break
                        elif entity.type == 'url':
                            if reply_msg.text:
                                url = reply_msg.text[entity.offset:entity.offset + entity.length]
                                if 'kinopoisk.ru' in url and ('/film/' in url or '/series/' in url):
                                    link = url
                                    break
            
            # Ищем ссылку в тексте команды
            if not link:
                link_match = re.search(r'(https?://[\w\./-]*kinopoisk\.ru/(film|series)/\d+)', original_text)
                link = link_match.group(1) if link_match else None
            
            # Если ссылка найдена в тексте команды, извлекаем оставшийся текст
            if link and original_text:
                remaining_text = original_text.replace('/plan', '').replace(link, '').strip().lower()
                if remaining_text:
                    text = remaining_text
            
            # Ищем ID кинопоиска
            kp_id = None
            if not link:
                id_match = re.search(r'^(\d+)', text.strip())
                if id_match:
                    kp_id = id_match.group(1)
                    with db_lock:
                        cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                        row = cursor.fetchone()
                        if row:
                            link = row.get('link') if isinstance(row, dict) else row[0]
                        else:
                            link = f"https://kinopoisk.ru/film/{kp_id}"
            
            plan_type = 'home' if 'дома' in text else 'cinema' if 'кино' in text else None
            logger.info(f"[PLAN] plan_type={plan_type}, text={text}")
            
            day_or_date = None
            
            # Сначала ищем день недели
            sorted_phrases = sorted(days_full.keys(), key=len, reverse=True)
            for phrase in sorted_phrases:
                if phrase in text:
                    day_or_date = phrase
                    break
            
            # Если день недели не найден, ищем дату
            if not day_or_date:
                if 'завтра' in text:
                    day_or_date = 'завтра'
                elif 'следующая неделя' in text or 'след неделя' in text or 'след. неделя' in text:
                    day_or_date = 'следующая неделя'
                else:
                    month_match = re.search(r'в\s+([а-яё]+)', text)
                    if month_match:
                        month_str = month_match.group(1)
                        if month_str.lower() in months_map:
                            day_or_date = f"в {month_str}"
            
            # Если специальные форматы не найдены, пробуем другие форматы
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
            
            # Проверяем, есть ли отдельно указанное время
            if day_or_date and plan_type == 'cinema':
                time_match = re.search(r'\b(\d{1,2})[: ](\d{1,2})\b', text)
                if time_match and ':' not in day_or_date and ' ' not in day_or_date.split()[-1]:
                    hour = int(time_match.group(1))
                    minute = int(time_match.group(2))
                    if 0 <= hour <= 23 and 0 <= minute <= 59:
                        day_or_date = f"{day_or_date} {hour}:{minute}"
            
            logger.info(f"[PLAN] link={link}, plan_type={plan_type}, day_or_date={day_or_date}")
            
            if link and plan_type and day_or_date:
                try:
                    # Получаем message_date_utc для проверки часового пояса
                    from datetime import datetime as dt
                    import pytz
                    message_date_utc = dt.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
                    
                    result = process_plan(bot, user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
                    if result == 'NEEDS_TIMEZONE':
                        # Сохраняем введенный текст в состоянии планирования для продолжения после выбора часового пояса
                        state = user_plan_state.get(user_id, {})
                        state['pending_text'] = message.text.strip()
                        state['pending_plan_dt'] = day_or_date
                        state['pending_message_date_utc'] = message_date_utc
                        state['link'] = link
                        state['plan_type'] = plan_type
                        state['chat_id'] = chat_id
                        user_plan_state[user_id] = state
                        logger.info(f"[PLAN] Сохранен текст для продолжения планирования: '{state['pending_text']}'")
                        show_timezone_selection(chat_id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
                        # НЕ удаляем состояние - оно нужно для продолжения планирования после выбора часового пояса
                except Exception as e:
                    bot.reply_to(message, f"Ошибка при планировании: {e}")
                    logger.error(f"Ошибка process_plan: {e}", exc_info=True)
                    return
                return
            
            # Если нет ссылки, отправляем новое сообщение
            if not link:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("❌ Выйти", callback_data="plan:cancel"))
                prompt_msg = bot.reply_to(message, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!", reply_markup=markup)
                user_plan_state[user_id] = {'step': 1, 'chat_id': chat_id, 'prompt_message_id': prompt_msg.message_id}
                logger.info(f"[PLAN] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id}")
                return
            
            if not plan_type:
                error_msg = bot.reply_to(message, "Не указан тип просмотра (дома/кино).")
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': None,
                    'day_or_date': None,
                    'missing': 'plan_type'
                }
                user_plan_state[user_id] = {'step': 2, 'link': link, 'chat_id': chat_id}
                return
            
            if not day_or_date:
                error_msg = bot.reply_to(message, "Не указан день или дата просмотра.")
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': plan_type,
                    'day_or_date': None,
                    'missing': 'day_or_date'
                }
                user_plan_state[user_id] = {'step': 3, 'link': link, 'plan_type': plan_type, 'chat_id': chat_id}
                return
        except Exception as e:
            logger.error(f"❌ Ошибка в /plan: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /plan")
            except:
                pass

def show_schedule(message):
    """Команда /schedule - показ расписания"""
    logger.info(f"[SCHEDULE COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[SCHEDULE COMMAND] /schedule вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/schedule', message.chat.id)
        logger.info(f"Команда /schedule от пользователя {message.from_user.id}")
        
        chat_id = message.chat.id
        user_id = message.from_user.id
        user_tz = get_user_timezone_or_default(user_id)
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        with db_lock:
            cursor_local.execute('''
                SELECT p.id, m.title, m.kp_id, m.link, p.plan_datetime, p.plan_type,
                       CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as has_ticket,
                       m.watched
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND m.watched = 0 AND p.film_id IS NOT NULL
                ORDER BY p.plan_type DESC, p.plan_datetime ASC
            ''', (chat_id,))
            rows = cursor_local.fetchall()
        
        if not rows:
            empty_markup = InlineKeyboardMarkup(row_width=1)
            empty_markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
            empty_markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
            empty_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            bot.reply_to(
                message,
                "В расписании нет фильмов, используйте /search, чтобы найти и добавить фильмы или сериалы, посмотрите, какие премьеры сейчас идут в кино, или просто пришлите ссылку на Кинопоиск на фильм или сериал",
                reply_markup=empty_markup
            )
            return
        
        # Разделяем на секции: сначала кино, потом дома
        cinema_plans = []
        home_plans = []
        
        for row in rows:
            if isinstance(row, dict):
                plan_id = row.get('id')
                title = row.get('title')
                kp_id = row.get('kp_id')
                link = row.get('link')
                plan_dt_value = row.get('plan_datetime')
                plan_type = row.get('plan_type')
                has_ticket = row.get('has_ticket', 0)
            else:
                plan_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None)
                title = row[1]
                kp_id = row[2]
                link = row[3]
                plan_dt_value = row[4]
                plan_type = row[5]
                has_ticket = row[6] if len(row) > 6 else 0
            
            # Преобразуем TIMESTAMP в дату в часовом поясе пользователя
            try:
                if isinstance(plan_dt_value, datetime):
                    if plan_dt_value.tzinfo is None:
                        plan_dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                    else:
                        plan_dt = plan_dt_value.astimezone(user_tz)
                elif isinstance(plan_dt_value, str):
                    plan_dt_iso = plan_dt_value
                    if plan_dt_iso.endswith('Z'):
                        plan_dt = datetime.fromisoformat(plan_dt_iso.replace('Z', '+00:00')).astimezone(user_tz)
                    elif '+' in plan_dt_iso or plan_dt_iso.count('-') > 2:
                        plan_dt = datetime.fromisoformat(plan_dt_iso).astimezone(user_tz)
                    else:
                        plan_dt = datetime.fromisoformat(plan_dt_iso + '+00:00').astimezone(user_tz)
                else:
                    logger.warning(f"Неожиданный тип plan_datetime: {type(plan_dt_value)}")
                    continue
                
                date_str = plan_dt.strftime('%d.%m %H:%M')
                plan_info = (plan_id, title, kp_id, link, date_str, has_ticket)
                
                if plan_type == 'cinema':
                    cinema_plans.append(plan_info)
                else:  # home
                    home_plans.append(plan_info)
            except Exception as e:
                logger.error(f"Ошибка при обработке даты {plan_dt_value}: {e}")
                if isinstance(plan_dt_value, str):
                    date_str = plan_dt_value[:10] if len(plan_dt_value) >= 10 else plan_dt_value
                else:
                    date_str = datetime.now(user_tz).strftime('%d.%m')
                plan_info = (plan_id, title, kp_id, link, date_str, has_ticket)
                
                if plan_type == 'cinema':
                    cinema_plans.append(plan_info)
                else:  # home
                    home_plans.append(plan_info)
        
        # Отправляем два отдельных сообщения: одно для кино, другое для дома
        cinema_message_id = None
        home_message_id = None
        
        # Сообщение 1: Премьеры в кино
        if cinema_plans:
            cinema_markup = InlineKeyboardMarkup(row_width=1)
            for plan_id, title, kp_id, link, date_str, has_ticket in cinema_plans:
                ticket_emoji = "🎟️ " if has_ticket else ""
                button_text = f"{ticket_emoji}{title} | {date_str}"
                
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                cinema_markup.add(InlineKeyboardButton(button_text, callback_data=f"back_to_film:{int(kp_id)}"))
            
            if not home_plans:
                cinema_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data=f"schedule_back:{chat_id}"))
            
            cinema_text = "🎬 <b>Премьеры в кино:</b>\n\n"
            for plan_id, title, kp_id, link, date_str, has_ticket in cinema_plans:
                ticket_emoji = "🎟️ " if has_ticket else ""
                cinema_text += f"{ticket_emoji}<b>{title}</b> — {date_str}\n"
            
            cinema_msg = bot.reply_to(message, cinema_text, reply_markup=cinema_markup, parse_mode='HTML')
            cinema_message_id = cinema_msg.message_id
        
        # Сообщение 2: Просмотры дома
        if home_plans:
            home_markup = InlineKeyboardMarkup(row_width=1)
            for plan_id, title, kp_id, link, date_str, has_ticket in home_plans:
                button_text = f"{title} | {date_str}"
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                home_markup.add(InlineKeyboardButton(button_text, callback_data=f"back_to_film:{int(kp_id)}"))
            
            home_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data=f"schedule_back:{chat_id}"))
            
            home_text = "🏠 <b>Просмотры дома:</b>\n\n"
            for plan_id, title, kp_id, link, date_str, has_ticket in home_plans:
                home_text += f"<b>{title}</b> — {date_str}\n"
            
            if cinema_plans:
                home_msg = bot.send_message(chat_id, home_text, reply_markup=home_markup, parse_mode='HTML')
            else:
                home_msg = bot.reply_to(message, home_text, reply_markup=home_markup, parse_mode='HTML')
            home_message_id = home_msg.message_id
        
        # Сохраняем message_id обоих сообщений для удаления при нажатии "Назад"
        if cinema_message_id and home_message_id:
            if not hasattr(show_schedule, '_schedule_messages'):
                show_schedule._schedule_messages = {}
            show_schedule._schedule_messages[chat_id] = {
                'cinema_message_id': cinema_message_id,
                'home_message_id': home_message_id
            }
        elif cinema_message_id:
            if not hasattr(show_schedule, '_schedule_messages'):
                show_schedule._schedule_messages = {}
            show_schedule._schedule_messages[chat_id] = {
                'cinema_message_id': cinema_message_id,
                'home_message_id': None
            }
        elif home_message_id:
            if not hasattr(show_schedule, '_schedule_messages'):
                show_schedule._schedule_messages = {}
            show_schedule._schedule_messages[chat_id] = {
                'cinema_message_id': None,
                'home_message_id': home_message_id
            }
        
        logger.info(f"✅ Ответ на /schedule отправлен пользователю {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /schedule (внешний блок): {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /schedule")
        except:
            pass

    @bot.message_handler(commands=['schedule'])
    def _show_schedule_handler(message):
        """Обертка для регистрации команды /schedule"""
        show_schedule(message)

    # Обработчик show_film_description удален - теперь используется единый back_to_film_description из film_callbacks.py
    # Все кнопки теперь используют callback_data="back_to_film:{kp_id}"
    # Старый обработчик show_film_description_callback больше не нужен, так как все кнопки используют back_to_film

    @bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("schedule_back:"))
    def schedule_back_callback(call):
        """Обработчик кнопки возврата из расписания - удаляет оба сообщения с планами"""
        try:
            bot.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            
            # Получаем сохраненные message_id обоих сообщений
            if hasattr(show_schedule, '_schedule_messages') and chat_id in show_schedule._schedule_messages:
                messages = show_schedule._schedule_messages[chat_id]
                cinema_message_id = messages.get('cinema_message_id')
                home_message_id = messages.get('home_message_id')
                
                # Удаляем оба сообщения
                if cinema_message_id:
                    try:
                        bot.delete_message(chat_id, cinema_message_id)
                    except Exception as e:
                        logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение с кино: {e}")
                
                if home_message_id:
                    try:
                        bot.delete_message(chat_id, home_message_id)
                    except Exception as e:
                        logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение с домом: {e}")
                
                # Удаляем из словаря
                del show_schedule._schedule_messages[chat_id]
            else:
                # Если не нашли сохраненные сообщения, удаляем текущее
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except Exception as e:
                    logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение: {e}")
            
            # Показываем главное меню после удаления сообщений (как в start.py)
            from moviebot.database.db_operations import (
                get_active_subscription,
                get_active_group_subscription_by_chat_id
            )
            from moviebot.utils.helpers import has_recommendations_access, has_tickets_access
            
            user_id = call.from_user.id
            
            # Информация о подписке
            subscription_info = ""
            if call.message.chat.type == 'private':
                sub = get_active_subscription(chat_id, user_id, 'personal')
                if sub:
                    plan_type = sub.get('plan_type', 'all')
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты',
                        'all': 'Все режимы'
                    }
                    plan_name = plan_names.get(plan_type, plan_type)
                    subscription_info = f"\n\n💎 <b>Ваша подписка:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n📦 <b>Базовая версия бота</b>\n"
            else:
                group_sub = get_active_group_subscription_by_chat_id(chat_id)
                if group_sub:
                    plan_type = group_sub.get('plan_type', 'all')
                    plan_names = {
                        'notifications': 'Уведомления о сериалах',
                        'recommendations': 'Рекомендации',
                        'tickets': 'Билеты',
                        'all': 'Все режимы'
                    }
                    plan_name = plan_names.get(plan_type, plan_type)
                    subscription_info = f"\n\n💎 <b>Подписка группы:</b> {plan_name}\n"
                else:
                    subscription_info = "\n\n📦 <b>Базовая версия бота</b>\n"
            
            welcome_text = f"""
🎬 <b>Главное меню</b>{subscription_info}

💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на Кинопоиске в бот.

Выберите раздел из меню ниже ⬇
            """.strip()
            
            markup = InlineKeyboardMarkup()
            
            has_shazam_access = has_recommendations_access(chat_id, user_id)
            has_tickets = has_tickets_access(chat_id, user_id)
            
            # Строка 1: Сериалы / Премьеры
            markup.row(
                InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"),
                InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres")
            )
            
            # Строка 2: Рандом
            markup.row(
                InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random")
            )
            
            # Строка 3: Поиск / Шазам
            elias_text = "🔮 Шазам" if has_shazam_access else "🔒 Шазам"
            markup.row(
                InlineKeyboardButton("🔍 Поиск", callback_data="start_menu:search"),
                InlineKeyboardButton(elias_text, callback_data="shazam:start")
            )
            
            # Строка 4: Расписание / Билеты
            tickets_text = "🎫 Билеты" if has_tickets else "🔒 Билеты"
            tickets_callback = "start_menu:tickets" if has_tickets else "start_menu:tickets_locked"
            markup.row(
                InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"),
                InlineKeyboardButton(tickets_text, callback_data=tickets_callback)
            )
            
            # Строка 5: Оплата / Настройки / Помощь (только эмодзи)
            markup.row(
                InlineKeyboardButton("💰", callback_data="start_menu:payment"),
                InlineKeyboardButton("⚙️", callback_data="start_menu:settings"),
                InlineKeyboardButton("❓", callback_data="start_menu:help")
            )
            
            try:
                bot.send_message(chat_id, welcome_text, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                logger.error(f"[SCHEDULE BACK] Ошибка при отправке главного меню: {e}")
            
            logger.info(f"[SCHEDULE BACK] Пользователь {call.from_user.id} вернулся из расписания")
        except Exception as e:
            logger.error(f"[SCHEDULE BACK] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    logger.info(f"[REGISTER PLAN HANDLERS] Все обработчики планирования зарегистрированы (включая show_film_description и schedule_back)")
    logger.info(f"[REGISTER PLAN HANDLERS] ===== END =====")
    logger.info("=" * 80)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_type:"))
def plan_type_callback(call):
    """Обработчик выбора типа плана"""
    logger.info("=" * 80)
    logger.info(f"[PLAN TYPE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[PLAN TYPE] ✅ ОБРАБОТЧИК ВЫЗВАН!")
    # TODO: Извлечь из moviebot.py строки 10827-10868
    try:
        logger.info(f"[PLAN TYPE] Вызов answer_callback_query")
        bot.answer_callback_query(call.id)
        logger.info(f"[PLAN TYPE] answer_callback_query выполнен")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_type = call.data.split(":")[1]  # 'home' или 'cinema'
        
        logger.info(f"[PLAN TYPE] Получен callback: user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}")
        logger.info(f"[PLAN TYPE] user_plan_state keys={list(user_plan_state.keys())}")
        logger.info(f"[PLAN TYPE] user_id in user_plan_state = {user_id in user_plan_state}")
        
        if user_id not in user_plan_state:
            logger.warning(f"[PLAN TYPE] Состояние не найдено для user_id={user_id}, текущие состояния: {list(user_plan_state.keys())}")
            bot.edit_message_text("❌ Ошибка: сессия истекла. Начните заново с /plan", chat_id, call.message.message_id)
            return
        
        state = user_plan_state[user_id]
        link = state.get('link')
        
        if not link:
            bot.edit_message_text("❌ Ошибка: не найдена ссылка на фильм. Начните заново с /plan", chat_id, call.message.message_id)
            del user_plan_state[user_id]
            return
        
        state['plan_type'] = plan_type
        state['step'] = 3
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        # Отправляем сообщение с запросом даты/времени и сохраняем его message_id
        prompt_msg = bot.send_message(chat_id, f"📅 Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\nМожно указать:\n• День недели (сегодня, завтра, понедельник и т.д.)\n• Дату (01.01, 1 января и т.д.)\n• Время (19:00, 20:30)")
        state['prompt_message_id'] = prompt_msg.message_id
        logger.info(f"[PLAN TYPE] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id}")
        
        logger.info(f"[PLAN TYPE] Пользователь {user_id} выбрал {plan_type}, link={link}")
    except Exception as e:
        logger.error(f"[PLAN TYPE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


    @bot.callback_query_handler(func=lambda call: call.data == "plan:cancel")
    def plan_cancel_callback(call):
        """Обработчик отмены плана"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        if user_id in user_plan_state:
            del user_plan_state[user_id]
            logger.info(f"[PLAN] Пользователь {user_id} вышел из режима планирования")
        
        bot.answer_callback_query(call.id, "Режим планирования отменён")
        bot.edit_message_text("✅ Режим планирования отменён. Можете использовать другие команды.", 
                             chat_id, call.message.message_id)

    @bot.callback_query_handler(func=lambda call: call.data == "cancel_plan")
    def cancel_plan_callback(call):
        """Обработчик отмены планирования из сообщения об ошибке"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        logger.info(f"[CANCEL PLAN] ===== START: user_id={user_id}, chat_id={chat_id}, message_id={message_id}")
        
        # Очищаем состояние планирования
        if user_id in user_plan_state:
            state_info = user_plan_state[user_id]
            logger.info(f"[CANCEL PLAN] Удаляем состояние планирования: {state_info}")
            del user_plan_state[user_id]
        else:
            logger.info(f"[CANCEL PLAN] Состояние планирования не найдено для user_id={user_id}")
        
        try:
            bot.answer_callback_query(call.id, "Планирование отменено")
            logger.info(f"[CANCEL PLAN] Callback query ответ отправлен")
            
            # Удаляем сообщение с кнопкой отмены
            try:
                bot.delete_message(chat_id, message_id)
                logger.info(f"[CANCEL PLAN] ✅ Сообщение {message_id} удалено")
            except Exception as del_e:
                logger.warning(f"[CANCEL PLAN] Не удалось удалить сообщение {message_id}: {del_e}, пробуем редактировать")
                try:
                    bot.edit_message_text("✅ Планирование отменено. Можете использовать другие команды.", 
                                         chat_id, message_id)
                    logger.info(f"[CANCEL PLAN] Сообщение отредактировано")
                except Exception as edit_e:
                    logger.warning(f"[CANCEL PLAN] Не удалось отредактировать сообщение: {edit_e}")
        except Exception as e:
            logger.error(f"[CANCEL PLAN] Ошибка при обработке отмены: {e}", exc_info=True)
            try:
                bot.send_message(chat_id, "✅ Планирование отменено. Можете использовать другие команды.")
            except:
                pass
        
        logger.info(f"[CANCEL PLAN] ===== END: планирование отменено для user_id={user_id}")


    @bot.callback_query_handler(func=lambda call: call.data == "plan_from_list")
    def plan_from_list_callback(call):
        """Обработчик планирования из списка"""
        # TODO: Извлечь из moviebot.py строки 10886-10909
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[PLAN FROM LIST] Пользователь {user_id} хочет запланировать фильм из /list")
            
            user_plan_state[user_id] = {
                'step': 1,
                'chat_id': chat_id
            }
            
            bot.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
            prompt_msg = bot.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
            # Сохраняем message_id промпта в состояние
            user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[PLAN FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[PLAN FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass


    @bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_from_added:"))
    def plan_from_added_callback(call):
        """Обработчик планирования из добавленного фильма"""
        logger.info(f"[PLAN FROM ADDED] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[PLAN FROM ADDED] Получен callback: call.data={call.data}, user_id={call.from_user.id}, chat_id={call.message.chat.id}")
        try:
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id)  # Отвечаем сразу, чтобы убрать "крутилку"
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            kp_id = call.data.split(":")[1]
            
            logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать фильм kp_id={kp_id}")
            
            # Проверяем, есть ли фильм в базе, если нет - добавляем
            from moviebot.bot.handlers.series import ensure_movie_in_database
            from moviebot.api.kinopoisk_api import extract_movie_info
            
            link = None
            film_id = None
            
            # Приводим kp_id к строке для корректного поиска в БД
            kp_id_str = str(kp_id)
            
            with db_lock:
                cursor.execute('SELECT id, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id_str))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    link = row.get('link') if isinstance(row, dict) else row[1]
                    logger.info(f"[PLAN FROM ADDED] Фильм найден в базе: film_id={film_id}, link={link}")
            
            if not film_id:
                # Фильм не в базе - добавляем
                if not link:
                    # Определяем, фильм это или сериал, чтобы использовать правильную ссылку
                    # Пока используем стандартную ссылку на фильм, API сам определит тип
                    link = f"https://www.kinopoisk.ru/film/{kp_id_str}/"
                
                logger.info(f"[PLAN FROM ADDED] Фильм не в базе, получаю информацию через API: link={link}")
                info = extract_movie_info(link)
                if info:
                    # Если это сериал, обновляем ссылку
                    if info.get('is_series') or info.get('plan_type') == 'TV_SERIES':
                        link = f"https://www.kinopoisk.ru/series/{kp_id_str}/"
                    
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id_str, link, info, user_id)
                    if was_inserted:
                        logger.info(f"[PLAN FROM ADDED] Фильм добавлен в базу при планировании: kp_id={kp_id_str}, film_id={film_id}")
                    if not film_id:
                        bot.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                        return
                else:
                    bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                    return
            
            if not link:
                link = f"https://www.kinopoisk.ru/film/{kp_id_str}/"
                logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
            
            user_plan_state[user_id] = {
                'step': 2,
                'link': link,
                'chat_id': chat_id,
                'kp_id': kp_id_str  # Сохраняем kp_id обязательно!
            }
            
            logger.info(f"[PLAN FROM ADDED] Состояние установлено: user_id={user_id}, state={user_plan_state[user_id]}")
            
            markup = InlineKeyboardMarkup()
            markup.add(
                InlineKeyboardButton("Дома 🏠", callback_data=f"plan_type:home:{kp_id_str}"),
                InlineKeyboardButton("В кино 🎥", callback_data=f"plan_type:cinema:{kp_id_str}")
            )
            
            logger.info(f"[PLAN FROM ADDED] Отправка сообщения с выбором типа просмотра...")
            prompt_msg = bot.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
            # Если хочешь — сохрани prompt_message_id, но не обязательно
            logger.info(f"[PLAN FROM ADDED] Сообщение отправлено успешно")
            
        except Exception as e:
            logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
        finally:
            logger.info(f"[PLAN FROM ADDED] ===== КОНЕЦ ОБРАБОТКИ =====")


    # Обработчик add_ticket: перенесен в ticket_callbacks.py
    # TODO: Добавить остальные callback handlers:
    # - plan_detail
    # - remove_from_calendar
    # - edit_plan handlers
    # и другие из moviebot.py
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("film_desc:"))
    def film_desc_from_schedule(call):
        """Обработчик кнопок фильмов из расписания - показывает описание фильма"""
        try:
            bot.answer_callback_query(call.id, text="⏳ Загружаю описание...")

            kp_id = int(call.data.split(":", 1)[1])
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            message_thread_id = getattr(call.message, 'message_thread_id', None)

            logger.info(f"[FILM DESC FROM SCHEDULE] kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")

            # Используем get_film_current_state для получения актуального состояния
            from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
            from moviebot.api.kinopoisk_api import extract_movie_info
            
            current_state = get_film_current_state(chat_id, kp_id, user_id)
            existing = current_state['existing']
            
            # Определяем ссылку
            link = None
            if existing:
                # Если фильм в базе, получаем ссылку из БД
                with db_lock:
                    cursor.execute('SELECT link, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                    row = cursor.fetchone()
                    if row:
                        link = row.get('link') if isinstance(row, dict) else row[0]
                        is_series = bool(row.get('is_series') if isinstance(row, dict) else (row[1] if len(row) > 1 else 0))
                        if not link:
                            link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            if not link:
                # Фильм не в базе, пробуем API для определения типа
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            # Получаем информацию через API
            info = extract_movie_info(link)
            
            if not info or not info.get('title'):
                # Если API не сработал, пробуем получить из БД
                if existing:
                    with db_lock:
                        cursor.execute('''
                            SELECT title, year, genres, description, director, actors, is_series, link
                            FROM movies WHERE id = %s AND chat_id = %s
                        ''', (existing[0], chat_id))
                        db_row = cursor.fetchone()
                        if db_row:
                            info = {
                                'title': db_row[0] if len(db_row) > 0 else None,
                                'year': db_row[1] if len(db_row) > 1 else None,
                                'genres': db_row[2] if len(db_row) > 2 else None,
                                'description': db_row[3] if len(db_row) > 3 else None,
                                'director': db_row[4] if len(db_row) > 4 else None,
                                'actors': db_row[5] if len(db_row) > 5 else None,
                                'is_series': bool(db_row[6]) if len(db_row) > 6 else False
                            }
                            if not link:
                                link = db_row[7] if len(db_row) > 7 else f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            if not info or not info.get('title'):
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Убеждаемся, что is_series правильно установлен
            if existing:
                with db_lock:
                    cursor.execute('SELECT is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                    row = cursor.fetchone()
                    if row:
                        info['is_series'] = bool(row.get('is_series') if isinstance(row, dict) else row[0])
            
            # Уточняем link для сериала
            if info.get('is_series'):
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            elif not link or '/series/' in link:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"

            # Вызываем show_film_info_with_buttons с актуальным existing
            # existing будет переопределен внутри функции через get_film_current_state, но передаем для оптимизации
            show_film_info_with_buttons(
                chat_id=chat_id,
                user_id=user_id,
                info=info,
                link=link,
                kp_id=kp_id,
                existing=existing,
                message_id=message_id,
                message_thread_id=message_thread_id
            )
            
            logger.info(f"[FILM DESC FROM SCHEDULE] Описание показано успешно")

        except Exception as e:
            logger.error(f"[FILM DESC FROM SCHEDULE] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка", show_alert=True)
            except:
                pass

def get_plan_link_internal(message, state):
    """Внутренняя функция для получения ссылки на фильм в /plan"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    link = None
    
    # КРИТИЧЕСКИЙ ФИКС: В личке принимаем следующее сообщение, в группах - только реплай
    from moviebot.bot.bot_init import BOT_ID
    is_private = message.chat.type == 'private'
    is_reply = (message.reply_to_message and 
               message.reply_to_message.from_user and 
               message.reply_to_message.from_user.id == BOT_ID)
    
    prompt_message_id = state.get('prompt_message_id')
    
    # В группах принимаем только реплаи на бота
    if not is_private:
        if not is_reply:
            logger.info(f"[PLAN LINK] В группе сообщение от пользователя {user_id} не является ответом на сообщение бота, игнорируем")
            return
        # Проверяем, что это ответ на правильный промпт
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            logger.info(f"[PLAN LINK] В группе реплай не на правильный промпт, игнорируем")
            return
    else:
        # В личке: принимаем реплай на промпт или следующее сообщение (если состояние активно)
        if is_reply:
            # Проверяем, что это ответ на правильное сообщение
            if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                logger.info(f"[PLAN LINK] В личке реплай не на правильное сообщение, игнорируем")
                return
        # Если не реплай, но состояние активно - принимаем как следующее сообщение
    
    # Извлекаем ссылку или ID из текста сообщения
    message_text = message.text or ''
    kp_id = None
    
    if message_text:
        # Используем extract_kp_id_from_text для извлечения ID
        kp_id = extract_kp_id_from_text(message_text)
        if kp_id:
            # Если это ссылка, извлекаем её
            if message_text.strip().startswith('http'):
                link = message_text.strip()
                logger.info(f"[PLAN] Найдена ссылка в тексте сообщения: {link}")
            else:
                # Это ID, проверяем в базе или создаем ссылку
                with db_lock:
                    cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                    row = cursor.fetchone()
                    if row:
                        link = row.get('link') if isinstance(row, dict) else row[0]
                        logger.info(f"[PLAN] Найден фильм по ID {kp_id} в тексте сообщения (из базы): {link}")
                    else:
                        link = f"https://kinopoisk.ru/film/{kp_id}/"
                        logger.info(f"[PLAN] Фильм с ID {kp_id} не найден в базе, создана ссылка: {link}")
    
    if not link:
        bot.reply_to(message, "❌ Не найдена ссылка на фильм. Пришлите ссылку или ID фильма.")
        if user_id in user_plan_state:
            del user_plan_state[user_id]
        return
    
    # Извлекаем kp_id из link (поддержка film/ и series/)
    kp_id = None
    if 'kinopoisk.ru' in link:
        import re
        match = re.search(r'/film/(\d+)', link) or re.search(r'/series/(\d+)', link)
        if match:
            kp_id = match.group(1)
    
    if not kp_id:
        # Если не удалось извлечь — пробуем по тексту сообщения (как раньше)
        kp_id = extract_kp_id_from_text(message_text)
    
    if not kp_id:
        bot.reply_to(message, "❌ Не удалось определить ID фильма. Попробуйте другую ссылку.")
        if user_id in user_plan_state:
            del user_plan_state[user_id]
        return

    user_plan_state[user_id]['link'] = link
    user_plan_state[user_id]['kp_id'] = kp_id  # Сохраняем kp_id в состояние
    user_plan_state[user_id]['step'] = 2

    markup = InlineKeyboardMarkup()
    markup.add(
        InlineKeyboardButton("Дома 🏠", callback_data=f"plan_type:home:{int(kp_id)}"),
        InlineKeyboardButton("В кино 🎥", callback_data=f"plan_type:cinema:{int(kp_id)}")
    )
    
    prompt_msg = bot.send_message(message.chat.id, "Где планируете смотреть?", reply_markup=markup)
    user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
    logger.info(f"[PLAN] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id} (step=2)")

def get_plan_day_or_date_internal(message, state):
    """Внутренняя функция для получения дня/даты в /plan"""
    logger.info("=" * 80)
    logger.info(f"[PLAN DAY/DATE INTERNAL] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    
    user_id = message.from_user.id
    plan_type = state.get('plan_type')
    link = state.get('link')
    prompt_message_id = state.get('prompt_message_id')
    
    logger.info(f"[PLAN DAY/DATE INTERNAL] prompt_message_id={prompt_message_id}, reply_to_message={message.reply_to_message.message_id if message.reply_to_message else None}")
    
    # КРИТИЧЕСКИЙ ФИКС: В личке принимаем следующее сообщение, в группах - только реплай
    from moviebot.bot.bot_init import BOT_ID
    is_private = message.chat.type == 'private'
    is_reply = (message.reply_to_message and 
                message.reply_to_message.from_user and 
                message.reply_to_message.from_user.id == BOT_ID)
    
    # В группах принимаем только реплаи на бота
    if not is_private:
        if not is_reply:
            logger.info(f"[PLAN DAY/DATE INTERNAL] В группе не реплай на бота → игнорируем")
            return
        # Проверяем, что это ответ на правильный промпт
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            logger.info(f"[PLAN DAY/DATE INTERNAL] В группе реплай не на правильный промпт → игнорируем")
            return
    else:
        # В личке: если это реплай, проверяем, что на правильный промпт
        if is_reply and prompt_message_id:
            if message.reply_to_message.message_id != prompt_message_id:
                logger.info(f"[PLAN DAY/DATE INTERNAL] В личке реплай не на правильный промпт → игнорируем")
                return
        # Если не реплай, но состояние активно - принимаем как следующее сообщение
    
    text = (message.text or "").strip()
    if not text:
        logger.warning("[PLAN DAY/DATE INTERNAL] Пустой текст → пропускаем")
        return
    
    text_lower = text.lower().strip()
    
    logger.info(f"[PLAN DAY/DATE INTERNAL] Текст: '{text_lower}', plan_type={plan_type}, link={link}")
    
    if not plan_type or not link:
        bot.reply_to(message, "❌ Ошибка: не указан тип просмотра или ссылка. Начните заново.")
        if user_id in user_plan_state:
            del user_plan_state[user_id]
        return
    
    user_tz = get_user_timezone_or_default(user_id)
    now = datetime.now(user_tz)
    plan_dt = None
    
    # Сначала пробуем использовать parse_session_time для более полной обработки дат
    parsed_dt = parse_session_time(text, user_tz)
    if parsed_dt:
        plan_dt = parsed_dt
        logger.info(f"[PLAN DAY/DATE INTERNAL] Использован parse_session_time: {plan_dt}")
    
    extracted_time = None
    if not plan_dt:
        # Пробуем найти время отдельно (например, "завтра 10:00", "в субботу 15:00", "10.01 20:30")
        # Ищем формат ЧЧ:ММ (два цифры, двоеточие, две цифры)
        time_match = re.search(r'\b(\d{1,2}):(\d{2})\b', text)
        if time_match:
            hour = int(time_match.group(1))
            minute = int(time_match.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                extracted_time = (hour, minute)
                logger.info(f"[PLAN DAY/DATE INTERNAL] Найдено время в тексте: {hour}:{minute:02d}")
    
    if not plan_dt:
        target_weekday = None
        for phrase, wd in days_full.items():
            if phrase in text_lower:
                target_weekday = wd
                logger.info(f"[PLAN DAY/DATE INTERNAL] Найден день недели: {phrase} -> {wd}")
                break
        
        if target_weekday is not None:
            current_wd = now.weekday()
            delta = (target_weekday - current_wd + 7) % 7
            if delta == 0:
                delta = 7
            plan_date = now.date() + timedelta(days=delta)
            
            # Используем извлеченное время, если есть, иначе стандартное
            if extracted_time:
                hour, minute = extracted_time
            elif plan_type == 'home':
                hour = 19 if target_weekday < 5 else 10
                minute = 0
            else:
                hour = 9
                minute = 0
            
            plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
            plan_dt = user_tz.localize(plan_dt)
            logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата по дню недели: {plan_dt}")
        else:
            # Обработка специальных форматов: "сегодня", "завтра", "следующая неделя"
            if 'сегодня' in text_lower:
                plan_date = now.date()
                # Используем извлеченное время, если есть, иначе стандартное
                if extracted_time:
                    hour, minute = extracted_time
                elif plan_type == 'home':
                    hour = 19 if plan_date.weekday() < 5 else 10
                    minute = 0
                else:
                    hour = 9
                    minute = 0
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                plan_dt = user_tz.localize(plan_dt)
                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата 'сегодня': {plan_dt}")
            elif 'завтра' in text_lower:
                plan_date = (now.date() + timedelta(days=1))
                # Используем извлеченное время, если есть, иначе стандартное
                if extracted_time:
                    hour, minute = extracted_time
                elif plan_type == 'home':
                    hour = 19 if plan_date.weekday() < 5 else 10
                    minute = 0
                else:
                    hour = 9
                    minute = 0
                plan_dt = datetime.combine(plan_date, datetime.min.time().replace(hour=hour, minute=minute))
                plan_dt = user_tz.localize(plan_dt)
                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата 'завтра': {plan_dt}")
                
            elif 'следующая неделя' in text_lower or 'след неделя' in text_lower or 'след. неделя' in text_lower or 'на следующей неделе' in text_lower:
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

            # ← УБРАН лишний else — теперь это просто следующий блок логики
            # Парсинг дат: "15 января", "15 января 17:00", "10.01", "14 апреля"
            # Сначала пробуем формат с временем: "15 января 17:00" или "10 января 20:30"
            date_time_match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2}):(\d{2})', text_lower)
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
                    date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text_lower)
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
                                # Используем извлеченное время, если есть, иначе стандартное
                                if extracted_time:
                                    hour, minute = extracted_time
                                elif plan_type == 'home':
                                    hour = 19 if datetime(year, month, day).weekday() < 5 else 10
                                    minute = 0
                                else:
                                    hour = 9
                                    minute = 0
                                plan_dt = user_tz.localize(datetime(year, month, day, hour, minute))
                                logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата текстовым форматом: {plan_dt}")
                            except ValueError as e:
                                logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга текстовой даты: {e}")
                    else:
                        # Парсинг "10.01" или "06.01", возможно с временем "10.01 20:30"
                        # Сначала пробуем формат с временем: "10.01 20:30"
                        date_time_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\s+(\d{1,2}):(\d{2})', text_lower)
                        if date_time_match:
                            day_num = int(date_time_match.group(1))
                            month_num = int(date_time_match.group(2))
                            year_str = date_time_match.group(3)
                            hour = int(date_time_match.group(4))
                            minute = int(date_time_match.group(5))
                            if 1 <= month_num <= 12 and 1 <= day_num <= 31 and 0 <= hour <= 23 and 0 <= minute <= 59:
                                try:
                                    year = now.year
                                    if year_str:
                                        year_part = int(year_str)
                                        if year_part < 100:
                                            year = 2000 + year_part
                                        else:
                                            year = year_part
                                    candidate = user_tz.localize(datetime(year, month_num, day_num, hour, minute))
                                    if candidate < now:
                                        year += 1
                                    plan_dt = user_tz.localize(datetime(year, month_num, day_num, hour, minute))
                                    logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата числовым форматом с временем: {plan_dt}")
                                except ValueError as e:
                                    logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга числовой даты с временем: {e}")
                        else:
                            # Парсинг "10.01" или "06.01" без времени
                            date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text_lower)
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
                                        # Используем извлеченное время, если есть, иначе стандартное
                                        if extracted_time:
                                            hour, minute = extracted_time
                                        elif plan_type == 'home':
                                            hour = 19 if datetime(year, month_num, day_num).weekday() < 5 else 10
                                            minute = 0
                                        else:
                                            hour = 9
                                            minute = 0
                                        plan_dt = user_tz.localize(datetime(year, month_num, day_num, hour, minute))
                                        logger.info(f"[PLAN DAY/DATE INTERNAL] Установлена дата числовым форматом: {plan_dt}")
                                    except ValueError as e:
                                        logger.warning(f"[PLAN DAY/DATE INTERNAL] Ошибка парсинга числовой даты: {e}")
    
    # Если всё-таки не удалось распознать
    if not plan_dt:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Не удалось распознать: '{text}'")
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Кнопка возврата к описанию, если есть kp_id
        kp_id = state.get('kp_id')
        if kp_id:
            try:
                kp_id_int = int(kp_id)
                markup.add(InlineKeyboardButton(
                    "◀️ Вернуться к описанию",
                    callback_data=f"back_to_film:{kp_id_int}"
                ))
            except:
                pass
        
        # Кнопка отмены планирования
        markup.add(InlineKeyboardButton(
            "❌ Отменить планирование",
            callback_data="cancel_plan"
        ))
        
        # Отправляем сообщение об ошибке и ВОЗОБНОВЛЯЕМ состояние планирования
        # В личке используем send_message, в группах - reply_to
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        error_text = "Не понял дату/время 😔\n\n" \
                    "Попробуй ещё раз. Примеры:\n" \
                    "• сегодня 21:00\n" \
                    "• завтра 19:30\n" \
                    "• пт 18:45\n" \
                    "• 15 января 20:00\n" \
                    "• 22.01 22:30\n" \
                    "• в субботу 19:00"
        
        if is_private:
            error_msg = bot.send_message(
                message.chat.id,
                error_text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        else:
            error_msg = bot.reply_to(
                message,
                error_text,
                reply_markup=markup,
                parse_mode='HTML'
            )
        
        # ВАЖНО: Сохраняем message_id ошибки в состояние, чтобы пользователь мог ответить на него
        state['prompt_message_id'] = error_msg.message_id
        # Состояние НЕ удаляем - пользователь может ввести снова
        logger.info(f"[PLAN DAY/DATE INTERNAL] Состояние планирования сохранено для повторного ввода, prompt_message_id={error_msg.message_id}")
        return   # ← СОСТОЯНИЕ ОСТАЁТСЯ! Пользователь может ввести снова
    
    # Если дата успешно распознана → идём дальше
    message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
    day_or_date_str = plan_dt.strftime('%d.%m.%Y %H:%M')
    
    result = process_plan(bot, user_id, message.chat.id, link, plan_type, day_or_date_str, message_date_utc)
    
    if result == 'NEEDS_TIMEZONE':
        # Сохраняем состояние планирования для продолжения после выбора часового пояса
        state['pending_text'] = text
        state['pending_plan_dt'] = day_or_date_str
        state['pending_message_date_utc'] = message_date_utc
        state['link'] = link
        state['plan_type'] = plan_type
        state['chat_id'] = message.chat.id
        user_plan_state[user_id] = state
        logger.info(f"[PLAN DAY/DATE INTERNAL] Состояние планирования сохранено для продолжения после выбора часового пояса")
        show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
        # НЕ удаляем состояние - оно нужно для продолжения планирования после выбора часового пояса
    elif result:
        # process_plan уже должен чистить состояние, но на всякий случай
        if user_id in user_plan_state:
            del user_plan_state[user_id]
            logger.info(f"[PLAN DAY/DATE INTERNAL] Состояние очищено после успеха")


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan:"))
def edit_plan_callback(call):
    """Обработчик выбора плана для редактирования"""
    logger.info(f"[EDIT PLAN] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        logger.info(f"[EDIT PLAN] Пользователь {user_id} хочет редактировать план {plan_id}")
        
        from moviebot.states import user_edit_state
        from moviebot.database.db_operations import get_user_timezone_or_default
        
        # Очищаем состояние редактирования при возврате к меню
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        if user_id in user_edit_state and user_edit_state[user_id].get('action') == 'edit_plan_datetime':
            # Оставляем только базовую информацию для меню редактирования
            user_edit_state[user_id] = {
                'action': 'edit_plan',
                'plan_id': plan_id,
                'from_settings': from_settings
            }
        
        # Получаем информацию о плане
        with db_lock:
            cursor.execute('''
                SELECT p.plan_type, p.plan_datetime, m.title, m.kp_id
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            plan_row = cursor.fetchone()
        
        if not plan_row:
            bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
            logger.warning(f"[EDIT PLAN] План {plan_id} не найден")
            return
        
        if isinstance(plan_row, dict):
            plan_type = plan_row.get('plan_type')
            plan_dt_value = plan_row.get('plan_datetime')
            title = plan_row.get('title')
            kp_id = plan_row.get('kp_id')
        else:
            plan_type = plan_row.get("plan_type") if isinstance(plan_row, dict) else (plan_row[0] if plan_row else None)
            plan_dt_value = plan_row[1]
            title = plan_row[2]
            kp_id = plan_row[3] if len(plan_row) > 3 else None
        
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
        
        # Сохраняем from_settings, если он был установлен
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        user_edit_state[user_id] = {
            'action': 'edit_plan',
            'plan_id': plan_id,
            'plan_type': plan_type,
            'kp_id': kp_id,  # Сохраняем kp_id для возврата к описанию
            'from_settings': from_settings
        }
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📅 Изменить дату/время", callback_data=f"edit_plan_datetime:{plan_id}"))
        if plan_type == 'cinema':
            markup.add(InlineKeyboardButton("🎟️ Загрузить билеты", callback_data=f"edit_plan_ticket:{plan_id}"))
            markup.add(InlineKeyboardButton("🏠 Переключить в 'дома'", callback_data=f"edit_plan_switch:{plan_id}"))
        else:
            markup.add(InlineKeyboardButton("📺 Изменить онлайн-кинотеатр", callback_data=f"edit_plan_streaming:{plan_id}"))
            markup.add(InlineKeyboardButton("🎦 Переключить в 'в кино'", callback_data=f"edit_plan_switch:{plan_id}"))
        markup.add(InlineKeyboardButton("🗑️ Удалить из расписания", callback_data=f"remove_from_calendar:{plan_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
        
        text = f"✏️ <b>Редактирование плана:</b>\n\n"
        text += f"<b>{title}</b>\n"
        text += f"Тип: {'🎦 в кино' if plan_type == 'cinema' else '🏠 дома'}\n"
        text += f"Дата/время: {date_str}\n\n"
        text += f"Что вы хотите изменить?"
        
        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[EDIT PLAN] Меню редактирования отправлено для плана {plan_id}")
    except Exception as e:
        logger.error(f"[EDIT PLAN] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("remove_from_calendar:"))
def handle_remove_from_calendar_callback(call):
    """Обработчик удаления фильма из календаря"""
    logger.info(f"[REMOVE FROM CALENDAR] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        plan_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[REMOVE FROM CALENDAR] Удаление плана {plan_id} пользователем {user_id}")
        
        bot.answer_callback_query(call.id)
        
        with db_lock:
            # Получаем информацию о плане (включая проверку наличия билетов)
            cursor.execute('''
                SELECT p.id, p.ticket_file_id, 
                       CASE WHEN p.film_id IS NOT NULL THEN m.title ELSE NULL END as title
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
                logger.warning(f"[REMOVE FROM CALENDAR] План {plan_id} не найден")
                return
            
            ticket_file_id = row.get('ticket_file_id') if isinstance(row, dict) else row[1]
            title = row.get('title') if isinstance(row, dict) else row[2]
            
            # Проверяем наличие билетов
            has_tickets = ticket_file_id is not None and ticket_file_id.strip() != ''
            
            if has_tickets:
                # Если есть билеты, показываем подтверждение
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_remove_plan:{plan_id}"))
                markup.add(InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_remove_plan:{plan_id}"))
                
                event_name = title if title else "мероприятие"
                bot.send_message(
                    chat_id,
                    f"⚠️ <b>Подтверждение удаления</b>\n\n"
                    f"Вы уверены, что хотите удалить <b>{event_name}</b> из расписания?\n\n"
                    f"⚠️ <b>Внимание:</b> При удалении из расписания будут также удалены все билеты, связанные с этим мероприятием.",
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            # Если билетов нет, удаляем сразу
            title = title if title else "мероприятие"
            cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            conn.commit()
        
        bot.answer_callback_query(call.id, f"✅ '{title}' удалён из календаря")
        logger.info(f"[REMOVE FROM CALENDAR] План {plan_id} удалён пользователем {user_id}")
        
        # Обновляем сообщение, убирая кнопки
        try:
            bot.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
        except Exception as e:
            logger.warning(f"[REMOVE FROM CALENDAR] Не удалось обновить сообщение: {e}")
    except Exception as e:
        logger.error(f"[REMOVE FROM CALENDAR] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("confirm_remove_plan:"))
def confirm_remove_plan_callback(call):
    """Обработчик подтверждения удаления плана с билетами"""
    try:
        plan_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        bot.answer_callback_query(call.id)
        
        with db_lock:
            # Получаем информацию о плане
            cursor.execute('''
                SELECT p.id, p.ticket_file_id,
                       CASE WHEN p.film_id IS NOT NULL THEN m.title ELSE NULL END as title
                FROM plans p
                LEFT JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
                return
            
            title = row.get('title') if isinstance(row, dict) else row[2]
            title = title if title else "мероприятие"
            
            # Удаляем план (билеты удалятся автоматически, так как они хранятся в ticket_file_id)
            cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            conn.commit()
        
        # Удаляем сообщение с подтверждением
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        bot.send_message(chat_id, f"✅ '{title}' удалён из расписания. Билеты также удалены.")
        logger.info(f"[CONFIRM REMOVE PLAN] План {plan_id} удалён пользователем {user_id} с билетами")
    except Exception as e:
        logger.error(f"[CONFIRM REMOVE PLAN] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("cancel_remove_plan:"))
def cancel_remove_plan_callback(call):
    """Обработчик отмены удаления плана"""
    try:
        bot.answer_callback_query(call.id, "Отменено")
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"[CANCEL REMOVE PLAN] Ошибка: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data.startswith("streaming_select:"))
def streaming_select_callback(call):
    try:
        bot.answer_callback_query(call.id)

        parts = call.data.split(":")
        plan_id = int(parts[1])
        chat_id = call.message.chat.id
        message_id = call.message.message_id

        # Получаем kp_id из плана (чтоб звать API)
        with db_lock:
            cursor.execute('SELECT film_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            row = cursor.fetchone()
            if not row:
                bot.edit_message_text("План не найден.", chat_id, message_id)
                return
            film_id = row[0] if isinstance(row, dict) else row[0]

            cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            kp_row = cursor.fetchone()
            kp_id = kp_row[0] if kp_row else None

        if not kp_id:
            bot.edit_message_text("Ошибка: kp_id не найден.", chat_id, message_id)
            return

        from moviebot.api.kinopoisk_api import get_external_sources
        sources = get_external_sources(kp_id)

        if not sources:
            bot.edit_message_text(
                "😔 Не найдено онлайн-кинотеатров для просмотра.\n\n◀️ Назад",
                chat_id, message_id,
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Назад", callback_data=f"back_to_plan:{plan_id}")
                )
            )
            return

        markup = InlineKeyboardMarkup(row_width=1)
        for platform, url in sources:
            markup.add(InlineKeyboardButton(platform, callback_data=f"select_streaming:{plan_id}:{platform}:{url.replace(':', '%3A')}"))  # эскейп :

        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"back_to_plan:{plan_id}"))

        bot.edit_message_text(
            "Выберите онлайн-кинотеатр для просмотра:",
            chat_id, message_id,
            reply_markup=markup
        )

    except Exception as e:
        logger.error(f"[STREAMING SELECT] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("streaming_done:"))
def streaming_done_callback(call):
    """Обработчик кнопки 'Завершить' - сохраняет флаг и обновляет сообщение с подтверждением планирования"""
    logger.info(f"[STREAMING DONE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        plan_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        user_id = call.from_user.id
        
        # Сохраняем флаг "Завершить" в базу и получаем информацию о плане
        with db_lock:
            # Получаем информацию о плане для отображения подтверждения
            cursor.execute('''
                SELECT p.film_id, p.plan_datetime, p.plan_type, m.title
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            plan_row = cursor.fetchone()
            
            if not plan_row:
                bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
                return
            
            if isinstance(plan_row, dict):
                film_id = plan_row.get('film_id')
                plan_datetime = plan_row.get('plan_datetime')
                plan_type = plan_row.get('plan_type')
                title = plan_row.get('title')
            else:
                film_id = plan_row[0]
                plan_datetime = plan_row[1]
                plan_type = plan_row[2]
                title = plan_row[3]
            
            # Обновляем флаг "Завершить"
            cursor.execute('''
                UPDATE plans 
                SET streaming_done = TRUE 
                WHERE id = %s AND chat_id = %s
            ''', (plan_id, chat_id))
            conn.commit()
            logger.info(f"[STREAMING DONE] Флаг streaming_done установлен для плана {plan_id}")
        
        bot.answer_callback_query(call.id, "✅")
        
        # Формируем текст подтверждения с названием фильма и датой
        if plan_datetime:
            user_tz = get_user_timezone_or_default(user_id)
            if isinstance(plan_datetime, str):
                from datetime import datetime
                import pytz
                plan_datetime = datetime.fromisoformat(plan_datetime.replace('Z', '+00:00'))
            if plan_datetime.tzinfo is None:
                plan_datetime = pytz.utc.localize(plan_datetime)
            plan_datetime_local = plan_datetime.astimezone(user_tz)
            
            # Форматируем дату
            date_str = plan_datetime_local.strftime('%d.%m.%Y %H:%M')
            tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
            date_str += f" {tz_name}"
        else:
            date_str = "дата не указана"
        
        type_text = "дома 🏠" if plan_type == 'home' else "в кино 🎥"
        confirmation_text = f"✅ <b>{title}</b> запланирован на {date_str} {type_text}"
        
        # Получаем kp_id для кнопки
        kp_id = None
        with db_lock:
            cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            movie_row = cursor.fetchone()
            if movie_row:
                kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
        
        # Создаём клавиатуру
        markup = InlineKeyboardMarkup(row_width=1)
        
        if kp_id:
            try:
                kp_id_int = int(kp_id)
                markup.add(
                    InlineKeyboardButton(
                        "◀️ Вернуться к описанию",
                        callback_data=f"back_to_film:{kp_id_int}"
                    )
                )
            except ValueError:
                logger.warning(f"[STREAMING DONE] kp_id не число: {kp_id}")
        
        # Обновляем сообщение вместо удаления
        try:
            bot.edit_message_text(
                confirmation_text,
                chat_id,
                message_id,
                reply_markup=markup if markup.keyboard else None,
                parse_mode='HTML'
            )
            logger.info(f"[STREAMING DONE] Сообщение {message_id} обновлено с подтверждением планирования")
        except Exception as e:
            logger.warning(f"[STREAMING DONE] Не удалось обновить сообщение: {e}, пробуем отправить новое")
            try:
                bot.send_message(chat_id, confirmation_text, parse_mode='HTML', reply_markup=markup)
                bot.delete_message(chat_id, message_id)
            except Exception as e2:
                logger.error(f"[STREAMING DONE] Не удалось отправить новое сообщение: {e2}")
    except Exception as e:
        logger.error(f"[STREAMING DONE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
        
@bot.message_handler(func=lambda message: message.from_user.id in user_plan_state and user_plan_state[message.from_user.id].get("step") == 3)
def handle_plan_date(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    state = user_plan_state.get(user_id)
    
    if not state:
        bot.send_message(chat_id, "❌ Состояние потеряно. Начните заново.")
        return
    
    day_or_date = message.text.strip()
    
    # Вызываем существующую process_plan (она принимает link, plan_type, day_or_date)
    result = process_plan(bot, user_id, chat_id, state['link'], state['plan_type'], day_or_date, pre_selected_film_id=state.get('film_id'))
    
    if result == 'NEEDS_TIMEZONE':
        show_timezone_selection(bot, chat_id, user_id)  # Если есть такая функция
    elif result:
        bot.send_message(chat_id, "✅ Просмотр успешно запланирован!")
        del user_plan_state[user_id]  # Очистка состояния
    else:
        bot.send_message(chat_id, "❌ Не понял дату/время. Попробуйте ещё раз (примеры: завтра, 15 января 19:00).")

def handle_edit_plan_datetime_internal(message, state):
    """Внутренняя функция для обработки изменения даты/времени плана"""
    logger.info(f"[EDIT PLAN DATETIME INTERNAL] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.bot.bot_init import BOT_ID
        from moviebot.states import user_edit_state
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        plan_id = state.get('plan_id')
        
        logger.info(f"[EDIT PLAN DATETIME INTERNAL] Обработка: text='{text}', plan_id={plan_id}")
        
        # Проверяем, что сообщение является реплаем на сообщение бота
        is_reply = (message.reply_to_message and 
                   message.reply_to_message.from_user and 
                   message.reply_to_message.from_user.id == BOT_ID)
        
        prompt_message_id = state.get('prompt_message_id')
        # Если сообщение не является ответом на нужное сообщение бота, просто игнорируем его
        if not is_reply or (prompt_message_id and message.reply_to_message.message_id != prompt_message_id):
            logger.info(f"[EDIT PLAN DATETIME INTERNAL] Сообщение от пользователя {user_id} не является ответом на сообщение бота, игнорируем")
            return
        
        if not plan_id:
            bot.reply_to(message, "❌ Ошибка: план не найден.")
            if user_id in user_edit_state:
                del user_edit_state[user_id]
            return
        
        # Получаем информацию о плане
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
            link = plan_row.get("link") if isinstance(plan_row, dict) else (plan_row[0] if plan_row else None)
            plan_type = plan_row[1]
        
        user_tz = get_user_timezone_or_default(user_id)
        
        # Парсим новую дату/время
        session_dt = parse_session_time(text, user_tz)
        
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
            logger.info(f"[EDIT PLAN DATETIME INTERNAL] План {plan_id} обновлен: {date_str}")
            if user_id in user_edit_state:
                del user_edit_state[user_id]
        else:
            bot.reply_to(message, "❌ Не удалось распознать дату/время. Попробуйте еще раз.")
            logger.warning(f"[EDIT PLAN DATETIME INTERNAL] Не удалось распознать дату/время из текста: '{text}'")
    except Exception as e:
        logger.error(f"[EDIT PLAN DATETIME INTERNAL] Ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке.")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("stream_sel:"))
def stream_sel_callback(call):
    """Обработчик кнопки 'Выбрать онлайн-кинотеатр' для фильма/сериала (не запланированного)"""
    try:
        # Проверяем устаревший callback
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id, "⏳ Загружаю...")
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[STREAM SEL] Callback query устарел, пропускаем: {answer_error}")
        
        if callback_is_old:
            return
        
        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        logger.info(f"[STREAM SEL] Показать источники для kp_id={kp_id}")
        
        # Получаем источники из API
        from moviebot.api.kinopoisk_api import get_external_sources
        sources = get_external_sources(kp_id)
        
        if not sources:
            bot.edit_message_text(
                "😔 Не найдено онлайн-кинотеатров для просмотра.",
                chat_id, message_id,
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{kp_id}")
                )
            )
            return
        
        # Формируем список кнопок с онлайн-кинотеатрами
        markup = InlineKeyboardMarkup(row_width=1)
        for platform, url in sources[:10]:  # Максимум 10 источников
            markup.add(InlineKeyboardButton(
                platform,
                url=url  # Прямая ссылка на платформу
            ))
        
        markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{kp_id}"))
        
        bot.edit_message_text(
            "📺 <b>Онлайн-кинотеатры для просмотра:</b>\n\nВыберите платформу:",
            chat_id, message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"[STREAM SEL] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("select_streaming:"))
def select_streaming_callback(call):
    try:
        bot.answer_callback_query(call.id, "Выбрано!")

        parts = call.data.split(":")
        plan_id = int(parts[1])
        platform = parts[2]
        url = ':'.join(parts[3:])  # собираем url обратно (если были :)

        chat_id = call.message.chat.id
        message_id = call.message.message_id

        # Сохраняем выбор
        with db_lock:
            cursor.execute('UPDATE plans SET streaming_platform = %s, streaming_url = %s WHERE id = %s AND chat_id = %s', (platform, url, plan_id, chat_id))
            conn.commit()

        bot.edit_message_text(
            f"✅ Запомнили: {platform}\nСсылка: {url}\n\nВ день просмотра напомним!",
            chat_id, message_id,
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("◀️ Назад к плану", callback_data=f"back_to_plan:{plan_id}")
            )
        )

    except Exception as e:
        logger.error(f"[SELECT STREAMING] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка сохранения", show_alert=True)