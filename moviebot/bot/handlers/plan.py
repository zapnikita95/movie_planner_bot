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
from moviebot.bot.bot_init import bot as bot_instance

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

# Используем DAYS_FULL из config
days_full = DAYS_FULL
months_map = MONTHS_MAP


def process_plan(bot_instance, user_id, chat_id, link, plan_type, day_or_date, message_date_utc=None):
    """
    Планирует просмотр фильма. Возвращает True при успехе, False при ошибке, 
    'NEEDS_TIMEZONE' если нужно уточнить часовой пояс.
    message_date_utc - время сообщения в UTC для определения часового пояса
    """
    # TODO: Извлечь полную реализацию из moviebot.py строки 22844-23279
    # Это большая функция, нужно скопировать весь код
    plan_dt = None
    
    # Проверяем, нужно ли уточнить часовой пояс
    if message_date_utc:
        needs_tz_check = check_timezone_change(user_id, message_date_utc)
        if needs_tz_check:
            return 'NEEDS_TIMEZONE'
    
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
    
    with db_lock:
        if kp_id:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
        else:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND link = %s', (chat_id, link))
        row = cursor.fetchone()
        if not row:
            info = extract_movie_info(link)
            if info:
                is_series_val = 1 if info.get('is_series') else 0
                cursor.execute('INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series', 
                             (chat_id, link, info['kp_id'], info['title'], info['year'], info['genres'], info['description'], info['director'], info['actors'], is_series_val))
                conn.commit()
                cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, info['kp_id']))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title = row.get('title') if isinstance(row, dict) else row[1]
                else:
                    bot_instance.send_message(chat_id, "Не удалось добавить фильм в базу.")
                    return False
            else:
                bot_instance.send_message(chat_id, "Не удалось извлечь информацию о фильме.")
                return False
        else:
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
        
        # TODO: Добавить обработку сериалов (episode_info) из moviebot.py строки 23196-23274
        
        plan_utc = plan_dt.astimezone(pytz.utc)
        cursor.execute('INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id) VALUES (%s, %s, %s, %s, %s) RETURNING id',
                      (chat_id, film_id, plan_type, plan_utc, user_id))
        plan_id_row = cursor.fetchone()
        plan_id = plan_id_row.get('id') if isinstance(plan_id_row, dict) else plan_id_row[0] if plan_id_row else None
        conn.commit()
        
        # Успешное планирование - фильм уже в базе (film_id получен выше)
        logger.info(f"[PLAN] Успешное планирование: plan_id={plan_id}, film_id={film_id}, plan_type={plan_type}, plan_datetime={plan_utc}")
    
    # Формируем сообщение об успехе
    date_str = plan_dt.strftime('%d.%m %H:%M')
    type_text = "дома" if plan_type == 'home' else "в кино"
    
    # Проверяем доступ к билетам для кнопки "Добавить билеты" (только для планов в кино)
    from moviebot.utils.helpers import has_tickets_access
    markup = InlineKeyboardMarkup()
    
    if plan_type == 'cinema' and plan_id:
        if has_tickets_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("🎟️ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
        else:
            markup.add(InlineKeyboardButton("🔒 Добавить билеты", callback_data=f"ticket_locked:{plan_id}"))
    
    bot_instance.send_message(chat_id, f"✅ <b>{title}</b> запланирован на {date_str} {type_text}", parse_mode='HTML', reply_markup=markup if markup.keyboard else None)
    
    return True


def register_plan_handlers(bot_instance):
    """Регистрирует обработчики команд /plan и /schedule"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER PLAN HANDLERS] ===== START: регистрация обработчиков планирования =====")
    logger.info(f"[REGISTER PLAN HANDLERS] bot_instance: {bot_instance}")
    
    @bot_instance.message_handler(commands=['plan'])
    def plan_handler(message):
        """Команда /plan - планирование просмотра"""
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
                        cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
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
                    
                    result = process_plan(bot_instance, user_id, chat_id, link, plan_type, day_or_date, message_date_utc)
                    if result == 'NEEDS_TIMEZONE':
                        # Сохраняем введенный текст в состоянии планирования для продолжения после выбора часового пояса
                        state = user_plan_state.get(user_id, {})
                        state['pending_text'] = message.text.strip()
                        state['pending_plan_dt'] = day_or_date
                        state['pending_message_date_utc'] = message_date_utc
                        state['link'] = link
                        state['type'] = plan_type
                        state['chat_id'] = chat_id
                        user_plan_state[user_id] = state
                        logger.info(f"[PLAN] Сохранен текст для продолжения планирования: '{state['pending_text']}'")
                        show_timezone_selection(chat_id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
                        # НЕ удаляем состояние - оно нужно для продолжения планирования после выбора часового пояса
                except Exception as e:
                    bot_instance.reply_to(message, f"Ошибка при планировании: {e}")
                    logger.error(f"Ошибка process_plan: {e}", exc_info=True)
                    return
                return
            
            # Если нет ссылки, отправляем новое сообщение
            if not link:
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("❌ Выйти", callback_data="plan:cancel"))
                bot_instance.reply_to(message, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!", reply_markup=markup)
                user_plan_state[user_id] = {'step': 1, 'chat_id': chat_id}
                return
            
            if not plan_type:
                error_msg = bot_instance.reply_to(message, "Не указан тип просмотра (дома/кино).")
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
                error_msg = bot_instance.reply_to(message, "Не указан день или дата просмотра.")
                plan_error_messages[error_msg.message_id] = {
                    'user_id': user_id,
                    'chat_id': chat_id,
                    'link': link,
                    'plan_type': plan_type,
                    'day_or_date': None,
                    'missing': 'day_or_date'
                }
                user_plan_state[user_id] = {'step': 3, 'link': link, 'type': plan_type, 'chat_id': chat_id}
                return
        except Exception as e:
            logger.error(f"❌ Ошибка в /plan: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "Произошла ошибка при обработке команды /plan")
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
        
        with db_lock:
            cursor.execute('''
                SELECT p.id, m.title, m.kp_id, m.link, p.plan_datetime, p.plan_type,
                       CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as has_ticket,
                       m.watched
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND m.watched = 0
                ORDER BY p.plan_type DESC, p.plan_datetime ASC
            ''', (chat_id,))
            rows = cursor.fetchall()
        
        if not rows:
            empty_markup = InlineKeyboardMarkup(row_width=1)
            empty_markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
            empty_markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
            empty_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            bot_instance.reply_to(
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
                plan_id = row[0]
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
                cinema_markup.add(InlineKeyboardButton(button_text, callback_data=f"show_film_description:{kp_id}"))
            
            if not home_plans:
                cinema_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data=f"schedule_back:{chat_id}"))
            
            cinema_text = "🎬 <b>Премьеры в кино:</b>\n\n"
            for plan_id, title, kp_id, link, date_str, has_ticket in cinema_plans:
                ticket_emoji = "🎟️ " if has_ticket else ""
                cinema_text += f"{ticket_emoji}<b>{title}</b> — {date_str}\n"
            
            cinema_msg = bot_instance.reply_to(message, cinema_text, reply_markup=cinema_markup, parse_mode='HTML')
            cinema_message_id = cinema_msg.message_id
        
        # Сообщение 2: Просмотры дома
        if home_plans:
            home_markup = InlineKeyboardMarkup(row_width=1)
            for plan_id, title, kp_id, link, date_str, has_ticket in home_plans:
                button_text = f"{title} | {date_str}"
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                home_markup.add(InlineKeyboardButton(button_text, callback_data=f"show_film_description:{kp_id}"))
            
            home_markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data=f"schedule_back:{chat_id}"))
            
            home_text = "🏠 <b>Просмотры дома:</b>\n\n"
            for plan_id, title, kp_id, link, date_str, has_ticket in home_plans:
                home_text += f"<b>{title}</b> — {date_str}\n"
            
            if cinema_plans:
                home_msg = bot_instance.send_message(chat_id, home_text, reply_markup=home_markup, parse_mode='HTML')
            else:
                home_msg = bot_instance.reply_to(message, home_text, reply_markup=home_markup, parse_mode='HTML')
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
        logger.error(f"❌ Ошибка в /schedule: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /schedule")
        except:
            pass

    @bot_instance.message_handler(commands=['schedule'])
    def _show_schedule_handler(message):
        """Обертка для регистрации команды /schedule"""
        show_schedule(message)

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("show_film_description:"))
    def show_film_description_callback(call):
        """Обработчик кнопки показа описания фильма из /schedule"""
        logger.info("=" * 80)
        logger.info(f"[SHOW FILM DESCRIPTION] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
        logger.info(f"[SHOW FILM DESCRIPTION] ✅ ОБРАБОТЧИК ВЫЗВАН!")
        try:
            logger.info(f"[SHOW FILM DESCRIPTION] Вызов answer_callback_query")
            bot_instance.answer_callback_query(call.id)
            logger.info(f"[SHOW FILM DESCRIPTION] answer_callback_query выполнен")
            kp_id = call.data.split(":")[1]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[SHOW FILM DESCRIPTION] Пользователь {user_id} хочет посмотреть описание фильма kp_id={kp_id}")
            
            # Получаем информацию о фильме
            from moviebot.api.kinopoisk_api import extract_movie_info
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            info = extract_movie_info(link)
            
            if not info:
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Проверяем, есть ли фильм в базе
            with db_lock:
                cursor.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
            
            existing = None
            if row:
                if isinstance(row, dict):
                    film_id = row.get('id')
                    title = row.get('title')
                    watched = row.get('watched')
                else:
                    film_id = row[0]
                    title = row[1]
                    watched = row[2]
                existing = (film_id, title, watched)
            
            # Показываем описание фильма
            logger.info(f"[SHOW FILM DESCRIPTION] Вызов show_film_info_with_buttons")
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=None)
            logger.info(f"[SHOW FILM DESCRIPTION] show_film_info_with_buttons завершен")
            logger.info(f"[SHOW FILM DESCRIPTION] ===== END (успешно) =====")
            
        except Exception as e:
            logger.error(f"[SHOW FILM DESCRIPTION] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
            logger.error(f"[SHOW FILM DESCRIPTION] Тип ошибки: {type(e).__name__}, args: {e.args}")
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("schedule_back:"))
    def schedule_back_callback(call):
        """Обработчик кнопки возврата из расписания - удаляет оба сообщения с планами"""
        try:
            bot_instance.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            
            # Получаем сохраненные message_id обоих сообщений
            if hasattr(show_schedule, '_schedule_messages') and chat_id in show_schedule._schedule_messages:
                messages = show_schedule._schedule_messages[chat_id]
                cinema_message_id = messages.get('cinema_message_id')
                home_message_id = messages.get('home_message_id')
                
                # Удаляем оба сообщения
                if cinema_message_id:
                    try:
                        bot_instance.delete_message(chat_id, cinema_message_id)
                    except Exception as e:
                        logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение с кино: {e}")
                
                if home_message_id:
                    try:
                        bot_instance.delete_message(chat_id, home_message_id)
                    except Exception as e:
                        logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение с домом: {e}")
                
                # Удаляем из словаря
                del show_schedule._schedule_messages[chat_id]
            else:
                # Если не нашли сохраненные сообщения, удаляем текущее
                try:
                    bot_instance.delete_message(chat_id, call.message.message_id)
                except Exception as e:
                    logger.warning(f"[SCHEDULE BACK] Не удалось удалить сообщение: {e}")
            
            # Показываем главное меню после удаления сообщений
            welcome_text = """
            🎬 <b>Главное меню</b>

            💌 Чтобы добавить в базу фильм или сериал, пришлите в сообщении ссылку на страницу фильма или сериала на кинопоиске в бот.

            Выберите раздел из меню ниже ⬇
            """.strip()
            
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("📺 Сериалы", callback_data="start_menu:seasons"))
            markup.add(InlineKeyboardButton("📅 Премьеры", callback_data="start_menu:premieres"))
            markup.add(InlineKeyboardButton("🎲 Рандом", callback_data="start_menu:random"))
            markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
            markup.add(InlineKeyboardButton("🗓️ Расписание", callback_data="start_menu:schedule"))
            markup.add(InlineKeyboardButton("💳 Оплата", callback_data="start_menu:payment"))
            markup.add(InlineKeyboardButton("⚙️ Настройки", callback_data="start_menu:settings"))
            markup.add(InlineKeyboardButton("❓ Помощь", callback_data="start_menu:help"))
            
            try:
                bot_instance.send_message(chat_id, welcome_text, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                logger.error(f"[SCHEDULE BACK] Ошибка при отправке главного меню: {e}")
            
            logger.info(f"[SCHEDULE BACK] Пользователь {call.from_user.id} вернулся из расписания")
        except Exception as e:
            logger.error(f"[SCHEDULE BACK] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_type:"))
    def plan_type_callback(call):
        """Обработчик выбора типа плана"""
        logger.info("=" * 80)
        logger.info(f"[PLAN TYPE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
        logger.info(f"[PLAN TYPE] ✅ ОБРАБОТЧИК ВЫЗВАН!")
        # TODO: Извлечь из moviebot.py строки 10827-10868
        try:
            logger.info(f"[PLAN TYPE] Вызов answer_callback_query")
            bot_instance.answer_callback_query(call.id)
            logger.info(f"[PLAN TYPE] answer_callback_query выполнен")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_type = call.data.split(":")[1]  # 'home' или 'cinema'
            
            logger.info(f"[PLAN TYPE] Получен callback: user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}")
            logger.info(f"[PLAN TYPE] user_plan_state keys={list(user_plan_state.keys())}")
            logger.info(f"[PLAN TYPE] user_id in user_plan_state = {user_id in user_plan_state}")
            
            if user_id not in user_plan_state:
                logger.warning(f"[PLAN TYPE] Состояние не найдено для user_id={user_id}, текущие состояния: {list(user_plan_state.keys())}")
                bot_instance.edit_message_text("❌ Ошибка: сессия истекла. Начните заново с /plan", chat_id, call.message.message_id)
                return
            
            state = user_plan_state[user_id]
            link = state.get('link')
            
            if not link:
                bot_instance.edit_message_text("❌ Ошибка: не найдена ссылка на фильм. Начните заново с /plan", chat_id, call.message.message_id)
                del user_plan_state[user_id]
                return
            
            state['type'] = plan_type
            state['step'] = 3
            
            try:
                bot_instance.delete_message(chat_id, call.message.message_id)
            except:
                pass
            
            bot_instance.send_message(chat_id, f"📅 Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\nМожно указать:\n• День недели (сегодня, завтра, понедельник и т.д.)\n• Дату (01.01, 1 января и т.д.)\n• Время (19:00, 20:30)")
            
            logger.info(f"[PLAN TYPE] Пользователь {user_id} выбрал {plan_type}, link={link}")
        except Exception as e:
            logger.error(f"[PLAN TYPE] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data == "plan:cancel")
    def plan_cancel_callback(call):
        """Обработчик отмены плана"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        if user_id in user_plan_state:
            del user_plan_state[user_id]
            logger.info(f"[PLAN] Пользователь {user_id} вышел из режима планирования")
        
        bot_instance.answer_callback_query(call.id, "Режим планирования отменён")
        bot_instance.edit_message_text("✅ Режим планирования отменён. Можете использовать другие команды.", 
                             chat_id, call.message.message_id)

    @bot_instance.callback_query_handler(func=lambda call: call.data == "plan_from_list")
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
            
            bot_instance.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
            bot_instance.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
            logger.info(f"[PLAN FROM LIST] Состояние установлено для пользователя {user_id}")
        except Exception as e:
            logger.error(f"[PLAN FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_from_added:"))
    def plan_from_added_callback(call):
        """Обработчик планирования из добавленного фильма"""
        logger.info(f"[PLAN FROM ADDED] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[PLAN FROM ADDED] Получен callback: call.data={call.data}, user_id={call.from_user.id}, chat_id={call.message.chat.id}")
        try:
            bot_instance.answer_callback_query(call.id)  # Отвечаем сразу, чтобы убрать "крутилку"
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            kp_id = call.data.split(":")[1]
            
            logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать фильм kp_id={kp_id}")
            
            # Проверяем, есть ли фильм в базе, если нет - добавляем
            from moviebot.bot.handlers.series import ensure_movie_in_database
            from moviebot.api.kinopoisk_api import extract_movie_info
            
            link = None
            film_id = None
            with db_lock:
                cursor.execute('SELECT id, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    link = row.get('link') if isinstance(row, dict) else row[1]
                    logger.info(f"[PLAN FROM ADDED] Фильм найден в базе: film_id={film_id}, link={link}")
            
            if not film_id:
                # Фильм не в базе - добавляем
                if not link:
                    link = f"https://kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                if info:
                    film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                    if was_inserted:
                        logger.info(f"[PLAN FROM ADDED] Фильм добавлен в базу при планировании: kp_id={kp_id}, film_id={film_id}")
                    if not film_id:
                        bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                        return
                else:
                    bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                    return
            
            if not link:
                link = f"https://kinopoisk.ru/film/{kp_id}/"
                logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
            
            user_plan_state[user_id] = {
                'step': 2,
                'link': link,
                'chat_id': chat_id
            }
            
            logger.info(f"[PLAN FROM ADDED] Состояние установлено: user_id={user_id}, state={user_plan_state[user_id]}")
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
            markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
            
            logger.info(f"[PLAN FROM ADDED] Отправка сообщения с выбором типа просмотра...")
            bot_instance.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
            logger.info(f"[PLAN FROM ADDED] Сообщение отправлено успешно")
        except Exception as e:
            logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
        finally:
            logger.info(f"[PLAN FROM ADDED] ===== КОНЕЦ ОБРАБОТКИ =====")

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("add_ticket:"))
    def add_ticket_from_plan_callback(call):
        """Обработчик кнопки 'Добавить билеты' из подтверждения /plan"""
        try:
            from moviebot.utils.helpers import has_tickets_access
            from moviebot.states import user_ticket_state
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot_instance.answer_callback_query(
                    call.id, 
                    "🎫 Билеты в кино доступны с подпиской 🎫 Билеты или 📦 Все режимы. Подключите подписку через /payment", 
                    show_alert=True
                )
                return
            
            user_ticket_state[user_id] = {
                'step': 'waiting_ticket_file',
                'plan_id': plan_id,
                'chat_id': chat_id
            }
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            bot_instance.answer_callback_query(call.id, "Загрузите билеты в чат")
            bot_instance.send_message(
                chat_id,
                "🎟️ <b>Загрузите билеты в чат</b>\n\n"
                "Отправьте фото или файл с билетами в следующем сообщении.",
                reply_markup=markup, parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[ADD TICKET] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    # TODO: Добавить остальные callback handlers:
    # - plan_detail
    # - remove_from_calendar
    # - edit_plan handlers
    # и другие из moviebot.py
