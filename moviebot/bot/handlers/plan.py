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
    elif plan_type == 'home' and plan_id and kp_id:
        # Для планов "дома" показываем онлайн-кинотеатры
        try:
            from moviebot.api.kinopoisk_api import get_external_sources
            sources = get_external_sources(kp_id)
            if sources:
                # Сохраняем источники в базу для будущего использования
                import json
                sources_dict = {platform: url for platform, url in sources[:6]}
                sources_json = json.dumps(sources_dict, ensure_ascii=False)
                with db_lock:
                    cursor.execute('''
                        UPDATE plans 
                        SET ticket_file_id = %s 
                        WHERE id = %s
                    ''', (sources_json, plan_id))
                    conn.commit()
                
                markup = InlineKeyboardMarkup(row_width=2)
                for platform, url in sources[:6]:
                    markup.add(InlineKeyboardButton(platform, callback_data=f"streaming_select:{plan_id}:{platform}"))
                markup.add(InlineKeyboardButton("✅ Завершить", callback_data=f"streaming_done:{plan_id}"))
        except Exception as e:
            logger.warning(f"[PROCESS PLAN] Не удалось получить онлайн-кинотеатры: {e}", exc_info=True)
    
    # Добавляем кнопку "Перейти к описанию" для обоих типов планов (если есть kp_id)
    if kp_id:
        if not markup.keyboard:
            markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"view_film_description:{kp_id}"))
    
    text = f"✅ <b>{title}</b> запланирован на {date_str} {type_text}"
    if plan_type == 'home' and markup.keyboard and any(btn.callback_data.startswith("streaming_select:") for row in markup.keyboard for btn in row):
        text += f"\n\n📺 <b>Выберите онлайн-кинотеатр для просмотра:</b>"
    
    bot_instance.send_message(chat_id, text, parse_mode='HTML', reply_markup=markup if markup.keyboard else None)
    
    # Очищаем состояние планирования после успешного завершения
    if user_id in user_plan_state:
        del user_plan_state[user_id]
        logger.info(f"[PROCESS PLAN] Состояние планирования очищено для user_id={user_id}")
    
    return True


def register_plan_handlers(bot_instance):
    """Регистрирует обработчики команд /plan и /schedule"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER PLAN HANDLERS] ===== START: регистрация обработчиков планирования =====")
    logger.info(f"[REGISTER PLAN HANDLERS] bot_instance: {bot_instance}")
    
    @bot_instance.message_handler(commands=['plan'], func=lambda m: not m.reply_to_message)
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
                prompt_msg = bot_instance.reply_to(message, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!", reply_markup=markup)
                user_plan_state[user_id] = {'step': 1, 'chat_id': chat_id, 'prompt_message_id': prompt_msg.message_id}
                logger.info(f"[PLAN] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id}")
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
            
            # Проверяем, есть ли фильм в базе
            with db_lock:
                cursor.execute('SELECT id, title, watched, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
            
            existing = None
            link = None
            if row:
                if isinstance(row, dict):
                    film_id = row.get('id')
                    title = row.get('title')
                    watched = row.get('watched')
                    link = row.get('link')
                else:
                    film_id = row[0]
                    title = row[1]
                    watched = row[2]
                    link = row[3] if len(row) > 3 else None
                existing = (film_id, title, watched)
            
            if not link:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            # Если фильм в базе, формируем info из БД (без API запроса)
            if existing:
                logger.info(f"[SHOW FILM DESCRIPTION] Фильм в базе, формирую info из БД без API запроса")
                film_id, title, watched = existing
                
                # Получаем полную информацию из БД
                with db_lock:
                    cursor.execute('''
                        SELECT year, genres, description, director, actors, is_series
                        FROM movies WHERE id = %s AND chat_id = %s
                    ''', (film_id, chat_id))
                    db_row = cursor.fetchone()
                
                if db_row:
                    if isinstance(db_row, dict):
                        year = db_row.get('year')
                        genres = db_row.get('genres')
                        description = db_row.get('description')
                        director = db_row.get('director')
                        actors = db_row.get('actors')
                        is_series = bool(db_row.get('is_series', 0))
                    else:
                        year = db_row[0]
                        genres = db_row[1]
                        description = db_row[2]
                        director = db_row[3]
                        actors = db_row[4]
                        is_series = bool(db_row[5] if len(db_row) > 5 else 0)
                    
                    # Формируем словарь info из данных БД
                    info = {
                        'title': title,
                        'year': year,
                        'genres': genres,
                        'description': description,
                        'director': director,
                        'actors': actors,
                        'is_series': is_series
                    }
                    
                    # Ищем существующее сообщение с описанием фильма в bot_messages
                    from moviebot.states import bot_messages
                    film_message_id = None
                    for msg_id, link_value in bot_messages.items():
                        if link_value and kp_id in str(link_value):
                            film_message_id = msg_id
                            logger.info(f"[SHOW FILM DESCRIPTION] Найдено сообщение с описанием фильма: message_id={film_message_id}")
                            break
                    
                    # Обновляем или отправляем новое сообщение
                    from moviebot.bot.handlers.series import show_film_info_with_buttons
                    show_film_info_with_buttons(
                        chat_id, user_id, info, link, kp_id, existing=existing,
                        message_id=film_message_id
                    )
                    logger.info(f"[SHOW FILM DESCRIPTION] Сообщение обновлено/отправлено из БД: message_id={film_message_id}")
                else:
                    logger.warning(f"[SHOW FILM DESCRIPTION] Не удалось получить данные из БД, делаю API запрос")
                    from moviebot.api.kinopoisk_api import extract_movie_info
                    info = extract_movie_info(link)
                    if info:
                        from moviebot.bot.handlers.series import show_film_info_with_buttons
                        show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=None)
                    else:
                        bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            else:
                # Фильм не в базе - получаем информацию через API
                logger.info(f"[SHOW FILM DESCRIPTION] Фильм не в базе, получаю информацию через API")
                from moviebot.api.kinopoisk_api import extract_movie_info
                if not link:
                    link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                info = extract_movie_info(link)
                
                if not info:
                    bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                    return
                
                from moviebot.bot.handlers.series import show_film_info_with_buttons
                show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=None, message_id=None)
            
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

    logger.info(f"[REGISTER PLAN HANDLERS] Все обработчики планирования зарегистрированы (включая show_film_description и schedule_back)")
    logger.info(f"[REGISTER PLAN HANDLERS] ===== END =====")
    logger.info("=" * 80)


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
        
        # Отправляем сообщение с запросом даты/времени и сохраняем его message_id
        prompt_msg = bot_instance.send_message(chat_id, f"📅 Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\nМожно указать:\n• День недели (сегодня, завтра, понедельник и т.д.)\n• Дату (01.01, 1 января и т.д.)\n• Время (19:00, 20:30)")
        state['prompt_message_id'] = prompt_msg.message_id
        logger.info(f"[PLAN TYPE] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id}")
        
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
            prompt_msg = bot_instance.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
            # Сохраняем message_id промпта в состояние
            user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[PLAN FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
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


def get_plan_link_internal(message, state):
    """Внутренняя функция для получения ссылки на фильм в /plan"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    link = None
    
    # Проверяем, что сообщение является реплаем на сообщение бота
    from moviebot.bot.bot_init import BOT_ID
    is_reply = (message.reply_to_message and 
               message.reply_to_message.from_user and 
               message.reply_to_message.from_user.id == BOT_ID)
    
    prompt_message_id = state.get('prompt_message_id')
    # Если сообщение не является ответом на нужное сообщение бота, просто игнорируем его
    if not is_reply or (prompt_message_id and message.reply_to_message.message_id != prompt_message_id):
        logger.info(f"[PLAN LINK] Сообщение от пользователя {user_id} не является ответом на сообщение бота, игнорируем")
        return
    
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
                    cursor.execute('SELECT link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    row = cursor.fetchone()
                    if row:
                        link = row.get('link') if isinstance(row, dict) else row[0]
                        logger.info(f"[PLAN] Найден фильм по ID {kp_id} в тексте сообщения (из базы): {link}")
                    else:
                        link = f"https://kinopoisk.ru/film/{kp_id}/"
                        logger.info(f"[PLAN] Фильм с ID {kp_id} не найден в базе, создана ссылка: {link}")
    
    if not link:
        bot_instance.reply_to(message, "❌ Не найдена ссылка на фильм. Пришлите ссылку или ID фильма.")
        if user_id in user_plan_state:
            del user_plan_state[user_id]
        return
    
    user_plan_state[user_id]['link'] = link
    user_plan_state[user_id]['step'] = 2
    markup = InlineKeyboardMarkup()
    markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
    markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
    prompt_msg = bot_instance.send_message(message.chat.id, "Где планируете смотреть?", reply_markup=markup)
    user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
    logger.info(f"[PLAN] Сохранен prompt_message_id={prompt_msg.message_id} для user_id={user_id} (step=2)")


def get_plan_day_or_date_internal(message, state):
    """Внутренняя функция для получения дня/даты в /plan"""
    logger.info("=" * 80)
    logger.info(f"[PLAN DAY/DATE INTERNAL] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    user_id = message.from_user.id
    plan_type = state.get('type')
    link = state.get('link')
    prompt_message_id = state.get('prompt_message_id')
    
    logger.info(f"[PLAN DAY/DATE INTERNAL] prompt_message_id={prompt_message_id}, reply_to_message={message.reply_to_message.message_id if message.reply_to_message else None}")
    
    # КРИТИЧЕСКИ ВАЖНО: Проверяем, что это реплай на правильное сообщение бота
    from moviebot.bot.bot_init import BOT_ID
    is_reply = (message.reply_to_message and 
               message.reply_to_message.from_user and 
               message.reply_to_message.from_user.id == BOT_ID)
    
    # Если сообщение не является ответом на нужное сообщение бота, просто игнорируем его
    if not is_reply or (prompt_message_id and message.reply_to_message.message_id != prompt_message_id):
        logger.info(f"[PLAN DAY/DATE INTERNAL] Сообщение от пользователя {user_id} не является ответом на сообщение бота, игнорируем")
        return
    
    # Берем текст из сообщения пользователя (не из реплая)
    text = message.text.strip() if message.text else ""
    if not text:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Текст сообщения пуст, пропускаем")
        return
    
    text = text.lower().strip()
    
    logger.info(f"[PLAN DAY/DATE INTERNAL] Обработка: text='{text}', plan_type={plan_type}, link={link}, reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}")
    
    if not plan_type or not link:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Отсутствует plan_type или link: plan_type={plan_type}, link={link}")
        bot_instance.reply_to(message, "❌ Ошибка: не указан тип просмотра или ссылка. Начните заново.")
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
            if phrase in text:
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
            if 'сегодня' in text:
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
            elif 'завтра' in text:
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
            elif 'следующая неделя' in text or 'след неделя' in text or 'след. неделя' in text or 'на следующей неделе' in text:
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
            else:
                # Парсинг дат: "15 января", "15 января 17:00", "10.01", "14 апреля"
                # Сначала пробуем формат с временем: "15 января 17:00" или "10 января 20:30"
                date_time_match = re.search(r'(\d{1,2})\s+([а-яё]+)\s+(\d{1,2}):(\d{2})', text)
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
                    date_match = re.search(r'(\d{1,2})\s+([а-яё]+)', text)
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
                        date_time_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?\s+(\d{1,2}):(\d{2})', text)
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
                            date_match = re.search(r'(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?', text)
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
    
    if not plan_dt:
        logger.warning(f"[PLAN DAY/DATE INTERNAL] Не удалось распознать дату из текста: '{text}'")
        bot_instance.reply_to(message, "Не удалось распознать день/дату. Попробуйте снова.")
        return
    
    # Вызываем process_plan
    message_date_utc = datetime.fromtimestamp(message.date, tz=pytz.utc) if message.date else None
    # Преобразуем plan_dt обратно в строку для process_plan
    day_or_date_str = plan_dt.strftime('%d.%m.%Y %H:%M') if plan_dt else None
    result = process_plan(bot_instance, user_id, message.chat.id, link, plan_type, day_or_date_str, message_date_utc)
    if result == 'NEEDS_TIMEZONE':
        show_timezone_selection(message.chat.id, user_id, "Для планирования фильма нужно выбрать часовой пояс:")
    elif result:
        # process_plan уже очистил состояние, но на всякий случай проверим
        if user_id in user_plan_state:
            del user_plan_state[user_id]
            logger.info(f"[PLAN DAY/DATE INTERNAL] Состояние планирования очищено для user_id={user_id}")
        if user_id in user_plan_state:
            del user_plan_state[user_id]


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan:"))
def edit_plan_callback(call):
    """Обработчик выбора плана для редактирования"""
    logger.info(f"[EDIT PLAN] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        logger.info(f"[EDIT PLAN] Пользователь {user_id} хочет редактировать план {plan_id}")
        
        from moviebot.states import user_edit_state
        from moviebot.database.db_operations import get_user_timezone_or_default
        
        # Очищаем состояние редактирования при возврате к меню
        if user_id in user_edit_state and user_edit_state[user_id].get('action') == 'edit_plan_datetime':
            # Оставляем только базовую информацию для меню редактирования
            user_edit_state[user_id] = {
                'action': 'edit_plan',
                'plan_id': plan_id
            }
        
        # Получаем информацию о плане
        with db_lock:
            cursor.execute('''
                SELECT p.plan_type, p.plan_datetime, m.title
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            plan_row = cursor.fetchone()
        
        if not plan_row:
            bot_instance.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
            logger.warning(f"[EDIT PLAN] План {plan_id} не найден")
            return
        
        if isinstance(plan_row, dict):
            plan_type = plan_row.get('plan_type')
            plan_dt_value = plan_row.get('plan_datetime')
            title = plan_row.get('title')
        else:
            plan_type = plan_row[0]
            plan_dt_value = plan_row[1]
            title = plan_row[2]
        
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
        
        user_edit_state[user_id] = {
            'action': 'edit_plan',
            'plan_id': plan_id,
            'plan_type': plan_type
        }
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📅 Изменить дату/время", callback_data=f"edit_plan_datetime:{plan_id}"))
        if plan_type == 'cinema':
            markup.add(InlineKeyboardButton("🎟️ Загрузить билеты", callback_data=f"edit_plan_ticket:{plan_id}"))
            markup.add(InlineKeyboardButton("🏠 Переключить в 'дома'", callback_data=f"edit_plan_switch:{plan_id}"))
        else:
            markup.add(InlineKeyboardButton("📺 Изменить онлайн-кинотеатр", callback_data=f"edit_plan_streaming:{plan_id}"))
            markup.add(InlineKeyboardButton("🎦 Переключить в 'в кино'", callback_data=f"edit_plan_switch:{plan_id}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
        
        text = f"✏️ <b>Редактирование плана:</b>\n\n"
        text += f"<b>{title}</b>\n"
        text += f"Тип: {'🎦 в кино' if plan_type == 'cinema' else '🏠 дома'}\n"
        text += f"Дата/время: {date_str}\n\n"
        text += f"Что вы хотите изменить?"
        
        bot_instance.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[EDIT PLAN] Меню редактирования отправлено для плана {plan_id}")
    except Exception as e:
        logger.error(f"[EDIT PLAN] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("remove_from_calendar:"))
def handle_remove_from_calendar_callback(call):
    """Обработчик удаления фильма из календаря"""
    logger.info(f"[REMOVE FROM CALENDAR] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        plan_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[REMOVE FROM CALENDAR] Удаление плана {plan_id} пользователем {user_id}")
        
        bot_instance.answer_callback_query(call.id)
        
        with db_lock:
            # Получаем информацию о плане
            cursor.execute('''
                SELECT p.id, m.title
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                bot_instance.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
                logger.warning(f"[REMOVE FROM CALENDAR] План {plan_id} не найден")
                return
            
            title = row.get('title') if isinstance(row, dict) else row[1]
            
            # Удаляем план
            cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            conn.commit()
        
        bot_instance.answer_callback_query(call.id, f"✅ Фильм '{title}' удалён из календаря")
        logger.info(f"[REMOVE FROM CALENDAR] План {plan_id} удалён пользователем {user_id}")
        
        # Обновляем сообщение, убирая кнопки
        try:
            bot_instance.edit_message_reply_markup(chat_id, call.message.message_id, reply_markup=None)
        except Exception as e:
            logger.warning(f"[REMOVE FROM CALENDAR] Не удалось обновить сообщение: {e}")
    except Exception as e:
        logger.error(f"[REMOVE FROM CALENDAR] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("streaming_select:"))
def streaming_select_callback(call):
    """Обработчик выбора онлайн-кинотеатра"""
    logger.info(f"[STREAMING SELECT] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        parts = call.data.split(":")
        plan_id = int(parts[1])
        platform = parts[2] if len(parts) > 2 else ''
        
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[STREAMING SELECT] Пользователь {user_id} выбрал кинотеатр {platform} для плана {plan_id}")
        
        # Получаем источники из базы (сохранены в ticket_file_id как JSON)
        with db_lock:
            cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            sources_row = cursor.fetchone()
            sources_json = sources_row.get('ticket_file_id') if sources_row and isinstance(sources_row, dict) else (sources_row[0] if sources_row else None)
            
            if sources_json:
                import json
                try:
                    sources_dict = json.loads(sources_json)
                    url = sources_dict.get(platform, '')
                    
                    if url:
                        # Сохраняем выбор кинотеатра в базу
                        cursor.execute('''
                            UPDATE plans 
                            SET streaming_service = %s, streaming_url = %s, streaming_done = FALSE
                            WHERE id = %s AND chat_id = %s
                        ''', (platform, url, plan_id, chat_id))
                        conn.commit()
                        
                        bot_instance.answer_callback_query(call.id, f"✅ Выбран {platform}")
                        logger.info(f"[STREAMING SELECT] Кинотеатр {platform} сохранен для плана {plan_id}")
                        
                        # Отправляем сообщение-подтверждение в чат
                        bot_instance.send_message(
                            chat_id,
                            f"✅ Онлайн-кинотеатр выбран: <b>{platform}</b>",
                            parse_mode='HTML'
                        )
                        logger.info(f"[STREAMING SELECT] Сообщение-подтверждение отправлено для плана {plan_id}")
                        
                        # Удаляем сообщение с выбором кинотеатра
                        try:
                            bot_instance.delete_message(chat_id, call.message.message_id)
                        except Exception as e:
                            logger.warning(f"[STREAMING SELECT] Не удалось удалить сообщение: {e}")
                    else:
                        bot_instance.answer_callback_query(call.id, "❌ Кинотеатр не найден", show_alert=True)
                except json.JSONDecodeError:
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка данных", show_alert=True)
            else:
                bot_instance.answer_callback_query(call.id, "❌ Источники не найдены", show_alert=True)
    except Exception as e:
        logger.error(f"[STREAMING SELECT] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("streaming_done:"))
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
                bot_instance.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
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
        
        bot_instance.answer_callback_query(call.id, "✅")
        
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
        
        type_text = "дома" if plan_type == 'home' else "в кино"
        confirmation_text = f"✅ <b>{title}</b> запланирован на {date_str} {type_text}"
        
        # Получаем kp_id для кнопки "Перейти к описанию"
        kp_id = None
        with db_lock:
            cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            movie_row = cursor.fetchone()
            if movie_row:
                kp_id = movie_row.get('kp_id') if isinstance(movie_row, dict) else movie_row[0]
        
        # Создаем кнопку "Перейти к описанию"
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup()
        if kp_id:
            markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data=f"view_film_description:{kp_id}"))
        
        # Обновляем сообщение вместо удаления
        try:
            bot_instance.edit_message_text(
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
                bot_instance.send_message(chat_id, confirmation_text, parse_mode='HTML')
                bot_instance.delete_message(chat_id, message_id)
            except Exception as e2:
                logger.error(f"[STREAMING DONE] Не удалось отправить новое сообщение: {e2}")
    except Exception as e:
        logger.error(f"[STREAMING DONE] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


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
            bot_instance.reply_to(message, "❌ Ошибка: план не найден.")
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
            bot_instance.reply_to(message, "❌ План не найден.")
            if user_id in user_edit_state:
                del user_edit_state[user_id]
            return
        
        if isinstance(plan_row, dict):
            link = plan_row.get('link')
            plan_type = plan_row.get('plan_type')
        else:
            link = plan_row[0]
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
            bot_instance.reply_to(message, f"✅ Дата и время плана обновлены: {date_str} {tz_name}")
            logger.info(f"[EDIT PLAN DATETIME INTERNAL] План {plan_id} обновлен: {date_str}")
            if user_id in user_edit_state:
                del user_edit_state[user_id]
        else:
            bot_instance.reply_to(message, "❌ Не удалось распознать дату/время. Попробуйте еще раз.")
            logger.warning(f"[EDIT PLAN DATETIME INTERNAL] Не удалось распознать дату/время из текста: '{text}'")
    except Exception as e:
        logger.error(f"[EDIT PLAN DATETIME INTERNAL] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке.")
        except:
            pass
