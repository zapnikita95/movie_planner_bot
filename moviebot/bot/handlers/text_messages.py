"""
Единый главный обработчик для всех текстовых сообщений
Обрабатывает состояния, реплаи, ссылки на Кинопоиск и т.д.
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

# КРИТИЧЕСКИ ВАЖНО: Импортируем bot_instance ДО всех декораторов
from moviebot.bot.bot_init import bot as bot_instance

# Логируем, что модуль импортирован (декораторы выполнятся при импорте)
logger = logging.getLogger(__name__)
logger.info("=" * 80)
logger.info("[TEXT MESSAGES] Модуль text_messages.py импортирован - декораторы будут зарегистрированы")
logger.info(f"[TEXT MESSAGES] bot_instance: {bot_instance} (тип: {type(bot_instance).__name__})")
logger.info("=" * 80)

from moviebot.database.db_operations import log_request, get_user_timezone_or_default, set_notification_setting
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import extract_movie_info, search_films
from moviebot.states import (
    user_search_state, user_plan_state, user_ticket_state,
    user_settings_state, user_edit_state, user_view_film_state,
    user_import_state, user_clean_state, user_cancel_subscription_state,
    user_refund_state, user_promo_state, user_promo_admin_state,
    user_unsubscribe_state, user_add_admin_state,
    bot_messages, plan_error_messages, list_messages, added_movie_messages, rating_messages
)
from moviebot.utils.parsing import parse_session_time, extract_kp_id_from_text
from moviebot.bot.handlers.series import search_films_with_type, show_film_info_with_buttons, show_film_info_without_adding
from moviebot.bot.handlers.list import handle_view_film_reply_internal
from moviebot.bot.bot_init import BOT_ID
# Импортируем обработчики промокодов для автоматической регистрации
import moviebot.bot.handlers.promo  # noqa: F401
# Импортируем обработчики админских команд для автоматической регистрации
import moviebot.bot.handlers.admin  # noqa: F401
from moviebot.database.db_operations import add_and_announce, is_bot_participant, get_watched_emojis

# logger уже создан выше
conn = get_db_connection()
cursor = get_db_cursor()


# ==================== ОБРАБОТЧИКИ С ПРИОРИТЕТАМИ (ДО main_text_handler) ====================

def add_reactions_check(message):
    """Проверка для обработчика add_reactions"""
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return False
    if not message.reply_to_message:
        return False
    if message.from_user.id not in user_settings_state:
        return False
    state = user_settings_state.get(message.from_user.id, {})
    if not state.get('adding_reactions'):
        return False
    if message.reply_to_message.message_id != state.get('settings_msg_id'):
        return False
    logger.info(f"[SETTINGS CHECK] add_reactions_check: True для user_id={message.from_user.id}")
    return True


@bot_instance.message_handler(func=add_reactions_check)
def add_reactions(message):
    """Обработчик добавления реакций"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    state = user_settings_state.get(user_id, {})
    settings_msg_id = state.get('settings_msg_id')
    action = state.get('action', 'replace')
    
    logger.info(f"[SETTINGS] add_reactions вызван для user_id={user_id}, action={action}")
    
    # Собираем обычные эмодзи и custom_id из сообщения
    emojis = []
    custom_ids = []
    
    # Обычные эмодзи из текста
    if message.text:
        emoji_pattern = re.compile(
            r'[\U0001F300-\U0001F9FF]'  # Различные символы и пиктограммы
            r'|[\U0001F600-\U0001F64F]'  # Эмодзи лиц
            r'|[\U0001F680-\U0001F6FF]'  # Транспорт и карты
            r'|[\U00002600-\U000026FF]'  # Разные символы
            r'|[\U00002700-\U000027BF]'  # Dingbats
            r'|[\U0001F900-\U0001F9FF]'  # Дополнительные символы
            r'|[\U0001FA00-\U0001FAFF]'  # Шахматы и другие
            r'|[\U00002B50-\U00002B55]'  # Звезды
            r'|👍|✅|❤️|🔥|🎉|😂|🤣|😍|😢|😡|👎|⭐|🌟|💯|🎬|🍿'  # Популярные эмодзи
        )
        emojis = emoji_pattern.findall(message.text)
    
    # Кастомные эмодзи из entities
    if message.entities:
        for entity in message.entities:
            if entity.type == 'custom_emoji' and hasattr(entity, 'custom_emoji_id'):
                custom_id = str(entity.custom_emoji_id)
                custom_ids.append(custom_id)
    
    new_reactions = emojis + [f"custom:{cid}" for cid in custom_ids]
    
    if not new_reactions:
        bot_instance.reply_to(message, "❌ Не нашёл эмодзи в вашем сообщении. Попробуйте отправить эмодзи снова.")
        return
    
    # Сохраняем в БД
    try:
        with db_lock:
            current_emojis_local = get_watched_emojis(chat_id)
            
            if action == "add":
                all_emojis = ''.join(current_emojis_local) + ''.join(emojis)
                seen = set()
                unique_emojis = ''.join(c for c in all_emojis if c not in seen and not seen.add(c))
            else:
                unique_emojis = ''.join(emojis)
            
            cursor.execute('''
                INSERT INTO settings (chat_id, key, value)
                VALUES (%s, 'watched_emoji', %s)
                ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
            ''', (chat_id, unique_emojis))
            conn.commit()
        
        action_text = "добавлены к текущим" if action == "add" else "заменены"
        bot_instance.reply_to(message, f"✅ Реакции {action_text}:\n{unique_emojis}")
        logger.info(f"[SETTINGS] Реакции обновлены для чата {chat_id}, user_id={user_id}: {unique_emojis}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка при сохранении реакций: {e}", exc_info=True)
        bot_instance.reply_to(message, "❌ Произошла ошибка при сохранении реакций.")
    
    # Очищаем состояние
    if user_id in user_settings_state:
        del user_settings_state[user_id]


@bot_instance.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in added_movie_messages and m.text and m.text.strip().isdigit() and 1 <= int(m.text.strip()) <= 10)
def handle_added_movie_rating_reply(message):
    """Обрабатывает реплай на сообщение 'Добавлено в базу' с числом от 1 до 10"""
    try:
        reply_msg_id = message.reply_to_message.message_id
        movie_data = added_movie_messages.get(reply_msg_id)
        if not movie_data:
            return
        
        rating = int(message.text.strip())
        user_id = message.from_user.id
        chat_id = message.chat.id
        film_id = movie_data['film_id']
        kp_id = movie_data['kp_id']
        title = movie_data['title']
        
        # Предлагаем зачесть оценку и просмотр
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("✅ Да, зачесть", callback_data=f"confirm_rating:{film_id}:{rating}"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="cancel_rating"))
        
        bot_instance.reply_to(
            message,
            f"💡 Зачесть оценку <b>{rating}/10</b> и отметить фильм <b>{title}</b> как просмотренный?",
            parse_mode='HTML',
            reply_markup=markup
        )
        logger.info(f"[ADDED MOVIE REPLY] Предложено зачесть оценку {rating} для фильма {title} (film_id={film_id})")
    except Exception as e:
        logger.error(f"[ADDED MOVIE REPLY] Ошибка: {e}", exc_info=True)


@bot_instance.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text)
def handle_rate_list_reply(message):
    """Обработчик реплаев на сообщения бота с оценками"""
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        return
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем, что это реплай на список фильмов
    reply_text = message.reply_to_message.text or ""
    if "Список просмотренных фильмов для оценки" not in reply_text:
        return
    
    text = message.text.strip()
    if not text:
        return
    
    # Парсим оценки: kp_id оценка (разделители: пробел, запятая, точка с запятой, таб)
    ratings_pattern = r'(\d+)\s*[,;:\t]?\s*(\d+)'
    matches = re.findall(ratings_pattern, text)
    
    if not matches:
        bot_instance.reply_to(message, "❌ Не удалось распознать оценки. Используйте формат: <code>kp_id оценка</code>", parse_mode='HTML')
        return
    
    results = []
    errors = []
    
    with db_lock:
        for kp_id_str, rating_str in matches:
            try:
                kp_id = kp_id_str.strip()
                rating = int(rating_str.strip())
                
                if not (1 <= rating <= 10):
                    errors.append(f"{kp_id}: оценка должна быть от 1 до 10")
                    continue
                
                # Находим фильм по kp_id
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, kp_id))
                film_row = cursor.fetchone()
                
                if not film_row:
                    errors.append(f"{kp_id}: фильм не найден или не просмотрен")
                    continue
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                # Проверяем, не оценил ли уже пользователь этот фильм
                cursor.execute('''
                    SELECT rating FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                existing = cursor.fetchone()
                
                if existing:
                    errors.append(f"{kp_id}: вы уже оценили этот фильм")
                    continue
                
                # Сохраняем оценку
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                
                results.append((kp_id, title, rating))
                
                # Проверяем, все ли активные пользователи оценили фильм
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM stats
                    WHERE chat_id = %s AND user_id IS NOT NULL
                ''', (chat_id,))
                active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
                # Получаем всех, кто оценил этот фильм (только неимпортированные оценки)
                cursor.execute('''
                    SELECT DISTINCT user_id FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id))
                rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
                # Если все активные пользователи оценили, отмечаем фильм как просмотренный
                if active_users and active_users.issubset(rated_users):
                    cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    logger.info(f"[RATE] Все активные пользователи оценили фильм {film_id}, отмечен как просмотренный")
                
            except ValueError:
                errors.append(f"{kp_id_str}: неверный формат оценки")
            except Exception as e:
                logger.error(f"Ошибка при сохранении оценки {kp_id_str}: {e}")
                errors.append(f"{kp_id_str}: ошибка обработки")
        
        conn.commit()
    
    # Формируем ответ
    response_text = ""
    
    if results:
        user_name = message.from_user.first_name or f"user_{user_id}"
        response_text += f"✅ <b>{user_name}</b> поставил(а) оценки:\n\n"
        for kp_id, title, rating in results:
            response_text += f"• <b>{kp_id}</b> — {title}: {rating}/10\n"
        response_text += "\n"
    
    if errors:
        response_text += "⚠️ <b>Ошибки:</b>\n"
        for error in errors:
            response_text += f"• {error}\n"
    
    if not results and not errors:
        response_text = "❌ Не удалось обработать оценки. Проверьте формат."
    
    bot_instance.reply_to(message, response_text, parse_mode='HTML')


def is_kinopoisk_link(message):
    """
    Извлекает все ссылки на Kinopoisk из сообщения через message.entities.
    Возвращает список ссылок или None, если ссылок нет.
    """
    links = []
    
    if not message.text:
        return None
    
    # Проверяем entities (рекомендуемый способ - надёжный)
    if message.entities:
        text = message.text
        for entity in message.entities:
            if entity.type == 'url':
                # Извлекаем URL из текста по offset и length
                link = text[entity.offset:entity.offset + entity.length]
                if 'kinopoisk.ru' in link or 'kinopoisk.com' in link:
                    links.append(link)
            elif entity.type == 'text_link':
                # Ссылка в виде text_link (гиперссылка)
                if 'kinopoisk.ru' in entity.url or 'kinopoisk.com' in entity.url:
                    links.append(entity.url)
    
    # Fallback: если entities не сработали, проверяем текст напрямую
    # (убираем угловые скобки, которые Telegram добавляет для форматирования)
    if not links:
        clean_text = message.text.replace('<', '').replace('>', '')
        # Ищем ссылки через regex
        found_links = re.findall(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', clean_text)
        links.extend(found_links)
    
    # Убираем дубликаты, сохраняя порядок
    seen = set()
    unique_links = []
    for link in links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    
    return unique_links if unique_links else None


@bot_instance.message_handler(func=lambda m: (
    m.text and 
    not m.text.strip().startswith('/plan') and
    is_kinopoisk_link(m) is not None
))
def save_movie_message(message):
    """Обрабатывает сообщения пользователей со ссылками на фильмы: добавляет в базу и отправляет карточку"""
    logger.info(f"[SAVE MOVIE] save_movie_message вызван для пользователя {message.from_user.id}, текст: '{message.text[:100]}'")
    
    # Извлекаем все ссылки на Kinopoisk
    links = is_kinopoisk_link(message)
    
    if not links:
        logger.info(f"[SAVE MOVIE] Ссылки на Kinopoisk не найдены")
        return
    
    # Сохраняем первую ссылку в bot_messages для обработки реакций
    try:
        if links:
            bot_messages[message.message_id] = links[0]
            logger.info(f"[SAVE MOVIE] Ссылка сохранена в bot_messages для message_id={message.message_id}: {links[0]}")
    except Exception as e:
        logger.warning(f"[SAVE MOVIE] Ошибка при сохранении ссылки в bot_messages: {e}")
    
    # Пропускаем, если пользователь работает с билетами или планированием
    if message.from_user.id in user_ticket_state:
        state = user_ticket_state.get(message.from_user.id, {})
        step = state.get('step')
        logger.info(f"[SAVE MOVIE] Пропущено - пользователь в user_ticket_state, step={step}")
        return
    
    if message.from_user.id in user_plan_state:
        logger.info(f"[SAVE MOVIE] Пропущено - пользователь в user_plan_state")
        return
    
    try:
        if links:
            chat_id = message.chat.id
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, 'add_movie', chat_id)
            logger.info(f"[SAVE MESSAGE] Найдено ссылок на фильмы: {len(links)}, chat_id={chat_id}")
            
            added_count = 0
            for link in links:
                if add_and_announce(link, chat_id):
                    added_count += 1
                    logger.info(f"[SAVE MESSAGE] Фильм обработан: {link}")
            
            if added_count > 1:
                bot_instance.send_message(chat_id, f"🎉 Добавлено {added_count} новых фильмов в базу!")
    except Exception as e:
        logger.warning(f"[SAVE MESSAGE] Ошибка при обработке сообщения с фильмом: {e}", exc_info=True)


@bot_instance.message_handler(content_types=['text'], func=lambda m: not (m.text and m.text.strip().startswith('/')))
def main_text_handler(message):
    """Единый главный хэндлер для всех текстовых сообщений (исключая команды)"""
    logger.info(f"[MAIN TEXT HANDLER] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}, text='{message.text[:100] if message.text else ''}'")
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    # 1. Проверяем состояния (ticket, settings, plan, edit, search, view_film)
    
    # === user_ticket_state ===
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_ticket_state, step={step}")
        
        # Обработка билета на мероприятие
        if state.get('type') == 'event':
            if step == 'event_name':
                # Получаем название мероприятия
                event_name = text.strip()
                if not event_name:
                    bot_instance.reply_to(message, "❌ Название мероприятия не может быть пустым. Попробуйте еще раз.")
                    return
                
                state['event_name'] = event_name
                state['step'] = 'event_datetime'
                
                bot_instance.reply_to(
                    message,
                    f"✅ Название мероприятия: <b>{event_name}</b>\n\n"
                    "Теперь укажите дату и время мероприятия в ответ на это сообщение.\n"
                    "Формат: 15 января 19:30 или 17.01 15:20",
                    parse_mode='HTML'
                )
                return
            
            elif step == 'event_datetime':
                # Получаем дату и время
                user_tz = get_user_timezone_or_default(user_id)
                event_dt = parse_session_time(text, user_tz)
                
                if not event_dt:
                    bot_instance.reply_to(message, "❌ Не удалось распознать дату и время. Попробуйте в формате:\n• 15 января 19:30\n• 17.01 15:20")
                    return
                
                state['event_datetime'] = event_dt
                state['step'] = 'event_file'
                
                import pytz
                event_utc = event_dt.astimezone(pytz.utc)
                state['event_datetime_utc'] = event_utc
                
                tz_name = "MSK" if user_tz.zone == 'Europe/Moscow' else "CET" if user_tz.zone == 'Europe/Belgrade' else "UTC"
                formatted_time = event_dt.strftime('%d.%m.%Y %H:%M')
                
                bot_instance.reply_to(
                    message,
                    f"✅ Дата и время: <b>{formatted_time} {tz_name}</b>\n\n"
                    "Теперь отправьте файл или картинку с билетом:",
                    parse_mode='HTML'
                )
                return
        
        if step == 'waiting_new_session':
            # Обработка ввода нового сеанса (фильм + дата)
            from moviebot.bot.handlers.series import handle_new_session_input_internal
            handle_new_session_input_internal(message, state)
            return
        
        if step == 'upload_ticket':
            # Если ждём билеты, но пришёл текст (например "готово")
            if text.lower().strip() == 'готово':
                from moviebot.bot.handlers.series import ticket_done_internal
                ticket_done_internal(message, state)
                return
            # Иначе игнорируем текст (билеты обрабатываются отдельным хэндлером для фото/документов)
            logger.info(f"[MAIN TEXT HANDLER] Игнорируем текст в режиме upload_ticket (ожидаются фото/документы)")
            return
        
        if step == 'waiting_session_time':
            from moviebot.bot.handlers.series import handle_edit_ticket_text_internal
            handle_edit_ticket_text_internal(message, state)
            return
    
    # === user_search_state ===
    logger.info(f"[MAIN TEXT HANDLER] Проверка user_search_state: user_id={user_id}, keys={list(user_search_state.keys())}")
    if user_id in user_search_state:
        state = user_search_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] ✅ Пользователь {user_id} в user_search_state: {state}")
        
        # Обработка ответа на /search без запроса
        # Проверяем, что это ответ на сообщение бота или просто текст от пользователя в состоянии поиска
        saved_message_id = state.get('message_id')
        is_reply_to_search = message.reply_to_message and message.reply_to_message.message_id == saved_message_id
        is_text_in_search_state = text and not message.reply_to_message  # Текст без ответа, но в состоянии поиска
        
        logger.info(f"[SEARCH STATE] saved_message_id={saved_message_id}, is_reply_to_search={is_reply_to_search}, is_text_in_search_state={is_text_in_search_state}, reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}")
        logger.info(f"[SEARCH STATE] text='{text}', text.strip()='{text.strip() if text else ''}'")
        
        # Если пользователь в состоянии поиска и отправил текст, обрабатываем его независимо от reply_to_message
        # Обрабатываем ЛЮБОЙ текст от пользователя в состоянии поиска
        if text and text.strip():
            logger.info(f"[SEARCH STATE] ✅ Обрабатываем запрос поиска: '{text.strip()}'")
            query = text
            if query:
                # Получаем тип поиска из состояния
                search_type = state.get('search_type', 'mixed')
                # Удаляем состояние
                del user_search_state[user_id]
                # Вызываем обработчик поиска
                logger.info(f"[SEARCH] Поиск по запросу '{query}' от пользователя {user_id}, тип: {search_type}")
                films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
                if not films:
                    bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
                    return
                
                # Формируем сообщение с результатами
                results_text = f"🔍 Результаты поиска '{query}':\n\n"
                markup = InlineKeyboardMarkup(row_width=1)
                
                for film in films[:10]:  # Показываем максимум 10 результатов на странице
                    title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                    year = film.get('year') or film.get('releaseYear') or 'N/A'
                    rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    # Определяем тип (сериал или фильм) по полю type из API
                    film_type = film.get('type', '').upper()  # "FILM" или "TV_SERIES"
                    is_series = film_type == 'TV_SERIES'
                    type_indicator = "📺" if is_series else "🎬"
                    
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
                
                bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                logger.info(f"✅ Ответ на /search отправлен пользователю {user_id}, найдено {len(films)} результатов")
            else:
                logger.warning(f"[SEARCH] Пустой запрос от пользователя {user_id}")
            return
        else:
            logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано: '{text}' (reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}, saved_message_id={saved_message_id})")
    
    # === user_import_state ===
    if user_id in user_import_state:
        state = user_import_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_import_state, step={step}")
        
        if step == 'waiting_user_id':
            from moviebot.bot.handlers.series import handle_import_user_id_internal
            handle_import_user_id_internal(message, state)
            return
    
    # === user_edit_state ===
    if user_id in user_edit_state:
        state = user_edit_state[user_id]
        action = state.get('action')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_edit_state, action={action}")
        
        if action == 'edit_rating':
            from moviebot.bot.handlers.rate import handle_edit_rating_internal
            handle_edit_rating_internal(message, state)
            return
        
        if action == 'edit_plan_datetime':
            from moviebot.bot.handlers.plan import handle_edit_plan_datetime_internal
            handle_edit_plan_datetime_internal(message, state)
            return
    
    # === user_settings_state ===
    if user_id in user_settings_state:
        state = user_settings_state.get(user_id)
        if state.get('waiting_notify_time'):
            time_str = message.text.strip()
            try:
                if ':' in time_str:
                    parts = time_str.split(':')
                    if len(parts) == 2:
                        hour = int(parts[0])
                        minute = int(parts[1])
                        if 0 <= hour <= 23 and 0 <= minute <= 59:
                            notify_type = state.get('waiting_notify_time')
                            
                            if notify_type == 'home' or notify_type.startswith('home_'):
                                if notify_type == 'home':
                                    set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'home_weekday':
                                    set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (будни) установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'home_weekend':
                                    set_notification_setting(chat_id, 'notify_home_weekend_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekend_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (выходные) установлено: {hour:02d}:{minute:02d}")
                            
                            elif notify_type == 'cinema' or notify_type.startswith('cinema_'):
                                if notify_type == 'cinema':
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'cinema_weekday':
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (будни) установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'cinema_weekend':
                                    set_notification_setting(chat_id, 'notify_cinema_weekend_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekend_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (выходные) установлено: {hour:02d}:{minute:02d}")
                            
                            if user_id in user_settings_state:
                                del user_settings_state[user_id]
                            return
                        else:
                            bot_instance.reply_to(message, "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)")
                            return
            except ValueError:
                bot_instance.reply_to(message, "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)")
                return
            except Exception as e:
                logger.error(f"[SETTINGS] Ошибка при сохранении времени напоминаний: {e}", exc_info=True)
                bot_instance.reply_to(message, "❌ Произошла ошибка при сохранении времени.")
                if user_id in user_settings_state:
                    del user_settings_state[user_id]
                return
        
        if message.reply_to_message:
            settings_msg_id = state.get('settings_msg_id')
            if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                if state.get('adding_reactions'):
                    from moviebot.bot.handlers.series import handle_settings_emojis
                    handle_settings_emojis(message)
                    return
    
    # === user_view_film_state ===
    if user_id in user_view_film_state:
        state = user_view_film_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_view_film_state")
        handle_view_film_reply_internal(message, state)
        return
    
    # === user_plan_state ===
    if user_id in user_plan_state:
        state = user_plan_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_plan_state, step={step}")
        
        if step == 1:
            from moviebot.bot.handlers.plan import get_plan_link_internal
            get_plan_link_internal(message, state)
            return
        
        if step == 3:
            from moviebot.bot.handlers.plan import get_plan_day_or_date_internal
            get_plan_day_or_date_internal(message, state)
            return
    
    # === user_clean_state ===
    if user_id in user_clean_state:
        if text.upper().strip() == 'ДА, УДАЛИТЬ':
            from moviebot.bot.handlers.series import handle_clean_confirm_internal
            handle_clean_confirm_internal(message)
            return
    
    # === user_promo_state ===
    if user_id in user_promo_state:
        state = user_promo_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_promo_state")
        
        # Проверяем, что это ответ на сообщение бота
        if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
            promo_code = text.strip().upper()
            
            # Применяем промокод
            from moviebot.utils.promo import apply_promocode
            success, discounted_price, message_text, promocode_id = apply_promocode(
                promo_code,
                state['original_price'],
                user_id,
                chat_id
            )
            
            if success:
                # Промокод применен успешно
                sub_type = state['sub_type']
                plan_type = state['plan_type']
                period_type = state['period_type']
                group_size = state.get('group_size')
                payment_id = state.get('payment_id', '')
                
                # Обновляем цену в состоянии платежа
                from moviebot.states import user_payment_state
                if user_id in user_payment_state:
                    payment_state = user_payment_state[user_id]
                    payment_state['price'] = discounted_price
                    payment_state['promocode_id'] = promocode_id
                    payment_state['promocode'] = promo_code
                    payment_state['original_price'] = state['original_price']
                
                # Формируем сообщение с обновленной ценой
                period_names = {
                    'month': 'месяц',
                    '3months': '3 месяца',
                    'year': 'год',
                    'lifetime': 'навсегда'
                }
                period_name = period_names.get(period_type, period_type)
                
                plan_names = {
                    'notifications': 'Уведомления о сериалах',
                    'recommendations': 'Персональные рекомендации',
                    'tickets': 'Билеты в кино',
                    'all': 'Все режимы'
                }
                plan_name = plan_names.get(plan_type, plan_type)
                
                subscription_type_name = 'Личная подписка' if sub_type == 'personal' else f'Групповая подписка (на {group_size} участников)'
                
                from moviebot.bot.callbacks.payment_callbacks import rubles_to_stars
                stars_amount = rubles_to_stars(discounted_price)
                
                text_result = f"✅ {message_text}\n\n"
                text_result += f"💳 <b>Оплата подписки</b>\n\n"
                text_result += f"📋 <b>Выбранный тариф:</b>\n"
                if sub_type == 'personal':
                    text_result += f"👤 Личная подписка\n"
                else:
                    text_result += f"👥 Групповая подписка (на {group_size} участников)\n"
                text_result += f"{plan_name}\n"
                text_result += f"⏰ Период: {period_name}\n"
                text_result += f"💰 Сумма: <b>{state['original_price']}₽</b> → <b>{discounted_price}₽</b>\n\n"
                text_result += "Нажмите кнопку ниже для перехода к оплате:"
                
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                markup = InlineKeyboardMarkup(row_width=1)
                
                # Если есть payment_id, создаем новый платеж с учетом скидки
                if payment_id and len(payment_id) > 8:
                    # Создаем новый платеж YooKassa с учетом скидки
                    from moviebot.bot.callbacks.payment_callbacks import calculate_discounted_price
                    from yookassa import Configuration, Payment
                    from moviebot.config import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
                    import os
                    import uuid as uuid_module
                    
                    Configuration.account_id = YOOKASSA_SHOP_ID.strip()
                    Configuration.secret_key = YOOKASSA_SECRET_KEY.strip()
                    
                    new_payment_id = str(uuid_module.uuid4())
                    return_url = os.getenv('YOOKASSA_RETURN_URL', 'tg://resolve?domain=movie_planner_bot')
                    
                    description = f"{subscription_type_name}: {plan_name}, период: {period_name}"
                    
                    metadata = {
                        "user_id": str(user_id),
                        "chat_id": str(chat_id),
                        "subscription_type": sub_type,
                        "plan_type": plan_type,
                        "period_type": period_type,
                        "payment_id": new_payment_id,
                        "promocode": promo_code
                    }
                    
                    if sub_type == 'group':
                        metadata["group_size"] = str(group_size) if group_size else ""
                    
                    try:
                        payment = Payment.create({
                            "amount": {
                                "value": f"{discounted_price:.2f}",
                                "currency": "RUB"
                            },
                            "confirmation": {
                                "type": "redirect",
                                "return_url": return_url
                            },
                            "capture": True,
                            "description": description,
                            "metadata": metadata
                        })
                        
                        from moviebot.database.db_operations import save_payment
                        save_payment(
                            payment_id=new_payment_id,
                            yookassa_payment_id=payment.id,
                            user_id=user_id,
                            chat_id=chat_id,
                            subscription_type=sub_type,
                            plan_type=plan_type,
                            period_type=period_type,
                            group_size=group_size,
                            amount=discounted_price,
                            status='pending'
                        )
                        
                        confirmation_url = payment.confirmation.confirmation_url
                        markup.add(InlineKeyboardButton("💳 Оплатить", url=confirmation_url))
                    except Exception as e:
                        logger.error(f"[PROMO] Ошибка создания платежа YooKassa: {e}", exc_info=True)
                
                callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id}"
                markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
                callback_data_promo = f"payment:promo:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id}:{discounted_price}"
                markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
                
                bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                
                # Удаляем состояние промокода
                del user_promo_state[user_id]
                return
            else:
                # Промокод недействителен
                error_text = f"❌ {message_text}\n\n"
                error_text += "Введите другой промокод или оплатите полную стоимость подписки."
                
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:back_from_promo:{state['sub_type']}:{state.get('group_size', '')}:{state['plan_type']}:{state['period_type']}:{state.get('payment_id', '')}:{state['original_price']}"))
                
                bot_instance.reply_to(message, error_text, reply_markup=markup)
                # Не удаляем состояние, чтобы пользователь мог попробовать другой промокод
                return
    
    # === user_promo_admin_state ===
    if user_id in user_promo_admin_state:
        state = user_promo_admin_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_promo_admin_state")
        logger.info(f"[MAIN TEXT HANDLER] user_promo_admin_state[{user_id}] = {state}")
        logger.info(f"[MAIN TEXT HANDLER] message.reply_to_message = {message.reply_to_message}")
        logger.info(f"[MAIN TEXT HANDLER] BOT_ID = {BOT_ID}")
        
        # Проверяем, что это ответ на сообщение бота
        if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
            logger.info(f"[MAIN TEXT HANDLER] Это ответ на сообщение бота, обрабатываем промокод")
            # Парсим ввод: код скидка количество
            parts = text.strip().split()
            if len(parts) != 3:
                bot_instance.reply_to(message, "❌ Неверный формат. Используйте: КОД СКИДКА КОЛИЧЕСТВО\n\nНапример: NEW2026 20% 100")
                return
            
            code = parts[0].strip()
            discount_input = parts[1].strip()
            total_uses_str = parts[2].strip()
            
            # Создаем промокод
            from moviebot.utils.promo import create_promocode
            success, result_message = create_promocode(code, discount_input, total_uses_str)
            
            if success:
                bot_instance.reply_to(message, f"✅ {result_message}")
            else:
                bot_instance.reply_to(message, f"❌ {result_message}")
            
            # Удаляем состояние
            del user_promo_admin_state[user_id]
            return
    
    # === user_cancel_subscription_state ===
    if user_id in user_cancel_subscription_state:
        state = user_cancel_subscription_state.get(user_id)
        if state:
            state_chat_id = state.get('chat_id')
            if state_chat_id and message.chat.id != state_chat_id:
                return
            
            is_reply = (message.reply_to_message and 
                       message.reply_to_message.from_user and 
                       message.reply_to_message.from_user.id == BOT_ID)
            
            if text.upper().strip() == 'ДА, ОТМЕНИТЬ':
                from moviebot.database.db_operations import cancel_subscription
                subscription_id = state.get('subscription_id')
                subscription_type = state.get('subscription_type')
                
                if subscription_id:
                    if cancel_subscription(subscription_id, user_id):
                        if subscription_type == 'group':
                            bot_instance.reply_to(message, "✅ <b>Групповая подписка отменена</b>\n\nВаша групповая подписка была успешно отменена.", parse_mode='HTML')
                        else:
                            bot_instance.reply_to(message, "✅ <b>Личная подписка отменена</b>\n\nВаша личная подписка была успешно отменена.", parse_mode='HTML')
                        del user_cancel_subscription_state[user_id]
                    else:
                        bot_instance.reply_to(message, "❌ Ошибка отмены подписки. Попробуйте позже.", parse_mode='HTML')
                        del user_cancel_subscription_state[user_id]
                return
    
    # === user_refund_state ===
    if user_id in user_refund_state:
        state = user_refund_state.get(user_id)
        if state:
            state_chat_id = state.get('chat_id')
            if state_chat_id and message.chat.id != state_chat_id:
                return
            
            # Обрабатываем ввод charge_id
            charge_id = text.strip()
            if charge_id:
                logger.info(f"[REFUND] Получен charge_id от пользователя {user_id}: {charge_id}")
                # Удаляем состояние
                del user_refund_state[user_id]
                # Обрабатываем возврат
                from moviebot.bot.handlers.stats import _process_refund
                _process_refund(message, charge_id)
                return
    
    # === user_unsubscribe_state ===
    if user_id in user_unsubscribe_state:
        state = user_unsubscribe_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_unsubscribe_state")
        
        # Проверяем, что это ответ на сообщение бота
        if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
            target_id_str = text.strip()
            
            try:
                target_id = int(target_id_str)
                is_group = target_id < 0  # Отрицательные ID обычно группы
                
                # Отменяем подписку
                from moviebot.bot.handlers.admin import cancel_subscription_by_id
                success, result_message, count = cancel_subscription_by_id(target_id, is_group)
                
                if success:
                    text_result = f"✅ {result_message}\n\n"
                    text_result += f"ID: <code>{target_id}</code>\n"
                    text_result += f"Тип: {'Группа' if is_group else 'Пользователь'}"
                    
                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
                    
                    bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                else:
                    bot_instance.reply_to(message, f"❌ {result_message}")
                
                # Удаляем состояние
                del user_unsubscribe_state[user_id]
                return
            except ValueError:
                bot_instance.reply_to(message, "❌ Неверный формат ID. Введите число.")
                return
    
    # === user_add_admin_state ===
    if user_id in user_add_admin_state:
        state = user_add_admin_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_add_admin_state")
        
        # Проверяем, что это ответ на сообщение бота
        if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
            admin_id_str = text.strip()
            
            try:
                admin_id = int(admin_id_str)
                
                # Добавляем администратора
                from moviebot.utils.admin import add_admin
                success, result_message = add_admin(admin_id, user_id)
                
                if success:
                    # Отправляем уведомление новому администратору
                    admin_text = "👑 <b>Вам выдан админский доступ</b>\n\n"
                    admin_text += "Доступные команды:\n\n"
                    admin_text += "<b>/unsubscribe</b> - Отменить подписку пользователя или группы\n"
                    admin_text += "   Введите ID пользователя или группы в ответном сообщении\n\n"
                    admin_text += "<b>/admin_stats</b> - Статистика бота\n"
                    admin_text += "   Показывает статистику пользователей, групп, подписок и т.д.\n\n"
                    admin_text += "<b>/refund_stars</b> - Возврат звезд\n"
                    admin_text += "   Введите charge_id платежа в ответном сообщении для возврата\n\n"
                    admin_text += "Все команды доступны только в личных сообщениях боту."
                    
                    try:
                        bot_instance.send_message(admin_id, admin_text, parse_mode='HTML')
                        logger.info(f"[ADD_ADMIN] Уведомление отправлено новому администратору: {admin_id}")
                    except Exception as e:
                        logger.warning(f"[ADD_ADMIN] Не удалось отправить уведомление администратору {admin_id}: {e}")
                    
                    text_result = f"✅ {result_message}\n\n"
                    text_result += f"ID администратора: <code>{admin_id}</code>\n\n"
                    text_result += "Уведомление отправлено новому администратору."
                    
                    from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back_to_list"))
                    
                    bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                else:
                    bot_instance.reply_to(message, f"❌ {result_message}")
                
                # Удаляем состояние
                del user_add_admin_state[user_id]
                return
            except ValueError:
                bot_instance.reply_to(message, "❌ Неверный формат ID. Введите число.")
                return
    
    # 2. Обработка реплаев
    
    # Реплай на сообщение бота с оценками
    if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
        reply_text = message.reply_to_message.text or ""
        
        if "Список просмотренных фильмов для оценки" in reply_text:
            from moviebot.bot.handlers.rate import handle_rate_list_reply_internal
            handle_rate_list_reply_internal(message)
            return
        
        reply_msg_id = message.reply_to_message.message_id
        if reply_msg_id in bot_messages:
            link = bot_messages.get(reply_msg_id)
            if link:
                from moviebot.bot.handlers.series import handle_random_plan_reply_internal
                handle_random_plan_reply_internal(message, link)
                return
    
    # Реплай на сообщение с ошибкой планирования
    if message.reply_to_message and message.reply_to_message.message_id in plan_error_messages:
        from moviebot.bot.handlers.plan import handle_plan_error_reply_internal
        handle_plan_error_reply_internal(message)
        return
    
    # Реплай на сообщение с оценкой
    if message.reply_to_message and message.text:
        text_stripped = message.text.strip()
        logger.info(f"[MAIN TEXT HANDLER] Проверка ответного сообщения с оценкой: text_stripped='{text_stripped}', reply_to_message_id={message.reply_to_message.message_id}")
        if (len(text_stripped) == 1 and text_stripped.isdigit() and 1 <= int(text_stripped) <= 9) or \
           (len(text_stripped) == 2 and text_stripped == "10"):
            rating = int(text_stripped)
            logger.info(f"[MAIN TEXT HANDLER] ✅ Обнаружена оценка: {rating}, вызов handle_rating_internal")
            try:
                from moviebot.bot.handlers.rate import handle_rating_internal
                handle_rating_internal(message, rating)
                logger.info(f"[MAIN TEXT HANDLER] handle_rating_internal завершен")
            except Exception as rating_e:
                logger.error(f"[MAIN TEXT HANDLER] ❌ Ошибка в handle_rating_internal: {rating_e}", exc_info=True)
            return
    
    # Реплай на голосование "в кино"
    if message.reply_to_message and text.lower() in ['да', 'нет']:
        from moviebot.bot.handlers.plan import handle_cinema_vote_internal
        handle_cinema_vote_internal(message, text.lower())
        return
    
    # Реплай на список фильмов
    if message.reply_to_message and message.reply_to_message.message_id in list_messages:
        from moviebot.bot.handlers.list import handle_list_reply_internal
        handle_list_reply_internal(message)
        return
    
    # 3. Обычные сообщения с фильмами (если нет состояния)
    # Обработка ссылок на Кинопоиск теперь выполняется отдельным обработчиком save_movie_message
    
    # Если ничего не подошло - игнорируем
    logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано: '{text[:50]}'")


@bot_instance.message_handler(content_types=['photo', 'document'])
def main_file_handler(message):
    """Единый хэндлер для всех фото и документов"""
    logger.info(f"[MAIN FILE HANDLER] Получено фото/документ от {message.from_user.id}")
    
    user_id = message.from_user.id
    
    # Обработка билетов
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        
        # Обработка билета на мероприятие
        if state.get('type') == 'event' and step == 'event_file':
            try:
                chat_id = state.get('chat_id')
                event_name = state.get('event_name')
                event_datetime_utc = state.get('event_datetime_utc')
                
                if not event_name or not event_datetime_utc:
                    bot_instance.reply_to(message, "❌ Ошибка: не найдены данные мероприятия.")
                    if user_id in user_ticket_state:
                        del user_ticket_state[user_id]
                    return
                
                # Получаем file_id
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                
                # Сохраняем билет на мероприятие в БД (film_id = NULL)
                with db_lock:
                    cursor.execute('''
                        INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id, ticket_file_id)
                        VALUES (%s, NULL, 'cinema', %s, %s, %s)
                    ''', (chat_id, event_datetime_utc, user_id, file_id))
                    conn.commit()
                
                logger.info(f"[EVENT TICKET] Билет на мероприятие сохранен: event_name={event_name}, chat_id={chat_id}, user_id={user_id}")
                
                bot_instance.reply_to(message, f"✅ Билет на мероприятие <b>{event_name}</b> сохранён! 🎟️", parse_mode='HTML')
                
                # Очищаем состояние
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            except Exception as e:
                logger.error(f"[EVENT TICKET] Ошибка при сохранении билета на мероприятие: {e}", exc_info=True)
                bot_instance.reply_to(message, "❌ Произошла ошибка при сохранении билета.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
        
        if step == 'upload_ticket':
            # Обработка загрузки билетов для фильма
            plan_id = state.get('plan_id')
            if not plan_id:
                bot_instance.reply_to(message, "❌ Ошибка: план не найден.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            
            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            
            with db_lock:
                cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (file_id, plan_id))
                conn.commit()
            
            title = state.get('film_title', 'фильм')
            dt = state.get('plan_dt', '')
            
            bot_instance.reply_to(message, f"✅ Билет прикреплён!\n\n<b>{title}</b> — {dt}\n\nМожете отправить ещё билеты или написать 'готово'.", parse_mode='HTML')
            return
        
        if step == 'waiting_ticket_file':
            # Пользователь выбрал сеанс и загружает билет
            plan_id = state.get('plan_id')
            if plan_id:
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                # Сохраняем билет в БД
                with db_lock:
                    cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (file_id, plan_id))
                    conn.commit()
                logger.info(f"[TICKET FILE] Билет сохранен в БД для plan_id={plan_id}, file_id={file_id}")
                bot_instance.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿")
                # Очищаем состояние пользователя, завершаем цикл работы с билетами
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                logger.info(f"[TICKET FILE] Состояние пользователя {user_id} очищено после сохранения билета")
                return
        
        # Сохраняем file_id для последующей обработки
        file_id = message.photo[-1].file_id if message.photo else message.document.file_id
        state['file_id'] = file_id
        bot_instance.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿")
        # Очищаем состояние пользователя, завершаем цикл работы с билетами
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        logger.info(f"[TICKET FILE] Состояние пользователя {user_id} очищено после получения файла")
        return
    
    # Если не в состоянии - игнорируем
    logger.info(f"[MAIN FILE HANDLER] Фото/документ не обработан (пользователь не в user_ticket_state)")


def register_text_message_handlers(bot_instance):
    """Регистрирует обработчики текстовых сообщений"""
    # Обработчики уже зарегистрированы через декораторы при импорте модуля
    # Эта функция нужна только для явного вызова в commands.py
    # Проверяем, что bot_instance совпадает с глобальным bot_instance
    if bot_instance != bot_instance:
        logger.warning("⚠️ Переданный bot_instance не совпадает с глобальным bot_instance из bot_init!")
    logger.info("✅ Обработчики текстовых сообщений зарегистрированы (декораторы выполнены при импорте)")

