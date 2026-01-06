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


# ==================== ОТДЕЛЬНЫЕ HANDLERS ДЛЯ КОНКРЕТНЫХ СЦЕНАРИЕВ ====================

def check_list_mark_watched_reply(message):
    """Проверка для handler ответа на сообщение из /list с ID фильмов для отметки как просмотренные"""
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "В ответном сообщении пришлите ID фильмов, и они будут отмечены как просмотренные" not in reply_text:
        return False
    if not message.text or not message.text.strip():
        return False
    return True


@bot_instance.message_handler(func=check_list_mark_watched_reply)
def handle_list_mark_watched_reply(message):
    """Обработчик ответа на сообщение из /list с ID фильмов для отметки как просмотренные"""
    logger.info(f"[LIST MARK WATCHED REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip()
        
        # Извлекаем ID фильмов из текста
        import re
        kp_ids = re.findall(r'\b(\d{4,})\b', text)
        
        if not kp_ids:
            bot_instance.reply_to(message, "❌ Не найдено ID фильмов в сообщении. Укажите ID фильмов (например: 1234567 7654321)")
            return
        
        # Отмечаем фильмы как просмотренные
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cursor = get_db_cursor()
        
        marked_count = 0
        errors = []
        marked_films = []  # Список отмеченных фильмов (kp_id, title)
        
        with db_lock:
            for kp_id in kp_ids:
                try:
                    # Сначала получаем название фильма
                    cursor.execute('SELECT title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    film_row = cursor.fetchone()
                    film_title = None
                    if film_row:
                        film_title = film_row.get('title') if isinstance(film_row, dict) else film_row[0]
                    
                    # Отмечаем фильм как просмотренный
                    cursor.execute('''
                        UPDATE movies 
                        SET watched = 1 
                        WHERE chat_id = %s AND kp_id = %s AND watched = 0
                    ''', (chat_id, kp_id))
                    if cursor.rowcount > 0:
                        marked_count += 1
                        marked_films.append((kp_id, film_title))
                except Exception as e:
                    errors.append(f"{kp_id}: {e}")
                    logger.error(f"[LIST MARK WATCHED] Ошибка при отметке фильма {kp_id}: {e}")
            
            conn.commit()
        
        # Формируем ответ с названиями фильмов
        if marked_count == 0:
            response_text = "❌ Не удалось отметить фильмы как просмотренные"
        else:
            response_text = f"✅ Отмечено как просмотренные: {marked_count} фильм(ов)\n\n"
            
            # Добавляем названия фильмов
            for kp_id, title in marked_films:
                if title:
                    response_text += f"• <b>{title}</b> [ID: {kp_id}]\n"
                else:
                    response_text += f"• [ID: {kp_id}]\n"
            
            if errors:
                response_text += f"\n⚠️ Ошибки: {len(errors)}"
        
        # Создаем кнопки для перехода к описанию
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup()
        
        # Добавляем кнопки для каждого отмеченного фильма (максимум 5, чтобы не перегружать)
        for kp_id, title in marked_films[:5]:
            button_text = f"📖 {title[:30]}..." if title and len(title) > 30 else (f"📖 {title}" if title else f"📖 ID: {kp_id}")
            markup.add(InlineKeyboardButton(button_text, callback_data=f"view_film_description:{kp_id}"))
        
        # Если фильмов больше 5, добавляем кнопку "Показать все"
        if len(marked_films) > 5:
            markup.add(InlineKeyboardButton("📋 Показать все отмеченные фильмы", callback_data="list:watched"))
        
        bot_instance.reply_to(message, response_text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[LIST MARK WATCHED REPLY] ✅ Завершено: отмечено {marked_count} фильмов")
    except Exception as e:
        logger.error(f"[LIST MARK WATCHED REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


def check_list_plan_reply(message):
    """Проверка для handler ответа на промпт планирования из /list"""
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!" not in reply_text:
        return False
    if not message.text or not message.text.strip():
        return False
    return True


@bot_instance.message_handler(func=check_list_plan_reply)
def handle_list_plan_reply(message):
    """Обработчик ответа на промпт планирования из /list (step=1) - ссылка/ID"""
    user_id = message.from_user.id
    text = message.text or ""
    logger.info(f"[LIST PLAN REPLY] ===== START: message_id={message.message_id}, user_id={user_id}, text='{text}'")
    try:
        from moviebot.bot.handlers.plan import get_plan_link_internal
        from moviebot.states import user_plan_state
        
        if user_id not in user_plan_state:
            user_plan_state[user_id] = {'step': 1, 'chat_id': message.chat.id}
        
        state = user_plan_state[user_id]
        state['prompt_message_id'] = message.reply_to_message.message_id
        
        logger.info(f"[LIST PLAN REPLY] Текст ответного сообщения: '{text}'")
        get_plan_link_internal(message, state)
        logger.info(f"[LIST PLAN REPLY] ✅ Завершено")
    except Exception as e:
        logger.error(f"[LIST PLAN REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


def check_plan_datetime_reply(message):
    """Проверка для handler ответа на промпт даты/времени планирования (step=3)"""
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "Когда планируете смотреть" not in reply_text:
        return False
    if not message.text or not message.text.strip():
        return False
    
    # Проверяем, что пользователь в состоянии планирования с step=3
    from moviebot.states import user_plan_state
    user_id = message.from_user.id
    if user_id not in user_plan_state:
        return False
    state = user_plan_state[user_id]
    if state.get('step') != 3:
        return False
    
    # Проверяем, что это ответ на правильный промпт
    prompt_message_id = state.get('prompt_message_id')
    if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
        return False
    
    return True


@bot_instance.message_handler(func=check_plan_datetime_reply)
def handle_plan_datetime_reply(message):
    """Обработчик ответа на промпт даты/времени планирования (step=3)"""
    user_id = message.from_user.id
    text = message.text or ""
    logger.info(f"[PLAN DATETIME REPLY] ===== START: message_id={message.message_id}, user_id={user_id}, text='{text}'")
    try:
        from moviebot.bot.handlers.plan import get_plan_day_or_date_internal
        from moviebot.states import user_plan_state
        
        state = user_plan_state[user_id]
        logger.info(f"[PLAN DATETIME REPLY] Текст ответного сообщения: '{text}'")
        get_plan_day_or_date_internal(message, state)
        logger.info(f"[PLAN DATETIME REPLY] ✅ Завершено")
    except Exception as e:
        logger.error(f"[PLAN DATETIME REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке даты/времени")
        except:
            pass


def check_plan_link_reply(message):
    """Проверка для handler ответа на промпт ссылки/ID планирования (step=1)"""
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "Пришлите ссылку или ID фильма" not in reply_text:
        return False
    if not message.text or not message.text.strip():
        return False
    
    # Проверяем, что пользователь в состоянии планирования с step=1
    from moviebot.states import user_plan_state
    user_id = message.from_user.id
    if user_id not in user_plan_state:
        return False
    state = user_plan_state[user_id]
    if state.get('step') != 1:
        return False
    
    # Проверяем, что это ответ на правильный промпт
    prompt_message_id = state.get('prompt_message_id')
    if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
        return False
    
    return True


@bot_instance.message_handler(func=check_plan_link_reply)
def handle_plan_link_reply(message):
    """Обработчик ответа на промпт ссылки/ID планирования (step=1) - только извлекает ID, не показывает описание"""
    user_id = message.from_user.id
    text = message.text or ""
    logger.info(f"[PLAN LINK REPLY] ===== START: message_id={message.message_id}, user_id={user_id}, text='{text}'")
    try:
        from moviebot.bot.handlers.plan import get_plan_link_internal
        from moviebot.states import user_plan_state
        
        state = user_plan_state[user_id]
        logger.info(f"[PLAN LINK REPLY] Текст ответного сообщения: '{text}'")
        get_plan_link_internal(message, state)
        logger.info(f"[PLAN LINK REPLY] ✅ Завершено")
    except Exception as e:
        logger.error(f"[PLAN LINK REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке ссылки/ID")
        except:
            pass


def check_clean_imported_ratings_reply(message):
    """Проверка для handler ответа на сообщение об удалении импортированных оценок"""
    # Проверяем, что это личный чат или реплай на сообщение бота
    is_private = message.chat.type == 'private'
    
    if is_private:
        # В личном чате проверяем состояние ожидания
        from moviebot.states import user_private_handler_state
        user_id = message.from_user.id
        if user_id in user_private_handler_state:
            state = user_private_handler_state[user_id]
            if state.get('handler') == 'clean_imported_ratings':
                text = message.text.strip().upper() if message.text else ""
                if text == "ДА, УДАЛИТЬ":
                    return True
    
    # Для групп проверяем реплай
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "Удаление импортированных оценок с Кинопоиска" not in reply_text:
        return False
    if not message.text or message.text.strip().upper() != "ДА, УДАЛИТЬ":
        return False
    return True


@bot_instance.message_handler(func=check_clean_imported_ratings_reply)
def handle_clean_imported_ratings_reply(message):
    """Обработчик ответа на сообщение об удалении импортированных оценок - ТОЛЬКО для 'ДА, УДАЛИТЬ'"""
    logger.info(f"[CLEAN IMPORTED RATINGS REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip().upper() if message.text else ""
        
        # Нормализуем текст: убираем пробелы, запятые, приводим к верхнему регистру
        normalized_text = text.replace(' ', '').replace(',', '').upper()
        # Проверяем различные варианты написания "ДА, УДАЛИТЬ"
        if normalized_text != 'ДАУДАЛИТЬ':
            logger.warning(f"[CLEAN IMPORTED RATINGS REPLY] Неверный текст подтверждения: '{text}' (нормализовано: '{normalized_text}')")
            return
        
        # Проверяем, что пользователь в состоянии user_clean_state с target='imported_ratings'
        from moviebot.states import user_clean_state, user_private_handler_state
        if user_id not in user_clean_state:
            logger.warning(f"[CLEAN IMPORTED RATINGS REPLY] Пользователь {user_id} не в состоянии user_clean_state")
            # Очищаем состояние для личных чатов, если оно есть
            if user_id in user_private_handler_state:
                del user_private_handler_state[user_id]
            return
        
        state = user_clean_state[user_id]
        if state.get('target') != 'imported_ratings':
            logger.warning(f"[CLEAN IMPORTED RATINGS REPLY] Неверный target в состоянии: {state.get('target')}")
            # Очищаем состояние для личных чатов, если оно есть
            if user_id in user_private_handler_state:
                del user_private_handler_state[user_id]
            return
        
        # Очищаем состояние для личных чатов после обработки
        if user_id in user_private_handler_state:
            del user_private_handler_state[user_id]
        
        # Вызываем обработчик удаления импортированных оценок
        from moviebot.bot.handlers.series import handle_clean_confirm_internal
        handle_clean_confirm_internal(message)
        logger.info(f"[CLEAN IMPORTED RATINGS REPLY] ✅ Завершено")
    except Exception as e:
        logger.error(f"[CLEAN IMPORTED RATINGS REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


def check_import_user_id_reply(message):
    """Проверка для handler ответа на сообщение об импорте базы из Кинопоиска с ID пользователя"""
    # Проверяем, что это ответ на сообщение бота
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    
    reply_text = message.reply_to_message.text or ""
    # Проверяем, что это сообщение об импорте
    if "Импорт базы из Кинопоиска" not in reply_text:
        return False
    if "Отправьте ID пользователя Кинопоиска или ссылку на профиль" not in reply_text:
        return False
    
    # Проверяем, что есть текст в сообщении
    if not message.text or not message.text.strip():
        return False
    
    # Проверяем, что пользователь в состоянии импорта
    from moviebot.states import user_import_state
    user_id = message.from_user.id
    if user_id not in user_import_state:
        return False
    
    state = user_import_state[user_id]
    if state.get('step') != 'waiting_user_id':
        return False
    
    return True


@bot_instance.message_handler(func=check_import_user_id_reply)
def handle_import_user_id_reply(message):
    """Обработчик ответа на сообщение об импорте базы из Кинопоиска с ID пользователя"""
    logger.info(f"[IMPORT USER ID REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        from moviebot.states import user_import_state
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        
        # Проверяем, что пользователь в состоянии импорта
        if user_id not in user_import_state:
            logger.warning(f"[IMPORT USER ID REPLY] Пользователь {user_id} не в состоянии user_import_state")
            return
        
        state = user_import_state[user_id]
        if state.get('step') != 'waiting_user_id':
            logger.warning(f"[IMPORT USER ID REPLY] Неверный step в состоянии: {state.get('step')}")
            return
        
        # Проверяем, что это ответ на правильное сообщение
        prompt_message_id = state.get('prompt_message_id')
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            logger.warning(f"[IMPORT USER ID REPLY] Ответ не на правильное сообщение: prompt_message_id={prompt_message_id}, reply_to_message_id={message.reply_to_message.message_id}")
            return
        
        # Обрабатываем ID пользователя
        try:
            from moviebot.bot.handlers.series import handle_import_user_id_internal
            handle_import_user_id_internal(message, state)
            logger.info(f"[IMPORT USER ID REPLY] ✅ Завершено")
        except Exception as e:
            logger.error(f"[IMPORT USER ID REPLY] Ошибка обработки: {e}", exc_info=True)
            bot_instance.reply_to(message, "❌ Не получилось обработать ID пользователя. Проверьте правильность ввода.")
    except Exception as e:
        logger.error(f"[IMPORT USER ID REPLY] ❌ Критическая ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


def check_list_view_film_reply(message):
    """Проверка для handler ответа на промпт просмотра описания из /list"""
    if not message.reply_to_message:
        return False
    if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
        return False
    reply_text = message.reply_to_message.text or ""
    if "Пришлите в ответном сообщении ссылку или ID фильма, чье описание хотите посмотреть" not in reply_text:
        return False
    if not message.text or not message.text.strip():
        return False
    return True


@bot_instance.message_handler(func=check_list_view_film_reply)
def handle_list_view_film_reply(message):
    """Обработчик ответа на промпт просмотра описания из /list"""
    logger.info(f"[LIST VIEW FILM REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        from moviebot.bot.handlers.list import handle_view_film_reply_internal
        from moviebot.states import user_view_film_state
        
        user_id = message.from_user.id
        if user_id not in user_view_film_state:
            user_view_film_state[user_id] = {'chat_id': message.chat.id}
        
        state = user_view_film_state[user_id]
        state['prompt_message_id'] = message.reply_to_message.message_id
        
        handle_view_film_reply_internal(message, state)
        logger.info(f"[LIST VIEW FILM REPLY] ✅ Завершено")
    except Exception as e:
        logger.error(f"[LIST VIEW FILM REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


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


@bot_instance.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text and "Введите промокод в ответном сообщении" in (m.reply_to_message.text or ""))
def handle_promo_reply_direct(message):
    """ОТДЕЛЬНЫЙ handler для реплаев на сообщение промокода - ВЫСОКИЙ ПРИОРИТЕТ"""
    logger.info(f"[PROMO REPLY DIRECT] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        promo_code = message.text.strip().upper() if message.text else ""
        
        if not promo_code:
            logger.warning(f"[PROMO REPLY DIRECT] Пустой промокод от пользователя {user_id}")
            bot_instance.reply_to(message, "❌ Промокод не может быть пустым. Введите промокод.")
            return
        
        logger.info(f"[PROMO REPLY DIRECT] Обрабатываем промокод: '{promo_code}' от пользователя {user_id}")
        
        # Получаем состояние промокода
        from moviebot.states import user_promo_state
        if user_id not in user_promo_state:
            logger.warning(f"[PROMO REPLY DIRECT] Пользователь {user_id} не в состоянии промокода")
            bot_instance.reply_to(message, "❌ Сессия истекла. Начните заново с /payment")
            return
        
        state = user_promo_state[user_id]
        logger.info(f"[PROMO REPLY DIRECT] Состояние: {state}")
        
        # Проверяем, не был ли уже применен промокод в текущей сессии платежа
        from moviebot.states import user_payment_state
        if user_id in user_payment_state:
            payment_state = user_payment_state[user_id]
            applied_promo = payment_state.get('promocode')
            applied_promo_id = payment_state.get('promocode_id')
            
            if applied_promo or applied_promo_id:
                logger.warning(f"[PROMO REPLY DIRECT] Промокод уже применен в текущей сессии платежа: promocode={applied_promo}, promocode_id={applied_promo_id}")
                error_text = f"❌ Промокод уже применен к этому платежу.\n\n"
                error_text += "Вы не можете применить промокод повторно."
                
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                
                bot_instance.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
                return
        
        # Применяем промокод к оригинальной цене (не к уже дисконтированной)
        original_price = state.get('original_price')
        if not original_price:
            # Если original_price не сохранен, берем из payment_state
            if user_id in user_payment_state:
                payment_state = user_payment_state[user_id]
                original_price = payment_state.get('original_price', state.get('original_price', 0))
            else:
                original_price = state.get('original_price', 0)
        
        from moviebot.utils.promo import apply_promocode
        success, discounted_price, message_text, promocode_id = apply_promocode(
            promo_code,
            original_price,
            user_id,
            chat_id
        )
        
        # Проверяем, что итоговая сумма не меньше 0
        if discounted_price < 0:
            discounted_price = 0
            logger.warning(f"[PROMO REPLY DIRECT] Итоговая сумма после применения промокода меньше 0, установлена в 0")
        
        logger.info(f"[PROMO REPLY DIRECT] Результат применения промокода: success={success}, discounted_price={discounted_price}, message='{message_text}'")
        
        if success:
            # Промокод применен успешно - используем существующую логику из main_text_handler
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
                
                if 'payment_data' in payment_state:
                    payment_state['payment_data']['amount'] = discounted_price
                    logger.info(f"[PROMO REPLY DIRECT] Обновлен payment_data.amount на {discounted_price}")
            
            # Формируем сообщение с обновленной ценой (копируем логику из main_text_handler)
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
            
            # Создаем платеж YooKassa с учетом скидки (копируем логику из main_text_handler)
            from moviebot.config import YOOKASSA_SHOP_ID, YOOKASSA_SECRET_KEY
            import os
            import uuid as uuid_module
            
            if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY:
                from yookassa import Configuration, Payment
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
                if group_size:
                    metadata["group_size"] = str(group_size)
                
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
                    markup.add(InlineKeyboardButton("💳 Оплатить картой/ЮMoney", url=confirmation_url))
                    logger.info(f"[PROMO REPLY DIRECT] Платеж YooKassa создан: payment_id={new_payment_id}, amount={discounted_price}")
                    payment_id = new_payment_id
                except Exception as e:
                    logger.error(f"[PROMO REPLY DIRECT] Ошибка создания платежа YooKassa: {e}", exc_info=True)
            
            # Обновляем состояние платежа
            if user_id in user_payment_state:
                payment_state = user_payment_state[user_id]
                payment_state['payment_id'] = payment_id
                payment_state['price'] = discounted_price
                payment_state['promocode_id'] = promocode_id
                payment_state['promocode'] = promo_code
                payment_state['original_price'] = state['original_price']
                
                if 'payment_data' in payment_state:
                    payment_state['payment_data']['payment_id'] = payment_id
                    payment_state['payment_data']['amount'] = discounted_price
                else:
                    payment_state['payment_data'] = {
                        'payment_id': payment_id,
                        'amount': discounted_price,
                        'sub_type': sub_type,
                        'plan_type': plan_type,
                        'period_type': period_type,
                        'group_size': group_size,
                        'chat_id': chat_id
                    }
            
            # Добавляем кнопки оплаты
            payment_id_short = payment_id[:8] if len(payment_id) > 8 else payment_id
            callback_data_stars = f"payment:pay_stars:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}"
            markup.add(InlineKeyboardButton(f"⭐ Оплатить звездами Telegram ({stars_amount}⭐)", callback_data=callback_data_stars))
            callback_data_promo = f"payment:promo:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}:{payment_id_short}:{discounted_price}"
            markup.add(InlineKeyboardButton("🏷️ Промокод", callback_data=callback_data_promo))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"payment:subscribe:{sub_type}:{group_size if group_size else ''}:{plan_type}:{period_type}" if group_size else f"payment:subscribe:{sub_type}:{plan_type}:{period_type}"))
            
            logger.info(f"[PROMO REPLY DIRECT] Отправка сообщения с результатом применения промокода")
            try:
                sent_msg = bot_instance.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[PROMO REPLY DIRECT] ✅ Сообщение отправлено успешно: message_id={sent_msg.message_id if sent_msg else 'None'}")
            except Exception as send_e:
                logger.error(f"[PROMO REPLY DIRECT] ❌ Ошибка отправки сообщения: {send_e}", exc_info=True)
                try:
                    sent_msg = bot_instance.send_message(chat_id, text_result, reply_markup=markup, parse_mode='HTML')
                    logger.info(f"[PROMO REPLY DIRECT] ✅ Сообщение отправлено через send_message: message_id={sent_msg.message_id if sent_msg else 'None'}")
                except Exception as send2_e:
                    logger.error(f"[PROMO REPLY DIRECT] ❌ Ошибка отправки через send_message: {send2_e}", exc_info=True)
            
            # Удаляем состояние промокода
            del user_promo_state[user_id]
            return
        else:
            # Промокод недействителен
            error_text = f"❌ {message_text}\n\n"
            error_text += "Введите другой промокод или оплатите полную стоимость подписки."
            
            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
            
            bot_instance.reply_to(message, error_text, reply_markup=markup)
            # Не удаляем состояние, чтобы пользователь мог попробовать другой промокод
            return
    except Exception as e:
        logger.error(f"[PROMO REPLY DIRECT] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)


@bot_instance.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text and "🔍 Укажите запрос для поиска" in (m.reply_to_message.text or ""))
def handle_search_reply_direct(message):
    """ОТДЕЛЬНЫЙ handler для реплаев на сообщение поиска - ВЫСОКИЙ ПРИОРИТЕТ"""
    logger.info(f"[SEARCH REPLY DIRECT] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        query = message.text.strip() if message.text else ""
        
        if not query:
            logger.warning(f"[SEARCH REPLY DIRECT] Пустой запрос от пользователя {user_id}")
            return
        
        logger.info(f"[SEARCH REPLY DIRECT] Обрабатываем поисковый запрос: '{query}' от пользователя {user_id}")
        
        # Получаем тип поиска из состояния или используем 'mixed'
        from moviebot.states import user_search_state
        search_type = 'mixed'
        if user_id in user_search_state:
            search_type = user_search_state[user_id].get('search_type', 'mixed')
        else:
            # Если состояния нет, создаем его
            user_search_state[user_id] = {
                'chat_id': chat_id,
                'message_id': message.reply_to_message.message_id if message.reply_to_message else None,
                'search_type': 'mixed'
            }
        
        # Выполняем поиск
        from moviebot.bot.handlers.series import search_films_with_type
        try:
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY DIRECT] ✅ Поиск завершен: найдено {len(films) if films else 0} результатов, страниц: {total_pages}")
        except Exception as search_e:
            logger.error(f"[SEARCH REPLY DIRECT] ❌ Ошибка при выполнении поиска: {search_e}", exc_info=True)
            bot_instance.reply_to(message, f"❌ Ошибка при выполнении поиска. Попробуйте еще раз.")
            return
        
        if not films:
            logger.warning(f"[SEARCH REPLY DIRECT] Ничего не найдено по запросу '{query}'")
            bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup(row_width=1)
        
        for idx, film in enumerate(films[:10]):
            try:
                title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                year = film.get('year') or film.get('releaseYear') or 'N/A'
                rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                
                film_type = film.get('type', '').upper() if film.get('type') else 'FILM'
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
            except Exception as film_e:
                logger.error(f"[SEARCH REPLY DIRECT] Ошибка обработки фильма {idx+1}: {film_e}", exc_info=True)
                continue
        
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        results_text += "\n\n🎬 - фильм\n📺 - сериал"
        
        if len(results_text) > 4096:
            results_text = results_text[:4000] + "\n\n... (показаны не все результаты)"
        
        try:
            sent_message = bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[SEARCH REPLY DIRECT] ✅ Результаты поиска отправлены: message_id={sent_message.message_id if sent_message else 'None'}")
            # Удаляем состояние после успешной отправки
            if user_id in user_search_state:
                del user_search_state[user_id]
        except Exception as send_e:
            logger.error(f"[SEARCH REPLY DIRECT] ❌ Ошибка отправки результатов: {send_e}", exc_info=True)
    except Exception as e:
        logger.error(f"[SEARCH REPLY DIRECT] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)


def check_admin_commands_reply(message):
    """Проверка для обработчика админских команд (refund_stars, unsubscribe, add_admin)"""
    if not message.text or message.text.startswith('/'):
        return False
    
    if not message.reply_to_message or message.reply_to_message.from_user.id != BOT_ID:
        return False
    
    user_id = message.from_user.id
    from moviebot.states import user_refund_state, user_unsubscribe_state, user_add_admin_state
    
    # Проверяем, что пользователь в одном из админских состояний
    if user_id not in user_refund_state and user_id not in user_unsubscribe_state and user_id not in user_add_admin_state:
        return False
    
    # Проверяем, что сообщение является реплаем на prompt_message_id
    if user_id in user_refund_state:
        state = user_refund_state.get(user_id)
        if state:
            prompt_message_id = state.get('prompt_message_id')
            if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
                return True
    
    if user_id in user_unsubscribe_state:
        state = user_unsubscribe_state[user_id]
        prompt_message_id = state.get('prompt_message_id')
        if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
            return True
    
    if user_id in user_add_admin_state:
        state = user_add_admin_state[user_id]
        prompt_message_id = state.get('prompt_message_id')
        if prompt_message_id and message.reply_to_message.message_id == prompt_message_id:
            return True
    
    return False


@bot_instance.message_handler(func=check_admin_commands_reply)
def handle_admin_commands_reply(message):
    """Обработчик реплаев для админских команд (refund_stars, unsubscribe, add_admin)"""
    logger.info(f"[ADMIN COMMANDS REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    
    # Импортируем обработчик из state_handlers, который уже содержит всю логику
    from moviebot.bot.handlers.state_handlers import handle_admin
    handle_admin(message)
    logger.info(f"[ADMIN COMMANDS REPLY] ===== END: обработано через state_handlers.handle_admin")


@bot_instance.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text)
def handle_rate_list_reply(message):
    """Обработчик реплаев на сообщения бота с оценками"""
    logger.info(f"[HANDLE RATE LIST REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    
    # Пропускаем команды
    if message.text and message.text.startswith('/'):
        logger.info(f"[HANDLE RATE LIST REPLY] Пропуск команды")
        return
    
    # Пропускаем сообщения для clean handler'а
    reply_text = message.reply_to_message.text or "" if message.reply_to_message else ""
    if "Удаление импортированных оценок с Кинопоиска" in reply_text:
        text = message.text.strip().upper() if message.text else ""
        if text == "ДА, УДАЛИТЬ":
            logger.info(f"[HANDLE RATE LIST REPLY] Пропуск сообщения - это сообщение для clean handler'а")
            return
    
    user_id = message.from_user.id
    
    # ВАЖНО: Пропускаем сообщения, если пользователь в любом состоянии
    # Эти сообщения должны обрабатываться через main_text_handler
    from moviebot.states import (
        user_plan_state, user_promo_state, user_promo_admin_state,
        user_ticket_state, user_search_state, user_settings_state,
        user_edit_state, user_view_film_state, user_import_state,
        user_clean_state, user_cancel_subscription_state, user_refund_state,
        user_unsubscribe_state, user_add_admin_state
    )
    
    logger.info(f"[HANDLE RATE LIST REPLY] Проверка состояний: user_search_state={user_id in user_search_state}, user_plan_state={user_id in user_plan_state}, user_ticket_state={user_id in user_ticket_state}")
    
    # КРИТИЧЕСКИ ВАЖНО: Пропускаем user_promo_state и user_promo_admin_state - они обрабатываются в main_text_handler
    if user_id in user_promo_state or user_id in user_promo_admin_state:
        logger.info(f"[HANDLE RATE LIST REPLY] Пропуск сообщения - пользователь в состоянии промокода (promo={user_id in user_promo_state}, promo_admin={user_id in user_promo_admin_state}), передаем в main_text_handler")
        return
    
    # ВАЖНО: Проверяем user_search_state ПЕРВЫМ, так как поиск должен обрабатываться в main_text_handler
    if user_id in user_search_state:
        logger.info(f"[HANDLE RATE LIST REPLY] ✅ Пропуск сообщения - пользователь в состоянии поиска, передаем в main_text_handler")
        return
    
    # Проверяем остальные состояния - ВАЖНО: проверяем ДО обработки, чтобы не перехватывать сообщения из состояний
    # НЕ проверяем админские состояния (user_refund_state, user_unsubscribe_state, user_add_admin_state) - 
    # они обрабатываются через отдельный обработчик handle_admin_commands_reply
    if (user_id in user_plan_state or 
        user_id in user_ticket_state or
        user_id in user_settings_state or
        user_id in user_edit_state or
        user_id in user_view_film_state or
        user_id in user_import_state or
        user_id in user_clean_state or
        user_id in user_cancel_subscription_state):
        logger.info(f"[HANDLE RATE LIST REPLY] ✅ Пропуск сообщения - пользователь в состоянии (plan={user_id in user_plan_state}, ticket={user_id in user_ticket_state}), передаем в main_text_handler")
        return
    
    # Обрабатываем сообщения с оценками (числа от 1 до 10) - они обрабатываются через rating_messages
    text_stripped = message.text.strip() if message.text else ""
    if (len(text_stripped) == 1 and text_stripped.isdigit() and 1 <= int(text_stripped) <= 9) or \
       (len(text_stripped) == 2 and text_stripped == "10"):
        # Это оценка - обрабатываем через rating_messages
        rating = int(text_stripped)
        logger.info(f"[HANDLE RATE LIST REPLY] Обнаружена оценка: {rating}, обрабатываем")
        
        # Проверяем, есть ли реплай и находится ли сообщение в rating_messages
        if message.reply_to_message:
            reply_msg_id = message.reply_to_message.message_id
            from moviebot.states import rating_messages
            if reply_msg_id in rating_messages:
                logger.info(f"[HANDLE RATE LIST REPLY] ✅ Найдено сообщение в rating_messages: reply_msg_id={reply_msg_id}")
                try:
                    from moviebot.bot.handlers.rate import handle_rating_internal
                    handle_rating_internal(message, rating)
                    logger.info(f"[HANDLE RATE LIST REPLY] handle_rating_internal завершен")
                except Exception as rating_e:
                    logger.error(f"[HANDLE RATE LIST REPLY] ❌ Ошибка в handle_rating_internal: {rating_e}", exc_info=True)
                return
            else:
                # Если реплай есть, но не в rating_messages, все равно пробуем обработать
                logger.info(f"[HANDLE RATE LIST REPLY] Реплай есть, но не в rating_messages, пробуем обработать оценку")
                try:
                    from moviebot.bot.handlers.rate import handle_rating_internal
                    handle_rating_internal(message, rating)
                    logger.info(f"[HANDLE RATE LIST REPLY] handle_rating_internal завершен")
                except Exception as rating_e:
                    logger.error(f"[HANDLE RATE LIST REPLY] ❌ Ошибка в handle_rating_internal: {rating_e}", exc_info=True)
                return
        else:
            # Если нет реплая, но это число от 1 до 10, пробуем обработать как оценку
            logger.info(f"[HANDLE RATE LIST REPLY] Нет реплая, но это число от 1 до 10, пробуем обработать как оценку")
            try:
                from moviebot.bot.handlers.rate import handle_rating_internal
                handle_rating_internal(message, rating)
                logger.info(f"[HANDLE RATE LIST REPLY] handle_rating_internal завершен")
            except Exception as rating_e:
                logger.error(f"[HANDLE RATE LIST REPLY] ❌ Ошибка в handle_rating_internal: {rating_e}", exc_info=True)
            return
    
    chat_id = message.chat.id
    
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
    """
    Fallback handler для текстовых сообщений (исключая команды)
    Все состояния теперь обрабатываются отдельными handlers в state_handlers.py
    Этот handler обрабатывает только специальные случаи и реплаи, которые не попали в другие handlers
    """
    logger.info(f"[MAIN TEXT HANDLER] ===== START (FALLBACK): message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}, text='{message.text[:100] if message.text else ''}'")
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    # ЛОГИКА ДЛЯ ЛИЧНЫХ ЧАТОВ: проверяем, есть ли ожидающее сообщение для handler'а
    is_private = message.chat.type == 'private'
    if is_private:
        from moviebot.states import user_private_handler_state
        if user_id in user_private_handler_state:
            state = user_private_handler_state[user_id]
            handler_name = state.get('handler')
            prompt_message_id = state.get('prompt_message_id')
            
            logger.info(f"[MAIN TEXT HANDLER] Личный чат: найдено ожидающее состояние handler='{handler_name}', prompt_message_id={prompt_message_id}")
            
            # Обрабатываем через соответствующий handler
            if handler_name == 'clean_imported_ratings':
                # Создаем fake reply_to_message для совместимости
                class FakeReplyMessage:
                    def __init__(self, message_id):
                        self.message_id = message_id
                        self.from_user = type('User', (), {'id': BOT_ID})()
                        self.text = "⚠️ Удаление импортированных оценок с Кинопоиска"
                
                # Сохраняем оригинальный reply_to_message, если он есть
                original_reply = getattr(message, 'reply_to_message', None)
                message.reply_to_message = FakeReplyMessage(prompt_message_id)
                
                # Вызываем handler
                handle_clean_imported_ratings_reply(message)
                
                # Восстанавливаем оригинальный reply_to_message
                if original_reply:
                    message.reply_to_message = original_reply
                else:
                    message.reply_to_message = None
                
                return
            
            # Для других handlers можно добавить аналогичную логику
            # После обработки состояние очищается в самом handler'е
    
    # Пропускаем сообщения со ссылками на Кинопоиск без реплая - они обрабатываются отдельным handler
    if text and ('kinopoisk.ru' in text.lower() or 'kinopoisk.com' in text.lower()):
        # Проверяем, что это не реплай на промпт планирования или другие специальные случаи
        if not message.reply_to_message or not any(prompt in (message.reply_to_message.text or "") for prompt in [
            "Пришлите ссылку или ID фильма в ответном сообщении",
            "Пришлите в ответном сообщении ссылку или ID фильма",
            "В ответном сообщении пришлите ID фильмов"
        ]):
            logger.info(f"[MAIN TEXT HANDLER] Пропускаем сообщение со ссылкой на Кинопоиск (будет обработано handle_kinopoisk_link)")
            return
    
    # Проверяем, не обрабатывается ли это сообщение одним из специализированных handlers
    # Если пользователь в каком-то состоянии, пропускаем - пусть специализированный handler обработает
    from moviebot.states import (
        user_ticket_state, user_search_state, user_import_state,
        user_edit_state, user_settings_state, user_plan_state,
        user_clean_state, user_promo_state, user_promo_admin_state,
        user_cancel_subscription_state, user_view_film_state
        # НЕ включаем админские состояния (user_refund_state, user_unsubscribe_state, user_add_admin_state) - 
        # они обрабатываются через отдельный обработчик handle_admin_commands_reply
    )
    
    # Пропускаем ответные сообщения об импорте - у них есть отдельный handler
    if message.reply_to_message:
        reply_text = message.reply_to_message.text or ""
        if "Импорт базы из Кинопоиска" in reply_text and "Отправьте ID пользователя Кинопоиска" in reply_text:
            logger.info(f"[MAIN TEXT HANDLER] Пропускаем ответное сообщение об импорте (обработает handle_import_user_id_reply)")
            return
    
    # Если пользователь в любом из состояний, пропускаем - специализированные handlers обработают
    if (user_id in user_ticket_state or user_id in user_search_state or 
        user_id in user_import_state or user_id in user_edit_state or 
        user_id in user_settings_state or user_id in user_plan_state or
        user_id in user_clean_state or user_id in user_promo_state or 
        user_id in user_promo_admin_state or user_id in user_cancel_subscription_state or
        user_id in user_view_film_state):
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в состоянии, пропускаем (обработает специализированный handler)")
        return
    
    # Обработка реплаев и специальных случаев (fallback для необработанных сообщений)
    
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
    
    # Реплай на голосование "в кино"
    if message.reply_to_message and text.lower() in ['да', 'нет']:
        from moviebot.bot.handlers.plan import handle_cinema_vote_internal
        handle_cinema_vote_internal(message, text.lower())
        return
    
    # Если сообщение не обработано ни одним handler, просто логируем
    logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано ни одним специализированным handler: text='{text[:100]}', user_id={user_id}, chat_id={chat_id}")
    return
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
            
            # Получаем существующие билеты и добавляем новый
            import json
            with db_lock:
                cursor.execute("SELECT ticket_file_id FROM plans WHERE id = %s", (plan_id,))
                ticket_row = cursor.fetchone()
                existing_tickets = []
                if ticket_row:
                    ticket_data = ticket_row.get('ticket_file_id') if isinstance(ticket_row, dict) else ticket_row[0]
                    if ticket_data:
                        try:
                            existing_tickets = json.loads(ticket_data)
                            if not isinstance(existing_tickets, list):
                                # Если это старый формат (один file_id), конвертируем в массив
                                existing_tickets = [ticket_data]
                        except:
                            # Если не JSON, значит это старый формат (один file_id)
                            existing_tickets = [ticket_data]
                
                # Добавляем новый билет
                existing_tickets.append(file_id)
                tickets_json = json.dumps(existing_tickets, ensure_ascii=False)
                
                cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (tickets_json, plan_id))
                conn.commit()
            
            title = state.get('film_title', 'фильм')
            dt = state.get('plan_dt', '')
            
            bot_instance.reply_to(message, f"✅ Билет прикреплён! (Всего билетов: {len(existing_tickets)})\n\n<b>{title}</b> — {dt}\n\nМожете отправить ещё билеты или написать 'готово'.", parse_mode='HTML')
            return
        
        if step == 'waiting_ticket_file':
            # Пользователь выбрал сеанс и загружает билет
            plan_id = state.get('plan_id')
            if plan_id:
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                # Сохраняем билет в БД как массив
                import json
                with db_lock:
                    # Получаем существующие билеты
                    cursor.execute("SELECT ticket_file_id FROM plans WHERE id = %s", (plan_id,))
                    ticket_row = cursor.fetchone()
                    existing_tickets = []
                    if ticket_row:
                        ticket_data = ticket_row.get('ticket_file_id') if isinstance(ticket_row, dict) else ticket_row[0]
                        if ticket_data:
                            try:
                                existing_tickets = json.loads(ticket_data)
                                if not isinstance(existing_tickets, list):
                                    existing_tickets = [ticket_data]
                            except:
                                existing_tickets = [ticket_data]
                    
                    # Добавляем новый билет
                    existing_tickets.append(file_id)
                    tickets_json = json.dumps(existing_tickets, ensure_ascii=False)
                    
                    cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (tickets_json, plan_id))
                    conn.commit()
                logger.info(f"[TICKET FILE] Билет сохранен в БД для plan_id={plan_id}, file_id={file_id}, всего билетов: {len(existing_tickets)}")
                
                # Добавляем кнопки после сохранения билета
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("✏️ Изменить время", callback_data=f"ticket_edit_time:{plan_id}"))
                markup.add(InlineKeyboardButton("➕ Добавить еще билет к сеансу", callback_data=f"add_ticket:{plan_id}"))
                markup.add(InlineKeyboardButton("🎟️ Вернуться к билетам", callback_data="ticket_new"))
                
                bot_instance.reply_to(message, f"✅ Файл получен. (Всего билетов: {len(existing_tickets)}) Можете отправить ещё билеты или написать 'готово'. 🍿", reply_markup=markup)
                # НЕ очищаем состояние - пользователь может добавить ещё билеты
                logger.info(f"[TICKET FILE] Состояние пользователя {user_id} сохранено для добавления дополнительных билетов")
                return
        
        if step == 'add_more_tickets':
            # Обработка добавления дополнительных билетов
            plan_id = state.get('plan_id')
            if not plan_id:
                bot_instance.reply_to(message, "❌ Ошибка: план не найден.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            
            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            
            # Получаем существующие билеты и добавляем новый
            import json
            with db_lock:
                cursor.execute("SELECT ticket_file_id FROM plans WHERE id = %s", (plan_id,))
                ticket_row = cursor.fetchone()
                existing_tickets = []
                if ticket_row:
                    ticket_data = ticket_row.get('ticket_file_id') if isinstance(ticket_row, dict) else ticket_row[0]
                    if ticket_data:
                        try:
                            existing_tickets = json.loads(ticket_data)
                            if not isinstance(existing_tickets, list):
                                existing_tickets = [ticket_data]
                        except:
                            existing_tickets = [ticket_data]
                
                # Добавляем новый билет
                existing_tickets.append(file_id)
                tickets_json = json.dumps(existing_tickets, ensure_ascii=False)
                
                cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (tickets_json, plan_id))
                conn.commit()
            
            bot_instance.reply_to(message, f"✅ Билет добавлен! (Всего билетов: {len(existing_tickets)})\n\nМожете отправить ещё билеты или написать 'готово'.")
            return
        
        # Сохраняем file_id для последующей обработки
        file_id = message.photo[-1].file_id if message.photo else message.document.file_id
        state['file_id'] = file_id
        
        # Добавляем кнопки после получения файла
        from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎟️ Вернуться к билетам", callback_data="ticket_new"))
        
        bot_instance.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿", reply_markup=markup)
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

