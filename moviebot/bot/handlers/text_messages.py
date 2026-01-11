# ==================== text_messages.py ====================
"""
Единый главный обработчик для всех текстовых сообщений
Обрабатывает состояния, реплаи, ссылки на Кинопоиск и т.д.
"""
import logging
import re
from datetime import datetime, timedelta
from collections import defaultdict

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot, BOT_ID
from moviebot.database.db_connection import db_lock, get_db_connection, get_db_cursor
from moviebot.database.db_operations import (
    log_request,
    get_user_timezone_or_default,
    set_notification_setting,
    add_and_announce,
    is_bot_participant,
    get_watched_emojis,
    get_watched_custom_emoji_ids
)
from moviebot.api.kinopoisk_api import extract_movie_info, search_films
from moviebot.states import (
    user_search_state, user_plan_state, user_ticket_state,
    user_settings_state, user_edit_state, user_view_film_state,
    user_import_state, user_clean_state, user_cancel_subscription_state,
    user_refund_state, user_promo_state, user_promo_admin_state,
    user_unsubscribe_state, user_add_admin_state,
    bot_messages, plan_error_messages, list_messages, added_movie_messages,
    rating_messages, plan_notification_messages, settings_messages,
    user_expected_text
)
from moviebot.utils.parsing import parse_session_time, extract_kp_id_from_text
from moviebot.bot.handlers.list import handle_view_film_reply_internal

# Импорты обработчиков, которые регистрируют себя через декораторы
import moviebot.bot.handlers.promo      # noqa: F401
import moviebot.bot.handlers.admin      # noqa: F401

logger = logging.getLogger(__name__)
logger.info("=" * 80)
logger.info("[TEXT MESSAGES] Модуль text_messages.py импортирован - декораторы будут зарегистрированы")
logger.info("=" * 80)

# ─── Защита от дублей обработки одного и того же сообщения ────────
# Ключ: user_id → (message_id, timestamp)
last_processed_private = defaultdict(lambda: (0, datetime(2000, 1, 1)))
PROTECT_WINDOW = timedelta(seconds=15)   # 15 секунд — достаточно даже для очень медленного интернета
# ──────────────────────────────────────────────────────────────────────

# Остальной код файла...

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


@bot.message_handler(func=add_reactions_check)
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
        bot.reply_to(message, "❌ Не нашёл эмодзи в вашем сообщении. Попробуйте отправить эмодзи снова.")
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
        bot.reply_to(message, f"✅ Реакции {action_text}:\n{unique_emojis}")
        logger.info(f"[SETTINGS] Реакции обновлены для чата {chat_id}, user_id={user_id}: {unique_emojis}")
    except Exception as e:
        logger.error(f"[SETTINGS] Ошибка при сохранении реакций: {e}", exc_info=True)
        bot.reply_to(message, "❌ Произошла ошибка при сохранении реакций.")
    
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


@bot.message_handler(func=check_list_mark_watched_reply)
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
            bot.reply_to(message, "❌ Не найдено ID фильмов в сообщении. Укажите ID фильмов (например: 1234567 7654321)")
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
                    cursor.execute('SELECT title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                    film_row = cursor.fetchone()
                    film_title = None
                    if film_row:
                        film_title = film_row.get('title') if isinstance(film_row, dict) else film_row[0]
                    
                    # Отмечаем фильм как просмотренный
                    cursor.execute('''
                        UPDATE movies 
                        SET watched = 1 
                        WHERE chat_id = %s AND kp_id = %s AND watched = 0
                    ''', (chat_id, str(str(kp_id))))
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
        markup = InlineKeyboardMarkup()
        
        # Добавляем кнопки для каждого отмеченного фильма (максимум 5, чтобы не перегружать)
        for kp_id, title in marked_films[:5]:
            button_text = f"📖 {title[:30]}..." if title and len(title) > 30 else (f"📖 {title}" if title else f"📖 ID: {kp_id}")
            markup.add(InlineKeyboardButton(button_text, callback_data=f"show_film_description:{int(kp_id)}"))
        
        # Если фильмов больше 5, добавляем кнопку "Показать все"
        if len(marked_films) > 5:
            markup.add(InlineKeyboardButton("📋 Показать все отмеченные фильмы", callback_data="list:watched"))
        
        bot.reply_to(message, response_text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[LIST MARK WATCHED REPLY] ✅ Завершено: отмечено {marked_count} фильмов")
    except Exception as e:
        logger.error(f"[LIST MARK WATCHED REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
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


@bot.message_handler(func=check_list_plan_reply)
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
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


def check_plan_datetime_reply(message):
    """Проверка для handler ответа на промпт даты/времени планирования (step=3)"""
    # КРИТИЧЕСКИЙ ФИКС: В личке принимаем следующее сообщение, в группах - только реплай
    is_private = message.chat.type == 'private'
    
    # Проверяем, что пользователь в состоянии планирования с step=3
    from moviebot.states import user_plan_state
    user_id = message.from_user.id
    if user_id not in user_plan_state:
        return False
    state = user_plan_state[user_id]
    if state.get('step') != 3:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # В группах принимаем только реплаи на бота
    if not is_private:
        if not message.reply_to_message:
            return False
        if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
            return False
        # Проверяем, что это ответ на правильный промпт (либо на запрос даты, либо на сообщение об ошибке)
        prompt_message_id = state.get('prompt_message_id')
        reply_text = message.reply_to_message.text or ""
        is_prompt = "Когда планируете смотреть" in reply_text
        is_error = "Не понял дату/время" in reply_text
        if not (is_prompt or is_error):
            return False
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            return False
    else:
        # В личке: принимаем реплай на промпт/ошибку или следующее сообщение (если состояние активно)
        if message.reply_to_message:
            # Если это реплай, проверяем, что это ответ на правильный промпт
            if message.reply_to_message.from_user and message.reply_to_message.from_user.id == BOT_ID:
                reply_text = message.reply_to_message.text or ""
                is_prompt = "Когда планируете смотреть" in reply_text
                is_error = "Не понял дату/время" in reply_text
                if not (is_prompt or is_error):
                    return False
                prompt_message_id = state.get('prompt_message_id')
                if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                    return False
        # Если не реплай, но состояние активно - принимаем как следующее сообщение
    
    return True

@bot.message_handler(func=check_plan_datetime_reply)
def handle_plan_datetime_reply(message):
    """Обработчик ответа на промпт даты/времени (создание ИЛИ редактирование плана)"""
    user_id = message.from_user.id
    text = message.text or ""

    logger.info(f"[PLAN DATETIME REPLY] ===== START: message_id={message.message_id}, user_id={user_id}, text='{text}'")

    try:
        from moviebot.bot.handlers.plan import get_plan_day_or_date_internal
        from moviebot.states import user_plan_state, user_edit_state

        state = None
        is_edit = False

        # 1. Проверяем состояние редактирования (приоритет выше)
        if user_id in user_edit_state:
            state = user_edit_state[user_id]
            is_edit = True
            logger.info(f"[PLAN DATETIME REPLY] Режим РЕДАКТИРОВАНИЯ: {state}")

        # 2. Если нет редактирования — смотрим обычное планирование
        elif user_id in user_plan_state:
            state = user_plan_state[user_id]
            logger.info(f"[PLAN DATETIME REPLY] Обычное планирование: {state}")

        # Если вообще нет состояния — выходим тихо
        else:
            logger.info(f"[PLAN DATETIME REPLY] Нет активного состояния планирования/редактирования для user_id={user_id}")
            return

        # КРИТИЧЕСКИЙ ФИКС: В личке принимаем следующее сообщение, в группах - только реплай
        try:
            chat_info = bot.get_chat(message.chat.id)
            is_private = chat_info.type == 'private'
        except:
            is_private = message.chat.id > 0
        
        # Проверяем, что ответ именно на наш промпт (по message_id)
        prompt_message_id = state.get('prompt_message_id')
        
        if not is_private:
            # В группах требуется реплай на правильный промпт
            is_reply_to_prompt = (
                message.reply_to_message and 
                message.reply_to_message.message_id == prompt_message_id
            )
            if not is_reply_to_prompt:
                logger.info(f"[PLAN DATETIME REPLY] В группе не ответ на промпт (ожидаемый id={prompt_message_id}), игнорируем")
                return
        else:
            # В личке: если это реплай, проверяем что на правильный промпт; если не реплай - принимаем как следующее сообщение
            if message.reply_to_message:
                # Если это реплай, проверяем что на правильный промпт
                if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                    logger.info(f"[PLAN DATETIME REPLY] В личке реплай не на правильный промпт (ожидаемый id={prompt_message_id}), игнорируем")
                    return
            # Если не реплай, но состояние активно - принимаем как следующее сообщение
            logger.info(f"[PLAN DATETIME REPLY] В личке принято следующее сообщение без реплая")

        # Вызываем общую функцию обработки даты/времени
        # get_plan_day_or_date_internal сам управляет состоянием:
        # - очищает его при успешном планировании
        # - оставляет его при ошибке для повторного ввода
        result = get_plan_day_or_date_internal(message, state)

        logger.info(f"[PLAN DATETIME REPLY] ✅ Завершено")

    except Exception as e:
        logger.error(f"[PLAN DATETIME REPLY] ❌ Ошибка: {e}", exc_info=True)
        # Не спамим пользователю лишними сообщениями — только лог

def check_plan_link_reply(message):
    """Проверка для handler ответа на промпт ссылки/ID планирования (step=1)"""
    # КРИТИЧЕСКИЙ ФИКС: В личке принимаем следующее сообщение, в группах - только реплай
    is_private = message.chat.type == 'private'
    
    # Проверяем, что пользователь в состоянии планирования с step=1
    from moviebot.states import user_plan_state
    user_id = message.from_user.id
    if user_id not in user_plan_state:
        return False
    state = user_plan_state[user_id]
    if state.get('step') != 1:
        return False
    
    if not message.text or not message.text.strip():
        return False
    
    # В группах принимаем только реплаи на бота
    if not is_private:
        if not message.reply_to_message:
            return False
        if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
            return False
        reply_text = message.reply_to_message.text or ""
        if "Пришлите ссылку или ID фильма" not in reply_text:
            return False
        # Проверяем, что это ответ на правильный промпт
        prompt_message_id = state.get('prompt_message_id')
        if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
            return False
    else:
        # В личке: принимаем реплай на промпт или следующее сообщение (если состояние активно)
        if message.reply_to_message:
            # Если это реплай, проверяем, что это ответ на правильный промпт
            if message.reply_to_message.from_user and message.reply_to_message.from_user.id == BOT_ID:
                reply_text = message.reply_to_message.text or ""
                if "Пришлите ссылку или ID фильма" not in reply_text:
                    return False
                prompt_message_id = state.get('prompt_message_id')
                if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                    return False
        # Если не реплай, но состояние активно - принимаем как следующее сообщение
    
    return True


@bot.message_handler(func=check_plan_link_reply)
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
            bot.reply_to(message, "❌ Произошла ошибка при обработке ссылки/ID")
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


@bot.message_handler(func=check_clean_imported_ratings_reply)
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
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
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


@bot.message_handler(func=check_import_user_id_reply)
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
            bot.reply_to(message, "❌ Не получилось обработать ID пользователя. Проверьте правильность ввода.")
    except Exception as e:
        logger.error(f"[IMPORT USER ID REPLY] ❌ Критическая ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
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


@bot.message_handler(func=check_list_view_film_reply)
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
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.message_id in added_movie_messages and m.text and m.text.strip().isdigit() and 1 <= int(m.text.strip()) <= 10)
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
        
        bot.reply_to(
            message,
            f"💡 Зачесть оценку <b>{rating}/10</b> и отметить фильм <b>{title}</b> как просмотренный?",
            parse_mode='HTML',
            reply_markup=markup
        )
        logger.info(f"[ADDED MOVIE REPLY] Предложено зачесть оценку {rating} для фильма {title} (film_id={film_id})")
    except Exception as e:
        logger.error(f"[ADDED MOVIE REPLY] Ошибка: {e}", exc_info=True)


@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text and "Введите промокод в ответном сообщении" in (m.reply_to_message.text or ""))
def handle_promo_reply_direct(message):
    """ОТДЕЛЬНЫЙ handler для реплаев на сообщение промокода - ВЫСОКИЙ ПРИОРИТЕТ"""
    logger.info(f"[PROMO REPLY DIRECT] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        promo_code = message.text.strip().upper() if message.text else ""
        
        if not promo_code:
            logger.warning(f"[PROMO REPLY DIRECT] Пустой промокод от пользователя {user_id}")
            bot.reply_to(message, "❌ Промокод не может быть пустым. Введите промокод.")
            return
        
        logger.info(f"[PROMO REPLY DIRECT] Обрабатываем промокод: '{promo_code}' от пользователя {user_id}")
        
        # Получаем состояние промокода
        from moviebot.states import user_promo_state
        if user_id not in user_promo_state:
            logger.warning(f"[PROMO REPLY DIRECT] Пользователь {user_id} не в состоянии промокода")
            bot.reply_to(message, "❌ Сессия истекла. Начните заново с /payment")
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
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
                
                bot.reply_to(message, error_text, reply_markup=markup, parse_mode='HTML')
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
                    # Для всех подписок кроме lifetime добавляем save_payment_method: True
                    payment_data = {
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
                    }
                    
                    # Добавляем save_payment_method для всех не-lifetime подписок
                    if period_type != 'lifetime':
                        payment_data["save_payment_method"] = True
                        logger.info(f"[PROMO REPLY DIRECT] save_payment_method=True добавлен для period_type={period_type}")
                    
                    payment = Payment.create(payment_data)
                    
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
                sent_msg = bot.reply_to(message, text_result, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[PROMO REPLY DIRECT] ✅ Сообщение отправлено успешно: message_id={sent_msg.message_id if sent_msg else 'None'}")
            except Exception as send_e:
                logger.error(f"[PROMO REPLY DIRECT] ❌ Ошибка отправки сообщения: {send_e}", exc_info=True)
                try:
                    sent_msg = bot.send_message(chat_id, text_result, reply_markup=markup, parse_mode='HTML')
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
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back_from_promo"))
            
            bot.reply_to(message, error_text, reply_markup=markup)
            # Не удаляем состояние, чтобы пользователь мог попробовать другой промокод
            return
    except Exception as e:
        logger.error(f"[PROMO REPLY DIRECT] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)


# ==================== ФУНКЦИЯ ДЛЯ УСТАНОВКИ ОЖИДАНИЯ ТЕКСТА ====================
def expect_text_from_user(user_id: int, chat_id: int, expected_for: str = 'search', message_id: int = None):
    """Бот начинает ожидать текстовый ответ от пользователя"""
    user_expected_text[user_id] = {
        'chat_id': chat_id,
        'expected_for': expected_for,  # 'search', 'plan_comment', 'rating_comment' и т.д.
        'message_id': message_id
    }
    logger.info(f"[EXPECT TEXT] Ожидаем текст от user_id={user_id} для '{expected_for}'")


# ==================== ОБЩАЯ ФУНКЦИЯ ДЛЯ ОБРАБОТКИ ПОИСКА ====================
def process_search_query(message, query, reply_to_message=None):
    """Единая логика поиска и отправки результатов. Используется обоими обработчиками."""
    # Ленивый импорт для избежания циклического импорта
    from moviebot.bot.handlers.series import search_films_with_type
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    try:
        # Получаем тип поиска (mixed по умолчанию)
        search_type = 'mixed'
        if user_id in user_search_state:
            search_type = user_search_state[user_id].get('search_type', 'mixed')
        
        # Выполняем поиск
        films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
        
        if not films:
            reply_text = f"❌ Ничего не найдено по запросу '{query}'"
            if reply_to_message:
                bot.reply_to(message, reply_text)
            else:
                bot.send_message(chat_id, reply_text)
            return
        
        # Формируем текст и кнопки
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
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
                logger.error(f"[SEARCH] Ошибка обработки фильма {idx+1}: {film_e}", exc_info=True)
                continue
        
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        results_text += "\n\n🎬 - фильм\n📺 - сериал"
        
        if len(results_text) > 4096:
            results_text = results_text[:4000] + "\n\n... (показаны не все результаты)"
        
        if reply_to_message:
            sent_message = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
        else:
            sent_message = bot.send_message(chat_id, results_text, reply_markup=markup, parse_mode='HTML')
        
        logger.info(f"[SEARCH] Результаты отправлены: message_id={sent_message.message_id}")
        
        # Очищаем состояние, если было
        if user_id in user_search_state:
            del user_search_state[user_id]
            
    except Exception as e:
        logger.error(f"[SEARCH] КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        error_text = "❌ Ошибка при поиске. Попробуйте позже."
        if reply_to_message:
            bot.reply_to(message, error_text)
        else:
            bot.send_message(chat_id, error_text)


# ==================== 1. ОБРАБОТЧИК ДЛЯ ЛС: ТОЛЬКО ЕСЛИ БОТ ОЖИДАЕТ ТЕКСТ ====================
def is_expected_text_in_private(message):
    """Проверка для обработчика ожидаемого текста в ЛС"""
    if message.chat.type != 'private':
        return False
    user_id = message.from_user.id
    if user_id not in user_expected_text:
        return False
    if not message.text or message.text.startswith('/'):
        return False
    if 'kinopoisk.ru' in message.text.lower():
        return False  # ссылки отдельно
    return True


@bot.message_handler(content_types=['text'], func=is_expected_text_in_private)
def handle_expected_text_in_private(message):
    """Обрабатывает ОДНО сообщение в ЛС, когда бот его ждёт"""
    user_id = message.from_user.id
    state = user_expected_text.get(user_id)
    if not state:
        return
    
    query = message.text.strip()
    expected_for = state['expected_for']
    
    logger.info(f"[EXPECTED TEXT PRIVATE] Получен текст от {user_id} для '{expected_for}': '{query[:50]}'")
    
    # Удаляем ожидание сразу — чтобы следующее сообщение НЕ обрабатывалось как поиск
    del user_expected_text[user_id]
    
    if expected_for == 'search':
        process_search_query(message, query, reply_to_message=None)
    elif expected_for == 'shazam_text':
        # Обработка текстового запроса Shazam в личке
        from moviebot.bot.handlers.shazam import process_shazam_text_query
        process_shazam_text_query(message, query, reply_to_message=None)
    # Здесь можно добавить elif для других сценариев: 'plan_comment', 'review' и т.д.
    else:
        # fallback или ошибка
        bot.send_message(message.chat.id, "⚠️ Неизвестный контекст ожидания текста.")


# ==================== 2. ОБРАБОТЧИК ДЛЯ ГРУПП: ТОЛЬКО REPLY НА СООБЩЕНИЕ БОТА ====================
@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and
                                      m.reply_to_message and
                                      m.reply_to_message.from_user.id == BOT_ID and
                                      m.text and
                                      "🔍 Укажите запрос для поиска" in (m.reply_to_message.text or ""))
def handle_group_search_reply(message):
    """Обработчик поиска в группах - только reply на сообщение бота"""
    query = message.text.strip()
    if not query:
        bot.reply_to(message, "❌ Пустой запрос.")
        return
    logger.info(f"[GROUP SEARCH REPLY] Получен запрос от {message.from_user.id}: '{query[:50]}'")
    process_search_query(message, query, reply_to_message=message.reply_to_message)


# ==================== ОБРАБОТЧИК ДЛЯ ПРИВАТНЫХ ЧАТОВ: SHAZAM ТЕКСТ (REPLY) ====================
@bot.message_handler(func=lambda m: m.chat.type == 'private' and
                                    m.reply_to_message and
                                    m.reply_to_message.from_user.id == BOT_ID and
                                    m.text and
                                    "Опишите, что есть в фильме?" in (m.reply_to_message.text or ""))
def handle_private_shazam_text_reply(message):
    """Обработчик текстового запроса Shazam в приватных чатах - reply на сообщение бота"""
    user_id = message.from_user.id
    msg_id = message.message_id

    now = datetime.utcnow()

    prev_msg_id, prev_time = last_processed[user_id]

    # Защита от дубля
    if msg_id == prev_msg_id and (now - prev_time) < PROTECT_WINDOW:
        logger.warning(f"[DUP] Игнорируем дубль сообщения {msg_id} от {user_id}")
        return

    # Обновляем кэш
    last_processed[user_id] = (msg_id, now)
    
    from moviebot.states import user_expected_text
    user_id = message.from_user.id
    
    # Удаляем ожидание через user_expected_text, если оно есть (чтобы избежать двойной обработки)
    if user_id in user_expected_text:
        if user_expected_text[user_id].get('expected_for') == 'shazam_text':
            del user_expected_text[user_id]
    
    query = message.text.strip()
    if not query:
        bot.reply_to(message, "❌ Пустое описание.")
        return
    
    # Проверяем длину (до 300 символов)
    if len(query) > 300:
        bot.reply_to(message, f"❌ Описание слишком длинное ({len(query)} символов). Максимум: 300 символов.")
        return
    
    logger.info(f"[PRIVATE SHAZAM TEXT REPLY] Получен запрос от {user_id}: '{query[:50]}'")
    from moviebot.bot.handlers.shazam import process_shazam_text_query
    process_shazam_text_query(message, query, reply_to_message=message.reply_to_message)


# ==================== ОБРАБОТЧИК ДЛЯ ГРУПП: SHAZAM ТЕКСТ (REPLY) ====================
@bot.message_handler(func=lambda m: m.chat.type in ['group', 'supergroup'] and
                                      m.reply_to_message and
                                      m.reply_to_message.from_user.id == BOT_ID and
                                      m.text and
                                      "Опишите, что есть в фильме?" in (m.reply_to_message.text or ""))
def handle_group_shazam_text_reply(message):
    """Обработчик текстового запроса Shazam в группах - только reply на сообщение бота"""
    query = message.text.strip()
    if not query:
        bot.reply_to(message, "❌ Пустое описание.")
        return
    
    # Проверяем длину (до 300 символов)
    if len(query) > 300:
        bot.reply_to(message, f"❌ Описание слишком длинное ({len(query)} символов). Максимум: 300 символов.")
        return
    
    logger.info(f"[GROUP SHAZAM TEXT REPLY] Получен запрос от {message.from_user.id}: '{query[:50]}'")
    
    # ФИКС: Отправляем сообщение "Обрабатываю..." сразу и запускаем обработку в фоне
    # Это позволяет вебхуку завершиться быстро и избежать таймаутов Telegram
    try:
        loading_msg = bot.reply_to(message, "🔍 Обрабатываю запрос...")
        
        # Запускаем обработку в отдельном потоке
        from threading import Thread
        from moviebot.bot.handlers.shazam import process_shazam_text_query
        
        def process_in_background():
            try:
                # Передаем loading_msg, чтобы не дублировать сообщение
                process_shazam_text_query(message, query, reply_to_message=message.reply_to_message, loading_msg=loading_msg)
            except Exception as e:
                logger.error(f"[GROUP SHAZAM TEXT REPLY] Ошибка в фоновой обработке: {e}", exc_info=True)
                try:
                    bot.reply_to(message, f"❌ Произошла ошибка при обработке: {str(e)[:200]}")
                except:
                    pass
        
        thread = Thread(target=process_in_background, daemon=True)
        thread.start()
        logger.info(f"[GROUP SHAZAM TEXT REPLY] Обработка запущена в фоновом потоке для user_id={message.from_user.id}")
        
    except Exception as e:
        logger.error(f"[GROUP SHAZAM TEXT REPLY] Критическая ошибка: {e}", exc_info=True)
        # Пытаемся обработать синхронно как fallback
        try:
            from moviebot.bot.handlers.shazam import process_shazam_text_query
            process_shazam_text_query(message, query, reply_to_message=message.reply_to_message)
        except:
            bot.reply_to(message, "❌ Произошла ошибка при обработке запроса.")


# ==================== СТАРЫЙ ОБРАБОТЧИК (ОСТАВЛЯЕМ ДЛЯ СОВМЕСТИМОСТИ) ====================
@bot.message_handler(func=lambda m: m.reply_to_message and m.reply_to_message.from_user.id == BOT_ID and m.text and "🔍 Укажите запрос для поиска" in (m.reply_to_message.text or ""))
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
            bot.reply_to(message, f"❌ Ошибка при выполнении поиска. Попробуйте еще раз.")
            return
        
        if not films:
            logger.warning(f"[SEARCH REPLY DIRECT] Ничего не найдено по запросу '{query}'")
            bot.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
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
            sent_message = bot.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[SEARCH REPLY DIRECT] ✅ Результаты поиска отправлены: message_id={sent_message.message_id if sent_message else 'None'}")
            # Удаляем состояние после успешной отправки
            if user_id in user_search_state:
                del user_search_state[user_id]
        except Exception as send_e:
            logger.error(f"[SEARCH REPLY DIRECT] ❌ Ошибка отправки результатов: {send_e}", exc_info=True)
    except Exception as e:
        logger.error(f"[SEARCH REPLY DIRECT] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)


# УДАЛЕНО: check_admin_commands_reply - теперь все обрабатывается через check_admin_message в state_handlers.py
# Оставляем пустую функцию для совместимости, но она больше не используется

def check_rate_reply(message):
    """Проверка для handler ответа на запрос оценки (реплай или следующее сообщение в личке)"""
    is_private = message.chat.type == 'private'
    
    from moviebot.states import rating_messages
    
    if not message.text or not message.text.strip().isdigit():
        return False
    rating = int(message.text.strip())
    if not 1 <= rating <= 10:
        return False
    
    if not message.reply_to_message:
        if is_private:
            # В личке принимаем следующее сообщение, если есть активный запрос оценки
            if rating_messages:  # если есть любой активный запрос
                return True
            return False
        else:
            return False  # в группе только реплай
    
    # Если реплай — проверяем, что это ответ на сообщение с запросом оценки
    reply_msg_id = message.reply_to_message.message_id
    if reply_msg_id in rating_messages:
        return True
    
    return False

@bot.message_handler(func=check_rate_reply)
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
    from moviebot.states import (
        user_plan_state, user_promo_state, user_promo_admin_state,
        user_ticket_state, user_search_state, user_settings_state,
        user_edit_state, user_view_film_state, user_import_state,
        user_clean_state, user_cancel_subscription_state, user_refund_state,
        user_unsubscribe_state, user_add_admin_state
    )
    
    logger.info(f"[HANDLE RATE LIST REPLY] Проверка состояний: user_search_state={user_id in user_search_state}, user_plan_state={user_id in user_plan_state}, user_ticket_state={user_id in user_ticket_state}")
    
    if user_id in user_promo_state or user_id in user_promo_admin_state:
        logger.info(f"[HANDLE RATE LIST REPLY] Пропуск — промокод")
        return
    
    if user_id in user_search_state:
        logger.info(f"[HANDLE RATE LIST REPLY] Пропуск — поиск")
        return
    
    # ФИКС: Если пользователь в планировании и на step=3 (ввод даты) — НЕ пропускаем
    if user_id in user_plan_state:
        state = user_plan_state[user_id]
        if state.get('step') == 3:
            logger.info(f"[HANDLE RATE LIST REPLY] НЕ пропускаем — пользователь на step=3 (ввод даты)")
            # Сообщение идёт дальше в handle_plan_datetime_reply
        else:
            logger.info(f"[HANDLE RATE LIST REPLY] Пропуск — пользователь в планировании, но не на step=3")
            return
    
    # Остальные состояния — пропускаем
    if (user_id in user_ticket_state or
        user_id in user_settings_state or
        user_id in user_edit_state or
        user_id in user_view_film_state or
        user_id in user_import_state or
        user_id in user_clean_state or
        user_id in user_cancel_subscription_state):
        logger.info(f"[HANDLE RATE LIST REPLY] Пропуск — другое состояние")
        return
    
    # Обрабатываем сообщения с оценками (числа от 1 до 10)
    text_stripped = message.text.strip() if message.text else ""
    
    # Строгая проверка: только чистые оценки 1–10
    if text_stripped in {'1', '2', '3', '4', '5', '6', '7', '8', '9', '10'}:
        rating = int(text_stripped)
        logger.info(f"[HANDLE RATE LIST REPLY] Обнаружена чистая оценка: {rating}, обрабатываем")
        
        reply_msg_id = message.reply_to_message.message_id if message.reply_to_message else None
        from moviebot.states import rating_messages
        
        try:
            from moviebot.bot.handlers.rate import handle_rating_internal
            handle_rating_internal(message, rating)
            logger.info(f"[HANDLE RATE LIST REPLY] handle_rating_internal завершен")
            
            # Удаляем rating_messages только после успешной обработки
            if reply_msg_id and reply_msg_id in rating_messages:
                del rating_messages[reply_msg_id]
                logger.info(f"[HANDLE RATE LIST REPLY] Очищено rating_messages для reply_msg_id={reply_msg_id}")
        except Exception as rating_e:
            logger.error(f"[HANDLE RATE LIST REPLY] ❌ Ошибка в handle_rating_internal: {rating_e}", exc_info=True)
            # Удаляем rating_messages даже при ошибке, чтобы не блокировать повторные попытки
            if reply_msg_id and reply_msg_id in rating_messages:
                del rating_messages[reply_msg_id]
                logger.info(f"[HANDLE RATE LIST REPLY] Очищено rating_messages после ошибки для reply_msg_id={reply_msg_id}")
        
        return
    
    chat_id = message.chat.id
    
    # Проверяем, что это реплай на список фильмов
    reply_text = message.reply_to_message.text or ""
    if "Список просмотренных фильмов для оценки" not in reply_text:
        return
    
    text = message.text.strip()
    if not text:
        return
    
    # Парсим оценки: kp_id оценка
    ratings_pattern = r'(\d+)\s*[,;:\t]?\s*(\d+)'
    matches = re.findall(ratings_pattern, text)
    
    if not matches:
        bot.reply_to(message, "❌ Не удалось распознать оценки. Используйте формат: <code>kp_id оценка</code>", parse_mode='HTML')
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
                
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, str(str(kp_id))))
                film_row = cursor.fetchone()
                
                if not film_row:
                    errors.append(f"{kp_id}: фильм не найден или не просмотрен")
                    continue
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                cursor.execute('''
                    SELECT rating FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                existing = cursor.fetchone()
                
                if existing:
                    errors.append(f"{kp_id}: вы уже оценили этот фильм")
                    continue
                
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                
                results.append((kp_id, title, rating))
                
                # Проверка, все ли оценили
                cursor.execute('''
                    SELECT DISTINCT user_id FROM stats WHERE chat_id = %s AND user_id IS NOT NULL
                ''', (chat_id,))
                active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
                cursor.execute('''
                    SELECT DISTINCT user_id FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                ''', (chat_id, film_id))
                rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                
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
    
    bot.reply_to(message, response_text, parse_mode='HTML')


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


@bot.message_handler(func=lambda m: (
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
                bot.send_message(chat_id, f"🎉 Добавлено {added_count} новых фильмов в базу!")
    except Exception as e:
        logger.warning(f"[SAVE MESSAGE] Ошибка при обработке сообщения с фильмом: {e}", exc_info=True)

@bot.message_handler(content_types=['text'], func=lambda m: not (m.text and m.text.strip().startswith('/')))
def main_text_handler(message):
    """
    Fallback handler для текстовых сообщений (исключая команды)
    Все состояния теперь обрабатываются отдельными handlers в state_handlers.py
    """
    import sys
    print(f"[MAIN TEXT HANDLER] ===== START (FALLBACK): message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}, text='{message.text[:100] if message.text else None}'", file=sys.stdout, flush=True)
    #Этот handler обрабатывает только специальные случаи и реплаи, которые не попали в другие handlers
    # logger.info(f"[MAIN TEXT HANDLER] ===== START (FALLBACK): message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}, text='{message.text[:100] if message.text else ''}'")
    
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
            # clean_imported_ratings теперь обрабатывается через handle_clean, не используем user_private_handler_state
            # if handler_name == 'clean_imported_ratings':
            #     ... (удалено, теперь обрабатывается через handle_clean)
            
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
        user_cancel_subscription_state, user_view_film_state,
        user_refund_state, user_unsubscribe_state, user_add_admin_state
        # Админские состояния обрабатываются через handle_admin в state_handlers.py
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
        user_id in user_view_film_state or user_id in user_refund_state or
        user_id in user_unsubscribe_state or user_id in user_add_admin_state):
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в состоянии, пропускаем (обработает специализированный handler)")
        return
    
    # Обработка реплаев и специальных случаев (fallback для необработанных сообщений)
    
    # Реплай на сообщение бота с оценками
    if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
        reply_text = message.reply_to_message.text or ""
        
        if "Список просмотренных фильмов для оценки" in reply_text:
#             from ...
#             handle_rate_list_reply_internal(message)
            return
        
        reply_msg_id = message.reply_to_message.message_id
        if reply_msg_id in bot_messages:
            link = bot_messages.get(reply_msg_id)
            if link:
                # Вместо старого вызова
                bot.reply_to(message, "Используй кнопку «📅 Запланировать просмотр» под карточкой фильма — там дата, время, дома/в кино всё нормально парсится.")
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
@bot.message_handler(content_types=['photo', 'document'])
def main_file_handler(message):
    """Единый хэндлер для всех фото и документов"""
    user_id = message.from_user.id
    
    # === ФИКС: Если пользователь на шаге ввода даты (step=3) — игнорируем файл ===
    from moviebot.states import user_plan_state
    if user_id in user_plan_state:
        state = user_plan_state[user_id]
        if state.get('step') == 3:
            logger.info(f"[MAIN FILE HANDLER] Игнорируем файл — пользователь на step=3 планирования (ввод даты)")
            return
    
    logger.info(f"[MAIN FILE HANDLER] Получено фото/документ от {user_id}")
    
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
                    bot.reply_to(message, "❌ Ошибка: не найдены данные мероприятия.")
                    if user_id in user_ticket_state:
                        del user_ticket_state[user_id]
                    return
                
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                
                # ФИКС: создаём conn и cursor
                conn = get_db_connection()
                cursor = get_db_cursor()
                
                with db_lock:
                    cursor.execute('''
                        INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id, ticket_file_id)
                        VALUES (%s, NULL, 'cinema', %s, %s, %s)
                    ''', (chat_id, event_datetime_utc, user_id, file_id))
                    conn.commit()
                
                logger.info(f"[EVENT TICKET] Билет на мероприятие сохранен: event_name={event_name}, chat_id={chat_id}, user_id={user_id}")
                bot.reply_to(message, f"✅ Билет на мероприятие <b>{event_name}</b> сохранён! 🎟️", parse_mode='HTML')
                
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            except Exception as e:
                logger.error(f"[EVENT TICKET] Ошибка при сохранении билета на мероприятие: {e}", exc_info=True)
                bot.reply_to(message, "❌ Произошла ошибка при сохранении билета.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
        
        if step == 'upload_ticket':
            plan_id = state.get('plan_id')
            chat_id_state = state.get('chat_id', message.chat.id)
            logger.info(f"[TICKET UPLOAD] Начало загрузки билета: plan_id={plan_id}, user_id={user_id}, chat_id={chat_id_state}")
            
            if not plan_id:
                logger.error(f"[TICKET UPLOAD] plan_id не найден в состоянии")
                bot.reply_to(message, "❌ Ошибка: план не найден.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            
            # Проверка реплаев в групповых чатах
            try:
                chat_info = bot.get_chat(message.chat.id)
                is_private = chat_info.type == 'private'
            except:
                is_private = message.chat.id > 0
            
            if not is_private:
                # В групповых чатах требуем реплай на сообщение бота
                if not message.reply_to_message:
                    logger.info(f"[TICKET UPLOAD] В групповом чате требуется реплай на сообщение бота")
                    return
                if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
                    logger.info(f"[TICKET UPLOAD] Реплай не на сообщение бота")
                    return
                # Проверяем prompt_message_id, если сохранён
                prompt_message_id = state.get('prompt_message_id')
                if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                    logger.info(f"[TICKET UPLOAD] Реплай не на правильное сообщение (ожидаемый id={prompt_message_id}, получен {message.reply_to_message.message_id})")
                    return

            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            logger.info(f"[TICKET UPLOAD] Получен file_id={file_id}, тип: {'photo' if message.photo else 'document'}")

            conn = get_db_connection()  # ФИКС: создаём в начале блока
            cursor = get_db_cursor()    # ФИКС: создаём в начале блока
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
                
                existing_tickets.append(file_id)
                tickets_json = json.dumps(existing_tickets, ensure_ascii=False)
                
                cursor.execute("UPDATE plans SET ticket_file_id = %s WHERE id = %s", (tickets_json, plan_id))
                conn.commit()
                logger.info(f"[TICKET UPLOAD] Билет сохранен в БД для plan_id={plan_id}")

            total_tickets = len(existing_tickets)
            was_first = total_tickets == 1
            logger.info(f"[TICKET UPLOAD] Всего билетов: {total_tickets}, первый билет: {was_first}")

            if was_first:
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("➕ Добавить ещё билет", callback_data=f"add_more_tickets:{plan_id}"))
                markup.add(InlineKeyboardButton("⬅️ К списку мероприятий", callback_data="ticket_new"))

                # В группах используем reply, в личке - следующее сообщение
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if is_private:
                    bot.send_message(
                        message.chat.id,
                        f"✅ Билет добавлен! (Всего: {total_tickets})\n\n"
                        f"Можете добавить ещё или вернуться к списку.",
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                else:
                    bot.reply_to(
                        message,
                        f"✅ Билет добавлен! (Всего: {total_tickets})\n\n"
                        f"Можете добавить ещё или вернуться к списку.",
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                state['step'] = 'add_more_tickets'
            else:
                # В группах используем reply, в личке - следующее сообщение
                try:
                    chat_info = bot.get_chat(message.chat.id)
                    is_private = chat_info.type == 'private'
                except:
                    is_private = message.chat.id > 0
                
                if is_private:
                    bot.send_message(message.chat.id, f"✅ Билет добавлен! (Всего: {total_tickets})")
                else:
                    bot.reply_to(message, f"✅ Билет добавлен! (Всего: {total_tickets})")

            logger.info(f"[TICKET UPLOAD] ✅ Завершено: билет добавлен к plan_id={plan_id}, всего: {total_tickets}")
            return
                
        if step == 'waiting_ticket_file':
            # Пользователь выбрал сеанс и загружает билет
            plan_id = state.get('plan_id')
            if plan_id:
                file_id = message.photo[-1].file_id if message.photo else message.document.file_id
                # Сохраняем билет в БД как массив
                import json

                conn = get_db_connection()   
                cursor = get_db_cursor()    
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
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("✏️ Изменить время", callback_data=f"ticket_edit_time:{plan_id}"))
                markup.add(InlineKeyboardButton("➕ Добавить еще билет к сеансу", callback_data=f"add_ticket:{plan_id}"))
                markup.add(InlineKeyboardButton("🎟️ Вернуться к билетам", callback_data="ticket_new"))
                
                bot.reply_to(message, f"✅ Файл получен. (Всего билетов: {len(existing_tickets)}) Можете отправить ещё билеты или написать 'готово'. 🍿", reply_markup=markup)
                # НЕ очищаем состояние - пользователь может добавить ещё билеты
                logger.info(f"[TICKET FILE] Состояние пользователя {user_id} сохранено для добавления дополнительных билетов")
                return
        
        if step == 'add_more_tickets':
            # Обработка добавления дополнительных билетов
            plan_id = state.get('plan_id')
            if not plan_id:
                bot.reply_to(message, "❌ Ошибка: план не найден.")
                if user_id in user_ticket_state:
                    del user_ticket_state[user_id]
                return
            
            # Проверка реплаев в групповых чатах
            try:
                chat_info = bot.get_chat(message.chat.id)
                is_private = chat_info.type == 'private'
            except:
                is_private = message.chat.id > 0
            
            if not is_private:
                # В групповых чатах требуем реплай на сообщение бота
                if not message.reply_to_message:
                    logger.info(f"[TICKET ADD MORE] В групповом чате требуется реплай на сообщение бота")
                    return
                if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
                    logger.info(f"[TICKET ADD MORE] Реплай не на сообщение бота")
                    return
                # Проверяем prompt_message_id, если сохранён
                prompt_message_id = state.get('prompt_message_id')
                if prompt_message_id and message.reply_to_message.message_id != prompt_message_id:
                    logger.info(f"[TICKET ADD MORE] Реплай не на правильное сообщение (ожидаемый id={prompt_message_id}, получен {message.reply_to_message.message_id})")
                    return
            
            file_id = message.photo[-1].file_id if message.photo else message.document.file_id
            
            # Получаем существующие билеты и добавляем новый
            conn = get_db_connection()   # ← добавь
            cursor = get_db_cursor()     # ← добавь

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
            
            # В группах используем reply, в личке - следующее сообщение
            try:
                chat_info = bot.get_chat(message.chat.id)
                is_private = chat_info.type == 'private'
            except:
                is_private = message.chat.id > 0
            
            if is_private:
                bot.send_message(message.chat.id, f"✅ Билет добавлен! (Всего билетов: {len(existing_tickets)})\n\nМожете отправить ещё билеты или написать 'готово'.")
            else:
                bot.reply_to(message, f"✅ Билет добавлен! (Всего билетов: {len(existing_tickets)})\n\nМожете отправить ещё билеты или написать 'готово'.")
            return
        
        # Сохраняем file_id для последующей обработки
        file_id = message.photo[-1].file_id if message.photo else message.document.file_id
        state['file_id'] = file_id
        
        # Добавляем кнопки после получения файла
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🎟️ Вернуться к билетам", callback_data="ticket_new"))
        
        bot.reply_to(message, "✅ Файл получен. Приятного просмотра! 🍿", reply_markup=markup)
        # Очищаем состояние пользователя, завершаем цикл работы с билетами
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
        logger.info(f"[TICKET FILE] Состояние пользователя {user_id} очищено после получения файла")
        return
    
    # Если не в состоянии - игнорируем
    logger.info(f"[MAIN FILE HANDLER] Фото/документ не обработан (пользователь не в user_ticket_state)")


@bot.message_reaction_handler(func=lambda r: True)
def handle_reaction(reaction):
    """Обработчик реакций на сообщения - отмечает фильмы как просмотренные через эмодзи"""
    logger.info(f"[REACTION] Получена реакция в чате {reaction.chat.id} на сообщение {reaction.message_id}")
    
    chat_id = reaction.chat.id
    message_id = reaction.message_id
    user_id = reaction.user.id if hasattr(reaction, 'user') and reaction.user else None
    
    # Проверяем участие в боте (только для реакций на сообщения о фильмах, не для настроек)
    if user_id and message_id not in settings_messages:
        if not is_bot_participant(chat_id, user_id):
            try:
                bot.send_message(
                    chat_id,
                    f"Чтобы взаимодействовать с ботом, начните участие в нём с любой команды, например, /join",
                    reply_to_message_id=message_id
                )
            except:
                pass
            return
    
    # Проверяем, не это ли реакция на сообщение settings
    if message_id in settings_messages:
        # Обработка реакций на настройки уже реализована в settings.py
        return
    
    # Получаем обычные эмодзи (как список символов) для этого чата
    ordinary_emojis = list(get_watched_emojis(chat_id))
    
    # Получаем кастомные эмодзи ID для этого чата
    custom_emoji_ids = get_watched_custom_emoji_ids(chat_id)
    
    logger.info(f"[REACTION] Проверка watched эмодзи для чата {chat_id}")
    logger.info(f"[REACTION] Доступные watched эмодзи: {ordinary_emojis}")
    logger.info(f"[REACTION] Доступные кастомные ID: {custom_emoji_ids}")
    
    is_watched = False
    
    if not reaction.new_reaction:
        logger.info("[REACTION] Нет новых реакций")
        return
    
    logger.info(f"[REACTION] Количество новых реакций: {len(reaction.new_reaction)}")
    
    # Нормализуем эмодзи для сравнения (убираем variation selector)
    def normalize_emoji(emoji_str):
        """Убирает variation selector (FE0F) из эмодзи для нормализации"""
        if not emoji_str:
            return emoji_str
        # Убираем variation selector (U+FE0F)
        return emoji_str.replace('\ufe0f', '')
    
    # Нормализуем список watched эмодзи
    normalized_watched = [normalize_emoji(e) for e in ordinary_emojis]
    
    for r in reaction.new_reaction:
        logger.info(f"[REACTION DEBUG] Реакция: type={getattr(r, 'type', 'unknown')}, emoji={getattr(r, 'emoji', None)}, custom_emoji_id={getattr(r, 'custom_emoji_id', None)}")
        
        if hasattr(r, 'type') and r.type == 'emoji' and hasattr(r, 'emoji'):
            normalized_reaction = normalize_emoji(r.emoji)
            if normalized_reaction in normalized_watched:
                logger.info(f"[REACTION DEBUG] ✅ Найден watched эмодзи: {r.emoji} (нормализован: {normalized_reaction})")
                is_watched = True
                break
            else:
                logger.info(f"[REACTION DEBUG] ❌ Эмодзи {r.emoji} (нормализован: {normalized_reaction}) не в списке watched: {normalized_watched}")
        elif hasattr(r, 'type') and r.type == 'custom_emoji' and hasattr(r, 'custom_emoji_id'):
            if str(r.custom_emoji_id) in custom_emoji_ids:
                logger.info(f"[REACTION DEBUG] ✅ Найден watched кастомный эмодзи ID: {r.custom_emoji_id}")
                is_watched = True
                break
            else:
                logger.info(f"[REACTION DEBUG] ❌ Кастомный ID {r.custom_emoji_id} не в списке watched: {custom_emoji_ids}")
        else:
            # Старый формат реакции (без type)
            if hasattr(r, 'emoji'):
                if r.emoji in ordinary_emojis:
                    logger.info(f"[REACTION DEBUG] ✅ Найден watched эмодзи (старый формат): {r.emoji}")
                    is_watched = True
                    break
                else:
                    logger.info(f"[REACTION DEBUG] ❌ Эмодзи {r.emoji} не в списке watched (старый формат): {ordinary_emojis}")
    
    # Получаем ссылку на фильм (нужно для отметки как просмотренного и предложения добавить эмодзи)
    link = bot_messages.get(message_id)
    if not link:
        plan_data = plan_notification_messages.get(message_id)
        if plan_data:
            link = plan_data.get('link')
    
    # Если не найдено, пытаемся найти в БД по message_id или другим способом
    if not link:
        logger.info(f"[REACTION] Не найдено в bot_messages и plan_notification_messages для message_id={message_id}")
        # Пробуем найти фильм в БД по последним добавленным фильмам в этом чате
        try:
            with db_lock:
                # Ищем последние фильмы в этом чате (за последний час)
                cursor.execute("""
                    SELECT link FROM movies 
                    WHERE chat_id = %s 
                    ORDER BY id DESC 
                    LIMIT 10
                """, (chat_id,))
                recent_links = cursor.fetchall()
                # Если в чате недавно был добавлен только один фильм, используем его
                if len(recent_links) == 1:
                    link = recent_links[0].get('link') if isinstance(recent_links[0], dict) else recent_links[0][0]
                    logger.info(f"[REACTION] Использована последняя ссылка из БД: {link}")
                    bot_messages[message_id] = link
        except Exception as e:
            logger.warning(f"[REACTION] Ошибка при поиске в БД: {e}")
    
    # Если эмодзи не в списке watched, предлагаем добавить его, но все равно отмечаем фильм как просмотренный
    if not is_watched and link:
        logger.info("[REACTION] Не watched эмодзи — предлагаем добавить и отмечаем фильм как просмотренный")
        
        user_id = reaction.user.id if reaction.user else None
        if user_id:
            try:
                # Получаем первое новое эмодзи (обычное или кастомное)
                new_emoji = None
                new_custom_emoji_id = None
                for r in reaction.new_reaction:
                    if hasattr(r, 'type') and r.type == 'emoji' and hasattr(r, 'emoji'):
                        new_emoji = r.emoji
                        break
                    elif hasattr(r, 'type') and r.type == 'custom_emoji' and hasattr(r, 'custom_emoji_id'):
                        new_custom_emoji_id = str(r.custom_emoji_id)
                        break
                    elif hasattr(r, 'emoji'):
                        new_emoji = r.emoji
                        break
                
                # Предлагаем добавить эмодзи (только если еще не предлагали)
                if new_emoji or new_custom_emoji_id:
                    emoji_for_key = new_emoji if new_emoji else f"custom:{new_custom_emoji_id}"
                    emoji_suggestion_key = f"{chat_id}:{emoji_for_key}:{message_id}"
                    if not hasattr(handle_reaction, '_emoji_suggestions'):
                        handle_reaction._emoji_suggestions = set()
                    
                    if emoji_suggestion_key not in handle_reaction._emoji_suggestions:
                        handle_reaction._emoji_suggestions.add(emoji_suggestion_key)
                        
                        # Предлагаем добавить эмодзи
                        markup = InlineKeyboardMarkup()
                        if new_emoji:
                            markup.add(InlineKeyboardButton("✅ Добавить", callback_data=f"add_emoji:{new_emoji}"))
                            emoji_display = new_emoji
                        else:
                            markup.add(InlineKeyboardButton("✅ Добавить", callback_data=f"add_custom_emoji:{new_custom_emoji_id}"))
                            emoji_display = f"кастомное эмодзи (ID: {new_custom_emoji_id})"
                        
                        markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"cancel_add_emoji:{message_id}"))
                        
                        bot.send_message(
                            chat_id,
                            f"💡 Хотите добавить {emoji_display} в список разрешённых для отметки о просмотре?",
                            reply_to_message_id=message_id,
                            reply_markup=markup
                        )
                        logger.info(f"[REACTION] Предложено добавить {emoji_display} для чата {chat_id} на сообщение {message_id}")
            except Exception as e:
                logger.error(f"[REACTION] Ошибка при предложении добавить эмодзи: {e}", exc_info=True)
    
    # Если нет ссылки на фильм, не можем обработать
    if not link:
        logger.info(f"[REACTION] Нет link для message_id={message_id}, chat_id={chat_id}. Реакция не обработана.")
        return
    
    # Отмечаем фильм как просмотренный (даже если эмодзи не в списке watched)
    # Это позволяет пользователю отмечать фильмы любым эмодзи, а не только из списка watched
    
    user_id = reaction.user.id if reaction.user else None
    if not user_id:
        logger.warning("[REACTION] Не удалось получить user_id")
        return
    
    with db_lock:
        cursor.execute("SELECT id, title FROM movies WHERE link = %s AND chat_id = %s", (link, chat_id))
        film = cursor.fetchone()
        if not film:
            logger.info("[REACTION] Фильм не найден")
            return
        
        film_id = film.get('id') if isinstance(film, dict) else film[0]
        film_title = film.get('title') if isinstance(film, dict) else film[1]
        
        # Проверяем, не просмотрел ли уже этот пользователь
        cursor.execute("SELECT id FROM watched_movies WHERE chat_id = %s AND film_id = %s AND user_id = %s", 
                      (chat_id, film_id, user_id))
        already_watched = cursor.fetchone()
        
        if already_watched:
            logger.info(f"[REACTION] Пользователь {user_id} уже отметил фильм {film_title} как просмотренный")
            return
        
        # Сохраняем просмотр для конкретного пользователя
        cursor.execute("""
            INSERT INTO watched_movies (chat_id, film_id, user_id, watched_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (chat_id, film_id, user_id) DO NOTHING
        """, (chat_id, film_id, user_id))
        
        # Обновляем watched для фильма (если хотя бы один просмотрел)
        cursor.execute("""
            UPDATE movies 
            SET watched = 1 
            WHERE id = %s AND (
                SELECT COUNT(*) FROM watched_movies WHERE film_id = %s AND chat_id = %s
            ) > 0
        """, (film_id, film_id, chat_id))
        
        conn.commit()
        logger.info(f"[REACTION] Фильм {film_title} отмечен просмотренным пользователем {user_id}")
        
        # Получаем kp_id для получения фактов
        cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
        kp_row = cursor.fetchone()
        kp_id = kp_row.get('kp_id') if isinstance(kp_row, dict) else (kp_row[0] if kp_row else None)
    
    # Отправляем персональное сообщение пользователю с упоминанием
    user_name = reaction.user.first_name if reaction.user else "Вы"
    user_mention = f"@{reaction.user.username}" if reaction.user and reaction.user.username else user_name
    msg = bot.send_message(chat_id, 
        f"🎬 {user_mention}, фильм <b>{film_title}</b> отмечен как просмотренный!\n\n"
        f"💬 Ответьте числом от 1 до 10 на это сообщение или на сообщение с фильмом, чтобы поставить оценку.",
        parse_mode='HTML')
    
    # Сохраняем связь message_id -> film_id для обработки оценки
    rating_messages[msg.message_id] = film_id
    logger.info(f"[REACTION] Сообщение об оценке отправлено для {user_name}, message_id={msg.message_id}, film_id={film_id}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("add_more_tickets:"))
def add_more_tickets_from_plan(call):
    bot.answer_callback_query(call.id)
    plan_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    user_ticket_state[user_id] = {
        'step': 'add_more_tickets',
        'plan_id': plan_id,
        'chat_id': call.message.chat.id
    }

    bot.edit_message_text(
        chat_id=call.message.chat.id,
        message_id=call.message.message_id,
        text="➕ Продолжаем добавлять билеты.\n\nОтправьте ещё фото/файлы.",
        parse_mode='HTML'
    )

@bot.callback_query_handler(func=lambda call: call.data == "ticket_new")
def back_to_ticket_list(call):
    bot.answer_callback_query(call.id)
    user_id = call.from_user.id
    if user_id in user_ticket_state:
        del user_ticket_state[user_id]

    # Вызываем твою функцию показа списка билетов/мероприятий
    from moviebot.bot.handlers.series import show_cinema_sessions  # или какая у тебя функция для /tickets
    show_cinema_sessions(call.message.chat.id, user_id, call.message.message_id)  # адаптируй под свою функцию

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_emoji:"))
def add_emoji_callback(call):
    """Обработчик кнопки 'Добавить' для обычного эмодзи"""
    try:
        emoji = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем текущие эмодзи
        current_emojis = list(get_watched_emojis(chat_id))
        
        # Добавляем новое эмодзи, если его еще нет
        if emoji not in current_emojis:
            current_emojis.append(emoji)
            emojis_str = ''.join(current_emojis)
            
            # Сохраняем в БД
            with db_lock:
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, 'watched_emoji', %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (chat_id, emojis_str))
                conn.commit()
            
            bot.answer_callback_query(call.id, f"✅ Эмодзи {emoji} добавлен!")
            bot.edit_message_text(
                f"✅ Эмодзи {emoji} добавлен в список разрешённых для отметки о просмотре.",
                chat_id,
                call.message.message_id
            )
            logger.info(f"[ADD EMOJI] Эмодзи {emoji} добавлен для чата {chat_id}")
        else:
            bot.answer_callback_query(call.id, "Эмодзи уже в списке")
            bot.delete_message(chat_id, call.message.message_id)
    except Exception as e:
        logger.error(f"[ADD EMOJI] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_custom_emoji:"))
def add_custom_emoji_callback(call):
    """Обработчик кнопки 'Добавить' для кастомного эмодзи"""
    try:
        custom_emoji_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Получаем текущие эмодзи и кастомные ID
        current_emojis = list(get_watched_emojis(chat_id))
        current_custom_ids = get_watched_custom_emoji_ids(chat_id)
        
        # Добавляем новое кастомное эмодзи, если его еще нет
        if custom_emoji_id not in current_custom_ids:
            current_custom_ids.append(custom_emoji_id)
            emojis_str = ''.join(current_emojis)
            if current_custom_ids:
                custom_str = ','.join([f"custom:{cid}" for cid in current_custom_ids])
                emojis_str = emojis_str + (',' + custom_str if emojis_str else custom_str)
            
            # Сохраняем в БД
            with db_lock:
                cursor.execute('''
                    INSERT INTO settings (chat_id, key, value)
                    VALUES (%s, 'watched_emoji', %s)
                    ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                ''', (chat_id, emojis_str))
                conn.commit()
            
            bot.answer_callback_query(call.id, f"✅ Кастомное эмодзи добавлено!")
            bot.edit_message_text(
                f"✅ Кастомное эмодзи (ID: {custom_emoji_id}) добавлено в список разрешённых для отметки о просмотре.",
                chat_id,
                call.message.message_id
            )
            logger.info(f"[ADD CUSTOM EMOJI] Кастомное эмодзи {custom_emoji_id} добавлено для чата {chat_id}")
        else:
            bot.answer_callback_query(call.id, "Эмодзи уже в списке")
            bot.delete_message(chat_id, call.message.message_id)
    except Exception as e:
        logger.error(f"[ADD CUSTOM EMOJI] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("cancel_add_emoji:"))
def cancel_add_emoji_callback(call):
    """Обработчик кнопки 'Отменить' для предложения добавить эмодзи"""
    try:
        bot.answer_callback_query(call.id, "Отменено")
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except Exception as e:
        logger.error(f"[CANCEL ADD EMOJI] Ошибка: {e}", exc_info=True)

def is_random_instruction_reply(message):
    """Реплай на инструкцию рандома 'Что дальше?'"""
    if not message.reply_to_message:
        return False
    reply_id = message.reply_to_message.message_id
    if reply_id not in bot_messages:
        return False
    link = bot_messages.get(reply_id)
    if not link or 'kinopoisk.ru/film/' not in link:
        return False
    text = (message.reply_to_message.text or "").lower()
    if "что дальше?" not in text:
        return False
    if not message.text or message.text.startswith('/'):
        return False
    return True

def is_random_film_reply(message):
    if not message.reply_to_message:
        return False
    reply_id = message.reply_to_message.message_id
    if reply_id not in bot_messages:
        return False
    link = bot_messages.get(reply_id)
    if not link or 'kinopoisk.ru/film/' not in link:
        return False
    if not message.text or message.text.startswith('/'):
        return False
    return True

@bot.message_handler(func=is_random_film_reply)
def handle_random_film_plan_reply(message):
    reply_id = message.reply_to_message.message_id
    link = bot_messages.get(reply_id)
    if not link:
        bot.reply_to(message, "Ссылка потерялась :(")
        return

    text = message.text.strip().lower()
    user_id = message.from_user.id
    chat_id = message.chat.id

    # Определяем plan_type по тексту
    plan_type = 'home' if 'дома' in text else 'cinema' if 'кино' in text else None
    if not plan_type:
        bot.reply_to(message, "Не понял место: напиши 'дома' или 'в кино'")
        return

    # Убираем место из текста, оставляем дату/время
    day_or_date = text.replace('дома', '').replace('в кино', '').replace('кино', '').strip()
    if not day_or_date:
        day_or_date = None  # если только "дома" — без даты

    logger.info(f"[RANDOM FILM REPLY] Планирование: {plan_type}, дата='{day_or_date}', link={link}")

    from moviebot.bot.handlers.plan import process_plan

    try:
        process_plan(bot, user_id, chat_id, link, plan_type, day_or_date)
    except Exception as e:
        logger.error(f"[RANDOM FILM REPLY] Ошибка process_plan: {e}")
        bot.reply_to(message, "Не смог добавить в план :(")

# ======== ОТВЕТЫ НА РАНДОМ И НА ИНСТРУКЦИЮ К РАНДОМУ ========
def is_random_instruction_reply(message):
    if not message.reply_to_message:
        return False
    reply_id = message.reply_to_message.message_id
    if reply_id not in bot_messages:
        return False
    link = bot_messages.get(reply_id)
    if not link or 'kinopoisk.ru/film/' not in link:
        return False
    reply_text = (message.reply_to_message.text or "").lower()
    if "что дальше?" not in reply_text:
        return False
    if not message.text or message.text.startswith('/'):
        return False
    return True

@bot.message_handler(func=is_random_instruction_reply)
def handle_random_instruction_plan_reply(message):
    reply_id = message.reply_to_message.message_id
    link = bot_messages.get(reply_id)
    if not link:
        bot.reply_to(message, "Ссылка потерялась :(")
        return

    text = message.text.strip().lower()
    user_id = message.from_user.id
    chat_id = message.chat.id

    plan_type = 'home' if 'дома' in text else 'cinema' if 'кино' in text else None
    if not plan_type:
        bot.reply_to(message, "Не понял место: напиши 'дома' или 'в кино'")
        return

    day_or_date = text.replace('дома', '').replace('в кино', '').replace('кино', '').strip()
    if not day_or_date:
        day_or_date = None

    logger.info(f"[RANDOM INSTR REPLY] Планирование: {plan_type}, дата='{day_or_date}', link={link}")

    from moviebot.bot.handlers.plan import process_plan

    try:
        process_plan(bot, user_id, chat_id, link, plan_type, day_or_date)
    except Exception as e:
        logger.error(f"[RANDOM INSTR REPLY] Ошибка process_plan: {e}")
        bot.reply_to(message, "Не смог добавить в план :(")

def register_text_message_handlers(bot):
    """Регистрирует обработчики текстовых сообщений"""
    # Обработчики уже зарегистрированы через декораторы при импорте модуля
    # Эта функция нужна только для явного вызова в commands.py
    logger.info("✅ Обработчики текстовых сообщений зарегистрированы (декораторы выполнены при импорте)")

