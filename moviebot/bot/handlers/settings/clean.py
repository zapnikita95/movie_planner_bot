from moviebot.bot.bot_init import bot, BOT_ID
"""
Обработчики команды /clean - очистка базы данных
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.states import user_clean_state, user_private_handler_state, clean_unwatched_votes

from datetime import datetime, timedelta


logger = logging.getLogger(__name__)

# Новое состояние для отслеживания голосований через текстовые сообщения для chat_db
clean_chat_text_votes = {}  # message_id: {'chat_id': int, 'members_count': int, 'voted': set(), 'active_members': set()}


@bot.message_handler(commands=['clean'])
def clean_command(message):
    logger.info(f"[HANDLER] /clean вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/clean', message.chat.id)
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    # Показываем меню только с опциями массового удаления
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("💥 Обнулить базу чата", callback_data="clean:chat_db"))
    markup.add(InlineKeyboardButton("👤 Обнулить базу пользователя", callback_data="clean:user_db"))
    markup.add(InlineKeyboardButton("🗑️ Удалить все непросмотренные фильмы", callback_data="clean:unwatched_movies"))
    markup.add(InlineKeyboardButton("📥 Удалить импорты с Кинопоиска", callback_data="clean:imported_ratings"))
    markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
    
    help_text = (
        "🧹 <b>Массовое удаление данных</b>\n\n"
        "<b>💥 Обнулить базу чата</b> — удаляет <b>ВСЕ данные чата</b>:\n"
        "• Все фильмы\n"
        "• Все оценки всех пользователей\n"
        "• Все планы и расписание всех пользователей\n"
        "• Все билеты\n"
        "• Все настройки\n\n"
        "<b>👤 Обнулить базу пользователя</b> — удаляет <b>только ваши данные в этом чате</b>:\n"
        "• Ваши оценки\n"
        "• Ваши планы и расписание\n"
        "• Ваши билеты\n"
        "• Ваша статистика\n"
        "• Ваши настройки (включая часовой пояс)\n\n"
        "<b>🗑️ Удалить все непросмотренные фильмы</b> — удаляет фильмы, которые:\n"
        "• Не находятся в расписании\n"
        "• У которых нет билетов\n"
        "• Которые не участвуют ни в каких активностях\n\n"
        "<b>📥 Удалить импорты с Кинопоиска</b> — удаляет все ваши импортированные оценки из Кинопоиска.\n"
        "• Удаляются только импортированные оценки (is_imported = TRUE)\n"
        "• Ваши обычные оценки и данные других пользователей останутся без изменений\n\n"
        "<i>Фильмы и данные других пользователей останутся без изменений.</i>\n\n"
        "Выберите действие:"
    )
    bot.reply_to(message, help_text, reply_markup=markup, parse_mode='HTML')


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("clean:"))
def clean_action_choice(call):
    """Обработчик выбора действия в /clean"""
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    action = call.data.split(":")[1]
    
    bot.answer_callback_query(call.id)
    
    user_clean_state[user_id] = {'action': action}
    
    if action == 'chat_db':
        # Обнуление базы чата - требует подтверждения всех активных участников через "ДА, УДАЛИТЬ"
        if call.message.chat.type in ['group', 'supergroup']:
            try:
                # Получаем список активных участников
                try:
                    chat_member_count = bot.get_chat_member_count(chat_id)
                    logger.info(f"[CLEAN] Количество участников чата через API: {chat_member_count}")
                except Exception as api_error:
                    logger.warning(f"[CLEAN] Не удалось получить количество участников через API: {api_error}")
                    chat_member_count = None
                
                # Получаем список активных участников из stats (за последние 30 дней)
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                        cursor_local.execute('''
                            SELECT DISTINCT user_id
                            FROM stats
                            WHERE chat_id = %s AND timestamp > %s
                        ''', (chat_id, thirty_days_ago))
                        rows = cursor_local.fetchall()
                        active_members_from_stats = set()
                        for row in rows:
                            user_id_val = row.get('user_id') if isinstance(row, dict) else row[0]
                            active_members_from_stats.add(user_id_val)
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
                
                # Исключаем бота из списка активных участников
                if BOT_ID and BOT_ID in active_members_from_stats:
                    active_members_from_stats.discard(BOT_ID)
                
                # ВАЖНО: Если active_members_from_stats пусто, добавляем инициатора
                if not active_members_from_stats:
                    active_members_from_stats.add(user_id)
                    logger.info(f"[CLEAN] active_members_from_stats пусто, добавлен инициатор user_id={user_id}")
                
                # Определяем количество участников для голосования
                if chat_member_count:
                    if chat_member_count > 0:
                        chat_member_count = max(1, chat_member_count - 1)
                    if chat_member_count > len(active_members_from_stats):
                        active_members_count = chat_member_count
                        active_members = active_members_from_stats
                    else:
                        active_members_count = max(len(active_members_from_stats), 1)
                        active_members = active_members_from_stats
                else:
                    active_members_count = max(len(active_members_from_stats), 1)
                    active_members = active_members_from_stats
                
                # Если active_members пусто, добавляем хотя бы инициатора
                if not active_members:
                    active_members.add(user_id)
                    active_members_count = 1
                
                if active_members_count < 1:
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата.\n\n"
                        f"Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                logger.info(f"[CLEAN] Определены участники для голосования (chat_db): active_members_count={active_members_count}, active_members={active_members}, chat_member_count={chat_member_count}")
                
                # Создаем inline кнопки для каждого участника
                markup = InlineKeyboardMarkup(row_width=1)
                for member_id in active_members:
                    # Получаем имя пользователя
                    try:
                        member_info = bot.get_chat_member(chat_id, member_id)
                        member_name = member_info.user.first_name or f"user_{member_id}"
                    except:
                        member_name = f"user_{member_id}"
                    markup.add(InlineKeyboardButton(f"✅ {member_name}", callback_data=f"clean_vote:chat:{member_id}"))
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                    f"Участников в чате: {active_members_count}\n"
                    f"Для подтверждения все активные участники должны нажать кнопку со своим именем ниже.\n\n"
                    f"Если не все участники подтвердят, база не будет удалена.",
                    parse_mode='HTML',
                    reply_markup=markup)
                
                clean_chat_text_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': active_members_count,
                    'voted': set(),
                    'active_members': active_members,
                    'action': 'chat'  # Явно указываем action для chat_db
                }
                
                logger.info(f"[CLEAN] Создано голосование для chat_db: message_id={msg.message_id}, chat_id={chat_id}, members_count={active_members_count}, active_members={active_members}")
                
                logger.info(f"[CLEAN] Создано голосование для chat_db: message_id={msg.message_id}, chat_id={chat_id}, members_count={active_members_count}, active_members={active_members}")
                
                bot.edit_message_text("✅ Запрос на обнуление базы отправлен. Ожидаю подтверждения всех активных участников.", call.message.chat.id, call.message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при инициировании голосования: {e}", exc_info=True)
                bot.edit_message_text("Ошибка при инициировании голосования.", call.message.chat.id, call.message.message_id)
        else:
            # В личном чате можно сразу удалить
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
            bot.edit_message_text(
                "⚠️ <b>Обнуление базы данных чата</b>\n\n"
                "Это удалит <b>ВСЕ данные чата</b>:\n"
                "• Все фильмы\n"
                "• Все оценки\n"
                "• Все планы и расписание\n"
                "• Все билеты\n"
                "• Все настройки\n\n"
                "Это действие необратимо!\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'chat'
            user_clean_state[user_id]['prompt_message_id'] = call.message.message_id
            
            # Устанавливаем user_private_handler_state для личных чатов
            user_private_handler_state[user_id] = {
                'handler': 'clean_chat',
                'prompt_message_id': call.message.message_id
            }
    
    elif action == 'user_db':
        # Обнуление базы пользователя - удаляет только данные конкретного пользователя в этом чате
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
        bot.edit_message_text(
            "⚠️ <b>Обнуление базы данных пользователя</b>\n\n"
            "Это удалит <b>только ваши данные в этом чате</b>:\n"
            "• Все ваши оценки\n"
            "• Все ваши планы и расписание\n"
            "• Все ваши билеты\n"
            "• Вашу статистику\n"
            "• Ваши настройки (включая часовой пояс)\n\n"
            "<i>Фильмы и данные других пользователей останутся без изменений.</i>\n\n"
            "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
            call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'user'
        user_clean_state[user_id]['prompt_message_id'] = call.message.message_id
        
        # Устанавливаем user_private_handler_state для личных чатов
        if call.message.chat.type == 'private':
            user_private_handler_state[user_id] = {
                'handler': 'clean_user',
                'prompt_message_id': call.message.message_id
            }
    
    elif action == 'unwatched_movies':
        # Удаление непросмотренных фильмов - требует голосования в группах
        if call.message.chat.type in ['group', 'supergroup']:
            try:
                # Получаем количество участников и активных участников (та же логика, что и для chat_db)
                try:
                    chat_member_count = bot.get_chat_member_count(chat_id)
                except Exception as api_error:
                    chat_member_count = None
                
                # Получаем список активных участников из stats (за последние 30 дней)
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                        cursor_local.execute('''
                            SELECT DISTINCT user_id
                            FROM stats
                            WHERE chat_id = %s AND timestamp > %s
                        ''', (chat_id, thirty_days_ago))
                        rows = cursor_local.fetchall()
                        active_members_from_stats = set()
                        for row in rows:
                            user_id_val = row.get('user_id') if isinstance(row, dict) else row[0]
                            active_members_from_stats.add(user_id_val)
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
                
                # Исключаем бота
                if BOT_ID and BOT_ID in active_members_from_stats:
                    active_members_from_stats.discard(BOT_ID)
                
                # ВАЖНО: Если active_members_from_stats пусто, добавляем инициатора
                if not active_members_from_stats:
                    active_members_from_stats.add(user_id)
                    logger.info(f"[CLEAN] active_members_from_stats пусто (unwatched_movies), добавлен инициатор user_id={user_id}")
                
                # Определяем количество участников для голосования
                if chat_member_count:
                    if chat_member_count > 0:
                        chat_member_count = max(1, chat_member_count - 1)
                    if chat_member_count > len(active_members_from_stats):
                        active_members_count = chat_member_count
                        active_members = active_members_from_stats
                    else:
                        active_members_count = max(len(active_members_from_stats), 1)
                        active_members = active_members_from_stats
                else:
                    active_members_count = max(len(active_members_from_stats), 1)
                    active_members = active_members_from_stats
                
                # Если active_members пусто, добавляем хотя бы инициатора
                if not active_members:
                    active_members.add(user_id)
                    active_members_count = 1
                
                if active_members_count < 1:
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата.\n\n"
                        f"Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                logger.info(f"[CLEAN] Определены участники для голосования (unwatched_movies): active_members_count={active_members_count}, active_members={active_members}, chat_member_count={chat_member_count}")
                
                # Создаем inline кнопки для каждого участника
                markup = InlineKeyboardMarkup(row_width=1)
                for member_id in active_members:
                    # Получаем имя пользователя
                    try:
                        member_info = bot.get_chat_member(chat_id, member_id)
                        member_name = member_info.user.first_name or f"user_{member_id}"
                    except:
                        member_name = f"user_{member_id}"
                    markup.add(InlineKeyboardButton(f"✅ {member_name}", callback_data=f"clean_vote:unwatched:{member_id}"))
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено удаление всех непросмотренных фильмов.\n\n"
                    f"Участников в чате: {active_members_count}\n"
                    f"Для подтверждения все активные участники должны нажать кнопку со своим именем ниже.\n\n"
                    f"Если не все участники подтвердят, фильмы не будут удалены.",
                    parse_mode='HTML',
                    reply_markup=markup)
                
                clean_chat_text_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': active_members_count,
                    'voted': set(),
                    'active_members': active_members,
                    'action': 'unwatched_movies'
                }
                
                logger.info(f"[CLEAN] Создано голосование для unwatched_movies: message_id={msg.message_id}, chat_id={chat_id}, members_count={active_members_count}, active_members={active_members}")
                
                bot.edit_message_text("✅ Запрос на удаление непросмотренных фильмов отправлен. Ожидаю голосования всех участников.", call.message.chat.id, call.message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при инициировании голосования: {e}", exc_info=True)
                bot.edit_message_text("Ошибка при инициировании голосования.", call.message.chat.id, call.message.message_id)
        else:
            # В личном чате можно сразу удалить
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
            bot.edit_message_text(
                "⚠️ <b>Удаление непросмотренных фильмов</b>\n\n"
                "Это удалит все фильмы, которые:\n"
                "• Не находятся в расписании\n"
                "• У которых нет билетов\n"
                "• Которые не участвуют ни в каких активностях\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'unwatched_movies'
            user_clean_state[user_id]['prompt_message_id'] = call.message.message_id
    
    elif action == 'imported_ratings':
        # Удаление импортированных оценок пользователя
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
        try:
            sent_msg = bot.edit_message_text(
                "⚠️ <b>Удаление импортированных оценок с Кинопоиска</b>\n\n"
                "Это удалит <b>только ваши импортированные оценки</b>:\n"
                "• Все оценки с пометкой is_imported = TRUE\n"
                "• Ваши обычные оценки останутся без изменений\n"
                "• Данные других пользователей останутся без изменений\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
            )
            prompt_message_id = call.message.message_id
        except Exception as e:
            logger.error(f"[CLEAN] Ошибка при редактировании сообщения: {e}")
            prompt_message_id = call.message.message_id
        
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'imported_ratings'
        user_clean_state[user_id]['prompt_message_id'] = prompt_message_id
        logger.info(f"[CLEAN] Сохранено состояние для imported_ratings: user_id={user_id}, prompt_message_id={prompt_message_id}")
        
        # Устанавливаем user_private_handler_state для личных чатов
        if call.message.chat.type == 'private':
            user_private_handler_state[user_id] = {
                'handler': 'clean_imported_ratings',
                'prompt_message_id': prompt_message_id
            }
    
    elif action == 'cancel':
        bot.edit_message_text("❌ Операция отменена.", call.message.chat.id, call.message.message_id)
        if user_id in user_clean_state:
            del user_clean_state[user_id]


@bot.callback_query_handler(func=lambda call: call.data and call.data == "clean:back")
def clean_back_callback(call):
    """Обработчик возврата к меню очистки из настроек"""
    logger.info(f"[CLEAN BACK] ===== START: callback_id={call.id}, user_id={call.from_user.id}")
    try:
        from moviebot.bot.bot_init import safe_answer_callback_query
        safe_answer_callback_query(bot, call.id)
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Очищаем состояние
        if user_id in user_clean_state:
            del user_clean_state[user_id]
        if user_id in user_private_handler_state:
            del user_private_handler_state[user_id]
        
        # Показываем меню очистки
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("💥 Обнулить базу чата", callback_data="clean:chat_db"))
        markup.add(InlineKeyboardButton("👤 Обнулить базу пользователя", callback_data="clean:user_db"))
        markup.add(InlineKeyboardButton("🗑️ Удалить все непросмотренные фильмы", callback_data="clean:unwatched_movies"))
        markup.add(InlineKeyboardButton("📥 Удалить импорты с Кинопоиска", callback_data="clean:imported_ratings"))
        markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
        
        help_text = (
            "🧹 <b>Массовое удаление данных</b>\n\n"
            "<b>💥 Обнулить базу чата</b> — удаляет <b>ВСЕ данные чата</b>:\n"
            "• Все фильмы\n"
            "• Все оценки всех пользователей\n"
            "• Все планы и расписание всех пользователей\n"
            "• Все билеты\n"
            "• Все настройки\n\n"
            "<b>👤 Обнулить базу пользователя</b> — удаляет <b>только ваши данные в этом чате</b>:\n"
            "• Ваши оценки\n"
            "• Ваши планы и расписание\n"
            "• Ваши билеты\n"
            "• Ваша статистика\n"
            "• Ваши настройки (включая часовой пояс)\n\n"
            "<b>🗑️ Удалить все непросмотренные фильмы</b> — удаляет фильмы, которые:\n"
            "• Не находятся в расписании\n"
            "• У которых нет билетов\n"
            "• Которые не участвуют ни в каких активностях\n\n"
            "<b>📥 Удалить импорты с Кинопоиска</b> — удаляет все ваши импортированные оценки из Кинопоиска.\n"
            "• Удаляются только импортированные оценки (is_imported = TRUE)\n"
            "• Ваши обычные оценки и данные других пользователей останутся без изменений\n\n"
            "<i>Фильмы и данные других пользователей останутся без изменений.</i>\n\n"
            "Выберите действие:"
        )
        try:
            bot.edit_message_text(help_text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as edit_e:
            logger.error(f"[CLEAN BACK] Ошибка редактирования сообщения: {edit_e}", exc_info=True)
            try:
                bot.send_message(chat_id, help_text, reply_markup=markup, parse_mode='HTML')
            except Exception as send_e:
                logger.error(f"[CLEAN BACK] Ошибка отправки сообщения: {send_e}", exc_info=True)
    except Exception as e:
        logger.error(f"[CLEAN BACK] Ошибка: {e}", exc_info=True)
        try:
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def check_clean_reply(message):
    """Проверка для handler ответа на сообщение об очистке базы"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip().upper() if message.text else ""
    
    # Нормализуем текст: убираем пробелы, запятые, приводим к верхнему регистру
    normalized_text = text.replace(' ', '').replace(',', '').replace('.', '').upper()
    if normalized_text != 'ДАУДАЛИТЬ':
        return False
    
    is_private = message.chat.type == 'private'
    
    # Для личных чатов проверяем user_private_handler_state
    if is_private:
        if user_id not in user_private_handler_state:
            return False
        state = user_private_handler_state[user_id]
        handler_name = state.get('handler')
        if handler_name in ['clean_chat', 'clean_user', 'clean_imported_ratings']:
            logger.info(f"[CHECK CLEAN REPLY] ✅ Личный чат: handler={handler_name}, user_id={user_id}")
            return True
        return False
    
    # Для групп:
    # 1. Проверяем user_clean_state для user_db и imported_ratings
    if user_id in user_clean_state:
        state = user_clean_state[user_id]
        target = state.get('target')
        if target in ['user', 'imported_ratings']:
            # Для групп нужен реплай
            if not message.reply_to_message:
                logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: нет reply_to_message для target={target}, user_id={user_id}")
                return False
            if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
                logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: reply не от бота для target={target}, user_id={user_id}")
                return False
            reply_text = message.reply_to_message.text or ""
            if target == 'user' and "Обнуление базы данных пользователя" not in reply_text:
                logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: текст reply не соответствует для target={target}, user_id={user_id}")
                return False
            if target == 'imported_ratings' and "Удаление импортированных оценок с Кинопоиска" not in reply_text:
                logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: текст reply не соответствует для target={target}, user_id={user_id}")
                return False
            logger.info(f"[CHECK CLEAN REPLY] ✅ Группа: target={target}, user_id={user_id}")
            return True
    
    # 2. Проверяем clean_chat_text_votes для chat_db и unwatched_movies
    # ВАЖНО: Проверяем ВСЕ сообщения в clean_chat_text_votes для этого чата, не только reply
    logger.info(f"[CHECK CLEAN REPLY] Проверка clean_chat_text_votes: всего голосований={len(clean_chat_text_votes)}, chat_id={chat_id}, user_id={user_id}")
    for reply_msg_id, vote_state in clean_chat_text_votes.items():
        logger.info(f"[CHECK CLEAN REPLY] Проверка голосования: reply_msg_id={reply_msg_id}, vote_chat_id={vote_state['chat_id']}, user_in_active={user_id in vote_state['active_members']}, active_members={vote_state['active_members']}")
        if vote_state['chat_id'] == chat_id:
            if user_id in vote_state['active_members']:
                # Если есть reply, проверяем, что это правильное сообщение
                if message.reply_to_message:
                    if message.reply_to_message.message_id == reply_msg_id:
                        logger.info(f"[CHECK CLEAN REPLY] ✅ Группа: найдено голосование reply_msg_id={reply_msg_id}, user_id={user_id}")
                        return True
                    else:
                        logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: reply_msg_id не совпадает: reply={message.reply_to_message.message_id}, ожидалось={reply_msg_id}")
                else:
                    # Если нет reply, но есть активное голосование в этом чате, тоже принимаем
                    # Это позволяет обрабатывать сообщения без reply, если они в правильном чате
                    logger.info(f"[CHECK CLEAN REPLY] ✅ Группа: найдено голосование без reply reply_msg_id={reply_msg_id}, user_id={user_id}")
                    return True
            else:
                logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: user_id={user_id} не в active_members={vote_state['active_members']}")
    
    logger.info(f"[CHECK CLEAN REPLY] ❌ Группа: не найдено подходящего голосования для user_id={user_id}, chat_id={chat_id}")
    return False


@bot.message_handler(func=check_clean_reply)
def handle_clean_reply(message):
    """Обработчик ответа на сообщение об очистке базы - ТОЛЬКО для 'ДА, УДАЛИТЬ'"""
    logger.info(f"[CLEAN REPLY] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, text='{message.text[:50] if message.text else ''}'")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip().upper() if message.text else ""
        
        # Нормализуем текст: убираем пробелы, запятые, приводим к верхнему регистру
        normalized_text = text.replace(' ', '').replace(',', '').upper()
        if normalized_text != 'ДАУДАЛИТЬ':
            logger.warning(f"[CLEAN REPLY] Неверный текст подтверждения: '{text}' (нормализовано: '{normalized_text}')")
            return
        
        is_private = message.chat.type == 'private'
        
        # Для личных чатов
        if is_private:
            if user_id not in user_private_handler_state:
                logger.warning(f"[CLEAN REPLY] Пользователь {user_id} не в состоянии user_private_handler_state")
                return
            
            state = user_private_handler_state[user_id]
            handler_name = state.get('handler')
            
            if handler_name == 'clean_chat':
                # Удаляем состояние и вызываем handle_clean_confirm_internal
                del user_private_handler_state[user_id]
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
                logger.info(f"[CLEAN REPLY] ✅ Завершено clean_chat для личного чата")
                return
            
            elif handler_name == 'clean_user':
                # Удаляем состояние и вызываем handle_clean_confirm_internal
                del user_private_handler_state[user_id]
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
                logger.info(f"[CLEAN REPLY] ✅ Завершено clean_user для личного чата")
                return
            
            elif handler_name == 'clean_imported_ratings':
                # Удаляем состояние и вызываем handle_clean_confirm_internal
                del user_private_handler_state[user_id]
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
                logger.info(f"[CLEAN REPLY] ✅ Завершено clean_imported_ratings для личного чата")
                return
        
        # Для групп
        # 1. Проверяем user_clean_state для user_db и imported_ratings
        if user_id in user_clean_state:
            state = user_clean_state[user_id]
            target = state.get('target')
            
            if target in ['user', 'imported_ratings']:
                # Проверяем реплай
                if not message.reply_to_message:
                    return
                if not message.reply_to_message.from_user or message.reply_to_message.from_user.id != BOT_ID:
                    return
                reply_text = message.reply_to_message.text or ""
                if target == 'user' and "Обнуление базы данных пользователя" not in reply_text:
                    return
                if target == 'imported_ratings' and "Удаление импортированных оценок с Кинопоиска" not in reply_text:
                    return
                
                # Вызываем handle_clean_confirm_internal
                from moviebot.bot.handlers.series import handle_clean_confirm_internal
                handle_clean_confirm_internal(message)
                logger.info(f"[CLEAN REPLY] ✅ Завершено {target} для группы")
                return
        
        # 2. Обрабатываем голосование для chat_db и unwatched_movies
        # Проверяем ВСЕ активные голосования в этом чате
        found_vote = False
        reply_msg_id = None
        
        if message.reply_to_message:
            reply_msg_id = message.reply_to_message.message_id
            if reply_msg_id in clean_chat_text_votes:
                vote_state = clean_chat_text_votes[reply_msg_id]
                if vote_state['chat_id'] == chat_id and user_id in vote_state['active_members']:
                    found_vote = True
        else:
            # Если нет reply, ищем активное голосование в этом чате
            for msg_id, vote_state in clean_chat_text_votes.items():
                if vote_state['chat_id'] == chat_id and user_id in vote_state['active_members']:
                    reply_msg_id = msg_id
                    found_vote = True
                    break
        
        if found_vote and reply_msg_id:
            vote_state = clean_chat_text_votes[reply_msg_id]
            # Добавляем пользователя в список проголосовавших
            if user_id not in vote_state['voted']:
                vote_state['voted'].add(user_id)
                action = vote_state.get('action', 'chat')
                logger.info(f"[CLEAN REPLY] Пользователь {user_id} проголосовал за {action}. Проголосовало: {len(vote_state['voted'])}/{vote_state['members_count']}")
                
                # Проверяем, все ли проголосовали
                if len(vote_state['voted']) >= vote_state['members_count']:
                    # Все проголосовали - выполняем удаление
                    logger.info(f"[CLEAN REPLY] Все участники проголосовали, выполняем удаление для {action}")
                    
                    # Создаем FakeMessage для handle_clean_confirm_internal
                    class FakeMessage:
                        def __init__(self, chat_id, user_id):
                            self.chat = type('obj', (object,), {'id': chat_id})()
                            class User:
                                def __init__(self, user_id):
                                    self.id = user_id
                            self.from_user = User(user_id)
                    
                    fake_msg = FakeMessage(chat_id, user_id)
                    
                    # Устанавливаем target в зависимости от action
                    target = 'chat' if action == 'chat' else 'unwatched_movies'
                    user_clean_state[user_id] = {'target': target, 'confirm_needed': True}
                    
                    # Вызываем handle_clean_confirm_internal
                    from moviebot.bot.handlers.series import handle_clean_confirm_internal
                    handle_clean_confirm_internal(fake_msg)
                    
                    # Удаляем состояние голосования
                    del clean_chat_text_votes[reply_msg_id]
                    
                    # Отправляем сообщение об успехе
                    if action == 'chat':
                        bot.send_message(chat_id, "✅ Все участники подтвердили. База данных чата обнулена.")
                        logger.info(f"[CLEAN REPLY] ✅ База данных чата обнулена")
                    else:
                        bot.send_message(chat_id, "✅ Все участники подтвердили. Непросмотренные фильмы удалены.")
                        logger.info(f"[CLEAN REPLY] ✅ Непросмотренные фильмы удалены")
                else:
                    # Еще не все проголосовали
                    remaining = vote_state['members_count'] - len(vote_state['voted'])
                    if message.reply_to_message:
                        bot.reply_to(message, f"✅ Ваш голос учтен. Осталось подтверждений: {remaining}")
                    else:
                        bot.send_message(chat_id, f"✅ Ваш голос учтен, {message.from_user.first_name}. Осталось подтверждений: {remaining}")
            else:
                if message.reply_to_message:
                    bot.reply_to(message, "✅ Вы уже проголосовали.")
                else:
                    bot.send_message(chat_id, f"✅ Вы уже проголосовали, {message.from_user.first_name}.")
            return
        
        logger.warning(f"[CLEAN REPLY] Не найдено соответствующего состояния для user_id={user_id}, chat_id={chat_id}")
    except Exception as e:
        logger.error(f"[CLEAN REPLY] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке")
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("clean_vote:"))
def clean_vote_callback(call):
    """Обработчик inline кнопок для голосования за удаление"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        # Парсим callback_data: clean_vote:action:member_id
        parts = call.data.split(":")
        if len(parts) < 3:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            return
        
        action = parts[1]  # 'chat' или 'unwatched'
        expected_member_id = int(parts[2])
        
        # Проверяем, что пользователь голосует за себя
        if user_id != expected_member_id:
            bot.answer_callback_query(call.id, "❌ Это не ваша кнопка", show_alert=True)
            return
        
        # Проверяем, есть ли голосование для этого сообщения
        if message_id not in clean_chat_text_votes:
            bot.answer_callback_query(call.id, "❌ Голосование не найдено", show_alert=True)
            return
        
        vote_state = clean_chat_text_votes[message_id]
        
        # Проверяем, что это правильный чат и пользователь в списке активных
        if vote_state['chat_id'] != chat_id:
            bot.answer_callback_query(call.id, "❌ Неверный чат", show_alert=True)
            return
        
        if user_id not in vote_state['active_members']:
            bot.answer_callback_query(call.id, "❌ Вы не в списке участников", show_alert=True)
            return
        
        # Проверяем, не голосовал ли уже
        if user_id in vote_state['voted']:
            bot.answer_callback_query(call.id, "✅ Вы уже проголосовали", show_alert=True)
            return
        
        # Добавляем голос
        vote_state['voted'].add(user_id)
        logger.info(f"[CLEAN VOTE] Пользователь {user_id} проголосовал за {action}. Проголосовало: {len(vote_state['voted'])}/{vote_state['members_count']}")
        
        # Обновляем кнопки - помечаем проголосовавших
        markup = InlineKeyboardMarkup(row_width=1)
        for member_id in vote_state['active_members']:
            try:
                member_info = bot.get_chat_member(chat_id, member_id)
                member_name = member_info.user.first_name or f"user_{member_id}"
            except:
                member_name = f"user_{member_id}"
            
            if member_id in vote_state['voted']:
                markup.add(InlineKeyboardButton(f"✅ {member_name} ✓", callback_data=f"clean_vote:{action}:{member_id}"))
            else:
                markup.add(InlineKeyboardButton(f"⏳ {member_name}", callback_data=f"clean_vote:{action}:{member_id}"))
        
        try:
            bot.edit_message_reply_markup(chat_id, message_id, reply_markup=markup)
        except Exception as e:
            logger.error(f"[CLEAN VOTE] Ошибка обновления кнопок: {e}", exc_info=True)
        
        # Проверяем, все ли проголосовали
        if len(vote_state['voted']) >= vote_state['members_count']:
            # Все проголосовали - выполняем удаление
            logger.info(f"[CLEAN VOTE] Все участники проголосовали, выполняем удаление для {action}")
            
            # Создаем FakeMessage для handle_clean_confirm_internal
            class FakeMessage:
                def __init__(self, chat_id, user_id):
                    self.chat = type('obj', (object,), {'id': chat_id})()
                    class User:
                        def __init__(self, user_id):
                            self.id = user_id
                    self.from_user = User(user_id)
            
            fake_msg = FakeMessage(chat_id, user_id)
            
            # Устанавливаем target в зависимости от action
            target = 'chat' if action == 'chat' else 'unwatched_movies'
            user_clean_state[user_id] = {'target': target, 'confirm_needed': True}
            
            # Вызываем handle_clean_confirm_internal
            from moviebot.bot.handlers.series import handle_clean_confirm_internal
            handle_clean_confirm_internal(fake_msg)
            
            # Удаляем состояние голосования
            del clean_chat_text_votes[message_id]
            
            # Отправляем сообщение об успехе
            if action == 'chat':
                bot.send_message(chat_id, "✅ Все участники подтвердили. База данных чата обнулена.")
                logger.info(f"[CLEAN VOTE] ✅ База данных чата обнулена")
            else:
                bot.send_message(chat_id, "✅ Все участники подтвердили. Непросмотренные фильмы удалены.")
                logger.info(f"[CLEAN VOTE] ✅ Непросмотренные фильмы удалены")
        else:
            # Еще не все проголосовали
            remaining = vote_state['members_count'] - len(vote_state['voted'])
            bot.answer_callback_query(call.id, f"✅ Ваш голос учтен. Осталось: {remaining}", show_alert=False)
    except Exception as e:
        logger.error(f"[CLEAN VOTE] ❌ Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

def register_clean_handlers(bot):
    """Регистрирует обработчики команды /clean"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики команды /clean зарегистрированы")
