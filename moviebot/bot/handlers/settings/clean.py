"""
Обработчики команды /clean - очистка базы данных
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.states import user_clean_state, clean_votes, clean_unwatched_votes
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


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
    markup.add(InlineKeyboardButton("🧹 Удалить фильмы, добавленные при импорте", callback_data="clean:clean_imported_movies"))
    
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
        "<b>🧹 Удалить фильмы, добавленные при импорте</b> — удаляет фильмы, которые были добавлены в базу только из-за импорта оценок.\n"
        "• Удаляются фильмы с только импортированными оценками\n"
        "• Фильмы с обычными оценками или в планах останутся\n\n"
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
    
    user_clean_state[user_id] = {'action': action}
    
    if action == 'chat_db':
        # Обнуление базы чата - требует голосования в группах
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
                with db_lock:
                    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute('''
                        SELECT DISTINCT user_id
                        FROM stats
                        WHERE chat_id = %s AND timestamp > %s
                    ''', (chat_id, thirty_days_ago))
                    rows = cursor.fetchall()
                    active_members_from_stats = set()
                    for row in rows:
                        user_id_val = row.get('user_id') if isinstance(row, dict) else row[0]
                        active_members_from_stats.add(user_id_val)
                
                # Исключаем бота из списка активных участников
                if BOT_ID and BOT_ID in active_members_from_stats:
                    active_members_from_stats.discard(BOT_ID)
                
                # Определяем количество участников для голосования
                if chat_member_count:
                    if chat_member_count > 0:
                        chat_member_count = max(1, chat_member_count - 1)
                    if chat_member_count > len(active_members_from_stats):
                        active_members_count = chat_member_count
                        active_members = active_members_from_stats
                    else:
                        active_members_count = max(len(active_members_from_stats), 2)
                        active_members = active_members_from_stats
                else:
                    active_members_count = max(len(active_members_from_stats), 2)
                    active_members = active_members_from_stats
                
                if active_members_count < 2:
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата за последние 30 дней.\n\n"
                        f"Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено полное обнуление базы данных чата.\n\n"
                    f"Участников в чате: {active_members_count}\n"
                    f"Для подтверждения все участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                    f"Если не все проголосуют, база не будет удалена.",
                    parse_mode='HTML')
                
                from moviebot.states import clean_votes
                clean_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': active_members_count,
                    'voted': set(),
                    'active_members': active_members
                }
                
                bot.edit_message_text("✅ Запрос на обнуление базы отправлен. Ожидаю голосования всех участников.", call.message.chat.id, call.message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при инициировании голосования: {e}", exc_info=True)
                bot.edit_message_text("Ошибка при инициировании голосования.", call.message.chat.id, call.message.message_id)
        else:
            # В личном чате можно сразу удалить
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
                call.message.chat.id, call.message.message_id, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'chat'
    
    elif action == 'user_db':
        # Обнуление базы пользователя - удаляет только данные конкретного пользователя в этом чате
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
            call.message.chat.id, call.message.message_id, parse_mode='HTML'
        )
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'user'
    
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
                with db_lock:
                    thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
                    cursor.execute('''
                        SELECT DISTINCT user_id
                        FROM stats
                        WHERE chat_id = %s AND timestamp > %s
                    ''', (chat_id, thirty_days_ago))
                    rows = cursor.fetchall()
                    active_members_from_stats = set()
                    for row in rows:
                        user_id_val = row.get('user_id') if isinstance(row, dict) else row[0]
                        active_members_from_stats.add(user_id_val)
                
                # Исключаем бота
                if BOT_ID and BOT_ID in active_members_from_stats:
                    active_members_from_stats.discard(BOT_ID)
                
                # Определяем количество участников для голосования
                if chat_member_count:
                    if chat_member_count > 0:
                        chat_member_count = max(1, chat_member_count - 1)
                    if chat_member_count > len(active_members_from_stats):
                        active_members_count = chat_member_count
                        active_members = active_members_from_stats
                    else:
                        active_members_count = max(len(active_members_from_stats), 2)
                        active_members = active_members_from_stats
                else:
                    active_members_count = max(len(active_members_from_stats), 2)
                    active_members = active_members_from_stats
                
                if active_members_count < 2:
                    error_msg = (
                        f"⚠️ Не найдено активных участников чата за последние 30 дней.\n\n"
                        f"Используйте /dbcheck для подробной диагностики БД"
                    )
                    bot.edit_message_text(error_msg, call.message.chat.id, call.message.message_id)
                    return
                
                msg = bot.send_message(chat_id, 
                    f"⚠️ <b>ВНИМАНИЕ!</b> Запрошено удаление всех непросмотренных фильмов.\n\n"
                    f"Участников в чате: {active_members_count}\n"
                    f"Для подтверждения все участники должны поставить 👍 (лайк) на это сообщение.\n\n"
                    f"Если не все проголосуют, фильмы не будут удалены.",
                    parse_mode='HTML')
                
                from moviebot.states import clean_unwatched_votes
                clean_unwatched_votes[msg.message_id] = {
                    'chat_id': chat_id,
                    'members_count': active_members_count,
                    'voted': set(),
                    'active_members': active_members
                }
                
                bot.edit_message_text("✅ Запрос на удаление непросмотренных фильмов отправлен. Ожидаю голосования всех участников.", call.message.chat.id, call.message.message_id)
            except Exception as e:
                logger.error(f"Ошибка при инициировании голосования: {e}", exc_info=True)
                bot.edit_message_text("Ошибка при инициировании голосования.", call.message.chat.id, call.message.message_id)
        else:
            # В личном чате можно сразу удалить
            bot.edit_message_text(
                "⚠️ <b>Удаление непросмотренных фильмов</b>\n\n"
                "Это удалит все фильмы, которые:\n"
                "• Не находятся в расписании\n"
                "• У которых нет билетов\n"
                "• Которые не участвуют ни в каких активностях\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, parse_mode='HTML'
            )
            user_clean_state[user_id]['confirm_needed'] = True
            user_clean_state[user_id]['target'] = 'unwatched_movies'
    
    elif action == 'imported_ratings':
        # Удаление импортированных оценок пользователя
        try:
            sent_msg = bot.edit_message_text(
                "⚠️ <b>Удаление импортированных оценок с Кинопоиска</b>\n\n"
                "Это удалит <b>только ваши импортированные оценки</b>:\n"
                "• Все оценки с пометкой is_imported = TRUE\n"
                "• Ваши обычные оценки останутся без изменений\n"
                "• Данные других пользователей останутся без изменений\n\n"
                "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
                call.message.chat.id, call.message.message_id, parse_mode='HTML'
            )
            # edit_message_text возвращает True/False, а не объект сообщения
            prompt_message_id = call.message.message_id
        except Exception as e:
            logger.error(f"[CLEAN] Ошибка при редактировании сообщения: {e}")
            prompt_message_id = call.message.message_id
        
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'imported_ratings'
        
        # Для личных чатов сохраняем состояние ожидания следующего сообщения
        if call.message.chat.type == 'private':
            from moviebot.states import user_private_handler_state
            user_private_handler_state[user_id] = {
                'handler': 'clean_imported_ratings',
                'prompt_message_id': prompt_message_id
            }
            logger.info(f"[CLEAN] Сохранено состояние для личного чата: user_id={user_id}, prompt_message_id={prompt_message_id}")
    
    elif action == 'clean_imported_movies':
        # Удаление фильмов, которые были добавлены только из-за импорта
        bot.edit_message_text(
            "⚠️ <b>Удаление фильмов, добавленных при импорте</b>\n\n"
            "Это удалит фильмы, которые:\n"
            "• Были добавлены в базу только из-за импорта оценок\n"
            "• Имеют только импортированные оценки (is_imported = TRUE)\n"
            "• Не имеют обычных оценок (is_imported = FALSE или NULL)\n"
            "• Не находятся в планах\n"
            "• Не просмотрены (watched = 0)\n\n"
            "Отправьте 'ДА, УДАЛИТЬ' для подтверждения.",
            call.message.chat.id, call.message.message_id, parse_mode='HTML'
        )
        user_clean_state[user_id]['confirm_needed'] = True
        user_clean_state[user_id]['target'] = 'clean_imported_movies'
    
    elif action == 'cancel':
        bot.edit_message_text("❌ Операция отменена.", call.message.chat.id, call.message.message_id)
        if user_id in user_clean_state:
            del user_clean_state[user_id]


def register_clean_handlers(bot):
    """Регистрирует обработчики команды /clean"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики команды /clean зарегистрированы")

