"""
Обработчики команды /join - участие в боте
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_operations import is_bot_participant
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot as bot_instance, BOT_ID

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot_instance.message_handler(commands=['join'])
def join_command(message):
    logger.info(f"[HANDLER] /join вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        logger.info(f"Команда /join от пользователя {message.from_user.id}, chat_id={message.chat.id}")
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Проверяем, является ли пользователь уже участником
        is_participant = is_bot_participant(chat_id, user_id)
        
        if not is_participant:
            # Регистрируем пользователя
            log_request(user_id, username, '/join', chat_id)
            response_text = "✅ Вы добавлены к участию в боте!"
        else:
            response_text = "ℹ️ Вы уже участвуете в боте"
        
        # Получаем список участников группы (только для групповых чатов)
        if chat_id < 0:  # Групповой чат
            try:
                # Получаем всех участников бота из stats
                from moviebot.bot.bot_init import BOT_ID
                with db_lock:
                    cursor.execute('''
                        SELECT DISTINCT user_id, username 
                        FROM stats 
                        WHERE chat_id = %s AND user_id != %s
                        ORDER BY username
                    ''', (chat_id, BOT_ID if BOT_ID else 0))
                    bot_participants = cursor.fetchall()
                
                bot_participant_ids = set()
                bot_participants_dict = {}
                for row in bot_participants:
                    p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    p_username = row.get('username') if isinstance(row, dict) else row[1]
                    # Исключаем бота из списка участников
                    if BOT_ID and p_user_id == BOT_ID:
                        continue
                    bot_participant_ids.add(p_user_id)
                    bot_participants_dict[p_user_id] = p_username
                
                # Получаем администраторов группы (они точно есть)
                try:
                    admins = bot_instance.get_chat_administrators(chat_id)
                    all_group_member_ids = set()
                    all_group_members = {}
                    
                    for admin in admins:
                        admin_user = admin.user
                        all_group_member_ids.add(admin_user.id)
                        all_group_members[admin_user.id] = {
                            'username': admin_user.username or f"user_{admin_user.id}",
                            'first_name': admin_user.first_name or '',
                            'is_premium': getattr(admin_user, 'is_premium', False)
                        }
                    
                    # Находим недобавленных участников
                    not_added = []
                    for member_id, member_info in all_group_members.items():
                        if member_id not in bot_participant_ids:
                            not_added.append({
                                'user_id': member_id,
                                'username': member_info['username'],
                                'first_name': member_info['first_name'],
                                'is_premium': member_info['is_premium']
                            })
                    
                    # Формируем ответ
                    if not_added or bot_participants:
                        response_text += "\n\n"
                        
                        # Показываем участников бота (исключая бота)
                        if bot_participants:
                            response_text += "✅ <b>Участники бота:</b>\n"
                            for row in bot_participants:
                                p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                                p_username = row.get('username') if isinstance(row, dict) else row[1]
                                
                                # Пропускаем бота
                                if p_user_id == BOT_ID:
                                    continue
                                
                                # Проверяем, есть ли у пользователя платный доступ
                                has_premium = False
                                try:
                                    user_info = all_group_members.get(p_user_id, {})
                                    has_premium = user_info.get('is_premium', False)
                                except:
                                    pass
                                
                                premium_mark = "⭐" if has_premium else ""
                                display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                                response_text += f"• {display_name} {premium_mark}\n"
                        
                        # Показываем недобавленных участников
                        if not_added:
                            response_text += "\n❌ <b>Недобавленные участники:</b>\n"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            for member in not_added[:20]:  # Ограничиваем до 20 кнопок
                                display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                                premium_mark = "⭐" if member['is_premium'] else ""
                                button_text = f"{display_name} {premium_mark}".strip()
                                if len(button_text) > 50:
                                    button_text = button_text[:47] + "..."
                                markup.add(InlineKeyboardButton(button_text, callback_data=f"join_add:{member['user_id']}"))
                            
                            bot_instance.reply_to(message, response_text, parse_mode='HTML', reply_markup=markup)
                            return
                except Exception as e:
                    logger.warning(f"[JOIN] Не удалось получить список администраторов: {e}")
                    # Если не удалось получить администраторов, просто показываем участников бота (исключая бота)
                    if bot_participants:
                        response_text += "\n\n✅ <b>Участники бота:</b>\n"
                        for row in bot_participants:
                            p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                            p_username = row.get('username') if isinstance(row, dict) else row[1]
                            
                            # Пропускаем бота
                            if p_user_id == BOT_ID:
                                continue
                            
                            display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                            response_text += f"• {display_name}\n"
            except Exception as e:
                logger.warning(f"[JOIN] Ошибка при получении участников: {e}")
        
        bot_instance.reply_to(message, response_text, parse_mode='HTML')
        logger.info(f"✅ Команда /join обработана для пользователя {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /join: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /join")
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("join_add:"))
def join_add_callback(call):
    """Обработчик добавления участника через кнопку в /join"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        target_user_id = int(call.data.split(":")[1])
        
        # Проверяем, является ли вызывающий участником бота
        if not is_bot_participant(chat_id, user_id):
            bot_instance.answer_callback_query(call.id, "❌ Вы не участвуете в боте. Используйте /join", show_alert=True)
            return
        
        # Проверяем, является ли целевой пользователь уже участником
        if is_bot_participant(chat_id, target_user_id):
            bot_instance.answer_callback_query(call.id, "✅ Этот пользователь уже участвует в боте")
            return
        
        # Регистрируем пользователя
        username = call.from_user.username or f"user_{target_user_id}"
        log_request(target_user_id, username, '/join', chat_id)
        
        bot_instance.answer_callback_query(call.id, "✅ Пользователь добавлен к участию в боте")
        
        # Обновляем сообщение, удаляя кнопку добавленного пользователя
        try:
            # Получаем текущий текст сообщения
            message_text = call.message.text or call.message.caption or ""
            
            # Получаем список недобавленных участников
            with db_lock:
                cursor.execute('''
                    SELECT DISTINCT user_id, username 
                    FROM stats 
                    WHERE chat_id = %s
                    ORDER BY username
                ''', (chat_id,))
                bot_participants = cursor.fetchall()
            
            bot_participant_ids = set()
            for row in bot_participants:
                p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                bot_participant_ids.add(p_user_id)
            
            # Получаем администраторов группы
            try:
                admins = bot_instance.get_chat_administrators(chat_id)
                all_group_member_ids = set()
                all_group_members = {}
                
                for admin in admins:
                    admin_user = admin.user
                    all_group_member_ids.add(admin_user.id)
                    all_group_members[admin_user.id] = {
                        'username': admin_user.username or f"user_{admin_user.id}",
                        'first_name': admin_user.first_name or '',
                        'is_premium': getattr(admin_user, 'is_premium', False)
                    }
                
                # Находим недобавленных участников
                not_added = []
                for member_id, member_info in all_group_members.items():
                    if member_id not in bot_participant_ids:
                        not_added.append({
                            'user_id': member_id,
                            'username': member_info['username'],
                            'first_name': member_info['first_name'],
                            'is_premium': member_info['is_premium']
                        })
                
                # Если есть еще недобавленные участники, обновляем сообщение
                if not_added:
                    response_text = message_text.split("\n\n")[0] if "\n\n" in message_text else message_text
                    response_text += "\n\n"
                    
                    # Показываем участников бота
                    if bot_participants:
                        response_text += "✅ <b>Участники бота:</b>\n"
                        for row in bot_participants:
                            p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                            p_username = row.get('username') if isinstance(row, dict) else row[1]
                            
                            has_premium = False
                            try:
                                user_info = all_group_members.get(p_user_id, {})
                                has_premium = user_info.get('is_premium', False)
                            except:
                                pass
                            
                            premium_mark = "⭐" if has_premium else ""
                            display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                            response_text += f"• {display_name} {premium_mark}\n"
                    
                    # Показываем недобавленных участников
                    if not_added:
                        response_text += "\n❌ <b>Недобавленные участники:</b>\n"
                        
                        markup = InlineKeyboardMarkup(row_width=1)
                        for member in not_added[:20]:  # Ограничиваем до 20 кнопок
                            display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                            premium_mark = "⭐" if member['is_premium'] else ""
                            button_text = f"{display_name} {premium_mark}".strip()
                            if len(button_text) > 50:
                                button_text = button_text[:47] + "..."
                            markup.add(InlineKeyboardButton(button_text, callback_data=f"join_add:{member['user_id']}"))
                        
                        bot_instance.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    else:
                        # Все участники добавлены, удаляем кнопки
                        bot_instance.edit_message_text(response_text, chat_id, call.message.message_id, parse_mode='HTML')
                else:
                    # Все участники добавлены
                    response_text = message_text.split("\n\n")[0] if "\n\n" in message_text else message_text
                    response_text += "\n\n✅ <b>Участники бота:</b>\n"
                    for row in bot_participants:
                        p_username = row.get('username') if isinstance(row, dict) else row[1]
                        display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                        response_text += f"• {display_name}\n"
                    bot_instance.edit_message_text(response_text, chat_id, call.message.message_id, parse_mode='HTML')
            except Exception as e:
                logger.warning(f"[JOIN ADD] Не удалось обновить сообщение: {e}")
        except Exception as e:
            logger.error(f"[JOIN ADD] Ошибка при обновлении сообщения: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[JOIN ADD] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def register_join_handlers(bot):
    """Регистрирует обработчики команды /join"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики команды /join зарегистрированы")

