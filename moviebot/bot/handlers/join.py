"""
Обработчики команды /join - участие в боте
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_operations import is_bot_participant
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot as bot_instance

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
                with db_lock:
                    cursor.execute('''
                        SELECT DISTINCT user_id, username 
                        FROM stats 
                        WHERE chat_id = %s
                        ORDER BY username
                    ''', (chat_id,))
                    bot_participants = cursor.fetchall()
                
                bot_participant_ids = set()
                bot_participants_dict = {}
                for row in bot_participants:
                    p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    p_username = row.get('username') if isinstance(row, dict) else row[1]
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
                        
                        # Показываем участников бота
                        if bot_participants:
                            response_text += "✅ <b>Участники бота:</b>\n"
                            for row in bot_participants:
                                p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                                p_username = row.get('username') if isinstance(row, dict) else row[1]
                                
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
                    # Если не удалось получить администраторов, просто показываем участников бота
                    if bot_participants:
                        response_text += "\n\n✅ <b>Участники бота:</b>\n"
                        for row in bot_participants:
                            p_username = row.get('username') if isinstance(row, dict) else row[1]
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


def register_join_handlers(bot):
    """Регистрирует обработчики команды /join"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики команды /join зарегистрированы")

