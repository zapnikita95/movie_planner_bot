from moviebot.bot.bot_init import bot
"""
Обработчики команды /join - участие в боте
"""
import logging
from datetime import datetime
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.database.db_operations import log_request

from moviebot.database.db_operations import is_bot_participant

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot.message_handler(commands=['join'])
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
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                # Получаем всех участников бота из stats
                from moviebot.bot.bot_init import BOT_ID
                with db_lock:
                    cursor_local.execute('''
                        SELECT DISTINCT user_id, username 
                        FROM stats 
                        WHERE chat_id = %s AND user_id != %s
                        ORDER BY username
                    ''', (chat_id, BOT_ID if BOT_ID else 0))
                    bot_participants = cursor_local.fetchall()
                
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
                    admins = bot.get_chat_administrators(chat_id)
                    all_group_member_ids = set()
                    all_group_members = {}
                    
                    for admin in admins:
                        admin_user = admin.user
                        # Исключаем бота из списка участников группы
                        if BOT_ID and admin_user.id == BOT_ID:
                            continue
                        all_group_member_ids.add(admin_user.id)
                        all_group_members[admin_user.id] = {
                            'username': admin_user.username or f"user_{admin_user.id}",
                            'first_name': admin_user.first_name or '',
                            'is_premium': getattr(admin_user, 'is_premium', False)
                        }
                    
                    # Находим недобавленных участников (исключая бота)
                    not_added = []
                    for member_id, member_info in all_group_members.items():
                        # Пропускаем бота
                        if BOT_ID and member_id == BOT_ID:
                            continue
                        if member_id not in bot_participant_ids:
                            not_added.append({
                                'user_id': member_id,
                                'username': member_info['username'],
                                'first_name': member_info['first_name'],
                                'is_premium': member_info['is_premium']
                            })
                    
                    # Получаем информацию о групповой подписке
                    paid_participants_count = 0
                    # Используем get_chat_member_count для точного подсчета всех участников (минус бот)
                    try:
                        chat_member_count = bot.get_chat_member_count(chat_id)
                        total_participants_count = max(1, chat_member_count - 1)  # Вычитаем бота
                    except:
                        # Fallback на количество администраторов, если не удалось получить общее количество
                        total_participants_count = len(all_group_member_ids) if all_group_member_ids else 1
                    group_subscription_info = None
                    try:
                        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
                        group_sub = get_active_group_subscription_by_chat_id(chat_id)
                        if group_sub:
                            subscription_id = group_sub.get('id') if isinstance(group_sub, dict) else group_sub[0]
                            if subscription_id:
                                paid_members = get_subscription_members(subscription_id)
                                paid_participants_count = len(paid_members) if paid_members else 0
                                group_size = group_sub.get('group_size') if isinstance(group_sub, dict) else (group_sub[11] if len(group_sub) > 11 else None)
                                group_subscription_info = {
                                    'subscription_id': subscription_id,
                                    'group_size': group_size,
                                    'paid_count': paid_participants_count
                                }
                    except Exception as e:
                        logger.warning(f"[JOIN] Ошибка при получении информации о групповой подписке: {e}")
                    
                    # Формируем ответ - если есть действующая подписка, показываем как в payment:active:group
                    if group_subscription_info:
                        # Есть действующая подписка - показываем информацию как в payment:active:group
                        subscription_id = group_subscription_info.get('subscription_id')
                        group_size = group_subscription_info.get('group_size')
                        paid_count = group_subscription_info.get('paid_count', 0)
                        
                        # Получаем информацию о подписке для отображения
                        from moviebot.database.db_operations import get_subscription_by_id
                        sub = get_subscription_by_id(subscription_id) if subscription_id else None
                        
                        if sub:
                            plan_type = sub.get('plan_type', 'all')
                            period_type = sub.get('period_type', 'month')
                            activated = sub.get('activated_at')
                            next_payment = sub.get('next_payment_date')
                            expires_at = sub.get('expires_at')
                            
                            plan_names = {
                                'notifications': 'Уведомления о сериалах',
                                'recommendations': 'Рекомендации',
                                'tickets': 'Билеты',
                                'all': 'Все режимы'
                            }
                            plan_name = plan_names.get(plan_type, plan_type)
                            
                            response_text = "👥 <b>Групповая подписка</b>\n\n"
                            response_text += f"📋 <b>Название подписки:</b> {plan_name}\n"
                            if group_size:
                                response_text += f"👥 <b>Размер группы:</b> {group_size} участников\n"
                            response_text += f"💰 <b>Платных участников:</b> {paid_count}/{total_participants_count}\n"
                            if activated:
                                response_text += f"📅 Дата активации: <b>{activated.strftime('%d.%m.%Y') if isinstance(activated, datetime) else activated}</b>\n"
                            if next_payment:
                                response_text += f"📅 Следующее списание: <b>{next_payment.strftime('%d.%m.%Y') if isinstance(next_payment, datetime) else next_payment}</b>\n"
                            if expires_at:
                                response_text += f"⏰ Действует до: <b>{expires_at.strftime('%d.%m.%Y') if isinstance(expires_at, datetime) else expires_at}</b>\n"
                            else:
                                response_text += f"⏰ Действует: <b>Навсегда</b>\n"
                            
                            markup = InlineKeyboardMarkup(row_width=1)
                            # Показываем кнопку списка участников только для реальных подписок (не виртуальных)
                            if subscription_id and subscription_id > 0:
                                markup.add(InlineKeyboardButton("👥 Список участников", callback_data=f"payment:group_members:{subscription_id}:0"))
                            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                            
                            bot.reply_to(message, response_text, parse_mode='HTML', reply_markup=markup)
                            return
                    
                    # Если нет подписки или не удалось получить информацию, показываем стандартное сообщение
                    if not_added or bot_participants:
                        response_text += "\n\n"
                        
                        # Показываем информацию о платных участниках (только для групповых чатов)
                        if group_subscription_info:
                            group_size = group_subscription_info.get('group_size')
                            if group_size:
                                response_text += f"💰 <b>Платных участников:</b> {paid_participants_count}/{total_participants_count}\n\n"
                            else:
                                response_text += f"💰 <b>Платных участников:</b> {paid_participants_count}/{total_participants_count}\n\n"
                        else:
                            response_text += f"💰 <b>Платных участников:</b> 0/0\n\n"
                        
                        # Показываем участников бота (исключая бота)
                        if bot_participants:
                            response_text += "✅ <b>Участники бота:</b>\n"
                            for row in bot_participants:
                                p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                                p_username = row.get('username') if isinstance(row, dict) else row[1]
                                
                                # Пропускаем бота
                                if BOT_ID and p_user_id == BOT_ID:
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
                        
                        # Показываем недобавленных участников (исключая бота)
                        if not_added:
                            # Фильтруем бота из недобавленных
                            not_added_filtered = [m for m in not_added if not (BOT_ID and m['user_id'] == BOT_ID)]
                            
                            if not_added_filtered:
                                response_text += "\n❌ <b>Недобавленные участники:</b>\n"
                                
                                markup = InlineKeyboardMarkup(row_width=1)
                                for member in not_added_filtered[:20]:  # Ограничиваем до 20 кнопок
                                    display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                                    premium_mark = "⭐" if member['is_premium'] else ""
                                    button_text = f"{display_name} {premium_mark}".strip()
                                    if len(button_text) > 50:
                                        button_text = button_text[:47] + "..."
                                    markup.add(InlineKeyboardButton(button_text, callback_data=f"join_add:{member['user_id']}"))
                                
                                bot.reply_to(message, response_text, parse_mode='HTML', reply_markup=markup)
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
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
        
        bot.reply_to(message, response_text, parse_mode='HTML')
        logger.info(f"✅ Команда /join обработана для пользователя {user_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /join: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /join")
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("join_add:"))
def join_add_callback(call):
    """Обработчик добавления участника через кнопку в /join"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        target_user_id = int(call.data.split(":")[1])
        
        # Проверяем, является ли вызывающий участником бота
        if not is_bot_participant(chat_id, user_id):
            bot.answer_callback_query(call.id, "❌ Вы не участвуете в боте. Используйте /join", show_alert=True)
            return
        
        # Проверяем, является ли целевой пользователь уже участником
        if is_bot_participant(chat_id, target_user_id):
            bot.answer_callback_query(call.id, "✅ Этот пользователь уже участвует в боте")
            return
        
        # Регистрируем пользователя
        username = call.from_user.username or f"user_{target_user_id}"
        log_request(target_user_id, username, '/join', chat_id)
        
        bot.answer_callback_query(call.id, "✅ Пользователь добавлен к участию в боте")
        
        # Обновляем сообщение, удаляя кнопку добавленного пользователя
        try:
            # Получаем текущий текст сообщения
            message_text = call.message.text or call.message.caption or ""
            
            # Получаем список недобавленных участников
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute('''
                        SELECT DISTINCT user_id, username 
                        FROM stats 
                        WHERE chat_id = %s
                        ORDER BY username
                    ''', (chat_id,))
                    bot_participants = cursor_local.fetchall()
                
                bot_participant_ids = set()
                for row in bot_participants:
                    p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    bot_participant_ids.add(p_user_id)
                
                # Получаем администраторов группы
                try:
                    admins = bot.get_chat_administrators(chat_id)
                    all_group_member_ids = set()
                    all_group_members = {}
                    
                    for admin in admins:
                        admin_user = admin.user
                        # Исключаем бота из списка участников группы
                        if BOT_ID and admin_user.id == BOT_ID:
                            continue
                        all_group_member_ids.add(admin_user.id)
                        all_group_members[admin_user.id] = {
                            'username': admin_user.username or f"user_{admin_user.id}",
                            'first_name': admin_user.first_name or '',
                            'is_premium': getattr(admin_user, 'is_premium', False)
                        }
                    
                    # Получаем информацию о групповой подписке
                    paid_participants_count = 0
                    # Используем get_chat_member_count для точного подсчета всех участников (минус бот)
                    try:
                        chat_member_count = bot.get_chat_member_count(chat_id)
                        total_participants_count = max(1, chat_member_count - 1)  # Вычитаем бота
                    except:
                        # Fallback на количество администраторов, если не удалось получить общее количество
                        total_participants_count = len(all_group_member_ids) if all_group_member_ids else 1
                    group_subscription_info = None
                    try:
                        from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
                        group_sub = get_active_group_subscription_by_chat_id(chat_id)
                        if group_sub:
                            subscription_id = group_sub.get('id') if isinstance(group_sub, dict) else group_sub[0]
                            if subscription_id:
                                paid_members = get_subscription_members(subscription_id)
                                paid_participants_count = len(paid_members) if paid_members else 0
                                group_size = group_sub.get('group_size') if isinstance(group_sub, dict) else (group_sub[11] if len(group_sub) > 11 else None)
                                group_subscription_info = {
                                    'subscription_id': subscription_id,
                                    'group_size': group_size,
                                    'paid_count': paid_participants_count
                                }
                    except Exception as e:
                        logger.warning(f"[JOIN ADD] Ошибка при получении информации о групповой подписке: {e}")
                    
                    # Находим недобавленных участников (исключая бота)
                    not_added = []
                    for member_id, member_info in all_group_members.items():
                        # Пропускаем бота
                        if BOT_ID and member_id == BOT_ID:
                            continue
                        if member_id not in bot_participant_ids:
                            not_added.append({
                                'user_id': member_id,
                                'username': member_info['username'],
                                'first_name': member_info['first_name'],
                                'is_premium': member_info['is_premium']
                            })
                    
                    # Если есть еще недобавленные участники, обновляем сообщение
                    not_added_filtered = [m for m in not_added if not (BOT_ID and m['user_id'] == BOT_ID)]
                
                    if not_added_filtered:
                        response_text = message_text.split("\n\n")[0] if "\n\n" in message_text else message_text
                        response_text += "\n\n"
                    
                        # Показываем информацию о платных участниках (только для групповых чатов)
                        if group_subscription_info:
                            group_size = group_subscription_info.get('group_size')
                            if group_size:
                                response_text += f"💰 <b>Платных участников:</b> {paid_participants_count}/{total_participants_count}\n\n"
                            else:
                                response_text += f"💰 <b>Платных участников:</b> {paid_participants_count}/{total_participants_count}\n\n"
                        else:
                            response_text += f"💰 <b>Платных участников:</b> 0/0\n\n"
                    
                        # Показываем участников бота (исключая бота)
                        if bot_participants:
                            response_text += "✅ <b>Участники бота:</b>\n"
                            for row in bot_participants:
                                p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                                p_username = row.get('username') if isinstance(row, dict) else row[1]
                            
                                # Пропускаем бота
                                if BOT_ID and p_user_id == BOT_ID:
                                    continue
                            
                                has_premium = False
                                try:
                                    user_info = all_group_members.get(p_user_id, {})
                                    has_premium = user_info.get('is_premium', False)
                                except:
                                    pass
                            
                                premium_mark = "⭐" if has_premium else ""
                                display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                                response_text += f"• {display_name} {premium_mark}\n"
                    
                        # Показываем недобавленных участников (исключая бота)
                        not_added_filtered = [m for m in not_added if not (BOT_ID and m['user_id'] == BOT_ID)]
                    
                        if not_added_filtered:
                            response_text += "\n❌ <b>Недобавленные участники:</b>\n"
                        
                            markup = InlineKeyboardMarkup(row_width=1)
                            for member in not_added_filtered[:20]:  # Ограничиваем до 20 кнопок
                                display_name = member['username'] if member['username'].startswith('user_') else f"@{member['username']}"
                                premium_mark = "⭐" if member['is_premium'] else ""
                                button_text = f"{display_name} {premium_mark}".strip()
                                if len(button_text) > 50:
                                    button_text = button_text[:47] + "..."
                                markup.add(InlineKeyboardButton(button_text, callback_data=f"join_add:{member['user_id']}"))
                        
                            bot.edit_message_text(response_text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                        else:
                            # Все участники добавлены, удаляем кнопки
                            bot.edit_message_text(response_text, chat_id, call.message.message_id, parse_mode='HTML')
                    else:
                        # Все участники добавлены
                        # Получаем информацию о групповой подписке
                        paid_participants_count = 0
                        total_participants_count = 0
                        try:
                            # Используем get_chat_member_count для точного подсчета всех участников (минус бот)
                            try:
                                chat_member_count = bot.get_chat_member_count(chat_id)
                                total_participants_count = max(1, chat_member_count - 1)  # Вычитаем бота
                            except:
                                # Fallback на количество администраторов, если не удалось получить общее количество
                                admins = bot.get_chat_administrators(chat_id)
                                all_group_member_ids = set()
                                for admin in admins:
                                    admin_user = admin.user
                                    if BOT_ID and admin_user.id == BOT_ID:
                                        continue
                                    all_group_member_ids.add(admin_user.id)
                                total_participants_count = len(all_group_member_ids) if all_group_member_ids else 1
                        
                            from moviebot.database.db_operations import get_active_group_subscription_by_chat_id, get_subscription_members
                            group_sub = get_active_group_subscription_by_chat_id(chat_id)
                            if group_sub:
                                subscription_id = group_sub.get('id') if isinstance(group_sub, dict) else group_sub[0]
                                if subscription_id:
                                    paid_members = get_subscription_members(subscription_id)
                                    paid_participants_count = len(paid_members) if paid_members else 0
                        except Exception as e:
                            logger.warning(f"[JOIN ADD] Ошибка при получении информации о групповой подписке: {e}")
                    
                        response_text = message_text.split("\n\n")[0] if "\n\n" in message_text else message_text
                        response_text += "\n\n"
                    
                        # Показываем информацию о платных участниках
                        response_text += f"💰 <b>Платных участников:</b> {paid_participants_count}/{total_participants_count}\n\n"
                    
                        response_text += "✅ <b>Участники бота:</b>\n"
                        for row in bot_participants:
                            p_user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                            p_username = row.get('username') if isinstance(row, dict) else row[1]
                        
                            # Пропускаем бота
                            if BOT_ID and p_user_id == BOT_ID:
                                continue
                        
                            display_name = p_username if p_username.startswith('user_') else f"@{p_username}"
                            response_text += f"• {display_name}\n"
                        bot.edit_message_text(response_text, chat_id, call.message.message_id, parse_mode='HTML')
                except Exception as admin_e:
                    logger.warning(f"[JOIN ADD] Не удалось получить администраторов: {admin_e}")
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
        except Exception as e:
            logger.error(f"[JOIN ADD] Ошибка при обновлении сообщения: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[JOIN ADD] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def register_join_handlers(bot):
    """Регистрирует обработчики команды /join"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики команды /join зарегистрированы")

