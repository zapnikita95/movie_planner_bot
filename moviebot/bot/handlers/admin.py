"""
Админские команды: /unsubscribe, /add_admin
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
import pytz

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.states import user_unsubscribe_state, user_add_admin_state
from moviebot.utils.admin import is_owner, is_admin, add_admin, remove_admin, get_all_admins
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

# ID владельца бота
OWNER_ID = 301810276


def cancel_subscription_by_id(target_id, is_group=False):
    """
    Отменяет подписку по ID пользователя или группы
    
    Args:
        target_id: ID пользователя или группы
        is_group: True если это группа
    
    Returns:
        (success: bool, message: str, count: int)
    """
    try:
        with db_lock:
            if is_group:
                # Отменяем групповую подписку
                cursor.execute("""
                    UPDATE subscriptions 
                    SET is_active = FALSE, cancelled_at = %s
                    WHERE chat_id = %s AND subscription_type = 'group'
                """, (datetime.now(pytz.UTC), target_id))
            else:
                # Отменяем персональную подписку
                cursor.execute("""
                    UPDATE subscriptions 
                    SET is_active = FALSE, cancelled_at = %s
                    WHERE user_id = %s AND subscription_type = 'personal'
                """, (datetime.now(pytz.UTC), target_id))
            
            count = cursor.rowcount
            conn.commit()
            
            if count > 0:
                return True, f"Отменено подписок: {count}", count
            else:
                return False, "Подписки не найдены", 0
    except Exception as e:
        logger.error(f"Ошибка при отмене подписки: {e}", exc_info=True)
        conn.rollback()
        return False, f"Ошибка при отмене подписки: {e}", 0


@bot_instance.message_handler(commands=['unsubscribe'])
def unsubscribe_command(message):
    """Команда /unsubscribe - отмена подписки пользователя или группы (только для владельца)"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверяем, что команда отправлена в личке
        if message.chat.type != 'private':
            bot_instance.reply_to(message, "❌ Команда /unsubscribe доступна только в личных сообщениях боту.")
            return
        
        # Проверяем права доступа (только владелец)
        if not is_owner(user_id):
            bot_instance.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        logger.info(f"[UNSUBSCRIBE] Команда /unsubscribe вызвана от {user_id}")
        
        text = "🔴 <b>Отмена подписки</b>\n\n"
        text += "Введите ID пользователя или группы в ответном сообщении.\n\n"
        text += "Для пользователя: введите его user_id (число)\n"
        text += "Для группы: введите chat_id группы (отрицательное число или число)\n\n"
        text += "Пример: <code>123456789</code> или <code>-1001234567890</code>"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
        
        msg = bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
        user_unsubscribe_state[user_id] = {
            'chat_id': message.chat.id,
            'message_id': msg.message_id if msg else None,
            'prompt_message_id': msg.message_id if msg else None
        }
        logger.info(f"[UNSUBSCRIBE] Ожидаем ввод ID от пользователя {user_id}, prompt_message_id={msg.message_id if msg else None}")
        
    except Exception as e:
        logger.error(f"[UNSUBSCRIBE] Ошибка в unsubscribe_command: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке команды /unsubscribe")
        except:
            pass


@bot_instance.message_handler(commands=['add_admin'])
def add_admin_command(message):
    """Команда /add_admin - управление администраторами (только для владельца)"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверяем, что команда отправлена в личке
        if message.chat.type != 'private':
            bot_instance.reply_to(message, "❌ Команда /add_admin доступна только в личных сообщениях боту.")
            return
        
        # Проверяем права доступа (только владелец)
        if not is_owner(user_id):
            bot_instance.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        logger.info(f"[ADD_ADMIN] Команда /add_admin вызвана от {user_id}")
        
        # Получаем список администраторов
        admins = get_all_admins()
        
        text = "👑 <b>Управление администраторами</b>\n\n"
        text += "Введите ID пользователя в ответном сообщении, чтобы добавить его как администратора.\n\n"
        text += "<b>Действующие администраторы:</b>\n"
        
        if admins:
            for admin in admins:
                admin_user_id = admin['user_id']
                is_owner_flag = "👑 Владелец" if is_owner(admin_user_id) else "👤 Админ"
                text += f"• {is_owner_flag} <code>{admin_user_id}</code>\n"
        else:
            text += "Нет администраторов\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Добавляем кнопки для каждого администратора
        for admin in admins:
            admin_user_id = admin['user_id']
            if not is_owner(admin_user_id):  # Не показываем кнопку для владельца
                button_text = f"👤 {admin_user_id}"
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"admin:info:{admin_user_id}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
        
        msg = bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
        user_add_admin_state[user_id] = {
            'message_id': msg.message_id if msg else None,
            'prompt_message_id': msg.message_id if msg else None
        }
        
    except Exception as e:
        logger.error(f"[ADD_ADMIN] Ошибка в add_admin_command: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке команды /add_admin")
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("admin:info:"))
def admin_info_callback(call):
    """Обработчик просмотра информации об администраторе"""
    try:
        bot_instance.answer_callback_query(call.id)
        admin_user_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        if not is_owner(user_id):
            bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        text = f"👤 <b>Администратор: {admin_user_id}</b>\n\n"
        text += "Выберите действие:"
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Убрать администратора", callback_data=f"admin:remove:{admin_user_id}"))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back_to_list"))
        
        try:
            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except:
            bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"[ADD_ADMIN] Ошибка в admin_info_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("admin:remove:"))
def admin_remove_callback(call):
    """Обработчик удаления администратора"""
    try:
        bot_instance.answer_callback_query(call.id)
        admin_user_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        if not is_owner(user_id):
            bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        # Удаляем администратора
        success, message = remove_admin(admin_user_id)
        
        if success:
            bot_instance.answer_callback_query(call.id, "✅ Администратор удален", show_alert=False)
            # Возвращаемся к списку администраторов
            from moviebot.bot.handlers.admin import add_admin_command
            class FakeMessage:
                def __init__(self, chat_id, user_id):
                    self.chat = type('obj', (object,), {'id': chat_id, 'type': 'private'})()
                    self.from_user = type('obj', (object,), {'id': user_id})()
                    self.text = '/add_admin'
            
            fake_msg = FakeMessage(call.message.chat.id, user_id)
            add_admin_command(fake_msg)
        else:
            bot_instance.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            
    except Exception as e:
        logger.error(f"[ADD_ADMIN] Ошибка в admin_remove_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data == "admin:back_to_list")
def admin_back_to_list_callback(call):
    """Обработчик возврата к списку администраторов"""
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        
        # Возвращаемся к списку администраторов
        from moviebot.bot.handlers.admin import add_admin_command
        class FakeMessage:
            def __init__(self, chat_id, user_id):
                self.chat = type('obj', (object,), {'id': chat_id, 'type': 'private'})()
                self.from_user = type('obj', (object,), {'id': user_id})()
                self.text = '/add_admin'
        
        fake_msg = FakeMessage(call.message.chat.id, user_id)
        add_admin_command(fake_msg)
        
    except Exception as e:
        logger.error(f"[ADD_ADMIN] Ошибка в admin_back_to_list_callback: {e}", exc_info=True)


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("unsubscribe:"))
def handle_unsubscribe_callback(call):
    """Обработчик выбора типа отмены подписки и отмены конкретной подписки"""
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        
        # Проверяем права доступа
        if not is_owner(user_id):
            bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        parts = call.data.split(":")
        if len(parts) < 3:
            logger.error(f"[UNSUBSCRIBE CALLBACK] Неверный формат callback_data: {call.data}")
            return
        
        action = parts[1]  # personal, paid, или cancel
        target_user_id = int(parts[2]) if len(parts) > 2 else None
        
        if action == "personal":
            # Показываем список личных подписок пользователя
            from moviebot.database.db_operations import get_user_personal_subscriptions
            subscriptions = get_user_personal_subscriptions(target_user_id)
            
            if not subscriptions:
                text = f"👤 <b>Личные подписки пользователя {target_user_id}</b>\n\n"
                text += "❌ У пользователя нет активных личных подписок."
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"unsubscribe:back:{target_user_id}"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                return
            
            text = f"👤 <b>Личные подписки пользователя {target_user_id}</b>\n\n"
            text += "Выберите подписку для отмены:\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            plan_names = {
                'notifications': '🔔 Уведомления',
                'recommendations': '🎯 Рекомендации',
                'tickets': '🎟️ Билеты',
                'all': '🎬 Все режимы'
            }
            
            period_names = {
                'month': 'месяц',
                '3months': '3 месяца',
                'year': 'год',
                'lifetime': 'навсегда'
            }
            
            for sub in subscriptions:
                if isinstance(sub, dict):
                    sub_id = sub.get('id')
                    plan_type = sub.get('plan_type', '')
                    period_type = sub.get('period_type', '')
                    expires_at = sub.get('expires_at')
                else:
                    sub_id = sub[0] if len(sub) > 0 else None
                    plan_type = sub[3] if len(sub) > 3 else ''
                    period_type = sub[4] if len(sub) > 4 else ''
                    expires_at = sub[9] if len(sub) > 9 else None
                
                plan_name = plan_names.get(plan_type, plan_type)
                period_name = period_names.get(period_type, period_type)
                
                if expires_at:
                    from datetime import datetime
                    import pytz
                    if isinstance(expires_at, str):
                        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    expires_str = expires_at.strftime('%d.%m.%Y')
                    button_text = f"{plan_name} ({period_name}) до {expires_str}"
                else:
                    button_text = f"{plan_name} ({period_name})"
                
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                
                markup.add(InlineKeyboardButton(button_text, callback_data=f"unsubscribe:cancel:{sub_id}"))
            
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"unsubscribe:back:{target_user_id}"))
            
            try:
                bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except:
                bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
        
        elif action == "paid":
            # Показываем список всех подписок, оплаченных пользователем
            with db_lock:
                cursor.execute("""
                    SELECT s.* FROM subscriptions s
                    INNER JOIN payments p ON s.payment_id = p.payment_id
                    WHERE p.user_id = %s AND s.is_active = TRUE 
                    AND (s.expires_at IS NULL OR s.expires_at > NOW())
                    ORDER BY s.created_at DESC
                """, (target_user_id,))
                subscriptions = cursor.fetchall()
            
            if not subscriptions:
                text = f"💳 <b>Оплаченные подписки пользователя {target_user_id}</b>\n\n"
                text += "❌ Пользователь не оплачивал активных подписок."
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"unsubscribe:back:{target_user_id}"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                return
            
            text = f"💳 <b>Оплаченные подписки пользователя {target_user_id}</b>\n\n"
            text += "Выберите подписку для отмены:\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            plan_names = {
                'notifications': '🔔 Уведомления',
                'recommendations': '🎯 Рекомендации',
                'tickets': '🎟️ Билеты',
                'all': '🎬 Все режимы'
            }
            
            period_names = {
                'month': 'месяц',
                '3months': '3 месяца',
                'year': 'год',
                'lifetime': 'навсегда'
            }
            
            for sub in subscriptions:
                if isinstance(sub, dict):
                    sub_id = sub.get('id')
                    subscription_type = sub.get('subscription_type', '')
                    plan_type = sub.get('plan_type', '')
                    period_type = sub.get('period_type', '')
                    chat_id = sub.get('chat_id')
                    group_size = sub.get('group_size')
                    expires_at = sub.get('expires_at')
                else:
                    sub_id = sub[0] if len(sub) > 0 else None
                    subscription_type = sub[2] if len(sub) > 2 else ''
                    plan_type = sub[3] if len(sub) > 3 else ''
                    period_type = sub[4] if len(sub) > 4 else ''
                    chat_id = sub[1] if len(sub) > 1 else None
                    group_size = sub[6] if len(sub) > 6 else None
                    expires_at = sub[9] if len(sub) > 9 else None
                
                plan_name = plan_names.get(plan_type, plan_type)
                period_name = period_names.get(period_type, period_type)
                
                type_prefix = "👥 Групповая" if subscription_type == 'group' else "👤 Личная"
                if subscription_type == 'group' and group_size:
                    type_prefix += f" ({group_size} чел.)"
                
                if expires_at:
                    from datetime import datetime
                    import pytz
                    if isinstance(expires_at, str):
                        expires_at = datetime.fromisoformat(expires_at.replace('Z', '+00:00'))
                    expires_str = expires_at.strftime('%d.%m.%Y')
                    button_text = f"{type_prefix}: {plan_name} ({period_name}) до {expires_str}"
                else:
                    button_text = f"{type_prefix}: {plan_name} ({period_name})"
                
                if len(button_text) > 50:
                    button_text = button_text[:47] + "..."
                
                markup.add(InlineKeyboardButton(button_text, callback_data=f"unsubscribe:cancel:{sub_id}"))
            
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"unsubscribe:back:{target_user_id}"))
            
            try:
                bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except:
                bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
        
        elif action == "cancel":
            # Отменяем конкретную подписку
            subscription_id = int(parts[2]) if len(parts) > 2 else None
            
            if not subscription_id:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка: неверный ID подписки", show_alert=True)
                return
            
            from moviebot.database.db_operations import cancel_subscription, get_subscription_by_id
            subscription = get_subscription_by_id(subscription_id)
            
            if not subscription:
                bot_instance.answer_callback_query(call.id, "❌ Подписка не найдена", show_alert=True)
                return
            
            if isinstance(subscription, dict):
                target_user_id_from_sub = subscription.get('user_id')
            else:
                target_user_id_from_sub = subscription[2] if len(subscription) > 2 else None
            
            if cancel_subscription(subscription_id, target_user_id_from_sub):
                plan_names = {
                    'notifications': '🔔 Уведомления',
                    'recommendations': '🎯 Рекомендации',
                    'tickets': '🎟️ Билеты',
                    'all': '🎬 Все режимы'
                }
                
                if isinstance(subscription, dict):
                    plan_type = subscription.get('plan_type', '')
                    subscription_type = subscription.get('subscription_type', '')
                else:
                    plan_type = subscription[3] if len(subscription) > 3 else ''
                    subscription_type = subscription[2] if len(subscription) > 2 else ''
                
                plan_name = plan_names.get(plan_type, plan_type)
                type_text = "Групповая" if subscription_type == 'group' else "Личная"
                
                text = f"✅ <b>Подписка отменена</b>\n\n"
                text += f"Тип: {type_text}\n"
                text += f"Тариф: {plan_name}\n"
                text += f"ID подписки: <code>{subscription_id}</code>"
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"unsubscribe:back:{target_user_id_from_sub}"))
                
                try:
                    bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
                
                bot_instance.answer_callback_query(call.id, "✅ Подписка отменена", show_alert=False)
                
                # Очищаем состояние после успешной отмены
                from moviebot.states import user_unsubscribe_state
                if user_id in user_unsubscribe_state:
                    del user_unsubscribe_state[user_id]
            else:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка отмены подписки", show_alert=True)
        
        elif action == "back":
            # Возврат к меню выбора типа отмены
            target_user_id = int(parts[2]) if len(parts) > 2 else None
            
            if target_user_id:
                from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
                
                text_result = f"👤 <b>Пользователь: {target_user_id}</b>\n\n"
                text_result += "Что вы хотите отменить?\n\n"
                text_result += "• <b>Личная подписка</b> - все личные подписки этого пользователя\n"
                text_result += "• <b>Оплаченные подписки</b> - все подписки, которые были оплачены этим пользователем (личные и групповые)"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("👤 Личная подписка", callback_data=f"unsubscribe:personal:{target_user_id}"))
                markup.add(InlineKeyboardButton("💳 Оплаченные подписки", callback_data=f"unsubscribe:paid:{target_user_id}"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="admin:back"))
                
                try:
                    bot_instance.edit_message_text(text_result, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                except:
                    bot_instance.send_message(call.message.chat.id, text_result, reply_markup=markup, parse_mode='HTML')
        
    except Exception as e:
        logger.error(f"[UNSUBSCRIBE CALLBACK] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data == "admin:back")
def admin_back_callback(call):
    """Обработчик кнопки 'Назад'"""
    try:
        bot_instance.answer_callback_query(call.id)
        # Просто закрываем сообщение или возвращаемся в меню
        try:
            bot_instance.delete_message(call.message.chat.id, call.message.message_id)
        except:
            pass
    except Exception as e:
        logger.error(f"[ADMIN] Ошибка в admin_back_callback: {e}", exc_info=True)

