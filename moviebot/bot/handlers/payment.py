"""
Обработчики команды /payment
"""
import logging
from moviebot.bot.bot_init import bot
from datetime import datetime
import pytz
from moviebot.bot.bot_init import bot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot
from moviebot.database.db_operations import log_request, get_active_subscription

logger = logging.getLogger(__name__)



def payment_command(message):
    """Команда /payment - управление подписками"""
    logger.info(f"[HANDLER] /payment вызван от {message.from_user.id}")
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/payment', chat_id)
        
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📋 Действующая подписка", callback_data="payment:active"))
        markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        text = "💳 <b>Оплата подписки</b>\n\n"
        text += "Выберите действие:"
        
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"❌ Ошибка в /payment: {e}", exc_info=True)
        try:
            bot.reply_to(message, "Произошла ошибка при обработке команды /payment")
        except:
            pass


def register_\0(bot):
    """Регистрирует обработчики команды /payment"""
    
    @bot.message_handler(commands=['payment'])
    def _payment_command_handler(message):
        """Обертка для регистрации команды /payment"""
        payment_command(message)

    @bot.callback_query_handler(func=lambda call: call.data and (
        call.data == "payment:active" or 
        call.data == "payment:tariffs" or 
        call.data == "payment:back" or 
        call.data == "payment:back_from_promo" or
        (call.data.startswith("payment:") and call.data.startswith("payment:reminder_ok"))
        # payment:subscribe, payment:promo, payment:back_from_promo, payment:modify, payment:cancel обрабатываются в payment_callbacks.py
    ))
    def handle_payment_menu_callback(call):
        """Обработчик callback для меню оплаты (active, tariffs, back, cancel)"""
        # Основные меню handlers остаются здесь, детальные обработчики в payment_callbacks.py
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            action = call.data.split(":", 1)[1]
            is_private = call.message.chat.type == 'private'
            
            logger.info(f"[PAYMENT MENU] Получен callback от {user_id}, action={action}, chat_id={chat_id}")
            
            if action.startswith("reminder_ok:"):
                # Подтверждение получения напоминания о списании
                try:
                    subscription_id = int(action.split(":")[1])
                    bot.answer_callback_query(call.id, "✅ Напоминание получено")
                    try:
                        bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
                    except:
                        pass
                    logger.info(f"[PAYMENT REMINDER] Пользователь {user_id} подтвердил получение напоминания для подписки {subscription_id}")
                except Exception as e:
                    logger.error(f"[PAYMENT REMINDER] Ошибка обработки подтверждения: {e}")
                return
            
            if action == "active":
                # Показываем действующие подписки
                markup = InlineKeyboardMarkup(row_width=1)
                
                # В групповом чате не показываем кнопку "Личная подписка"
                if is_private:
                    markup.add(InlineKeyboardButton("👤 Личная подписка", callback_data="payment:active:personal"))
                    markup.add(InlineKeyboardButton("👥 Групповая подписка", callback_data="payment:active:group"))
                    text = "📋 <b>Действующая подписка</b>\n\nВыберите тип подписки:"
                else:
                    # В групповом чате показываем только групповую подписку
                    markup.add(InlineKeyboardButton("👥 Групповая подписка", callback_data="payment:active:group"))
                    text = "📋 <b>Действующая подписка</b>\n\n"
                    text += "💡 <i>Личные подписки вы можете посмотреть в личных сообщениях бота</i>"
                
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
                
                try:
                    bot.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action == "tariffs":
                # Показываем тарифы
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("👤 Личные подписки", callback_data="payment:tariffs:personal"))
                markup.add(InlineKeyboardButton("👥 Групповые подписки", callback_data="payment:tariffs:group"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="payment:back"))
                
                try:
                    bot.edit_message_text(
                        "💰 <b>Тарифы</b>\n\nВыберите тип подписки:",
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action == "back":
                # Возврат к главному меню оплаты
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("📋 Действующая подписка", callback_data="payment:active"))
                markup.add(InlineKeyboardButton("💰 Тарифы", callback_data="payment:tariffs"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                text = "💳 <b>Оплата подписки</b>\n\n"
                text += "Выберите действие:"
                
                try:
                    bot.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                except Exception as e:
                    if "message is not modified" not in str(e):
                        logger.error(f"[PAYMENT] Ошибка редактирования сообщения: {e}")
                return
            
            if action == "back_from_promo":
                # Возврат к сообщению с кнопками оплаты после промокода
                # Обработка полностью в payment_callbacks.py, здесь только отвечаем на callback
                # чтобы избежать предупреждения
                bot.answer_callback_query(call.id)
                # Передаем управление в payment_callbacks.py (он обработает этот callback)
                return
            
            # TODO: Добавить обработку остальных действий в payment_callbacks.py:
            # - payment:active:personal
            # - payment:active:group
            # - payment:tariffs:personal
            # - payment:tariffs:group
            # - payment:cancel
            # - payment:subscribe:personal:...
            # - payment:subscribe:group:...
            # и другие из moviebot.py строки 17604-21362
            
            logger.warning(f"[PAYMENT MENU] Необработанное действие: {action}")
        except Exception as e:
            logger.error(f"[PAYMENT MENU] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
