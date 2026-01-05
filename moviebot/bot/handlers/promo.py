"""
Обработчики команды /promo для управления промокодами
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.states import user_promo_admin_state
from moviebot.utils.promo import get_active_promocodes, deactivate_promocode, get_promocode_info

logger = logging.getLogger(__name__)

# ID владельца бота (получаем из переменной окружения)
BOT_OWNER_ID = None  # Будет установлен при инициализации


def get_bot_owner_id():
    """Получает ID владельца бота"""
    global BOT_OWNER_ID
    if BOT_OWNER_ID is None:
        import os
        owner_id_str = os.getenv('BOT_OWNER_ID')
        if owner_id_str:
            try:
                BOT_OWNER_ID = int(owner_id_str)
            except ValueError:
                logger.warning(f"BOT_OWNER_ID имеет неверный формат: {owner_id_str}")
        else:
            # Если не задан, используем ID создателя бота из stats.py (301810276)
            # В реальности лучше задать через переменную окружения
            BOT_OWNER_ID = 301810276  # ID создателя бота
            logger.info(f"BOT_OWNER_ID не задан в переменных окружения. Используется значение по умолчанию: {BOT_OWNER_ID}")
    return BOT_OWNER_ID


# Обработчики регистрируются автоматически через декораторы
@bot_instance.message_handler(commands=['promo'])
def promo_command(message):
    """Команда /promo - управление промокодами (только для владельца бота)"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверяем, что команда отправлена в личке
        if message.chat.type != 'private':
            bot_instance.reply_to(message, "❌ Команда /promo доступна только в личных сообщениях боту.")
            return
        
        # Проверяем права доступа (владелец бота)
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot_instance.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        logger.info(f"[PROMO] Команда /promo вызвана от {user_id}")
        
        # Получаем список активных промокодов
        active_promocodes = get_active_promocodes()
        
        text = "🏷️ <b>Управление промокодами</b>\n\n"
        text += "Задайте промокод, скидку и количество купонов.\n\n"
        text += "Формат: <code>КОД СКИДКА КОЛИЧЕСТВО</code>\n"
        text += "Пример: <code>NEW2026 20% 100</code>\n\n"
        text += "<b>Действующие промокоды:</b>\n"
        
        if active_promocodes:
            for promo in active_promocodes:
                discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} руб/звезд"
                remaining = promo['total_uses'] - promo['used_count']
                text += f"• <code>{promo['code']}</code> — {discount_str} (осталось: {remaining}/{promo['total_uses']})\n"
        else:
            text += "Нет активных промокодов\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Добавляем кнопки для каждого активного промокода
        for promo in active_promocodes:
            discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} руб/звезд"
            remaining = promo['total_uses'] - promo['used_count']
            button_text = f"🏷️ {promo['code']} ({discount_str}, осталось: {remaining})"
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"promo:info:{promo['id']}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_start_menu"))
        
        # Устанавливаем состояние для обработки ответа
        msg = bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
        user_promo_admin_state[user_id] = {'message_id': msg.message_id}
        
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_command: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке команды /promo")
        except:
            pass

@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("promo:info:"))
def promo_info_callback(call):
    """Обработчик просмотра информации о промокоде"""
    try:
        bot_instance.answer_callback_query(call.id)
        promocode_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        # Получаем информацию о промокоде
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cursor = get_db_cursor()
        
        with db_lock:
            cursor.execute('''
                SELECT code, discount_type, discount_value, total_uses, used_count, is_active
                FROM promocodes
                WHERE id = %s
            ''', (promocode_id,))
            row = cursor.fetchone()
        
        if not row:
            bot_instance.answer_callback_query(call.id, "❌ Промокод не найден", show_alert=True)
            return
        
        if isinstance(row, dict):
            code = row['code']
            discount_type = row['discount_type']
            discount_value = float(row['discount_value'])
            total_uses = row['total_uses']
            used_count = row['used_count']
            is_active = bool(row['is_active'])
        else:
            code = row[0]
            discount_type = row[1]
            discount_value = float(row[2])
            total_uses = row[3]
            used_count = row[4]
            is_active = bool(row[5])
        
        discount_str = f"{discount_value}%" if discount_type == 'percent' else f"{int(discount_value)} руб/звезд"
        remaining = total_uses - used_count
        
        text = f"🏷️ <b>Промокод: {code}</b>\n\n"
        text += f"Скидка: {discount_str}\n"
        text += f"Использовано: {used_count}/{total_uses}\n"
        text += f"Осталось: {remaining}\n"
        text += f"Статус: {'✅ Активен' if is_active else '❌ Деактивирован'}\n"
        
        markup = InlineKeyboardMarkup()
        if is_active:
            markup.add(InlineKeyboardButton("❌ Деактивировать", callback_data=f"promo:deactivate:{promocode_id}"))
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="promo:back"))
        
        try:
            bot_instance.edit_message_text(text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        except:
            bot_instance.send_message(call.message.chat.id, text, reply_markup=markup, parse_mode='HTML')
            
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_info_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("promo:deactivate:"))
def promo_deactivate_callback(call):
    """Обработчик деактивации промокода"""
    try:
        bot_instance.answer_callback_query(call.id)
        promocode_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot_instance.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        # Деактивируем промокод
        success, message = deactivate_promocode(promocode_id)
        
        if success:
            bot_instance.answer_callback_query(call.id, "✅ Промокод деактивирован", show_alert=False)
            # Возвращаемся к списку промокодов
            from moviebot.bot.handlers.promo import promo_command
            # Создаем фиктивное сообщение для вызова команды
            class FakeMessage:
                def __init__(self, chat_id, user_id):
                    self.chat = type('obj', (object,), {'id': chat_id, 'type': 'private'})()
                    self.from_user = type('obj', (object,), {'id': user_id})()
                    self.text = '/promo'
            
            fake_msg = FakeMessage(call.message.chat.id, user_id)
            promo_command(fake_msg)
        else:
            bot_instance.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_deactivate_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot_instance.callback_query_handler(func=lambda call: call.data == "promo:back")
def promo_back_callback(call):
    """Обработчик возврата к списку промокодов"""
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        
        # Возвращаемся к списку промокодов
        from moviebot.bot.handlers.promo import promo_command
        class FakeMessage:
            def __init__(self, chat_id, user_id):
                self.chat = type('obj', (object,), {'id': chat_id, 'type': 'private'})()
                self.from_user = type('obj', (object,), {'id': user_id})()
                self.text = '/promo'
        
        fake_msg = FakeMessage(call.message.chat.id, user_id)
        promo_command(fake_msg)
        
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_back_callback: {e}", exc_info=True)

