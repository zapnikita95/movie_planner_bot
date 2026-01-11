from moviebot.bot.bot_init import bot
"""
Обработчики команды /promo для управления промокодами
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.states import user_promo_admin_state

from moviebot.utils.promo import get_active_promocodes, get_all_promocodes, deactivate_promocode, get_promocode_info


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
@bot.message_handler(commands=['promo'])
def promo_command(message):
    """Команда /promo - управление промокодами (только для владельца бота)"""
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        
        # Проверяем, что команда отправлена в личке
        if message.chat.type != 'private':
            bot.reply_to(message, "❌ Команда /promo доступна только в личных сообщениях боту.")
            return
        
        # Проверяем права доступа (владелец бота)
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot.reply_to(message, "❌ У вас нет доступа к этой команде.")
            return
        
        logger.info(f"[PROMO] Команда /promo вызвана от {user_id}")
        
        # Получаем список всех промокодов
        promocodes = get_all_promocodes()
        
        text = "🏷️ <b>Управление промокодами</b>\n\n"
        text += "Задайте промокод, скидку и количество купонов.\n\n"
        text += "Формат: <code>КОД СКИДКА КОЛИЧЕСТВО</code>\n"
        text += "Пример: <code>NEW2026 20% 100</code>\n\n"
        text += "<b>Все промокоды:</b>\n"

        if promocodes:
            for promo in promocodes:
                status = "✅" if promo['is_active'] else "🔴"
                remaining = promo['total_uses'] - promo['used_count']
                if remaining < 0:
                    remaining = 0
                exhausted = " (исчерпан)" if promo['used_count'] >= promo['total_uses'] else ""
                discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} ₽"
                text += f"{status} <code>{promo['code']}</code> — {discount_str} (осталось: {remaining}/{promo['total_uses']}{exhausted})\n"
        else:
            text += "Нет промокодов\n"

        markup = InlineKeyboardMarkup(row_width=1)
        for promo in promocodes:
            status = "✅" if promo['is_active'] else "🔴"
            remaining = promo['total_uses'] - promo['used_count']
            if remaining < 0:
                remaining = 0
            discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} ₽"
            button_text = f"{status} {promo['code']} ({discount_str}, осталось: {remaining})"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"promo:info:{promo['id']}"))

        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_start_menu"))
        
        # Устанавливаем состояние для обработки ответа
        try:
            # Проверяем, что message имеет атрибут message_id (не FakeMessage)
            if hasattr(message, 'message_id') and message.message_id:
                msg = bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
            else:
                # Если это FakeMessage или нет message_id, отправляем новое сообщение
                msg = bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='HTML')
        except Exception as send_error:
            logger.error(f"[PROMO] Ошибка отправки сообщения: {send_error}", exc_info=True)
            # Пробуем отправить без reply_to
            try:
                msg = bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode='HTML')
            except Exception as send_error2:
                logger.error(f"[PROMO] Критическая ошибка отправки: {send_error2}", exc_info=True)
                msg = None
        
        user_promo_admin_state[user_id] = {
            'message_id': msg.message_id if msg else None,
            'chat_id': message.chat.id
        }
        logger.info(f"[PROMO] Состояние установлено: message_id={msg.message_id if msg else None}, chat_id={message.chat.id}")
        
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_command: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке команды /promo")
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("promo:info:"))
def promo_info_callback(call):
    """Обработчик просмотра информации о промокоде"""
    try:
        bot.answer_callback_query(call.id)
        promocode_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        # Получаем свежие данные о промокоде
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
            bot.answer_callback_query(call.id, "❌ Промокод не найден", show_alert=True)
            return
        
        # Парсим строку или dict
        if isinstance(row, dict):
            code = row['code']
            discount_type = row['discount_type']
            discount_value = float(row['discount_value'])
            total_uses = row['total_uses']
            used_count = row['used_count']
            is_active = bool(row['is_active'])
        else:
            code = row.get("code") if isinstance(row, dict) else (row[0] if row else None)
            discount_type = row[1]
            discount_value = float(row[2])
            total_uses = row[3]
            used_count = row[4]
            is_active = bool(row[5])
        
        discount_str = f"{discount_value}%" if discount_type == 'percent' else f"{int(discount_value)} руб/звезд"
        remaining = max(0, total_uses - used_count)
        status_text = "✅ Активен" if is_active else "🔴 Деактивирован"
        
        text = f"🏷️ <b>Промокод: {code}</b>\n\n"
        text += f"Скидка: {discount_str}\n"
        text += f"Использовано: {used_count}/{total_uses}\n"
        text += f"Осталось: {remaining}\n"
        text += f"Статус: {status_text}\n"
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Кнопка активации/деактивации — в зависимости от текущего статуса
        if is_active:
            markup.add(InlineKeyboardButton("🔴 Деактивировать", callback_data=f"promo:deactivate:{promocode_id}"))
        else:
            markup.add(InlineKeyboardButton("✅ Активировать", callback_data=f"promo:activate:{promocode_id}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад к списку", callback_data="promo:back_to_list"))
        
        # Редактируем текущее сообщение
        bot.edit_message_text(
            text=text,
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
            
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_info_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("promo:deactivate:"))
def promo_deactivate_callback(call):
    """Обработчик деактивации промокода"""
    try:
        promocode_id = int(call.data.split(":")[2])
        user_id = call.from_user.id
        
        # Проверяем права доступа
        owner_id = get_bot_owner_id()
        if owner_id and user_id != owner_id:
            bot.answer_callback_query(call.id, "❌ У вас нет доступа", show_alert=True)
            return
        
        # Деактивируем промокод
        success, message = deactivate_promocode(promocode_id)
        
        if success:
            bot.answer_callback_query(call.id, "✅ Промокод деактивирован", show_alert=False)
            # Обновляем текущее сообщение с новой информацией
            promo_info_callback(call)
        else:
            bot.answer_callback_query(call.id, f"❌ {message}", show_alert=True)
            
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_deactivate_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "promo:back")
def promo_back_callback(call):
    """Обработчик возврата к списку промокодов"""
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
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
        
        # Редактируем текущее сообщение
        bot.edit_message_text(
            text=text,
            chat_id=chat_id,
            message_id=call.message.message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_back_callback: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
        
@bot.callback_query_handler(func=lambda call: call.data.startswith("promo:activate:"))
def promo_activate_callback(call):
    try:
        bot.answer_callback_query(call.id, "✅ Промокод активирован")
        promocode_id = int(call.data.split(":")[2])
        
        from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
        conn = get_db_connection()
        cursor = get_db_cursor()
        
        with db_lock:
            cursor.execute("UPDATE promocodes SET is_active = TRUE WHERE id = %s", (promocode_id,))
            conn.commit()
        
        promo_info_callback(call)  # Обновляем карточку
    except Exception as e:
        logger.error(f"[PROMO] Ошибка активации: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "promo:back_to_list")
def promo_back_to_list_callback(call):
    """Возврат к списку всех промокодов из карточки промокода"""
    try:
        bot.answer_callback_query(call.id)
        
        # Используем ту же логику, что и в promo_command
        promocodes = get_all_promocodes()
        
        text = "🏷️ <b>Управление промокодами</b>\n\n"
        text += "Задайте промокод, скидку и количество купонов.\n\n"
        text += "Формат: <code>КОД СКИДКА КОЛИЧЕСТВО</code>\n"
        text += "Пример: <code>NEW2026 20% 100</code>\n\n"
        text += "<b>Все промокоды:</b>\n"
        
        if promocodes:
            for promo in promocodes:
                status = "✅" if promo.get('is_active', True) else "🔴"
                remaining = max(0, promo['total_uses'] - promo['used_count'])
                exhausted = " (исчерпан)" if promo['used_count'] >= promo['total_uses'] else ""
                discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} ₽"
                text += f"{status} <code>{promo['code']}</code> — {discount_str} (осталось: {remaining}/{promo['total_uses']}{exhausted})\n"
        else:
            text += "Нет промокодов\n"

        markup = InlineKeyboardMarkup(row_width=1)
        for promo in promocodes:
            status = "✅" if promo.get('is_active', True) else "🔴"
            remaining = max(0, promo['total_uses'] - promo['used_count'])
            discount_str = f"{promo['discount_value']}%" if promo['discount_type'] == 'percent' else f"{int(promo['discount_value'])} ₽"
            button_text = f"{status} {promo['code']} ({discount_str}, осталось: {remaining})"
            if len(button_text) > 60:
                button_text = button_text[:57] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"promo:info:{promo['id']}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data="back_to_start_menu"))
        
        bot.edit_message_text(
            text=text,
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )
        
    except Exception as e:
        logger.error(f"[PROMO] Ошибка в promo_back_to_list_callback: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)

# ────────────────────────────────────────────────────────────────
# Добавляем в конец файла promo.py (после всех существующих функций)

@bot.message_handler(func=lambda m: m.from_user.id in user_promo_admin_state)
def handle_promo_admin_text(message):
    """
    Обрабатывает текст после команды /promo (создание промокода)
    """
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()

    logger.info(f"[PROMO ADMIN TEXT] Получен от {user_id}: '{text}'")

    # Выход по отмене
    if text.lower() in ['отмена', 'cancel', 'выход', '/cancel']:
        bot.reply_to(message, "Ввод промокода отменён.")
        user_promo_admin_state.pop(user_id, None)
        return

    # Ожидаемый формат: КОД СКИДКА КОЛИЧЕСТВО
    # Примеры: DIM 95% 1    SALE 500 50
    try:
        parts = text.split(maxsplit=2)
        if len(parts) != 3:
            raise ValueError("Нужно ровно 3 части: КОД СКИДКА КОЛИЧЕСТВО")

        code = parts[0].strip().upper()
        discount_str = parts[1].strip()
        total_uses_str = parts[2].strip()

        total_uses = int(total_uses_str)

        # Парсинг скидки
        if discount_str.endswith('%'):
            discount_type = 'percent'
            discount_value = float(discount_str[:-1])
        else:
            discount_type = 'fixed'
            discount_value = float(discount_str)

        if discount_value <= 0:
            raise ValueError("Скидка должна быть больше 0")

        # Создаём промокод (используем существующую функцию)
        new_promo = create_promocode(
            code=code,
            discount_input=f"{discount_value}{'%' if discount_type == 'percent' else ''}",
            total_uses=total_uses
        )

        discount_display = f"{new_promo['discount_value']}%" if new_promo['discount_type'] == 'percent' else f"{int(new_promo['discount_value'])} ₽"

        response = (
            "✅ Промокод успешно создан!\n\n"
            f"Код: <code>{new_promo['code']}</code>\n"
            f"Скидка: {discount_display}\n"
            f"Количество использований: {new_promo['total_uses']}"
        )

        bot.reply_to(message, response, parse_mode='HTML')

        # Удаляем состояние после успешного создания
        user_promo_admin_state.pop(user_id, None)

    except ValueError as ve:
        bot.reply_to(
            message,
            f"❌ Неверный формат.\n\n{str(ve)}\n\n"
            "Пример правильного ввода:\n"
            "<code>DIM 95% 1</code>\n"
            "<code>SALE 500 50</code>\n\n"
            "Или напишите 'отмена'",
            parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"[PROMO ADMIN TEXT] Ошибка при создании: {e}", exc_info=True)
        bot.reply_to(message, "❌ Ошибка при создании промокода. Попробуйте позже.")

def register_promo_handlers(bot):
    """
    Регистрация всех handlers из promo.py
    Вызывается один раз при старте бота
    """
    # Здесь ничего не нужно писать — все @bot.message_handler и @bot.callback_query_handler
    # уже зарегистрированы автоматически при импорте файла
    pass