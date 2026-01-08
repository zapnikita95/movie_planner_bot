# moviebot/bot/callbacks/ticket_callbacks.py

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.states import user_ticket_state
from moviebot.utils.helpers import has_tickets_access
from moviebot.bot.bot_init import bot
from moviebot.bot.handlers.series import show_cinema_sessions  # Импортируем функцию списка сеансов
import logging

logger = logging.getLogger(__name__)

# 1. Основной хэндлер — нажатие "Добавить билеты" после планирования
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_ticket:"))
def add_ticket_from_plan_callback(call):
    logger.info(f"[TICKET CALLBACK] 🔥 add_ticket сработал: data='{call.data}', user_id={call.from_user.id}")

    try:
        bot.answer_callback_query(call.id)

        user_id = call.from_user.id
        chat_id = call.message.chat.id

        try:
            plan_id = int(call.data.split(":")[1])
        except:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            return

        if not has_tickets_access(chat_id, user_id):
            bot.answer_callback_query(
                call.id,
                "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
                show_alert=True
            )
            return

        # Устанавливаем состояние как при первом добавлении билетов
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',  # Этот шаг уже обрабатывается в main_file_handler
            'plan_id': plan_id,
            'chat_id': chat_id
        }

        # Простое приглашение — без упоминания "готово"
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="🎟️ <b>Загрузка билетов</b>\n\n"
                 "Отправьте фото или файл с билетом(ами).\n"
                 "Можно отправить несколько сообщений подряд.",
            parse_mode='HTML'
        )

        logger.info(f"[TICKET] Начато добавление билетов к plan_id={plan_id}")

    except Exception as e:
        logger.error(f"[TICKET CALLBACK] Ошибка в add_ticket: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)


# 2. Кнопка "Добавить ещё билет" — после первого файла
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_more_tickets:"))
def add_more_tickets_from_plan(call):
    logger.info(f"[TICKET CALLBACK] add_more_tickets сработал: data='{call.data}'")

    bot.answer_callback_query(call.id)

    try:
        plan_id = int(call.data.split(":")[1])
    except:
        bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        return

    user_id = call.from_user.id
    chat_id = call.message.chat.id

    # Переходим в режим продолжения добавления
    user_ticket_state[user_id] = {
        'step': 'add_more_tickets',  # Этот шаг уже есть в main_file_handler
        'plan_id': plan_id,
        'chat_id': chat_id
    }

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text="➕ <b>Добавляем ещё билеты</b>\n\n"
             "Отправьте дополнительные фото или файлы с билетами.",
        parse_mode='HTML'
    )

    logger.info(f"[TICKET] Перешли в режим add_more_tickets для plan_id={plan_id}")


# 3. Кнопка "⬅️ К списку мероприятий" — завершение и возврат в /ticket
@bot.callback_query_handler(func=lambda call: call.data == "ticket_new")
def back_to_ticket_list(call):
    logger.info(f"[TICKET CALLBACK] ticket_new (возврат к списку) сработал, user_id={call.from_user.id}")

    bot.answer_callback_query(call.id)

    user_id = call.from_user.id
    chat_id = call.message.chat.id

    # Очищаем состояние
    if user_id in user_ticket_state:
        del user_ticket_state[user_id]
        logger.info(f"[TICKET] Состояние очищено при возврате к списку")

    # Показываем список сеансов (как при /ticket без файла)
    show_cinema_sessions(chat_id, user_id, None)


# 4. Заблокированная кнопка (для бесплатных пользователей)
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("ticket_locked:"))
def handle_ticket_locked(call):
    bot.answer_callback_query(
        call.id,
        "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
        show_alert=True
    )