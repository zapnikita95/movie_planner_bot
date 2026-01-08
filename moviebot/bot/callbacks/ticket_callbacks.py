# moviebot/bot/callbacks/ticket_callbacks.py

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.states import user_ticket_state
from moviebot.utils.helpers import has_tickets_access
from moviebot.bot.bot_init import bot  # Импортируем bot оттуда же, откуда все
import logging

logger = logging.getLogger(__name__)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_ticket:"))
def add_ticket_from_plan_callback(call):
    logger.info(f"[ADD TICKET CALLBACK] 🔥 СРАБОТАЛ! data='{call.data}', user_id={call.from_user.id}")

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

        # Устанавливаем состояние — используем 'upload_ticket', потому что в main_file_handler есть обработка для него
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',
            'plan_id': plan_id,
            'chat_id': chat_id
        }

        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))

        # Начальное сообщение — просто приглашение загрузить билеты
        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="🎟️ <b>Загрузка билетов</b>\n\n"
                 "Отправьте фото или файл с билетом(ами).\n"
                 "Можно несколько сообщений подряд.",
            parse_mode='HTML'
        )

        # Состояние — используем 'upload_ticket' для первого добавления
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',
            'plan_id': plan_id,
            'chat_id': chat_id
        }

        logger.info(f"[ADD TICKET] Начато добавление билетов для plan_id={plan_id} (ожидается первый файл)")

    except Exception as e:
        logger.error(f"[ADD TICKET CALLBACK] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)


# Также добавляем обработчик заблокированной кнопки (чтобы не было "крутилки")
@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_locked:"))
def handle_ticket_locked(call):
    bot.answer_callback_query(
        call.id,
        "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
        show_alert=True
    )