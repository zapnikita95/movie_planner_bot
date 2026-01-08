# moviebot/bot/handlers/ticket_callbacks.py
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.states import user_ticket_state
from moviebot.utils.helpers import has_tickets_access
import logging

logger = logging.getLogger(__name__)

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_ticket:"))
def add_ticket_from_plan_callback(call):
    """
    Самый приоритетный хэндлер — добавление билетов после планирования в кино.
    Должен быть зарегистрирован ПЕРВЫМ среди всех callback_query_handler'ов!
    """
    logger.info(f"[ADD TICKET FROM PLAN] 🔥 CALLBACK ПОЛУЧЕН: data='{call.data}', user_id={call.from_user.id}, message_id={call.message.message_id}")

    try:
        bot.answer_callback_query(call.id)  # Убираем крутилку сразу

        user_id = call.from_user.id
        chat_id = call.message.chat.id

        # Парсим plan_id
        try:
            plan_id = int(call.data.split(":")[1])
        except:
            logger.error(f"[ADD TICKET] Не удалось распарсить plan_id из {call.data}")
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            return

        # Проверка подписки
        if not has_tickets_access(chat_id, user_id):
            bot.answer_callback_query(
                call.id,
                "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
                show_alert=True
            )
            return

        # Устанавливаем состояние — используем тот шаг, который уже ловится в main_file_handler
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',  # или 'waiting_ticket_file' — главное совпадение с main_file_handler
            'plan_id': plan_id,
            'chat_id': chat_id
        }

        # Красивое сообщение с инструкцией
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))

        bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="🎟️ <b>Загрузка билетов</b>\n\n"
                 "Отправьте фото или файлы с билетами.\n"
                 "Можно несколько сообщений подряд.\n\n"
                 "Когда закончите — напишите <code>готово</code>.",
            reply_markup=markup,
            parse_mode='HTML'
        )

        logger.info(f"[ADD TICKET] Состояние upload_ticket установлено для plan_id={plan_id}, user_id={user_id}")

    except Exception as e:
        logger.error(f"[ADD TICKET FROM PLAN] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)