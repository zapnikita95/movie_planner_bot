# moviebot/bot/callbacks/ticket_callbacks.py

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.states import user_ticket_state
from moviebot.utils.helpers import has_tickets_access
from moviebot.bot.bot_init import bot
import logging

logger = logging.getLogger(__name__)

# 1. Основной хэндлер — нажатие "Добавить билеты" после планирования
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("add_ticket:"))
def add_ticket_from_plan_callback(call):
    logger.info(f"[TICKET CALLBACK] 🔥 add_ticket сработал: data='{call.data}', user_id={call.from_user.id}, chat_id={call.message.chat.id}")

    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        try:
            bot.answer_callback_query(call.id, "Открываю загрузку билетов...")  # видимый тултип
        except Exception as answer_error:
            # Игнорируем ошибку устаревшего callback query
            if "query is too old" in str(answer_error) or "query ID is invalid" in str(answer_error):
                logger.warning(f"[TICKET CALLBACK] Callback query устарел, продолжаем без answer: {answer_error}")
            else:
                logger.error(f"[TICKET CALLBACK] Ошибка answer_callback_query: {answer_error}", exc_info=True)

        try:
            plan_id = int(call.data.split(":")[1])
            logger.info(f"[TICKET CALLBACK] Обработка plan_id={plan_id}")
        except Exception as parse_error:
            logger.error(f"[TICKET CALLBACK] Ошибка парсинга plan_id: {parse_error}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            except:
                pass
            return

        if not has_tickets_access(chat_id, user_id):
            logger.warning(f"[TICKET CALLBACK] У пользователя {user_id} нет доступа к билетам")
            try:
                bot.answer_callback_query(
                    call.id,
                    "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
                    show_alert=True
                )
            except:
                pass
            return

        # Состояние
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',
            'plan_id': plan_id,
            'chat_id': chat_id
        }

        # Клавиатура с отменой
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data=f"cancel_ticket_upload:{plan_id}"))

        text = "🎟️ <b>Загрузка билетов</b>\n\nОтправьте фото или файл с билетом(ами).\n\n💡 В группе отправьте в ответ на это сообщение, в личке можно отправить следующим сообщением."
        
        try:
            sent_msg = bot.edit_message_text(
                chat_id=chat_id,
                message_id=message_id,
                text=text,
                parse_mode='HTML',
                reply_markup=markup
            )
            # Сохраняем message_id для проверки реплаев в групповых чатах
            user_ticket_state[user_id]['prompt_message_id'] = message_id
        except Exception as edit_error:
            logger.error(f"[TICKET CALLBACK] Ошибка редактирования сообщения: {edit_error}", exc_info=True)
            # Пытаемся отправить новое сообщение
            try:
                send_kwargs = {'text': text, 'chat_id': chat_id, 'reply_markup': markup, 'parse_mode': 'HTML'}
                if message_thread_id is not None:
                    send_kwargs['message_thread_id'] = message_thread_id
                sent_msg = bot.send_message(**send_kwargs)
                # Сохраняем message_id для проверки реплаев в групповых чатах
                if sent_msg:
                    user_ticket_state[user_id]['prompt_message_id'] = sent_msg.message_id
            except Exception as send_error:
                logger.error(f"[TICKET CALLBACK] Критическая ошибка отправки сообщения: {send_error}", exc_info=True)

        logger.info(f"[TICKET CALLBACK] Начато добавление билетов к plan_id={plan_id}, user_id={user_id}, chat_id={chat_id}")

    except Exception as e:
        logger.error(f"[TICKET CALLBACK] Критическая ошибка в add_ticket: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Произошла ошибка", show_alert=True)
        except:
            pass


# 2. Кнопка "Добавить ещё билет"
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

    user_ticket_state[user_id] = {
        'step': 'add_more_tickets',
        'plan_id': plan_id,
        'chat_id': chat_id
    }

    try:
        sent_msg = bot.edit_message_text(
            chat_id=chat_id,
            message_id=call.message.message_id,
            text="➕ <b>Добавляем ещё билеты</b>\n\n"
                 "Отправьте дополнительные фото или файлы с билетами.\n\n💡 В группе отправьте в ответ на это сообщение, в личке можно отправить следующим сообщением.",
            parse_mode='HTML'
        )
        # Сохраняем message_id для проверки реплаев в групповых чатах
        user_ticket_state[user_id]['prompt_message_id'] = call.message.message_id
    except Exception as edit_error:
        logger.error(f"[TICKET CALLBACK] Ошибка редактирования сообщения в add_more_tickets: {edit_error}", exc_info=True)
        # Пытаемся отправить новое сообщение
        try:
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            send_kwargs = {
                'text': "➕ <b>Добавляем ещё билеты</b>\n\nОтправьте дополнительные фото или файлы с билетами.\n\n💡 В группе отправьте в ответ на это сообщение, в личке можно отправить следующим сообщением.",
                'chat_id': chat_id,
                'parse_mode': 'HTML'
            }
            if message_thread_id is not None:
                send_kwargs['message_thread_id'] = message_thread_id
            sent_msg = bot.send_message(**send_kwargs)
            # Сохраняем message_id для проверки реплаев в групповых чатах
            if sent_msg:
                user_ticket_state[user_id]['prompt_message_id'] = sent_msg.message_id
        except Exception as send_error:
            logger.error(f"[TICKET CALLBACK] Критическая ошибка отправки сообщения в add_more_tickets: {send_error}", exc_info=True)

    logger.info(f"[TICKET] Перешли в режим add_more_tickets для plan_id={plan_id}")


# 3. Кнопка "⬅️ К списку мероприятий"
@bot.callback_query_handler(func=lambda call: call.data == "ticket_new")
def back_to_ticket_list(call):
    logger.info(f"[TICKET CALLBACK] ticket_new (возврат к списку) сработал, user_id={call.from_user.id}")

    bot.answer_callback_query(call.id)

    user_id = call.from_user.id
    chat_id = call.message.chat.id

    if user_id in user_ticket_state:
        del user_ticket_state[user_id]
        logger.info(f"[TICKET] Состояние очищено при возврате к списку")

    # Ленивый импорт — безопасно, без цикла
    from moviebot.bot.handlers.series import show_cinema_sessions

    show_cinema_sessions(chat_id, user_id, None)


# 4. Заблокированная кнопка
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("ticket_locked:"))
def handle_ticket_locked(call):
    bot.answer_callback_query(
        call.id,
        "🎫 Загрузка билетов доступна только с подпиской «Билеты» или «Все режимы».\nПодключите через /payment",
        show_alert=True
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_ticket_upload:"))
def cancel_ticket_upload(call):
    try:
        bot.answer_callback_query(call.id)

        user_id = call.from_user.id
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]

        bot.edit_message_text(
            "Загрузка билетов отменена.",
            call.message.chat.id,
            call.message.message_id
        )
    except Exception as e:
        logger.error(f"[CANCEL TICKET] Ошибка: {e}")