# moviebot/bot/callbacks/ticket_callbacks.py

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
import time
from moviebot.states import user_ticket_state
from moviebot.utils.helpers import has_ticket_features_access, has_pro_access, maybe_send_ticket_limit_message
from moviebot.bot.bot_init import bot
import logging

logger = logging.getLogger(__name__)

# 1. Основной хэндлер — нажатие "Добавить билеты" после планирования
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("series_mark_episode:"))
def series_mark_episode_early_callback(call):
    """Ранний обработчик «Отметить серию» — ticket_callbacks грузится первым, чтобы callback не терялся."""
    logger.info(f"[SERIES MARK EPISODE EARLY] Вызван callback: {call.data}")
    from moviebot.bot.callbacks.series_callbacks import _handle_series_mark_episode
    _handle_series_mark_episode(call)


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

        if not has_ticket_features_access(chat_id, user_id):
            bot.answer_callback_query(call.id)
            maybe_send_ticket_limit_message(bot, chat_id, user_id, message_thread_id)
            return

        # Состояние (TTL 15 мин)
        user_ticket_state[user_id] = {
            'step': 'upload_ticket',
            'plan_id': plan_id,
            'chat_id': chat_id,
            'created_at': time.time()
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


# 2. Кнопка "Добавить ещё билет" (требуется 💎 Movie Planner PRO)
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

    if not has_pro_access(chat_id, user_id):
        try:
            bot.answer_callback_query(
                call.id,
                "➕ Добавление билетов доступно с подпиской 💎 Movie Planner PRO. Подключите через /payment",
                show_alert=True
            )
        except:
            pass
        return

    user_ticket_state[user_id] = {
        'step': 'add_more_tickets',
        'plan_id': plan_id,
        'chat_id': chat_id,
        'created_at': time.time()
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


# 3. Кнопка "⬅️ Назад к событиям" - возвращает к списку событий
@bot.callback_query_handler(func=lambda call: call.data == "ticket_back_to_list")
def ticket_back_to_list_callback(call):
    logger.info(f"[TICKET CALLBACK] ticket_back_to_list сработал, user_id={call.from_user.id}")
    
    bot.answer_callback_query(call.id)
    
    user_id = call.from_user.id
    chat_id = call.message.chat.id
    
    if user_id in user_ticket_state:
        del user_ticket_state[user_id]
        logger.info(f"[TICKET] Состояние очищено при возврате к списку")
    
    # Ленивый импорт — безопасно, без цикла
    from moviebot.bot.handlers.series import show_cinema_sessions
    
    show_cinema_sessions(chat_id, user_id, None)


# 4. Кнопка "⬅️ К списку мероприятий" / "➕ Добавить новое событие" - показывает список мероприятий
@bot.callback_query_handler(func=lambda call: call.data.startswith("ticket_new"))
def ticket_new_callback(call):
    """Обработчик кнопки 'К списку мероприятий' - показывает список запланированных сеансов (как /ticket)"""
    logger.info(f"[TICKET CALLBACK] ticket_new (список мероприятий) сработал, user_id={call.from_user.id}, data={call.data}")
    
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # Парсим file_id, если есть (формат: ticket_new:file_id)
        parts = call.data.split(":")
        file_id = parts[1] if len(parts) > 1 else None
        
        # Очищаем состояние, если есть
        if user_id in user_ticket_state:
            del user_ticket_state[user_id]
            logger.info(f"[TICKET] Состояние очищено при переходе к списку мероприятий")
        
        # Показываем список мероприятий (как команда /ticket)
        from moviebot.bot.handlers.series import show_cinema_sessions
        show_cinema_sessions(chat_id, user_id, file_id)
        
        # Удаляем старое сообщение, если возможно
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass  # Игнорируем ошибки удаления
        
    except Exception as e:
        logger.error(f"[TICKET NEW] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


# 4. Заблокированная кнопка (лимит 3 плана с билетами или группа без PRO)
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("ticket_locked:"))
def handle_ticket_locked(call):
    bot.answer_callback_query(call.id)
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    maybe_send_ticket_limit_message(bot, chat_id, user_id, message_thread_id)

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