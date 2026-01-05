"""
Единый главный обработчик для всех текстовых сообщений
Обрабатывает состояния, реплаи, ссылки на Кинопоиск и т.д.
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_user_timezone_or_default, set_notification_setting
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import extract_movie_info, search_films
from moviebot.bot.bot_init import bot as bot_instance
from moviebot.states import (
    user_search_state, user_plan_state, user_ticket_state,
    user_settings_state, user_edit_state, user_view_film_state,
    user_import_state, user_clean_state, user_cancel_subscription_state,
    bot_messages, plan_error_messages, list_messages, added_movie_messages
)
from moviebot.utils.parsing import parse_session_time, extract_kp_id_from_text
from moviebot.bot.handlers.series import search_films_with_type, show_film_info_with_buttons
from moviebot.bot.handlers.list import handle_view_film_reply_internal
from moviebot.database.db_operations import add_and_announce
from moviebot.bot.bot_init import BOT_ID

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot_instance.message_handler(content_types=['text'], func=lambda m: not (m.text and m.text.strip().startswith('/')))
def main_text_handler(message):
    """Единый главный хэндлер для всех текстовых сообщений (исключая команды)"""
    logger.info(f"[MAIN TEXT HANDLER] Получено текстовое сообщение от {message.from_user.id}: '{message.text[:100] if message.text else ''}'")
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip() if message.text else ""
    
    # 1. Проверяем состояния (ticket, settings, plan, edit, search, view_film)
    
    # === user_ticket_state ===
    if user_id in user_ticket_state:
        state = user_ticket_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_ticket_state, step={step}")
        
        if step == 'waiting_new_session':
            # Обработка ввода нового сеанса (фильм + дата)
            from moviebot.bot.handlers.series import handle_new_session_input_internal
            handle_new_session_input_internal(message, state)
            return
        
        if step == 'upload_ticket':
            # Если ждём билеты, но пришёл текст (например "готово")
            if text.lower().strip() == 'готово':
                from moviebot.bot.handlers.series import ticket_done_internal
                ticket_done_internal(message, state)
                return
            # Иначе игнорируем текст (билеты обрабатываются отдельным хэндлером для фото/документов)
            logger.info(f"[MAIN TEXT HANDLER] Игнорируем текст в режиме upload_ticket (ожидаются фото/документы)")
            return
        
        if step == 'waiting_session_time':
            from moviebot.bot.handlers.series import handle_edit_ticket_text_internal
            handle_edit_ticket_text_internal(message, state)
            return
    
    # === user_search_state ===
    if user_id in user_search_state:
        state = user_search_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_search_state")
        
        # Обработка ответа на /search без запроса
        # Проверяем, что это ответ на сообщение бота или просто текст от пользователя в состоянии поиска
        saved_message_id = state.get('message_id')
        is_reply_to_search = message.reply_to_message and message.reply_to_message.message_id == saved_message_id
        is_text_in_search_state = text and not message.reply_to_message  # Текст без ответа, но в состоянии поиска
        
        if is_reply_to_search or is_text_in_search_state:
            query = text
            if query:
                # Получаем тип поиска из состояния
                search_type = state.get('search_type', 'mixed')
                # Удаляем состояние
                del user_search_state[user_id]
                # Вызываем обработчик поиска
                logger.info(f"[SEARCH] Поиск по запросу '{query}' от пользователя {user_id}, тип: {search_type}")
                films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
                if not films:
                    bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
                    return
                
                # Формируем сообщение с результатами
                results_text = f"🔍 Результаты поиска '{query}':\n\n"
                markup = InlineKeyboardMarkup(row_width=1)
                
                for film in films[:10]:  # Показываем максимум 10 результатов на странице
                    title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                    year = film.get('year') or film.get('releaseYear') or 'N/A'
                    rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                    kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                    
                    # Определяем тип (сериал или фильм) по полю type из API
                    film_type = film.get('type', '').upper()  # "FILM" или "TV_SERIES"
                    is_series = film_type == 'TV_SERIES'
                    type_indicator = "📺" if is_series else "🎬"
                    
                    if kp_id:
                        # Ограничиваем длину текста кнопки
                        type_indicator = "📺" if is_series else "🎬"
                        button_text = f"{type_indicator} {title} ({year})"
                        if len(button_text) > 50:
                            button_text = button_text[:47] + "..."
                        results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                        if rating != 'N/A':
                            results_text += f" ⭐ {rating}"
                        results_text += "\n"
                        # Сохраняем тип в callback_data для правильного формирования ссылки
                        markup.add(InlineKeyboardButton(button_text, callback_data=f"add_film_{kp_id}:{film_type}"))
                
                # Добавляем пагинацию, если нужно
                if total_pages > 1:
                    pagination_row = []
                    query_encoded = query.replace(' ', '_')
                    pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
                    if total_pages > 1:
                        pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
                    markup.row(*pagination_row)
                
                # Добавляем кнопку "Назад в меню"
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                # Добавляем пояснение про эмодзи
                results_text += "\n\n🎬 - фильм\n📺 - сериал"
                
                bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
                logger.info(f"✅ Ответ на /search отправлен пользователю {user_id}, найдено {len(films)} результатов")
            else:
                logger.warning(f"[SEARCH] Пустой запрос от пользователя {user_id}")
            return
        else:
            logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано: '{text}' (reply_to_message_id={message.reply_to_message.message_id if message.reply_to_message else None}, saved_message_id={saved_message_id})")
    
    # === user_import_state ===
    if user_id in user_import_state:
        state = user_import_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_import_state, step={step}")
        
        if step == 'waiting_user_id':
            from moviebot.bot.handlers.series import handle_import_user_id_internal
            handle_import_user_id_internal(message, state)
            return
    
    # === user_edit_state ===
    if user_id in user_edit_state:
        state = user_edit_state[user_id]
        action = state.get('action')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_edit_state, action={action}")
        
        if action == 'edit_rating':
            from moviebot.bot.handlers.rate import handle_edit_rating_internal
            handle_edit_rating_internal(message, state)
            return
        
        if action == 'edit_plan_datetime':
            from moviebot.bot.handlers.plan import handle_edit_plan_datetime_internal
            handle_edit_plan_datetime_internal(message, state)
            return
    
    # === user_settings_state ===
    if user_id in user_settings_state:
        state = user_settings_state.get(user_id)
        if state.get('waiting_notify_time'):
            time_str = message.text.strip()
            try:
                if ':' in time_str:
                    parts = time_str.split(':')
                    if len(parts) == 2:
                        hour = int(parts[0])
                        minute = int(parts[1])
                        if 0 <= hour <= 23 and 0 <= minute <= 59:
                            notify_type = state.get('waiting_notify_time')
                            
                            if notify_type == 'home' or notify_type.startswith('home_'):
                                if notify_type == 'home':
                                    set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'home_weekday':
                                    set_notification_setting(chat_id, 'notify_home_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (будни) установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'home_weekend':
                                    set_notification_setting(chat_id, 'notify_home_weekend_hour', hour)
                                    set_notification_setting(chat_id, 'notify_home_weekend_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для домашнего просмотра (выходные) установлено: {hour:02d}:{minute:02d}")
                            
                            elif notify_type == 'cinema' or notify_type.startswith('cinema_'):
                                if notify_type == 'cinema':
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'cinema_weekday':
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekday_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (будни) установлено: {hour:02d}:{minute:02d}")
                                elif notify_type == 'cinema_weekend':
                                    set_notification_setting(chat_id, 'notify_cinema_weekend_hour', hour)
                                    set_notification_setting(chat_id, 'notify_cinema_weekend_minute', minute)
                                    bot_instance.reply_to(message, f"✅ Время напоминаний для просмотра в кино (выходные) установлено: {hour:02d}:{minute:02d}")
                            
                            if user_id in user_settings_state:
                                del user_settings_state[user_id]
                            return
                        else:
                            bot_instance.reply_to(message, "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)")
                            return
            except ValueError:
                bot_instance.reply_to(message, "❌ Неверный формат времени. Используйте ЧЧ:ММ (например, 19:00 или 09:00)")
                return
            except Exception as e:
                logger.error(f"[SETTINGS] Ошибка при сохранении времени напоминаний: {e}", exc_info=True)
                bot_instance.reply_to(message, "❌ Произошла ошибка при сохранении времени.")
                if user_id in user_settings_state:
                    del user_settings_state[user_id]
                return
        
        if message.reply_to_message:
            settings_msg_id = state.get('settings_msg_id')
            if settings_msg_id and message.reply_to_message.message_id == settings_msg_id:
                if state.get('adding_reactions'):
                    from moviebot.bot.handlers.series import handle_settings_emojis
                    handle_settings_emojis(message)
                    return
    
    # === user_view_film_state ===
    if user_id in user_view_film_state:
        state = user_view_film_state[user_id]
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_view_film_state")
        handle_view_film_reply_internal(message, state)
        return
    
    # === user_plan_state ===
    if user_id in user_plan_state:
        state = user_plan_state[user_id]
        step = state.get('step')
        logger.info(f"[MAIN TEXT HANDLER] Пользователь {user_id} в user_plan_state, step={step}")
        
        if step == 1:
            from moviebot.bot.handlers.plan import get_plan_link_internal
            get_plan_link_internal(message, state)
            return
        
        if step == 3:
            from moviebot.bot.handlers.plan import get_plan_day_or_date_internal
            get_plan_day_or_date_internal(message, state)
            return
    
    # === user_clean_state ===
    if user_id in user_clean_state:
        if text.upper().strip() == 'ДА, УДАЛИТЬ':
            from moviebot.bot.handlers.series import handle_clean_confirm_internal
            handle_clean_confirm_internal(message)
            return
    
    # === user_cancel_subscription_state ===
    if user_id in user_cancel_subscription_state:
        state = user_cancel_subscription_state.get(user_id)
        if state:
            state_chat_id = state.get('chat_id')
            if state_chat_id and message.chat.id != state_chat_id:
                return
            
            is_reply = (message.reply_to_message and 
                       message.reply_to_message.from_user and 
                       message.reply_to_message.from_user.id == BOT_ID)
            
            if text.upper().strip() == 'ДА, ОТМЕНИТЬ':
                from moviebot.database.db_operations import cancel_subscription
                subscription_id = state.get('subscription_id')
                subscription_type = state.get('subscription_type')
                
                if subscription_id:
                    if cancel_subscription(subscription_id, user_id):
                        if subscription_type == 'group':
                            bot_instance.reply_to(message, "✅ <b>Групповая подписка отменена</b>\n\nВаша групповая подписка была успешно отменена.", parse_mode='HTML')
                        else:
                            bot_instance.reply_to(message, "✅ <b>Личная подписка отменена</b>\n\nВаша личная подписка была успешно отменена.", parse_mode='HTML')
                        del user_cancel_subscription_state[user_id]
                    else:
                        bot_instance.reply_to(message, "❌ Ошибка отмены подписки. Попробуйте позже.", parse_mode='HTML')
                        del user_cancel_subscription_state[user_id]
                return
    
    # 2. Обработка реплаев
    
    # Реплай на сообщение бота с оценками
    if message.reply_to_message and message.reply_to_message.from_user.id == BOT_ID:
        reply_text = message.reply_to_message.text or ""
        
        if "Список просмотренных фильмов для оценки" in reply_text:
            from moviebot.bot.handlers.rate import handle_rate_list_reply_internal
            handle_rate_list_reply_internal(message)
            return
        
        reply_msg_id = message.reply_to_message.message_id
        if reply_msg_id in bot_messages:
            link = bot_messages.get(reply_msg_id)
            if link:
                from moviebot.bot.handlers.series import handle_random_plan_reply_internal
                handle_random_plan_reply_internal(message, link)
                return
    
    # Реплай на сообщение с ошибкой планирования
    if message.reply_to_message and message.reply_to_message.message_id in plan_error_messages:
        from moviebot.bot.handlers.plan import handle_plan_error_reply_internal
        handle_plan_error_reply_internal(message)
        return
    
    # Реплай на сообщение с оценкой
    if message.reply_to_message and message.text:
        text_stripped = message.text.strip()
        if (len(text_stripped) == 1 and text_stripped.isdigit() and 1 <= int(text_stripped) <= 9) or \
           (len(text_stripped) == 2 and text_stripped == "10"):
            rating = int(text_stripped)
            from moviebot.bot.handlers.rate import handle_rating_internal
            handle_rating_internal(message, rating)
            return
    
    # Реплай на голосование "в кино"
    if message.reply_to_message and text.lower() in ['да', 'нет']:
        from moviebot.bot.handlers.plan import handle_cinema_vote_internal
        handle_cinema_vote_internal(message, text.lower())
        return
    
    # Реплай на список фильмов
    if message.reply_to_message and message.reply_to_message.message_id in list_messages:
        from moviebot.bot.handlers.list import handle_list_reply_internal
        handle_list_reply_internal(message)
        return
    
    # 3. Обычные сообщения с фильмами (если нет состояния)
    
    # Сообщения с ссылками на Кинопоиск
    if 'kinopoisk.ru' in text or 'kinopoisk.com' in text:
        link_match = re.search(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', text)
        if link_match:
            link = link_match.group(1)
            # Сохраняем ссылку в bot_messages для обработки реакций
            bot_messages[message.message_id] = link
            logger.info(f"[MAIN TEXT HANDLER] Ссылка сохранена в bot_messages для message_id={message.message_id}: {link}")
            
            # Добавляем фильм в базу
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, 'add_movie', chat_id)
            
            added_count = 0
            links = re.findall(r'(https?://[\w\./-]*(?:kinopoisk\.ru|kinopoisk\.com)/(?:film|series)/\d+)', text)
            for link_item in links:
                if add_and_announce(link_item, chat_id):
                    added_count += 1
                    logger.info(f"[MAIN TEXT HANDLER] Фильм обработан: {link_item}")
            
            if added_count > 1:
                bot_instance.send_message(chat_id, f"🎉 Добавлено {added_count} новых фильмов в базу!")
            return
    
    # Сообщения с entities (URL в тексте)
    if message.entities:
        links = []
        for entity in message.entities:
            if entity.type == 'url':
                link = text[entity.offset:entity.offset + entity.length]
                if 'kinopoisk.ru' in link or 'kinopoisk.com' in link:
                    links.append(link)
        
        if links:
            for link in links:
                bot_messages[message.message_id] = link
                if add_and_announce(link, chat_id):
                    logger.info(f"[MAIN TEXT HANDLER] Фильм обработан через entities: {link}")
            return
    
    # Если ничего не подошло - игнорируем
    logger.info(f"[MAIN TEXT HANDLER] Сообщение не обработано: '{text[:50]}'")


def register_text_message_handlers(bot_instance):
    """Регистрирует обработчики текстовых сообщений"""
    # Обработчик уже зарегистрирован через декоратор
    logger.info("Обработчики текстовых сообщений зарегистрированы")

