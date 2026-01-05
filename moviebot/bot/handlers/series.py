"""
Обработчики команд связанных с сериалами, поиском, рандомом, премьерами, билетами, настройками и помощью
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import (
    log_request, get_user_timezone_or_default, set_user_timezone,
    get_watched_emojis, get_user_timezone, get_notification_settings, set_notification_setting
)
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import search_films, extract_movie_info, get_premieres_for_period, get_seasons_data
from moviebot.utils.helpers import has_tickets_access, has_recommendations_access, has_notifications_access
from moviebot.bot.handlers.seasons import get_series_airing_status, count_episodes_for_watch_check
from moviebot.bot.bot_init import bot as bot_instance
from moviebot.config import KP_TOKEN, PLANS_TZ
import requests
from moviebot.states import (
    user_search_state, user_random_state, user_ticket_state,
    user_settings_state, settings_messages, bot_messages, added_movie_messages,
    dice_game_state, user_import_state
)
from moviebot.utils.parsing import extract_kp_id_from_text, show_timezone_selection, extract_kp_user_id
from datetime import datetime
import pytz
import telebot.types

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


# Обработчик выбора типа поиска (фильм/сериал) - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
# КРИТИЧЕСКИ ВАЖНО: Этот обработчик регистрируется при импорте модуля
# ВАЖНО: Используем bot_instance из импорта, который должен быть тем же объектом, что и bot в register_series_handlers
logger.info("=" * 80)
logger.info(f"[SEARCH TYPE HANDLER] Регистрация обработчика search_type_callback")
logger.info(f"[SEARCH TYPE HANDLER] bot_instance={bot_instance}, type={type(bot_instance)}")
logger.info(f"[SEARCH TYPE HANDLER] id(bot_instance)={id(bot_instance)}")
logger.info("=" * 80)

@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("search_type:"))
def search_type_callback(call):
    """Обработчик выбора типа поиска (фильм или сериал)"""
    logger.info("=" * 80)
    logger.info(f"[SEARCH TYPE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    logger.info(f"[SEARCH TYPE] call.data={call.data}, call.message.message_id={call.message.message_id if call.message else 'N/A'}")
    logger.info(f"[SEARCH TYPE] bot_instance={bot_instance}, type={type(bot_instance)}")
    try:
        # Отвечаем на callback сразу, чтобы убрать "крутилку"
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        search_type = call.data.split(":")[1]  # 'film' или 'series'
        
        logger.info(f"[SEARCH TYPE] Пользователь {user_id} выбрал тип поиска: {search_type}, chat_id={chat_id}")
        
        # Обновляем состояние
        if user_id in user_search_state:
            user_search_state[user_id]['search_type'] = search_type
            user_search_state[user_id]['message_id'] = call.message.message_id
        else:
            user_search_state[user_id] = {
                'chat_id': chat_id,
                'message_id': call.message.message_id,
                'search_type': search_type
            }
        logger.info(f"[SEARCH TYPE] ✅ Состояние обновлено: {user_search_state[user_id]}")
        
        # Обновляем сообщение с указанием выбранного типа (как в старом файле)
        type_text = "🎬 фильмы" if search_type == 'film' else "📺 сериалы" if search_type == 'series' else "🎬📺 фильмы и сериалы"
        
        # Обновляем кнопки, чтобы показать выбранный тип
        markup = InlineKeyboardMarkup(row_width=2)
        if search_type == 'film':
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм ✅", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
        else:  # series
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал ✅", callback_data="search_type:series")
            )
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # answer_callback_query уже вызван выше (строка 50)
        logger.info(f"[SEARCH TYPE] Тип поиска выбран: {type_text}")
        
        try:
            bot_instance.edit_message_text(
                f"🔍 Укажите запрос для поиска {type_text} в ответном сообщении, например: джон уик",
                chat_id,
                call.message.message_id,
                reply_markup=markup
            )
            logger.info(f"[SEARCH TYPE] ✅ Сообщение обновлено успешно")
        except Exception as edit_e:
            logger.error(f"[SEARCH TYPE] Ошибка редактирования сообщения: {edit_e}", exc_info=True)
            # Пробуем отправить новое сообщение
            try:
                bot_instance.send_message(
                    chat_id,
                    f"🔍 Укажите запрос для поиска {type_text} в ответном сообщении, например: джон уик",
                    reply_markup=markup
                )
                logger.info(f"[SEARCH TYPE] ✅ Новое сообщение отправлено")
            except Exception as send_e:
                logger.error(f"[SEARCH TYPE] ❌ Ошибка отправки нового сообщения: {send_e}", exc_info=True)
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
    except Exception as e:
        logger.error(f"[SEARCH TYPE] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except Exception as answer_e:
            logger.error(f"[SEARCH TYPE] Не удалось вызвать answer_callback_query: {answer_e}")
    finally:
        logger.info(f"[SEARCH TYPE] ===== END: callback_id={call.id}")


# Вспомогательная функция для поиска с фильтрацией по типу
def search_films_with_type(query, page=1, search_type='mixed'):
    """
    Поиск фильмов с фильтрацией по типу
    Использует фильтрацию на стороне клиента, так как API не поддерживает фильтрацию по типу
    """
    films, total_pages = search_films(query, page)
    
    if search_type == 'film':
        # Фильтруем только фильмы
        films = [f for f in films if f.get('type', '').upper() != 'TV_SERIES']
    elif search_type == 'series':
        # Фильтруем только сериалы
        films = [f for f in films if f.get('type', '').upper() == 'TV_SERIES']
    # Если search_type == 'mixed', возвращаем все
    
    return films, total_pages


def handle_search(message):
    """Команда /search - поиск фильмов и сериалов"""
    logger.info(f"[HANDLER] /search вызван от {message.from_user.id}")
    try:
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/search', message.chat.id)
        
        query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None
        if not query:
            # Создаем кнопки для выбора типа поиска
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            reply_msg = bot_instance.reply_to(message, "🔍 Укажите запрос для поиска в ответном сообщении, например: джон уик", reply_markup=markup)
            # Сохраняем состояние для получения запроса (по умолчанию смешанный поиск)
            user_id = message.from_user.id
            user_search_state[user_id] = {
                'chat_id': message.chat.id, 
                'message_id': reply_msg.message_id, 
                'search_type': 'mixed'
            }
            logger.info(f"[SEARCH] Состояние поиска установлено для user_id={user_id}: {user_search_state[user_id]}")
            return
        
        logger.info(f"Команда /search от пользователя {message.from_user.id}, запрос: {query}")
        
        # Получаем тип поиска из состояния, если есть
        search_type = user_search_state.get(message.from_user.id, {}).get('search_type', 'mixed')
        films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
        if not films:
            bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
            return
        
        # Формируем сообщение с результатами
        results_text = f"🔍 Результаты поиска '{query}':\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for film in films[:10]:  # Показываем максимум 10 результатов на странице
            # Пробуем разные варианты полей для совместимости с разными версиями API
            title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
            year = film.get('year') or film.get('releaseYear') or 'N/A'
            rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
            # Пробуем разные варианты ID
            kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
            
            # Определяем тип (сериал или фильм) по полю type из API
            film_type = film.get('type', '').upper()  # "FILM" или "TV_SERIES"
            is_series = film_type == 'TV_SERIES'
            
            logger.info(f"[SEARCH] Фильм: title={title}, year={year}, kp_id={kp_id}, type={film_type}, is_series={is_series}")
            
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
            else:
                logger.warning(f"[SEARCH] Фильм без ID: {film}")
        
        # Добавляем пагинацию, если нужно
        if total_pages > 1:
            pagination_row = []
            # Кодируем запрос для callback_data (заменяем пробелы на подчеркивания)
            query_encoded = query.replace(' ', '_')
            pagination_row.append(InlineKeyboardButton(f"Страница 1/{total_pages}", callback_data="noop"))
            if total_pages > 1:
                pagination_row.append(InlineKeyboardButton("Далее ▶️", callback_data=f"search_{query_encoded}_2"))
            markup.row(*pagination_row)
        
        # Добавляем кнопку "Назад в меню"
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # Добавляем пояснение про эмодзи
        results_text += "\n\n🎬 - фильм\n📺 - сериал"
        
        results_msg = bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
        # Сохраняем message_id результатов поиска для кнопки "Назад"
        if results_msg:
            user_search_state[message.from_user.id] = {
                'chat_id': message.chat.id,
                'message_id': results_msg.message_id,
                'search_type': search_type,
                'query': query,
                'results_text': results_text,
                'films': films[:10],  # Сохраняем первые 10 фильмов для восстановления
                'total_pages': total_pages
            }
        logger.info(f"✅ Ответ на /search отправлен пользователю {message.from_user.id}, найдено {len(films)} результатов")
    except Exception as e:
        logger.error(f"❌ Ошибка в /search: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /search")
        except:
            pass


def random_start(message):
        """Команда /random - рандомный выбор фильма"""
        # TODO: Извлечь из moviebot.py строки 10210-10296
        try:
            logger.info(f"[RANDOM] ===== START: user_id={message.from_user.id}, chat_id={message.chat.id}")
            user_id = message.from_user.id
            chat_id = message.chat.id
            
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(user_id, username, '/random', chat_id)
            
            # Инициализируем состояние
            user_random_state[user_id] = {
                'step': 'mode',
                'mode': None,  # 'my_votes', 'group_votes', или None (обычный режим)
                'periods': [],
                'genres': [],
                'directors': [],
                'actors': []
            }
            
            # Шаг 0: Выбор режима
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            
            # Проверяем доступ к рекомендациям
            has_rec_access = has_recommendations_access(chat_id, user_id)
            
            if has_rec_access:
                markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
                markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
                markup.add(InlineKeyboardButton("🔒 По оценкам в базе", callback_data="rand_mode_locked:group_votes"))
            
            # TODO: Добавить проверку количества оценок и групповых оценок
            # Проверяем, есть ли у пользователя больше 50 оценок
            with db_lock:
                cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                user_ratings_count = cursor.fetchone()
                user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
                
                if has_rec_access and user_ratings >= 50:
                    markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
                else:
                    if not has_rec_access:
                        markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
                    else:
                        markup.add(InlineKeyboardButton("🔒 Откроется от 50 оценок с КП", callback_data="rand_mode_locked:my_votes"))
            
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            bot_instance.reply_to(message, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
            logger.info(f"✅ Ответ на /random отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /random: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "Произошла ошибка при обработке команды /random")
            except:
                pass


def premieres_command(message):
        """Команда /premieres - премьеры фильмов"""
        logger.info(f"[HANDLER] /premieres вызван от {message.from_user.id}")
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/premieres', message.chat.id)
        
        # Показываем выбор периода
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📅 Текущий месяц", callback_data="premieres_period:current_month"))
        markup.add(InlineKeyboardButton("📅 Следующий месяц", callback_data="premieres_period:next_month"))
        markup.add(InlineKeyboardButton("📅 3 месяца", callback_data="premieres_period:3_months"))
        markup.add(InlineKeyboardButton("📅 6 месяцев", callback_data="premieres_period:6_months"))
        markup.add(InlineKeyboardButton("📅 Текущий год", callback_data="premieres_period:current_year"))
        markup.add(InlineKeyboardButton("📅 Ближайший год", callback_data="premieres_period:next_year"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        bot_instance.reply_to(message, "📅 <b>Выберите период для просмотра премьер:</b>", reply_markup=markup, parse_mode='HTML')


def ticket_command(message):
    """Команда /ticket - работа с билетами"""
    # TODO: Извлечь из moviebot.py строки 17031-17333
    logger.info(f"[TICKET COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[TICKET COMMAND] message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        username = message.from_user.username or f"user_{user_id}"
        logger.info(f"[TICKET COMMAND] Вызов log_request")
        log_request(user_id, username, '/ticket', chat_id)
        logger.info(f"[TICKET COMMAND] log_request выполнен")
        
        # Проверяем доступ к функциям билетов
        logger.info(f"[TICKET COMMAND] Проверка доступа к билетам")
        if not has_tickets_access(chat_id, user_id):
            logger.info(f"[TICKET COMMAND] Нет доступа, отправка сообщения о подписке")
            text = "🎫 <b>Билеты в кино</b>\n\n"
            text += "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
            text += "Используйте /payment для оформления подписки."
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("🎫 К подписке Билеты", callback_data="payment:tariffs:personal"))
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            logger.info(f"[TICKET COMMAND] Вызов reply_to для сообщения о подписке")
            bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[TICKET COMMAND] Сообщение о подписке отправлено")
            return
        
        # Проверяем, есть ли файл в сообщении
        logger.info(f"[TICKET COMMAND] Проверка наличия файла")
        has_photo = message.photo is not None and len(message.photo) > 0
        has_document = message.document is not None
        logger.info(f"[TICKET COMMAND] has_photo={has_photo}, has_document={has_document}")
        
        if has_photo or has_document:
            # Сохраняем file_id для последующей обработки
            if has_photo:
                file_id = message.photo[-1].file_id  # Берем самое большое фото
            else:
                file_id = message.document.file_id
            
            logger.info(f"[TICKET COMMAND] Файл найден, file_id={file_id}")
            user_ticket_state[user_id] = {
                'step': 'select_session',
                'file_id': file_id,
                'chat_id': chat_id
            }
            
            # Показываем список сеансов в кино
            logger.info(f"[TICKET COMMAND] Вызов show_cinema_sessions с file_id")
            show_cinema_sessions(chat_id, user_id, file_id)
            logger.info(f"[TICKET COMMAND] show_cinema_sessions завершен")
        else:
            # Нет файла - показываем список сеансов для выбора или сообщение об отсутствии билетов
            logger.info(f"[TICKET COMMAND] Файла нет, вызов show_cinema_sessions без file_id")
            show_cinema_sessions(chat_id, user_id, None)
            logger.info(f"[TICKET COMMAND] show_cinema_sessions завершен")
        
        logger.info(f"[TICKET COMMAND] ===== КОНЕЦ (успешно) =====")
    except Exception as e:
        logger.error(f"[TICKET COMMAND] ❌ КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        logger.error(f"[TICKET COMMAND] Тип ошибки: {type(e).__name__}, args: {e.args}")
        try:
            logger.info(f"[TICKET COMMAND] Попытка отправить сообщение об ошибке")
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /ticket")
            logger.info(f"[TICKET COMMAND] Сообщение об ошибке отправлено")
        except Exception as send_error:
            logger.error(f"[TICKET COMMAND] ❌ Не удалось отправить сообщение об ошибке: {send_error}", exc_info=True)


def settings_command(message):
    """Команда /settings - настройки"""
    # TODO: Извлечь из moviebot.py строки 10627-10992
    logger.info(f"[HANDLER] /settings вызван от {message.from_user.id}")
    try:
        chat_id = message.chat.id
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        log_request(user_id, username, '/settings', chat_id)
        logger.info(f"Команда /settings от пользователя {user_id}")
        
        # Проверяем на reset
        if message.text and 'reset' in message.text.lower():
            with db_lock:
                cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
                conn.commit()
            bot_instance.reply_to(message, "✅ Реакции сброшены к значению по умолчанию (✅)")
            logger.info(f"Реакции сброшены для чата {chat_id}")
            return
        
        # Сначала показываем меню выбора действия
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("😀 Настроить эмодзи просмотра", callback_data="settings:emoji"))
        markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
        
        # Проверяем доступ к настройкам напоминаний (требуется подписка на уведомления)
        if has_notifications_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
        else:
            markup.add(InlineKeyboardButton("🔒 Настройки напоминаний", callback_data="settings:notifications_locked"))
        
        # Проверяем доступ к импорту базы (требуется подписка на рекомендации)
        if has_recommendations_access(chat_id, user_id):
            markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
        else:
            markup.add(InlineKeyboardButton("🔒 Импорт базы из Кинопоиска", callback_data="settings:import_locked"))
        
        # Проверяем, является ли чат личным (случайные события доступны только в группах)
        is_private = message.chat.type == 'private'
        if is_private:
            markup.add(InlineKeyboardButton("🔒 Случайные события", callback_data="settings:random_events_locked"))
        else:
            markup.add(InlineKeyboardButton("🎲 Случайные события", callback_data="settings:random_events"))
        markup.add(InlineKeyboardButton("✏️ Редактировать записи", callback_data="settings:edit"))
        markup.add(InlineKeyboardButton("🗑️ Очистка базы", callback_data="settings:clean"))
        markup.add(InlineKeyboardButton("👥 Участие", callback_data="settings:join"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        sent = bot_instance.send_message(chat_id,
            f"⚙️ <b>Настройки</b>\n\n"
            f"Выберите, что хотите настроить:",
            reply_markup=markup,
            parse_mode='HTML')
        
        logger.info(f"Настройки открыты для {user_id}, msg_id: {sent.message_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка в /settings: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "Произошла ошибка при обработке команды /settings")
        except:
            pass


def help_command(message):
    """Команда /help - помощь"""
    logger.info(f"[HANDLER] /help вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/help', message.chat.id)
    logger.info(f"Команда /help от пользователя {message.from_user.id}")
    text = """🎬 Помощь по командам бота:

/list — Показать список непросмотренных фильмов
/random — Выбрать случайный непросмотренный фильм с фильтрами (год, жанр, режиссёр)
/search — Поиск фильмов через Kinopoisk API
/total — Статистика: фильмы, жанры, режиссёры, актёры, оценки
/stats — Детальная статистика группы и участников
/rate — Оценить просмотренные фильмы
/plan — Запланировать просмотр фильма (дома/в кино)
/schedule — Показать список запланированных просмотров
/settings — Настроить эмодзи для отметки просмотренных фильмов
/clean — Удалить оценку, просмотр, план или обнулить базу
/help — Эта справка

Как использовать бота:

Есть два варианта, использование лично или использование в группе. Чтобы бот работал в группе, нужно добавить бота и сделать админом группы. В боте могут участвовать не все члены группы: для того, чтобы начать участие, нужно отправить любую команду боту. Вы можете добавить других членов группы к участию в боте по команде /join.

Сценарии работы с ботом:

1) Добавление фильмов
1. Отправьте ссылку на фильм с Кинопоиска — бот автоматически добавит его
2. Запланируйте просмотр фильма — дома или в кино. При домашнем просмотре, будут предложены онлайн-кинотеатры, где можно посмотреть фильм.
3. В день просмотра вам придет напоминание о просмотре со ссылкой на кинотеатр, если смотрите дома, или с билетами, если вы подгрузили билет в кино.
4. После просмотра, поставьте реакцию на сообщение с фильмом — фильм будет отмечен как просмотренный
5. После отметки напишите оценку от 1 до 10

При групповом участии, учитываются оценки всех участников. К высоко оцененным фильмам предлагаются похожие, а также оцененные фильмы участвуют в рекомендательных функциях

2) Сериалы
Можно добавлять сериалы, трекать просмотренные серии и подписаться на уведомления

3) Планирование премьер
Если фильм ещё не вышел, вы можете подписаться на его дату выхода

4) Поиск
Вы можете искать фильмы и сериалы с командой /search, а также искать премьеры по /premiere, там будет актуальный список премьер

5) Планирование походов в кино
Вы можете запланировать, хотите вы посмотреть тот или иной фильм дома или в кино. При просмотре фильма дома, вам будут предложны онлайн-кинотеатры, а при просмотре в кино — предложена возможность загрузить билет и указать время сеанса. В день просмотра фильма придет уведомление и напоминание с билетами заранее (функционал платный). Время уведомлений можно настроить.

Приятного просмотра! 🍿

Если у вас возникли сложности с ботом или оплатой, напишите нам:
@zap_nikita
movie-planner-bot@yandex.com"""
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    # Переключаемся на HTML, так как Markdown может вызывать ошибки парсинга
    text_html = text.replace('*', '').replace('_', '')
    # Добавляем базовое форматирование
    text_html = text_html.replace('🎬 Помощь по командам бота:', '<b>🎬 Помощь по командам бота:</b>')
    text_html = text_html.replace('Как использовать бота:', '<b>Как использовать бота:</b>')
    text_html = text_html.replace('Сценарии работы с ботом:', '<b>Сценарии работы с ботом:</b>')
    text_html = text_html.replace('1) Добавление фильмов', '<b>1) Добавление фильмов</b>')
    text_html = text_html.replace('2) Сериалы', '<b>2) Сериалы</b>')
    text_html = text_html.replace('3) Планирование премьер', '<b>3) Планирование премьер</b>')
    text_html = text_html.replace('4) Поиск', '<b>4) Поиск</b>')
    text_html = text_html.replace('5) Планирование походов в кино', '<b>5) Планирование походов в кино</b>')
    text_html = text_html.replace('Приятного просмотра!', '<b>Приятного просмотра!</b>')
    text_html = text_html.replace('Если у вас возникли сложности с ботом или оплатой, напишите нам:', '<b>Если у вас возникли сложности с ботом или оплатой, напишите нам:</b>')
    bot_instance.reply_to(message, text_html, reply_markup=markup, parse_mode='HTML')


def show_cinema_sessions(chat_id, user_id, file_id=None):
    """Показывает список запланированных сеансов в кино"""
    logger.info(f"[SHOW SESSIONS] Показываем сеансы для пользователя {user_id}, chat_id={chat_id}, file_id={file_id}")
    try:
        with db_lock:
            cursor.execute('''
                SELECT p.id, m.title, p.plan_datetime, 
                       CASE WHEN p.ticket_file_id IS NOT NULL THEN 1 ELSE 0 END as ticket_count
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.chat_id = %s AND p.plan_type = 'cinema'
                ORDER BY p.plan_datetime
                LIMIT 20
            ''', (chat_id,))
            sessions = cursor.fetchall()
        
        logger.info(f"[SHOW SESSIONS] Найдено сеансов: {len(sessions) if sessions else 0}")
        
        if not sessions:
            logger.info(f"[SHOW SESSIONS] Нет сеансов, отправляем сообщение пользователю {user_id}")
            if file_id:
                # Если есть файл, но нет сеансов, предлагаем создать новый
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data=f"ticket_new:{file_id}"))
                markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
                bot_instance.send_message(chat_id, "❌ Нет запланированных сеансов в кино.\n\n📎 Файл готов к добавлению. Создайте новый сеанс.", reply_markup=markup, parse_mode='HTML')
            else:
                # Нет файла и нет сеансов
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data="ticket_new"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                bot_instance.send_message(chat_id, "❌ Нет запланированных сеансов в кино.", reply_markup=markup, parse_mode='HTML')
            return
        
        user_tz = get_user_timezone_or_default(user_id)
        markup = InlineKeyboardMarkup(row_width=1)
        
        for row in sessions:
            if isinstance(row, dict):
                plan_id = row.get('id')
                title = row.get('title')
                plan_dt_value = row.get('plan_datetime')
                ticket_count = row.get('ticket_count', 0)
            else:
                plan_id = row[0]
                title = row[1]
                plan_dt_value = row[2]
                ticket_count = row[3] if len(row) > 3 else 0
            
            if plan_dt_value:
                if isinstance(plan_dt_value, datetime):
                    if plan_dt_value.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt_value).astimezone(user_tz)
                    else:
                        dt = plan_dt_value.astimezone(user_tz)
                else:
                    dt = datetime.fromisoformat(str(plan_dt_value).replace('Z', '+00:00')).astimezone(user_tz)
                
                date_str = dt.strftime('%d.%m %H:%M')
                ticket_emoji = "🎟️ " if ticket_count > 0 else ""
                button_text = f"{ticket_emoji}{title} | {date_str}"
                
                if len(button_text) > 30:
                    short_title = title[:20] + "..."
                    button_text = f"{ticket_emoji}{short_title} | {date_str}"
                    if len(button_text) > 30:
                        button_text = button_text[:27] + "..."
                
                callback_data = f"ticket_session:{plan_id}"
                if file_id:
                    callback_data += f":{file_id}"
                markup.add(InlineKeyboardButton(button_text, callback_data=callback_data))
        
        if file_id:
            markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data=f"ticket_new:{file_id}"))
        else:
            markup.add(InlineKeyboardButton("➕ Добавить новый сеанс", callback_data="ticket_new"))
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        text = "🎟️ <b>Выберите сеанс:</b>\n\n"
        if file_id:
            text += "📎 Файл готов к добавлению. Выберите сеанс или создайте новый."
        else:
            text += "Выберите сеанс для просмотра билетов или добавления новых."
        
        bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        logger.info(f"[SHOW SESSIONS] Сообщение с сеансами отправлено пользователю {user_id}")
    except Exception as e:
        logger.error(f"[SHOW SESSIONS] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.send_message(chat_id, "❌ Произошла ошибка при загрузке сеансов.")
        except:
            pass


def register_series_handlers(bot_param):
    """Регистрирует обработчики команд связанных с сериалами"""
    logger.info("=" * 80)
    logger.info(f"[REGISTER SERIES HANDLERS] ===== START: регистрация обработчиков сериалов =====")
    logger.info(f"[REGISTER SERIES HANDLERS] bot_param: {bot_param}")
    logger.info(f"[REGISTER SERIES HANDLERS] bot_instance (из импорта): {bot_instance}")
    logger.info(f"[REGISTER SERIES HANDLERS] bot_param == bot_instance: {bot_param == bot_instance}")
    logger.info(f"[REGISTER SERIES HANDLERS] id(bot_param): {id(bot_param)}, id(bot_instance): {id(bot_instance)}")
    
    # КРИТИЧЕСКИ ВАЖНО: Используем bot_param (переданный параметр) для регистрации handlers внутри функции
    # Но обработчик search_type_callback уже зарегистрирован на верхнем уровне модуля с bot_instance
    # Проверяем, что это один и тот же объект
    if bot_param != bot_instance:
        logger.error(f"[REGISTER SERIES HANDLERS] ❌ КРИТИЧЕСКАЯ ОШИБКА: bot_param != bot_instance!")
        logger.error(f"[REGISTER SERIES HANDLERS] bot_param id: {id(bot_param)}, bot_instance id: {id(bot_instance)}")
        logger.error(f"[REGISTER SERIES HANDLERS] Это означает, что search_type_callback зарегистрирован на другом экземпляре бота!")
        logger.error(f"[REGISTER SERIES HANDLERS] Перерегистрируем search_type_callback на правильном экземпляре...")
        
        # Перерегистрируем обработчик на правильном экземпляре бота
        @bot_param.callback_query_handler(func=lambda call: call.data and call.data.startswith("search_type:"))
        def search_type_callback_fixed(call):
            """Перерегистрированный обработчик выбора типа поиска"""
            # Вызываем оригинальный обработчик
            search_type_callback(call)
        
        logger.info(f"[REGISTER SERIES HANDLERS] ✅ search_type_callback перерегистрирован на bot_param")
    else:
        logger.info(f"[REGISTER SERIES HANDLERS] ✅ bot_param == bot_instance, обработчик search_type_callback зарегистрирован правильно")
    
    @bot_param.message_handler(commands=['search'])
    def _handle_search_handler(message):
        """Обертка для регистрации команды /search"""
        handle_search(message)
    
    @bot_param.message_handler(commands=['random'])
    def _random_start_handler(message):
        """Обертка для регистрации команды /random"""
        random_start(message)
    
    @bot_param.message_handler(commands=['premieres'])
    def _premieres_command_handler(message):
        """Обертка для регистрации команды /premieres"""
        premieres_command(message)
    
    @bot_param.message_handler(commands=['ticket'])
    def _ticket_command_handler(message):
        """Обертка для регистрации команды /ticket"""
        ticket_command(message)
    
    @bot_param.message_handler(commands=['settings'])
    def _settings_command_handler(message):
        """Обертка для регистрации команды /settings"""
        settings_command(message)
    
    @bot_param.message_handler(commands=['help'])
    def _help_command_handler(message):
        """Обертка для регистрации команды /help"""
        help_command(message)

    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_mode:"))
    def handle_rand_mode(call):
        """Обработчик выбора режима рандомайзера"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== START: callback_id={call.id}, user_id={call.from_user.id}, data={call.data}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            mode = call.data.split(":")[1]
            
            logger.info(f"[RANDOM CALLBACK] Mode: {mode}, user_id={user_id}, chat_id={chat_id}")
            
            # Проверяем доступ к рекомендациям для режимов, требующих подписку
            if mode in ['kinopoisk', 'my_votes', 'group_votes']:
                has_rec_access = has_recommendations_access(chat_id, user_id)
                logger.info(f"[RANDOM CALLBACK] Mode {mode} requires recommendations access: {has_rec_access}")
                if not has_rec_access:
                    bot_instance.answer_callback_query(
                        call.id, 
                        "❌ Этот режим доступен только с подпиской на рекомендации. Используйте /payment для оформления подписки.", 
                        show_alert=True
                    )
                    logger.warning(f"[RANDOM CALLBACK] Access denied for mode {mode}, user_id={user_id}")
                    return
            
            if user_id not in user_random_state:
                logger.error(f"[RANDOM CALLBACK] State not found for user_id={user_id}, state keys: {list(user_random_state.keys())}")
                bot_instance.answer_callback_query(call.id, "❌ Состояние не найдено. Начните заново с /random", show_alert=True)
                return
            
            logger.info(f"[RANDOM CALLBACK] State found: {user_random_state[user_id]}")
            
            user_random_state[user_id]['mode'] = mode
            user_random_state[user_id]['step'] = 'period'
            
            logger.info(f"[RANDOM CALLBACK] State updated: mode={mode}, step=period")
            
            # Добавляем справку о режиме
            mode_descriptions = {
                'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
                'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм по вашим фильтрам.',
                'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
                'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nПолучите рекомендацию, основанную на оценках в вашей локальной базе.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
            }
            mode_description = mode_descriptions.get(mode, '')
            
            # Для режима kinopoisk пропускаем периоды и сразу переходим к выбору года и жанра
            if mode == 'kinopoisk':
                user_random_state[user_id]['step'] = 'year'
                bot_instance.answer_callback_query(call.id)
                logger.info(f"[RANDOM CALLBACK] Mode kinopoisk selected, moving to year selection")
                # TODO: Вызвать _show_year_step
                return
            
            # Шаг 1: Выбор периода - показываем только те периоды, где есть фильмы
            all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            available_periods = []
            
            logger.info(f"[RANDOM CALLBACK] Checking available periods for mode={mode}")
            
            with db_lock:
                if mode == 'my_votes':
                    # Для режима "по моим оценкам" - получаем годы из импортированных фильмов с оценкой 9-10
                    cursor.execute("""
                        SELECT DISTINCT m.year
                        FROM movies m
                        JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                        WHERE m.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                        AND m.year IS NOT NULL
                        ORDER BY m.year
                    """, (chat_id, user_id))
                    years_rows = cursor.fetchall()
                    years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row]
                    
                    logger.info(f"[RANDOM CALLBACK] Found {len(years)} years for my_votes mode")
                    
                    # Определяем доступные периоды на основе найденных годов
                    for period in all_periods:
                        if period == "До 1980":
                            if any(y < 1980 for y in years):
                                available_periods.append(period)
                        elif period == "1980–1990":
                            if any(1980 <= y <= 1990 for y in years):
                                available_periods.append(period)
                        elif period == "1990–2000":
                            if any(1990 <= y <= 2000 for y in years):
                                available_periods.append(period)
                        elif period == "2000–2010":
                            if any(2000 <= y <= 2010 for y in years):
                                available_periods.append(period)
                        elif period == "2010–2020":
                            if any(2010 <= y <= 2020 for y in years):
                                available_periods.append(period)
                        elif period == "2020–сейчас":
                            if any(y >= 2020 for y in years):
                                available_periods.append(period)
                elif mode == 'group_votes':
                    # Для режима "По оценкам в базе" - получаем годы из фильмов со средней оценкой группы >= 9
                    cursor.execute("""
                        SELECT DISTINCT m.year
                        FROM movies m
                        WHERE m.chat_id = %s AND m.year IS NOT NULL
                        AND EXISTS (
                            SELECT 1 FROM ratings r 
                            WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                            GROUP BY r.film_id, r.chat_id 
                            HAVING AVG(r.rating) >= 9
                        )
                        ORDER BY m.year
                    """, (chat_id,))
                    years_rows = cursor.fetchall()
                    years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row]
                    
                    logger.info(f"[RANDOM CALLBACK] Found {len(years)} years for group_votes mode")
                    
                    # Определяем доступные периоды на основе найденных годов
                    for period in all_periods:
                        if period == "До 1980":
                            if any(y < 1980 for y in years):
                                available_periods.append(period)
                        elif period == "1980–1990":
                            if any(1980 <= y <= 1990 for y in years):
                                available_periods.append(period)
                        elif period == "1990–2000":
                            if any(1990 <= y <= 2000 for y in years):
                                available_periods.append(period)
                        elif period == "2000–2010":
                            if any(2000 <= y <= 2010 for y in years):
                                available_periods.append(period)
                        elif period == "2010–2020":
                            if any(2010 <= y <= 2020 for y in years):
                                available_periods.append(period)
                        elif period == "2020–сейчас":
                            if any(y >= 2020 for y in years):
                                available_periods.append(period)
                else:
                    # Для режима database - используем старую логику
                    base_query = """
                        SELECT COUNT(DISTINCT m.id) 
                        FROM movies m
                        LEFT JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id AND r.is_imported = TRUE
                        WHERE m.chat_id = %s AND m.watched = 0 AND r.id IS NULL
                    """
                    params = [chat_id]
                
                    for period in all_periods:
                        if period == "До 1980":
                            condition = "m.year < 1980"
                        elif period == "1980–1990":
                            condition = "(m.year >= 1980 AND m.year <= 1990)"
                        elif period == "1990–2000":
                            condition = "(m.year >= 1990 AND m.year <= 2000)"
                        elif period == "2000–2010":
                            condition = "(m.year >= 2000 AND m.year <= 2010)"
                        elif period == "2010–2020":
                            condition = "(m.year >= 2010 AND m.year <= 2020)"
                        elif period == "2020–сейчас":
                            condition = "m.year >= 2020"
                        
                        query = f"{base_query} AND {condition}"
                        cursor.execute(query, tuple(params))
                        count_row = cursor.fetchone()
                        count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                        
                        if count > 0:
                            available_periods.append(period)
            
            logger.info(f"[RANDOM CALLBACK] Available periods: {available_periods}")
            
            user_random_state[user_id]['available_periods'] = available_periods
            
            markup = InlineKeyboardMarkup(row_width=1)
            if available_periods:
                for period in available_periods:
                    markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            
            bot_instance.answer_callback_query(call.id)
            text = f"{mode_description}\n\n🎲 <b>Шаг 1/4: Выберите период</b>\n\n(можно выбрать несколько или пропустить)"
            bot_instance.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[RANDOM CALLBACK] ✅ Mode selected: {mode}, moving to period selection, user_id={user_id}")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_mode: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_mode_locked:"))
    def handle_rand_mode_locked(call):
        """Обработчик заблокированных режимов рандомайзера"""
        try:
            logger.info(f"[RANDOM CALLBACK] Locked mode handler: data={call.data}, user_id={call.from_user.id}")
            mode = call.data.split(":")[1]  # kinopoisk, my_votes, group_votes
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if mode == "kinopoisk":
                message_text = "🎬 Рандом по Кинопоиску доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            elif mode == "group_votes":
                message_text = "⭐ Режим \"По оценкам в базе\" доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            elif mode == "my_votes":
                # Проверяем количество оценок
                with db_lock:
                    cursor.execute('SELECT COUNT(*) FROM ratings WHERE chat_id = %s AND user_id = %s', (chat_id, user_id))
                    user_ratings_count = cursor.fetchone()
                    user_ratings = user_ratings_count.get('count') if isinstance(user_ratings_count, dict) else (user_ratings_count[0] if user_ratings_count else 0)
                
                if user_ratings < 50:
                    message_text = "⭐ Режим \"По моим оценкам\" откроется после добавления 50 оценок в базу. Оцените больше фильмов!"
                else:
                    message_text = "⭐ Режим \"По моим оценкам\" доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment"
            else:
                message_text = "🔒 Этот режим недоступен. Подключите подписку через /payment"
            
            bot_instance.answer_callback_query(
                call.id,
                message_text,
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[RAND MODE LOCKED] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(
                    call.id,
                    "🔒 Функционал можно подключить через /payment",
                    show_alert=True
                )
            except:
                pass
    
    @bot_param.callback_query_handler(func=lambda call: call.data.startswith("rand_period:"))
    def handle_rand_period(call):
        """Обработчик выбора периода для рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== PERIOD HANDLER: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            data = call.data.split(":", 1)[1]
            
            if user_id not in user_random_state:
                logger.warning(f"[RANDOM CALLBACK] State not found for user {user_id}, reinitializing")
                user_random_state[user_id] = {'step': 'period', 'periods': [], 'genres': [], 'directors': [], 'actors': []}
            
            if data == "skip":
                logger.info(f"[RANDOM CALLBACK] Period skipped, moving to genre")
                user_random_state[user_id]['periods'] = []
                user_random_state[user_id]['step'] = 'genre'
                # TODO: Вызвать _show_genre_step
                bot_instance.answer_callback_query(call.id, "Период пропущен. Функция в разработке.")
                return
            elif data == "done":
                logger.info(f"[RANDOM CALLBACK] Periods confirmed, moving to genre")
                user_random_state[user_id]['step'] = 'genre'
                # TODO: Вызвать _show_genre_step
                bot_instance.answer_callback_query(call.id, "Период выбран. Функция в разработке.")
                return
            else:
                # Toggle периода
                periods = user_random_state[user_id].get('periods', [])
                if data in periods:
                    periods.remove(data)
                    logger.info(f"[RANDOM CALLBACK] Period removed: {data}")
                else:
                    periods.append(data)
                    logger.info(f"[RANDOM CALLBACK] Period added: {data}")
                
                user_random_state[user_id]['periods'] = periods
                
                # Получаем доступные периоды из состояния
                available_periods = user_random_state[user_id].get('available_periods', [])
                if not available_periods:
                    available_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
                
                # Обновляем кнопки
                markup = InlineKeyboardMarkup(row_width=1)
                if available_periods:
                    for p in available_periods:
                        label = f"✓ {p}" if p in periods else p
                        markup.add(InlineKeyboardButton(label, callback_data=f"rand_period:{p}"))
                
                if periods:
                    markup.add(InlineKeyboardButton("Продолжить ➡️", callback_data="rand_period:done"))
                else:
                    markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
                
                selected = ', '.join(periods) if periods else 'ничего'
                try:
                    bot_instance.edit_message_text(f"🎲 <b>Шаг 1/4: Выберите период</b>\n\nВыбрано: {selected}\n\n(можно выбрать несколько)", 
                                        chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                    bot_instance.answer_callback_query(call.id)
                    logger.info(f"[RANDOM CALLBACK] Period keyboard updated, selected={selected}")
                except Exception as e:
                    logger.error(f"[RANDOM CALLBACK] Error updating period keyboard: {e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "Ошибка обновления")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_period: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Ошибка обработки")
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("ticket_locked:"))
    def handle_ticket_locked(call):
        """Обработчик заблокированных кнопок билетов"""
        try:
            bot_instance.answer_callback_query(
                call.id,
                "🎫 Билеты в кино доступны с подпиской 🎫 Билеты или 📦 Все режимы. Подключите подписку через /payment",
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[TICKET LOCKED] Ошибка: {e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("ticket_session:"))
    def ticket_session_callback(call):
        """Обработчик выбора сеанса - показывает информацию о сеансе и билеты"""
        try:
            from moviebot.utils.helpers import has_tickets_access
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Парсим plan_id и file_id (если есть)
            parts = call.data.split(":")
            plan_id = int(parts[1])
            file_id = parts[2] if len(parts) > 2 else None
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot_instance.edit_message_text(
                    "🎫 <b>Билеты в кино</b>\n\n"
                    "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                    "Используйте /payment для оформления подписки.",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                return
            
            # Получаем информацию о сеансе
            with db_lock:
                cursor.execute('''
                    SELECT p.id, p.plan_datetime, p.ticket_file_id, m.title, m.kp_id
                    FROM plans p
                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.id = %s AND p.chat_id = %s AND p.plan_type = 'cinema'
                ''', (plan_id, chat_id))
                plan_row = cursor.fetchone()
            
            if not plan_row:
                bot_instance.answer_callback_query(call.id, "❌ Сеанс не найден", show_alert=True)
                return
            
            if isinstance(plan_row, dict):
                plan_dt = plan_row.get('plan_datetime')
                ticket_file_id = plan_row.get('ticket_file_id')
                title = plan_row.get('title')
                kp_id = plan_row.get('kp_id')
            else:
                plan_dt = plan_row[1]
                ticket_file_id = plan_row[2]
                title = plan_row[3]
                kp_id = plan_row[4]
            
            # Форматируем дату и время
            user_tz = get_user_timezone_or_default(user_id)
            if plan_dt:
                if isinstance(plan_dt, datetime):
                    if plan_dt.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt).astimezone(user_tz)
                    else:
                        dt = plan_dt.astimezone(user_tz)
                else:
                    dt = datetime.fromisoformat(str(plan_dt).replace('Z', '+00:00')).astimezone(user_tz)
                date_str = dt.strftime('%d.%m.%Y %H:%M')
            else:
                date_str = "Не указано"
            
            # Формируем текст и кнопки
            text = f"🎬 <b>{title}</b>\n\n"
            text += f"📅 <b>Дата и время:</b> {date_str}\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            
            if ticket_file_id:
                text += "🎟️ <b>Билеты загружены</b>\n\n"
                text += "Билеты будут отправлены вам перед сеансом."
                markup.add(InlineKeyboardButton("📎 Показать билеты", callback_data=f"show_ticket:{plan_id}"))
                markup.add(InlineKeyboardButton("🔄 Заменить билеты", callback_data=f"add_ticket:{plan_id}"))
            else:
                text += "🎟️ <b>Билеты не загружены</b>\n\n"
                text += "Загрузите билеты, чтобы получать их перед сеансом."
                markup.add(InlineKeyboardButton("➕ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
            
            # Добавляем кнопку "✏️ Изменить" для изменения времени сеанса
            markup.add(InlineKeyboardButton("✏️ Изменить", callback_data=f"ticket_edit_time:{plan_id}"))
            
            if file_id:
                # Если есть file_id, значит пользователь хочет добавить билеты к этому сеансу
                user_ticket_state[user_id] = {
                    'step': 'upload_ticket',
                    'plan_id': plan_id,
                    'chat_id': chat_id,
                    'file_id': file_id
                }
                text += "\n\n📎 Файл готов к добавлению. Нажмите '➕ Добавить билеты' для продолжения."
            
            markup.add(InlineKeyboardButton("⬅️ Назад к сеансам", callback_data="ticket_new"))
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            # Показываем информацию о сеансе
            try:
                bot_instance.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
            except telebot.apihelper.ApiTelegramException as e:
                error_str = str(e).lower()
                if "message is not modified" in error_str:
                    # Если сообщение не изменилось, просто обновляем клавиатуру
                    try:
                        bot_instance.edit_message_reply_markup(
                            chat_id=chat_id,
                            message_id=call.message.message_id,
                            reply_markup=markup
                        )
                    except:
                        pass
                else:
                    raise
        except Exception as e:
            logger.error(f"[TICKET SESSION] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("ticket_new"))
    def ticket_new_callback(call):
        """Обработчик кнопки 'Добавить новый сеанс' - показывает выбор типа билета"""
        try:
            from moviebot.states import user_ticket_state
            from moviebot.utils.helpers import has_tickets_access
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot_instance.edit_message_text(
                    "🎫 <b>Билеты в кино</b>\n\n"
                    "Вы можете загружать билеты и получать их в боте прямо перед сеансом с подпиской <b>\"Билеты\"</b>.\n\n"
                    "Используйте /payment для оформления подписки.",
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                return
            
            # Парсим file_id из callback_data, если есть
            parts = call.data.split(":")
            file_id = parts[1] if len(parts) > 1 else None
            
            # Показываем выбор: добавить билет на фильм или на мероприятие
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("➕ Добавить фильм", callback_data=f"ticket_new_film:{file_id}" if file_id else "ticket_new_film"))
            markup.add(InlineKeyboardButton("🎤 Добавить билет", callback_data="ticket:add_event"))
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
            
            bot_instance.edit_message_text(
                "🎫 <b>Добавление билета</b>\n\n"
                "Выберите тип билета:",
                chat_id,
                call.message.message_id,
                reply_markup=markup,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET NEW] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data == "ticket:add_event")
    def ticket_add_event_callback(call):
        """Обработчик кнопки 'Добавить билет' - начинает флоу добавления билета на мероприятие"""
        try:
            from moviebot.states import user_ticket_state
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Начинаем флоу добавления билета на мероприятие
            user_ticket_state[user_id] = {
                'step': 'event_name',
                'chat_id': chat_id,
                'type': 'event'
            }
            
            bot_instance.edit_message_text(
                "🎤 <b>Добавление билета на мероприятие</b>\n\n"
                "Напишите название мероприятия в ответ на это сообщение:",
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET ADD EVENT] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("ticket_new_film"))
    def ticket_new_film_callback(call):
        """Обработчик кнопки 'Добавить фильм' - начинает флоу добавления билета на фильм"""
        try:
            from moviebot.states import user_ticket_state
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Парсим file_id из callback_data, если есть
            parts = call.data.split(":")
            file_id = parts[1] if len(parts) > 1 else None
            
            # Начинаем флоу добавления билета на фильм
            user_ticket_state[user_id] = {
                'step': 'waiting_new_session',
                'chat_id': chat_id,
                'type': 'film',
                'file_id': file_id
            }
            
            # Проверяем, не совпадает ли текст с текущим сообщением
            current_text = call.message.text or ""
            new_text = (
                "🎬 <b>Добавление билета на фильм</b>\n\n"
                "Отправьте ссылку на фильм или его ID с Кинопоиска и укажите дату/время сеанса.\n"
                "Формат: ссылка или ID + дата + время\n"
                "Например: https://kinopoisk.ru/film/123456/ 15 января 19:30"
            )
            
            # Если текст совпадает, просто обновляем клавиатуру или отправляем новое сообщение
            if current_text.strip() == new_text.strip():
                # Текст не изменился, отправляем новое сообщение
                bot_instance.send_message(
                    chat_id,
                    new_text,
                    parse_mode='HTML'
                )
            else:
                # Текст изменился, обновляем сообщение
                try:
                    bot_instance.edit_message_text(
                        new_text,
                        chat_id,
                        call.message.message_id,
                        parse_mode='HTML'
                    )
                except telebot.apihelper.ApiTelegramException as e:
                    error_str = str(e).lower()
                    if "message is not modified" in error_str:
                        # Если сообщение не изменилось, отправляем новое
                        bot_instance.send_message(
                            chat_id,
                            new_text,
                            parse_mode='HTML'
                        )
                    else:
                        raise
        except Exception as e:
            logger.error(f"[TICKET NEW FILM] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("show_ticket:"))
    def show_ticket_callback(call):
        """Обработчик кнопки 'Показать билеты' - отправляет билеты пользователю"""
        try:
            from moviebot.utils.helpers import has_tickets_access
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Проверяем доступ к функциям билетов
            if not has_tickets_access(chat_id, user_id):
                bot_instance.answer_callback_query(
                    call.id,
                    "🎫 Билеты в кино доступны с подпиской 🎫 Билеты или 📦 Все режимы. Подключите подписку через /payment",
                    show_alert=True
                )
                return
            
            # Получаем ticket_file_id
            with db_lock:
                cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                ticket_row = cursor.fetchone()
            
            if not ticket_row:
                bot_instance.answer_callback_query(call.id, "❌ Билеты не найдены", show_alert=True)
                return
            
            if isinstance(ticket_row, dict):
                ticket_file_id = ticket_row.get('ticket_file_id')
            else:
                ticket_file_id = ticket_row[0]
            
            if not ticket_file_id:
                bot_instance.answer_callback_query(call.id, "❌ Билеты не загружены", show_alert=True)
                return
            
            # Отправляем билеты
            try:
                bot_instance.send_photo(chat_id, ticket_file_id, caption="🎟️ Ваши билеты")
                bot_instance.answer_callback_query(call.id, "✅ Билеты отправлены")
            except Exception as send_e:
                logger.error(f"[SHOW TICKET] Ошибка отправки билетов: {send_e}", exc_info=True)
                try:
                    # Пробуем отправить как документ
                    bot_instance.send_document(chat_id, ticket_file_id, caption="🎟️ Ваши билеты")
                    bot_instance.answer_callback_query(call.id, "✅ Билеты отправлены")
                except Exception as doc_e:
                    logger.error(f"[SHOW TICKET] Ошибка отправки билетов как документа: {doc_e}", exc_info=True)
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка отправки билетов", show_alert=True)
        except Exception as e:
            logger.error(f"[SHOW TICKET] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("ticket_edit_time:"))
    def ticket_edit_time_callback(call):
        """Обработчик кнопки 'Изменить время' - позволяет изменить время сеанса"""
        try:
            from moviebot.states import user_ticket_state
            from moviebot.utils.parsing import parse_session_time
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            plan_id = int(call.data.split(":")[1])
            
            # Получаем текущее время сеанса
            with db_lock:
                cursor.execute('SELECT plan_datetime FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
                plan_row = cursor.fetchone()
            
            if not plan_row:
                bot_instance.answer_callback_query(call.id, "❌ Сеанс не найден", show_alert=True)
                return
            
            plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[0]
            
            # Устанавливаем состояние для изменения времени
            user_ticket_state[user_id] = {
                'step': 'edit_time',
                'plan_id': plan_id,
                'chat_id': chat_id
            }
            
            # Формируем сообщение с примером
            current_time_str = ""
            if plan_dt:
                user_tz = get_user_timezone_or_default(user_id)
                if isinstance(plan_dt, datetime):
                    if plan_dt.tzinfo is None:
                        dt = pytz.utc.localize(plan_dt).astimezone(user_tz)
                    else:
                        dt = plan_dt.astimezone(user_tz)
                else:
                    dt = datetime.fromisoformat(str(plan_dt).replace('Z', '+00:00')).astimezone(user_tz)
                current_time_str = f"\n\nТекущее время: {dt.strftime('%d.%m.%Y %H:%M')}"
            
            text = (
                "✏️ <b>Изменение времени сеанса</b>\n\n"
                "Напишите новую дату и время сеанса в ответ на это сообщение.\n"
                "Формат: дата + время\n"
                "Например: 18 января 19:30 или 18.01 19:30" + current_time_str
            )
            
            bot_instance.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML'
            )
        except Exception as e:
            logger.error(f"[TICKET EDIT TIME] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data == "ticket:cancel")
    def ticket_cancel_callback(call):
        """Обработчик кнопки 'Отмена' для билетов"""
        try:
            from moviebot.states import user_ticket_state
            
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            if user_id in user_ticket_state:
                del user_ticket_state[user_id]
            
            bot_instance.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
        except Exception as e:
            logger.error(f"[TICKET CANCEL] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data == "random_event:close")
    def handle_random_event_close(call):
        """Обработчик кнопки 'Закрыть' для случайных уведомлений"""
        try:
            bot_instance.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            
            # Удаляем сообщение
            try:
                bot_instance.delete_message(chat_id, message_id)
                logger.info(f"[RANDOM EVENTS] Сообщение {message_id} закрыто пользователем {call.from_user.id}")
            except Exception as e:
                logger.warning(f"[RANDOM EVENTS] Не удалось удалить сообщение {message_id}: {e}")
                # Если не удалось удалить, просто отвечаем на callback
                bot_instance.answer_callback_query(call.id, "Сообщение закрыто")
        except Exception as e:
            logger.error(f"[RANDOM EVENTS] Ошибка при закрытии случайного события: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot_instance.callback_query_handler(func=lambda call: call.data == "dice_game:start")
    def handle_dice_game_start(call):
        """Обработчик кнопки 'Бросить кубик' для игры в кубик"""
        try:
            from moviebot.bot.bot_init import BOT_ID
            from moviebot.utils.random_events import update_dice_game_message
            from datetime import datetime, timedelta
            
            bot_instance.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            
            # Проверяем, что это групповой чат
            try:
                chat_info = bot_instance.get_chat(chat_id)
                if chat_info.type == 'private':
                    bot_instance.answer_callback_query(call.id, "Игра в кубик работает только в групповых чатах", show_alert=True)
                    return
            except Exception as e:
                logger.warning(f"[DICE GAME] Не удалось получить информацию о чате {chat_id}: {e}")
            
            # Если состояние игры не инициализировано, инициализируем его
            if chat_id not in dice_game_state:
                logger.info(f"[DICE GAME] Инициализация состояния игры для чата {chat_id}")
                dice_game_state[chat_id] = {
                    'participants': {},
                    'message_id': message_id,
                    'start_time': datetime.now(PLANS_TZ),
                    'dice_messages': {}
                }
            
            game_state = dice_game_state[chat_id]
            
            # Проверяем, не истекло ли время игры (24 часа)
            if (datetime.now(PLANS_TZ) - game_state['start_time']).total_seconds() > 86400:
                del dice_game_state[chat_id]
                bot_instance.answer_callback_query(call.id, "Время игры истекло", show_alert=True)
                return
            
            # Проверяем, не бросил ли уже пользователь кубик
            if user_id in game_state.get('participants', {}) and 'dice_message_id' in game_state['participants'][user_id]:
                bot_instance.answer_callback_query(call.id, "Вы уже бросили кубик!", show_alert=True)
                return
            
            # Отправляем стикер игральной кости
            try:
                logger.info(f"[DICE GAME] Попытка отправить кубик для chat_id={chat_id}, user_id={user_id}")
                try:
                    dice_msg = bot_instance.send_dice(chat_id, emoji='🎲')
                    logger.info(f"[DICE GAME] Кубик отправлен с emoji, message_id={dice_msg.message_id if dice_msg else None}")
                except TypeError as e:
                    # Если emoji не поддерживается, используем стандартный кубик
                    logger.warning(f"[DICE GAME] emoji не поддерживается, используем стандартный кубик: {e}")
                    dice_msg = bot_instance.send_dice(chat_id)
                    logger.info(f"[DICE GAME] Стандартный кубик отправлен, message_id={dice_msg.message_id if dice_msg else None}")
                except Exception as e:
                    logger.error(f"[DICE GAME] Ошибка при отправке кубика: {e}", exc_info=True)
                    raise
                
                if dice_msg:
                    # Сохраняем message_id для получения значения позже
                    game_state['dice_messages'] = game_state.get('dice_messages', {})
                    game_state['dice_messages'][dice_msg.message_id] = user_id
                    
                    # Сохраняем информацию об участнике
                    username = call.from_user.username or call.from_user.first_name or f"user_{user_id}"
                    game_state['participants'][user_id] = {
                        'username': username,
                        'dice_message_id': dice_msg.message_id,
                        'user_id': user_id
                    }
                    
                    # Фиксируем в БД, кто бросил кубик
                    with db_lock:
                        cursor.execute('''
                            INSERT INTO stats (user_id, username, command_or_action, timestamp, chat_id)
                            VALUES (%s, %s, %s, %s, %s)
                        ''', (
                            user_id,
                            username,
                            'dice_game:thrown',
                            datetime.now(PLANS_TZ).isoformat(),
                            chat_id
                        ))
                        conn.commit()
                    
                    logger.info(f"[DICE GAME] Пользователь {user_id} ({username}) бросил кубик в чате {chat_id}, message_id={dice_msg.message_id}")
                    
                    # Обновляем сообщение с результатами
                    message_id_to_update = game_state.get('message_id', message_id)
                    update_dice_game_message(chat_id, game_state, message_id_to_update, BOT_ID)
                else:
                    raise Exception("Не удалось отправить кубик")
            except Exception as e:
                logger.error(f"[DICE GAME] Ошибка при отправке кубика: {e}", exc_info=True)
                bot_instance.answer_callback_query(call.id, "Ошибка при отправке кубика", show_alert=True)
        except Exception as e:
            logger.error(f"[DICE GAME] Ошибка в handle_dice_game_start: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    @bot_instance.message_handler(content_types=['dice'])
    def handle_dice_result(message):
        """Обработчик получения значения кубика"""
        try:
            from moviebot.bot.bot_init import BOT_ID
            from moviebot.utils.random_events import update_dice_game_message
            from datetime import datetime, timedelta
            
            if not message.dice or message.dice.emoji != '🎲':
                return
            
            chat_id = message.chat.id
            if chat_id not in dice_game_state:
                return
            
            game_state = dice_game_state[chat_id]
            dice_message_id = message.message_id
            dice_value = message.dice.value
            
            # Находим пользователя по message_id кубика
            user_id = game_state.get('dice_messages', {}).get(dice_message_id)
            if not user_id:
                # Пробуем найти по участникам
                for uid, p in game_state.get('participants', {}).items():
                    if p.get('dice_message_id') == dice_message_id:
                        user_id = uid
                        break
            
            if not user_id:
                return
            
            # Сохраняем значение кубика
            if user_id in game_state['participants']:
                game_state['participants'][user_id]['value'] = dice_value
                game_state['last_dice_time'] = datetime.now(PLANS_TZ)  # Обновляем время последнего броска
                
                # Обновляем сообщение с результатами
                if 'message_id' in game_state:
                    update_dice_game_message(chat_id, game_state, game_state['message_id'], BOT_ID)
        except Exception as e:
            logger.error(f"[DICE GAME] Ошибка в handle_dice_result: {e}", exc_info=True)

    @bot_instance.callback_query_handler(func=lambda call: call.data.startswith("timezone:"))
    def handle_timezone_callback(call):
        """Обработчик выбора часового пояса"""
        try:
            bot_instance.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            timezone_name = call.data.split(":", 1)[1]  # "Moscow" или "Serbia"
            
            if set_user_timezone(user_id, timezone_name):
                tz_display = "Москва" if timezone_name == "Moscow" else "Сербия"
                tz_obj = pytz.timezone('Europe/Moscow' if timezone_name == "Moscow" else 'Europe/Belgrade')
                current_time = datetime.now(tz_obj).strftime('%H:%M')
                
                bot_instance.edit_message_text(
                    f"✅ Часовой пояс установлен: <b>{tz_display}</b>\n\n"
                    f"Текущее время: <b>{current_time}</b>\n\n"
                    f"Все время будет отображаться и планироваться в часовом поясе {tz_display}.\n"
                    f"Часовой пояс будет автоматически обновляться при путешествиях.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                logger.info(f"Часовой пояс установлен для user_id={user_id}: {timezone_name}")
                
                # Проверяем, есть ли сохраненный текст для продолжения планирования
                from moviebot.states import user_plan_state, user_view_film_state
                # Проверяем user_view_film_state
                if user_id in user_view_film_state:
                    state = user_view_film_state[user_id]
                    chat_id = state.get('chat_id', message.chat.id)
                    
                    logger.info(f"[VIEW FILM REPLY] Пользователь {user_id} в user_view_film_state, chat_id={chat_id}")
                    
                    # Обработка ответного сообщения для просмотра фильма
                    from moviebot.bot.handlers.list import handle_view_film_reply_internal
                    handle_view_film_reply_internal(message, state)
                    return
                
                # Проверяем user_plan_state
                if user_id in user_plan_state:
                    state = user_plan_state[user_id]
                    pending_text = state.get('pending_text')
                    if pending_text:
                        logger.info(f"[TIMEZONE CALLBACK] Продолжаем планирование с сохраненным текстом: '{pending_text}'")
                        # Продолжаем планирование с сохраненными данными
                        link = state.get('link')
                        plan_type = state.get('type')
                        pending_plan_dt = state.get('pending_plan_dt')
                        pending_message_date_utc = state.get('pending_message_date_utc')
                        chat_id_from_state = state.get('chat_id', chat_id)
                        
                        if link and plan_type and pending_plan_dt:
                            # Импортируем process_plan из handlers/plan
                            from moviebot.bot.handlers.plan import process_plan
                            # Вызываем process_plan с сохраненными данными
                            result = process_plan(bot_instance, user_id, chat_id_from_state, link, plan_type, pending_plan_dt, pending_message_date_utc)
                            if result:
                                # Очищаем сохраненные данные
                                if 'pending_text' in state:
                                    del state['pending_text']
                                if 'pending_plan_dt' in state:
                                    del state['pending_plan_dt']
                                if 'pending_message_date_utc' in state:
                                    del state['pending_message_date_utc']
                                del user_plan_state[user_id]
                                logger.info(f"[TIMEZONE CALLBACK] Планирование успешно завершено")
                            else:
                                logger.warning(f"[TIMEZONE CALLBACK] Ошибка при продолжении планирования")
                        else:
                            logger.warning(f"[TIMEZONE CALLBACK] Недостаточно данных для продолжения планирования: link={link}, plan_type={plan_type}, pending_plan_dt={pending_plan_dt}")
            else:
                bot_instance.answer_callback_query(call.id, "Ошибка сохранения часового пояса", show_alert=True)
        except Exception as e:
            logger.error(f"[SETTINGS] Ошибка в handle_timezone_callback: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "Произошла ошибка", show_alert=True)
            except:
                pass

    # Обработчик ссылок на Кинопоиск вынесен на уровень модуля для правильной регистрации
    pass

# Обработчик ссылок на Кинопоиск - вынесен на уровень модуля для правильной регистрации
@bot_instance.message_handler(content_types=['text'], func=lambda m: m.text and not m.text.strip().startswith('/') and ('kinopoisk.ru' in m.text.lower() or 'kinopoisk.com' in m.text.lower()))
def handle_kinopoisk_link(message):
    """Обработчик текстовых сообщений со ссылками на Кинопоиск"""
    logger.info(f"[KINOPOISK LINK] ===== START: message_id={message.message_id}, user_id={message.from_user.id}, chat_id={message.chat.id}")
    try:
        from moviebot.bot.bot_init import BOT_ID
        
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip()
        
        logger.info(f"[KINOPOISK LINK] Текст сообщения: '{text[:100]}'")
        
        # Пропускаем обработку, если это реплай на сообщение бота (для таких случаев есть отдельные handlers)
        if message.reply_to_message and message.reply_to_message.from_user and message.reply_to_message.from_user.id == BOT_ID:
            reply_text = message.reply_to_message.text or ""
            # Проверяем, не является ли это реплаем на конкретные промпты бота
            if any(prompt in reply_text for prompt in [
                "Пришлите ссылку или ID фильма в ответном сообщении",
                "Пришлите в ответном сообщении ссылку или ID фильма",
                "В ответном сообщении пришлите ID фильмов"
            ]):
                logger.info(f"[KINOPOISK LINK] Сообщение является реплаем на промпт бота, пропускаем обработку (будет обработано отдельным handler)")
                return
        
        # Проверяем, не находится ли пользователь в состоянии планирования или просмотра фильма
        from moviebot.states import user_plan_state, user_view_film_state
        if user_id in user_plan_state:
            # Если пользователь в состоянии планирования и пришла ссылка - прерываем планирование
            logger.info(f"[KINOPOISK LINK] Пользователь {user_id} в состоянии планирования, прерываем планирование и обрабатываем ссылку")
            bot_instance.reply_to(message, "⚠️ Планирование прервано. Обрабатываю ссылку...")
            del user_plan_state[user_id]
        elif user_id in user_view_film_state:
            # Если пользователь в состоянии просмотра фильма - пропускаем обработку ссылки
            logger.info(f"[KINOPOISK LINK] Пользователь {user_id} в состоянии просмотра фильма, пропускаем обработку ссылки")
            return
        
        logger.info(f"[KINOPOISK LINK] Получена ссылка от {user_id}: {text[:100]}")
        
        # Используем extract_kp_id_from_text для извлечения ID (он уже правильно обрабатывает все форматы)
        kp_id = extract_kp_id_from_text(text)
        if not kp_id:
            logger.warning(f"[KINOPOISK LINK] Не удалось извлечь kp_id из текста: {text[:200]}")
            bot_instance.reply_to(message, f"❌ Не удалось извлечь ID из ссылки: {text}")
            return
        
        # Нормализуем ссылку - используем единый формат без www
        if text.strip().startswith('http'):
            # Если это ссылка, нормализуем её
            link = text.strip()
            # Убираем www для единообразия
            link = re.sub(r'https?://www\.', 'https://', link)
            # Убираем trailing slash если есть
            link = link.rstrip('/')
        else:
            # Если это просто ID, создаем ссылку
            link = f"https://kinopoisk.ru/film/{kp_id}"
        
        logger.info(f"[KINOPOISK LINK] Обработка ссылки: {link}, kp_id={kp_id}")
        
        # ВСЕГДА получаем информацию о фильме/сериале из API (даже если фильм уже в базе)
        # Это нужно для получения актуальных данных (описание, актеры, режиссер и т.д.)
        logger.info(f"[KINOPOISK LINK] ⚠️ ВАЖНО: Отправка запроса к API Кинопоиска для получения актуальной информации (даже если фильм уже в базе)")
        logger.info(f"[KINOPOISK LINK] Вызов extract_movie_info для link={link}")
        try:
            info = extract_movie_info(link)
            if not info:
                logger.warning(f"[KINOPOISK LINK] extract_movie_info вернул None для link={link}")
                bot_instance.reply_to(message, "❌ Не удалось получить информацию о фильме/сериале.")
                return
            logger.info(f"[KINOPOISK LINK] ✅ extract_movie_info успешно, получены актуальные данные: title={info.get('title')}, is_series={info.get('is_series')}")
        except Exception as api_e:
            logger.error(f"[KINOPOISK LINK] ❌ Ошибка extract_movie_info: {api_e}", exc_info=True)
            bot_instance.reply_to(message, f"❌ Ошибка при получении информации о фильме/сериале: {str(api_e)}")
            return
        
        is_series = info.get('is_series', False)
        
        # Проверяем, есть ли уже в базе (для определения статуса просмотра и оценки)
        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if row:
                # Уже в базе - ОБНОВЛЯЕМ данные в базе актуальными данными из API
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                logger.info(f"[KINOPOISK LINK] Фильм уже в базе (film_id={film_id}), ОБНОВЛЯЮ данные актуальными данными из API")
                
                # Обновляем данные в базе актуальными данными из API
                cursor.execute('''
                    UPDATE movies 
                    SET title = %s, year = %s, genres = %s, description = %s, 
                        director = %s, actors = %s, is_series = %s, link = %s
                    WHERE id = %s
                ''', (
                    info['title'],
                    info['year'],
                    info.get('genres', '—'),
                    info.get('description', 'Нет описания'),
                    info.get('director', 'Не указан'),
                    info.get('actors', '—'),
                    1 if is_series else 0,
                    link,
                    film_id
                ))
                conn.commit()
                logger.info(f"[KINOPOISK LINK] ✅ Данные в базе обновлены актуальными данными из API")
                
                # Получаем обновленные данные из базы
                cursor.execute("SELECT title, watched FROM movies WHERE id = %s", (film_id,))
                movie_row = cursor.fetchone()
                title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[0]
                watched = movie_row.get('watched') if isinstance(movie_row, dict) else movie_row[1]
                
                logger.info(f"[KINOPOISK LINK] Вызываю show_film_info_with_buttons с актуальными данными из API (обновленными в базе)")
                show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title, watched), message_id=None)
                logger.info(f"[KINOPOISK LINK] show_film_info_with_buttons завершена для kp_id={kp_id}")
                return
        
        # НЕ в базе - показываем описание с ВСЕМИ кнопками БЕЗ добавления в базу
        logger.info(f"[KINOPOISK LINK] Фильм НЕ в базе, вызываю show_film_info_without_adding: kp_id={kp_id}, chat_id={chat_id}")
        show_film_info_without_adding(chat_id, user_id, info, link, kp_id)
        logger.info(f"[KINOPOISK LINK] show_film_info_without_adding завершена для kp_id={kp_id}")
        
    except Exception as e:
        logger.error(f"[KINOPOISK LINK] ===== END: КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке ссылки.")
        except:
            pass
    finally:
        logger.info(f"[KINOPOISK LINK] ===== END: message_id={getattr(message, 'message_id', 'N/A')}")

    @bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("settings:"))
    def handle_settings_callback(call):
        """Обработчик callback для настроек"""
        logger.info(f"[SETTINGS CALLBACK] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[SETTINGS CALLBACK] callback_id={call.id}, message_id={call.message.message_id if call.message else None}")
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            action = call.data.split(":", 1)[1]
            is_private = call.message.chat.type == 'private'
            
            logger.info(f"[SETTINGS CALLBACK] Получен callback от {user_id}, action={action}, chat_id={chat_id}, is_private={is_private}, callback_data={call.data}")
            logger.info(f"[SETTINGS CALLBACK] bot_instance: {bot_instance}, type: {type(bot_instance)}")
            
            # Вызываем answer_callback_query в самом начале (как в рабочей версии)
            # Но сначала обрабатываем заблокированные кнопки
            if action == "notifications_locked":
                # Заблокированная кнопка настроек напоминаний
                try:
                    bot_instance.answer_callback_query(
                        call.id,
                        "⏰ Настройки напоминаний доступны с подпиской 🔔 Уведомления или 📦 Все режимы. Подключите подписку через /payment",
                        show_alert=True
                    )
                except Exception as e:
                    logger.error(f"[SETTINGS] Ошибка при ответе на callback для notifications_locked: {e}")
                return
            
            if action == "import_locked":
                # Заблокированная кнопка импорта базы
                try:
                    bot_instance.answer_callback_query(
                        call.id,
                        "📥 Импорт базы из Кинопоиска доступен с подпиской 🎯 Рекомендации или 📦 Все режимы. Подключите подписку через /payment",
                        show_alert=True
                    )
                except Exception as e:
                    logger.error(f"[SETTINGS] Ошибка при ответе на callback для import_locked: {e}")
                return
            
            if action == "random_events_locked":
                # Показываем сообщение о том, что раздел доступен только в групповых чатах
                try:
                    bot_instance.answer_callback_query(
                        call.id,
                        "🎲 Случайные события доступны только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                        show_alert=True
                    )
                except Exception as e:
                    logger.error(f"[SETTINGS] Ошибка при ответе на callback для random_events_locked: {e}")
                return
            
            # Проверяем random_events для личных чатов ПЕРЕД общим answer_callback_query
            if action == "random_events":
                # Проверяем, что это не личный чат
                if is_private:
                    bot_instance.answer_callback_query(
                        call.id,
                        "Раздел доступен только в групповых чатах. Создайте групповой чат с друзьями, добавьте в него бота и планируйте просмотр кино вместе 👥",
                        show_alert=True
                    )
                    return
                
                # Для групповых чатов вызываем answer_callback_query
                bot_instance.answer_callback_query(call.id)
                
                # Показываем настройку случайных событий
                with db_lock:
                    cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'random_events_enabled'", (chat_id,))
                    row = cursor.fetchone()
                    is_enabled = True
                    if row:
                        value = row.get('value') if isinstance(row, dict) else row[0]
                        is_enabled = value == 'true'
                
                markup = InlineKeyboardMarkup(row_width=1)
                if is_enabled:
                    markup.add(InlineKeyboardButton("❌ Выключить", callback_data="settings:random_events:disable"))
                else:
                    markup.add(InlineKeyboardButton("✅ Включить", callback_data="settings:random_events:enable"))
                markup.add(InlineKeyboardButton("📋 Пример события с участником", callback_data="settings:random_events:example:with_user"))
                markup.add(InlineKeyboardButton("📋 Пример события без участника", callback_data="settings:random_events:example:without_user"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                status_text = "включены" if is_enabled else "выключены"
                bot_instance.edit_message_text(
                    f"🎲 <b>Случайные события</b>\n\n"
                    f"Текущий статус: <b>{status_text}</b>\n\n"
                    f"Случайные события включают:\n"
                    f"• Предложение рандомного фильма, если на выходных нет планов\n"
                    f"• Выбор случайного участника для выбора фильма (раз в 2 недели)\n"
                    f"• Игра в кубик для выбора фильма (раз в 2 недели)\n"
                    f"• Напоминание о премьерах, если давно не добавляли фильмы в кино",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            # Для остальных действий вызываем обычный answer_callback_query в начале
            bot_instance.answer_callback_query(call.id)
            
            if action == "emoji":
                # Показываем настройки эмодзи
                logger.info(f"[SETTINGS CALLBACK] Обработка action=emoji для user_id={user_id}, chat_id={chat_id}")
                current = get_watched_emojis(chat_id)
                current_emojis_str = ''.join(current) if isinstance(current, list) else str(current)
                logger.info(f"[SETTINGS CALLBACK] Текущие эмодзи: {current_emojis_str}")
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("➕ Добавить к текущим", callback_data="settings:add"))
                markup.add(InlineKeyboardButton("🔄 Заменить полностью", callback_data="settings:replace"))
                markup.add(InlineKeyboardButton("🗑️ Сбросить", callback_data="settings:reset"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                bot_instance.edit_message_text(
                    f"😀 <b>Настройка эмодзи просмотра</b>\n\n"
                    f"<b>Текущие реакции:</b> {current_emojis_str}\n\n"
                    f"Выберите действие или поставьте реакцию на это сообщение — она автоматически добавится к текущим.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                logger.info(f"[SETTINGS CALLBACK] Сообщение с настройками эмодзи обновлено для user_id={user_id}")
                
                # Сохраняем состояние для обработки реакций
                user_settings_state[user_id] = {
                    'settings_msg_id': call.message.message_id,
                    'chat_id': chat_id,
                    'adding_reactions': False
                }
                settings_messages[call.message.message_id] = {
                    'user_id': user_id,
                    'action': 'add',
                    'chat_id': chat_id
                }
                return
            
            if action == "notifications":
                # Проверяем доступ к настройкам напоминаний
                if not has_notifications_access(chat_id, user_id):
                    bot_instance.answer_callback_query(
                        call.id,
                        "🔒 Функционал можно подключить через /payment",
                        show_alert=True
                    )
                    return
                
                # Показываем настройки времени напоминаний
                notify_settings = get_notification_settings(chat_id)
                
                separate = notify_settings.get('separate_weekdays', 'true') == 'true'
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("📅 Разделять будни/выходные", callback_data="settings:notify:separate_toggle"))
                markup.add(InlineKeyboardButton("🏠 Домашний просмотр", callback_data="settings:notify:home"))
                markup.add(InlineKeyboardButton("🎬 Просмотр в кино", callback_data="settings:notify:cinema"))
                markup.add(InlineKeyboardButton("🎫 Билеты на сеанс", callback_data="settings:notify:tickets"))
                markup.add(InlineKeyboardButton("📋 Регулярные напоминания", callback_data="settings:notify:regular_reminders"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                separate_text = "✅ Включено" if separate else "❌ Выключено"
                home_weekday = f"{notify_settings.get('home_weekday_hour', 19):02d}:{notify_settings.get('home_weekday_minute', 0):02d}"
                home_weekend = f"{notify_settings.get('home_weekend_hour', 9):02d}:{notify_settings.get('home_weekend_minute', 0):02d}"
                cinema_weekday = f"{notify_settings.get('cinema_weekday_hour', 9):02d}:{notify_settings.get('cinema_weekday_minute', 0):02d}"
                cinema_weekend = f"{notify_settings.get('cinema_weekend_hour', 9):02d}:{notify_settings.get('cinema_weekend_minute', 0):02d}"
                ticket_minutes = notify_settings.get('ticket_before_minutes', 10)
                
                if ticket_minutes == -1:
                    ticket_text = "Не присылать отдельно"
                elif ticket_minutes == 0:
                    ticket_text = "Вместе с уведомлением"
                else:
                    ticket_text = f"За {ticket_minutes} минут"
                
                text = f"⏰ <b>Настройки напоминаний</b>\n\n"
                text += f"📅 Разделение будни/выходные: <b>{separate_text}</b>\n\n"
                text += f"🏠 <b>Домашний просмотр:</b>\n"
                if separate:
                    text += f"   Будни: <b>{home_weekday}</b>\n"
                    text += f"   Выходные: <b>{home_weekend}</b>\n"
                else:
                    text += f"   Время: <b>{home_weekday}</b>\n"
                text += f"\n🎬 <b>Просмотр в кино:</b>\n"
                if separate:
                    text += f"   Будни: <b>{cinema_weekday}</b>\n"
                    text += f"   Выходные: <b>{cinema_weekend}</b>\n"
                else:
                    text += f"   Время: <b>{cinema_weekday}</b>\n"
                text += f"\n🎫 <b>Билеты на сеанс:</b> <b>{ticket_text}</b>"
                
                bot_instance.edit_message_text(
                    text,
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            if action == "import":
                # Импорт базы из Кинопоиска
                user_import_state[user_id] = {
                    'step': 'waiting_user_id',
                    'kp_user_id': None,
                    'count': None
                }
                bot_instance.edit_message_text(
                    f"📥 <b>Импорт базы из Кинопоиска</b>\n\n"
                    f"Отправьте ID пользователя Кинопоиска или ссылку на профиль.\n\n"
                    f"Примеры:\n"
                    f"• <code>1931396</code>\n"
                    f"• <code>https://www.kinopoisk.ru/user/1931396</code>",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                logger.info(f"[SETTINGS] Импорт базы - состояние установлено для user_id={user_id}")
                return
            
            
            if action.startswith("random_events:example:"):
                # Отправка примера случайного события
                example_type = action.split(":")[-1]  # with_user или without_user
                
                # Проверяем, что это групповой чат
                try:
                    chat_info = bot_instance.get_chat(chat_id)
                    if chat_info.type == 'private':
                        bot_instance.answer_callback_query(call.id, "Примеры событий работают только в групповых чатах", show_alert=True)
                        return
                except Exception as e:
                    logger.warning(f"[RANDOM EVENTS EXAMPLE] Не удалось получить информацию о чате {chat_id}: {e}")
                    bot_instance.answer_callback_query(call.id, "Ошибка при отправке примера", show_alert=True)
                    return
                
                bot_instance.answer_callback_query(call.id, "Отправляю пример события...")
                
                import random
                
                if example_type == "with_user":
                    # Пример события с участником (выбор случайного участника)
                    with db_lock:
                        cursor.execute('''
                            SELECT DISTINCT user_id, username 
                            FROM stats 
                            WHERE chat_id = %s 
                            LIMIT 10
                        ''', (chat_id,))
                        participants = cursor.fetchall()
                    
                    if participants:
                        participant = random.choice(participants)
                        p_user_id = participant.get('user_id') if isinstance(participant, dict) else participant[0]
                        username = participant.get('username') if isinstance(participant, dict) else participant[1]
                        
                        if username:
                            user_name = f"@{username}"
                        else:
                            try:
                                user_info = bot_instance.get_chat_member(chat_id, p_user_id)
                                user_name = user_info.user.first_name or "участник"
                            except:
                                user_name = "участник"
                    else:
                        user_name = "участник"
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🎲 Найти фильм", callback_data="rand_final:go"))
                    markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
                    markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
                    
                    text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
                    text += f"Он выбрал <b>{user_name}</b> для выбора фильма для вашей компании."
                    
                    bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                else:
                    # Пример события без участника (игра в кубик)
                    # Проверяем количество участников (исключая бота)
                    from moviebot.bot.bot_init import BOT_ID
                    from moviebot.database.db_operations import is_bot_participant
                    
                    with db_lock:
                        if BOT_ID:
                            cursor.execute('''
                                SELECT COUNT(DISTINCT user_id) 
                                FROM stats 
                                WHERE chat_id = %s 
                                AND user_id != %s
                            ''', (chat_id, BOT_ID))
                        else:
                            cursor.execute('''
                                SELECT COUNT(DISTINCT user_id) 
                                FROM stats 
                                WHERE chat_id = %s
                            ''', (chat_id,))
                        participants_count_row = cursor.fetchone()
                        participants_count = participants_count_row.get('count') if isinstance(participants_count_row, dict) else (participants_count_row[0] if participants_count_row else 0)
                    
                    # Если в группе только 1 участник (человек) + бот = всего 2, то недостаточно
                    if participants_count < 2:
                        bot_instance.answer_callback_query(
                            call.id,
                            "Не хватает еще хотя бы одного человека в группе. Для игры в кубик нужно минимум 2 участника (исключая бота).",
                            show_alert=True
                        )
                        return
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("🎲 Бросить кубик", callback_data="dice_game:start"))
                    markup.add(InlineKeyboardButton("❌ Отменить такие уведомления", callback_data="reminder:disable:random_events"))
                    markup.add(InlineKeyboardButton("❌ Закрыть", callback_data="random_event:close"))
                    
                    text = "🔮 Вас посетил дух выбора случайного фильма!\n\n"
                    text += "Испытайте удачу и определите, кто выберет фильм для вашей компании.\n\n"
                    text += f"⏳ Осталось бросить кубик: {participants_count} участник(ов)"
                    
                    sent_msg = bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                    
                    # Инициализируем состояние игры для примера события
                    if chat_id not in dice_game_state:
                        dice_game_state[chat_id] = {
                            'participants': {},
                            'message_id': sent_msg.message_id,
                            'start_time': datetime.now(PLANS_TZ),
                            'dice_messages': {}
                        }
                        logger.info(f"[RANDOM EVENTS EXAMPLE] Инициализировано состояние игры для примера события в чате {chat_id}, message_id={sent_msg.message_id}")
                
                return
            
            if action.startswith("random_events:"):
                # Включение/выключение случайных событий
                sub_action = action.split(":", 1)[1]
                new_value = 'true' if sub_action == 'enable' else 'false'
                
                with db_lock:
                    cursor.execute('''
                        INSERT INTO settings (chat_id, key, value)
                        VALUES (%s, 'random_events_enabled', %s)
                        ON CONFLICT (chat_id, key) DO UPDATE SET value = EXCLUDED.value
                    ''', (chat_id, new_value))
                    conn.commit()
                
                status_text = "включены" if new_value == 'true' else "выключены"
                bot_instance.answer_callback_query(call.id, f"Случайные события {status_text}")
                
                # Обновляем сообщение
                markup = InlineKeyboardMarkup(row_width=1)
                if new_value == 'true':
                    markup.add(InlineKeyboardButton("❌ Выключить", callback_data="settings:random_events:disable"))
                else:
                    markup.add(InlineKeyboardButton("✅ Включить", callback_data="settings:random_events:enable"))
                markup.add(InlineKeyboardButton("📋 Пример события с участником", callback_data="settings:random_events:example:with_user"))
                markup.add(InlineKeyboardButton("📋 Пример события без участника", callback_data="settings:random_events:example:without_user"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                bot_instance.edit_message_text(
                    f"🎲 <b>Случайные события</b>\n\n"
                    f"Текущий статус: <b>{status_text}</b>\n\n"
                    f"Случайные события включают:\n"
                    f"• Предложение рандомного фильма, если на выходных нет планов\n"
                    f"• Выбор случайного участника для выбора фильма (раз в 2 недели)\n"
                    f"• Игра в кубик для выбора фильма (раз в 2 недели)\n"
                    f"• Напоминание о премьерах, если давно не добавляли фильмы в кино",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            if action == "timezone":
                # Показываем выбор часового пояса
                current_tz = get_user_timezone(user_id)
                current_tz_name = "Москва" if not current_tz or current_tz.zone == 'Europe/Moscow' else "Сербия"
                
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🇷🇺 Москва (Europe/Moscow)", callback_data="timezone:Moscow"))
                markup.add(InlineKeyboardButton("🇷🇸 Сербия (Europe/Belgrade)", callback_data="timezone:Serbia"))
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:back"))
                
                bot_instance.edit_message_text(
                    f"🕐 <b>Выбор часового пояса</b>\n\n"
                    f"Текущий: <b>{current_tz_name}</b>\n\n"
                    f"Выберите часовой пояс. Все время будет отображаться и планироваться в выбранном часовом поясе.",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            if action == "edit":
                # Вызываем команду /edit
                from moviebot.bot.handlers.edit import edit_command
                
                # Удаляем сообщение перед вызовом команды (как в рабочей версии)
                try:
                    bot_instance.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                
                # Создаем полноценный fake_message с всеми необходимыми атрибутами
                class FakeMessage:
                    def __init__(self, call):
                        self.message_id = call.message.message_id
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.date = call.message.date
                        self.text = '/edit'
                    
                    def reply_to(self, text, **kwargs):
                        return bot_instance.send_message(self.chat.id, text, **kwargs)
                
                fake_message = FakeMessage(call)
                edit_command(fake_message)
                return
            
            if action == "clean":
                # Вызываем команду /clean
                from moviebot.bot.handlers.clean import clean_command
                
                # Удаляем сообщение перед вызовом команды (как в рабочей версии)
                try:
                    bot_instance.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                
                # Создаем полноценный fake_message с всеми необходимыми атрибутами
                class FakeMessage:
                    def __init__(self, call):
                        self.message_id = call.message.message_id
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.date = call.message.date
                        self.text = '/clean'
                    
                    def reply_to(self, text, **kwargs):
                        return bot_instance.send_message(self.chat.id, text, **kwargs)
                
                fake_message = FakeMessage(call)
                clean_command(fake_message)
                return
            
            if action == "join":
                # Вызываем команду /join
                from moviebot.bot.handlers.join import join_command
                
                # Удаляем сообщение перед вызовом команды (как в рабочей версии)
                try:
                    bot_instance.delete_message(chat_id, call.message.message_id)
                except:
                    pass
                
                # Создаем полноценный fake_message с всеми необходимыми атрибутами
                class FakeMessage:
                    def __init__(self, call):
                        self.message_id = call.message.message_id
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.date = call.message.date
                        self.text = '/join'
                    
                    def reply_to(self, text, **kwargs):
                        return bot_instance.send_message(self.chat.id, text, **kwargs)
                
                fake_message = FakeMessage(call)
                join_command(fake_message)
                return
            
            if action == "back":
                # Возврат к главному меню settings
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("😀 Настроить эмодзи просмотра", callback_data="settings:emoji"))
                markup.add(InlineKeyboardButton("🕐 Выбрать часовой пояс", callback_data="settings:timezone"))
                
                # Проверяем доступ к настройкам напоминаний
                if has_notifications_access(chat_id, user_id):
                    markup.add(InlineKeyboardButton("⏰ Настройки напоминаний", callback_data="settings:notifications"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Настройки напоминаний", callback_data="settings:notifications_locked"))
                
                # Проверяем доступ к импорту базы
                if has_recommendations_access(chat_id, user_id):
                    markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Импорт базы из Кинопоиска", callback_data="settings:import_locked"))
                
                # Проверяем, является ли чат личным (случайные события доступны только в группах)
                if is_private:
                    markup.add(InlineKeyboardButton("🔒 Случайные события", callback_data="settings:random_events_locked"))
                else:
                    markup.add(InlineKeyboardButton("🎲 Случайные события", callback_data="settings:random_events"))
                markup.add(InlineKeyboardButton("✏️ Редактировать записи", callback_data="settings:edit"))
                markup.add(InlineKeyboardButton("🗑️ Очистка базы", callback_data="settings:clean"))
                markup.add(InlineKeyboardButton("👥 Участие", callback_data="settings:join"))
                markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
                
                bot_instance.edit_message_text(
                    f"⚙️ <b>Настройки</b>\n\n"
                    f"Выберите, что хотите настроить:",
                    call.message.chat.id,
                    call.message.message_id,
                    reply_markup=markup,
                    parse_mode='HTML'
                )
                return
            
            if action == "reset":
                # Сброс к значению по умолчанию для этого чата
                with db_lock:
                    cursor.execute("DELETE FROM settings WHERE chat_id = %s AND key = 'watched_emoji'", (chat_id,))
                    conn.commit()
                bot_instance.edit_message_text(
                    "✅ Реакции сброшены к значению по умолчанию (✅)",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                logger.info(f"Реакции сброшены для чата {chat_id} пользователем {user_id}")
                if user_id in user_settings_state:
                    del user_settings_state[user_id]
                return
            
            if action == "add" or action == "replace":
                # Для add и replace - сохраняем режим и просим отправить эмодзи
                user_settings_state[user_id] = {
                    'adding_reactions': True,
                    'settings_msg_id': call.message.message_id,
                    'action': action,  # "add" или "replace"
                    'chat_id': chat_id
                }
                
                mode_text = "добавлены к текущим" if action == "add" else "заменят текущие"
                bot_instance.edit_message_text(
                    f"⚙️ <b>Настройки реакций</b>\n\n"
                    f"📝 Поставьте выбранный эмодзи в ответ на это сообщение.\n\n"
                    f"Новые реакции будут {mode_text}.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode='HTML'
                )
                # Обновляем информацию о сообщении settings
                if call.message.message_id in settings_messages:
                    settings_messages[call.message.message_id]['action'] = action
                else:
                    settings_messages[call.message.message_id] = {
                        'user_id': user_id,
                        'action': action,
                        'chat_id': call.message.chat.id
                    }
                logger.info(f"[SETTINGS] Пользователь {user_id} выбрал режим: {action}")
                return
            
            # Обработка подменю настроек напоминаний
            if action.startswith("notify:"):
                sub_action = action.split(":", 1)[1]
                notify_settings = get_notification_settings(chat_id)
                
                if sub_action == "separate_toggle":
                    # Переключение разделения будни/выходные
                    current = notify_settings.get('separate_weekdays', 'true')
                    new_value = 'false' if current == 'true' else 'true'
                    set_notification_setting(chat_id, 'notify_separate_weekdays', new_value)
                    bot_instance.answer_callback_query(call.id, f"Разделение будни/выходные {'включено' if new_value == 'true' else 'выключено'}")
                    # Возвращаемся к меню настроек напоминаний
                    action = "notifications"
                    # Рекурсивно вызываем обработчик для обновления меню
                    call.data = f"settings:{action}"
                    handle_settings_callback(call)
                    return
                
                elif sub_action == "tickets":
                    # Настройка времени отправки билетов
                    ticket_minutes = notify_settings.get('ticket_before_minutes', 10)
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(InlineKeyboardButton("⏰ За 10 минут", callback_data="settings:notify:tickets:10"))
                    markup.add(InlineKeyboardButton("⏰ За 30 минут", callback_data="settings:notify:tickets:30"))
                    markup.add(InlineKeyboardButton("⏰ За 1 час", callback_data="settings:notify:tickets:60"))
                    markup.add(InlineKeyboardButton("📨 Вместе с уведомлением", callback_data="settings:notify:tickets:0"))
                    markup.add(InlineKeyboardButton("❌ Не присылать отдельно", callback_data="settings:notify:tickets:-1"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                    
                    if ticket_minutes == -1:
                        ticket_text = "Не присылать отдельно"
                    elif ticket_minutes == 0:
                        ticket_text = "Вместе с уведомлением"
                    else:
                        ticket_text = f"За {ticket_minutes} минут"
                    
                    text = f"🎫 <b>Настройка отправки билетов на сеанс</b>\n\n"
                    text += f"Текущая настройка: <b>{ticket_text}</b>\n\n"
                    text += f"Выберите, когда присылать билеты:"
                    
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    return
                
                elif sub_action.startswith("tickets:"):
                    # Сохранение настройки времени отправки билетов
                    minutes = int(sub_action.split(":", 1)[1])
                    set_notification_setting(chat_id, 'ticket_before_minutes', minutes)
                    
                    if minutes == -1:
                        ticket_text = "Не присылать отдельно"
                    elif minutes == 0:
                        ticket_text = "Вместе с уведомлением"
                    else:
                        ticket_text = f"За {minutes} минут"
                    
                    bot_instance.answer_callback_query(call.id, f"Билеты: {ticket_text}")
                    # Возвращаемся к меню настроек напоминаний
                    call.data = "settings:notifications"
                    handle_settings_callback(call)
                    return
                
                elif sub_action == "home":
                    # Настройка времени для домашнего просмотра
                    separate = notify_settings.get('separate_weekdays', 'true') == 'true'
                    markup = InlineKeyboardMarkup(row_width=1)
                    if separate:
                        markup.add(InlineKeyboardButton("📅 Будни", callback_data="settings:notify:home:weekday"))
                        markup.add(InlineKeyboardButton("🌴 Выходные", callback_data="settings:notify:home:weekend"))
                    else:
                        markup.add(InlineKeyboardButton("⏰ Установить время", callback_data="settings:notify:home:time"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                    
                    home_weekday = f"{notify_settings.get('home_weekday_hour', 19):02d}:{notify_settings.get('home_weekday_minute', 0):02d}"
                    home_weekend = f"{notify_settings.get('home_weekend_hour', 9):02d}:{notify_settings.get('home_weekend_minute', 0):02d}"
                    
                    text = f"🏠 <b>Настройка времени напоминаний для домашнего просмотра</b>\n\n"
                    if separate:
                        text += f"📅 Будни: <b>{home_weekday}</b>\n"
                        text += f"🌴 Выходные: <b>{home_weekend}</b>\n"
                    else:
                        text += f"⏰ Время: <b>{home_weekday}</b>\n"
                    text += f"\nОтправьте время в формате ЧЧ:ММ (например, 19:00 или 09:00)"
                    
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    # Сохраняем состояние для обработки ввода времени
                    if user_id not in user_settings_state:
                        user_settings_state[user_id] = {}
                    user_settings_state[user_id]['waiting_notify_time'] = 'home'
                    user_settings_state[user_id]['notify_separate'] = separate
                    return
                
                elif sub_action.startswith("home:"):
                    # Обработка выбора будни/выходные для домашнего просмотра
                    time_type = sub_action.split(":", 1)[1]  # "weekday" или "weekend"
                    if user_id not in user_settings_state:
                        user_settings_state[user_id] = {}
                    user_settings_state[user_id]['waiting_notify_time'] = f'home_{time_type}'
                    user_settings_state[user_id]['notify_separate'] = True
                    
                    bot_instance.answer_callback_query(call.id)
                    bot_instance.edit_message_text(
                        f"🏠 <b>Настройка времени для домашнего просмотра</b>\n\n"
                        f"📅 {'Будни' if time_type == 'weekday' else 'Выходные'}\n\n"
                        f"Отправьте время в формате ЧЧ:ММ (например, 19:00 или 09:00)",
                        call.message.chat.id,
                        call.message.message_id,
                        parse_mode='HTML'
                    )
                    return
                
                elif sub_action == "cinema":
                    # Настройка времени для кино
                    separate = notify_settings.get('separate_weekdays', 'true') == 'true'
                    markup = InlineKeyboardMarkup(row_width=1)
                    if separate:
                        markup.add(InlineKeyboardButton("📅 Будни", callback_data="settings:notify:cinema:weekday"))
                        markup.add(InlineKeyboardButton("🌴 Выходные", callback_data="settings:notify:cinema:weekend"))
                    else:
                        markup.add(InlineKeyboardButton("⏰ Установить время", callback_data="settings:notify:cinema:time"))
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                    
                    cinema_weekday = f"{notify_settings.get('cinema_weekday_hour', 9):02d}:{notify_settings.get('cinema_weekday_minute', 0):02d}"
                    cinema_weekend = f"{notify_settings.get('cinema_weekend_hour', 9):02d}:{notify_settings.get('cinema_weekend_minute', 0):02d}"
                    
                    text = f"🎬 <b>Настройка времени напоминаний для просмотра в кино</b>\n\n"
                    if separate:
                        text += f"📅 Будни: <b>{cinema_weekday}</b>\n"
                        text += f"🌴 Выходные: <b>{cinema_weekend}</b>\n"
                    else:
                        text += f"⏰ Время: <b>{cinema_weekday}</b>\n"
                    text += f"\nОтправьте время в формате ЧЧ:ММ (например, 09:00)"
                    
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    if user_id not in user_settings_state:
                        user_settings_state[user_id] = {}
                    user_settings_state[user_id]['waiting_notify_time'] = 'cinema'
                    user_settings_state[user_id]['notify_separate'] = separate
                    return
                
                elif sub_action.startswith("cinema:"):
                    # Обработка выбора будни/выходные для кино
                    time_type = sub_action.split(":", 1)[1]  # "weekday" или "weekend"
                    if user_id not in user_settings_state:
                        user_settings_state[user_id] = {}
                    user_settings_state[user_id]['waiting_notify_time'] = f'cinema_{time_type}'
                    user_settings_state[user_id]['notify_separate'] = True
                    
                    bot_instance.answer_callback_query(call.id)
                    bot_instance.edit_message_text(
                        f"🎬 <b>Настройка времени для просмотра в кино</b>\n\n"
                        f"📅 {'Будни' if time_type == 'weekday' else 'Выходные'}\n\n"
                        f"Отправьте время в формате ЧЧ:ММ (например, 09:00)",
                        call.message.chat.id,
                        call.message.message_id,
                        parse_mode='HTML'
                    )
                    return
                
                elif sub_action == "regular_reminders":
                    # Показываем меню регулярных напоминаний
                    with db_lock:
                        # Проверяем статус каждого напоминания
                        cursor.execute("SELECT key, value FROM settings WHERE chat_id = %s AND key IN ('reminder_weekend_films_disabled', 'reminder_cinema_premieres_disabled', 'random_events_enabled')", (chat_id,))
                        reminder_rows = cursor.fetchall()
                        
                        reminders_status = {}
                        for row in reminder_rows:
                            key = row.get('key') if isinstance(row, dict) else row[0]
                            value = row.get('value') if isinstance(row, dict) else row[1]
                            reminders_status[key] = value
                    
                    markup = InlineKeyboardMarkup(row_width=1)
                    
                    # Напоминание о фильмах на выходных
                    weekend_films_disabled = reminders_status.get('reminder_weekend_films_disabled', 'false') == 'true'
                    if weekend_films_disabled:
                        markup.add(InlineKeyboardButton("⏰ Включить: Фильмы на выходных", callback_data="reminder:enable:weekend_films"))
                    else:
                        markup.add(InlineKeyboardButton("❌ Отменить: Фильмы на выходных", callback_data="reminder:disable:weekend_films"))
                    
                    # Напоминание о премьерах в кино
                    cinema_premieres_disabled = reminders_status.get('reminder_cinema_premieres_disabled', 'false') == 'true'
                    if cinema_premieres_disabled:
                        markup.add(InlineKeyboardButton("⏰ Включить: Премьеры в кино", callback_data="reminder:enable:cinema_premieres"))
                    else:
                        markup.add(InlineKeyboardButton("❌ Отменить: Премьеры в кино", callback_data="reminder:disable:cinema_premieres"))
                    
                    # Случайные события (все сразу)
                    random_events_enabled = reminders_status.get('random_events_enabled', 'true') == 'true'
                    if not random_events_enabled:
                        markup.add(InlineKeyboardButton("⏰ Включить: Случайные события", callback_data="reminder:enable:random_events"))
                    else:
                        markup.add(InlineKeyboardButton("❌ Отменить: Случайные события", callback_data="reminder:disable:random_events"))
                    
                    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="settings:notifications"))
                    
                    text = "📋 <b>Регулярные напоминания</b>\n\n"
                    text += "Управление регулярными напоминаниями бота:\n\n"
                    text += "• <b>Фильмы на выходных</b> — напоминание каждую субботу, если нет планов\n"
                    text += "• <b>Премьеры в кино</b> — напоминание о премьерах, если давно не добавляли фильмы в кино\n"
                    text += "• <b>Случайные события</b> — все случайные события (выбор участника, игра в кубик и т.д.)"
                    
                    bot_instance.edit_message_text(
                        text,
                        call.message.chat.id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    return
            
            logger.warning(f"[SETTINGS CALLBACK] Необработанное действие: {action}, callback_data={call.data}")
            try:
                bot_instance.answer_callback_query(call.id, f"Действие '{action}' будет реализовано позже", show_alert=True)
            except:
                pass
        except Exception as e:
            logger.error(f"[SETTINGS CALLBACK] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
        finally:
            logger.info(f"[SETTINGS CALLBACK] ===== КОНЕЦ ОБРАБОТКИ =====")

    # Обработчик текстовых сообщений для поиска (ответы на сообщения поиска)
    @bot_instance.message_handler(content_types=['text'], func=lambda m: m.text and not m.text.strip().startswith('/') and m.from_user.id in user_search_state)
    def handle_search_reply(message):
        """Обработчик ответных сообщений для поиска"""
        logger.info(f"[SEARCH REPLY] ===== НАЧАЛО ОБРАБОТКИ =====")
        logger.info(f"[SEARCH REPLY] Получено сообщение: user_id={message.from_user.id}, text={message.text[:50] if message.text else 'None'}, has_reply={message.reply_to_message is not None}")
        try:
            user_id = message.from_user.id
            chat_id = message.chat.id
            query = message.text.strip()
            
            logger.info(f"[SEARCH REPLY] Проверка состояния: user_id={user_id}, user_search_state keys={list(user_search_state.keys())}")
            
            # Проверяем, находится ли пользователь в состоянии поиска
            if user_id not in user_search_state:
                logger.info(f"[SEARCH REPLY] Пользователь {user_id} не в состоянии поиска, пропускаем")
                return  # Не обрабатываем, если пользователь не в состоянии поиска
            
            state = user_search_state[user_id]
            reply_to_message = message.reply_to_message
            
            logger.info(f"[SEARCH REPLY] Состояние найдено: state={state}, reply_to_message_id={reply_to_message.message_id if reply_to_message else 'None'}, state_message_id={state.get('message_id')}")
            
            # Если пользователь в состоянии поиска, обрабатываем его сообщение
            # Не требуем точного совпадения message_id, так как состояние может быть обновлено
            logger.info(f"[SEARCH REPLY] Пользователь {user_id} в состоянии поиска, обрабатываем запрос: {query}")
            
            # Получаем тип поиска из состояния
            search_type = state.get('search_type', 'mixed')
            logger.info(f"[SEARCH REPLY] Тип поиска: {search_type}")
            
            # Выполняем поиск
            logger.info(f"[SEARCH REPLY] Вызов search_films_with_type для query={query}, search_type={search_type}")
            films, total_pages = search_films_with_type(query, page=1, search_type=search_type)
            logger.info(f"[SEARCH REPLY] Поиск завершен: найдено {len(films)} результатов, страниц: {total_pages}")
            
            if not films:
                logger.warning(f"[SEARCH REPLY] Ничего не найдено по запросу '{query}'")
                bot_instance.reply_to(message, f"❌ Ничего не найдено по запросу '{query}'")
                # Очищаем состояние
                del user_search_state[user_id]
                return
            
            # Формируем сообщение с результатами
            results_text = f"🔍 Результаты поиска '{query}':\n\n"
            markup = InlineKeyboardMarkup(row_width=1)
            
            for film in films[:10]:  # Показываем максимум 10 результатов на странице
                title = film.get('nameRu') or film.get('nameEn') or film.get('title') or "Без названия"
                year = film.get('year') or film.get('releaseYear') or 'N/A'
                rating = film.get('ratingKinopoisk') or film.get('rating') or film.get('ratingImdb') or 'N/A'
                kp_id = film.get('kinopoiskId') or film.get('filmId') or film.get('id')
                
                # Определяем тип (сериал или фильм)
                film_type = film.get('type', '').upper()
                is_series = film_type == 'TV_SERIES'
                
                if kp_id:
                    type_indicator = "📺" if is_series else "🎬"
                    button_text = f"{type_indicator} {title} ({year})"
                    if len(button_text) > 50:
                        button_text = button_text[:47] + "..."
                    results_text += f"• {type_indicator} <b>{title}</b> ({year})"
                    if rating != 'N/A':
                        results_text += f" ⭐ {rating}"
                    results_text += "\n"
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
            
            logger.info(f"[SEARCH REPLY] Отправка результатов поиска пользователю {user_id}")
            results_msg = bot_instance.reply_to(message, results_text, reply_markup=markup, parse_mode='HTML')
            
            # Обновляем состояние
            if results_msg:
                user_search_state[user_id] = {
                    'chat_id': chat_id,
                    'message_id': results_msg.message_id,
                    'search_type': search_type,
                    'query': query,
                    'results_text': results_text,
                    'films': films[:10],
                    'total_pages': total_pages
                }
            
            logger.info(f"[SEARCH REPLY] Результаты поиска отправлены пользователю {user_id}, найдено {len(films)} результатов")
        except Exception as e:
            logger.error(f"[SEARCH REPLY] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.reply_to(message, "❌ Произошла ошибка при обработке запроса поиска")
            except:
                pass

# Обработчик кнопки результата поиска "add_film_{kp_id}:{film_type}" - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("add_film_"))
def add_film_from_search_callback(call):
        """Обработчик кнопки результата поиска - показывает информацию о фильме"""
        logger.info("=" * 80)
        logger.info(f"[ADD FILM FROM SEARCH] ===== START: callback_id={call.id}, callback_data={call.data}")
        try:
            bot_instance.answer_callback_query(call.id, text="⏳ Загружаю информацию...")
            logger.info(f"[ADD FILM FROM SEARCH] answer_callback_query вызван, callback_id={call.id}")
            
            # Парсим callback_data: add_film_{kp_id}:{film_type}
            parts = call.data.split(":")
            if len(parts) < 2:
                logger.error(f"[ADD FILM FROM SEARCH] Неверный формат callback_data: {call.data}")
                bot_instance.answer_callback_query(call.id, "❌ Ошибка: неверный формат", show_alert=True)
                return
            
            kp_id = parts[0].replace("add_film_", "")
            film_type = parts[1] if len(parts) > 1 else "FILM"
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[ADD FILM FROM SEARCH] kp_id={kp_id}, film_type={film_type}, user_id={user_id}, chat_id={chat_id}")
            
            # Формируем ссылку на Кинопоиск
            if film_type == "TV_SERIES" or film_type == "MINI_SERIES":
                link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            else:
                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            
            # Получаем информацию о фильме через API
            from moviebot.api.kinopoisk_api import extract_movie_info
            info = extract_movie_info(link)
            
            if not info:
                logger.error(f"[ADD FILM FROM SEARCH] Не удалось получить информацию о фильме: kp_id={kp_id}")
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Проверяем, есть ли фильм уже в базе
            from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
            conn = get_db_connection()
            cursor = get_db_cursor()
            
            existing = None
            with db_lock:
                cursor.execute("SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                row = cursor.fetchone()
                if row:
                    film_id = row.get('id') if isinstance(row, dict) else row[0]
                    title = row.get('title') if isinstance(row, dict) else row[1]
                    watched = row.get('watched') if isinstance(row, dict) else row[2]
                    existing = (film_id, title, watched)
            
            # Показываем карточку фильма с кнопками
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing)
            
            logger.info(f"[ADD FILM FROM SEARCH] ===== END: успешно показана информация о фильме {kp_id}")
        except Exception as e:
            logger.error(f"[ADD FILM FROM SEARCH] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
        finally:
            logger.info(f"[ADD FILM FROM SEARCH] ===== END: callback_id={call.id}")

# Обработчик кнопки "➕ Добавить в базу" - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("add_to_database:"))
def add_to_database_callback(call):
    """Обработчик кнопки '➕ Добавить в базу'"""
    logger.info("=" * 80)
    logger.info(f"[ADD TO DATABASE] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        bot_instance.answer_callback_query(call.id, text="⏳ Добавляю в базу...")
        logger.info(f"[ADD TO DATABASE] answer_callback_query вызван, callback_id={call.id}")
        
        kp_id = call.data.split(":")[1]
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        logger.info(f"[ADD TO DATABASE] Пользователь {user_id} хочет добавить фильм kp_id={kp_id} в базу, chat_id={chat_id}")
        
        # Получаем информацию о фильме/сериале
        # Проверяем, фильм это или сериал
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        logger.info(f"[ADD TO DATABASE] Вызываю extract_movie_info для link={link}")
        try:
            info = extract_movie_info(link)
            logger.info(f"[ADD TO DATABASE] extract_movie_info завершен, info={'получен' if info else 'None'}")
        except Exception as api_e:
            logger.error(f"[ADD TO DATABASE] Ошибка в extract_movie_info: {api_e}", exc_info=True)
            bot_instance.answer_callback_query(call.id, "❌ Ошибка при получении информации о фильме", show_alert=True)
            return
        
        if not info:
            logger.error(f"[ADD TO DATABASE] Не удалось получить информацию о фильме для kp_id={kp_id}")
            bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            return
        
        logger.info(f"[ADD TO DATABASE] Информация получена, title={info.get('title', 'N/A')}, is_series={info.get('is_series', False)}")
        
        # Если это сериал, используем правильную ссылку
        if info.get('is_series') or info.get('type') == 'TV_SERIES':
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
            logger.info(f"[ADD TO DATABASE] Это сериал, обновлена ссылка: {link}")
        
        # Добавляем фильм в базу
        logger.info(f"[ADD TO DATABASE] Вызываю ensure_movie_in_database: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}")
        try:
            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            logger.info(f"[ADD TO DATABASE] ensure_movie_in_database завершен: film_id={film_id}, was_inserted={was_inserted}")
        except Exception as db_e:
            logger.error(f"[ADD TO DATABASE] Ошибка в ensure_movie_in_database: {db_e}", exc_info=True)
            bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
            return
        if not film_id:
            logger.error(f"[ADD TO DATABASE] Не удалось добавить фильм в базу для kp_id={kp_id}")
            bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
            return
        
        title = info.get('title', 'Фильм')
        
        if was_inserted:
            bot_instance.answer_callback_query(call.id, f"✅ {title} добавлен в базу!", show_alert=False)
            logger.info(f"[ADD TO DATABASE] Фильм добавлен в базу: film_id={film_id}, title={title}")
            
            # Обновляем сообщение, показывая что фильм теперь в базе
            # Получаем обновленные данные из базы
            with db_lock:
                cursor.execute("SELECT title, watched FROM movies WHERE id = %s", (film_id,))
                movie_row = cursor.fetchone()
                title_db = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[0]
                watched = movie_row.get('watched') if isinstance(movie_row, dict) else movie_row[1]
            
            # Показываем описание с кнопками (теперь фильм в базе)
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title_db, watched), message_id=call.message.message_id)
        else:
            bot_instance.answer_callback_query(call.id, f"ℹ️ {title} уже в базе", show_alert=False)
            logger.info(f"[ADD TO DATABASE] Фильм уже был в базе: film_id={film_id}, title={title}")
    except Exception as e:
        logger.error(f"[ADD TO DATABASE] КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except Exception as answer_e:
            logger.error(f"[ADD TO DATABASE] Не удалось вызвать answer_callback_query: {answer_e}")
    finally:
        logger.info(f"[ADD TO DATABASE] ===== END: callback_id={call.id}")


def show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=None, message_id=None, message_thread_id=None):
    """Показывает описание фильма с кнопками действий
    
    Args:
        chat_id: ID чата
        user_id: ID пользователя
        info: Информация о фильме из API
        link: Ссылка на Кинопоиск
        kp_id: ID фильма на Кинопоиске
        existing: Кортеж (film_id, title, watched) или None
        message_id: ID сообщения для обновления (если None - отправляет новое)
        message_thread_id: ID треда для групповых чатов
    """
    logger.info(f"[SHOW FILM INFO] ===== START: chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, message_id={message_id}, existing={existing}")
    try:
        logger.info(f"[SHOW FILM INFO] info keys: {list(info.keys()) if info else 'None'}")
        if not info:
            logger.error(f"[SHOW FILM INFO] info is None или пустой!")
            bot_instance.send_message(chat_id, "❌ Произошла ошибка: информация о фильме не получена.")
            return
        
        is_series = info.get('is_series', False)
        type_emoji = "📺" if is_series else "🎬"
        logger.info(f"[SHOW FILM INFO] is_series={is_series}, type_emoji={type_emoji}")
        
        # Формируем текст описания
        text = f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
        logger.info(f"[SHOW FILM INFO] Текст начала формироваться, title={info.get('title')}")
        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"
        logger.info(f"[SHOW FILM INFO] Базовый текст сформирован, is_series={is_series}")
        
        # Если это сериал, добавляем информацию о статусе выхода серий
        if is_series:
            logger.info(f"[SHOW FILM INFO] Получение статуса выхода серий для kp_id={kp_id}")
            try:
                is_airing, next_episode = get_series_airing_status(kp_id)
                logger.info(f"[SHOW FILM INFO] is_airing={is_airing}, next_episode={next_episode}")
                if is_airing and next_episode:
                    text += f"\n🟢 <b>Сериал выходит сейчас</b>\n"
                    text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
                else:
                    text += f"\n🔴 <b>Сериал не выходит</b>\n"
            except Exception as airing_e:
                logger.error(f"[SHOW FILM INFO] Ошибка get_series_airing_status: {airing_e}", exc_info=True)
                # Продолжаем без информации о статусе выхода
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        logger.info(f"[SHOW FILM INFO] Ссылка добавлена, existing={existing}")
        
        # Если фильм не в базе, добавляем строку "Ещё не просмотрено"
        if not existing:
            text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
        
        # Если фильм уже в базе, показываем дополнительную информацию
        if existing:
            logger.info(f"[SHOW FILM INFO] Фильм в базе, обрабатываем existing={existing}")
            logger.info(f"[SHOW FILM INFO] Тип existing: {type(existing)}, isinstance dict: {isinstance(existing, dict)}, isinstance tuple: {isinstance(existing, tuple)}")
            try:
                if isinstance(existing, dict):
                    logger.info(f"[SHOW FILM INFO] existing - словарь, извлекаю через .get()")
                    film_id = existing.get('id')
                    watched = existing.get('watched')
                else:
                    logger.info(f"[SHOW FILM INFO] existing - не словарь, извлекаю через индексы, len={len(existing) if hasattr(existing, '__len__') else 'N/A'}")
                    film_id = existing[0] if len(existing) > 0 else None
                    watched = existing[2] if len(existing) > 2 else None
                logger.info(f"[SHOW FILM INFO] Извлечены film_id={film_id}, watched={watched}")
            except Exception as extract_e:
                logger.error(f"[SHOW FILM INFO] ❌ ОШИБКА при извлечении film_id и watched: {extract_e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] existing type: {type(existing)}, value: {existing}")
                # Пытаемся продолжить с дефолтными значениями
                film_id = None
                watched = False
            
            if watched:
                logger.info(f"[SHOW FILM INFO] Фильм просмотрен, запрашиваем оценки...")
                avg = None
                user_rating = None
                try:
                    # Чтение безопасно без блокировки, используем короткий таймаут только для защиты от deadlock
                    import threading
                    lock_acquired = False
                    try:
                        # Короткий таймаут 1 секунда - если lock занят, просто пропускаем запрос
                        lock_acquired = db_lock.acquire(timeout=1.0)
                        if lock_acquired:
                            logger.info(f"[SHOW FILM INFO] db_lock получен, выполняю запрос AVG...")
                            try:
                                cursor.execute('SELECT AVG(rating) as avg FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                                avg_result = cursor.fetchone()
                                logger.info(f"[SHOW FILM INFO] AVG запрос выполнен, результат: {avg_result}")
                                if avg_result:
                                    avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                                    avg = float(avg) if avg is not None else None
                                else:
                                    avg = None
                                
                                # Получаем личную оценку пользователя (если есть)
                                if user_id:
                                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки пользователя user_id={user_id}...")
                                    cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                                    user_rating_row = cursor.fetchone()
                                    logger.info(f"[SHOW FILM INFO] Личная оценка получена: {user_rating_row}")
                                    if user_rating_row:
                                        user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                                    else:
                                        user_rating = None
                            finally:
                                db_lock.release()
                                logger.info(f"[SHOW FILM INFO] db_lock освобожден")
                        else:
                            logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос оценок (не критично)")
                            avg = None
                            user_rating = None
                    except Exception as lock_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при получении lock для оценок: {lock_e}")
                        if lock_acquired:
                            try:
                                db_lock.release()
                            except:
                                pass
                        avg = None
                        user_rating = None
                except Exception as db_e:
                    logger.warning(f"[SHOW FILM INFO] Ошибка при запросе оценок (не критично): {db_e}")
                    avg = None
                    user_rating = None
                
                text += f"\n\n✅ <b>Просмотрено</b>"
                if avg:
                    text += f"\n⭐ <b>Средняя оценка: {avg:.1f}/10</b>"
                # Добавляем строку о личной оценке пользователя (чтобы текст всегда менялся при обновлении)
                if user_rating is not None:
                    text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                else:
                    text += f"\n⭐ <b>Ваша оценка: —</b>"
                logger.info(f"[SHOW FILM INFO] Текст для просмотренного фильма сформирован")
            else:
                logger.info(f"[SHOW FILM INFO] Фильм не просмотрен (watched=False), проверяем личную оценку...")
                text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
                # Добавляем строку о личной оценке пользователя даже если фильм не просмотрен (чтобы текст всегда менялся)
                if user_id and film_id:
                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки (без блокировки, чтение безопасно)...")
                    user_rating = None
                    try:
                        # Чтение безопасно без блокировки, используем короткий таймаут только для защиты от deadlock
                        import threading
                        lock_acquired = False
                        try:
                            # Короткий таймаут 1 секунда - если lock занят, просто пропускаем запрос
                            lock_acquired = db_lock.acquire(timeout=1.0)
                            if lock_acquired:
                                try:
                                    cursor.execute('SELECT rating FROM ratings WHERE chat_id = %s AND film_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id, user_id))
                                    user_rating_row = cursor.fetchone()
                                    logger.info(f"[SHOW FILM INFO] Запрос личной оценки выполнен, результат: {user_rating_row}")
                                    if user_rating_row:
                                        user_rating = user_rating_row.get('rating') if isinstance(user_rating_row, dict) else user_rating_row[0]
                                finally:
                                    db_lock.release()
                                    logger.info(f"[SHOW FILM INFO] db_lock освобожден")
                            else:
                                logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос оценки (не критично)")
                        except Exception as lock_e:
                            logger.warning(f"[SHOW FILM INFO] Ошибка при получении lock для оценки: {lock_e}")
                            if lock_acquired:
                                try:
                                    db_lock.release()
                                except:
                                    pass
                        
                        # Добавляем оценку в текст
                        if user_rating is not None:
                            text += f"\n⭐ <b>Ваша оценка: {user_rating}/10</b>"
                        else:
                            text += f"\n⭐ <b>Ваша оценка: —</b>"
                    except Exception as db_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при запросе оценки (не критично): {db_e}")
                        text += f"\n⭐ <b>Ваша оценка: —</b>"
                else:
                    logger.info(f"[SHOW FILM INFO] user_id или film_id отсутствуют, пропускаем запрос оценки")
                    text += f"\n⭐ <b>Ваша оценка: —</b>"
            logger.info(f"[SHOW FILM INFO] Обработка existing завершена")
        
        # Создаем кнопки
        logger.info(f"[SHOW FILM INFO] Создание кнопок...")
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверяем премьеру
        logger.info(f"[SHOW FILM INFO] Проверка премьеры...")
        russia_release = info.get('russia_release')
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    from datetime import date as date_class
                    today = date_class.today()
                    
                    for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                        date_value = data_main.get(date_field)
                        if date_value:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                                premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                                break
                            except:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения информации о премьере: {e}")
        
        # Если премьера еще не состоялась, добавляем кнопку
        if premiere_date:
            from datetime import date as date_class
            today = date_class.today()
            if premiere_date > today:
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:current_month"))
        
        # Получаем film_id для проверки оценок и планов
        logger.info(f"[SHOW FILM INFO] Получение film_id...")
        film_id = None
        if existing:
            film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
            logger.info(f"[SHOW FILM INFO] film_id из existing: {film_id}")
        else:
            logger.info(f"[SHOW FILM INFO] Запрос film_id из БД...")
            try:
                import threading
                lock_acquired = db_lock.acquire(timeout=1.0)
                if lock_acquired:
                    try:
                        cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                        film_row = cursor.fetchone()
                        if film_row:
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                        logger.info(f"[SHOW FILM INFO] Запрос film_id выполнен, film_id={film_id}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после запроса film_id")
                else:
                    logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос film_id (не критично)")
                    film_id = None
            except Exception as film_id_e:
                logger.warning(f"[SHOW FILM INFO] Ошибка при запросе film_id (не критично): {film_id_e}")
                film_id = None
            logger.info(f"[SHOW FILM INFO] film_id из БД: {film_id}")
        
        # Проверяем, есть ли уже план для этого фильма
        logger.info(f"[SHOW FILM INFO] Проверка планов для film_id={film_id}...")
        has_plan = False
        if film_id:
            try:
                import threading
                lock_acquired = db_lock.acquire(timeout=1.0)
                if lock_acquired:
                    try:
                        cursor.execute('SELECT id FROM plans WHERE film_id = %s AND chat_id = %s LIMIT 1', (film_id, chat_id))
                        plan_row = cursor.fetchone()
                        has_plan = plan_row is not None
                        logger.info(f"[SHOW FILM INFO] Запрос планов выполнен, has_plan={has_plan}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после проверки планов")
                else:
                    logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем проверку планов (не критично)")
                    has_plan = False
            except Exception as plan_e:
                logger.warning(f"[SHOW FILM INFO] Ошибка при проверке планов (не критично): {plan_e}")
                has_plan = False
        logger.info(f"[SHOW FILM INFO] Проверка планов завершена, has_plan={has_plan}")
        
        # Если фильм не в базе, добавляем кнопку "➕ Добавить в базу"
        if not film_id:
            markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{kp_id}"))
        
        # Добавляем кнопку "Запланировать просмотр" только если фильм не запланирован
        if not has_plan:
            markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
        
        # Добавляем кнопки "Интересные факты" и "Оценить" всегда (для фильмов в базе и не в базе)
        logger.info(f"[SHOW FILM INFO] Добавление кнопок оценок для film_id={film_id}...")
        if film_id:
            # Получаем информацию об оценках
            logger.info(f"[SHOW FILM INFO] Запрос оценок из БД...")
            avg_rating = None
            rating_text = "💬 Оценить"
            try:
                import threading
                lock_acquired = db_lock.acquire(timeout=1.0)
                if lock_acquired:
                    try:
                        # Получаем среднюю оценку
                        cursor.execute('''
                            SELECT AVG(rating) as avg FROM ratings 
                            WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        avg_result = cursor.fetchone()
                        if avg_result:
                            avg = avg_result.get('avg') if isinstance(avg_result, dict) else avg_result[0]
                            avg_rating = float(avg) if avg is not None else None
                        
                        # Получаем активных пользователей
                        cursor.execute('''
                            SELECT DISTINCT user_id
                            FROM stats
                            WHERE chat_id = %s AND user_id IS NOT NULL
                        ''', (chat_id,))
                        active_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                        
                        # Получаем всех, кто оценил этот фильм
                        cursor.execute('''
                            SELECT DISTINCT user_id FROM ratings
                            WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)
                        ''', (chat_id, film_id))
                        rated_users = {row.get('user_id') if isinstance(row, dict) else row[0] for row in cursor.fetchall()}
                        
                        # Определяем текст и эмодзи кнопки
                        # Показываем среднюю оценку, если есть хотя бы одна оценка
                        if avg_rating is not None:
                            rating_int = int(round(avg_rating))
                            if 1 <= rating_int <= 4:
                                emoji = "💩"
                            elif 5 <= rating_int <= 7:
                                emoji = "💬"
                            else:  # 8-10
                                emoji = "🏆"
                            rating_text = f"{emoji} {avg_rating:.0f}/10"
                        logger.info(f"[SHOW FILM INFO] Запрос оценок выполнен, avg_rating={avg_rating}, rating_text={rating_text}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после запроса оценок")
                else:
                    logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем запрос оценок (не критично)")
                    rating_text = "💬 Оценить"
            except Exception as rating_e:
                logger.warning(f"[SHOW FILM INFO] Ошибка при запросе оценок (не критично): {rating_e}")
                rating_text = "💬 Оценить"
            logger.info(f"[SHOW FILM INFO] Оценки получены, rating_text={rating_text}")
            
            markup.row(
                InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
                InlineKeyboardButton(rating_text, callback_data=f"rate_film:{kp_id}")
            )
        else:
            # Фильм не в базе - добавляем кнопки "Интересные факты" и "Оценить"
            markup.row(
                InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
                InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_id}")
            )
        logger.info(f"[SHOW FILM INFO] Кнопки оценок добавлены")
        
        # Если это сериал, добавляем кнопки для сериалов (для фильмов в базе и не в базе)
        logger.info(f"[SHOW FILM INFO] Проверка сериала: is_series={is_series}, user_id={user_id}")
        if is_series and user_id:
            if film_id:
                # Фильм в базе - проверяем доступ к функциям уведомлений
                # Проверяем доступ к функциям уведомлений
                has_access = has_notifications_access(chat_id, user_id)
                logger.info(f"[SHOW FILM INFO] Сериал: is_series=True, user_id={user_id}, chat_id={chat_id}, has_notifications_access={has_access}")
                
                if has_access:
                    # Проверяем, все ли серии просмотрены
                    logger.info(f"[SHOW FILM INFO] Получение данных о сезонах для kp_id={kp_id}...")
                    seasons_data = get_seasons_data(kp_id)
                    logger.info(f"[SHOW FILM INFO] Данные о сезонах получены: {seasons_data is not None}")
                    all_episodes_watched = False
                    if seasons_data and film_id:
                        # Проверяем, выходит ли сериал
                        logger.info(f"[SHOW FILM INFO] Проверка статуса выхода сериала для kp_id={kp_id}...")
                        is_airing, _ = get_series_airing_status(kp_id)
                        logger.info(f"[SHOW FILM INFO] Статус выхода сериала: is_airing={is_airing}")
                        
                        # Получаем просмотренные эпизоды
                        with db_lock:
                            cursor.execute('''
                                SELECT season_number, episode_number 
                                FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id, user_id))
                            watched_rows = cursor.fetchall()
                            watched_set = set()
                            for w_row in watched_rows:
                                if isinstance(w_row, dict):
                                    watched_set.add((str(w_row.get('season_number')), str(w_row.get('episode_number'))))
                                else:
                                    watched_set.add((str(w_row[0]), str(w_row[1])))
                        
                        # Подсчитываем эпизоды
                        total_episodes, watched_episodes = count_episodes_for_watch_check(
                            seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                        )
                        
                        if total_episodes > 0 and watched_episodes == total_episodes:
                            all_episodes_watched = True
                            # Отмечаем сериал как просмотренный в БД
                            with db_lock:
                                cursor.execute("UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                                conn.commit()
                    
                    # Проверяем подписку
                    is_subscribed = False
                    if film_id:
                        with db_lock:
                            cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                            sub_row = cursor.fetchone()
                            is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
                    
                    # Добавляем строку о статусе подписки в текст (чтобы текст всегда менялся)
                    if is_subscribed:
                        text += f"\n\n🔔 <b>Статус подписки: ✅ Подписан</b>"
                    else:
                        text += f"\n\n🔔 <b>Статус подписки: ❌ Не подписан</b>"
                    
                    # Показываем соответствующую кнопку
                    if all_episodes_watched:
                        markup.add(InlineKeyboardButton("✅ Просмотрено", callback_data=f"series_track:{kp_id}"))
                    else:
                        markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                    
                    # Кнопка подписки
                    if is_subscribed:
                        markup.add(InlineKeyboardButton("🔕 Убрать подписку на новые серии", callback_data=f"series_unsubscribe:{kp_id}"))
                    else:
                        markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
                else:
                    # Нет доступа - показываем заблокированные кнопки
                    logger.info(f"[SHOW FILM INFO] Нет доступа к уведомлениям для user_id={user_id}, chat_id={chat_id}, показываем заблокированные кнопки")
                    markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                    markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
            else:
                # Фильм не в базе - проверяем доступ к функциям уведомлений
                has_access = has_notifications_access(chat_id, user_id)
                if has_access:
                    markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                    markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
                else:
                    markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                    markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
        logger.info(f"[SHOW FILM INFO] Обработка сериала завершена")
        
        # Проверяем, есть ли план для этого фильма (дома)
        logger.info(f"[SHOW FILM INFO] Проверка планов для film_id={film_id}...")
        plan_row = None
        if film_id:
            try:
                import threading
                lock_acquired = db_lock.acquire(timeout=1.0)
                if lock_acquired:
                    try:
                        cursor.execute('''
                            SELECT id, plan_type FROM plans 
                            WHERE film_id = %s AND chat_id = %s
                            ORDER BY plan_datetime ASC
                            LIMIT 1
                        ''', (film_id, chat_id))
                        plan_row = cursor.fetchone()
                        logger.info(f"[SHOW FILM INFO] Запрос планов выполнен, plan_row={plan_row is not None}")
                    finally:
                        db_lock.release()
                        logger.info(f"[SHOW FILM INFO] db_lock освобожден после проверки планов")
                else:
                    logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем проверку планов (не критично)")
                    plan_row = None
            except Exception as plan_e:
                logger.warning(f"[SHOW FILM INFO] Ошибка при проверке планов (не критично): {plan_e}")
                plan_row = None
            
            if plan_row:
                plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[1]
                
                # Проверяем наличие билетов для планов "в кино"
                ticket_file_id = None
                if plan_type == 'cinema':
                    try:
                        import threading
                        lock_acquired = db_lock.acquire(timeout=1.0)
                        if lock_acquired:
                            try:
                                cursor.execute('SELECT ticket_file_id FROM plans WHERE id = %s', (plan_id,))
                                ticket_row = cursor.fetchone()
                                if ticket_row:
                                    ticket_file_id = ticket_row.get('ticket_file_id') if isinstance(ticket_row, dict) else ticket_row[0]
                                logger.info(f"[SHOW FILM INFO] Запрос билетов выполнен, ticket_file_id={ticket_file_id is not None}")
                            finally:
                                db_lock.release()
                                logger.info(f"[SHOW FILM INFO] db_lock освобожден после проверки билетов")
                        else:
                            logger.info(f"[SHOW FILM INFO] db_lock занят, пропускаем проверку билетов (не критично)")
                            ticket_file_id = None
                    except Exception as ticket_e:
                        logger.warning(f"[SHOW FILM INFO] Ошибка при проверке билетов (не критично): {ticket_e}")
                        ticket_file_id = None
                
                if plan_type == 'home':
                    # Кнопка "Отметить просмотренным" (если фильм еще не просмотрен)
                    if existing:
                        watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
                        if not watched:
                            markup.add(InlineKeyboardButton("✅ Отметить просмотренным", callback_data=f"mark_watched_from_description:{film_id}"))
                    
                    # Кнопки "Изменить" и "Удалить" в одной строке
                    markup.row(
                        InlineKeyboardButton("✏️ Изменить", callback_data=f"edit_plan:{plan_id}"),
                        InlineKeyboardButton("🗑️ Удалить", callback_data=f"remove_from_calendar:{plan_id}")
                    )
                elif plan_type == 'cinema':
                    # Кнопки для планов "в кино" с проверкой доступа к билетам
                    if has_tickets_access(chat_id, user_id):
                        if ticket_file_id:
                            markup.add(InlineKeyboardButton("🎟️ Перейти к билетам", callback_data=f"ticket_session:{plan_id}"))
                        else:
                            markup.add(InlineKeyboardButton("🎟️ Добавить билеты", callback_data=f"add_ticket:{plan_id}"))
                    else:
                        if ticket_file_id:
                            markup.add(InlineKeyboardButton("🔒 Перейти к билетам", callback_data=f"ticket_locked:{plan_id}"))
                        else:
                            markup.add(InlineKeyboardButton("🔒 Добавить билеты", callback_data=f"ticket_locked:{plan_id}"))
                    
                    # Кнопки "Изменить" и "Удалить" в одной строке
                    markup.row(
                        InlineKeyboardButton("✏️ Изменить", callback_data=f"edit_plan:{plan_id}"),
                        InlineKeyboardButton("🗑️ Удалить", callback_data=f"remove_from_calendar:{plan_id}")
                    )
        logger.info(f"[SHOW FILM INFO] Обработка планов завершена")
        
        # Проверяем длину текста перед отправкой
        logger.info(f"[SHOW FILM INFO] Текст сформирован, длина={len(text)}, message_id={message_id}")
        if len(text) > 4096:
            logger.warning(f"[SHOW FILM INFO] Текст слишком длинный ({len(text)} символов), обрезаю до 4096")
            text = text[:4093] + "..."
        
        # Проверяем валидность markup перед отправкой
        markup_valid = True
        markup_json = None
        try:
            if markup:
                import json
                markup_dict = markup.to_dict()
                markup_json = json.dumps(markup_dict)
                logger.info(f"[SHOW FILM INFO] Markup валиден, количество кнопок: {len(markup_dict.get('inline_keyboard', []))}")
            else:
                logger.info(f"[SHOW FILM INFO] Markup отсутствует (None)")
        except Exception as markup_e:
            logger.error(f"[SHOW FILM INFO] ❌ Ошибка при проверке markup: {markup_e}", exc_info=True)
            markup_valid = False
            markup = None  # Отправляем без клавиатуры
        
        # Проверяем, что text не пустой
        if not text or not text.strip():
            logger.error(f"[SHOW FILM INFO] ❌ Текст пустой или None!")
            text = f"🎬 <b>{info.get('title', 'Фильм')}</b>\n\n❌ Произошла ошибка при формировании описания."
        
        logger.info(f"[SHOW FILM INFO] Финальные проверки: text_length={len(text)}, markup_valid={markup_valid}, markup={markup is not None}")
        
        # Детальное логирование перед отправкой
        if markup:
            try:
                markup_dict = markup.to_dict()
                keyboard = markup_dict.get('inline_keyboard', [])
                total_buttons = sum(len(row) for row in keyboard)
                logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup кнопок: {total_buttons} (строк: {len(keyboard)})")
            except Exception as markup_log_e:
                logger.warning(f"[SHOW FILM INFO] Не удалось получить информацию о markup для логирования: {markup_log_e}")
                logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup присутствует")
        else:
            logger.info(f"[SHOW FILM INFO] Финальный текст длиной {len(text)}, markup отсутствует")
        
        logger.info(f"[SHOW FILM INFO] Отправляю сообщение в чат...")
        
        # Отправляем или обновляем сообщение
        if message_id:
            # Обновляем существующее сообщение
            logger.info(f"[SHOW FILM INFO] Обновление существующего сообщения message_id={message_id}")
            try:
                if message_thread_id:
                    # Для тредов используем API напрямую
                    import json
                    reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                    params = {
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'text': text,
                        'parse_mode': 'HTML',
                        'disable_web_page_preview': False,
                        'message_thread_id': message_thread_id
                    }
                    if reply_markup_json:
                        params['reply_markup'] = reply_markup_json
                    logger.info(f"[SHOW FILM INFO] Вызов api_call editMessageText для треда")
                    bot_instance.api_call('editMessageText', params)
                else:
                    logger.info(f"[SHOW FILM INFO] Вызов edit_message_text")
                    logger.info(f"[SHOW FILM INFO] Параметры edit: chat_id={chat_id}, message_id={message_id}, text_length={len(text)}, has_markup={markup is not None}")
                    bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=False)
                logger.info(f"[SHOW FILM INFO] Сообщение обновлено успешно: {info.get('title')}, kp_id={kp_id}, message_id={message_id}")
            except telebot.apihelper.ApiTelegramException as e:
                error_str = str(e).lower()
                logger.error(f"[SHOW FILM INFO] Telegram API ошибка при обновлении сообщения: {e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] error_code={getattr(e, 'error_code', 'N/A')}, result_json={getattr(e, 'result_json', {})}")
                
                # Проверяем, является ли это ошибкой "message is not modified"
                if "message is not modified" in error_str or "message_not_modified" in error_str or "bad request: message is not modified" in error_str:
                    # Если текст не изменился — просто обновляем клавиатуру
                    logger.info(f"[SHOW FILM INFO] Текст не изменился, обновляю только клавиатуру...")
                    try:
                        if message_thread_id:
                            import json
                            reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                            params = {
                                'chat_id': chat_id,
                                'message_id': message_id,
                                'message_thread_id': message_thread_id
                            }
                            if reply_markup_json:
                                params['reply_markup'] = reply_markup_json
                            bot_instance.api_call('editMessageReplyMarkup', params)
                        else:
                            bot_instance.edit_message_reply_markup(chat_id=chat_id, message_id=message_id, reply_markup=markup)
                        logger.info(f"[SHOW FILM INFO] Клавиатура обновлена успешно")
                    except Exception as e2:
                        logger.error(f"[SHOW FILM INFO] Не удалось обновить markup: {e2}", exc_info=True)
                        # При ошибке отправляем новое сообщение
                        try:
                            if message_thread_id:
                                bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                            else:
                                bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                            logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                        except Exception as send_e:
                            logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
                else:
                    # Другая ошибка API - отправляем новое сообщение
                    logger.warning(f"[SHOW FILM INFO] Другая ошибка Telegram API, отправляю новое сообщение")
                    try:
                        if message_thread_id:
                            bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                        else:
                            bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                        logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                    except Exception as send_e:
                        logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
            except Exception as e:
                logger.error(f"[SHOW FILM INFO] Неизвестная ошибка обновления сообщения: {e}", exc_info=True)
                # При ошибке отправляем новое сообщение
                try:
                    if message_thread_id:
                        bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup, message_thread_id=message_thread_id)
                    else:
                        bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
                    logger.info(f"[SHOW FILM INFO] Отправлено новое сообщение вместо обновления: {info.get('title')}, kp_id={kp_id}")
                except Exception as send_e:
                    logger.error(f"[SHOW FILM INFO] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
        else:
            # Отправляем новое сообщение
            logger.info(f"[SHOW FILM INFO] ===== ОТПРАВКА НОВОГО СООБЩЕНИЯ =====")
            logger.info(f"[SHOW FILM INFO] chat_id={chat_id}, text_length={len(text)}, has_markup={markup is not None}, markup_valid={markup_valid}")
            try:
                logger.info(f"[SHOW FILM INFO] Вызов send_message, chat_id={chat_id}, text_length={len(text)}")
                
                # Подготавливаем параметры для отправки
                send_params = {
                    'chat_id': chat_id,
                    'text': text,
                    'parse_mode': 'HTML',
                    'disable_web_page_preview': False
                }
                
                # Добавляем markup только если он валиден
                if markup and markup_valid:
                    send_params['reply_markup'] = markup
                    logger.info(f"[SHOW FILM INFO] Markup добавлен в параметры отправки")
                else:
                    logger.info(f"[SHOW FILM INFO] Markup не добавлен (valid={markup_valid}, exists={markup is not None})")
                
                # Добавляем message_thread_id если есть
                if message_thread_id:
                    send_params['message_thread_id'] = message_thread_id
                    logger.info(f"[SHOW FILM INFO] Отправка в тред message_thread_id={message_thread_id}")
                
                logger.info(f"[SHOW FILM INFO] Параметры подготовлены, вызываю send_message...")
                logger.info(f"[SHOW FILM INFO] send_params keys: {list(send_params.keys())}, text_length: {len(send_params.get('text', ''))}")
                msg = bot_instance.send_message(**send_params)
                logger.info(f"[SHOW FILM INFO] ✅ Описание фильма отправлено: {info.get('title')}, kp_id={kp_id}, message_id={msg.message_id if msg else 'None'}")
                logger.info(f"[SHOW FILM INFO] Сообщение отправлено успешно")
                
            except telebot.apihelper.ApiTelegramException as api_e:
                error_code = getattr(api_e, 'error_code', None)
                error_str = str(api_e).lower()
                logger.error(f"[SHOW FILM INFO] ❌ Telegram API ошибка при отправке сообщения: {api_e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] error_code={error_code}, result_json={getattr(api_e, 'result_json', {})}")
                
                # Пытаемся отправить упрощенное сообщение без markup
                try:
                    logger.info(f"[SHOW FILM INFO] Попытка отправить упрощенное сообщение без markup...")
                    fallback_text = f"🎬 <b>{info.get('title', 'Фильм')}</b> ({info.get('year', '—')})\n\n"
                    if info.get('description'):
                        desc = info.get('description', '')[:500]  # Ограничиваем описание
                        fallback_text += f"{desc}...\n\n"
                    fallback_text += f"<a href='{link}'>Кинопоиск</a>"
                    
                    if len(fallback_text) > 4096:
                        fallback_text = fallback_text[:4093] + "..."
                    
                    bot_instance.send_message(chat_id, fallback_text, parse_mode='HTML', disable_web_page_preview=False)
                    logger.info(f"[SHOW FILM INFO] ✅ Упрощенное сообщение отправлено")
                except Exception as fallback_e:
                    logger.error(f"[SHOW FILM INFO] ❌ Не удалось отправить даже упрощенное сообщение: {fallback_e}", exc_info=True)
                    # Последняя попытка - самое простое сообщение
                    try:
                        simple_text = f"🎬 {info.get('title', 'Фильм')}\n\n<a href='{link}'>Кинопоиск</a>"
                        bot_instance.send_message(chat_id, simple_text, parse_mode='HTML', disable_web_page_preview=False)
                        logger.info(f"[SHOW FILM INFO] ✅ Простейшее сообщение отправлено")
                    except Exception as simple_e:
                        logger.error(f"[SHOW FILM INFO] ❌ КРИТИЧЕСКАЯ ОШИБКА: не удалось отправить даже простое сообщение: {simple_e}", exc_info=True)
                        
            except Exception as send_e:
                error_type = type(send_e).__name__
                error_str = str(send_e)
                logger.error(f"[SHOW FILM INFO] ❌ КРИТИЧЕСКАЯ ОШИБКА при отправке сообщения: {send_e}", exc_info=True)
                logger.error(f"[SHOW FILM INFO] Тип ошибки: {error_type}, args: {send_e.args}")
                logger.error(f"[SHOW FILM INFO] text length: {len(text) if text else 'None'}, markup: {markup is not None}")
                
                # Пытаемся отправить упрощенное сообщение
                try:
                    logger.info(f"[SHOW FILM INFO] Попытка отправить упрощенное сообщение...")
                    fallback_text = f"🎬 <b>{info.get('title', 'Фильм')}</b>\n\n<a href='{link}'>Кинопоиск</a>"
                    if len(fallback_text) > 4096:
                        fallback_text = fallback_text[:4093] + "..."
                    bot_instance.send_message(chat_id, fallback_text, parse_mode='HTML', disable_web_page_preview=False)
                    logger.info(f"[SHOW FILM INFO] ✅ Упрощенное сообщение отправлено после ошибки")
                except Exception as fallback_e:
                    logger.error(f"[SHOW FILM INFO] ❌ Не удалось отправить упрощенное сообщение: {fallback_e}", exc_info=True)
                    # НЕ пробрасываем ошибку дальше - бот должен продолжать работать
        
    except Exception as e:
        error_type = type(e).__name__
        error_str = str(e)
        logger.error(f"[SHOW FILM INFO] ❌ КРИТИЧЕСКАЯ ОШИБКА в show_film_info_with_buttons: {e}", exc_info=True)
        logger.error(f"[SHOW FILM INFO] Тип ошибки: {error_type}, args: {e.args}")
        logger.error(f"[SHOW FILM INFO] chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, existing={existing}")
        
        # Пытаемся отправить сообщение об ошибке
        try:
            error_text = f"🎬 <b>{info.get('title', 'Фильм') if info else 'Фильм'}</b>\n\n"
            if link:
                error_text += f"<a href='{link}'>Кинопоиск</a>\n\n"
            error_text += "❌ Произошла ошибка при формировании описания."
            bot_instance.send_message(chat_id, error_text, parse_mode='HTML', disable_web_page_preview=False)
            logger.info(f"[SHOW FILM INFO] ✅ Сообщение об ошибке отправлено")
        except Exception as send_error_e:
            logger.error(f"[SHOW FILM INFO] ❌ Не удалось отправить даже сообщение об ошибке: {send_error_e}", exc_info=True)
        # НЕ пробрасываем ошибку дальше - бот должен продолжать работать
        logger.error(f"[SHOW FILM INFO] Traceback: {e.__traceback__}")
        try:
            logger.info(f"[SHOW FILM INFO] Попытка отправить сообщение об ошибке")
            bot_instance.send_message(chat_id, "❌ Произошла ошибка при показе описания фильма.")
            logger.info(f"[SHOW FILM INFO] Сообщение об ошибке отправлено")
        except Exception as send_error:
            logger.error(f"[SHOW FILM INFO] ❌ Не удалось отправить сообщение об ошибке: {send_error}", exc_info=True)


def ensure_movie_in_database(chat_id, kp_id, link, info, user_id=None):
    """
    Добавляет фильм/сериал в базу, если его еще нет.
    Возвращает (film_id, was_inserted), где was_inserted = True если фильм был добавлен.
    """
    logger.info(f"[ENSURE MOVIE] ===== START: chat_id={chat_id}, kp_id={kp_id}, user_id={user_id}, link={link}")
    try:
        logger.info(f"[ENSURE MOVIE] Входим в db_lock")
        with db_lock:
            logger.info(f"[ENSURE MOVIE] db_lock получен, проверяю существование фильма")
            # Проверяем, существует ли фильм
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                logger.info(f"[ENSURE MOVIE] Фильм уже в базе: film_id={film_id}, kp_id={kp_id}")
                logger.info(f"[ENSURE MOVIE] ===== END (уже в базе) =====")
                return film_id, False
            
            # Добавляем фильм в базу
            logger.info(f"[ENSURE MOVIE] Фильм не найден, добавляю в БД")
            logger.info(f"[ENSURE MOVIE] Данные: title={info.get('title', 'N/A')}, year={info.get('year', 'N/A')}, is_series={info.get('is_series', False)}")
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                RETURNING id
            ''', (chat_id, link, kp_id, info['title'], info['year'], info['genres'], info['description'], 
                  info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
            
            result = cursor.fetchone()
            logger.info(f"[ENSURE MOVIE] INSERT выполнен, result={result}")
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            logger.info(f"[ENSURE MOVIE] film_id извлечен: {film_id}")
            conn.commit()
            logger.info(f"[ENSURE MOVIE] commit выполнен")
            
            logger.info(f"[ENSURE MOVIE] Фильм добавлен в базу: film_id={film_id}, kp_id={kp_id}, title={info['title']}")
            logger.info(f"[ENSURE MOVIE] ===== END (добавлен) =====")
            return film_id, True
            
    except Exception as e:
        logger.error(f"[ENSURE MOVIE] КРИТИЧЕСКАЯ ОШИБКА при добавлении фильма в базу: {e}", exc_info=True)
        try:
            conn.rollback()
            logger.info(f"[ENSURE MOVIE] rollback выполнен")
        except Exception as rollback_e:
            logger.error(f"[ENSURE MOVIE] Ошибка при rollback: {rollback_e}")
        logger.info(f"[ENSURE MOVIE] ===== END (ошибка) =====")
        return None, False


def show_film_info_without_adding(chat_id, user_id, info, link, kp_id):
    """
    Показывает описание фильма/сериала с ВСЕМИ кнопками БЕЗ добавления в базу.
    Используется когда пользователь отправляет ссылку на сериал.
    """
    logger.info(f"[SHOW FILM INFO WITHOUT ADDING] ===== START: chat_id={chat_id}, user_id={user_id}, kp_id={kp_id}, link={link}")
    try:
        if not info:
            logger.error(f"[SHOW FILM INFO WITHOUT ADDING] info is None или пустой!")
            bot_instance.send_message(chat_id, "❌ Произошла ошибка: информация о фильме не получена.")
            return
        
        is_series = info.get('is_series', False)
        type_emoji = "📺" if is_series else "🎬"
        logger.info(f"[SHOW FILM INFO WITHOUT ADDING] is_series={is_series}, type_emoji={type_emoji}, title={info.get('title')}")
        
        # Формируем текст описания
        text = f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
        logger.info(f"[SHOW FILM INFO WITHOUT ADDING] Текст начала формироваться")
        if info.get('director'):
            text += f"<i>Режиссёр:</i> {info['director']}\n"
        if info.get('genres'):
            text += f"<i>Жанры:</i> {info['genres']}\n"
        if info.get('actors'):
            text += f"<i>В ролях:</i> {info['actors']}\n"
        if info.get('description'):
            text += f"\n<i>Кратко:</i> {info['description']}\n"
        
        # Если это сериал, добавляем информацию о статусе выхода серий
        if is_series:
            is_airing, next_episode = get_series_airing_status(kp_id)
            if is_airing and next_episode:
                text += f"\n🟢 <b>Сериал выходит сейчас</b>\n"
                text += f"📅 Следующая серия: Сезон {next_episode['season']}, Эпизод {next_episode['episode']} — {next_episode['date'].strftime('%d.%m.%Y')}\n"
            else:
                text += f"\n🔴 <b>Сериал не выходит</b>\n"
        
        text += f"\n<a href='{link}'>Кинопоиск</a>"
        text += f"\n\n⏳ <b>Ещё не просмотрено</b>"
        
        # Создаем кнопки
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверяем премьеру
        russia_release = info.get('russia_release')
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            try:
                headers = {'X-API-KEY': KP_TOKEN, 'Content-Type': 'application/json'}
                url_main = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
                response_main = requests.get(url_main, headers=headers, timeout=15)
                if response_main.status_code == 200:
                    data_main = response_main.json()
                    from datetime import date as date_class
                    today = date_class.today()
                    
                    for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                        date_value = data_main.get(date_field)
                        if date_value:
                            try:
                                if 'T' in str(date_value):
                                    premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                                else:
                                    premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                                premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                                break
                            except:
                                continue
            except Exception as e:
                logger.warning(f"[SHOW FILM INFO] Ошибка получения информации о премьере: {e}")
        
        # Если премьера еще не состоялась, добавляем кнопку
        if premiere_date:
            from datetime import date as date_class
            today = date_class.today()
            if premiere_date > today:
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
                markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:current_month"))
        
        # Добавляем кнопку "➕ Добавить в базу"
        markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{kp_id}"))
        
        # Проверяем, есть ли фильм в базе и запланирован ли он
        # (для show_film_info_without_adding фильм обычно не в базе, но проверим на всякий случай)
        film_id = None
        has_plan = False
        with db_lock:
            cursor.execute("SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
            film_row = cursor.fetchone()
            if film_row:
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                # Проверяем наличие планов
                cursor.execute('SELECT id FROM plans WHERE film_id = %s AND chat_id = %s LIMIT 1', (film_id, chat_id))
                plan_row = cursor.fetchone()
                has_plan = plan_row is not None
        
        # Добавляем кнопку "Запланировать просмотр" только если фильм не запланирован
        if not has_plan:
            markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data=f"plan_from_added:{kp_id}"))
        
        # Добавляем кнопки для всех действий (фильм/сериал будет добавлен в базу при нажатии только при определенных действиях)
        markup.row(
            InlineKeyboardButton("🤔 Интересные факты", callback_data=f"show_facts:{kp_id}"),
            InlineKeyboardButton("💬 Оценить", callback_data=f"rate_film:{kp_id}")
        )
        
        # Если это сериал, добавляем кнопки для сериалов
        if is_series:
            if user_id and has_notifications_access(chat_id, user_id):
                markup.add(InlineKeyboardButton("✅ Отметить просмотренные серии", callback_data=f"series_track:{kp_id}"))
                markup.add(InlineKeyboardButton("🔔 Подписаться на новые серии", callback_data=f"series_subscribe:{kp_id}"))
            else:
                markup.add(InlineKeyboardButton("🔒 Отметить просмотренные серии", callback_data=f"series_locked:{kp_id}"))
                markup.add(InlineKeyboardButton("🔒 Подписаться на новые серии", callback_data=f"series_locked:{kp_id}"))
        
        # Отправляем сообщение
        logger.info(f"[SHOW FILM INFO WITHOUT ADDING] Отправка сообщения: chat_id={chat_id}, text_length={len(text)}, has_markup={markup is not None}")
        try:
            msg = bot_instance.send_message(chat_id, text, parse_mode='HTML', disable_web_page_preview=False, reply_markup=markup)
            logger.info(f"[SHOW FILM INFO WITHOUT ADDING] Описание фильма отправлено БЕЗ добавления в базу: {info.get('title')}, kp_id={kp_id}, message_id={msg.message_id if msg else 'None'}")
            return msg
        except Exception as send_e:
            logger.error(f"[SHOW FILM INFO WITHOUT ADDING] Ошибка отправки сообщения: {send_e}", exc_info=True)
            raise  # Пробрасываем ошибку дальше
        
    except Exception as e:
        logger.error(f"[SHOW FILM INFO WITHOUT ADDING] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.send_message(chat_id, "❌ Произошла ошибка при показе описания фильма.")
        except:
            pass
    finally:
        logger.info(f"[SHOW FILM INFO WITHOUT ADDING] ===== КОНЕЦ =====")
        return None


def import_kp_ratings(kp_user_id, chat_id, user_id, max_count=100):
    """Импортирует оценки из Кинопоиска"""
    headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
    base_url = f"https://kinopoiskapiunofficial.tech/api/v1/kp_users/{kp_user_id}/votes"
    
    imported_count = 0
    page = 1
    max_pages = min(75, (max_count + 19) // 20)  # Максимум 75 страниц, по 20 фильмов на странице
    
    try:
        while imported_count < max_count and page <= max_pages:
            url = f"{base_url}?page={page}"
            logger.info(f"[IMPORT] Запрос страницы {page}: {url}")
            
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code != 200:
                logger.error(f"[IMPORT] Ошибка {response.status_code}: {response.text[:200]}")
                break
            
            data = response.json()
            items = data.get('items', [])
            
            if not items or len(items) == 0:
                logger.info(f"[IMPORT] Нет больше фильмов на странице {page}")
                break
            
            # Обрабатываем фильмы на странице
            for item in items:
                if imported_count >= max_count:
                    break
                
                kp_id = str(item.get('kinopoiskId'))
                if not kp_id:
                    continue
                
                # Проверяем тип - только FILM
                if item.get('type') != 'FILM':
                    continue
                
                user_rating = item.get('userRating')
                if not user_rating or user_rating < 1 or user_rating > 10:
                    continue
                
                link = f"https://kinopoisk.ru/film/{kp_id}/"
                
                # Импортированные оценки НЕ добавляют фильмы в базу группы
                # Они существуют только как оценки в таблице ratings с is_imported = TRUE
                # Для импортированных оценок используем film_id = NULL или создаем виртуальный film_id
                try:
                    with db_lock:
                        # Проверяем, есть ли фильм в базе группы (добавлен через бота)
                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                        film_row = cursor.fetchone()
                        
                        if film_row:
                            # Фильм уже есть в базе группы - можем добавить импортированную оценку
                            film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                            logger.debug(f"[IMPORT] Фильм {kp_id} уже существует в базе группы, film_id={film_id}")
                            
                            # Проверяем, есть ли уже оценка у этого пользователя для этого фильма
                            cursor.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s
                            ''', (chat_id, film_id, user_id))
                            existing_rating = cursor.fetchone()
                            
                            if existing_rating:
                                # Оценка уже есть, пропускаем
                                cursor.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                                title_row = cursor.fetchone()
                                title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                                logger.debug(f"[IMPORT] Фильм {title} уже имеет оценку, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку для существующего фильма
                            cursor.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id)
                                VALUES (%s, %s, %s, %s, TRUE, %s)
                                ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = TRUE, kp_id = EXCLUDED.kp_id
                            ''', (chat_id, film_id, user_id, user_rating, kp_id))
                            conn.commit()
                            
                            imported_count += 1
                            cursor.execute('SELECT title FROM movies WHERE id = %s', (film_id,))
                            title_row = cursor.fetchone()
                            title = title_row.get('title') if isinstance(title_row, dict) else (title_row[0] if title_row else 'Неизвестно')
                            logger.info(f"[IMPORT] Импортирован фильм {title} с оценкой {user_rating}")
                        else:
                            # Фильма нет в базе группы - создаем импортированную оценку БЕЗ добавления фильма в movies
                            # Используем film_id = NULL и kp_id для хранения импортированных оценок
                            title = item.get('nameRu') or item.get('nameEn') or 'Без названия'
                            
                            # Проверяем, есть ли уже импортированная оценка для этого kp_id и пользователя
                            cursor.execute('''
                                SELECT rating FROM ratings 
                                WHERE chat_id = %s AND kp_id = %s AND user_id = %s AND film_id IS NULL
                            ''', (chat_id, kp_id, user_id))
                            existing_imported_rating = cursor.fetchone()
                            
                            if existing_imported_rating:
                                logger.debug(f"[IMPORT] Импортированная оценка для фильма {kp_id} ({title}) уже существует, пропускаем")
                                continue
                            
                            # Добавляем импортированную оценку БЕЗ film_id (film_id = NULL)
                            cursor.execute('''
                                INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported, kp_id)
                                VALUES (%s, NULL, %s, %s, TRUE, %s)
                            ''', (chat_id, user_id, user_rating, kp_id))
                            conn.commit()
                            
                            imported_count += 1
                            logger.info(f"[IMPORT] Импортирован фильм {title} (kp_id={kp_id}) с оценкой {user_rating} (без добавления в базу группы)")
                except Exception as db_error:
                    logger.error(f"[IMPORT] Ошибка при работе с БД для фильма {kp_id}: {db_error}", exc_info=True)
                    continue
            
            # Если получили меньше 20 фильмов, значит страницы закончились
            if len(items) < 20:
                logger.info(f"[IMPORT] Получено меньше 20 фильмов, заканчиваем")
                break
            
            page += 1
        
        return imported_count
    except Exception as e:
        logger.error(f"[IMPORT] Ошибка при импорте: {e}", exc_info=True)
        return imported_count


def handle_import_user_id_internal(message, state):
    """Обрабатывает ввод user_id для импорта"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text.strip()
    
    kp_user_id = extract_kp_user_id(text)
    
    if not kp_user_id:
        bot_instance.reply_to(message, "❌ Не удалось извлечь ID пользователя. Отправьте ID или ссылку на профиль Кинопоиска.")
        return
    
    state['kp_user_id'] = kp_user_id
    state['step'] = 'waiting_count'
    
    markup = InlineKeyboardMarkup(row_width=2)
    markup.add(InlineKeyboardButton("50", callback_data=f"import_count:50"))
    markup.add(InlineKeyboardButton("100", callback_data=f"import_count:100"))
    markup.add(InlineKeyboardButton("300", callback_data=f"import_count:300"))
    markup.add(InlineKeyboardButton("500", callback_data=f"import_count:500"))
    markup.add(InlineKeyboardButton("1000", callback_data=f"import_count:1000"))
    markup.add(InlineKeyboardButton("1500", callback_data=f"import_count:1500"))
    
    bot_instance.reply_to(message, 
        f"✅ ID пользователя: <code>{kp_user_id}</code>\n\n"
        f"Сколько фильмов загрузить?",
        reply_markup=markup, parse_mode='HTML')


# Обработчик выбора количества фильмов для импорта - НА ВЕРХНЕМ УРОВНЕ МОДУЛЯ
@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("import_count:"))
def handle_import_count_callback(call):
    """Обработчик выбора количества фильмов для импорта"""
    try:
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        count = int(call.data.split(":")[1])
        
        if user_id not in user_import_state:
            bot_instance.answer_callback_query(call.id, "❌ Состояние импорта потеряно", show_alert=True)
            return
        
        state = user_import_state[user_id]
        kp_user_id = state.get('kp_user_id')
        
        if not kp_user_id:
            bot_instance.answer_callback_query(call.id, "❌ ID пользователя не найден", show_alert=True)
            return
        
        bot_instance.answer_callback_query(call.id, f"⏳ Начинаю импорт {count} фильмов...")
        status_msg = bot_instance.edit_message_text(
            f"📥 <b>Импорт базы из Кинопоиска</b>\n\n"
            f"ID пользователя: <code>{kp_user_id}</code>\n"
            f"Количество: {count}\n\n"
            f"⏳ Импорт начат в фоновом режиме, это может занять некоторое время...\n"
            f"Вы получите уведомление по завершении.",
            chat_id, call.message.message_id, parse_mode='HTML'
        )
        
        # Удаляем состояние
        del user_import_state[user_id]
        
        # Запускаем импорт в фоновом потоке
        import threading
        
        def background_import():
            try:
                imported = import_kp_ratings(kp_user_id, chat_id, user_id, count)
                
                # Отправляем результат
                bot_instance.edit_message_text(
                    f"✅ <b>Импорт завершён!</b>\n\n"
                    f"ID пользователя: <code>{kp_user_id}</code>\n"
                    f"Загружено новых оценок: <b>{imported}</b>\n\n"
                    f"Оценки загружены в базу! 🎉",
                    chat_id, status_msg.message_id, parse_mode='HTML'
                )
                
                logger.info(f"[IMPORT] Импорт завершён для user_id={user_id}, kp_user_id={kp_user_id}, imported={imported}")
            except Exception as e:
                logger.error(f"[IMPORT] Ошибка в фоновом импорте: {e}", exc_info=True)
                try:
                    bot_instance.edit_message_text(
                        f"❌ <b>Ошибка при импорте</b>\n\n"
                        f"Произошла ошибка: {str(e)[:200]}",
                        chat_id, status_msg.message_id, parse_mode='HTML'
                    )
                except:
                    pass
        
        # Запускаем в отдельном потоке
        import_thread = threading.Thread(target=background_import, daemon=True)
        import_thread.start()
    except Exception as e:
        logger.error(f"[IMPORT] Ошибка в handle_import_count_callback: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка при импорте", show_alert=True)
        except:
            pass
