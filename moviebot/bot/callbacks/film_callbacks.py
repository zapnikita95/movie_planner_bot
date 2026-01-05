"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import extract_movie_info, get_facts
from moviebot.states import user_plan_state

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


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
        from moviebot.bot.handlers.series import ensure_movie_in_database, show_film_info_with_buttons
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


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_from_added:"))
def plan_from_added_callback(call):
    """Обработчик планирования из добавленного фильма"""
    logger.info(f"[PLAN FROM ADDED] ===== НАЧАЛО ОБРАБОТКИ =====")
    logger.info(f"[PLAN FROM ADDED] Получен callback: call.data={call.data}, user_id={call.from_user.id}, chat_id={call.message.chat.id}")
    try:
        bot_instance.answer_callback_query(call.id)  # Отвечаем сразу, чтобы убрать "крутилку"
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        kp_id = call.data.split(":")[1]
        
        logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать фильм kp_id={kp_id}")
        
        # Проверяем, есть ли фильм в базе, если нет - добавляем
        from moviebot.bot.handlers.series import ensure_movie_in_database
        
        link = None
        film_id = None
        with db_lock:
            cursor.execute('SELECT id, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if row:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                link = row.get('link') if isinstance(row, dict) else row[1]
                logger.info(f"[PLAN FROM ADDED] Фильм найден в базе: film_id={film_id}, link={link}")
        
        if not film_id:
            # Фильм не в базе - добавляем
            if not link:
                link = f"https://kinopoisk.ru/film/{kp_id}/"
            info = extract_movie_info(link)
            if info:
                film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                if was_inserted:
                    logger.info(f"[PLAN FROM ADDED] Фильм добавлен в базу при планировании: kp_id={kp_id}, film_id={film_id}")
                if not film_id:
                    bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                    return
            else:
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
        
        if not link:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
            logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
        
        user_plan_state[user_id] = {
            'step': 2,
            'link': link,
            'chat_id': chat_id
        }
        
        logger.info(f"[PLAN FROM ADDED] Состояние установлено: user_id={user_id}, state={user_plan_state[user_id]}")
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("Дома", callback_data="plan_type:home"))
        markup.add(InlineKeyboardButton("В кино", callback_data="plan_type:cinema"))
        
        logger.info(f"[PLAN FROM ADDED] Отправка сообщения с выбором типа просмотра...")
        bot_instance.send_message(chat_id, "Где планируете смотреть?", reply_markup=markup)
        logger.info(f"[PLAN FROM ADDED] Сообщение отправлено успешно")
    except Exception as e:
        logger.error(f"[PLAN FROM ADDED] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[PLAN FROM ADDED] ===== КОНЕЦ ОБРАБОТКИ =====")


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("show_facts:") or call.data.startswith("facts:"))
def show_facts_callback(call):
    """Обработчик кнопки 'Интересные факты'"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[SHOW FACTS] Пользователь {user_id} запросил факты для kp_id={kp_id}")
        
        # Получаем факты
        facts = get_facts(kp_id)
        if facts:
            bot_instance.send_message(chat_id, facts, parse_mode='HTML')
            bot_instance.answer_callback_query(call.id, "Факты отправлены")
        else:
            bot_instance.answer_callback_query(call.id, "Факты не найдены", show_alert=True)
    except Exception as e:
        logger.error(f"[SHOW FACTS] Ошибка: {e}", exc_info=True)
    finally:
        # ВСЕГДА отвечаем на callback!
        try:
            bot_instance.answer_callback_query(call.id)
        except Exception as answer_e:
            logger.error(f"[SHOW FACTS] Не удалось ответить на callback: {answer_e}", exc_info=True)


def register_film_callbacks(bot_instance):
    """Регистрирует callback handlers для карточки фильма (уже зарегистрированы через декораторы)"""
    # Handlers уже зарегистрированы через декораторы @bot_instance.callback_query_handler
    # при импорте модуля, поэтому эта функция просто для совместимости
    pass

