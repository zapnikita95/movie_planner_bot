"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.api.kinopoisk_api import get_facts
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
        
        # Проверяем, есть ли фильм уже в базе
        with db_lock:
            cursor.execute('SELECT id, title, link, watched, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
        
        if row:
            # Фильм уже в базе
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title_db = row.get('title') if isinstance(row, dict) else row[1]
            link = row.get('link') if isinstance(row, dict) else row[2]
            watched = row.get('watched') if isinstance(row, dict) else row[3]
            
            logger.info(f"[ADD TO DATABASE] Фильм уже в базе: film_id={film_id}, title={title_db}")
            bot_instance.answer_callback_query(call.id, f"ℹ️ {title_db} уже в базе", show_alert=False)
            
            # Обновляем сообщение, показывая что фильм в базе
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            # Получаем минимальную информацию из базы для обновления карточки
            # Не делаем запрос к API - используем данные из базы
            info = {
                'title': title_db,
                'year': None,  # Можно получить из базы, но не обязательно
                'is_series': bool(row.get('is_series') if isinstance(row, dict) else row[4]) if len(row) > 4 else False
            }
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title_db, watched), message_id=call.message.message_id)
            return
        
        # Фильма нет в базе - добавляем с полной информацией из текста сообщения
        # НЕ ДЕЛАЕМ ЗАПРОС К API - используем информацию из сообщения
        message_text = call.message.text or ""
        logger.info(f"[ADD TO DATABASE] Фильм не найден в базе, извлекаю информацию из сообщения")
        
        # Извлекаем всю информацию из HTML-текста сообщения
        import re
        from html import unescape
        
        # Название и год
        title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>\s*\((\d{4})\)', message_text)
        if title_match:
            title = unescape(title_match.group(1))
            year = int(title_match.group(2))
        else:
            title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>', message_text)
            if title_match:
                title = unescape(title_match.group(1))
                year_match = re.search(r'\((\d{4})\)', message_text)
                year = int(year_match.group(1)) if year_match else None
            else:
                title_match = re.search(r'[📺🎬]\s*(.+?)\s*\(', message_text)
                if title_match:
                    title = title_match.group(1).strip()
                    year_match = re.search(r'\((\d{4})\)', message_text)
                    year = int(year_match.group(1)) if year_match else None
                else:
                    title = f"Фильм {kp_id}"
                    year = None
        
        # Режиссёр
        director_match = re.search(r'<i>Режиссёр:</i>\s*(.+?)(?:\n|$)', message_text)
        director = unescape(director_match.group(1).strip()) if director_match else None
        
        # Жанры
        genres_match = re.search(r'<i>Жанры:</i>\s*(.+?)(?:\n|$)', message_text)
        genres = unescape(genres_match.group(1).strip()) if genres_match else None
        
        # В ролях
        actors_match = re.search(r'<i>В ролях:</i>\s*(.+?)(?:\n|$)', message_text)
        actors = unescape(actors_match.group(1).strip()) if actors_match else None
        
        # Описание
        description_match = re.search(r'<i>Кратко:</i>\s*(.+?)(?:\n|🟢|🔴|Кинопоиск|$)', message_text, re.DOTALL)
        description = unescape(description_match.group(1).strip()) if description_match else None
        
        # Определяем, фильм это или сериал по эмодзи в сообщении
        is_series = '📺' in message_text
        link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        logger.info(f"[ADD TO DATABASE] Добавляю фильм в базу: title={title}, year={year}, is_series={is_series}, link={link}")
        
        # Добавляем фильм в базу с полной информацией
        with db_lock:
            cursor.execute('''
                INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'button')
                ON CONFLICT (chat_id, kp_id) DO UPDATE SET 
                    link = EXCLUDED.link,
                    title = EXCLUDED.title,
                    year = COALESCE(EXCLUDED.year, movies.year),
                    genres = COALESCE(EXCLUDED.genres, movies.genres),
                    description = COALESCE(EXCLUDED.description, movies.description),
                    director = COALESCE(EXCLUDED.director, movies.director),
                    actors = COALESCE(EXCLUDED.actors, movies.actors),
                    is_series = EXCLUDED.is_series
                RETURNING id, title, watched, year, genres, description, director, actors
            ''', (chat_id, link, kp_id, title, year, genres, description, director, actors, 1 if is_series else 0, user_id))
            
            result = cursor.fetchone()
            film_id = result.get('id') if isinstance(result, dict) else result[0]
            title_db = result.get('title') if isinstance(result, dict) else result[1]
            watched = result.get('watched') if isinstance(result, dict) else result[2]
            year_db = result.get('year') if isinstance(result, dict) else (result[3] if len(result) > 3 else None)
            genres_db = result.get('genres') if isinstance(result, dict) else (result[4] if len(result) > 4 else None)
            description_db = result.get('description') if isinstance(result, dict) else (result[5] if len(result) > 5 else None)
            director_db = result.get('director') if isinstance(result, dict) else (result[6] if len(result) > 6 else None)
            actors_db = result.get('actors') if isinstance(result, dict) else (result[7] if len(result) > 7 else None)
            conn.commit()
        
        logger.info(f"[ADD TO DATABASE] Фильм добавлен в базу: film_id={film_id}, title={title_db}")
        bot_instance.answer_callback_query(call.id, f"✅ {title_db} добавлен в базу!", show_alert=False)
        
        # Обновляем сообщение, показывая что фильм теперь в базе с полной информацией
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        info = {
            'title': title_db,
            'year': year_db,
            'is_series': is_series,
            'genres': genres_db,
            'description': description_db,
            'director': director_db,
            'actors': actors_db
        }
        show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=(film_id, title_db, watched), message_id=call.message.message_id)
        
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
        
        # Проверяем, есть ли фильм в базе, если нет - добавляем с минимальной информацией
        
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
            # Фильм не в базе - добавляем с минимальной информацией из сообщения
            # НЕ ДЕЛАЕМ ЗАПРОС К API - используем информацию из сообщения
            message_text = call.message.text or ""
            import re
            title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>', message_text)
            if title_match:
                title = title_match.group(1)
            else:
                title_match = re.search(r'[📺🎬]\s*(.+?)\s*\(', message_text)
                if title_match:
                    title = title_match.group(1).strip()
                else:
                    title = f"Фильм {kp_id}"
            
            is_series = '📺' in message_text
            if not link:
                link = f"https://kinopoisk.ru/series/{kp_id}/" if is_series else f"https://kinopoisk.ru/film/{kp_id}/"
            
            logger.info(f"[PLAN FROM ADDED] Добавляю фильм в базу при планировании: title={title}, kp_id={kp_id}")
            
            # Добавляем фильм в базу с минимальной информацией
            with db_lock:
                cursor.execute('''
                    INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                    VALUES (%s, %s, %s, %s, NULL, NULL, NULL, NULL, NULL, %s, %s, NOW(), 'plan_button')
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link
                    RETURNING id
                ''', (chat_id, link, kp_id, title, 1 if is_series else 0, user_id))
                
                result = cursor.fetchone()
                film_id = result.get('id') if isinstance(result, dict) else result[0]
                conn.commit()
            
            if not film_id:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
                return
            
            logger.info(f"[PLAN FROM ADDED] Фильм добавлен в базу при планировании: kp_id={kp_id}, film_id={film_id}")
        
        if not link:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
            logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
        
        # Убеждаемся, что link установлен
        if not link:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
            logger.info(f"[PLAN FROM ADDED] Ссылка не найдена в базе, используем стандартную: {link}")
        
        user_plan_state[user_id] = {
            'step': 2,
            'link': link,
            'chat_id': chat_id,
            'kp_id': kp_id  # Сохраняем kp_id для отладки
        }
        
        logger.info(f"[PLAN FROM ADDED] Состояние установлено: user_id={user_id}, state={user_plan_state[user_id]}")
        logger.info(f"[PLAN FROM ADDED] Проверка состояния после установки: user_id in user_plan_state = {user_id in user_plan_state}")
        
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

