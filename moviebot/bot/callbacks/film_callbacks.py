"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock, db_semaphore
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
        # КРИТИЧЕСКИЙ ФИКС: Добавляем rollback при ошибках транзакции
        try:
            # Сначала делаем rollback на случай если предыдущая транзакция упала
            try:
                conn.rollback()
            except:
                pass
            
            with db_semaphore:
                with db_lock:
                    cursor.execute('SELECT id, title, link, watched, is_series FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                    row = cursor.fetchone()
        except Exception as e:
            logger.error(f"[ADD TO DATABASE] Ошибка при проверке фильма в базе: {e}", exc_info=True)
            try:
                conn.rollback()
            except:
                pass
            bot_instance.answer_callback_query(call.id, "❌ Ошибка проверки базы", show_alert=True)
            return
        
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
        try:
            with db_semaphore:
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
        except Exception as e:
            conn.rollback()
            logger.error(f"[ADD TO DATABASE] Ошибка при добавлении фильма в базу: {e}", exc_info=True)
            bot_instance.answer_callback_query(call.id, "❌ Ошибка добавления в базу", show_alert=True)
            return
        
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
            conn.rollback()
        except:
            pass
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


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_type:"), priority=1)
def plan_type_callback_fallback(call):
    """Запасной обработчик выбора типа плана (на случай, если основной не срабатывает)"""
    logger.info("=" * 80)
    logger.info(f"[PLAN TYPE FALLBACK] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_type = call.data.split(":")[1]  # 'home' или 'cinema'
        
        logger.info(f"[PLAN TYPE FALLBACK] Получен callback: user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}")
        logger.info(f"[PLAN TYPE FALLBACK] user_plan_state keys={list(user_plan_state.keys())}")
        logger.info(f"[PLAN TYPE FALLBACK] user_id in user_plan_state = {user_id in user_plan_state}")
        
        if user_id not in user_plan_state:
            logger.warning(f"[PLAN TYPE FALLBACK] Состояние не найдено для user_id={user_id}")
            bot_instance.edit_message_text("❌ Ошибка: сессия истекла. Начните заново с /plan", chat_id, call.message.message_id)
            return
        
        state = user_plan_state[user_id]
        link = state.get('link')
        
        if not link:
            logger.warning(f"[PLAN TYPE FALLBACK] Ссылка не найдена в состоянии: {state}")
            bot_instance.edit_message_text("❌ Ошибка: не найдена ссылка на фильм. Начните заново с /plan", chat_id, call.message.message_id)
            del user_plan_state[user_id]
            return
        
        state['type'] = plan_type
        state['step'] = 3
        
        try:
            bot_instance.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        bot_instance.send_message(chat_id, f"📅 Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\nМожно указать:\n• День недели (сегодня, завтра, понедельник и т.д.)\n• Дату (01.01, 1 января и т.д.)\n• Время (19:00, 20:30)")
        
        logger.info(f"[PLAN TYPE FALLBACK] Пользователь {user_id} выбрал {plan_type}, link={link}")
    except Exception as e:
        logger.error(f"[PLAN TYPE FALLBACK] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[PLAN TYPE FALLBACK] ===== END: callback_id={call.id}")


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("show_film_description:"))
def show_film_description_callback(call):
    """Обработчик кнопки '◀️ Вернуться к описанию' - показывает описание фильма из БД без API запроса"""
    logger.info("=" * 80)
    logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        bot_instance.answer_callback_query(call.id, text="⏳ Загружаю описание...")
        logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] answer_callback_query вызван, callback_id={call.id}")
        
        kp_id = call.data.split(":")[1]
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_thread_id = None
        if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
            message_thread_id = call.message.message_thread_id
        
        logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] kp_id={kp_id}, user_id={user_id}, chat_id={chat_id}")
        
        # Получаем информацию о фильме из БД (без API запроса)
        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                FROM movies WHERE chat_id = %s AND kp_id = %s
            ''', (chat_id, kp_id))
            row = cursor.fetchone()
        
        if not row:
            logger.error(f"[SHOW FILM DESCRIPTION FROM RATE] Фильм не найден в БД: kp_id={kp_id}, chat_id={chat_id}")
            bot_instance.answer_callback_query(call.id, "❌ Фильм не найден в базе", show_alert=True)
            return
        
        # Извлекаем данные
        if isinstance(row, dict):
            film_id = row.get('id')
            title = row.get('title')
            watched = row.get('watched')
            link = row.get('link')
            year = row.get('year')
            genres = row.get('genres')
            description = row.get('description')
            director = row.get('director')
            actors = row.get('actors')
            is_series = bool(row.get('is_series', 0))
        else:
            film_id = row[0]
            title = row[1]
            watched = row[2]
            link = row[3]
            year = row[4] if len(row) > 4 else None
            genres = row[5] if len(row) > 5 else None
            description = row[6] if len(row) > 6 else None
            director = row[7] if len(row) > 7 else None
            actors = row[8] if len(row) > 8 else None
            is_series = bool(row[9] if len(row) > 9 else 0)
        
        if not link:
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        # Формируем словарь info из данных БД (без API запроса)
        info = {
            'title': title,
            'year': year,
            'genres': genres,
            'description': description,
            'director': director,
            'actors': actors,
            'is_series': is_series
        }
        
        existing = (film_id, title, watched)
        
        # Ищем существующее сообщение с описанием фильма в bot_messages
        from moviebot.states import bot_messages
        film_message_id = None
        for msg_id, link_value in bot_messages.items():
            if link_value and kp_id in str(link_value):
                film_message_id = msg_id
                logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] Найдено сообщение с описанием фильма: message_id={film_message_id}")
                break
        
        # Обновляем или отправляем новое сообщение
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=existing,
            message_id=film_message_id, message_thread_id=message_thread_id
        )
        
        # Удаляем сообщение с оценкой, если оно есть
        if call.message:
            try:
                rating_message_id = call.message.message_id
                bot_instance.delete_message(chat_id, rating_message_id)
                logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] Сообщение с оценкой удалено: message_id={rating_message_id}")
            except Exception as del_e:
                logger.warning(f"[SHOW FILM DESCRIPTION FROM RATE] Не удалось удалить сообщение с оценкой: {del_e}")
        
        logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] Описание фильма показано из БД: kp_id={kp_id}")
        
    except Exception as e:
        logger.error(f"[SHOW FILM DESCRIPTION FROM RATE] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[SHOW FILM DESCRIPTION FROM RATE] ===== END: callback_id={call.id}")


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("mark_watched_from_description:"))
def mark_watched_from_description_callback(call):
    """Обработчик кнопки '✅ Отметить просмотренным' - отмечает фильм как просмотренный и обновляет сообщение"""
    logger.info("=" * 80)
    logger.info(f"[MARK WATCHED] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        # Отвечаем на callback сразу
        bot_instance.answer_callback_query(call.id, text="⏳ Отмечаю как просмотренный...")
        logger.info(f"[MARK WATCHED] answer_callback_query вызван, callback_id={call.id}")
        
        film_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id if call.message else None
        message_thread_id = None
        if call.message and hasattr(call.message, 'message_thread_id') and call.message.message_thread_id:
            message_thread_id = call.message.message_thread_id
        
        logger.info(f"[MARK WATCHED] film_id={film_id}, user_id={user_id}, chat_id={chat_id}, message_id={message_id}")
        
        # Получаем информацию о фильме из БД
        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, kp_id, year, genres, description, director, actors, is_series
                FROM movies WHERE id = %s AND chat_id = %s
            ''', (film_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                logger.error(f"[MARK WATCHED] Фильм не найден: film_id={film_id}, chat_id={chat_id}")
                bot_instance.answer_callback_query(call.id, "❌ Фильм не найден", show_alert=True)
                return
            
            # Извлекаем данные
            if isinstance(row, dict):
                title = row.get('title')
                watched = row.get('watched')
                link = row.get('link')
                kp_id = row.get('kp_id')
                year = row.get('year')
                genres = row.get('genres')
                description = row.get('description')
                director = row.get('director')
                actors = row.get('actors')
                is_series = bool(row.get('is_series', 0))
            else:
                title = row[1]
                watched = row[2]
                link = row[3]
                kp_id = row[4]
                year = row[5]
                genres = row[6]
                description = row[7]
                director = row[8]
                actors = row[9]
                is_series = bool(row[10] if len(row) > 10 else 0)
            
            # Отмечаем фильм как просмотренный
            cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
            logger.info(f"[MARK WATCHED] Фильм {film_id} отмечен как просмотренный")
        
        # Формируем словарь info из данных БД (без API запроса)
        info = {
            'title': title,
            'year': year,
            'genres': genres,
            'description': description,
            'director': director,
            'actors': actors,
            'is_series': is_series
        }
        
        # Обновляем existing (теперь watched=1)
        existing = (film_id, title, True)
        
        # Обновляем сообщение с описанием фильма
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=existing,
            message_id=message_id, message_thread_id=message_thread_id
        )
        
        logger.info(f"[MARK WATCHED] Сообщение обновлено: film_id={film_id}, kp_id={kp_id}")
        bot_instance.answer_callback_query(call.id, text="✅ Фильм отмечен как просмотренный", show_alert=False)
        
    except Exception as e:
        logger.error(f"[MARK WATCHED] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[MARK WATCHED] ===== END: callback_id={call.id}")


def register_film_callbacks(bot_instance):
    """Регистрирует callback handlers для карточки фильма (уже зарегистрированы через декораторы)"""
    # Handlers уже зарегистрированы через декораторы @bot_instance.callback_query_handler
    # при импорте модуля, поэтому эта функция просто для совместимости
    pass

