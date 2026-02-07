from moviebot.bot.bot_init import bot, BOT_ID
"""
Обработчики команды /rate
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.states import rating_messages, bot_messages
from moviebot.bot.handlers.series import ensure_movie_in_database
from moviebot.api.kinopoisk_api import extract_movie_info
from moviebot.database.db_operations import log_request
from moviebot.utils.parsing import extract_kp_id_from_text
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_rate_handlers(bot):
    """Регистрирует обработчики команды /rate"""
    
    @bot.message_handler(commands=['rate'], func=lambda m: not m.reply_to_message)
    def rate_movie(message):
        """Команда /rate - оценить просмотренные фильмы (только чистая команда без реплая)"""
        username = message.from_user.username or f"user_{message.from_user.id}"
        log_request(message.from_user.id, username, '/rate', message.chat.id)
        logger.info(f"Команда /rate от пользователя {message.from_user.id}")
        chat_id = message.chat.id
        user_id = message.from_user.id
        
        # Проверяем, есть ли аргументы в команде
        text = message.text or ""
        parts = text.split(None, 2)  # Разбиваем на максимум 3 части: /rate, kp_id/url, rating
        
        if len(parts) >= 3:
            # Есть аргументы - пытаемся поставить оценку напрямую
            kp_id_or_url = parts[1]
            rating_str = parts[2]
            
            # Извлекаем kp_id
            kp_id = extract_kp_id_from_text(kp_id_or_url)
            if not kp_id:
                bot.reply_to(message, "❌ Не удалось распознать kp_id. Используйте формат:\n<code>/rate 81682 10</code>\nили\n<code>/rate https://www.kinopoisk.ru/film/81682/ 10</code>", parse_mode='HTML')
                return
            
            # Парсим оценку
            try:
                rating = int(rating_str.strip())
                if not (1 <= rating <= 10):
                    bot.reply_to(message, "❌ Оценка должна быть от 1 до 10")
                    return
            except ValueError:
                bot.reply_to(message, "❌ Неверный формат оценки. Используйте число от 1 до 10")
                return
            
            # Используем локальные соединение и курсор
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            # Ищем фильм в базе
            with db_lock:
                try:
                    cursor_local.execute('''
                        SELECT id, title FROM movies
                        WHERE chat_id = %s AND kp_id = %s AND watched = 1
                    ''', (chat_id, str(str(kp_id))))
                    film_row = cursor_local.fetchone()
                except Exception as db_e:
                    logger.error(f"[RATE] Ошибка запроса БД: {db_e}", exc_info=True)
                    bot.reply_to(message, "❌ Ошибка доступа к базе данных")
                    return
                
                if not film_row:
                    bot.reply_to(message, f"❌ Фильм с kp_id={kp_id} не найден в базе или не помечен как просмотренный")
                    return
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                try:
                    # Проверяем, не оценил ли уже пользователь этот фильм
                    cursor_local.execute('''
                        SELECT rating FROM ratings
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s
                    ''', (chat_id, film_id, user_id))
                    existing = cursor_local.fetchone()
                    
                    if existing:
                        old_rating = existing.get('rating') if isinstance(existing, dict) else existing[0]
                        # Обновляем оценку
                        cursor_local.execute('''
                            UPDATE ratings SET rating = %s, is_imported = FALSE
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s
                        ''', (rating, chat_id, film_id, user_id))
                        conn_local.commit()
                        bot.reply_to(message, f"✅ Оценка обновлена!\n\n<b>{title}</b>\nСтарая оценка: {old_rating}/10\nНовая оценка: {rating}/10", parse_mode='HTML')
                        logger.info(f"[RATE] Пользователь {user_id} обновил оценку для фильма {kp_id} с {old_rating} на {rating}")
                    else:
                        # Сохраняем новую оценку
                        cursor_local.execute('''
                            INSERT INTO ratings (chat_id, film_id, user_id, rating)
                            VALUES (%s, %s, %s, %s)
                        ''', (chat_id, film_id, user_id, rating))
                        conn_local.commit()
                        bot.reply_to(message, f"✅ Оценка сохранена!\n\n<b>{title}</b>\nОценка: {rating}/10", parse_mode='HTML')
                        logger.info(f"[RATE] Пользователь {user_id} поставил оценку {rating} для фильма {kp_id}")
                        try:
                            from moviebot.achievements_notify import notify_new_achievements
                            notify_new_achievements(user_id, context={'film_title': title or 'фильм'})
                        except Exception as ach_e:
                            logger.debug(f"[RATE] Achievement notify: {ach_e}")
                except Exception as db_e:
                    logger.error(f"[RATE] Ошибка сохранения оценки: {db_e}", exc_info=True)
                    try:
                        conn_local.rollback()
                    except:
                        pass
                    bot.reply_to(message, "❌ Ошибка при сохранении оценки")
                    return
            
            return
        
        # Если аргументов нет - показываем список как раньше
        # TODO: Извлечь полную логику из moviebot.py строки 10484-10626
        # Используем локальные соединение и курсор (если еще не определены выше)
        if 'conn_local' not in locals():
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
        
        # Получаем все просмотренные фильмы (максимум 10), исключая фильмы с только импортированными оценками
        with db_lock:
            try:
                cursor_local.execute('''
                    SELECT m.id, m.kp_id, m.title, m.year
                    FROM movies m
                    WHERE m.chat_id = %s AND m.watched = 1
                    AND NOT (
                        NOT EXISTS (
                            SELECT 1 FROM ratings r 
                            WHERE r.chat_id = m.chat_id 
                            AND r.film_id = m.id 
                            AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                        )
                        AND EXISTS (
                            SELECT 1 FROM ratings r 
                            WHERE r.chat_id = m.chat_id 
                            AND r.film_id = m.id 
                            AND r.is_imported = TRUE
                        )
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM ratings r 
                        WHERE r.chat_id = m.chat_id 
                        AND r.film_id = m.id 
                        AND r.user_id = %s
                        AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    )
                    ORDER BY m.title
                    LIMIT 10
                ''', (chat_id, user_id))
                unwatched_films = cursor_local.fetchall()
            except Exception as db_e:
                logger.error(f"[RATE] Ошибка запроса списка фильмов: {db_e}", exc_info=True)
                bot.reply_to(message, "❌ Ошибка доступа к базе данных")
                return
        
        if not unwatched_films:
            text = "✅ Все просмотренные фильмы уже оценены!\n\nВы можете:\n• Отметить фильм просмотренным в базе\n• Найти фильм, который вы смотрели, через поиск"
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🗃️ Перейти в базу", callback_data="database:unwatched"))
            markup.add(InlineKeyboardButton("🔍 Найти фильм", callback_data="start_menu:search"))
            markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
            bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')
            return
        
        # Формируем список фильмов для оценки
        text = "⭐ <b>Оцените просмотренные фильмы:</b>\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        
        for row in unwatched_films:
            if isinstance(row, dict):
                film_id = row.get('id')
                kp_id = row.get('kp_id')
                title = row.get('title')
                year = row.get('year')
            else:
                film_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None)
                kp_id = row[1]
                title = row[2]
                year = row[3] if len(row) > 3 else '—'
            
            text += f"• <b>{title}</b> ({year})\n"
            # Добавляем кнопку с фильмом - при нажатии откроется описание фильма
            button_text = f"{title} ({year})"
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"rate_from_list:{int(kp_id)}"))
        
        text += "\n<i>Нажмите на фильм, чтобы открыть его описание и оценить</i>"
        markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
        
        bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_rating:"))
    def handle_confirm_rating(call):
        """Обработчик подтверждения оценки"""
        # TODO: Извлечь из moviebot.py строки 7696-7749
        try:
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                logger.warning(f"[RATE] Не удалось ответить на callback в handle_confirm_rating (query too old): {e}")
            # TODO: Реализовать логику подтверждения оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_confirm_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_rating:"))
    def handle_cancel_rating(call):
        """Обработчик отмены оценки"""
        # TODO: Извлечь из moviebot.py строки 7750-7776
        try:
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                logger.warning(f"[RATE] Не удалось ответить на callback в handle_cancel_rating (query too old): {e}")
            # TODO: Реализовать логику отмены оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_cancel_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("rate_from_list:"))
    def rate_from_list_callback(call):
        """Обработчик выбора фильма из списка /rate - открывает описание фильма"""
        # Отвечаем на callback сразу, оборачивая в try-except для старых колбеков
        callback_answered = False
        try:
            try:
                bot.answer_callback_query(call.id, text="⏳ Загружаю...")
                callback_answered = True
            except Exception as e:
                logger.warning(f"[RATE FROM LIST] Не удалось ответить на callback (query too old): {e}")
                callback_answered = False
            
            kp_id = call.data.split(":")[1]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[RATE FROM LIST] Пользователь {user_id} выбрал фильм kp_id={kp_id} из списка /rate")
            
            # Используем локальные соединение и курсор
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            # Получаем информацию о фильме из базы
            with db_lock:
                try:
                    cursor_local.execute('SELECT id, title, link, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                    row = cursor_local.fetchone()
                except Exception as db_e:
                    logger.error(f"[RATE FROM LIST] Ошибка запроса БД: {db_e}", exc_info=True)
                    if not callback_answered:
                        try:
                            bot.answer_callback_query(call.id, "❌ Ошибка доступа к базе данных", show_alert=True)
                        except:
                            pass
                    return
            
            if not row:
                if not callback_answered:
                    try:
                        bot.answer_callback_query(call.id, "❌ Фильм не найден в базе", show_alert=True)
                    except:
                        pass
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
            link = row.get('link') if isinstance(row, dict) else row[2]
            watched = row.get('watched') if isinstance(row, dict) else row[3]
            
            # Получаем информацию о фильме через API
            # ВАЖНО: extract_movie_info уже импортирован глобально в начале файла (строка 10)
            info = extract_movie_info(link)
            
            if not info:
                if not callback_answered:
                    try:
                        bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                    except:
                        pass
                return
            
            # Формируем existing для передачи в show_film_info_with_buttons
            existing = (film_id, title, watched)
            
            # Получаем message_id и message_thread_id для обновления сообщения
            message_id = call.message.message_id if call.message else None
            message_thread_id = getattr(call.message, 'message_thread_id', None) if call.message else None
            
            # Показываем описание фильма со всеми базовыми кнопками
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            show_film_info_with_buttons(
                chat_id=chat_id, 
                user_id=user_id, 
                info=info, 
                link=link, 
                kp_id=kp_id, 
                existing=existing,
                message_id=message_id,
                message_thread_id=message_thread_id
            )
            
        except Exception as e:
            logger.error(f"[RATE FROM LIST] Ошибка: {e}", exc_info=True)
            if not callback_answered:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except:
                    pass


def handle_rating_internal(message, rating):
    """Внутренняя функция для обработки оценки - добавляет фильм в базу при успешной оценке"""
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    film_id = None
    kp_id = None
    
    # 1. Поиск по реплаю
    if message.reply_to_message:
        reply_msg_id = message.reply_to_message.message_id
        
        rating_msg_value = rating_messages.get(reply_msg_id)
        if rating_msg_value and isinstance(rating_msg_value, str) and rating_msg_value.startswith("kp_id:"):
            kp_id = rating_msg_value.split(":")[1]
            logger.info(f"[RATE] kp_id из rating_messages (прямой реплай): {kp_id}")
        else:
            film_id = rating_messages.get(reply_msg_id)
            if isinstance(film_id, str) and film_id.startswith("kp_id:"):
                kp_id = film_id.split(":")[1]
                film_id = None
                logger.info(f"[RATE] kp_id из rating_messages (прямая строка): {kp_id}")
            else:
                if not film_id:
                    current_msg = message.reply_to_message
                    checked_ids = set()
                    while current_msg and current_msg.message_id not in checked_ids:
                        checked_ids.add(current_msg.message_id)
                        if current_msg.message_id in rating_messages:
                            val = rating_messages[current_msg.message_id]
                            if isinstance(val, str) and val.startswith("kp_id:"):
                                kp_id = val.split(":")[1]
                                logger.info(f"[RATE] kp_id из цепочки: {kp_id}")
                                break
                            elif isinstance(val, int):
                                film_id = val
                                break
                        if current_msg.message_id in bot_messages:
                            reply_link = bot_messages[current_msg.message_id]
                            if reply_link:
                                match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                                if match:
                                    kp_id = match.group(2)
                                    conn = get_db_connection()
                                    cur = get_db_cursor()
                                    with db_lock:
                                        try:
                                            cur.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                                            row = cur.fetchone()
                                            if row:
                                                film_id = row.get('id') if isinstance(row, dict) else row[0]
                                                break
                                        except Exception as e:
                                            logger.warning(f"[RATE] Ошибка поиска по kp_id: {e}")
                                    cur.close()
                                    conn.close()
                        current_msg = current_msg.reply_to_message if hasattr(current_msg, 'reply_to_message') else None

    # 2. Проверка user_private_handler_state для личных чатов без реплая
    if not film_id and not kp_id and not message.reply_to_message and message.chat.type == 'private':
        from moviebot.states import user_private_handler_state
        if user_id in user_private_handler_state:
            state = user_private_handler_state[user_id]
            if state.get('handler') == 'rate_film':
                kp_id = state.get('kp_id')
                film_id = state.get('film_id')
                logger.info(f"[RATE] kp_id={kp_id}, film_id={film_id} из user_private_handler_state")
                # Очищаем состояние после использования
                del user_private_handler_state[user_id]
    
    # 3. Глобальный поиск — один lock на весь цикл
    if not film_id and not kp_id and not message.reply_to_message:
        logger.info("[RATE] Глобальный поиск по rating_messages")
        conn = get_db_connection()
        cur = get_db_cursor()
        try:
            with db_lock:
                for msg_id, value in list(rating_messages.items()):
                    if isinstance(value, int):
                        cur.execute('SELECT id, kp_id FROM movies WHERE id = %s AND chat_id = %s', (value, chat_id))
                        row = cur.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                            kp_id = row.get('kp_id') if isinstance(row, dict) else row[1]
                            logger.info(f"[RATE] Нашли film_id={film_id}, kp_id={kp_id}")
                            break
                    elif isinstance(value, str) and value.startswith("kp_id:"):
                        kp_cand = value.split(":")[1]
                        cur.execute('SELECT id FROM movies WHERE kp_id = %s AND chat_id = %s', (str(kp_cand), chat_id))
                        row = cur.fetchone()
                        if row:
                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                            kp_id = kp_cand
                            logger.info(f"[RATE] Нашли по kp_id={kp_id}")
                            break
        except Exception as e:
            logger.warning(f"[RATE] Ошибка глобального поиска: {e}")
        finally:
            cur.close()
            conn.close()

    # 4. kp_id из текста
    if not film_id and not kp_id:
        text = message.text or ""
        if 'kinopoisk.ru' in text or 'kinopoisk.com' in text:
            kp_id = extract_kp_id_from_text(text)
        elif message.reply_to_message and message.reply_to_message.text:
            reply_text = message.reply_to_message.text
            if 'kinopoisk.ru' in reply_text or 'kinopoisk.com' in reply_text:
                kp_id = extract_kp_id_from_text(reply_text)

    # 5. Добавление фильма
    if not film_id and kp_id:
        logger.info(f"[RATE] Добавляем фильм: kp_id={kp_id}")
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        if info and info.get('is_series'):
            link = f"https://www.kinopoisk.ru/series/{kp_id}/"
        if info:
            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            logger.info(f"[RATE] film_id={film_id}, inserted={was_inserted}")
            if was_inserted and info.get('is_series'):
                from moviebot.utils.helpers import maybe_send_series_limit_message
                maybe_send_series_limit_message(bot, chat_id, user_id, None)
        else:
            bot.reply_to(message, "❌ Не удалось получить данные фильма.")
            return

    # Основная часть — оценка
    if film_id:
        conn_local = get_db_connection()
        cursor_local = None
        try:
            with db_lock:
                cursor_local = conn_local.cursor()
                cursor_local.execute('SELECT watched FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                watched_row = cursor_local.fetchone()
                is_watched_before = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0]) if watched_row else False

                cursor_local.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                conn_local.commit()

                cursor_local.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                avg_row = cursor_local.fetchone()
                avg = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row else None)

                cursor_local.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                kp_row = cursor_local.fetchone()
                if kp_row:
                    kp_id_from_db = kp_row.get('kp_id') if isinstance(kp_row, dict) else kp_row[0]
                    if kp_id_from_db:
                        kp_id = str(kp_id_from_db)
                # Если kp_id не был установлен ранее и не найден в БД, оставляем None

            avg_str = f"{avg:.1f}" if avg else "—"

            if not is_watched_before:
                conn_watch = get_db_connection()
                cursor_watch = None
                try:
                    with db_lock:
                        cursor_watch = conn_watch.cursor()
                        cursor_watch.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                        conn_watch.commit()
                    logger.info(f"[RATE] Отмечен просмотренным")
                finally:
                    if cursor_watch:
                        try:
                            cursor_watch.close()
                        except:
                            pass
                    try:
                        conn_watch.close()
                    except:
                        pass

            # Сообщение пользователю
            if not is_watched_before:
                markup = InlineKeyboardMarkup()
                if kp_id:
                    markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{int(kp_id)}"))
                bot.reply_to(
                    message,
                    f"Спасибо! Фильм отмечен просмотренным, оценка {rating}/10.\nСредняя: {avg_str}/10",
                    reply_markup=markup
                )
            else:
                bot.reply_to(message, f"✅ Оценка {rating}/10 сохранена. Средняя: {avg_str}/10")
            try:
                from moviebot.achievements_notify import notify_new_achievements
                with db_lock:
                    cursor_local.execute('SELECT title FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    trow = cursor_local.fetchone()
                film_title = (trow.get('title') if isinstance(trow, dict) else (trow[0] if trow else None)) or 'фильм'
                notify_new_achievements(user_id, context={'film_title': film_title})
            except Exception as ach_e:
                logger.debug(f"[RATE] Achievement notify: {ach_e}")

            # Обновление описания
            if kp_id:
                try:
                    film_message_id = None
                    for msg_id, link_value in bot_messages.items():
                        if link_value and str(kp_id) in str(link_value):
                            film_message_id = msg_id
                            break

                    if film_message_id:
                        from moviebot.bot.handlers.series import show_film_info_with_buttons
                        existing = None
                        link = None
                        info = None
                        conn_info = get_db_connection()
                        cursor_info = None
                        try:
                            with db_lock:
                                cursor_info = conn_info.cursor()
                                cursor_info.execute('''
                                    SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                                    FROM movies WHERE id = %s AND chat_id = %s
                                ''', (film_id, chat_id))
                                row = cursor_info.fetchone()
                                if row:
                                    existing = (row.get('id'), row.get('title'), row.get('watched'))
                                    info = {
                                        'title': row.get('title'),
                                        'year': row.get('year'),
                                        'genres': row.get('genres'),
                                        'description': row.get('description'),
                                        'director': row.get('director'),
                                        'actors': row.get('actors'),
                                        'is_series': bool(row.get('is_series', 0))
                                    }
                                    link = row.get('link') or f"https://www.kinopoisk.ru/film/{kp_id}/"
                        finally:
                            if cursor_info:
                                try:
                                    cursor_info.close()
                                except:
                                    pass
                            try:
                                conn_info.close()
                            except:
                                pass

                        if info and existing:
                            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing, message_id=film_message_id)
                            logger.info(f"[RATE] Описание обновлено")
                except Exception as e:
                    logger.warning(f"[RATE] Ошибка обновления описания: {e}")

            # Похожие фильмы
            if kp_id:
                try:
                    from moviebot.utils.helpers import has_recommendations_access
                    if has_recommendations_access(chat_id, user_id):
                        is_group = chat_id < 0 or (hasattr(message.chat, 'type') and message.chat.type in ['group', 'supergroup'])

                        should_send = False
                        rec_text = ""

                        if is_group:
                            avg_rating = None
                            active_count = 0
                            rated_count = 0
                            conn_rec = get_db_connection()
                            cursor_rec = None
                            try:
                                with db_lock:
                                    cursor_rec = conn_rec.cursor()
                                    cursor_rec.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                                    avg_row = cursor_rec.fetchone()
                                    avg_rating = avg_row.get('avg') if avg_row else None

                                    # Участники чата за вычетом бота (бот не голосует)
                                    if BOT_ID is not None:
                                        cursor_rec.execute('SELECT COUNT(DISTINCT user_id) FROM stats WHERE chat_id = %s AND user_id IS NOT NULL AND user_id != %s', (chat_id, BOT_ID))
                                    else:
                                        cursor_rec.execute('SELECT COUNT(DISTINCT user_id) FROM stats WHERE chat_id = %s AND user_id IS NOT NULL', (chat_id,))
                                    active_count_row = cursor_rec.fetchone()
                                    active_count = active_count_row.get('count', 0) if isinstance(active_count_row, dict) else (active_count_row[0] if active_count_row else 0)

                                    cursor_rec.execute('SELECT COUNT(DISTINCT user_id) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                                    rated_count_row = cursor_rec.fetchone()
                                    rated_count = rated_count_row.get('count', 0) if isinstance(rated_count_row, dict) else (rated_count_row[0] if rated_count_row else 0)
                            finally:
                                if cursor_rec:
                                    try:
                                        cursor_rec.close()
                                    except:
                                        pass
                                try:
                                    conn_rec.close()
                                except:
                                    pass

                            if avg_rating and avg_rating > 8.5 and active_count > 0:
                                percentage = rated_count / active_count
                                if percentage >= 0.65:
                                    should_send = True
                                    rec_text = f"🔥 Средняя {avg_rating:.1f}/10, {rated_count}/{active_count} ({percentage*100:.0f}%) — похожие:\n\n"
                        else:
                            if rating >= 9:
                                should_send = True
                                rec_text = f"🔥 Ты поставил {rating}/10 — вот похожие:\n\n"

                        if should_send and kp_id and kp_id != "None":
                            try:
                                from moviebot.api.kinopoisk_api import get_similars
                                kp_id_int = int(kp_id)
                                similars = get_similars(kp_id_int)
                                logger.info(f"[RATE] Похожие: {len(similars)} для {kp_id_int}")

                                if similars:
                                    markup = InlineKeyboardMarkup(row_width=1)
                                    for sim_id, name, is_series in similars[:6]:
                                        short = name[:48] + '...' if len(name) > 48 else name
                                        icon = '📺' if is_series else '🎬'
                                        markup.add(InlineKeyboardButton(f"{icon} {short}", callback_data=f"back_to_film:{sim_id}"))
                                    markup.add(InlineKeyboardButton("✅ Закрыть", callback_data="delete_this_message"))

                                    bot.send_message(chat_id, rec_text, reply_markup=markup, parse_mode='HTML')
                                    logger.info(f"[RATE] Похожие отправлены")
                                else:
                                    logger.info("[RATE] Похожих нет")
                            except (ValueError, TypeError) as e:
                                logger.error(f"[RATE] Ошибка преобразования kp_id в int: kp_id={kp_id}, error={e}")
                            except Exception as e:
                                logger.error(f"[RATE] Ошибка получения похожих: {e}", exc_info=True)
                    else:
                        logger.info("[RATE] Нет доступа к рекомендациям")
                except Exception as e:
                    logger.error(f"[RATE] Ошибка рекомендаций: {e}", exc_info=True)

        except Exception as e:
            logger.error(f"[RATE] Критическая ошибка: {e}", exc_info=True)
            try:
                conn_local.rollback()
            except:
                pass
            bot.reply_to(message, "❌ Ошибка при сохранении оценки.")
        finally:
            if cursor_local:
                try:
                    cursor_local.close()
                except:
                    pass
            try:
                conn_local.close()
            except:
                pass
    else:
        bot.reply_to(message, "❌ Не удалось найти фильм. Ответь на сообщение с фильмом.")

def handle_edit_rating_internal(message, state):
    """Внутренняя функция для обработки изменения оценки"""
    logger.info(f"[EDIT RATING INTERNAL] ===== START: message_id={message.message_id}, user_id={message.from_user.id}")
    try:
        user_id = message.from_user.id
        chat_id = message.chat.id
        text = message.text.strip() if message.text else ""
        film_id = state.get('film_id')
        
        logger.info(f"[EDIT RATING INTERNAL] Обработка: text='{text}', film_id={film_id}")
        
        if not film_id:
            bot.reply_to(message, "❌ Ошибка: фильм не найден.")
            from moviebot.states import user_edit_state
            if user_id in user_edit_state:
                del user_edit_state[user_id]
            return
        
        # Получаем kp_id для кнопки "К описанию"
        kp_id = None
        conn_kp = get_db_connection()
        cursor_kp = None
        try:
            with db_lock:
                cursor_kp = conn_kp.cursor()
                cursor_kp.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                kp_row = cursor_kp.fetchone()
                if kp_row:
                    kp_id = str(kp_row.get('kp_id') if isinstance(kp_row, dict) else kp_row[0])
        except Exception as e:
            logger.warning(f"[EDIT RATING INTERNAL] Ошибка получения kp_id: {e}")
        finally:
            if cursor_kp:
                try:
                    cursor_kp.close()
                except:
                    pass
            try:
                conn_kp.close()
            except:
                pass
        
        # Создаем кнопки для сообщений об ошибке
        markup_error = InlineKeyboardMarkup(row_width=1)
        if kp_id:
            markup_error.add(InlineKeyboardButton("📌 К описанию", callback_data=f"back_to_film:{int(kp_id)}"))
        markup_error.add(InlineKeyboardButton("❌ Отменить", callback_data="edit:cancel"))
        
        # Парсим оценку
        try:
            rating = int(text)
            if not (1 <= rating <= 10):
                bot.reply_to(message, "❌ Оценка должна быть от 1 до 10", reply_markup=markup_error)
                return
        except ValueError:
            bot.reply_to(message, "❌ Неверный формат оценки. Используйте число от 1 до 10", reply_markup=markup_error)
            return
        
        # Используем локальные соединение и курсор
        conn_local_edit = get_db_connection()
        cursor_local_edit = get_db_cursor()
        
        # Обновляем оценку
        with db_lock:
            try:
                cursor_local_edit.execute('''
                    UPDATE ratings SET rating = %s, is_imported = FALSE
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (rating, chat_id, film_id, user_id))
                conn_local_edit.commit()
                
                # Получаем информацию о фильме
                cursor_local_edit.execute('SELECT title FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                film_row = cursor_local_edit.fetchone()
                title = film_row.get('title') if isinstance(film_row, dict) else (film_row[0] if film_row else "Фильм")
            except Exception as db_e:
                logger.error(f"[EDIT RATING INTERNAL] Ошибка работы с БД: {db_e}", exc_info=True)
                try:
                    conn_local_edit.rollback()
                except:
                    pass
                bot.reply_to(message, "❌ Ошибка при обновлении оценки")
                return
        
        bot.reply_to(message, f"✅ Оценка обновлена!\n\n<b>{title}</b>\nНовая оценка: {rating}/10", parse_mode='HTML')
        logger.info(f"[EDIT RATING INTERNAL] Оценка обновлена для фильма {film_id}: {rating}/10")
        
        from moviebot.states import user_edit_state
        if user_id in user_edit_state:
            del user_edit_state[user_id]
    except Exception as e:
        logger.error(f"[EDIT RATING INTERNAL] Ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке.")
        except:
            pass
