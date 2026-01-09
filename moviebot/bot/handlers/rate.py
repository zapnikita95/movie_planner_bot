from moviebot.bot.bot_init import bot
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
            
            # Ищем фильм в базе
            with db_lock:
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, str(str(kp_id))))
                film_row = cursor.fetchone()
                
                if not film_row:
                    bot.reply_to(message, f"❌ Фильм с kp_id={kp_id} не найден в базе или не помечен как просмотренный")
                    return
                
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                # Проверяем, не оценил ли уже пользователь этот фильм
                cursor.execute('''
                    SELECT rating FROM ratings
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s
                ''', (chat_id, film_id, user_id))
                existing = cursor.fetchone()
                
                if existing:
                    old_rating = existing.get('rating') if isinstance(existing, dict) else existing[0]
                    # Обновляем оценку
                    cursor.execute('''
                        UPDATE ratings SET rating = %s, is_imported = FALSE
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s
                    ''', (rating, chat_id, film_id, user_id))
                    conn.commit()
                    bot.reply_to(message, f"✅ Оценка обновлена!\n\n<b>{title}</b>\nСтарая оценка: {old_rating}/10\nНовая оценка: {rating}/10", parse_mode='HTML')
                    logger.info(f"[RATE] Пользователь {user_id} обновил оценку для фильма {kp_id} с {old_rating} на {rating}")
                else:
                    # Сохраняем новую оценку
                    cursor.execute('''
                        INSERT INTO ratings (chat_id, film_id, user_id, rating)
                        VALUES (%s, %s, %s, %s)
                    ''', (chat_id, film_id, user_id, rating))
                    conn.commit()
                    bot.reply_to(message, f"✅ Оценка сохранена!\n\n<b>{title}</b>\nОценка: {rating}/10", parse_mode='HTML')
                    logger.info(f"[RATE] Пользователь {user_id} поставил оценку {rating} для фильма {kp_id}")
            
            return
        
        # Если аргументов нет - показываем список как раньше
        # TODO: Извлечь полную логику из moviebot.py строки 10484-10626
        # Получаем все просмотренные фильмы (максимум 10), исключая фильмы с только импортированными оценками
        with db_lock:
            cursor.execute('''
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
            unwatched_films = cursor.fetchall()
        
        if not unwatched_films:
            bot.reply_to(message, "✅ Все просмотренные фильмы уже оценены!")
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
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        bot.reply_to(message, text, reply_markup=markup, parse_mode='HTML')

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_rating:"))
    def handle_confirm_rating(call):
        """Обработчик подтверждения оценки"""
        # TODO: Извлечь из moviebot.py строки 7696-7749
        try:
            bot.answer_callback_query(call.id)
            # TODO: Реализовать логику подтверждения оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_confirm_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_rating:"))
    def handle_cancel_rating(call):
        """Обработчик отмены оценки"""
        # TODO: Извлечь из moviebot.py строки 7750-7776
        try:
            bot.answer_callback_query(call.id)
            # TODO: Реализовать логику отмены оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_cancel_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("rate_from_list:"))
    def rate_from_list_callback(call):
        """Обработчик выбора фильма из списка /rate - открывает описание фильма"""
        try:
            bot.answer_callback_query(call.id)
            kp_id = call.data.split(":")[1]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[RATE FROM LIST] Пользователь {user_id} выбрал фильм kp_id={kp_id} из списка /rate")
            
            # Получаем информацию о фильме из базы
            with db_lock:
                cursor.execute('SELECT id, title, link, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                row = cursor.fetchone()
            
            if not row:
                bot.answer_callback_query(call.id, "❌ Фильм не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
            link = row.get('link') if isinstance(row, dict) else row[2]
            watched = row.get('watched') if isinstance(row, dict) else row[3]
            
            # Получаем информацию о фильме через API
            from moviebot.api.kinopoisk_api import extract_movie_info
            info = extract_movie_info(link)
            
            if not info:
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Формируем existing для передачи в show_film_info_with_buttons
            existing = (film_id, title, watched)
            
            # Показываем описание фильма со всеми базовыми кнопками
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing)
            
        except Exception as e:
            logger.error(f"[RATE FROM LIST] Ошибка: {e}", exc_info=True)
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
    
    if message.reply_to_message:
        reply_msg_id = message.reply_to_message.message_id
        
        # Сначала проверяем rating_messages на наличие kp_id (формат "kp_id:123")
        rating_msg_value = rating_messages.get(reply_msg_id)
        if rating_msg_value and isinstance(rating_msg_value, str) and rating_msg_value.startswith("kp_id:"):
            kp_id = rating_msg_value.split(":")[1]
            logger.info(f"[RATE INTERNAL] Найден kp_id из rating_messages: {kp_id}")
        else:
            # Проверяем все возможные источники film_id: rating_messages, bot_messages (цепочка реплаев)
            # Сначала проверяем прямое сообщение
            film_id = rating_messages.get(reply_msg_id)
            
            # Если film_id - это строка "kp_id:...", извлекаем kp_id
            if isinstance(film_id, str) and film_id.startswith("kp_id:"):
                kp_id = film_id.split(":")[1]
                film_id = None
                logger.info(f"[RATE INTERNAL] Найден kp_id из rating_messages (прямая проверка): {kp_id}")
            else:
                # Если не найдено, проверяем цепочку реплаев рекурсивно
                if not film_id:
                    current_msg = message.reply_to_message
                    checked_ids = set()  # Чтобы избежать циклов
                    while current_msg and current_msg.message_id not in checked_ids:
                        checked_ids.add(current_msg.message_id)
                        # Проверяем rating_messages
                        if current_msg.message_id in rating_messages:
                            rating_value = rating_messages[current_msg.message_id]
                            # Проверяем, это kp_id или film_id
                            if isinstance(rating_value, str) and rating_value.startswith("kp_id:"):
                                kp_id = rating_value.split(":")[1]
                                logger.info(f"[RATE INTERNAL] Найден kp_id из rating_messages (цепочка реплаев): {kp_id}")
                                break
                            elif isinstance(rating_value, int):
                                film_id = rating_value
                                break
                        # Проверяем bot_messages (сообщения с фильмами)
                        if current_msg.message_id in bot_messages:
                            reply_link = bot_messages[current_msg.message_id]
                            if reply_link:
                                # Извлекаем kp_id из ссылки для поиска
                                match = re.search(r'kinopoisk\.ru/(film|series)/(\d+)', reply_link)
                                if match:
                                    kp_id = match.group(2)
                                    with db_lock:
                                        cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                                        row = cursor.fetchone()
                                        if row:
                                            film_id = row.get('id') if isinstance(row, dict) else row[0]
                                            break
                        # Переходим к родительскому сообщению
                        current_msg = current_msg.reply_to_message if hasattr(current_msg, 'reply_to_message') else None
    
    # Если film_id не найден, но есть kp_id, добавляем фильм в базу
    if not film_id and kp_id:
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        if info:
            film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
            if was_inserted:
                logger.info(f"[RATE INTERNAL] Фильм добавлен в базу при оценке: kp_id={kp_id}, film_id={film_id}")
        else:
            logger.warning(f"[RATE INTERNAL] Не удалось получить информацию о фильме для kp_id={kp_id}")
            bot.reply_to(message, "❌ Не удалось получить информацию о фильме для оценки.")
            return
    
    # Если film_id все еще не найден и нет реплая, пытаемся найти последнее сообщение бота с запросом на оценку
    if not film_id and not message.reply_to_message:
        logger.info(f"[RATE INTERNAL] Нет реплая, ищем последнее сообщение бота с запросом на оценку в rating_messages")
        # Пробуем найти последнее сообщение в rating_messages для этого чата
        # Для этого нужно получить последние сообщения бота в этом чате
        # Но так как мы не можем получить историю, попробуем другой подход:
        # Ищем в rating_messages все значения, которые являются film_id (числа) или kp_id (строки "kp_id:...")
        # и проверяем, есть ли такой фильм в базе для этого чата
        
        # Сначала пробуем найти последний добавленный film_id из rating_messages
        # Для этого ищем все значения в rating_messages и проверяем, есть ли такой film_id в базе
        found_film_id = None
        found_kp_id = None
        
        # Проходим по всем значениям в rating_messages
        for msg_id, value in rating_messages.items():
            if isinstance(value, int):
                # Это film_id - проверяем, есть ли такой фильм в базе для этого чата
                with db_lock:
                    cursor.execute('SELECT id, kp_id FROM movies WHERE id = %s AND chat_id = %s', (value, chat_id))
                    row = cursor.fetchone()
                    if row:
                        found_film_id = value
                        found_kp_id = row.get('kp_id') if isinstance(row, dict) else row[1]
                        logger.info(f"[RATE INTERNAL] Найден film_id={found_film_id} из rating_messages для chat_id={chat_id}")
                        break
            elif isinstance(value, str) and value.startswith("kp_id:"):
                # Это kp_id - проверяем, есть ли такой фильм в базе для этого чата
                kp_id_candidate = value.split(":")[1]
                with db_lock:
                    cursor.execute('SELECT id FROM movies WHERE kp_id = %s AND chat_id = %s', (str(kp_id_candidate), chat_id))
                    row = cursor.fetchone()
                    if row:
                        found_film_id = row.get('id') if isinstance(row, dict) else row[0]
                        found_kp_id = kp_id_candidate
                        logger.info(f"[RATE INTERNAL] Найден kp_id={found_kp_id} из rating_messages для chat_id={chat_id}, film_id={found_film_id}")
                        break
        
        if found_film_id:
            film_id = found_film_id
            if found_kp_id:
                kp_id = found_kp_id
        else:
            logger.warning(f"[RATE INTERNAL] Не найдено подходящего film_id в rating_messages для chat_id={chat_id}")
    
    # Если film_id все еще не найден, пытаемся найти через kp_id из сообщения
    if not film_id:
        # Пытаемся извлечь kp_id из текста сообщения или ссылки
        text = message.text or ""
        if 'kinopoisk.ru' in text or 'kinopoisk.com' in text:
            kp_id = extract_kp_id_from_text(text)
        elif message.reply_to_message and message.reply_to_message.text:
            reply_text = message.reply_to_message.text
            if 'kinopoisk.ru' in reply_text or 'kinopoisk.com' in reply_text:
                kp_id = extract_kp_id_from_text(reply_text)
        
        if kp_id:
            link = f"https://www.kinopoisk.ru/film/{kp_id}/"
            info = extract_movie_info(link)
            if info:
                film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
                if was_inserted:
                    logger.info(f"[RATE INTERNAL] Фильм добавлен в базу при оценке: kp_id={kp_id}, film_id={film_id}")
            else:
                logger.warning(f"[RATE INTERNAL] Не удалось получить информацию о фильме для kp_id={kp_id}")
                bot.reply_to(message, "❌ Не удалось получить информацию о фильме для оценки.")
                return
    
    if film_id:
        try:
            with db_lock:
                # Проверяем, просмотрен ли фильм ДО сохранения оценки
                cursor.execute('SELECT watched FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                watched_row = cursor.fetchone()
                is_watched_before = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                is_watched_before = bool(is_watched_before) if is_watched_before is not None else False
                
                cursor.execute('''
                    INSERT INTO ratings (chat_id, film_id, user_id, rating, is_imported)
                    VALUES (%s, %s, %s, %s, FALSE)
                    ON CONFLICT (chat_id, film_id, user_id) DO UPDATE SET rating = EXCLUDED.rating, is_imported = FALSE
                ''', (chat_id, film_id, user_id, rating))
                conn.commit()
                
                cursor.execute('SELECT AVG(rating) FROM ratings WHERE chat_id = %s AND film_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (chat_id, film_id))
                avg_row = cursor.fetchone()
                avg = avg_row.get('avg') if isinstance(avg_row, dict) else (avg_row[0] if avg_row and len(avg_row) > 0 else None)
                
                # Получаем kp_id
                cursor.execute('SELECT kp_id FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                kp_row = cursor.fetchone()
                kp_id = kp_row.get('kp_id') if isinstance(kp_row, dict) else (kp_row[0] if kp_row else None)
                
                avg_str = f"{avg:.1f}" if avg else "—"
                
                # Если фильм не был просмотрен — отмечаем как просмотренный + кнопка назад
                if not is_watched_before:
                    cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    conn.commit()
                    logger.info(f"[RATE INTERNAL] Фильм {film_id} отмечен как просмотренный после оценки пользователем {user_id}")
                    
                    # Кнопка "Вернуться к описанию"
                    markup = InlineKeyboardMarkup()
                    if kp_id:
                        markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{int(kp_id)}"))
                    
                    reply_msg = bot.reply_to(
                        message,
                        f"Спасибо! Фильм отмечен как просмотренный, ваша оценка {rating}/10 сохранена.\nСредняя: {avg_str}/10",
                        reply_markup=markup
                    )
                    
                    if kp_id and reply_msg:
                        rating_messages[reply_msg.message_id] = film_id
                else:
                    bot.reply_to(message, f"✅ Оценка {rating}/10 сохранена!\nСредняя: {avg_str}/10")
                
                # Твой оригинальный блок обновления описания фильма — оставляю полностью
                if kp_id:
                    try:
                        from moviebot.states import bot_messages
                        film_message_id = None
                        for msg_id, link_value in bot_messages.items():
                            if link_value and kp_id in str(link_value):
                                film_message_id = msg_id
                                logger.info(f"[RATE INTERNAL] Найдено сообщение с описанием фильма: message_id={film_message_id}")
                                break
                        
                        if film_message_id:
                            from moviebot.bot.handlers.series import show_film_info_with_buttons
                            
                            with db_lock:
                                cursor.execute('''
                                    SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                                    FROM movies WHERE id = %s AND chat_id = %s
                                ''', (film_id, chat_id))
                                existing_row = cursor.fetchone()
                                existing = None
                                link = None
                                info = None
                                
                                if existing_row:
                                    if isinstance(existing_row, dict):
                                        film_id_db = existing_row.get('id')
                                        title = existing_row.get('title')
                                        watched = existing_row.get('watched')
                                        link = existing_row.get('link')
                                        year = existing_row.get('year')
                                        genres = existing_row.get('genres')
                                        description = existing_row.get('description')
                                        director = existing_row.get('director')
                                        actors = existing_row.get('actors')
                                        is_series = bool(existing_row.get('is_series', 0))
                                    else:
                                        film_id_db = existing_row[0]
                                        title = existing_row[1]
                                        watched = existing_row[2]
                                        link = existing_row[3]
                                        year = existing_row[4] if len(existing_row) > 4 else None
                                        genres = existing_row[5] if len(existing_row) > 5 else None
                                        description = existing_row[6] if len(existing_row) > 6 else None
                                        director = existing_row[7] if len(existing_row) > 7 else None
                                        actors = existing_row[8] if len(existing_row) > 8 else None
                                        is_series = bool(existing_row[9] if len(existing_row) > 9 else 0)
                                    
                                    existing = (film_id_db, title, watched)
                                    
                                    info = {
                                        'title': title,
                                        'year': year,
                                        'genres': genres,
                                        'description': description,
                                        'director': director,
                                        'actors': actors,
                                        'is_series': is_series
                                    }
                            
                            if not link:
                                link = f"https://www.kinopoisk.ru/film/{kp_id}/"
                            
                            if info and existing:
                                show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing, message_id=film_message_id)
                                logger.info(f"[RATE INTERNAL] Сообщение с описанием фильма обновлено из БД: message_id={film_message_id}")
                            else:
                                logger.warning(f"[RATE INTERNAL] Не удалось получить данные из БД, делаю API запрос")
                                from moviebot.api.kinopoisk_api import extract_movie_info
                                info = extract_movie_info(link)
                                if info:
                                    show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing, message_id=film_message_id)
                                    logger.info(f"[RATE INTERNAL] Сообщение с описанием фильма обновлено через API: message_id={film_message_id}")
                    except Exception as update_e:
                        logger.warning(f"[RATE INTERNAL] Не удалось обновить сообщение с описанием фильма: {update_e}", exc_info=True)
                
                # Отправляем похожие фильмы после оценки 10
                if rating == 10 and kp_id:
                    try:
                        from moviebot.utils.helpers import has_recommendations_access
                        if has_recommendations_access(chat_id, user_id):
                            from moviebot.api.kinopoisk_api import get_similars

                            similars = get_similars(kp_id)
                            if similars:
                                rec_text = "🔥 Поскольку вы поставили 10/10, вот похожие фильмы, которые могут понравиться:\n\n"
                                rec_markup = InlineKeyboardMarkup(row_width=1)

                                for film_id_sim, name, is_series_sim in similars:
                                    short_name = (name[:50] + '...') if len(name) > 50 else name
                                    button_text = f"{'📺' if is_series_sim else '🎬'} {short_name}"
                                    rec_markup.add(InlineKeyboardButton(button_text, callback_data=f"show_film_description:{film_id_sim}"))

                                rec_markup.add(InlineKeyboardButton("✅ Готово", callback_data="delete_this_message"))

                                bot.send_message(
                                    chat_id,
                                    rec_text,
                                    reply_markup=rec_markup,
                                    parse_mode='HTML'
                                )
                                logger.info(f"[RATE INTERNAL] Похожие фильмы отправлены после оценки 10 для kp_id={kp_id}")
                            else:
                                logger.info("[RATE INTERNAL] Похожих фильмов не найдено после оценки 10")
                        else:
                            logger.info("[RATE INTERNAL] Нет доступа к рекомендациям после оценки 10")
                    except Exception as rec_e:
                        logger.warning(f"[RATE INTERNAL] Ошибка при отправке похожих фильмов после оценки 10: {rec_e}", exc_info=True)
        except Exception as e:
            logger.error(f"[RATE INTERNAL] Ошибка при сохранении оценки: {e}", exc_info=True)
            bot.reply_to(message, "❌ Произошла ошибка при сохранении оценки.")
    else:
        logger.warning(f"[RATE INTERNAL] Не удалось найти film_id для сохранения оценки")
        bot.reply_to(message, "❌ Не удалось найти фильм для оценки. Убедитесь, что вы отвечаете на сообщение с фильмом.")


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
        
        # Парсим оценку
        try:
            rating = int(text)
            if not (1 <= rating <= 10):
                bot.reply_to(message, "❌ Оценка должна быть от 1 до 10")
                return
        except ValueError:
            bot.reply_to(message, "❌ Неверный формат оценки. Используйте число от 1 до 10")
            return
        
        # Обновляем оценку
        with db_lock:
            cursor.execute('''
                UPDATE ratings SET rating = %s, is_imported = FALSE
                WHERE chat_id = %s AND film_id = %s AND user_id = %s
            ''', (rating, chat_id, film_id, user_id))
            conn.commit()
            
            # Получаем информацию о фильме
            cursor.execute('SELECT title FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            film_row = cursor.fetchone()
            title = film_row.get('title') if isinstance(film_row, dict) else (film_row[0] if film_row else "Фильм")
        
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
