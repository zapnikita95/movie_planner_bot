"""
Обработчики команды /rate
"""
import logging
import re
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.utils.parsing import extract_kp_id_from_text
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot as bot_instance

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_rate_handlers(bot):
    """Регистрирует обработчики команды /rate"""
    
    @bot.message_handler(commands=['rate'])
    def rate_movie(message):
        """Команда /rate - оценить просмотренные фильмы"""
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
                bot_instance.reply_to(message, "❌ Не удалось распознать kp_id. Используйте формат:\n<code>/rate 81682 10</code>\nили\n<code>/rate https://www.kinopoisk.ru/film/81682/ 10</code>", parse_mode='HTML')
                return
            
            # Парсим оценку
            try:
                rating = int(rating_str.strip())
                if not (1 <= rating <= 10):
                    bot_instance.reply_to(message, "❌ Оценка должна быть от 1 до 10")
                    return
            except ValueError:
                bot_instance.reply_to(message, "❌ Неверный формат оценки. Используйте число от 1 до 10")
                return
            
            # Ищем фильм в базе
            with db_lock:
                cursor.execute('''
                    SELECT id, title FROM movies
                    WHERE chat_id = %s AND kp_id = %s AND watched = 1
                ''', (chat_id, kp_id))
                film_row = cursor.fetchone()
                
                if not film_row:
                    bot_instance.reply_to(message, f"❌ Фильм с kp_id={kp_id} не найден в базе или не помечен как просмотренный")
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
                    bot_instance.reply_to(message, f"✅ Оценка обновлена!\n\n<b>{title}</b>\nСтарая оценка: {old_rating}/10\nНовая оценка: {rating}/10", parse_mode='HTML')
                    logger.info(f"[RATE] Пользователь {user_id} обновил оценку для фильма {kp_id} с {old_rating} на {rating}")
                else:
                    # Сохраняем новую оценку
                    cursor.execute('''
                        INSERT INTO ratings (chat_id, film_id, user_id, rating)
                        VALUES (%s, %s, %s, %s)
                    ''', (chat_id, film_id, user_id, rating))
                    conn.commit()
                    bot_instance.reply_to(message, f"✅ Оценка сохранена!\n\n<b>{title}</b>\nОценка: {rating}/10", parse_mode='HTML')
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
            bot_instance.reply_to(message, "✅ Все просмотренные фильмы уже оценены!")
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
                film_id = row[0]
                kp_id = row[1]
                title = row[2]
                year = row[3] if len(row) > 3 else '—'
            
            text += f"• <b>{title}</b> ({year})\n"
            # Добавляем кнопку с фильмом - при нажатии откроется описание фильма
            button_text = f"{title} ({year})"
            if len(button_text) > 50:
                button_text = button_text[:47] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"rate_from_list:{kp_id}"))
        
        text += "\n<i>Нажмите на фильм, чтобы открыть его описание и оценить</i>"
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
            bot_instance.reply_to(message, text, reply_markup=markup, parse_mode='HTML')

    @bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_rating:"))
    def handle_confirm_rating(call):
        """Обработчик подтверждения оценки"""
        # TODO: Извлечь из moviebot.py строки 7696-7749
        try:
            bot_instance.answer_callback_query(call.id)
            # TODO: Реализовать логику подтверждения оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_confirm_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("cancel_rating:"))
    def handle_cancel_rating(call):
        """Обработчик отмены оценки"""
        # TODO: Извлечь из moviebot.py строки 7750-7776
        try:
            bot_instance.answer_callback_query(call.id)
            # TODO: Реализовать логику отмены оценки
        except Exception as e:
            logger.error(f"[RATE] Ошибка в handle_cancel_rating: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("rate_from_list:"))
    def rate_from_list_callback(call):
        """Обработчик выбора фильма из списка /rate - открывает описание фильма"""
        try:
            bot_instance.answer_callback_query(call.id)
            kp_id = call.data.split(":")[1]
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[RATE FROM LIST] Пользователь {user_id} выбрал фильм kp_id={kp_id} из списка /rate")
            
            # Получаем информацию о фильме из базы
            with db_lock:
                cursor.execute('SELECT id, title, link, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
                row = cursor.fetchone()
            
            if not row:
                bot_instance.answer_callback_query(call.id, "❌ Фильм не найден в базе", show_alert=True)
                return
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
            link = row.get('link') if isinstance(row, dict) else row[2]
            watched = row.get('watched') if isinstance(row, dict) else row[3]
            
            # Получаем информацию о фильме через API
            from moviebot.api.kinopoisk_api import extract_movie_info
            info = extract_movie_info(link)
            
            if not info:
                bot_instance.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                return
            
            # Формируем existing для передачи в show_film_info_with_buttons
            existing = (film_id, title, watched)
            
            # Показываем описание фильма со всеми базовыми кнопками
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing)
            
        except Exception as e:
            logger.error(f"[RATE FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
