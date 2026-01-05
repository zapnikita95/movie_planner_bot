"""
Обработчики команд /stats, /total, /admin_stats
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_admin_statistics
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_stats_handlers(bot):
    """Регистрирует обработчики команд статистики"""
    
    @bot.message_handler(commands=['stats'])
    def stats_command(message):
        """Команда /stats - детальная статистика группы и участников"""
        # TODO: Извлечь из moviebot.py строки 8407-9153
        logger.info(f"[HANDLER] /stats вызван от {message.from_user.id}")
        try:
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/stats', message.chat.id)
            logger.info(f"Команда /stats от пользователя {message.from_user.id}, chat_id={message.chat.id}")
            chat_id = message.chat.id
            
            with db_lock:
                # Получаем всех участников из разных источников: stats, ratings, watched_movies, plans
                all_users = {}
                
                # Из stats (команды)
                cursor.execute('''
                    SELECT 
                        user_id,
                        username,
                        COUNT(*) as command_count,
                        MAX(timestamp) as last_activity
                    FROM stats
                    WHERE chat_id = %s AND user_id IS NOT NULL
                    GROUP BY user_id, username
                ''', (chat_id,))
                for row in cursor.fetchall():
                    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    username = row.get('username') if isinstance(row, dict) else row[1]
                    command_count = row.get('command_count') if isinstance(row, dict) else row[2]
                    last_activity = row.get('last_activity') if isinstance(row, dict) else row[3]
                    all_users[user_id] = {
                        'username': username,
                        'command_count': command_count,
                        'last_activity': last_activity
                    }
                
                # Из ratings (оценки)
                cursor.execute('''
                    SELECT DISTINCT user_id
                    FROM ratings
                    WHERE chat_id = %s AND user_id IS NOT NULL
                ''', (chat_id,))
                for row in cursor.fetchall():
                    user_id = row.get('user_id') if isinstance(row, dict) else row[0]
                    if user_id not in all_users:
                        all_users[user_id] = {
                            'username': None,
                            'command_count': 0,
                            'last_activity': None
                        }
                
                # TODO: Добавить остальную логику из moviebot.py строки 8579-9153
                # Это очень большая функция, нужно скопировать весь код
                
                # Временная заглушка
                text = f"📊 <b>Статистика группы</b>\n\n"
                text += f"Участников: {len(all_users)}\n\n"
                text += "<i>Полная статистика будет доступна после завершения рефакторинга</i>"
                
                bot.reply_to(message, text, parse_mode='HTML')
                logger.info(f"✅ Ответ на /stats отправлен пользователю {message.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /stats: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /stats")
            except:
                pass

    @bot.message_handler(commands=['total'])
    def total_stats(message):
        """Команда /total - статистика: фильмы, жанры, режиссёры, актёры и оценки"""
        # TODO: Извлечь из moviebot.py строки 9188-9387
        logger.info(f"[HANDLER] /total вызван от {message.from_user.id}")
        try:
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/total', message.chat.id)
            logger.info(f"Команда /total от пользователя {message.from_user.id}")
            chat_id = message.chat.id
            
            with db_lock:
                # Исключаем фильмы, добавленные только через импорт
                cursor.execute('''
                    SELECT COUNT(*) as count FROM movies m
                    WHERE m.chat_id = %s
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
                ''', (chat_id,))
                total_row = cursor.fetchone()
                total = total_row.get('count') if isinstance(total_row, dict) else (total_row[0] if total_row and len(total_row) > 0 else 0)
                
                cursor.execute('''
                    SELECT COUNT(*) as count FROM movies m
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
                ''', (chat_id,))
                watched_row = cursor.fetchone()
                watched = watched_row.get('count') if isinstance(watched_row, dict) else (watched_row[0] if watched_row and len(watched_row) > 0 else 0)
                unwatched = total - watched
                
                # Если нет данных, отправляем сообщение
                if total == 0:
                    bot.reply_to(message, "📊 Нет данных о вашей статистике.\n\nОцените первый фильм, чтобы статистика начала собираться.")
                    return
                
                # TODO: Добавить полную логику из moviebot.py строки 9353-9487
                # Это очень большая функция, нужно скопировать весь код
                
                # Временная заглушка
                text = f"📊 <b>Статистика кино-группы</b>\n\n"
                text += f"🎬 Всего фильмов: <b>{total}</b>\n"
                text += f"✅ Просмотрено: <b>{watched}</b>\n"
                text += f"⏳ Ждёт просмотра: <b>{unwatched}</b>\n"
                text += "\n<i>Полная статистика будет доступна после завершения рефакторинга</i>"
                
                bot.reply_to(message, text, parse_mode='HTML')
                logger.info(f"✅ Ответ на /total отправлен пользователю {message.from_user.id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /total: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /total")
            except:
                pass

    @bot.message_handler(commands=['admin_stats'])
    def admin_stats_command(message):
        """Команда /admin_stats - статистика для администратора"""
        # TODO: Извлечь из moviebot.py строки 8756-8842
        user_id = message.from_user.id
        
        # Проверяем, что это администратор
        if user_id != 301810276:
            bot.reply_to(message, "❌ Эта команда доступна только администратору.")
            return
        
        try:
            stats = get_admin_statistics()
            # TODO: Форматировать и отправить статистику
            bot.reply_to(message, f"📊 <b>Статистика администратора</b>\n\n<i>Полная статистика будет доступна после завершения рефакторинга</i>", parse_mode='HTML')
        except Exception as e:
            logger.error(f"❌ Ошибка в /admin_stats: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /admin_stats")
            except:
                pass
