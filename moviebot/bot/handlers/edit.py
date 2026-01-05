"""
Обработчики команды /edit - редактирование расписания и оценок
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request, get_user_timezone_or_default
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot as bot_instance
from moviebot.states import user_edit_state
from moviebot.utils.parsing import parse_session_time, extract_kp_id_from_text
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot_instance.message_handler(commands=['edit'])
def edit_command(message):
    """Команда /edit - редактирование расписания и оценок"""
    logger.info(f"[EDIT COMMAND] ===== ФУНКЦИЯ ВЫЗВАНА =====")
    logger.info(f"[EDIT COMMAND] /edit вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/edit', message.chat.id)
    
    user_id = message.from_user.id
    chat_id = message.chat.id
    
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📅 Изменить фильм в расписании", callback_data="edit:plan"))
    markup.add(InlineKeyboardButton("⭐ Изменить оценку", callback_data="edit:rating"))
    markup.add(InlineKeyboardButton("🗑️ Удалить оценку", callback_data="edit:delete_rating"))
    markup.add(InlineKeyboardButton("👁️ Удалить просмотр", callback_data="edit:delete_watched"))
    markup.add(InlineKeyboardButton("📅 Удалить задачу из планов", callback_data="edit:delete_plan"))
    markup.add(InlineKeyboardButton("🎬 Удалить фильм из базы", callback_data="edit:delete_movie"))
    
    help_text = (
        "✏️ <b>Что вы хотите изменить?</b>\n\n"
        "<b>📅 Изменить фильм в расписании</b> — изменить дату/время или переключить между 'дома' и 'в кино'\n"
        "<b>⭐ Изменить оценку</b> — изменить вашу оценку фильма\n\n"
        "<b>Остальные опции:</b> удаление оценок, просмотров, планов и фильмов"
    )
    
    try:
        bot_instance.reply_to(message, help_text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[EDIT COMMAND] ❌ Ошибка отправки меню: {e}", exc_info=True)


@bot_instance.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit:"))
def edit_action_callback(call):
    """Обработчик выбора действия в /edit"""
    logger.info(f"[EDIT ACTION] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot_instance.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        action = call.data.split(":")[1]
        
        logger.info(f"[EDIT ACTION] Действие: {action}, user_id={user_id}")
        
        if action == "plan":
            # Показываем список планов для редактирования
            with db_lock:
                cursor.execute('''
                    SELECT p.id, m.title, p.plan_type, p.plan_datetime
                    FROM plans p
                    JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                    WHERE p.chat_id = %s
                    ORDER BY p.plan_datetime
                    LIMIT 20
                ''', (chat_id,))
                plans = cursor.fetchall()
            
            if not plans:
                bot_instance.edit_message_text("Нет планов для редактирования.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for plan_row in plans:
                if isinstance(plan_row, dict):
                    plan_id = plan_row.get('id')
                    title = plan_row.get('title')
                    plan_type = plan_row.get('plan_type')
                    plan_dt = plan_row.get('plan_datetime')
                else:
                    plan_id = plan_row[0]
                    title = plan_row[1]
                    plan_type = plan_row[2]
                    plan_dt = plan_row[3]
                
                type_text = "🎦" if plan_type == 'cinema' else "🏠"
                if plan_dt:
                    if isinstance(plan_dt, datetime):
                        dt_str = plan_dt.strftime('%d.%m.%Y %H:%M')
                    else:
                        dt_str = str(plan_dt)[:16]
                else:
                    dt_str = "не указана"
                
                button_text = f"{type_text} {title} ({dt_str})"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_plan:{plan_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot_instance.edit_message_text("📅 <b>Выберите план для редактирования:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "rating":
            # Показываем список фильмов с оценками для изменения
            with db_lock:
                cursor.execute('''
                    SELECT m.id, m.title, m.year, r.rating
                    FROM movies m
                    JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    WHERE m.chat_id = %s AND r.user_id = %s
                    ORDER BY m.title
                    LIMIT 20
                ''', (chat_id, user_id))
                movies = cursor.fetchall()
            
            if not movies:
                bot_instance.edit_message_text("Нет фильмов с вашими оценками для изменения.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for movie_row in movies:
                if isinstance(movie_row, dict):
                    film_id = movie_row.get('id')
                    title = movie_row.get('title')
                    year = movie_row.get('year')
                    rating = movie_row.get('rating')
                else:
                    film_id = movie_row[0]
                    title = movie_row[1]
                    year = movie_row[2]
                    rating = movie_row[3]
                
                year_str = f" ({year})" if year else ""
                button_text = f"⭐ {title}{year_str} — {rating}/10"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_rating:{film_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot_instance.edit_message_text("⭐ <b>Выберите фильм для изменения оценки:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "cancel":
            # Очищаем состояние редактирования
            if user_id in user_edit_state:
                del user_edit_state[user_id]
            bot_instance.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
        
        else:
            logger.warning(f"[EDIT ACTION] Неизвестное действие: {action}")
            bot_instance.answer_callback_query(call.id, "❌ Неизвестное действие", show_alert=True)
    except Exception as e:
        logger.error(f"[EDIT ACTION] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def register_edit_handlers(bot):
    """Регистрирует обработчики команды /edit"""
    # Обработчики уже зарегистрированы через декораторы
    logger.info("Обработчики команды /edit зарегистрированы")

