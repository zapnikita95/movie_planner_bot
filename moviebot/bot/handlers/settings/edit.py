from moviebot.bot.bot_init import bot
"""
Обработчики команды /edit - редактирование расписания и оценок
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
import pytz
import json

from moviebot.database.db_operations import log_request, get_user_timezone_or_default
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.states import user_edit_state
from moviebot.utils.parsing import parse_session_time, extract_kp_id_from_text

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


@bot.message_handler(commands=['edit'])
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
    markup.add(InlineKeyboardButton("📅 Удалить задачу из планов", callback_data="edit:delete_plan"))
    markup.add(InlineKeyboardButton("🎬 Удалить фильм из базы", callback_data="edit:delete_movie"))
    
    # Проверяем, вызван ли из настроек
    from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
    if from_settings:
        markup.add(InlineKeyboardButton("◀️ Назад к настройкам", callback_data="settings:back"))
    else:
        # Если from_settings не установлен, устанавливаем False
        if user_id not in user_edit_state:
            user_edit_state[user_id] = {}
        user_edit_state[user_id]['from_settings'] = False
    
    help_text = (
        "✏️ <b>Что вы хотите изменить?</b>\n\n"
        "<b>📅 Изменить фильм в расписании</b> — изменить дату/время или переключить между 'дома' и 'в кино'\n"
        "<b>⭐ Изменить оценку</b> — изменить вашу оценку фильма\n\n"
        "<b>Остальные опции:</b> удаление оценок, просмотров, планов и фильмов"
    )
    
    try:
        bot.reply_to(message, help_text, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[EDIT COMMAND] ❌ Ошибка отправки меню: {e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit:"))
def edit_action_callback(call):
    """Обработчик выбора действия в /edit"""
    logger.info(f"[EDIT ACTION] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
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
                bot.edit_message_text("Нет планов для редактирования.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for plan_row in plans:
                plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                title = plan_row.get('title') if isinstance(plan_row, dict) else plan_row[1]
                plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[2]
                plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[3]
                
                type_text = "🎦" if plan_type == 'cinema' else "🏠"
                dt_str = plan_dt.strftime('%d.%m.%Y %H:%M') if isinstance(plan_dt, datetime) else "не указана"
                
                button_text = f"{type_text} {title} ({dt_str})"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_plan:{plan_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot.edit_message_text("📅 <b>Выберите план для редактирования:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
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
                bot.edit_message_text("Нет фильмов с вашими оценками для изменения.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for movie_row in movies:
                film_id = movie_row.get('id') if isinstance(movie_row, dict) else movie_row[0]
                title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[1]
                year = movie_row.get('year') if isinstance(movie_row, dict) else movie_row[2]
                rating = movie_row.get('rating') if isinstance(movie_row, dict) else movie_row[3]
                
                year_str = f" ({year})" if year else ""
                button_text = f"⭐ {title}{year_str} — {rating}/10"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_rating:{film_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot.edit_message_text("⭐ <b>Выберите фильм для изменения оценки:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "delete_rating":
            # Показываем список фильмов с оценками для удаления
            with db_lock:
                cursor.execute('''
                    SELECT m.id, m.title, m.year, r.rating
                    FROM movies m
                    JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                    WHERE m.chat_id = %s AND r.user_id = %s AND (r.is_imported = FALSE OR r.is_imported IS NULL)
                    ORDER BY m.title
                    LIMIT 20
                ''', (chat_id, user_id))
                movies = cursor.fetchall()
            
            if not movies:
                bot.edit_message_text("Нет фильмов с вашими оценками для удаления.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for movie_row in movies:
                film_id = movie_row.get('id') if isinstance(movie_row, dict) else movie_row[0]
                title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[1]
                year = movie_row.get('year') if isinstance(movie_row, dict) else movie_row[2]
                rating = movie_row.get('rating') if isinstance(movie_row, dict) else movie_row[3]
                
                year_str = f" ({year})" if year else ""
                button_text = f"🗑️ {title}{year_str} — {rating}/10"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_delete_rating:{film_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot.edit_message_text("🗑️ <b>Выберите фильм для удаления оценки:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "delete_plan":
            # Показываем список планов для удаления
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
                bot.edit_message_text("Нет планов для удаления.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for plan_row in plans:
                plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                title = plan_row.get('title') if isinstance(plan_row, dict) else plan_row[1]
                plan_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[2]
                plan_dt = plan_row.get('plan_datetime') if isinstance(plan_row, dict) else plan_row[3]
                
                type_text = "🎦" if plan_type == 'cinema' else "🏠"
                dt_str = plan_dt.strftime('%d.%m.%Y %H:%M') if isinstance(plan_dt, datetime) else "не указана"
                
                button_text = f"🗑️ {type_text} {title} ({dt_str})"
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_delete_plan:{plan_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot.edit_message_text("🗑️ <b>Выберите план для удаления:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "delete_movie":
            # Показываем список фильмов для удаления
            with db_lock:
                cursor.execute('''
                    SELECT id, title, year
                    FROM movies
                    WHERE chat_id = %s
                    ORDER BY title
                    LIMIT 30
                ''', (chat_id,))
                movies = cursor.fetchall()
            
            if not movies:
                bot.edit_message_text("Нет фильмов в базе для удаления.", chat_id, call.message.message_id)
                return
            
            markup = InlineKeyboardMarkup(row_width=1)
            for movie_row in movies:
                film_id = movie_row.get('id') if isinstance(movie_row, dict) else movie_row[0]
                title = movie_row.get('title') if isinstance(movie_row, dict) else movie_row[1]
                year = movie_row.get('year') if isinstance(movie_row, dict) else movie_row[2]
                
                year_str = f" ({year})" if year else ""
                button_text = f"🗑️ {title}{year_str}"
                if len(button_text) > 60:
                    button_text = button_text[:57] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"edit_delete_movie:{film_id}"))
            
            markup.add(InlineKeyboardButton("❌ Отмена", callback_data="edit:cancel"))
            bot.edit_message_text("🗑️ <b>Выберите фильм для удаления из базы:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
        
        elif action == "cancel":
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id)
            
            # Проверяем, откуда пришли (из настроек или из обычного редактирования)
            from_settings = False
            kp_id = None
            if user_id in user_edit_state:
                state = user_edit_state[user_id]
                from_settings = state.get('from_settings', False)
                kp_id = state.get('kp_id')
                del user_edit_state[user_id]
            
            # Если пришли из настроек, возвращаемся в настройки
            if from_settings:
                try:
                    from moviebot.bot.handlers.settings import settings_command
                    # Создаем fake message для settings_command
                    class FakeMessage:
                        def __init__(self, call):
                            self.from_user = call.from_user
                            self.chat = call.message.chat
                            self.text = '/settings'
                    
                    fake_message = FakeMessage(call)
                    settings_command(fake_message)
                    # Удаляем старое сообщение
                    try:
                        bot.delete_message(chat_id, call.message.message_id)
                    except:
                        pass
                except Exception as e:
                    logger.error(f"[EDIT CANCEL] Ошибка при возврате в настройки: {e}", exc_info=True)
                    bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
                return
            
            # Если есть kp_id и не из настроек, возвращаемся к описанию фильма/сериала
            if kp_id:
                try:
                    from moviebot.bot.handlers.series import show_film_info_with_buttons
                    from moviebot.api.kinopoisk_api import extract_movie_info
                    
                    # Получаем информацию о фильме из базы
                    with db_lock:
                        cursor.execute('SELECT id, title, watched, link FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                        row = cursor.fetchone()
                    
                    if row:
                        film_id = row.get('id') if isinstance(row, dict) else row[0]
                        title = row.get('title') if isinstance(row, dict) else row[1]
                        watched = row.get('watched') if isinstance(row, dict) else row[2]
                        link = row.get('link') if isinstance(row, dict) else row[3]
                        
                        existing = (film_id, title, watched)
                        info = extract_movie_info(link)
                        
                        if info:
                            show_film_info_with_buttons(
                                chat_id=chat_id,
                                user_id=user_id,
                                info=info,
                                link=link,
                                kp_id=kp_id,
                                existing=existing,
                                message_id=call.message.message_id
                            )
                            return
                    
                    bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
                except Exception as e:
                    logger.error(f"[EDIT CANCEL] Ошибка при возврате к описанию: {e}", exc_info=True)
                    bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
            else:
                bot.edit_message_text("❌ Операция отменена.", chat_id, call.message.message_id)
        
        else:
            logger.warning(f"[EDIT ACTION] Неизвестное действие: {action}")
            bot.answer_callback_query(call.id, "❌ Неизвестное действие", show_alert=True)
    except Exception as e:
        logger.error(f"[EDIT ACTION] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan_datetime:"))
def edit_plan_datetime_callback(call):
    """Обработчик изменения даты/времени плана"""
    logger.info(f"[EDIT PLAN DATETIME] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        # Сохраняем from_settings, если он был установлен
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        user_edit_state[user_id] = {
            'action': 'edit_plan_datetime',
            'plan_id': plan_id,
            'prompt_message_id': call.message.message_id,
            'from_settings': from_settings
        }
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"edit_plan:{plan_id}"))
        
        bot.edit_message_text(
            "📅 <b>Введите новую дату и время:</b>\n\n"
            "Формат:\n"
            "• 15 января 10:30\n"
            "• 17.01 15:20\n"
            "• 10.05.2025 21:40\n"
            "• завтра\n"
            "• в субботу 15:00",
            chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
        logger.info(f"[EDIT PLAN DATETIME] Состояние установлено для плана {plan_id}")
    except Exception as e:
        logger.error(f"[EDIT PLAN DATETIME] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan_streaming:"))
def edit_plan_streaming_callback(call):
    """Обработчик изменения онлайн-кинотеатра для домашнего плана"""
    logger.info(f"[EDIT PLAN STREAMING] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        with db_lock:
            cursor.execute('''
                SELECT p.ticket_file_id, m.kp_id, p.streaming_service
                FROM plans p
                JOIN movies m ON p.film_id = m.id AND p.chat_id = m.chat_id
                WHERE p.id = %s AND p.chat_id = %s
            ''', (plan_id, chat_id))
            plan_row = cursor.fetchone()
        
        if not plan_row:
            bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
            return
        
        sources_json = plan_row.get('ticket_file_id') if isinstance(plan_row, dict) else plan_row[0]
        kp_id = plan_row.get('kp_id') if isinstance(plan_row, dict) else plan_row[1]
        current_service = plan_row.get('streaming_service') if isinstance(plan_row, dict) else plan_row[2]
        
        sources_dict = {}
        if sources_json:
            try:
                sources_dict = json.loads(sources_json)
            except:
                pass
        
        if not sources_dict and kp_id:
            from moviebot.api.kinopoisk_api import get_external_sources
            sources = get_external_sources(kp_id)
            if sources:
                sources_dict = {platform: url for platform, url in sources[:6]}
                sources_json = json.dumps(sources_dict, ensure_ascii=False)
                with db_lock:
                    cursor.execute('UPDATE plans SET ticket_file_id = %s WHERE id = %s', (sources_json, plan_id))
                    conn.commit()
        
        if not sources_dict:
            bot.answer_callback_query(call.id, "❌ Онлайн-кинотеатры не найдены", show_alert=True)
            return
        
        markup = InlineKeyboardMarkup(row_width=2)
        for platform, url in sources_dict.items():
            button_text = f"✅ {platform}" if platform == current_service else platform
            markup.add(InlineKeyboardButton(button_text, callback_data=f"streaming_select:{plan_id}:{platform}"))
        
        if not current_service:
            markup.add(InlineKeyboardButton("✅ Завершить", callback_data=f"streaming_done:{plan_id}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"edit_plan:{plan_id}"))
        
        text = "📺 <b>Выберите онлайн-кинотеатр:</b>"
        if current_service:
            text += f"\n\n✅ Текущий: <b>{current_service}</b>"
        
        bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
    except Exception as e:
        logger.error(f"[EDIT PLAN STREAMING] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan_ticket:"))
def edit_plan_ticket_callback(call):
    """Обработчик загрузки билетов через /edit"""
    logger.info(f"[EDIT PLAN TICKET] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        from moviebot.states import user_ticket_state
        user_ticket_state[user_id] = {
            'step': 'waiting_ticket_file',
            'plan_id': plan_id,
            'chat_id': chat_id
        }
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="ticket:cancel"))
        
        bot.edit_message_text(
            "🎟️ <b>Пришлите билеты скриншотом или вложением</b>\n\n"
            "Отправьте фото или файл с билетами в следующем сообщении.",
            chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"[EDIT PLAN TICKET] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_plan_switch:"))
def edit_plan_switch_callback(call):
    """Обработчик переключения типа плана (дома <-> в кино)"""
    logger.info(f"[EDIT PLAN SWITCH] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        with db_lock:
            cursor.execute('SELECT plan_type FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            plan_row = cursor.fetchone()
            
            if not plan_row:
                bot.answer_callback_query(call.id, "❌ План не найден", show_alert=True)
                return
            
            current_type = plan_row.get('plan_type') if isinstance(plan_row, dict) else plan_row[0]
            new_type = 'cinema' if current_type == 'home' else 'home'
            
            cursor.execute('UPDATE plans SET plan_type = %s WHERE id = %s', (new_type, plan_id))
            conn.commit()
        
        type_text = "в кино" if new_type == 'cinema' else "дома"
        bot.edit_message_text(
            f"✅ Тип плана изменен на: <b>{type_text}</b>",
            chat_id, call.message.message_id, parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"[EDIT PLAN SWITCH] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_rating:"))
def edit_rating_callback(call):
    """Обработчик изменения оценки"""
    logger.info(f"[EDIT RATING] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        film_id = int(call.data.split(":")[1])
        
        # Сохраняем from_settings, если он был установлен
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        user_edit_state[user_id] = {
            'action': 'edit_rating',
            'film_id': film_id,
            'from_settings': from_settings
        }
        
        bot.edit_message_text(
            "⭐ <b>Введите новую оценку (1-10):</b>\n\n"
            "Ответьте на это сообщение числом от 1 до 10.",
            chat_id, call.message.message_id, parse_mode='HTML'
        )
    except Exception as e:
        logger.error(f"[EDIT RATING] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_delete_rating:"))
def edit_delete_rating_callback(call):
    """Обработчик удаления оценки"""
    logger.info(f"[EDIT DELETE RATING] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        film_id = int(call.data.split(":")[1])
        
        with db_lock:
            cursor.execute('DELETE FROM ratings WHERE film_id = %s AND chat_id = %s AND user_id = %s AND (is_imported = FALSE OR is_imported IS NULL)', (film_id, chat_id, user_id))
            conn.commit()
        
        bot.edit_message_text("✅ Оценка удалена.", chat_id, call.message.message_id)
        
        # Проверяем, нужно ли вернуться в настройки
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        if from_settings:
            try:
                from moviebot.bot.handlers.settings import settings_command
                class FakeMessage:
                    def __init__(self, call):
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.text = '/settings'
                
                fake_message = FakeMessage(call)
                settings_command(fake_message)
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
            except Exception as e:
                logger.error(f"[EDIT DELETE RATING] Ошибка при возврате в настройки: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[EDIT DELETE RATING] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_delete_plan:"))
def edit_delete_plan_callback(call):
    """Обработчик удаления плана"""
    logger.info(f"[EDIT DELETE PLAN] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_id = int(call.data.split(":")[1])
        
        with db_lock:
            cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s', (plan_id, chat_id))
            conn.commit()
        
        bot.edit_message_text("✅ План удален из расписания.", chat_id, call.message.message_id)
        
        # Проверяем, нужно ли вернуться в настройки
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        if from_settings:
            try:
                from moviebot.bot.handlers.settings import settings_command
                class FakeMessage:
                    def __init__(self, call):
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.text = '/settings'
                
                fake_message = FakeMessage(call)
                settings_command(fake_message)
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
            except Exception as e:
                logger.error(f"[EDIT DELETE PLAN] Ошибка при возврате в настройки: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[EDIT DELETE PLAN] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("edit_delete_movie:"))
def edit_delete_movie_callback(call):
    """Обработчик удаления фильма из базы"""
    logger.info(f"[EDIT DELETE MOVIE] ===== START: callback_id={call.id}, callback_data={call.data}, user_id={call.from_user.id}")
    try:
        bot.answer_callback_query(call.id)
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        film_id = int(call.data.split(":")[1])
        
        with db_lock:
            # Удаляем фильм и все связанные данные
            cursor.execute('DELETE FROM ratings WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
            cursor.execute('DELETE FROM plans WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
            cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
        
        bot.edit_message_text("✅ Фильм удален из базы.", chat_id, call.message.message_id)
        
        # Проверяем, нужно ли вернуться в настройки
        from_settings = user_edit_state.get(user_id, {}).get('from_settings', False)
        if from_settings:
            try:
                from moviebot.bot.handlers.settings import settings_command
                class FakeMessage:
                    def __init__(self, call):
                        self.from_user = call.from_user
                        self.chat = call.message.chat
                        self.text = '/settings'
                
                fake_message = FakeMessage(call)
                settings_command(fake_message)
                try:
                    bot.delete_message(chat_id, call.message.message_id)
                except:
                    pass
            except Exception as e:
                logger.error(f"[EDIT DELETE MOVIE] Ошибка при возврате в настройки: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[EDIT DELETE MOVIE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def register_edit_handlers(bot):
    """Регистрирует обработчики команды /edit"""
    logger.info("Обработчики команды /edit зарегистрированы")