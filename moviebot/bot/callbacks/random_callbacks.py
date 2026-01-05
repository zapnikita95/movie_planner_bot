"""
Обработчики callback для рандома
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot as bot_instance
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.states import user_random_state
from moviebot.utils.helpers import has_recommendations_access

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_random_callbacks(bot):
    """Регистрирует обработчики callback для рандома"""
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode:"))
    def random_mode_handler(call):
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
                logger.error(f"[RANDOM CALLBACK] State not found for user_id={user_id}")
                bot_instance.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
                return
            
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
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in random_mode_handler: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data.startswith("rand_mode_locked:"))
    def random_mode_locked_handler(call):
        """Обработчик заблокированных режимов рандомайзера"""
        try:
            logger.info(f"[RANDOM CALLBACK] Locked mode handler: data={call.data}, user_id={call.from_user.id}")
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            mode = call.data.split(":")[1]
            
            # Проверяем, заблокирован ли режим из-за отсутствия подписки
            has_rec_access = has_recommendations_access(chat_id, user_id)
            
            if not has_rec_access:
                # Режим заблокирован из-за отсутствия подписки
                mode_messages = {
                    'kinopoisk': 'Подключить расширенные рекомендации можно в /payment (💳 Оплата)',
                    'my_votes': 'Подключить расширенные рекомендации можно в /payment (💳 Оплата)',
                    'group_votes': 'Подключить расширенные рекомендации можно в /payment (💳 Оплата)'
                }
                message = mode_messages.get(mode, 'Подключить расширенные рекомендации можно в /payment (💳 Оплата)')
                bot_instance.answer_callback_query(call.id, message, show_alert=True)
            else:
                # Режим заблокирован по другим причинам (недостаточно оценок)
                bot_instance.answer_callback_query(call.id, "🔒 Этот режим пока недоступен", show_alert=True)
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in random_mode_locked_handler: {e}", exc_info=True)
    
    logger.info("✅ Random callbacks registered")

