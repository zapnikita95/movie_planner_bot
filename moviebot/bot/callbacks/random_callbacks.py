from moviebot.bot.bot_init import bot
"""
Обработчики callback для рандома
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from moviebot.database.db_operations import get_user_films_count
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.handlers.text_messages import expect_text_from_user, user_search_state
from moviebot.states import user_random_state

from moviebot.utils.helpers import has_recommendations_access


logger = logging.getLogger(__name__)


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
                    bot.answer_callback_query(
                        call.id, 
                        "❌ Этот режим доступен только с подпиской на рекомендации. Используйте /payment для оформления подписки.", 
                        show_alert=True
                    )
                    logger.warning(f"[RANDOM CALLBACK] Access denied for mode {mode}, user_id={user_id}")
                    return
                
            # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←
            # НОВАЯ ПРОВЕРКА ПУСТОЙ БАЗЫ ДЛЯ РЕЖИМА database
            if mode == 'database':
                count = get_user_films_count(user_id)
                if count == 0:
                    markup = InlineKeyboardMarkup(row_width=1)
                    markup.add(
                        InlineKeyboardButton("🔍 Начать поиск фильмов", callback_data="start_search"),
                    )
                    markup.add(
                        InlineKeyboardButton("⬅️ Назад", callback_data="rand_mode:back")  # ← существующий callback
                    )

                    bot.edit_message_text(
                        chat_id=chat_id,
                        message_id=call.message.message_id,
                        text=(
                            "😔 <b>В вашей базе пока нет фильмов</b>\n\n"
                            "Чтобы использовать рандом по своей базе, добавьте хотя бы один фильм.\n\n"
                            "Что делаем сейчас?"
                        ),
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    bot.answer_callback_query(call.id)
                    logger.info(f"[RANDOM] Пустая база для user_id={user_id}, показываем предложение добавить фильмы")
                    return
            # ←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←←


            # Для режима my_votes проверяем наличие импортированных оценок
            if mode == 'my_votes':
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                
                try:
                    with db_lock:
                        cursor_local.execute("""
                            SELECT COUNT(*) 
                            FROM movies m
                            JOIN ratings r ON m.id = r.film_id AND m.chat_id = r.chat_id
                            WHERE m.chat_id = %s AND r.user_id = %s AND r.is_imported = TRUE
                        """, (chat_id, user_id))
                        imported_count = cursor_local.fetchone()
                        imported_ratings = imported_count.get('count') if isinstance(imported_count, dict) else (imported_count[0] if imported_count else 0)
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
                
                if imported_ratings == 0:
                    # Нет импортированных оценок - показываем сообщение с кнопкой на импорт
                    markup = InlineKeyboardMarkup()
                    markup.add(InlineKeyboardButton("📥 Импорт базы из Кинопоиска", callback_data="settings:import"))
                    markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="rand_mode:back"))
                    
                    bot.answer_callback_query(call.id)
                    bot.edit_message_text(
                        "📥 <b>Загрузите ваши оценки из базы Кинопоиска</b>\n\n"
                        "Для использования режима \"По моим оценкам\" необходимо импортировать ваши оценки с Кинопоиска.",
                        chat_id,
                        call.message.message_id,
                        reply_markup=markup,
                        parse_mode='HTML'
                    )
                    return
            
            if user_id not in user_random_state:
                logger.error(f"[RANDOM CALLBACK] State not found for user_id={user_id}")
                bot.answer_callback_query(call.id, "❌ Состояние не найдено", show_alert=True)
                return
            
            user_random_state[user_id]['mode'] = mode
            # Для режима kinopoisk начинаем с выбора типа контента
            if mode == 'kinopoisk':
                user_random_state[user_id]['step'] = 'content_type'
            else:
                user_random_state[user_id]['step'] = 'period'
            
            logger.info(f"[RANDOM CALLBACK] State updated: mode={mode}, step={user_random_state[user_id]['step']}")
            
            # Добавляем справку о режиме
            mode_descriptions = {
                'database': '🎲 <b>Рандом по своей базе</b>\n\nВыбираем случайный фильм из вашей базы по заданным фильтрам.',
                'kinopoisk': '🎬 <b>Рандом по кинопоиску</b>\n\nНайдите случайный фильм на Кинопоиске по заданным фильтрам.',
                'my_votes': '⭐ <b>По моим оценкам (9-10)</b>\n\nПолучите рекомендацию, основанную на ваших оценках на Кинопоиске.',
                'group_votes': '👥 <b>По оценкам в базе (9-10)</b>\n\nНа основании фильмов в вашей базе будет выбран случайный фильм на Кинопоиске, который может вам понравиться.\n\n💡 <i>Чем больше оценок в базе, тем больше будет вариантов фильмов и жанров.</i>'
            }
            mode_description = mode_descriptions.get(mode, '')
            
            # Шаг 1: Выбор периода - показываем только те периоды, где есть фильмы
            # Для режима kinopoisk тоже показываем периоды на основе фильмов в базе
            all_periods = ["До 1980", "1980–1990", "1990–2000", "2000–2010", "2010–2020", "2020–сейчас"]
            available_periods = []
            
            logger.info(f"[RANDOM CALLBACK] Checking available periods for mode={mode}")
            
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            
            try:
                with db_lock:
                    if mode == 'my_votes':
                        # Для режима "по моим оценкам" - получаем годы из импортированных фильмов с оценкой 9-10
                        # Используем UNION для объединения:
                        # 1. Годы из фильмов, которые уже в базе группы (film_id IS NOT NULL)
                        # 2. Годы из импортированных оценок без film_id (film_id IS NULL) - используем сохраненный year
                        cursor_local.execute("""
                            SELECT DISTINCT COALESCE(m.year, r.year) as year
                            FROM ratings r
                            LEFT JOIN movies m ON m.id = r.film_id AND m.chat_id = r.chat_id
                            WHERE r.chat_id = %s AND r.user_id = %s AND r.rating IN (9, 10) AND r.is_imported = TRUE
                            AND (m.year IS NOT NULL OR r.year IS NOT NULL)
                            ORDER BY year
                        """, (chat_id, user_id))
                        years_rows = cursor_local.fetchall()
                        years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row and (row.get('year') if isinstance(row, dict) else row[0])]
                        
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
                        # Для режима "По оценкам в базе" - получаем годы из фильмов со средней оценкой группы >= 7.5
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            WHERE m.chat_id = %s AND m.year IS NOT NULL
                            AND EXISTS (
                                SELECT 1 FROM ratings r 
                                WHERE r.film_id = m.id AND r.chat_id = m.chat_id AND (r.is_imported = FALSE OR r.is_imported IS NULL) 
                                GROUP BY r.film_id, r.chat_id 
                                HAVING AVG(r.rating) >= 7.5
                            )
                            ORDER BY m.year
                        """, (chat_id,))
                        years_rows = cursor_local.fetchall()
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
                    elif mode == 'kinopoisk':
                        # Для режима "Рандом по кинопоиску" - получаем годы из всех фильмов в базе
                        cursor_local.execute("""
                            SELECT DISTINCT m.year
                            FROM movies m
                            WHERE m.chat_id = %s AND m.year IS NOT NULL
                            ORDER BY m.year
                        """, (chat_id,))
                        years_rows = cursor_local.fetchall()
                        years = [row.get('year') if isinstance(row, dict) else row[0] for row in years_rows if row]
                        
                        logger.info(f"[RANDOM CALLBACK] Found {len(years)} years for kinopoisk mode")
                        
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
                            cursor_local.execute(query, tuple(params))
                            count_row = cursor_local.fetchone()
                            count = count_row.get('count') if isinstance(count_row, dict) else (count_row[0] if count_row else 0)
                            
                            if count > 0:
                                available_periods.append(period)
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
            logger.info(f"[RANDOM CALLBACK] Available periods: {available_periods}")
            
            user_random_state[user_id]['available_periods'] = available_periods
            
            # Для режима kinopoisk сначала показываем выбор типа контента
            if mode == 'kinopoisk':
                markup = InlineKeyboardMarkup(row_width=1)
                markup.add(InlineKeyboardButton("🎬 Фильм", callback_data="rand_content_type:FILM"))
                markup.add(InlineKeyboardButton("📺 Сериал", callback_data="rand_content_type:TV_SERIES"))
                markup.add(InlineKeyboardButton("🎬 Фильм и Сериал", callback_data="rand_content_type:ALL"))
                markup.add(InlineKeyboardButton("⬅️ Назад", callback_data="rand_mode:back"))
                
                bot.answer_callback_query(call.id)
                text = f"{mode_description}\n\n🎬 <b>Шаг 1/3: Выберите тип контента</b>"
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[RANDOM CALLBACK] ✅ Mode kinopoisk selected, moving to content type selection, user_id={user_id}")
                return
            
            # Для остальных режимов показываем выбор периодов
            markup = InlineKeyboardMarkup(row_width=1)
            if available_periods:
                for period in available_periods:
                    markup.add(InlineKeyboardButton(period, callback_data=f"rand_period:{period}"))
            markup.add(InlineKeyboardButton("Пропустить ➡️", callback_data="rand_period:skip"))
            markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="rand_mode:back"))
            
            bot.answer_callback_query(call.id)
            # Для режимов group_votes показываем Шаг 1/4 (изменилось), для остальных - Шаг 1/4
            if mode == 'group_votes':
                step_text = "🎲 <b>Шаг 1/4: Выберите период</b>"
            else:
                step_text = "🎲 <b>Шаг 1/4: Выберите период</b>"
            text = f"{mode_description}\n\n{step_text}\n\n(можно выбрать несколько или пропустить)"
            bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            logger.info(f"[RANDOM CALLBACK] ✅ Mode selected: {mode}, moving to period selection, user_id={user_id}")
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in random_mode_handler: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
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
                bot.answer_callback_query(call.id, message, show_alert=True)
            else:
                # Режим заблокирован по другим причинам (недостаточно оценок)
                bot.answer_callback_query(call.id, "🔒 Этот режим пока недоступен", show_alert=True)
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in random_mode_locked_handler: {e}", exc_info=True)
    
    logger.info("✅ Random callbacks registered")
    
    @bot.callback_query_handler(func=lambda call: call.data == "rand_mode:back")
    def handle_rand_mode_back(call):
        """Обработчик возврата к выбору режима рандома"""
        try:
            logger.info(f"[RANDOM CALLBACK] ===== MODE BACK: user_id={call.from_user.id}")
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Очищаем состояние (или сбрасываем шаг на mode)
            if user_id in user_random_state:
                user_random_state[user_id]['step'] = 'mode'
                user_random_state[user_id]['mode'] = None
                user_random_state[user_id]['periods'] = []
                user_random_state[user_id]['genres'] = []
                user_random_state[user_id]['directors'] = []
                user_random_state[user_id]['actors'] = []
            
            # Показываем выбор режима (используем код из random_start)
            from moviebot.utils.helpers import has_recommendations_access
            markup = InlineKeyboardMarkup(row_width=1)
            markup.add(InlineKeyboardButton("🎲 Рандом по своей базе", callback_data="rand_mode:database"))
            
            has_rec_access = has_recommendations_access(chat_id, user_id)
            
            if has_rec_access:
                markup.add(InlineKeyboardButton("🎬 Рандом по кинопоиску", callback_data="rand_mode:kinopoisk"))
                markup.add(InlineKeyboardButton("⭐ По оценкам в базе", callback_data="rand_mode:group_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 Рандом по кинопоиску", callback_data="rand_mode_locked:kinopoisk"))
                markup.add(InlineKeyboardButton("🔒 По оценкам в базе", callback_data="rand_mode_locked:group_votes"))
            
            if has_rec_access:
                markup.add(InlineKeyboardButton("⭐ По моим оценкам (9-10)", callback_data="rand_mode:my_votes"))
            else:
                markup.add(InlineKeyboardButton("🔒 По моим оценкам (9-10)", callback_data="rand_mode_locked:my_votes"))
            
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            try:
                bot.edit_message_text("🎲 <b>Выберите режим рандома:</b>", chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                logger.warning(f"[RANDOM MODE BACK] Edit failed, sending new message: {e}")
                bot.send_message(chat_id, "🎲 <b>Выберите режим рандома:</b>", reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[RANDOM CALLBACK] ❌ ERROR in handle_rand_mode_back: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка обработки")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "start_search")
    def handle_start_search_callback(call):
        """
        Запускает процесс поиска из экрана "пустая база" → 
        показывает выбор типа + устанавливает ожидание текста
        """
        try:
            bot.answer_callback_query(call.id)
            
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            is_private = call.message.chat.type == 'private'
            
            # Создаём кнопки выбора типа (как в /search без текста)
            markup = InlineKeyboardMarkup(row_width=2)
            markup.add(
                InlineKeyboardButton("🎬 Найти фильм", callback_data="search_type:film"),
                InlineKeyboardButton("📺 Найти сериал", callback_data="search_type:series")
            )
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
            prompt_text = "🔍 Укажите запрос для поиска в ответном сообщении, например: джон уик"
            
            # Отправляем новое сообщение (не редактируем старое о пустой базе — лучше не путать пользователя)
            sent_msg = bot.send_message(
                chat_id,
                prompt_text,
                reply_markup=markup
            )
            
            # Сохраняем состояние (по аналогии с handle_search)
            user_search_state[user_id] = {
                'chat_id': chat_id,
                'message_id': sent_msg.message_id,
                'search_type': 'mixed'  # по умолчанию mixed, пользователь может уточнить тип
            }
            logger.info(f"[START_SEARCH] Состояние поиска установлено: {user_search_state[user_id]}")
            
            # Устанавливаем ожидание текста — это самое важное!
            if is_private and sent_msg:
                expect_text_from_user(
                    user_id=user_id,
                    chat_id=chat_id,
                    expected_for='search',
                    message_id=sent_msg.message_id
                )
            
            logger.info(f"[START_SEARCH] Ожидание текста установлено для user_id={user_id}")
            
        except Exception as e:
            logger.error(f"[START_SEARCH] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Не получилось запустить поиск 😔", show_alert=True)
            except:
                pass