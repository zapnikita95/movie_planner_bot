from moviebot.bot.bot_init import bot
"""
Обработчики команды /list
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.database.db_operations import log_request

from moviebot.states import user_list_state, list_messages, user_view_film_state, user_plan_state, user_mark_watched_state

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.api.kinopoisk_api import extract_movie_info


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_list_handlers(bot):
    """Регистрирует обработчики команды /list"""
    
    @bot.message_handler(commands=['list'], func=lambda m: not m.reply_to_message)
    def list_movies(message):
        """Команда /list - только если это чистая команда без реплая"""
        logger.info(f"[HANDLER] /list вызван от {message.from_user.id}")
        try:
            username = message.from_user.username or f"user_{message.from_user.id}"
            log_request(message.from_user.id, username, '/list', message.chat.id)
            logger.info(f"Команда /list от пользователя {message.from_user.id}")
            chat_id = message.chat.id
            user_id = message.from_user.id
            
            show_list_page(bot, chat_id, user_id, page=1)
            logger.info(f"✅ Ответ на /list отправлен пользователю {user_id}")
        except Exception as e:
            logger.error(f"❌ Ошибка в /list: {e}", exc_info=True)
            try:
                bot.reply_to(message, "Произошла ошибка при обработке команды /list")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data.startswith("list_page:"))
    def handle_list_page(call):
        """Обработчик переключения страниц в /list"""
        user_id = call.from_user.id
        
        # Проверяем, не устарел ли callback query
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id)
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[LIST PAGE] Callback query устарел, ПРОПУСКАЕМ: {answer_error}")
        
        if callback_is_old:
            return
        
        try:
            page = int(call.data.split(":")[1])
            
            state = user_list_state.get(user_id)
            if not state:
                try:
                    bot.answer_callback_query(call.id, "Сессия устарела. Используйте /list заново")
                except:
                    pass
                return
            
            chat_id = state['chat_id']
            show_list_page(bot, chat_id, user_id, page, call.message.message_id)
        except Exception as e:
            logger.error(f"[LIST] Ошибка в handle_list_page: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка переключения страницы")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "noop")
    def handle_noop(call):
        """Обработчик для неактивных кнопок (noop)"""
        try:
            bot.answer_callback_query(call.id)
        except Exception:
            # Игнорируем ошибки устаревших callback queries для noop
            pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "plan_from_list")
    def plan_from_list_callback(call):
        """Обработчик кнопки 'Запланировать просмотр' из /list"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # КРИТИЧЕСКИ ВАЖНО: Проверяем, не устарел ли callback query ДО начала операций
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[PLAN FROM LIST] Callback query устарел, ПРОПУСКАЕМ обработку: {answer_error}")
            else:
                logger.error(f"[PLAN FROM LIST] Ошибка answer_callback_query: {answer_error}", exc_info=True)
        
        # Если callback устарел - СРАЗУ выходим
        if callback_is_old:
            logger.info(f"[PLAN FROM LIST] ⚠️ Пропущен устаревший callback, выходим БЕЗ обработки")
            return
        
        try:
            logger.info(f"[PLAN FROM LIST] Пользователь {user_id} хочет запланировать фильм из /list")
            
            # Устанавливаем состояние для планирования
            user_plan_state[user_id] = {
                'step': 1,
                'chat_id': chat_id
            }
            
            prompt_msg = bot.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
            # Сохраняем message_id промпта в состояние
            user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[PLAN FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[PLAN FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "view_film_from_list")
    def view_film_from_list_callback(call):
        """Обработчик кнопки 'Перейти к описанию' из /list"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # КРИТИЧЕСКИ ВАЖНО: Проверяем, не устарел ли callback query ДО начала операций
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[VIEW FILM FROM LIST] Callback query устарел, ПРОПУСКАЕМ обработку: {answer_error}")
            else:
                logger.error(f"[VIEW FILM FROM LIST] Ошибка answer_callback_query: {answer_error}", exc_info=True)
        
        # Если callback устарел - СРАЗУ выходим
        if callback_is_old:
            logger.info(f"[VIEW FILM FROM LIST] ⚠️ Пропущен устаревший callback, выходим БЕЗ обработки")
            return
        
        try:
            logger.info(f"[VIEW FILM FROM LIST] Пользователь {user_id} хочет посмотреть страницу фильма из /list")
            
            # Устанавливаем состояние для просмотра фильма
            user_view_film_state[user_id] = {
                'chat_id': chat_id
            }
            
            prompt_msg = bot.send_message(chat_id, "Пришлите в ответном сообщении ссылку или ID фильма, чье описание хотите посмотреть")
            # Сохраняем message_id промпта в состояние
            user_view_film_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[VIEW FILM FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[VIEW FILM FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "mark_watched_from_list")
    def mark_watched_from_list_callback(call):
        """Обработчик кнопки 'Отметить просмотренным' из /list"""
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        
        # КРИТИЧЕСКИ ВАЖНО: Проверяем, не устарел ли callback query ДО начала операций
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id)
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[MARK WATCHED FROM LIST] Callback query устарел, ПРОПУСКАЕМ обработку: {answer_error}")
            else:
                logger.error(f"[MARK WATCHED FROM LIST] Ошибка answer_callback_query: {answer_error}", exc_info=True)
        
        # Если callback устарел - СРАЗУ выходим
        if callback_is_old:
            logger.info(f"[MARK WATCHED FROM LIST] ⚠️ Пропущен устаревший callback, выходим БЕЗ обработки")
            return
        
        try:
            logger.info(f"[MARK WATCHED FROM LIST] Пользователь {user_id} хочет отметить фильм просмотренным из /list")
            
            # Устанавливаем состояние для отметки просмотренным
            user_mark_watched_state[user_id] = {
                'chat_id': chat_id
            }
            
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад к списку", callback_data="back_to_list"))
            
            prompt_msg = bot.send_message(
                chat_id, 
                "👁️ Отметить просмотренным\n\nПришлите ID фильма из списка или ссылку на фильм, который хотите отметить просмотренным. Фильм автоматически отметится просмотренным.",
                reply_markup=markup
            )
            # Сохраняем message_id промпта в состояние
            user_mark_watched_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[MARK WATCHED FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[MARK WATCHED FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "back_to_list")
    def back_to_list_callback(call):
        """Обработчик кнопки 'Назад к списку'"""
        try:
            bot.answer_callback_query(call.id)
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            # Очищаем состояние отметки просмотренным
            if user_id in user_mark_watched_state:
                del user_mark_watched_state[user_id]
            
            # Получаем текущую страницу из состояния
            state = user_list_state.get(user_id)
            if state:
                page = state.get('page', 1)
                show_list_page(bot, chat_id, user_id, page, call.message.message_id)
            else:
                # Если состояния нет, показываем первую страницу
                show_list_page(bot, chat_id, user_id, 1, call.message.message_id)
        except Exception as e:
            logger.error(f"[BACK TO LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
            except:
                pass


def show_list_page(bot, chat_id, user_id, page=1, message_id=None):
    """Показывает страницу списка фильмов"""
    # ВАЖНО: Используем локальные соединения вместо глобальных
    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        MOVIES_PER_PAGE = 15
        
        with db_lock:
            # Получаем все непросмотренные фильмы, отсортированные по алфавиту
            cursor_local.execute('''
                SELECT DISTINCT m.id, m.kp_id, m.title, m.year, m.genres, m.link 
                FROM movies m
                WHERE m.chat_id = %s 
                  AND m.watched = 0
                ORDER BY m.title
            ''', (chat_id,))
            rows = cursor_local.fetchall()
        
        if not rows:
            text = "⏳ Нет непросмотренных фильмов!"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
            if message_id:
                try:
                    bot.edit_message_text(text, chat_id, message_id, reply_markup=markup)
                except:
                    bot.send_message(chat_id, text, reply_markup=markup)
            else:
                bot.send_message(chat_id, text, reply_markup=markup)
            logger.info(f"✅ Ответ на /list отправлен пользователю {user_id}: нет фильмов")
            return
        else:
            total_movies = len(rows)
            total_pages = (total_movies + MOVIES_PER_PAGE - 1) // MOVIES_PER_PAGE
            page = max(1, min(page, total_pages))
            
            # Вычисляем диапазон фильмов для текущей страницы
            start_idx = (page - 1) * MOVIES_PER_PAGE
            end_idx = min(start_idx + MOVIES_PER_PAGE, total_movies)
            page_movies = rows[start_idx:end_idx]
            
            # Формируем текст страницы
            text = f"⏳ Непросмотренные фильмы (страница {page}/{total_pages}):\n\n"
            for row in page_movies:
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                kp_id = row.get('kp_id') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                title = row.get('title') if isinstance(row, dict) else row[2]
                year = row.get('year') if isinstance(row, dict) else (row[3] if len(row) > 3 else '—')
                genres = row.get('genres') if isinstance(row, dict) else (row[4] if len(row) > 4 else None)
                link = row.get('link') if isinstance(row, dict) else (row[5] if len(row) > 5 else '')
                
                # Извлекаем первый жанр
                first_genre = None
                if genres and genres != '—' and genres.strip():
                    genres_list = [g.strip() for g in genres.split(',')]
                    if genres_list:
                        first_genre = genres_list[0]
                
                # Используем kp_id если есть, иначе film_id
                movie_id = kp_id or film_id
                genre_str = f" • {first_genre}" if first_genre else ""
                # Форматируем год: показываем только если он есть и не None
                year_str = f" ({year})" if year and year != '—' and str(year).lower() != 'none' else ""
                text += f"• <b>{title}</b>{year_str}{genre_str} [ID: {movie_id}]\n<a href='{link}'>{link}</a>\n\n"
            
            # Создаем кнопки пагинации
            markup = InlineKeyboardMarkup()
            
            # Пагинация (только если больше одной страницы)
            if total_pages > 1:
                pagination_buttons = []
                
                # Если страниц немного (<= 20), показываем все
                if total_pages <= 20:
                    for p in range(1, total_pages + 1):
                        label = f"•{p}" if p == page else str(p)
                        pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"list_page:{p}"))
                    # Разбиваем кнопки на строки по 10 штук
                    for i in range(0, len(pagination_buttons), 10):
                        markup.row(*pagination_buttons[i:i+10])
                else:
                    # Для большого количества страниц используем умную пагинацию
                    start_page = max(1, page - 2)
                    end_page = min(total_pages, page + 2)
                    
                    # Если текущая страница далеко от начала, показываем первую страницу и "..."
                    if start_page > 2:
                        pagination_buttons.append(InlineKeyboardButton("1", callback_data="list_page:1"))
                        pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                    elif start_page == 2:
                        pagination_buttons.append(InlineKeyboardButton("1", callback_data="list_page:1"))
                    
                    # Добавляем страницы вокруг текущей
                    for p in range(start_page, end_page + 1):
                        label = f"•{p}" if p == page else str(p)
                        pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"list_page:{p}"))
                    
                    # Если текущая страница далеко от конца, показываем "..." и последнюю страницу
                    if end_page < total_pages - 1:
                        pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                        pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"list_page:{total_pages}"))
                    elif end_page < total_pages:
                        pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"list_page:{total_pages}"))
                    
                    # Разбиваем на строки по 10 кнопок
                    for i in range(0, len(pagination_buttons), 10):
                        markup.row(*pagination_buttons[i:i+10])
                
                # Добавляем кнопки навигации (без кнопки "Страница X/Y")
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"list_page:{page-1}"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"list_page:{page+1}"))
                if nav_buttons:
                    markup.row(*nav_buttons)
            
            # Добавляем кнопки действий (каждая в отдельном ряду - большие кнопки)
            markup.add(InlineKeyboardButton("📖 Перейти к описанию", callback_data="view_film_from_list"))
            markup.add(InlineKeyboardButton("📅 Запланировать просмотр", callback_data="plan_from_list"))
            markup.add(InlineKeyboardButton("👁️ Отметить просмотренным", callback_data="mark_watched_from_list"))
            
            # Добавляем кнопку "Назад в базу"
            markup.add(InlineKeyboardButton("◀️ Назад в базу", callback_data="back_to_database"))
            
            # Сохраняем состояние
            user_list_state[user_id] = {
                'page': page,
                'total_pages': total_pages,
                'chat_id': chat_id
            }
        
        if message_id:
            try:
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
                # Обновляем message_id в list_messages для обработки ответов
                list_messages[message_id] = chat_id
            except Exception as e:
                logger.error(f"[LIST] Ошибка редактирования сообщения: {e}", exc_info=True)
                msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
                list_messages[msg.message_id] = chat_id
        else:
            msg = bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', disable_web_page_preview=True)
            # Сохраняем message_id для обработки ответов
            list_messages[msg.message_id] = chat_id
            return msg.message_id
    except Exception as e:
        logger.error(f"[LIST] Ошибка в show_list_page: {e}", exc_info=True)
        return None
    finally:
        # Закрываем локальные соединения
        if 'cursor_local' in locals():
            try:
                cursor_local.close()
            except:
                pass
        if 'conn_local' in locals():
            try:
                conn_local.close()
            except:
                pass


def handle_view_film_reply_internal(message, state):
    """Обработка ответного сообщения для просмотра страницы фильма"""
    try:
        import re
        from moviebot.bot.bot_init import BOT_ID
        
        user_id = message.from_user.id
        chat_id = state.get('chat_id', message.chat.id)
        text = message.text or ""
        
        logger.info(f"[VIEW FILM REPLY] Обработка ответного сообщения от {user_id}, текст: {text[:100]}")
        
        # Проверяем, что сообщение является реплаем на сообщение бота
        is_reply = (message.reply_to_message and 
                   message.reply_to_message.from_user and 
                   message.reply_to_message.from_user.id == BOT_ID)
        
        prompt_message_id = state.get('prompt_message_id')
        # Если сообщение не является ответом на нужное сообщение бота, просто игнорируем его
        if not is_reply or (prompt_message_id and message.reply_to_message.message_id != prompt_message_id):
            logger.info(f"[VIEW FILM REPLY] Сообщение от пользователя {user_id} не является ответом на сообщение бота, игнорируем")
            return
        
        # Проверяем, есть ли tag_id для возврата в подборку
        tag_id = state.get('tag_id')
        
        # Удаляем состояние
        if user_id in user_view_film_state:
            del user_view_film_state[user_id]
        
        # Извлекаем ссылку или ID из текста сообщения
        from moviebot.utils.parsing import extract_kp_id_from_text
        
        kp_id = extract_kp_id_from_text(text)
        if not kp_id:
            bot.reply_to(message, "❌ Не удалось найти ссылку или ID фильма в сообщении. Попробуйте еще раз.")
            return
        
        # ОПТИМИЗАЦИЯ: Используем те же принципы, что и в back_to_film_description
        # 1. Сначала проверяем БД (быстро)
        # 2. Только если нет в БД - запрашиваем API (медленно)
        from moviebot.bot.handlers.series import get_film_current_state, show_film_info_with_buttons
        from moviebot.database.db_connection import get_db_connection, get_db_cursor
        
        # Получаем актуальное состояние (быстро!)
        current_state = get_film_current_state(chat_id, kp_id, user_id)
        existing = current_state['existing']
        
        # Формируем ссылку
        if text.strip().startswith('http'):
            link = text.strip()
        else:
            # Определяем is_series из БД или используем базовую ссылку
            # ВАЖНО: Используем отдельное соединение для каждого блока
            conn_local_1 = get_db_connection()
            cursor_local_1 = get_db_cursor()
            is_series = False
            link_from_db = None
            try:
                with db_lock:
                    try:
                        cursor_local_1.execute("""
                            SELECT is_series, link
                            FROM movies
                            WHERE chat_id = %s AND kp_id = %s
                        """, (chat_id, str(kp_id)))
                        row = cursor_local_1.fetchone()
                        if row:
                            is_series = bool(row.get('is_series') if isinstance(row, dict) else row[0])
                            link_from_db = row.get('link') if isinstance(row, dict) else (row[1] if len(row) > 1 else None)
                    except Exception as e:
                        logger.warning(f"[VIEW FILM REPLY] Ошибка получения is_series из БД: {e}")
            finally:
                try:
                    cursor_local_1.close()
                except:
                    pass
                try:
                    conn_local_1.close()
                except:
                    pass
            
            if link_from_db:
                link = link_from_db
            else:
                link = f"https://kinopoisk.ru/series/{kp_id}/" if is_series else f"https://kinopoisk.ru/film/{kp_id}/"
        
        # ОПТИМИЗАЦИЯ: Если фильм в базе, используем данные из БД вместо API
        info = None
        if existing:
            # Фильм в базе - получаем данные из БД (быстро!)
            # ВАЖНО: Используем отдельное соединение для каждого блока
            conn_local_2 = get_db_connection()
            cursor_local_2 = get_db_cursor()
            try:
                with db_lock:
                    try:
                        cursor_local_2.execute("""
                            SELECT title, year, genres, description, director, actors, is_series, link
                            FROM movies
                            WHERE chat_id = %s AND kp_id = %s
                        """, (chat_id, str(kp_id)))
                        row = cursor_local_2.fetchone()
                        if row:
                            info = {}
                            if isinstance(row, dict):
                                info = {
                                    'title': row.get('title'),
                                    'year': row.get('year'),
                                    'genres': row.get('genres'),
                                    'description': row.get('description'),
                                    'director': row.get('director'),
                                    'actors': row.get('actors'),
                                    'is_series': bool(row.get('is_series', 0))
                                }
                                if not link_from_db:
                                    link_from_db = row.get('link')
                            else:
                                info = {
                                    'title': row[0] if len(row) > 0 else None,
                                    'year': row[1] if len(row) > 1 else None,
                                    'genres': row[2] if len(row) > 2 else None,
                                    'description': row[3] if len(row) > 3 else None,
                                    'director': row[4] if len(row) > 4 else None,
                                    'actors': row[5] if len(row) > 5 else None,
                                    'is_series': bool(row[6]) if len(row) > 6 else False
                                }
                                if not link_from_db and len(row) > 7:
                                    link_from_db = row[7]
                            if link_from_db:
                                link = link_from_db
                            logger.info(f"[VIEW FILM REPLY] Данные получены из БД (быстро!): {info.get('title')}")
                    except Exception as e:
                        logger.error(f"[VIEW FILM REPLY] Ошибка чтения БД: {e}", exc_info=True)
            finally:
                try:
                    cursor_local_2.close()
                except:
                    pass
                try:
                    conn_local_2.close()
                except:
                    pass
        
        # Если фильм НЕ в базе или БД не дала данных, запрашиваем API
        if not info or not info.get('title'):
            logger.info(f"[VIEW FILM REPLY] Фильм не в базе или данных нет - запрашиваем API (может занять 1-3 сек)")
            info = extract_movie_info(link)
            if not info:
                bot.reply_to(message, f"❌ Не удалось получить информацию о фильме. Проверьте ссылку или ID: {kp_id}")
                return
        
        # Формируем existing для передачи в show_film_info_with_buttons
        existing_tuple = existing  # Уже получено из get_film_current_state
        
        # Показываем описание фильма с кнопками (всегда новое сообщение, не редактируем)
        show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing_tuple, message_id=None)
        
        # Если есть tag_id, после показа фильма возвращаемся в подборку
        if tag_id:
            # Не делаем ничего - пользователь может вернуться через кнопку в описании фильма
            # Или можно добавить автоматический возврат, но это может быть навязчиво
            pass
        
    except Exception as e:
        logger.error(f"[VIEW FILM REPLY] Ошибка: {e}", exc_info=True)
        try:
            bot.reply_to(message, "❌ Произошла ошибка при обработке запроса. Попробуйте еще раз.")
        except:
            pass

