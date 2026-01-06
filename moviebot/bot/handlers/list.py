"""
Обработчики команды /list
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.states import user_list_state, list_messages, user_view_film_state, user_plan_state
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot as bot_instance
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
        try:
            user_id = call.from_user.id
            page = int(call.data.split(":")[1])
            
            state = user_list_state.get(user_id)
            if not state:
                bot.answer_callback_query(call.id, "Сессия устарела. Используйте /list заново")
                return
            
            chat_id = state['chat_id']
            show_list_page(bot, chat_id, user_id, page, call.message.message_id)
            bot.answer_callback_query(call.id)
        except Exception as e:
            logger.error(f"[LIST] Ошибка в handle_list_page: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "Ошибка переключения страницы")
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "noop")
    def handle_noop(call):
        """Обработчик для неактивных кнопок (noop)"""
        bot.answer_callback_query(call.id)
    
    @bot.callback_query_handler(func=lambda call: call.data == "plan_from_list")
    def plan_from_list_callback(call):
        """Обработчик кнопки 'Запланировать просмотр' из /list"""
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[PLAN FROM LIST] Пользователь {user_id} хочет запланировать фильм из /list")
            
            # Устанавливаем состояние для планирования
            user_plan_state[user_id] = {
                'step': 1,
                'chat_id': chat_id
            }
            
            bot_instance.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
            prompt_msg = bot_instance.send_message(chat_id, "Пришлите ссылку или ID фильма в ответном сообщении и напишите, где (дома или в кино) и когда вы хотели бы его посмотреть!")
            # Сохраняем message_id промпта в состояние
            user_plan_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[PLAN FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[PLAN FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
    
    @bot.callback_query_handler(func=lambda call: call.data == "view_film_from_list")
    def view_film_from_list_callback(call):
        """Обработчик кнопки 'Посмотреть страницу фильма' из /list"""
        try:
            user_id = call.from_user.id
            chat_id = call.message.chat.id
            
            logger.info(f"[VIEW FILM FROM LIST] Пользователь {user_id} хочет посмотреть страницу фильма из /list")
            
            # Устанавливаем состояние для просмотра фильма
            user_view_film_state[user_id] = {
                'chat_id': chat_id
            }
            
            bot_instance.answer_callback_query(call.id, "Пришлите ссылку или ID фильма")
            prompt_msg = bot_instance.send_message(chat_id, "Пришлите в ответном сообщении ссылку или ID фильма, чье описание хотите посмотреть")
            # Сохраняем message_id промпта в состояние
            user_view_film_state[user_id]['prompt_message_id'] = prompt_msg.message_id
            logger.info(f"[VIEW FILM FROM LIST] Состояние установлено для пользователя {user_id}, prompt_message_id={prompt_msg.message_id}")
        except Exception as e:
            logger.error(f"[VIEW FILM FROM LIST] Ошибка: {e}", exc_info=True)
            try:
                bot_instance.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass


def show_list_page(bot, chat_id, user_id, page=1, message_id=None):
    """Показывает страницу списка фильмов"""
    try:
        MOVIES_PER_PAGE = 15
        
        with db_lock:
            # Получаем все непросмотренные фильмы, отсортированные по алфавиту
            cursor.execute('''
                SELECT DISTINCT m.id, m.kp_id, m.title, m.year, m.genres, m.link 
                FROM movies m
                WHERE m.chat_id = %s 
                  AND m.watched = 0
                ORDER BY m.title
            ''', (chat_id,))
            rows = cursor.fetchall()
        
        if not rows:
            text = "⏳ Нет непросмотренных фильмов!"
            markup = InlineKeyboardMarkup()
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
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
                text += f"• <b>{title}</b> ({year}){genre_str} [ID: {movie_id}]\n<a href='{link}'>{link}</a>\n\n"
            
            text += "\n<i>В ответном сообщении пришлите ID фильмов, и они будут отмечены как просмотренные</i>"
            
            # Создаем кнопки пагинации
            markup = InlineKeyboardMarkup()
            
            # Добавляем кнопки действий
            markup.row(
                InlineKeyboardButton("📅 Запланировать просмотр", callback_data="plan_from_list"),
                InlineKeyboardButton("👁️ Посмотреть страницу фильма", callback_data="view_film_from_list")
            )
            
            # Если страниц немного (<= 20), показываем все
            if total_pages <= 20:
                buttons = []
                for p in range(1, total_pages + 1):
                    label = f"•{p}" if p == page else str(p)
                    buttons.append(InlineKeyboardButton(label, callback_data=f"list_page:{p}"))
                # Разбиваем кнопки на строки по 10 штук
                for i in range(0, len(buttons), 10):
                    markup.row(*buttons[i:i+10])
            else:
                # Для большого количества страниц используем умную пагинацию
                buttons = []
                
                # Показываем страницы вокруг текущей
                start_page = max(1, page - 2)
                end_page = min(total_pages, page + 2)
                
                # Если текущая страница далеко от начала, показываем первую страницу и "..."
                if start_page > 2:
                    buttons.append(InlineKeyboardButton("1", callback_data="list_page:1"))
                    buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                elif start_page == 2:
                    buttons.append(InlineKeyboardButton("1", callback_data="list_page:1"))
                
                # Добавляем страницы вокруг текущей
                for p in range(start_page, end_page + 1):
                    label = f"•{p}" if p == page else str(p)
                    buttons.append(InlineKeyboardButton(label, callback_data=f"list_page:{p}"))
                
                # Если текущая страница далеко от конца, показываем "..." и последнюю страницу
                if end_page < total_pages - 1:
                    buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                    buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"list_page:{total_pages}"))
                elif end_page < total_pages:
                    buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"list_page:{total_pages}"))
                
                # Разбиваем на строки по 10 кнопок
                for i in range(0, len(buttons), 10):
                    markup.row(*buttons[i:i+10])
                
                # Добавляем кнопки навигации
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"list_page:{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"list_page:{page+1}"))
                if nav_buttons:
                    markup.row(*nav_buttons)
            
            # Добавляем кнопку "Назад в меню"
            markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
            
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
        
        # Удаляем состояние
        if user_id in user_view_film_state:
            del user_view_film_state[user_id]
        
        # Извлекаем ссылку или ID из текста сообщения
        from moviebot.utils.parsing import extract_kp_id_from_text
        
        kp_id = extract_kp_id_from_text(text)
        if not kp_id:
            bot_instance.reply_to(message, "❌ Не удалось найти ссылку или ID фильма в сообщении. Попробуйте еще раз.")
            return
        
        # Формируем ссылку
        if text.strip().startswith('http'):
            link = text.strip()
        else:
            link = f"https://kinopoisk.ru/film/{kp_id}/"
        
        # Получаем информацию о фильме
        info = extract_movie_info(link)
        if not info:
            bot_instance.reply_to(message, f"❌ Не удалось получить информацию о фильме. Проверьте ссылку или ID: {kp_id}")
            return
        
        # Проверяем, есть ли фильм в базе
        with db_lock:
            cursor.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            existing = cursor.fetchone()
        
        # Формируем existing для передачи в show_film_info_with_buttons
        if existing:
            film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
            title = existing.get('title') if isinstance(existing, dict) else existing[1]
            watched = existing.get('watched') if isinstance(existing, dict) else existing[2]
            existing_tuple = (film_id, title, watched)
        else:
            existing_tuple = None
        
        # Показываем описание фильма с кнопками
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing_tuple)
        
    except Exception as e:
        logger.error(f"[VIEW FILM REPLY] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.reply_to(message, "❌ Произошла ошибка при обработке запроса. Попробуйте еще раз.")
        except:
            pass

