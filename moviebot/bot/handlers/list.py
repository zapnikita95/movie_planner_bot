"""
Обработчики команды /list
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.states import user_list_state, list_messages
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def register_list_handlers(bot):
    """Регистрирует обработчики команды /list"""
    
    @bot.message_handler(commands=['list'])
    def list_movies(message):
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
            markup = None
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

