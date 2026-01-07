"""
Обработчики команды /seasons
"""
import logging
import json
from moviebot.bot.bot_init import bot
from datetime import datetime as dt
from moviebot.bot.bot_init import bot
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.bot.bot_init import bot
from moviebot.database.db_operations import log_request
from moviebot.bot.bot_init import bot
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.bot.bot_init import bot
from moviebot.utils.helpers import has_notifications_access
from moviebot.bot.bot_init import bot
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info
from moviebot.bot.bot_init import bot
from moviebot.states import user_episodes_state


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def get_series_airing_status(kp_id):
    """Определяет, выходит ли сериал (есть ли будущие эпизоды)"""
    try:
        seasons_data = get_seasons_data(kp_id)
        if not seasons_data:
            return False, None
        
        now = dt.now()
        is_airing = False
        next_episode = None
        next_episode_date = None
        
        for season in seasons_data:
            episodes = season.get('episodes', [])
            for ep in episodes:
                release_str = ep.get('releaseDate', '')
                if release_str and release_str != '—':
                    try:
                        release_date = None
                        for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                            try:
                                release_date = dt.strptime(release_str.split('T')[0], fmt)
                                break
                            except:
                                continue
                        
                        if release_date and release_date > now:
                            if not next_episode_date or release_date < next_episode_date:
                                next_episode_date = release_date
                                next_episode = {
                                    'season': season.get('number', ''),
                                    'episode': ep.get('episodeNumber', ''),
                                    'date': release_date
                                }
                                is_airing = True
                    except:
                        pass
        
        return is_airing, next_episode
    except Exception as e:
        logger.warning(f"[GET_SERIES_AIRING_STATUS] Ошибка: {e}")
        return False, None


def count_episodes_for_watch_check(seasons_data, is_airing, watched_set, chat_id, film_id, user_id):
    """
    Подсчитывает общее количество эпизодов и просмотренных для проверки "все просмотрены"
    """
    now = dt.now()
    
    total_episodes = 0
    watched_episodes = 0
    
    for season in seasons_data:
        episodes = season.get('episodes', [])
        season_num = str(season.get('number', ''))
        for ep in episodes:
            ep_num = str(ep.get('episodeNumber', ''))
            release_str = ep.get('releaseDate', '')
            
            should_count = False
            
            if is_airing:
                if release_str and release_str != '—':
                    try:
                        release_date = None
                        for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%Y-%m-%dT%H:%M:%S']:
                            try:
                                release_date = dt.strptime(release_str.split('T')[0], fmt)
                                break
                            except:
                                continue
                        if release_date and release_date <= now:
                            should_count = True
                    except:
                        pass
            else:
                should_count = True
            
            if should_count:
                total_episodes += 1
                if (season_num, ep_num) in watched_set:
                    watched_episodes += 1
    
    return total_episodes, watched_episodes


def show_episodes_page(kp_id, season_num, chat_id, user_id, page=1, message_id=None, message_thread_id=None):
    """Показывает страницу эпизодов сезона с пагинацией"""
    try:
        logger.info(f"[SHOW EPISODES PAGE] Начало: kp_id={kp_id}, season={season_num}, chat_id={chat_id}, user_id={user_id}, page={page}, message_id={message_id}, message_thread_id={message_thread_id}")
        EPISODES_PER_PAGE = 20
        
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"[SHOW EPISODES PAGE] Сериал не найден: chat_id={chat_id}, kp_id={kp_id}")
                return False
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
            logger.info(f"[SHOW EPISODES PAGE] Сериал найден: film_id={film_id}, title='{title}'")
        
        seasons_data = get_seasons_data(kp_id)
        season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
        if not season:
            logger.warning(f"[SHOW EPISODES PAGE] Сезон не найден: season={season_num}, kp_id={kp_id}")
            return False
        
        episodes = season.get('episodes', [])
        total_episodes = len(episodes)
        total_pages = (total_episodes + EPISODES_PER_PAGE - 1) // EPISODES_PER_PAGE
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * EPISODES_PER_PAGE
        end_idx = min(start_idx + EPISODES_PER_PAGE, total_episodes)
        page_episodes = episodes[start_idx:end_idx]
        
        text = f"Сезон {season_num}\n\n"
        if total_episodes > EPISODES_PER_PAGE:
            text += f"Страница {page}/{total_pages}\n\n"
        
        markup = InlineKeyboardMarkup(row_width=2)
        
        for ep in page_episodes:
            ep_num = ep.get('episodeNumber', '')
            
            with db_lock:
                cursor.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor.fetchone()
                is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            
            mark = "Просмотрено" if is_watched else "Не просмотрено"
            button_text = f"{mark} {ep_num}"
            if len(button_text) > 20:
                button_text = button_text[:17] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_episode:{kp_id}:{season_num}:{ep_num}"))
        
        if total_pages > 1:
            pagination_buttons = []
            
            if total_pages <= 20:
                for p in range(1, total_pages + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"episodes_page:{kp_id}:{season_num}:{p}"))
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
            else:
                start_page = max(1, page - 2)
                end_page = min(total_pages, page + 2)
                
                if start_page > 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"episodes_page:{kp_id}:{season_num}:1"))
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                elif start_page == 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"episodes_page:{kp_id}:{season_num}:1"))
                
                for p in range(start_page, end_page + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"episodes_page:{kp_id}:{season_num}:{p}"))
                
                if end_page < total_pages - 1:
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"episodes_page:{kp_id}:{season_num}:{total_pages}"))
                elif end_page < total_pages:
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"episodes_page:{kp_id}:{season_num}:{total_pages}"))
                
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
                
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("Назад", callback_data=f"episodes_page:{kp_id}:{season_num}:{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд", callback_data=f"episodes_page:{kp_id}:{season_num}:{page+1}"))
                if nav_buttons:
                    markup.row(*nav_buttons)
        
        text += "Нажмите на эпизод, чтобы отметить как просмотренный"
        
        all_watched = True
        with db_lock:
            for ep in episodes:
                ep_num = ep.get('episodeNumber', '')
                cursor.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor.fetchone()
                is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                if not is_watched:
                    all_watched = False
                    break
        
        logger.info(f"[SHOW EPISODES PAGE] Все эпизоды просмотрены: {all_watched}, страница {page}/{total_pages}")
        
        if not all_watched:
            markup.add(InlineKeyboardButton("Все просмотрены", callback_data=f"series_season_all:{kp_id}:{season_num}"))
        
        markup.add(InlineKeyboardButton("К сезонам", callback_data=f"series_track:{kp_id}"))
        
        user_episodes_state[user_id] = {
            'kp_id': kp_id,
            'season_num': season_num,
            'page': page,
            'total_pages': total_pages,
            'chat_id': chat_id
        }
        
        if message_id:
            try:
                logger.info(f"[SHOW EPISODES PAGE] Обновление сообщения: message_id={message_id}, message_thread_id={message_thread_id}")
                if message_thread_id:
                    reply_markup_json = json.dumps(markup.to_dict()) if markup else None
                    params = {
                        'chat_id': chat_id,
                        'message_id': message_id,
                        'text': text,
                        'parse_mode': 'HTML',
                        'message_thread_id': message_thread_id
                    }
                    if reply_markup_json:
                        params['reply_markup'] = reply_markup_json
                    bot.api_call('editMessageText', params)
                else:
                    bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[SHOW EPISODES PAGE] Сообщение обновлено успешно")
            except Exception as e:
                logger.error(f"[SHOW EPISODES PAGE] Ошибка редактирования сообщения: {e}", exc_info=True)
                if message_thread_id:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                else:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        else:
            logger.info(f"[SHOW EPISODES PAGE] Отправка нового сообщения")
            if message_thread_id:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
            else:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        logger.info(f"[SHOW EPISODES PAGE] Завершено успешно")
        return True
    except Exception as e:
        logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
        return False

def show_seasons_list(chat_id, user_id, message_id=None, message_thread_id=None, bot=None):
    """
    Основная функция показа списка сериалов.
    Если message_id передан — редактирует существующее сообщение, иначе отправляет новое.
    bot — обязательный параметр для единообразия (передаётся из хэндлера)
    """
    if bot is None:
        from moviebot.bot.bot_init import bot as global_bot
        bot = global_bot

    logger.info(f"[SHOW_SEASONS_LIST] chat_id={chat_id}, user_id={user_id}, message_id={message_id}, thread_id={message_thread_id}")

    has_access = has_notifications_access(chat_id, user_id)
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        series = cursor.fetchall()
    
    if not series:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("Найти сериалы", callback_data="search_series_from_seasons"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        text = "Нет сериалов в базе. Используйте /search, чтобы найти и добавить сериалы, или просто пришлите ссылку на Кинопоиск на сериал"
        
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
        return

    markup = InlineKeyboardMarkup(row_width=1)

    has_completed = False

    for row in series:
        film_id = row[0] if not isinstance(row, dict) else row.get('id')
        title = row[1] if not isinstance(row, dict) else row.get('title')
        kp_id = row[2] if not isinstance(row, dict) else row.get('kp_id')

        is_airing, next_episode = get_series_airing_status(kp_id)
        seasons_data = get_seasons_data(kp_id)

        watched_set = set()
        with db_lock:
            cursor.execute('''
                SELECT season_number, episode_number FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
            ''', (chat_id, film_id, user_id))
            for w_row in cursor.fetchall():
                s_num = str(w_row[0] if not isinstance(w_row, dict) else w_row.get('season_number'))
                e_num = str(w_row[1] if not isinstance(w_row, dict) else w_row.get('episode_number'))
                watched_set.add((s_num, e_num))

        total_ep, watched_ep = count_episodes_for_watch_check(seasons_data, is_airing, watched_set, chat_id, film_id, user_id)

        if total_ep == watched_ep and total_ep > 0 and not is_airing:
            has_completed = True
            continue

        status = ""
        if is_airing:
            status = "🟢 "
        elif watched_ep > 0:
            status = f"⏳ {watched_ep}/{total_ep} "

        button_text = f"{status}{title}"
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{kp_id}"))

    if has_completed:
        markup.add(InlineKeyboardButton("✅ Просмотренные", callback_data="show_completed_series"))

    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))

    lower_buttons = 1
    if has_completed:
        lower_buttons += 1

    num_series = len(markup.keyboard) - lower_buttons
    text = f"📺 Активные сериалы в базе ({num_series})"

    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)

def show_completed_series_list(chat_id: int, user_id: int, message_id: int = None):
    """
    Показывает список полностью просмотренных сериалов (аналогично show_seasons_list, но только completed).
    """
    logger.info(f"[SHOW_COMPLETED_SERIES_LIST] chat_id={chat_id}, user_id={user_id}, message_id={message_id}")

    has_access = has_notifications_access(chat_id, user_id)
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        all_series = cursor.fetchall()
    
    completed_series = []
    for row in all_series:
        film_id = row[0] if not isinstance(row, dict) else row.get('id')
        title = row[1] if not isinstance(row, dict) else row.get('title')
        kp_id = row[2] if not isinstance(row, dict) else row.get('kp_id')

        is_airing, _ = get_series_airing_status(kp_id)
        seasons_data = get_seasons_data(kp_id)

        watched_set = set()
        with db_lock:
            cursor.execute('''
                SELECT season_number, episode_number FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
            ''', (chat_id, film_id, user_id))
            for w_row in cursor.fetchall():
                s_num = str(w_row[0] if not isinstance(w_row, dict) else w_row.get('season_number'))
                e_num = str(w_row[1] if not isinstance(w_row, dict) else w_row.get('episode_number'))
                watched_set.add((s_num, e_num))

        total_ep, watched_ep = count_episodes_for_watch_check(seasons_data, is_airing, watched_set, chat_id, film_id, user_id)

        if total_ep == watched_ep and total_ep > 0 and not is_airing:  # Только полностью просмотренные и не выходящие
            status = "✅ "  # Эмодзи в начале
            button_text = f"{status}{title}"
            completed_series.append((kp_id, button_text))

    if not completed_series:
        text = "Нет полностью просмотренных сериалов."
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))
        if message_id:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        return

    markup = InlineKeyboardMarkup(row_width=1)
    for kp_id, button_text in completed_series:
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{kp_id}"))

    markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))

    text = f"✅ Просмотренные сериалы ({len(completed_series)})"

    if message_id:
        bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')

@bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
def handle_seasons_kp(call):
    """Клик по сериалу в /seasons → показываем стандартное описание с постером и кнопками"""
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю описание...")

        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_thread_id = getattr(call.message, 'message_thread_id', None)

        logger.info(f"[SEASONS_KP → ОПИСАНИЕ] kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")

        # Получаем данные из БД
        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                FROM movies WHERE chat_id = %s AND kp_id = %s
            ''', (chat_id, str(kp_id)))
            row = cursor.fetchone()

        if not row:
            bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
            return

        # Формируем данные (как в других местах бота)
        if isinstance(row, dict):
            film_id = row['id']
            title = row['title']
            watched = row['watched']
            link = row.get('link') or f"https://www.kinopoisk.ru/film/{kp_id}/"
            year = row.get('year')
            genres = row.get('genres')
            description = row.get('description')
            director = row.get('director')
            actors = row.get('actors')
            is_series = bool(row.get('is_series', 0))
        else:
            film_id = row[0]
            title = row[1]
            watched = row[2]
            link = row[3] if len(row) > 3 else f"https://www.kinopoisk.ru/film/{kp_id}/"
            year = row[4] if len(row) > 4 else None
            genres = row[5] if len(row) > 5 else None
            description = row[6] if len(row) > 6 else None
            director = row[7] if len(row) > 7 else None
            actors = row[8] if len(row) > 8 else None
            is_series = bool(row[9] if len(row) > 9 else 0)

        info = {
            'title': title,
            'year': year,
            'genres': genres,
            'description': description,
            'director': director,
            'actors': actors,
            'is_series': is_series
        }

        existing = (film_id, title, watched)

        # ←←← ВОЛШЕБСТВО: локальный импорт здесь, внутри функции
        from moviebot.bot.handlers.series import show_film_info_with_buttons

        # Показываем карточку (редактируем текущее сообщение)
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=existing,
            message_id=call.message.message_id,  # ← заменяем список сериалов на карточку
            message_thread_id=message_thread_id
        )

    except Exception as e:
        logger.error(f"[SEASONS_KP → ОПИСАНИЕ] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "show_completed_series")
def handle_show_completed_series(call):
    bot.answer_callback_query(call.id, "⏳ Загружаем просмотренные...")  # ← прелоадер
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    show_completed_series_list(chat_id, user_id, message_id=message_id)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_seasons_list")
def handle_back_to_seasons_list(call):
    bot.answer_callback_query(call.id, "⏳ Возвращаемся...")  # ← прелоадер
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    show_seasons_list(chat_id, user_id, message_id=message_id)

def register_seasons_handlers(bot):  # параметр bot оставляем
    """Регистрируем обработчики в основной init"""
    @bot.message_handler(commands=['seasons'])
    def handle_seasons_command(message):
        log_request(message)
        chat_id = message.chat.id
        user_id = message.from_user.id
        message_thread_id = getattr(message, 'message_thread_id', None)
        
        # ← Прелоадер: отправляем временное сообщение
        preload_msg = bot.send_message(
            chat_id, 
            "⏳ Загружаем сериалы и сезоны...", 
            message_thread_id=message_thread_id
        )
        
        # Показываем список
        show_seasons_list(chat_id, user_id, message_thread_id=message_thread_id)
        
        # Удаляем прелоадер после загрузки (добавь этот try в конец show_seasons_list, если хочешь универсально, или здесь)
        try:
            bot.delete_message(chat_id, preload_msg.message_id, message_thread_id=message_thread_id)
        except Exception as e:
            logger.warning(f"[SEASONS COMMAND] Не удалось удалить прелоадер: {e}")