from moviebot.bot.bot_init import bot
"""
Обработчики команды /seasons
"""
import logging
import json
import math
from datetime import datetime, date, timedelta


from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.utils.helpers import has_notifications_access
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info
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
        
        now = datetime.now()
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
    now = datetime.now()
    
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


def show_episodes_page(kp_id, season_num, chat_id, user_id, page=1, message_id=None, message_thread_id=None, bot=None):
    """Показывает страницу эпизодов сезона с пагинацией"""
    if bot is None:
        logger.error("[SHOW_EPISODES_PAGE] bot is None! Cannot proceed.")
        return False

    try:
        logger.info(f"[SHOW EPISODES PAGE] Начало: kp_id={kp_id}, season={season_num}, chat_id={chat_id}, user_id={user_id}, page={page}, message_id={message_id}, message_thread_id={message_thread_id}")
        EPISODES_PER_PAGE = 20
        
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
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
                bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[SHOW EPISODES PAGE] Сообщение обновлено успешно")
            except Exception as e:
                logger.error(f"[SHOW EPISODES PAGE] Ошибка редактирования сообщения: {e}", exc_info=True)
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
        else:
            logger.info(f"[SHOW EPISODES PAGE] Отправка нового сообщения")
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
        
        logger.info(f"[SHOW EPISODES PAGE] Завершено успешно")
        return True
    except Exception as e:
        logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
        return False

def show_seasons_list(chat_id, user_id, message_id=None, message_thread_id=None, page=1, bot=None):
    """Основная функция показа списка сериалов с пагинацией"""
    if bot is None:
        logger.error("[SHOW_SEASONS_LIST] bot is None")
        return

    series_data = get_user_series_page(chat_id, user_id, page=page)

    if not series_data['items']:
        text = "У тебя пока нет сериалов в списке.\nДобавь их через поиск!"
        try:
            common_kwargs = {
                'text': text,
                'chat_id': chat_id,
                'parse_mode': 'HTML'
            }
            if message_thread_id is not None:
                common_kwargs['message_thread_id'] = message_thread_id

            if message_id:
                common_kwargs['message_id'] = message_id
                edit_kwargs = common_kwargs.copy()
                edit_kwargs.pop('message_thread_id', None)  # ← убираем то, что edit не жрёт
                bot.edit_message_text(**edit_kwargs)
            else:
                bot.send_message(**common_kwargs)
        except Exception as e:
            logger.error(f"[SHOW_SEASONS_LIST] Ошибка отправки пустого списка: {e}")
        return

    items = series_data['items']

    # Текст сообщения — короткий и чистый
    text = f"<b>📺 Твои сериалы</b> ({series_data['total_count']} шт.)\n\n"
    if series_data['total_pages'] > 1:
        text += f"<i>Страница {page}/{series_data['total_pages']}</i>\n\n"
    text += "Нажми на сериал → описание и сезоны"

    markup = InlineKeyboardMarkup(row_width=1)

    for item in items:
        kp_id = item['kp_id']
        title = item['title']
        year = item['year']
        watched = item['watched_count']

        # Обновление кэша (оставляем как есть)
        need_update = (
            item['last_api_update'] is None or
            (datetime.now() - item['last_api_update']) > timedelta(days=1)
        )
        if need_update:
            is_airing, next_ep = get_series_airing_status(kp_id)
            seasons_data = get_seasons_data(kp_id)
            seasons_count = len(seasons_data) if seasons_data else 0

            def default_serializer(o):
                if isinstance(o, (datetime, date)):
                    return o.isoformat()
                raise TypeError("not serializable")

            next_ep_json = json.dumps(next_ep, default=default_serializer) if next_ep else None

            with db_lock:
                cursor.execute("""
                    UPDATE movies 
                    SET is_ongoing = %s, seasons_count = %s, next_episode = %s, last_api_update = NOW()
                    WHERE chat_id = %s AND kp_id = %s
                """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                conn.commit()

            item['is_ongoing'] = is_airing
            item['seasons_count'] = seasons_count
            item['next_episode'] = next_ep

        # Строгий порядок эмодзи — как в твоём примере
        emojis = ""
        if item['is_ongoing']:
            emojis += "🟢"
            if item['has_subscription']:
                emojis += "🔔"
            if watched == 0:
                emojis += "⏳"
        else:
            emojis += "🔴"
            if watched == 0:
                emojis += "⏳"

        # Кнопка
        button_text = f"{emojis} {title} ({year})"

        # Обрезка длинных названий
        if len(button_text) > 62:
            available_len = 62 - len(emojis) - len(f" ({year})") - 4
            short_title = title[:available_len] + "..."
            button_text = f"{emojis} {short_title} ({year})"

        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{kp_id}"))

    # Пагинация
    if series_data['total_pages'] > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"seasons_page:{page-1}"))
        if page < series_data['total_pages']:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"seasons_page:{page+1}"))
        markup.row(*nav_buttons)

    # Кнопка назад в главное меню — всегда внизу, отдельной строкой
    markup.row(InlineKeyboardButton("◀️ Назад в меню", callback_data="back_to_start_menu"))

    # Отправка/редактирование с безопасным thread_id
    try:
        common_kwargs = {
            'text': text,
            'chat_id': chat_id,
            'reply_markup': markup,
            'parse_mode': 'HTML',
            'disable_web_page_preview': True
        }
        if message_thread_id is not None:
            common_kwargs['message_thread_id'] = message_thread_id

        if message_id:
            common_kwargs['message_id'] = message_id
            edit_kwargs = common_kwargs.copy()
            edit_kwargs.pop('message_thread_id', None)  # ← убираем то, что edit не жрёт
            bot.edit_message_text(**edit_kwargs)
        else:
            bot.send_message(**common_kwargs)

    except Exception as e:
        logger.error(f"[SHOW_SEASONS_LIST] Ошибка отправки: {e}", exc_info=True)

    # Пагинация (в стиле show_episodes_page)
    if series_data['total_pages'] > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"seasons_page:{page-1}"))
        if page < series_data['total_pages']:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"seasons_page:{page+1}"))
        nav_buttons.append(InlineKeyboardButton("🔄 Обновить", callback_data=f"seasons_refresh:{page}"))
        markup.row(*nav_buttons)

    try:
        common_kwargs = {
            'text': text,
            'chat_id': chat_id,
            'reply_markup': markup,
            'parse_mode': 'HTML'
        }
        if message_thread_id is not None:
            common_kwargs['message_thread_id'] = message_thread_id

        if message_id:
            common_kwargs['message_id'] = message_id
            edit_kwargs = common_kwargs.copy()
            edit_kwargs.pop('message_thread_id', None)  # ← убираем то, что edit не жрёт
            bot.edit_message_text(**edit_kwargs)
        else:
            bot.send_message(**common_kwargs)

    except Exception as e:
        logger.error(f"[SHOW_SEASONS_LIST] Ошибка редактирования/отправки: {e}", exc_info=True)
        # Фолбэк без message_thread_id (на случай очень старой версии telebot)
        try:
            fallback_kwargs = {
                'text': text,
                'chat_id': chat_id,
                'reply_markup': markup,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            if message_id:
                fallback_kwargs['message_id'] = message_id
                bot.edit_message_text(**fallback_kwargs)
            else:
                bot.send_message(**fallback_kwargs)
        except Exception as e2:
            logger.error(f"[SHOW_SEASONS_LIST] Полный фейл отправки: {e2}")

def show_completed_series_list(chat_id: int, user_id: int, message_id: int = None, message_thread_id: int = None, bot=None):
    if bot is None:
        logger.error("[SHOW_COMPLETED_SERIES_LIST] bot is None!")
        return

    logger.info(f"[SHOW_COMPLETED_SERIES_LIST] chat_id={chat_id}, user_id={user_id}, message_id={message_id}")

    has_access = has_notifications_access(chat_id, user_id)
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        all_series = cursor.fetchall()
    
    completed_series = []
    for row in all_series:
        # Упрощаем получение полей — cursor возвращает DictRow, так что .get() везде безопасно
        film_id = row.get('id') if row else None
        title = row.get('title')
        kp_id = row.get('kp_id')

        is_airing, _ = get_series_airing_status(kp_id)
        seasons_data = get_seasons_data(kp_id)

        watched_set = set()
        with db_lock:
            cursor.execute('''
                SELECT season_number, episode_number FROM series_tracking 
                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
            ''', (chat_id, film_id, user_id))
            for w_row in cursor.fetchall():
                s_num = str(w_row.get('season_number', ''))
                e_num = str(w_row.get('episode_number', ''))
                watched_set.add((s_num, e_num))

        total_ep, watched_ep = count_episodes_for_watch_check(seasons_data, is_airing, watched_set, chat_id, film_id, user_id)

        if total_ep == watched_ep and total_ep > 0 and not is_airing:
            button_text = f"✅ {title}"
            completed_series.append((kp_id, button_text))

    # Общий kwargs для отправки/редактирования
    if not completed_series:
        text = "Нет полностью просмотренных сериалов."
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))
    else:
        text = f"✅ Просмотренные сериалы ({len(completed_series)})"
        markup = InlineKeyboardMarkup(row_width=1)
        for kp_id, button_text in completed_series:
            markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{kp_id}"))
        markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))

    try:
        common_kwargs = {
            'text': text,
            'chat_id': chat_id,
            'reply_markup': markup,
            'parse_mode': 'HTML'
        }
        if message_thread_id is not None:
            common_kwargs['message_thread_id'] = message_thread_id

        if message_id:
            common_kwargs['message_id'] = message_id
            bot.edit_message_text(**common_kwargs)
        else:
            bot.send_message(**common_kwargs)
    except Exception as e:
        logger.error(f"[SHOW_COMPLETED_SERIES_LIST] Ошибка отправки/редактирования: {e}", exc_info=True)
        # Фолбэк без thread_id на случай совсем старой библиотеки
        try:
            fallback_kwargs = {
                'text': text,
                'chat_id': chat_id,
                'reply_markup': markup,
                'parse_mode': 'HTML'
            }
            if message_id:
                common_kwargs['message_id'] = message_id
                edit_kwargs = common_kwargs.copy()
                edit_kwargs.pop('message_thread_id', None)  # ← убираем то, что edit не жрёт
                bot.edit_message_text(**edit_kwargs)
            else:
                bot.send_message(**common_kwargs)
        except Exception as e2:
            logger.error(f"[SHOW_COMPLETED_SERIES_LIST] Полная ошибка отправки: {e2}")
            
@bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
def handle_seasons_kp(call):
    """Клик по сериалу в /seasons → показываем стандартное описание с постером и кнопками"""
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю описание...")

        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)

        logger.info(f"[SEASONS_KP → ОПИСАНИЕ] kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")

        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                FROM movies WHERE chat_id = %s AND kp_id = %s
            ''', (chat_id, str(str(kp_id))))
            row = cursor.fetchone()

        if not row:
            bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
            return

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
            film_id = row.get("id") if isinstance(row, dict) else (row[0] if row else None)
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

        from moviebot.bot.handlers.series import show_film_info_with_buttons

        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=existing,
            message_id=message_id,
            message_thread_id=message_thread_id
        )

    except Exception as e:
        logger.error(f"[SEASONS_KP → ОПИСАНИЕ] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda c: c.data.startswith('seasons_page:') or c.data.startswith('seasons_refresh:'))
def handle_seasons_pagination(call):
    chat_id = call.message.chat.id
    
    if call.data.startswith('seasons_refresh:'):
        page = int(call.data.split(':')[1])
        # Force update: для текущей страницы обнови все API
        current_items = get_user_series_page(chat_id, page)['items']
        for item in current_items:
            # Обнови кэш forcibly
            is_ongoing, next_episode = get_series_airing_status(item['kp_id'])
            seasons_count = len(get_seasons(item['kp_id']))
            with db_lock:
                cursor.execute("""
                    UPDATE movies SET 
                        is_ongoing = %s, seasons_count = %s, next_episode = %s, last_api_update = NOW()
                    WHERE chat_id = %s AND kp_id = %s
                """, (is_ongoing, seasons_count, next_episode, chat_id, item['kp_id']))
                conn.commit()
    else:
        page = int(call.data.split(':')[1])
    
    series_page = get_user_series_page(chat_id, page=page)
    text, markup = build_series_page_message(series_page['items'], page=page, total_pages=series_page['total_pages'], chat_id=chat_id)
    
    bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, disable_web_page_preview=True, parse_mode='HTML')
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "show_completed_series")
def handle_show_completed_series(call):
    bot.answer_callback_query(call.id, "⏳ Загружаем просмотренные...")  
    
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    
    show_completed_series_list(
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        message_thread_id=message_thread_id,
        bot=bot
    )


@bot.callback_query_handler(func=lambda call: call.data == "back_to_seasons_list")
def handle_back_to_seasons_list(call):
    bot.answer_callback_query(call.id, "⏳ Возвращаемся...")  
    
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    
    show_seasons_list(
        chat_id=chat_id,
        user_id=user_id,
        message_id=message_id,
        message_thread_id=message_thread_id,
        bot=bot
    )

@bot.message_handler(commands=['seasons'])
def handle_seasons_command(message):
    log_request(message)
    chat_id = message.chat.id
    user_id = message.from_user.id
    message_thread_id = getattr(message, 'message_thread_id', None)
    
    try:
        preload_msg = bot.send_message(
            chat_id=chat_id,
            text="⏳ Загружаем твои сериалы...",
            message_thread_id=message_thread_id
        )
        preload_message_id = preload_msg.message_id
    except Exception as e:
        logger.warning(f"[SEASONS COMMAND] Не удалось отправить прелоадер: {e}")
        preload_message_id = None

    show_seasons_list(
        chat_id=chat_id,
        user_id=user_id,
        message_id=preload_message_id,
        message_thread_id=message_thread_id,
        page=1,
        bot=bot
    )

def get_user_series_page(chat_id: int, user_id: int, page: int = 1, page_size: int = 10):
    """Возвращает страницу сериалов пользователя с пагинацией"""
    offset = (page - 1) * page_size
    items = []
    total_count = 0
    total_pages = 1

    try:
        with db_lock:
            # Считаем общее количество
            cursor.execute("""
                SELECT COUNT(DISTINCT m.id) AS total_count
                FROM movies m
                WHERE m.chat_id = %s AND m.is_series = 1
            """, (chat_id,))
            count_row = cursor.fetchone()
            total_count = count_row['total_count'] if count_row else 0  # ← по ключу!
            total_pages = math.ceil(total_count / page_size) if total_count > 0 else 1

            # Основной запрос
            cursor.execute("""
                SELECT 
                    m.id AS film_id,
                    m.kp_id,
                    m.title,
                    m.year,
                    m.poster_url,
                    m.link,
                    m.is_ongoing,
                    m.seasons_count,
                    m.next_episode,
                    m.last_api_update,
                    COUNT(st.id) AS watched_episodes_count,
                    BOOL_OR(ss.subscribed = TRUE) AS has_subscription
                FROM movies m
                LEFT JOIN series_tracking st 
                    ON st.film_id = m.id 
                    AND st.chat_id = %s 
                    AND st.user_id = %s
                LEFT JOIN series_subscriptions ss 
                    ON ss.film_id = m.id 
                    AND ss.chat_id = %s 
                    AND ss.user_id = %s
                WHERE m.chat_id = %s AND m.is_series = 1
                GROUP BY m.id
                ORDER BY
                    (m.is_ongoing = TRUE AND BOOL_OR(ss.subscribed = TRUE)) DESC,
                    (m.is_ongoing = TRUE) DESC,
                    (COUNT(st.id) > 0 AND BOOL_OR(ss.subscribed = TRUE)) DESC,
                    (COUNT(st.id) > 0) DESC,
                    m.added_date DESC
                LIMIT %s OFFSET %s
            """, (chat_id, user_id, chat_id, user_id, chat_id, page_size, offset))

            rows = cursor.fetchall()

        for row in rows:
            next_episode = row['next_episode']
            if isinstance(next_episode, str):
                try:
                    next_episode = json.loads(next_episode)
                except:
                    next_episode = None

            items.append({
                'film_id': row['film_id'],
                'kp_id': row['kp_id'],
                'title': row['title'],
                'year': row['year'],
                'poster_url': row['poster_url'],
                'link': row['link'] or f"https://www.kinopoisk.ru/series/{row['kp_id']}/",
                'is_ongoing': row['is_ongoing'],
                'seasons_count': row['seasons_count'],
                'next_episode': next_episode,
                'last_api_update': row['last_api_update'],
                'watched_count': row['watched_episodes_count'],
                'has_subscription': row['has_subscription'],
            })

    except Exception as e:
        logger.error(f"[GET_USER_SERIES_PAGE] Ошибка: {e}", exc_info=True)

    return {
        'items': items,
        'total_pages': total_pages,
        'total_count': total_count,
        'current_page': page
    }

@bot.callback_query_handler(func=lambda c: c.data.startswith(('seasons_page:', 'seasons_refresh:')))
def handle_seasons_pagination(call):
    try:
        bot.answer_callback_query(call.id)
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        message_id = call.message.message_id

        if call.data.startswith('seasons_refresh:'):
            page = int(call.data.split(':')[1])
            # Принудительно обновляем кэш для текущей страницы
            series_data = get_user_series_page(chat_id, user_id, page=page)
            for item in series_data['items']:
                kp_id = item['kp_id']
                is_airing, next_ep = get_series_airing_status(kp_id)
                seasons_count = len(get_seasons_data(str(kp_id))) if get_seasons_data(kp_id) else 0
                next_ep_json = json.dumps(next_ep) if next_ep else None
                with db_lock:
                    cursor.execute("""
                        UPDATE movies SET is_ongoing = %s, seasons_count = %s, next_episode = %s, last_api_update = NOW()
                        WHERE chat_id = %s AND kp_id = %s
                    """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                    conn.commit()
        else:
            page = int(call.data.split(':')[1])

        show_seasons_list(
            chat_id=chat_id,
            user_id=user_id,
            message_id=message_id,
            message_thread_id=message_thread_id,
            page=page,
            bot=bot
        )
    except Exception as e:
        logger.error(f"[SEASONS PAGINATION] Ошибка: {e}")
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

def register_seasons_handlers(bot):
    """Регистрирует все обработчики из модуля seasons"""
    # Здесь ничего не нужно делать — все обработчики уже зарегистрированы через декораторы @bot.message_handler и @bot.callback_query_handler
    logger.info("✅ seasons handlers зарегистрированы")