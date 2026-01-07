"""
Обработчики команды /seasons
"""
import logging
import json
from datetime import datetime as dt
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.utils.helpers import has_notifications_access
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info
from moviebot.bot.bot_init import bot as bot_instance
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
    # (оставляем без изменений — код тот же, что был у тебя)
    try:
        logger.info(f"[SHOW EPISODES PAGE] Начало: kp_id={kp_id}, season={season_num}, chat_id={chat_id}, user_id={user_id}, page={page}, message_id={message_id}, message_thread_id={message_thread_id}")
        EPISODES_PER_PAGE = 20
        
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
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
        
        text = f"📺 <b>{title}</b> - Сезон {season_num}\n\n"
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
            
            mark = "✅" if is_watched else "⬜"
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
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"episodes_page:{kp_id}:{season_num}:{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"episodes_page:{kp_id}:{season_num}:{page+1}"))
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
            markup.add(InlineKeyboardButton("✅ Все просмотрены", callback_data=f"series_season_all:{kp_id}:{season_num}"))
        
        markup.add(InlineKeyboardButton("◀️ К сезонам", callback_data=f"series_track:{kp_id}"))
        
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
                    bot_instance.api_call('editMessageText', params)
                else:
                    bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
                logger.info(f"[SHOW EPISODES PAGE] Сообщение обновлено успешно")
            except Exception as e:
                logger.error(f"[SHOW EPISODES PAGE] Ошибка редактирования сообщения: {e}", exc_info=True)
                if message_thread_id:
                    bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                else:
                    bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        else:
            logger.info(f"[SHOW EPISODES PAGE] Отправка нового сообщения")
            if message_thread_id:
                bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
            else:
                bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        logger.info(f"[SHOW EPISODES PAGE] Завершено успешно")
        return True
    except Exception as e:
        logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
        return False


def show_seasons_list(chat_id: int, user_id: int, message_id: int = None):
    """
    Основная функция показа списка сериалов.
    Если message_id передан — редактирует существующее сообщение, иначе отправляет новое.
    """
    logger.info(f"[SHOW_SEASONS_LIST] chat_id={chat_id}, user_id={user_id}, message_id={message_id}")

    has_access = has_notifications_access(chat_id, user_id)
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        series = cursor.fetchall()
    
    if not series:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔍 Найти сериалы", callback_data="search_series_from_seasons"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        text = "📺 Нет сериалов в базе. Используйте /search, чтобы найти и добавить сериалы, или просто пришлите ссылку на Кинопоиск на сериал"
        
        if message_id:
            bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        else:
            bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        return
    
    fully_watched_series = []
    partially_watched_series = []
    not_watched_series = []
    
    for row in series:
        if isinstance(row, dict):
            title = row.get('title')
            kp_id = row.get('kp_id')
            film_id = row.get('id')
        else:
            film_id = row[0]
            title = row[1]
            kp_id = row[2]
        
        is_subscribed = False
        if has_access:
            with db_lock:
                cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                sub_row = cursor.fetchone()
                is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        
        all_episodes_watched = False
        has_some_watched = False
        if has_access:
            seasons_data = get_seasons_data(kp_id)
            if seasons_data:
                is_airing, _ = get_series_airing_status(kp_id)
                
                with db_lock:
                    cursor.execute('''
                        SELECT season_number, episode_number 
                        FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                    ''', (chat_id, film_id, user_id))
                    watched_rows = cursor.fetchall()
                    watched_set = {(str(r[0]), str(r[1])) for r in watched_rows}
                
                total_episodes, watched_episodes = count_episodes_for_watch_check(
                    seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                )
                
                if total_episodes > 0:
                    if watched_episodes == total_episodes:
                        all_episodes_watched = True
                    elif watched_episodes > 0:
                        has_some_watched = True
        
        series_info = {
            'title': title,
            'kp_id': kp_id,
            'film_id': film_id,
            'is_subscribed': is_subscribed,
            'all_watched': all_episodes_watched
        }
        
        if all_episodes_watched:
            fully_watched_series.append(series_info)
        elif has_some_watched:
            partially_watched_series.append(series_info)
        else:
            not_watched_series.append(series_info)
    
    markup = InlineKeyboardMarkup(row_width=1)
    
    for series_info in partially_watched_series:
        button_text = f"👁️ {series_info['title']}"
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
    
    for series_info in not_watched_series:
        button_text = series_info['title']
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
    
    if has_access and fully_watched_series:
        watched_button_text = f"✅ Просмотренные ({len(fully_watched_series)})"
        markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_series_list"))
    
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    
    text = "📺 <b>Выберите сериал:</b>"
    
    if message_id:
        bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
    else:
        bot_instance.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')


@bot_instance.message_handler(commands=['seasons'])
def seasons_command(message):
    """Команда /seasons - просмотр сезонов сериалов"""
    logger.info(f"[HANDLER] /seasons вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/seasons', message.chat.id)
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    show_seasons_list(chat_id, user_id)


@bot_instance.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
def handle_seasons_kp(call):
    """Обработка выбора сериала — показ сезонов"""
    try:
        bot_instance.answer_callback_query(call.id)

        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id

        logger.info(f"[SEASONS_KP] Выбран сериал kp_id={kp_id}, user_id={user_id}")

        series_info = extract_movie_info(kp_id)
        if not series_info:
            bot_instance.edit_message_text("❌ Не удалось загрузить информацию о сериале.", chat_id, message_id)
            return

        title = series_info.get('name_ru') or series_info.get('name_original') or "Сериал"

        seasons_data = get_seasons_data(kp_id)
        if not seasons_data:
            bot_instance.edit_message_text(f"<b>{title}</b>\n\n❌ Нет данных о сезонах.", chat_id, message_id, parse_mode='HTML')
            return

        markup = InlineKeyboardMarkup(row_width=1)

        with db_lock:
            cursor.execute('SELECT id FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            film_id = row[0] if row else None

        for season in seasons_data:
            season_num = season.get('number')
            episodes = season.get('episodes', [])
            episodes_count = len(episodes)
            if not episodes_count:
                continue

            is_airing, _ = get_series_airing_status(kp_id)
            watched_set = set()
            if film_id:
                with db_lock:
                    cursor.execute('''
                        SELECT season_number, episode_number FROM series_tracking
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                    ''', (chat_id, film_id, user_id))
                    for w_row in cursor.fetchall():
                        watched_set.add((str(w_row[0]), str(w_row[1])))

            _, watched_in_season = count_episodes_for_watch_check([season], is_airing, watched_set, chat_id, film_id, user_id)
            total_in_season, _ = count_episodes_for_watch_check([season], is_airing, set(), chat_id, film_id, user_id)

            mark = "✅ " if watched_in_season == total_in_season and total_in_season > 0 else ""
            button_text = f"{mark}Сезон {season_num} ({episodes_count} серий)"
            markup.add(InlineKeyboardButton(button_text, callback_data=f"series_season:{kp_id}:{season_num}"))

        markup.add(InlineKeyboardButton("◀️ Назад к списку сериалов", callback_data="back_to_seasons_list"))

        text = f"📺 <b>{title}</b>\n\nВыберите сезон:"

        bot_instance.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')

    except Exception as e:
        logger.error(f"[SEASONS_KP] Ошибка: {e}", exc_info=True)
        try:
            bot_instance.answer_callback_query(call.id, "Ошибка, попробуйте позже", show_alert=True)
        except:
            pass


@bot_instance.callback_query_handler(func=lambda call: call.data == "back_to_seasons_list")
def back_to_seasons_list(call):
    """Возврат из сезонов обратно к списку сериалов"""
    try:
        bot_instance.answer_callback_query(call.id)
        
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id
        
        show_seasons_list(chat_id, user_id, message_id=message_id)
        
    except Exception as e:
        logger.error(f"[BACK_TO_SEASONS_LIST] Ошибка: {e}", exc_info=True)


def register_seasons_handlers(bot):
    """Для совместимости с main.py — ничего не делает, всё регистрируется через декораторы"""
    pass