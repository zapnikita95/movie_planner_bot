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
    
    Args:
        seasons_data: данные о сезонах из API
        is_airing: выходит ли сериал (есть ли будущие эпизоды)
        watched_set: set из (season_number, episode_number) просмотренных эпизодов
        chat_id, film_id, user_id: для логирования
    
    Returns:
        (total_episodes, watched_episodes) - количество эпизодов для проверки и просмотренных
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
                # Для выходящих сериалов считаем только вышедшие эпизоды
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
                # Для завершенных сериалов считаем все эпизоды
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
        
        # Получаем film_id
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id))
            row = cursor.fetchone()
            if not row:
                logger.warning(f"[SHOW EPISODES PAGE] Сериал не найден: chat_id={chat_id}, kp_id={kp_id}")
                return False
            
            film_id = row.get('id') if isinstance(row, dict) else row[0]
            title = row.get('title') if isinstance(row, dict) else row[1]
            logger.info(f"[SHOW EPISODES PAGE] Сериал найден: film_id={film_id}, title='{title}'")
        
        # Получаем эпизоды сезона
        seasons_data = get_seasons_data(kp_id)
        season = next((s for s in seasons_data if str(s.get('number', '')) == str(season_num)), None)
        if not season:
            logger.warning(f"[SHOW EPISODES PAGE] Сезон не найден: season={season_num}, kp_id={kp_id}")
            return False
        
        episodes = season.get('episodes', [])
        total_episodes = len(episodes)
        total_pages = (total_episodes + EPISODES_PER_PAGE - 1) // EPISODES_PER_PAGE
        page = max(1, min(page, total_pages))
        
        # Вычисляем диапазон эпизодов для текущей страницы
        start_idx = (page - 1) * EPISODES_PER_PAGE
        end_idx = min(start_idx + EPISODES_PER_PAGE, total_episodes)
        page_episodes = episodes[start_idx:end_idx]
        
        # Формируем текст
        text = f"📺 <b>{title}</b> - Сезон {season_num}\n\n"
        if total_episodes > EPISODES_PER_PAGE:
            text += f"Страница {page}/{total_pages}\n\n"
        
        # Создаем разметку
        markup = InlineKeyboardMarkup(row_width=2)
        
        # Добавляем кнопки эпизодов
        for ep in page_episodes:
            ep_num = ep.get('episodeNumber', '')
            
            # Проверяем, просмотрен ли эпизод
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
        
        # Добавляем пагинацию, если страниц больше 1
        if total_pages > 1:
            pagination_buttons = []
            
            # Если страниц немного (<= 20), показываем все
            if total_pages <= 20:
                for p in range(1, total_pages + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"episodes_page:{kp_id}:{season_num}:{p}"))
                # Разбиваем кнопки на строки по 10 штук
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
            else:
                # Для большого количества страниц используем умную пагинацию
                start_page = max(1, page - 2)
                end_page = min(total_pages, page + 2)
                
                # Если текущая страница далеко от начала, показываем первую страницу и "..."
                if start_page > 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"episodes_page:{kp_id}:{season_num}:1"))
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                elif start_page == 2:
                    pagination_buttons.append(InlineKeyboardButton("1", callback_data=f"episodes_page:{kp_id}:{season_num}:1"))
                
                # Добавляем страницы вокруг текущей
                for p in range(start_page, end_page + 1):
                    label = f"•{p}" if p == page else str(p)
                    pagination_buttons.append(InlineKeyboardButton(label, callback_data=f"episodes_page:{kp_id}:{season_num}:{p}"))
                
                # Если текущая страница далеко от конца, показываем "..." и последнюю страницу
                if end_page < total_pages - 1:
                    pagination_buttons.append(InlineKeyboardButton("...", callback_data="noop"))
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"episodes_page:{kp_id}:{season_num}:{total_pages}"))
                elif end_page < total_pages:
                    pagination_buttons.append(InlineKeyboardButton(str(total_pages), callback_data=f"episodes_page:{kp_id}:{season_num}:{total_pages}"))
                
                # Разбиваем на строки по 10 кнопок
                for i in range(0, len(pagination_buttons), 10):
                    markup.row(*pagination_buttons[i:i+10])
                
                # Добавляем кнопки навигации
                nav_buttons = []
                if page > 1:
                    nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"episodes_page:{kp_id}:{season_num}:{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"episodes_page:{kp_id}:{season_num}:{page+1}"))
                if nav_buttons:
                    markup.row(*nav_buttons)
        
        text += "Нажмите на эпизод, чтобы отметить как просмотренный"
        
        # Проверяем, все ли эпизоды просмотрены
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
        
        # Добавляем кнопку "Все просмотрены" если не все просмотрены
        if not all_watched:
            markup.add(InlineKeyboardButton("✅ Все просмотрены", callback_data=f"series_season_all:{kp_id}:{season_num}"))
        
        # Всегда добавляем кнопку "Назад"
        markup.add(InlineKeyboardButton("◀️ К сезонам", callback_data=f"series_track:{kp_id}"))
        
        # Сохраняем состояние
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
                # Для обновления сообщения в треде используем API напрямую
                if message_thread_id:
                    # Используем API напрямую для поддержки тредов
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
                # При ошибке отправляем новое сообщение
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


@bot_instance.message_handler(commands=['seasons'])
def seasons_command(message):
    """Команда /seasons - просмотр сезонов сериалов"""
    logger.info(f"[HANDLER] /seasons вызван от {message.from_user.id}")
    username = message.from_user.username or f"user_{message.from_user.id}"
    log_request(message.from_user.id, username, '/seasons', message.chat.id)
    
    chat_id = message.chat.id
    user_id = message.from_user.id
    
    # Проверяем доступ к функциям уведомлений
    has_access = has_notifications_access(chat_id, user_id)
    
    with db_lock:
        cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
        series = cursor.fetchall()
    
    if not series:
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("🔍 Найти сериалы", callback_data="search_series_from_seasons"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        bot_instance.reply_to(
            message,
            "📺 Нет сериалов в базе. Используйте /search, чтобы найти и добавить сериалы, или просто пришлите ссылку на Кинопоиск на сериал",
            reply_markup=markup
        )
        return
    
    # Разделяем сериалы на категории
    fully_watched_series = []  # Все серии просмотрены
    partially_watched_series = []  # Частично просмотрены
    not_watched_series = []  # Не просмотрены
    
    for row in series:
        if isinstance(row, dict):
            title = row.get('title')
            kp_id = row.get('kp_id')
            film_id = row.get('id')
        else:
            film_id = row[0]
            title = row[1]
            kp_id = row[2]
        
        # Проверяем, подписан ли пользователь на этот сериал (только если есть доступ)
        is_subscribed = False
        if has_access:
            with db_lock:
                cursor.execute('SELECT subscribed FROM series_subscriptions WHERE chat_id = %s AND film_id = %s AND user_id = %s', (chat_id, film_id, user_id))
                sub_row = cursor.fetchone()
                is_subscribed = sub_row and (sub_row.get('subscribed') if isinstance(sub_row, dict) else sub_row[0])
        
        # Проверяем статус просмотра (только если есть доступ)
        all_episodes_watched = False
        has_some_watched = False
        if has_access:
            seasons_data = get_seasons_data(kp_id)
            if seasons_data:
                now = dt.now()
                # Проверяем, выходит ли сериал (есть ли будущие эпизоды)
                is_airing, _ = get_series_airing_status(kp_id)
                
                # Если сериал не выходит, проверяем, все ли серии просмотрены
                if not is_airing:
                    total_episodes, watched_episodes = count_episodes_for_watch_check(
                        seasons_data, is_airing, set(), chat_id, film_id, user_id
                    )
                    
                    # Получаем просмотренные эпизоды
                    with db_lock:
                        cursor.execute('''
                            SELECT season_number, episode_number 
                            FROM series_tracking 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                        ''', (chat_id, film_id, user_id))
                        watched_rows = cursor.fetchall()
                        watched_set = set()
                        for w_row in watched_rows:
                            if isinstance(w_row, dict):
                                watched_set.add((str(w_row.get('season_number')), str(w_row.get('episode_number'))))
                            else:
                                watched_set.add((str(w_row[0]), str(w_row[1])))
                    
                    total_episodes, watched_episodes = count_episodes_for_watch_check(
                        seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                    )
                    
                    if total_episodes > 0:
                        if watched_episodes == total_episodes:
                            all_episodes_watched = True
                        elif watched_episodes > 0:
                            has_some_watched = True
                else:
                    # Если сериал выходит, проверяем, все ли вышедшие серии просмотрены
                    with db_lock:
                        cursor.execute('''
                            SELECT season_number, episode_number 
                            FROM series_tracking 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                        ''', (chat_id, film_id, user_id))
                        watched_rows = cursor.fetchall()
                        watched_set = set()
                        for w_row in watched_rows:
                            if isinstance(w_row, dict):
                                watched_set.add((str(w_row.get('season_number')), str(w_row.get('episode_number'))))
                            else:
                                watched_set.add((str(w_row[0]), str(w_row[1])))
                    
                    total_episodes, watched_episodes = count_episodes_for_watch_check(
                        seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                    )
                    
                    if total_episodes > 0:
                        if watched_episodes == total_episodes:
                            all_episodes_watched = True
                        elif watched_episodes > 0:
                            has_some_watched = True
        
        # Классифицируем сериал
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
    
    # Формируем разметку: сначала частично просмотренные, потом не просмотренные
    markup = InlineKeyboardMarkup(row_width=1)
    
    # Частично просмотренные сериалы (приоритетные) - в начале
    for series_info in partially_watched_series:
        button_text = f"👁️ {series_info['title']}"
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
    
    # Не просмотренные сериалы
    for series_info in not_watched_series:
        button_text = series_info['title']
        if len(button_text) > 30:
            button_text = button_text[:27] + "..."
        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
    
    # Добавляем кнопку "Просмотренные сериалы" если есть доступ и есть просмотренные сериалы
    if has_access and fully_watched_series:
        watched_button_text = "✅ Просмотренные"
        if len(fully_watched_series) > 0:
            # Показываем количество просмотренных сериалов
            watched_button_text = f"✅ Просмотренные ({len(fully_watched_series)})"
        markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_series_list"))
    
    # Добавляем кнопку "Назад в меню"
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    
    # Сохраняем message_id для возможности вернуться назад
    bot_instance.reply_to(message, "📺 <b>Выберите сериал:</b>", reply_markup=markup, parse_mode='HTML')


def register_seasons_handlers(bot):
    """Регистрирует обработчики для команды /seasons
    
    Примечание: Основные обработчики сезонов и серий регистрируются через
    register_series_callbacks в series_callbacks.py. Эта функция добавлена
    для совместимости с main.py.
    """
    # Обработчики сезонов и серий регистрируются в series_callbacks.py
    # через функцию register_series_callbacks
    # Команда /seasons регистрируется через декоратор @bot_instance.message_handler
    pass
