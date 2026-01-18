from moviebot.bot.bot_init import bot
"""
Обработчики команды /seasons
"""
import logging
import json
import math
from datetime import datetime, date, timedelta
import psycopg2
import telebot

from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.utils.helpers import has_notifications_access
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info
from moviebot.states import user_episodes_state, user_episode_auto_mark_state

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
                                release_date = datetime.strptime(release_str.split('T')[0], fmt)
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
                                release_date = datetime.strptime(release_str.split('T')[0], fmt)
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
    """Показывает страницу эпизодов сезона с пагинацией.

    ВАЖНО: bot может не быть передан из callback'ов (series_callbacks),
    поэтому при необходимости пытаемся взять глобальный bot из bot_init.
    """
    if bot is None:
        try:
            # Ленивая загрузка, чтобы избежать цикличных импортов при старте
            from moviebot.bot.bot_init import bot as global_bot
            bot = global_bot
        except Exception as e:
            logger.error(f"[SHOW_EPISODES_PAGE] bot is None и не удалось импортировать глобальный bot: {e}", exc_info=True)
            return False

        if bot is None:
            logger.error("[SHOW_EPISODES_PAGE] Глобальный bot также None. Невозможно продолжить.")
            return False

    conn_local = get_db_connection()
    cursor_local = get_db_cursor()
    
    try:
        logger.info(f"[SHOW EPISODES PAGE] Начало: kp_id={kp_id}, season={season_num}, chat_id={chat_id}, user_id={user_id}, page={page}, message_id={message_id}, message_thread_id={message_thread_id}")
        
        with db_lock:
            cursor_local.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
            row = cursor_local.fetchone()
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
        
        # Определяем количество серий на странице: если страниц больше 5, используем 80, иначе 20
        EPISODES_PER_PAGE_BASE = 20
        total_pages_base = (total_episodes + EPISODES_PER_PAGE_BASE - 1) // EPISODES_PER_PAGE_BASE
        
        if total_pages_base > 5:
            EPISODES_PER_PAGE = 80  # 20 строк × 4 столбца
        else:
            EPISODES_PER_PAGE = 20
        
        total_pages = (total_episodes + EPISODES_PER_PAGE - 1) // EPISODES_PER_PAGE
        page = max(1, min(page, total_pages))
        
        start_idx = (page - 1) * EPISODES_PER_PAGE
        end_idx = min(start_idx + EPISODES_PER_PAGE, total_episodes)
        page_episodes = episodes[start_idx:end_idx]
        
        text = f"Сезон {season_num}\n\n"
        if total_episodes > EPISODES_PER_PAGE:
            text += f"Страница {page}/{total_pages}\n\n"
        
        markup = InlineKeyboardMarkup()
        
        # Если страниц больше 5 (при расчете с 20 сериями на странице), используем сетку 4 столбца (4 кнопки в ряд, 80 серий на странице)
        # Иначе обычное расположение (2 колонки, 20 серий на странице)
        use_4_columns = total_pages_base > 5
        if use_4_columns:
            # Сетка 4 столбца по 20 строк (вертикальное заполнение):
            # Столбец 1: 1, 2, 3, 4, ...
            # Столбец 2: 21, 22, 23, 24, ...
            # Столбец 3: 41, 42, 43, 44, ...
            # Столбец 4: 61, 62, 63, 64, ...
            # Отображается как:
            # ⬜ 1  ⬜ 21 ⬜ 41 ⬜ 61
            # ⬜ 2  ⬜ 22 ⬜ 42 ⬜ 62
            # ⬜ 3  ⬜ 23 ⬜ 43 ⬜ 63
            # ...
            
            # Сначала создаем все кнопки
            buttons_list = []
            for ep in page_episodes:
                # ВАЖНО: Всегда приводим к строке для единообразия
                ep_num = str(ep.get('episodeNumber', ''))
                
                with db_lock:
                    cursor_local.execute('''
                        SELECT watched FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND season_number = %s AND episode_number = %s
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                    watched_row = cursor_local.fetchone()
                    is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                
                mark = "✅" if is_watched else "⬜"
                button_text = f"{mark} {ep_num}"
                if len(button_text) > 20:
                    button_text = button_text[:17] + "..."
                button = InlineKeyboardButton(button_text, callback_data=f"series_episode:{kp_id}:{season_num}:{ep_num}")
                buttons_list.append(button)
            
            # Теперь создаем вертикальные столбцы: 4 столбца, каждый по 20 кнопок (или меньше, если эпизодов меньше 80)
            COLUMNS_COUNT = 4
            ROWS_PER_COLUMN = 20
            
            # Создаем матрицу столбцов: каждый столбец - список кнопок
            columns = [[] for _ in range(COLUMNS_COUNT)]
            
            # Распределяем кнопки по столбцам вертикально
            # Нужно: Столбец 0: [0, 1, 2, ..., 19], Столбец 1: [20, 21, 22, ..., 39], и т.д.
            # Индекс кнопки i попадает в столбец i // ROWS_PER_COLUMN
            for i, button in enumerate(buttons_list):
                column_index = i // ROWS_PER_COLUMN  # 0 для 0-19, 1 для 20-39, 2 для 40-59, 3 для 60-79
                if column_index < COLUMNS_COUNT:
                    columns[column_index].append(button)
            
            # Теперь формируем ряды: берем по одной кнопке из каждого столбца
            # Идем по строкам: строка 0 = первая кнопка из каждого столбца, строка 1 = вторая кнопка, и т.д.
            max_rows = max(len(col) for col in columns) if columns else 0
            for row_idx in range(max_rows):
                row_buttons = []
                for col in columns:
                    if row_idx < len(col):
                        row_buttons.append(col[row_idx])
                if row_buttons:
                    markup.row(*row_buttons)
        else:
            # Обычное расположение: 2 колонки
            markup.row_width = 2
            for ep in page_episodes:
                # ВАЖНО: Всегда приводим к строке для единообразия
                ep_num = str(ep.get('episodeNumber', ''))
                
                with db_lock:
                    cursor_local.execute('''
                        SELECT watched FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                        AND season_number = %s AND episode_number = %s
                    ''', (chat_id, film_id, user_id, season_num, ep_num))
                    watched_row = cursor_local.fetchone()
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
                    nav_buttons.append(InlineKeyboardButton("Назад", callback_data=f"episodes_page:{kp_id}:{season_num}:{page-1}"))
                nav_buttons.append(InlineKeyboardButton(f"Страница {page}/{total_pages}", callback_data="noop"))
                if page < total_pages:
                    nav_buttons.append(InlineKeyboardButton("Вперёд", callback_data=f"episodes_page:{kp_id}:{season_num}:{page+1}"))
                if nav_buttons:
                    markup.row(*nav_buttons)
        
        text += "Нажмите на эпизод, чтобы отметить как просмотренный\n\n"
        text += "• одно нажатие на серию — отметка серии как просмотренной\n"
        text += "• повторное нажатие на отмеченную просмотренной серию — отметка всех серий до выбранной просмотренными"
        
        all_watched = True
        with db_lock:
            for ep in episodes:
                ep_num = ep.get('episodeNumber', '')
                cursor_local.execute('''
                    SELECT watched FROM series_tracking 
                    WHERE chat_id = %s AND film_id = %s AND user_id = %s 
                    AND season_number = %s AND episode_number = %s
                ''', (chat_id, film_id, user_id, season_num, ep_num))
                watched_row = cursor_local.fetchone()
                is_watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                if not is_watched:
                    all_watched = False
                    break
        
        logger.info(f"[SHOW EPISODES PAGE] Все эпизоды просмотрены: {all_watched}, страница {page}/{total_pages}")
        
        # Проверяем, есть ли состояние автоотметки для показа кнопки отмены
        # Кнопка показывается ТОЛЬКО после второго клика (когда была выполнена автоотметка)
        # Это значит, что в episodes должно быть больше одного эпизода (автоотметка нескольких серий)
        has_auto_mark = False
        if user_id in user_episode_auto_mark_state:
            auto_state = user_episode_auto_mark_state[user_id]
            if str(auto_state.get('kp_id')) == str(kp_id):
                auto_episodes = auto_state.get('episodes', [])
                # Проверяем, что это действительно автоотметка (больше одного эпизода)
                # или что это список эпизодов после автоотметки (не просто last_clicked_ep)
                if auto_episodes and len(auto_episodes) > 1:
                    has_auto_mark = True
                    markup.add(InlineKeyboardButton("❌ Отмена автоотметки", callback_data=f"series_episode_cancel_auto:{kp_id}:{season_num}"))
        
        if not all_watched:
            markup.add(InlineKeyboardButton("✅ Все просмотрены", callback_data=f"series_season_all:{kp_id}:{season_num}"))
        
        markup.add(InlineKeyboardButton("◀️ К сезонам", callback_data=f"series_track:{int(kp_id)}"))
        
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
                try:
                    bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
                    logger.info(f"[SHOW EPISODES PAGE] Сообщение обновлено успешно")
                except Exception as edit_e:
                    error_str = str(edit_e).lower()
                    if "message is not modified" in error_str:
                        # Если сообщение не изменилось, пытаемся обновить только клавиатуру
                        logger.warning(f"[SHOW EPISODES PAGE] Сообщение не изменилось, обновляю только клавиатуру")
                        try:
                            bot.edit_message_reply_markup(chat_id, message_id, reply_markup=markup)
                            logger.info(f"[SHOW EPISODES PAGE] Клавиатура обновлена успешно")
                        except Exception as markup_e:
                            logger.error(f"[SHOW EPISODES PAGE] Ошибка обновления клавиатуры: {markup_e}", exc_info=True)
                            # Последняя попытка - новое сообщение
                            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                    else:
                        raise  # Прокидываем другие ошибки дальше
            except Exception as e:
                logger.error(f"[SHOW EPISODES PAGE] Ошибка редактирования сообщения: {e}", exc_info=True)
                try:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
                except Exception as send_e:
                    logger.error(f"[SHOW EPISODES PAGE] Не удалось отправить новое сообщение: {send_e}", exc_info=True)
        else:
            logger.info(f"[SHOW EPISODES PAGE] Отправка нового сообщения")
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML', message_thread_id=message_thread_id)
        
        logger.info(f"[SHOW EPISODES PAGE] Завершено успешно")
        return True
    except Exception as e:
        logger.error(f"[EPISODES PAGE] Ошибка: {e}", exc_info=True)
        return False
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

def show_seasons_list(chat_id, user_id, message_id=None, message_thread_id=None, page=1, bot=None):
    """Основная функция показа списка сериалов с пагинацией"""
    if bot is None:
        logger.error("[SHOW_SEASONS_LIST] bot is None")
        return

    series_data = get_user_series_page(chat_id, user_id, page=page)

    if not series_data['items']:
        text = "У вас пока нет активных сериалов в списке.\nДобавьте их через поиск!"
        
        # Создаем кнопки как в примере
        markup = InlineKeyboardMarkup(row_width=1)
        
        # Проверяем, есть ли просмотренные сериалы - используем ту же логику, что и в основном списке
        has_access = has_notifications_access(chat_id, user_id)
        if has_access:
            # Проверяем реально просмотренные сериалы (по эпизодам)
            watched_count = 0
            conn_check = get_db_connection()
            cursor_check = None
            try:
                with db_lock:
                    cursor_check = conn_check.cursor()
                    cursor_check.execute('SELECT id, kp_id FROM movies WHERE chat_id = %s AND is_series = 1', (chat_id,))
                    all_series_rows = cursor_check.fetchall()
                
                # Для каждого сериала проверяем, просмотрен ли он полностью
                for row in all_series_rows:
                    film_id_check = row.get('id') if isinstance(row, dict) else row[0]
                    kp_id_check = row.get('kp_id') if isinstance(row, dict) else row[1]
                    
                    is_airing_check, _ = get_series_airing_status(kp_id_check)
                    if is_airing_check:
                        continue
                    
                    seasons_data_check = get_seasons_data(kp_id_check)
                    if not seasons_data_check:
                        continue
                    
                    watched_set_check = set()
                    conn_watch_check = get_db_connection()
                    cursor_watch_check = None
                    try:
                        with db_lock:
                            cursor_watch_check = conn_watch_check.cursor()
                            cursor_watch_check.execute('''
                                SELECT season_number, episode_number FROM series_tracking 
                                WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                            ''', (chat_id, film_id_check, user_id))
                            for w_row in cursor_watch_check.fetchall():
                                s_num = str(w_row.get('season_number') if isinstance(w_row, dict) else w_row[0])
                                e_num = str(w_row.get('episode_number') if isinstance(w_row, dict) else w_row[1])
                                watched_set_check.add((s_num, e_num))
                    finally:
                        if cursor_watch_check:
                            try:
                                cursor_watch_check.close()
                            except:
                                pass
                        try:
                            conn_watch_check.close()
                        except:
                            pass
                    
                    total_ep_check, watched_ep_check = count_episodes_for_watch_check(
                        seasons_data_check, False, watched_set_check, chat_id, film_id_check, user_id
                    )
                    
                    if total_ep_check == watched_ep_check and total_ep_check > 0:
                        watched_count += 1
                
                if watched_count > 0:
                    markup.add(InlineKeyboardButton(f"✅ Просмотренные ({watched_count})", callback_data="watched_series_list"))
            finally:
                if cursor_check:
                    try:
                        cursor_check.close()
                    except:
                        pass
                try:
                    conn_check.close()
                except:
                    pass
        
        markup.add(InlineKeyboardButton("🔍 Поиск фильмов и сериалов", callback_data="start_menu:search"))
        markup.add(InlineKeyboardButton("⬅️ Назад к режимам", callback_data="start_menu:seasons"))
        
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
            logger.error(f"[SHOW_SEASONS_LIST] Ошибка отправки пустого списка: {e}")
        return

    items = series_data['items']

    # Текст сообщения — короткий и чистый
    text = f"<b>📺 Твои сериалы</b> ({series_data['total_count']} шт.)\n\n"
    unwatched_count = series_data.get('unwatched_count', series_data['total_count'])
    if series_data['total_pages'] > 1:
        text += f"<i>Страница {page}/{series_data['total_pages']}</i>\n\n"
    
    # Добавляем легенду эмодзи — коротко и понятно
    text += (
        "<b>Что означают значки:</b>\n"
        "🟢 — сериал продолжается\n"
        "🔴 — сериал завершён\n"
        "🔔 — на него есть твоя подписка\n"
        "⏳ — ещё не все сезоны просмотрены\n\n"
        "Нажми на сериал → описание и сезоны"
    )

    markup = InlineKeyboardMarkup(row_width=1)

    for item in items:
        kp_id = item['kp_id']
        title = item['title']
        year = item['year']
        watched = item['watched_count']

        # Обновление кэша (оставляем как есть)
        # Исправление: используем UTC для сравнения с last_api_update из БД
        from moviebot.config import PLANS_TZ
        import pytz
        now_utc = datetime.now(pytz.utc)
        last_update = item['last_api_update']
        # Если last_api_update не имеет timezone, добавляем UTC
        if last_update and last_update.tzinfo is None:
            last_update = pytz.utc.localize(last_update)
        elif last_update and last_update.tzinfo:
            # Если уже с timezone, конвертируем в UTC для сравнения
            last_update = last_update.astimezone(pytz.utc)
        
        need_update = (
            last_update is None or
            (now_utc - last_update) > timedelta(days=1)
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

            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            try:
                with db_lock:
                    cursor_local.execute("""
                        UPDATE movies 
                        SET is_ongoing = %s, seasons_count = %s, next_episode = %s, last_api_update = NOW()
                        WHERE chat_id = %s AND kp_id = %s
                    """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                    conn_local.commit()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass

            item['is_ongoing'] = is_airing
            item['seasons_count'] = seasons_count
            item['next_episode'] = next_ep

        # Строгий порядок эмодзи
        emojis = ""
        if item['is_ongoing']:
            emojis += "🟢"
            if item['has_subscription']:
                emojis += "🔔"
            if watched > 0:
                emojis += "⏳"
        else:
            emojis += "🔴"
            if item['has_subscription']:
                emojis += "🔔"
            if watched > 0:
                emojis += "⏳"

        # Кнопка
        button_text = f"{emojis} {title} ({year})"

        # Обрезка длинных названий
        if len(button_text) > 62:
            available_len = 62 - len(emojis) - len(f" ({year})") - 4
            short_title = title[:available_len] + "..."
            button_text = f"{emojis} {short_title} ({year})"

        markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{int(kp_id)}"))

    # Кнопка "Просмотренные" - показываем только если есть реально просмотренные сериалы
    has_access = has_notifications_access(chat_id, user_id)
    if has_access:
        # Проверяем реально просмотренные сериалы (по эпизодам, а не по полю watched)
        # Используем ту же логику, что и в show_completed_series_list
        watched_count = 0
        conn_check = get_db_connection()
        cursor_check = None
        try:
            with db_lock:
                cursor_check = conn_check.cursor()
                # Получаем все сериалы пользователя
                cursor_check.execute('SELECT id, kp_id FROM movies WHERE chat_id = %s AND is_series = 1', (chat_id,))
                all_series_rows = cursor_check.fetchall()
            
            # Для каждого сериала проверяем, просмотрен ли он полностью
            for row in all_series_rows:
                film_id_check = row.get('id') if isinstance(row, dict) else row[0]
                kp_id_check = row.get('kp_id') if isinstance(row, dict) else row[1]
                
                # Получаем статус выхода сериала
                is_airing_check, _ = get_series_airing_status(kp_id_check)
                if is_airing_check:
                    continue  # Выпускающиеся сериалы не могут быть полностью просмотрены
                
                seasons_data_check = get_seasons_data(kp_id_check)
                if not seasons_data_check:
                    continue
                
                # Собираем просмотренные эпизоды
                watched_set_check = set()
                conn_watch_check = get_db_connection()
                cursor_watch_check = None
                try:
                    with db_lock:
                        cursor_watch_check = conn_watch_check.cursor()
                        cursor_watch_check.execute('''
                            SELECT season_number, episode_number FROM series_tracking 
                            WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                        ''', (chat_id, film_id_check, user_id))
                        for w_row in cursor_watch_check.fetchall():
                            s_num = str(w_row.get('season_number') if isinstance(w_row, dict) else w_row[0])
                            e_num = str(w_row.get('episode_number') if isinstance(w_row, dict) else w_row[1])
                            watched_set_check.add((s_num, e_num))
                finally:
                    if cursor_watch_check:
                        try:
                            cursor_watch_check.close()
                        except:
                            pass
                    try:
                        conn_watch_check.close()
                    except:
                        pass
                
                # Считаем эпизоды
                total_ep_check, watched_ep_check = count_episodes_for_watch_check(
                    seasons_data_check, False, watched_set_check, chat_id, film_id_check, user_id
                )
                
                # Если все эпизоды просмотрены - увеличиваем счетчик
                if total_ep_check == watched_ep_check and total_ep_check > 0:
                    watched_count += 1
            
            if watched_count > 0:
                markup.add(InlineKeyboardButton(f"✅ Просмотренные ({watched_count})", callback_data="watched_series_list"))
        finally:
            if cursor_check:
                try:
                    cursor_check.close()
                except:
                    pass
            try:
                conn_check.close()
            except:
                pass
    
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
            edit_kwargs = common_kwargs.copy()
            edit_kwargs.pop('message_thread_id', None)  # ← убираем то, что edit не жрёт
            edit_kwargs['message_id'] = message_id
            try:
                bot.edit_message_text(**edit_kwargs)
            except telebot.apihelper.ApiTelegramException as api_exc:
                if api_exc.error_code == 400 and "message is not modified" in str(api_exc).lower():
                    logger.debug("[SHOW_SEASONS_LIST] Сообщение не изменилось — пропускаем")
                    # Это нормальная ситуация при повторном вызове той же страницы
                    pass
                else:
                    raise  # кидаем все остальные ошибки дальше
        else:
            bot.send_message(**common_kwargs)

    except Exception as e:
        logger.error(f"[SHOW_SEASONS_LIST] Ошибка редактирования/отправки: {e}", exc_info=True)
        # Фоллбек — отправляем новое сообщение, если редактирование совсем сломалось
        if not message_id:  # Если это было send_message, не отправляем фоллбек
            return
        try:
            send_kwargs = {
                'text': text,
                'chat_id': chat_id,
                'reply_markup': markup,
                'parse_mode': 'HTML',
                'disable_web_page_preview': True
            }
            if message_thread_id is not None:
                send_kwargs['message_thread_id'] = message_thread_id
            bot.send_message(**send_kwargs)
        except Exception as send_e:
            logger.error(f"[SHOW_SEASONS_LIST] Полный фейл отправки: {send_e}")

def show_completed_series_list(chat_id: int, user_id: int, message_id: int = None, message_thread_id: int = None, bot=None):
    if bot is None:
        logger.error("[SHOW_COMPLETED_SERIES_LIST] bot is None!")
        return

    logger.info(f"[SHOW_COMPLETED_SERIES_LIST] chat_id={chat_id}, user_id={user_id}, message_id={message_id}")

    has_access = has_notifications_access(chat_id, user_id)
    
    conn_local = get_db_connection()
    cursor_local = None
    
    try:
        with db_lock:
            cursor_local = conn_local.cursor()
            cursor_local.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
            all_series = cursor_local.fetchall()
        
        completed_series = []
        for row in all_series:
            film_id = row.get('id') if row else None
            title = row.get('title')
            kp_id = row.get('kp_id')

            # Получаем статус выхода сериала
            is_airing, _ = get_series_airing_status(kp_id)
            seasons_data = get_seasons_data(kp_id)

            # Собираем ВСЕ отмеченные серии пользователя - используем локальный курсор
            watched_set = set()
            conn_watch = get_db_connection()
            cursor_watch = None
            try:
                with db_lock:
                    cursor_watch = conn_watch.cursor()
                    cursor_watch.execute('''
                        SELECT season_number, episode_number FROM series_tracking 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s AND watched = TRUE
                    ''', (chat_id, film_id, user_id))
                    for w_row in cursor_watch.fetchall():
                        s_num = str(w_row.get('season_number') if isinstance(w_row, dict) else w_row[0])
                        e_num = str(w_row.get('episode_number') if isinstance(w_row, dict) else w_row[1])
                        watched_set.add((s_num, e_num))
            finally:
                if cursor_watch:
                    try:
                        cursor_watch.close()
                    except:
                        pass
                try:
                    conn_watch.close()
                except:
                    pass

            # Считаем ВСЕ эпизоды сериала и сколько просмотрено
            total_ep, watched_ep = count_episodes_for_watch_check(seasons_data, is_airing, watched_set, chat_id, film_id, user_id)

            logger.info(f"[SHOW_COMPLETED_SERIES_LIST] {title} (kp_id={kp_id}): total_ep={total_ep}, watched_ep={watched_ep}, is_airing={is_airing}")

            # Проверяем также поле watched в таблице movies - используем локальный курсор
            conn_check = get_db_connection()
            cursor_check = None
            movie_watched = False
            try:
                with db_lock:
                    cursor_check = conn_check.cursor()
                    cursor_check.execute('SELECT watched FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    movie_row = cursor_check.fetchone()
                    if movie_row:
                        movie_watched = bool(movie_row.get('watched') if isinstance(movie_row, dict) else movie_row[0])
            finally:
                if cursor_check:
                    try:
                        cursor_check.close()
                    except:
                        pass
                try:
                    conn_check.close()
                except:
                    pass
            
            # Условие: все эпизоды просмотрены, сериал завершён, есть хотя бы один эпизод
            # НЕ требуем movie_watched, так как оно может быть не обновлено
            if total_ep == watched_ep and total_ep > 0 and not is_airing:
                button_text = f"✅ {title}"
                completed_series.append((kp_id, button_text))
                logger.info(f"[SHOW_COMPLETED_SERIES_LIST] Сериал {title} добавлен в просмотренные: total={total_ep}, watched={watched_ep}")
            else:
                logger.info(f"[SHOW_COMPLETED_SERIES_LIST] Сериал {title} НЕ завершён: total={total_ep}, watched={watched_ep}, airing={is_airing}, movie_watched={movie_watched}")

    finally:
        if cursor_local:
            try:
                cursor_local.close()
            except:
                pass
        try:
            conn_local.close()
        except:
            pass

    # Общий kwargs для отправки/редактирования
    if not completed_series:
        text = "Нет полностью просмотренных сериалов."
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))
    else:
        text = f"✅ Просмотренные сериалы ({len(completed_series)})"
        markup = InlineKeyboardMarkup(row_width=1)
        for kp_id, button_text in completed_series:
            markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{int(kp_id)}"))
        markup.add(InlineKeyboardButton("◀️ К активным сериалам", callback_data="back_to_seasons_list"))

    try:
        common_kwargs = {
            'text': text,
            'chat_id': chat_id,
            'reply_markup': markup,
            'parse_mode': 'HTML'
        }

        if message_id:
            # edit_message_text не поддерживает message_thread_id
            edit_kwargs = common_kwargs.copy()
            edit_kwargs['message_id'] = message_id
            edit_kwargs.pop('message_thread_id', None)
            try:
                bot.edit_message_text(**edit_kwargs)
            except Exception as edit_e:
                # Обрабатываем ошибку "message is not modified" - это нормально
                if "message is not modified" in str(edit_e).lower():
                    logger.debug(f"[SHOW_COMPLETED_SERIES_LIST] Сообщение не изменилось (это нормально)")
                else:
                    raise
        else:
            # send_message поддерживает message_thread_id
            if message_thread_id is not None:
                common_kwargs['message_thread_id'] = message_thread_id
            bot.send_message(**common_kwargs)
    except Exception as e:
        logger.error(f"[SHOW_COMPLETED_SERIES_LIST] Ошибка отправки/редактирования: {e}", exc_info=True)
            
@bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
def handle_seasons_kp(call):
    """Клик по сериалу в /seasons → показываем стандартное описание с постером и кнопками"""
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю описание...")

        kp_id_str = call.data.split(":")[1]
        kp_id = int(kp_id_str)  # для логов и вызовов
        kp_id_db = str(kp_id)   # для SQL-запросов (kp_id в БД — TEXT)

        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id
        message_thread_id = getattr(call.message, 'message_thread_id', None)

        logger.info(f"[SEASONS_KP → ОПИСАНИЕ] kp_id={kp_id}, chat_id={chat_id}, user_id={user_id}")

        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT id, title, watched, link, year, genres, description, director, actors, is_series
                    FROM movies WHERE chat_id = %s AND kp_id = %s
                ''', (chat_id, kp_id_db))

                row = cursor_local.fetchone()

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
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
    except Exception as e:
        logger.error(f"[SEASONS_KP → ОПИСАНИЕ] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка загрузки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data == "watched_series_list")
def handle_watched_series_list(call):
    """Обработчик кнопки 'Просмотренные сериалы'"""
    try:
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
    except Exception as e:
        logger.error(f"[WATCHED_SERIES_LIST] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

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

def get_user_series_page(chat_id: int, user_id: int, page: int = 1, page_size: int = 5):
    """Возвращает страницу сериалов пользователя с пагинацией"""
    items = []
    total_count = 0
    total_pages = 1

    conn_local = get_db_connection()
    cursor_local = get_db_cursor()

    try:
        with db_lock:
            # Используем локальное соединение
            # Основной большой запрос
            cursor_local.execute("""
                SELECT 
                    m.id AS film_id,
                    m.kp_id,
                    m.title,
                    m.year,
                    COALESCE(m.poster_url, '') AS poster_url,
                    m.link,
                    COALESCE(m.is_ongoing, FALSE) AS is_ongoing,
                    COALESCE(m.seasons_count, 0) AS seasons_count,
                    m.next_episode,
                    m.last_api_update,
                    COUNT(st.id) AS watched_episodes_count,
                    BOOL_OR(ss.subscribed = TRUE) AS has_subscription,
                    (COALESCE(m.watched, 0) = 1) AS all_watched
                FROM movies m
                LEFT JOIN series_tracking st 
                    ON st.film_id = m.id 
                    AND st.chat_id = %s 
                    AND st.user_id = %s
                    AND st.watched = TRUE
                LEFT JOIN series_subscriptions ss 
                    ON ss.film_id = m.id 
                    AND ss.chat_id = %s 
                    AND ss.user_id = %s
                    AND ss.subscribed = TRUE
                WHERE m.chat_id = %s AND m.is_series = 1
                GROUP BY m.id
                ORDER BY m.id DESC
            """, (chat_id, user_id, chat_id, user_id, chat_id))

            rows = cursor_local.fetchall()

            for row in rows:
                # Безопасное извлечение данных с учетом RealDictCursor
                film_id = row.get('film_id') if isinstance(row, dict) else row[0]
                kp_id = row.get('kp_id') if isinstance(row, dict) else row[1]
                title = row.get('title') if isinstance(row, dict) else row[2]
                year = row.get('year') if isinstance(row, dict) else row[3]
                poster_url = row.get('poster_url') if isinstance(row, dict) else (row[4] if len(row) > 4 else '')
                link = row.get('link') if isinstance(row, dict) else (row[5] if len(row) > 5 else None)
                is_ongoing = bool(row.get('is_ongoing') if isinstance(row, dict) else (row[6] if len(row) > 6 else False))
                seasons_count = row.get('seasons_count') if isinstance(row, dict) else (row[7] if len(row) > 7 else 0)
                next_episode_raw = row.get('next_episode') if isinstance(row, dict) else (row[8] if len(row) > 8 else None)
                last_api_update = row.get('last_api_update') if isinstance(row, dict) else (row[9] if len(row) > 9 else None)
                watched_count = row.get('watched_episodes_count') if isinstance(row, dict) else (row[10] if len(row) > 10 else 0)
                has_subscription = bool(row.get('has_subscription') if isinstance(row, dict) else (row[11] if len(row) > 11 else False))
                all_watched = bool(row.get('all_watched') if isinstance(row, dict) else (row[12] if len(row) > 12 else False))
                
                # Обработка next_episode
                next_episode = next_episode_raw
                if isinstance(next_episode, str):
                    try:
                        next_episode = json.loads(next_episode)
                    except:
                        next_episode = None

                items.append({
                    'film_id': film_id,
                    'kp_id': kp_id,
                    'title': title,
                    'year': year,
                    'poster_url': poster_url or '',
                    'link': link or f"https://www.kinopoisk.ru/series/{kp_id}/",
                    'is_ongoing': is_ongoing,
                    'seasons_count': seasons_count or 0,
                    'next_episode': next_episode,
                    'last_api_update': last_api_update,
                    'watched_count': watched_count or 0,
                    'has_subscription': has_subscription,
                    'all_watched': all_watched,
                })
            
            # Сортировка по приоритету: ВСЕ начатые сериалы выше не начатых
            # Среди начатых: с подпиской выше без подписки, выходящие выше не выходящих
            # Среди не начатых: с подпиской выше без подписки, выходящие выше не выходящих
            def get_sort_priority(item):
                is_ongoing = item['is_ongoing'] or False
                has_subscription = item['has_subscription'] or False
                watched_count = item['watched_count'] or 0
                is_started = watched_count > 0  # Начатый = watched_count > 0
                
                # НАЧАТЫЕ сериалы (приоритет 1-4) - ВСЕГДА выше не начатых
                if is_started:
                    if is_ongoing and has_subscription:
                        return 1  # Начатые выходящие с подпиской
                    elif is_ongoing and not has_subscription:
                        return 2  # Начатые выходящие без подписки
                    elif not is_ongoing and has_subscription:
                        return 3  # Начатые не выходящие с подпиской
                    else:
                        return 4  # Начатые не выходящие без подписки
                # НЕ НАЧАТЫЕ сериалы (приоритет 5-8) - ВСЕГДА ниже начатых
                else:
                    if is_ongoing and has_subscription:
                        return 5  # Не начатые выходящие с подпиской
                    elif is_ongoing and not has_subscription:
                        return 6  # Не начатые выходящие без подписки
                    elif not is_ongoing and has_subscription:
                        return 7  # Не начатые не выходящие с подпиской
                    else:
                        return 8  # Не начатые не выходящие без подписки
            
            # Разделяем на непросмотренные и просмотренные
            unwatched_items = [item for item in items if not item.get('all_watched', False)]
            watched_items = [item for item in items if item.get('all_watched', False)]
            
            # Сортируем непросмотренные по приоритету
            unwatched_items.sort(key=get_sort_priority)
            # Просмотренные сортируем по названию
            watched_items.sort(key=lambda x: x.get('title', ''))
            
            # Пагинация только для непросмотренных
            unwatched_count = len(unwatched_items)
            unwatched_total_pages = math.ceil(unwatched_count / page_size) if unwatched_count > 0 else 1
            unwatched_offset = (page - 1) * page_size
            unwatched_page_items = unwatched_items[unwatched_offset:unwatched_offset + page_size]
            
            # Просмотренные сериалы НЕ показываются в общем списке - только в разделе "Просмотренные"
            items = unwatched_page_items
            total_count = unwatched_count
            total_pages = unwatched_total_pages

    except psycopg2.InterfaceError as e:
        logger.error(f"[GET_USER_SERIES_PAGE] Cursor error: {e}")
        try:
            conn_local.rollback()
        except:
            pass
        return {'items': [], 'total_pages': 1, 'total_count': 0, 'unwatched_count': 0, 'current_page': page}

    except Exception as e:
        logger.error(f"[GET_USER_SERIES_PAGE] Ошибка: {e}", exc_info=True)
        try:
            conn_local.rollback()
        except:
            pass
        return {'items': [], 'total_pages': 1, 'total_count': 0, 'unwatched_count': 0, 'current_page': page}
    finally:
        try:
            cursor_local.close()
        except:
            pass
        try:
            conn_local.close()
        except:
            pass

    # Вычисляем unwatched_count если его еще нет (для случая ошибок)
    if 'unwatched_count' not in locals():
        unwatched_count = len([i for i in items if not i.get('all_watched', False)]) if items else 0
    
    return {
        'items': items,
        'total_pages': total_pages,
        'total_count': total_count,
        'unwatched_count': unwatched_count,
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
                
                # Сериализуем next_ep с обработкой datetime
                def default_serializer(o):
                    if isinstance(o, datetime):
                        return o.isoformat()
                    raise TypeError("not serializable")
                
                next_ep_json = json.dumps(next_ep, default=default_serializer) if next_ep else None
                conn_local = get_db_connection()
                cursor_local = get_db_cursor()
                try:
                    with db_lock:
                        cursor_local.execute("""
                            UPDATE movies SET is_ongoing = %s, seasons_count = %s, next_episode = %s, last_api_update = NOW()
                            WHERE chat_id = %s AND kp_id = %s
                        """, (is_airing, seasons_count, next_ep_json, chat_id, kp_id))
                        conn_local.commit()
                finally:
                    try:
                        cursor_local.close()
                    except:
                        pass
                    try:
                        conn_local.close()
                    except:
                        pass
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