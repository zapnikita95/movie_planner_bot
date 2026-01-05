"""
Обработчики команды /seasons
"""
import logging
from datetime import datetime as dt
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_operations import log_request
from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock
from moviebot.utils.helpers import has_notifications_access
from moviebot.api.kinopoisk_api import get_seasons_data, extract_movie_info

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
                        # Считаем только вышедшие эпизоды
                        if release_date and release_date <= now:
                            should_count = True
                    except:
                        pass
                else:
                    # Если дата не указана - считаем как вышедший (для старых сериалов)
                    should_count = True
            else:
                # Для завершённых сериалов считаем все эпизоды
                should_count = True
            
            if should_count:
                total_episodes += 1
                if (season_num, ep_num) in watched_set:
                    watched_episodes += 1
    
    return total_episodes, watched_episodes


def register_seasons_handlers(bot):
    """Регистрирует обработчики команды /seasons"""
    
    @bot.message_handler(commands=['seasons'])
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
            bot.reply_to(
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
            
            # Проверяем статус выхода сериала (для всех, независимо от доступа)
            is_airing = False
            try:
                is_airing, _ = get_series_airing_status(kp_id)
            except Exception as e:
                logger.warning(f"[SEASONS] Ошибка проверки статуса выхода для kp_id={kp_id}: {e}")
            
            # Проверяем статус просмотра в БД (для всех, независимо от доступа)
            watched_in_db = False
            with db_lock:
                cursor.execute("SELECT watched FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                watched_row = cursor.fetchone()
                if watched_row:
                    watched_in_db = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            
            # Проверяем статус просмотра эпизодов (только если есть доступ)
            all_episodes_watched = False
            has_some_watched = False
            if has_access:
                seasons_data = get_seasons_data(kp_id)
                if seasons_data:
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
                    
                    # Подсчитываем эпизоды
                    total_episodes, watched_episodes = count_episodes_for_watch_check(
                        seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                    )
                    
                    if total_episodes > 0:
                        if watched_episodes == total_episodes:
                            all_episodes_watched = True
                        elif watched_episodes > 0:
                            has_some_watched = True
            
            # Если сериал помечен как просмотренный в БД, считаем его полностью просмотренным
            if watched_in_db:
                all_episodes_watched = True
            
            # Классифицируем сериал
            series_info = {
                'title': title,
                'kp_id': kp_id,
                'film_id': film_id,
                'is_subscribed': is_subscribed,
                'all_watched': all_episodes_watched,
                'is_airing': is_airing
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
            # Добавляем статус выхода сериала
            airing_emoji = "🟢" if series_info.get('is_airing', False) else "🔴"
            # Добавляем колокольчик, если есть подписка
            subscription_emoji = "🔔 " if series_info.get('is_subscribed', False) else ""
            button_text = f"👁️ {airing_emoji} {subscription_emoji}{series_info['title']}"
            if len(button_text) > 30:
                button_text = button_text[:27] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
        
        # Не просмотренные сериалы
        for series_info in not_watched_series:
            # Добавляем статус выхода сериала
            airing_emoji = "🟢" if series_info.get('is_airing', False) else "🔴"
            # Добавляем колокольчик, если есть подписка
            subscription_emoji = "🔔 " if series_info.get('is_subscribed', False) else ""
            button_text = f"{airing_emoji} {subscription_emoji}{series_info['title']}"
            if len(button_text) > 30:
                button_text = button_text[:27] + "..."
            markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
        
        # Добавляем кнопку "Просмотренные сериалы" если есть просмотренные сериалы
        if fully_watched_series:
            watched_button_text = "✅ Просмотренные"
            if len(fully_watched_series) > 0:
                # Показываем количество просмотренных сериалов
                watched_button_text = f"✅ Просмотренные ({len(fully_watched_series)})"
            markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_series_list"))
        
        # Добавляем кнопку "Назад в меню"
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        # Сохраняем message_id для возможности вернуться назад
        bot.reply_to(message, "📺 <b>Выберите сериал:</b>", reply_markup=markup, parse_mode='HTML')

    @bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_locked:"))
    def seasons_locked_callback(call):
        """Обработчик заблокированных кнопок сериалов"""
        try:
            bot.answer_callback_query(
                call.id, 
                "🔒 Функционал можно подключить через /payment", 
                show_alert=True
            )
        except Exception as e:
            logger.error(f"[SEASONS] ERROR in seasons_locked_callback: {e}", exc_info=True)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("seasons_kp:"))
    def show_seasons_callback(call):
        """Показывает описание выбранного сериала"""
        try:
            kp_id = call.data.split(":")[1]
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            
            logger.info(f"[SHOW SEASONS] Начало: user_id={user_id}, chat_id={chat_id}, kp_id={kp_id}")
            
            # Отвечаем на callback_query сразу для улучшения отзывчивости
            bot.answer_callback_query(call.id)
            
            # Получаем информацию о сериале из базы
            with db_lock:
                cursor.execute("SELECT id, title, link FROM movies WHERE chat_id = %s AND kp_id = %s", (chat_id, kp_id))
                row = cursor.fetchone()
                if not row:
                    bot.answer_callback_query(call.id, "❌ Сериал не найден в базе", show_alert=True)
                    return
                
                film_id = row.get('id') if isinstance(row, dict) else row[0]
                title = row.get('title') if isinstance(row, dict) else row[1]
                link = row.get('link') if isinstance(row, dict) else row[2]
                
                # Проверяем, просмотрен ли сериал
                cursor.execute("SELECT watched FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                watched_row = cursor.fetchone()
                watched = watched_row and (watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
            
            # Получаем информацию о сериале через API
            info = extract_movie_info(link)
            
            if not info:
                bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о сериале", show_alert=True)
                return
            
            # Формируем existing для передачи в show_film_info_with_buttons
            existing = (film_id, title, watched)
            
            # Показываем описание сериала со всеми кнопками
            # TODO: Импортировать show_film_info_with_buttons из handlers/series.py когда будет реализовано
            # Временно: используем прямой вызов через bot для отображения информации
            # Функция show_film_info_with_buttons будет реализована в handlers/series.py
            try:
                # Пытаемся импортировать из handlers/series (когда будет реализовано)
                from moviebot.bot.handlers.series import show_film_info_with_buttons
                show_film_info_with_buttons(bot, chat_id, user_id, info, link, kp_id, existing)
            except (ImportError, AttributeError):
                # Временная заглушка: показываем базовую информацию о сериале
                logger.warning("[SEASONS] show_film_info_with_buttons не найден, используем временное отображение")
                is_series = info.get('is_series', False)
                type_emoji = "📺" if is_series else "🎬"
                text = f"{type_emoji} <b>{info['title']}</b> ({info['year'] or '—'})\n"
                if info.get('director'):
                    text += f"<i>Режиссёр:</i> {info['director']}\n"
                if info.get('genres'):
                    text += f"<i>Жанры:</i> {info['genres']}\n"
                if info.get('description'):
                    text += f"\n<i>Кратко:</i> {info['description']}\n"
                text += f"\n<a href='{link}'>Кинопоиск</a>"
                
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="seasons_list"))
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            
        except Exception as e:
            logger.error(f"[SEASONS] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "seasons_list")
    def seasons_list_callback(call):
        """Обработчик возврата к списку сериалов"""
        try:
            bot.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            message_id = call.message.message_id
            
            # Получаем список сериалов
            with db_lock:
                cursor.execute('SELECT id, title, kp_id FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
                series = cursor.fetchall()
            
            if not series:
                bot.edit_message_text("📺 Нет сериалов в базе.", chat_id, message_id, parse_mode='HTML')
                return
            
            # Разделяем сериалы на категории
            fully_watched_series = []
            partially_watched_series = []
            not_watched_series = []
            
            user_id = call.from_user.id
            
            # Проверяем доступ к функциям уведомлений
            has_access = has_notifications_access(chat_id, user_id)
            
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
                
                # Проверяем статус выхода сериала (для всех, независимо от доступа)
                is_airing = False
                try:
                    is_airing, _ = get_series_airing_status(kp_id)
                except Exception as e:
                    logger.warning(f"[SEASONS LIST] Ошибка проверки статуса выхода для kp_id={kp_id}: {e}")
                
                # Проверяем статус просмотра в БД (для всех, независимо от доступа)
                watched_in_db = False
                with db_lock:
                    cursor.execute("SELECT watched FROM movies WHERE id = %s AND chat_id = %s", (film_id, chat_id))
                    watched_row = cursor.fetchone()
                    if watched_row:
                        watched_in_db = bool(watched_row.get('watched') if isinstance(watched_row, dict) else watched_row[0])
                
                # Проверяем статус просмотра эпизодов (только если есть доступ)
                all_episodes_watched = False
                has_some_watched = False
                if has_access:
                    seasons_data = get_seasons_data(kp_id)
                    if seasons_data:
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
                        
                        # Подсчитываем эпизоды
                        total_episodes, watched_episodes = count_episodes_for_watch_check(
                            seasons_data, is_airing, watched_set, chat_id, film_id, user_id
                        )
                        
                        if total_episodes > 0:
                            if watched_episodes == total_episodes:
                                all_episodes_watched = True
                            elif watched_episodes > 0:
                                has_some_watched = True
                
                # Если сериал помечен как просмотренный в БД, считаем его полностью просмотренным
                if watched_in_db:
                    all_episodes_watched = True
                
                # Классифицируем сериал
                series_info = {
                    'title': title,
                    'kp_id': kp_id,
                    'film_id': film_id,
                    'is_subscribed': is_subscribed,
                    'all_watched': all_episodes_watched,
                    'is_airing': is_airing
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
                # Добавляем статус выхода сериала
                airing_emoji = "🟢" if series_info.get('is_airing', False) else "🔴"
                # Добавляем колокольчик, если есть подписка
                subscription_emoji = "🔔 " if series_info.get('is_subscribed', False) else ""
                button_text = f"👁️ {airing_emoji} {subscription_emoji}{series_info['title']}"
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
            
            # Не просмотренные сериалы
            for series_info in not_watched_series:
                # Добавляем статус выхода сериала
                airing_emoji = "🟢" if series_info.get('is_airing', False) else "🔴"
                # Добавляем колокольчик, если есть подписка
                subscription_emoji = "🔔 " if series_info.get('is_subscribed', False) else ""
                button_text = f"{airing_emoji} {subscription_emoji}{series_info['title']}"
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
            
            # Добавляем кнопку "Просмотренные сериалы" если есть просмотренные
            if fully_watched_series:
                watched_button_text = "✅ Просмотренные"
                if len(fully_watched_series) > 0:
                    watched_button_text = f"✅ Просмотренные ({len(fully_watched_series)})"
                markup.add(InlineKeyboardButton(watched_button_text, callback_data="watched_series_list"))
            
            bot.edit_message_text("📺 <b>Выберите сериал:</b>", chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[SEASONS LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass

    @bot.callback_query_handler(func=lambda call: call.data == "watched_series_list")
    def watched_series_list_callback(call):
        """Обработчик показа просмотренных сериалов (не выходящие + все серии просмотрены)"""
        try:
            bot.answer_callback_query(call.id)
            chat_id = call.message.chat.id
            user_id = call.from_user.id
            message_id = call.message.message_id
            
            # Проверяем доступ к функциям уведомлений
            has_access = has_notifications_access(chat_id, user_id)
            
            # Получаем все сериалы
            with db_lock:
                cursor.execute('SELECT id, title, kp_id, watched FROM movies WHERE chat_id = %s AND is_series = 1 ORDER BY title', (chat_id,))
                series = cursor.fetchall()
            
            if not series:
                bot.edit_message_text("📺 Нет сериалов в базе.", chat_id, message_id, parse_mode='HTML')
                return
            
            watched_series = []
            now = dt.now()
            
            for row in series:
                if isinstance(row, dict):
                    film_id = row.get('id')
                    title = row.get('title')
                    kp_id = row.get('kp_id')
                    watched_in_db = bool(row.get('watched'))
                else:
                    film_id = row[0]
                    title = row[1]
                    kp_id = row[2]
                    watched_in_db = bool(row[3]) if len(row) > 3 else False
                
                # Если сериал помечен как просмотренный в БД, добавляем его в список
                if watched_in_db:
                    watched_series.append({
                        'title': title,
                        'kp_id': kp_id,
                        'film_id': film_id,
                        'total_episodes': 0  # Не важно для отображения
                    })
                    continue
                
                # Если нет доступа, пропускаем проверку эпизодов
                if not has_access:
                    continue
                
                # Получаем данные о сезонах
                seasons_data = get_seasons_data(kp_id)
                if not seasons_data:
                    continue
                
                # Проверяем, выходит ли сериал (есть ли будущие эпизоды)
                is_airing = False
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
                                    is_airing = True
                                    break
                            except:
                                pass
                    if is_airing:
                        break
                
                # Если сериал выходит, пропускаем
                if is_airing:
                    continue
                
                # Проверяем, все ли серии просмотрены
                all_watched = True
                total_episodes = 0
                watched_episodes = 0
                
                # Получаем просмотренные эпизоды из базы
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
                            watched_set.add((w_row.get('season_number'), w_row.get('episode_number')))
                        else:
                            watched_set.add((w_row[0], w_row[1]))
                
                # Проверяем все эпизоды
                for season in seasons_data:
                    episodes = season.get('episodes', [])
                    season_num = season.get('number', '')
                    for ep in episodes:
                        total_episodes += 1
                        ep_num = str(ep.get('episodeNumber', ''))
                        if (season_num, ep_num) in watched_set:
                            watched_episodes += 1
                        else:
                            all_watched = False
                
                # Если все серии просмотрены и сериал не выходит, добавляем в список просмотренных
                if all_watched and total_episodes > 0:
                    watched_series.append({
                        'title': title,
                        'kp_id': kp_id,
                        'film_id': film_id,
                        'total_episodes': total_episodes
                    })
            
            if not watched_series:
                text = "✅ <b>Просмотренные сериалы</b>\n\n"
                text += "Нет полностью просмотренных сериалов, которые больше не выходят."
                markup = InlineKeyboardMarkup()
                markup.add(InlineKeyboardButton("◀️ Назад", callback_data="seasons_list"))
                bot.edit_message_text(text, chat_id, message_id, parse_mode='HTML', reply_markup=markup)
                return
            
            # Формируем список
            text = f"✅ <b>Просмотренные сериалы</b>\n\n"
            text += f"Найдено сериалов: <b>{len(watched_series)}</b>\n\n"
            
            markup = InlineKeyboardMarkup(row_width=1)
            for series_info in watched_series:
                button_text = series_info['title']
                
                # Проверяем, есть ли подписка на уведомления
                with db_lock:
                    cursor.execute('''
                        SELECT subscribed FROM series_subscriptions 
                        WHERE chat_id = %s AND film_id = %s AND user_id = %s AND subscribed = TRUE
                    ''', (chat_id, series_info['film_id'], user_id))
                    has_subscription = cursor.fetchone() is not None
                
                if has_subscription:
                    button_text = f"🔔 {button_text}"
                
                if len(button_text) > 30:
                    button_text = button_text[:27] + "..."
                
                markup.add(InlineKeyboardButton(button_text, callback_data=f"seasons_kp:{series_info['kp_id']}"))
            
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="seasons_list"))
            
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            logger.error(f"[WATCHED SERIES LIST] Ошибка: {e}", exc_info=True)
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
            except:
                pass
