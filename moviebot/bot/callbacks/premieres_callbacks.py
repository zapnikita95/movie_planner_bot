from moviebot.bot.bot_init import bot
"""
Callback handlers для работы с премьерами
"""
import logging
import re
import requests
from datetime import datetime, date, time, timedelta

import pytz
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton


from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock

from moviebot.database.db_operations import get_notification_settings, log_request

from moviebot.api.kinopoisk_api import get_premieres_for_period, extract_movie_info

from moviebot.bot.handlers.series import ensure_movie_in_database

from moviebot.config import KP_TOKEN


logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()


def get_film_distribution(kp_id):
    """Получает информацию о прокате фильма в России"""
    headers = {
        'X-API-KEY': KP_TOKEN,
        'accept': 'application/json'
    }
    url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/distributions"
    try:
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code == 200:
            data = response.json()
            items = data.get('items', [])
            # Ищем прокат в России (COUNTRY_SPECIFIC для России)
            for item in items:
                if item.get('type') == 'COUNTRY_SPECIFIC':
                    country = item.get('country', {})
                    if isinstance(country, dict) and country.get('country') == 'Россия':
                        date_str = item.get('date')
                        if date_str:
                            try:
                                release_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                                today = date.today()
                                # Возвращаем только если дата в будущем
                                if release_date > today:
                                    return {
                                        'date': release_date,
                                        'date_str': release_date.strftime('%d.%m.%Y')
                                    }
                            except Exception as e:
                                logger.warning(f"[DISTRIBUTION] Ошибка парсинга даты {date_str}: {e}")
        return None
    except Exception as e:
        logger.warning(f"[DISTRIBUTION] Ошибка получения информации о прокате для {kp_id}: {e}")
        return None


def show_premieres_page(call, premieres, period, page=0):
    """Показывает страницу премьер с пагинацией"""
    try:
        chat_id = call.message.chat.id
        items_per_page = 10
        total_pages = (len(premieres) + items_per_page - 1) // items_per_page
        start_idx = page * items_per_page
        end_idx = min(start_idx + items_per_page, len(premieres))
        
        period_names = {
            'current_month': 'текущего месяца',
            'next_month': 'следующего месяца',
            '3_months': '3 месяцев',
            '6_months': '6 месяцев',
            'current_year': 'текущего года',
            'next_year': 'ближайшего года'
        }
        period_name = period_names.get(period, 'периода')
        
        text = f"📅 <b>Премьеры {period_name}:</b>\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
    
        # Сортируем премьеры по дате выхода (сначала ближайшие, потом более поздние)
        def get_premiere_date(p):
            """Извлекает дату премьеры из данных"""
            # Пробуем разные форматы дат
            if p.get('premiereRuDate'):
                try:
                    date_str = p.get('premiereRuDate')
                    if 'T' in str(date_str):
                        date_str = str(date_str).split('T')[0]
                    return datetime.strptime(date_str, '%Y-%m-%d').date()
                except:
                    pass
            if p.get('premiereWorldDate'):
                try:
                    date_str = p.get('premiereWorldDate')
                    if 'T' in str(date_str):
                        date_str = str(date_str).split('T')[0]
                    return datetime.strptime(date_str, '%Y-%m-%d').date()
                except:
                    pass
            if p.get('year') and p.get('month'):
                try:
                    day = p.get('day', 1)
                    return datetime(int(p.get('year')), int(p.get('month')), int(day)).date()
                except:
                    pass
            # Для фильмов без даты - ставим в конец
            return datetime(2099, 12, 31).date()
        
        premieres_sorted = sorted(premieres, key=get_premiere_date)
        
        for p in premieres_sorted[start_idx:end_idx]:
            kp_id = p.get('kinopoiskId') or p.get('filmId')
            title_ru = p.get('nameRu') or p.get('nameEn') or "Без названия"
            
            # Получаем дату выхода
            premiere_date = get_premiere_date(p)
            date_str = ""
            if premiere_date and premiere_date.year < 2099:
                date_str = f" ({premiere_date.strftime('%d.%m.%Y')})"
            elif p.get('year') and p.get('month'):
                year = p.get('year')
                month = p.get('month')
                day = p.get('day')
                if day:
                    date_str = f" ({day:02d}.{month:02d}.{year})"
                else:
                    date_str = f" ({month:02d}.{year})"
            
            text += f"• <b>{title_ru}</b>{date_str}\n"
            
            button_text = title_ru
            if len(button_text) > 30:
                button_text = button_text[:27] + "..."
            # Сохраняем период в callback_data для возможности вернуться назад
            markup.add(InlineKeyboardButton(button_text, callback_data=f"premiere_detail:{kp_id}:{period}"))
        
        # Кнопки пагинации
        nav_buttons = []
        if page > 0:
            nav_buttons.append(InlineKeyboardButton("◀️ Назад", callback_data=f"premieres_page:{period}:{page-1}"))
        if page < total_pages - 1:
            nav_buttons.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"premieres_page:{period}:{page+1}"))
        
        if nav_buttons:
            markup.add(*nav_buttons)
        
        # Кнопка возврата к периодам
        markup.add(InlineKeyboardButton("◀️ Назад к периодам", callback_data="premieres_back_to_periods"))
        
        text += f"\nСтраница {page + 1} из {total_pages}"
        text += "\n\nВыберите фильм для подробностей:"
        
        # Используем edit_message_text вместо send_message, если это callback
        if call.message.message_id:
            try:
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                error_str = str(e)
                # Игнорируем ошибку "message is not modified" и "there is no text in the message to edit"
                if "message is not modified" not in error_str and "there is no text in the message to edit" not in error_str:
                    logger.error(f"[PREMIERES PAGE] Ошибка редактирования сообщения: {e}")
                # Если не получилось отредактировать, отправляем новое
                try:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                except:
                    pass
        else:
            # Если message_id нет, отправляем новое сообщение
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        
        if call.id:
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_period:"))
def premieres_period_callback(call):
    """Обработчик выбора периода для премьер"""
    try:
        period = call.data.split(":")[1]
        chat_id = call.message.chat.id
        
        # Получаем премьеры для выбранного периода
        premieres = get_premieres_for_period(period)
        
        if not premieres:
            bot.edit_message_text("❌ Не удалось получить список премьер для выбранного периода.", chat_id, call.message.message_id)
            bot.answer_callback_query(call.id)
            return
    
        # Сохраняем премьеры для пагинации (можно использовать временное хранилище или передавать через callback_data)
        # Для простоты будем показывать первую страницу
        show_premieres_page(call, premieres, period, page=0)
        
    except Exception as e:
        logger.error(f"[PREMIERES PERIOD] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_page:"))
def premieres_page_callback(call):
    """Обработчик пагинации премьер"""
    try:
        parts = call.data.split(":")
        period = parts[1]
        page = int(parts[2])
        
        # Получаем премьеры заново (можно оптимизировать, сохраняя в кэш)
        premieres = get_premieres_for_period(period)
        show_premieres_page(call, premieres, period, page)
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "premieres_back_to_periods")
def premieres_back_to_periods_callback(call):
    """Обработчик возврата к выбору периода"""
    try:
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        # Показываем выбор периода (как в команде /premieres)
        markup = InlineKeyboardMarkup(row_width=1)
        markup.add(InlineKeyboardButton("📅 Текущий месяц", callback_data="premieres_period:current_month"))
        markup.add(InlineKeyboardButton("📅 Следующий месяц", callback_data="premieres_period:next_month"))
        markup.add(InlineKeyboardButton("📅 3 месяца", callback_data="premieres_period:3_months"))
        markup.add(InlineKeyboardButton("📅 6 месяцев", callback_data="premieres_period:6_months"))
        markup.add(InlineKeyboardButton("📅 Текущий год", callback_data="premieres_period:current_year"))
        markup.add(InlineKeyboardButton("📅 Ближайший год", callback_data="premieres_period:next_year"))
        markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
        
        try:
            bot.edit_message_text("📅 <b>Выберите период для просмотра премьер:</b>", chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            # Если не удалось отредактировать, отправляем новое сообщение
            bot.send_message(chat_id, "📅 <b>Выберите период для просмотра премьер:</b>", reply_markup=markup, parse_mode='HTML')
        
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[PREMIERES BACK TO PERIODS] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_detail:"))
def premiere_detail_handler(call):
    """Показывает детали премьеры с постером и трейлером"""
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        kp_id = parts[1]
        period = parts[2] if len(parts) > 2 else 'current_month'  # Период для возврата назад
        chat_id = call.message.chat.id
        
        # Получаем полную информацию о фильме
        headers = {'X-API-KEY': KP_TOKEN}
        url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}"
        
        response = requests.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            bot.answer_callback_query(call.id, "Не удалось загрузить данные фильма", show_alert=True)
            return
        
        data = response.json()
        
        title = data.get('nameRu') or data.get('nameOriginal') or "Без названия"
        year = data.get('year') or '—'
        poster_url = data.get('posterUrlPreview') or data.get('posterUrl')
        trailer_url = None
        
        # Получаем трейлер через отдельный запрос к API
        try:
            videos_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/videos"
            videos_headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
            videos_response = requests.get(videos_url, headers=videos_headers, timeout=15)
            if videos_response.status_code == 200:
                videos_data = videos_response.json()
                items = videos_data.get('items', [])
                if items:
                    trailer_url = items[0].get('url')
                    logger.info(f"[PREMIERES DETAIL] Найден трейлер для {kp_id}: {trailer_url}")
        except Exception as e:
            logger.error(f"[PREMIERES DETAIL] Ошибка получения трейлера: {e}", exc_info=True)
        
        if not trailer_url:
            videos = data.get('videos', {}).get('trailers', [])
            if videos:
                trailer_url = videos[0].get('url')
        
        description = data.get('description') or data.get('shortDescription') or "Нет описания"
        genres = ', '.join([g['genre'] for g in data.get('genres', [])]) or '—'
        countries = ', '.join([c['country'] for c in data.get('countries', [])]) or '—'
        
        directors = data.get('directors', [])
        director_str = ', '.join([d.get('nameRu') or d.get('nameEn', '') for d in directors if d.get('nameRu') or d.get('nameEn')]) or '—'
        
        russia_release = get_film_distribution(kp_id)
        premiere_date = None
        premiere_date_str = ""
        
        if russia_release and russia_release.get('date'):
            premiere_date = russia_release['date']
            premiere_date_str = russia_release.get('date_str', premiere_date.strftime('%d.%m.%Y'))
        else:
            for date_field in ['premiereWorld', 'premiereRu', 'premiereWorldDate', 'premiereRuDate']:
                date_value = data.get(date_field)
                if date_value:
                    try:
                        if 'T' in str(date_value):
                            premiere_date = datetime.strptime(str(date_value).split('T')[0], '%Y-%m-%d').date()
                        else:
                            premiere_date = datetime.strptime(str(date_value), '%Y-%m-%d').date()
                        premiere_date_str = premiere_date.strftime('%d.%m.%Y')
                        break
                    except:
                        continue
        
        text = f"<b>{title}</b> ({year})\n\n"
        if premiere_date_str:
            if russia_release:
                text += f"📅 Премьера в России: {premiere_date_str}\n"
            else:
                text += f"📅 Премьера: {premiere_date_str}\n"
        if director_str != '—':
            text += f"🎥 Режиссёр: {director_str}\n"
        if countries != '—':
            text += f"🌍 {countries}"

        text += f"\n{description}\n\n"
        text += f"🎭 {genres}\n"
        
        # Улучшенная проверка: получаем id, title и watched за один запрос
        existing_row = None
        with db_lock:
            cursor.execute(
                'SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s',
                (chat_id, str(kp_id))
            )
            existing_row = cursor.fetchone()
        
        in_database = existing_row is not None
        
        # Опционально: добавляем статус в текст (очень полезно для пользователя)
        if in_database:
            watched_emoji = " ✅" if existing_row[2] else ""
            text += f"\n\n🎬 Фильм уже в твоём списке{watched_emoji}"
        
        markup = InlineKeyboardMarkup(row_width=1)
        
        today = date.today()
        show_notify_button = False
        date_for_callback = ''
        
        if premiere_date:
            if premiere_date > today:
                show_notify_button = True
                date_for_callback = premiere_date_str.replace(':', '-') if premiere_date_str else ''
        elif not premiere_date:
            year_val = data.get('year')
            if year_val:
                try:
                    year_int = int(year_val)
                    current_year = today.year
                    if year_int > current_year or (year_int == current_year and today.month < 12):
                        show_notify_button = True
                except:
                    pass
        
        if show_notify_button:
            markup.add(InlineKeyboardButton("🔔 Уведомить о премьере", callback_data=f"premiere_notify:{kp_id}:{date_for_callback}:{period}"))
        
        # Кнопки добавить / удалить
        if in_database:
            markup.add(InlineKeyboardButton("🗑️ Удалить из базы", callback_data=f"remove_from_database:{kp_id}"))
        else:
            markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{kp_id}"))
        
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=f"premieres_back:{period}"))
        
        # Отправка с постером
        if poster_url:
            try:
                bot.send_photo(
                    chat_id,
                    poster_url,
                    caption=text,
                    parse_mode='HTML',
                    reply_markup=markup
                )
                bot.delete_message(chat_id, call.message.message_id)
            except Exception as e:
                logger.error(f"[PREMIERES DETAIL] Ошибка отправки фото: {e}")
                bot.edit_message_text(
                    text,
                    chat_id,
                    call.message.message_id,
                    parse_mode='HTML',
                    reply_markup=markup,
                    disable_web_page_preview=False
                )
        else:
            bot.edit_message_text(
                text,
                chat_id,
                call.message.message_id,
                parse_mode='HTML',
                reply_markup=markup,
                disable_web_page_preview=False
            )
        
        # Трейлер
        if trailer_url:
            try:
                bot.send_video(chat_id, trailer_url, caption=f"📺 Трейлер: <b>{title}</b>", parse_mode='HTML')
            except Exception as e:
                logger.error(f"[PREMIERES DETAIL] Ошибка отправки трейлера как видео: {e}")
                try:
                    bot.send_message(chat_id, f"📺 <a href='{trailer_url}'>Смотреть трейлер: {title}</a>", parse_mode='HTML')
                except Exception as e2:
                    logger.error(f"[PREMIERES DETAIL] Ошибка отправки трейлера как ссылки: {e2}")
        
    except Exception as e:
        logger.error(f"[PREMIERES DETAIL] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка загрузки фильма", show_alert=True)
        except:
            pass
        
@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_add:"))
def premiere_add_to_db(call):
    """Добавляет премьеру в базу и показывает описание фильма с кнопками БЕЗ повторного API запроса"""
    logger.info("=" * 80)
    logger.info(f"[PREMIERE ADD] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        bot.answer_callback_query(call.id, text="⏳ Добавляю в базу...")
        logger.info(f"[PREMIERE ADD] answer_callback_query вызван, callback_id={call.id}")
        
        kp_id = call.data.split(":")[1]
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id
        
        logger.info(f"[PREMIERE ADD] kp_id={kp_id}, user_id={user_id}, chat_id={chat_id}")
        
        # Проверяем, есть ли фильм уже в базе
        with db_lock:
            cursor.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
            existing_row = cursor.fetchone()
        
        if existing_row:
            # Фильм уже есть - получаем полную информацию из БД и показываем описание
            film_id = existing_row.get('id') if isinstance(existing_row, dict) else existing_row[0]
            title = existing_row.get('title') if isinstance(existing_row, dict) else existing_row[1]
            watched = existing_row.get('watched') if isinstance(existing_row, dict) else existing_row[2]
            
            logger.info(f"[PREMIERE ADD] Фильм уже в базе: film_id={film_id}, title={title}")
            bot.answer_callback_query(call.id, f"ℹ️ {title} уже в базе", show_alert=False)
            
            # Получаем полную информацию из БД (без API запроса)
            with db_lock:
                cursor.execute('''
                    SELECT year, genres, description, director, actors, is_series, link
                    FROM movies WHERE id = %s AND chat_id = %s
                ''', (film_id, chat_id))
                db_row = cursor.fetchone()
            
            if db_row:
                if isinstance(db_row, dict):
                    year = db_row.get('year')
                    genres = db_row.get('genres')
                    description = db_row.get('description')
                    director = db_row.get('director')
                    actors = db_row.get('actors')
                    is_series = bool(db_row.get('is_series', 0))
                    link = db_row.get('link') or link
                else:
                    year = db_row.get('year') if isinstance(db_row, dict) else (db_row[0] if db_row else None)
                    genres = db_row[1] if len(db_row) > 1 else None
                    description = db_row[2] if len(db_row) > 2 else None
                    director = db_row[3] if len(db_row) > 3 else None
                    actors = db_row[4] if len(db_row) > 4 else None
                    is_series = bool(db_row[5] if len(db_row) > 5 else 0)
                    link = db_row[6] if len(db_row) > 6 else link
                
                # Формируем словарь info из данных БД (без API запроса)
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
                
                # Удаляем сообщение с премьерой
                try:
                    bot.delete_message(chat_id, message_id)
                except Exception as e:
                    logger.warning(f"[PREMIERE ADD] Не удалось удалить сообщение: {e}")
                
                # Показываем описание фильма с кнопками
                from moviebot.bot.handlers.series import show_film_info_with_buttons
                show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=None)
                logger.info(f"[PREMIERE ADD] Описание фильма показано из БД: kp_id={kp_id}")
            return
        
        # Фильма нет в базе - получаем информацию через API и добавляем
        logger.info(f"[PREMIERE ADD] Фильм не найден в базе, получаю информацию через API")
        info = extract_movie_info(link)
        if not info:
            bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            return
        
        # Добавляем в базу
        film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
        
        if not film_id:
            bot.answer_callback_query(call.id, "❌ Не удалось добавить фильм", show_alert=True)
            return
        
        logger.info(f"[PREMIERE ADD] Фильм добавлен в базу: film_id={film_id}, was_inserted={was_inserted}")
        bot.answer_callback_query(call.id, "✅ Фильм добавлен в базу!", show_alert=False)
        
        # Получаем полную информацию из БД (без повторного API запроса)
        with db_lock:
            cursor.execute('''
                SELECT title, watched, year, genres, description, director, actors, is_series, link
                FROM movies WHERE id = %s AND chat_id = %s
            ''', (film_id, chat_id))
            db_row = cursor.fetchone()
        
        if db_row:
            if isinstance(db_row, dict):
                title = db_row.get('title')
                watched = db_row.get('watched')
                year = db_row.get('year')
                genres = db_row.get('genres')
                description = db_row.get('description')
                director = db_row.get('director')
                actors = db_row.get('actors')
                is_series = bool(db_row.get('is_series', 0))
                link = db_row.get('link') or link
            else:
                title = db_row[0] if len(db_row) > 0 else info.get('title')
                watched = db_row[1] if len(db_row) > 1 else 0
                year = db_row[2] if len(db_row) > 2 else info.get('year')
                genres = db_row[3] if len(db_row) > 3 else info.get('genres')
                description = db_row[4] if len(db_row) > 4 else info.get('description')
                director = db_row[5] if len(db_row) > 5 else info.get('director')
                actors = db_row[6] if len(db_row) > 6 else info.get('actors')
                is_series = bool(db_row[7] if len(db_row) > 7 else info.get('is_series', False))
                link = db_row[8] if len(db_row) > 8 else link
            
            # Формируем словарь info из данных БД (без повторного API запроса)
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
            
            # Удаляем сообщение с премьерой
            try:
                bot.delete_message(chat_id, message_id)
            except Exception as e:
                logger.warning(f"[PREMIERE ADD] Не удалось удалить сообщение: {e}")
            
            # Показываем описание фильма с кнопками
            from moviebot.bot.handlers.series import show_film_info_with_buttons
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=None)
            logger.info(f"[PREMIERE ADD] Описание фильма показано из БД: kp_id={kp_id}")
        else:
            logger.error(f"[PREMIERE ADD] Не удалось получить данные из БД после добавления: film_id={film_id}")
            bot.answer_callback_query(call.id, "❌ Ошибка при получении данных из базы", show_alert=True)
            
    except Exception as e:
        logger.error(f"[PREMIERE ADD] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass
    finally:
        logger.info(f"[PREMIERE ADD] ===== END: callback_id={call.id}")


@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_notify:"))
def premiere_notify_handler(call):
    """Обработчик уведомления о выходе премьеры - добавляет в расписание, затем в базу"""
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        kp_id = parts[1]
        date_str = parts[2] if len(parts) > 2 else ''
        period = parts[3] if len(parts) > 3 else 'current_month'
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        # Парсим дату
        try:
            premiere_date = datetime.strptime(date_str.replace('-', '.'), '%d.%m.%Y').date()
        except:
            bot.answer_callback_query(call.id, "❌ Ошибка парсинга даты", show_alert=True)
            return
        
        # Получаем информацию о фильме (но НЕ добавляем в базу пока)
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        if not info:
            bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
            return
        
        title = info.get('title', 'Фильм')
        
        # Проверяем, есть ли фильм уже в базе
        with db_lock:
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
            existing = cursor.fetchone()
            
            if existing:
                film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                title = existing.get('title') if isinstance(existing, dict) else existing[1]
                film_already_in_db = True
            else:
                film_id = None
                film_already_in_db = False
        
        # Получаем часовой пояс пользователя
        user_tz = pytz.timezone('Europe/Moscow')  # По умолчанию
        try:
            with db_lock:
                cursor.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                tz_row = cursor.fetchone()
                if tz_row:
                    tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                    user_tz = pytz.timezone(tz_str)
        except:
            pass
        
        # Получаем настройки уведомлений для определения времени по умолчанию
        notify_settings = get_notification_settings(chat_id)
        
        # Определяем, будний день или выходной (1-5 = понедельник-пятница, 6-7 = суббота-воскресенье)
        weekday_num = premiere_date.isoweekday()
        is_weekend = weekday_num >= 6
        
        # Получаем время из настроек в зависимости от дня недели
        if is_weekend:
            hour = notify_settings.get('cinema_weekend_hour', 9)
            minute = notify_settings.get('cinema_weekend_minute', 0)
        else:
            hour = notify_settings.get('cinema_weekday_hour', 9)
            minute = notify_settings.get('cinema_weekday_minute', 0)
        
        # Время сеанса из настроек пользователя
        session_time = time(int(hour), int(minute))
        session_dt = user_tz.localize(datetime.combine(premiere_date, session_time))
        plan_utc = session_dt.astimezone(pytz.utc)
        
        # Используем транзакцию: добавляем фильм в базу только если план успешно добавлен
        plan_id = None
        film_added = False
        
        with db_lock:
            try:
                # Если фильма нет в базе, добавляем его (но не коммитим пока)
                if not film_already_in_db:
                    cursor.execute('''
                        INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                        ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                        RETURNING id
                    ''', (chat_id, link, kp_id, info['title'], info['year'], info['genres'], info['description'], 
                          info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
                    
                    result = cursor.fetchone()
                    film_id = result.get('id') if isinstance(result, dict) else result[0]
                    film_added = True
                    logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в транзакции: film_id={film_id}, title={title}")
                
                # Проверяем, нет ли уже плана на эту дату
                cursor.execute('''
                    SELECT id FROM plans 
                    WHERE chat_id = %s AND film_id = %s AND plan_type = 'cinema' AND DATE(plan_datetime AT TIME ZONE 'UTC' AT TIME ZONE %s) = %s
                ''', (chat_id, film_id, str(user_tz), premiere_date))
                existing_plan = cursor.fetchone()
                
                if not existing_plan:
                    # Добавляем план в расписание
                    cursor.execute('''
                        INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
                        VALUES (%s, %s, 'cinema', %s, %s)
                        RETURNING id
                    ''', (chat_id, film_id, plan_utc, user_id))
                    
                    plan_row = cursor.fetchone()
                    if plan_row:
                        plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                        # Коммитим транзакцию только если план успешно добавлен
                        conn.commit()
                        logger.info(f"[PREMIERE NOTIFY] План успешно добавлен: plan_id={plan_id}, film_id={film_id}")
                        if film_added:
                            logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в базу как следствие успешного добавления плана")
                    else:
                        logger.error(f"[PREMIERE NOTIFY] План не был создан, но ошибки не было")
                        conn.rollback()
                        bot.answer_callback_query(call.id, "❌ Ошибка при добавлении в расписание", show_alert=True)
                        return
                else:
                    plan_id = existing_plan.get('id') if isinstance(existing_plan, dict) else existing_plan[0]
                    # Если план уже существует, коммитим только если фильм был добавлен
                    if film_added:
                        conn.commit()
                        logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в базу, план уже существовал: plan_id={plan_id}")
                    else:
                        logger.info(f"[PREMIERE NOTIFY] План уже существует: plan_id={plan_id}")
                    
            except Exception as e:
                conn.rollback()
                logger.error(f"[PREMIERE NOTIFY] Ошибка в транзакции: {e}", exc_info=True)
                bot.answer_callback_query(call.id, "❌ Ошибка при добавлении в расписание", show_alert=True)
                return
        
        # Отправляем сообщение-подтверждение
        time_str = f"{int(hour):02d}:{int(minute):02d}"
        confirm_text = f"✅ <b>Уведомление установлено!</b>\n\n"
        confirm_text += f"📺 <b>{title}</b>\n"
        confirm_text += f"📅 Дата выхода: {premiere_date.strftime('%d.%m.%Y')}\n"
        confirm_text += f"🎬 Добавлено в расписание: в кино на {premiere_date.strftime('%d.%m.%Y')} в {time_str}\n\n"
        confirm_text += f"Если это ошибка, нажмите кнопку ниже для отмены."
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("🔗 Перейти к описанию", callback_data=f"show_film_description:{kp_id}"))
        markup.add(InlineKeyboardButton("❌ Отменить", callback_data=f"premiere_cancel:{kp_id}:{plan_id}"))
        
        bot.send_message(chat_id, confirm_text, parse_mode='HTML', reply_markup=markup)
        bot.answer_callback_query(call.id, "✅ Уведомление установлено!")
        
        logger.info(f"[PREMIERE NOTIFY] Уведомление установлено для фильма {title} (kp_id={kp_id}) пользователем {user_id}, plan_id={plan_id}")
    except Exception as e:
        logger.error(f"[PREMIERE NOTIFY] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при установке уведомления", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_cancel:"))
def premiere_cancel_handler(call):
    """Обработчик отмены уведомления о премьере - удаляет из базы и расписания"""
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        kp_id = parts[1]
        plan_id = int(parts[2]) if len(parts) > 2 else None
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        with db_lock:
            # Удаляем из расписания
            if plan_id:
                cursor.execute('DELETE FROM plans WHERE id = %s AND chat_id = %s AND user_id = %s', (plan_id, chat_id, user_id))
            
            # Получаем film_id
            cursor.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
            film_row = cursor.fetchone()
            
            if film_row:
                film_id = film_row.get('id') if isinstance(film_row, dict) else film_row[0]
                title = film_row.get('title') if isinstance(film_row, dict) else film_row[1]
                
                # Проверяем, есть ли другие планы или оценки для этого фильма
                cursor.execute('SELECT COUNT(*) FROM plans WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
                plans_count = cursor.fetchone()
                plans_count = plans_count.get('COUNT(*)') if isinstance(plans_count, dict) else plans_count[0]
                
                cursor.execute('SELECT COUNT(*) FROM ratings WHERE film_id = %s AND chat_id = %s', (film_id, chat_id))
                ratings_count = cursor.fetchone()
                ratings_count = ratings_count.get('COUNT(*)') if isinstance(ratings_count, dict) else ratings_count[0]
                
                # Удаляем фильм из базы только если нет других планов и оценок
                if plans_count == 0 and ratings_count == 0:
                    cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
                    deleted_text = f"❌ <b>Отменено</b>\n\nФильм <b>{title}</b> удалён из базы и расписания."
                else:
                    deleted_text = f"❌ <b>Отменено</b>\n\nПлан просмотра фильма <b>{title}</b> удалён из расписания."
                
                conn.commit()
                
                # Обновляем сообщение
                try:
                    bot.edit_message_text(deleted_text, chat_id, call.message.message_id, parse_mode='HTML')
                except:
                    bot.send_message(chat_id, deleted_text, parse_mode='HTML')
                
                logger.info(f"[PREMIERE CANCEL] Отменено уведомление для фильма {title} (kp_id={kp_id}) пользователем {user_id}")
            else:
                bot.answer_callback_query(call.id, "❌ Фильм не найден", show_alert=True)
    except Exception as e:
        logger.error(f"[PREMIERE CANCEL] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка при отмене", show_alert=True)
        except:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_back:"))
def premieres_back_handler(call):
    """Обработчик возврата к списку премьер"""
    try:
        bot.answer_callback_query(call.id)
        parts = call.data.split(":")
        period = parts[1] if len(parts) > 1 else 'current_month'
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        
        # Удаляем сообщение о фильме
        try:
            bot.delete_message(chat_id, message_id)
        except Exception as e:
            logger.warning(f"[PREMIERES BACK] Не удалось удалить сообщение: {e}")
        
        # Получаем премьеры для периода
        premieres = get_premieres_for_period(period)
        
        if not premieres:
            try:
                bot.send_message(chat_id, "❌ Не удалось получить список премьер.")
            except:
                pass
            return
        
        # Показываем первую страницу - отправляем новое сообщение
        # Создаем фиктивный call объект для show_premieres_page
        class FakeCall:
            def __init__(self, chat_id):
                self.message = type('obj', (object,), {'chat': type('obj', (object,), {'id': chat_id})(), 'message_id': None})()
                self.id = None
        
        fake_call = FakeCall(chat_id)
        show_premieres_page(fake_call, premieres, period, page=0)
        
    except Exception as e:
        logger.error(f"[PREMIERES BACK] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except:
            pass


def register_premieres_callbacks(bot):
    """Регистрирует обработчики премьер (уже зарегистрированы через декораторы)"""
    pass

