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
from moviebot.utils.helpers import extract_film_info_from_existing
from moviebot.database.db_operations import get_notification_settings, log_request

from moviebot.api.kinopoisk_api import get_premieres_for_period, extract_movie_info, get_film_distribution

from moviebot.bot.handlers.series import ensure_movie_in_database

from moviebot.config import KP_TOKEN


logger = logging.getLogger(__name__)

ITEMS_PER_PAGE = 5

PERIOD_NAMES = {
    'current_month': 'текущего месяца',
    'next_month': 'следующего месяца',
    '3_months': '3 месяцев',
    '6_months': '6 месяцев',
    'current_year': 'текущего года',
    'next_year': 'ближайшего года'
}


def _get_premiere_date(p):
    """Извлекает дату премьеры в РФ. API: premiereRu 'YYYY-MM-DD', premiereRuDate и т.д."""
    for key in ('premiereRu', 'premiereRuDate', 'premiereWorld', 'premiereWorldDate'):
        val = p.get(key)
        if val:
            try:
                s = str(val).split('T')[0] if 'T' in str(val) else str(val)
                return datetime.strptime(s, '%Y-%m-%d').date()
            except Exception:
                pass
    if p.get('year') and p.get('month'):
        try:
            day = p.get('day', 1)
            return datetime(int(p['year']), int(p['month']), int(day)).date()
        except Exception:
            pass
    return datetime(2099, 12, 31).date()


def _format_premiere_block(p, include_genre=True):
    """Компактный блок для списка: дата, название, жанр. Без реж/актёров (без доп. запросов)."""
    kp_id = p.get('kinopoiskId') or p.get('filmId')
    title = p.get('nameRu') or p.get('nameEn') or "Без названия"
    d = _get_premiere_date(p)
    date_str = d.strftime('%d.%m') if d.year < 2099 else "—"
    genres = p.get('genres') or []
    first_genre = genres[0].get('genre', '—') if genres else '—'
    lines = [f"• <b>{date_str}</b> {title}"]
    if include_genre and first_genre != '—':
        lines.append(f"🎭 {first_genre}")
    return '\n'.join(lines), kp_id, title


def _show_sort_selection(chat_id, message_id=None, edit=True):
    """Сообщение «Выберите вариант сортировки» + По датам / По жанрам / Назад в меню."""
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📆 По датам", callback_data="premieres_mode:date"))
    markup.add(InlineKeyboardButton("🎭 По жанрам", callback_data="premieres_mode:genre"))
    markup.add(InlineKeyboardButton("⬅️ Назад в меню", callback_data="back_to_start_menu"))
    text = "Выберите вариант сортировки:"
    if edit and message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            if "message is not modified" not in str(e).lower() and "there is no text" not in str(e).lower():
                logger.warning(f"[PREMIERES] edit sort selection: {e}")
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            except Exception:
                pass
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')


def _show_period_selection(chat_id, message_id=None, edit=True):
    """Выбор периода для «По датам»."""
    markup = InlineKeyboardMarkup(row_width=1)
    markup.add(InlineKeyboardButton("📅 Текущий месяц", callback_data="premieres_period:current_month"))
    markup.add(InlineKeyboardButton("📅 Следующий месяц", callback_data="premieres_period:next_month"))
    markup.add(InlineKeyboardButton("📅 3 месяца", callback_data="premieres_period:3_months"))
    markup.add(InlineKeyboardButton("📅 6 месяцев", callback_data="premieres_period:6_months"))
    markup.add(InlineKeyboardButton("📅 Текущий год", callback_data="premieres_period:current_year"))
    markup.add(InlineKeyboardButton("📅 Ближайший год", callback_data="premieres_period:next_year"))
    markup.add(InlineKeyboardButton("◀️ Назад", callback_data="premieres_back_to_sort"))
    text = "📅 <b>Премьеры по датам</b>\n\nВыберите период:"
    if edit and message_id:
        try:
            bot.edit_message_text(text, chat_id, message_id, reply_markup=markup, parse_mode='HTML')
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                logger.warning(f"[PREMIERES] edit period selection: {e}")
            try:
                bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
            except Exception:
                pass
    else:
        bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')


def show_premieres_page(call, premieres, period, page=0, mode='date', genre_name=None):
    """Страница премьер: 5 на страницу, компактный формат (дата, название, жанр)."""
    try:
        chat_id = call.message.chat.id
        premieres_sorted = sorted(premieres, key=_get_premiere_date)
        total_pages = max(1, (len(premieres_sorted) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE)
        start_idx = page * ITEMS_PER_PAGE
        end_idx = min(start_idx + ITEMS_PER_PAGE, len(premieres_sorted))
        page_premieres = premieres_sorted[start_idx:end_idx]

        if mode == 'date':
            period_name = PERIOD_NAMES.get(period, 'периода')
            title = f"📅 <b>Премьеры {period_name}</b>"
            back_data = "premieres_back_to_sort"
            page_cb = f"premieres_page:{period}"
            detail_fmt = "premiere_detail:{}:date:{}".format
        else:
            title = f"🎭 <b>Премьеры — {genre_name}</b>"
            back_data = "premieres_back_to_sort"
            page_cb = f"premieres_genre_page:{genre_name}"
            detail_fmt = "premiere_detail:{}:genre:{}".format

        text = title + "\n\n"
        markup = InlineKeyboardMarkup(row_width=1)
        include_genre = True

        for p in page_premieres:
            block, kp_id, title_ru = _format_premiere_block(p, include_genre=include_genre)
            text += block + "\n\n"
            btn = title_ru[:27] + "..." if len(title_ru) > 30 else title_ru
            if mode == 'date':
                cb = detail_fmt(kp_id, period)
            else:
                cb = detail_fmt(kp_id, genre_name)
            markup.add(InlineKeyboardButton(btn, callback_data=cb))

        nav = []
        if page > 0:
            nav.append(InlineKeyboardButton("◀️ Назад", callback_data=f"{page_cb}:{page - 1}"))
        if page < total_pages - 1:
            nav.append(InlineKeyboardButton("Вперёд ▶️", callback_data=f"{page_cb}:{page + 1}"))
        if nav:
            markup.row(*nav)
        markup.add(InlineKeyboardButton("◀️ Назад", callback_data=back_data))

        text += f"Страница {page + 1} из {total_pages}\n\nВыберите фильм для подробностей:"

        if getattr(call.message, 'message_id', None):
            try:
                bot.edit_message_text(text, chat_id, call.message.message_id, reply_markup=markup, parse_mode='HTML')
            except Exception as e:
                err = str(e).lower()
                if "message is not modified" not in err and "there is no text" not in err:
                    logger.warning(f"[PREMIERES PAGE] edit: {e}")
                try:
                    bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
                except Exception:
                    pass
        else:
            bot.send_message(chat_id, text, reply_markup=markup, parse_mode='HTML')
        if getattr(call, 'id', None):
            bot.answer_callback_query(call.id)
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data == "premieres_back_to_sort")
def premieres_back_to_sort_callback(call):
    """Возврат к выбору сортировки (По датам / По жанрам)."""
    try:
        bot.answer_callback_query(call.id)
        _show_sort_selection(call.message.chat.id, call.message.message_id, edit=True)
    except Exception as e:
        logger.error(f"[PREMIERES BACK TO SORT] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_mode:"))
def premieres_mode_callback(call):
    """По датам -> периоды; По жанрам -> список жанров."""
    try:
        bot.answer_callback_query(call.id)
        mode = call.data.split(":")[1]
        chat_id = call.message.chat.id
        msg_id = call.message.message_id

        if mode == "date":
            _show_period_selection(chat_id, msg_id, edit=True)
            return
        if mode == "genre":
            premieres = get_premieres_for_period("6_months")
            if not premieres:
                try:
                    bot.edit_message_text(
                        "❌ Не удалось загрузить премьеры для выбора жанров.",
                        chat_id, msg_id
                    )
                except Exception:
                    bot.send_message(chat_id, "❌ Не удалось загрузить премьеры для выбора жанров.")
                return
            genres_set = set()
            for p in premieres:
                for g in (p.get("genres") or []):
                    name = (g.get("genre") or "").strip()
                    if name:
                        genres_set.add(name)
            genres_sorted = sorted(genres_set, key=str.lower)
            markup = InlineKeyboardMarkup(row_width=1)
            for g in genres_sorted:
                markup.add(InlineKeyboardButton(f"🎭 {g}", callback_data=f"premieres_genre_page:{g}:0"))
            markup.add(InlineKeyboardButton("◀️ Назад", callback_data="premieres_back_to_sort"))
            try:
                bot.edit_message_text(
                    "🎭 <b>Премьеры по жанрам</b>\n\nВыберите жанр:",
                    chat_id, msg_id, reply_markup=markup, parse_mode="HTML"
                )
            except Exception as e:
                if "message is not modified" not in str(e).lower():
                    logger.warning(f"[PREMIERES GENRE] edit: {e}")
                try:
                    bot.send_message(chat_id, "🎭 <b>Премьеры по жанрам</b>\n\nВыберите жанр:", reply_markup=markup, parse_mode="HTML")
                except Exception:
                    pass
            return
    except Exception as e:
        logger.error(f"[PREMIERES MODE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_period:"))
def premieres_period_callback(call):
    """Выбор периода -> список премьер по датам."""
    try:
        period = call.data.split(":")[1]
        premieres = get_premieres_for_period(period)
        if not premieres:
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text(
                    "❌ Не удалось получить список премьер для выбранного периода.",
                    call.message.chat.id, call.message.message_id
                )
            except Exception:
                bot.send_message(call.message.chat.id, "❌ Не удалось получить список премьер.")
            return
        show_premieres_page(call, premieres, period, page=0, mode='date')
    except Exception as e:
        logger.error(f"[PREMIERES PERIOD] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_page:"))
def premieres_page_callback(call):
    """Пагинация: премьеры по датам."""
    try:
        parts = call.data.split(":")
        period = parts[1]
        page = int(parts[2])
        premieres = get_premieres_for_period(period)
        show_premieres_page(call, premieres, period, page=page, mode='date')
    except Exception as e:
        logger.error(f"[PREMIERES PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
            pass


@bot.callback_query_handler(func=lambda call: call.data.startswith("premieres_genre_page:"))
def premieres_genre_page_callback(call):
    """Премьеры по жанру: список или пагинация."""
    try:
        parts = call.data.split(":", 2)
        genre_name = parts[1]
        page = int(parts[2])
        premieres_all = get_premieres_for_period("6_months")
        genre_lower = genre_name.lower()
        filtered = [
            p for p in premieres_all
            if any((g.get("genre") or "").lower() == genre_lower for g in (p.get("genres") or []))
        ]
        if not filtered:
            bot.answer_callback_query(call.id)
            try:
                bot.edit_message_text(
                    f"❌ Нет премьер в жанре «{genre_name}».",
                    call.message.chat.id, call.message.message_id
                )
            except Exception:
                pass
            return
        show_premieres_page(call, filtered, None, page=page, mode='genre', genre_name=genre_name)
    except Exception as e:
        logger.error(f"[PREMIERES GENRE PAGE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка", show_alert=True)
        except Exception:
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
#        trailer_url = None
        
        # Получаем трейлер через отдельный запрос к API
#        try:
#            videos_url = f"https://kinopoiskapiunofficial.tech/api/v2.2/films/{kp_id}/videos"
#            videos_headers = {'X-API-KEY': KP_TOKEN, 'accept': 'application/json'}
#            videos_response = requests.get(videos_url, headers=videos_headers, timeout=15)
#            if videos_response.status_code == 200:
#                videos_data = videos_response.json()
#                items = videos_data.get('items', [])
#                if items:
#                    trailer_url = items[0].get('url')
#                    logger.info(f"[PREMIERES DETAIL] Найден трейлер для {kp_id}: {trailer_url}")
#        except Exception as e:
#            logger.error(f"[PREMIERES DETAIL] Ошибка получения трейлера: {e}", exc_info=True)
        
#        if not trailer_url:
#            videos = data.get('videos', {}).get('trailers', [])
#            if videos:
#                trailer_url = videos[0].get('url')
        
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
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute(
                    'SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s',
                    (chat_id, str(kp_id))
                )
                existing_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
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
            markup.add(InlineKeyboardButton("🗑️ Удалить из базы", callback_data=f"remove_from_database:{int(kp_id)}"))
        else:
            markup.add(InlineKeyboardButton("➕ Добавить в базу", callback_data=f"add_to_database:{int(kp_id)}"))
        
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
#        if trailer_url:
#            try:
#                bot.send_video(chat_id, trailer_url, caption=f"📺 Трейлер: <b>{title}</b>", parse_mode='HTML')
#            except Exception as e:
#                logger.error(f"[PREMIERES DETAIL] Ошибка отправки трейлера как видео: {e}")
#                try:
#                    bot.send_message(chat_id, f"📺 <a href='{trailer_url}'>Смотреть трейлер: {title}</a>", parse_mode='HTML')
#                except Exception as e2:
#                    logger.error(f"[PREMIERES DETAIL] Ошибка отправки трейлера как ссылки: {e2}")
        
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
        # Проверяем, не устарел ли callback, но продолжаем выполнение даже если устарел
        callback_is_old = False
        try:
            bot.answer_callback_query(call.id, text="⏳ Добавляю в базу...")
            logger.info(f"[PREMIERE ADD] answer_callback_query вызван, callback_id={call.id}")
        except Exception as answer_error:
            error_str = str(answer_error)
            if "query is too old" in error_str or "query ID is invalid" in error_str or "timeout expired" in error_str:
                callback_is_old = True
                logger.warning(f"[PREMIERE ADD] Callback query устарел, но продолжаем выполнение: {answer_error}")
            else:
                logger.error(f"[PREMIERE ADD] Ошибка answer_callback_query: {answer_error}", exc_info=True)
        
        kp_id = call.data.split(":")[1]
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id if not callback_is_old else None
        
        logger.info(f"[PREMIERE ADD] kp_id={kp_id}, user_id={user_id}, chat_id={chat_id}")
        
        # Проверяем, есть ли фильм уже в базе
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        existing_row = None
        try:
            with db_lock:
                cursor_local.execute('SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(kp_id)))
                existing_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        if existing_row:
            film_id, watched = extract_film_info_from_existing(existing_row)
            title = existing_row[1] if not isinstance(existing_row, dict) else existing_row.get('title')
            
            logger.info(f"[PREMIERE ADD] Фильм уже в базе: film_id={film_id}, title={title}")
            if not callback_is_old:
                try:
                    bot.answer_callback_query(call.id, f"ℹ️ {title} уже в базе", show_alert=False)
                except:
                    pass
            
            # Получаем полную информацию из БД (без API запроса)
            conn_local = get_db_connection()
            cursor_local = get_db_cursor()
            db_row = None
            try:
                with db_lock:
                    cursor_local.execute('''
                        SELECT year, genres, description, director, actors, is_series, link
                        FROM movies WHERE id = %s AND chat_id = %s
                    ''', (film_id, chat_id))
                    db_row = cursor_local.fetchone()
            finally:
                try:
                    cursor_local.close()
                except:
                    pass
                try:
                    conn_local.close()
                except:
                    pass
            
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
                show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=message_id, message_thread_id=getattr(call.message, 'message_thread_id', None))
                logger.info(f"[PREMIERE ADD] Описание фильма показано из БД: kp_id={kp_id}")
            return
        
        # Фильма нет в базе - получаем информацию через API и добавляем
        logger.info(f"[PREMIERE ADD] Фильм не найден в базе, получаю информацию через API")
        info = extract_movie_info(link)
        if not info:
            if not callback_is_old:
                try:
                    bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                except:
                    pass
            else:
                # Если callback устарел, отправляем новое сообщение об ошибке
                try:
                    send_kwargs = {
                        'text': "❌ Не удалось получить информацию о фильме",
                        'chat_id': chat_id
                    }
                    if message_thread_id is not None:
                        send_kwargs['message_thread_id'] = message_thread_id
                    bot.send_message(**send_kwargs)
                except:
                    pass
            return
        
        # Добавляем в базу
        film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
        
        if not film_id:
            if not callback_is_old:
                try:
                    bot.answer_callback_query(call.id, "❌ Не удалось добавить фильм", show_alert=True)
                except:
                    pass
            else:
                # Если callback устарел, отправляем новое сообщение об ошибке
                try:
                    send_kwargs = {
                        'text': "❌ Не удалось добавить фильм",
                        'chat_id': chat_id
                    }
                    if message_thread_id is not None:
                        send_kwargs['message_thread_id'] = message_thread_id
                    bot.send_message(**send_kwargs)
                except:
                    pass
            return
        
        logger.info(f"[PREMIERE ADD] Фильм добавлен в базу: film_id={film_id}, was_inserted={was_inserted}")
        if not callback_is_old:
            try:
                bot.answer_callback_query(call.id, "✅ Фильм добавлен в базу!", show_alert=False)
            except:
                pass
        
        # Получаем полную информацию из БД (без повторного API запроса)
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        db_row = None
        try:
            with db_lock:
                cursor_local.execute('''
                    SELECT title, watched, year, genres, description, director, actors, is_series, link
                    FROM movies WHERE id = %s AND chat_id = %s
                ''', (film_id, chat_id))
                db_row = cursor_local.fetchone()
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
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
            message_thread_id = getattr(call.message, 'message_thread_id', None)
            show_film_info_with_buttons(chat_id, user_id, info, link, kp_id, existing=existing, message_id=message_id, message_thread_id=message_thread_id)
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
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        film_id = None
        film_already_in_db = False
        try:
            with db_lock:
                cursor_local.execute('SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
                existing = cursor_local.fetchone()
                
                if existing:
                    film_id = existing.get('id') if isinstance(existing, dict) else existing[0]
                    title = existing.get('title') if isinstance(existing, dict) else existing[1]
                    film_already_in_db = True
                else:
                    film_id = None
                    film_already_in_db = False
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Получаем часовой пояс пользователя
        user_tz = pytz.timezone('Europe/Moscow')  # По умолчанию
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                cursor_local.execute("SELECT value FROM settings WHERE chat_id = %s AND key = 'timezone'", (chat_id,))
                tz_row = cursor_local.fetchone()
                if tz_row:
                    tz_str = tz_row.get('value') if isinstance(tz_row, dict) else tz_row[0]
                    user_tz = pytz.timezone(tz_str)
        except:
            pass
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
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
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                try:
                    # Если фильма нет в базе, добавляем его (но не коммитим пока)
                    if not film_already_in_db:
                        cursor_local.execute('''
                            INSERT INTO movies (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'link')
                            ON CONFLICT (chat_id, kp_id) DO UPDATE SET link = EXCLUDED.link, is_series = EXCLUDED.is_series
                            RETURNING id
                        ''', (chat_id, link, kp_id, info['title'], info['year'], info['genres'], info['description'], 
                              info['director'], info['actors'], 1 if info.get('is_series') else 0, user_id))
                        
                        result = cursor_local.fetchone()
                        film_id = result.get('id') if isinstance(result, dict) else result[0]
                        film_added = True
                        logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в транзакции: film_id={film_id}, title={title}")
                    
                    # Проверяем, нет ли уже плана на эту дату
                    cursor_local.execute('''
                        SELECT id FROM plans 
                        WHERE chat_id = %s AND film_id = %s AND plan_type = 'cinema' AND DATE(plan_datetime AT TIME ZONE 'UTC' AT TIME ZONE %s) = %s
                    ''', (chat_id, film_id, str(user_tz), premiere_date))
                    existing_plan = cursor_local.fetchone()
                    
                    if not existing_plan:
                        # Добавляем план в расписание
                        cursor_local.execute('''
                            INSERT INTO plans (chat_id, film_id, plan_type, plan_datetime, user_id)
                            VALUES (%s, %s, 'cinema', %s, %s)
                            RETURNING id
                        ''', (chat_id, film_id, plan_utc, user_id))
                        
                        plan_row = cursor_local.fetchone()
                        if plan_row:
                            plan_id = plan_row.get('id') if isinstance(plan_row, dict) else plan_row[0]
                            # Коммитим транзакцию только если план успешно добавлен
                            conn_local.commit()
                            logger.info(f"[PREMIERE NOTIFY] План успешно добавлен: plan_id={plan_id}, film_id={film_id}")
                            if film_added:
                                logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в базу как следствие успешного добавления плана")
                        else:
                            logger.error(f"[PREMIERE NOTIFY] План не был создан, но ошибки не было")
                            conn_local.rollback()
                            bot.answer_callback_query(call.id, "❌ Ошибка при добавлении в расписание", show_alert=True)
                            return
                    else:
                        plan_id = existing_plan.get('id') if isinstance(existing_plan, dict) else existing_plan[0]
                        # Если план уже существует, коммитим только если фильм был добавлен
                        if film_added:
                            conn_local.commit()
                            logger.info(f"[PREMIERE NOTIFY] Фильм добавлен в базу, план уже существовал: plan_id={plan_id}")
                        else:
                            logger.info(f"[PREMIERE NOTIFY] План уже существует: plan_id={plan_id}")
                        
                except Exception as e:
                    conn_local.rollback()
                    logger.error(f"[PREMIERE NOTIFY] Ошибка в транзакции: {e}", exc_info=True)
                    bot.answer_callback_query(call.id, "❌ Ошибка при добавлении в расписание", show_alert=True)
                    return
        finally:
            try:
                cursor_local.close()
            except:
                pass
            try:
                conn_local.close()
            except:
                pass
        
        # Отправляем сообщение-подтверждение
        time_str = f"{int(hour):02d}:{int(minute):02d}"
        confirm_text = f"✅ <b>Уведомление установлено!</b>\n\n"
        confirm_text += f"📺 <b>{title}</b>\n"
        confirm_text += f"📅 Дата выхода: {premiere_date.strftime('%d.%m.%Y')}\n"
        confirm_text += f"🎬 Добавлено в расписание: в кино на {premiere_date.strftime('%d.%m.%Y')} в {time_str}\n\n"
        confirm_text += f"Если это ошибка, нажмите кнопку ниже для отмены."
        
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton("◀️ Вернуться к описанию", callback_data=f"back_to_film:{int(kp_id)}"))
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

# Обработчик show_film_description удален - теперь используется единый back_to_film_description из film_callbacks.py
# Все кнопки теперь используют callback_data="back_to_film:{kp_id}"


# Обработчик "Отменить" (если ещё не работает — замени полностью)
@bot.callback_query_handler(func=lambda call: call.data.startswith("premiere_cancel:"))
def premiere_cancel_handler(call):
    try:
        bot.answer_callback_query(call.id)
        
        parts = call.data.split(":")
        kp_id = parts[1]
        plan_id = int(parts[2]) if len(parts) > 2 else None
        
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        deleted_text = "❌ <b>Уведомление отменено</b>"
        
        conn_local = get_db_connection()
        cursor_local = get_db_cursor()
        try:
            with db_lock:
                if plan_id:
                    cursor_local.execute(
                        'DELETE FROM plans WHERE id = %s AND chat_id = %s AND user_id = %s',
                        (plan_id, chat_id, user_id)
                    )
                    cursor_local.rowcount  # проверяем, удалили ли
                
                cursor_local.execute(
                    'SELECT id, title FROM movies WHERE chat_id = %s AND kp_id = %s',
                    (chat_id, kp_id)
                )
                film = cursor_local.fetchone()
                
                if film:
                    film_id, title = film
                    deleted_text += f"\n\nФильм <b>{title}</b> остаётся в базе."
                
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
        
        bot.edit_message_text(deleted_text, chat_id, call.message.message_id, parse_mode='HTML')
        logger.info(f"[PREMIERE CANCEL] Отменено: kp_id={kp_id}, plan_id={plan_id}")
        
    except Exception as e:
        logger.error(f"[PREMIERE CANCEL] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "❌ Ошибка отмены", show_alert=True)

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

# Дублирующий обработчик удален - используется premiere_show_description выше

def register_premieres_callbacks(bot):
    """Регистрирует обработчики премьер (уже зарегистрированы через декораторы)"""
    pass

