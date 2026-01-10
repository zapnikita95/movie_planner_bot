from moviebot.bot.bot_init import bot
"""
Callback handlers для карточки фильма (add_to_database, plan_from_added, show_facts и т.д.)
"""
import logging
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton

from moviebot.database.db_connection import get_db_connection, get_db_cursor, db_lock, db_semaphore
from moviebot.api.kinopoisk_api import get_facts
from moviebot.api.kinopoisk_api import get_external_sources  # Добавил это для фикса NameError
from moviebot.utils.helpers import extract_film_info_from_existing
from psycopg2.extras import RealDictCursor
from moviebot.states import user_plan_state

logger = logging.getLogger(__name__)
conn = get_db_connection()
cursor = get_db_cursor()

# Глобальный кэш источников (должен быть один раз в начале файла, если нет — добавь)
if 'streaming_sources_cache' not in globals():
    streaming_sources_cache = {}
    
@bot.callback_query_handler(func=lambda call: call.data.startswith("add_to_database:"))
def add_to_database_callback(call):
    """Обработчик кнопки '➕ Добавить в базу'"""
    logger.info("=" * 80)
    logger.info(f"[ADD TO DATABASE] START: callback_id={call.id}, data={call.data}")

    film_id = None
    title_db = None
    watched = 0
    existing = None

    try:
        bot.answer_callback_query(call.id, text="⏳ Добавляю в базу...")

        kp_id_str = call.data.split(":")[1]
        kp_id = int(kp_id_str)
        chat_id = call.message.chat.id
        user_id = call.from_user.id

        logger.info(f"[ADD TO DATABASE] kp_id={kp_id}, chat_id={chat_id}")

        conn.rollback()  # чистим транзакцию на всякий

        with db_lock:
            cursor.execute("""
                SELECT id, title, link, watched, is_series 
                FROM movies 
                WHERE chat_id = %s AND kp_id = %s
            """, (chat_id, kp_id_str))
            row = cursor.fetchone()

        if row:
            # Уже в базе — берём ВСЁ из базы
            film_id = row[0]
            title_db = row[1]
            link = row[2] or f"https://www.kinopoisk.ru/film/{kp_id}/"
            watched = row[3] or 0
            is_series = bool(row[4])

            existing = (film_id, title_db, watched)

            # Делаем второй запрос — берём полные данные (description и т.д.)
            with db_lock:
                cursor.execute("""
                    SELECT title, year, genres, description, director, actors, is_series
                    FROM movies 
                    WHERE id = %s
                """, (film_id,))
                full_row = cursor.fetchone()

            if full_row:
                info = {
                    'title': full_row[0],
                    'year': full_row[1],
                    'genres': full_row[2],
                    'description': full_row[3],
                    'director': full_row[4],
                    'actors': full_row[5],
                    'is_series': bool(full_row[6])
                }
            else:
                info = {
                    'title': title_db,
                    'year': None,
                    'genres': None,
                    'description': None,
                    'director': None,
                    'actors': None,
                    'is_series': is_series
                }

            logger.info(f"[ADD TO DATABASE] Уже в базе: film_id={film_id}, title={title_db}")
            bot.answer_callback_query(call.id, f"ℹ️ {title_db} уже в базе", show_alert=False)

        else:
            # Новый сериал/фильм — парсим из сообщения
            logger.info("[ADD TO DATABASE] Не найден → парсим из сообщения")

            message_text = call.message.text or ""

            import re
            from html import unescape

            # Название + год
            title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>\s*\((\d{4})\)', message_text)
            if title_match:
                title = unescape(title_match.group(1))
                year = int(title_match.group(2))
            else:
                title_match = re.search(r'[📺🎬]\s*<b>(.*?)</b>', message_text)
                title = unescape(title_match.group(1)) if title_match else f"Фильм {kp_id}"
                year_match = re.search(r'\((\d{4})\)', message_text)
                year = int(year_match.group(1)) if year_match else None

            director = unescape(re.search(r'<i>Режиссёр:</i>\s*(.+?)(?:\n|$)', message_text).group(1).strip()) if re.search(r'<i>Режиссёр:</i>', message_text) else None
            genres = unescape(re.search(r'<i>Жанры:</i>\s*(.+?)(?:\n|$)', message_text).group(1).strip()) if re.search(r'<i>Жанры:</i>', message_text) else None
            actors = unescape(re.search(r'<i>В ролях:</i>\s*(.+?)(?:\n|$)', message_text).group(1).strip()) if re.search(r'<i>В ролях:</i>', message_text) else None
            desc_match = re.search(r'<i>Кратко:</i>\s*(.+?)(?:\n|🟢|🔴|Кинопоиск|$)', message_text, re.DOTALL)
            description = unescape(desc_match.group(1).strip()) if desc_match else None

            is_series = '📺' in message_text
            link = f"https://www.kinopoisk.ru/series/{kp_id}/" if is_series else f"https://www.kinopoisk.ru/film/{kp_id}/"

            info = {
                'title': title,
                'year': year,
                'genres': genres,
                'description': description,
                'director': director,
                'actors': actors,
                'is_series': is_series
            }

            # Добавляем в базу
            with db_lock:
                cursor.execute('''
                    INSERT INTO movies 
                    (chat_id, link, kp_id, title, year, genres, description, director, actors, is_series, added_by, added_at, source)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), 'button')
                    ON CONFLICT (chat_id, kp_id) DO UPDATE SET 
                        link = EXCLUDED.link,
                        title = EXCLUDED.title,
                        year = COALESCE(EXCLUDED.year, movies.year),
                        genres = COALESCE(EXCLUDED.genres, movies.genres),
                        description = COALESCE(EXCLUDED.description, movies.description),
                        director = COALESCE(EXCLUDED.director, movies.director),
                        actors = COALESCE(EXCLUDED.actors, movies.actors),
                        is_series = EXCLUDED.is_series
                    RETURNING id, title, watched
                ''', (
                    chat_id, link, kp_id_str, title, year, genres, description, director, actors,
                    1 if is_series else 0, user_id
                ))

                result = cursor.fetchone()
                conn.commit()

                if result:
                    if isinstance(result, dict):
                        film_id = result.get('id')
                        title_db = result.get('title')
                        watched = result.get('watched', 0)
                    else:
                        film_id = result[0]
                        title_db = result[1]
                        watched = result[2] if len(result) > 2 else 0

                    existing = (film_id, title_db, watched)
                else:
                    film_id = None
                    title_db = title
                    watched = 0
                    existing = None
                    logger.warning("[ADD TO DATABASE] RETURNING вернул None — использую данные из сообщения")

            bot.answer_callback_query(call.id, f"✅ {title_db} добавлен в базу!", show_alert=False)

        # Показываем карточку с полными данными
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            existing=existing,
            message_id=call.message.message_id,
            message_thread_id=getattr(call.message, 'message_thread_id', None)
        )

    except Exception as e:
        logger.error(f"[ADD TO DATABASE] КРИТИЧЕСКАЯ ОШИБКА: {e}", exc_info=True)
        try:
            conn.rollback()
        except:
            pass
        try:
            bot.answer_callback_query(call.id, "❌ Ошибка добавления", show_alert=True)
        except:
            pass

    finally:
        logger.info(f"[ADD TO DATABASE] END")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_from_added:"))
def plan_from_added_callback(call):
    """Обработчик 'Запланировать просмотр' — добавляет фильм в базу, если его нет, и запускает планирование"""
    logger.info(f"[PLAN FROM ADDED] ===== НАЧАЛО ОБРАБОТКИ =====")
    try:
        from moviebot.bot.bot_init import safe_answer_callback_query
        safe_answer_callback_query(bot, call.id)
        
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        kp_id_str = call.data.split(":")[1]
        kp_id = int(kp_id_str)  # для логов и вызовов
        kp_id_db = str(kp_id)   # для SQL-запросов (kp_id в БД — TEXT)
        
        logger.info(f"[PLAN FROM ADDED] Пользователь {user_id} хочет запланировать kp_id={kp_id}")
        
        # === ФИКС: берём реальное название и is_series из API или БД ===
        title = None
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        is_series = False

        # 1. Пробуем взять из базы (самое быстрое)
        try:
            conn_check = get_db_connection()                    # ← новое имя
            cur_check = conn_check.cursor(cursor_factory=RealDictCursor)
            cur_check.execute(
                'SELECT title, link, is_series FROM movies WHERE chat_id = %s AND kp_id = %s',
                (chat_id, str(kp_id))
            )
            row = cur_check.fetchone()
            if row:
                title = row['title']
                link = row['link']
                is_series = bool(row['is_series'])
                logger.info(f"[PLAN FROM ADDED] Название взято из базы: {title}")
            cur_check.close()
            conn_check.close()
        except Exception as db_e:
            logger.error(f"[PLAN FROM ADDED] Ошибка чтения из БД: {db_e}", exc_info=True)
            title = None
            link = None
            is_series = False
        finally:
            if 'cursor' in locals():
                try:
                    cursor.close()
                except:
                    pass
                
        # 2. Если в базе нет — берём из API (надёжно)
        if not title:
            from moviebot.api.kinopoisk_api import extract_movie_info
            temp_link = f"https://www.kinopoisk.ru/series/{kp_id}/" if 'series' in call.message.text.lower() else f"https://www.kinopoisk.ru/film/{kp_id}/"
            info = extract_movie_info(temp_link)
            if info and info.get('title'):
                title = info['title']
                link = info.get('link', temp_link)
                is_series = info.get('is_series', False)
                logger.info(f"[PLAN FROM ADDED] Название взято из API: {title}")
            else:
                title = f"Фильм {kp_id}"  # Только крайний фолбек
                link = temp_link
                logger.warning(f"[PLAN FROM ADDED] Не удалось получить название из API, используем фолбек")
        
        # Добавляем фильм в базу, если его нет
        film_id = None
        watched = 0  # дефолт для нового фильма
        existing = None
        try:
            with db_semaphore:
                with db_lock:
                    cur_add = conn.cursor(cursor_factory=RealDictCursor)
                    # Проверяем наличие + сразу берём нужные поля
                    cur_add.execute(
                        'SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s',
                        (chat_id, str(kp_id))
                    )
                    row = cur_add.fetchone()

                    if row:
                        existing = row  # row — RealDictRow
                        film_id, watched = extract_film_info_from_existing(existing)
                        logger.info(f"[PLAN FROM ADDED] Фильм уже в базе: film_id={film_id}, watched={watched}")
                    else:
                        # Добавляем новый
                        is_series_int = 1 if is_series else 0
                        cur_add.execute('''
                            INSERT INTO movies (chat_id, kp_id, title, link, is_series, added_by, added_at, source)
                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), 'plan_button')
                            ON CONFLICT (chat_id, kp_id) DO NOTHING
                            RETURNING id, title, watched
                        ''', (chat_id, str(kp_id), title, link, is_series_int, user_id))
                        
                        result = cur_add.fetchone()
                        if result:
                            existing = result
                            film_id, watched = extract_film_info_from_existing(existing)
                            logger.info(f"[PLAN FROM ADDED] Фильм добавлен: film_id={film_id}")
                        
                        conn.commit()
                    
                    cur_add.close()
        except Exception as db_e:
            conn.rollback()
            logger.error(f"[PLAN FROM ADDED] Ошибка БД при добавлении фильма: {db_e}", exc_info=True)
            bot.send_message(chat_id, "❌ Ошибка при добавлении фильма в базу.")
            return
        
        if not film_id:
            bot.send_message(chat_id, "❌ Не удалось добавить фильм в базу.")
            return
        
        logger.info(f"[PLAN FROM ADDED] Фильм готов к планированию: film_id={film_id}, kp_id={kp_id}, title={title}")
        
        # Запускаем планирование
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Дома 🏠", callback_data=f"plan_type:home:{int(kp_id)}"),
            InlineKeyboardButton("В кино 🎥", callback_data=f"plan_type:cinema:{int(kp_id)}")
        )
        
        # Убираем старые кнопки (опционально, если хочешь)
        try:
            bot.edit_message_reply_markup(chat_id=chat_id, message_id=call.message.message_id, reply_markup=None)
        except:
            pass
        
        bot.send_message(
            chat_id,
            f"✅ Фильм '<b>{title}</b>' добавлен в базу!\n\nГде планируете смотреть?",
            reply_markup=markup,
            parse_mode='HTML'
        )
        
        # Если хочешь — очисти fake_message и start_plan_home_or_cinema
        # (оставь, если нужно, или удали, если уже используешь состояния)
        
    except Exception as e:
        logger.error(f"[PLAN FROM ADDED] Критическая ошибка: {e}", exc_info=True)
        try:
            try:
                bot.answer_callback_query(call.id, "❌ Ошибка планирования", show_alert=True)
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
    finally:
        logger.info(f"[PLAN FROM ADDED] ===== КОНЕЦ ОБРАБОТКИ =====")
        
@bot.callback_query_handler(func=lambda call: call.data.startswith("show_facts:") or call.data.startswith("facts:"))
def show_facts_callback(call):
    """Обработчик кнопки 'Интересные факты'"""
    try:
        kp_id = call.data.split(":")[1]
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        
        logger.info(f"[SHOW FACTS] Пользователь {user_id} запросил факты для kp_id={kp_id}")
        
        # Получаем факты
        facts = get_facts(kp_id)
        if facts:
            bot.send_message(chat_id, facts, parse_mode='HTML')
            try:
                try:
                    bot.answer_callback_query(call.id, "Факты отправлены")
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        else:
            try:
                try:
                    bot.answer_callback_query(call.id, "Факты не найдены", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
    except Exception as e:
        logger.error(f"[SHOW FACTS] Ошибка: {e}", exc_info=True)
    finally:
        # ВСЕГДА отвечаем на callback!
        try:
            try:
                try:
                    bot.answer_callback_query(call.id)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as answer_e:
            logger.error(f"[SHOW FACTS] Не удалось ответить на callback: {answer_e}", exc_info=True)


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_type:"), priority=1)
def plan_type_callback_fallback(call):
    """Запасной обработчик выбора типа плана (на случай, если основной не срабатывает)"""
    logger.info("=" * 80)
    logger.info(f"[PLAN TYPE FALLBACK] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        try:
            try:
                bot.answer_callback_query(call.id)
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        plan_type = call.data.split(":")[1]  # 'home' или 'cinema'
        
        logger.info(f"[PLAN TYPE FALLBACK] Получен callback: user_id={user_id}, chat_id={chat_id}, plan_type={plan_type}")
        logger.info(f"[PLAN TYPE FALLBACK] user_plan_state keys={list(user_plan_state.keys())}")
        logger.info(f"[PLAN TYPE FALLBACK] user_id in user_plan_state = {user_id in user_plan_state}")
        
        if user_id not in user_plan_state:
            logger.warning(f"[PLAN TYPE FALLBACK] Состояние не найдено для user_id={user_id}")
            bot.edit_message_text("❌ Ошибка: сессия истекла. Начните заново с /plan", chat_id, call.message.message_id)
            return
        
        state = user_plan_state[user_id]
        link = state.get('link')
        
        if not link:
            logger.warning(f"[PLAN TYPE FALLBACK] Ссылка не найдена в состоянии: {state}")
            bot.edit_message_text("❌ Ошибка: не найдена ссылка на фильм. Начните заново с /plan", chat_id, call.message.message_id)
            del user_plan_state[user_id]
            return
        
        state['type'] = plan_type
        state['step'] = 3
        
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except:
            pass
        
        bot.send_message(chat_id, f"📅 Когда планируете смотреть {'дома' if plan_type == 'home' else 'в кино'}?\n\nМожно указать:\n• День недели (сегодня, завтра, понедельник и т.д.)\n• Дату (01.01, 1 января и т.д.)\n• Время (19:00, 20:30)")
        
        logger.info(f"[PLAN TYPE FALLBACK] Пользователь {user_id} выбрал {plan_type}, link={link}")
    except Exception as e:
        logger.error(f"[PLAN TYPE FALLBACK] Ошибка: {e}", exc_info=True)
        try:
            try:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except:
            pass
    finally:
        logger.info(f"[PLAN TYPE FALLBACK] ===== END: callback_id={call.id}")

@bot.callback_query_handler(func=lambda call: call.data.startswith('plan_type:'))
@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("plan_type:"))
def handle_plan_type(call):
    try:
        bot.answer_callback_query(call.id, "Выбрано!")
    except Exception as e:
        logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")

    try:
        parts = call.data.split(':')
        if len(parts) < 2:
            logger.warning(f"[PLAN TYPE] Неправильный формат callback_data: {call.data}")
            bot.send_message(call.message.chat.id, "❌ Ошибка формата. Попробуйте заново.")
            return

        plan_type = parts[1]  # 'home' или 'cinema'
        kp_id = None
        
        # Если есть третья часть — берём kp_id оттуда
        if len(parts) >= 3 and parts[2]:
            try:
                kp_id = int(parts[2])
                logger.info(f"[PLAN TYPE] kp_id взят из callback: {kp_id}")
            except ValueError:
                logger.warning(f"[PLAN TYPE] Некорректный kp_id в callback: {parts[2]}")
        
        # Если kp_id не в callback — пытаемся взять из состояния (если ранее сохранён)
        if kp_id is None:
            state = user_plan_state.get(call.from_user.id, {})
            kp_id = state.get('kp_id')
            if kp_id:
                logger.info(f"[PLAN TYPE] kp_id взят из состояния: {kp_id}")
            else:
                logger.warning(f"[PLAN TYPE] kp_id не найден ни в callback, ни в состоянии: {call.data}")
                bot.send_message(call.message.chat.id, "❌ Фильм не определён. Начните планирование заново.")
                return

        user_id = call.from_user.id
        chat_id = call.message.chat.id

        # Ищем в БД film_id, link, title, watched (чтобы existing был готов)
        with db_semaphore:
            with db_lock:
                cursor.execute('''
                    SELECT id, title, watched, link 
                    FROM movies 
                    WHERE chat_id = %s AND kp_id = %s
                ''', (chat_id, str(kp_id)))
                row = cursor.fetchone()
                
                if not row:
                    bot.send_message(chat_id, "❌ Фильм не найден в базе. Попробуйте заново.")
                    return
                
                # Создаём existing ДО вызова extract
                existing = row  # row — это RealDictRow или tuple с id, title, watched, link
                
                # Извлекаем film_id и watched
                film_id, watched = extract_film_info_from_existing(existing)
                
                # link берём из row
                if isinstance(row, dict):
                    link = row.get('link')
                else:  # tuple
                    link = row[3] if len(row) > 3 else None
        
        # Сохраняем состояние с step=3
        user_plan_state[user_id] = {
            'step': 3,
            'plan_type': plan_type,
            'link': link,
            'kp_id': kp_id,
            'film_id': film_id
        }
        logger.info(f"[PLAN TYPE] Состояние сохранено для user {user_id}: step=3, plan_type={plan_type}, kp_id={kp_id}")

        # Удаляем сообщение с кнопками
        try:
            bot.delete_message(chat_id, call.message.message_id)
        except Exception as e:
            logger.debug(f"[PLAN TYPE] Не удалось удалить сообщение: {e}")

        bot.send_message(
            chat_id,
            "📅 Когда планируете смотреть?\n\nПримеры:\n• сегодня\n• завтра 20:00\n• 15.01\n• понедельник вечером"
        )
        
    except Exception as e:
        logger.error(f"[PLAN TYPE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка, попробуйте заново.", show_alert=True)
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback: {e}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_film:"))
def back_to_film_description(call):
    """Кнопка '◀️ Вернуться к описанию' — показывает свежую карточку через show_film_info_with_buttons"""
    logger.info(f"[BACK TO FILM] START: data={call.data}")
    
    chat_id = call.message.chat.id
    user_id = call.from_user.id
    message_id = call.message.message_id
    message_thread_id = getattr(call.message, 'message_thread_id', None)
    
    try:
        bot.answer_callback_query(call.id, text="⏳ Загружаю описание...")
        
        # Парсим kp_id как строку (PostgreSQL хранит как TEXT)
        kp_id_str = call.data.split(":")[1]
        kp_id = str(int(kp_id_str))  # для логов и вызовов
        kp_id_db = str(kp_id)   # для SQL-запросов в БД
        
        logger.info(f"[BACK TO FILM] kp_id={kp_id}, chat_id={chat_id}")
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        
        # ─── 1. Пробуем свежие данные из API ───────────────────────────────
        info = None
        try:
            from moviebot.api.kinopoisk_api import extract_movie_info
            info = extract_movie_info(link)
            if info and info.get('title'):
                logger.info(f"[BACK TO FILM] Свежие данные из API: {info['title']}")
        except Exception as api_e:
            logger.warning(f"[BACK TO FILM] API ошибка: {api_e}")
        
        # ─── 2. Fallback на БД (с правильной обработкой транзакций) ────────
        if not info:
            logger.info("[BACK TO FILM] API не сработал → БД")
            with db_lock:
                # TRY-FINALLY для отката транзакции при ошибке
                try:
                    cursor.execute('''
                        SELECT title, year, genres, description, director, actors, is_series, id, watched
                        FROM movies 
                        WHERE chat_id = %s AND kp_id = %s
                    ''', (chat_id, kp_id_db))  # ← kp_id как STRING!
                    row = cursor.fetchone()
                    
                    if row:
                        info = {
                            'title': row[0],
                            'year': row[1],
                            'genres': row[2],
                            'description': row[3],
                            'director': row[4],
                            'actors': row[5],
                            'is_series': bool(row[6])
                        }
                        logger.info(f"[BACK TO FILM] Данные из БД: {info['title']}")
                    conn.commit()
                    
                except Exception as db_e:
                    logger.error(f"[BACK TO FILM] SQL ошибка: {db_e}")
                    conn.rollback()  # ← КРИТИЧНО: откатываем aborted transaction
                    raise
        
        if not info or not info.get('title'):
            bot.edit_message_text(
                "❌ Не удалось загрузить информацию о фильме",
                chat_id, message_id, message_thread_id=message_thread_id,
                parse_mode='HTML'
            )
            return
        
        # ─── 3. Определяем existing (с транзакционной защитой) ────────────
        existing = None
        with db_lock:
            try:
                cursor.execute(
                    "SELECT id, title, watched FROM movies WHERE chat_id = %s AND kp_id = %s",
                    (chat_id, kp_id_db)  # ← снова STRING!
                )
                row = cursor.fetchone()
                
                if row:
                    film_id = row[0]
                    title_db = row[1]
                    watched = row[2]
                    existing = (film_id, title_db, watched)
                    logger.info(f"[BACK TO FILM] existing найден: {film_id}")
                
                conn.commit()
            except Exception as db_e:
                logger.error(f"[BACK TO FILM] Ошибка existing: {db_e}")
                conn.rollback()
                # existing=None — продолжаем без него
        
        # ─── 4. Показываем карточку (именно то, что ты хотел!) ───────────
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
        
        logger.info(f"[BACK TO FILM] Карточка показана kp_id={kp_id}")
        
    except Exception as e:
        logger.error(f"[BACK TO FILM] Критическая ошибка: {e}", exc_info=True)
        try:
            # Финальный откат на всякий случай
            with db_lock:
                conn.rollback()
            bot.edit_message_text(
                "❌ Ошибка загрузки описания",
                chat_id, message_id, message_thread_id=message_thread_id
            )
        except:
            pass
    
    logger.info(f"[BACK TO FILM] END")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("mark_watched_from_description:"))
def mark_watched_from_description_callback(call):
    """Обработчик кнопки '👁️ Просмотрено' - отмечает фильм как просмотренный и обновляет сообщение"""
    logger.info("=" * 80)
    logger.info(f"[MARK WATCHED] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        # Отвечаем на callback сразу
        try:
            try:
                bot.answer_callback_query(call.id, text="⏳ Отмечаю как просмотренный...")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        logger.info(f"[MARK WATCHED] answer_callback_query вызван, callback_id={call.id}")
        
        film_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        logger.info(f"[MARK WATCHED] film_id={film_id}, user_id={user_id}, chat_id={chat_id}, message_id={message_id}")
        
        # Получаем информацию о фильме из БД
        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, kp_id, year, genres, description, director, actors, is_series
                FROM movies WHERE id = %s AND chat_id = %s
            ''', (film_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                logger.error(f"[MARK WATCHED] Фильм не найден: film_id={film_id}, chat_id={chat_id}")
                try:
                    try:
                        bot.answer_callback_query(call.id, "❌ Фильм не найден", show_alert=True)
                    except Exception as e:
                        logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
                return
            
            # Извлекаем данные
            if isinstance(row, dict):
                title = row.get('title')
                watched = row.get('watched')
                link = row.get('link')
                kp_id = row.get('kp_id')
                year = row.get('year')
                genres = row.get('genres')
                description = row.get('description')
                director = row.get('director')
                actors = row.get('actors')
                is_series = bool(row.get('is_series', 0))
            else:
                title = row[1]
                watched = row[2]
                link = row[3]
                kp_id = row[4]
                year = row[5]
                genres = row[6]
                description = row[7]
                director = row[8]
                actors = row[9]
                is_series = bool(row[10] if len(row) > 10 else 0)
            
            # Отмечаем фильм как просмотренный
            cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
            logger.info(f"[MARK WATCHED] Фильм {film_id} отмечен как просмотренный")
        
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
        
        # Обновляем existing (теперь watched=1)
        existing = (film_id, title, True)
        
        # Обновляем сообщение с описанием фильма
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=existing,
            message_id=message_id, message_thread_id=message_thread_id
        )
        
        logger.info(f"[MARK WATCHED] Сообщение обновлено: film_id={film_id}, kp_id={kp_id}")
        try:
            try:
                bot.answer_callback_query(call.id, text="✅ Фильм отмечен как просмотренный", show_alert=False)
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        
    except Exception as e:
        logger.error(f"[MARK WATCHED] Ошибка: {e}", exc_info=True)
        try:
            try:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except:
            pass
    finally:
        logger.info(f"[MARK WATCHED] ===== END: callback_id={call.id}")

# Глобальный кэш источников (в памяти, живёт пока бот запущен)
if 'streaming_sources_cache' not in globals():
    streaming_sources_cache = {}

@bot.callback_query_handler(func=lambda call: call.data.startswith("streaming_select:"))
def streaming_select_callback(call):
    try:
        bot.answer_callback_query(call.id)

        # Извлекаем kp_id
        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        message_id = call.message.message_id

        # Получаем источники
        sources = get_external_sources(kp_id)

        if not sources:
            bot.edit_message_text(
                "😔 Для этого фильма/сериала нет доступных онлайн-платформ в России.\n"
                "Можно поискать на торрентах или зарубежных сервисах (VPN).\n\n"
                "◀️ Назад к описанию",
                chat_id,
                message_id,
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Назад к описанию", callback_data=f"back_to_film:{kp_id}")
                )
            )
            return

        # Сохраняем источники в кэш по kp_id (ключ — строка!)
        streaming_sources_cache[str(kp_id)] = sources

        # Создаём клавиатуру с КОРОТКИМИ callback_data
        markup = InlineKeyboardMarkup(row_width=1)
        for idx, (platform, url) in enumerate(sources):
            # Короткий callback: sel:kp_id:индекс
            callback_data = f"sel:{kp_id}:{idx}"
            markup.add(InlineKeyboardButton(platform, callback_data=callback_data))

        markup.add(InlineKeyboardButton("◀️ Назад к описанию", callback_data=f"back_to_film:{kp_id}"))

        bot.edit_message_text(
            "Выберите онлайн-кинотеатр:",
            chat_id,
            message_id,
            reply_markup=markup
        )

    except Exception as e:
        logger.error(f"[STREAMING SELECT] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка, попробуйте позже", show_alert=True)

# Добавь новый обработчик (или в существующий callback-хэндлер)
@bot.callback_query_handler(func=lambda call: call.data.startswith('s:'))
def streaming_source_select(call):
    try:
        _, kp_id_str, idx_str = call.data.split(':')
        kp_id = str(int(kp_id_str))
        idx = int(idx_str)
        
        sources = streaming_sources_cache.get(str(kp_id), [])
        if idx >= len(sources):
            bot.answer_callback_query(call.id, "Источник не найден", show_alert=True)
            return
        
        source = sources[idx]
        url = source.get('url', '#')
        platform = source.get('platform', 'Платформа')
        
        bot.edit_message_text(
            chat_id=call.message.chat.id,
            message_id=call.message.message_id,
            text=f"Смотрите на {platform}:\n{url}",
            reply_markup=InlineKeyboardMarkup().add(
                InlineKeyboardButton("Открыть", url=url),
                InlineKeyboardButton("← Назад", callback_data=f"stream_sel:{kp_id}")
            )
        )
        bot.answer_callback_query(call.id, f"Открываем {platform}")
        
    except Exception as e:
        logger.error(f"[STREAMING SOURCE] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("sel:"))
def select_platform_callback(call):
    try:
        bot.answer_callback_query(call.id, "Открываем...")

        # Разбираем: sel:kp_id:idx
        parts = call.data.split(":")
        if len(parts) != 3:
            raise ValueError("Неверный формат callback_data")

        kp_id = int(parts[1])
        idx = int(parts[2])

        # Восстанавливаем источник из кэша
        sources = streaming_sources_cache.get(str(kp_id), [])
        if idx >= len(sources) or idx < 0:
            bot.edit_message_text(
                "Источник не найден. Попробуйте заново.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=InlineKeyboardMarkup().add(
                    InlineKeyboardButton("◀️ Назад к выбору", callback_data=f"streaming_select:{kp_id}")
                )
            )
            return

        platform, url = sources[idx]

        # Показываем ссылку
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton(f"Открыть {platform}", url=url),
            InlineKeyboardButton("◀️ Назад к выбору", callback_data=f"streaming_select:{kp_id}")
        )

        bot.edit_message_text(
            f"Смотрите на **{platform}**:\n\n{url}",
            call.message.chat.id,
            call.message.message_id,
            parse_mode='Markdown',
            reply_markup=markup
        )

        logger.info(f"[SELECT PLATFORM] Открыт источник {platform} для kp_id={kp_id}")

        # Опционально: очищаем кэш после использования
        if str(kp_id) in streaming_sources_cache:
            del streaming_sources_cache[str(kp_id)]

    except Exception as e:
        logger.error(f"[SELECT PLATFORM] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка, попробуйте позже", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data.startswith("streaming_done:"))
def streaming_done_callback(call):
    """Завершение выбора онлайн-кинотеатров"""
    try:
        bot.answer_callback_query(call.id, "Готово!")
    except:
        pass
    
    plan_id = int(call.data.split(":")[1])
    chat_id = call.message.chat.id
    
    # Ставим streaming_done = True и убираем приписку + кнопки
    with db_lock:
        cursor.execute('''
            UPDATE plans 
            SET streaming_done = TRUE
            WHERE id = %s AND chat_id = %s
        ''', (plan_id, chat_id))
        conn.commit()
    
    # Убираем текст "Выберите..." и кнопки
    original_text = call.message.text.split("\n\n📺")[0].strip()
    if "✅ Выбран:" in original_text:
        original_text = original_text.split("\n\n✅ Выбран:")[0].strip()
    
    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text=original_text,
        parse_mode='HTML',
        reply_markup=None
    )
    
    logger.info(f"[STREAMING DONE] План {plan_id} завершён — кнопки убраны")

@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("mark_watched_from_description_kp:"))
def mark_watched_from_description_kp_callback(call):
    """Обработчик кнопки '👁️ Просмотрено' для фильмов, не добавленных в базу - добавляет фильм в базу как просмотренный"""
    logger.info("=" * 80)
    logger.info(f"[MARK WATCHED KP] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        # Отвечаем на callback сразу
        try:
            try:
                bot.answer_callback_query(call.id, text="⏳ Отмечаю как просмотренный...")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        
        kp_id = call.data.split(":")[1]
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        logger.info(f"[MARK WATCHED KP] kp_id={kp_id}, user_id={user_id}, chat_id={chat_id}, message_id={message_id}")
        
        # Получаем информацию о фильме через API
        from moviebot.api.kinopoisk_api import extract_movie_info
        link = f"https://www.kinopoisk.ru/film/{kp_id}/"
        info = extract_movie_info(link)
        
        if not info:
            try:
                try:
                    bot.answer_callback_query(call.id, "❌ Не удалось получить информацию о фильме", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            return
        
        # Добавляем фильм в базу как просмотренный
        from moviebot.bot.handlers.series import ensure_movie_in_database
        film_id, was_inserted = ensure_movie_in_database(chat_id, kp_id, link, info, user_id)
        
        if not film_id:
            from moviebot.bot.bot_init import safe_answer_callback_query
            safe_answer_callback_query(bot, call.id, "❌ Ошибка при добавлении фильма в базу", show_alert=True)
            return

        # Отмечаем фильм как просмотренный
        with db_lock:
            cursor.execute('UPDATE movies SET watched = 1 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
            logger.info(f"[MARK WATCHED KP] Фильм {film_id} добавлен в базу и отмечен как просмотренный")
        
        # Обновляем existing (теперь watched=1)
        existing = (film_id, info.get('title'), True)
        
        # Обновляем сообщение с описанием фильма
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=existing,
            message_id=message_id, message_thread_id=message_thread_id
        )
        
        logger.info(f"[MARK WATCHED KP] Сообщение обновлено: film_id={film_id}, kp_id={kp_id}")
        try:
            try:
                bot.answer_callback_query(call.id, text="✅ Фильм добавлен в базу и отмечен как просмотренный", show_alert=False)
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        
    except Exception as e:
        logger.error(f"[MARK WATCHED KP] Ошибка: {e}", exc_info=True)
        try:
            try:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except:
            pass
    finally:
        logger.info(f"[MARK WATCHED KP] ===== END: callback_id={call.id}")


@bot.callback_query_handler(func=lambda call: call.data and call.data.startswith("toggle_watched_from_description:"))
def toggle_watched_from_description_callback(call):
    """Обработчик кнопки '✅ Просмотрено' - снимает отметку просмотра"""
    logger.info("=" * 80)
    logger.info(f"[TOGGLE WATCHED] ===== START: callback_id={call.id}, callback_data={call.data}")
    try:
        # Отвечаем на callback сразу
        try:
            try:
                bot.answer_callback_query(call.id, text="⏳ Снимаю отметку просмотра...")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        
        film_id = int(call.data.split(":")[1])
        user_id = call.from_user.id
        chat_id = call.message.chat.id
        message_id = call.message.message_id if call.message else None
        message_thread_id = getattr(call.message, 'message_thread_id', None)
        
        logger.info(f"[TOGGLE WATCHED] film_id={film_id}, user_id={user_id}, chat_id={chat_id}, message_id={message_id}")
        
        # Получаем информацию о фильме из БД
        with db_lock:
            cursor.execute('''
                SELECT id, title, watched, link, kp_id, year, genres, description, director, actors, is_series
                FROM movies WHERE id = %s AND chat_id = %s
            ''', (film_id, chat_id))
            row = cursor.fetchone()
            
            if not row:
                logger.error(f"[TOGGLE WATCHED] Фильм не найден: film_id={film_id}, chat_id={chat_id}")
                try:
                    try:
                        bot.answer_callback_query(call.id, "❌ Фильм не найден", show_alert=True)
                    except Exception as e:
                        logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
                return
            
            # Извлекаем данные
            if isinstance(row, dict):
                title = row.get('title')
                watched = row.get('watched')
                link = row.get('link')
                kp_id = row.get('kp_id')
                year = row.get('year')
                genres = row.get('genres')
                description = row.get('description')
                director = row.get('director')
                actors = row.get('actors')
                is_series = bool(row.get('is_series', 0))
            else:
                title = row[1]
                watched = row[2]
                link = row[3]
                kp_id = row[4]
                year = row[5]
                genres = row[6]
                description = row[7]
                director = row[8]
                actors = row[9]
                is_series = bool(row[10] if len(row) > 10 else 0)
            
            # Снимаем отметку просмотра
            cursor.execute('UPDATE movies SET watched = 0 WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()
            logger.info(f"[TOGGLE WATCHED] Фильм {film_id} - отметка просмотра снята")
        
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
        
        # Обновляем existing (теперь watched=0)
        existing = (film_id, title, False)
        
        # Обновляем сообщение с описанием фильма
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id, user_id, info, link, kp_id, existing=existing,
            message_id=message_id, message_thread_id=message_thread_id
        )
        
        logger.info(f"[TOGGLE WATCHED] Сообщение обновлено: film_id={film_id}, kp_id={kp_id}")
        try:
            try:
                bot.answer_callback_query(call.id, text="✅ Отметка просмотра снята", show_alert=False)
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except Exception as e:
            logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        
    except Exception as e:
        logger.error(f"[TOGGLE WATCHED] Ошибка: {e}", exc_info=True)
        try:
            try:
                try:
                    bot.answer_callback_query(call.id, "❌ Ошибка обработки", show_alert=True)
                except Exception as e:
                    logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
            except Exception as e:
                logger.warning(f"[CALLBACK] Не удалось ответить на callback (query too old или ошибка): {e}")
        except:
            pass
    finally:
        logger.info(f"[TOGGLE WATCHED] ===== END: callback_id={call.id}")

@bot.callback_query_handler(func=lambda call: call.data.startswith("remove_from_database:"))
def remove_from_database_prompt(call):
    """Первый шаг: запрос подтверждения удаления фильма из базы"""
    try:
        bot.answer_callback_query(call.id)

        try:
            kp_id = int(call.data.split(":")[1])
        except (IndexError, ValueError):
            bot.answer_callback_query(call.id, "Ошибка: неверный ID фильма", show_alert=True)
            return

        kp_id_str = str(kp_id)

        chat_id = call.message.chat.id
        message_id = call.message.message_id
        user_id = call.from_user.id

        # Получаем название фильма для подтверждения
        with db_lock:
            cursor.execute('SELECT title FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, kp_id_str))
            row = cursor.fetchone()

        if not row:
            bot.edit_message_text(
                "Фильм уже удалён или не найден в вашей базе.",
                chat_id, message_id
            )
            return

        # Безопасно берём title — row это DictRow
        title = row.get('title') or "Фильм/сериал"
        short_title = (title[:50] + '...') if len(title) > 50 else title

        # Клавиатура подтверждения
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("✅ Да, удалить", callback_data=f"confirm_delete:{int(kp_id)}"),
            InlineKeyboardButton("❌ Нет", callback_data="delete_cancel")
        )

        bot.edit_message_text(
            f"🗑️ Вы уверены, что хотите удалить из базы?\n\n"
            f"<b>{short_title}</b>\n\n"
            f"Это действие нельзя отменить.",
            chat_id=chat_id,
            message_id=message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )

        logger.info(f"[REMOVE FROM DB PROMPT] Запрос подтверждения: user_id={user_id}, kp_id={kp_id}, title={title}")

    except Exception as e:
        logger.error(f"[REMOVE FROM DB PROMPT] Ошибка: user_id={user_id}, data={call.data} | {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка при обработке", show_alert=True)
        except:
            pass
        
@bot.callback_query_handler(func=lambda call: call.data == "delete_cancel")
def delete_cancel(call):
    """Отмена удаления"""
    try:
        bot.answer_callback_query(call.id)
        bot.edit_message_text("❌ Удаление отменено.", call.message.chat.id, call.message.message_id)
    except:
        pass

@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_delete:"))
def confirm_remove_from_database(call):
    """Финальное удаление после подтверждения"""
    logger.info("=" * 80)
    logger.info(f"[CONFIRM DELETE] START: callback_id={call.id}, data={call.data}")

    try:
        bot.answer_callback_query(call.id)

        kp_id_str = call.data.split(":")[1]
        kp_id = int(kp_id_str)
        chat_id = call.message.chat.id
        message_id = call.message.message_id
        user_id = call.from_user.id

        with db_lock:
            cursor.execute("""
                SELECT id, title 
                FROM movies 
                WHERE chat_id = %s AND kp_id = %s
            """, (chat_id, kp_id_str))
            film = cursor.fetchone()

            if not film:
                bot.edit_message_text(
                    "Фильм уже удалён или не найден.",
                    chat_id, message_id
                )
                return

            film_id = film[0] if isinstance(film, tuple) else film.get('id')
            title = film[1] if isinstance(film, tuple) else film.get('title', f"ID {kp_id}")

            # Удаляем всё связанное
            cursor.execute('DELETE FROM ratings WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            cursor.execute('DELETE FROM plans WHERE chat_id = %s AND film_id = %s', (chat_id, film_id))
            cursor.execute('DELETE FROM movies WHERE id = %s AND chat_id = %s', (film_id, chat_id))
            conn.commit()

        # Кнопка "Перейти к описанию"
        markup = InlineKeyboardMarkup()
        markup.add(InlineKeyboardButton(
            "📖 Перейти к описанию",
            callback_data=f"show_film_description:{kp_id}"
        ))

        bot.edit_message_text(
            f"✅ <b>{title}</b> успешно удалён из базы!",
            chat_id,
            message_id,
            reply_markup=markup,
            parse_mode='HTML'
        )

        logger.info(f"[REMOVE FROM DB] Успешно удалён: kp_id={kp_id}, title='{title}', user_id={user_id}")

    except Exception as e:
        logger.error(f"[CONFIRM DELETE] Ошибка: {e}", exc_info=True)
        try:
            bot.answer_callback_query(call.id, "Ошибка при удалении", show_alert=True)
            bot.edit_message_text(
                "Произошла ошибка при удалении фильма.",
                chat_id, message_id
            )
        except:
            pass
        
@bot.callback_query_handler(func=lambda call: call.data.startswith("confirm_remove:"))
def confirm_remove(call):
    try:
        bot.answer_callback_query(call.id)
    except:
        pass

    kp_id = call.data.split(":")[1]
    chat_id = call.message.chat.id

    with db_lock:
        cursor.execute('DELETE FROM movies WHERE chat_id = %s AND kp_id = %s', (chat_id, str(str(kp_id))))
        conn.commit()

    bot.edit_message_text(
        chat_id=chat_id,
        message_id=call.message.message_id,
        text="✅ Фильм удалён из базы.",
        reply_markup=None
    )

@bot.callback_query_handler(func=lambda call: call.data.startswith("back_to_film:"))
def back_to_film_description(call):
    try:
        bot.answer_callback_query(call.id)

        kp_id = int(call.data.split(":")[1])
        chat_id = call.message.chat.id
        user_id = call.from_user.id
        message_id = call.message.message_id

        # Получаем is_series из БД
        is_series = False
        try:
            with db_lock:
                cursor.execute("SELECT is_series FROM movies WHERE kp_id = %s", (str(kp_id),))
                row = cursor.fetchone()
                if row:
                    is_series = bool(row[0] if isinstance(row, tuple) else row.get('is_series'))
        except Exception as e:
            logger.warning(f"[BACK TO FILM] Не удалось получить is_series: {e}")

        # Ссылка на Кинопоиск (для текста, но не для парсинга)
        link = f"https://www.kinopoisk.ru/{'series' if is_series else 'film'}/{kp_id}/"

        # Пытаемся взять info из кэша или БД (если у тебя есть кэш — используй его)
        info = None
        try:
            # Если есть кэш в глобальной переменной (рекомендую добавить в будущем)
            if 'film_info_cache' in globals():
                info = film_info_cache.get(str(kp_id))
            
            # Если кэша нет — берём базовую инфу из БД
            if not info:
                with db_lock:
                    cursor.execute("""
                        SELECT title, year, description, director, genres, actors 
                        FROM movies 
                        WHERE kp_id = %s
                    """, (kp_id,))
                    row = cursor.fetchone()
                    if row:
                        info = {
                            'title': row[0] if isinstance(row, tuple) else row.get('title'),
                            'year': row[1] if isinstance(row, tuple) else row.get('year'),
                            'description': row[2] if isinstance(row, tuple) else row.get('description'),
                            'director': row[3] if isinstance(row, tuple) else row.get('director'),
                            'genres': row[4] if isinstance(row, tuple) else row.get('genres'),
                            'actors': row[5] if isinstance(row, tuple) else row.get('actors'),
                            'is_series': is_series
                        }
        except Exception as e:
            logger.warning(f"[BACK TO FILM] Не удалось взять info из БД: {e}")

        # Если info всё равно нет — показываем минимальное сообщение
        if not info:
            bot.edit_message_text(
                f"🎬 Фильм/сериал на Кинопоиске\n\n<a href='{link}'>Открыть на Кинопоиске</a>",
                chat_id,
                message_id,
                parse_mode='HTML',
                disable_web_page_preview=False
            )
            return

        # Вызываем твою основную функцию отображения
        from moviebot.bot.handlers.series import show_film_info_with_buttons
        show_film_info_with_buttons(
            chat_id=chat_id,
            user_id=user_id,
            info=info,
            link=link,
            kp_id=kp_id,
            message_id=message_id,
            existing=None  # или передай, если знаешь, что фильм уже в базе
        )

        logger.info(f"[BACK TO FILM] Успешный возврат к описанию kp_id={kp_id}")

    except Exception as e:
        logger.error(f"[BACK TO FILM] Ошибка: {e}", exc_info=True)
        bot.answer_callback_query(call.id, "Ошибка возврата", show_alert=True)

@bot.callback_query_handler(func=lambda call: call.data == "delete_this_message")
def delete_recommendations_message(call):
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id)
    except Exception as e:
        logger.warning(f"[DELETE MESSAGE] Не удалось удалить: {e}")

def register_film_callbacks(bot):
    """Регистрирует callback handlers для карточки фильма (уже зарегистрированы через декораторы)"""
    # Handlers уже зарегистрированы через декораторы @bot.callback_query_handler
    # при импорте модуля, поэтому эта функция просто для совместимости
    pass


